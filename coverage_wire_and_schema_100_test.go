package bigquery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
)

func TestWire_DispatchGate_RoundTrip_Exhaustive(t *testing.T) {
	t.Parallel()

	// 1. GET attempts > 3 returns response_limit (line 88)
	gate := &dispatchGate{base: rt(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})}
	req, _ := http.NewRequest("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p/jobs", nil)
	for i := 0; i < 3; i++ {
		_, _ = gate.RoundTrip(req)
	}
	if _, err := gate.RoundTrip(req); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit on 4th GET, got %v", err)
	}

	// 2. beforeDispatch error (lines 106-107)
	gateErr := &dispatchGate{
		base: rt(func(req *http.Request) (*http.Response, error) { return nil, nil }),
		beforeDispatch: func() error {
			return errors.New("simulated beforeDispatch error")
		},
	}
	req2, _ := http.NewRequest("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p/jobs", nil)
	if _, err := gateErr.RoundTrip(req2); err == nil || !strings.Contains(err.Error(), "simulated beforeDispatch error") {
		t.Fatalf("expected beforeDispatch error, got %v", err)
	}

	// 3. Context canceled inside awaitOwnedDependency (lines 101, 111, 144)
	gateCanceled := &dispatchGate{
		base: rt(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled before call
	req3, _ := http.NewRequestWithContext(ctx, "GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p/jobs", nil)
	if _, err := gateCanceled.RoundTrip(req3); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on canceled req, got %v", err)
	}
}

func TestWire_BoundedTransport_ValidateResponse_Exhaustive(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{
		Profiles:  []SourceProfile{testProfile(t)},
		Provider:  &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:    NewMemoryLedger(),
		Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	scope := &operationScope{
		deadline: time.Now().Add(time.Hour),
		bounds:   Bounds{ResponseBytes: 1024, HTTPMs: 1000},
	}
	guard := &dispatchGate{}
	transport := &boundedTransport{
		c:        client,
		s:        scope,
		guard:    guard,
		ctx:      context.Background(),
		deadline: time.Now().Add(time.Hour),
	}

	// 1. resp == nil or resp.Body == nil (line 215)
	if _, err := transport.validateResponse(nil, nil); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for nil resp, got %v", err)
	}
	if _, err := transport.validateResponse(&http.Response{}, nil); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for resp with nil body, got %v", err)
	}

	// 2. Unsupported Content-Encoding (line 240)
	respBr := &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Encoding": []string{"br"}},
		Body:       io.NopCloser(strings.NewReader("{}")),
	}
	if _, err := transport.validateResponse(respBr, nil); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for br encoding, got %v", err)
	}

	// 3. Corrupt gzip body (line 244)
	respCorruptGz := &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Encoding": []string{"gzip"}},
		Body:       io.NopCloser(strings.NewReader("not-gzipped-bytes")),
	}
	if _, err := transport.validateResponse(respCorruptGz, nil); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for corrupt gzip, got %v", err)
	}

	// 4. Response exceeds limit (line 269)
	oversized := bytes.Repeat([]byte("a"), 2000)
	respOversized := &http.Response{
		StatusCode: 200,
		Header:     http.Header{},
		Body:       io.NopCloser(bytes.NewReader(oversized)),
	}
	if _, err := transport.validateResponse(respOversized, nil); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit for oversized resp, got %v", err)
	}

	// 5. JSON parse error (line 274)
	respBadJSON := &http.Response{
		StatusCode: 200,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader("invalid-json{")),
	}
	if _, err := transport.validateResponse(respBadJSON, nil); err == nil {
		t.Fatal("expected error for bad json")
	}

	// 6. JSON is array instead of object (line 278)
	respArrayJSON := &http.Response{
		StatusCode: 200,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader("[1, 2, 3]")),
	}
	if _, err := transport.validateResponse(respArrayJSON, nil); err == nil {
		t.Fatal("expected error for array json")
	}

	// 7. BoundedTransport.RoundTrip guard mismatch (line 196)
	req1, _ := http.NewRequest("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p/jobs", nil)
	guard.method = "POST" // mismatch
	if _, err := transport.RoundTrip(req1); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for guard mismatch, got %v", err)
	}
}

func TestWire_Call_RetryDate_And_Deadlines(t *testing.T) {
	t.Parallel()

	// 429 response with Retry-After as HTTP Date string (line 459)
	retryDate := time.Now().Add(500 * time.Millisecond).UTC().Format(http.TimeFormat)
	calls := 0
	client, err := NewClient(Config{
		Profiles: []SourceProfile{testProfile(t)},
		Provider: &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:   NewMemoryLedger(),
		Prepare:  func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
		Transport: rt(func(req *http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				resp := &http.Response{
					StatusCode: 429,
					Header:     http.Header{"Retry-After": []string{retryDate}},
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":429}}`)),
				}
				return resp, nil
			}
			return &http.Response{
				StatusCode: 200,
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader(`{"kind":"bigquery#job"}`)),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	scope := &operationScope{
		deadline: time.Now().Add(10 * time.Second),
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 5000},
	}
	principal := Principal{Kind: "workload", Subject: "s", Generation: "1"}
	_, err = client.call(context.Background(), scope, &principal, false, func(api sdkAPI) error {
		_, e := api.service.Jobs.Get("valid-project", "j").Context(api.ctx).Do()
		return e
	})
	if err != nil {
		t.Fatalf("expected retry after date to succeed, got %v", err)
	}

	// Retry-After exceeding deadline (line 468)
	calls = 0
	scopeShort := &operationScope{
		deadline: time.Now().Add(100 * time.Millisecond), // short deadline
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 5000},
	}
	clientShort, _ := NewClient(Config{
		Profiles: []SourceProfile{testProfile(t)},
		Provider: &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:   NewMemoryLedger(),
		Prepare:  func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
		Transport: rt(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 429,
				Header:     http.Header{"Retry-After": []string{"30"}}, // 30 seconds > 100ms deadline
				Body:       io.NopCloser(strings.NewReader(`{"error":{"code":429}}`)),
			}, nil
		}),
	})
	_, err = clientShort.call(context.Background(), scopeShort, &principal, false, func(api sdkAPI) error {
		_, e := api.service.Jobs.Get("valid-project", "j").Context(api.ctx).Do()
		return e
	})
	if errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped when retry exceeds deadline, got %v", err)
	}
}

func TestWire_CheckIdentity_InvalidPrincipal(t *testing.T) {
	t.Parallel()

	// checkIdentity when provider returns invalid principal (line 511)
	client, err := NewClient(Config{
		Profiles:  []SourceProfile{testProfile(t)},
		Provider:  &testProvider{identity: Identity{Principal: Principal{Kind: "invalid-kind"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:    NewMemoryLedger(),
		Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	p := Principal{Kind: "workload", Subject: "s", Generation: "1"}
	if err := client.checkIdentity(context.Background(), p); errorCode(err) != "auth_required" {
		t.Fatalf("expected auth_required for invalid principal kind, got %v", err)
	}
}

func TestDiscovery_Gate_ViewParam_Coverage(t *testing.T) {
	t.Parallel()

	locator := DiscoveryLocator{SourceID: "src-1", SourceProject: "valid-project", DatasetID: "ds", TableID: "tbl"}
	gate := discoveryGate{provider: discoveryProvider{locator: locator}}

	// Line 298: table path with q.Get("view") != "STORAGE_STATS"
	reqTableWrongView, _ := http.NewRequest("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/valid-project/datasets/ds/tables/tbl?view=WRONG", nil)
	if _, err := gate.RoundTrip(reqTableWrongView); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for wrong view param on table, got %v", err)
	}

	// Line 308: dataset path with view param
	reqDatasetWithView, _ := http.NewRequest("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/valid-project/datasets/ds?datasetView=METADATA&view=STORAGE_STATS", nil)
	if _, err := gate.RoundTrip(reqDatasetWithView); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for view param on dataset, got %v", err)
	}

	// Line 312: provider check returns error (sanitized to auth_required)
	discClient := &DiscoveryClient{
		authorize: func(context.Context, DiscoveryLocator) (DiscoveryBinding, error) {
			return DiscoveryBinding{}, errors.New("simulated auth check error")
		},
	}
	gateRevoked := discoveryGate{
		provider: discoveryProvider{
			client:  discClient,
			locator: locator,
		},
	}
	reqValidTable, _ := http.NewRequest("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/valid-project/datasets/ds/tables/tbl?view=STORAGE_STATS", nil)
	if _, err := gateRevoked.RoundTrip(reqValidTable); errorCode(err) != "auth_required" {
		t.Fatalf("expected auth_required when check fails, got %v", err)
	}
}

func TestSourceRows_Cursor_VerifyFinal_ChangedBranches(t *testing.T) {
	t.Parallel()

	// 1. verifyFinal: tableMetadata fails (line 204)
	calls := 0
	dbFailFinalMeta := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		calls++
		switch {
		case strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset"):
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		case strings.HasSuffix(req.URL.Path, "/tables/sample") && calls <= 2:
			return jsonResp(http.StatusOK, map[string]any{
				"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
				"type":             "TABLE",
				"etag":             "etag-1",
				"lastModifiedTime": "1000",
				"numRows":          "1",
				"schema":           map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
			})
		case strings.Contains(req.URL.Path, "/data"):
			return jsonResp(http.StatusOK, map[string]any{
				"kind":      "bigquery#tableDataList",
				"totalRows": "1",
				"rows":      []any{map[string]any{"f": []any{map[string]any{"v": "1"}}}},
			})
		default: // verifyFinal tableMetadata call fails
			return jsonResp(http.StatusNotFound, map[string]any{"error": map[string]any{"code": 404, "message": "table deleted"}})
		}
	}))

	cur, err := dbFailFinalMeta.OpenSourceRows(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer cur.Close()
	// Next returns the 1 row
	if _, err := cur.Next(); err != nil {
		t.Fatal(err)
	}
	// Next triggers verifyFinal and returns error
	if _, err := cur.Next(); err == nil {
		t.Fatal("expected error on final tableMetadata failure")
	}

	// 2. verifyFinal: numRows changed (line 211)
	calls = 0
	dbChangedNumRows := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		calls++
		switch {
		case strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset"):
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		case strings.HasSuffix(req.URL.Path, "/tables/sample") && calls <= 2:
			return jsonResp(http.StatusOK, map[string]any{
				"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
				"type":             "TABLE",
				"etag":             "etag-1",
				"lastModifiedTime": "1000",
				"numRows":          "1",
				"schema":           map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
			})
		case strings.Contains(req.URL.Path, "/data"):
			return jsonResp(http.StatusOK, map[string]any{
				"kind":      "bigquery#tableDataList",
				"totalRows": "1",
				"rows":      []any{map[string]any{"f": []any{map[string]any{"v": "1"}}}},
			})
		default: // verifyFinal sees numRows changed to 2
			return jsonResp(http.StatusOK, map[string]any{
				"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
				"type":             "TABLE",
				"etag":             "etag-1",
				"lastModifiedTime": "1000",
				"numRows":          "2", // Changed!
				"schema":           map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
			})
		}
	}))

	cur2, err := dbChangedNumRows.OpenSourceRows(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer cur2.Close()
	if _, err := cur2.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := cur2.Next(); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed when numRows changed, got %v", err)
	}
}
