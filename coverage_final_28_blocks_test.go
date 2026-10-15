package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
)

// Not parallel: mutates discoveryObjectFn
func TestDiscovery_ObjectSeam_Branches(t *testing.T) {
	orig := discoveryObjectFn
	defer func() { discoveryObjectFn = orig }()

	// 1. datasetReference not an object (discovery.go:165)
	c1, _, _, _ := discoveryFixtureClient(t, rt(func(req *http.Request) (*http.Response, error) {
		return jsonResp(http.StatusOK, map[string]any{
			"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
			"location":         "US",
		})
	}))
	discoveryObjectFn = func(v any) (map[string]any, error) {
		m, err := orig(v)
		if err == nil && m["datasetReference"] != nil {
			m["datasetReference"] = 12345
		}
		return m, err
	}
	if _, err := c1.Discover(context.Background(), "fixture-source", DefaultBounds()); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 2. tableReference not an object (discovery.go:187)
	c2, _, _, _ := discoveryFixtureClient(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
			"location":       "US",
			"type":           "TABLE",
			"schema":         map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
		})
	}))
	discoveryObjectFn = func(v any) (map[string]any, error) {
		m, err := orig(v)
		if err == nil && m["tableReference"] != nil {
			m["tableReference"] = 12345
		}
		return m, err
	}
	if _, err := c2.Discover(context.Background(), "fixture-source", DefaultBounds()); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 3. publicDiscoverySchema error (discovery.go:206)
	c3, _, _, _ := discoveryFixtureClient(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
			"location":       "US",
			"type":           "TABLE",
			"schema":         map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
		})
	}))
	discoveryObjectFn = func(v any) (map[string]any, error) {
		m, err := orig(v)
		if err == nil && m["tableReference"] != nil {
			m["schema"] = "invalid-schema"
		}
		return m, err
	}
	if _, err := c3.Discover(context.Background(), "fixture-source", DefaultBounds()); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}
}

// Not parallel: mutates sourceGetFn
func TestSourceSchema_And_Rows_SourceGetFn_Branches(t *testing.T) {
	origGet := sourceGetFn
	defer func() { sourceGetFn = origGet }()

	baseDB := func() *ReadOnlyDatabase {
		return sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
			path := req.URL.Path
			switch {
			case strings.HasSuffix(path, "/fixture_dataset"):
				return jsonResp(http.StatusOK, map[string]any{"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}})
			case strings.HasSuffix(path, "/tables/sample"):
				return jsonResp(http.StatusOK, map[string]any{
					"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
					"type":             "TABLE",
					"etag":             "etag-1",
					"lastModifiedTime": "1000",
					"numRows":          "1",
					"schema":           map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
				})
			default:
				return jsonResp(http.StatusOK, map[string]any{})
			}
		}))
	}

	db := baseDB()
	ctx := context.Background()
	principal := Principal{Kind: "workload", Subject: "reader", Generation: "1"}

	// 1. source_schema.go:51 (tables not array)
	sourceGetFn = func(d *sourceDatabase, ctx context.Context, p Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{"tables": "not-an-array", "datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}}, 10, nil
	}
	if _, err := db.source.relations(ctx, principal); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 2. source_schema.go:56 (item not object)
	sourceGetFn = func(d *sourceDatabase, ctx context.Context, p Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{"tables": []any{"not-an-object"}, "datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}}, 10, nil
	}
	if _, err := db.source.relations(ctx, principal); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 3. source_schema.go:60 (tableReference not object)
	sourceGetFn = func(d *sourceDatabase, ctx context.Context, p Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{"tables": []any{map[string]any{"tableReference": "not-an-object"}}, "datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}}, 10, nil
	}
	if _, err := db.source.relations(ctx, principal); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 4. source_schema.go:81 (nextPageToken invalid)
	sourceGetFn = func(d *sourceDatabase, ctx context.Context, p Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{
			"tables": []any{map[string]any{
				"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "t1"},
				"type":           "TABLE",
			}},
			"totalItems":       json.Number("1"),
			"nextPageToken":    12345, // invalid token type!
			"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
		}, 10, nil
	}
	if _, err := db.source.relations(ctx, principal); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 5. source_schema.go:144 (view not object)
	sourceGetFn = func(d *sourceDatabase, ctx context.Context, p Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{
			"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
			"tables": []any{map[string]any{
				"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "v1"},
				"type":           "VIEW",
			}},
			"totalItems":     json.Number("1"),
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "v1"},
			"type":           "VIEW",
			"view":           "not-an-object",
		}, 10, nil
	}
	if _, err := db.ListSourceViews(ctx); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 6. source_schema.go:151 (view fields error)
	sourceGetFn = func(d *sourceDatabase, ctx context.Context, p Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{
			"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
			"tables": []any{map[string]any{
				"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "v1"},
				"type":           "VIEW",
			}},
			"totalItems":     json.Number("1"),
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "v1"},
			"type":           "VIEW",
			"view":           map[string]any{"query": "SELECT 1"},
			"schema":         "invalid-schema",
		}, 10, nil
	}
	if _, err := db.ListSourceViews(ctx); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 7. source_schema.go:397 (DescribeCollection fields error)
	sourceGetFn = func(d *sourceDatabase, ctx context.Context, p Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
			"type":           "TABLE",
			"schema":         "invalid-schema",
		}, 10, nil
	}
	if _, err := db.DescribeCollection(ctx, ptr(dal.NewCollectionRef("sample", "", nil))); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 8. source_schema.go:401 (DescribeCollection constraints error)
	sourceGetFn = func(d *sourceDatabase, ctx context.Context, p Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{
			"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
			"type":             "TABLE",
			"schema":           map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
			"tableConstraints": "invalid-constraints",
		}, 10, nil
	}
	if _, err := db.DescribeCollection(ctx, ptr(dal.NewCollectionRef("sample", "", nil))); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 9. source_rows.go:64 (OpenSourceRows numRows error)
	sourceGetFn = func(d *sourceDatabase, ctx context.Context, p Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{
			"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
			"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
			"type":             "TABLE",
			"etag":             "etag-1",
			"lastModifiedTime": "1000",
			"schema":           map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
			// numRows is missing!
		}, 10, nil
	}
	if _, err := db.OpenSourceRows(ctx, ptr(dal.NewCollectionRef("sample", "", nil))); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 10. source_rows.go:166 (loadPage totalRows error)
	sourceGetFn = origGet
	cur, err := db.OpenSourceRows(ctx, ptr(dal.NewCollectionRef("sample", "", nil)))
	if err != nil {
		t.Fatalf("OpenSourceRows failed: %v", err)
	}
	defer cur.Close()
	sourceGetFn = func(d *sourceDatabase, ctx context.Context, p Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{
			"kind": "bigquery#tableDataList",
			// totalRows is missing!
		}, 10, nil
	}
	if _, err := cur.Next(); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 11. source_rows.go:173 (loadPage rows not an array)
	sourceGetFn = func(d *sourceDatabase, ctx context.Context, p Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{
			"kind":      "bigquery#tableDataList",
			"totalRows": "1",
			"rows":      "not-an-array",
		}, 10, nil
	}
	if _, err := cur.Next(); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 12. source_rows.go:188 (loadPage pageToken error)
	sourceGetFn = func(d *sourceDatabase, ctx context.Context, p Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{
			"kind":      "bigquery#tableDataList",
			"totalRows": "1",
			"rows":      []any{map[string]any{"f": []any{map[string]any{"v": "1"}}}},
			"pageToken": 12345, // not a string!
		}, 10, nil
	}
	if _, err := cur.Next(); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 13. source_rows.go:211 (verifyFinal count != expected)
	sourceGetFn = origGet
	cur2, err := db.OpenSourceRows(ctx, ptr(dal.NewCollectionRef("sample", "", nil)))
	if err != nil {
		t.Fatalf("OpenSourceRows failed: %v", err)
	}
	defer cur2.Close()
	sourceGetFn = func(d *sourceDatabase, ctx context.Context, p Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{
			"kind":             "bigquery#tableDataList",
			"totalRows":        "1",
			"rows":             []any{map[string]any{"f": []any{map[string]any{"v": "1"}}}},
			"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
			"type":             "TABLE",
			"etag":             "etag-1",
			"lastModifiedTime": "1000",
			"numRows":          "999", // count != expected!
			"schema":           map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
		}, 10, nil
	}
	_, _ = cur2.Next()
	if _, err := cur2.Next(); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed on count mismatch in verifyFinal, got %v", err)
	}
}

// Not parallel: mutates normalizeScalarFn
func TestSourceDecodeRow_CustomType_DefaultBranch(t *testing.T) {
	orig := normalizeScalarFn
	defer func() { normalizeScalarFn = orig }()

	normalizeScalarFn = func(Field, any) (Cell, error) {
		return Cell{Type: "CUSTOM_UNKNOWN", Value: "val"}, nil
	}
	_, err := sourceDecodeRow(map[string]any{"f": []any{map[string]any{"v": "val"}}}, []Field{{Name: "col", Type: "CUSTOM_UNKNOWN"}})
	if errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type for custom type default branch, got %v", err)
	}
}

type countCancelCtx struct {
	context.Context
	errCalls atomic.Int32
}

func (c *countCancelCtx) Err() error {
	if c.errCalls.Add(1) >= 5 {
		return context.Canceled
	}
	return nil
}

// Not parallel: exercises admission slots and deadlines
func TestWire_DispatchGate_And_BoundedTransport_RemainingBlocks(t *testing.T) {
	// 1. line 101/110: dispatchGate.RoundTrip with context cancelled in beforeDispatch
	ctxCanceled, cancel := context.WithCancel(context.Background())
	gate := &dispatchGate{
		base: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		beforeDispatch: func() error {
			cancel()
			return nil
		},
	}
	reqCanceled, _ := http.NewRequestWithContext(ctxCanceled, "GET", "https://bigquery.googleapis.com/bigquery/v2/test", nil)
	_, err := gate.RoundTrip(reqCanceled)
	if errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on canceled req context, got %v", err)
	}

	// 2. line 144: dispatchGate.RoundTrip context cancelled after roundtrip, closes resp body
	bodyClosed := make(chan struct{})
	cb := io.NopCloser(strings.NewReader("{}"))
	ctxReq := &countCancelCtx{Context: context.Background()}
	gate2 := &dispatchGate{base: rt(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: &trackedCloseReader{ReadCloser: cb, onClose: func() { close(bodyClosed) }}}, nil
	})}
	req2, _ := http.NewRequestWithContext(ctxReq, "GET", "https://bigquery.googleapis.com/bigquery/v2/test", nil)
	_, err = gate2.RoundTrip(req2)
	if errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped, got %v", err)
	}
	select {
	case <-bodyClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("expected body to be closed")
	}

	// 3. line 401: gate.beforeDispatch remaining <= 0
	client, _ := NewClient(Config{
		Profiles:  []SourceProfile{testProfile(t)},
		Provider:  &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:    NewMemoryLedger(),
		Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	scopeZeroBytes := &operationScope{
		bounds: Bounds{ResponseBytes: 1024, TotalResponseBytes: 0, HTTPMs: 1000},
	}
	principal := Principal{Kind: "workload", Subject: "s", Generation: "1"}
	_, err = client.call(context.Background(), scopeZeroBytes, &principal, false, func(sdkAPI) error { return nil })
	if errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit when remaining <= 0, got %v", err)
	}

	// 4. line 434: client.call with ctx.Err() != nil and non-*Error
	ctxCanc, cancelCall := context.WithCancel(context.Background())
	_, err = client.call(ctxCanc, newPreviewScope(DefaultBounds(), time.Now()), &principal, false, func(sdkAPI) error {
		cancelCall()
		return errors.New("non-safe error")
	})
	if errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped, got %v", err)
	}

	// 5. line 471: retry backoff clock.Sleep returns error on canceled context
	fakeCl := &fakeClock{now: time.Now()}
	clientRetry, _ := NewClient(Config{
		Profiles: []SourceProfile{testProfile(t)},
		Provider: &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:   NewMemoryLedger(),
		Clock:    fakeCl,
		Transport: rt(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 500, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("{}"))}, nil
		}),
		Prepare: func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	ctxRetry, cancelRetry := context.WithCancel(context.Background())
	_, err = clientRetry.call(ctxRetry, newPreviewScope(DefaultBounds(), fakeCl.Now()), &principal, false, func(api sdkAPI) error {
		cancelRetry() // cancel context so Sleep returns error
		_, e := api.service.Jobs.Get("valid-project", "j").Context(api.ctx).Do()
		return e
	})
	if err == nil {
		t.Fatal("expected error from cancelled retry sleep")
	}

	// 6. lines 261 & 285 in boundedTransport: deadline expired after charge and ParseJSON
	btExp := &boundedTransport{
		c:        client,
		s:        newPreviewScope(DefaultBounds(), time.Now()),
		ctx:      context.Background(),
		deadline: time.Now().Add(-time.Second), // expired!
	}
	respValid := &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}
	_, err = btExp.validateResponse(respValid, nil)
	if errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on expired deadline in validateResponse, got %v", err)
	}
}

// Not parallel: mutates loadRunFn, preparationFn, reauthorizeFn, callFn, mutateRunContextFn
func TestReader_Ensure_ClockSleep_Error(t *testing.T) {
	origLoad := loadRunFn
	origPrep := preparationFn
	origReauth := reauthorizeFn
	origCall := callFn
	origMutate := mutateRunContextFn
	defer func() {
		loadRunFn = origLoad
		preparationFn = origPrep
		reauthorizeFn = origReauth
		callFn = origCall
		mutateRunContextFn = origMutate
	}()

	fakeCl := &fakeClock{now: time.Now()}
	client, _ := NewClient(Config{
		Profiles:  []SourceProfile{testProfile(t)},
		Provider:  &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:    NewMemoryLedger(),
		Clock:     fakeCl,
		Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})

	loadRunFn = func(*Client, string) (runRecord, error) {
		return runRecord{
			ActivePrincipal: Principal{Kind: "workload", Subject: "s", Generation: "1"},
			Receipt: Receipt{
				ExecutionDeadline: time.Now().Add(time.Hour),
				Bounds:            Bounds{MaxRows: 10, PageSize: 10},
				Job:               &JobRef{ProjectID: "valid-project", JobID: "j", Location: "US"},
			},
		}, nil
	}
	preparationFn = func(*Client, context.Context, *operationScope, bool) (context.Context, context.CancelFunc, error) {
		return context.Background(), context.CancelFunc(func() {}), nil
	}
	reauthorizeFn = func(*Client, context.Context, ReadPlan, string) error {
		return nil
	}
	mutateRunContextFn = func(*Client, context.Context, string, func(*runRecord) error) error {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	callFn = func(*Client, context.Context, *operationScope, *Principal, bool, func(sdkAPI) error) (any, error) {
		cancel() // cancel ctx so clock.Sleep fails
		return map[string]any{
			"jobComplete":         false,
			"totalBytesProcessed": "0",
			"totalBytesBilled":    "0",
		}, nil
	}

	r := &Run{
		c:      client,
		ctx:    ctx,
		id:     "test-run",
		access: Principal{Kind: "workload", Subject: "s", Generation: "1"},
	}
	err := r.ensure()
	if errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on clock.Sleep error, got %v", err)
	}
}

type trackedCloseReader struct {
	io.ReadCloser
	onClose func()
}

func (t *trackedCloseReader) Close() error {
	if t.onClose != nil {
		t.onClose()
	}
	return t.ReadCloser.Close()
}
