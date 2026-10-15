package bigquery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dtql"
)

func TestDiscovery_CoverageGaps(t *testing.T) {
	t.Parallel()

	// 1. NewDiscoveryClient with nil clock sets realClock
	locator := DiscoveryLocator{SourceID: "src-1", SourceProject: "valid-project", DatasetID: "ds", TableID: "tbl"}
	cfg := DiscoveryConfig{
		Allowlist:    []DiscoveryLocator{locator},
		Authorize:    func(context.Context, DiscoveryLocator) (DiscoveryBinding, error) { return DiscoveryBinding{}, nil },
		Provider:     &testProvider{},
		Transport:    rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		EvidenceKind: "provider-metadata",
		Clock:        nil, // hits line 96: c.clock = realClock{}
	}
	client, err := NewDiscoveryClient(cfg)
	if err != nil || client.clock == nil {
		t.Fatalf("expected realClock set, got %v, err=%v", client, err)
	}

	// 2. binding: empty PolicyRevision returns auth_required (line 116)
	cfgEmptyRev := cfg
	cfgEmptyRev.Authorize = func(context.Context, DiscoveryLocator) (DiscoveryBinding, error) {
		return DiscoveryBinding{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, PolicyRevision: ""}, nil
	}
	clientEmptyRev, _ := NewDiscoveryClient(cfgEmptyRev)
	if _, err := clientEmptyRev.Discover(context.Background(), "src-1", DefaultBounds()); errorCode(err) != "auth_required" {
		t.Fatalf("expected auth_required for empty PolicyRevision, got %v", err)
	}

	// 3. discoveryProvider.Authorize error branch (line 247)
	providerErr := &failingProvider{err: errors.New("provider auth fail")}
	cfgFailAuth := cfg
	cfgFailAuth.Provider = providerErr
	cfgFailAuth.Authorize = func(context.Context, DiscoveryLocator) (DiscoveryBinding, error) {
		return DiscoveryBinding{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, PolicyRevision: "rev-1"}, nil
	}
	clientFailAuth, _ := NewDiscoveryClient(cfgFailAuth)
	if _, err := clientFailAuth.Discover(context.Background(), "src-1", DefaultBounds()); err == nil {
		t.Fatal("expected error on provider auth fail")
	}

	// 4. discoveryGate.RoundTrip validation error branches:
	// q.Get("view") != "STORAGE_STATS" for table (line 298)
	gate := discoveryGate{
		provider: discoveryProvider{
			locator: locator,
		},
	}
	badViewReq := httptest.NewRequest("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/valid-project/datasets/ds/tables/tbl?view=WRONG", nil)
	if _, err := gate.RoundTrip(badViewReq); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for wrong view param, got %v", err)
	}

	// view param on dataset URL (line 308)
	badDatasetReq := httptest.NewRequest("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/valid-project/datasets/ds?datasetView=METADATA&view=STORAGE_STATS", nil)
	if _, err := gate.RoundTrip(badDatasetReq); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for view on dataset URL, got %v", err)
	}

	// 5. publicDiscoverySchema errors:
	// item not object in fields (line 347)
	badFieldItem := map[string]any{"fields": []any{"not-a-map"}}
	if _, err := publicDiscoverySchema(badFieldItem); err == nil {
		t.Fatal("expected error for non-map field item")
	}

	// missing type in field (line 356)
	missingTypeField := map[string]any{"fields": []any{map[string]any{"name": "col1"}}}
	if _, err := publicDiscoverySchema(missingTypeField); err == nil {
		t.Fatal("expected error for missing type")
	}
}

type failingProvider struct {
	err error
}

func (f *failingProvider) Authorize(context.Context, http.RoundTripper) (Identity, http.RoundTripper, error) {
	return Identity{}, nil, f.err
}

// Not parallel: mutates seams in reader.go.
func TestReader_Resume_SeamErrorBranches(t *testing.T) {
	data, err := os.ReadFile("testdata/contract/http-state-r3.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Profile   SourceProfile `json:"profile"`
		Query     string        `json:"query"`
		Execution Execution     `json:"execution"`
		Bounds    Bounds        `json:"bounds"`
		HTTP      []struct {
			Status int    `json:"status"`
			Body   string `json:"body"`
		} `json:"http"`
	}
	if err = json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	sc := fixtures[0]
	q, err := dtql.Deserialize([]byte(sc.Query))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Compile(sc.Profile, q)
	if err != nil {
		t.Fatal(err)
	}

	newClientAndPage := func() (*Client, Page) {
		calls := 0
		client, err := NewClient(Config{
			Profiles: []SourceProfile{sc.Profile},
			Provider: &testProvider{identity: Identity{Principal: sc.Execution.Principal, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
			Ledger:   NewMemoryLedger(),
			Prepare:  func(context.Context) (ReadPlan, string, error) { return plan, "policy", nil },
			Transport: rt(func(req *http.Request) (*http.Response, error) {
				step := sc.HTTP[calls]
				calls++
				return jsonResp(step.Status, json.RawMessage(step.Body))
			}),
		})
		if err != nil {
			t.Fatal(err)
		}
		preview, _ := client.Preview(context.Background(), plan, sc.Execution, sc.Bounds)
		approval, _ := client.Approve(preview, preview.ApprovalDigest)
		run, _ := client.Execute(context.Background(), approval)
		page, _ := run.NextPage()
		return client, page
	}

	// 1. loadRunFn error (line 516)
	c1, p1 := newClientAndPage()
	oldLoad := loadRunFn
	oldLease := ledgerLeaseFn
	defer func() {
		loadRunFn = oldLoad
		ledgerLeaseFn = oldLease
	}()

	ledgerLeaseFn = func(l Ledger, ctx context.Context, id string) (func(), error) {
		loadRunFn = func(c *Client, id string) (runRecord, error) {
			return runRecord{}, errors.New("simulated loadRun error")
		}
		return oldLease(l, ctx, id)
	}
	_, err = c1.Resume(context.Background(), p1.Receipt, p1.Cursor)
	if err == nil || !strings.Contains(err.Error(), "simulated loadRun error") {
		t.Fatalf("expected simulated loadRun error, got %v", err)
	}
	loadRunFn = oldLoad
	ledgerLeaseFn = oldLease

	// 2. Cursor mismatch in ledger record (line 518)
	c2, p2 := newClientAndPage()
	ledgerLeaseFn = func(l Ledger, ctx context.Context, id string) (func(), error) {
		loadRunFn = func(c *Client, id string) (runRecord, error) {
			rec, err := oldLoad(c, id)
			rec.Cursor = nil
			return rec, err
		}
		return oldLease(l, ctx, id)
	}
	if _, err := c2.Resume(context.Background(), p2.Receipt, p2.Cursor); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid when ledger cursor mutated, got %v", err)
	}
	loadRunFn = oldLoad
	ledgerLeaseFn = oldLease

	// 3. preparationFn error (line 523)
	c3, p3 := newClientAndPage()
	oldPrep := preparationFn
	defer func() { preparationFn = oldPrep }()
	preparationFn = func(c *Client, ctx context.Context, scope *operationScope, control bool) (context.Context, context.CancelFunc, error) {
		return nil, nil, errors.New("simulated preparation error")
	}
	if _, err := c3.Resume(context.Background(), p3.Receipt, p3.Cursor); err == nil || !strings.Contains(err.Error(), "simulated preparation error") {
		t.Fatalf("expected simulated preparation error, got %v", err)
	}
	preparationFn = oldPrep

	// 4. mutateRunContextFn error on cursor update (line 536)
	c4, p4 := newClientAndPage()
	oldMutate := mutateRunContextFn
	defer func() { mutateRunContextFn = oldMutate }()
	callCount := 0
	mutateRunContextFn = func(c *Client, ctx context.Context, id string, fn func(*runRecord) error) error {
		callCount++
		if callCount == 2 { // line 534
			return errors.New("simulated mutate error")
		}
		return oldMutate(c, ctx, id, fn)
	}
	var cur cursorState
	raw, _ := base64.RawURLEncoding.DecodeString(p4.Cursor)
	_ = json.Unmarshal(raw, &cur)
	cur.PageDigest = ""
	encodedRaw, _ := json.Marshal(cur)
	encoded := base64.RawURLEncoding.EncodeToString(encodedRaw)
	_ = c4.mutateRunContext(context.Background(), p4.Receipt.RunID, func(r *runRecord) error {
		r.Cursor = &cur
		return nil
	})
	if _, err := c4.Resume(context.Background(), p4.Receipt, encoded); err == nil || !strings.Contains(err.Error(), "simulated mutate error") {
		t.Fatalf("expected simulated mutate error, got %v", err)
	}
	mutateRunContextFn = oldMutate
}

func TestSourceRows_CoverageGaps_Remaining(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// 1. OpenSourceRows: provider identity fails (line 41)
	providerErr := &failingProvider{err: errors.New("auth fail")}
	dbFailAuth, _ := NewReadOnlyDatabase(SourceConfig{
		ProjectID: "fixture-project", DatasetID: "fixture_dataset",
		Provider: providerErr, Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
	})
	if _, err := dbFailAuth.OpenSourceRows(ctx, ptr(dal.NewCollectionRef("sample", "", nil))); err == nil {
		t.Fatal("expected error on auth fail")
	}

	// 2. OpenSourceRows: tableMetadata fails (line 50)
	dbTableMetaFail := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusNotFound, map[string]any{"error": map[string]any{"code": 404, "message": "table missing"}})
	}))
	if _, err := dbTableMetaFail.OpenSourceRows(ctx, ptr(dal.NewCollectionRef("sample", "", nil))); err == nil {
		t.Fatal("expected error on tableMetadata fail")
	}

	// 3. OpenSourceRows: numRows not a number (line 64)
	dbBadNumRows := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
			"type":             "TABLE",
			"etag":             "etag-1",
			"lastModifiedTime": "1000",
			"numRows":          "not-a-number",
			"schema":           map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
		})
	}))
	if _, err := dbBadNumRows.OpenSourceRows(ctx, ptr(dal.NewCollectionRef("sample", "", nil))); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for invalid numRows, got %v", err)
	}

	// 4. OpenSourceRows: missing etag or lastModifiedTime (lines 73, 78)
	dbMissingEtag := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
			"type":             "TABLE",
			"lastModifiedTime": "1000",
			"numRows":          "1",
			"schema":           map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
		})
	}))
	if _, err := dbMissingEtag.OpenSourceRows(ctx, ptr(dal.NewCollectionRef("sample", "", nil))); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for missing etag, got %v", err)
	}

	// 5. sourceDecodeRow: object(item) error (line 235) and missing "v" (line 239)
	field := []Field{{Name: "id", Type: "INT64"}}
	if _, err := sourceDecodeRow(map[string]any{"f": []any{"not-an-object"}}, field); err == nil {
		t.Fatal("expected error for non-object cell")
	}
	if _, err := sourceDecodeRow(map[string]any{"f": []any{map[string]any{"wrong_key": "1"}}}, field); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for cell missing 'v', got %v", err)
	}

	// 6. DescribeSourceFields: tableMetadata error (line 437)
	dbFieldsMetaFail := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": 500, "message": "server error"}})
	}))
	if _, err := dbFieldsMetaFail.DescribeSourceFields(ctx, ptr(dal.NewCollectionRef("sample", "", nil))); err == nil {
		t.Fatal("expected error on tableMetadata failure in DescribeSourceFields")
	}
}
