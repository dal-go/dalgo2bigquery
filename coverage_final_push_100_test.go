package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/dtql"
)

func ptr[T any](v T) *T {
	return &v
}

func TestSourceSchema_ParseListTotal(t *testing.T) {
	t.Parallel()

	// Not a json.Number
	if _, err := parseListTotal("123"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for string, got %v", err)
	}

	// Invalid number string
	if _, err := parseListTotal(json.Number("not-a-number")); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for invalid number, got %v", err)
	}

	// Negative number
	if _, err := parseListTotal(json.Number("-5")); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for negative number, got %v", err)
	}

	// Valid number
	val, err := parseListTotal(json.Number("42"))
	if err != nil || val != 42 {
		t.Fatalf("expected 42, got %v, err=%v", val, err)
	}
}

func TestSourceSchema_UnsupportedMethods(t *testing.T) {
	t.Parallel()

	db := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("no transport")
	}))

	ctx := context.Background()
	ref := ptr(dal.NewCollectionRef("sample", "", nil))

	// ListIndexes
	if _, err := db.ListIndexes(ctx, ref); err == nil || !strings.Contains(err.Error(), "search and vector indexes are not exposed") {
		t.Fatalf("expected NotSupportedError for ListIndexes, got %v", err)
	}

	// ListReferrers
	if _, err := db.ListReferrers(ctx, ref); err == nil || !strings.Contains(err.Error(), "no reverse constraint catalog") {
		t.Fatalf("expected NotSupportedError for ListReferrers, got %v", err)
	}
}

func TestSourceSchema_DescribeSourceFields_And_ListConstraints(t *testing.T) {
	t.Parallel()

	dummyTransport := rt(func(*http.Request) (*http.Response, error) { return nil, errors.New("no mock") })
	db := sourceTestDatabase(t, dummyTransport)
	ctx := context.Background()
	if _, err := db.DescribeSourceFields(ctx, nil); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for nil ref, got %v", err)
	}
	if _, err := db.DescribeSourceFields(ctx, ptr(dal.NewCollectionRef("bad name!", "", nil))); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad name, got %v", err)
	}
	if _, err := db.ListConstraints(ctx, nil); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for ListConstraints nil ref, got %v", err)
	}

	// Success branch with mock transport
	step := 0
	transport := rt(func(req *http.Request) (*http.Response, error) {
		step++
		switch {
		case strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset"):
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		case strings.HasSuffix(req.URL.Path, "/tables/sample"):
			return jsonResp(http.StatusOK, map[string]any{
				"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
				"type":             "TABLE",
				"etag":             "etag-1",
				"lastModifiedTime": "1000",
				"numRows":          "1",
				"schema": map[string]any{
					"fields": []any{
						map[string]any{"name": "id", "type": "INT64", "mode": "REQUIRED", "description": "stable id"},
						map[string]any{"name": "score", "type": "FLOAT64", "mode": "NULLABLE"},
						map[string]any{"name": "active", "type": "BOOL", "mode": "NULLABLE"},
					},
				},
				"tableConstraints": map[string]any{
					"primaryKey": map[string]any{"columns": []string{"id"}},
					"foreignKeys": []any{
						map[string]any{
							"name":            "fk_parent",
							"referencedTable": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "parent"},
							"columnReferences": []any{
								map[string]any{"referencingColumn": "id", "referencedColumn": "id"},
							},
						},
					},
				},
			})
		default:
			return nil, fmt.Errorf("unexpected URL: %s", req.URL.Path)
		}
	})

	db = sourceTestDatabase(t, transport)
	ref := ptr(dal.NewCollectionRef("sample", "", nil))

	fields, err := db.DescribeSourceFields(ctx, ref)
	if err != nil || len(fields) != 3 {
		t.Fatalf("expected 3 fields, got %v, err=%v", fields, err)
	}
	if fields[0].Description != "stable id" {
		t.Fatalf("expected description 'stable id', got %q", fields[0].Description)
	}

	constraints, err := db.ListConstraints(ctx, ref)
	if err != nil || len(constraints) != 2 {
		t.Fatalf("expected 2 constraints (PK + FK), got %v, err=%v", constraints, err)
	}
	if constraints[0].Type != "primary-key" || constraints[1].Type != "foreign-key" {
		t.Fatalf("unexpected constraints: %+v", constraints)
	}
}

func TestSourceSchema_Relations_ErrorBranches(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// 1. Dataset metadata fails
	dbFailsDataset := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		return jsonResp(http.StatusNotFound, map[string]any{"error": map[string]any{"code": 404, "message": "dataset not found"}})
	}))
	if _, err := dbFailsDataset.ListCollections(ctx, nil); err == nil {
		t.Fatal("expected error when dataset metadata fails")
	}

	// 2. malformed tables.list response: "tables" is not a slice
	dbBadTables := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"totalItems": json.Number("1"),
			"tables":     "not-a-slice",
		})
	}))
	if _, err := dbBadTables.ListCollections(ctx, nil); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-slice tables, got %v", err)
	}

	// 3. table with ineligible kind (e.g. "MODEL")
	dbIneligible := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"totalItems": json.Number("1"),
			"tables": []any{
				map[string]any{
					"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "model1"},
					"type":           "MODEL",
				},
			},
		})
	}))
	if _, err := dbIneligible.ListCollections(ctx, nil); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for MODEL, got %v", err)
	}

	// 4. totalItems mismatch (e.g. totalItems says 2, but only 1 table returned)
	dbTotalMismatch := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"totalItems": json.Number("2"),
			"tables": []any{
				map[string]any{
					"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "t1"},
					"type":           "TABLE",
				},
			},
		})
	}))
	if _, err := dbTotalMismatch.ListCollections(ctx, nil); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed for total mismatch, got %v", err)
	}

	// 5. Duplicate tableId in tables list
	dbDuplicate := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"totalItems": json.Number("2"),
			"tables": []any{
				map[string]any{
					"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "t1"},
					"type":           "TABLE",
				},
				map[string]any{
					"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "t1"},
					"type":           "TABLE",
				},
			},
		})
	}))
	if _, err := dbDuplicate.ListCollections(ctx, nil); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed for duplicate tableId, got %v", err)
	}

	// 6. Wrong project ID in tableReference
	dbWrongProject := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"totalItems": json.Number("1"),
			"tables": []any{
				map[string]any{
					"tableReference": map[string]any{"projectId": "other-project", "datasetId": "fixture_dataset", "tableId": "t1"},
					"type":           "TABLE",
				},
			},
		})
	}))
	if _, err := dbWrongProject.ListCollections(ctx, nil); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed for wrong project ID, got %v", err)
	}

	// 7. Success for ListSourceViews
	dbViews := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		switch {
		case strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset"):
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		case strings.HasSuffix(req.URL.Path, "/tables"):
			return jsonResp(http.StatusOK, map[string]any{
				"totalItems": json.Number("2"),
				"tables": []any{
					map[string]any{
						"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "tbl1"},
						"type":           "TABLE",
					},
					map[string]any{
						"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "view1"},
						"type":           "VIEW",
					},
				},
			})
		case strings.HasSuffix(req.URL.Path, "/tables/view1"):
			return jsonResp(http.StatusOK, map[string]any{
				"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "view1"},
				"type":           "VIEW",
				"view":           map[string]any{"query": "SELECT 1 as x"},
				"schema": map[string]any{
					"fields": []any{
						map[string]any{"name": "x", "type": "INT64", "mode": "NULLABLE"},
					},
				},
			})
		default:
			return nil, fmt.Errorf("unexpected path: %s", req.URL.Path)
		}
	}))
	views, err := dbViews.ListSourceViews(ctx)
	if err != nil || len(views) != 1 || views[0].Name != "view1" {
		t.Fatalf("expected 1 view 'view1', got %v, err=%v", views, err)
	}
}

func TestSourceRows_DecodeTypes_And_Errors(t *testing.T) {
	t.Parallel()

	// 1. Unsupported type in schema
	unsupportedField := []Field{{Name: "col", Type: "INTERVAL", Mode: "NULLABLE"}}
	rawRow := map[string]any{"f": []any{map[string]any{"v": "1"}}}
	if _, err := sourceDecodeRow(rawRow, unsupportedField); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type, got %v", err)
	}

	// 2. malformed wire: row not an object
	if _, err := sourceDecodeRow("not-a-map", unsupportedField); err == nil {
		t.Fatal("expected error for non-map row")
	}

	// 3. malformed wire: len(f) != len(fields)
	if _, err := sourceDecodeRow(map[string]any{"f": []any{}}, unsupportedField); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for field length mismatch, got %v", err)
	}

	// 4. Parse errors for types:
	// INT64 parse error
	intField := []Field{{Name: "num", Type: "INT64", Mode: "NULLABLE"}}
	if _, err := sourceDecodeRow(map[string]any{"f": []any{map[string]any{"v": "not-an-int"}}}, intField); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for invalid INT64, got %v", err)
	}

	// FLOAT64 parse error
	floatField := []Field{{Name: "flt", Type: "FLOAT64", Mode: "NULLABLE"}}
	if _, err := sourceDecodeRow(map[string]any{"f": []any{map[string]any{"v": "not-a-float"}}}, floatField); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for invalid FLOAT64, got %v", err)
	}

	// BYTES decode error
	bytesField := []Field{{Name: "b", Type: "BYTES", Mode: "NULLABLE"}}
	if _, err := sourceDecodeRow(map[string]any{"f": []any{map[string]any{"v": "!invalid base64!"}}}, bytesField); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for invalid BYTES, got %v", err)
	}

	// TIMESTAMP parse error
	tsField := []Field{{Name: "ts", Type: "TIMESTAMP", Mode: "NULLABLE"}}
	if _, err := sourceDecodeRow(map[string]any{"f": []any{map[string]any{"v": "not-micros"}}}, tsField); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for invalid TIMESTAMP, got %v", err)
	}

	// Valid BOOL decode
	boolField := []Field{{Name: "b", Type: "BOOL", Mode: "NULLABLE"}}
	row, err := sourceDecodeRow(map[string]any{"f": []any{map[string]any{"v": "true"}}}, boolField)
	if err != nil || row.Values["b"] != true {
		t.Fatalf("expected true, got %v, err=%v", row.Values["b"], err)
	}
}

// Not parallel: mutates normalizeScalarFn seam.
func TestSourceRows_DecodeTypes_NotOkBranches(t *testing.T) {
	oldFn := normalizeScalarFn
	defer func() { normalizeScalarFn = oldFn }()

	// Return non-string / non-bool for types
	normalizeScalarFn = func(f Field, v any) (Cell, error) {
		return Cell{Value: 12345}, nil // int instead of string or bool
	}

	for _, fieldType := range []string{"INT64", "FLOAT64", "BYTES", "TIMESTAMP", "BOOL"} {
		f := []Field{{Name: "col", Type: fieldType, Mode: "NULLABLE"}}
		if _, err := sourceDecodeRow(map[string]any{"f": []any{map[string]any{"v": "anything"}}}, f); errorCode(err) != "malformed_wire" {
			t.Fatalf("expected malformed_wire for %s with non-string/non-bool Value, got %v", fieldType, err)
		}
	}

	// Return invalid string for parse errors
	normalizeScalarFn = func(f Field, v any) (Cell, error) {
		return Cell{Value: "invalid-string"}, nil
	}
	for _, fieldType := range []string{"INT64", "FLOAT64", "BYTES", "TIMESTAMP"} {
		f := []Field{{Name: "col", Type: fieldType, Mode: "NULLABLE"}}
		if _, err := sourceDecodeRow(map[string]any{"f": []any{map[string]any{"v": "anything"}}}, f); errorCode(err) != "malformed_wire" {
			t.Fatalf("expected malformed_wire for %s with unparseable string, got %v", fieldType, err)
		}
	}
}

func TestSourceRows_OpenSourceRows_Errors(t *testing.T) {
	t.Parallel()

	dummyTransport := rt(func(*http.Request) (*http.Response, error) { return nil, errors.New("no mock") })
	db := sourceTestDatabase(t, dummyTransport)
	ctx := context.Background()

	// Invalid input
	if _, err := db.OpenSourceRows(ctx, nil); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for nil ref, got %v", err)
	}
	if _, err := db.OpenSourceRows(ctx, ptr(dal.NewCollectionRef("bad name!", "", nil))); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad name, got %v", err)
	}

	// Ineligible type: VIEW instead of TABLE
	dbView := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "v1"},
			"type":           "VIEW",
			"etag":           "etag-v1",
		})
	}))
	if _, err := dbView.OpenSourceRows(ctx, ptr(dal.NewCollectionRef("v1", "", nil))); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for VIEW, got %v", err)
	}

	// Streaming buffer present -> source_ineligible
	dbStreaming := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference":  map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "t1"},
			"type":            "TABLE",
			"etag":            "etag-t1",
			"streamingBuffer": map[string]any{"estimatedRows": "10"},
		})
	}))
	if _, err := dbStreaming.OpenSourceRows(ctx, ptr(dal.NewCollectionRef("t1", "", nil))); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for streamingBuffer, got %v", err)
	}
}

func TestWire_RejectTransport_And_DispatchedCases(t *testing.T) {
	t.Parallel()

	// rejectTransport RoundTrip
	res, err := rejectTransport{}.RoundTrip(nil)
	if res != nil || errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input from rejectTransport, got res=%v, err=%v", res, err)
	}

	// dispatchGate.RoundTrip with invalid URL scheme/host
	gate := &dispatchGate{}
	badReq := httptest.NewRequest("GET", "http://insecure.example.com", nil)
	if _, err := gate.RoundTrip(badReq); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for insecure scheme, got %v", err)
	}

	// dispatchGate.RoundTrip with body > 256KB
	tooLargeBody := bytes.Repeat([]byte("a"), (256<<10)+10)
	largeReq := httptest.NewRequest("POST", "https://bigquery.googleapis.com/bigquery/v2/projects/p/jobs", bytes.NewReader(tooLargeBody))
	if _, err := gate.RoundTrip(largeReq); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for oversized body, got %v", err)
	}
}

func TestDiscovery_PublicDiscoverySchema_Errors(t *testing.T) {
	t.Parallel()

	// 1. Not an object
	if _, err := publicDiscoverySchema("not-a-map"); err == nil {
		t.Fatal("expected error for non-map schema")
	}

	// 2. Duplicate field name (case insensitive)
	dupSchema := map[string]any{
		"fields": []any{
			map[string]any{"name": "colA", "type": "STRING"},
			map[string]any{"name": "cola", "type": "STRING"},
		},
	}
	if _, err := publicDiscoverySchema(dupSchema); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for duplicate field name, got %v", err)
	}

	// 3. Unknown field type
	unknownType := map[string]any{
		"fields": []any{
			map[string]any{"name": "col1", "type": "UNKNOWN_TYPE_XYZ"},
		},
	}
	if _, err := publicDiscoverySchema(unknownType); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for unknown type, got %v", err)
	}

	// 4. Invalid mode (not NULLABLE/REQUIRED/REPEATED)
	badMode := map[string]any{
		"fields": []any{
			map[string]any{"name": "col1", "type": "STRING", "mode": "INVALID_MODE"},
		},
	}
	if _, err := publicDiscoverySchema(badMode); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for bad mode, got %v", err)
	}

	// 5. Non-record type has sub-fields present
	primitiveWithFields := map[string]any{
		"fields": []any{
			map[string]any{"name": "col1", "type": "STRING", "fields": []any{}},
		},
	}
	if _, err := publicDiscoverySchema(primitiveWithFields); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for primitive with fields, got %v", err)
	}

	// 6. Max depth exceeded (> 8)
	current := map[string]any{"name": "leaf", "type": "STRING"}
	for i := 0; i < 9; i++ {
		current = map[string]any{
			"name":   fmt.Sprintf("level%d", i),
			"type":   "RECORD",
			"fields": []any{current},
		}
	}
	deepSchema := map[string]any{"fields": []any{current}}
	if _, err := publicDiscoverySchema(deepSchema); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit for depth > 8, got %v", err)
	}
}

// Not parallel: mutates ledgerLeaseFn seam.
func TestJobs_Execute_LedgerLeaseError(t *testing.T) {
	oldLease := ledgerLeaseFn
	defer func() { ledgerLeaseFn = oldLease }()

	ledgerLeaseFn = func(l Ledger, ctx context.Context, id string) (func(), error) {
		return nil, errors.New("simulated lease error")
	}

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

	preview, err := client.Preview(context.Background(), plan, sc.Execution, sc.Bounds)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := client.Approve(preview, preview.ApprovalDigest)
	if err != nil {
		t.Fatal(err)
	}

	run, err := client.Execute(context.Background(), approval)
	if err == nil || !strings.Contains(err.Error(), "simulated lease error") {
		t.Fatalf("expected simulated lease error, got run=%v, err=%v", run, err)
	}
}

// Not parallel: mutates ledgerLeaseFn seam.
func TestReader_Resume_LedgerLeaseError(t *testing.T) {
	oldLease := ledgerLeaseFn
	defer func() { ledgerLeaseFn = oldLease }()

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

	preview, err := client.Preview(context.Background(), plan, sc.Execution, sc.Bounds)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := client.Approve(preview, preview.ApprovalDigest)
	if err != nil {
		t.Fatal(err)
	}

	run, err := client.Execute(context.Background(), approval)
	if err != nil {
		t.Fatal(err)
	}

	page, err := run.NextPage()
	if err != nil {
		t.Fatal(err)
	}

	// Now set ledgerLeaseFn error for Resume
	ledgerLeaseFn = func(l Ledger, ctx context.Context, id string) (func(), error) {
		return nil, errors.New("simulated resume lease error")
	}

	_, err = client.Resume(context.Background(), page.Receipt, page.Cursor)
	if err == nil || !strings.Contains(err.Error(), "simulated resume lease error") {
		t.Fatalf("expected simulated resume lease error, got %v", err)
	}
}

func jsonResp(status int, v any) (*http.Response, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(b)),
	}, nil
}

func TestSourceSchema_TypesAndExamples(t *testing.T) {
	t.Parallel()

	// Test sourceExampleValue exhaustive
	for _, typ := range []string{"INT64", "NUMERIC", "BIGNUMERIC", "FLOAT64", "TIMESTAMP", "BOOL", "DATE", "TIME", "DATETIME", "JSON", "STRING", "BYTES"} {
		val := sourceExampleValue(typ)
		if val == nil {
			t.Fatalf("expected non-nil example value for %s", typ)
		}
	}

	// Test sourcePortableType exhaustive
	for typ, expected := range map[string]dbschema.Type{
		"INT64":      dbschema.Int,
		"NUMERIC":    dbschema.Decimal,
		"BIGNUMERIC": dbschema.Decimal,
		"FLOAT64":    dbschema.Float,
		"BOOL":       dbschema.Bool,
		"BYTES":      dbschema.Bytes,
		"DATE":       dbschema.Time,
		"TIME":       dbschema.Time,
		"DATETIME":   dbschema.Time,
		"TIMESTAMP":  dbschema.Time,
		"STRING":     dbschema.String,
		"OTHER":      dbschema.String,
	} {
		if got := sourcePortableType(typ); got != expected {
			t.Fatalf("expected %v for %s, got %v", expected, typ, got)
		}
	}
}

func TestSourceSchema_FieldsValidation(t *testing.T) {
	t.Parallel()

	// 1. schema fields length == 0
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{}}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for empty fields, got %v", err)
	}

	// 2. collation present (absentConfigs error)
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{
		map[string]any{"name": "col1", "type": "STRING", "collation": "und:ci"},
	}}}); err == nil {
		t.Fatal("expected error for collation")
	}

	// 3. mode not string
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{
		map[string]any{"name": "col1", "type": "STRING", "mode": 123},
	}}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for int mode, got %v", err)
	}

	// 4. mode REPEATED
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{
		map[string]any{"name": "col1", "type": "STRING", "mode": "REPEATED"},
	}}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for REPEATED, got %v", err)
	}

	// 5. description not string
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{
		map[string]any{"name": "col1", "type": "STRING", "description": 123},
	}}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for int description, got %v", err)
	}

	// 6. precision / scale not string
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{
		map[string]any{"name": "col1", "type": "NUMERIC", "precision": 123},
	}}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for int precision, got %v", err)
	}
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{
		map[string]any{"name": "col1", "type": "NUMERIC", "precision": "10", "scale": 123},
	}}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for int scale, got %v", err)
	}

	// 7. scale without precision
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{
		map[string]any{"name": "col1", "type": "NUMERIC", "scale": "2"},
	}}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for scale without precision, got %v", err)
	}

	// 8. nested fields present (sub-fields)
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{
		map[string]any{"name": "col1", "type": "RECORD", "fields": []any{}},
	}}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for record with fields, got %v", err)
	}
}

func TestSourceSchema_ConstraintsValidation(t *testing.T) {
	t.Parallel()

	fields := []Field{{Name: "id", Type: "INT64"}, {Name: "parent_id", Type: "INT64"}}

	// 1. tableConstraints == nil -> returns nil, nil, nil
	pk, fks, err := sourceConstraints(map[string]any{}, fields, "proj", "ds")
	if err != nil || pk != nil || fks != nil {
		t.Fatalf("expected nil constraints, got pk=%v, fks=%v, err=%v", pk, fks, err)
	}

	// 2. primaryKey columns not a slice or empty
	if _, _, err := sourceConstraints(map[string]any{
		"tableConstraints": map[string]any{"primaryKey": map[string]any{"columns": "not-slice"}},
	}, fields, "proj", "ds"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}
	if _, _, err := sourceConstraints(map[string]any{
		"tableConstraints": map[string]any{"primaryKey": map[string]any{"columns": []any{}}},
	}, fields, "proj", "ds"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 3. primaryKey unknown column or duplicate
	if _, _, err := sourceConstraints(map[string]any{
		"tableConstraints": map[string]any{"primaryKey": map[string]any{"columns": []any{"unknown_col"}}},
	}, fields, "proj", "ds"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for unknown pk column, got %v", err)
	}
	if _, _, err := sourceConstraints(map[string]any{
		"tableConstraints": map[string]any{"primaryKey": map[string]any{"columns": []any{"id", "id"}}},
	}, fields, "proj", "ds"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for duplicate pk column, got %v", err)
	}

	// 4. foreignKeys not slice
	if _, _, err := sourceConstraints(map[string]any{
		"tableConstraints": map[string]any{"foreignKeys": "not-slice"},
	}, fields, "proj", "ds"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-slice foreignKeys, got %v", err)
	}

	// 5. foreignKeys bad tableId
	if _, _, err := sourceConstraints(map[string]any{
		"tableConstraints": map[string]any{"foreignKeys": []any{
			map[string]any{"referencedTable": map[string]any{"tableId": "!bad table!"}},
		}},
	}, fields, "proj", "ds"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for bad fk tableId, got %v", err)
	}

	// 6. foreignKeys bad projectId / datasetId regex
	if _, _, err := sourceConstraints(map[string]any{
		"tableConstraints": map[string]any{"foreignKeys": []any{
			map[string]any{"referencedTable": map[string]any{"tableId": "t", "projectId": "!bad-proj!"}},
		}},
	}, fields, "proj", "ds"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for bad fk projectId, got %v", err)
	}
	if _, _, err := sourceConstraints(map[string]any{
		"tableConstraints": map[string]any{"foreignKeys": []any{
			map[string]any{"referencedTable": map[string]any{"tableId": "t", "datasetId": "!bad-ds!"}},
		}},
	}, fields, "proj", "ds"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for bad fk datasetId, got %v", err)
	}

	// 7. foreignKeys columnReferences not slice or empty
	if _, _, err := sourceConstraints(map[string]any{
		"tableConstraints": map[string]any{"foreignKeys": []any{
			map[string]any{
				"referencedTable":  map[string]any{"tableId": "t"},
				"columnReferences": "not-slice",
			},
		}},
	}, fields, "proj", "ds"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-slice columnReferences, got %v", err)
	}

	// 8. referencingColumn unknown
	if _, _, err := sourceConstraints(map[string]any{
		"tableConstraints": map[string]any{"foreignKeys": []any{
			map[string]any{
				"referencedTable":  map[string]any{"tableId": "t"},
				"columnReferences": []any{map[string]any{"referencingColumn": "unknown", "referencedColumn": "id"}},
			},
		}},
	}, fields, "proj", "ds"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for unknown referencingColumn, got %v", err)
	}

	// 9. referencedColumn bad regex
	if _, _, err := sourceConstraints(map[string]any{
		"tableConstraints": map[string]any{"foreignKeys": []any{
			map[string]any{
				"referencedTable":  map[string]any{"tableId": "t"},
				"columnReferences": []any{map[string]any{"referencingColumn": "id", "referencedColumn": "!bad!"}},
			},
		}},
	}, fields, "proj", "ds"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for bad referencedColumn, got %v", err)
	}

	// 10. Valid cross-project / cross-dataset FK
	pk, fks, err = sourceConstraints(map[string]any{
		"tableConstraints": map[string]any{
			"primaryKey": map[string]any{"columns": []any{"id"}},
			"foreignKeys": []any{
				map[string]any{
					"name": "fk_cross",
					"referencedTable": map[string]any{
						"projectId": "other-proj",
						"datasetId": "other_ds",
						"tableId":   "parent",
					},
					"columnReferences": []any{
						map[string]any{"referencingColumn": "parent_id", "referencedColumn": "id"},
					},
				},
			},
		},
	}, fields, "proj", "ds")
	if err != nil || len(pk) != 1 || len(fks) != 1 {
		t.Fatalf("expected success, got pk=%v, fks=%v, err=%v", pk, fks, err)
	}
	if fks[0].ReferencedNamespace != "other-proj.other_ds" || fks[0].Name != "fk_cross" {
		t.Fatalf("unexpected fk: %+v", fks[0])
	}
}

func TestSourceRows_Cursor_LoadPage_And_VerifyFinal(t *testing.T) {
	t.Parallel()

	// 1. DescribeCollection with VIEW type -> source_ineligible
	dbView := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "v1"},
			"type":           "VIEW",
		})
	}))
	if _, err := dbView.DescribeCollection(context.Background(), ptr(dal.NewCollectionRef("v1", "", nil))); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for VIEW DescribeCollection, got %v", err)
	}

	// 2. Cursor loadPage: tableDataList kind mismatch
	dbBadKind := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		switch {
		case strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset"):
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		case strings.HasSuffix(req.URL.Path, "/tables/sample"):
			return jsonResp(http.StatusOK, map[string]any{
				"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
				"type":             "TABLE",
				"etag":             "etag-1",
				"lastModifiedTime": "1000",
				"numRows":          "1",
				"schema": map[string]any{"fields": []any{
					map[string]any{"name": "id", "type": "INT64"},
				}},
			})
		case strings.Contains(req.URL.Path, "/data"):
			return jsonResp(http.StatusOK, map[string]any{
				"kind": "wrong#kind",
			})
		default:
			return nil, fmt.Errorf("unexpected URL: %s", req.URL.Path)
		}
	}))

	cur, err := dbBadKind.OpenSourceRows(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer cur.Close()
	if _, err := cur.Next(); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for wrong kind, got %v", err)
	}

	// 3. Cursor loadPage: totalRows mismatch
	dbTotalRowsMismatch := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		switch {
		case strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset"):
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		case strings.HasSuffix(req.URL.Path, "/tables/sample"):
			return jsonResp(http.StatusOK, map[string]any{
				"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
				"type":             "TABLE",
				"etag":             "etag-1",
				"lastModifiedTime": "1000",
				"numRows":          "1",
				"schema": map[string]any{"fields": []any{
					map[string]any{"name": "id", "type": "INT64"},
				}},
			})
		case strings.Contains(req.URL.Path, "/data"):
			return jsonResp(http.StatusOK, map[string]any{
				"kind":      "bigquery#tableDataList",
				"totalRows": "999", // Mismatch with numRows=1
			})
		default:
			return nil, fmt.Errorf("unexpected URL: %s", req.URL.Path)
		}
	}))
	cur2, err := dbTotalRowsMismatch.OpenSourceRows(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer cur2.Close()
	if _, err := cur2.Next(); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed for totalRows mismatch, got %v", err)
	}

	// 4. Cursor loadPage: duplicate pageToken
	dbDupToken := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		switch {
		case strings.HasSuffix(req.URL.Path, "/datasets/fixture_dataset"):
			return jsonResp(http.StatusOK, map[string]any{
				"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
				"location":         "US",
			})
		case strings.HasSuffix(req.URL.Path, "/tables/sample"):
			return jsonResp(http.StatusOK, map[string]any{
				"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
				"type":             "TABLE",
				"etag":             "etag-1",
				"lastModifiedTime": "1000",
				"numRows":          "2",
				"schema": map[string]any{"fields": []any{
					map[string]any{"name": "id", "type": "INT64"},
				}},
			})
		case strings.Contains(req.URL.Path, "/data"):
			return jsonResp(http.StatusOK, map[string]any{
				"kind":      "bigquery#tableDataList",
				"totalRows": "2",
				"rows":      []any{map[string]any{"f": []any{map[string]any{"v": "1"}}}},
				"pageToken": "same-token", // next == previous on second call
			})
		default:
			return nil, fmt.Errorf("unexpected URL: %s", req.URL.Path)
		}
	}))
	cur3, err := dbDupToken.OpenSourceRows(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer cur3.Close()
	// First row succeeds
	if _, err := cur3.Next(); err != nil {
		t.Fatal(err)
	}
	// Second page returns same-token again
	if _, err := cur3.Next(); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid for duplicate token, got %v", err)
	}
}
