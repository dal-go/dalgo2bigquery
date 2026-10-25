package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	bq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

func TestDiscovery_Discover_ErrorBranches(t *testing.T) {
	t.Parallel()

	// 1. datasetReference is not an object (line 159)
	c1, _, _, _ := discoveryFixtureClient(t, rt(func(req *http.Request) (*http.Response, error) {
		return jsonResp(http.StatusOK, map[string]any{
			"datasetReference": "not-an-object",
			"location":         "US",
		})
	}))
	if _, err := c1.Discover(context.Background(), "fixture-source", DefaultBounds()); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for invalid datasetReference, got %v", err)
	}

	// 2. tableReference is not an object (line 181)
	c2, _, _, _ := discoveryFixtureClient(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/tables/") {
			return jsonResp(http.StatusOK, map[string]any{
				"tableReference": "not-an-object",
				"location":       "US",
				"type":           "TABLE",
				"schema":         map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
			"location":         "US",
		})
	}))
	if _, err := c2.Discover(context.Background(), "fixture-source", DefaultBounds()); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for invalid tableReference, got %v", err)
	}

	// 3. table type is missing (line 191)
	c3, _, _, _ := discoveryFixtureClient(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/tables/") {
			return jsonResp(http.StatusOK, map[string]any{
				"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
				"location":       "US",
				"schema":         map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
			"location":         "US",
		})
	}))
	if _, err := c3.Discover(context.Background(), "fixture-source", DefaultBounds()); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for missing table type, got %v", err)
	}

	// 4. table schema invalid (line 200)
	c4, _, _, _ := discoveryFixtureClient(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/tables/") {
			return jsonResp(http.StatusOK, map[string]any{
				"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
				"location":       "US",
				"type":           "TABLE",
				"schema":         "invalid-schema",
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
			"location":         "US",
		})
	}))
	if _, err := c4.Discover(context.Background(), "fixture-source", DefaultBounds()); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for invalid schema, got %v", err)
	}
}

// Not parallel: mutates discovery seams
func TestDiscovery_Discover_SeamBranches(t *testing.T) {
	baseTransport := rt(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/tables/") {
			return jsonResp(http.StatusOK, map[string]any{
				"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
				"location":       "US",
				"type":           "TABLE",
				"schema":         map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"},
			"location":         "US",
		})
	})
	client, _, _, _ := discoveryFixtureClient(t, baseTransport)

	// 1. discoveryObjectFn error on dataset (line 155)
	origObj := discoveryObjectFn
	defer func() { discoveryObjectFn = origObj }()
	discoveryObjectFn = func(any) (map[string]any, error) { return nil, fail("malformed_wire") }
	if _, err := client.Discover(context.Background(), "fixture-source", DefaultBounds()); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 2. discoveryObjectFn error on table (line 177)
	calls := 0
	discoveryObjectFn = func(v any) (map[string]any, error) {
		calls++
		if calls > 1 {
			return nil, fail("malformed_wire")
		}
		return origObj(v)
	}
	if _, err := client.Discover(context.Background(), "fixture-source", DefaultBounds()); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on table, got %v", err)
	}
	discoveryObjectFn = origObj

	// 3. discoveryMarshalFn error (line 205)
	origMarshal := discoveryMarshalFn
	defer func() { discoveryMarshalFn = origMarshal }()
	discoveryMarshalFn = func(any) ([]byte, error) { return nil, errors.New("marshal error") }
	if _, err := client.Discover(context.Background(), "fixture-source", DefaultBounds()); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on marshal error, got %v", err)
	}
	discoveryMarshalFn = origMarshal

	// 4. discoveryCanonicalJSONFn error (line 209)
	origCanon := discoveryCanonicalJSONFn
	defer func() { discoveryCanonicalJSONFn = origCanon }()
	discoveryCanonicalJSONFn = func([]byte) ([]byte, error) { return nil, fail("malformed_wire") }
	if _, err := client.Discover(context.Background(), "fixture-source", DefaultBounds()); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on canonical error, got %v", err)
	}
}

func TestReader_Ensure_ContextCanceledDuringSleep(t *testing.T) {
	t.Parallel()

	// In ensure(): when !r.loaded, clock.Sleep(r.ctx, ...) is called.
	// If context is canceled during that sleep, it returns error (lines 343-345).
	l := NewMemoryLedger()
	p := testProfile(t)
	plan, _ := Compile(p, testQuery())
	ctx, cancel := context.WithCancel(context.Background())
	clock := &fakeClock{now: time.Now()}
	identity := &testProvider{identity: Identity{Principal{"workload", "operator:fixture", "1"}, clock.Now().Add(time.Hour), true, true}}

	transport := rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/ds") {
			return response(`{"datasetReference":{"projectId":"source-project","datasetId":"ds"},"location":"EU"}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
			return response(`{"tableReference":{"projectId":"source-project","datasetId":"ds","tableId":"tbl"},"type":"TABLE","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`), nil
		}
		if req.Method == "POST" {
			var m map[string]any
			if req.Body != nil {
				_ = json.NewDecoder(req.Body).Decode(&m)
			}
			if m["dryRun"] == true {
				return response(`{"totalBytesProcessed":"100"}`), nil
			}
			return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":false}`), nil
		}
		cancel() // Cancel context during job poll!
		return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":false}`), nil
	})

	c, err := NewClient(Config{[]SourceProfile{p}, identity, transport, l, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	if err != nil {
		t.Fatal(err)
	}
	prev := previewFor(t, c, "3000")
	a, _ := c.Approve(prev, prev.ApprovalDigest)
	run, err := c.Execute(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()

	_, _, err = run.Next()
	if err == nil {
		t.Fatal("expected error from canceled context during sleep, got nil")
	}
}

func TestReadOnlyDatabase_OpenSourceRows_Errors(t *testing.T) {
	t.Parallel()

	// 1. datasetMetadata error (line 45-47)
	dbErr := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/fixture_dataset") {
			return jsonResp(http.StatusNotFound, map[string]any{"error": map[string]any{"code": 404}})
		}
		return nil, nil
	}))
	if _, err := dbErr.OpenSourceRows(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil))); errorCode(err) != "result_expired" {
		t.Fatalf("expected result_expired on dataset 404, got %v", err)
	}

	// 2. numRows not a number (line 64-66)
	dbInvalidRows := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}})
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
	if _, err := dbInvalidRows.OpenSourceRows(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil))); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for invalid numRows, got %v", err)
	}

	// 3. lastModifiedTime missing (line 78-80)
	dbMissingTime := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
			"type":           "TABLE",
			"etag":           "etag-1",
			"numRows":        "1",
			"schema":         map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
		})
	}))
	if _, err := dbMissingTime.OpenSourceRows(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil))); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for missing lastModifiedTime, got %v", err)
	}
}

func TestSourceRowCursor_Next_ExcessRows(t *testing.T) {
	t.Parallel()

	db := sourceTestDatabase(t, rt(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	}))

	// In Next(): c.count > c.expected (lines 118-120)
	cursor := &sourceRowCursor{
		expected:  0,
		count:     0,
		rows:      []dbschema.SourceRow{{Values: map[string]any{"id": int64(1)}}},
		index:     0,
		db:        db.source,
		principal: Principal{Kind: "workload", Subject: "reader", Generation: "1"},
		ctx:       context.Background(),
		cancel:    func() {},
	}
	if _, err := cursor.Next(); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed when count > expected, got %v", err)
	}
}

func TestSourceRowCursor_LoadPage_Errors(t *testing.T) {
	t.Parallel()

	dbBase := func(h func(*http.Request) (*http.Response, error)) *ReadOnlyDatabase {
		return sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
			path := req.URL.Path
			switch {
			case strings.HasSuffix(path, "/data"):
				return h(req)
			case strings.HasSuffix(path, "/tables/sample"):
				return jsonResp(http.StatusOK, map[string]any{
					"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
					"type":             "TABLE",
					"etag":             "etag-1",
					"lastModifiedTime": "1000",
					"numRows":          "1",
					"schema":           map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
				})
			case strings.HasSuffix(path, "/fixture_dataset"):
				return jsonResp(http.StatusOK, map[string]any{"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}})
			default:
				return jsonResp(http.StatusNotFound, map[string]any{"error": map[string]any{"code": 404}})
			}
		}))
	}

	// 1. totalRows != expected (line 168-169)
	db1 := dbBase(func(*http.Request) (*http.Response, error) {
		return jsonResp(http.StatusOK, map[string]any{
			"kind":      "bigquery#tableDataList",
			"totalRows": "2", // c.expected is 1, so total != c.expected
		})
	})
	cur1, err := db1.OpenSourceRows(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil)))
	if err != nil {
		t.Fatalf("open err: %v", err)
	}
	defer cur1.Close()
	if _, err := cur1.Next(); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed for totalRows != expected, got %v", err)
	}

	// 2. rows not an array (line 173-174)
	db2 := dbBase(func(*http.Request) (*http.Response, error) {
		return jsonResp(http.StatusOK, map[string]any{
			"kind":      "bigquery#tableDataList",
			"totalRows": "1",
			"rows":      "not-an-array",
		})
	})
	cur2, _ := db2.OpenSourceRows(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil)))
	defer cur2.Close()
	if _, err := cur2.Next(); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for invalid rows type, got %v", err)
	}

	// 3. c.count+len(rows) > c.expected (line 184-185)
	db3 := dbBase(func(*http.Request) (*http.Response, error) {
		return jsonResp(http.StatusOK, map[string]any{
			"kind":      "bigquery#tableDataList",
			"totalRows": "1",
			"rows": []any{
				map[string]any{"f": []any{map[string]any{"v": "1"}}},
				map[string]any{"f": []any{map[string]any{"v": "2"}}},
			},
		})
	})
	cur3, _ := db3.OpenSourceRows(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil)))
	defer cur3.Close()
	if _, err := cur3.Next(); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed when rows exceed expected, got %v", err)
	}

	// 4. pageToken invalid (line 188-189)
	db4 := dbBase(func(*http.Request) (*http.Response, error) {
		return jsonResp(http.StatusOK, map[string]any{
			"kind":      "bigquery#tableDataList",
			"totalRows": "1",
			"rows":      []any{map[string]any{"f": []any{map[string]any{"v": "1"}}}},
			"pageToken": 12345, // not a string!
		})
	})
	cur4, _ := db4.OpenSourceRows(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil)))
	defer cur4.Close()
	if _, err := cur4.Next(); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for invalid pageToken, got %v", err)
	}
}

func TestSourceRowCursor_VerifyFinal_FieldsChanged(t *testing.T) {
	t.Parallel()

	// In verifyFinal: fields changed (line 215-216)
	callCount := 0
	db := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		path := req.URL.Path
		switch {
		case strings.HasSuffix(path, "/data"):
			return jsonResp(http.StatusOK, map[string]any{
				"kind":      "bigquery#tableDataList",
				"totalRows": "1",
				"rows":      []any{map[string]any{"f": []any{map[string]any{"v": "1"}}}},
			})
		case strings.HasSuffix(path, "/tables/sample"):
			callCount++
			fields := []any{map[string]any{"name": "id", "type": "INT64"}}
			if callCount > 1 {
				// In verifyFinal, schema has a different field name!
				fields = []any{map[string]any{"name": "id_renamed", "type": "INT64"}}
			}
			return jsonResp(http.StatusOK, map[string]any{
				"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
				"type":             "TABLE",
				"etag":             "etag-1",
				"lastModifiedTime": "1000",
				"numRows":          "1",
				"schema":           map[string]any{"fields": fields},
			})
		case strings.HasSuffix(path, "/fixture_dataset"):
			return jsonResp(http.StatusOK, map[string]any{"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}})
		default:
			return jsonResp(http.StatusNotFound, map[string]any{"error": map[string]any{"code": 404}})
		}
	}))

	cur, err := db.OpenSourceRows(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer cur.Close()
	if _, err := cur.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := cur.Next(); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed when fields change, got %v", err)
	}
}

func TestSourceDecodeRow_UnsupportedType_And_NormalizeSeam(t *testing.T) {
	t.Parallel()

	// 1. Unsupported field type with non-nil cell (line 284)
	fields := []Field{{Name: "geo", Type: "GEOGRAPHY"}}
	raw := map[string]any{"f": []any{map[string]any{"v": "POINT(1 2)"}}}
	if _, err := sourceDecodeRow(raw, fields); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type for GEOGRAPHY, got %v", err)
	}
}

// Not parallel: mutates normalizeScalarFn
func TestSourceDecodeRow_NormalizeScalar_NonStringForStringField(t *testing.T) {
	orig := normalizeScalarFn
	defer func() { normalizeScalarFn = orig }()
	// Return non-string Value for STRING field (line 256)
	normalizeScalarFn = func(Field, any) (Cell, error) {
		return Cell{Value: 12345}, nil
	}

	fields := []Field{{Name: "s", Type: "STRING"}}
	raw := map[string]any{"f": []any{map[string]any{"v": "hello"}}}
	if _, err := sourceDecodeRow(raw, fields); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire when STRING field value is not string, got %v", err)
	}
}

func TestReadOnlyDatabase_Relations_Errors_And_Pagination(t *testing.T) {
	t.Parallel()

	// Helper for relations test
	dsHandler := func(req *http.Request) (*http.Response, bool) {
		if strings.HasSuffix(req.URL.Path, "/fixture_dataset") {
			resp, _ := jsonResp(http.StatusOK, map[string]any{"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}})
			return resp, true
		}
		return nil, false
	}

	// 1. Pagination across 2 pages (token != "", line 34, lines 89-90)
	page := 0
	dbPages := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if r, ok := dsHandler(req); ok {
			return r, nil
		}
		page++
		if page == 1 {
			return jsonResp(http.StatusOK, map[string]any{
				"totalItems":    2,
				"nextPageToken": "token-2",
				"tables": []any{
					map[string]any{
						"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "t1"},
						"type":           "TABLE",
					},
				},
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"totalItems": 2,
			"tables": []any{
				map[string]any{
					"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "t2"},
					"type":           "TABLE",
				},
			},
		})
	}))
	rels, err := dbPages.source.relations(context.Background(), Principal{Kind: "workload", Subject: "reader", Generation: "1"})
	if err != nil || len(rels) != 2 {
		t.Fatalf("expected 2 relations, got %v, err=%v", rels, err)
	}

	// 2. totalItems mismatch across pages (line 45)
	page = 0
	dbMismatch := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if r, ok := dsHandler(req); ok {
			return r, nil
		}
		page++
		if page == 1 {
			return jsonResp(http.StatusOK, map[string]any{
				"totalItems":    2,
				"nextPageToken": "token-2",
				"tables": []any{
					map[string]any{
						"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "t1"},
						"type":           "TABLE",
					},
				},
			})
		}
		return jsonResp(http.StatusOK, map[string]any{
			"totalItems": 5, // Mismatch!
			"tables":     []any{},
		})
	}))
	if _, err := dbMismatch.source.relations(context.Background(), Principal{Kind: "workload", Subject: "reader", Generation: "1"}); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed on totalItems mismatch, got %v", err)
	}

	// 3. tables not an array (line 51)
	dbNotArr := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if r, ok := dsHandler(req); ok {
			return r, nil
		}
		return jsonResp(http.StatusOK, map[string]any{"tables": "not-an-array"})
	}))
	if _, err := dbNotArr.source.relations(context.Background(), Principal{Kind: "workload", Subject: "reader", Generation: "1"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for tables not an array, got %v", err)
	}

	// 4. item not an object (line 56)
	dbItemNotObj := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if r, ok := dsHandler(req); ok {
			return r, nil
		}
		return jsonResp(http.StatusOK, map[string]any{"tables": []any{"not-an-object"}})
	}))
	if _, err := dbItemNotObj.source.relations(context.Background(), Principal{Kind: "workload", Subject: "reader", Generation: "1"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 5. tableReference not an object (line 60)
	dbRefNotObj := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if r, ok := dsHandler(req); ok {
			return r, nil
		}
		return jsonResp(http.StatusOK, map[string]any{"tables": []any{map[string]any{"tableReference": "str"}}})
	}))
	if _, err := dbRefNotObj.source.relations(context.Background(), Principal{Kind: "workload", Subject: "reader", Generation: "1"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 6. tableId missing (line 64)
	dbMissingID := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if r, ok := dsHandler(req); ok {
			return r, nil
		}
		return jsonResp(http.StatusOK, map[string]any{"tables": []any{map[string]any{"tableReference": map[string]any{}}}})
	}))
	if _, err := dbMissingID.source.relations(context.Background(), Principal{Kind: "workload", Subject: "reader", Generation: "1"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 7. type missing (line 68)
	dbMissingType := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if r, ok := dsHandler(req); ok {
			return r, nil
		}
		return jsonResp(http.StatusOK, map[string]any{"tables": []any{map[string]any{
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "t1"},
		}}})
	}))
	if _, err := dbMissingType.source.relations(context.Background(), Principal{Kind: "workload", Subject: "reader", Generation: "1"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 8. nextPageToken invalid (line 81)
	dbInvalidToken := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if r, ok := dsHandler(req); ok {
			return r, nil
		}
		return jsonResp(http.StatusOK, map[string]any{
			"tables":        []any{},
			"nextPageToken": 12345, // not a string!
		})
	}))
	if _, err := dbInvalidToken.source.relations(context.Background(), Principal{Kind: "workload", Subject: "reader", Generation: "1"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 9. next == token (looping token) (line 87)
	loopPage := 0
	dbLoopToken := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if r, ok := dsHandler(req); ok {
			return r, nil
		}
		loopPage++
		tableID := "t1"
		if loopPage > 1 {
			tableID = "t2"
		}
		return jsonResp(http.StatusOK, map[string]any{
			"totalItems":    10,
			"nextPageToken": "same-token",
			"tables": []any{
				map[string]any{
					"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": tableID},
					"type":           "TABLE",
				},
			},
		})
	}))
	if _, err := dbLoopToken.source.relations(context.Background(), Principal{Kind: "workload", Subject: "reader", Generation: "1"}); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on looping token, got %v", err)
	}

	// 10. page >= MaxPages (line 27)
	dbMaxPages := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if r, ok := dsHandler(req); ok {
			return r, nil
		}
		return jsonResp(http.StatusOK, map[string]any{
			"totalItems":    10,
			"nextPageToken": "tok",
			"tables": []any{
				map[string]any{
					"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "t1"},
					"type":           "TABLE",
				},
			},
		})
	}))
	dbMaxPages.source.limits.MaxPages = 1 // Limit to 1 page
	if _, err := dbMaxPages.source.relations(context.Background(), Principal{Kind: "workload", Subject: "reader", Generation: "1"}); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit on max pages exceeded, got %v", err)
	}
}

func TestReadOnlyDatabase_ListSourceViews_Errors(t *testing.T) {
	t.Parallel()

	// 1. identity error (line 124-125)
	dbAuth := sourceTestDatabase(t, rt(func(*http.Request) (*http.Response, error) { return nil, nil }))
	dbAuth.source.provider = &testProvider{}
	if _, err := dbAuth.ListSourceViews(context.Background()); errorCode(err) != "auth_required" {
		t.Fatalf("expected auth_required, got %v", err)
	}

	// 2. relations error (line 128-129)
	dbRelErr := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/fixture_dataset") {
			return jsonResp(http.StatusOK, map[string]any{"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}})
		}
		if strings.HasSuffix(req.URL.Path, "/tables") {
			return jsonResp(http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": 500}})
		}
		return nil, nil
	}))
	if _, err := dbRelErr.ListSourceViews(context.Background()); errorCode(err) != "remote_failed" {
		t.Fatalf("expected remote_failed, got %v", err)
	}

	// Helper for single view in relations
	dbWithView := func(viewHandler func(*http.Request) (*http.Response, error)) *ReadOnlyDatabase {
		return sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
			if strings.HasSuffix(req.URL.Path, "/fixture_dataset") {
				return jsonResp(http.StatusOK, map[string]any{"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}})
			}
			if strings.HasSuffix(req.URL.Path, "/tables") {
				return jsonResp(http.StatusOK, map[string]any{
					"totalItems": 1,
					"tables": []any{
						map[string]any{
							"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "v1"},
							"type":           "VIEW",
						},
					},
				})
			}
			return viewHandler(req)
		}))
	}

	// 3. tableMetadata error (line 137-138)
	dbMetaErr := dbWithView(func(*http.Request) (*http.Response, error) {
		return jsonResp(http.StatusNotFound, map[string]any{"error": map[string]any{"code": 404}})
	})
	if _, err := dbMetaErr.ListSourceViews(context.Background()); errorCode(err) != "result_expired" {
		t.Fatalf("expected result_expired on 404 view, got %v", err)
	}

	// 4. m["type"] != "VIEW" (line 140-141)
	dbNotView := dbWithView(func(*http.Request) (*http.Response, error) {
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "v1"},
			"type":           "TABLE", // Changed to TABLE!
		})
	})
	if _, err := dbNotView.ListSourceViews(context.Background()); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed, got %v", err)
	}

	// 5. object(m["view"]) error (line 144-145)
	dbViewNotObj := dbWithView(func(*http.Request) (*http.Response, error) {
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "v1"},
			"type":           "VIEW",
			"view":           "not-an-object",
		})
	})
	if _, err := dbViewNotObj.ListSourceViews(context.Background()); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 6. requiredString(v, "query") error (line 147-148)
	dbViewNoQuery := dbWithView(func(*http.Request) (*http.Response, error) {
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "v1"},
			"type":           "VIEW",
			"view":           map[string]any{}, // no query!
		})
	})
	if _, err := dbViewNoQuery.ListSourceViews(context.Background()); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 7. sourceFields(m) error (line 151-152)
	dbViewBadSchema := dbWithView(func(*http.Request) (*http.Response, error) {
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "v1"},
			"type":           "VIEW",
			"view":           map[string]any{"query": "SELECT 1"},
			"schema":         "invalid-schema",
		})
	})
	if _, err := dbViewBadSchema.ListSourceViews(context.Background()); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}
}

func TestSourceFields_Errors(t *testing.T) {
	t.Parallel()

	// 1. schema not an object (line 167)
	if _, err := sourceFields(map[string]any{"schema": "str"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 2. field item not an object (line 178)
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{"not-obj"}}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 3. unknown field in item (line 181)
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{
		map[string]any{"name": "x", "type": "STRING", "unknownKey": true},
	}}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible, got %v", err)
	}

	// 4. name missing (line 188)
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{
		map[string]any{"type": "STRING"},
	}}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 5. type missing (line 192)
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{
		map[string]any{"name": "x"},
	}}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 6. NormalizeScalar error: invalid precision/scale (line 230-231)
	if _, err := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{
		map[string]any{"name": "x", "type": "NUMERIC", "precision": "0", "scale": "1"},
	}}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible on invalid precision/scale, got %v", err)
	}
}

func TestSourceConstraints_Errors(t *testing.T) {
	t.Parallel()

	fields := []Field{{Name: "id", Type: "INT64"}}

	// 1. tableConstraints not an object (line 282-283)
	if _, _, err := sourceConstraints(map[string]any{"tableConstraints": "str"}, fields, "p", "d"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 2. primaryKey not an object (line 292-293)
	if _, _, err := sourceConstraints(map[string]any{"tableConstraints": map[string]any{"primaryKey": "str"}}, fields, "p", "d"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 3. foreignKeys item not an object (line 317-318)
	if _, _, err := sourceConstraints(map[string]any{"tableConstraints": map[string]any{"foreignKeys": []any{"str"}}}, fields, "p", "d"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 4. referencedTable not an object (line 321-322)
	if _, _, err := sourceConstraints(map[string]any{"tableConstraints": map[string]any{
		"foreignKeys": []any{map[string]any{"referencedTable": "str"}},
	}}, fields, "p", "d"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 5. tableId missing in referencedTable (line 325-326)
	if _, _, err := sourceConstraints(map[string]any{"tableConstraints": map[string]any{
		"foreignKeys": []any{map[string]any{"referencedTable": map[string]any{}}},
	}}, fields, "p", "d"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 6. columnReferences item not an object (line 359-360)
	if _, _, err := sourceConstraints(map[string]any{"tableConstraints": map[string]any{
		"foreignKeys": []any{map[string]any{
			"referencedTable":  map[string]any{"tableId": "ref_table"},
			"columnReferences": []any{"str"},
		}},
	}}, fields, "p", "d"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}
}

func TestReadOnlyDatabase_DescribeCollection_Errors_And_Precision(t *testing.T) {
	t.Parallel()

	// 1. identity error (line 386-387)
	dbAuth := sourceTestDatabase(t, rt(func(*http.Request) (*http.Response, error) { return nil, nil }))
	dbAuth.source.provider = &testProvider{}
	if _, err := dbAuth.DescribeCollection(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil))); errorCode(err) != "auth_required" {
		t.Fatalf("expected auth_required, got %v", err)
	}

	// 2. tableMetadata error (line 390-391)
	dbMetaErr := sourceTestDatabase(t, rt(func(*http.Request) (*http.Response, error) {
		return jsonResp(http.StatusNotFound, map[string]any{"error": map[string]any{"code": 404}})
	}))
	if _, err := dbMetaErr.DescribeCollection(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil))); errorCode(err) != "result_expired" {
		t.Fatalf("expected result_expired, got %v", err)
	}

	// 3. sourceFields error (line 397-398)
	dbSchemaErr := sourceTestDatabase(t, rt(func(*http.Request) (*http.Response, error) {
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
			"type":           "TABLE",
			"schema":         "invalid-schema",
		})
	}))
	if _, err := dbSchemaErr.DescribeCollection(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil))); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 4. sourceConstraints error (line 401-402)
	dbConstrErr := sourceTestDatabase(t, rt(func(*http.Request) (*http.Response, error) {
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference":   map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
			"type":             "TABLE",
			"schema":           map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}},
			"tableConstraints": "invalid-constraints",
		})
	}))
	if _, err := dbConstrErr.DescribeCollection(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil))); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 5. Field with precision and scale set (lines 407-410)
	dbPrecision := sourceTestDatabase(t, rt(func(*http.Request) (*http.Response, error) {
		return jsonResp(http.StatusOK, map[string]any{
			"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
			"type":           "TABLE",
			"schema": map[string]any{"fields": []any{
				map[string]any{"name": "num", "type": "NUMERIC", "precision": "10", "scale": "2"},
			}},
		})
	}))
	def, err := dbPrecision.DescribeCollection(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil)))
	if err != nil {
		t.Fatal(err)
	}
	if def.Fields[0].Precision == nil || def.Fields[0].Precision.Total != 10 || def.Fields[0].Precision.Scale != 2 {
		t.Fatalf("expected precision 10,2, got %v", def.Fields[0].Precision)
	}

	// 6. DescribeSourceFields identity error (line 433-434)
	if _, err := dbAuth.DescribeSourceFields(context.Background(), ptr(dal.NewCollectionRef("sample", "", nil))); errorCode(err) != "auth_required" {
		t.Fatalf("expected auth_required, got %v", err)
	}
}

func TestWire_DispatchGate_RoundTrip_Cancellations(t *testing.T) {
	t.Parallel()

	// 1. Canceled before dispatch (line 101-103)
	ctx1, cancel1 := context.WithCancel(context.Background())
	cancel1()
	gate1 := &dispatchGate{base: rt(func(*http.Request) (*http.Response, error) { return nil, nil })}
	req1, _ := http.NewRequestWithContext(ctx1, "GET", "https://bigquery.googleapis.com/bigquery/v2/test", nil)
	if _, err := gate1.RoundTrip(req1); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on canceled req, got %v", err)
	}

	// 2. Canceled inside beforeDispatch (line 111-113)
	ctx2, cancel2 := context.WithCancel(context.Background())
	gate2 := &dispatchGate{
		base: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		beforeDispatch: func() error {
			cancel2() // cancel during beforeDispatch!
			return nil
		},
	}
	req2, _ := http.NewRequestWithContext(ctx2, "GET", "https://bigquery.googleapis.com/bigquery/v2/test", nil)
	if _, err := gate2.RoundTrip(req2); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped when canceled in beforeDispatch, got %v", err)
	}

	// 3. base.RoundTrip returns both resp with Body AND error (line 123-124)
	gate3 := &dispatchGate{
		base: rt(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 500,
				Body:       io.NopCloser(strings.NewReader("err")),
			}, errors.New("underlying transport error")
		}),
	}
	req3, _ := http.NewRequestWithContext(context.Background(), "GET", "https://bigquery.googleapis.com/bigquery/v2/test", nil)
	_, _ = gate3.RoundTrip(req3)

	// 4. Canceled after response (line 144-145)
	ctx4, cancel4 := context.WithCancel(context.Background())
	gate4 := &dispatchGate{
		base: rt(func(*http.Request) (*http.Response, error) {
			cancel4() // cancel right after response is ready
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader("{}")),
			}, nil
		}),
	}
	req4, _ := http.NewRequestWithContext(ctx4, "GET", "https://bigquery.googleapis.com/bigquery/v2/test", nil)
	if _, err := gate4.RoundTrip(req4); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on cancel after response, got %v", err)
	}
}

func TestWire_BoundedTransport_RoundTrip_Errors(t *testing.T) {
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

	transport := &boundedTransport{
		c:        client,
		guard:    &dispatchGate{},
		gate:     rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		ctx:      context.Background(),
		deadline: time.Now().Add(time.Hour),
	}

	// 1. Request body read error (line 180-181)
	errBody := io.NopCloser(errReader{err: errors.New("read error")})
	req1, _ := http.NewRequest("POST", "https://bigquery.googleapis.com/test", errBody)
	if _, err := transport.RoundTrip(req1); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on body read error, got %v", err)
	}

	// 2. Request body > 256KB (line 180-181)
	hugeBody := io.NopCloser(bytes.NewReader(make([]byte, 256<<10+2)))
	req2, _ := http.NewRequest("POST", "https://bigquery.googleapis.com/test", hugeBody)
	if _, err := transport.RoundTrip(req2); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on oversized body, got %v", err)
	}

	// 3. Context canceled during awaitDependency (line 200-201)
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	transportSlow := &boundedTransport{
		c:     client,
		guard: &dispatchGate{},
		gate: rt(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
		}),
		ctx:      ctxCancel,
		deadline: time.Now().Add(time.Hour),
	}
	req3, _ := http.NewRequestWithContext(ctxCancel, "GET", "https://bigquery.googleapis.com/test", nil)
	_, _ = transportSlow.RoundTrip(req3)
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestWire_BoundedTransport_ValidateResponse_Remaining_And_Charge_Errors(t *testing.T) {
	t.Parallel()

	client, _ := NewClient(Config{
		Profiles:  []SourceProfile{testProfile(t)},
		Provider:  &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:    NewMemoryLedger(),
		Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})

	// 1. c.remaining fails because runID does not exist (line 230-231)
	scopeRunNotFound := &operationScope{
		runID:    "nonexistent-run",
		deadline: time.Now().Add(time.Hour),
		bounds:   Bounds{ResponseBytes: 1024, HTTPMs: 1000},
	}
	transport1 := &boundedTransport{
		c:        client,
		s:        scopeRunNotFound,
		ctx:      context.Background(),
		deadline: time.Now().Add(time.Hour),
	}
	resp1 := &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}
	if _, err := transport1.validateResponse(resp1, nil); err == nil {
		t.Fatal("expected error from remaining on nonexistent run")
	}

	// 2. t.bound() was set / deadline expired (line 223-225 & line 261-263)
	transport3 := &boundedTransport{
		c:        client,
		s:        &operationScope{deadline: time.Now().Add(time.Hour), bounds: Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 1000}},
		ctx:      context.Background(),
		deadline: time.Now().Add(-time.Hour), // Expired!
	}
	resp3 := &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}
	if _, err := transport3.validateResponse(resp3, nil); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped when deadline passed, got %v", err)
	}

	// 4. t.ctx.Err() != nil right before return (line 285-287)
	ctxCanceled, cancel := context.WithCancel(context.Background())
	cancel()
	transport4 := &boundedTransport{
		c:        client,
		s:        &operationScope{deadline: time.Now().Add(time.Hour), bounds: Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 1000}},
		ctx:      ctxCanceled,
		deadline: time.Now().Add(time.Hour),
	}
	resp4 := &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}
	if _, err := transport4.validateResponse(resp4, nil); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped when ctx canceled before return, got %v", err)
	}
}

func TestWire_Call_Remaining_And_ControlDeadlines(t *testing.T) {
	t.Parallel()

	client, _ := NewClient(Config{
		Profiles: []SourceProfile{testProfile(t)},
		Provider: &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, Cancel: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:   NewMemoryLedger(),
		Transport: rt(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
		}),
		Prepare: func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})

	principal := Principal{Kind: "workload", Subject: "s", Generation: "1"}

	// 1. control && s.controlDeadline.IsZero() (line 320-321)
	scopeControl := &operationScope{
		deadline: time.Now().Add(time.Hour),
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 5000},
	}
	_, _ = client.call(context.Background(), scopeControl, &principal, true, func(api sdkAPI) error { return nil })

	// 2. c.remaining(s) fails at start of call (line 328-329)
	scopeBadRun := &operationScope{
		runID:    "nonexistent",
		deadline: time.Now().Add(time.Hour),
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 5000},
	}
	if _, err := client.call(context.Background(), scopeBadRun, &principal, false, func(api sdkAPI) error { return nil }); err == nil {
		t.Fatal("expected error on nonexistent runID")
	}

	// 3. s.controlDeadline.Before(deadline) (line 337-338)
	scopeBefore := &operationScope{
		controlDeadline: time.Now().Add(100 * time.Millisecond),
		deadline:        time.Now().Add(time.Hour),
		bounds:          Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 5000},
	}
	_, _ = client.call(context.Background(), scopeBefore, &principal, true, func(api sdkAPI) error { return nil })

	// 4. !c.clock.Now().Before(deadline) with control (line 340-341)
	scopeExpiredControl := &operationScope{
		controlDeadline: time.Now().Add(-time.Second), // Already expired!
		deadline:        time.Now().Add(time.Hour),
		bounds:          Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 5000},
	}
	if _, err := client.call(context.Background(), scopeExpiredControl, &principal, true, func(api sdkAPI) error { return nil }); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on expired controlDeadline, got %v", err)
	}

	// 5. ctx.Err() != nil when call fn errors (line 434-435)
	scopeCtxErr := &operationScope{
		deadline: time.Now().Add(time.Hour),
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 5000},
	}
	ctxCanceled, cancel := context.WithCancel(context.Background())
	if _, err := client.call(ctxCanceled, scopeCtxErr, &principal, false, func(api sdkAPI) error {
		cancel() // Cancel context!
		return errors.New("call fn error")
	}); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped when ctx canceled during error, got %v", err)
	}

	// 6. Retry delay sleep canceled by context (line 471-472)
	calls := 0
	ctxRetry, cancelRetry := context.WithCancel(context.Background())
	clientRetry, _ := NewClient(Config{
		Profiles: []SourceProfile{testProfile(t)},
		Provider: &testProvider{identity: Identity{Principal: principal, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:   NewMemoryLedger(),
		Transport: rt(func(*http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				cancelRetry() // Cancel context before sleep!
				return &http.Response{
					StatusCode: 429,
					Header:     http.Header{"Retry-After": []string{"2"}},
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":429}}`)),
				}, nil
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
		}),
		Prepare: func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	scopeRetry := &operationScope{
		deadline: time.Now().Add(10 * time.Second),
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 5000},
	}
	_, err := clientRetry.call(ctxRetry, scopeRetry, &principal, false, func(api sdkAPI) error {
		_, e := api.service.Jobs.Get("valid-project", "j").Context(api.ctx).Do()
		return e
	})
	if err == nil {
		t.Fatal("expected error on canceled sleep retry")
	}

	// 7. checkIdentity context canceled before & after authorize (lines 501-502 & 505-506)
	ctxEarlyCancel, cancelEarly := context.WithCancel(context.Background())
	cancelEarly()
	if err := client.checkIdentity(ctxEarlyCancel, principal); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on canceled checkIdentity, got %v", err)
	}

	ctxLateCancel, cancelLate := context.WithCancel(context.Background())
	clientLate, _ := NewClient(Config{
		Profiles:  []SourceProfile{testProfile(t)},
		Provider:  cancelProvider{cancel: cancelLate},
		Ledger:    NewMemoryLedger(),
		Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	if err := clientLate.checkIdentity(ctxLateCancel, principal); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on late canceled checkIdentity, got %v", err)
	}
}

type cancelProvider struct {
	cancel func()
}

func (cp cancelProvider) Authorize(ctx context.Context, base http.RoundTripper) (Identity, http.RoundTripper, error) {
	cp.cancel()
	return Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}, base, nil
}

// Not parallel: mutates bqNewServiceFn
func TestWire_Call_BqNewServiceFn_Error(t *testing.T) {
	orig := bqNewServiceFn
	defer func() { bqNewServiceFn = orig }()
	bqNewServiceFn = func(context.Context, ...option.ClientOption) (*bq.Service, error) {
		return nil, errors.New("service creation failed")
	}

	client, _ := NewClient(Config{
		Profiles:  []SourceProfile{testProfile(t)},
		Provider:  &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:    NewMemoryLedger(),
		Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	scope := &operationScope{
		deadline: time.Now().Add(time.Hour),
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 5000},
	}
	principal := Principal{Kind: "workload", Subject: "s", Generation: "1"}
	if _, err := client.call(context.Background(), scope, &principal, false, func(api sdkAPI) error { return nil }); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on bqNewService error, got %v", err)
	}
}

// Not parallel: mutates wireDefaultAttempts
func TestWire_Call_DefaultAttempts_Zero(t *testing.T) {
	orig := wireDefaultAttempts
	defer func() { wireDefaultAttempts = orig }()
	wireDefaultAttempts = 0 // loop does not execute, hits line 486: return nil, last

	client, _ := NewClient(Config{
		Profiles:  []SourceProfile{testProfile(t)},
		Provider:  &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:    NewMemoryLedger(),
		Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	scope := &operationScope{
		deadline: time.Now().Add(time.Hour),
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 5000},
	}
	principal := Principal{Kind: "workload", Subject: "s", Generation: "1"}
	val, err := client.call(context.Background(), scope, &principal, false, func(api sdkAPI) error { return nil })
	if val != nil || err != nil {
		t.Fatalf("expected nil, nil when attempts == 0, got val=%v, err=%v", val, err)
	}
}

func TestWire_Call_BeforeDispatch_Errors(t *testing.T) {
	t.Parallel()

	client, _ := NewClient(Config{
		Profiles: []SourceProfile{testProfile(t)},
		Provider: &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:   NewMemoryLedger(),
		Transport: rt(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"kind":"bigquery#job"}`))}, nil
		}),
		Prepare: func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	principal := Principal{Kind: "workload", Subject: "s", Generation: "1"}

	// 1. remaining <= 0 in beforeDispatch (line 400-401)
	scopeExhausted := &operationScope{
		deadline: time.Now().Add(time.Hour),
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 100, HTTPMs: 5000},
		bytes:    100, // Remaining is 0!
	}
	_, err := client.call(context.Background(), scopeExhausted, &principal, false, func(api sdkAPI) error {
		_, e := api.service.Jobs.Get("valid-project", "j").Context(api.ctx).Do()
		return e
	})
	if errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit on exhausted remaining in beforeDispatch, got %v", err)
	}

	// 2. remaining fails in beforeDispatch (line 397-398)
	// We can set s.runID to nonexistent run right before dispatch!
	scopeRunNotFound := &operationScope{
		deadline: time.Now().Add(time.Hour),
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 5000},
	}
	_, err = client.call(context.Background(), scopeRunNotFound, &principal, false, func(api sdkAPI) error {
		scopeRunNotFound.runID = "nonexistent-run-id" // cause remaining() to fail inside beforeDispatch!
		_, e := api.service.Jobs.Get("valid-project", "j").Context(api.ctx).Do()
		return e
	})
	if err == nil {
		t.Fatal("expected error on failed remaining in beforeDispatch")
	}

	// 3. op.Err() != nil in beforeDispatch (line 391-392)
	// 3. !c.clock.Now().Before(deadline) in beforeDispatch (line 391-392)
	fakeCl := &fakeClock{now: time.Now()}
	clientExpClock, _ := NewClient(Config{
		Profiles:  []SourceProfile{testProfile(t)},
		Provider:  &testProvider{identity: Identity{Principal: principal, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:    NewMemoryLedger(),
		Clock:     fakeCl,
		Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	scopeCancel := &operationScope{
		deadline: fakeCl.Now().Add(5 * time.Second),
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 5000},
	}
	_, err = clientExpClock.call(context.Background(), scopeCancel, &principal, false, func(api sdkAPI) error {
		fakeCl.now = fakeCl.now.Add(10 * time.Second)
		_, e := api.service.Jobs.Get("valid-project", "j").Context(api.ctx).Do()
		return e
	})
	if errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on expired clock in beforeDispatch, got %v", err)
	}
}

type failingUpdateLedger struct {
	Ledger
}

func (f failingUpdateLedger) updateContext(ctx context.Context, fn func(*ledgerState) error) error {
	return fail("malformed_wire")
}

func TestWire_BoundedTransport_ValidateResponse_Charge_Error(t *testing.T) {
	t.Parallel()
	memLedger := NewMemoryLedger()
	_ = memLedger.updateContext(context.Background(), func(s *ledgerState) error {
		s.Runs["test-run"] = runRecord{Receipt: Receipt{Bounds: Bounds{TotalResponseBytes: 10000}}}
		return nil
	})
	client, _ := NewClient(Config{
		Profiles:  []SourceProfile{testProfile(t)},
		Provider:  &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:    failingUpdateLedger{Ledger: memLedger},
		Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	scopeChargeErr := &operationScope{
		runID:    "test-run",
		deadline: time.Now().Add(time.Hour),
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 1000},
	}
	transportChargeErr := &boundedTransport{
		c:        client,
		s:        scopeChargeErr,
		ctx:      context.Background(),
		deadline: time.Now().Add(time.Hour),
	}
	respCharge := &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}
	if _, err := transportChargeErr.validateResponse(respCharge, nil); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on charge error, got %v", err)
	}
}
