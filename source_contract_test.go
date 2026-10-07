package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
)

func sourceTestDatabase(t *testing.T, transport http.RoundTripper) *ReadOnlyDatabase {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)}
	provider := &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "reader", Generation: "1"}, Read: true, ExpiresAt: clock.Now().Add(time.Hour)}}
	db, e := NewReadOnlyDatabase(SourceConfig{ProjectID: "fixture-project", DatasetID: "fixture_dataset", Provider: provider, Transport: transport, Clock: clock})
	if e != nil {
		t.Fatal(e)
	}
	return db
}

func sourceTestJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

func sourceTestTable(fields []any, count, etag string) string {
	return sourceTestJSON(map[string]any{
		"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"},
		"type":           "TABLE", "etag": etag, "lastModifiedTime": "1000", "numRows": count,
		"schema":           map[string]any{"fields": fields},
		"tableConstraints": map[string]any{"primaryKey": map[string]any{"columns": []string{"id"}}, "foreignKeys": []any{map[string]any{"name": "fk_parent", "referencedTable": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "parent"}, "columnReferences": []any{map[string]any{"referencingColumn": "id", "referencedColumn": "id"}}}}},
	})
}

func sourceTestFields() []any {
	return []any{
		map[string]any{"name": "id", "type": "INT64", "mode": "REQUIRED", "description": "stable source id"},
		map[string]any{"name": "amount", "type": "NUMERIC", "mode": "NULLABLE"},
		map[string]any{"name": "blob", "type": "BYTES", "mode": "NULLABLE"},
		map[string]any{"name": "empty_blob", "type": "BYTES", "mode": "NULLABLE"},
		map[string]any{"name": "day", "type": "DATE", "mode": "NULLABLE"},
		map[string]any{"name": "when", "type": "TIMESTAMP", "mode": "NULLABLE"},
	}
}

func sourceTestRow(values ...any) map[string]any {
	f := make([]any, len(values))
	for i, v := range values {
		f[i] = map[string]any{"v": v}
	}
	return map[string]any{"f": f}
}

func TestReadOnlySourceSchemaAndPages(t *testing.T) {
	var requests []string
	page := 0
	db := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		requests = append(requests, req.Method+" "+req.URL.String())
		path := req.URL.Path
		switch {
		case strings.HasSuffix(path, "/fixture_dataset"):
			return response(sourceTestJSON(map[string]any{"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}})), nil
		case strings.HasSuffix(path, "/tables"):
			return response(sourceTestJSON(map[string]any{"totalItems": 1, "tables": []any{map[string]any{"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"}, "type": "TABLE"}}})), nil
		case strings.HasSuffix(path, "/tables/sample"):
			return response(sourceTestTable(sourceTestFields(), "2", "e1")), nil
		case strings.HasSuffix(path, "/tables/sample/data"):
			page++
			if req.URL.Query().Get("formatOptions.useInt64Timestamp") != "true" {
				t.Fatal("missing exact timestamp format")
			}
			if page == 1 {
				return response(sourceTestJSON(map[string]any{"kind": "bigquery#tableDataList", "totalRows": "2", "pageToken": "two", "rows": []any{sourceTestRow("9007199254740993", "12345678901234567890.12", "AP8A", "", "2026-10-07", "1791370000000000")}})), nil
			}
			return response(sourceTestJSON(map[string]any{"kind": "bigquery#tableDataList", "totalRows": "2", "rows": []any{sourceTestRow("9007199254740994", nil, nil, "", nil, nil)}})), nil
		}
		t.Fatalf("unexpected request %s", req.URL.String())
		return nil, nil
	}))
	refs, e := dbschema.ListCollections(context.Background(), db, nil)
	if e != nil || len(refs) != 1 || refs[0].Name() != "sample" {
		t.Fatalf("collections %v: %v", refs, e)
	}
	def, e := dbschema.DescribeCollection(context.Background(), db, &refs[0])
	if e != nil || len(def.Fields) != 6 || len(def.PrimaryKey) != 1 || len(def.ForeignKeys) != 1 || def.ForeignKeys[0].Enforcement != dbschema.ForeignKeyEnforcementDisabled {
		t.Fatalf("definition %#v: %v", def, e)
	}
	fields, e := db.DescribeSourceFields(context.Background(), &refs[0])
	if e != nil || fields[0].Description != "stable source id" {
		t.Fatalf("source fields %#v: %v", fields, e)
	}
	cursor, e := db.OpenSourceRows(context.Background(), &refs[0])
	if e != nil {
		t.Fatal(e)
	}
	defer cursor.Close()
	first, e := cursor.Next()
	if e != nil {
		t.Fatal(e)
	}
	if first.Values["id"] != int64(9007199254740993) || first.Values["amount"] != "12345678901234567890.12" || !reflect.DeepEqual(first.Values["blob"], []byte{0, 255, 0}) || !reflect.DeepEqual(first.Values["empty_blob"], []byte{}) || first.Values["day"] != "2026-10-07" {
		t.Fatalf("first row %#v", first.Values)
	}
	if _, ok := first.Values["when"].(time.Time); !ok {
		t.Fatalf("timestamp %T", first.Values["when"])
	}
	second, e := cursor.Next()
	if e != nil {
		t.Fatal(e)
	}
	if second.Values["amount"] != nil || second.Values["blob"] != nil || !reflect.DeepEqual(second.Values["empty_blob"], []byte{}) {
		t.Fatalf("null/empty %#v", second.Values)
	}
	if _, e = cursor.Next(); !errors.Is(e, io.EOF) {
		t.Fatalf("end: %v", e)
	}
	for _, req := range requests {
		if !strings.HasPrefix(req, "GET https://bigquery.googleapis.com/bigquery/v2/projects/fixture-project/datasets/fixture_dataset") || strings.Contains(req, "/jobs") {
			t.Fatal(req)
		}
	}
	if page != 2 {
		t.Fatalf("pages=%d", page)
	}
}

func TestReadOnlySourceRejectsUnsupportedRowsBeforeData(t *testing.T) {
	dataRequests := 0
	db := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/data") {
			dataRequests++
		}
		if strings.HasSuffix(req.URL.Path, "/tables/sample") {
			return response(sourceTestTable([]any{map[string]any{"name": "nested", "type": "RECORD", "mode": "NULLABLE", "fields": []any{map[string]any{"name": "x", "type": "INT64"}}}}, "1", "e1")), nil
		}
		return response(sourceTestJSON(map[string]any{"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}})), nil
	}))
	ref := dal.NewRootCollectionRef("sample", "")
	if _, e := db.OpenSourceRows(context.Background(), &ref); e == nil || dataRequests != 0 {
		t.Fatalf("unsupported nested schema reached data route: %v, %d", e, dataRequests)
	}
}

func TestReadOnlySourcePageFailuresAreNotEOF(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second string
		finalEtag     string
	}{
		{"token-loop", sourceTestJSON(map[string]any{"kind": "bigquery#tableDataList", "totalRows": "2", "pageToken": "again", "rows": []any{sourceTestRow("1")}}), sourceTestJSON(map[string]any{"kind": "bigquery#tableDataList", "totalRows": "2", "pageToken": "again", "rows": []any{sourceTestRow("2")}}), "e1"},
		{"total-drift", sourceTestJSON(map[string]any{"kind": "bigquery#tableDataList", "totalRows": "2", "pageToken": "again", "rows": []any{sourceTestRow("1")}}), sourceTestJSON(map[string]any{"kind": "bigquery#tableDataList", "totalRows": "3", "rows": []any{sourceTestRow("2")}}), "e1"},
		{"short-read", sourceTestJSON(map[string]any{"kind": "bigquery#tableDataList", "totalRows": "2", "rows": []any{sourceTestRow("1")}}), "", "e1"},
		{"metadata-drift", sourceTestJSON(map[string]any{"kind": "bigquery#tableDataList", "totalRows": "2", "rows": []any{sourceTestRow("1"), sourceTestRow("2")}}), "", "e2"},
		{"bad-bytes", sourceTestJSON(map[string]any{"kind": "bigquery#tableDataList", "totalRows": "2", "rows": []any{sourceTestRow("1", "not-base64!")}}), "", "e1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pages, gets := 0, 0
			fields := []any{map[string]any{"name": "id", "type": "INT64", "mode": "REQUIRED"}}
			if tc.name == "bad-bytes" {
				fields = append(fields, map[string]any{"name": "blob", "type": "BYTES"})
			}
			db := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
				switch {
				case strings.HasSuffix(req.URL.Path, "/fixture_dataset"):
					return response(sourceTestJSON(map[string]any{"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}})), nil
				case strings.HasSuffix(req.URL.Path, "/tables/sample"):
					gets++
					etag := "e1"
					if gets > 1 {
						etag = tc.finalEtag
					}
					return response(sourceTestTable(fields, "2", etag)), nil
				case strings.HasSuffix(req.URL.Path, "/tables/sample/data"):
					pages++
					if pages == 1 {
						return response(tc.first), nil
					}
					return response(tc.second), nil
				}
				t.Fatalf("unexpected request %s", req.URL)
				return nil, nil
			}))
			ref := dal.NewRootCollectionRef("sample", "")
			cursor, e := db.OpenSourceRows(context.Background(), &ref)
			if e != nil {
				t.Fatal(e)
			}
			defer cursor.Close()
			var terminal error
			for i := 0; i < 4; i++ {
				_, terminal = cursor.Next()
				if terminal != nil {
					break
				}
			}
			if terminal == nil || errors.Is(terminal, io.EOF) {
				t.Fatalf("incomplete/changed source reported clean EOF: %v", terminal)
			}
		})
	}
}

func TestReadOnlySourceMetadataInventoryAndView(t *testing.T) {
	requests := 0
	db := sourceTestDatabase(t, rt(func(req *http.Request) (*http.Response, error) {
		requests++
		if strings.HasSuffix(req.URL.Path, "/fixture_dataset") {
			return response(sourceTestJSON(map[string]any{"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}})), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables") {
			return response(sourceTestJSON(map[string]any{"totalItems": 2, "tables": []any{
				map[string]any{"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "sample"}, "type": "TABLE"},
				map[string]any{"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "report"}, "type": "VIEW"},
			}})), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/report") {
			return response(sourceTestJSON(map[string]any{"tableReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset", "tableId": "report"}, "type": "VIEW", "view": map[string]any{"query": "SELECT id FROM sample"}, "schema": map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INT64"}}}})), nil
		}
		t.Fatalf("unexpected request %s", req.URL)
		return nil, nil
	}))
	refs, e := dbschema.ListCollections(context.Background(), db, nil)
	if e != nil || len(refs) != 1 || refs[0].Name() != "sample" {
		t.Fatalf("table inventory %v %v", refs, e)
	}
	views, e := db.ListSourceViews(context.Background())
	if e != nil || len(views) != 1 || views[0].Name != "report" || len(views[0].Columns) != 1 || views[0].Columns[0] != "id" || views[0].CreateSQL != "" {
		t.Fatalf("view inventory %v %v", views, e)
	}
	ref := dal.NewRootCollectionRef("report", "")
	if _, e := db.OpenSourceRows(context.Background(), &ref); e == nil {
		t.Fatal("view data should be refused")
	}
	if requests < 5 {
		t.Fatalf("incomplete metadata path: %d", requests)
	}
}

func TestReadOnlySourceTransportAllowsOnlyMetadataAndTabledataGet(t *testing.T) {
	requests := 0
	guard := sourceTransport{base: rt(func(*http.Request) (*http.Response, error) { requests++; return response(`{}`), nil }), project: "fixture-project", dataset: "fixture_dataset"}
	for _, methodAndURL := range []struct{ method, url string }{
		{"POST", "https://bigquery.googleapis.com/bigquery/v2/projects/fixture-project/jobs"},
		{"GET", "https://bigquery.googleapis.com/bigquery/v2/projects/other-project/datasets/fixture_dataset/tables/sample/data?formatOptions.useInt64Timestamp=true"},
		{"GET", "https://bigquery.googleapis.com/bigquery/v2/projects/fixture-project/datasets/fixture_dataset/tables/sample/data?selectedFields=id&formatOptions.useInt64Timestamp=true"},
		{"GET", "https://bigquery.googleapis.com/bigquery/v2/projects/fixture-project/datasets/fixture_dataset/tables/sample/data"},
	} {
		req, e := http.NewRequest(methodAndURL.method, methodAndURL.url, nil)
		if e != nil {
			t.Fatal(e)
		}
		if _, e := guard.RoundTrip(req); e == nil {
			t.Fatalf("unsafe request dispatched: %s", methodAndURL.url)
		}
	}
	if requests != 0 {
		t.Fatalf("unsafe requests reached transport: %d", requests)
	}
}

func TestReadOnlySourceScalarFidelityAndPreflight(t *testing.T) {
	fields := []Field{
		{Name: "n", Type: "BIGNUMERIC", Mode: "NULLABLE"},
		{Name: "f", Type: "FLOAT64", Mode: "NULLABLE"},
		{Name: "b", Type: "BOOL", Mode: "NULLABLE"},
		{Name: "s", Type: "STRING", Mode: "NULLABLE"},
		{Name: "t", Type: "TIME", Mode: "NULLABLE"},
		{Name: "dt", Type: "DATETIME", Mode: "NULLABLE"},
		{Name: "j", Type: "JSON", Mode: "NULLABLE"},
	}
	row, e := sourceDecodeRow(sourceTestRow("123456789012345678901234567890.123456789012345678", "1.25e2", "true", "", "12:34:56.123456", "2026-10-07 12:34:56", `{"x":12345678901234567890}`), fields)
	if e != nil {
		t.Fatal(e)
	}
	if row.Values["n"] != "123456789012345678901234567890.123456789012345678" || row.Values["f"] != float64(125) || row.Values["b"] != true || row.Values["s"] != "" || row.Values["t"] != "12:34:56.123456" || row.Values["dt"] != "2026-10-07T12:34:56" || row.Values["j"] != `{"x":12345678901234567890}` {
		t.Fatalf("scalar drift: %#v", row.Values)
	}
	for _, bad := range []any{sourceTestRow("0", "NaN", "true", "", "12:34:56", "2026-10-07 12:34:56", "null"), sourceTestRow("0", "1", "1", "", "12:34:56", "2026-10-07 12:34:56", "null"), sourceTestRow("0", "1", "true", "", "12:34:56", "2026-10-07 12:34:56", "{")} {
		if _, e := sourceDecodeRow(bad, fields); e == nil {
			t.Fatalf("accepted malformed scalar %#v", bad)
		}
	}
	for _, invalid := range []any{
		map[string]any{"name": "x", "type": "INT64", "mode": "REPEATED"},
		map[string]any{"name": "x", "type": "STRING", "mode": "NULLABLE", "maxLength": "4"},
		map[string]any{"name": "x", "type": "BIGNUMERIC", "mode": "NULLABLE", "scale": "2"},
		map[string]any{"name": "x", "type": "GEOGRAPHY", "mode": "NULLABLE"},
	} {
		if _, e := sourceFields(map[string]any{"schema": map[string]any{"fields": []any{invalid}}}); e == nil {
			t.Fatalf("accepted unsupported schema %#v", invalid)
		}
	}
}

func TestReadOnlySourceCancellationAndLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	requests := 0
	db := sourceTestDatabase(t, rt(func(*http.Request) (*http.Response, error) { requests++; return response(`{}`), nil }))
	if _, e := dbschema.ListCollections(ctx, db, nil); e == nil || requests != 0 {
		t.Fatalf("canceled metadata dispatched: %v, %d", e, requests)
	}
	if _, e := NewReadOnlyDatabase(SourceConfig{ProjectID: "fixture-project", DatasetID: "fixture_dataset", Provider: db.source.provider, Transport: db.source.transport, Limits: SourceLimits{PageSize: 1001}}); e == nil {
		t.Fatal("invalid bounds accepted")
	}
	limited, e := NewReadOnlyDatabase(SourceConfig{ProjectID: "fixture-project", DatasetID: "fixture_dataset", Provider: db.source.provider, Transport: rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/fixture_dataset") {
			return response(sourceTestJSON(map[string]any{"datasetReference": map[string]any{"projectId": "fixture-project", "datasetId": "fixture_dataset"}})), nil
		}
		return response(sourceTestTable([]any{map[string]any{"name": "id", "type": "INT64", "mode": "REQUIRED"}}, "2", "e1")), nil
	}), Clock: db.source.clock, Limits: SourceLimits{MaxRows: 1}})
	if e != nil {
		t.Fatal(e)
	}
	ref := dal.NewRootCollectionRef("sample", "")
	if _, e := limited.OpenSourceRows(context.Background(), &ref); e == nil {
		t.Fatal("max rows silently truncated")
	}
}
