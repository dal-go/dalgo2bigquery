package bigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	bq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestMetadata_ValidateFields_Exhaustive(t *testing.T) {
	t.Parallel()

	// Empty fields
	if err := validateFields(nil); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for empty fields, got %v", err)
	}

	// Invalid identifier
	if err := validateFields([]Field{{Name: "123bad", Type: "STRING", Mode: "NULLABLE"}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for bad identifier, got %v", err)
	}

	// Duplicate field name
	if err := validateFields([]Field{
		{Name: "col1", Type: "STRING", Mode: "NULLABLE"},
		{Name: "col1", Type: "INT64", Mode: "NULLABLE"},
	}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for duplicate field, got %v", err)
	}

	// Empty type
	if err := validateFields([]Field{{Name: "col1", Type: "", Mode: "NULLABLE"}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for empty type, got %v", err)
	}

	// Invalid mode
	if err := validateFields([]Field{{Name: "col1", Type: "STRING", Mode: "INVALID"}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for invalid mode, got %v", err)
	}

	// RECORD with len(Fields) < 1
	if err := validateFields([]Field{{Name: "rec", Type: "RECORD", Mode: "NULLABLE"}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for empty RECORD, got %v", err)
	}

	// RECORD with invalid child field
	if err := validateFields([]Field{{Name: "rec", Type: "RECORD", Mode: "NULLABLE", Fields: []Field{
		{Name: "bad child", Type: "STRING", Mode: "NULLABLE"},
	}}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for bad child in RECORD, got %v", err)
	}

	// Non-RECORD with len(Fields) > 0
	if err := validateFields([]Field{{Name: "col1", Type: "STRING", Mode: "NULLABLE", Fields: []Field{
		{Name: "sub", Type: "STRING", Mode: "NULLABLE"},
	}}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for string with subfields, got %v", err)
	}

	// Scale without Precision
	if err := validateFields([]Field{{Name: "num", Type: "NUMERIC", Mode: "NULLABLE", Scale: "2"}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for scale without precision, got %v", err)
	}

	// Precision on non-NUMERIC/BIGNUMERIC
	if err := validateFields([]Field{{Name: "num", Type: "INT64", Mode: "NULLABLE", Precision: "10"}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for precision on INT64, got %v", err)
	}

	// Invalid Precision string (leading zero or negative)
	if err := validateFields([]Field{{Name: "num", Type: "NUMERIC", Mode: "NULLABLE", Precision: "05"}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for precision 05, got %v", err)
	}

	// Invalid Scale string
	if err := validateFields([]Field{{Name: "num", Type: "NUMERIC", Mode: "NULLABLE", Precision: "10", Scale: "02"}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for scale 02, got %v", err)
	}

	// Scale > maxScale (NUMERIC maxScale=9)
	if err := validateFields([]Field{{Name: "num", Type: "NUMERIC", Mode: "NULLABLE", Precision: "20", Scale: "10"}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for NUMERIC scale 10, got %v", err)
	}

	// Precision < max(1, s)
	if err := validateFields([]Field{{Name: "num", Type: "NUMERIC", Mode: "NULLABLE", Precision: "2", Scale: "5"}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for precision < scale, got %v", err)
	}

	// Precision > maxI + s (NUMERIC maxI=29)
	if err := validateFields([]Field{{Name: "num", Type: "NUMERIC", Mode: "NULLABLE", Precision: "40", Scale: "2"}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for precision too large, got %v", err)
	}

	// Valid NUMERIC with precision & scale
	if err := validateFields([]Field{{Name: "num", Type: "NUMERIC", Mode: "NULLABLE", Precision: "10", Scale: "2"}}); err != nil {
		t.Fatalf("expected valid NUMERIC, got %v", err)
	}

	// Valid BIGNUMERIC with precision & scale
	if err := validateFields([]Field{{Name: "bignum", Type: "BIGNUMERIC", Mode: "REQUIRED", Precision: "50", Scale: "20"}}); err != nil {
		t.Fatalf("expected valid BIGNUMERIC, got %v", err)
	}

	// Depth > 8
	nested := Field{Name: "leaf", Type: "STRING", Mode: "NULLABLE"}
	for i := 0; i < 9; i++ {
		nested = Field{Name: fmt.Sprintf("n%d", i), Type: "RECORD", Mode: "NULLABLE", Fields: []Field{nested}}
	}
	if err := validateFields([]Field{nested}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for depth > 8, got %v", err)
	}

	// Count > 1024
	many := make([]Field, 1025)
	for i := range many {
		many[i] = Field{Name: fmt.Sprintf("f%d", i), Type: "INT64", Mode: "NULLABLE"}
	}
	if err := validateFields(many); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for count > 1024, got %v", err)
	}
}

func TestMetadata_DecodeSchema_Exhaustive(t *testing.T) {
	t.Parallel()

	// Non-object
	if _, err := decodeSchema("not-an-object"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for string schema, got %v", err)
	}

	// Unknown key in schema object
	if _, err := decodeSchema(map[string]any{"fields": []any{}, "unknown": 123}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for unknown key in schema, got %v", err)
	}

	// fields is not a slice
	if _, err := decodeSchema(map[string]any{"fields": "not-a-slice"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-slice fields, got %v", err)
	}

	// fields is empty
	if _, err := decodeSchema(map[string]any{"fields": []any{}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for empty fields slice, got %v", err)
	}

	// Field item is not an object
	if _, err := decodeSchema(map[string]any{"fields": []any{"not-field-object"}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-object field item, got %v", err)
	}

	// Field item has unknown key
	if _, err := decodeSchema(map[string]any{"fields": []any{
		map[string]any{"name": "a", "type": "STRING", "bad_unknown": true},
	}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for unknown key in field item, got %v", err)
	}

	// Field item has rejected config (e.g. collation)
	if _, err := decodeSchema(map[string]any{"fields": []any{
		map[string]any{"name": "a", "type": "STRING", "collation": "und:ci"},
	}}); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for collation in field item, got %v", err)
	}

	// Missing name
	if _, err := decodeSchema(map[string]any{"fields": []any{
		map[string]any{"type": "STRING"},
	}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for missing name, got %v", err)
	}

	// Missing type
	if _, err := decodeSchema(map[string]any{"fields": []any{
		map[string]any{"name": "a"},
	}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for missing type, got %v", err)
	}

	// Mode is not string
	if _, err := decodeSchema(map[string]any{"fields": []any{
		map[string]any{"name": "a", "type": "STRING", "mode": 123},
	}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-string mode, got %v", err)
	}

	// Precision is not string
	if _, err := decodeSchema(map[string]any{"fields": []any{
		map[string]any{"name": "a", "type": "NUMERIC", "precision": 10},
	}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-string precision, got %v", err)
	}

	// Scale is not string
	if _, err := decodeSchema(map[string]any{"fields": []any{
		map[string]any{"name": "a", "type": "NUMERIC", "precision": "10", "scale": 2},
	}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-string scale, got %v", err)
	}

	// Nested fields decode error
	if _, err := decodeSchema(map[string]any{"fields": []any{
		map[string]any{"name": "rec", "type": "RECORD", "fields": "not-a-slice"},
	}}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for bad nested fields, got %v", err)
	}

	// Count > 1024 fields
	manyItems := make([]any, 1025)
	for i := range manyItems {
		manyItems[i] = map[string]any{"name": fmt.Sprintf("f%d", i), "type": "STRING"}
	}
	if _, err := decodeSchema(map[string]any{"fields": manyItems}); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit for >1024 fields, got %v", err)
	}

	// Valid schema with precision, scale, and description
	fs, err := decodeSchema(map[string]any{"fields": []any{
		map[string]any{"name": "num", "type": "NUMERIC", "mode": "REQUIRED", "precision": "10", "scale": "2", "description": "a number"},
	}})
	if err != nil || len(fs) != 1 || fs[0].Precision != "10" || fs[0].Scale != "2" {
		t.Fatalf("expected valid decodeSchema, got %+v, err: %v", fs, err)
	}
}

func TestMetadata_ValidateDataset_Exhaustive(t *testing.T) {
	t.Parallel()

	p := testProfile(t)

	// Non-object raw
	if err := validateDataset("not-an-object", p); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-object dataset, got %v", err)
	}

	// Unknown metadata key
	if err := validateDataset(map[string]any{"unknown_key": true}, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for unknown key in dataset, got %v", err)
	}

	// Rejected config present
	if err := validateDataset(map[string]any{"restrictions": "some restriction"}, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for rejected restrictions in dataset, got %v", err)
	}

	// Ineligible dataset type (e.g. EXTERNAL)
	if err := validateDataset(map[string]any{
		"datasetReference": map[string]any{"projectId": p.SourceProject, "datasetId": p.DatasetID},
		"location":         p.Location,
		"type":             "EXTERNAL",
	}, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for EXTERNAL type, got %v", err)
	}

	// datasetReference is not an object
	if err := validateDataset(map[string]any{
		"datasetReference": "not-an-object",
		"location":         p.Location,
	}, p); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-object datasetReference, got %v", err)
	}

	// datasetReference has unknown key
	if err := validateDataset(map[string]any{
		"datasetReference": map[string]any{"projectId": p.SourceProject, "datasetId": p.DatasetID, "extra": 1},
		"location":         p.Location,
	}, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for extra key in datasetReference, got %v", err)
	}

	// Source changed (project mismatch)
	if err := validateDataset(map[string]any{
		"datasetReference": map[string]any{"projectId": "other-proj", "datasetId": p.DatasetID},
		"location":         p.Location,
	}, p); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed for project mismatch, got %v", err)
	}

	// Source changed (dataset mismatch)
	if err := validateDataset(map[string]any{
		"datasetReference": map[string]any{"projectId": p.SourceProject, "datasetId": "other-ds"},
		"location":         p.Location,
	}, p); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed for datasetId mismatch, got %v", err)
	}

	// Source changed (location mismatch)
	if err := validateDataset(map[string]any{
		"datasetReference": map[string]any{"projectId": p.SourceProject, "datasetId": p.DatasetID},
		"location":         "ASIA",
	}, p); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed for location mismatch, got %v", err)
	}

	// Valid dataset with type PUBLIC
	if err := validateDataset(map[string]any{
		"datasetReference": map[string]any{"projectId": p.SourceProject, "datasetId": p.DatasetID},
		"location":         p.Location,
		"type":             "PUBLIC",
	}, p); err != nil {
		t.Fatalf("expected valid dataset with PUBLIC type, got %v", err)
	}
}

func TestMetadata_ValidateTable_Exhaustive(t *testing.T) {
	t.Parallel()

	p := testProfile(t)
	baseTable := func() map[string]any {
		return map[string]any{
			"tableReference": map[string]any{"projectId": p.SourceProject, "datasetId": p.DatasetID, "tableId": p.TableID},
			"location":       p.Location,
			"type":           "TABLE",
			"schema": map[string]any{
				"fields": []any{
					map[string]any{"name": "n", "type": p.Schema[0].Type, "mode": "NULLABLE"},
				},
			},
			"etag":             "etag-123",
			"lastModifiedTime": "1700000000000",
		}
	}

	// Non-object
	if _, err := validateTable("not-object", p); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// Unknown key
	tUnknown := baseTable()
	tUnknown["unknown_field"] = "bad"
	if _, err := validateTable(tUnknown, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for unknown field, got %v", err)
	}

	// Rejected config (e.g. view)
	tRejected := baseTable()
	tRejected["view"] = map[string]any{"query": "SELECT 1"}
	if _, err := validateTable(tRejected, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for view, got %v", err)
	}

	// tableReference not object
	tBadRef := baseTable()
	tBadRef["tableReference"] = "not-object"
	if _, err := validateTable(tBadRef, p); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// tableReference unknown key
	tRefExtra := baseTable()
	tRefExtra["tableReference"] = map[string]any{"projectId": p.SourceProject, "datasetId": p.DatasetID, "tableId": p.TableID, "extra": 1}
	if _, err := validateTable(tRefExtra, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible, got %v", err)
	}

	// tableReference mismatch
	tRefDiff := baseTable()
	tRefDiff["tableReference"] = map[string]any{"projectId": p.SourceProject, "datasetId": p.DatasetID, "tableId": "other-tbl"}
	if _, err := validateTable(tRefDiff, p); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed, got %v", err)
	}

	// type != TABLE
	tNotTable := baseTable()
	tNotTable["type"] = "VIEW"
	if _, err := validateTable(tNotTable, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for VIEW, got %v", err)
	}

	// location mismatch
	tLocDiff := baseTable()
	tLocDiff["location"] = "ASIA"
	if _, err := validateTable(tLocDiff, p); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed for location diff, got %v", err)
	}

	// schema decode failure
	tBadSchema := baseTable()
	tBadSchema["schema"] = "bad"
	if _, err := validateTable(tBadSchema, p); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for bad schema, got %v", err)
	}

	// schema diff
	tDiffSchema := baseTable()
	tDiffSchema["schema"] = map[string]any{
		"fields": []any{
			map[string]any{"name": "other_col", "type": "STRING", "mode": "NULLABLE"},
		},
	}
	if _, err := validateTable(tDiffSchema, p); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed for schema mismatch, got %v", err)
	}

	// nil config value for supported key
	tNilConfig := baseTable()
	tNilConfig["clustering"] = nil
	if _, err := validateTable(tNilConfig, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for nil clustering, got %v", err)
	}

	// requirePartitionFilter not bool
	tBadFilter := baseTable()
	tBadFilter["requirePartitionFilter"] = "true"
	if _, err := validateTable(tBadFilter, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for string requirePartitionFilter, got %v", err)
	}

	// valid requirePartitionFilter
	tGoodFilter := baseTable()
	tGoodFilter["requirePartitionFilter"] = true
	if obs, err := validateTable(tGoodFilter, p); err != nil || obs.Config["requirePartitionFilter"] != true {
		t.Fatalf("expected valid table with requirePartitionFilter, got %v", err)
	}

	// Clustering:
	// 1. Not an object
	tClustNonObj := baseTable()
	tClustNonObj["clustering"] = "bad"
	if _, err := validateTable(tClustNonObj, p); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-object clustering, got %v", err)
	}
	// 2. Unknown key in clustering
	tClustUnknown := baseTable()
	tClustUnknown["clustering"] = map[string]any{"fields": []any{"n"}, "extra": 1}
	if _, err := validateTable(tClustUnknown, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for extra key in clustering, got %v", err)
	}
	// 3. fields not slice or empty or > 4
	tClustEmpty := baseTable()
	tClustEmpty["clustering"] = map[string]any{"fields": []any{}}
	if _, err := validateTable(tClustEmpty, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for empty clustering fields, got %v", err)
	}
	tClustTooMany := baseTable()
	tClustTooMany["clustering"] = map[string]any{"fields": []any{"n", "n", "n", "n", "n"}}
	if _, err := validateTable(tClustTooMany, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for >4 clustering fields, got %v", err)
	}
	// 4. field not exists
	tClustNotExist := baseTable()
	tClustNotExist["clustering"] = map[string]any{"fields": []any{"nonexistent"}}
	if _, err := validateTable(tClustNotExist, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for nonexistent cluster field, got %v", err)
	}
	// 5. valid clustering
	tClustValid := baseTable()
	tClustValid["clustering"] = map[string]any{"fields": []any{"n"}}
	if obs, err := validateTable(tClustValid, p); err != nil || obs.Config["clustering"] == nil {
		t.Fatalf("expected valid clustering, got %v", err)
	}

	// Time Partitioning:
	// 1. Both timePartitioning and rangePartitioning
	tBoth := baseTable()
	tBoth["timePartitioning"] = map[string]any{"type": "DAY"}
	tBoth["rangePartitioning"] = map[string]any{"field": "n"}
	if _, err := validateTable(tBoth, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for both partitionings, got %v", err)
	}
	// 2. timePartitioning not object
	tTimeNonObj := baseTable()
	tTimeNonObj["timePartitioning"] = "bad"
	if _, err := validateTable(tTimeNonObj, p); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}
	// 3. Unknown key
	tTimeExtra := baseTable()
	tTimeExtra["timePartitioning"] = map[string]any{"type": "DAY", "extra": 1}
	if _, err := validateTable(tTimeExtra, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible, got %v", err)
	}
	// 4. Invalid type (e.g. MINUTE)
	tTimeBadType := baseTable()
	tTimeBadType["timePartitioning"] = map[string]any{"type": "MINUTE"}
	if _, err := validateTable(tTimeBadType, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for MINUTE type, got %v", err)
	}
	// 5. Partition field not DATE/TIMESTAMP/DATETIME
	tTimeBadField := baseTable()
	tTimeBadField["timePartitioning"] = map[string]any{"type": "DAY", "field": "n"}
	if _, err := validateTable(tTimeBadField, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for INT64 partition field, got %v", err)
	}
	// 6. Invalid expirationMs
	tTimeBadExp := baseTable()
	tTimeBadExp["timePartitioning"] = map[string]any{"type": "DAY", "expirationMs": "not-a-number"}
	if _, err := validateTable(tTimeBadExp, p); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for bad expirationMs, got %v", err)
	}
	// 7. requirePartitionFilter not bool in timePartitioning
	tTimeBadFilter := baseTable()
	tTimeBadFilter["timePartitioning"] = map[string]any{"type": "DAY", "requirePartitionFilter": "yes"}
	if _, err := validateTable(tTimeBadFilter, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for non-bool filter, got %v", err)
	}
	// 8. valid timePartitioning with HOUR and expirationMs
	tTimeValid := baseTable()
	tTimeValid["timePartitioning"] = map[string]any{"type": "HOUR", "expirationMs": "3600000", "requirePartitionFilter": true}
	if obs, err := validateTable(tTimeValid, p); err != nil || obs.Config["timePartitioning"] == nil {
		t.Fatalf("expected valid timePartitioning, got %v", err)
	}

	// Range Partitioning:
	// 1. Not an object
	tRangeNonObj := baseTable()
	tRangeNonObj["rangePartitioning"] = "bad"
	if _, err := validateTable(tRangeNonObj, p); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}
	// 2. Unknown key
	tRangeExtra := baseTable()
	tRangeExtra["rangePartitioning"] = map[string]any{"field": "n", "extra": 1}
	if _, err := validateTable(tRangeExtra, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible, got %v", err)
	}
	// 3. Field does not exist
	tRangeNoField := baseTable()
	tRangeNoField["rangePartitioning"] = map[string]any{"field": "nonexistent"}
	if _, err := validateTable(tRangeNoField, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for nonexistent range field, got %v", err)
	}
	// 4. range is not an object
	tRangeBadRange := baseTable()
	tRangeBadRange["rangePartitioning"] = map[string]any{"field": "n", "range": "not-obj"}
	if _, err := validateTable(tRangeBadRange, p); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for bad range, got %v", err)
	}
	// 5. range has unknown key
	tRangeRangeExtra := baseTable()
	tRangeRangeExtra["rangePartitioning"] = map[string]any{"field": "n", "range": map[string]any{"start": "0", "end": "100", "interval": "10", "extra": 1}}
	if _, err := validateTable(tRangeRangeExtra, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for extra key in range, got %v", err)
	}
	// 6. start not integer text
	tRangeBadStart := baseTable()
	tRangeBadStart["rangePartitioning"] = map[string]any{"field": "n", "range": map[string]any{"start": "abc", "end": "100", "interval": "10"}}
	if _, err := validateTable(tRangeBadStart, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for bad start, got %v", err)
	}
	// 7. start >= end
	tRangeStartGteEnd := baseTable()
	tRangeStartGteEnd["rangePartitioning"] = map[string]any{"field": "n", "range": map[string]any{"start": "100", "end": "50", "interval": "10"}}
	if _, err := validateTable(tRangeStartGteEnd, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for start >= end, got %v", err)
	}
	// 8. interval < 1
	tRangeZeroInterval := baseTable()
	tRangeZeroInterval["rangePartitioning"] = map[string]any{"field": "n", "range": map[string]any{"start": "0", "end": "100", "interval": "0"}}
	if _, err := validateTable(tRangeZeroInterval, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible for interval < 1, got %v", err)
	}
	// 9. valid range partitioning
	tRangeValid := baseTable()
	tRangeValid["rangePartitioning"] = map[string]any{"field": "n", "range": map[string]any{"start": "0", "end": "100", "interval": "10"}}
	if obs, err := validateTable(tRangeValid, p); err != nil || obs.Config["rangePartitioning"] == nil {
		t.Fatalf("expected valid rangePartitioning, got %v", err)
	}
}

func TestMetadata_DecimalString_And_Observe(t *testing.T) {
	t.Parallel()

	p := testProfile(t)
	prov := &testProvider{identity: Identity{Principal{"workload", "sub", "1"}, time.Now().Add(time.Hour), true, false}}
	ledg := NewMemoryLedger()

	c, err := NewClient(Config{
		Provider: prov, Transport: rt(func(req *http.Request) (*http.Response, error) { return metadataResponse(req) }),
		Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	// decimalString int64 overflow
	mOver := map[string]any{"big": "9999999999999999999999999999999999999999"}
	if _, err := decimalString(mOver, "big"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for overflowing decimalString, got %v", err)
	}

	// c.Observe with invalid profile
	badP := p
	badP.SourceProject = "invalid project!"
	if _, err := c.Observe(context.Background(), badP); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for c.Observe with bad profile, got %v", err)
	}
}

func TestLoad_Getters_And_Errors(t *testing.T) {
	t.Parallel()

	// LoadOutcomeUnknownError Error and Unwrap
	var nilErr *LoadOutcomeUnknownError
	if nilErr.Error() != ErrLoadOutcomeUnknown.Error() {
		t.Fatalf("expected base error text, got %s", nilErr.Error())
	}
	ref := LoadJobRef{JobID: "job-1", ProjectID: "proj", DatasetID: "ds", TableID: "tbl", Location: "US"}
	nonNilErr := &LoadOutcomeUnknownError{Job: ref}
	if !strings.Contains(nonNilErr.Error(), "job-1") {
		t.Fatalf("expected job-1 in error text, got %s", nonNilErr.Error())
	}
	if !errors.Is(nonNilErr, ErrLoadOutcomeUnknown) {
		t.Fatal("expected Unwrap to match ErrLoadOutcomeUnknown")
	}

	// NewLoadWriter input validation
	client := http.DefaultClient
	if _, err := NewLoadWriter(context.Background(), LoadConfig{ProjectID: "bad project", DatasetID: "ds", Location: "US", HTTPClient: client}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad project, got %v", err)
	}
	if _, err := NewLoadWriter(context.Background(), LoadConfig{ProjectID: "valid-proj", DatasetID: "123bad", Location: "US", HTTPClient: client}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad dataset, got %v", err)
	}
	if _, err := NewLoadWriter(context.Background(), LoadConfig{ProjectID: "valid-proj", DatasetID: "ds", Location: "bad location!", HTTPClient: client}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad location, got %v", err)
	}
	if _, err := NewLoadWriter(context.Background(), LoadConfig{ProjectID: "valid-proj", DatasetID: "ds", Location: "US", HTTPClient: nil}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for nil HTTPClient, got %v", err)
	}

	// NewLoadWriter success and getters
	clientNoTransport := &http.Client{}
	w, err := NewLoadWriter(context.Background(), LoadConfig{ProjectID: "valid-proj", DatasetID: "valid_ds", Location: "US", HTTPClient: clientNoTransport})
	if err != nil {
		t.Fatal(err)
	}
	if w.ProjectID() != "valid-proj" || w.DatasetID() != "valid_ds" || w.Location() != "US" {
		t.Fatalf("getter mismatch: %s %s %s", w.ProjectID(), w.DatasetID(), w.Location())
	}

	// allowedBigQueryOrigin branches
	if allowedBigQueryOrigin(nil) {
		t.Fatal("expected nil URL to not be allowed")
	}
	uHttp, _ := url.Parse("http://bigquery.googleapis.com")
	if allowedBigQueryOrigin(uHttp) {
		t.Fatal("expected http scheme to not be allowed")
	}
	uUser, _ := url.Parse("https://user@bigquery.googleapis.com")
	if allowedBigQueryOrigin(uUser) {
		t.Fatal("expected user to not be allowed")
	}
	uFrag, _ := url.Parse("https://bigquery.googleapis.com#fragment")
	if allowedBigQueryOrigin(uFrag) {
		t.Fatal("expected fragment to not be allowed")
	}
	uBadHost, _ := url.Parse("https://evil.com")
	if allowedBigQueryOrigin(uBadHost) {
		t.Fatal("expected evil.com to not be allowed")
	}
	uBadPort, _ := url.Parse("https://bigquery.googleapis.com:8080")
	if allowedBigQueryOrigin(uBadPort) {
		t.Fatal("expected port 8080 to not be allowed")
	}
	uPort443, _ := url.Parse("https://bigquery.googleapis.com:443")
	if !allowedBigQueryOrigin(uPort443) {
		t.Fatal("expected port 443 to be allowed")
	}
	uWww, _ := url.Parse("https://www.googleapis.com")
	if !allowedBigQueryOrigin(uWww) {
		t.Fatal("expected www.googleapis.com to be allowed")
	}

	// bigQueryOriginTransport.RoundTrip branches
	bot := bigQueryOriginTransport{base: http.DefaultTransport}
	if _, err := bot.RoundTrip(nil); err == nil {
		t.Fatal("expected error for nil req")
	}
	reqNoURL := &http.Request{}
	if _, err := bot.RoundTrip(reqNoURL); err == nil {
		t.Fatal("expected error for nil URL")
	}
	reqBadOrigin, _ := http.NewRequest(http.MethodGet, "https://evil.com", nil)
	if _, err := bot.RoundTrip(reqBadOrigin); err == nil {
		t.Fatal("expected error for evil.com")
	}
}

func TestLoad_EnsureDataset_Branches(t *testing.T) {
	t.Parallel()

	schema := []Field{{Name: "n", Type: "INT64", Mode: "NULLABLE"}}

	// 1. Dataset exists but location mismatch
	w1, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			return loadResponse(req, http.StatusOK, `{"datasetReference":{"projectId":"valid-proj","datasetId":"ds"},"location":"EU"}`), nil
		})},
	})
	if err := w1.EnsureDataset(context.Background()); !errors.Is(err, ErrDatasetLocationMismatch) {
		t.Fatalf("expected ErrDatasetLocationMismatch, got %v", err)
	}

	// 2. Dataset get returns non-404 error
	w2, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			return loadResponse(req, http.StatusInternalServerError, `{"error":{"code":500,"message":"server error"}}`), nil
		})},
	})
	if err := w2.EnsureDataset(context.Background()); err == nil {
		t.Fatal("expected error on 500 get dataset")
	}

	// 3. Dataset not found, create returns non-conflict error
	w3, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodGet {
				return loadResponse(req, http.StatusNotFound, `{"error":{"code":404}}`), nil
			}
			return loadResponse(req, http.StatusBadRequest, `{"error":{"code":400,"message":"bad request"}}`), nil
		})},
	})
	if err := w3.EnsureDataset(context.Background()); err == nil {
		t.Fatal("expected error on bad request create dataset")
	}

	// 4. Dataset not found, create conflict, re-read returns location mismatch
	var w4Calls atomic.Int32
	w4Custom, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			count := w4Calls.Add(1)
			if count == 1 {
				return loadResponse(req, http.StatusNotFound, `{"error":{"code":404}}`), nil
			}
			if req.Method == http.MethodPost {
				return loadResponse(req, http.StatusConflict, `{"error":{"code":409}}`), nil
			}
			return loadResponse(req, http.StatusOK, `{"datasetReference":{"projectId":"valid-proj","datasetId":"ds"},"location":"EU"}`), nil
		})},
	})
	if err := w4Custom.EnsureDataset(context.Background()); !errors.Is(err, ErrDatasetLocationMismatch) {
		t.Fatalf("expected ErrDatasetLocationMismatch on conflict re-read, got %v", err)
	}

	// 5. Dataset not found, create conflict, re-read returns error
	var w5Calls atomic.Int32
	w5, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			count := w5Calls.Add(1)
			if count == 1 {
				return loadResponse(req, http.StatusNotFound, `{"error":{"code":404}}`), nil
			}
			if req.Method == http.MethodPost {
				return loadResponse(req, http.StatusConflict, `{"error":{"code":409}}`), nil
			}
			return loadResponse(req, http.StatusInternalServerError, `{"error":{"code":500}}`), nil
		})},
	})
	if err := w5.EnsureDataset(context.Background()); err == nil {
		t.Fatal("expected error on re-read failure after conflict")
	}

	_ = schema
}

func TestLoad_CreateTable_And_Constraints_Branches(t *testing.T) {
	t.Parallel()

	schema := []Field{{Name: "n", Type: "INT64", Mode: "NULLABLE"}}

	// CreateTable: invalid tableID
	w, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(*http.Request) (*http.Response, error) { return nil, nil })},
	})
	if err := w.CreateTable(context.Background(), "123bad", schema, nil); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad tableID, got %v", err)
	}

	// CreateTable: table exists on initial check
	wExists, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			return loadResponse(req, http.StatusOK, `{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"}}`), nil
		})},
	})
	if err := wExists.CreateTable(context.Background(), "tbl", schema, nil); !errors.Is(err, ErrTableExists) {
		t.Fatalf("expected ErrTableExists, got %v", err)
	}

	// CreateTable: table get non-404 error
	wGetErr, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			return loadResponse(req, http.StatusInternalServerError, `{"error":{"code":500}}`), nil
		})},
	})
	if err := wGetErr.CreateTable(context.Background(), "tbl", schema, nil); err == nil {
		t.Fatal("expected error on 500 get table")
	}

	// CreateTable: insert conflict (409)
	var wInsConfCalls atomic.Int32
	wInsConf, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			count := wInsConfCalls.Add(1)
			if count == 1 {
				return loadResponse(req, http.StatusNotFound, `{"error":{"code":404}}`), nil
			}
			return loadResponse(req, http.StatusConflict, `{"error":{"code":409}}`), nil
		})},
	})
	if err := wInsConf.CreateTable(context.Background(), "tbl", schema, nil); !errors.Is(err, ErrTableExists) {
		t.Fatalf("expected ErrTableExists on 409 insert, got %v", err)
	}

	// CreateTable: insert generic error
	var wInsErrCalls atomic.Int32
	wInsErr, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			count := wInsErrCalls.Add(1)
			if count == 1 {
				return loadResponse(req, http.StatusNotFound, `{"error":{"code":404}}`), nil
			}
			return loadResponse(req, http.StatusInternalServerError, `{"error":{"code":500}}`), nil
		})},
	})
	if err := wInsErr.CreateTable(context.Background(), "tbl", schema, nil); err == nil {
		t.Fatal("expected error on insert failure")
	}

	// CreateTable: verify get error
	var wVerErrCalls atomic.Int32
	wVerErr, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			count := wVerErrCalls.Add(1)
			if count == 1 {
				return loadResponse(req, http.StatusNotFound, `{"error":{"code":404}}`), nil
			}
			if req.Method == http.MethodPost {
				return loadResponse(req, http.StatusOK, `{}`), nil
			}
			return loadResponse(req, http.StatusInternalServerError, `{"error":{"code":500}}`), nil
		})},
	})
	if err := wVerErr.CreateTable(context.Background(), "tbl", schema, nil); err == nil {
		t.Fatal("expected error on verify get failure")
	}

	// CreateTable: verify ownership mismatch (missing owner label or empty etag)
	var wOwnErrCalls atomic.Int32
	wOwnErr, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			count := wOwnErrCalls.Add(1)
			if count == 1 {
				return loadResponse(req, http.StatusNotFound, `{"error":{"code":404}}`), nil
			}
			if req.Method == http.MethodPost {
				return loadResponse(req, http.StatusOK, `{}`), nil
			}
			// Return table with empty etag and no owner tag
			return loadResponse(req, http.StatusOK, `{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"etag":""}`), nil
		})},
	})
	if err := wOwnErr.CreateTable(context.Background(), "tbl", schema, nil); !errors.Is(err, ErrTableOwnershipChanged) {
		t.Fatalf("expected ErrTableOwnershipChanged, got %v", err)
	}

	// SetConstraints: invalid tableID or nil constraints
	constraints := &bq.TableConstraints{}
	if err := w.SetConstraints(context.Background(), "123bad", constraints); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input, got %v", err)
	}
	if err := w.SetConstraints(context.Background(), "tbl", nil); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for nil constraints, got %v", err)
	}

	// SetConstraints: table not owned
	if err := w.SetConstraints(context.Background(), "unowned", constraints); !errors.Is(err, ErrTableNotOwned) {
		t.Fatalf("expected ErrTableNotOwned, got %v", err)
	}

	// SetConstraints: table get error
	wOwned := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
		service: wGetErr.service,
	}
	if err := wOwned.SetConstraints(context.Background(), "tbl", constraints); err == nil {
		t.Fatal("expected error on get table in SetConstraints")
	}

	// SetConstraints: matchesOwnedTable fails
	wMatchFail := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
	}
	wMatchFail.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		return loadResponse(req, http.StatusOK, `{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"etag":"diff-etag"}`), nil
	})}))
	if err := wMatchFail.SetConstraints(context.Background(), "tbl", constraints); !errors.Is(err, ErrTableOwnershipChanged) {
		t.Fatalf("expected ErrTableOwnershipChanged on etag mismatch, got %v", err)
	}

	// SetConstraints: patch returns 412 PreconditionFailed
	wPatch412 := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
	}
	wPatch412.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet {
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"labels":{"%s":"tag"},"etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel)), nil
		}
		return loadResponse(req, http.StatusPreconditionFailed, `{"error":{"code":412}}`), nil
	})}))
	if err := wPatch412.SetConstraints(context.Background(), "tbl", constraints); !errors.Is(err, ErrTableOwnershipChanged) {
		t.Fatalf("expected ErrTableOwnershipChanged on 412 patch, got %v", err)
	}

	// SetConstraints: patch generic error
	wPatchErr := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
	}
	wPatchErr.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet {
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"labels":{"%s":"tag"},"etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel)), nil
		}
		return loadResponse(req, http.StatusInternalServerError, `{"error":{"code":500}}`), nil
	})}))
	if err := wPatchErr.SetConstraints(context.Background(), "tbl", constraints); err == nil {
		t.Fatal("expected error on generic patch error")
	}

	// SetConstraints: verify get error
	var patchCount atomic.Int32
	wVerPatchErr := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
	}
	wVerPatchErr.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		c := patchCount.Add(1)
		if c == 1 {
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"labels":{"%s":"tag"},"etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel)), nil
		}
		if req.Method == http.MethodPatch {
			return loadResponse(req, http.StatusOK, `{}`), nil
		}
		return loadResponse(req, http.StatusInternalServerError, `{"error":{"code":500}}`), nil
	})}))
	if err := wVerPatchErr.SetConstraints(context.Background(), "tbl", constraints); err == nil {
		t.Fatal("expected error on verify get after patch")
	}

	// SetConstraints: verify ownsTableResource fails
	var patchOwnCount atomic.Int32
	wVerPatchOwn := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
	}
	wVerPatchOwn.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		c := patchOwnCount.Add(1)
		if c == 1 {
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"labels":{"%s":"tag"},"etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel)), nil
		}
		if req.Method == http.MethodPatch {
			return loadResponse(req, http.StatusOK, `{}`), nil
		}
		// Missing owner tag in updated table
		return loadResponse(req, http.StatusOK, `{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"etag":"etag-2"}`), nil
	})}))
	if err := wVerPatchOwn.SetConstraints(context.Background(), "tbl", constraints); !errors.Is(err, ErrTableOwnershipChanged) {
		t.Fatalf("expected ErrTableOwnershipChanged, got %v", err)
	}

	// SetConstraints: success
	var patchSuccCount atomic.Int32
	wSuccPatch := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
	}
	wSuccPatch.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		c := patchSuccCount.Add(1)
		if c == 1 {
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"labels":{"%s":"tag"},"etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel)), nil
		}
		if req.Method == http.MethodPatch {
			return loadResponse(req, http.StatusOK, `{}`), nil
		}
		return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"labels":{"%s":"tag"},"etag":"etag-2","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel)), nil
	})}))
	if err := wSuccPatch.SetConstraints(context.Background(), "tbl", constraints); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if wSuccPatch.created["tbl"].etag != "etag-2" {
		t.Fatalf("expected updated etag etag-2, got %s", wSuccPatch.created["tbl"].etag)
	}
}

func TestLoad_CheckTablesAbsent_Branches(t *testing.T) {
	t.Parallel()

	// Invalid input: bad identifier
	w, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(*http.Request) (*http.Response, error) { return nil, nil })},
	})
	if err := w.CheckTablesAbsent(context.Background(), []string{"123bad"}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad table ID, got %v", err)
	}

	// Table exists (HTTP 200)
	wExists, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			return loadResponse(req, http.StatusOK, `{}`), nil
		})},
	})
	if err := wExists.CheckTablesAbsent(context.Background(), []string{"tbl"}); !errors.Is(err, ErrTableExists) {
		t.Fatalf("expected ErrTableExists, got %v", err)
	}

	// Table check returns 500 error
	w500, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			return loadResponse(req, http.StatusInternalServerError, `{"error":{"code":500}}`), nil
		})},
	})
	if err := w500.CheckTablesAbsent(context.Background(), []string{"tbl"}); err == nil {
		t.Fatal("expected error on 500 in CheckTablesAbsent")
	}

	// Success (HTTP 404 for all)
	w404, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
			return loadResponse(req, http.StatusNotFound, `{"error":{"code":404}}`), nil
		})},
	})
	if err := w404.CheckTablesAbsent(context.Background(), []string{"tbl1", "tbl2"}); err != nil {
		t.Fatalf("expected success on 404, got %v", err)
	}

	// Invalid input: duplicate table ID (case insensitive)
	if err := w404.CheckTablesAbsent(context.Background(), []string{"tbl", "TBL"}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for duplicate table ID, got %v", err)
	}
}

func TestLoad_LoadTable_And_Recovery_Branches(t *testing.T) {
	t.Parallel()

	schema := []Field{{Name: "n", Type: "INT64", Mode: "NULLABLE"}}
	diffSchema := []Field{{Name: "other", Type: "STRING", Mode: "NULLABLE"}}

	// Invalid input: bad tableID, bad schema, or nil ndjson
	w, _ := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID: "valid-proj", DatasetID: "ds", Location: "US",
		HTTPClient: &http.Client{Transport: loadRoundTripper(func(*http.Request) (*http.Response, error) { return nil, nil })},
	})
	if _, err := w.LoadTable(context.Background(), "123bad", schema, strings.NewReader("")); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad table ID, got %v", err)
	}
	if _, err := w.LoadTable(context.Background(), "tbl", nil, strings.NewReader("")); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for nil schema, got %v", err)
	}
	if _, err := w.LoadTable(context.Background(), "tbl", schema, nil); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for nil reader, got %v", err)
	}

	// Table not owned
	if _, err := w.LoadTable(context.Background(), "unowned", schema, strings.NewReader("")); !errors.Is(err, ErrTableNotOwned) {
		t.Fatalf("expected ErrTableNotOwned, got %v", err)
	}

	// Table busy / loadStarted
	wBusy := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema, busy: true}},
	}
	if _, err := wBusy.LoadTable(context.Background(), "tbl", schema, strings.NewReader("")); !errors.Is(err, ErrTableBusy) {
		t.Fatalf("expected ErrTableBusy, got %v", err)
	}

	// Schema does not match owned table schema
	wDiffSchema := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
	}
	if _, err := wDiffSchema.LoadTable(context.Background(), "tbl", diffSchema, strings.NewReader("")); !errors.Is(err, ErrTableOwnershipChanged) {
		t.Fatalf("expected ErrTableOwnershipChanged on schema mismatch, got %v", err)
	}

	// Get destination table returns error
	wGetErr := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
	}
	wGetErr.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		return loadResponse(req, http.StatusInternalServerError, `{"error":{"code":500}}`), nil
	})}))
	if _, err := wGetErr.LoadTable(context.Background(), "tbl", schema, strings.NewReader("")); err == nil {
		t.Fatal("expected error on get table failure in LoadTable")
	}

	// matchesOwnedTable false (etag mismatch)
	wMatchFalse := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
	}
	wMatchFalse.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		return loadResponse(req, http.StatusOK, `{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"etag":"other-etag"}`), nil
	})}))
	if _, err := wMatchFalse.LoadTable(context.Background(), "tbl", schema, strings.NewReader("")); !errors.Is(err, ErrTableOwnershipChanged) {
		t.Fatalf("expected ErrTableOwnershipChanged, got %v", err)
	}
	// Jobs.Insert returns mismatch jobReference
	wJobRefMismatch := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
	}
	wJobRefMismatch.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet {
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"labels":{"%s":"tag"},"etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel)), nil
		}
		// Return wrong location in jobReference
		return loadResponse(req, http.StatusOK, `{"jobReference":{"projectId":"valid-proj","jobId":"j1","location":"WRONG"}}`), nil
	})}))
	if _, err := wJobRefMismatch.LoadTable(context.Background(), "tbl", schema, strings.NewReader("")); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown on jobReference mismatch, got %v", err)
	}

	// ReconcileLoad branches:
	ref := LoadJobRef{JobID: "j1", ProjectID: "valid-proj", DatasetID: "ds", TableID: "tbl", Location: "US"}
	// 1. Canceled context
	cancCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := w.reconcileLoad(cancCtx, ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown on canceled ctx, got %v", err)
	}

	// 2. Jobs.Get error
	wReconcileGetErr := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US",
		service: wGetErr.service,
	}
	if _, err := wReconcileGetErr.reconcileLoad(context.Background(), ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown on Jobs.Get error, got %v", err)
	}

	// 3. matchesLoadReference false
	wReconcileMatchFalse := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US",
	}
	wReconcileMatchFalse.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		return loadResponse(req, http.StatusOK, `{"jobReference":{"projectId":"valid-proj","jobId":"different","location":"US"}}`), nil
	})}))
	if _, err := wReconcileMatchFalse.reconcileLoad(context.Background(), ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown on matchesLoadReference false, got %v", err)
	}

	// 4. State DONE with load_job_failed
	wReconcileDoneFail := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US",
	}
	wReconcileDoneFail.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		return loadResponse(req, http.StatusOK, `{"jobReference":{"projectId":"valid-proj","jobId":"j1","location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"}}},"status":{"state":"DONE","errorResult":{"message":"failed"}}}`), nil
	})}))
	if _, err := wReconcileDoneFail.reconcileLoad(context.Background(), ref); errorCode(err) != "load_job_failed" {
		t.Fatalf("expected load_job_failed, got %v", err)
	}

	// 5. State PENDING in reconcileLoad -> polls and finishes
	var reconcilePollCalls atomic.Int32
	wReconcilePoll := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US",
	}
	wReconcilePoll.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		c := reconcilePollCalls.Add(1)
		if c == 1 {
			return loadResponse(req, http.StatusOK, `{"jobReference":{"projectId":"valid-proj","jobId":"j1","location":"US"},"status":{"state":"PENDING"}}`), nil
		}
		return loadResponse(req, http.StatusOK, `{"jobReference":{"projectId":"valid-proj","jobId":"j1","location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"}}},"status":{"state":"DONE"},"statistics":{"load":{"outputRows":"10"}}}`), nil
	})}))
	receipt, err := wReconcilePoll.reconcileLoad(context.Background(), ref)
	if err != nil || receipt.Rows != 10 {
		t.Fatalf("expected success in reconcileLoad polling, got %+v, err: %v", receipt, err)
	}

	// 6. Other state in reconcileLoad (unknown state)
	wReconcileUnknownState := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US",
	}
	wReconcileUnknownState.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		return loadResponse(req, http.StatusOK, `{"jobReference":{"projectId":"valid-proj","jobId":"j1","location":"US"},"status":{"state":"UNKNOWN_STATE"}}`), nil
	})}))
	if _, err := wReconcileUnknownState.reconcileLoad(context.Background(), ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown on unknown state, got %v", err)
	}

	// RecoverLoad branches:
	// 1. Invalid job ref
	if _, err := w.RecoverLoad(context.Background(), LoadJobRef{}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for invalid ref, got %v", err)
	}
	// 2. Canceled context
	if _, err := w.RecoverLoad(cancCtx, ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown for canceled ctx in RecoverLoad, got %v", err)
	}
	// 3. Jobs.Get error
	if _, err := wReconcileGetErr.RecoverLoad(context.Background(), ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown for Get error in RecoverLoad, got %v", err)
	}
	// 4. matchesLoadReference false
	if _, err := wReconcileMatchFalse.RecoverLoad(context.Background(), ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown for mismatch in RecoverLoad, got %v", err)
	}
	// 5. State DONE with success
	wRecoverDone := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US",
	}
	wRecoverDone.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		return loadResponse(req, http.StatusOK, `{"jobReference":{"projectId":"valid-proj","jobId":"j1","location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"}}},"status":{"state":"DONE"},"statistics":{"load":{"outputRows":"5"}}}`), nil
	})}))
	if r, err := wRecoverDone.RecoverLoad(context.Background(), ref); err != nil || r.Rows != 5 {
		t.Fatalf("expected 5 rows in RecoverLoad, got %+v, err: %v", r, err)
	}

	// waitForLoad branches:
	// 1. Context canceled
	if _, err := w.waitForLoad(cancCtx, ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown for canceled ctx in waitForLoad, got %v", err)
	}
	// 2. Jobs.Get error
	if _, err := wReconcileGetErr.waitForLoad(context.Background(), ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown for Get error in waitForLoad, got %v", err)
	}
	// 3. matchesLoadReference false
	if _, err := wReconcileMatchFalse.waitForLoad(context.Background(), ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown for mismatch in waitForLoad, got %v", err)
	}

	// receiptFromJob branches:
	// nil job
	if _, err := w.receiptFromJob(nil, ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown for nil job, got %v", err)
	}
	// missing destination table
	jobNoDest := &bq.Job{
		JobReference:  &bq.JobReference{ProjectId: "valid-proj", JobId: "j1", Location: "US"},
		Status:        &bq.JobStatus{State: "DONE"},
		Configuration: &bq.JobConfiguration{Load: &bq.JobConfigurationLoad{}},
	}
	if _, err := w.receiptFromJob(jobNoDest, ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown for missing destination, got %v", err)
	}
	// destination mismatch
	jobDiffDest := &bq.Job{
		JobReference: &bq.JobReference{ProjectId: "valid-proj", JobId: "j1", Location: "US"},
		Status:       &bq.JobStatus{State: "DONE"},
		Configuration: &bq.JobConfiguration{Load: &bq.JobConfigurationLoad{
			DestinationTable: &bq.TableReference{ProjectId: "other", DatasetId: "ds", TableId: "tbl"},
		}},
	}
	if _, err := w.receiptFromJob(jobDiffDest, ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown for destination mismatch, got %v", err)
	}
	// state != DONE
	jobNotDone := &bq.Job{
		JobReference: &bq.JobReference{ProjectId: "valid-proj", JobId: "j1", Location: "US"},
		Status:       &bq.JobStatus{State: "PENDING"},
		Configuration: &bq.JobConfiguration{Load: &bq.JobConfigurationLoad{
			DestinationTable: &bq.TableReference{ProjectId: "valid-proj", DatasetId: "ds", TableId: "tbl"},
		}},
	}
	if _, err := w.receiptFromJob(jobNotDone, ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown for state not DONE, got %v", err)
	}
	// stats nil
	jobNoStats := &bq.Job{
		JobReference: &bq.JobReference{ProjectId: "valid-proj", JobId: "j1", Location: "US"},
		Status:       &bq.JobStatus{State: "DONE"},
		Configuration: &bq.JobConfiguration{Load: &bq.JobConfigurationLoad{
			DestinationTable: &bq.TableReference{ProjectId: "valid-proj", DatasetId: "ds", TableId: "tbl"},
		}},
	}
	if _, err := w.receiptFromJob(jobNoStats, ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("expected ErrLoadOutcomeUnknown for missing stats, got %v", err)
	}

	// finishTableOperation: table instance mismatch
	tabObj1 := &createdLoadTable{etag: "e1"}
	tabObj2 := &createdLoadTable{etag: "e2"}
	wForget := &LoadWriter{created: map[string]*createdLoadTable{"tbl": tabObj1}}
	wForget.finishTableOperation("tbl", tabObj2, "", false)
	// finishTableOperation: with forget=true
	wForget.finishTableOperation("tbl", tabObj1, "", true)
	if _, exists := wForget.created["tbl"]; exists {
		t.Fatal("expected tbl to be deleted when forget is true")
	}

	// safeLoadError branches:
	// with googleapi.Error
	gErr := &googleapi.Error{Code: 403, Message: "forbidden"}
	errFmt := safeLoadError("test op", gErr)
	if !strings.Contains(errFmt.Error(), "HTTP 403") {
		t.Fatalf("expected HTTP 403 in error string, got %v", errFmt)
	}
	// with generic error
	genericErr := errors.New("generic")
	errFmtGeneric := safeLoadError("test op", genericErr)
	if strings.Contains(errFmtGeneric.Error(), "HTTP") {
		t.Fatalf("did not expect HTTP in generic error, got %v", errFmtGeneric)
	}

	// apiSchema & schemaEqualAPI branches:
	schemaWithPrecScale := []Field{
		{Name: "num", Type: "NUMERIC", Mode: "", Description: "desc", Precision: "10", Scale: "2"},
	}
	apiF := apiSchema(schemaWithPrecScale)
	if len(apiF) != 1 || apiF[0].Precision != 10 || apiF[0].Scale != 2 || apiF[0].Mode != "NULLABLE" {
		t.Fatalf("apiSchema unexpected output: %+v", apiF[0])
	}
	if schemaEqualAPI(schemaWithPrecScale, nil) {
		t.Fatal("expected false for nil schema")
	}
	if schemaEqualAPI(schemaWithPrecScale, &bq.TableSchema{Fields: []*bq.TableFieldSchema{}}) {
		t.Fatal("expected false for length mismatch")
	}
	if schemaEqualAPI(schemaWithPrecScale, &bq.TableSchema{Fields: []*bq.TableFieldSchema{{Name: "other"}}}) {
		t.Fatal("expected false for name mismatch")
	}
	if schemaEqualAPI(schemaWithPrecScale, &bq.TableSchema{Fields: []*bq.TableFieldSchema{{Name: "num", Type: "STRING"}}}) {
		t.Fatal("expected false for type mismatch")
	}
	if schemaEqualAPI(schemaWithPrecScale, &bq.TableSchema{Fields: []*bq.TableFieldSchema{{Name: "num", Type: "NUMERIC", Mode: "REQUIRED"}}}) {
		t.Fatal("expected false for mode mismatch")
	}
	if schemaEqualAPI(schemaWithPrecScale, &bq.TableSchema{Fields: []*bq.TableFieldSchema{{Name: "num", Type: "NUMERIC", Mode: "NULLABLE", Description: "diff"}}}) {
		t.Fatal("expected false for desc mismatch")
	}
	if schemaEqualAPI(schemaWithPrecScale, &bq.TableSchema{Fields: []*bq.TableFieldSchema{{Name: "num", Type: "NUMERIC", Mode: "NULLABLE", Description: "desc", Precision: 5}}}) {
		t.Fatal("expected false for precision mismatch")
	}
	if schemaEqualAPI(schemaWithPrecScale, &bq.TableSchema{Fields: []*bq.TableFieldSchema{{Name: "num", Type: "NUMERIC", Mode: "NULLABLE", Description: "desc", Precision: 10, Scale: 1}}}) {
		t.Fatal("expected false for scale mismatch")
	}

	// effectiveMode:
	if effectiveMode("") != "NULLABLE" {
		t.Fatal("expected NULLABLE for empty mode")
	}
	if effectiveMode("REQUIRED") != "REQUIRED" {
		t.Fatal("expected REQUIRED for REQUIRED mode")
	}

	// validateLoadSchema:
	// name > 300
	longNameField := Field{Name: strings.Repeat("a", 301), Type: "STRING", Mode: "NULLABLE"}
	if err := validateLoadSchema([]Field{longNameField}); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type for long name, got %v", err)
	}
	// REPEATED mode
	repeatedField := Field{Name: "arr", Type: "STRING", Mode: "REPEATED"}
	if err := validateLoadSchema([]Field{repeatedField}); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type for repeated field, got %v", err)
	}
	// len(Fields) > 0
	subField := Field{Name: "rec", Type: "RECORD", Mode: "NULLABLE", Fields: []Field{{Name: "s", Type: "STRING", Mode: "NULLABLE"}}}
	if err := validateLoadSchema([]Field{subField}); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type for subfield in load, got %v", err)
	}
	// unsupported type (RECORD is not in switch)
	recField := Field{Name: "rec", Type: "RECORD", Mode: "NULLABLE"}
	if err := validateLoadSchema([]Field{recField}); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type for RECORD, got %v", err)
	}
}

// Not parallel: temporarily replaces schemaEqualAPIFn
func TestLoad_Isolated_Vars(t *testing.T) {
	orig := schemaEqualAPIFn
	defer func() { schemaEqualAPIFn = orig }()

	schema := []Field{{Name: "n", Type: "INT64", Mode: "NULLABLE"}}
	wDestSchemaChanged := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
	}
	wDestSchemaChanged.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"labels":{"%s":"tag"},"etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel)), nil
	})}))
	schemaEqualAPIFn = func(want []Field, got *bq.TableSchema) bool {
		return false
	}
	if _, err := wDestSchemaChanged.LoadTable(context.Background(), "tbl", schema, strings.NewReader("")); errorCode(err) != "destination_schema_changed" {
		t.Fatalf("expected destination_schema_changed, got %v", err)
	}

	// Test loadCheckRedirectFn
	if err := loadCheckRedirectFn(nil, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("expected ErrUseLastResponse, got %v", err)
	}

	// Test bqNewServiceFn error
	origServiceFn := bqNewServiceFn
	defer func() { bqNewServiceFn = origServiceFn }()
	bqNewServiceFn = func(ctx context.Context, opts ...option.ClientOption) (*bq.Service, error) {
		return nil, errors.New("boom")
	}
	if _, err := NewLoadWriter(context.Background(), LoadConfig{
		ProjectID:  "valid-project",
		DatasetID:  "valid_dataset",
		Location:   "US",
		HTTPClient: &http.Client{},
	}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on bqNewService error, got %v", err)
	}
	bqNewServiceFn = origServiceFn

	// Test waitForLoadAfter
	origAfter := waitForLoadAfter
	defer func() { waitForLoadAfter = origAfter }()
	calledAfter := false
	waitForLoadAfter = func(d time.Duration) <-chan time.Time {
		calledAfter = true
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
	wWait := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
	}
	step := 0
	wWait.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		step++
		if step == 1 {
			// First poll: RUNNING
			return loadResponse(req, http.StatusOK, `{"jobReference":{"projectId":"valid-proj","jobId":"job1","location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"}}},"status":{"state":"RUNNING"}}`), nil
		}
		// Second poll: DONE
		return loadResponse(req, http.StatusOK, `{"jobReference":{"projectId":"valid-proj","jobId":"job1","location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"}}},"status":{"state":"DONE"},"statistics":{"load":{"outputRows":"10"}}}`), nil
	})}))
	ref := LoadJobRef{
		JobID:     "job1",
		ProjectID: "valid-proj",
		DatasetID: "ds",
		TableID:   "tbl",
		Location:  "US",
	}
	rc, err := wWait.waitForLoad(context.Background(), ref)
	if err != nil || rc.Rows != 10 || !calledAfter {
		t.Fatalf("waitForLoad failed: rc=%v, err=%v, calledAfter=%v", rc, err, calledAfter)
	}

	// Test waitForLoad select ctx.Done()
	waitForLoadAfter = func(d time.Duration) <-chan time.Time {
		return make(chan time.Time)
	}
	ctxSelect, cancelSelect := context.WithCancel(context.Background())
	wSelect := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
	}
	wSelect.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		cancelSelect()
		return loadResponse(req, http.StatusOK, `{"jobReference":{"projectId":"valid-proj","jobId":"job1","location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"}}},"status":{"state":"RUNNING"}}`), nil
	})}))
	if _, err := wSelect.waitForLoad(ctxSelect, ref); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("expected unknownLoad error on select ctx.Done, got %v", err)
	}
	waitForLoadAfter = origAfter
}

func TestMetadata_ValidateTable_RangePartitioningOverflow(t *testing.T) {
	t.Parallel()
	p := SourceProfile{
		SourceProject: "proj",
		DatasetID:     "ds",
		TableID:       "tbl",
		Location:      "US",
		Schema:        []Field{{Name: "n", Type: "INTEGER", Mode: "NULLABLE"}},
	}
	m := map[string]any{
		"tableReference": map[string]any{"projectId": p.SourceProject, "datasetId": p.DatasetID, "tableId": p.TableID},
		"location":       p.Location,
		"type":           "TABLE",
		"schema": map[string]any{
			"fields": []any{
				map[string]any{"name": "n", "type": p.Schema[0].Type, "mode": "NULLABLE"},
			},
		},
		"etag":             "etag-123",
		"lastModifiedTime": "1700000000000",
		"rangePartitioning": map[string]any{
			"field": "n",
			"range": map[string]any{
				"start":    "99999999999999999999999999999999999999",
				"end":      "100",
				"interval": "1",
			},
		},
	}
	if _, err := validateTable(m, p); errorCode(err) != "source_ineligible" {
		t.Fatalf("expected source_ineligible on range start overflow, got %v", err)
	}
}

func TestLoad_ValidateSchema_UnsupportedType(t *testing.T) {
	t.Parallel()
	schema := []Field{{Name: "geo", Type: "GEOGRAPHY", Mode: "NULLABLE"}}
	if err := validateLoadSchema(schema); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type for GEOGRAPHY, got %v", err)
	}
}

func TestLoad_EnsureDataset_SuccessPaths(t *testing.T) {
	t.Parallel()
	// 1. Dataset already exists with matching location
	w1 := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
	}
	w1.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		return loadResponse(req, http.StatusOK, `{"datasetReference":{"projectId":"valid-proj","datasetId":"ds"},"location":"US"}`), nil
	})}))
	if err := w1.EnsureDataset(context.Background()); err != nil {
		t.Fatalf("EnsureDataset failed on existing dataset: %v", err)
	}

	// 2. Conflict on insert, reconcile succeeds with matching location
	w2 := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
	}
	call := 0
	w2.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		call++
		if call == 1 {
			return loadResponse(req, http.StatusNotFound, `{"error":{"code":404,"message":"Not Found"}}`), nil
		}
		if call == 2 {
			return loadResponse(req, http.StatusConflict, `{"error":{"code":409,"message":"Already Exists"}}`), nil
		}
		return loadResponse(req, http.StatusOK, `{"datasetReference":{"projectId":"valid-proj","datasetId":"ds"},"location":"US"}`), nil
	})}))
	if err := w2.EnsureDataset(context.Background()); err != nil {
		t.Fatalf("EnsureDataset failed on conflict reconcile: %v", err)
	}
}

func TestLoad_CreateTable_NilCreatedMap_Success(t *testing.T) {
	t.Parallel()
	w := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: nil,
	}
	schema := []Field{{Name: "n", Type: "INT64", Mode: "NULLABLE"}}
	call := 0
	w.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		call++
		if call == 1 {
			// First Get: 404
			return loadResponse(req, http.StatusNotFound, `{"error":{"code":404,"message":"Not Found"}}`), nil
		}
		if req.Method == http.MethodPost {
			return loadResponse(req, http.StatusOK, `{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"}}`), nil
		}
		return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"labels":{"%s":"tag"},"etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel)), nil
	})}))
	if err := w.CreateTable(context.Background(), "tbl", schema, nil); err != nil {
		t.Fatalf("CreateTable failed with nil created map: %v", err)
	}
	if w.created == nil || w.created["tbl"] == nil || w.created["tbl"].etag != "etag-1" {
		t.Fatalf("CreateTable did not initialize created map properly: %v", w.created)
	}
}

func TestLoad_EnsureDataset_InsertSuccess(t *testing.T) {
	t.Parallel()
	w := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
	}
	call := 0
	w.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		call++
		if call == 1 {
			// First Get: 404
			return loadResponse(req, http.StatusNotFound, `{"error":{"code":404,"message":"Not Found"}}`), nil
		}
		// Insert: 200 OK
		return loadResponse(req, http.StatusOK, `{"datasetReference":{"projectId":"valid-proj","datasetId":"ds"},"location":"US"}`), nil
	})}))
	if err := w.EnsureDataset(context.Background()); err != nil {
		t.Fatalf("EnsureDataset failed on insert success: %v", err)
	}
}

func TestLoad_LoadTable_InsertReconcile_And_InsertWait(t *testing.T) {
	t.Parallel()
	schema := []Field{{Name: "n", Type: "INT64", Mode: "NULLABLE"}}

	// Case 1: Jobs.Insert fails -> reconcileLoad succeeds
	wReconcile := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
	}
	wReconcile.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/tables/") {
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"labels":{"%s":"tag"},"etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel)), nil
		}
		if req.Method == http.MethodPost && strings.Contains(req.URL.Path, "/jobs") {
			return loadResponse(req, http.StatusInternalServerError, `{"error":{"code":500,"message":"internal error"}}`), nil
		}
		// Jobs.Get for reconcile
		return loadResponse(req, http.StatusOK, `{"jobReference":{"projectId":"valid-proj","jobId":"`+strings.Split(req.URL.Path, "/jobs/")[1]+`","location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"}}},"status":{"state":"DONE"},"statistics":{"load":{"outputRows":"5"}}}`), nil
	})}))
	rc1, err := wReconcile.LoadTable(context.Background(), "tbl", schema, strings.NewReader(""))
	if err != nil || rc1.Rows != 5 {
		t.Fatalf("expected successful reconcileLoad, got rc=%v, err=%v", rc1, err)
	}

	// Case 2: Jobs.Insert succeeds -> waitForLoad succeeds
	wWait := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
		created: map[string]*createdLoadTable{"tbl": {etag: "etag-1", schema: schema}},
	}
	wWait.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/tables/") {
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"},"labels":{"%s":"tag"},"etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel)), nil
		}
		if req.Method == http.MethodPost && strings.Contains(req.URL.Path, "/jobs") {
			body, _ := io.ReadAll(req.Body)
			sub := regexp.MustCompile(`"jobId":"([^"]+)"`).FindStringSubmatch(string(body))
			jobID := "mock-job"
			if len(sub) > 1 {
				jobID = sub[1]
			}
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"jobReference":{"projectId":"valid-proj","jobId":"%s","location":"US"}}`, jobID)), nil
		}
		// Jobs.Get for waitForLoad
		parts := strings.Split(req.URL.Path, "/jobs/")
		jobID := "mock-job"
		if len(parts) > 1 {
			jobID = parts[1]
		}
		return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"jobReference":{"projectId":"valid-proj","jobId":"%s","location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"}}},"status":{"state":"DONE"},"statistics":{"load":{"outputRows":"15"}}}`, jobID)), nil
	})}))
	rc2, err := wWait.LoadTable(context.Background(), "tbl", schema, strings.NewReader(""))
	if err != nil || rc2.Rows != 15 {
		t.Fatalf("expected successful waitForLoad from LoadTable, got rc=%v, err=%v", rc2, err)
	}
}

func TestLoad_RecoverLoad_WaitForLoad(t *testing.T) {
	t.Parallel()
	w := &LoadWriter{
		projectID: "valid-proj", datasetID: "ds", location: "US", ownerTag: "tag",
	}
	call := 0
	w.service, _ = bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		call++
		state := "RUNNING"
		if call > 1 {
			state = "DONE"
		}
		return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"jobReference":{"projectId":"valid-proj","jobId":"job-rec","location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"valid-proj","datasetId":"ds","tableId":"tbl"}}},"status":{"state":"%s"},"statistics":{"load":{"outputRows":"42"}}}`, state)), nil
	})}))
	ref := LoadJobRef{
		JobID:     "job-rec",
		ProjectID: "valid-proj",
		DatasetID: "ds",
		TableID:   "tbl",
		Location:  "US",
	}
	rc, err := w.RecoverLoad(context.Background(), ref)
	if err != nil || rc.Rows != 42 {
		t.Fatalf("RecoverLoad waitForLoad failed: rc=%v, err=%v", rc, err)
	}
}

func TestMetadata_SchemaDigest(t *testing.T) {
	t.Parallel()
	fs := []Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}}
	d := schemaDigest(fs)
	if d == "" {
		t.Fatal("expected non-empty schemaDigest")
	}
}
