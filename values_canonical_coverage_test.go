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
)

func TestValues_NormalizeScalar_AllBranches(t *testing.T) {
	t.Parallel()

	// Unsupported types / repeated mode
	if _, err := NormalizeScalar(Field{Type: "RECORD"}, "foo"); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type for RECORD, got %v", err)
	}
	if _, err := NormalizeScalar(Field{Type: "STRING", Mode: "REPEATED"}, "foo"); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type for REPEATED, got %v", err)
	}
	if _, err := NormalizeScalar(Field{Type: "STRING", Mode: "INVALID_MODE"}, "unsupported_type"); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type for INVALID_MODE, got %v", err)
	}

	// Null value handling
	if _, err := NormalizeScalar(Field{Type: "STRING", Mode: "REQUIRED"}, nil); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for nil in REQUIRED field, got %v", err)
	}
	cell, err := NormalizeScalar(Field{Type: "STRING", Mode: "NULLABLE"}, nil)
	if err != nil || cell.Value != nil || cell.Type != "STRING" {
		t.Fatalf("expected null cell, got %+v, err: %v", cell, err)
	}

	// Non-string value
	if _, err := NormalizeScalar(Field{Type: "STRING"}, 123); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-string, got %v", err)
	}

	// Non-utf8 string
	if _, err := NormalizeScalar(Field{Type: "STRING"}, "\xff\xfe"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for invalid utf8, got %v", err)
	}

	// Oversized string (> 1MB)
	oversized := strings.Repeat("a", 1024*1024+1)
	if _, err := NormalizeScalar(Field{Type: "STRING"}, oversized); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit for oversized value, got %v", err)
	}

	// Type aliases: INTEGER -> INT64, FLOAT -> FLOAT64, BOOLEAN -> BOOL
	for _, alias := range []struct {
		in   string
		val  string
		want string
	}{
		{"INTEGER", "42", "INT64"},
		{"FLOAT", "3.14", "FLOAT64"},
		{"BOOLEAN", "true", "BOOL"},
	} {
		c, err := NormalizeScalar(Field{Type: alias.in}, alias.val)
		if err != nil || c.Type != alias.want {
			t.Fatalf("expected alias %s -> %s, got %+v, err: %v", alias.in, alias.want, c, err)
		}
	}

	// INT64 / TIMESTAMP
	if _, err := NormalizeScalar(Field{Type: "INT64"}, "not-number"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value, got %v", err)
	}
	if _, err := NormalizeScalar(Field{Type: "INT64"}, "99999999999999999999999999999999"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for overflow, got %v", err)
	}
	// TIMESTAMP boundary check
	if _, err := NormalizeScalar(Field{Type: "TIMESTAMP"}, "-62135596800000001"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for underflow timestamp, got %v", err)
	}
	if _, err := NormalizeScalar(Field{Type: "TIMESTAMP"}, "253402300800000000"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for overflow timestamp, got %v", err)
	}
	if c, err := NormalizeScalar(Field{Type: "TIMESTAMP"}, "1609459200000000"); err != nil || c.Value != "1609459200000000" {
		t.Fatalf("expected valid timestamp, got %+v, err: %v", c, err)
	}

	// NUMERIC / BIGNUMERIC
	if _, err := NormalizeScalar(Field{Type: "NUMERIC"}, "bad..decimal"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for bad decimal, got %v", err)
	}
	// Bad Precision / Scale
	if _, err := NormalizeScalar(Field{Type: "NUMERIC", Precision: "bad"}, "1.23"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for bad precision, got %v", err)
	}
	if _, err := NormalizeScalar(Field{Type: "NUMERIC", Precision: "10", Scale: "bad"}, "1.23"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for bad scale, got %v", err)
	}
	// configuredScale out of bounds
	if _, err := NormalizeScalar(Field{Type: "NUMERIC", Precision: "10", Scale: "15"}, "1.23"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for scale > maxScale, got %v", err)
	}
	// scale > maxScale in number
	if _, err := NormalizeScalar(Field{Type: "NUMERIC"}, "1.1234567890"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for scale > 9, got %v", err)
	}
	// integer digits > maxInteger
	if _, err := NormalizeScalar(Field{Type: "NUMERIC"}, strings.Repeat("1", 30)); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for int digits > 29, got %v", err)
	}
	// BIGNUMERIC scaled overflow
	if _, err := NormalizeScalar(Field{Type: "BIGNUMERIC"}, strings.Repeat("9", 39)); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for BIGNUMERIC overflow, got %v", err)
	}
	// BIGNUMERIC negative overflow
	if _, err := NormalizeScalar(Field{Type: "BIGNUMERIC"}, "-"+strings.Repeat("9", 39)); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for negative BIGNUMERIC overflow, got %v", err)
	}
	// Canonical number formatting: whole is empty ("0.5" -> "0.5"), trailing zeros stripped
	if c, err := NormalizeScalar(Field{Type: "NUMERIC"}, "0.5000"); err != nil || c.Value != "0.5" {
		t.Fatalf("expected 0.5, got %v, err: %v", c.Value, err)
	}
	if c, err := NormalizeScalar(Field{Type: "NUMERIC"}, "-0.000"); err != nil || c.Value != "0" {
		t.Fatalf("expected 0 for -0.000, got %v, err: %v", c.Value, err)
	}
	if c, err := NormalizeScalar(Field{Type: "NUMERIC"}, "-12.340"); err != nil || c.Value != "-12.34" {
		t.Fatalf("expected -12.34, got %v, err: %v", c.Value, err)
	}

	// Precision and Scale configured successfully
	if c, err := NormalizeScalar(Field{Type: "NUMERIC", Precision: "10", Scale: "2"}, "12.34"); err != nil || c.Value != "12.34" {
		t.Fatalf("expected 12.34 for configured precision/scale, got %+v, err: %v", c, err)
	}

	// FLOAT64
	if _, err := NormalizeScalar(Field{Type: "FLOAT64"}, "not-a-float"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value, got %v", err)
	}
	// Float infinity overflow
	if _, err := NormalizeScalar(Field{Type: "FLOAT64"}, "1e309"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for 1e309 infinity, got %v", err)
	}
	if c, err := NormalizeScalar(Field{Type: "FLOAT64"}, "0.0"); err != nil || c.Value != "0" {
		t.Fatalf("expected 0, got %v, err: %v", c.Value, err)
	}
	// Large float formatted with e notation
	if c, err := NormalizeScalar(Field{Type: "FLOAT64"}, "1e25"); err != nil || !strings.Contains(c.Value.(string), "e+") {
		t.Fatalf("expected e+ in large float, got %v, err: %v", c.Value, err)
	}
	// Small float formatted with e notation
	if c, err := NormalizeScalar(Field{Type: "FLOAT64"}, "1e-8"); err != nil || !strings.Contains(c.Value.(string), "e-") {
		t.Fatalf("expected e- in small float, got %v, err: %v", c.Value, err)
	}

	// BOOL
	if c, err := NormalizeScalar(Field{Type: "BOOL"}, "true"); err != nil || c.Value != true {
		t.Fatalf("expected true, got %+v", c)
	}
	if c, err := NormalizeScalar(Field{Type: "BOOL"}, "false"); err != nil || c.Value != false {
		t.Fatalf("expected false, got %+v", c)
	}
	if _, err := NormalizeScalar(Field{Type: "BOOL"}, "maybe"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for maybe, got %v", err)
	}

	// BYTES
	if _, err := NormalizeScalar(Field{Type: "BYTES"}, "bad-base64-!!"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for bad base64, got %v", err)
	}
	if c, err := NormalizeScalar(Field{Type: "BYTES"}, "aGVsbG8="); err != nil || c.Value != "aGVsbG8=" {
		t.Fatalf("expected valid bytes, got %+v, err: %v", c, err)
	}

	// DATE
	if _, err := NormalizeScalar(Field{Type: "DATE"}, "2023-13-45"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for bad date, got %v", err)
	}
	if _, err := NormalizeScalar(Field{Type: "DATE"}, "0000-01-01"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for year 0, got %v", err)
	}
	if c, err := NormalizeScalar(Field{Type: "DATE"}, "2023-01-15"); err != nil || c.Value != "2023-01-15" {
		t.Fatalf("expected valid date, got %+v, err: %v", c, err)
	}

	// TIME
	if _, err := NormalizeScalar(Field{Type: "TIME"}, "not-digits"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for not-digits, got %v", err)
	}
	if _, err := NormalizeScalar(Field{Type: "TIME"}, "25:00:00"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for 25:00:00, got %v", err)
	}
	if c, err := NormalizeScalar(Field{Type: "TIME"}, "14:30:00.123456"); err != nil || c.Value != "14:30:00.123456" {
		t.Fatalf("expected valid time, got %+v, err: %v", c, err)
	}

	// DATETIME
	if _, err := NormalizeScalar(Field{Type: "DATETIME"}, "bad-datetime"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value, got %v", err)
	}
	if _, err := NormalizeScalar(Field{Type: "DATETIME"}, "2023-01-15T25:00:00"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value, got %v", err)
	}
	// DATETIME with space separator
	if c, err := NormalizeScalar(Field{Type: "DATETIME"}, "2023-01-15 14:30:00"); err != nil || c.Value != "2023-01-15T14:30:00" {
		t.Fatalf("expected datetime with T, got %+v, err: %v", c, err)
	}

	// JSON
	if _, err := NormalizeScalar(Field{Type: "JSON"}, "not-json{"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for invalid json, got %v", err)
	}
	if c, err := NormalizeScalar(Field{Type: "JSON"}, `{"key":"val"}`); err != nil || c.Value != `{"key":"val"}` {
		t.Fatalf("expected valid json cell, got %+v, err: %v", c, err)
	}
}

func TestValues_DecodeRows_AllBranches(t *testing.T) {
	t.Parallel()

	fields := []Field{{Name: "x", Type: "INT64"}}

	// len(fields) < 1
	if _, err := DecodeRows([]byte("[]"), nil, 1000); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for empty fields, got %v", err)
	}
	// len(fields) > 128
	manyFields := make([]Field, 129)
	if _, err := DecodeRows([]byte("[]"), manyFields, 1000); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for >128 fields, got %v", err)
	}

	// ParseJSON failure
	if _, err := DecodeRows([]byte("corrupt-json"), fields, 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// Not []any (e.g. object)
	if _, err := DecodeRows([]byte(`{"not":"array"}`), fields, 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-array, got %v", err)
	}

	// len(rows) > 1000
	var b bytes.Buffer
	b.WriteString("[")
	for i := 0; i < 1001; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"f":[{"v":"1"}]}`)
	}
	b.WriteString("]")
	if _, err := DecodeRows(b.Bytes(), fields, 10000000); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit for >1000 rows, got %v", err)
	}

	// Row not a map with len 1
	if _, err := DecodeRows([]byte(`[{"f":[{"v":"1"}],"extra":true}]`), fields, 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for extra row keys, got %v", err)
	}

	// Row without "f"
	if _, err := DecodeRows([]byte(`[{"wrong":[{"v":"1"}]}]`), fields, 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for missing f, got %v", err)
	}

	// Length of "f" mismatch with fields
	if _, err := DecodeRows([]byte(`[{"f":[]}]`), fields, 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for field length mismatch, got %v", err)
	}

	// Cell not map with len 1
	if _, err := DecodeRows([]byte(`[{"f":[{"v":"1","bad":2}]}]`), fields, 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for cell extra keys, got %v", err)
	}

	// Cell without "v"
	if _, err := DecodeRows([]byte(`[{"f":[{"no_v":"1"}]}]`), fields, 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for cell missing v, got %v", err)
	}

	// NormalizeScalar failure on cell
	if _, err := DecodeRows([]byte(`[{"f":[{"v":"not-int"}]}]`), fields, 1000); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value from cell normalize, got %v", err)
	}

	// Success
	rows, err := DecodeRows([]byte(`[{"f":[{"v":"42"}]}]`), fields, 1000)
	if err != nil || len(rows) != 1 || rows[0][0].Value != "42" {
		t.Fatalf("expected valid row decoded, got %+v, err: %v", rows, err)
	}
}

func TestCanonical_ParseJSONAndSurrogates(t *testing.T) {
	t.Parallel()

	// Error.Error()
	e := &Error{Code: "test_code", Reason: "test_reason"}
	if e.Error() != "test_code" {
		t.Fatalf("expected test_code, got %s", e.Error())
	}

	// ParseJSON limit checks
	if _, err := ParseJSON([]byte(`{}`), 0); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit for limit 0, got %v", err)
	}
	if _, err := ParseJSON([]byte(`{}`), MaxResponseBytes+1); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit for limit > MaxResponseBytes, got %v", err)
	}
	if _, err := ParseJSON([]byte(`{"a":123}`), 3); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit for bytes > limit, got %v", err)
	}

	// ParseJSON non-utf8
	if _, err := ParseJSON([]byte{0xff, 0xfe}, 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-utf8, got %v", err)
	}

	// Trailing bytes after JSON
	if _, err := ParseJSON([]byte(`{"a":1} 2`), 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for trailing token, got %v", err)
	}

	// validSurrogates branches
	// Escaped non-u character (covers line 63)
	if !validSurrogates([]byte(`"\"hello\""`)) {
		t.Fatal("expected true for escaped quotes")
	}
	// Trailing backslash
	if validSurrogates([]byte(`{"a":"\`)) {
		t.Fatal("expected false for trailing backslash")
	}
	// \u with < 4 hex chars
	if validSurrogates([]byte(`{"a":"\u12"`)) {
		t.Fatal("expected false for short unicode escape")
	}
	// \u with invalid hex
	if validSurrogates([]byte(`{"a":"\uZZZZ"`)) {
		t.Fatal("expected false for invalid hex in unicode escape")
	}
	// Lone low surrogate
	if validSurrogates([]byte(`{"a":"\udc00"`)) {
		t.Fatal("expected false for lone low surrogate")
	}
	// High surrogate not followed by \u
	if validSurrogates([]byte(`{"a":"\ud800x"`)) {
		t.Fatal("expected false for high surrogate not followed by \\u")
	}
	// High surrogate followed by invalid low surrogate
	if validSurrogates([]byte(`{"a":"\ud800\u0000"`)) {
		t.Fatal("expected false for high surrogate with bad low surrogate")
	}
	// High surrogate with invalid low surrogate hex
	if validSurrogates([]byte(`{"a":"\ud800\uZZZZ"`)) {
		t.Fatal("expected false for high surrogate with non-hex low surrogate")
	}
	// Valid surrogate pair
	if !validSurrogates([]byte(`{"a":"\ud83d\ude00"}`)) {
		t.Fatal("expected true for valid surrogate pair")
	}

	// parseValue: depth > MaxDepth
	deep := strings.Repeat("[", MaxDepth+2) + strings.Repeat("]", MaxDepth+2)
	if _, err := ParseJSON([]byte(deep), 10000); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit for depth > MaxDepth, got %v", err)
	}

	// Duplicate key in map
	if _, err := ParseJSON([]byte(`{"k":1,"k":2}`), 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for duplicate key, got %v", err)
	}

	// Unclosed object
	if _, err := ParseJSON([]byte(`{"k":1`), 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for unclosed object, got %v", err)
	}

	// Unclosed array
	if _, err := ParseJSON([]byte(`[1, 2`), 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for unclosed array, got %v", err)
	}

	// CanonicalJSON success
	raw, err := CanonicalJSON([]byte(`{"b": 2, "a": 1}`))
	if err != nil || string(raw) != `{"a":1,"b":2}` {
		t.Fatalf("expected sorted canonical json, got %s, err: %v", string(raw), err)
	}

	// parseValue: null token
	nullVal, err := ParseJSON([]byte(`null`), 1000)
	if err != nil || nullVal != nil {
		t.Fatalf("expected nil for null token, got %v, err: %v", nullVal, err)
	}

	// parseValue: invalid JSON value
	if _, err := ParseJSON([]byte(`]`), 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}
	// parseValue: object key error
	if _, err := ParseJSON([]byte(`{1:2}`), 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-string key, got %v", err)
	}
	// parseValue: object value syntax error
	if _, err := ParseJSON([]byte(`{"a": [}`), 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for nested syntax error, got %v", err)
	}
	// parseValue: object delim error
	if _, err := ParseJSON([]byte(`{"a": 1]}`), 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for object delim error, got %v", err)
	}
	// parseValue: array delim error
	if _, err := ParseJSON([]byte(`[1}`), 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for array delim error, got %v", err)
	}

	// CanonicalJSON errors
	if _, err := CanonicalJSON([]byte(`{`)); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on invalid CanonicalJSON, got %v", err)
	}

	// utf16Less length comparison
	if !utf16Less("a", "ab") {
		t.Fatal("expected true for a < ab")
	}

	// writeString escape sequences: \b, \t, \n, \f, \r, control characters, quotes, backslashes
	controlStr := "hello\b\t\n\f\r\x01\"world\"\\"
	var buf bytes.Buffer
	writeString(&buf, controlStr)
	if !strings.Contains(buf.String(), `\b`) || !strings.Contains(buf.String(), `\"`) || !strings.Contains(buf.String(), `\\`) {
		t.Fatalf("expected escaped characters, got %s", buf.String())
	}

	// encodeCanonical: bool true
	buf.Reset()
	if err := encodeCanonical(&buf, true); err != nil || buf.String() != "true" {
		t.Fatalf("expected true, got %s, err: %v", buf.String(), err)
	}
	// encodeCanonical: array with multiple items
	buf.Reset()
	if err := encodeCanonical(&buf, []any{"a", "b"}); err != nil || buf.String() != `["a","b"]` {
		t.Fatalf("expected [\"a\",\"b\"], got %s, err: %v", buf.String(), err)
	}
	// encodeCanonical: array element error
	if err := encodeCanonical(&buf, []any{make(chan int)}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for unsupported array element, got %v", err)
	}
	// encodeCanonical: map value error
	if err := encodeCanonical(&buf, map[string]any{"bad": make(chan int)}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for unsupported map value, got %v", err)
	}
	// encodeCanonical: unsupported root type
	if err := encodeCanonical(&buf, make(chan int)); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for channel, got %v", err)
	}
	// encodeCanonical: json.Number overflow
	if err := encodeCanonical(&buf, json.Number("999999999999999999")); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for json.Number overflow, got %v", err)
	}
	// encodeCanonical: bool false (covers line 208)
	buf.Reset()
	if err := encodeCanonical(&buf, false); err != nil || buf.String() != "false" {
		t.Fatalf("expected false, got %s, err: %v", buf.String(), err)
	}
	// canonical failure (covers line 160)
	if _, err := canonical(make(chan int)); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input from canonical, got %v", err)
	}
	// ParseJSON on standalone closing delimiter
	if _, err := ParseJSON([]byte("}"), 1000); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on }, got %v", err)
	}
	// parseValue directly on closing delimiter (covers line 142)
	decClosed := json.NewDecoder(strings.NewReader("{}"))
	_, _ = decClosed.Token() // consumes '{'
	if _, err := parseValue(decClosed, 0); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on parseValue Delim('}'), got %v", err)
	}

	// HashPayload: invalid JSON
	if _, _, err := HashPayload("Observation", []byte(`{`)); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}
	// HashPayload: ReadPlan
	validReadPlan := `{"version":1,"sourceDigest":"s","projection":[],"where":null,"order":[],"limit":10,"parameters":[],"sql":"SELECT 1","digest":"d"}`
	rpHash, _, err := HashPayload("ReadPlan", []byte(validReadPlan))
	if err != nil || len(rpHash) != 64 {
		t.Fatalf("expected 64-char hash for ReadPlan, got %s, err: %v", rpHash, err)
	}
	// HashPayload: SourceProfile
	validSourceProfile := `{"version":1,"sourceId":"s","descriptorDigest":"d","logicalCollection":"c","sourceProject":"p","datasetId":"ds","tableId":"t","location":"US","schema":[],"publisherReviewRef":"r","rightsReviewRef":"rr","use":"u"}`
	spHash, _, err := HashPayload("SourceProfile", []byte(validSourceProfile))
	if err != nil || len(spHash) != 64 {
		t.Fatalf("expected 64-char hash for SourceProfile, got %s, err: %v", spHash, err)
	}

	// HashPayload branches
	// Unknown payload name
	if _, _, err := HashPayload("UnknownType", []byte(`{}`)); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for unknown type, got %v", err)
	}
	// Non-object payload
	if _, _, err := HashPayload("Observation", []byte(`"not-an-object"`)); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for non-object, got %v", err)
	}
	// Missing required field
	if _, _, err := HashPayload("Observation", []byte(`{"table":{"projectId":"p","datasetId":"d","tableId":"t"}}`)); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for missing fields, got %v", err)
	}
	// Disallowed field
	validObs := `{"table":{"projectId":"p","datasetId":"d","tableId":"t"},"location":"US","type":"TABLE","config":{},"schema":[],"extra_bad":1}`
	if _, _, err := HashPayload("Observation", []byte(validObs)); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for extra disallowed field, got %v", err)
	}
	// Valid Observation with optional fields
	validObsWithOptional := `{"table":{"projectId":"p","datasetId":"d","tableId":"t"},"location":"US","type":"TABLE","config":{},"schema":[],"digest":"d","observedAt":"t","etag":"e","lastModified":"m"}`
	hash, _, err := HashPayload("Observation", []byte(validObsWithOptional))
	if err != nil || len(hash) != 64 {
		t.Fatalf("expected 64-char sha256 hex, got %s, err: %v", hash, err)
	}

	// Valid Approval with optional fields
	validApproval := `{"effectivePlanDigest":"p","observationDigest":"o","policyDigest":"pol","principal":{"kind":"k","subject":"s","generation":"g"},"jobProject":"jp","location":"US","maximumBytesBilled":"100","sessionBudgetBytes":"1000","bounds":{"pageSize":10,"maxRows":10,"maxPages":1,"responseBytes":1000,"totalResponseBytes":1000,"wallMs":1000,"httpMs":1000,"concurrency":1},"estimatedBytes":"100","createdAt":"t","expiresAt":"e","nonce":"n"}`
	appHash, _, err := HashPayload("Approval", []byte(validApproval))
	if err != nil || len(appHash) != 64 {
		t.Fatalf("expected 64-char hash for Approval, got %s, err: %v", appHash, err)
	}

	// HashPayload canonical error (covers line 300)
	obsBadNum := `{"table":{"projectId":"p","datasetId":"d","tableId":"t"},"location":"US","type":"TABLE","config":{"num":99999999999999999999},"schema":[]}`
	if _, _, err := HashPayload("Observation", []byte(obsBadNum)); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for overflowing json.Number in HashPayload, got %v", err)
	}
}

func TestSourceHTTP_RoundTripAndMethods(t *testing.T) {
	t.Parallel()

	st := sourceTransport{
		base: rt(func(req *http.Request) (*http.Response, error) {
			return response(`{"status":"ok"}`), nil
		}),
		project: "p1",
		dataset: "ds1",
	}

	mustReq := func(method, urlStr string) *http.Request {
		req, _ := http.NewRequest(method, urlStr, nil)
		return req
	}

	// Invalid methods, body, scheme, host, user, fragment, escaped path
	for _, badReq := range []*http.Request{
		mustReq("POST", "https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1?datasetView=METADATA"),
		mustReq("GET", "http://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1?datasetView=METADATA"),
		mustReq("GET", "https://other.googleapis.com/bigquery/v2/projects/p1/datasets/ds1?datasetView=METADATA"),
		mustReq("GET", "https://user@bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1?datasetView=METADATA"),
		mustReq("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1#frag"),
		mustReq("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1?datasetView=WRONG"),
		mustReq("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1/unknown_route"),
		mustReq("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1/tables/bad..id?view=STORAGE_STATS"),
		mustReq("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1/tables/t1/data?formatOptions.useInt64Timestamp=false"),
		mustReq("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1/tables/t1?view=WRONG"),
		mustReq("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1/tables?maxResults=bad"),
		mustReq("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1/tables?maxResults=2000"),
		mustReq("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1/tables?pageToken="),
		mustReq("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1/tables?alt=xml"),
		mustReq("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1/tables?prettyPrint=true"),
	} {
		if _, err := st.RoundTrip(badReq); errorCode(err) != "invalid_input" {
			t.Fatalf("expected invalid_input for req %s %s, got %v", badReq.Method, badReq.URL.String(), err)
		}
	}

	// Valid RoundTrip calls
	for _, goodURL := range []string{
		"https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1?datasetView=METADATA&alt=json&prettyPrint=false",
		"https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1/tables?maxResults=100&pageToken=token123",
		"https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1/tables/t1?view=STORAGE_STATS",
		"https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets/ds1/tables/t1/data?formatOptions.useInt64Timestamp=true",
	} {
		req, _ := http.NewRequest("GET", goodURL, nil)
		resp, err := st.RoundTrip(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("expected 200 for %s, got resp %+v, err: %v", goodURL, resp, err)
		}
	}

	// sourceToken helper
	if tok, err := sourceToken(map[string]any{}, "missing"); err != nil || tok != "" {
		t.Fatalf("expected empty token for missing key, got %q, err: %v", tok, err)
	}
	if _, err := sourceToken(map[string]any{"tok": 123}, "tok"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-string token, got %v", err)
	}
	if _, err := sourceToken(map[string]any{"tok": strings.Repeat("a", 4097)}, "tok"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for >4096 token, got %v", err)
	}
	if tok, err := sourceToken(map[string]any{"tok": "valid_token"}, "tok"); err != nil || tok != "valid_token" {
		t.Fatalf("expected valid_token, got %q, err: %v", tok, err)
	}

	// sourceCount helper
	if _, err := sourceCount(map[string]any{}, "missing"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for missing count, got %v", err)
	}
	if _, err := sourceCount(map[string]any{"c": "not-int"}, "c"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-int, got %v", err)
	}
	if _, err := sourceCount(map[string]any{"c": "-5"}, "c"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for negative count, got %v", err)
	}
	if n, err := sourceCount(map[string]any{"c": "42"}, "c"); err != nil || n != 42 {
		t.Fatalf("expected 42, got %d, err: %v", n, err)
	}

	// sourceDeadline
	ctx, cancel := sourceDeadline(context.Background(), time.Minute)
	defer cancel()
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("expected deadline to be set")
	}

	// sourceDatabase methods
	prov := staticProvider{id: Identity{Principal: Principal{"workload", "s", "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}}
	baseTransport := rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/ds1") {
			return response(`{"datasetReference":{"projectId":"p1","datasetId":"ds1"}}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/t1") {
			return response(`{"tableReference":{"projectId":"p1","datasetId":"ds1","tableId":"t1"}}`), nil
		}
		return response(`{}`), nil
	})
	dbFull := &sourceDatabase{
		project:   "p1",
		dataset:   "ds1",
		provider:  prov,
		transport: baseTransport,
		clock:     realClock{},
		limits:    SourceLimits{ResponseBytes: 1024 * 1024},
	}

	// db.client()
	if c := dbFull.client(); c == nil {
		t.Fatal("expected non-nil client")
	}

	// db.identity() success
	p, err := dbFull.identity(context.Background())
	if err != nil || p.Subject != "s" {
		t.Fatalf("expected principal s, got %+v, err: %v", p, err)
	}

	// db.identity() with auth error
	dbBadAuth := &sourceDatabase{project: "p1", dataset: "ds1", provider: staticProvider{err: errors.New("auth fail")}, clock: realClock{}}
	if _, err := dbBadAuth.identity(context.Background()); errorCode(err) != "auth_required" {
		t.Fatalf("expected auth_required, got %v", err)
	}

	// db.identity() with !validPrincipal
	dbBadPrin := &sourceDatabase{project: "p1", dataset: "ds1", provider: staticProvider{id: Identity{Principal: Principal{}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}}, clock: realClock{}}
	if _, err := dbBadPrin.identity(context.Background()); errorCode(err) != "auth_required" {
		t.Fatalf("expected auth_required, got %v", err)
	}

	// db.identity() with !Read
	dbNoRead := &sourceDatabase{project: "p1", dataset: "ds1", provider: staticProvider{id: Identity{Principal: Principal{"workload", "s", "1"}, Read: false, ExpiresAt: time.Now().Add(time.Hour)}}, clock: realClock{}}
	if _, err := dbNoRead.identity(context.Background()); errorCode(err) != "auth_required" {
		t.Fatalf("expected auth_required for !Read, got %v", err)
	}

	// db.identity() expired
	dbExpired := &sourceDatabase{project: "p1", dataset: "ds1", provider: staticProvider{id: Identity{Principal: Principal{"workload", "s", "1"}, Read: true, ExpiresAt: time.Now().Add(-time.Hour)}}, clock: realClock{}}
	if _, err := dbExpired.identity(context.Background()); errorCode(err) != "auth_expired" {
		t.Fatalf("expected auth_expired, got %v", err)
	}

	// db.datasetMetadata() success
	if err := dbFull.datasetMetadata(context.Background(), p); err != nil {
		t.Fatalf("unexpected error on datasetMetadata: %v", err)
	}

	// db.datasetMetadata() mismatch
	dbMismatch := &sourceDatabase{
		project: "p1", dataset: "ds1", provider: prov, clock: realClock{}, limits: SourceLimits{ResponseBytes: 1024 * 1024},
		transport: rt(func(*http.Request) (*http.Response, error) {
			return response(`{"datasetReference":{"projectId":"wrong","datasetId":"ds1"}}`), nil
		}),
	}
	if err := dbMismatch.datasetMetadata(context.Background(), p); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed on project mismatch, got %v", err)
	}

	// db.tableMetadata() success
	tblMap, err := dbFull.tableMetadata(context.Background(), p, "t1")
	if err != nil || tblMap == nil {
		t.Fatalf("unexpected error on tableMetadata: %v", err)
	}

	// db.tableMetadata() mismatch
	dbTblMismatch := &sourceDatabase{
		project: "p1", dataset: "ds1", provider: prov, clock: realClock{}, limits: SourceLimits{ResponseBytes: 1024 * 1024},
		transport: rt(func(*http.Request) (*http.Response, error) {
			return response(`{"tableReference":{"projectId":"p1","datasetId":"ds1","tableId":"other"}}`), nil
		}),
	}
	if _, err := dbTblMismatch.tableMetadata(context.Background(), p, "t1"); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed on table mismatch, got %v", err)
	}

	// db.tableMetadata with invalid name
	if _, err := dbFull.tableMetadata(context.Background(), p, "bad..table"); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad table name, got %v", err)
	}

	// db.datasetMetadata() get error (covers line 118)
	dbErr := &sourceDatabase{
		project: "p1", dataset: "ds1", provider: prov, clock: realClock{}, limits: SourceLimits{ResponseBytes: 1024 * 1024},
		transport: rt(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(strings.NewReader(`{"error":"boom"}`))}, nil
		}),
	}
	if err := dbErr.datasetMetadata(context.Background(), p); err == nil {
		t.Fatal("expected error on failing transport for datasetMetadata")
	}

	// db.tableMetadata() get error (covers line 136)
	if _, err := dbErr.tableMetadata(context.Background(), p, "t1"); err == nil {
		t.Fatal("expected error on failing transport for tableMetadata")
	}
}

// Not parallel: fills global dependencySlots channel.
func TestDependency_CapacityAndSlots(t *testing.T) {
	// 1. ctx.Err() != nil at start
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err, admitted := awaitOwnedDependency[int](canceledCtx, func() (int, error) { return 1, nil }, nil, nil); errorCode(err) != "local_stopped" || admitted {
		t.Fatalf("expected local_stopped and admitted=false on canceled context, got %v", err)
	}

	// 2. Slots capacity exhaustion
	// Fill all 128 slots
	var slots []struct{}
	for {
		select {
		case dependencySlots <- struct{}{}:
			slots = append(slots, struct{}{})
		default:
			goto filled
		}
	}
filled:
	defer func() {
		for range slots {
			<-dependencySlots
		}
	}()

	// Now awaitOwnedDependency must hit default branch (local_stopped)
	if _, err, admitted := awaitOwnedDependency[int](context.Background(), func() (int, error) { return 1, nil }, nil, nil); errorCode(err) != "local_stopped" || admitted {
		t.Fatalf("expected local_stopped on slot exhaustion, got %v, admitted: %v", err, admitted)
	}

	// discardAsync with full slots hits default branch (no-op)
	discardAsync(42, func(int) {})

	// Release 1 slot
	<-dependencySlots
	slots = slots[1:]

	// Test done received but ctx canceled before accepted
	countErr := 0
	fCtx := fakeErrContext{Context: context.Background(), checkCount: &countErr}
	discarded := make(chan bool, 1)
	_, errAOD, admittedAOD := awaitOwnedDependency[int](fCtx, func() (int, error) {
		return 99, nil
	}, func(v int) {
		if v == 99 {
			discarded <- true
		}
	}, nil)
	if errorCode(errAOD) != "local_stopped" || !admittedAOD {
		t.Fatalf("expected local_stopped and admitted=true, got err: %v, admitted: %v", errAOD, admittedAOD)
	}
	select {
	case <-discarded:
	case <-time.After(time.Second):
		t.Fatal("discard was not called")
	}

	// enterActualRequest busy flag
	leave, err := enterActualRequest("req-1")
	if err != nil {
		t.Fatal(err)
	}
	defer leave()
	if _, err := enterActualRequest("req-1"); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on busy request id, got %v", err)
	}

	// boundedContext with d <= 0
	bCtx, bCancel := boundedContext(context.Background(), 0)
	defer bCancel()
	if _, ok := bCtx.Deadline(); !ok {
		t.Fatal("expected deadline set on boundedContext(0)")
	}

	// trackedBody Read on eof/closed
	tb := newTrackedBody(io.NopCloser(strings.NewReader("")), func() {})
	tb.closed = true
	if _, err := tb.Read(make([]byte, 10)); err != io.EOF {
		t.Fatalf("expected io.EOF on closed trackedBody, got %v", err)
	}

	// trackedBody Close when closeRequested == nil (direct physicalClose)
	tbDirect := &trackedBody{body: io.NopCloser(strings.NewReader("abc")), leave: func() {}}
	if err := tbDirect.Close(); err != nil {
		t.Fatalf("expected nil error on direct Close, got %v", err)
	}

	// trackedBody closeWithin timeout
	hangBody := io.NopCloser(&hangReader{})
	tbHang := newTrackedBody(hangBody, func() {})
	deadCtx, deadCancel := context.WithCancel(context.Background())
	deadCancel()
	if err := tbHang.closeWithin(deadCtx); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped from closeWithin on canceled context, got %v", err)
	}
}

type hangReader struct{}

func (hangReader) Read([]byte) (int, error) { return 0, nil }

type fakeErrContext struct {
	context.Context
	checkCount *int
}

func (f fakeErrContext) Err() error {
	*f.checkCount++
	if *f.checkCount > 1 {
		return context.Canceled
	}
	return nil
}

func (fakeErrContext) Done() <-chan struct{} {
	return nil
}
