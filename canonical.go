// Package bigquery contains the bounded analytical protocol building blocks.
// Transport and execution integration are developed in subsequent tranches.
package bigquery

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// Error contains a stable, sanitized code, never query values or provider bodies.
type Error struct{ Code string }

func (e *Error) Error() string { return e.Code }
func fail(code string) error   { return &Error{Code: code} }

const MaxResponseBytes = 10 * 1024 * 1024
const MaxDepth = 32

// ParseJSON validates bounded raw bytes without collapsing null or losing integers.
// Callers must bound decompressed reads before allocating this buffer.
func ParseJSON(raw []byte, limit int) (any, error) {
	if limit <= 0 || limit > MaxResponseBytes || len(raw) > limit {
		return nil, fail("response_limit")
	}
	if !utf8.Valid(raw) || !validSurrogates(raw) {
		return nil, fail("malformed_wire")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	value, err := parseValue(dec, 0)
	if err != nil {
		return nil, err
	}
	if _, err = dec.Token(); err != io.EOF {
		return nil, fail("malformed_wire")
	}
	return value, nil
}
func validSurrogates(raw []byte) bool {
	// Decoder validates syntax. This pass rejects lone escaped UTF-16 surrogates
	// before encoding/json can replace them with U+FFFD. Escaped backslashes skip.
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		n, e := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if e != nil {
			return false
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			low, e := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if e != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}
func parseValue(d *json.Decoder, depth int) (any, error) {
	if depth > MaxDepth {
		return nil, fail("response_limit")
	}
	t, e := d.Token()
	if e != nil {
		return nil, fail("malformed_wire")
	}
	switch t {
	case json.Delim('{'):
		m := map[string]any{}
		for d.More() {
			k, e := d.Token()
			if e != nil {
				return nil, fail("malformed_wire")
			}
			s, ok := k.(string)
			if !ok {
				return nil, fail("malformed_wire")
			}
			if _, exists := m[s]; exists {
				return nil, fail("malformed_wire")
			}
			v, e := parseValue(d, depth+1)
			if e != nil {
				return nil, e
			}
			m[s] = v
		}
		end, e := d.Token()
		if e != nil || end != json.Delim('}') {
			return nil, fail("malformed_wire")
		}
		return m, nil
	case json.Delim('['):
		a := []any{}
		for d.More() {
			v, e := parseValue(d, depth+1)
			if e != nil {
				return nil, e
			}
			a = append(a, v)
		}
		end, e := d.Token()
		if e != nil || end != json.Delim(']') {
			return nil, fail("malformed_wire")
		}
		return a, nil
	}
	switch t.(type) {
	case string, bool, json.Number:
		return t, nil
	}
	if t == nil {
		return nil, nil
	}
	return nil, fail("malformed_wire")
}

// CanonicalJSON implements RFC8785 for adapter-owned payloads restricted to safe
// integer JSON tokens. Warehouse integer/decimal/float values are string tokens.
func CanonicalJSON(raw []byte) ([]byte, error) {
	v, e := ParseJSON(raw, MaxResponseBytes)
	if e != nil {
		return nil, e
	}
	return canonical(v)
}
func canonical(v any) ([]byte, error) {
	var b bytes.Buffer
	if e := encodeCanonical(&b, v); e != nil {
		return nil, e
	}
	return b.Bytes(), nil
}
func utf16Less(a, b string) bool {
	aa, bb := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(aa) && i < len(bb); i++ {
		if aa[i] != bb[i] {
			return aa[i] < bb[i]
		}
	}
	return len(aa) < len(bb)
}
func writeString(b *bytes.Buffer, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\b':
			b.WriteString(`\b`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\f':
			b.WriteString(`\f`)
		case '\r':
			b.WriteString(`\r`)
		default:
			if r < 32 {
				fmt.Fprintf(b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}
func encodeCanonical(b *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if x {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case string:
		writeString(b, x)
	case json.Number:
		n, e := strconv.ParseInt(string(x), 10, 64)
		if e != nil || n > 9007199254740991 || n < -9007199254740991 {
			return fail("unsupported_value")
		}
		b.WriteString(strconv.FormatInt(n, 10))
	case []any:
		b.WriteByte('[')
		for i, v := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			if e := encodeCanonical(b, v); e != nil {
				return e
			}
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return utf16Less(keys[i], keys[j]) })
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			writeString(b, k)
			b.WriteByte(':')
			if e := encodeCanonical(b, x[k]); e != nil {
				return e
			}
		}
		b.WriteByte('}')
	default:
		return fail("invalid_input")
	}
	return nil
}

// HashPayload uses explicit named projections, never recursive digest deletion.
// Non-bound observation times and approval nonce/times may appear at top level.
func HashPayload(name string, raw []byte) (string, []byte, error) {
	v, e := ParseJSON(raw, MaxResponseBytes)
	if e != nil {
		return "", nil, e
	}
	m, ok := v.(map[string]any)
	if !ok {
		return "", nil, fail("invalid_input")
	}
	var fields, optional []string
	switch name {
	case "ReadPlan":
		fields = strings.Split("version sourceDigest projection where order limit parameters sql", " ")
		optional = []string{"digest"}
	case "SourceProfile":
		fields = strings.Split("version sourceId descriptorDigest logicalCollection sourceProject datasetId tableId location schema publisherReviewRef rightsReviewRef use", " ")
	case "Observation":
		fields = strings.Split("table location type config schema", " ")
		optional = strings.Split("digest observedAt etag lastModified", " ")
	case "Approval":
		fields = strings.Split("effectivePlanDigest observationDigest policyDigest principal jobProject location maximumBytesBilled sessionBudgetBytes bounds estimatedBytes", " ")
		optional = strings.Split("digest createdAt expiresAt nonce", " ")
	default:
		return "", nil, fail("invalid_input")
	}
	allowed := map[string]bool{}
	projection := map[string]any{}
	for _, k := range fields {
		value, present := m[k]
		if !present {
			return "", nil, fail("invalid_input")
		}
		allowed[k] = true
		projection[k] = value
	}
	for _, k := range optional {
		allowed[k] = true
	}
	for k := range m {
		if !allowed[k] {
			return "", nil, fail("invalid_input")
		}
	}
	b, e := canonical(projection)
	if e != nil {
		return "", nil, e
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), b, nil
}
