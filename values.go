package bigquery

import (
	"encoding/base64"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Mode string `json:"mode"`
}
type Cell struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

var integerText = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)
var decimalText = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)

// Decimal FLOAT64 wire grammar is shared with JavaScript; no language-specific
// literals, separators, whitespace or nonfinite names are accepted.
var float64InputText = regexp.MustCompile(`^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$`)
var clockText = regexp.MustCompile(`^[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?$`)

// NormalizeScalar preserves SQL NULL separately from JSON text "null".
func NormalizeScalar(field Field, value any) (Cell, error) {
	typ := strings.ToUpper(field.Type)
	switch typ {
	case "INTEGER":
		typ = "INT64"
	case "FLOAT":
		typ = "FLOAT64"
	case "BOOLEAN":
		typ = "BOOL"
	}
	supported := map[string]bool{"INT64": true, "NUMERIC": true, "BIGNUMERIC": true, "FLOAT64": true, "BOOL": true, "STRING": true, "BYTES": true, "DATE": true, "TIME": true, "DATETIME": true, "TIMESTAMP": true, "JSON": true}
	if !supported[typ] || field.Mode == "REPEATED" {
		return Cell{}, fail("unsupported_type")
	}
	if field.Mode != "" && field.Mode != "NULLABLE" && field.Mode != "REQUIRED" {
		return Cell{}, fail("unsupported_type")
	}
	cell := Cell{Type: typ}
	if value == nil {
		if field.Mode == "REQUIRED" {
			return Cell{}, fail("malformed_wire")
		}
		return cell, nil
	}
	s, ok := value.(string)
	if !ok {
		return Cell{}, fail("malformed_wire")
	}
	if !utf8.ValidString(s) {
		return Cell{}, fail("malformed_wire")
	}
	if len(s) > 1024*1024 {
		return Cell{}, fail("response_limit")
	}
	cell.Value = s
	switch typ {
	case "INT64", "TIMESTAMP":
		if !integerText.MatchString(s) {
			return Cell{}, fail("unsupported_value")
		}
		n, e := strconv.ParseInt(s, 10, 64)
		if e != nil {
			return Cell{}, fail("unsupported_value")
		}
		if typ == "TIMESTAMP" && (n < -62135596800000000 || n > 253402300799999999) {
			return Cell{}, fail("unsupported_value")
		}
		cell.Value = strconv.FormatInt(n, 10)
	case "NUMERIC", "BIGNUMERIC":
		if !decimalText.MatchString(s) {
			return Cell{}, fail("unsupported_value")
		}
		digits := strings.TrimPrefix(s, "-")
		parts := strings.Split(digits, ".")
		scale := 0
		if len(parts) == 2 {
			scale = len(parts[1])
			digits = parts[0] + parts[1]
		}
		maxScale, maxInteger := 9, 29
		if typ == "BIGNUMERIC" {
			maxScale, maxInteger = 38, 39
		}
		if scale > maxScale || len(strings.TrimLeft(parts[0], "0")) > maxInteger {
			return Cell{}, fail("unsupported_value")
		}
		number := new(big.Int)
		number.SetString(digits, 10)
		if typ == "BIGNUMERIC" {
			scaled := new(big.Int).Mul(number, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(38-scale)), nil))
			max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 255), big.NewInt(1))
			if strings.HasPrefix(s, "-") {
				max.Add(max, big.NewInt(1))
			}
			if scaled.Cmp(max) > 0 {
				return Cell{}, fail("unsupported_value")
			}
		}
		whole := strings.TrimLeft(parts[0], "0")
		if whole == "" {
			whole = "0"
		}
		fraction := ""
		if len(parts) == 2 {
			fraction = strings.TrimRight(parts[1], "0")
		}
		canonical := whole
		if fraction != "" {
			canonical += "." + fraction
		}
		if strings.HasPrefix(s, "-") && canonical != "0" {
			canonical = "-" + canonical
		}
		cell.Value = canonical
	case "FLOAT64":
		if !float64InputText.MatchString(s) {
			return Cell{}, fail("unsupported_value")
		}
		f, e := strconv.ParseFloat(s, 64)
		if e != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return Cell{}, fail("unsupported_value")
		}
		cell.Value = float64Text(f)
	case "BOOL":
		if s == "true" {
			cell.Value = true
		} else if s == "false" {
			cell.Value = false
		} else {
			return Cell{}, fail("unsupported_value")
		}
	case "BYTES":
		b, e := base64.StdEncoding.Strict().DecodeString(s)
		if e != nil || base64.StdEncoding.EncodeToString(b) != s {
			return Cell{}, fail("unsupported_value")
		}
	case "DATE":
		t, e := time.Parse("2006-01-02", s)
		if e != nil || t.Year() < 1 || t.Year() > 9999 || t.Format("2006-01-02") != s {
			return Cell{}, fail("unsupported_value")
		}
	case "TIME":
		if !validClock(s) {
			return Cell{}, fail("unsupported_value")
		}
	case "DATETIME":
		parts := strings.Split(s, "T")
		if len(parts) != 2 {
			parts = strings.Split(s, " ")
		}
		if len(parts) != 2 {
			return Cell{}, fail("unsupported_value")
		}
		if _, e := NormalizeScalar(Field{Type: "DATE"}, parts[0]); e != nil || !validClock(parts[1]) {
			return Cell{}, fail("unsupported_value")
		}
		cell.Value = parts[0] + "T" + parts[1]
	case "JSON":
		if _, e := ParseJSON([]byte(s), 1024*1024); e != nil {
			return Cell{}, e
		}
	}
	return cell, nil
}
func validClock(s string) bool {
	if !clockText.MatchString(s) {
		return false
	}
	_, e := time.Parse("15:04:05.999999", s)
	return e == nil
}

// float64Text follows ECMAScript NumberToString exponent thresholds and -0.
func float64Text(f float64) string {
	if f == 0 {
		return "0"
	}
	abs := math.Abs(f)
	format := byte('e')
	if abs >= 1e-6 && abs < 1e21 {
		format = 'f'
	}
	s := strconv.FormatFloat(f, format, -1, 64)
	if format == 'e' {
		parts := strings.Split(s, "e")
		n, _ := strconv.Atoi(parts[1])
		sign := ""
		if n >= 0 {
			sign = "+"
		}
		s = parts[0] + "e" + sign + strconv.Itoa(n)
	}
	return s
}

// DecodeRows validates exact warehouse f/v shapes against an already validated
// projection schema. Schema metadata eligibility is a separate mandatory gate.
func DecodeRows(raw []byte, fields []Field, limit int) ([][]Cell, error) {
	if len(fields) < 1 || len(fields) > 128 {
		return nil, fail("invalid_input")
	}
	value, e := ParseJSON(raw, limit)
	if e != nil {
		return nil, e
	}
	rows, ok := value.([]any)
	if !ok {
		return nil, fail("malformed_wire")
	}
	if len(rows) > 1000 {
		return nil, fail("response_limit")
	}
	result := make([][]Cell, 0, len(rows))
	for _, row := range rows {
		r, ok := row.(map[string]any)
		if !ok || len(r) != 1 {
			return nil, fail("malformed_wire")
		}
		wire, ok := r["f"].([]any)
		if !ok || len(wire) != len(fields) {
			return nil, fail("malformed_wire")
		}
		cells := make([]Cell, len(fields))
		for i, v := range wire {
			m, ok := v.(map[string]any)
			if !ok || len(m) != 1 {
				return nil, fail("malformed_wire")
			}
			value, present := m["v"]
			if !present {
				return nil, fail("malformed_wire")
			}
			cell, e := NormalizeScalar(fields[i], value)
			if e != nil {
				return nil, e
			}
			cells[i] = cell
		}
		result = append(result, cells)
	}
	return result, nil
}
