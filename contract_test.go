package bigquery

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type scenario struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	Input     string `json:"input"`
	Canonical string `json:"canonical"`
	Digest    string `json:"digest"`
	Field     Field  `json:"field"`
	Value     any    `json:"value"`
	InputHex  string `json:"input_hex"`
	Expected  any    `json:"expected"`
	Error     string `json:"error"`
	Payload   string `json:"payload"`
}

func errorCode(err error) string {
	if err == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return err.Error()
}
func TestSharedCorpus(t *testing.T) {
	raw, e := os.ReadFile("testdata/contract/manifest.json")
	if e != nil {
		t.Fatal(e)
	}
	var manifest struct {
		Files         map[string]string `json:"files"`
		Revision      int               `json:"revision"`
		ScenarioCount int               `json:"scenario_count"`
	}
	if e = json.Unmarshal(raw, &manifest); e != nil {
		t.Fatal(e)
	}
	if manifest.Revision != 2 {
		t.Fatal("corpus revision")
	}
	seen := map[string]bool{}
	for name, digest := range manifest.Files {
		data, e := os.ReadFile(filepath.Join("testdata/contract", name))
		if e != nil {
			t.Fatal(e)
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != digest {
			t.Fatal(name, "corpus hash")
		}
		var cases []scenario
		if e = json.Unmarshal(data, &cases); e != nil {
			t.Fatal(e)
		}
		for _, c := range cases {
			if c.ID == "" || seen[c.ID] {
				t.Fatal("duplicate/empty scenario id", c.ID)
			}
			seen[c.ID] = true
			t.Run(c.ID, func(t *testing.T) {
				switch c.Kind {
				case "canonical":
					got, e := CanonicalJSON([]byte(c.Input))
					if errorCode(e) != c.Error {
						t.Fatalf("code=%s", errorCode(e))
					}
					if e == nil && string(got) != c.Canonical {
						t.Fatalf("canonical %q != %q", got, c.Canonical)
					}
					if e == nil {
						sum := sha256.Sum256(got)
						if hex.EncodeToString(sum[:]) != c.Digest {
							t.Fatal("digest")
						}
					}
				case "scalar":
					got, e := NormalizeScalar(c.Field, c.Value)
					if errorCode(e) != c.Error {
						t.Fatalf("code=%s", errorCode(e))
					}
					if e == nil {
						actual, _ := json.Marshal(got.Value)
						expected, _ := json.Marshal(c.Expected)
						if string(actual) != string(expected) {
							t.Fatalf("value %s != %s", actual, expected)
						}
					}
				case "scalar-bytes":
					raw, e := hex.DecodeString(c.InputHex)
					if e != nil {
						t.Fatal(e)
					}
					got, e := NormalizeScalar(c.Field, string(raw))
					if errorCode(e) != c.Error {
						t.Fatalf("code=%s", errorCode(e))
					}
					if e == nil {
						actual, _ := json.Marshal(got.Value)
						expected, _ := json.Marshal(c.Expected)
						if string(actual) != string(expected) {
							t.Fatalf("value %s != %s", actual, expected)
						}
					}
				case "rows":
					got, e := DecodeRows([]byte(c.Input), []Field{c.Field}, MaxResponseBytes)
					if errorCode(e) != c.Error {
						t.Fatalf("code=%s", errorCode(e))
					}
					if e == nil {
						actual, _ := json.Marshal(got)
						expected, _ := json.Marshal(c.Expected)
						if string(actual) != string(expected) {
							t.Fatalf("rows %s != %s", actual, expected)
						}
					}
				case "hash":
					got, canonical, e := HashPayload(c.Payload, []byte(c.Input))
					if errorCode(e) != c.Error {
						t.Fatalf("code=%s", errorCode(e))
					}
					if e == nil && (got != c.Digest || string(canonical) != c.Canonical) {
						t.Fatal("named payload")
					}
				default:
					t.Fatal("unknown scenario kind")
				}
			})
		}
	}
	if len(seen) != manifest.ScenarioCount {
		t.Fatal("manifest scenario count", len(seen), manifest.ScenarioCount)
	}
}
func TestRawBoundsAndSurrogates(t *testing.T) {
	for _, raw := range [][]byte{[]byte(`{"x":1,"x":2}`), []byte(`[] []`), []byte(`"\ud800"`), []byte(`"\udfff"`), {0xff}} {
		if _, e := ParseJSON(raw, 100); e == nil {
			t.Fatalf("accepted invalid wire %q", raw)
		}
	}
	for _, raw := range []string{`"\ud800\udc00"`, `"\\ud800"`} {
		if _, e := ParseJSON([]byte(raw), 100); e != nil {
			t.Fatal(raw, e)
		}
	}
	if _, e := ParseJSON([]byte(`null`), 3); errorCode(e) != "response_limit" {
		t.Fatal(e)
	}
	if _, e := ParseJSON([]byte(strings.Repeat("[", 34)+"0"+strings.Repeat("]", 34)), 1000); errorCode(e) != "response_limit" {
		t.Fatal(e)
	}
}
func TestNamedPayloadDoesNotRecursivelyRemoveDigest(t *testing.T) {
	base := `{"version":1,"sourceDigest":"s","projection":["n"],"where":{"digest":"bound"},"order":[],"limit":1,"parameters":[],"sql":"s","digest":"ignored"}`
	a, _, e := HashPayload("ReadPlan", []byte(base))
	if e != nil {
		t.Fatal(e)
	}
	b, _, _ := HashPayload("ReadPlan", []byte(strings.Replace(base, "ignored", "other", 1)))
	c, _, _ := HashPayload("ReadPlan", []byte(strings.Replace(base, "bound", "changed", 1)))
	if a != b || a == c {
		t.Fatal("digest projection")
	}
	if _, _, e = HashPayload("ReadPlan", []byte(strings.Replace(base, `"limit":1`, `"limit":1,"unexpected":2`, 1))); e == nil {
		t.Fatal("unknown field")
	}
}
func TestOriginalExecutionDeadline(t *testing.T) {
	start := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	deadline := start.Add(120 * time.Second)
	at119 := start.Add(119 * time.Second)
	got, e := OperationDeadline(at119, deadline, time.Time{}, 15*time.Second, false, 100)
	if e != nil || !got.Equal(deadline) {
		t.Fatal(got, e)
	}
	if _, e = OperationDeadline(start.Add(121*time.Second), deadline, time.Time{}, 15*time.Second, false, 100); errorCode(e) != "local_stopped" {
		t.Fatal("resume renewed deadline", e)
	}
	control, e := OperationDeadline(start.Add(121*time.Second), deadline, time.Time{}, 15*time.Second, true, 100)
	if e != nil || !control.Equal(start.Add(136*time.Second)) {
		t.Fatal(control, e)
	}
	if _, e = OperationDeadline(start.Add(121*time.Second), deadline, time.Time{}, 15*time.Second, true, 0); errorCode(e) != "response_limit" {
		t.Fatal("bytes reset", e)
	}
}

func TestNormalizeScalarUnicodeBoundary(t *testing.T) {
	for _, raw := range [][]byte{{0xff}, {0xed, 0xa0, 0x80}, {0xf0, 0x9f, 0x92}} {
		if _, e := NormalizeScalar(Field{Type: "STRING"}, string(raw)); errorCode(e) != "malformed_wire" {
			t.Fatalf("invalid UTF-8 accepted: %v", e)
		}
	}
	for _, value := range []any{nil, "", "é", "e\u0301", "😀<>&\u2028\u2029"} {
		got, e := NormalizeScalar(Field{Type: "STRING"}, value)
		if e != nil || got.Value != value {
			t.Fatalf("changed scalar %q: %v", value, e)
		}
	}
}
func TestNormalizeScalarFloatLexicalBoundary(t *testing.T) {
	for _, s := range []string{"0x1p2", "1_0", "0x10", " 1", "1 ", "", "+", ".", "1e", "NaN", "Infinity"} {
		if _, e := NormalizeScalar(Field{Type: "FLOAT64"}, s); errorCode(e) != "unsupported_value" {
			t.Fatalf("accepted invalid decimal lexeme %q: %v", s, e)
		}
	}
}
