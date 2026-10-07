package bigquery

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

func TestTimestampRequestFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/requests/timestamp-parameters-r1.json")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != "0b78ab6eca04922b20dca7cfbf0a8e449dc8744a9fdb57556cba71747d30f930" {
		t.Fatal("immutable supplemental JS fixture digest drift")
	}
	var fixture struct {
		Version int    `json:"version"`
		Kind    string `json:"kind"`
		Cases   []struct {
			Epoch string `json:"epochMicroseconds"`
			Wire  string `json:"wireUTC"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Version != 1 || fixture.Kind != "synthetic-timestamp-rest-parameters" || len(fixture.Cases) != 13 {
		t.Fatal("supplemental fixture contract drift")
	}
	profile := testProfile(t)
	profile.Schema = []Field{{Name: "ts", Type: "TIMESTAMP", Mode: "NULLABLE"}}
	for _, example := range fixture.Cases {
		for _, op := range []dal.Operator{dal.Equal, dal.In} {
			for _, dry := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/dry=%t", example.Epoch, op, dry), func(t *testing.T) {
					var input any = example.Epoch
					var canonical any = example.Epoch
					parameterType := map[string]any{"type": "TIMESTAMP"}
					parameterValue := map[string]any{"value": example.Wire}
					if op == dal.In {
						input = []string{example.Epoch}
						canonical = []any{example.Epoch}
						parameterType = map[string]any{"type": "ARRAY", "arrayType": map[string]any{"type": "TIMESTAMP"}}
						parameterValue = map[string]any{"arrayValues": []any{map[string]any{"value": example.Wire}}}
					}
					query := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "", nil))).Limit(1).
						Where(dal.NewComparison(dal.NewFieldRef("", "ts"), op, dal.Constant{Value: input})).
						SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "ts")})
					plan, err := Compile(profile, query)
					if err != nil {
						t.Fatal(err)
					}
					if len(plan.Parameters) != 1 || !reflect.DeepEqual(plan.Parameters[0].Value, canonical) {
						t.Fatalf("canonical parameter drift: %#v", plan.Parameters)
					}
					before, err := json.Marshal(plan)
					if err != nil {
						t.Fatal(err)
					}
					request, err := queryRequest(plan, Execution{MaximumBytesBilled: "1000"}, DefaultBounds(), "EU", dry)
					if err != nil {
						t.Fatal(err)
					}
					serialized, err := json.Marshal(request)
					if err != nil {
						t.Fatal(err)
					}
					var envelope map[string]json.RawMessage
					if err := json.Unmarshal(serialized, &envelope); err != nil {
						t.Fatal(err)
					}
					var actual any
					if err := json.Unmarshal(envelope["queryParameters"], &actual); err != nil {
						t.Fatal(err)
					}
					want := []any{map[string]any{"name": "p0", "parameterType": parameterType, "parameterValue": parameterValue}}
					if !reflect.DeepEqual(actual, want) {
						t.Errorf("parameters %s; want %#v", envelope["queryParameters"], want)
					}
					after, err := json.Marshal(plan)
					if err != nil || string(before) != string(after) {
						t.Fatal("request serialization mutated canonical plan or digest")
					}
				})
			}
		}
	}
}

func TestTimestampRequestNullAndEmpty(t *testing.T) {
	for _, dry := range []bool{true, false} {
		plan := ReadPlan{SQL: "internally compiled", Parameters: []Parameter{{"p0", "TIMESTAMP", nil}, {"p1", "ARRAY<TIMESTAMP>", []any{}}}}
		request, err := queryRequest(plan, Execution{MaximumBytesBilled: "1"}, DefaultBounds(), "EU", dry)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(request.QueryParameters)
		if err != nil {
			t.Fatal(err)
		}
		want := `[{"name":"p0","parameterType":{"type":"TIMESTAMP"},"parameterValue":{"value":null}},{"name":"p1","parameterType":{"arrayType":{"type":"TIMESTAMP"},"type":"ARRAY"},"parameterValue":{"arrayValues":[]}}]`
		if string(raw) != want {
			t.Fatalf("lost typed null/empty timestamp parameters: %s", raw)
		}
	}
}

func TestTimestampRequestRejectsInvalidValues(t *testing.T) {
	profile := testProfile(t)
	profile.Schema = []Field{{Name: "ts", Type: "TIMESTAMP", Mode: "NULLABLE"}}
	for _, value := range []string{"-62135596800000001", "253402300800000000", "1.5", "1\n", "9223372036854775808"} {
		for _, op := range []dal.Operator{dal.Equal, dal.In} {
			var input any = value
			if op == dal.In {
				input = []string{value}
			}
			query := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "", nil))).Limit(1).
				Where(dal.NewComparison(dal.NewFieldRef("", "ts"), op, dal.Constant{Value: input})).
				SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "ts")})
			if _, err := Compile(profile, query); errorCode(err) != "unsupported_value" {
				t.Fatalf("compiler accepted invalid timestamp %q/%s: %v", value, op, err)
			}
			parameter := Parameter{"p0", "TIMESTAMP", value}
			if op == dal.In {
				parameter = Parameter{"p0", "ARRAY<TIMESTAMP>", []any{value}}
			}
			for _, dry := range []bool{true, false} {
				if _, err := queryRequest(ReadPlan{Parameters: []Parameter{parameter}}, Execution{MaximumBytesBilled: "1"}, DefaultBounds(), "EU", dry); errorCode(err) != "unsupported_value" {
					t.Fatalf("request accepted invalid timestamp %q/%s: %v", value, op, err)
				}
			}
		}
	}
}
