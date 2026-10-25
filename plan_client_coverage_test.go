package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPlan_SourceDigest_Errors(t *testing.T) {
	t.Parallel()

	p := testProfile(t)

	// Valid digest
	d, err := sourceDigest(p)
	if err != nil || d == "" {
		t.Fatalf("expected valid digest, got %q, err: %v", d, err)
	}

	// Invalid UTF-8 in strings
	badUTF8 := p
	badUTF8.SourceID = "\xff\xff"
	if _, err := sourceDigest(badUTF8); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad utf8, got %v", err)
	}

	// String length > 4096
	tooLong := p
	tooLong.SourceID = strings.Repeat("a", 4097)
	if _, err := sourceDigest(tooLong); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for long string, got %v", err)
	}

	// Version != 1
	badVer := p
	badVer.Version = 2
	if _, err := sourceDigest(badVer); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for Version != 1, got %v", err)
	}

	// LogicalCollection invalid
	badColl := p
	badColl.LogicalCollection = "123bad"
	if _, err := sourceDigest(badColl); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad collection name, got %v", err)
	}

	// Use not connection-test or admitted
	badUse := p
	badUse.Use = "invalid-use"
	if _, err := sourceDigest(badUse); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad Use, got %v", err)
	}

	// Use == "connection-test"
	connUse := p
	connUse.Use = "connection-test"
	if _, err := sourceDigest(connUse); err != nil {
		t.Fatalf("expected success for connection-test, got %v", err)
	}

	// validateFields error
	badFields := p
	badFields.Schema = []Field{{Name: "123bad_field", Type: "STRING"}}
	if _, err := sourceDigest(badFields); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad fields, got %v", err)
	}
}

func TestPlan_FieldRef_Branches(t *testing.T) {
	t.Parallel()

	// Nil pointer to FieldRef
	var nilPtr *dal.FieldRef
	if _, err := fieldRef(nilPtr); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for nil *FieldRef, got %v", err)
	}

	// Non-FieldRef expression
	if _, err := fieldRef(dal.Constant{Value: 123}); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for Constant, got %v", err)
	}

	// Source != ""
	if _, err := fieldRef(dal.NewFieldRef("src", "col")); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for non-empty source, got %v", err)
	}

	// IsID == true
	if _, err := fieldRef(dal.DocumentID()); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for ID field, got %v", err)
	}

	// Invalid identifier
	if _, err := fieldRef(dal.NewFieldRef("", "123bad")); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for invalid identifier, got %v", err)
	}

	// Valid pointer to FieldRef
	validRef := dal.NewFieldRef("", "col_name")
	if name, err := fieldRef(&validRef); err != nil || name != "col_name" {
		t.Fatalf("expected col_name, got %q, err: %v", name, err)
	}
}

type nilFromQuery struct {
	dal.StructuredQuery
}

func (nilFromQuery) From() dal.FromSource { return nil }

type dummyHavingQuery struct {
	dal.StructuredQuery
}

func (dummyHavingQuery) Having() dal.Condition {
	return dal.NewComparison(dal.NewFieldRef("", "col"), "=", dal.Constant{Value: "1"})
}

type nilBaseFromSource struct {
	dal.FromSource
}

func (nilBaseFromSource) Base() dal.RecordsetSource {
	var nilColl *dal.CollectionRef
	return nilColl
}

type stringBaseFromSource struct {
	dal.FromSource
}

func (stringBaseFromSource) Base() dal.RecordsetSource {
	return nil
}

type customFromQuery struct {
	dal.StructuredQuery
	from dal.FromSource
}

func (c customFromQuery) From() dal.FromSource { return c.from }

func TestPlan_QueryShape_Branches(t *testing.T) {
	t.Parallel()

	// ValidateQuery nil
	if err := ValidateQuery(nil); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query on nil query, got %v", err)
	}

	base := func() *dal.QueryBuilder {
		return dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "", nil)))
	}
	col := dal.Column{Expression: dal.NewFieldRef("", "col")}

	// Offset != 0
	if _, err := queryShape(base().Offset(1).Limit(10).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for offset != 0, got %v", err)
	}
	// StartFrom != ""
	if _, err := queryShape(base().StartFrom("s").Limit(10).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for startFrom, got %v", err)
	}
	// StartAfter != ""
	if _, err := queryShape(base().StartAfter("s").Limit(10).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for startAfter, got %v", err)
	}
	// GroupBy > 0
	if _, err := queryShape(base().GroupBy(dal.NewFieldRef("", "col")).Limit(10).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for groupBy, got %v", err)
	}
	// Having != nil
	if _, err := queryShape(base().Having(dal.NewComparison(dal.NewFieldRef("", "col"), "=", dal.Constant{Value: "1"})).Limit(10).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for having, got %v", err)
	}
	// IntoRecord != nil
	if _, err := queryShape(base().Limit(10).SelectIntoRecordset()); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for IntoRecord, got %v", err)
	}
	// Limit < 1
	if _, err := queryShape(base().Limit(0).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for limit < 1, got %v", err)
	}
	// Limit > 10000
	if _, err := queryShape(base().Limit(10001).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for limit > 10000, got %v", err)
	}

	// From == nil
	if _, err := queryShape(nilFromQuery{base().Limit(10).SelectColumns(col)}); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for nil From, got %v", err)
	}
	// Joins != 0
	if _, err := queryShape(customFromQuery{base().Limit(10).SelectColumns(col), dal.From(dal.NewCollectionRef("sample", "", nil)).Join(dal.NewJoinedSource(dal.NewCollectionRef("other", "", nil), dal.JoinInner))}); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for joins != 0, got %v", err)
	}
	// Base nil pointer
	if _, err := queryShape(customFromQuery{base().Limit(10).SelectColumns(col), nilBaseFromSource{dal.From(dal.NewCollectionRef("sample", "", nil))}}); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for nil base pointer, got %v", err)
	}
	// Base non-CollectionRef
	if _, err := queryShape(customFromQuery{base().Limit(10).SelectColumns(col), stringBaseFromSource{dal.From(dal.NewCollectionRef("sample", "", nil))}}); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for non-CollectionRef base, got %v", err)
	}

	// Base invalid root attributes
	if _, err := queryShape(dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("123bad", "", nil))).Limit(10).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for invalid name, got %v", err)
	}
	if _, err := queryShape(dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "alias", nil))).Limit(10).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for alias, got %v", err)
	}
	if _, err := queryShape(dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "", record.NewKeyWithID("p", "1")))).Limit(10).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for parent, got %v", err)
	}
	if _, err := queryShape(dal.NewQueryBuilder(dal.From(dal.NewQualifiedRootCollectionRef("sch", "sample", ""))).Limit(10).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for schema, got %v", err)
	}
	if _, err := queryShape(dal.NewQueryBuilder(dal.From(dal.NewDatabaseCollectionRef("db", "", "sample", ""))).Limit(10).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for database, got %v", err)
	}
	if _, err := queryShape(dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "", nil).WithScan(5))).Limit(10).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for scanLimit, got %v", err)
	}
	if _, err := queryShape(dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "", nil).WithScan(0, dal.Ascending(dal.NewFieldRef("", "col"))))).Limit(10).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for scanOrders, got %v", err)
	}

	// Columns len < 1
	if _, err := queryShape(base().Limit(10).SelectColumns()); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for empty columns, got %v", err)
	}
	// Columns len > 128
	manyCols := make([]dal.Column, 129)
	for i := range manyCols {
		manyCols[i] = dal.Column{Expression: dal.NewFieldRef("", "c"+strconv.Itoa(i))}
	}
	if _, err := queryShape(base().Limit(10).SelectColumns(manyCols...)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for >128 columns, got %v", err)
	}

	// OrderBy len > 16
	manyOrders := make([]dal.OrderExpression, 17)
	for i := range manyOrders {
		manyOrders[i] = dal.Ascending(dal.NewFieldRef("", "col"))
	}
	if _, err := queryShape(base().Limit(10).OrderBy(manyOrders...).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for >16 orders, got %v", err)
	}

	// Column with alias
	if _, err := queryShape(base().Limit(10).SelectColumns(dal.Column{Alias: "a", Expression: dal.NewFieldRef("", "col")})); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for alias, got %v", err)
	}

	// Duplicate column name
	if _, err := queryShape(base().Limit(10).SelectColumns(col, col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for duplicate column, got %v", err)
	}

	// OrderBy containing nil
	if _, err := queryShape(base().Limit(10).OrderBy(nil).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for nil order, got %v", err)
	}

	// OrderBy with invalid field expression
	if _, err := queryShape(base().Limit(10).OrderBy(dal.Ascending(dal.Constant{Value: 1})).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for bad order expression, got %v", err)
	}

	// Where error
	if _, err := queryShape(base().Limit(10).Where(dal.NewComparison(dal.Constant{Value: 1}, "=", dal.Constant{Value: 2})).SelectColumns(col)); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query on bad where condition, got %v", err)
	}
}

func TestPlan_Compile_Branches(t *testing.T) {
	t.Parallel()

	p := testProfile(t)
	p.Schema = append(p.Schema,
		Field{Name: "num", Type: "FLOAT", Mode: "NULLABLE"},
		Field{Name: "flag", Type: "BOOLEAN", Mode: "NULLABLE"},
		Field{Name: "rep", Type: "STRING", Mode: "REPEATED"},
		Field{Name: "rec", Type: "RECORD", Mode: "NULLABLE", Fields: []Field{{Name: "x", Type: "INT64", Mode: "NULLABLE"}}},
		Field{Name: "blob", Type: "BYTES", Mode: "NULLABLE"},
	)

	// LogicalCollection mismatch
	badCollQ := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("different_collection", "", nil))).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(p, badCollQ); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query on collection mismatch, got %v", err)
	}

	// Column not in schema
	missingColQ := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "unknown_col")})
	if _, err := Compile(p, missingColQ); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query on missing column, got %v", err)
	}

	// Column with REPEATED mode
	repColQ := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "rep")})
	if _, err := Compile(p, repColQ); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type on repeated column, got %v", err)
	}

	// Column with RECORD (len(Fields) > 0)
	recColQ := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "rec")})
	if _, err := Compile(p, recColQ); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type on record column, got %v", err)
	}

	// OrderBy column not in schema
	badOrderQ := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(10).OrderBy(dal.Ascending(dal.NewFieldRef("", "unknown_col"))).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(p, badOrderQ); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query on missing order column, got %v", err)
	}

	// OrderBy column not ordered (BYTES)
	bytesOrderQ := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(10).OrderBy(dal.Ascending(dal.NewFieldRef("", "blob"))).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(p, bytesOrderQ); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query on non-ordered order column, got %v", err)
	}

	// OrderBy Descending
	descQ := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(10).OrderBy(dal.Descending(dal.NewFieldRef("", "n"))).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	plan, err := Compile(p, descQ)
	if err != nil || len(plan.Order) != 1 || plan.Order[0].Direction != "DESC" {
		t.Fatalf("expected DESC order, got %+v, err: %v", plan, err)
	}
}

func TestPlan_CanonicalAndScalarTypes(t *testing.T) {
	t.Parallel()

	// canonicalType
	if canonicalType("INTEGER") != "INT64" || canonicalType("integer") != "INT64" {
		t.Fatal("expected INT64")
	}
	if canonicalType("FLOAT") != "FLOAT64" || canonicalType("float") != "FLOAT64" {
		t.Fatal("expected FLOAT64")
	}
	if canonicalType("BOOLEAN") != "BOOL" || canonicalType("boolean") != "BOOL" {
		t.Fatal("expected BOOL")
	}
	if canonicalType("string") != "STRING" {
		t.Fatal("expected STRING")
	}

	// scalarType
	for _, typ := range []string{"INT64", "NUMERIC", "BIGNUMERIC", "FLOAT64", "BOOL", "STRING", "BYTES", "DATE", "TIME", "DATETIME", "TIMESTAMP", "JSON"} {
		if !scalarType(typ) {
			t.Fatalf("expected true for %s", typ)
		}
	}
	if scalarType("UNKNOWN") || scalarType("") {
		t.Fatal("expected false for unknown scalar types")
	}

	// ordered
	if !ordered(Field{Type: "INT64"}) || !ordered(Field{Type: "STRING"}) {
		t.Fatal("expected INT64 and STRING to be ordered")
	}
	if ordered(Field{Type: "JSON"}) || ordered(Field{Type: "BYTES"}) || ordered(Field{Type: "BOOL"}) || ordered(Field{Type: "INT64", Mode: "REPEATED"}) || ordered(Field{Type: "INT64", Fields: []Field{{}}}) {
		t.Fatal("expected unordered types to return false")
	}
}

func TestPlan_ParseCondition_Branches(t *testing.T) {
	t.Parallel()

	fields := map[string]Field{
		"num":  {Name: "num", Type: "INT64"},
		"flag": {Name: "flag", Type: "BOOL"},
		"blob": {Name: "blob", Type: "BYTES"},
		"rep":  {Name: "rep", Type: "INT64", Mode: "REPEATED"},
		"rec":  {Name: "rec", Type: "RECORD", Fields: []Field{{}}},
	}

	// Depth / count limits
	var nested dal.Condition = dal.NewComparison(dal.NewFieldRef("", "num"), "=", dal.Constant{Value: "1"})
	for i := 0; i < 9; i++ {
		nested = dal.NewGroupCondition("AND", nested)
	}
	count := 0
	if _, err := parseCondition(nested, 0, &count, fields, nil); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for depth limit, got %v", err)
	}

	// Nil pointers
	count = 0
	var nilGrp *dal.GroupCondition
	if _, err := parseCondition(nilGrp, 0, &count, fields, nil); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for nil GroupCondition, got %v", err)
	}
	var nilNull *dal.IsNullCondition
	if _, err := parseCondition(nilNull, 0, &count, fields, nil); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for nil IsNullCondition, got %v", err)
	}
	var nilCmp *dal.Comparison
	if _, err := parseCondition(nilCmp, 0, &count, fields, nil); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for nil Comparison, got %v", err)
	}

	// GroupCondition invalid operator or empty
	count = 0
	if _, err := parseCondition(dal.NewGroupCondition("XOR", dal.NewComparison(dal.NewFieldRef("", "num"), "=", dal.Constant{Value: "1"})), 0, &count, fields, nil); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for XOR, got %v", err)
	}
	count = 0
	if _, err := parseCondition(dal.NewGroupCondition("AND"), 0, &count, fields, nil); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for empty conditions, got %v", err)
	}
	count = 0
	if _, err := parseCondition(dal.NewGroupCondition("AND", nil), 0, &count, fields, nil); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for nil child, got %v", err)
	}

	// IsNullCondition branches
	count = 0
	// Negated is null
	negNull := dal.NewIsNotNullCondition(dal.NewFieldRef("", "num"))
	pred, err := parseCondition(negNull, 0, &count, fields, nil)
	if err != nil || pred.Op != "IS NOT NULL" {
		t.Fatalf("expected IS NOT NULL, got %+v, err: %v", pred, err)
	}
	// Missing field
	count = 0
	if _, err := parseCondition(dal.NewIsNullCondition(dal.NewFieldRef("", "missing")), 0, &count, fields, nil); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type on missing field, got %v", err)
	}
	// REPEATED field in IsNull
	count = 0
	if _, err := parseCondition(dal.NewIsNullCondition(dal.NewFieldRef("", "rep")), 0, &count, fields, nil); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type on repeated field, got %v", err)
	}

	// Comparison branches
	count = 0
	// Nil right constant pointer
	var nilConst *dal.Constant
	if _, err := parseCondition(dal.Comparison{Left: dal.NewFieldRef("", "num"), Operator: "=", Right: nilConst}, 0, &count, fields, nil); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query on nil constant, got %v", err)
	}
	// Non-constant right
	if _, err := parseCondition(dal.Comparison{Left: dal.NewFieldRef("", "num"), Operator: "=", Right: dal.NewFieldRef("", "other")}, 0, &count, fields, nil); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query on non-constant, got %v", err)
	}
	// Operator == and In aliases
	var params []Parameter
	count = 0
	pEq, err := parseCondition(dal.Comparison{Left: dal.NewFieldRef("", "num"), Operator: "==", Right: dal.Constant{Value: "10"}}, 0, &count, fields, &params)
	if err != nil || pEq.Op != "=" {
		t.Fatalf("expected = op, got %+v, err: %v", pEq, err)
	}
	count = 0
	pIn, err := parseCondition(dal.Comparison{Left: dal.NewFieldRef("", "num"), Operator: "In", Right: dal.Constant{Value: []any{"1", "2"}}}, 0, &count, fields, &params)
	if err != nil || pIn.Op != "IN" {
		t.Fatalf("expected IN op, got %+v, err: %v", pIn, err)
	}

	// IN with non-slice or >1000 items or nil item
	count = 0
	if _, err := parseCondition(dal.Comparison{Left: dal.NewFieldRef("", "num"), Operator: "IN", Right: dal.Constant{Value: "not-a-slice"}}, 0, &count, fields, &params); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query on non-slice IN, got %v", err)
	}
	count = 0
	if _, err := parseCondition(dal.Comparison{Left: dal.NewFieldRef("", "num"), Operator: "IN", Right: dal.Constant{Value: []any{nil}}}, 0, &count, fields, &params); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query on slice with nil item, got %v", err)
	}

	// Field not found or unsupported type in Comparison
	count = 0
	if _, err := parseCondition(dal.Comparison{Left: dal.NewFieldRef("", "missing"), Operator: "=", Right: dal.Constant{Value: "x"}}, 0, &count, fields, &params); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type for missing field, got %v", err)
	}
	count = 0
	if _, err := parseCondition(dal.Comparison{Left: dal.NewFieldRef("", "blob"), Operator: "=", Right: dal.Constant{Value: "x"}}, 0, &count, fields, &params); errorCode(err) != "unsupported_type" {
		t.Fatalf("expected unsupported_type for BYTES field, got %v", err)
	}

	// Inequality on non-ordered field
	count = 0
	if _, err := parseCondition(dal.Comparison{Left: dal.NewFieldRef("", "flag"), Operator: "<", Right: dal.Constant{Value: true}}, 0, &count, fields, &params); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for < on BOOL, got %v", err)
	}

	// Comparison without fields (only validate shape)
	count = 0
	if _, err := parseCondition(dal.Comparison{Left: dal.NewFieldRef("", "any_col"), Operator: "=", Right: dal.Constant{Value: "x"}}, 0, &count, nil, nil); err != nil {
		t.Fatalf("expected shape validation success without fields, got %v", err)
	}
}

func TestPlan_ParameterValue_Branches(t *testing.T) {
	t.Parallel()

	// Non-BOOL field with non-string value
	if _, err := parameterValue(Field{Type: "INT64"}, 123); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for non-string on INT64, got %v", err)
	}

	// BOOL field with non-bool value
	if _, err := parameterValue(Field{Type: "BOOL"}, "true"); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for string on BOOL, got %v", err)
	}

	// BOOL field with true
	if v, err := parameterValue(Field{Type: "BOOL"}, true); err != nil || v != true {
		t.Fatalf("expected true, got %v, err: %v", v, err)
	}

	// BOOL field with false
	if v, err := parameterValue(Field{Type: "BOOL"}, false); err != nil || v != false {
		t.Fatalf("expected false, got %v, err: %v", v, err)
	}

	// NormalizeScalar failure (e.g. invalid date format)
	if _, err := parameterValue(Field{Type: "DATE"}, "not-a-date"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value on bad date, got %v", err)
	}
}

func TestPlan_RenderPlan_And_Predicate_Branches(t *testing.T) {
	t.Parallel()

	p := testProfile(t)

	// renderPlan with invalid column identifier in projection
	badPlan := ReadPlan{
		Projection: []string{"123bad"},
		Limit:      10,
	}
	if _, err := renderPlan(p, badPlan); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on bad column identifier, got %v", err)
	}

	// renderPlan with invalid order
	badOrderPlan := ReadPlan{
		Projection: []string{"n"},
		Order:      []Order{{Column: "n", Direction: "INVALID"}},
		Limit:      10,
	}
	if _, err := renderPlan(p, badOrderPlan); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on bad order direction, got %v", err)
	}

	// renderPredicate: invalid column identifier
	if _, err := renderPredicate(Predicate{Column: "123bad", Op: "IS NULL"}, nil); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on bad column in predicate, got %v", err)
	}

	// renderPredicate: invalid parameter format
	if _, err := renderPredicate(Predicate{Column: "n", Op: "=", Parameter: "badparam"}, nil); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on bad param name, got %v", err)
	}

	// renderPredicate: IN with empty parameter value
	inPred := Predicate{Column: "n", Op: "IN", Parameter: "p0"}
	inParams := []Parameter{{Name: "p0", Type: "ARRAY<INT64>", Value: []any{}}}
	s, err := renderPredicate(inPred, inParams)
	if err != nil || s != "FALSE" {
		t.Fatalf("expected FALSE for empty IN unnest, got %q, err: %v", s, err)
	}

	// renderPredicate: unknown op
	if _, err := renderPredicate(Predicate{Column: "n", Op: "LIKE", Parameter: "p0"}, inParams); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on unknown predicate op, got %v", err)
	}

	// equalJSON
	if !equalJSON(map[string]int{"a": 1}, map[string]int{"a": 1}) {
		t.Fatal("expected equalJSON true")
	}
	if equalJSON(map[string]int{"a": 1}, map[string]int{"a": 2}) {
		t.Fatal("expected equalJSON false")
	}
}

func TestClient_NewClient_Branches(t *testing.T) {
	t.Parallel()

	p := testProfile(t)
	prov := &testProvider{identity: Identity{Principal{"workload", "sub", "1"}, time.Now().Add(time.Hour), true, false}}
	trans := rt(func(*http.Request) (*http.Response, error) { return response("{}"), nil })
	ledg := NewMemoryLedger()
	prep := func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil }

	// Missing fields
	if _, err := NewClient(Config{}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for empty config, got %v", err)
	}
	if _, err := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Prepare: prep, Profiles: nil}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for nil profiles, got %v", err)
	}

	// Nil clock defaults to realClock
	c, err := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Prepare: prep, Profiles: []SourceProfile{p}})
	if err != nil || c.clock == nil {
		t.Fatalf("expected success with realClock, got %v", err)
	}

	// Duplicate profiles
	if _, err := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Prepare: prep, Profiles: []SourceProfile{p, p}}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for duplicate profiles, got %v", err)
	}

	// Invalid profile
	badP := p
	badP.Version = 99
	if _, err := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Prepare: prep, Profiles: []SourceProfile{badP}}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad profile, got %v", err)
	}
}

// Not parallel: modifies jsonMarshal seam.
func TestClient_NewClient_JSONCopyError(t *testing.T) {
	orig := jsonMarshal
	defer func() { jsonMarshal = orig }()

	p := testProfile(t)
	prov := &testProvider{identity: Identity{Principal{"workload", "sub", "1"}, time.Now().Add(time.Hour), true, false}}
	trans := rt(func(*http.Request) (*http.Response, error) { return response("{}"), nil })
	ledg := NewMemoryLedger()
	prep := func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil }

	jsonMarshal = func(v any) ([]byte, error) {
		return nil, errors.New("marshal fail")
	}
	if _, err := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Prepare: prep, Profiles: []SourceProfile{p}}); err == nil {
		t.Fatal("expected error when jsonMarshal fails in NewClient")
	}
}

func TestClient_ValidatePlan_Branches(t *testing.T) {
	t.Parallel()

	p := testProfile(t)
	p.Schema = append(p.Schema,
		Field{Name: "int_col", Type: "INT64", Mode: "NULLABLE"},
		Field{Name: "bool_col", Type: "BOOL", Mode: "NULLABLE"},
		Field{Name: "bytes_col", Type: "BYTES", Mode: "NULLABLE"},
		Field{Name: "rep_col", Type: "INT64", Mode: "REPEATED"},
		Field{Name: "rec_col", Type: "RECORD", Mode: "NULLABLE", Fields: []Field{{Name: "sub", Type: "INT64", Mode: "NULLABLE"}}},
	)
	prov := &testProvider{identity: Identity{Principal{"workload", "sub", "1"}, time.Now().Add(time.Hour), true, false}}
	trans := rt(func(*http.Request) (*http.Response, error) { return response("{}"), nil })
	ledg := NewMemoryLedger()
	prep := func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil }

	c, err := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Prepare: prep, Profiles: []SourceProfile{p}})
	if err != nil {
		t.Fatal(err)
	}

	validPlan, err := Compile(p, dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "int_col")}))
	if err != nil {
		t.Fatal(err)
	}

	// Unknown source digest
	badDigestPlan := validPlan
	badDigestPlan.SourceDigest = "unknown-digest"
	if _, _, err := c.validatePlan(badDigestPlan); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for unknown digest, got %v", err)
	}

	// Version != 1
	badVerPlan := validPlan
	badVerPlan.Version = 2
	if _, _, err := c.validatePlan(badVerPlan); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for version != 1, got %v", err)
	}

	// Limit bounds
	badLim1 := validPlan
	badLim1.Limit = 0
	if _, _, err := c.validatePlan(badLim1); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for limit < 1, got %v", err)
	}
	badLim2 := validPlan
	badLim2.Limit = 10001
	if _, _, err := c.validatePlan(badLim2); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for limit > 10000, got %v", err)
	}

	// Empty projection
	badProj := validPlan
	badProj.Projection = []string{}
	if _, _, err := c.validatePlan(badProj); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for empty projection, got %v", err)
	}

	// Raw json > 256KB
	hugePlan := validPlan
	hugePlan.Parameters = []Parameter{{Name: "p0", Type: "STRING", Value: strings.Repeat("x", 300<<10)}}
	if _, _, err := c.validatePlan(hugePlan); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for plan > 256KB, got %v", err)
	}

	// Projection: unknown field, duplicate field, non-scalar/repeated field
	badColProj := validPlan
	badColProj.Projection = []string{"missing_col"}
	if _, _, err := c.validatePlan(badColProj); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for missing projection col, got %v", err)
	}
	dupProj := validPlan
	dupProj.Projection = []string{"int_col", "int_col"}
	if _, _, err := c.validatePlan(dupProj); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for duplicate projection col, got %v", err)
	}
	repProj := validPlan
	repProj.Projection = []string{"rep_col"}
	if _, _, err := c.validatePlan(repProj); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for repeated projection col, got %v", err)
	}

	// Order: unknown field, !ordered(f), invalid direction
	badOrderPlan1 := validPlan
	badOrderPlan1.Order = []Order{{Column: "missing", Direction: "ASC"}}
	if _, _, err := c.validatePlan(badOrderPlan1); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for missing order col, got %v", err)
	}
	badOrderPlan2 := validPlan
	badOrderPlan2.Order = []Order{{Column: "bytes_col", Direction: "ASC"}}
	if _, _, err := c.validatePlan(badOrderPlan2); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for unordered order col, got %v", err)
	}
	badOrderPlan3 := validPlan
	badOrderPlan3.Order = []Order{{Column: "int_col", Direction: "SIDEWAYS"}}
	if _, _, err := c.validatePlan(badOrderPlan3); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for bad order direction, got %v", err)
	}

	// Parameter: name != "p%d"
	badParamName := validPlan
	badParamName.Parameters = []Parameter{{Name: "wrong_name", Type: "INT64", Value: "1"}}
	if _, _, err := c.validatePlan(badParamName); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad parameter name, got %v", err)
	}

	// Predicate validation branches
	// AND/OR with non-empty column or parameter
	badAndPred := validPlan
	badAndPred.Where = &Predicate{Op: "AND", Column: "int_col", Children: []Predicate{}}
	if _, _, err := c.validatePlan(badAndPred); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for AND with column, got %v", err)
	}

	// IS NULL with parameter != ""
	badNullPred := validPlan
	badNullPred.Where = &Predicate{Op: "IS NULL", Column: "int_col", Parameter: "p0"}
	if _, _, err := c.validatePlan(badNullPred); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for IS NULL with parameter, got %v", err)
	}

	// BYTES field in predicate
	badBytesPred := validPlan
	badBytesPred.Where = &Predicate{Op: "=", Column: "bytes_col", Parameter: "p0"}
	badBytesPred.Parameters = []Parameter{{Name: "p0", Type: "BYTES", Value: "abc"}}
	if _, _, err := c.validatePlan(badBytesPred); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for BYTES in predicate, got %v", err)
	}

	// Inequality on non-ordered field (BOOL)
	badIneqPred := validPlan
	badIneqPred.Where = &Predicate{Op: "<", Column: "bool_col", Parameter: "p0"}
	badIneqPred.Parameters = []Parameter{{Name: "p0", Type: "BOOL", Value: true}}
	if _, _, err := c.validatePlan(badIneqPred); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for < on BOOL, got %v", err)
	}

	// Unknown parameter in predicate
	badMissingParam := validPlan
	badMissingParam.Where = &Predicate{Op: "=", Column: "int_col", Parameter: "p99"}
	if _, _, err := c.validatePlan(badMissingParam); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for missing param in predicate, got %v", err)
	}

	// IN predicate validation: wrong type, not slice, nil element
	badInType := validPlan
	badInType.Where = &Predicate{Op: "IN", Column: "int_col", Parameter: "p0"}
	badInType.Parameters = []Parameter{{Name: "p0", Type: "INT64", Value: []any{"1"}}}
	if _, _, err := c.validatePlan(badInType); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for IN with non-ARRAY type, got %v", err)
	}
	badInSlice := validPlan
	badInSlice.Where = &Predicate{Op: "IN", Column: "int_col", Parameter: "p0"}
	badInSlice.Parameters = []Parameter{{Name: "p0", Type: "ARRAY<INT64>", Value: "not-a-slice"}}
	if _, _, err := c.validatePlan(badInSlice); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for IN with non-slice value, got %v", err)
	}
	badInNilElem := validPlan
	badInNilElem.Where = &Predicate{Op: "IN", Column: "int_col", Parameter: "p0"}
	badInNilElem.Parameters = []Parameter{{Name: "p0", Type: "ARRAY<INT64>", Value: []any{nil}}}
	if _, _, err := c.validatePlan(badInNilElem); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for IN with nil element, got %v", err)
	}

	// Scalar parameter validation: wrong type, nil value
	badScalarType := validPlan
	badScalarType.Where = &Predicate{Op: "=", Column: "int_col", Parameter: "p0"}
	badScalarType.Parameters = []Parameter{{Name: "p0", Type: "STRING", Value: "1"}}
	if _, _, err := c.validatePlan(badScalarType); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for scalar with wrong type, got %v", err)
	}
	badScalarNil := validPlan
	badScalarNil.Where = &Predicate{Op: "=", Column: "int_col", Parameter: "p0"}
	badScalarNil.Parameters = []Parameter{{Name: "p0", Type: "INT64", Value: nil}}
	if _, _, err := c.validatePlan(badScalarNil); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for scalar with nil value, got %v", err)
	}

	// Unused parameter
	unusedParamPlan := validPlan
	unusedParamPlan.Parameters = []Parameter{{Name: "p0", Type: "INT64", Value: "1"}}
	if _, _, err := c.validatePlan(unusedParamPlan); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for unused parameter, got %v", err)
	}

	// Mismatched rendered SQL
	mismatchedSQL := validPlan
	mismatchedSQL.SQL = "SELECT * FROM `bad`"
	if _, _, err := c.validatePlan(mismatchedSQL); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for mismatched SQL, got %v", err)
	}

	// Mismatched digest
	mismatchedDigest := validPlan
	mismatchedDigest.Digest = "bad-digest"
	if _, _, err := c.validatePlan(mismatchedDigest); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed for bad digest, got %v", err)
	}
}

func TestClient_Reauthorize_Branches(t *testing.T) {
	t.Parallel()

	p := testProfile(t)
	prov := &testProvider{identity: Identity{Principal{"workload", "sub", "1"}, time.Now().Add(time.Hour), true, false}}
	trans := rt(func(*http.Request) (*http.Response, error) { return response("{}"), nil })
	ledg := NewMemoryLedger()

	validPlan, err := Compile(p, dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}))
	if err != nil {
		t.Fatal(err)
	}

	// Prepare returns error
	cErr, _ := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) {
			return ReadPlan{}, "", errors.New("auth fail")
		},
	})
	if err := cErr.reauthorize(context.Background(), validPlan, "policy"); errorCode(err) != "auth_required" {
		t.Fatalf("expected auth_required on prepare error, got %v", err)
	}

	// Prepare returns plan with digest mismatch
	otherPlan, _ := Compile(p, dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(20).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}))
	cDiff, _ := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) {
			return otherPlan, "policy", nil
		},
	})
	if err := cDiff.reauthorize(context.Background(), validPlan, "policy"); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed on digest mismatch, got %v", err)
	}

	// Prepare returns policy mismatch
	cPolicy, _ := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) {
			return validPlan, "other_policy", nil
		},
	})
	if err := cPolicy.reauthorize(context.Background(), validPlan, "policy"); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed on policy mismatch, got %v", err)
	}

	// Success
	cOk, _ := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) {
			return validPlan, "policy", nil
		},
	})
	if err := cOk.reauthorize(context.Background(), validPlan, "policy"); err != nil {
		t.Fatalf("unexpected error on valid reauthorize: %v", err)
	}
}

func TestClient_Preview_And_Approve_Branches(t *testing.T) {
	t.Parallel()

	p := testProfile(t)
	prov := &testProvider{identity: Identity{Principal{"workload", "sub", "1"}, time.Now().Add(time.Hour), true, false}}
	trans := rt(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/queries") {
			return response(`{"totalBytesProcessed":"100"}`), nil
		}
		return metadataResponse(req)
	})
	ledg := NewMemoryLedger()

	validPlan, err := Compile(p, dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}))
	if err != nil {
		t.Fatal(err)
	}

	c, err := NewClient(Config{
		Provider: prov, Transport: trans, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) {
			return validPlan, "policy", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	exec := Execution{
		JobProject:         "job-project",
		Principal:          Principal{Kind: "workload", Subject: "sub", Generation: "1"},
		MaximumBytesBilled: "1000",
		SessionBudgetBytes: "2000",
	}
	bounds := Bounds{
		PageSize:           50,
		MaxRows:            100,
		MaxPages:           10,
		ResponseBytes:      100000,
		TotalResponseBytes: 1000000,
		WallMs:             5000,
		HTTPMs:             1000,
		Concurrency:        1,
	}

	// Preview: invalid plan
	badPlan := validPlan
	badPlan.Version = 2
	if _, err := c.Preview(context.Background(), badPlan, exec, bounds); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad plan, got %v", err)
	}

	// Preview: invalid bounds
	badBounds := bounds
	badBounds.Concurrency = 2
	if _, err := c.Preview(context.Background(), validPlan, exec, badBounds); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad bounds, got %v", err)
	}

	// Preview: invalid MaximumBytesBilled
	badExec1 := exec
	badExec1.MaximumBytesBilled = "-5"
	if _, err := c.Preview(context.Background(), validPlan, badExec1, bounds); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for negative max bytes, got %v", err)
	}

	// Preview: session budget < max bytes
	badExec2 := exec
	badExec2.SessionBudgetBytes = "500"
	if _, err := c.Preview(context.Background(), validPlan, badExec2, bounds); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for budget < cap, got %v", err)
	}

	// Preview: invalid JobProject
	badExec3 := exec
	badExec3.JobProject = "bad_project_ID"
	if _, err := c.Preview(context.Background(), validPlan, badExec3, bounds); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad JobProject, got %v", err)
	}

	// Preview: invalid Principal
	badExec4 := exec
	badExec4.Principal = Principal{Kind: "bad", Subject: ""}
	if _, err := c.Preview(context.Background(), validPlan, badExec4, bounds); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad Principal, got %v", err)
	}

	// Preview success
	prev, err := c.Preview(context.Background(), validPlan, exec, bounds)
	if err != nil {
		t.Fatalf("unexpected error on Preview: %v", err)
	}
	if prev.ApprovalDigest == "" || prev.Nonce == "" {
		t.Fatalf("expected non-empty preview digest/nonce, got %+v", prev)
	}

	// Approve: empty digest or digest mismatch
	if _, err := c.Approve(prev, ""); errorCode(err) != "approval_required" {
		t.Fatalf("expected approval_required for empty digest, got %v", err)
	}
	if _, err := c.Approve(prev, "wrong-digest"); errorCode(err) != "approval_required" {
		t.Fatalf("expected approval_required for wrong digest, got %v", err)
	}

	// Approve: nonce not found
	unknownNoncePrev := prev
	unknownNoncePrev.Nonce = "missing-nonce"
	if _, err := c.Approve(unknownNoncePrev, unknownNoncePrev.ApprovalDigest); errorCode(err) != "approval_required" {
		t.Fatalf("expected approval_required for missing nonce, got %v", err)
	}

	// Approve: preview modified
	tamperedPrev := prev
	tamperedPrev.EstimatedBytes = "9999"
	if _, err := c.Approve(tamperedPrev, tamperedPrev.ApprovalDigest); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed for tampered preview, got %v", err)
	}

	// Approve: success
	appr, err := c.Approve(prev, prev.ApprovalDigest)
	if err != nil || appr.nonce != prev.Nonce {
		t.Fatalf("expected approval success, got %+v, err: %v", appr, err)
	}
}

func TestClient_QueryRequest_And_SDKParameterValue_Branches(t *testing.T) {
	t.Parallel()

	exec := Execution{MaximumBytesBilled: "1000"}
	bounds := Bounds{WallMs: 1000, PageSize: 50}

	// queryRequest: invalid MaximumBytesBilled
	badExec := Execution{MaximumBytesBilled: "-1"}
	if _, err := queryRequest(ReadPlan{}, badExec, bounds, "EU", false); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for negative bytes, got %v", err)
	}

	// queryRequest: ARRAY parameter with non-slice Value
	badArrayPlan := ReadPlan{
		Parameters: []Parameter{{Name: "p0", Type: "ARRAY<INT64>", Value: "not-a-slice"}},
	}
	if _, err := queryRequest(badArrayPlan, exec, bounds, "EU", false); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for non-slice array value, got %v", err)
	}

	// sdkParameterValue: nil
	pv, err := sdkParameterValue("INT64", nil)
	if err != nil || len(pv.NullFields) == 0 {
		t.Fatalf("expected NullFields on nil, got %+v, err: %v", pv, err)
	}

	// sdkParameterValue: bool
	pvTrue, err := sdkParameterValue("BOOL", true)
	if err != nil || pvTrue.Value != "true" {
		t.Fatalf("expected true, got %+v, err: %v", pvTrue, err)
	}
	pvFalse, err := sdkParameterValue("BOOL", false)
	if err != nil || pvFalse.Value != "false" {
		t.Fatalf("expected false, got %+v, err: %v", pvFalse, err)
	}

	// sdkParameterValue: non-string/non-bool
	if _, err := sdkParameterValue("INT64", 123); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for int value, got %v", err)
	}

	// sdkParameterValue: TIMESTAMP valid
	pvTime, err := sdkParameterValue("TIMESTAMP", "1700000000000000")
	if err != nil || !strings.Contains(pvTime.Value, "UTC") {
		t.Fatalf("expected UTC timestamp string, got %+v, err: %v", pvTime, err)
	}

	// sdkParameterValue: TIMESTAMP invalid format
	if _, err := sdkParameterValue("TIMESTAMP", "not-a-ts"); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value on bad timestamp, got %v", err)
	}
}

func TestClient_Estimate_Branches(t *testing.T) {
	t.Parallel()

	p := testProfile(t)
	prov := &testProvider{identity: Identity{Principal{"workload", "sub", "1"}, time.Now().Add(time.Hour), true, false}}
	ledg := NewMemoryLedger()

	validPlan, err := Compile(p, dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}))
	if err != nil {
		t.Fatal(err)
	}

	exec := Execution{
		JobProject:         "job-project",
		Principal:          Principal{Kind: "workload", Subject: "sub", Generation: "1"},
		MaximumBytesBilled: "1000",
		SessionBudgetBytes: "2000",
	}
	bounds := Bounds{
		PageSize:           50,
		MaxRows:            100,
		MaxPages:           10,
		ResponseBytes:      100000,
		TotalResponseBytes: 1000000,
		WallMs:             5000,
		HTTPMs:             1000,
		Concurrency:        1,
	}

	// Remote returned non-object JSON
	cNonObj, _ := NewClient(Config{
		Provider: prov, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Transport: rt(func(*http.Request) (*http.Response, error) { return response(`"not an object"`), nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return validPlan, "", nil },
	})
	scope := newPreviewScope(bounds, time.Now())
	if _, err := cNonObj.estimate(context.Background(), validPlan, exec, "EU", scope); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire for non-object, got %v", err)
	}

	// Remote returned errors
	cErrs, _ := NewClient(Config{
		Provider: prov, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Transport: rt(func(*http.Request) (*http.Response, error) {
			return response(`{"jobComplete":true,"errors":[{"message":"something failed"}]}`), nil
		}),
		Prepare: func(context.Context) (ReadPlan, string, error) { return validPlan, "", nil },
	})
	if _, err := cErrs.estimate(context.Background(), validPlan, exec, "EU", scope); errorCode(err) != "remote_failed" {
		t.Fatalf("expected remote_failed for error response, got %v", err)
	}

	// totalBytesProcessed missing
	cMiss, _ := NewClient(Config{
		Provider: prov, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Transport: rt(func(*http.Request) (*http.Response, error) { return response(`{}`), nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return validPlan, "", nil },
	})
	if _, err := cMiss.estimate(context.Background(), validPlan, exec, "EU", scope); errorCode(err) != "estimate_missing" {
		t.Fatalf("expected estimate_missing when bytes missing, got %v", err)
	}

	// totalBytesProcessed > MaximumBytesBilled (cap_exceeded)
	cExceed, _ := NewClient(Config{
		Provider: prov, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Transport: rt(func(*http.Request) (*http.Response, error) { return response(`{"totalBytesProcessed":"5000"}`), nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return validPlan, "", nil },
	})
	if _, err := cExceed.estimate(context.Background(), validPlan, exec, "EU", scope); errorCode(err) != "cap_exceeded" {
		t.Fatalf("expected cap_exceeded when estimate > cap, got %v", err)
	}
}

type testCustomCondition struct{}

func (testCustomCondition) String() string { return "custom" }

func TestPlan_Exhaustive_Branches(t *testing.T) {
	t.Parallel()

	p := testProfile(t)
	prov := &testProvider{identity: Identity{Principal{"workload", "sub", "1"}, time.Now().Add(time.Hour), true, false}}
	trans := rt(func(*http.Request) (*http.Response, error) { return response(`{}`), nil })
	ledg := NewMemoryLedger()

	validPlan, err := Compile(p, dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}))
	if err != nil {
		t.Fatal(err)
	}
	exec := Execution{
		JobProject:         "job-project",
		Principal:          Principal{Kind: "workload", Subject: "sub", Generation: "1"},
		MaximumBytesBilled: "1000",
		SessionBudgetBytes: "2000",
	}
	bounds := Bounds{
		PageSize:           50,
		MaxRows:            100,
		MaxPages:           10,
		ResponseBytes:      100000,
		TotalResponseBytes: 1000000,
		WallMs:             5000,
		HTTPMs:             1000,
		Concurrency:        1,
	}

	c, _ := NewClient(Config{
		Provider: prov, Transport: trans, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) { return validPlan, "policy", nil },
	})

	// *dal.CollectionRef non-nil in queryShape / ValidateQuery
	colRef := dal.NewCollectionRef(p.LogicalCollection, "", nil)
	qColPtr := dal.NewQueryBuilder(dal.From(&colRef)).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if err := ValidateQuery(qColPtr); err != nil {
		t.Fatal(err)
	}

	// Compile with bad profile (sourceDigest fails)
	badP := p
	badP.SourceProject = "invalid project!"
	if _, err := Compile(badP, qColPtr); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad profile, got %v", err)
	}

	// *dal.GroupCondition non-nil in parseCondition
	leafComp := dal.NewComparison(dal.NewFieldRef("", "n"), dal.Equal, dal.Constant{Value: "42"})
	grp := dal.NewGroupCondition(dal.And, leafComp)
	qGrp := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Where(&grp).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(p, qGrp); err != nil {
		t.Fatal(err)
	}

	// *dal.IsNullCondition non-nil in parseCondition
	nullCond := dal.NewIsNullCondition(dal.NewFieldRef("", "n"))
	qNull := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Where(&nullCond).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(p, qNull); err != nil {
		t.Fatal(err)
	}

	// IsNullCondition with invalid operand expression
	badNull := dal.NewIsNullCondition(dal.Constant{Value: "bad"})
	qBadNull := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Where(badNull).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(p, qBadNull); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query, got %v", err)
	}

	// *dal.Comparison non-nil in parseCondition
	comp := dal.NewComparison(dal.NewFieldRef("", "n"), dal.Equal, dal.Constant{Value: "42"})
	qComp := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Where(&comp).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(p, qComp); err != nil {
		t.Fatal(err)
	}

	// Comparison with *dal.Constant non-nil
	constVal := dal.Constant{Value: "42"}
	compConst := dal.Comparison{Left: dal.NewFieldRef("", "n"), Operator: "=", Right: &constVal}
	qConst := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Where(compConst).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(p, qConst); err != nil {
		t.Fatal(err)
	}

	// Custom condition type (default branch of parseCondition)
	qCust := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Where(testCustomCondition{}).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(p, qCust); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query, got %v", err)
	}

	// renderPlan with invalid where predicate
	badWherePlan := ReadPlan{
		Projection: []string{"n"},
		Limit:      10,
		Where:      &Predicate{Column: "123bad"},
	}
	if _, err := renderPlan(p, badWherePlan); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad where predicate, got %v", err)
	}

	// renderPredicate branches
	if _, err := renderPredicate(Predicate{Op: "AND", Children: []Predicate{{Column: "123bad"}}}, nil); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input, got %v", err)
	}
	if s, err := renderPredicate(Predicate{Op: "AND", Children: []Predicate{{Op: "IS NULL", Column: "n"}}}, nil); err != nil || s != "(`n` IS NULL)" {
		t.Fatalf("expected (`n` IS NULL), got %s, err: %v", s, err)
	}
	if s, err := renderPredicate(Predicate{Op: "IS NULL", Column: "n"}, nil); err != nil || s != "`n` IS NULL" {
		t.Fatalf("expected `n` IS NULL, got %s, err: %v", s, err)
	}
	if s, err := renderPredicate(Predicate{Op: "IS NOT NULL", Column: "n"}, nil); err != nil || s != "`n` IS NOT NULL" {
		t.Fatalf("expected `n` IS NOT NULL, got %s, err: %v", s, err)
	}
	if s, err := renderPredicate(Predicate{Op: "IN", Column: "n", Parameter: "p0"}, []Parameter{{Name: "p0", Value: []any{}}}); err != nil || s != "FALSE" {
		t.Fatalf("expected FALSE for empty IN, got %s, err: %v", s, err)
	}
	if s, err := renderPredicate(Predicate{Op: "IN", Column: "n", Parameter: "p0"}, []Parameter{{Name: "p0", Value: []any{"1"}}}); err != nil || s != "`n` IN UNNEST(@p0)" {
		t.Fatalf("expected UNNEST for non-empty IN, got %s, err: %v", s, err)
	}
	if _, err := renderPredicate(Predicate{Op: "INVALID_OP", Column: "n", Parameter: "p0"}, nil); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input, got %v", err)
	}

	// validatePlan: NaN parameter value (jsonCopy fails)
	planNaN := validPlan
	planNaN.Parameters = []Parameter{{Name: "p0", Value: math.NaN()}}
	if _, _, err := c.validatePlan(planNaN); err == nil {
		t.Fatal("expected error for NaN parameter")
	}

	// validatePlan: depth >= 8
	pred8 := &Predicate{Op: "AND", Children: []Predicate{}}
	curr := pred8
	for i := 0; i < 8; i++ {
		curr.Children = []Predicate{{Op: "AND", Children: []Predicate{}}}
		curr = &curr.Children[0]
	}
	curr.Children = []Predicate{{Op: "IS NULL", Column: "n"}}
	plan8 := validPlan
	plan8.Where = pred8
	if _, _, err := c.validatePlan(plan8); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for depth >= 8, got %v", err)
	}

	// validatePlan: visit child error
	planChildErr := validPlan
	planChildErr.Where = &Predicate{Op: "AND", Children: []Predicate{{Op: "BAD"}}}
	if _, _, err := c.validatePlan(planChildErr); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for child error, got %v", err)
	}

	// validatePlan: leaf predicate has children
	planLeafChild := validPlan
	planLeafChild.Where = &Predicate{Op: "IS NULL", Column: "n", Children: []Predicate{{Op: "IS NULL"}}}
	if _, _, err := c.validatePlan(planLeafChild); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for leaf with children, got %v", err)
	}

	// validatePlan: IS NULL valid leaf
	planIsNull := validPlan
	planIsNull.Where = &Predicate{Op: "IS NULL", Column: "n"}
	planIsNull.SQL, _ = renderPlan(p, planIsNull)
	planIsNull.Digest, _ = digestObject("ReadPlan", planIsNull)
	if _, _, err := c.validatePlan(planIsNull); err != nil {
		t.Fatal(err)
	}

	// validatePlan: unsupported op
	planBadOp := validPlan
	planBadOp.Parameters = []Parameter{{Name: "p0", Type: "INT64", Value: "42"}}
	planBadOp.Where = &Predicate{Op: "LIKE", Column: "n", Parameter: "p0"}
	if _, _, err := c.validatePlan(planBadOp); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for LIKE, got %v", err)
	}

	// validatePlan: IN parameter value check fail
	planBadInVal := validPlan
	planBadInVal.Parameters = []Parameter{{Name: "p0", Type: "ARRAY<INT64>", Value: []any{"not-an-int"}}}
	planBadInVal.Where = &Predicate{Op: "IN", Column: "n", Parameter: "p0"}
	if _, _, err := c.validatePlan(planBadInVal); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad IN val, got %v", err)
	}

	// validatePlan: leaf comparison parameter value check fail
	planBadLeafVal := validPlan
	planBadLeafVal.Parameters = []Parameter{{Name: "p0", Type: "INT64", Value: "not-an-int"}}
	planBadLeafVal.Where = &Predicate{Op: "=", Column: "n", Parameter: "p0"}
	if _, _, err := c.validatePlan(planBadLeafVal); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad leaf val, got %v", err)
	}

	// validatePlan: valid leaf comparison
	planGoodLeaf := validPlan
	planGoodLeaf.Parameters = []Parameter{{Name: "p0", Type: "INT64", Value: "42"}}
	planGoodLeaf.Where = &Predicate{Op: "=", Column: "n", Parameter: "p0"}
	planGoodLeaf.SQL, _ = renderPlan(p, planGoodLeaf)
	planGoodLeaf.Digest, _ = digestObject("ReadPlan", planGoodLeaf)
	if _, _, err := c.validatePlan(planGoodLeaf); err != nil {
		t.Fatal(err)
	}

	// reauthorize with canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.reauthorize(canceledCtx, validPlan, "policy"); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on canceled ctx, got %v", err)
	}

	// reauthorize when prepare returns invalid plan
	cBadPrep, _ := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) {
			bad := validPlan
			bad.Limit = 0
			return bad, "policy", nil
		},
	})
	if err := cBadPrep.reauthorize(context.Background(), validPlan, "policy"); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input when prepare returns invalid plan, got %v", err)
	}

	// Preview with canceled context
	if _, err := c.Preview(canceledCtx, validPlan, exec, bounds); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on canceled ctx, got %v", err)
	}

	// Preview when prepare returns error
	cPrepErr, _ := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", errors.New("auth fail") },
	})
	if _, err := cPrepErr.Preview(context.Background(), validPlan, exec, bounds); errorCode(err) != "auth_required" {
		t.Fatalf("expected auth_required when prepare fails, got %v", err)
	}

	// Preview when prepare returns plan with invalid limit
	cPrepBadPlan, _ := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) {
			bad := validPlan
			bad.Limit = 0
			return bad, "policy", nil
		},
	})
	if _, err := cPrepBadPlan.Preview(context.Background(), validPlan, exec, bounds); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input when prepared plan fails validation, got %v", err)
	}

	// Preview when prepared plan has different digest
	otherPlan, _ := Compile(p, dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(20).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}))
	cDiffPlan, _ := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) { return otherPlan, "policy", nil },
	})
	if _, err := cDiffPlan.Preview(context.Background(), validPlan, exec, bounds); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed when digest mismatches, got %v", err)
	}

	// Preview when observe fails
	cObsFail, _ := NewClient(Config{
		Provider: prov, Transport: rt(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("observe network error")
		}),
		Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) { return validPlan, "policy", nil },
	})
	if _, err := cObsFail.Preview(context.Background(), validPlan, exec, bounds); err == nil {
		t.Fatal("expected error when observe fails in Preview")
	}

	// Preview when estimate fails
	cEstFail, _ := NewClient(Config{
		Provider: prov, Transport: rt(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "queries") {
				return nil, errors.New("query error")
			}
			return metadataResponse(req)
		}),
		Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) { return validPlan, "policy", nil },
	})
	if _, err := cEstFail.Preview(context.Background(), validPlan, exec, bounds); err == nil {
		t.Fatal("expected error when estimate fails")
	}

	// estimate with invalid MaximumBytesBilled (queryRequest fails)
	scope := newPreviewScope(bounds, time.Now())
	badExecBytes := exec
	badExecBytes.MaximumBytesBilled = "-1"
	if _, err := c.estimate(context.Background(), validPlan, badExecBytes, "EU", scope); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad MaximumBytesBilled in estimate, got %v", err)
	}

	// Comparison with unsupported operator
	compBadOp := dal.Comparison{Left: dal.NewFieldRef("", "n"), Operator: dal.Operator("LIKE"), Right: dal.Constant{Value: "foo"}}
	qCompBadOp := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Where(compBadOp).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(p, qCompBadOp); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for LIKE in Compile, got %v", err)
	}

	// validatePlan: valid AND with children returning nil
	planAndValid := validPlan
	planAndValid.Where = &Predicate{Op: "AND", Children: []Predicate{{Op: "IS NULL", Column: "n"}}}
	planAndValid.SQL, _ = renderPlan(p, planAndValid)
	planAndValid.Digest, _ = digestObject("ReadPlan", planAndValid)
	if _, _, err := c.validatePlan(planAndValid); err != nil {
		t.Fatalf("expected valid AND with children to pass validatePlan, got %v", err)
	}

	// Preview: caller deadline expired before preparation (c.preparation fails)
	pastCtx, pastCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer pastCancel()
	if _, err := c.Preview(pastCtx, validPlan, exec, bounds); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on past deadline in Preview, got %v", err)
	}

	// Compile: BOOL column with < condition fails parseCondition with fields != nil
	pBool := p
	pBool.Schema = append([]Field{}, p.Schema...)
	pBool.Schema = append(pBool.Schema, Field{Name: "b", Type: "BOOL", Mode: "NULLABLE"})
	compBool := dal.NewComparison(dal.NewFieldRef("", "b"), dal.Operator("<"), dal.Constant{Value: true})
	qCompBool := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Where(compBool).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(pBool, qCompBool); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for bool comparison <, got %v", err)
	}

	// Comparison with nil Constant value
	compNil := dal.Comparison{Left: dal.NewFieldRef("", "n"), Operator: "=", Right: dal.Constant{Value: nil}}
	qCompNil := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Where(compNil).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(p, qCompNil); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query for nil constant, got %v", err)
	}

	// Compile: IN parameter with invalid value (fails parameterValue)
	compInBad := dal.Comparison{Left: dal.NewFieldRef("", "n"), Operator: "IN", Right: dal.Constant{Value: []any{"not-an-int"}}}
	qInBad := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Where(compInBad).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(p, qInBad); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for bad IN value in Compile, got %v", err)
	}

	// Compile: comparison parameter with invalid value (fails parameterValue)
	compScalarBad := dal.Comparison{Left: dal.NewFieldRef("", "n"), Operator: "=", Right: dal.Constant{Value: "not-an-int"}}
	qScalarBad := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Where(compScalarBad).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := Compile(p, qScalarBad); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value for bad scalar in Compile, got %v", err)
	}

	// validatePlan: IS NULL with non-empty Parameter
	planIsNullParam := validPlan
	planIsNullParam.Where = &Predicate{Op: "IS NULL", Column: "n", Parameter: "p0"}
	if _, _, err := c.validatePlan(planIsNullParam); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for IS NULL with parameter, got %v", err)
	}

	// reauthorize: context canceled inside prepare
	var cancelReauth context.CancelFunc
	cCancelReauth, _ := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) {
			if cancelReauth != nil {
				cancelReauth()
			}
			return validPlan, "policy", nil
		},
	})
	ctxReauth, cancelReauthFunc := context.WithCancel(context.Background())
	cancelReauth = cancelReauthFunc
	if err := cCancelReauth.reauthorize(ctxReauth, validPlan, "policy"); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on cancel during prepare in reauthorize, got %v", err)
	}

	// Preview: context canceled inside prepare
	var cancelPrev context.CancelFunc
	cCancelPrev, _ := NewClient(Config{Provider: prov, Transport: trans, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) {
			if cancelPrev != nil {
				cancelPrev()
			}
			return validPlan, "policy", nil
		},
	})
	ctxPrev, cancelPrevFunc := context.WithCancel(context.Background())
	cancelPrev = cancelPrevFunc
	if _, err := cCancelPrev.Preview(ctxPrev, validPlan, exec, bounds); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on cancel during prepare in Preview, got %v", err)
	}

	// Preview: ledger updateContext fails
	failLedger := &mockFailingLedger{MemoryLedger: NewMemoryLedger(), failUpdate: true}
	cLedgFail, _ := NewClient(Config{
		Provider: prov, Transport: rt(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "queries") {
				return response(`{"jobComplete":true,"totalBytesProcessed":"500"}`), nil
			}
			return metadataResponse(req)
		}),
		Ledger: failLedger, Clock: realClock{}, Profiles: []SourceProfile{p},
		Prepare: func(context.Context) (ReadPlan, string, error) { return validPlan, "policy", nil },
	})
	if _, err := cLedgFail.Preview(context.Background(), validPlan, exec, bounds); err == nil {
		t.Fatal("expected error on ledger failure in Preview")
	}

	// queryRequest branches:
	// 1. ARRAY parameter where Value is not []any
	planBadArrayType := validPlan
	planBadArrayType.Parameters = []Parameter{{Name: "p0", Type: "ARRAY<INT64>", Value: "not-a-slice"}}
	if _, err := queryRequest(planBadArrayType, exec, bounds, "EU", false); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for non-slice array param, got %v", err)
	}

	// 2. ARRAY parameter with bad element
	planBadArrayElem := validPlan
	planBadArrayElem.Parameters = []Parameter{{Name: "p0", Type: "ARRAY<INT64>", Value: []any{123}}}
	if _, err := queryRequest(planBadArrayElem, exec, bounds, "EU", false); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for non-string array elem, got %v", err)
	}

	// 3. ARRAY parameter valid
	planGoodArray := validPlan
	planGoodArray.Parameters = []Parameter{{Name: "p0", Type: "ARRAY<INT64>", Value: []any{"1", "2"}}}
	if qr, err := queryRequest(planGoodArray, exec, bounds, "EU", false); err != nil || len(qr.QueryParameters) != 1 {
		t.Fatalf("expected valid query request with array param, got %+v, err: %v", qr, err)
	}

	// 4. Scalar parameter error
	planBadScalarParam := validPlan
	planBadScalarParam.Parameters = []Parameter{{Name: "p0", Type: "INT64", Value: 123}}
	if _, err := queryRequest(planBadScalarParam, exec, bounds, "EU", false); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for non-string scalar param, got %v", err)
	}

	// 5. Scalar parameter valid
	planGoodScalarParam := validPlan
	planGoodScalarParam.Parameters = []Parameter{{Name: "p0", Type: "INT64", Value: "123"}}
	if qr, err := queryRequest(planGoodScalarParam, exec, bounds, "EU", false); err != nil || len(qr.QueryParameters) != 1 {
		t.Fatalf("expected valid query request with scalar param, got %+v, err: %v", qr, err)
	}
}

type mockFailingLedger struct {
	*MemoryLedger
	failUpdate bool
}

func (m *mockFailingLedger) updateContext(ctx context.Context, fn func(*ledgerState) error) error {
	if m.failUpdate {
		return errors.New("ledger error")
	}
	return m.MemoryLedger.updateContext(ctx, fn)
}

// Not parallel: temporarily replaces package-level vars
func TestPlan_Client_Isolated_Vars(t *testing.T) {
	p := testProfile(t)
	prov := &testProvider{identity: Identity{Principal{"workload", "sub", "1"}, time.Now().Add(time.Hour), true, false}}
	ledg := NewMemoryLedger()

	validPlan, err := Compile(p, dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}))
	if err != nil {
		t.Fatal(err)
	}
	exec := Execution{
		JobProject:         "job-project",
		Principal:          Principal{Kind: "workload", Subject: "sub", Generation: "1"},
		MaximumBytesBilled: "1000",
		SessionBudgetBytes: "2000",
	}
	bounds := Bounds{
		PageSize:           50,
		MaxRows:            100,
		MaxPages:           10,
		ResponseBytes:      100000,
		TotalResponseBytes: 1000000,
		WallMs:             5000,
		HTTPMs:             1000,
		Concurrency:        1,
	}

	// renderPlanFn error in Compile
	origRender := renderPlanFn
	renderPlanFn = func(SourceProfile, ReadPlan) (string, error) { return "", errors.New("render failure") }
	_, err = Compile(p, dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(p.LogicalCollection, "", nil))).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}))
	renderPlanFn = origRender
	if err == nil {
		t.Fatal("expected renderPlan error in Compile")
	}

	// strconvParseInt error in sdkParameterValue
	origParseInt := strconvParseInt
	strconvParseInt = func(string, int, int) (int64, error) { return 0, errors.New("parse error") }
	_, err = sdkParameterValue("TIMESTAMP", "1700000000000000")
	strconvParseInt = origParseInt
	if errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value on ParseInt failure, got %v", err)
	}

	// objectFn error in estimate
	origObject := objectFn
	objectFn = func(any) (map[string]any, error) { return nil, fail("malformed_wire") }
	cObjErr, _ := NewClient(Config{
		Provider: prov, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Transport: rt(func(*http.Request) (*http.Response, error) { return response(`{"jobComplete":true}`), nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return validPlan, "", nil },
	})
	scope := newPreviewScope(bounds, time.Now())
	_, err = cObjErr.estimate(context.Background(), validPlan, exec, "EU", scope)
	objectFn = origObject
	if errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on objectFn error, got %v", err)
	}

	// jsonMarshal error in Preview (approvalDigest failure)
	origMarshal := jsonMarshal
	cMarshal, _ := NewClient(Config{
		Provider: prov, Ledger: ledg, Clock: realClock{}, Profiles: []SourceProfile{p},
		Transport: rt(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "queries") {
				return response(`{"jobComplete":true,"totalBytesProcessed":"500"}`), nil
			}
			return metadataResponse(req)
		}),
		Prepare: func(context.Context) (ReadPlan, string, error) { return validPlan, "policy", nil },
	})
	// Inject jsonMarshal failure only after validation
	jsonMarshal = func(v any) ([]byte, error) {
		if m, ok := v.(map[string]any); ok && m["effectivePlanDigest"] != nil {
			return nil, errors.New("marshal error")
		}
		return json.Marshal(v)
	}
	_, err = cMarshal.Preview(context.Background(), validPlan, exec, bounds)
	jsonMarshal = origMarshal
	if errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on approvalDigest failure, got %v", err)
	}
}
