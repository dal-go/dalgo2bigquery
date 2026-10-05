package bigquery

import (
	"encoding/json"
	"github.com/dal-go/dalgo/dal"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var projectID = regexp.MustCompile(`^[a-z][a-z0-9-]{4,61}[a-z0-9]$`)
var locationID = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9-]{0,63}$`)

func sourceDigest(p SourceProfile) (string, error) {
	for _, s := range []string{p.SourceID, p.DescriptorDigest, p.PublisherReviewRef, p.RightsReviewRef} {
		if !utf8.ValidString(s) || len(s) > 4096 {
			return "", fail("invalid_input")
		}
	}
	if p.Version != 1 || p.SourceID == "" || p.DescriptorDigest == "" || !identifier.MatchString(p.LogicalCollection) || !projectID.MatchString(p.SourceProject) || !identifier.MatchString(p.DatasetID) || !identifier.MatchString(p.TableID) || !locationID.MatchString(p.Location) || p.PublisherReviewRef == "" || p.RightsReviewRef == "" || (p.Use != "connection-test" && p.Use != "admitted") {
		return "", fail("invalid_input")
	}
	if e := validateFields(p.Schema); e != nil {
		return "", fail("invalid_input")
	}
	return digestObject("SourceProfile", p)
}
func fieldRef(expr dal.Expression) (string, error) {
	var f dal.FieldRef
	switch v := expr.(type) {
	case dal.FieldRef:
		f = v
	case *dal.FieldRef:
		if v == nil {
			return "", fail("unsupported_query")
		}
		f = *v
	default:
		return "", fail("unsupported_query")
	}
	if f.Source() != "" || f.IsID() || !identifier.MatchString(f.Name()) {
		return "", fail("unsupported_query")
	}
	return f.Name(), nil
}

// ValidateQuery visits the whole supported AST before any policy wrapper or
// generic DALgo executor can choose a paid leaf scan fallback.
func ValidateQuery(query dal.Query) error { _, e := queryShape(query); return e }
func queryShape(query dal.Query) (dal.StructuredQuery, error) {
	q, ok := query.(dal.StructuredQuery)
	if !ok || q == nil {
		return nil, fail("unsupported_query")
	}
	if q.Offset() != 0 || q.StartFrom() != "" || q.StartAfter() != "" || len(q.GroupBy()) > 0 || q.Having() != nil || q.IntoRecord() != nil || q.Limit() < 1 || q.Limit() > 10000 {
		return nil, fail("unsupported_query")
	}
	f := q.From()
	if f == nil || len(f.Joins()) != 0 {
		return nil, fail("unsupported_query")
	}
	var root dal.CollectionRef
	switch v := f.Base().(type) {
	case dal.CollectionRef:
		root = v
	case *dal.CollectionRef:
		if v == nil {
			return nil, fail("unsupported_query")
		}
		root = *v
	default:
		return nil, fail("unsupported_query")
	}
	if !identifier.MatchString(root.Name()) || root.Alias() != "" || root.Parent() != nil || root.Schema() != "" || root.Database() != "" || root.ScanLimit() != 0 || len(root.ScanOrders()) != 0 {
		return nil, fail("unsupported_query")
	}
	cols := q.Columns()
	if len(cols) < 1 || len(cols) > 128 || len(q.OrderBy()) > 16 {
		return nil, fail("unsupported_query")
	}
	seen := map[string]bool{}
	for _, c := range cols {
		if c.Alias != "" || c.Wildcard != nil {
			return nil, fail("unsupported_query")
		}
		n, e := fieldRef(c.Expression)
		if e != nil || seen[n] {
			return nil, fail("unsupported_query")
		}
		seen[n] = true
	}
	for _, o := range q.OrderBy() {
		if o == nil {
			return nil, fail("unsupported_query")
		}
		if _, e := fieldRef(o.Expression()); e != nil {
			return nil, e
		}
	}
	count := 0
	if _, e := parseCondition(q.Where(), 0, &count, nil, nil); e != nil {
		return nil, e
	}
	return q, nil
}
func Compile(profile SourceProfile, query dal.Query) (ReadPlan, error) {
	q, e := queryShape(query)
	if e != nil {
		return ReadPlan{}, e
	}
	d, e := sourceDigest(profile)
	if e != nil {
		return ReadPlan{}, e
	}
	if q.From().Base().Name() != profile.LogicalCollection {
		return ReadPlan{}, fail("unsupported_query")
	}
	fields := map[string]Field{}
	for _, f := range profile.Schema {
		fields[f.Name] = f
	}
	plan := ReadPlan{Version: 1, SourceDigest: d, Projection: []string{}, Order: []Order{}, Parameters: []Parameter{}, Limit: q.Limit()}
	for _, c := range q.Columns() {
		n, _ := fieldRef(c.Expression)
		f, ok := fields[n]
		if !ok {
			return ReadPlan{}, fail("unsupported_query")
		}
		if _, e := NormalizeScalar(Field{Name: f.Name, Type: f.Type, Mode: "NULLABLE"}, nil); e != nil || f.Mode == "REPEATED" || len(f.Fields) > 0 {
			return ReadPlan{}, fail("unsupported_type")
		}
		plan.Projection = append(plan.Projection, n)
	}
	for _, o := range q.OrderBy() {
		n, _ := fieldRef(o.Expression())
		f, ok := fields[n]
		if !ok || !ordered(f) {
			return ReadPlan{}, fail("unsupported_query")
		}
		dir := "ASC"
		if o.Descending() {
			dir = "DESC"
		}
		plan.Order = append(plan.Order, Order{n, dir})
	}
	count := 0
	plan.Where, e = parseCondition(q.Where(), 0, &count, fields, &plan.Parameters)
	if e != nil {
		return ReadPlan{}, e
	}
	plan.SQL, e = renderPlan(profile, plan)
	if e != nil {
		return ReadPlan{}, e
	}
	plan.Digest, e = digestObject("ReadPlan", plan)
	return plan, e
}
func ordered(f Field) bool {
	typ := canonicalType(f.Type)
	return typ != "JSON" && typ != "BYTES" && typ != "BOOL" && f.Mode != "REPEATED" && len(f.Fields) == 0 && scalarType(typ)
}
func canonicalType(s string) string {
	switch strings.ToUpper(s) {
	case "INTEGER":
		return "INT64"
	case "FLOAT":
		return "FLOAT64"
	case "BOOLEAN":
		return "BOOL"
	}
	return strings.ToUpper(s)
}
func scalarType(s string) bool {
	switch s {
	case "INT64", "NUMERIC", "BIGNUMERIC", "FLOAT64", "BOOL", "STRING", "BYTES", "DATE", "TIME", "DATETIME", "TIMESTAMP", "JSON":
		return true
	}
	return false
}
func parseCondition(c dal.Condition, depth int, count *int, fields map[string]Field, parameters *[]Parameter) (*Predicate, error) {
	if c == nil {
		return nil, nil
	}
	*count++
	if *count > 128 || depth >= 8 {
		return nil, fail("unsupported_query")
	}
	switch v := c.(type) {
	case *dal.GroupCondition:
		if v == nil {
			return nil, fail("unsupported_query")
		}
		return parseCondition(*v, depth, count, fields, parameters)
	case dal.GroupCondition:
		op := string(v.Operator())
		if (op != "AND" && op != "OR") || len(v.Conditions()) < 1 {
			return nil, fail("unsupported_query")
		}
		p := &Predicate{Op: op, Children: []Predicate{}}
		for _, child := range v.Conditions() {
			if child == nil {
				return nil, fail("unsupported_query")
			}
			cc, e := parseCondition(child, depth+1, count, fields, parameters)
			if e != nil {
				return nil, e
			}
			p.Children = append(p.Children, *cc)
		}
		return p, nil
	case *dal.IsNullCondition:
		if v == nil {
			return nil, fail("unsupported_query")
		}
		return parseCondition(*v, depth, count, fields, parameters)
	case dal.IsNullCondition:
		n, e := fieldRef(v.Operand())
		if e != nil {
			return nil, e
		}
		if fields != nil {
			f, ok := fields[n]
			if !ok || !scalarType(canonicalType(f.Type)) || f.Mode == "REPEATED" || len(f.Fields) > 0 {
				return nil, fail("unsupported_type")
			}
		}
		op := "IS NULL"
		if v.Negated() {
			op = "IS NOT NULL"
		}
		return &Predicate{Op: op, Column: n}, nil
	case *dal.Comparison:
		if v == nil {
			return nil, fail("unsupported_query")
		}
		return parseCondition(*v, depth, count, fields, parameters)
	case dal.Comparison:
		n, e := fieldRef(v.Left)
		if e != nil {
			return nil, e
		}
		var value any
		switch r := v.Right.(type) {
		case dal.Constant:
			value = r.Value
		case *dal.Constant:
			if r == nil {
				return nil, fail("unsupported_query")
			}
			value = r.Value
		default:
			return nil, fail("unsupported_query")
		}
		op := string(v.Operator)
		if op == "==" {
			op = "="
		}
		if op == "In" {
			op = "IN"
		}
		switch op {
		case "=", "!=", "<", "<=", ">", ">=", "IN":
		default:
			return nil, fail("unsupported_query")
		}
		if value == nil {
			return nil, fail("unsupported_query")
		}
		p := &Predicate{Op: op, Column: n}
		if op == "IN" {
			rv := reflect.ValueOf(value)
			if rv.Kind() != reflect.Slice || rv.Len() > 1000 {
				return nil, fail("unsupported_query")
			}
			for i := 0; i < rv.Len(); i++ {
				if rv.Index(i).Interface() == nil {
					return nil, fail("unsupported_query")
				}
			}
		}
		if fields == nil {
			return p, nil
		}
		f, ok := fields[n]
		if !ok || f.Mode == "REPEATED" || len(f.Fields) > 0 || !scalarType(canonicalType(f.Type)) || canonicalType(f.Type) == "JSON" || canonicalType(f.Type) == "BYTES" {
			return nil, fail("unsupported_type")
		}
		if op != "=" && op != "!=" && op != "IN" && !ordered(f) {
			return nil, fail("unsupported_query")
		}
		parameter := Parameter{Name: "p" + strconv.Itoa(len(*parameters)), Type: canonicalType(f.Type)}
		if op == "IN" {
			rv := reflect.ValueOf(value)
			a := []any{}
			for i := 0; i < rv.Len(); i++ {
				vv, e := parameterValue(f, rv.Index(i).Interface())
				if e != nil {
					return nil, e
				}
				a = append(a, vv)
			}
			parameter.Type = "ARRAY<" + parameter.Type + ">"
			parameter.Value = a
		} else {
			parameter.Value, e = parameterValue(f, value)
			if e != nil {
				return nil, e
			}
		}
		p.Parameter = parameter.Name
		*parameters = append(*parameters, parameter)
		return p, nil
	default:
		return nil, fail("unsupported_query")
	}
}
func parameterValue(f Field, v any) (any, error) {
	// Runtime values are exact scalar strings except BOOL, whose input is boolean.
	if canonicalType(f.Type) != "BOOL" && v != nil {
		if _, ok := v.(string); !ok {
			return nil, fail("invalid_input")
		}
	}
	if canonicalType(f.Type) == "BOOL" {
		b, ok := v.(bool)
		if !ok {
			return nil, fail("invalid_input")
		}
		if b {
			v = "true"
		} else {
			v = "false"
		}
	}
	f.Mode = "NULLABLE"
	cell, e := NormalizeScalar(f, v)
	if e != nil {
		return nil, e
	}
	return cell.Value, nil
}
func renderPlan(p SourceProfile, plan ReadPlan) (string, error) {
	projection := make([]string, len(plan.Projection))
	for i, n := range plan.Projection {
		if !identifier.MatchString(n) {
			return "", fail("invalid_input")
		}
		projection[i] = "`" + n + "`"
	}
	sql := "SELECT " + strings.Join(projection, ", ") + " FROM `" + p.SourceProject + "." + p.DatasetID + "." + p.TableID + "`"
	if plan.Where != nil {
		w, e := renderPredicate(*plan.Where, plan.Parameters)
		if e != nil {
			return "", e
		}
		sql += " WHERE " + w
	}
	if len(plan.Order) > 0 {
		a := []string{}
		for _, o := range plan.Order {
			if !identifier.MatchString(o.Column) || (o.Direction != "ASC" && o.Direction != "DESC") {
				return "", fail("invalid_input")
			}
			a = append(a, "`"+o.Column+"` "+o.Direction)
		}
		sql += " ORDER BY " + strings.Join(a, ", ")
	}
	return sql + " LIMIT " + strconv.Itoa(plan.Limit), nil
}
func renderPredicate(p Predicate, params []Parameter) (string, error) {
	if p.Op == "AND" || p.Op == "OR" {
		a := []string{}
		for _, c := range p.Children {
			s, e := renderPredicate(c, params)
			if e != nil {
				return "", e
			}
			a = append(a, s)
		}
		return "(" + strings.Join(a, " "+p.Op+" ") + ")", nil
	}
	if !identifier.MatchString(p.Column) {
		return "", fail("invalid_input")
	}
	c := "`" + p.Column + "`"
	if p.Op == "IS NULL" || p.Op == "IS NOT NULL" {
		return c + " " + p.Op, nil
	}
	if !regexp.MustCompile(`^p[0-9]+$`).MatchString(p.Parameter) {
		return "", fail("invalid_input")
	}
	if p.Op == "IN" {
		for _, v := range params {
			if v.Name == p.Parameter {
				if a, ok := v.Value.([]any); ok && len(a) == 0 {
					return "FALSE", nil
				}
			}
		}
		return c + " IN UNNEST(@" + p.Parameter + ")", nil
	}
	switch p.Op {
	case "=", "!=", "<", "<=", ">", ">=":
		return c + " " + p.Op + " @" + p.Parameter, nil
	}
	return "", fail("invalid_input")
}
func equalJSON(a, b any) bool {
	aa, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(aa) == string(bb)
}
