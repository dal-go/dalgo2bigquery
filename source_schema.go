package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/record"
)

type sourceRelation struct{ name, kind string }

func (d *sourceDatabase) relations(ctx context.Context, principal Principal) ([]sourceRelation, error) {
	if e := d.datasetMetadata(ctx, principal); e != nil {
		return nil, e
	}
	seenTokens, seenNames := map[string]bool{}, map[string]bool{}
	var all []sourceRelation
	var token string
	totalItems := int64(-1)
	for page := 0; ; page++ {
		if page >= d.limits.MaxPages {
			return nil, fail("response_limit")
		}
		var m map[string]any
		var e error
		m, e = d.get(ctx, principal, func(api sdkAPI) error {
			call := api.service.Tables.List(d.project, d.dataset).MaxResults(1000)
			if token != "" {
				call = call.PageToken(token)
			}
			_, e := call.Context(api.ctx).Do()
			return e
		})
		if e != nil {
			return nil, e
		}
		if raw, present := m["totalItems"]; present {
			total, parseErr := parseListTotal(raw)
			if parseErr != nil || (totalItems >= 0 && totalItems != total) {
				return nil, fail("source_changed")
			}
			totalItems = total
		}
		items, ok := m["tables"].([]any)
		if !ok && m["tables"] != nil {
			return nil, fail("malformed_wire")
		}
		for _, item := range items {
			table, e := object(item)
			if e != nil {
				return nil, e
			}
			ref, e := object(table["tableReference"])
			if e != nil {
				return nil, e
			}
			name, e := requiredString(ref, "tableId")
			if e != nil {
				return nil, e
			}
			kind, e := requiredString(table, "type")
			if e != nil {
				return nil, e
			}
			if ref["projectId"] != d.project || ref["datasetId"] != d.dataset || !identifier.MatchString(name) || seenNames[name] {
				return nil, fail("source_changed")
			}
			if kind != "TABLE" && kind != "VIEW" {
				return nil, fail("source_ineligible")
			}
			seenNames[name] = true
			all = append(all, sourceRelation{name, kind})
		}
		next, e := sourceToken(m, "nextPageToken")
		if e != nil {
			return nil, e
		}
		if next == "" {
			break
		}
		if next == token || seenTokens[next] {
			return nil, fail("cursor_invalid")
		}
		seenTokens[next] = true
		token = next
	}
	if totalItems < 0 || totalItems != int64(len(all)) {
		return nil, fail("source_changed")
	}
	sort.Slice(all, func(i, j int) bool { return all[i].name < all[j].name })
	return all, nil
}

func (d *ReadOnlyDatabase) ListCollections(ctx context.Context, _ *record.Key) ([]dal.CollectionRef, error) {
	ctx, cancel := sourceDeadline(ctx, d.source.limits.WallTime)
	defer cancel()
	principal, e := d.source.identity(ctx)
	if e != nil {
		return nil, e
	}
	all, e := d.source.relations(ctx, principal)
	if e != nil {
		return nil, e
	}
	out := make([]dal.CollectionRef, 0, len(all))
	for _, rel := range all {
		if rel.kind == "TABLE" {
			out = append(out, dal.NewRootCollectionRef(rel.name, ""))
		}
	}
	return out, nil
}

func (d *ReadOnlyDatabase) ListSourceViews(ctx context.Context) ([]dbschema.SourceViewDef, error) {
	ctx, cancel := sourceDeadline(ctx, d.source.limits.WallTime)
	defer cancel()
	principal, e := d.source.identity(ctx)
	if e != nil {
		return nil, e
	}
	all, e := d.source.relations(ctx, principal)
	if e != nil {
		return nil, e
	}
	views := []dbschema.SourceViewDef{}
	for _, rel := range all {
		if rel.kind != "VIEW" {
			continue
		}
		m, e := d.source.tableMetadata(ctx, principal, rel.name)
		if e != nil {
			return nil, e
		}
		if m["type"] != "VIEW" {
			return nil, fail("source_changed")
		}
		v, e := object(m["view"])
		if e != nil {
			return nil, e
		}
		if _, e := requiredString(v, "query"); e != nil {
			return nil, e
		}
		fields, e := sourceFields(m)
		if e != nil {
			return nil, e
		}
		columns := make([]string, len(fields))
		for i := range fields {
			columns[i] = fields[i].Name
		}
		// BigQuery supplies the SELECT body, not its original CREATE VIEW DDL.
		// Keep CreateSQL empty rather than inventing a restorable definition.
		views = append(views, dbschema.SourceViewDef{Name: rel.name, Columns: columns})
	}
	return views, nil
}

func sourceFields(m map[string]any) ([]Field, error) {
	schema, e := object(m["schema"])
	if e != nil {
		return nil, e
	}
	a, ok := schema["fields"].([]any)
	if !ok || len(a) == 0 || len(a) > 1024 {
		return nil, fail("source_ineligible")
	}
	seen := map[string]bool{}
	out := make([]Field, 0, len(a))
	for _, item := range a {
		f, e := object(item)
		if e != nil {
			return nil, e
		}
		if e := known(f, []string{"name", "type", "mode", "description", "precision", "scale", "fields"}, []string{"collation", "defaultValueExpression", "generatedColumn", "maxLength", "policyTags", "rangeElementType", "roundingMode", "timestampPrecision", "dataPolicies", "dataPolicyList", "foreignTypeDefinition"}); e != nil {
			return nil, e
		}
		if e := absentConfigs(f, []string{"collation", "defaultValueExpression", "generatedColumn", "maxLength", "policyTags", "rangeElementType", "roundingMode", "timestampPrecision", "dataPolicies", "dataPolicyList", "foreignTypeDefinition"}); e != nil {
			return nil, e
		}
		name, e := requiredString(f, "name")
		if e != nil {
			return nil, e
		}
		typ, e := requiredString(f, "type")
		if e != nil {
			return nil, e
		}
		mode := "NULLABLE"
		if raw, ok := f["mode"]; ok {
			mode, ok = raw.(string)
			if !ok {
				return nil, fail("malformed_wire")
			}
		}
		typ = canonicalType(typ)
		if !identifier.MatchString(name) || seen[name] || !scalarType(typ) || mode == "REPEATED" || (mode != "NULLABLE" && mode != "REQUIRED") || f["fields"] != nil {
			return nil, fail("source_ineligible")
		}
		seen[name] = true
		field := Field{Name: name, Type: typ, Mode: mode}
		if raw, present := f["description"]; present {
			s, ok := raw.(string)
			if !ok {
				return nil, fail("malformed_wire")
			}
			field.Description = s
		}
		if raw, ok := f["precision"]; ok {
			field.Precision, ok = raw.(string)
			if !ok {
				return nil, fail("malformed_wire")
			}
		}
		if raw, ok := f["scale"]; ok {
			field.Scale, ok = raw.(string)
			if !ok {
				return nil, fail("malformed_wire")
			}
		}
		if field.Scale != "" && field.Precision == "" {
			return nil, fail("source_ineligible")
		}
		if _, e := NormalizeScalar(field, sourceExampleValue(typ)); e != nil {
			return nil, fail("source_ineligible")
		}
		out = append(out, field)
	}
	return out, nil
}

// sourceExampleValue checks scalar field metadata without assuming a row value.
func sourceExampleValue(typ string) any {
	switch typ {
	case "INT64", "NUMERIC", "BIGNUMERIC", "FLOAT64", "TIMESTAMP":
		return "0"
	case "BOOL":
		return "false"
	case "DATE":
		return "2000-01-01"
	case "TIME":
		return "00:00:00"
	case "DATETIME":
		return "2000-01-01T00:00:00"
	case "JSON":
		return "null"
	default:
		return ""
	}
}

func sourcePortableType(typ string) dbschema.Type {
	switch typ {
	case "INT64":
		return dbschema.Int
	case "NUMERIC", "BIGNUMERIC":
		return dbschema.Decimal
	case "FLOAT64":
		return dbschema.Float
	case "BOOL":
		return dbschema.Bool
	case "BYTES":
		return dbschema.Bytes
	case "DATE", "TIME", "DATETIME", "TIMESTAMP":
		return dbschema.Time
	default:
		return dbschema.String
	}
}

func sourceConstraints(m map[string]any, fields []Field, project, dataset string) ([]dal.FieldName, []dbschema.ForeignKeyDef, error) {
	if m["tableConstraints"] == nil {
		return nil, nil, nil
	}
	constraints, e := object(m["tableConstraints"])
	if e != nil {
		return nil, nil, e
	}
	knownFields := map[string]bool{}
	for _, f := range fields {
		knownFields[f.Name] = true
	}
	var pk []dal.FieldName
	if constraints["primaryKey"] != nil {
		primary, e := object(constraints["primaryKey"])
		if e != nil {
			return nil, nil, e
		}
		a, ok := primary["columns"].([]any)
		if !ok || len(a) == 0 {
			return nil, nil, fail("malformed_wire")
		}
		seenKey := map[string]bool{}
		for _, v := range a {
			s, ok := v.(string)
			if !ok || !knownFields[s] || seenKey[s] {
				return nil, nil, fail("malformed_wire")
			}
			seenKey[s] = true
			pk = append(pk, dal.FieldName(s))
		}
	}
	var fks []dbschema.ForeignKeyDef
	if constraints["foreignKeys"] != nil {
		a, ok := constraints["foreignKeys"].([]any)
		if !ok {
			return nil, nil, fail("malformed_wire")
		}
		for _, value := range a {
			fk, e := object(value)
			if e != nil {
				return nil, nil, e
			}
			ref, e := object(fk["referencedTable"])
			if e != nil {
				return nil, nil, e
			}
			name, e := requiredString(ref, "tableId")
			if e != nil {
				return nil, nil, e
			}
			if !identifier.MatchString(name) {
				return nil, nil, fail("malformed_wire")
			}
			out := dbschema.ForeignKeyDef{ReferencedCollection: name, Enforcement: dbschema.ForeignKeyEnforcementDisabled}
			if s, ok := fk["name"].(string); ok {
				out.Name = s
			}
			refProject, refDataset := project, dataset
			if raw, ok := ref["projectId"]; ok {
				var valid bool
				refProject, valid = raw.(string)
				if !valid || !projectID.MatchString(refProject) {
					return nil, nil, fail("malformed_wire")
				}
			}
			if raw, ok := ref["datasetId"]; ok {
				var valid bool
				refDataset, valid = raw.(string)
				if !valid || !identifier.MatchString(refDataset) {
					return nil, nil, fail("malformed_wire")
				}
			}
			if refProject != project || refDataset != dataset {
				out.ReferencedNamespace = fmt.Sprintf("%s.%s", refProject, refDataset)
			}
			pairs, ok := fk["columnReferences"].([]any)
			if !ok || len(pairs) == 0 {
				return nil, nil, fail("malformed_wire")
			}
			for _, value := range pairs {
				pair, e := object(value)
				if e != nil {
					return nil, nil, e
				}
				from, e := requiredString(pair, "referencingColumn")
				if e != nil || !knownFields[from] {
					return nil, nil, fail("malformed_wire")
				}
				to, e := requiredString(pair, "referencedColumn")
				if e != nil || !identifier.MatchString(to) {
					return nil, nil, fail("malformed_wire")
				}
				out.Fields = append(out.Fields, dal.FieldName(from))
				out.ReferencedFields = append(out.ReferencedFields, dal.FieldName(to))
			}
			fks = append(fks, out)
		}
	}
	return pk, fks, nil
}

func (d *ReadOnlyDatabase) DescribeCollection(ctx context.Context, ref *dal.CollectionRef) (*dbschema.CollectionDef, error) {
	if ref == nil || ref.Schema() != "" || !identifier.MatchString(ref.Name()) {
		return nil, fail("invalid_input")
	}
	ctx, cancel := sourceDeadline(ctx, d.source.limits.WallTime)
	defer cancel()
	principal, e := d.source.identity(ctx)
	if e != nil {
		return nil, e
	}
	m, e := d.source.tableMetadata(ctx, principal, ref.Name())
	if e != nil {
		return nil, e
	}
	if m["type"] != "TABLE" {
		return nil, fail("source_ineligible")
	}
	fields, e := sourceFields(m)
	if e != nil {
		return nil, e
	}
	pk, fks, e := sourceConstraints(m, fields, d.source.project, d.source.dataset)
	if e != nil {
		return nil, e
	}
	def := &dbschema.CollectionDef{Name: ref.Name(), PrimaryKey: pk, ForeignKeys: fks, SourceDefinition: &dbschema.SourceDefinition{Dialect: "bigquery"}}
	for _, f := range fields {
		field := dbschema.FieldDef{Name: dal.FieldName(f.Name), Type: sourcePortableType(f.Type), Nullable: f.Mode != "REQUIRED"}
		if f.Precision != "" {
			p, _ := strconv.Atoi(f.Precision)
			s, _ := strconv.Atoi(f.Scale)
			field.Precision = &dbschema.Precision{Total: p, Scale: s}
		}
		def.Fields = append(def.Fields, field)
		position := 0
		for j, key := range pk {
			if string(key) == f.Name {
				position = j + 1
			}
		}
		def.SourceDefinition.Columns = append(def.SourceDefinition.Columns, dbschema.SourceColumnDef{Name: f.Name, DeclaredType: f.Type, NotNull: f.Mode == "REQUIRED", PrimaryKeyPosition: position})
	}
	return def, nil
}

// DescribeSourceFields retains BigQuery column descriptions, which the portable
// dbschema.FieldDef has no property for. The returned slice is independent data.
func (d *ReadOnlyDatabase) DescribeSourceFields(ctx context.Context, ref *dal.CollectionRef) ([]Field, error) {
	if ref == nil || ref.Schema() != "" || !identifier.MatchString(ref.Name()) {
		return nil, fail("invalid_input")
	}
	ctx, cancel := sourceDeadline(ctx, d.source.limits.WallTime)
	defer cancel()
	principal, e := d.source.identity(ctx)
	if e != nil {
		return nil, e
	}
	m, e := d.source.tableMetadata(ctx, principal, ref.Name())
	if e != nil {
		return nil, e
	}
	return sourceFields(m)
}

func (d *ReadOnlyDatabase) ListIndexes(context.Context, *dal.CollectionRef) ([]dbschema.IndexDef, error) {
	return nil, &dbschema.NotSupportedError{Op: "ListIndexes", Backend: "bigquery", Reason: "search and vector indexes are not exposed by tables.get"}
}
func (d *ReadOnlyDatabase) ListConstraints(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.ConstraintDef, error) {
	def, e := d.DescribeCollection(ctx, ref)
	if e != nil {
		return nil, e
	}
	out := []dbschema.ConstraintDef{}
	if len(def.PrimaryKey) != 0 {
		out = append(out, dbschema.ConstraintDef{Type: "primary-key"})
	}
	for _, fk := range def.ForeignKeys {
		out = append(out, dbschema.ConstraintDef{Name: fk.Name, Type: "foreign-key"})
	}
	return out, nil
}
func (d *ReadOnlyDatabase) ListReferrers(context.Context, *dal.CollectionRef) ([]dbschema.Referrer, error) {
	return nil, &dbschema.NotSupportedError{Op: "ListReferrers", Backend: "bigquery", Reason: "no reverse constraint catalog in tables.get"}
}

func parseListTotal(v any) (int64, error) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, fail("malformed_wire")
	}
	x, e := strconv.ParseInt(string(n), 10, 64)
	if e != nil || x < 0 {
		return 0, fail("malformed_wire")
	}
	return x, nil
}
