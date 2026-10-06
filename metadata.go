package bigquery

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// Discovery classifications are pinned to google.golang.org/api v0.296.0.
// Known nonexecution statistics/labels may vary. Every other field is explicitly
// validated and bound below, or rejected when present and non-null.
var tableMetadata = strings.Fields("creationTime description etag expirationTime friendlyName id kind labels lastModifiedTime numActiveLogicalBytes numActivePhysicalBytes numBytes numCurrentPhysicalBytes numLongTermBytes numLongTermLogicalBytes numLongTermPhysicalBytes numPartitions numPhysicalBytes numRows numTimeTravelPhysicalBytes numTotalLogicalBytes numTotalPhysicalBytes resourceTags selfLink streamingBuffer")
var tableSupported = strings.Fields("tableReference location type schema timePartitioning rangePartitioning clustering requirePartitionFilter")
var tableRejected = strings.Fields("biglakeConfiguration cloneDefinition defaultCollation defaultRoundingMode encryptionConfiguration externalCatalogTableOptions externalDataConfiguration managedTableType materializedView materializedViewStatus maxStaleness model partitionDefinition replicas restrictions snapshotDefinition tableConstraints tableReplicationInfo view")
var datasetMetadata = strings.Fields("access creationTime defaultPartitionExpirationMs defaultTableExpirationMs description etag friendlyName id kind labels lastModifiedTime maxTimeTravelHours resourceTags satisfiesPzi satisfiesPzs selfLink storageBillingModel tags")
var datasetRejected = strings.Fields("catalogSource defaultCollation defaultEncryptionConfiguration defaultRoundingMode externalCatalogDatasetOptions externalDatasetReference isCaseInsensitive linkedDatasetMetadata linkedDatasetSource restrictions type")

func object(v any) (map[string]any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fail("malformed_wire")
	}
	return m, nil
}
func known(m map[string]any, lists ...[]string) error {
	a := map[string]bool{}
	for _, list := range lists {
		for _, k := range list {
			a[k] = true
		}
	}
	for k := range m {
		if !a[k] {
			return fail("source_ineligible")
		}
	}
	return nil
}
func absentConfigs(m map[string]any, keys []string) error {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != nil {
			return fail("source_ineligible")
		}
	}
	return nil
}
func requiredString(m map[string]any, k string) (string, error) {
	s, ok := m[k].(string)
	if !ok || s == "" {
		return "", fail("malformed_wire")
	}
	return s, nil
}
func decimalString(m map[string]any, k string) (*string, error) {
	v, ok := m[k]
	if !ok {
		return nil, nil
	}
	s, ok := v.(string)
	if !ok || !regexpUnsigned.MatchString(s) {
		return nil, fail("malformed_wire")
	}
	if _, e := strconv.ParseInt(s, 10, 64); e != nil {
		return nil, fail("malformed_wire")
	}
	return &s, nil
}
func validateFields(fields []Field) error {
	count := 0
	var visit func([]Field, int) error
	visit = func(fs []Field, depth int) error {
		if depth > 8 {
			return fail("source_ineligible")
		}
		seen := map[string]bool{}
		for _, f := range fs {
			count++
			if count > 1024 || !identifier.MatchString(f.Name) || seen[f.Name] || f.Type == "" {
				return fail("source_ineligible")
			}
			seen[f.Name] = true
			if f.Mode != "NULLABLE" && f.Mode != "REQUIRED" && f.Mode != "REPEATED" {
				return fail("source_ineligible")
			}
			typ := canonicalType(f.Type)
			if typ == "RECORD" || typ == "STRUCT" {
				if len(f.Fields) < 1 {
					return fail("source_ineligible")
				}
				if e := visit(f.Fields, depth+1); e != nil {
					return e
				}
			} else if len(f.Fields) > 0 {
				return fail("source_ineligible")
			}
			if f.Scale != "" && f.Precision == "" {
				return fail("source_ineligible")
			}
			if f.Precision != "" {
				if typ != "NUMERIC" && typ != "BIGNUMERIC" {
					return fail("source_ineligible")
				}
				p, e := strconv.Atoi(f.Precision)
				if e != nil || !regexpUnsigned.MatchString(f.Precision) {
					return fail("source_ineligible")
				}
				s := 0
				if f.Scale != "" {
					s, e = strconv.Atoi(f.Scale)
					if e != nil || !regexpUnsigned.MatchString(f.Scale) {
						return fail("source_ineligible")
					}
				}
				maxS, maxI := 9, 29
				if typ == "BIGNUMERIC" {
					maxS, maxI = 38, 38
				}
				if s > maxS || p < max(1, s) || p > maxI+s {
					return fail("source_ineligible")
				}
			}
		}
		return nil
	}
	if len(fields) < 1 {
		return fail("source_ineligible")
	}
	return visit(fields, 0)
}
func decodeSchema(v any) ([]Field, error) {
	m, e := object(v)
	if e != nil {
		return nil, e
	}
	if e = known(m, []string{"fields"}); e != nil {
		return nil, e
	}
	var decode func(any, int) ([]Field, error)
	count := 0
	decode = func(v any, depth int) ([]Field, error) {
		a, ok := v.([]any)
		if !ok || len(a) < 1 || depth > 8 {
			return nil, fail("malformed_wire")
		}
		fs := []Field{}
		for _, x := range a {
			count++
			if count > 1024 {
				return nil, fail("response_limit")
			}
			f, e := object(x)
			if e != nil {
				return nil, e
			}
			if e = known(f, strings.Fields("name type mode fields precision scale description categories collation dataGovernanceTagsInfo dataPolicies dataPolicyList defaultValueExpression foreignTypeDefinition generatedColumn maxLength policyTags rangeElementType roundingMode timestampPrecision")); e != nil {
				return nil, e
			}
			if e = absentConfigs(f, strings.Fields("collation dataGovernanceTagsInfo dataPolicies dataPolicyList defaultValueExpression foreignTypeDefinition generatedColumn maxLength policyTags rangeElementType roundingMode timestampPrecision")); e != nil {
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
			if x, ok := f["mode"]; ok {
				mode, ok = x.(string)
				if !ok {
					return nil, fail("malformed_wire")
				}
			}
			ff := Field{Name: name, Type: typ, Mode: mode}
			for _, k := range []string{"precision", "scale"} {
				if x, ok := f[k]; ok {
					s, ok := x.(string)
					if !ok {
						return nil, fail("malformed_wire")
					}
					if k == "precision" {
						ff.Precision = s
					} else {
						ff.Scale = s
					}
				}
			}
			if nested, ok := f["fields"]; ok {
				ff.Fields, e = decode(nested, depth+1)
				if e != nil {
					return nil, e
				}
			}
			fs = append(fs, ff)
		}
		return fs, nil
	}
	fs, e := decode(m["fields"], 0)
	if e != nil {
		return nil, e
	}
	return fs, validateFields(fs)
}
func validateDataset(raw any, p SourceProfile) error {
	m, e := object(raw)
	if e != nil {
		return e
	}
	if e = known(m, datasetMetadata, datasetRejected, []string{"datasetReference", "location"}); e != nil {
		return e
	}
	if e = absentConfigs(m, datasetRejected); e != nil {
		return e
	}
	ref, e := object(m["datasetReference"])
	if e != nil {
		return e
	}
	if e = known(ref, []string{"projectId", "datasetId"}); e != nil {
		return e
	}
	if ref["projectId"] != p.SourceProject || ref["datasetId"] != p.DatasetID || m["location"] != p.Location {
		return fail("source_changed")
	}
	return nil
}
func validateTable(raw any, p SourceProfile) (Observation, error) {
	m, e := object(raw)
	if e != nil {
		return Observation{}, e
	}
	if e = known(m, tableMetadata, tableSupported, tableRejected); e != nil {
		return Observation{}, e
	}
	if e = absentConfigs(m, tableRejected); e != nil {
		return Observation{}, e
	}
	ref, e := object(m["tableReference"])
	if e != nil {
		return Observation{}, e
	}
	if e = known(ref, []string{"projectId", "datasetId", "tableId"}); e != nil {
		return Observation{}, e
	}
	if ref["projectId"] != p.SourceProject || ref["datasetId"] != p.DatasetID || ref["tableId"] != p.TableID {
		return Observation{}, fail("source_changed")
	}
	if m["type"] != "TABLE" {
		return Observation{}, fail("source_ineligible")
	}
	if l, ok := m["location"]; ok && l != p.Location {
		return Observation{}, fail("source_changed")
	}
	fs, e := decodeSchema(m["schema"])
	if e != nil {
		return Observation{}, e
	}
	if !equalJSON(fs, p.Schema) {
		return Observation{}, fail("source_changed")
	}
	config := map[string]any{}
	fieldMap := map[string]Field{}
	for _, f := range fs {
		fieldMap[f.Name] = f
	}
	for _, k := range []string{"timePartitioning", "rangePartitioning", "clustering", "requirePartitionFilter"} {
		x, ok := m[k]
		if !ok {
			continue
		}
		if x == nil {
			return Observation{}, fail("source_ineligible")
		}
		switch k {
		case "requirePartitionFilter":
			if _, ok = x.(bool); !ok {
				return Observation{}, fail("source_ineligible")
			}
		case "clustering":
			o, e := object(x)
			if e != nil {
				return Observation{}, e
			}
			if e = known(o, []string{"fields"}); e != nil {
				return Observation{}, e
			}
			a, ok := o["fields"].([]any)
			if !ok || len(a) < 1 || len(a) > 4 {
				return Observation{}, fail("source_ineligible")
			}
			seen := map[string]bool{}
			for _, v := range a {
				n, ok := v.(string)
				f, exists := fieldMap[n]
				if !ok || !exists || !ordered(f) || seen[n] {
					return Observation{}, fail("source_ineligible")
				}
				seen[n] = true
			}
		case "timePartitioning":
			if _, both := m["rangePartitioning"]; both {
				return Observation{}, fail("source_ineligible")
			}
			o, e := object(x)
			if e != nil {
				return Observation{}, e
			}
			if e = known(o, []string{"type", "field", "expirationMs", "requirePartitionFilter"}); e != nil {
				return Observation{}, e
			}
			if o["type"] != "DAY" && o["type"] != "HOUR" && o["type"] != "MONTH" && o["type"] != "YEAR" {
				return Observation{}, fail("source_ineligible")
			}
			if n, ok := o["field"]; ok {
				f, exists := fieldMap[asString(n)]
				if !exists || (f.Type != "DATE" && f.Type != "TIMESTAMP" && f.Type != "DATETIME") {
					return Observation{}, fail("source_ineligible")
				}
			}
			if _, e = decimalString(o, "expirationMs"); e != nil {
				return Observation{}, e
			}
			if v, ok := o["requirePartitionFilter"]; ok {
				if _, ok = v.(bool); !ok {
					return Observation{}, fail("source_ineligible")
				}
			}
		case "rangePartitioning":
			o, e := object(x)
			if e != nil {
				return Observation{}, e
			}
			if e = known(o, []string{"field", "range"}); e != nil {
				return Observation{}, e
			}
			f, exists := fieldMap[asString(o["field"])]
			if !exists || canonicalType(f.Type) != "INT64" {
				return Observation{}, fail("source_ineligible")
			}
			r, e := object(o["range"])
			if e != nil {
				return Observation{}, e
			}
			if e = known(r, []string{"start", "end", "interval"}); e != nil {
				return Observation{}, e
			}
			nums := []int64{}
			for _, n := range []string{"start", "end", "interval"} {
				s, ok := r[n].(string)
				if !ok || !integerText.MatchString(s) {
					return Observation{}, fail("source_ineligible")
				}
				nn, e := strconv.ParseInt(s, 10, 64)
				if e != nil {
					return Observation{}, fail("source_ineligible")
				}
				nums = append(nums, nn)
			}
			if nums[0] >= nums[1] || nums[2] < 1 {
				return Observation{}, fail("source_ineligible")
			}
		}
		config[k] = x
	}
	obs := Observation{Table: TableRef{p.SourceProject, p.DatasetID, p.TableID}, Location: p.Location, Type: "TABLE", Config: config, Schema: fs, Etag: asString(m["etag"]), LastModified: asString(m["lastModifiedTime"])}
	obs.Digest, e = digestObject("Observation", obs)
	return obs, e
}
func asString(v any) string { s, _ := v.(string); return s }

// ObserveConfigured validates metadata for a profile frozen by NewClient, selected
// by its SourceDigest (also exposed by Compile). Bounds apply to the entire
// inspection and both metadata responses. It uses authenticated datasets.get and
// tables.get only: no query preparation, dry run, job, rows, or ledger access.
// A successful observation does not approve execution or admit a public source.
func (c *Client) ObserveConfigured(ctx context.Context, sourceDigest string, bounds Bounds) (Observation, error) {
	if e := bounds.validate(); e != nil {
		return Observation{}, e
	}
	p, ok := c.profiles[sourceDigest]
	if !ok {
		return Observation{}, fail("invalid_input")
	}
	scope := newPreviewScope(bounds, c.clock.Now())
	ctx, cancel := boundedContext(ctx, time.Duration(bounds.WallMs)*time.Millisecond)
	defer cancel()
	return c.observe(ctx, p, scope, nil)
}

func (c *Client) Observe(ctx context.Context, p SourceProfile) (Observation, error) {
	if _, e := sourceDigest(p); e != nil {
		return Observation{}, e
	}
	scope := newPreviewScope(DefaultBounds(), c.clock.Now())
	return c.observe(ctx, p, scope, nil)
}
func (c *Client) observe(ctx context.Context, p SourceProfile, s *operationScope, principal *Principal) (Observation, error) {
	d, e := c.call(ctx, s, principal, false, func(api sdkAPI) error {
		_, e := api.service.Datasets.Get(p.SourceProject, p.DatasetID).Context(api.ctx).Do()
		return e
	})
	if e != nil {
		return Observation{}, e
	}
	if e = validateDataset(d, p); e != nil {
		return Observation{}, e
	}
	t, e := c.call(ctx, s, principal, false, func(api sdkAPI) error {
		_, e := api.service.Tables.Get(p.SourceProject, p.DatasetID, p.TableID).Context(api.ctx).Do()
		return e
	})
	if e != nil {
		return Observation{}, e
	}
	o, e := validateTable(t, p)
	o.ObservedAt = c.clock.Now()
	return o, e
}
func schemaDigest(fs []Field) string {
	b, _ := json.Marshal(fs)
	v, _ := ParseJSON(b, MaxResponseBytes)
	d, _ := canonical(v)
	return hashBytes(d)
}
