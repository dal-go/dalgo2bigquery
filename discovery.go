package bigquery

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// DiscoveryLocator is a protected, exact metadata target, never an execution
// profile. NewDiscoveryClient copies the allowlist; Discover accepts only its ID.
type DiscoveryLocator struct {
	SourceID      string `json:"source_id"`
	SourceProject string `json:"source_project"`
	DatasetID     string `json:"dataset_id"`
	TableID       string `json:"table_id"`
}

// DiscoveryBinding is private operator-attested state. PolicyRevision must bind
// the current owner, consent, selected source and session; it is never published.
type DiscoveryBinding struct {
	Principal      Principal
	PolicyRevision string
}

// DiscoveryAuthorize reads protected current state, not request claims. It must
// reject absent consent, sign-out or disallowed targets and honor cancellation.
type DiscoveryAuthorize func(context.Context, DiscoveryLocator) (DiscoveryBinding, error)

type DiscoveryConfig struct {
	Allowlist []DiscoveryLocator
	Authorize DiscoveryAuthorize
	Provider  Provider
	Transport http.RoundTripper
	Clock     Clock
	// EvidenceKind describes the trusted transport's origin, not an admission.
	// Only provider-metadata and synthetic-fixture are supported.
	EvidenceKind string
}

// DiscoveryClient has no SourceProfile, query preparer, ledger or job methods.
type DiscoveryClient struct {
	locators     map[string]DiscoveryLocator
	authorize    DiscoveryAuthorize
	provider     Provider
	transport    http.RoundTripper
	clock        Clock
	evidenceKind string
}

// PublicSchemaField is intentionally separate from executable Field. Optional
// descriptors, descriptions and security properties are never copied.
type PublicSchemaField struct {
	Name   string              `json:"name"`
	Type   string              `json:"type"`
	Mode   string              `json:"mode"`
	Fields []PublicSchemaField `json:"fields,omitempty"`
}

type DiscoveryProvenance struct {
	Kind     string `json:"kind"`
	Method   string `json:"method"`
	Verifier string `json:"verifier"`
}

// PublicDiscovery is an observation-only, partial registry projection. It
// contains no execution, rights, cost, retention or admission fields. SHA256
// hashes canonical JSON of this envelope with only the sha256 property omitted.
type PublicDiscovery struct {
	Format        string              `json:"format"`
	SourceID      string              `json:"source_id"`
	SourceProject string              `json:"source_project"`
	DatasetID     string              `json:"dataset_id"`
	TableID       string              `json:"table_id"`
	Location      string              `json:"location"`
	ObjectType    string              `json:"object_type"`
	ObservedAt    string              `json:"observed_at"`
	Schema        []PublicSchemaField `json:"schema"`
	Projection    string              `json:"projection"`
	Provenance    DiscoveryProvenance `json:"provenance"`
	SHA256        string              `json:"sha256,omitempty"`
}

var discoverySourceID = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
var discoveryFieldID = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,299}$`)
var discoveryLocation = regexp.MustCompile(`^(US|EU|[a-z][a-z0-9]*(-[a-z0-9]+)+)$`)

func NewDiscoveryClient(cfg DiscoveryConfig) (*DiscoveryClient, error) {
	if len(cfg.Allowlist) < 1 || len(cfg.Allowlist) > 16 || cfg.Authorize == nil || cfg.Provider == nil || cfg.Transport == nil || (cfg.EvidenceKind != "provider-metadata" && cfg.EvidenceKind != "synthetic-fixture") {
		return nil, fail("invalid_input")
	}
	c := &DiscoveryClient{locators: map[string]DiscoveryLocator{}, authorize: cfg.Authorize, provider: cfg.Provider, transport: cfg.Transport, clock: cfg.Clock, evidenceKind: cfg.EvidenceKind}
	if c.clock == nil {
		c.clock = realClock{}
	}
	for _, l := range cfg.Allowlist {
		if !discoverySourceID.MatchString(l.SourceID) || len(l.SourceID) > 80 || !projectID.MatchString(l.SourceProject) || !identifier.MatchString(l.DatasetID) || len(l.DatasetID) > 1024 || !identifier.MatchString(l.TableID) || len(l.TableID) > 1024 {
			return nil, fail("invalid_input")
		}
		if _, exists := c.locators[l.SourceID]; exists {
			return nil, fail("invalid_input")
		}
		c.locators[l.SourceID] = l
	}
	return c, nil
}

func (c *DiscoveryClient) binding(ctx context.Context, l DiscoveryLocator) (DiscoveryBinding, error) {
	b, e := awaitDependency(ctx, func() (DiscoveryBinding, error) { return c.authorize(ctx, l) }, nil)
	if e != nil {
		return DiscoveryBinding{}, sanitizedAuth(e)
	}
	if !validPrincipal(b.Principal) || b.PolicyRevision == "" || len(b.PolicyRevision) > 1024 {
		return DiscoveryBinding{}, fail("auth_required")
	}
	return b, nil
}

// Discover gets one dataset's METADATA and one exact table's STORAGE_STATS.
// The existing guarded SDK transport supplies decoded per-response/cumulative
// byte, retry and deadline bounds. It does not cap encoded gzip wire bytes.
// No API credential is discovered.
func (c *DiscoveryClient) Discover(ctx context.Context, sourceID string, bounds Bounds) (PublicDiscovery, error) {
	if e := bounds.validate(); e != nil {
		return PublicDiscovery{}, e
	}
	l, ok := c.locators[sourceID]
	if !ok || !discoverySourceID.MatchString(sourceID) || len(sourceID) > 80 {
		return PublicDiscovery{}, fail("invalid_input")
	}
	ctx, cancel := boundedContext(ctx, time.Duration(bounds.WallMs)*time.Millisecond)
	defer cancel()
	scope := newPreviewScope(bounds, c.clock.Now())
	ctx = context.WithValue(ctx, clockBoundKey{}, scope.deadline)
	b, e := c.binding(ctx, l)
	if e != nil {
		return PublicDiscovery{}, e
	}
	provider := discoveryProvider{client: c, locator: l, binding: b}
	core := &Client{provider: provider, transport: discoveryPhysicalTransport{base: c.transport, provider: provider}, clock: c.clock}
	d, e := core.call(ctx, scope, &b.Principal, false, func(api sdkAPI) error {
		_, e := api.service.Datasets.Get(l.SourceProject, l.DatasetID).DatasetView("METADATA").Context(api.ctx).Do()
		return e
	})
	if e != nil {
		return PublicDiscovery{}, e
	}
	if e = core.checkIdentity(ctx, b.Principal); e != nil {
		return PublicDiscovery{}, e
	}
	dataset, e := object(d)
	if e != nil {
		return PublicDiscovery{}, e
	}
	ref, e := object(dataset["datasetReference"])
	if e != nil {
		return PublicDiscovery{}, e
	}
	if ref["projectId"] != l.SourceProject || ref["datasetId"] != l.DatasetID {
		return PublicDiscovery{}, fail("source_changed")
	}
	location, e := requiredString(dataset, "location")
	if e != nil || !discoveryLocation.MatchString(location) || len(location) > 64 {
		return PublicDiscovery{}, fail("malformed_wire")
	}
	t, e := core.call(ctx, scope, &b.Principal, false, func(api sdkAPI) error {
		_, e := api.service.Tables.Get(l.SourceProject, l.DatasetID, l.TableID).View("STORAGE_STATS").Context(api.ctx).Do()
		return e
	})
	if e != nil {
		return PublicDiscovery{}, e
	}
	table, e := object(t)
	if e != nil {
		return PublicDiscovery{}, e
	}
	ref, e = object(table["tableReference"])
	if e != nil {
		return PublicDiscovery{}, e
	}
	if ref["projectId"] != l.SourceProject || ref["datasetId"] != l.DatasetID || ref["tableId"] != l.TableID {
		return PublicDiscovery{}, fail("source_changed")
	}
	if v, present := table["location"]; present && v != location {
		return PublicDiscovery{}, fail("source_changed")
	}
	typ, e := requiredString(table, "type")
	if e != nil {
		return PublicDiscovery{}, e
	}
	switch typ {
	case "TABLE", "VIEW", "EXTERNAL", "MATERIALIZED_VIEW", "SNAPSHOT":
	default:
		return PublicDiscovery{}, fail("source_ineligible")
	}
	schema, e := publicDiscoverySchema(table["schema"])
	if e != nil {
		return PublicDiscovery{}, e
	}
	o := PublicDiscovery{Format: "ovdb-bigquery-observation/draft-1", SourceID: l.SourceID, SourceProject: l.SourceProject, DatasetID: l.DatasetID, TableID: l.TableID, Location: location, ObjectType: typ, ObservedAt: c.clock.Now().UTC().Truncate(time.Second).Format(time.RFC3339), Schema: schema, Projection: "partial-public-schema", Provenance: DiscoveryProvenance{Kind: c.evidenceKind, Method: "datasets.get+tables.get", Verifier: "public-projection-review-v1"}}
	raw, e := json.Marshal(o)
	if e != nil {
		return PublicDiscovery{}, fail("invalid_input")
	}
	canonical, e := CanonicalJSON(raw)
	if e != nil {
		return PublicDiscovery{}, e
	}
	o.SHA256 = "sha256:" + hashBytes(canonical)
	raw, e = json.Marshal(o)
	if e != nil || len(raw) > 64<<10 {
		return PublicDiscovery{}, fail("response_limit")
	}
	// Re-read both protected consent/source state and provider identity immediately
	// before delivery, including changes while a response was in flight.
	if e = core.checkIdentity(ctx, b.Principal); e != nil {
		return PublicDiscovery{}, e
	}
	return o, nil
}

type discoveryProvider struct {
	client  *DiscoveryClient
	locator DiscoveryLocator
	binding DiscoveryBinding
}

func (p discoveryProvider) check(ctx context.Context) error {
	b, e := p.client.binding(ctx, p.locator)
	if e != nil {
		return e
	}
	if b != p.binding {
		return fail("approval_changed")
	}
	return nil
}

func (p discoveryProvider) Authorize(ctx context.Context, gate http.RoundTripper) (Identity, http.RoundTripper, error) {
	if e := p.check(ctx); e != nil {
		return Identity{}, nil, e
	}
	i, transport, e := p.client.provider.Authorize(ctx, discoveryGate{base: gate, provider: p})
	if e != nil {
		return Identity{}, nil, e
	}
	// A provider call can outlive a consent change. Re-read protected state
	// after it returns, including the final reject-only delivery attestation.
	if e = p.check(ctx); e != nil {
		return Identity{}, nil, e
	}
	return i, transport, nil
}

type discoveryGate struct {
	base     http.RoundTripper
	provider discoveryProvider
}

// This wrapper runs inside the lower admitted physical worker, after its queue
// and deadline checks. No scheduling boundary follows the final protected-state
// read before the trusted transport is invoked.
type discoveryPhysicalTransport struct {
	base     http.RoundTripper
	provider discoveryProvider
}

func (t discoveryPhysicalTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if e := t.provider.attest(req.Context()); e != nil {
		return nil, e
	}
	return t.base.RoundTrip(req)
}

func (p discoveryProvider) attest(ctx context.Context) error {
	identity := &Client{provider: p.client.provider, clock: p.client.clock}
	if e := identity.checkIdentity(ctx, p.binding.Principal); e != nil {
		return e
	}
	return p.check(ctx)
}

func (g discoveryGate) RoundTrip(req *http.Request) (*http.Response, error) {
	l := g.provider.locator
	path := "/bigquery/v2/projects/" + l.SourceProject + "/datasets/" + l.DatasetID
	q := req.URL.Query()
	if req.Method != "GET" || req.Body != nil || req.URL.Scheme != "https" || req.URL.Host != "bigquery.googleapis.com" || req.URL.User != nil || req.URL.Fragment != "" || req.URL.EscapedPath() != req.URL.Path {
		return nil, fail("invalid_input")
	}
	if req.URL.Path == path {
		if q.Get("datasetView") != "METADATA" {
			return nil, fail("invalid_input")
		}
	} else if req.URL.Path == path+"/tables/"+l.TableID {
		if q.Get("view") != "STORAGE_STATS" {
			return nil, fail("invalid_input")
		}
	} else {
		return nil, fail("invalid_input")
	}
	for k, values := range q {
		if len(values) != 1 || (k != "datasetView" && k != "view" && k != "alt" && k != "prettyPrint") {
			return nil, fail("invalid_input")
		}
		if (k == "alt" && values[0] != "json") || (k == "prettyPrint" && values[0] != "false") || (k == "view" && req.URL.Path == path) || (k == "datasetView" && req.URL.Path != path) {
			return nil, fail("invalid_input")
		}
	}
	if e := g.provider.check(req.Context()); e != nil {
		return nil, e
	}
	// Authentication middleware can delay or rotate its credential after the
	// outer authorization. Re-attest current identity with a reject-only gate;
	// this check cannot itself perform a metadata or job request.
	if e := g.provider.attest(req.Context()); e != nil {
		return nil, e
	}
	return g.base.RoundTrip(req)
}

func publicDiscoverySchema(v any) ([]PublicSchemaField, error) {
	m, e := object(v)
	if e != nil {
		return nil, e
	}
	count := 0
	var decode func(any, int) ([]PublicSchemaField, error)
	decode = func(v any, depth int) ([]PublicSchemaField, error) {
		a, ok := v.([]any)
		if !ok || len(a) == 0 {
			return nil, fail("malformed_wire")
		}
		if depth > 8 {
			return nil, fail("response_limit")
		}
		seen := map[string]bool{}
		fields := make([]PublicSchemaField, 0, min(len(a), 500))
		for _, item := range a {
			count++
			if count > 500 {
				return nil, fail("response_limit")
			}
			f, e := object(item)
			if e != nil {
				return nil, e
			}
			name, e := requiredString(f, "name")
			if e != nil || !discoveryFieldID.MatchString(name) || seen[strings.ToLower(name)] {
				return nil, fail("malformed_wire")
			}
			seen[strings.ToLower(name)] = true
			typ, e := requiredString(f, "type")
			if e != nil {
				return nil, e
			}
			switch typ {
			case "STRING", "BYTES", "INTEGER", "INT64", "FLOAT", "FLOAT64", "BOOLEAN", "BOOL", "TIMESTAMP", "DATE", "TIME", "DATETIME", "GEOGRAPHY", "NUMERIC", "BIGNUMERIC", "JSON", "RECORD", "STRUCT", "RANGE":
			default:
				return nil, fail("source_ineligible")
			}
			mode := "NULLABLE"
			if v, present := f["mode"]; present {
				mode, ok = v.(string)
				if !ok {
					return nil, fail("malformed_wire")
				}
			}
			if mode != "NULLABLE" && mode != "REQUIRED" && mode != "REPEATED" {
				return nil, fail("malformed_wire")
			}
			field := PublicSchemaField{Name: name, Type: typ, Mode: mode}
			if typ == "RECORD" || typ == "STRUCT" {
				field.Fields, e = decode(f["fields"], depth+1)
				if e != nil {
					return nil, e
				}
			} else if _, present := f["fields"]; present {
				return nil, fail("malformed_wire")
			}
			fields = append(fields, field)
		}
		return fields, nil
	}
	return decode(m["fields"], 1)
}
