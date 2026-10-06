package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func discoveryFixture(t *testing.T, file string) string {
	t.Helper()
	raw, e := os.ReadFile("examples/metadata-discovery/testdata/" + file + ".json")
	if e != nil {
		t.Fatal(e)
	}
	return string(raw)
}

func discoveryFixtureClient(t *testing.T, transport http.RoundTripper) (*DiscoveryClient, *testProvider, *DiscoveryBinding, *fakeClock) {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 10, 6, 12, 13, 14, 123456789, time.UTC)}
	principal := Principal{"workload", "private-principal", "private-generation"}
	provider := &testProvider{identity: Identity{Principal: principal, ExpiresAt: clock.Now().Add(time.Hour), Read: true}}
	binding := &DiscoveryBinding{Principal: principal, PolicyRevision: "private-owner-consent-source-revision"}
	allowlist := []DiscoveryLocator{{SourceID: "fixture-source", SourceProject: "fixture-project", DatasetID: "fixture_dataset", TableID: "sample"}}
	c, e := NewDiscoveryClient(DiscoveryConfig{Allowlist: allowlist, Provider: provider, Transport: transport, Clock: clock, EvidenceKind: "synthetic-fixture", Authorize: func(context.Context, DiscoveryLocator) (DiscoveryBinding, error) { return *binding, nil }})
	if e != nil {
		t.Fatal(e)
	}
	allowlist[0].SourceProject = "caller-mutation"
	return c, provider, binding, clock
}

func TestDiscoveryPublicProjectionAndExactDispatch(t *testing.T) {
	dataset, table := discoveryFixture(t, "dataset"), discoveryFixture(t, "table")
	var requests []string
	c, _, _, _ := discoveryFixtureClient(t, rt(func(req *http.Request) (*http.Response, error) {
		requests = append(requests, req.Method+" "+req.URL.String())
		if strings.HasSuffix(req.URL.Path, "/tables/sample") {
			return response(table), nil
		}
		return response(dataset), nil
	}))
	o, e := c.Discover(context.Background(), "fixture-source", DefaultBounds())
	if e != nil {
		t.Fatal(e)
	}
	want := []string{
		"GET https://bigquery.googleapis.com/bigquery/v2/projects/fixture-project/datasets/fixture_dataset?alt=json&datasetView=METADATA&prettyPrint=false",
		"GET https://bigquery.googleapis.com/bigquery/v2/projects/fixture-project/datasets/fixture_dataset/tables/sample?alt=json&prettyPrint=false&view=STORAGE_STATS",
	}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("unexpected metadata dispatch: %v", requests)
	}
	if o.ObservedAt != "2026-10-06T12:13:14Z" || o.Location != "EU" || o.Schema[0].Mode != "NULLABLE" || o.Provenance.Kind != "synthetic-fixture" || o.Projection != "partial-public-schema" {
		t.Fatal(o)
	}
	raw, _ := json.Marshal(o)
	for _, forbidden := range []string{"PRIVATE", "private-principal", "private-generation", "private-owner", "precision", "scale", "policyTags", "rangeElementType", "etag", "numRows", `"view"`, "queryActivation"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("private/omitted property leaked: %s", forbidden)
		}
	}
	digest := o.SHA256
	o.SHA256 = ""
	raw, _ = json.Marshal(o)
	canonical, e := CanonicalJSON(raw)
	if e != nil || "sha256:"+hashBytes(canonical) != digest {
		t.Fatal("noncanonical public digest", e)
	}
	// Explicitly no query client, profile, preparer or ledger is supplied anywhere.
}

// The registry and Directory authored this identical golden public fixture.
// Production metadata dispatch/projection must reproduce its entire envelope.
func TestDiscoverySharedRegistryGolden(t *testing.T) {
	var want PublicDiscovery
	if e := json.Unmarshal([]byte(discoveryFixture(t, "public-observation")), &want); e != nil {
		t.Fatal(e)
	}
	principal := Principal{"workload", "private-fixture-principal", "1"}
	observed, e := time.Parse(time.RFC3339, want.ObservedAt)
	if e != nil {
		t.Fatal(e)
	}
	dataset, _ := json.Marshal(map[string]any{"datasetReference": map[string]string{"projectId": want.SourceProject, "datasetId": want.DatasetID}, "location": want.Location})
	table, _ := json.Marshal(map[string]any{"tableReference": map[string]string{"projectId": want.SourceProject, "datasetId": want.DatasetID, "tableId": want.TableID}, "type": want.ObjectType, "schema": map[string]any{"fields": want.Schema}})
	c, e := NewDiscoveryClient(DiscoveryConfig{
		Allowlist: []DiscoveryLocator{{want.SourceID, want.SourceProject, want.DatasetID, want.TableID}},
		Provider:  &testProvider{identity: Identity{Principal: principal, ExpiresAt: observed.Add(time.Hour), Read: true}},
		Authorize: func(context.Context, DiscoveryLocator) (DiscoveryBinding, error) {
			return DiscoveryBinding{principal, "private-fixture-policy"}, nil
		},
		Clock: &fakeClock{now: observed}, EvidenceKind: "synthetic-fixture",
		Transport: rt(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/tables/") {
				return response(string(table)), nil
			}
			return response(string(dataset)), nil
		}),
	})
	if e != nil {
		t.Fatal(e)
	}
	got, e := c.Discover(context.Background(), want.SourceID, DefaultBounds())
	if e != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("registry projection mismatch: %#v %#v %v", got, want, e)
	}
}

func TestDiscoveryDenialAndResponseChanges(t *testing.T) {
	for _, name := range []string{"unknown-source", "zero-bounds", "expired", "read-denied", "bad-principal", "cancelled", "policy-before-dispatch", "policy-after-dataset", "identity-after-dataset", "policy-after-table", "identity-after-table", "dataset-ref", "dataset-location", "table-ref", "table-location", "unknown-type", "response-bound", "cumulative-bound", "duplicate-json", "401", "403", "404", "redirect"} {
		t.Run(name, func(t *testing.T) {
			dataset, table := discoveryFixture(t, "dataset"), discoveryFixture(t, "table")
			var provider *testProvider
			var binding *DiscoveryBinding
			calls := 0
			c, p, b, clock := discoveryFixtureClient(t, rt(func(req *http.Request) (*http.Response, error) {
				calls++
				isTable := strings.HasSuffix(req.URL.Path, "/tables/sample")
				if (name == "policy-after-dataset" && !isTable) || (name == "policy-after-table" && isTable) {
					binding.PolicyRevision = "changed"
				}
				if (name == "identity-after-dataset" && !isTable) || (name == "identity-after-table" && isTable) {
					provider.identity.Principal.Generation = "changed"
				}
				body := dataset
				if isTable {
					body = table
				}
				r := response(body)
				switch name {
				case "401":
					r.StatusCode = 401
				case "403":
					r.StatusCode = 403
				case "404":
					r.StatusCode = 404
				case "redirect":
					r.StatusCode = 302
					r.Header.Set("Location", "https://untrusted.invalid")
				}
				return r, nil
			}))
			provider, binding = p, b
			source, bounds := "fixture-source", DefaultBounds()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch name {
			case "unknown-source":
				source = "unlisted"
			case "zero-bounds":
				bounds = Bounds{}
			case "expired":
				p.identity.ExpiresAt = clock.Now()
			case "read-denied":
				p.identity.Read = false
			case "bad-principal":
				p.identity.Principal.Subject = ""
			case "cancelled":
				cancel()
			case "policy-before-dispatch":
				checks := 0
				c.authorize = func(context.Context, DiscoveryLocator) (DiscoveryBinding, error) {
					checks++
					if checks >= 3 {
						return DiscoveryBinding{}, fail("policy_denied")
					}
					return *b, nil
				}
			case "dataset-ref":
				dataset = strings.ReplaceAll(dataset, "fixture_dataset", "other_dataset")
			case "dataset-location":
				dataset = strings.ReplaceAll(dataset, `"EU"`, `""`)
			case "table-ref":
				table = strings.ReplaceAll(table, `"sample"`, `"other"`)
			case "table-location":
				table = strings.ReplaceAll(table, `"EU"`, `"US"`)
			case "unknown-type":
				table = strings.ReplaceAll(table, `"TABLE"`, `"FUTURE"`)
			case "response-bound":
				bounds.ResponseBytes = len(dataset) - 1
			case "cumulative-bound":
				bounds.TotalResponseBytes = int64(len(dataset) + len(table) - 1)
			case "duplicate-json":
				dataset = `{"location":"EU","location":"US"}`
			}
			got, e := c.Discover(ctx, source, bounds)
			if e == nil || got.Format != "" {
				t.Fatal("unsafe discovery delivered", got, e)
			}
			var safe *Error
			if !errors.As(e, &safe) {
				t.Fatal("unsanitized error", e)
			}
			if calls > 2 {
				t.Fatal("unexpected additional API dispatch", calls)
			}
			if strings.HasPrefix(name, "policy-after-dataset") || name == "identity-after-dataset" {
				if calls != 1 {
					t.Fatal("table dispatched after revoked binding", calls)
				}
			}
			if name == "unknown-source" || name == "zero-bounds" || name == "expired" || name == "read-denied" || name == "bad-principal" || name == "cancelled" || name == "policy-before-dispatch" {
				if calls != 0 {
					t.Fatal("dispatch before authorization", calls)
				}
			}
		})
	}
}

type discoveryMutatingProvider struct {
	base   Provider
	mutate func(*http.Request)
}

func (p discoveryMutatingProvider) Authorize(ctx context.Context, gate http.RoundTripper) (Identity, http.RoundTripper, error) {
	i, _, e := p.base.Authorize(ctx, gate)
	return i, rt(func(req *http.Request) (*http.Response, error) {
		req = req.Clone(req.Context())
		p.mutate(req)
		return gate.RoundTrip(req)
	}), e
}

func TestDiscoveryGuardRejectsProviderRequestMutation(t *testing.T) {
	for name, mutate := range map[string]func(*http.Request){
		"jobs":        func(r *http.Request) { r.URL.Path = "/bigquery/v2/projects/fixture-project/queries"; r.Method = "POST" },
		"other-table": func(r *http.Request) { r.URL.Path += "/tables/unlisted" },
		"full-view":   func(r *http.Request) { q := r.URL.Query(); q.Set("datasetView", "FULL"); r.URL.RawQuery = q.Encode() },
		"unknown-query": func(r *http.Request) {
			q := r.URL.Query()
			q.Set("credentials", "private")
			r.URL.RawQuery = q.Encode()
		},
		"duplicate-view": func(r *http.Request) { r.URL.RawQuery += "&datasetView=METADATA" },
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			c, p, _, _ := discoveryFixtureClient(t, rt(func(*http.Request) (*http.Response, error) { calls++; return response(`{}`), nil }))
			c.provider = discoveryMutatingProvider{p, mutate}
			if _, e := c.Discover(context.Background(), "fixture-source", DefaultBounds()); e == nil || calls != 0 {
				t.Fatal("mutated request escaped exact gate", calls, e)
			}
		})
	}
}

func TestDiscoveryIdentityRotationInAuthenticationMiddleware(t *testing.T) {
	calls := 0
	c, p, _, _ := discoveryFixtureClient(t, rt(func(*http.Request) (*http.Response, error) { calls++; return response(`{}`), nil }))
	c.provider = discoveryMutatingProvider{p, func(*http.Request) { p.identity.Principal.Generation = "changed-before-dispatch" }}
	if _, e := c.Discover(context.Background(), "fixture-source", DefaultBounds()); e == nil || calls != 0 {
		t.Fatal("rotated credential dispatched", calls, e)
	}
}

func TestDiscoveryConfigAndEnvelopeBounds(t *testing.T) {
	dataset, table := discoveryFixture(t, "dataset"), discoveryFixture(t, "table")
	c, _, _, _ := discoveryFixtureClient(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/tables/") {
			return response(table), nil
		}
		return response(dataset), nil
	}))
	base := DiscoveryConfig{Allowlist: []DiscoveryLocator{c.locators["fixture-source"]}, Authorize: c.authorize, Provider: c.provider, Transport: c.transport, Clock: c.clock, EvidenceKind: "synthetic-fixture"}
	for _, mutate := range []func(*DiscoveryConfig){
		func(cfg *DiscoveryConfig) { cfg.Allowlist = nil }, func(cfg *DiscoveryConfig) { cfg.Authorize = nil }, func(cfg *DiscoveryConfig) { cfg.Provider = nil }, func(cfg *DiscoveryConfig) { cfg.Transport = nil }, func(cfg *DiscoveryConfig) { cfg.EvidenceKind = "unattested" },
		func(cfg *DiscoveryConfig) { cfg.Allowlist = append(cfg.Allowlist, cfg.Allowlist[0]) },
		func(cfg *DiscoveryConfig) {
			cfg.Allowlist = []DiscoveryLocator{{SourceID: "fixture-source", SourceProject: "fixture-project", DatasetID: "fixture_dataset", TableID: "../unlisted"}}
		},
	} {
		cfg := base
		mutate(&cfg)
		if _, e := NewDiscoveryClient(cfg); errorCode(e) != "invalid_input" {
			t.Fatal("invalid discovery configuration accepted", e)
		}
	}
	var body map[string]any
	if e := json.Unmarshal([]byte(table), &body); e != nil {
		t.Fatal(e)
	}
	fields := []any{}
	for i := 0; i < 500; i++ {
		fields = append(fields, map[string]any{"name": "f" + strings.Repeat("a", 290) + strconv.Itoa(i), "type": "STRING"})
	}
	body["schema"] = map[string]any{"fields": fields}
	raw, _ := json.Marshal(body)
	table = string(raw)
	if got, e := c.Discover(context.Background(), "fixture-source", DefaultBounds()); errorCode(e) != "response_limit" || got.Format != "" {
		t.Fatal("oversized public projection delivered", e)
	}
}

// Independent review r1 found consent withdrawal in these exact provider
// callback windows. Keep the reproductions in the normal production-path suite.
type discoveryRevokingProvider struct {
	base       *testProvider
	binding    *DiscoveryBinding
	afterTable *bool
	dispatch   bool
	revoked    bool
}

func (p *discoveryRevokingProvider) Authorize(ctx context.Context, gate http.RoundTripper) (Identity, http.RoundTripper, error) {
	if p.dispatch && !p.revoked {
		if _, ok := gate.(rejectTransport); ok {
			p.binding.PolicyRevision = "withdrawn-during-attestation"
			p.revoked = true
		}
	}
	if !p.dispatch && p.afterTable != nil && *p.afterTable {
		p.binding.PolicyRevision = "withdrawn-during-attestation"
		p.revoked = true
	}
	return p.base.Authorize(ctx, gate)
}

func TestDiscoveryConsentWithdrawalDuringDispatchAttestation(t *testing.T) {
	calls := 0
	dataset := discoveryFixture(t, "dataset")
	c, p, b, _ := discoveryFixtureClient(t, rt(func(*http.Request) (*http.Response, error) { calls++; return response(dataset), nil }))
	c.provider = &discoveryRevokingProvider{base: p, binding: b, dispatch: true}
	got, e := c.Discover(context.Background(), "fixture-source", DefaultBounds())
	if e == nil || got.Format != "" || calls != 0 {
		t.Fatal("dispatch after provider-window consent withdrawal", calls, e)
	}
}

func TestDiscoveryConsentWithdrawalDuringDeliveryAttestation(t *testing.T) {
	afterTable := false
	dataset, table := discoveryFixture(t, "dataset"), discoveryFixture(t, "table")
	c, p, b, _ := discoveryFixtureClient(t, rt(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/tables/") {
			afterTable = true
			return response(table), nil
		}
		return response(dataset), nil
	}))
	c.provider = &discoveryRevokingProvider{base: p, binding: b, afterTable: &afterTable}
	got, e := c.Discover(context.Background(), "fixture-source", DefaultBounds())
	if e == nil || got.Format != "" || !afterTable {
		t.Fatal("delivery after provider-window consent withdrawal", e)
	}
}

func TestDiscoveryRegistrySourceIDParity(t *testing.T) {
	for _, source := range []string{"fixture--source", "fixture-source-", strings.Repeat("a", 81)} {
		c, _, _, _ := discoveryFixtureClient(t, rt(func(*http.Request) (*http.Response, error) { t.Fatal("invalid source dispatched"); return nil, nil }))
		locator := c.locators["fixture-source"]
		locator.SourceID = source
		_, e := NewDiscoveryClient(DiscoveryConfig{Allowlist: []DiscoveryLocator{locator}, Authorize: c.authorize, Provider: c.provider, Transport: c.transport, Clock: c.clock, EvidenceKind: "synthetic-fixture"})
		if errorCode(e) != "invalid_input" {
			t.Fatal("registry-invalid source admitted", source, e)
		}
	}
	for _, source := range []string{"0", "fixture-123-source", strings.Repeat("a", 80)} {
		c, _, _, _ := discoveryFixtureClient(t, rt(func(*http.Request) (*http.Response, error) { return nil, nil }))
		locator := c.locators["fixture-source"]
		locator.SourceID = source
		_, e := NewDiscoveryClient(DiscoveryConfig{Allowlist: []DiscoveryLocator{locator}, Authorize: c.authorize, Provider: c.provider, Transport: c.transport, Clock: c.clock, EvidenceKind: "synthetic-fixture"})
		if e != nil {
			t.Fatal("registry-valid source rejected", source, e)
		}
	}
}

// Independent review r2 pauses the actual lower worker after upper attestation,
// so a consent change cannot be hidden by a fast, uncontended fixture transport.
type discoveryWorkerClock struct {
	base    Clock
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *discoveryWorkerClock) Now() time.Time {
	pcs := make([]uintptr, 20)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.Contains(f.Function, "(*dispatchGate).RoundTrip.func") {
			c.once.Do(func() { close(c.entered); <-c.release })
			break
		}
		if !more {
			break
		}
	}
	return c.base.Now()
}

func (c *discoveryWorkerClock) Sleep(ctx context.Context, d time.Duration) error {
	return c.base.Sleep(ctx, d)
}

func TestDiscoveryConsentWithdrawalWhilePhysicalWorkerDelayed(t *testing.T) {
	var calls atomic.Int32
	dataset := discoveryFixture(t, "dataset")
	c, _, b, clock := discoveryFixtureClient(t, rt(func(*http.Request) (*http.Response, error) { calls.Add(1); return response(dataset), nil }))
	workerClock := &discoveryWorkerClock{base: clock, entered: make(chan struct{}), release: make(chan struct{})}
	c.clock = workerClock
	var mu sync.Mutex
	c.authorize = func(context.Context, DiscoveryLocator) (DiscoveryBinding, error) {
		mu.Lock()
		defer mu.Unlock()
		return *b, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := c.Discover(ctx, "fixture-source", DefaultBounds()); done <- e }()
	select {
	case <-workerClock.entered:
	case <-ctx.Done():
		close(workerClock.release)
		t.Fatal("physical worker instrumentation not reached")
	}
	mu.Lock()
	b.PolicyRevision = "withdrawn-while-physical-worker-delayed"
	mu.Unlock()
	close(workerClock.release)
	e := <-done
	if e == nil || calls.Load() != 0 {
		t.Fatal("physical worker dispatched after withdrawal", calls.Load(), e)
	}
}

func TestDiscoveryRecursiveSchemaBounds(t *testing.T) {
	for name, schema := range map[string]any{
		"empty":                   map[string]any{"fields": []any{}},
		"case-duplicate":          map[string]any{"fields": []any{map[string]any{"name": "a", "type": "STRING"}, map[string]any{"name": "A", "type": "STRING"}}},
		"null-mode":               map[string]any{"fields": []any{map[string]any{"name": "a", "type": "STRING", "mode": nil}}},
		"scalar-children":         map[string]any{"fields": []any{map[string]any{"name": "a", "type": "STRING", "fields": []any{}}}},
		"missing-record-children": map[string]any{"fields": []any{map[string]any{"name": "a", "type": "RECORD"}}},
		"flexible-name":           map[string]any{"fields": []any{map[string]any{"name": "with space", "type": "STRING"}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, e := publicDiscoverySchema(schema); e == nil {
				t.Fatal("malformed schema accepted")
			}
		})
	}
	fields := []any{map[string]any{"name": "leaf", "type": "STRING"}}
	for i := 0; i < 9; i++ {
		fields = []any{map[string]any{"name": "nested", "type": "RECORD", "fields": fields}}
	}
	if _, e := publicDiscoverySchema(map[string]any{"fields": fields}); errorCode(e) != "response_limit" {
		t.Fatal("depth not bounded", e)
	}
	fields = nil
	for i := 0; i < 501; i++ {
		fields = append(fields, map[string]any{"name": "f" + strconv.Itoa(i), "type": "STRING"})
	}
	if _, e := publicDiscoverySchema(map[string]any{"fields": fields}); errorCode(e) != "response_limit" {
		t.Fatal("field count not bounded", e)
	}
}
