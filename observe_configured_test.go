package bigquery

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const observedDataset = `{"datasetReference":{"projectId":"source-project","datasetId":"ds"},"location":"EU"}`
const observedTable = `{"tableReference":{"projectId":"source-project","datasetId":"ds","tableId":"tbl"},"type":"TABLE","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`

// Metadata inspection must not read, reserve, or mutate the query ledger.
type forbiddenObservationLedger struct{ Ledger }

func (forbiddenObservationLedger) update(func(*ledgerState) error) error {
	panic("metadata updated ledger")
}
func (forbiddenObservationLedger) updateContext(context.Context, func(*ledgerState) error) error {
	panic("metadata updated ledger")
}
func (forbiddenObservationLedger) readContext(context.Context, func(*ledgerState) error) error {
	panic("metadata read ledger")
}
func (forbiddenObservationLedger) lease(context.Context, string) (func(), error) {
	panic("metadata leased ledger")
}

func configuredObservationClient(t *testing.T, transport rt) (*Client, *testProvider, *fakeClock, string) {
	t.Helper()
	p := testProfile(t)
	plan, e := Compile(p, testQuery())
	if e != nil {
		t.Fatal(e)
	}
	clock := &fakeClock{now: time.Now()}
	provider := &testProvider{identity: Identity{Principal{"workload", "operator:fixture", "1"}, clock.Now().Add(time.Hour), true, false}}
	c, e := NewClient(Config{
		Profiles: []SourceProfile{p}, Provider: provider, Transport: transport,
		Ledger: forbiddenObservationLedger{}, Clock: clock,
		Prepare: func(context.Context) (ReadPlan, string, error) { panic("metadata prepared query") },
	})
	if e != nil {
		t.Fatal(e)
	}
	// NewClient owns a frozen profile, including its native schema.
	p.Schema[0].Name = "caller-mutated"
	return c, provider, clock, plan.SourceDigest
}

func metadataResponse(req *http.Request) (*http.Response, error) {
	if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
		return response(observedTable), nil
	}
	return response(observedDataset), nil
}

func TestObserveConfiguredMetadataOnly(t *testing.T) {
	var requests []string
	c, _, clock, digest := configuredObservationClient(t, func(req *http.Request) (*http.Response, error) {
		if req.Method != "GET" || req.URL.Host != "bigquery.googleapis.com" || req.Body != nil {
			return nil, fail("invalid_input")
		}
		requests = append(requests, req.URL.Path)
		return metadataResponse(req)
	})
	got, e := c.ObserveConfigured(context.Background(), digest, DefaultBounds())
	if e != nil {
		t.Fatal(e)
	}
	want := []string{"/bigquery/v2/projects/source-project/datasets/ds", "/bigquery/v2/projects/source-project/datasets/ds/tables/tbl"}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("unexpected dispatch (jobs/tabledata forbidden): %v", requests)
	}
	if got.Digest == "" || got.Type != "TABLE" || got.Location != "EU" || got.Schema[0].Name != "n" || !got.ObservedAt.Equal(clock.Now()) {
		t.Fatal("invalid native metadata observation", got)
	}
}

func TestObserveConfiguredRejectsBeforeDispatch(t *testing.T) {
	for _, name := range []string{"unknown-source", "zero-bounds", "oversized-bounds", "expired", "read-denied", "invalid-principal", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			c, provider, clock, digest := configuredObservationClient(t, func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				return metadataResponse(req)
			})
			bounds := DefaultBounds()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := "invalid_input"
			switch name {
			case "unknown-source":
				digest = "unconfigured"
			case "zero-bounds":
				bounds = Bounds{}
			case "oversized-bounds":
				bounds.HTTPMs = 15001
			case "expired":
				provider.identity.ExpiresAt = clock.Now()
				want = "auth_expired"
			case "read-denied":
				provider.identity.Read = false
				want = "scope_missing"
			case "invalid-principal":
				provider.identity.Principal.Subject = ""
				want = "auth_required"
			case "cancelled":
				cancel()
				want = "local_stopped"
			}
			got, e := c.ObserveConfigured(ctx, digest, bounds)
			if codeOf(e) != want || got.Digest != "" || calls.Load() != 0 {
				t.Fatalf("observation=%v error=%v dispatches=%d", got, e, calls.Load())
			}
		})
	}
}

func TestObserveConfiguredResponseBounds(t *testing.T) {
	for _, cumulative := range []bool{false, true} {
		t.Run(map[bool]string{false: "per-response", true: "cumulative"}[cumulative], func(t *testing.T) {
			var calls atomic.Int32
			c, _, _, digest := configuredObservationClient(t, func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				return metadataResponse(req)
			})
			bounds := DefaultBounds()
			wantCalls := int32(1)
			if cumulative {
				bounds.TotalResponseBytes = int64(len(observedDataset) + len(observedTable) - 1)
				wantCalls = 2
			} else {
				bounds.ResponseBytes = len(observedDataset) - 1
			}
			got, e := c.ObserveConfigured(context.Background(), digest, bounds)
			if codeOf(e) != "response_limit" || got.Digest != "" || calls.Load() != wantCalls {
				t.Fatalf("observation=%v error=%v dispatches=%d", got, e, calls.Load())
			}
		})
	}
}

func TestObserveConfiguredWallDeadlineSpansRequests(t *testing.T) {
	var calls atomic.Int32
	var clock *fakeClock
	c, _, fake, digest := configuredObservationClient(t, func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		clock.advance(2 * time.Second)
		return metadataResponse(req)
	})
	clock = fake
	bounds := DefaultBounds()
	bounds.WallMs = 3000
	got, e := c.ObserveConfigured(context.Background(), digest, bounds)
	if codeOf(e) != "local_stopped" || got.Digest != "" || calls.Load() != 2 {
		t.Fatalf("wall bound reset between requests: observation=%v error=%v dispatches=%d", got, e, calls.Load())
	}
}

func TestObserveConfiguredHTTPDeadline(t *testing.T) {
	var calls atomic.Int32
	c, _, _, digest := configuredObservationClient(t, func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	bounds := DefaultBounds()
	bounds.HTTPMs = 100
	got, e := c.ObserveConfigured(context.Background(), digest, bounds)
	if codeOf(e) != "local_stopped" || got.Digest != "" || calls.Load() != 1 {
		t.Fatalf("HTTP bound ignored: observation=%v error=%v dispatches=%d", got, e, calls.Load())
	}
}

func TestObserveConfiguredRejectsChangedMetadata(t *testing.T) {
	for _, name := range []string{"location", "schema", "view"} {
		t.Run(name, func(t *testing.T) {
			c, _, _, digest := configuredObservationClient(t, func(req *http.Request) (*http.Response, error) {
				if name == "location" && strings.HasSuffix(req.URL.Path, "/datasets/ds") {
					return response(strings.Replace(observedDataset, `"EU"`, `"US"`, 1)), nil
				}
				if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
					raw := observedTable
					if name == "schema" {
						raw = strings.Replace(raw, `"INTEGER"`, `"STRING"`, 1)
					} else if name == "view" {
						raw = strings.Replace(raw, `"TABLE"`, `"VIEW"`, 1)
					}
					return response(raw), nil
				}
				return metadataResponse(req)
			})
			got, e := c.ObserveConfigured(context.Background(), digest, DefaultBounds())
			want := "source_changed"
			if name == "view" {
				want = "source_ineligible"
			}
			if codeOf(e) != want || got.Digest != "" {
				t.Fatal("metadata mismatch accepted", got, e)
			}
		})
	}
}

func TestObserveConfiguredRechecksAuthentication(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "read-denied", true: "expired"}[expired], func(t *testing.T) {
			var calls atomic.Int32
			var provider *testProvider
			var clock *fakeClock
			c, auth, fake, digest := configuredObservationClient(t, func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				if expired {
					provider.identity.ExpiresAt = clock.Now()
				} else {
					provider.identity.Read = false
				}
				return metadataResponse(req)
			})
			provider, clock = auth, fake
			got, e := c.ObserveConfigured(context.Background(), digest, DefaultBounds())
			want := "scope_missing"
			if expired {
				want = "auth_expired"
			}
			if codeOf(e) != want || got.Digest != "" || calls.Load() != 1 {
				t.Fatalf("second GET bypassed auth: observation=%v error=%v dispatches=%d", got, e, calls.Load())
			}
		})
	}
}

func TestObserveRetainsCallerProfileBehavior(t *testing.T) {
	var calls atomic.Int32
	c, _, _, _ := configuredObservationClient(t, func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		return metadataResponse(req)
	})
	p := testProfile(t)
	p.SourceID = "other-reviewed-source"
	digest, e := sourceDigest(p)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.ObserveConfigured(context.Background(), digest, DefaultBounds()); codeOf(e) != "invalid_input" || calls.Load() != 0 {
		t.Fatal("unconfigured profile accepted", e, calls.Load())
	}
	got, e := c.Observe(context.Background(), p)
	if e != nil || got.Digest == "" || calls.Load() != 2 {
		t.Fatal("existing Observe behavior changed", got, e, calls.Load())
	}
}
