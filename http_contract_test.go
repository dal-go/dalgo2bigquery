package bigquery

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/dal-go/dalgo/dtql"
	"io"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (f *fakeClock) Now() time.Time { f.mu.Lock(); defer f.mu.Unlock(); return f.now }
func (f *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if ctx.Err() != nil {
		return fail("local_stopped")
	}
	f.advance(d)
	return nil
}
func (f *fakeClock) advance(d time.Duration) { f.mu.Lock(); defer f.mu.Unlock(); f.now = f.now.Add(d) }

type testProvider struct {
	identity Identity
	retry    bool
}

func (p *testProvider) Authorize(ctx context.Context, base http.RoundTripper) (Identity, http.RoundTripper, error) {
	return p.identity, rt(func(r *http.Request) (*http.Response, error) {
		resp, e := base.RoundTrip(r)
		if p.retry && e != nil {
			if r.Body != nil {
				raw, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(strings.NewReader(string(raw)))
			}
			return base.RoundTrip(r)
		}
		return resp, e
	}), nil
}

type httpRequestFixture struct {
	Method string            `json:"method"`
	Path   string            `json:"path"`
	Query  map[string]string `json:"query"`
	Body   string            `json:"body"`
}
type httpFixture struct {
	Request            httpRequestFixture `json:"request"`
	Status             int                `json:"status"`
	Headers            map[string]string  `json:"headers"`
	Body               string             `json:"body"`
	BodyHex            string             `json:"body_hex"`
	Gzip               bool               `json:"gzip"`
	AdvanceMS          int                `json:"advance_ms"`
	ResponseChunkBytes int                `json:"response_chunk_bytes"`
	TransportError     bool               `json:"transport_error"`
}
type actionFixture struct {
	Op                string `json:"op"`
	Values            []any  `json:"values"`
	Error             string `json:"error"`
	MS                int    `json:"ms"`
	CountersUnchanged bool   `json:"counters_unchanged"`
	MaximumContextMS  int    `json:"maximum_context_ms"`
}
type httpScenario struct {
	ExpectedRuntime map[string]struct {
		Bytes int64 `json:"bytes"`
	} `json:"expected_runtime_observations"`
	ExpectedPlan        ReadPlan        `json:"expected_plan"`
	ExpectedReservation string          `json:"expected_reservation_bytes"`
	ID                  string          `json:"id"`
	Profile             SourceProfile   `json:"profile"`
	Query               string          `json:"query"`
	Execution           Execution       `json:"execution"`
	Bounds              Bounds          `json:"bounds"`
	HTTP                []httpFixture   `json:"http"`
	Actions             []actionFixture `json:"actions"`
	ExpectedRequests    int             `json:"expected_requests"`
	ExpectedPaid        int             `json:"expected_paid_submissions"`
	ExpectedError       string          `json:"expected_error"`
	ExpectedState       string          `json:"expected_state"`
	ExpectedRetained    bool            `json:"expected_retained_reservation"`
}

var observedHTTP = struct {
	sync.Mutex
	cases map[string]any
}{cases: map[string]any{}}

func writeHTTPReport(t *testing.T) {
	if path := os.Getenv("BIGQUERY_CONTRACT_REPORT"); path != "" {
		observedHTTP.Lock()
		defer observedHTTP.Unlock()
		ids := []string{}
		for id := range observedHTTP.cases {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		cases := []any{}
		for _, id := range ids {
			cases = append(cases, observedHTTP.cases[id])
		}
		raw, e := json.MarshalIndent(cases, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, append(raw, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
}
func runHTTPScenario(t *testing.T, id string) {
	t.Helper()
	raw, e := os.ReadFile("testdata/contract/http-state-r3.json")
	if e != nil {
		t.Fatal(e)
	}
	var scenarios []httpScenario
	if e = json.Unmarshal(raw, &scenarios); e != nil {
		t.Fatal(e)
	}
	var sc httpScenario
	for _, candidate := range scenarios {
		if candidate.ID == id {
			sc = candidate
			break
		}
	}
	if sc.ID == "" {
		t.Fatal("missing http scenario", id)
	}
	q, e := dtql.Deserialize([]byte(sc.Query))
	if e != nil {
		t.Fatal(e)
	}
	plan, e := Compile(sc.Profile, q)
	if e != nil {
		t.Fatal(e)
	}
	if !equalJSON(plan, sc.ExpectedPlan) {
		t.Fatal("compiler plan differs from frozen expectation")
	}
	clock := &fakeClock{now: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)}
	provider := &testProvider{identity: Identity{sc.Execution.Principal, clock.Now().Add(time.Hour), true, true}}
	ledger := NewMemoryLedger()
	calls, paid := 0, 0
	policy := "unrestricted-explicit"
	maximumContext := 0
	requests := []any{}
	actions := []any{}
	transport := rt(func(req *http.Request) (*http.Response, error) {
		if calls >= len(sc.HTTP) {
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.Path)
		}
		fixture := sc.HTTP[calls]
		calls++
		if req.URL.Scheme != "https" || req.URL.Host != "bigquery.googleapis.com" || req.Method != fixture.Request.Method || req.URL.Path != fixture.Request.Path {
			t.Fatalf("request %d: %s %s expected %s %s", calls, req.Method, req.URL, fixture.Request.Method, fixture.Request.Path)
		}
		query := req.URL.Query()
		query.Del("alt")
		query.Del("prettyPrint")
		if len(query) != len(fixture.Request.Query) {
			t.Fatalf("query %d: %v expected %v", calls, query, fixture.Request.Query)
		}
		for k, v := range fixture.Request.Query {
			if query.Get(k) != v {
				t.Fatalf("query %d %s=%s expected %s", calls, k, query.Get(k), v)
			}
		}
		if maximumContext > 0 {
			deadline, ok := req.Context().Deadline()
			if !ok || time.Until(deadline) > time.Duration(maximumContext+50)*time.Millisecond {
				t.Fatal("control/remaining context unbounded", time.Until(deadline))
			}
		}
		var actual []byte
		if req.Body != nil {
			actual, _ = io.ReadAll(req.Body)
		}
		if fixture.Request.Body == "" {
			if len(actual) > 0 {
				t.Fatalf("unexpected body %s", actual)
			}
		} else {
			got, e := CanonicalJSON(actual)
			if e != nil {
				t.Fatal(e)
			}
			expected, e := CanonicalJSON([]byte(fixture.Request.Body))
			if e != nil {
				t.Fatal(e)
			}
			if string(got) != string(expected) {
				t.Fatalf("request %d body %s expected %s", calls, got, expected)
			}
			v, _ := ParseJSON(actual, 256<<10)
			m, _ := object(v)
			if m["dryRun"] == false {
				paid++
			}
		}
		if req.GetBody != nil || req.Header.Get("Idempotency-Key") != "" {
			t.Fatal("replayable POST")
		}
		bodyCanonical := ""
		if len(actual) > 0 {
			b, _ := CanonicalJSON(actual)
			bodyCanonical = string(b)
		}
		normalizedQuery := map[string]string{}
		for k := range query {
			normalizedQuery[k] = query.Get(k)
		}
		requests = append(requests, map[string]any{"method": req.Method, "path": req.URL.Path, "query": normalizedQuery, "body": bodyCanonical})
		if fixture.TransportError {
			return nil, errors.New("fixture connection loss sensitive")
		}
		wire := []byte(fixture.Body)
		if fixture.BodyHex != "" {
			wire, _ = hex.DecodeString(fixture.BodyHex)
		}
		if fixture.Gzip {
			var b bytes.Buffer
			gz := gzip.NewWriter(&b)
			_, _ = gz.Write(wire)
			_ = gz.Close()
			wire = b.Bytes()
		}
		clock.advance(time.Duration(fixture.AdvanceMS) * time.Millisecond)
		resp := response(string(wire))
		if fixture.ResponseChunkBytes > 0 {
			resp.Body = io.NopCloser(chunkReader{reader: strings.NewReader(string(wire)), size: fixture.ResponseChunkBytes})
		}
		resp.StatusCode = fixture.Status
		for k, v := range fixture.Headers {
			resp.Header.Set(k, v)
		}
		return resp, nil
	})
	client, e := NewClient(Config{[]SourceProfile{sc.Profile}, provider, transport, ledger, func(context.Context) (ReadPlan, string, error) { return plan, policy, nil }, clock})
	if e != nil {
		t.Fatal(e)
	}
	var heldLease func()
	var preview Preview
	var run *Run
	var cursor, oldCursor string
	var lastErr error
	var start time.Time
	for _, action := range sc.Actions {
		maximumContext = action.MaximumContextMS
		before := Counters{}
		if run != nil {
			before = run.Receipt().Counters
		}
		var err error
		var delivered any
		switch action.Op {
		case "preview":
			preview, err = client.Preview(context.Background(), plan, sc.Execution, sc.Bounds)
		case "execute":
			approval, e := client.Approve(preview, preview.ApprovalDigest)
			if e != nil {
				err = e
			} else {
				run, err = client.Execute(context.Background(), approval)
				if run != nil {
					start = run.Receipt().ExecutionDeadline
				}
			}
		case "page":
			var page Page
			page, err = run.NextPage()
			if err == nil {
				values := []any{}
				for _, row := range page.Rows {
					for _, cell := range row {
						values = append(values, cell.Value)
					}
				}
				if !reflect.DeepEqual(values, action.Values) {
					t.Fatalf("page values %v expected %v", values, action.Values)
				}
				delivered = page.Rows
				cursor = page.Cursor
			}
		case "row":
			row, rs, e := run.Next()
			err = e
			if e == nil {
				data, e := row.Data(rs)
				if e != nil {
					t.Fatal(e)
				}
				if !reflect.DeepEqual(data, action.Values) {
					t.Fatalf("row values %v expected %v", data, action.Values)
				}
				delivered = data
				cursor, err = run.Cursor()
			}
		case "advance":
			clock.advance(time.Duration(action.MS) * time.Millisecond)
		case "close":
			err = run.Close()
		case "hold_lease":
			heldLease, err = client.ledger.lease(context.Background(), run.id)
		case "release_lease":
			heldLease()
		case "rebind":
			oldCursor = cursor
			var result RebindResult
			result, err = client.RebindJob(context.Background(), run.Receipt(), cursor)
			if err == nil {
				cursor = result.Cursor
				if result.Receipt.Principal != sc.Execution.Principal {
					t.Fatal("original principal rewritten")
				}
			}
		case "restore_cursor":
			cursor = oldCursor
		case "kind":
			provider.identity.Principal.Kind = "google-user"
		case "resume":
			resumed, resumeErr := client.Resume(context.Background(), run.Receipt(), cursor)
			err = resumeErr
			if resumed != nil {
				run = resumed
			}
		case "status":
			_, err = client.Status(context.Background(), *run.Receipt().Job)
		case "cancel":
			_, err = client.CancelJob(context.Background(), *run.Receipt().Job)
		case "expire":
			provider.identity.ExpiresAt = clock.Now()
		case "scope_off":
			provider.identity.Read = false
		case "generation":
			provider.identity.Principal.Generation = "2"
		case "subject":
			provider.identity.Principal.Subject = "operator:other"
		case "policy_change":
			policy = "changed"
		case "exhaust_bytes":
			err = client.mutateRun(run.id, func(r *runRecord) error { r.Receipt.Counters.Bytes = r.Receipt.Bounds.TotalResponseBytes; return nil })
		case "tamper_cursor":
			decoded, _ := base64.RawURLEncoding.DecodeString(cursor)
			var value map[string]any
			_ = json.Unmarshal(decoded, &value)
			value["offset"] = 0
			encoded, _ := json.Marshal(value)
			cursor = base64.RawURLEncoding.EncodeToString(encoded)
		default:
			t.Fatal("unknown action", action.Op)
		}
		observed := map[string]any{"op": action.Op, "errorCode": errorCode(err)}
		if delivered != nil {
			observed["values"] = delivered
		}
		if run != nil {
			rr, _ := client.loadRun(run.id)
			observed["counters"] = rr.Receipt.Counters
			observed["state"] = rr.Receipt.State
			observed["reservationBytes"] = strconv.FormatInt(rr.Reservation, 10)
			observed["principal"] = rr.Receipt.Principal
			observed["executionDeadline"] = rr.Receipt.ExecutionDeadline
			observed["job"] = rr.Receipt.Job
			observed["schemaDigest"] = rr.Receipt.SchemaDigest
			if rr.Cursor != nil {
				value, _ := jsonCopy(*rr.Cursor)
				value.RunID = ""
				value.LedgerRef = ""
				observed["cursorLogical"] = value
			}
		}
		actions = append(actions, observed)
		if errorCode(err) != action.Error {
			t.Fatalf("action %s code=%s expected %s", action.Op, errorCode(err), action.Error)
		}
		if err != nil {
			lastErr = err
		}
		if run != nil && !start.IsZero() && !run.Receipt().ExecutionDeadline.Equal(start) {
			t.Fatal("deadline renewed")
		}
		if action.CountersUnchanged && run.Receipt().Counters != before {
			t.Fatal("resume counters changed")
		}
	}
	if calls != sc.ExpectedRequests || paid != sc.ExpectedPaid {
		t.Fatalf("requests %d/%d expected %d/%d", calls, paid, sc.ExpectedRequests, sc.ExpectedPaid)
	}
	if errorCode(lastErr) != sc.ExpectedError {
		t.Fatal("final error", lastErr)
	}
	if run != nil {
		receipt := run.Receipt()
		if expected, ok := sc.ExpectedRuntime["go"]; ok && receipt.Counters.Bytes != expected.Bytes {
			t.Fatalf("actual exposed bytes %d expected %d", receipt.Counters.Bytes, expected.Bytes)
		}
		if receipt.State != sc.ExpectedState {
			t.Fatalf("state %s expected %s", receipt.State, sc.ExpectedState)
		}
		rr, e := client.loadRun(run.id)
		if e != nil {
			t.Fatal(e)
		}
		if strconv.FormatInt(rr.Reservation, 10) != sc.ExpectedReservation {
			t.Fatalf("reservation %d expected retained=%v", rr.Reservation, sc.ExpectedRetained)
		}
		if strings.Contains(receipt.Reason, "sensitive") {
			t.Fatal("error leak")
		}
	}
	observedHTTP.Lock()
	observedHTTP.cases[id] = map[string]any{"id": id, "plan": plan, "actions": actions, "requests": requests, "paidSubmissions": paid}
	observedHTTP.Unlock()
}

type chunkReader struct {
	reader io.Reader
	size   int
}

func (r chunkReader) Read(p []byte) (int, error) {
	if len(p) > r.size {
		p = p[:r.size]
	}
	return r.reader.Read(p)
}
