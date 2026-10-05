package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testProfile(t *testing.T) SourceProfile {
	t.Helper()
	raw, _ := os.ReadFile("testdata/contract/http-state-r3.json")
	var scenarios []httpScenario
	_ = json.Unmarshal(raw, &scenarios)
	return scenarios[0].Profile
}
func testQuery() dal.StructuredQuery {
	return dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "", nil))).Limit(100).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
}
func TestCompilerWholeQueryGuard(t *testing.T) {
	p := testProfile(t)
	base := func() dal.IQueryBuilder {
		return dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "", nil))).Limit(100)
	}
	tests := map[string]dal.Query{
		"star": base().SelectIntoRecordset(), "alias": base().SelectColumns(dal.Column{Alias: "alias", Expression: dal.NewFieldRef("", "n")}),
		"computed": base().SelectColumns(dal.CountAs(dal.NewFieldRef("", "n"), "count")), "nested-path": base().SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n.x")}),
		"qualified": base().SelectColumns(dal.Column{Expression: dal.NewFieldRef("src", "n")}), "group": base().GroupBy(dal.NewFieldRef("", "n")).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}),
		"offset": base().Offset(1).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}), "start": base().StartFrom("start").SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}), "start-after": base().StartAfter("after").SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}),
		"limit-missing":    dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "", nil))).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}),
		"null-comparison":  base().Where(dal.NewComparison(dal.NewFieldRef("", "n"), dal.Equal, dal.Constant{Value: nil})).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}),
		"unknown-operator": base().Where(dal.NewComparison(dal.NewFieldRef("", "n"), "LIKE", dal.String("s"))).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")}),
	}
	for id, q := range tests {
		t.Run(id, func(t *testing.T) {
			if e := ValidateQuery(q); errorCode(e) != "unsupported_query" {
				t.Fatal(e)
			}
			if _, e := Compile(p, q); errorCode(e) != "unsupported_query" {
				t.Fatal(e)
			}
		})
	}
	for _, op := range []dal.Operator{dal.Equal, "!=", dal.LessThen, dal.LessOrEqual, dal.GreaterThen, dal.GreaterOrEqual} {
		q := base().Where(dal.NewComparison(dal.NewFieldRef("", "n"), op, dal.String("9223372036854775807"))).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
		plan, e := Compile(p, q)
		if e != nil || !strings.Contains(plan.SQL, "@p0") || plan.Parameters[0].Value != "9223372036854775807" {
			t.Fatal(plan, e)
		}
	}
	empty := base().Where(dal.NewComparison(dal.NewFieldRef("", "n"), dal.In, dal.Constant{Value: []string{}})).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	plan, e := Compile(p, empty)
	if e != nil || !strings.Contains(plan.SQL, " WHERE FALSE ") || plan.Parameters[0].Type != "ARRAY<INT64>" {
		t.Fatal(plan, e)
	}
	null := base().Where(dal.NewFieldRef("", "n").IsNull()).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	plan, e = Compile(p, null)
	if e != nil || !strings.Contains(plan.SQL, "`n` IS NULL") {
		t.Fatal(plan, e)
	}
	for _, v := range []any{int64(9007199254740993), 1.0, []any{"1", nil}} {
		q := base().Where(dal.NewComparison(dal.NewFieldRef("", "n"), func() dal.Operator {
			if _, ok := v.([]any); ok {
				return dal.In
			}
			return dal.Equal
		}(), dal.Constant{Value: v})).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
		if _, e := Compile(p, q); e == nil {
			t.Fatal("unsafe parameter accepted")
		}
	}
}
func TestFieldPrecisionAndDiscovery(t *testing.T) {
	for _, field := range []Field{{Name: "n", Type: "NUMERIC", Mode: "NULLABLE", Precision: "5", Scale: "2"}, {Name: "n", Type: "BIGNUMERIC", Mode: "NULLABLE", Precision: "5", Scale: "2"}} {
		if _, e := NormalizeScalar(field, "123.45"); e != nil {
			t.Fatal(e)
		}
		for _, s := range []string{"1234.56", "1.234"} {
			if _, e := NormalizeScalar(field, s); errorCode(e) != "unsupported_value" {
				t.Fatal(s, e)
			}
		}
	}
	profile := testProfile(t)
	profile.Schema = append(profile.Schema, Field{Name: "nested", Type: "RECORD", Mode: "NULLABLE", Fields: []Field{{Name: "child", Type: "STRING", Mode: "NULLABLE"}}}, Field{Name: "geo", Type: "GEOGRAPHY", Mode: "NULLABLE"})
	if _, e := sourceDigest(profile); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"nested", "geo"} {
		q := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "", nil))).Limit(10).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", name)})
		if _, e := Compile(profile, q); errorCode(e) != "unsupported_type" {
			t.Fatal(e)
		}
	}
}
func TestTypedNullEmptyParameterSerialization(t *testing.T) {
	plan := ReadPlan{Parameters: []Parameter{{"p0", "STRING", nil}, {"p1", "STRING", ""}, {"p2", "ARRAY<INT64>", []any{}}}, SQL: "internally compiled"}
	req, e := queryRequest(plan, Execution{MaximumBytesBilled: "1"}, DefaultBounds(), "EU", false)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(req)
	if e != nil {
		t.Fatal(e)
	}
	v, _ := ParseJSON(raw, MaxResponseBytes)
	m, _ := object(v)
	p := m["queryParameters"].([]any)
	for i, expected := range []any{nil, "", []any{}} {
		value := p[i].(map[string]any)["parameterValue"].(map[string]any)
		key := "value"
		if i == 2 {
			key = "arrayValues"
		}
		actual, exists := value[key]
		if !exists || !equalJSON(actual, expected) {
			t.Fatalf("lost null/empty %s", raw)
		}
	}
}
func dynamicClient(t *testing.T, ledger Ledger, budget string) (*Client, *testProvider, *fakeClock, *int) {
	t.Helper()
	p := testProfile(t)
	plan, e := Compile(p, testQuery())
	if e != nil {
		t.Fatal(e)
	}
	clock := &fakeClock{now: time.Now()}
	identity := &testProvider{identity: Identity{Principal{"workload", "operator:fixture", "1"}, clock.Now().Add(time.Hour), true, true}}
	var mu sync.Mutex
	paid := 0
	transport := rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/ds") {
			return response(`{"datasetReference":{"projectId":"source-project","datasetId":"ds"},"location":"EU"}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
			return response(`{"tableReference":{"projectId":"source-project","datasetId":"ds","tableId":"tbl"},"type":"TABLE","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`), nil
		}
		if req.Method == "POST" {
			var m map[string]any
			_ = json.NewDecoder(req.Body).Decode(&m)
			if m["dryRun"] == true {
				return response(`{"totalBytesProcessed":"100"}`), nil
			}
			mu.Lock()
			paid++
			mu.Unlock()
			return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"rows":[{"f":[{"v":"1"}]},{"f":[{"v":"2"}]}]}`), nil
		}
		return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"rows":[{"f":[{"v":"1"}]},{"f":[{"v":"2"}]}]}`), nil
	})
	c, e := NewClient(Config{[]SourceProfile{p}, identity, transport, ledger, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	if e != nil {
		t.Fatal(e)
	}
	_ = budget
	return c, identity, clock, &paid
}
func previewFor(t *testing.T, c *Client, budget string) Preview {
	t.Helper()
	plan, _, _ := c.prepare(context.Background())
	p, e := c.Preview(context.Background(), plan, Execution{"job-project", Principal{"workload", "operator:fixture", "1"}, "1000", budget}, DefaultBounds())
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func TestFileLedgerConcurrentReservationAndNonce(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private-ledger")
	l1, e := NewFileLedger(dir)
	if e != nil {
		t.Fatal(e)
	}
	l2, e := NewFileLedger(dir)
	if e != nil {
		t.Fatal(e)
	}
	c1, _, _, paid1 := dynamicClient(t, l1, "1000")
	c2, _, _, paid2 := dynamicClient(t, l2, "1000")
	p1 := previewFor(t, c1, "1000")
	p2 := previewFor(t, c2, "1000")
	a1, _ := c1.Approve(p1, p1.ApprovalDigest)
	a2, _ := c2.Approve(p2, p2.ApprovalDigest)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, entry := range []struct {
		c *Client
		a Approval
	}{{c1, a1}, {c2, a2}} {
		wg.Add(1)
		go func(c *Client, a Approval) { defer wg.Done(); _, e := c.Execute(context.Background(), a); errs <- e }(entry.c, entry.a)
	}
	wg.Wait()
	close(errs)
	success, exhausted := 0, 0
	for e := range errs {
		if e == nil {
			success++
		} else if errorCode(e) == "budget_exhausted" {
			exhausted++
		} else {
			t.Fatal(e)
		}
	}
	if success != 1 || exhausted != 1 || *paid1+*paid2 != 1 {
		t.Fatal(success, exhausted, *paid1, *paid2)
	}
	state := emptyLedger()
	if e = l1.update(func(s *ledgerState) error { state = *s; return nil }); e != nil {
		t.Fatal(e)
	}
	if len(state.Runs) != 1 {
		t.Fatal("reservation lost")
	}
	_, e = os.Stat(filepath.Join(dir, "session.json"))
	if e != nil || !ledgerPrivatePath(filepath.Join(dir, "session.json"), false) {
		t.Fatal("private mode")
	}
	for _, pr := range state.Previews {
		if pr.Used {
			var c *Client
			var a Approval
			if pr.Preview.Nonce == p1.Nonce {
				c, a = c1, a1
			} else {
				c, a = c2, a2
			}
			if _, e = c.Execute(context.Background(), a); errorCode(e) != "approval_required" {
				t.Fatal("nonce reused", e)
			}
		}
	}
}
func TestFileLedgerPartialResumeAcrossClient(t *testing.T) {
	l, e := NewFileLedger(filepath.Join(t.TempDir(), "private-ledger"))
	if e != nil {
		t.Fatal(e)
	}
	c, _, clock, _ := dynamicClient(t, l, "3000")
	p := previewFor(t, c, "3000")
	a, _ := c.Approve(p, p.ApprovalDigest)
	run, e := c.Execute(context.Background(), a)
	if e != nil {
		t.Fatal(e)
	}
	row, rs, e := run.Next()
	if e != nil {
		t.Fatal(e)
	}
	v, _ := row.Data(rs)
	if v[0] != "1" || rs.GetColumnByIndex(0).DbType() != "INTEGER" {
		t.Fatal(v)
	}
	cursor, _ := run.Cursor()
	receipt := run.Receipt()
	c2, _, _, _ := dynamicClient(t, l, "3000")
	c2.clock = clock
	clock.advance(119 * time.Second)
	resumed, e := c2.Resume(context.Background(), receipt, cursor)
	if e != nil {
		t.Fatal(e)
	}
	if !resumed.Receipt().ExecutionDeadline.Equal(receipt.ExecutionDeadline) || resumed.Receipt().Counters.Rows != 1 || resumed.Receipt().Counters.Bytes <= receipt.Counters.Bytes {
		t.Fatal("renewed/reset ledger")
	}
	row, rs, e = resumed.Next()
	if e != nil {
		t.Fatal(e)
	}
	v, _ = row.Data(rs)
	if v[0] != "2" {
		t.Fatal("skipped row", v)
	}
	if _, e = resumed.NextPage(); errorCode(e) != "invalid_input" {
		t.Fatal("mixed page/row")
	}
}
func TestApprovalMutationsAndBoundsZeroDispatch(t *testing.T) {
	c, _, _, paid := dynamicClient(t, NewMemoryLedger(), "3000")
	plan, _, _ := c.prepare(context.Background())
	execution := Execution{"job-project", Principal{"workload", "operator:fixture", "1"}, "1000", "3000"}
	for _, cap := range []string{"", "0", "-1", "1.0", "9223372036854775808"} {
		bad := execution
		bad.MaximumBytesBilled = cap
		if _, e := c.Preview(context.Background(), plan, bad, DefaultBounds()); e == nil {
			t.Fatal("bad cap", cap)
		}
	}
	mutated := plan
	mutated.SQL = "DELETE FROM source"
	mutated.Digest, _ = digestObject("ReadPlan", mutated)
	if _, e := c.Preview(context.Background(), mutated, execution, DefaultBounds()); e == nil {
		t.Fatal("SQL accepted")
	}
	if *paid != 0 {
		t.Fatal("paid request before approval")
	}
	p := previewFor(t, c, "3000")
	for _, change := range []func(*Preview){func(p *Preview) { p.Execution.MaximumBytesBilled = "1001" }, func(p *Preview) { p.Execution.Principal.Generation = "2" }, func(p *Preview) { p.PolicyDigest = "changed" }} {
		altered, _ := jsonCopy(p)
		change(&altered)
		d, _ := approvalDigest(altered)
		if d == p.ApprovalDigest {
			t.Fatal("unbound amendment")
		}
		if _, e := c.Approve(altered, p.ApprovalDigest); errorCode(e) != "approval_changed" {
			t.Fatal(e)
		}
	}
	altered := p
	altered.CreatedAt = altered.CreatedAt.Add(time.Second)
	altered.ExpiresAt = altered.ExpiresAt.Add(time.Second)
	altered.Observation.ObservedAt = altered.Observation.ObservedAt.Add(time.Second)
	d, _ := approvalDigest(altered)
	if d != p.ApprovalDigest {
		t.Fatal("volatile time bound")
	}
}
func TestDispatchGateBelowRetryAndAuthentication(t *testing.T) {
	calls := 0
	gate := &dispatchGate{base: rt(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("response lost") }), method: "POST", url: apiOrigin + "projects/job-project/queries", bodyDigest: hashBytes([]byte(`{}`))}
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest("POST", gate.url, strings.NewReader(`{}`))
		_, e := gate.RoundTrip(req)
		if i == 1 && errorCode(e) != "submission_unknown" {
			t.Fatal(e)
		}
	}
	if calls != 1 {
		t.Fatal("POST replayed")
	}
	req, _ := http.NewRequest("DELETE", gate.url, strings.NewReader(`{}`))
	if _, e := gate.RoundTrip(req); errorCode(e) != "invalid_input" {
		t.Fatal("middleware mutation accepted", e)
	}
}
func TestSDKLoggerAndErrorsDoNotLeak(t *testing.T) {
	var buffer strings.Builder
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buffer, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	c, _, _, _ := dynamicClient(t, NewMemoryLedger(), "3000")
	_ = previewFor(t, c, "3000")
	if buffer.Len() > 0 {
		t.Fatal("SDK body logging enabled")
	}
	c.transport = rt(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 403, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":{"message":"SQL secret-token private-value","code":403}}`))}, nil
	})
	plan, _, _ := c.prepare(context.Background())
	_, e := c.Preview(context.Background(), plan, Execution{"job-project", Principal{"workload", "operator:fixture", "1"}, "1000", "3000"}, DefaultBounds())
	if errorCode(e) != "scope_missing" || strings.Contains(e.Error(), "secret") {
		t.Fatal(e)
	}
}
func TestExecutorNoKeyedOrGenericFallback(t *testing.T) {
	c, _, _, paid := dynamicClient(t, NewMemoryLedger(), "3000")
	p := previewFor(t, c, "3000")
	a, _ := c.Approve(p, p.ApprovalDigest)
	executor := Executor{c, testProfile(t), a, p.Plan}
	if _, e := executor.ExecuteQueryToRecordsReader(context.Background(), testQuery()); e != dal.ErrNotSupported {
		t.Fatal(e)
	}
	query := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "", nil))).Limit(100).SelectColumns(dal.CountAs(dal.NewFieldRef("", "n"), "count"))
	if _, e := executor.ExecuteQueryToRecordsetReader(context.Background(), query, recordset.WithName("test")); errorCode(e) != "unsupported_query" {
		t.Fatal(e)
	}
	if *paid != 0 {
		t.Fatal("hidden paid fallback")
	}
}

func TestPreviewCallerMutationCannotChangeTrustedPlan(t *testing.T) {
	c, _, _, paid := dynamicClient(t, NewMemoryLedger(), "3000")
	p := previewFor(t, c, "3000")
	original, _ := jsonCopy(p)
	p.Plan.SQL = "DELETE FROM source"
	p.Observation.Schema[0].Type = "STRING"
	p.Bounds.MaxRows = 10000
	if _, e := c.Approve(p, p.ApprovalDigest); errorCode(e) != "approval_changed" {
		t.Fatal("caller mutated trusted preview", e)
	}
	a, e := c.Approve(original, original.ApprovalDigest)
	if e != nil {
		t.Fatal(e)
	}
	run, e := c.Execute(context.Background(), a)
	if e != nil {
		t.Fatal(e)
	}
	receipt := run.Receipt()
	receipt.Warnings = append(receipt.Warnings, "caller mutation")
	if len(run.Receipt().Warnings) != 0 {
		t.Fatal("receipt shares ledger storage")
	}
	if *paid != 1 {
		t.Fatal(*paid)
	}
}

func TestRebindFileLedgerCASAndImmutableProvenance(t *testing.T) {
	ledger, e := NewFileLedger(filepath.Join(t.TempDir(), "private-ledger"))
	if e != nil {
		t.Fatal(e)
	}
	c, provider, clock, paid := dynamicClient(t, ledger, "3000")
	p := previewFor(t, c, "3000")
	a, _ := c.Approve(p, p.ApprovalDigest)
	run, e := c.Execute(context.Background(), a)
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = run.Next(); e != nil {
		t.Fatal(e)
	}
	cursor, _ := run.Cursor()
	before, _ := c.loadRun(run.id)
	provider.identity.Principal.Generation = "2"
	held, e := ledger.lease(context.Background(), run.id)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.RebindJob(context.Background(), run.Receipt(), cursor); errorCode(e) != "local_stopped" {
		t.Fatal("contested lease", e)
	}
	held()
	result, e := c.RebindJob(context.Background(), run.Receipt(), cursor)
	if e != nil {
		t.Fatal(e)
	}
	after, _ := c.loadRun(run.id)
	if !equalJSON(before.Receipt, after.Receipt) || before.Reservation != after.Reservation || before.Offset != after.Offset || before.PageDigest != after.PageDigest || !equalJSON(before.Schema, after.Schema) || after.ActivePrincipal.Generation != "2" || result.Cursor == cursor {
		t.Fatal("rebind changed execution or failed rotation")
	}
	if _, e = c.RebindJob(context.Background(), run.Receipt(), cursor); errorCode(e) != "cursor_invalid" {
		t.Fatal("stale cursor", e)
	}
	if _, _, e = run.Next(); errorCode(e) != "approval_changed" {
		t.Fatal("old buffered authority", e)
	}
	c2, p2, _, _ := dynamicClient(t, ledger, "3000")
	c2.clock = clock
	p2.identity = provider.identity
	restored, e := c2.Resume(context.Background(), result.Receipt, result.Cursor)
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = restored.Next(); e != nil {
		t.Fatal(e)
	}
	if *paid != 1 || restored.Receipt().Principal != before.Receipt.Principal || !restored.Receipt().ExecutionDeadline.Equal(before.Receipt.ExecutionDeadline) {
		t.Fatal("replayed or rebound original provenance")
	}
}

func TestEmptyResultPageRequiresCurrentGrantAndIsDeliveredOnce(t *testing.T) {
	c, provider, _, _ := dynamicClient(t, NewMemoryLedger(), "3000")
	original := c.transport
	c.transport = rt(func(req *http.Request) (*http.Response, error) {
		if req.Method == "POST" {
			b, _ := io.ReadAll(req.Body)
			req.Body = io.NopCloser(bytes.NewReader(b))
			v, _ := ParseJSON(b, 256<<10)
			m, _ := object(v)
			if m["dryRun"] == false {
				return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"rows":[]}`), nil
			}
		}
		return original.RoundTrip(req)
	})
	p := previewFor(t, c, "3000")
	a, _ := c.Approve(p, p.ApprovalDigest)
	run, e := c.Execute(context.Background(), a)
	if e != nil {
		t.Fatal(e)
	}
	provider.identity.Read = false
	if _, e = run.NextPage(); errorCode(e) != "scope_missing" {
		t.Fatal("empty schema exported without grant", e)
	}
	provider.identity.Read = true
	page, e := run.NextPage()
	if e != nil || len(page.Schema) != 1 || len(page.Rows) != 0 {
		t.Fatal(page, e)
	}
	if _, e = run.NextPage(); e != dal.ErrNoMoreRecords {
		t.Fatal("empty page repeated", e)
	}
}
