package bigquery

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func approvedRun(t *testing.T, c *Client, b Bounds) (*Run, error) {
	t.Helper()
	plan, _ := Compile(testProfile(t), testQuery())
	p, e := c.Preview(context.Background(), plan, Execution{"job-project", Principal{"workload", "operator:fixture", "1"}, "1000", "3000"}, b)
	if e != nil {
		t.Fatal(e)
	}
	a, e := c.Approve(p, p.ApprovalDigest)
	if e != nil {
		t.Fatal(e)
	}
	return c.Execute(context.Background(), a)
}
func TestUndeliveredRecordsetsStayPrivateAndDetached(t *testing.T) {
	for _, change := range []string{"grant", "policy", "maxrows", "deadline"} {
		t.Run(change, func(t *testing.T) {
			c, provider, _, _ := dynamicClient(t, NewMemoryLedger(), "3000")
			b := DefaultBounds()
			if change == "maxrows" {
				b.MaxRows = 1
			}
			run, e := approvedRun(t, c, b)
			if e != nil {
				t.Fatal(e)
			}
			if rs := run.Recordset(); rs.RowsCount() != 0 || rs.GetRow(0) != nil {
				t.Fatal("raw buffered row escaped")
			}
			row, rs, e := run.Next()
			if e != nil {
				t.Fatal(e)
			}
			if rs.RowsCount() != 1 || rs.GetRow(1) != nil {
				t.Fatal("Next exposed future rows")
			}
			_ = row.SetValueByIndex(0, "tampered", rs)
			view := run.Recordset()
			_ = view.GetRow(0).SetValueByIndex(0, "also-tampered", view)
			if change == "grant" {
				provider.identity.Read = false
			}
			if change == "policy" {
				original := c.prepare
				c.prepare = func(ctx context.Context) (ReadPlan, string, error) { p, _, e := original(ctx); return p, "changed", e }
			}
			if change == "deadline" {
				c.clock.(*fakeClock).advance(time.Duration(b.WallMs) * time.Millisecond)
			}
			if _, _, e = run.Next(); e == nil {
				t.Fatal("unauthorized future row delivered")
			}
			if run.Receipt().Counters.Rows != 1 || run.Recordset().RowsCount() != 1 {
				t.Fatal("delivery accounting")
			}
			if change == "grant" {
				provider.identity.Read = true
				row, rs, e = run.Next()
				if e != nil {
					t.Fatal(e)
				}
				v, _ := row.Data(rs)
				if v[0] != "2" {
					t.Fatal("view mutation altered private delivery", v)
				}
			}
		})
	}
}
func shortBounds() Bounds { b := DefaultBounds(); b.WallMs = 1000; b.HTTPMs = 60; return b }
func requirePrompt(t *testing.T, started time.Time, e error) {
	t.Helper()
	if e == nil || time.Since(started) > 200*time.Millisecond {
		t.Fatalf("unbounded elapsed %v error %v", time.Since(started), e)
	}
}
func slowPrepare(original Prepare, cooperative bool, completed chan<- struct{}) Prepare {
	return func(ctx context.Context) (ReadPlan, string, error) {
		if cooperative {
			select {
			case <-ctx.Done():
				return ReadPlan{}, "", ctx.Err()
			case <-time.After(400 * time.Millisecond):
			}
		} else {
			time.Sleep(400 * time.Millisecond)
		}
		if completed != nil {
			completed <- struct{}{}
		}
		return original(ctx)
	}
}
func TestActualTimePreparationBoundsAndNoLateSubmission(t *testing.T) {
	for _, phase := range []string{"preview", "execute", "resume", "status", "cancel", "rebind"} {
		for _, cooperative := range []bool{true, false} {
			t.Run(phase+map[bool]string{true: "-cooperative", false: "-ignores"}[cooperative], func(t *testing.T) {
				c, _, _, paid := dynamicClient(t, NewMemoryLedger(), "3000")
				plan, _ := Compile(testProfile(t), testQuery())
				b := shortBounds()
				exec := Execution{"job-project", Principal{"workload", "operator:fixture", "1"}, "1000", "3000"}
				var p Preview
				var run *Run
				var cursor string
				if phase != "preview" {
					var e error
					p, e = c.Preview(context.Background(), plan, exec, b)
					if e != nil {
						t.Fatal(e)
					}
				}
				if phase == "resume" || phase == "status" || phase == "cancel" || phase == "rebind" {
					a, _ := c.Approve(p, p.ApprovalDigest)
					var e error
					run, e = c.Execute(context.Background(), a)
					if e != nil {
						t.Fatal(e)
					}
					_, _, e = run.Next()
					if e != nil {
						t.Fatal(e)
					}
					cursor, _ = run.Cursor()
				}
				done := make(chan struct{}, 1)
				c.prepare = slowPrepare(c.prepare, cooperative, done)
				started := time.Now()
				var e error
				switch phase {
				case "preview":
					_, e = c.Preview(context.Background(), plan, exec, b)
				case "execute":
					a, _ := c.Approve(p, p.ApprovalDigest)
					run, e = c.Execute(context.Background(), a)
				case "resume":
					_, e = c.Resume(context.Background(), run.Receipt(), cursor)
				case "status":
					_, e = c.Status(context.Background(), *run.Receipt().Job)
				case "cancel":
					_, e = c.CancelJob(context.Background(), *run.Receipt().Job)
				case "rebind":
					_, e = c.RebindJob(context.Background(), run.Receipt(), cursor)
				}
				requirePrompt(t, started, e)
				before := Receipt{}
				if run != nil {
					before = run.Receipt()
				}
				if !cooperative {
					<-done
				}
				time.Sleep(10 * time.Millisecond)
				if run != nil && !equalJSON(before, run.Receipt()) {
					t.Fatal("late preparation wrote state")
				}
				expected := 0
				if phase != "preview" && phase != "execute" {
					expected = 1
				}
				if *paid != expected {
					t.Fatal("late paid dispatch", *paid)
				}
			})
		}
	}
}

type delayedProvider struct {
	original Provider
	done     chan struct{}
}

func (p delayedProvider) Authorize(ctx context.Context, base http.RoundTripper) (Identity, http.RoundTripper, error) {
	time.Sleep(400 * time.Millisecond)
	close(p.done)
	return p.original.Authorize(ctx, base)
}
func TestActualTimeProviderCannotDispatchAfterReturn(t *testing.T) {
	c, _, _, paid := dynamicClient(t, NewMemoryLedger(), "3000")
	plan, _ := Compile(testProfile(t), testQuery())
	p, e := c.Preview(context.Background(), plan, Execution{"job-project", Principal{"workload", "operator:fixture", "1"}, "1000", "3000"}, shortBounds())
	if e != nil {
		t.Fatal(e)
	}
	a, _ := c.Approve(p, p.ApprovalDigest)
	done := make(chan struct{})
	c.provider = delayedProvider{c.provider, done}
	started := time.Now()
	run, e := c.Execute(context.Background(), a)
	requirePrompt(t, started, e)
	before := run.Receipt()
	<-done
	time.Sleep(10 * time.Millisecond)
	if *paid != 0 || !equalJSON(before, run.Receipt()) {
		t.Fatal("late provider gained authority")
	}
}
func TestActualTimePaidTransportRetainsUnknownAndNoLateWrites(t *testing.T) {
	c, _, _, _ := dynamicClient(t, NewMemoryLedger(), "3000")
	original := c.transport
	done := make(chan struct{})
	var posts atomic.Int32
	c.transport = rt(func(req *http.Request) (*http.Response, error) {
		if req.Method == "POST" {
			body, _ := io.ReadAll(req.Body)
			req.Body = io.NopCloser(strings.NewReader(string(body)))
			if strings.Contains(string(body), `"dryRun":false`) {
				posts.Add(1)
				time.Sleep(400 * time.Millisecond)
				defer close(done)
			}
		}
		return original.RoundTrip(req)
	})
	plan, _ := Compile(testProfile(t), testQuery())
	preview, e := c.Preview(context.Background(), plan, Execution{"job-project", Principal{"workload", "operator:fixture", "1"}, "1000", "3000"}, shortBounds())
	if e != nil {
		t.Fatal(e)
	}
	approval, _ := c.Approve(preview, preview.ApprovalDigest)
	started := time.Now()
	run, e := c.Execute(context.Background(), approval)
	requirePrompt(t, started, e)
	before := run.Receipt()
	rr, _ := c.loadRun(run.id)
	if before.State != "submission_unknown" || before.Job != nil || rr.Reservation != 1000 {
		t.Fatal(before, rr.Reservation)
	}
	<-done
	time.Sleep(10 * time.Millisecond)
	if posts.Load() != 1 || !equalJSON(before, run.Receipt()) {
		t.Fatal("late result mutation/replay")
	}
}
func TestLedgerReservationWaitHonorsOriginalDeadline(t *testing.T) {
	l, e := NewFileLedger(filepath.Join(t.TempDir(), "private-ledger"))
	if e != nil {
		t.Fatal(e)
	}
	c, _, _, paid := dynamicClient(t, l, "3000")
	plan, _ := Compile(testProfile(t), testQuery())
	b := shortBounds()
	b.WallMs = 100
	p, e := c.Preview(context.Background(), plan, Execution{"job-project", Principal{"workload", "operator:fixture", "1"}, "1000", "3000"}, b)
	if e != nil {
		t.Fatal(e)
	}
	a, _ := c.Approve(p, p.ApprovalDigest)
	lock, e := l.lock("session.lock")
	if e != nil {
		t.Fatal(e)
	}
	if e = ledgerLock(lock, true); e != nil {
		t.Fatal(e)
	}
	defer lock.Close()
	defer ledgerUnlock(lock)
	started := time.Now()
	_, e = c.Execute(context.Background(), a)
	requirePrompt(t, started, e)
	if *paid != 0 {
		t.Fatal("dispatch after budget-lock timeout")
	}
}

type waitingBody struct {
	body    io.ReadCloser
	started chan struct{}
	release chan struct{}
	done    chan struct{}
	reads   atomic.Int32
}

func (b *waitingBody) Read(p []byte) (int, error) {
	if b.reads.Add(1) == 1 {
		close(b.started)
		<-b.release
		defer close(b.done)
	}
	return b.body.Read(p)
}

// A fast Close may not actually interrupt an injected Read.
func (b *waitingBody) Close() error { return nil }
func TestActualTimeControlBodyRetainsPhysicalRunGate(t *testing.T) {
	c, _, _, _ := dynamicClient(t, NewMemoryLedger(), "3000")
	run, e := approvedRun(t, c, shortBounds())
	if e != nil {
		t.Fatal(e)
	}
	job := *run.Receipt().Job
	original := c.transport
	blocked := &waitingBody{body: io.NopCloser(strings.NewReader(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"DONE"},"statistics":{"query":{"totalBytesBilled":"0"}}}`)), started: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	var requests atomic.Int32
	c.transport = rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/jobs/j") {
			requests.Add(1)
			resp := response("{}")
			resp.Body = blocked
			return resp, nil
		}
		return original.RoundTrip(req)
	})
	before := run.Receipt()
	started := time.Now()
	_, e = c.Status(context.Background(), job)
	requirePrompt(t, started, e)
	<-blocked.started
	// Lease is released for caller recovery, but physical work still forbids another HTTP operation.
	started = time.Now()
	_, e = c.Status(context.Background(), job)
	requirePrompt(t, started, e)
	if requests.Load() != 1 {
		t.Fatal("second request while first body remains active")
	}
	close(blocked.release)
	<-blocked.done
	time.Sleep(20 * time.Millisecond)
	if !equalJSON(before, run.Receipt()) {
		t.Fatal("late body wrote ledger/billing")
	}
	if blocked.reads.Load() != 1 {
		t.Fatal("new body read after deadline")
	}
}

type delayedAuthProvider struct {
	original Provider
	done     chan struct{}
}

func (p delayedAuthProvider) Authorize(ctx context.Context, base http.RoundTripper) (Identity, http.RoundTripper, error) {
	identity, _, e := p.original.Authorize(ctx, base)
	return identity, rt(func(req *http.Request) (*http.Response, error) {
		if req.Method == "POST" {
			raw, _ := io.ReadAll(req.Body)
			req.Body = io.NopCloser(strings.NewReader(string(raw)))
			if strings.Contains(string(raw), `"dryRun":false`) {
				time.Sleep(400 * time.Millisecond)
				defer close(p.done)
			}
		}
		return base.RoundTrip(req)
	}), e
}
func TestActualTimeLateAuthenticationCannotDispatch(t *testing.T) {
	c, _, _, paid := dynamicClient(t, NewMemoryLedger(), "3000")
	plan, _ := Compile(testProfile(t), testQuery())
	p, e := c.Preview(context.Background(), plan, Execution{"job-project", Principal{"workload", "operator:fixture", "1"}, "1000", "3000"}, shortBounds())
	if e != nil {
		t.Fatal(e)
	}
	a, _ := c.Approve(p, p.ApprovalDigest)
	done := make(chan struct{})
	c.provider = delayedAuthProvider{c.provider, done}
	started := time.Now()
	run, e := c.Execute(context.Background(), a)
	requirePrompt(t, started, e)
	before := run.Receipt()
	<-done
	time.Sleep(20 * time.Millisecond)
	if *paid != 0 || !equalJSON(before, run.Receipt()) {
		t.Fatal("late middleware submitted or wrote state")
	}
}
func TestAbandonedDependenciesHaveFiniteWorkerBound(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, cap(dependencySlots))
	done := make(chan struct{}, cap(dependencySlots))
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	for i := 0; i < cap(dependencySlots); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		_, e := awaitDependency(ctx, func() (struct{}, error) { started <- struct{}{}; <-release; done <- struct{}{}; return struct{}{}, nil }, nil)
		cancel()
		if e == nil {
			t.Fatal("unexpected success")
		}
		<-started
	}
	called := false
	_, e := awaitDependency(context.Background(), func() (struct{}, error) { called = true; return struct{}{}, nil }, nil)
	if codeOf(e) != "local_stopped" || called {
		t.Fatal("worker cap exceeded", e)
	}
	// Release in the function body and wait until every finite worker has physically exited.
	close(release)
	released = true
	for i := 0; i < cap(dependencySlots); i++ {
		<-done
	}
	for len(dependencySlots) > 0 {
		time.Sleep(time.Millisecond)
	}

}
