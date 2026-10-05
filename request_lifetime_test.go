package bigquery

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitDependenciesIdle(t *testing.T) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for len(dependencySlots) > 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if len(dependencySlots) != 0 {
		t.Fatal("finite dependency workers did not exit")
	}
}
func fillDependencySlots(t *testing.T, n int) func() {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case dependencySlots <- struct{}{}:
		default:
			t.Fatal("unexpected worker occupancy")
		}
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			for i := 0; i < n; i++ {
				<-dependencySlots
			}
		})
	}
	t.Cleanup(release)
	return release
}
func TestWorkerAdmissionRefusalReleasesRunMarker(t *testing.T) {
	waitDependenciesIdle(t)
	release := fillDependencySlots(t, cap(dependencySlots))
	id := "r2-direct-admission"
	var calls, before atomic.Int32
	base := rt(func(*http.Request) (*http.Response, error) { calls.Add(1); return response(`{}`), nil })
	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://bigquery.googleapis.com/bigquery/v2/projects/job-project/jobs/j", nil)
	gate := &dispatchGate{runID: id, base: base, beforeDispatch: func() error { before.Add(1); return nil }}
	_, e := gate.RoundTrip(req)
	if codeOf(e) != "local_stopped" || calls.Load() != 0 || before.Load() != 0 || gate.wasDispatched() {
		t.Fatal("refusal acquired dispatch authority", e)
	}
	if _, held := actualRequests.Load(id); held {
		t.Fatal("refused worker leaked run marker")
	}
	release()
	retry := &dispatchGate{runID: id, base: base}
	resp, e := retry.RoundTrip(req)
	if e != nil {
		t.Fatal("recovery denied", e)
	}
	if e = resp.Body.Close(); e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 1 {
		t.Fatal("actual dispatch count", calls.Load())
	}
	waitDependenciesIdle(t)
	if _, held := actualRequests.Load(id); held {
		t.Fatal("closed recovery request retained marker")
	}
}
func TestStatusRecoversAfterActualWorkerAdmissionRefusal(t *testing.T) {
	c, _, _, _ := dynamicClient(t, NewMemoryLedger(), "3000")
	run, e := approvedRun(t, c, shortBounds())
	if e != nil {
		t.Fatal(e)
	}
	waitDependenciesIdle(t)
	before := run.Receipt()
	storedBefore, _ := c.loadRun(run.id)
	var calls atomic.Int32
	c.transport = rt(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"RUNNING"}}`), nil
	})
	// Two outer SDK/auth workers can start; the actual transport worker cannot.
	release := fillDependencySlots(t, cap(dependencySlots)-2)
	started := time.Now()
	_, e = c.Status(context.Background(), *before.Job)
	requirePrompt(t, started, e)
	if codeOf(e) != "local_stopped" || calls.Load() != 0 {
		t.Fatal("refused Status dispatched", e, calls.Load())
	}
	if _, held := actualRequests.Load(run.id); held {
		t.Fatal("Status refusal leaked physical marker")
	}
	release()
	waitDependenciesIdle(t)
	after, _ := c.loadRun(run.id)
	if !equalJSON(before, run.Receipt()) || storedBefore.Reservation != after.Reservation {
		t.Fatal("refusal altered authority/budget")
	}
	status, e := c.Status(context.Background(), *before.Job)
	if e != nil || status.State != "running" || calls.Load() != 1 {
		t.Fatal("known job cannot recover", status, e, calls.Load())
	}
	waitDependenciesIdle(t)
	if _, held := actualRequests.Load(run.id); held {
		t.Fatal("successful Status retained marker")
	}
}

type boundedCloseBody struct {
	reader           io.Reader
	entered, release chan struct{}
	closes           atomic.Int32
}

func (b *boundedCloseBody) Read(p []byte) (int, error) { return b.reader.Read(p) }
func (b *boundedCloseBody) Close() error {
	if b.closes.Add(1) != 1 {
		return fail("invalid_input")
	}
	close(b.entered)
	select {
	case <-b.release:
		return nil
	case <-time.After(time.Second):
		return fail("local_stopped")
	}
}
func TestEOFRequiresPhysicalCloseBeforeNextRequest(t *testing.T) {
	waitDependenciesIdle(t)
	id := "r2-eof-close"
	leave, e := enterActualRequest(id)
	if e != nil {
		t.Fatal(e)
	}
	body := &boundedCloseBody{reader: strings.NewReader("ok"), entered: make(chan struct{}), release: make(chan struct{})}
	tracked := &trackedBody{body: body, leave: leave}
	_, e = io.ReadAll(tracked)
	if e != nil {
		t.Fatal(e)
	}
	if _, held := actualRequests.Load(id); !held {
		t.Fatal("EOF released physical marker")
	}
	done := make(chan struct{})
	go func() { _ = tracked.Close(); close(done) }()
	select {
	case <-body.entered:
	case <-time.After(time.Second):
		t.Fatal("Close not invoked")
	}
	if next, e := enterActualRequest(id); e == nil {
		next()
		t.Fatal("new request during physical Close")
	}
	close(body.release)
	<-done
	if _, held := actualRequests.Load(id); held {
		t.Fatal("physical Close did not release marker")
	}
	_ = tracked.Close()
	if body.closes.Load() != 1 {
		t.Fatal("Close repeated")
	}
}
func TestStatusEOFCloseTimeoutPreservesRecoveryGate(t *testing.T) {
	c, _, _, _ := dynamicClient(t, NewMemoryLedger(), "3000")
	run, e := approvedRun(t, c, shortBounds())
	if e != nil {
		t.Fatal(e)
	}
	waitDependenciesIdle(t)
	before := run.Receipt()
	storedBefore, _ := c.loadRun(run.id)
	body := &boundedCloseBody{reader: strings.NewReader(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"DONE"},"statistics":{"query":{"totalBytesBilled":"0"}}}`), entered: make(chan struct{}), release: make(chan struct{})}
	var calls atomic.Int32
	c.transport = rt(func(*http.Request) (*http.Response, error) {
		n := calls.Add(1)
		if n == 1 {
			resp := response("{}")
			resp.Body = body
			return resp, nil
		}
		return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"RUNNING"}}`), nil
	})
	started := time.Now()
	_, e = c.Status(context.Background(), *before.Job)
	requirePrompt(t, started, e)
	select {
	case <-body.entered:
	case <-time.After(time.Second):
		t.Fatal("EOF body cleanup not started")
	}
	if _, held := actualRequests.Load(run.id); !held {
		t.Fatal("caller timeout released pending Close")
	}
	snapshot := run.Receipt()
	started = time.Now()
	_, e = c.Status(context.Background(), *before.Job)
	requirePrompt(t, started, e)
	if calls.Load() != 1 {
		t.Fatal("second request while physical Close active")
	}
	// The original admitted worker owns cleanup: completion needs no new slot.
	n := cap(dependencySlots) - len(dependencySlots)
	releaseSlots := fillDependencySlots(t, n)
	close(body.release)
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		if _, held := actualRequests.Load(run.id); !held {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, held := actualRequests.Load(run.id); held {
		t.Fatal("cleanup required another worker admission")
	}
	releaseSlots()
	waitDependenciesIdle(t)
	after, _ := c.loadRun(run.id)
	if !equalJSON(snapshot, run.Receipt()) || storedBefore.Reservation != after.Reservation || before.ExecutionDeadline != after.Receipt.ExecutionDeadline || before.Principal != after.Receipt.Principal {
		t.Fatal("late Close mutated ledger/authority")
	}
	status, e := c.Status(context.Background(), *before.Job)
	if e != nil || status.State != "running" || calls.Load() != 2 {
		t.Fatal("finite Close cannot recover", status, e, calls.Load())
	}
	if body.closes.Load() != 1 {
		t.Fatal("physical Close repeated")
	}
	waitDependenciesIdle(t)
}
