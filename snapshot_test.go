package bigquery

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func snapshotRun(t *testing.T, ledger Ledger) (*Client, *Run, Receipt, string) {
	t.Helper()
	c, _, _, _ := dynamicClient(t, ledger, "3000")
	run, e := approvedRun(t, c, DefaultBounds())
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = run.Next(); e != nil {
		t.Fatal(e)
	}
	cursor, e := run.Cursor()
	if e != nil {
		t.Fatal(e)
	}
	return c, run, run.Receipt(), cursor
}
func forbidSnapshotWork(c *Client) {
	c.prepare = func(context.Context) (ReadPlan, string, error) { panic("snapshot prepared a query") }
	c.provider = nil
	c.transport = rt(func(*http.Request) (*http.Response, error) { panic("snapshot dispatched HTTP") })
}
func TestSnapshotRefreshesTerminalBillingAndControlBytes(t *testing.T) {
	c, run, previous, cursor := snapshotRun(t, NewMemoryLedger())
	c.transport = rt(func(*http.Request) (*http.Response, error) {
		return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"DONE"},"statistics":{"query":{"totalBytesBilled":"42"}}}`), nil
	})
	status, e := c.Status(context.Background(), *previous.Job)
	if e != nil || status.State != "completed" {
		t.Fatal(status, e)
	}
	trustedBefore, _ := c.loadRun(run.id)
	forbidSnapshotWork(c)
	result, e := c.Snapshot(context.Background(), previous)
	if e != nil {
		t.Fatal(e)
	}
	if result.Receipt.State != "completed" || result.Receipt.BilledBytes == nil || *result.Receipt.BilledBytes != "42" || result.Receipt.Counters.Bytes <= previous.Counters.Bytes {
		t.Fatal("stale control receipt", result.Receipt)
	}
	if result.Cursor != cursor || result.Receipt.Counters.Rows != previous.Counters.Rows || result.Receipt.Counters.Pages != previous.Counters.Pages || result.Receipt.ExecutionDeadline != previous.ExecutionDeadline || result.Receipt.Principal != previous.Principal {
		t.Fatal("snapshot renewed delivery authority")
	}
	trustedAfter, _ := c.loadRun(run.id)
	if !equalJSON(trustedBefore, trustedAfter) || trustedAfter.Reservation != 42 {
		t.Fatal("snapshot changed ledger/budget")
	}
}
func TestSnapshotPreservesCancelStateAndPartialOutcome(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "partial-error"}[partial], func(t *testing.T) {
			c, run, previous, cursor := snapshotRun(t, NewMemoryLedger())
			c.transport = rt(func(*http.Request) (*http.Response, error) {
				if partial {
					return response(`{"job":{"jobReference":{"projectId":"job-project","jobId":"wrong","location":"EU"}}}`), nil
				}
				return response(`{"job":{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"}}}`), nil
			})
			outcome, controlErr := c.CancelJob(context.Background(), *previous.Job)
			if partial {
				if codeOf(controlErr) != "cancellation_unknown" || outcome.State != "unknown" {
					t.Fatal(outcome, controlErr)
				}
			} else if controlErr != nil || outcome.State != "cancel_requested" {
				t.Fatal(outcome, controlErr)
			}
			before, _ := c.loadRun(run.id)
			forbidSnapshotWork(c)
			result, e := c.Snapshot(context.Background(), previous)
			if e != nil {
				t.Fatal(e)
			}
			if result.Receipt.Counters.Bytes <= previous.Counters.Bytes || result.Cursor != cursor {
				t.Fatal("lost control debit/cursor")
			}
			if !partial && result.Receipt.State != "cancel_requested" {
				t.Fatal("stale cancel receipt")
			}
			if partial && result.Receipt.State == "cancelled" {
				t.Fatal("invented confirmed cancellation")
			}
			raw, _ := json.Marshal(result)
			var keys map[string]json.RawMessage
			_ = json.Unmarshal(raw, &keys)
			if len(keys) != 2 || keys["receipt"] == nil || keys["cursor"] == nil {
				t.Fatal("snapshot exposed control/row/private state")
			}
			after, _ := c.loadRun(run.id)
			if !equalJSON(before, after) || after.Reservation != 1000 {
				t.Fatal("snapshot inferred reservation release")
			}
		})
	}
}
func TestSnapshotAfterExpiryAndWithoutIssuedCursor(t *testing.T) {
	c, run, previous, cursor := snapshotRun(t, NewMemoryLedger())
	c.clock.(*fakeClock).advance(time.Duration(previous.Bounds.WallMs+1) * time.Millisecond)
	forbidSnapshotWork(c)
	before, _ := c.loadRun(run.id)
	result, e := c.Snapshot(context.Background(), previous)
	if e != nil || result.Cursor != cursor {
		t.Fatal(result, e)
	}
	after, _ := c.loadRun(run.id)
	if !equalJSON(before, after) {
		t.Fatal("expired snapshot changed state")
	}
	if _, e = c.Resume(context.Background(), result.Receipt, result.Cursor); codeOf(e) != "local_stopped" {
		t.Fatal("expired cursor granted execution", e)
	}
	// A known run may have no delivered cursor, e.g. initial/unknown submission.
	c2, _, _, _ := dynamicClient(t, NewMemoryLedger(), "3000")
	run2, e := approvedRun(t, c2, DefaultBounds())
	if e != nil {
		t.Fatal(e)
	}
	receipt := run2.Receipt()
	forbidSnapshotWork(c2)
	result, e = c2.Snapshot(context.Background(), receipt)
	if e != nil || result.Cursor != "" {
		t.Fatal("snapshot minted cursor", result, e)
	}
}
func TestSnapshotRejectsForgedImmutableAuthority(t *testing.T) {
	c, _, original, _ := snapshotRun(t, NewMemoryLedger())
	forbidSnapshotWork(c)
	edits := map[string]func(*Receipt){
		"version": func(r *Receipt) { r.Version++ }, "run": func(r *Receipt) { r.RunID = "unknown" }, "approval": func(r *Receipt) { r.ApprovalDigest = "forged" }, "source": func(r *Receipt) { r.SourceDigest = "forged" }, "observation": func(r *Receipt) { r.ObservationDigest = "forged" }, "schema": func(r *Receipt) { r.SchemaDigest = "forged" }, "principal": func(r *Receipt) { r.Principal.Subject = "other" }, "generation": func(r *Receipt) { r.Principal.Generation = "other" }, "job": func(r *Receipt) { r.Job.JobID = "other" }, "missing-job": func(r *Receipt) { r.Job = nil }, "bounds": func(r *Receipt) { r.Bounds.MaxRows++ }, "started": func(r *Receipt) { r.RunStartedAt = r.RunStartedAt.Add(time.Second) }, "deadline": func(r *Receipt) { r.ExecutionDeadline = r.ExecutionDeadline.Add(time.Second) },
	}
	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			receipt, _ := jsonCopy(original)
			edit(&receipt)
			if got, e := c.Snapshot(context.Background(), receipt); codeOf(e) != "cursor_invalid" || got.Receipt.RunID != "" || got.Cursor != "" {
				t.Fatal("forged authority escaped", got, e)
			}
		})
	}
	edited, _ := jsonCopy(original)
	edited.Counters.Bytes = 0
	edited.State = "caller-state"
	edited.BilledBytes = new(string)
	got, e := c.Snapshot(context.Background(), edited)
	if e != nil || !equalJSON(got.Receipt, original) {
		t.Fatal("mutable caller fields were trusted", got, e)
	}
}
func TestSnapshotDefensiveCopiesAndPrivateFileUnchanged(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ledger")
	ledger, e := NewFileLedger(dir)
	if e != nil {
		t.Fatal(e)
	}
	c, run, previous, cursor := snapshotRun(t, ledger)
	if e = c.mutateRun(run.id, func(r *runRecord) error {
		billing := "77"
		r.Receipt.BilledBytes = &billing
		r.Receipt.Warnings = []string{"fixture-warning"}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(filepath.Join(dir, "session.json"))
	if e != nil {
		t.Fatal(e)
	}
	forbidSnapshotWork(c)
	first, e := c.Snapshot(context.Background(), previous)
	if e != nil {
		t.Fatal(e)
	}
	first.Receipt.Job.JobID = "tampered"
	*first.Receipt.BilledBytes = "0"
	first.Receipt.Warnings[0] = "tampered"
	first.Receipt.Counters.Bytes = 0
	first.Cursor = "tampered"
	second, e := c.Snapshot(context.Background(), previous)
	if e != nil || second.Cursor != cursor || second.Receipt.Job.JobID != "j" || *second.Receipt.BilledBytes != "77" || second.Receipt.Warnings[0] != "fixture-warning" {
		t.Fatal("snapshot mutation altered trusted state", second, e)
	}
	after, e := os.ReadFile(filepath.Join(dir, "session.json"))
	if e != nil || string(before) != string(after) {
		t.Fatal("snapshot persisted ledger changes", e)
	}
	forged := previous
	forged.RunID = "ffffffffffffffffffffffffffffffffffffffffffffffff"
	if _, e = c.Snapshot(context.Background(), forged); codeOf(e) != "cursor_invalid" {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(dir, "run-"+forged.RunID+".lock")); !os.IsNotExist(e) {
		t.Fatal("forged authority created a lease file")
	}
}
func TestSnapshotCancellationAndConcurrentControl(t *testing.T) {
	ledger := NewMemoryLedger()
	c, run, previous, cursor := snapshotRun(t, ledger)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := c.Snapshot(ctx, previous); codeOf(e) != "local_stopped" {
		t.Fatal("cancelled snapshot", e)
	}
	ledger.mu.Lock()
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	started := time.Now()
	_, e := c.Snapshot(ctx, previous)
	cancel()
	ledger.mu.Unlock()
	requirePrompt(t, started, e)
	entered, release := make(chan struct{}), make(chan struct{})
	var requests atomic.Int32
	c.transport = rt(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		close(entered)
		select {
		case <-release:
		case <-time.After(time.Second):
			return nil, fail("local_stopped")
		}
		return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"DONE"},"statistics":{"query":{"totalBytesBilled":"19"}}}`), nil
	})
	done := make(chan error, 1)
	go func() { _, e := c.Status(context.Background(), *previous.Job); done <- e }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("control not started")
	}
	started = time.Now()
	_, e = c.Snapshot(context.Background(), previous)
	requirePrompt(t, started, e)
	if requests.Load() != 1 {
		t.Fatal("snapshot dispatched concurrently")
	}
	close(release)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	forbidSnapshotWork(c)
	result, e := c.Snapshot(context.Background(), previous)
	if e != nil || result.Cursor != cursor || result.Receipt.State != "completed" || *result.Receipt.BilledBytes != "19" {
		t.Fatal("inconsistent post-control snapshot", result, e)
	}
	rr, _ := c.loadRun(run.id)
	if rr.Reservation != 19 {
		t.Fatal("billing state not atomic")
	}
}
