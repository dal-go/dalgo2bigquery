package bigquery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"
)

type mockLedger struct {
	updateFunc        func(func(*ledgerState) error) error
	updateContextFunc func(context.Context, func(*ledgerState) error) error
	readContextFunc   func(context.Context, func(*ledgerState) error) error
	leaseFunc         func(context.Context, string) (func(), error)
}

func (m *mockLedger) update(fn func(*ledgerState) error) error {
	if m.updateFunc != nil {
		return m.updateFunc(fn)
	}
	return nil
}
func (m *mockLedger) updateContext(ctx context.Context, fn func(*ledgerState) error) error {
	if m.updateContextFunc != nil {
		return m.updateContextFunc(ctx, fn)
	}
	return nil
}
func (m *mockLedger) readContext(ctx context.Context, fn func(*ledgerState) error) error {
	if m.readContextFunc != nil {
		return m.readContextFunc(ctx, fn)
	}
	return nil
}
func (m *mockLedger) lease(ctx context.Context, id string) (func(), error) {
	if m.leaseFunc != nil {
		return m.leaseFunc(ctx, id)
	}
	return func() {}, nil
}

// Not parallel: mutates package-level jsonMarshal seam.
func TestSnapshot_SeamErrors(t *testing.T) {
	orig := jsonMarshal
	defer func() { jsonMarshal = orig }()

	c, _, receipt, _ := snapshotRun(t, NewMemoryLedger())

	// 1. Fail on Receipt copy (covers line 38)
	jsonMarshal = func(v any) ([]byte, error) {
		if _, ok := v.(Receipt); ok {
			return nil, errors.New("marshal receipt failed")
		}
		return orig(v)
	}
	if _, err := c.Snapshot(context.Background(), receipt); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on receipt copy error, got %v", err)
	}

	// 2. Fail on cursor marshal (covers line 44)
	jsonMarshal = func(v any) ([]byte, error) {
		if _, ok := v.(*cursorState); ok {
			return nil, errors.New("marshal cursor failed")
		}
		return orig(v)
	}
	if _, err := c.Snapshot(context.Background(), receipt); err == nil || err.Error() != "marshal cursor failed" {
		t.Fatalf("expected marshal cursor error, got %v", err)
	}
}

func TestSnapshot_MockLedgerBranches(t *testing.T) {
	t.Parallel()

	runID := "0123456789abcdef0123456789abcdef0123456789abcdef"
	receipt := Receipt{
		RunID:     runID,
		Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"},
	}

	readCount := 0
	ml := &mockLedger{
		readContextFunc: func(ctx context.Context, fn func(*ledgerState) error) error {
			readCount++
			if readCount == 1 {
				// First call (read(nil)): succeed
				state := emptyLedger()
				state.Runs[runID] = runRecord{Receipt: receipt}
				return fn(&state)
			}
			// Second call (read(&result)): fail
			return errors.New("second read failed")
		},
	}
	client := &Client{ledger: ml, clock: realClock{}}
	if _, err := client.Snapshot(context.Background(), receipt); err == nil || err.Error() != "second read failed" {
		t.Fatalf("expected second read failed, got %v", err)
	}

	// Cancel context during second read to trigger if ctx.Err() != nil at line 66
	readCount = 0
	cancellableCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ml2 := &mockLedger{
		readContextFunc: func(ctx context.Context, fn func(*ledgerState) error) error {
			readCount++
			state := emptyLedger()
			state.Runs[runID] = runRecord{Receipt: receipt}
			_ = fn(&state)
			if readCount == 2 {
				cancel()
			}
			return nil
		},
	}
	client2 := &Client{ledger: ml2, clock: realClock{}}
	if _, err := client2.Snapshot(cancellableCtx, receipt); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected deadline error when context canceled during read, got %v", err)
	}
}

type staticProvider struct {
	id  Identity
	err error
}

func (s staticProvider) Authorize(_ context.Context, base http.RoundTripper) (Identity, http.RoundTripper, error) {
	if s.err != nil {
		return Identity{}, nil, s.err
	}
	return s.id, base, nil
}

type advancingProvider struct {
	fc *fakeClock
	id Identity
}

func (a advancingProvider) Authorize(context.Context, http.RoundTripper) (Identity, http.RoundTripper, error) {
	a.fc.now = a.fc.now.Add(20 * time.Second)
	return a.id, nil, nil
}

func TestRebindJob_Branches(t *testing.T) {
	t.Parallel()

	c, run, receipt, cursor := snapshotRun(t, NewMemoryLedger())
	origPrepare := c.prepare
	origProvider := c.provider

	// 1. Missing run in ledger
	bogusReceipt := receipt
	bogusReceipt.RunID = "missing-run-id"
	if _, err := c.RebindJob(context.Background(), bogusReceipt, cursor); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid for missing run, got %v", err)
	}

	// 2. Lease failure
	origLedger := c.ledger
	c.ledger = &mockLedger{
		readContextFunc: origLedger.readContext,
		leaseFunc: func(context.Context, string) (func(), error) {
			return nil, fail("local_stopped")
		},
	}
	if _, err := c.RebindJob(context.Background(), receipt, cursor); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on lease fail, got %v", err)
	}
	c.ledger = origLedger

	// 3. Second loadRun failure
	loadCount := 0
	c.ledger = &mockLedger{
		readContextFunc: func(ctx context.Context, fn func(*ledgerState) error) error {
			loadCount++
			if loadCount == 1 {
				return origLedger.readContext(ctx, fn)
			}
			return errors.New("second load failed")
		},
		leaseFunc: func(context.Context, string) (func(), error) { return func() {}, nil },
	}
	if _, err := c.RebindJob(context.Background(), receipt, cursor); err == nil || err.Error() != "second load failed" {
		t.Fatalf("expected second load failed, got %v", err)
	}
	c.ledger = origLedger

	// 4. Job == nil
	noJobReceipt := receipt
	noJobReceipt.Job = nil
	_ = c.ledger.update(func(s *ledgerState) error {
		r := s.Runs[run.id]
		r.Receipt = noJobReceipt
		s.Runs[run.id] = r
		return nil
	})
	if _, err := c.RebindJob(context.Background(), noJobReceipt, cursor); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid when Job == nil, got %v", err)
	}

	// Reset run
	_ = c.ledger.update(func(s *ledgerState) error {
		r := s.Runs[run.id]
		r.Receipt = receipt
		s.Runs[run.id] = r
		return nil
	})

	// 5. Cursor mismatch
	if _, err := c.RebindJob(context.Background(), receipt, "bad-cursor"); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on cursor mismatch, got %v", err)
	}

	// 6. Cursor == nil but caller passed encoded cursor
	_ = c.ledger.update(func(s *ledgerState) error {
		r := s.Runs[run.id]
		r.Cursor = nil
		s.Runs[run.id] = r
		return nil
	})
	if _, err := c.RebindJob(context.Background(), receipt, cursor); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid when cursor is nil but token passed, got %v", err)
	}

	// 7. preparation failure (total response bytes exceeded)
	_ = c.ledger.update(func(s *ledgerState) error {
		r := s.Runs[run.id]
		r.Receipt.Counters.Bytes = r.Receipt.Bounds.TotalResponseBytes
		s.Runs[run.id] = r
		return nil
	})
	badReceipt := receipt
	badReceipt.Counters.Bytes = receipt.Bounds.TotalResponseBytes
	if _, err := c.RebindJob(context.Background(), badReceipt, ""); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit when bytes exhausted, got %v", err)
	}

	// Reset counters
	_ = c.ledger.update(func(s *ledgerState) error {
		r := s.Runs[run.id]
		r.Receipt.Counters.Bytes = 100
		s.Runs[run.id] = r
		return nil
	})

	// 8. reauthorize error
	c.prepare = func(context.Context) (ReadPlan, string, error) {
		return ReadPlan{}, "", errors.New("prepare fail")
	}
	if _, err := c.RebindJob(context.Background(), receipt, ""); err == nil {
		t.Fatal("expected error from reauthorize")
	}
	c.prepare = origPrepare

	// 9. provider.Authorize returns error
	c.provider = staticProvider{err: errors.New("auth provider error")}
	if _, err := c.RebindJob(context.Background(), receipt, ""); errorCode(err) != "auth_required" {
		t.Fatalf("expected auth_required, got %v", err)
	}

	// 10. !validPrincipal(identity.Principal)
	now := c.clock.Now()
	c.provider = staticProvider{id: Identity{Principal: Principal{}, Read: true, ExpiresAt: now.Add(time.Hour)}}
	if _, err := c.RebindJob(context.Background(), receipt, ""); errorCode(err) != "auth_required" {
		t.Fatalf("expected auth_required for invalid principal, got %v", err)
	}

	// 11. !identity.Read
	c.provider = staticProvider{id: Identity{
		Principal: Principal{Kind: "workload", Subject: "operator:fixture", Generation: "2"},
		Read:      false,
		ExpiresAt: now.Add(time.Hour),
	}}
	if _, err := c.RebindJob(context.Background(), receipt, ""); errorCode(err) != "scope_missing" {
		t.Fatalf("expected scope_missing, got %v", err)
	}

	// 12. identity.ExpiresAt expired
	c.provider = staticProvider{id: Identity{
		Principal: Principal{Kind: "workload", Subject: "operator:fixture", Generation: "2"},
		Read:      true,
		ExpiresAt: now.Add(-time.Hour),
	}}
	if _, err := c.RebindJob(context.Background(), receipt, ""); errorCode(err) != "auth_expired" {
		t.Fatalf("expected auth_expired, got %v", err)
	}

	// 13. Principal approval_changed (same generation as active)
	c.provider = staticProvider{id: Identity{
		Principal: Principal{Kind: "workload", Subject: "operator:fixture", Generation: "1"}, // same gen
		Read:      true,
		ExpiresAt: now.Add(time.Hour),
	}}
	if _, err := c.RebindJob(context.Background(), receipt, ""); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed for same generation, got %v", err)
	}

	// 14. Principal approval_changed (different subject)
	c.provider = staticProvider{id: Identity{
		Principal: Principal{Kind: "workload", Subject: "other-user", Generation: "2"},
		Read:      true,
		ExpiresAt: now.Add(time.Hour),
	}}
	if _, err := c.RebindJob(context.Background(), receipt, ""); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed for different subject, got %v", err)
	}

	// 15. ExecutionDeadline before HTTPMs deadline (covers line 99)
	validNewPrincipal := Principal{Kind: "workload", Subject: "operator:fixture", Generation: "2"}
	c.provider = staticProvider{id: Identity{
		Principal: validNewPrincipal,
		Read:      true,
		ExpiresAt: now.Add(time.Hour),
	}}
	_ = c.ledger.update(func(s *ledgerState) error {
		r := s.Runs[run.id]
		r.Receipt.ExecutionDeadline = now.Add(100 * time.Millisecond) // sooner than HTTPMs
		r.Cursor = nil
		s.Runs[run.id] = r
		return nil
	})
	receiptWithShortDeadline := receipt
	receiptWithShortDeadline.ExecutionDeadline = now.Add(100 * time.Millisecond)

	// Successful rebind with nil Cursor (creates new cursor state!)
	res, err := c.RebindJob(context.Background(), receiptWithShortDeadline, "")
	if err != nil {
		t.Fatalf("unexpected error on valid rebind: %v", err)
	}
	if res.Cursor == "" {
		t.Fatal("expected non-empty cursor generated")
	}

	// 16. MutateRun race condition: current.ActivePrincipal != rr.ActivePrincipal
	gen3Principal := Principal{Kind: "workload", Subject: "operator:fixture", Generation: "3"}
	cRace := &Client{
		ledger: &mockLedger{
			readContextFunc: origLedger.readContext,
			leaseFunc:       origLedger.lease,
			updateContextFunc: func(ctx context.Context, fn func(*ledgerState) error) error {
				return origLedger.updateContext(ctx, func(s *ledgerState) error {
					r := s.Runs[run.id]
					r.ActivePrincipal.Generation = "raced-gen"
					s.Runs[run.id] = r
					return fn(s)
				})
			},
		},
		clock:     c.clock,
		prepare:   c.prepare,
		provider:  staticProvider{id: Identity{Principal: gen3Principal, Read: true, ExpiresAt: now.Add(time.Hour)}},
		profiles:  c.profiles,
		transport: c.transport,
	}
	if _, err := cRace.RebindJob(context.Background(), receiptWithShortDeadline, res.Cursor); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on active principal race, got %v", err)
	}

	// 17. Expired run rebind (covers line 97)
	gen4Principal := Principal{Kind: "workload", Subject: "operator:fixture", Generation: "4"}
	c.provider = staticProvider{id: Identity{Principal: gen4Principal, Read: true, ExpiresAt: now.Add(time.Hour)}}
	_ = c.ledger.update(func(s *ledgerState) error {
		r := s.Runs[run.id]
		r.Receipt.ExecutionDeadline = now.Add(-time.Hour)
		r.Cursor = nil
		s.Runs[run.id] = r
		return nil
	})
	expiredReceipt := receiptWithShortDeadline
	expiredReceipt.ExecutionDeadline = now.Add(-time.Hour)
	if _, err := c.RebindJob(context.Background(), expiredReceipt, ""); err != nil {
		t.Fatalf("unexpected error re-binding expired job: %v", err)
	}

	// 18. Clock advanced past deadline (covers line 102)
	latestRun, _ := c.loadRun(run.id)
	rawCur, _ := json.Marshal(latestRun.Cursor)
	curToken := base64.RawURLEncoding.EncodeToString(rawCur)

	fc := &fakeClock{now: now}
	notExpiredReceipt := latestRun.Receipt
	notExpiredReceipt.ExecutionDeadline = now.Add(10 * time.Hour)
	_ = c.ledger.update(func(s *ledgerState) error {
		r := s.Runs[run.id]
		r.Receipt = notExpiredReceipt
		s.Runs[run.id] = r
		return nil
	})
	cFC := &Client{
		ledger:    origLedger,
		clock:     fc,
		prepare:   origPrepare,
		provider:  advancingProvider{fc: fc, id: Identity{Principal: Principal{Kind: "workload", Subject: "operator:fixture", Generation: "5"}, Read: true, ExpiresAt: now.Add(20 * time.Hour)}},
		profiles:  c.profiles,
		transport: c.transport,
	}
	if _, err := cFC.RebindJob(context.Background(), notExpiredReceipt, curToken); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected deadline error when clock advanced past deadline, got %v", err)
	}

	// 19. remaining(scope) failure in preparation (covers line 20)
	readCalls := 0
	cRemainingFail := &Client{
		ledger: &mockLedger{
			leaseFunc: origLedger.lease,
			readContextFunc: func(ctx context.Context, fn func(*ledgerState) error) error {
				readCalls++
				if readCalls == 3 { // First 2 are loadRun in RebindJob, 3rd is loadRun in remaining()
					return errors.New("remaining loadRun failed")
				}
				return origLedger.readContext(ctx, fn)
			},
		},
		clock:     c.clock,
		prepare:   origPrepare,
		provider:  c.provider,
		profiles:  c.profiles,
		transport: c.transport,
	}
	if _, err := cRemainingFail.RebindJob(context.Background(), notExpiredReceipt, curToken); err == nil || err.Error() != "remaining loadRun failed" {
		t.Fatalf("expected remaining loadRun failed, got %v", err)
	}
	c.provider = origProvider
}

func TestRebind_CheckBound(t *testing.T) {
	t.Parallel()

	c := &Client{clock: realClock{}}

	// ctx.Err() != nil
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.checkBound(canceledCtx); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on canceled context, got %v", err)
	}

	// clockBoundKey in the past
	pastCtx := context.WithValue(context.Background(), clockBoundKey{}, time.Now().Add(-time.Hour))
	if err := c.checkBound(pastCtx); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected deadline error on past clock bound, got %v", err)
	}

	// clockBoundKey in the future
	futureCtx := context.WithValue(context.Background(), clockBoundKey{}, time.Now().Add(time.Hour))
	if err := c.checkBound(futureCtx); err != nil {
		t.Fatalf("expected nil error on future bound, got %v", err)
	}
}
