package bigquery

import (
	"context"
	"errors"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type funcClock struct {
	realClock
	nowFn func() time.Time
}

func (f funcClock) Now() time.Time {
	if f.nowFn != nil {
		return f.nowFn()
	}
	return time.Now()
}

// Not parallel: mutates sourceGetFn
func TestCoverage10_SourceRows_LoadPage_Branches(t *testing.T) {
	orig := sourceGetFn
	defer func() { sourceGetFn = orig }()

	// 1. source_rows.go:173 - m["rows"] not a slice
	sourceGetFn = func(*sourceDatabase, context.Context, Principal, func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{
			"kind":      "bigquery#tableDataList",
			"totalRows": "0",
			"rows":      "invalid_rows_not_a_slice",
		}, 10, nil
	}
	cursor := &sourceRowCursor{
		db:       &sourceDatabase{limits: SourceLimits{MaxPages: 10, MaxBytes: 10000, PageSize: 10}},
		expected: 0,
		seen:     map[string]bool{},
	}
	err := cursor.loadPage()
	if errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 2. source_rows.go:188 - sourceToken returns error for invalid pageToken
	sourceGetFn = func(*sourceDatabase, context.Context, Principal, func(sdkAPI) error) (map[string]any, int64, error) {
		return map[string]any{
			"kind":      "bigquery#tableDataList",
			"totalRows": "0",
			"rows":      []any{},
			"pageToken": 12345, // int instead of string
		}, 10, nil
	}
	cursor = &sourceRowCursor{
		db:       &sourceDatabase{limits: SourceLimits{MaxPages: 10, MaxBytes: 10000, PageSize: 10}},
		expected: 0,
		seen:     map[string]bool{},
	}
	err = cursor.loadPage()
	if errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on invalid pageToken, got %v", err)
	}
}

type lineCancelCtx struct {
	context.Context
	targetLine string
}

func (c *lineCancelCtx) Err() error {
	if c.targetLine != "" {
		buf := make([]byte, 1024)
		n := runtime.Stack(buf, false)
		if strings.Contains(string(buf[:n]), c.targetLine) {
			return context.Canceled
		}
	}
	return nil
}

func (c *lineCancelCtx) Done() <-chan struct{} {
	return make(chan struct{})
}

func TestCoverage10_Wire_DispatchGate_RoundTrip_ContextCancelled(t *testing.T) {
	t.Parallel()

	// 3. wire.go:101-103 - req.Context().Err() non-nil at line 100
	ctx100 := &lineCancelCtx{Context: context.Background(), targetLine: "wire.go:100"}
	gate1 := &dispatchGate{
		base: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
	}
	req1, _ := http.NewRequestWithContext(ctx100, "GET", "https://bigquery.googleapis.com/bigquery/v2/test", nil)
	_, err := gate1.RoundTrip(req1)
	if errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on line 100 cancel, got %v", err)
	}

	// 4. wire.go:144-145 - req.Context().Err() non-nil at line 142 with non-nil resp
	ctx142 := &lineCancelCtx{Context: context.Background(), targetLine: "wire.go:142"}
	bodyClosed := make(chan struct{})
	gate2 := &dispatchGate{
		base: rt(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       &trackedCloseReader{ReadCloser: io.NopCloser(strings.NewReader("{}")), onClose: func() { close(bodyClosed) }},
			}, nil
		}),
	}
	req2, _ := http.NewRequestWithContext(ctx142, "GET", "https://bigquery.googleapis.com/bigquery/v2/test", nil)
	_, err = gate2.RoundTrip(req2)
	if errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on line 142 cancel, got %v", err)
	}
	select {
	case <-bodyClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("expected body to be closed via line 144")
	}
}

func TestCoverage10_Wire_BoundedTransport_RoundTrip_Discard_Close(t *testing.T) {
	t.Parallel()

	// 5. wire.go:200-201 - discard callback closes body when context cancelled during roundtrip
	bodyClosed := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	transport := &boundedTransport{
		guard: &dispatchGate{},
		gate: rt(func(r *http.Request) (*http.Response, error) {
			cancel() // cancel context during roundtrip
			return &http.Response{
				StatusCode: 200,
				Body:       &trackedCloseReader{ReadCloser: io.NopCloser(strings.NewReader("{}")), onClose: func() { close(bodyClosed) }},
			}, nil
		}),
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://bigquery.googleapis.com/bigquery/v2/test", nil)
	_, _ = transport.RoundTrip(req)
	select {
	case <-bodyClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("expected body to be closed via discard in line 200-201")
	}
}

type advanceOnMutateLedger struct {
	Ledger
	advanceFn func()
}

func (a advanceOnMutateLedger) updateContext(ctx context.Context, fn func(*ledgerState) error) error {
	if a.advanceFn != nil {
		a.advanceFn()
	}
	return a.Ledger.updateContext(ctx, fn)
}

func TestCoverage10_Wire_ValidateResponse_Bound_After_Charge(t *testing.T) {
	t.Parallel()

	// 6. wire.go:261-263 - bound becomes non-nil right after charge
	memLedger := NewMemoryLedger()
	_ = memLedger.updateContext(context.Background(), func(s *ledgerState) error {
		s.Runs["test-run"] = runRecord{Receipt: Receipt{Bounds: Bounds{TotalResponseBytes: 10000}}}
		return nil
	})
	start := time.Now()
	var clockOffset atomic.Int64
	clk := funcClock{nowFn: func() time.Time {
		return start.Add(time.Duration(clockOffset.Load()))
	}}

	ledger := advanceOnMutateLedger{
		Ledger: memLedger,
		advanceFn: func() {
			clockOffset.Store(int64(2 * time.Hour)) // advance clock past deadline during charge
		},
	}
	client, _ := NewClient(Config{
		Profiles:  []SourceProfile{testProfile(t)},
		Provider:  &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(10 * time.Hour)}},
		Ledger:    ledger,
		Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
		Clock:     clk,
	})
	scope := &operationScope{
		runID:    "test-run",
		deadline: start.Add(time.Hour),
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 1000},
	}
	bt := &boundedTransport{
		c:        client,
		s:        scope,
		ctx:      context.Background(),
		deadline: start.Add(time.Hour),
	}
	resp := &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}
	_, err := bt.validateResponse(resp, nil)
	if errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped after charge deadline expiry, got %v", err)
	}
}

type stackClock struct {
	realClock
	start      time.Time
	targetLine string
}

func (c *stackClock) Now() time.Time {
	if c.targetLine != "" {
		buf := make([]byte, 1024)
		n := runtime.Stack(buf, false)
		if strings.Contains(string(buf[:n]), c.targetLine) {
			return c.start.Add(2 * time.Hour) // expired
		}
	}
	return c.start
}

func TestCoverage10_Wire_ValidateResponse_Deadline_After_Object(t *testing.T) {
	t.Parallel()

	// 7. wire.go:285-287 - deadline expires after object decoding (lines 284-287)
	memLedger := NewMemoryLedger()
	_ = memLedger.updateContext(context.Background(), func(s *ledgerState) error {
		s.Runs["test-run"] = runRecord{Receipt: Receipt{Bounds: Bounds{TotalResponseBytes: 10000}}}
		return nil
	})
	start := time.Now()
	clk := &stackClock{start: start, targetLine: "wire.go:284"}
	client, _ := NewClient(Config{
		Profiles:  []SourceProfile{testProfile(t)},
		Provider:  &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(10 * time.Hour)}},
		Ledger:    memLedger,
		Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
		Clock:     clk,
	})
	scope := &operationScope{
		runID:    "test-run",
		deadline: start.Add(time.Hour),
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 65536, HTTPMs: 1000},
	}
	bt := &boundedTransport{
		c:        client,
		s:        scope,
		ctx:      context.Background(),
		deadline: start.Add(time.Hour),
	}
	resp := &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}
	_, err := bt.validateResponse(resp, nil)
	if errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped after object decoding deadline expiry, got %v", err)
	}
}

func TestCoverage10_Wire_Call_Remaining_Zero_In_BeforeDispatch(t *testing.T) {
	t.Parallel()

	// 8. wire.go:401-402 - remaining <= 0 in gate.beforeDispatch
	client, _ := NewClient(Config{
		Profiles: []SourceProfile{testProfile(t)},
		Provider: &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:   NewMemoryLedger(),
		Transport: rt(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
		}),
		Prepare: func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	scope := &operationScope{
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 1000, HTTPMs: 1000},
		deadline: time.Now().Add(time.Hour),
	}
	principal := Principal{Kind: "workload", Subject: "s", Generation: "1"}
	_, err := client.call(context.Background(), scope, &principal, false, func(api sdkAPI) error {
		scope.bytes = 1000 // consume all remaining bytes
		call := api.service.Datasets.Get("p", "d")
		_, err := call.Context(api.ctx).Do()
		return err
	})
	if errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit, got %v", err)
	}
}

func TestCoverage10_Wire_Call_Raw_Error_With_Canceled_Ctx(t *testing.T) {
	t.Parallel()

	// 9. wire.go:434-435 - untyped non-Error error with ctx.Err() != nil at line 433
	client, _ := NewClient(Config{
		Profiles:  []SourceProfile{testProfile(t)},
		Provider:  &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:    NewMemoryLedger(),
		Transport: rt(func(*http.Request) (*http.Response, error) { return nil, nil }),
		Prepare:   func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
	})
	scope := &operationScope{
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 1000, HTTPMs: 1000},
		deadline: time.Now().Add(time.Hour),
	}
	principal := Principal{Kind: "workload", Subject: "s", Generation: "1"}
	ctx := &lineCancelCtx{Context: context.Background(), targetLine: "wire.go:433"}
	_, err := client.call(ctx, scope, &principal, false, func(api sdkAPI) error {
		return errors.New("raw non-Error failure")
	})
	if errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped, got %v", err)
	}
}

type sleepErrClock struct {
	realClock
}

func (sleepErrClock) Sleep(ctx context.Context, d time.Duration) error {
	return fail("local_stopped")
}

func TestCoverage10_Wire_Call_Sleep_Error_On_GET_Retry(t *testing.T) {
	t.Parallel()

	// 10. wire.go:471-472 - error returned from c.clock.Sleep during GET retry
	client, _ := NewClient(Config{
		Profiles: []SourceProfile{testProfile(t)},
		Provider: &testProvider{identity: Identity{Principal: Principal{Kind: "workload", Subject: "s", Generation: "1"}, Read: true, ExpiresAt: time.Now().Add(time.Hour)}},
		Ledger:   NewMemoryLedger(),
		Transport: rt(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader("{}"))}, nil
		}),
		Prepare: func(context.Context) (ReadPlan, string, error) { return ReadPlan{}, "", nil },
		Clock:   sleepErrClock{},
	})
	scope := &operationScope{
		bounds:   Bounds{ResponseBytes: 1024, TotalResponseBytes: 1000, HTTPMs: 1000},
		deadline: time.Now().Add(time.Hour),
	}
	principal := Principal{Kind: "workload", Subject: "s", Generation: "1"}
	_, err := client.call(context.Background(), scope, &principal, false, func(api sdkAPI) error {
		call := api.service.Datasets.Get("p", "d")
		_, err := call.Context(api.ctx).Do()
		return err
	})
	if errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on sleep failure, got %v", err)
	}
}
