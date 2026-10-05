package bigquery

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"
)

// A finite ignored-cancellation dependency may outlive its caller. It owns no
// continuation authority. This process-wide bound prevents repeated local calls
// from accumulating unlimited abandoned workers; it is not a job/session limit.
var dependencySlots = make(chan struct{}, 64)

type dependencyResult[T any] struct {
	value T
	err   error
}

func awaitDependency[T any](ctx context.Context, fn func() (T, error), discard func(T)) (T, error) {
	value, err, _ := awaitOwnedDependency(ctx, fn, discard, nil)
	return value, err
}

// admitted transfers lifecycle ownership even if the caller times out before the
// worker runs. finish remains inside that worker's bounded admission slot.
func awaitOwnedDependency[T any](ctx context.Context, fn func() (T, error), discard func(T), finish func(T)) (value T, err error, admitted bool) {
	var zero T
	if ctx.Err() != nil {
		return zero, deadlineError(), false
	}
	select {
	case dependencySlots <- struct{}{}:
	default:
		return zero, fail("local_stopped"), false
	}
	done := make(chan dependencyResult[T])
	accepted := make(chan bool, 1)
	go func() {
		defer func() { <-dependencySlots }()
		v, e := fn()
		if finish != nil {
			defer finish(v)
		}
		select {
		case done <- dependencyResult[T]{v, e}:
			if !<-accepted && discard != nil {
				discard(v)
			}
		case <-ctx.Done():
			if discard != nil {
				discard(v)
			}
		}
	}()
	select {
	case result := <-done:
		if ctx.Err() != nil {
			accepted <- false
			return zero, deadlineError(), true
		}
		accepted <- true
		return result.value, result.err, true
	case <-ctx.Done():
		return zero, deadlineError(), true
	}
}
func discardAsync[T any](v T, fn func(T)) {
	select {
	case dependencySlots <- struct{}{}:
		go func() { defer func() { <-dependencySlots }(); fn(v) }()
	default:
	}
}
func closeBody(body io.Closer) {
	if tracked, ok := body.(*trackedBody); ok && tracked.closeRequested != nil {
		tracked.requestClose()
		return
	}
	if body != nil {
		discardAsync(body, func(b io.Closer) { _ = b.Close() })
	}
}

type authorized struct {
	identity  Identity
	transport http.RoundTripper
}

func (c *Client) authorize(ctx context.Context, base http.RoundTripper) (Identity, http.RoundTripper, error) {
	value, e := awaitDependency(ctx, func() (authorized, error) { i, t, e := c.provider.Authorize(ctx, base); return authorized{i, t}, e }, nil)
	return value.identity, value.transport, e
}

type prepared struct {
	plan   ReadPlan
	policy string
}

func (c *Client) prepareBounded(ctx context.Context) (ReadPlan, string, error) {
	v, e := awaitDependency(ctx, func() (prepared, error) { p, s, e := c.prepare(ctx); return prepared{p, s}, e }, nil)
	return v.plan, v.policy, e
}

var actualRequests sync.Map

func enterActualRequest(id string) (func(), error) {
	if id == "" {
		return func() {}, nil
	}
	_, busy := actualRequests.LoadOrStore(id, true)
	if busy {
		return nil, fail("local_stopped")
	}
	var once sync.Once
	return func() { once.Do(func() { actualRequests.Delete(id) }) }, nil
}
func boundedContext(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		d = time.Nanosecond
	}
	return context.WithTimeout(ctx, d)
}

// EOF stops further reads but never ends close ownership. An admitted transport
// worker retains its slot until physical Close completes. Concurrent active Read
// also keeps the run marker held, even if the injected Close returns early.
type trackedBody struct {
	body           io.ReadCloser
	leave          func()
	mu             sync.Mutex
	reads          int
	eof, closed    bool
	once           sync.Once
	closeOnce      sync.Once
	requestOnce    sync.Once
	closeRequested chan struct{}
	closeDone      chan struct{}
	closeErr       error
}

func newTrackedBody(body io.ReadCloser, leave func()) *trackedBody {
	return &trackedBody{body: body, leave: leave, closeRequested: make(chan struct{}), closeDone: make(chan struct{})}
}
func (t *trackedBody) releaseLocked() {
	if t.closed && t.reads == 0 {
		t.once.Do(t.leave)
	}
}
func (t *trackedBody) Read(p []byte) (int, error) {
	t.mu.Lock()
	if t.eof || t.closed {
		t.mu.Unlock()
		return 0, io.EOF
	}
	t.reads++
	t.mu.Unlock()
	n, e := t.body.Read(p)
	t.mu.Lock()
	t.reads--
	if e == io.EOF {
		t.eof = true
	}
	t.releaseLocked()
	t.mu.Unlock()
	return n, e
}
func (t *trackedBody) requestClose() { t.requestOnce.Do(func() { close(t.closeRequested) }) }
func (t *trackedBody) physicalClose() {
	t.closeOnce.Do(func() {
		e := t.body.Close()
		t.mu.Lock()
		t.closeErr = e
		t.closed = true
		t.releaseLocked()
		t.mu.Unlock()
		if t.closeDone != nil {
			close(t.closeDone)
		}
	})
}

// Called only by the already-admitted actual transport worker: cleanup cannot
// lose admission at a full pool or spawn another unbounded worker.
func (t *trackedBody) ownClose() { <-t.closeRequested; t.physicalClose() }
func (t *trackedBody) Close() error {
	if t.closeRequested != nil {
		t.requestClose()
		<-t.closeDone
	} else {
		t.physicalClose()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closeErr
}
func (t *trackedBody) closeWithin(ctx context.Context) error {
	t.requestClose()
	select {
	case <-t.closeDone:
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.closeErr
	case <-ctx.Done():
		return deadlineError()
	}
}

// Recovery never waits indefinitely for a contended ledger. If it cannot commit,
// the durable submitting/reserved record conservatively retains its entire cap.
func (c *Client) settleRun(id string, fn func(*runRecord) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	return c.mutateRunContext(ctx, id, fn)
}
