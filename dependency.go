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
	var zero T
	if ctx.Err() != nil {
		return zero, deadlineError()
	}
	select {
	case dependencySlots <- struct{}{}:
	default:
		return zero, fail("local_stopped")
	}
	done := make(chan dependencyResult[T])
	accepted := make(chan bool, 1)
	go func() {
		defer func() { <-dependencySlots }()
		v, e := fn()
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
			return zero, deadlineError()
		}
		accepted <- true
		return result.value, result.err
	case <-ctx.Done():
		return zero, deadlineError()
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
	return func() { actualRequests.Delete(id) }, nil
}
func boundedContext(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		d = time.Nanosecond
	}
	return context.WithTimeout(ctx, d)
}

// Keep actual in-flight authority until both a read and close have physically
// finished. A malicious Close returning early cannot free a still-running Read.
type trackedBody struct {
	body     io.ReadCloser
	leave    func()
	mu       sync.Mutex
	reads    int
	finished bool
	once     sync.Once
}

func (t *trackedBody) releaseLocked() {
	if t.finished && t.reads == 0 {
		t.once.Do(t.leave)
	}
}
func (t *trackedBody) Read(p []byte) (int, error) {
	t.mu.Lock()
	if t.finished {
		t.mu.Unlock()
		return 0, io.EOF
	}
	t.reads++
	t.mu.Unlock()
	n, e := t.body.Read(p)
	t.mu.Lock()
	t.reads--
	if e == io.EOF {
		t.finished = true
	}
	t.releaseLocked()
	t.mu.Unlock()
	return n, e
}
func (t *trackedBody) Close() error {
	e := t.body.Close()
	t.mu.Lock()
	t.finished = true
	t.releaseLocked()
	t.mu.Unlock()
	return e
}

// Recovery never waits indefinitely for a contended ledger. If it cannot commit,
// the durable submitting/reserved record conservatively retains its entire cap.
func (c *Client) settleRun(id string, fn func(*runRecord) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	return c.mutateRunContext(ctx, id, fn)
}
