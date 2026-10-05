package bigquery

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	bq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const apiOrigin = "https://bigquery.googleapis.com/bigquery/v2/"

func hashBytes(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

type operationScope struct {
	result          bool
	dispatched      bool
	controlDeadline time.Time
	bounds          Bounds
	deadline        time.Time
	bytes           int64
	runID           string
	submit          bool
}

func newPreviewScope(b Bounds, now time.Time) *operationScope {
	return &operationScope{bounds: b, deadline: now.Add(time.Duration(b.WallMs) * time.Millisecond)}
}

type sdkAPI struct {
	service *bq.Service
	ctx     context.Context
}
type dispatchGate struct {
	boundedResponse func(*http.Response, error) (*http.Response, error)
	beforeDispatch  func() error
	base            http.RoundTripper
	mu              sync.Mutex
	method, url     string
	bodyDigest      string
	attempts        int
	submit          bool
	dispatched      bool
}

func (g *dispatchGate) RoundTrip(req *http.Request) (*http.Response, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if req.URL.Scheme != "https" || req.URL.Host != "bigquery.googleapis.com" || req.URL.User != nil || !strings.HasPrefix(req.URL.Path, "/bigquery/v2/") || req.URL.Fragment != "" {
		return nil, fail("invalid_input")
	}
	var digest string
	if req.Body != nil {
		raw, e := io.ReadAll(io.LimitReader(req.Body, 256<<10+1))
		req.Body.Close()
		if e != nil || len(raw) > 256<<10 {
			return nil, fail("invalid_input")
		}
		digest = hashBytes(raw)
		req.Body = io.NopCloser(bytes.NewReader(raw))
		req.GetBody = nil
	}
	if g.method == "" {
		g.method, g.url, g.bodyDigest = req.Method, req.URL.String(), digest
	} else if g.method != req.Method || g.url != req.URL.String() || g.bodyDigest != digest {
		return nil, fail("invalid_input")
	}
	g.attempts++
	if req.Method != "GET" && g.attempts > 1 {
		return nil, fail("submission_unknown")
	}
	if req.Method == "GET" && g.attempts > 3 {
		return nil, fail("response_limit")
	}
	// No idempotency markers and no replayable body reaches the actual dispatch.
	req.Header.Del("Idempotency-Key")
	req.Header.Del("X-Idempotency-Key")
	if g.beforeDispatch != nil {
		if e := g.beforeDispatch(); e != nil {
			return nil, e
		}
	}
	g.dispatched = true
	resp, e := g.base.RoundTrip(req)
	if g.boundedResponse != nil {
		return g.boundedResponse(resp, e)
	}
	return resp, e
}

type boundedTransport struct {
	guard      *dispatchGate
	gate       http.RoundTripper
	c          *Client
	s          *operationScope
	raw        any
	status     int
	retryAfter string
	readErr    error
	ctx        context.Context
	deadline   time.Time
}

func (t *boundedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var expected []byte
	if req.Body != nil {
		var e error
		expected, e = io.ReadAll(io.LimitReader(req.Body, 256<<10+1))
		req.Body.Close()
		if e != nil || len(expected) > 256<<10 {
			return nil, fail("invalid_input")
		}
		req.Body = io.NopCloser(bytes.NewReader(expected))
		req.GetBody = nil
	}
	digest := ""
	if req.Body != nil {
		digest = hashBytes(expected)
	}
	t.guard.mu.Lock()
	if t.guard.method == "" {
		t.guard.method, t.guard.url, t.guard.bodyDigest = req.Method, req.URL.String(), digest
	}
	matches := t.guard.method == req.Method && t.guard.url == req.URL.String() && t.guard.bodyDigest == digest
	t.guard.mu.Unlock()
	if !matches {
		return nil, fail("invalid_input")
	}
	return t.gate.RoundTrip(req)
}
func (t *boundedTransport) validateResponse(resp *http.Response, e error) (*http.Response, error) {
	t.raw = nil
	t.readErr = nil
	t.status = 0
	t.retryAfter = ""
	if e != nil {
		return nil, e
	}
	if resp == nil || resp.Body == nil {
		return nil, fail("malformed_wire")
	}
	t.status = resp.StatusCode
	t.retryAfter = resp.Header.Get("Retry-After")
	defer resp.Body.Close()
	if e := t.bound(); e != nil {
		t.readErr = e
		return nil, e
	}
	var reader io.Reader = resp.Body
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" {
		if encoding != "gzip" {
			return nil, fail("malformed_wire")
		}
		gz, e := gzip.NewReader(reader)
		if e != nil {
			return nil, fail("malformed_wire")
		}
		defer gz.Close()
		reader = gz
		resp.Header.Del("Content-Encoding")
	}
	remaining, e := t.c.remaining(t.s)
	if e != nil {
		return nil, e
	}
	limit := int64(t.s.bounds.ResponseBytes)
	if remaining < limit {
		limit = remaining
	}
	raw, e := io.ReadAll(io.LimitReader(boundReader{t, reader}, limit+1))
	if charge := t.c.charge(t.s, int64(len(raw))); charge != nil {
		t.readErr = charge
		return nil, charge
	}
	if bound := t.bound(); bound != nil {
		t.readErr = bound
		return nil, bound
	}
	if e != nil {
		t.readErr = fail("malformed_wire")
		return nil, t.readErr
	}
	if int64(len(raw)) > limit {
		t.readErr = fail("response_limit")
		return nil, t.readErr
	}
	v, e := ParseJSON(raw, t.s.bounds.ResponseBytes)
	if e != nil {
		t.readErr = e
		return nil, e
	}
	if _, e = object(v); e != nil {
		t.readErr = e
		return nil, e
	}
	t.raw = v
	if t.ctx != nil && (t.ctx.Err() != nil || !t.c.clock.Now().Before(t.deadline)) {
		t.readErr = deadlineError()
		return nil, t.readErr
	}
	resp.Body = io.NopCloser(bytes.NewReader(raw))
	resp.ContentLength = int64(len(raw))
	return resp, nil
}
func (c *Client) remaining(s *operationScope) (int64, error) {
	if s.runID != "" {
		r, e := c.loadRun(s.runID)
		if e != nil {
			return 0, e
		}
		return r.Receipt.Bounds.TotalResponseBytes - r.Receipt.Counters.Bytes, nil
	}
	return s.bounds.TotalResponseBytes - s.bytes, nil
}
func (c *Client) charge(s *operationScope, n int64) error {
	if s.runID != "" {
		return c.mutateRun(s.runID, func(r *runRecord) error { r.Receipt.Counters.Bytes += n; return nil })
	}
	s.bytes += n
	return nil
}
func validPrincipal(p Principal) bool {
	return (p.Kind == "google-user" || p.Kind == "workload") && p.Subject != "" && p.Generation != "" && len(p.Subject) <= 1024 && len(p.Generation) <= 1024
}
func (c *Client) call(ctx context.Context, s *operationScope, principal *Principal, control bool, fn func(sdkAPI) error) (any, error) {
	if control && s.controlDeadline.IsZero() {
		s.controlDeadline = c.clock.Now().Add(15 * time.Second)
	}
	attempts := 1
	var last error
	gate := &dispatchGate{base: c.transport, submit: s.submit}
	for attempt := 0; attempt < attempts; attempt++ {
		remaining, e := c.remaining(s)
		if e != nil {
			return nil, e
		}
		caller := c.callerDeadline(ctx)
		deadline, e := OperationDeadline(c.clock.Now(), s.deadline, caller, time.Duration(s.bounds.HTTPMs)*time.Millisecond, control, remaining)
		if e != nil {
			return nil, e
		}
		if control {
			if s.controlDeadline.Before(deadline) {
				deadline = s.controlDeadline
			}
			if !c.clock.Now().Before(deadline) {
				return nil, fail("local_stopped")
			}
		}
		if ctx.Err() != nil {
			return nil, fail("local_stopped")
		}
		op, cancel := context.WithTimeout(ctx, deadline.Sub(c.clock.Now()))
		identity, auth, e := c.provider.Authorize(op, gate)
		if op.Err() != nil || !c.clock.Now().Before(deadline) {
			cancel()
			return nil, deadlineError()
		}
		if e != nil {
			cancel()
			return nil, sanitizedAuth(e)
		}
		if !validPrincipal(identity.Principal) || auth == nil {
			cancel()
			return nil, fail("auth_required")
		}
		if !c.clock.Now().Before(identity.ExpiresAt) {
			cancel()
			return nil, fail("auth_expired")
		}
		if !identity.Read || (control && s.submit && !identity.Cancel) {
			cancel()
			return nil, fail("scope_missing")
		}
		if principal != nil && identity.Principal != *principal {
			cancel()
			return nil, fail("approval_changed")
		}
		// The provider may retry internally; the lower attempt gate still counts every
		// actual dispatch. The adapter's GET retry count is shared across outer calls.

		bounded := &boundedTransport{gate: auth, guard: gate, c: c, s: s, ctx: op, deadline: deadline}
		gate.boundedResponse = bounded.validateResponse
		previousBefore := func() error {
			if !s.result || s.runID == "" {
				return nil
			}
			return c.mutateRun(s.runID, func(r *runRecord) error {
				if r.Receipt.Counters.Pages >= r.Receipt.Bounds.MaxPages {
					return fail("response_limit")
				}
				r.Receipt.Counters.Pages++
				return nil
			})
		}
		gate.beforeDispatch = func() error {
			if op.Err() != nil || !c.clock.Now().Before(deadline) {
				return fail("local_stopped")
			}
			remaining, e := c.remaining(s)
			if e != nil {
				return e
			}
			if remaining <= 0 {
				return fail("response_limit")
			}
			if previousBefore != nil {
				return previousBefore()
			}
			return nil
		}
		httpClient := &http.Client{Transport: bounded, CheckRedirect: func(*http.Request, []*http.Request) error { return fail("remote_failed") }}
		service, e := bq.NewService(op, option.WithHTTPClient(httpClient), option.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
		if e != nil {
			cancel()
			return nil, fail("invalid_input")
		}
		service.BasePath = apiOrigin
		e = fn(sdkAPI{service, op})
		if s.submit && gate.dispatched {
			s.dispatched = true
		}
		cancel()
		if s.submit && !control && s.runID != "" && gate.dispatched {
			if m, ok := bounded.raw.(map[string]any); ok {
				_ = c.captureJob(s.runID, m)
			}
		}
		if e == nil {
			return bounded.raw, nil
		}
		if bounded.readErr != nil {
			return bounded.raw, bounded.readErr
		}
		var safe *Error
		if errors.As(e, &safe) {
			return bounded.raw, safe
		}
		if ctx.Err() != nil {
			return bounded.raw, fail("local_stopped")
		}
		code := "remote_failed"
		switch bounded.status {
		case 0:
			code = "remote_failed"
		case 401:
			code = "auth_expired"
		case 403:
			code = "scope_missing"
		case 404:
			code = "result_expired"
		default:
			if bounded.status >= 200 && bounded.status < 300 {
				code = "malformed_wire"
			}
		}
		last = fail(code)
		if gate.method == "GET" && (bounded.status == 429 || bounded.status >= 500 || bounded.status == 0) {
			attempts = 3
			if attempt+1 < attempts {
				delay := time.Duration(100*(1<<attempt)) * time.Millisecond
				if seconds, err := strconv.Atoi(bounded.retryAfter); err == nil && seconds >= 0 {
					delay = time.Duration(seconds) * time.Second
				} else if at, err := http.ParseTime(bounded.retryAfter); err == nil {
					delay = at.Sub(c.clock.Now())
				}
				if delay < 0 {
					delay = 0
				}
				if delay > 15*time.Second {
					delay = 15 * time.Second
				}
				if (!control && !c.clock.Now().Add(delay).Before(s.deadline)) || (control && !c.clock.Now().Add(delay).Before(s.controlDeadline)) {
					return bounded.raw, deadlineError()
				}
				if e = c.clock.Sleep(ctx, delay); e != nil {
					return bounded.raw, e
				}
				continue
			}
		}
		if s.submit && !control && gate.dispatched {
			if s.runID != "" {
				if r, err := c.loadRun(s.runID); err == nil && r.Receipt.Job != nil {
					return bounded.raw, last
				}
			}
			return bounded.raw, fail("submission_unknown")
		}
		return bounded.raw, last
	}
	return nil, last
}
func sanitizedAuth(e error) error {
	var safe *Error
	if errors.As(e, &safe) {
		switch safe.Code {
		case "auth_required", "auth_expired", "scope_missing", "policy_denied", "no_policies":
			return safe
		}
	}
	return fail("auth_required")
}

func (c *Client) checkIdentity(ctx context.Context, p Principal) error {
	if e := c.checkBound(ctx); e != nil {
		return e
	}
	identity, _, e := c.provider.Authorize(ctx, rejectTransport{})
	if bound := c.checkBound(ctx); bound != nil {
		return bound
	}
	if e != nil {
		return sanitizedAuth(e)
	}
	if !validPrincipal(identity.Principal) {
		return fail("auth_required")
	}
	if !c.clock.Now().Before(identity.ExpiresAt) {
		return fail("auth_expired")
	}
	if !identity.Read {
		return fail("scope_missing")
	}
	if identity.Principal != p {
		return fail("approval_changed")
	}
	return nil
}

type rejectTransport struct{}

func (rejectTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, fail("invalid_input")
}

func (t *boundedTransport) bound() error {
	if t.ctx != nil && (t.ctx.Err() != nil || !t.c.clock.Now().Before(t.deadline)) {
		return deadlineError()
	}
	return nil
}

type boundReader struct {
	t      *boundedTransport
	reader io.Reader
}

func (b boundReader) Read(p []byte) (int, error) {
	if e := b.t.bound(); e != nil {
		return 0, e
	}
	return b.reader.Read(p)
}
