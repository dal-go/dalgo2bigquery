package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/tugql"
	bigquery "github.com/dal-go/dalgo2bigquery"
)

type operatorProvider struct{ principal bigquery.Principal }

func (p operatorProvider) Authorize(ctx context.Context, base http.RoundTripper) (bigquery.Identity, http.RoundTripper, error) {
	return bigquery.Identity{Principal: p.principal, ExpiresAt: time.Now().Add(time.Hour), Read: true, Cancel: true}, base, nil
}

type testTransport func(*http.Request) (*http.Response, error)

func (f testTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestHandlerActualPreviewExecutePageStatus(t *testing.T) {
	t.Parallel()
	data, e := os.ReadFile("../../testdata/contract/http-state-r3.json")
	if e != nil {
		t.Fatal(e)
	}
	var fixtures []struct {
		ID        string                 `json:"id"`
		Profile   bigquery.SourceProfile `json:"profile"`
		Query     string                 `json:"query"`
		Execution bigquery.Execution     `json:"execution"`
		Bounds    bigquery.Bounds        `json:"bounds"`
		HTTP      []struct {
			Status  int    `json:"status"`
			Body    string `json:"body"`
			Request struct {
				Method string `json:"method"`
				Path   string `json:"path"`
			} `json:"request"`
		} `json:"http"`
	}
	if e = json.Unmarshal(data, &fixtures); e != nil {
		t.Fatal(e)
	}
	sc := fixtures[0]
	q, e := tugql.Deserialize([]byte(sc.Query))
	if e != nil {
		t.Fatal(e)
	}
	plan, e := bigquery.Compile(sc.Profile, q)
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	client, e := bigquery.NewClient(bigquery.Config{Profiles: []bigquery.SourceProfile{sc.Profile}, Provider: operatorProvider{sc.Execution.Principal}, Ledger: bigquery.NewMemoryLedger(), Prepare: func(context.Context) (bigquery.ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, Transport: testTransport(func(req *http.Request) (*http.Response, error) {
		if calls >= len(sc.HTTP) {
			t.Fatal("extra backend dispatch")
		}
		step := sc.HTTP[calls]
		calls++
		if req.Method != step.Request.Method || req.URL.Path != step.Request.Path {
			t.Fatal(req.Method, req.URL.Path)
		}
		return &http.Response{StatusCode: step.Status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(step.Body))}, nil
	})})
	if e != nil {
		t.Fatal(e)
	}
	handler, e := NewHandler(Config{client, plan, sc.Execution, sc.Bounds, "https://datatug.example"})
	if e != nil {
		t.Fatal(e)
	}
	post := func(path string, input any, output any) {
		raw, _ := json.Marshal(input)
		request := httptest.NewRequest("POST", path, bytes.NewReader(raw))
		request.RemoteAddr = "127.0.0.1:12345"
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Origin", "https://datatug.example")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != 200 {
			t.Fatal(path, recorder.Code, recorder.Body.String())
		}
		if e = json.Unmarshal(recorder.Body.Bytes(), output); e != nil {
			t.Fatal(e)
		}
	}
	var preview bigquery.Preview
	post("/preview", struct{}{}, &preview)
	var first bigquery.Page
	post("/execute", map[string]any{"preview": preview, "approveDigest": preview.ApprovalDigest}, &first)
	if len(first.Rows) != 2 || first.Rows[0][0].Value != "1" {
		t.Fatal(first)
	}
	var second bigquery.Page
	post("/page", map[string]any{"receipt": first.Receipt, "cursor": first.Cursor}, &second)
	if len(second.Rows) != 1 || second.Rows[0][0].Value != "9223372036854775807" {
		t.Fatal(second)
	}
	var status bigquery.JobStatus
	post("/status", map[string]any{"job": second.Receipt.Job}, &status)
	if status.State != "completed" || status.BilledBytes == nil || *status.BilledBytes != "80" {
		t.Fatal(status)
	}
	if calls != 11 {
		t.Fatal(calls)
	}
	for _, tc := range []struct{ remote, origin, body string }{{"192.0.2.1:1234", "", "{}"}, {"127.0.0.1:1234", "https://untrusted.invalid", "{}"}, {"127.0.0.1:1234", "", "{\"executionProject\":\"attacker\"}"}, {"127.0.0.1:1234", "", "{\"x\":1,\"x\":2}"}} {
		req := httptest.NewRequest("POST", "/preview", strings.NewReader(tc.body))
		req.RemoteAddr = tc.remote
		req.Header.Set("Origin", tc.origin)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code == 200 {
			t.Fatal("untrusted input accepted")
		}
	}
	if calls != 11 {
		t.Fatal("rejected server input dispatched")
	}
}

func newTestClient(t *testing.T) (*bigquery.Client, bigquery.ReadPlan, bigquery.SourceProfile) {
	t.Helper()
	data, e := os.ReadFile("../../testdata/contract/http-state-r3.json")
	if e != nil {
		t.Fatal(e)
	}
	var fixtures []struct {
		Profile   bigquery.SourceProfile `json:"profile"`
		Query     string                 `json:"query"`
		Execution bigquery.Execution     `json:"execution"`
	}
	if e = json.Unmarshal(data, &fixtures); e != nil {
		t.Fatal(e)
	}
	sc := fixtures[0]
	q, e := tugql.Deserialize([]byte(sc.Query))
	if e != nil {
		t.Fatal(e)
	}
	plan, e := bigquery.Compile(sc.Profile, q)
	if e != nil {
		t.Fatal(e)
	}
	c, e := bigquery.NewClient(bigquery.Config{
		Profiles: []bigquery.SourceProfile{sc.Profile},
		Provider: operatorProvider{sc.Execution.Principal},
		Ledger:   bigquery.NewMemoryLedger(),
		Prepare:  func(context.Context) (bigquery.ReadPlan, string, error) { return plan, "test-policy", nil },
		Transport: testTransport(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
		}),
	})
	if e != nil {
		t.Fatal(e)
	}
	return c, plan, sc.Profile
}

func TestNewHandler_Validation(t *testing.T) {
	t.Parallel()
	h, err := NewHandler(Config{})
	if err == nil || h != nil {
		t.Fatal("expected error for nil client")
	}
}

func TestHandler_RequestValidation(t *testing.T) {
	t.Parallel()
	c, _, _ := newTestClient(t)
	handler, e := NewHandler(Config{Client: c, BrowserOrigin: "https://trusted.example"})
	if e != nil {
		t.Fatal(e)
	}

	// Method not POST
	req := httptest.NewRequest("GET", "/preview", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}

	// Content-Type not application/json
	req = httptest.NewRequest("POST", "/preview", strings.NewReader("{}"))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "text/plain")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad content type, got %d", rec.Code)
	}

	// Body > 256KB
	huge := strings.Repeat(" ", (256<<10)+2)
	req = httptest.NewRequest("POST", "/preview", strings.NewReader(huge))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for oversized body, got %d", rec.Code)
	}

	// Unknown path
	req = httptest.NewRequest("POST", "/unknown", strings.NewReader("{}"))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown path, got %d", rec.Code)
	}
}

func TestHandler_DecodeAndActionErrors(t *testing.T) {
	t.Parallel()
	c, _, _ := newTestClient(t)
	handler, e := NewHandler(Config{Client: c, BrowserOrigin: "https://trusted.example"})
	if e != nil {
		t.Fatal(e)
	}

	paths := []string{"/preview", "/execute", "/page", "/rebind", "/status", "/cancel"}
	for _, p := range paths {
		// Invalid input structure / unknown fields
		req := httptest.NewRequest("POST", p, strings.NewReader(`{"unexpectedField": true}`))
		req.RemoteAddr = "127.0.0.1:12345"
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("path %s expected 400 on unexpected field, got %d", p, rec.Code)
		}
	}

	// Execute with approval failure
	req := httptest.NewRequest("POST", "/execute", strings.NewReader(`{"preview":{},"approveDigest":"bad"}`))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on bad approval digest, got %d", rec.Code)
	}

	// Execute with execution failure
	validPreview := bigquery.Preview{
		Nonce:          "0123456789abcdef0123456789abcdef0123456789abcdef",
		Execution:      bigquery.Execution{JobProject: "p", Principal: bigquery.Principal{Kind: "workload", Subject: "s", Generation: "1"}},
		ApprovalDigest: "valid",
	}
	// RebindJob call
	req = httptest.NewRequest("POST", "/rebind", strings.NewReader(`{"receipt":{},"cursor":""}`))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on empty receipt rebind, got %d", rec.Code)
	}

	// CancelJob call
	req = httptest.NewRequest("POST", "/cancel", strings.NewReader(`{"job":{"projectId":"p","location":"US","jobId":"job1"}}`))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on unconfigured cancel, got %d", rec.Code)
	}

	// /execute error with run == nil (canceled context)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	req = httptest.NewRequestWithContext(canceledCtx, "POST", "/execute", strings.NewReader(`{"preview":{},"approveDigest":""}`))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on canceled execute, got %d", rec.Code)
	}

	// /page error with run == nil (invalid cursor)
	req = httptest.NewRequest("POST", "/page", strings.NewReader(`{"receipt":{},"cursor":"invalid"}`))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid page cursor, got %d", rec.Code)
	}
	_ = validPreview
}

func TestServe_LoopbackAndErrors(t *testing.T) {
	t.Parallel()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	// Invalid split host port
	if err := Serve(context.Background(), "invalid-addr", h); err == nil {
		t.Fatal("expected error on invalid-addr")
	}

	// Non-loopback IP
	if err := Serve(context.Background(), "192.168.1.1:8080", h); err == nil {
		t.Fatal("expected error on non-loopback")
	}

	// Invalid port
	if err := Serve(context.Background(), "127.0.0.1:-1", h); err == nil {
		t.Fatal("expected error on invalid port")
	}

	// Normal startup and cancellation
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- Serve(ctx, "127.0.0.1:0", h)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil error on graceful server shutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not exit within timeout")
	}
}

type failAcceptListener struct {
	net.Listener
}

func (f failAcceptListener) Accept() (net.Conn, error) {
	return nil, errors.New("simulated accept error")
}

// Not parallel: mutates listen seam.
func TestServe_ListenerError(t *testing.T) {
	oldListen := listen
	defer func() { listen = oldListen }()

	listen = func(network, address string) (net.Listener, error) {
		l, err := net.Listen(network, address)
		if err != nil {
			return nil, err
		}
		return failAcceptListener{l}, nil
	}

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	err := Serve(context.Background(), "127.0.0.1:0", h)
	if err == nil || !strings.Contains(err.Error(), "simulated accept error") {
		t.Fatalf("expected simulated accept error, got %v", err)
	}
}

func TestHandler_ExecuteAndResumeWithRunError(t *testing.T) {
	t.Parallel()
	data, e := os.ReadFile("../../testdata/contract/http-state-r3.json")
	if e != nil {
		t.Fatal(e)
	}
	var fixtures []struct {
		Profile   bigquery.SourceProfile `json:"profile"`
		Query     string                 `json:"query"`
		Execution bigquery.Execution     `json:"execution"`
		Bounds    bigquery.Bounds        `json:"bounds"`
		HTTP      []struct {
			Status  int    `json:"status"`
			Body    string `json:"body"`
			Request struct {
				Method string `json:"method"`
				Path   string `json:"path"`
			} `json:"request"`
		} `json:"http"`
	}
	if e = json.Unmarshal(data, &fixtures); e != nil {
		t.Fatal(e)
	}
	sc := fixtures[0]
	q, e := tugql.Deserialize([]byte(sc.Query))
	if e != nil {
		t.Fatal(e)
	}
	plan, e := bigquery.Compile(sc.Profile, q)
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	// Transport returns 500 error on non-dryRun query dispatch
	client, e := bigquery.NewClient(bigquery.Config{
		Profiles: []bigquery.SourceProfile{sc.Profile},
		Provider: operatorProvider{sc.Execution.Principal},
		Ledger:   bigquery.NewMemoryLedger(),
		Prepare:  func(context.Context) (bigquery.ReadPlan, string, error) { return plan, "test-policy", nil },
		Transport: testTransport(func(req *http.Request) (*http.Response, error) {
			if calls >= len(sc.HTTP) {
				t.Fatal("extra backend dispatch")
			}
			step := sc.HTTP[calls]
			calls++
			if calls == 6 && req.Method == "POST" && strings.Contains(req.URL.Path, "/queries") {
				return &http.Response{StatusCode: 500, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":{"code":500,"message":"backend fail"}}`))}, nil
			}
			return &http.Response{StatusCode: step.Status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(step.Body))}, nil
		}),
	})
	if e != nil {
		t.Fatal(e)
	}
	handler, e := NewHandler(Config{client, plan, sc.Execution, sc.Bounds, "https://datatug.example"})
	if e != nil {
		t.Fatal(e)
	}

	preview, e := client.Preview(context.Background(), plan, sc.Execution, sc.Bounds)
	if e != nil {
		t.Fatal(e)
	}

	// /execute when backend fails returns run != nil error
	raw, _ := json.Marshal(map[string]any{"preview": preview, "approveDigest": preview.ApprovalDigest})
	req := httptest.NewRequest("POST", "/execute", bytes.NewReader(raw))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://datatug.example")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on execute failure, got %d", rec.Code)
	}
	var resp struct {
		Result *bigquery.Receipt `json:"result"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.Result == nil {
		t.Fatalf("expected receipt in error response, got %s", rec.Body.String())
	}

	// /execute with canceled context after successful Approve on fresh preview gives run == nil error
	preview2, e := client.Preview(context.Background(), plan, sc.Execution, sc.Bounds)
	if e != nil {
		t.Fatal(e)
	}
	raw2, _ := json.Marshal(map[string]any{"preview": preview2, "approveDigest": preview2.ApprovalDigest})
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	reqCanceled := httptest.NewRequestWithContext(canceledCtx, "POST", "/execute", bytes.NewReader(raw2))
	reqCanceled.RemoteAddr = "127.0.0.1:12345"
	reqCanceled.Header.Set("Content-Type", "application/json")
	reqCanceled.Header.Set("Origin", "https://datatug.example")
	recCanceled := httptest.NewRecorder()
	handler.ServeHTTP(recCanceled, reqCanceled)
	if recCanceled.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on canceled execute, got %d", recCanceled.Code)
	}
}

func TestHandler_ResumeExpiredReturnsRunWithError(t *testing.T) {
	t.Parallel()
	data, e := os.ReadFile("../../testdata/contract/http-state-r3.json")
	if e != nil {
		t.Fatal(e)
	}
	var fixtures []struct {
		Profile   bigquery.SourceProfile `json:"profile"`
		Query     string                 `json:"query"`
		Execution bigquery.Execution     `json:"execution"`
		Bounds    bigquery.Bounds        `json:"bounds"`
		HTTP      []struct {
			Status  int    `json:"status"`
			Body    string `json:"body"`
			Request struct {
				Method string `json:"method"`
				Path   string `json:"path"`
			} `json:"request"`
		} `json:"http"`
	}
	if e = json.Unmarshal(data, &fixtures); e != nil {
		t.Fatal(e)
	}
	sc := fixtures[0]
	sc.Bounds.WallMs = 300 // Short wall limit so execution deadline expires quickly
	q, e := tugql.Deserialize([]byte(sc.Query))
	if e != nil {
		t.Fatal(e)
	}
	plan, e := bigquery.Compile(sc.Profile, q)
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	client, e := bigquery.NewClient(bigquery.Config{
		Profiles: []bigquery.SourceProfile{sc.Profile},
		Provider: operatorProvider{sc.Execution.Principal},
		Ledger:   bigquery.NewMemoryLedger(),
		Prepare:  func(context.Context) (bigquery.ReadPlan, string, error) { return plan, "explicit-no-policies", nil },
		Transport: testTransport(func(req *http.Request) (*http.Response, error) {
			if calls >= len(sc.HTTP) {
				t.Fatal("extra backend dispatch")
			}
			step := sc.HTTP[calls]
			calls++
			return &http.Response{StatusCode: step.Status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(step.Body))}, nil
		}),
	})
	if e != nil {
		t.Fatal(e)
	}
	handler, e := NewHandler(Config{client, plan, sc.Execution, sc.Bounds, "https://datatug.example"})
	if e != nil {
		t.Fatal(e)
	}
	preview, e := client.Preview(context.Background(), plan, sc.Execution, sc.Bounds)
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(map[string]any{"preview": preview, "approveDigest": preview.ApprovalDigest})
	req := httptest.NewRequest("POST", "/execute", bytes.NewReader(raw))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://datatug.example")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on execute, got %d: %s", rec.Code, rec.Body.String())
	}
	var first bigquery.Page
	if err := json.Unmarshal(rec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}

	// Sleep until execution deadline passes
	time.Sleep(350 * time.Millisecond)

	// Now /page with valid first.Receipt and first.Cursor hits expired deadline -> run != nil and error!
	rawPage, _ := json.Marshal(map[string]any{"receipt": first.Receipt, "cursor": first.Cursor})
	req = httptest.NewRequest("POST", "/page", bytes.NewReader(rawPage))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://datatug.example")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on expired page, got %d", rec.Code)
	}
	var pageResp struct {
		Result *bigquery.Receipt `json:"result"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &pageResp); err != nil || pageResp.Result == nil {
		t.Fatalf("expected receipt in error response, got %s", rec.Body.String())
	}
}
