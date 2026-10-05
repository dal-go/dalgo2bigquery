package server

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/dal-go/dalgo/dtql"
	bigquery "github.com/dal-go/dalgo2bigquery"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type operatorProvider struct{ principal bigquery.Principal }

func (p operatorProvider) Authorize(ctx context.Context, base http.RoundTripper) (bigquery.Identity, http.RoundTripper, error) {
	return bigquery.Identity{Principal: p.principal, ExpiresAt: time.Now().Add(time.Hour), Read: true, Cancel: true}, base, nil
}

type testTransport func(*http.Request) (*http.Response, error)

func (f testTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestHandlerActualPreviewExecutePageStatus(t *testing.T) {
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
	q, e := dtql.Deserialize([]byte(sc.Query))
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
