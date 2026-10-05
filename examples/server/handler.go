// Package server is an operator-owned loopback service harness. It never loads
// credentials or chooses a cost principal from a browser request.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	bigquery "github.com/dal-go/dalgo2bigquery"
	"io"
	"net"
	"net/http"
	"time"
)

type Config struct {
	Client        *bigquery.Client
	Plan          bigquery.ReadPlan
	Execution     bigquery.Execution
	Bounds        bigquery.Bounds
	BrowserOrigin string
}

func NewHandler(cfg Config) (http.Handler, error) {
	if cfg.Client == nil {
		return nil, &bigquery.Error{Code: "invalid_input"}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		respond := func(v any, e error) {
			if e != nil {
				code := "remote_failed"
				if safe, ok := e.(*bigquery.Error); ok {
					code = safe.Code
				}
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": code, "result": v})
				return
			}
			_ = json.NewEncoder(w).Encode(v)
		}
		host, _, e := net.SplitHostPort(r.RemoteAddr)
		if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			respond(nil, &bigquery.Error{Code: "invalid_input"})
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" && (cfg.BrowserOrigin == "" || origin != cfg.BrowserOrigin) {
			respond(nil, &bigquery.Error{Code: "invalid_input"})
			return
		}
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Content-Type") != "application/json" {
			respond(nil, &bigquery.Error{Code: "invalid_input"})
			return
		}
		raw, e := io.ReadAll(io.LimitReader(r.Body, 256<<10+1))
		if e != nil || len(raw) > 256<<10 {
			respond(nil, &bigquery.Error{Code: "invalid_input"})
			return
		}
		if _, e = bigquery.ParseJSON(raw, 256<<10); e != nil {
			respond(nil, e)
			return
		}
		decode := func(v any) error {
			d := json.NewDecoder(bytes.NewReader(raw))
			d.DisallowUnknownFields()
			if e := d.Decode(v); e != nil {
				return &bigquery.Error{Code: "invalid_input"}
			}
			return nil
		}
		switch r.URL.Path {
		case "/preview":
			var input struct{}
			if e = decode(&input); e != nil {
				respond(nil, e)
				return
			}
			p, e := cfg.Client.Preview(r.Context(), cfg.Plan, cfg.Execution, cfg.Bounds)
			respond(p, e)
		case "/execute":
			var input struct {
				Preview       bigquery.Preview `json:"preview"`
				ApproveDigest string           `json:"approveDigest"`
			}
			if e = decode(&input); e != nil {
				respond(nil, e)
				return
			}
			a, e := cfg.Client.Approve(input.Preview, input.ApproveDigest)
			if e != nil {
				respond(nil, e)
				return
			}
			run, e := cfg.Client.Execute(r.Context(), a)
			if e != nil {
				if run != nil {
					respond(run.Receipt(), e)
				} else {
					respond(nil, e)
				}
				return
			}
			page, e := run.NextPage()
			respond(page, e)
		case "/page":
			var input struct {
				Receipt bigquery.Receipt `json:"receipt"`
				Cursor  string           `json:"cursor"`
			}
			if e = decode(&input); e != nil {
				respond(nil, e)
				return
			}
			run, e := cfg.Client.Resume(r.Context(), input.Receipt, input.Cursor)
			if e != nil {
				if run != nil {
					respond(run.Receipt(), e)
				} else {
					respond(nil, e)
				}
				return
			}
			page, e := run.NextPage()
			respond(page, e)
		case "/rebind":
			var input struct {
				Receipt bigquery.Receipt `json:"receipt"`
				Cursor  string           `json:"cursor"`
			}
			if e = decode(&input); e != nil {
				respond(nil, e)
				return
			}
			result, e := cfg.Client.RebindJob(r.Context(), input.Receipt, input.Cursor)
			respond(result, e)
		case "/status", "/cancel":
			var input struct {
				Job bigquery.JobRef `json:"job"`
			}
			if e = decode(&input); e != nil {
				respond(nil, e)
				return
			}
			if r.URL.Path == "/status" {
				status, e := cfg.Client.Status(r.Context(), input.Job)
				respond(status, e)
			} else {
				cancel, e := cfg.Client.CancelJob(r.Context(), input.Job)
				respond(cancel, e)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}), nil
}

// Serve starts only when deliberately called by an operator's runnable program.
// The caller owns ctx and its cancellation; no package initialization binds ports.
// The operator injects NewHandler(Config) after establishing trusted credentials.
func Serve(ctx context.Context, address string, handler http.Handler) error {
	host, _, e := net.SplitHostPort(address)
	if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return &bigquery.Error{Code: "invalid_input"}
	}
	listener, e := net.Listen("tcp", address)
	if e != nil {
		return e
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdown)
		case <-done:
		}
	}()
	e = srv.Serve(listener)
	if e == http.ErrServerClosed {
		return nil
	}
	return e
}
