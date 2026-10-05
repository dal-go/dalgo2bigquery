package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	bigquery "github.com/dal-go/dalgo2bigquery"
)

type testProvider struct{}

func (testProvider) Authorize(context.Context, http.RoundTripper) (bigquery.Identity, http.RoundTripper, error) {
	panic("composition must not authenticate or dispatch")
}

type noTransport struct{}

func (noTransport) RoundTrip(*http.Request) (*http.Response, error) {
	panic("composition must not perform HTTP")
}
func TestUnconfiguredScaffoldRefusesBeforeMachineState(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "absent")
	e := run(context.Background(), []string{"--ledger", dir, "--origin", "http://localhost:4200"}, nil, func(context.Context, string, http.Handler) error { t.Fatal("listener started"); return nil })
	if !errors.Is(e, errProviderMissing) {
		t.Fatal(e)
	}
	if _, e := os.Stat(dir); !os.IsNotExist(e) {
		t.Fatal("default created ledger")
	}
}
func TestInjectedOperatorCompositionWithoutListenerOrCloud(t *testing.T) {
	raw, e := os.ReadFile("../../testdata/contract/http-state-r3.json")
	if e != nil {
		t.Fatal(e)
	}
	var cases []struct {
		Profile bigquery.SourceProfile `json:"profile"`
	}
	if e = json.Unmarshal(raw, &cases); e != nil {
		t.Fatal(e)
	}
	profile := cases[0].Profile
	q := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef(profile.LogicalCollection, "", nil))).Limit(1).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	plan, e := bigquery.Compile(profile, q)
	if e != nil {
		t.Fatal(e)
	}
	called := false
	factory := func(ctx context.Context) (operatorConfig, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 15*time.Second {
			t.Fatal("unbounded setup")
		}
		return operatorConfig{Profile: profile, Plan: plan, Prepare: func(context.Context) (bigquery.ReadPlan, string, error) { return plan, "operator-policy", nil }, Provider: testProvider{}, Transport: noTransport{}, Execution: bigquery.Execution{JobProject: "job-project", Principal: bigquery.Principal{Kind: "workload", Subject: "operator:verified", Generation: "1"}, MaximumBytesBilled: "1000", SessionBudgetBytes: "3000"}, Bounds: bigquery.DefaultBounds()}, nil
	}
	e = run(context.Background(), []string{"--ledger", filepath.Join(t.TempDir(), "session"), "--origin", "http://localhost:4200"}, factory, func(ctx context.Context, addr string, handler http.Handler) error {
		called = true
		if handler == nil || addr != "127.0.0.1:8080" {
			t.Fatal("composition")
		}
		return nil
	})
	if e != nil || !called {
		t.Fatal(e, called)
	}
}
