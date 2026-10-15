package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	bigquery "github.com/dal-go/dalgo2bigquery"
	"github.com/dal-go/dalgo2bigquery/examples/server"
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
	t.Parallel()
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
	t.Parallel()
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

func TestRun_FlagsAndValidationErrors(t *testing.T) {
	t.Parallel()
	serve := func(context.Context, string, http.Handler) error { return nil }

	// Extra positional arg
	if e := run(context.Background(), []string{"extra"}, nil, serve); e == nil {
		t.Fatal("expected error with positional arg")
	}

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
	validFactory := func(ctx context.Context) (operatorConfig, error) {
		return operatorConfig{
			Profile:   profile,
			Plan:      plan,
			Prepare:   func(context.Context) (bigquery.ReadPlan, string, error) { return plan, "policy", nil },
			Provider:  testProvider{},
			Transport: noTransport{},
			Execution: bigquery.Execution{JobProject: "job-project", Principal: bigquery.Principal{Kind: "workload", Subject: "s", Generation: "1"}, MaximumBytesBilled: "1000", SessionBudgetBytes: "3000"},
			Bounds:    bigquery.DefaultBounds(),
		}, nil
	}

	// Missing origin
	if e := run(context.Background(), []string{"--ledger", t.TempDir()}, validFactory, serve); e == nil {
		t.Fatal("expected error with missing origin")
	}

	// Missing ledger
	if e := run(context.Background(), []string{"--origin", "http://localhost:4200"}, validFactory, serve); e == nil {
		t.Fatal("expected error with missing ledger")
	}

	// Factory returns error
	errFactory := func(ctx context.Context) (operatorConfig, error) {
		return operatorConfig{}, errors.New("simulated setup error")
	}
	if e := run(context.Background(), []string{"--ledger", t.TempDir(), "--origin", "http://localhost:4200"}, errFactory, serve); e == nil {
		t.Fatal("expected error when factory fails")
	}

	// Canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := run(canceledCtx, []string{"--ledger", t.TempDir(), "--origin", "http://localhost:4200"}, validFactory, serve); e == nil {
		t.Fatal("expected error on canceled context")
	}

	// Relative ledger dir (NewFileLedger error)
	if e := run(context.Background(), []string{"--ledger", "relative-dir", "--origin", "http://localhost:4200"}, validFactory, serve); e == nil {
		t.Fatal("expected error on relative ledger dir")
	}

	// Client creation error (invalid config)
	badClientFactory := func(ctx context.Context) (operatorConfig, error) {
		cfg, _ := validFactory(ctx)
		cfg.Profile.SourceProject = "invalid project with spaces"
		return cfg, nil
	}
	if e := run(context.Background(), []string{"--ledger", filepath.Join(t.TempDir(), "s"), "--origin", "http://localhost:4200"}, badClientFactory, serve); e == nil {
		t.Fatal("expected error on invalid client config")
	}
}

// Not parallel: mutates os.Args, providerFactory, serveFunc, and exit seam.
func TestMain_SuccessAndFailure(t *testing.T) {
	oldArgs := os.Args
	oldFactory := providerFactory
	oldServe := serveFunc
	oldExit := exit
	defer func() {
		os.Args = oldArgs
		providerFactory = oldFactory
		serveFunc = oldServe
		exit = oldExit
	}()

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
	providerFactory = func(ctx context.Context) (operatorConfig, error) {
		return operatorConfig{
			Profile:   profile,
			Plan:      plan,
			Prepare:   func(context.Context) (bigquery.ReadPlan, string, error) { return plan, "policy", nil },
			Provider:  testProvider{},
			Transport: noTransport{},
			Execution: bigquery.Execution{JobProject: "job-project", Principal: bigquery.Principal{Kind: "workload", Subject: "s", Generation: "1"}, MaximumBytesBilled: "1000", SessionBudgetBytes: "3000"},
			Bounds:    bigquery.DefaultBounds(),
		}, nil
	}
	serveFunc = func(context.Context, string, http.Handler) error { return nil }

	// Successful run
	exitCalled := false
	exit = func(code int) { exitCalled = true }
	os.Args = []string{"cmd", "--ledger", filepath.Join(t.TempDir(), "session"), "--origin", "http://localhost:4200"}
	main()
	if exitCalled {
		t.Fatal("exit called on successful main()")
	}

	// Failing run with bad flag
	exitCode := 0
	exit = func(code int) {
		exitCalled = true
		exitCode = code
	}
	os.Args = []string{"cmd", "--invalid-flag"}
	main()
	if !exitCalled || exitCode != 1 {
		t.Fatalf("expected exit(1), got exitCalled=%v code=%d", exitCalled, exitCode)
	}
}

// Not parallel: mutates newServerHandler seam.
func TestRun_NewServerHandlerError(t *testing.T) {
	oldHandler := newServerHandler
	defer func() { newServerHandler = oldHandler }()

	newServerHandler = func(server.Config) (http.Handler, error) {
		return nil, errors.New("simulated handler error")
	}

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
	validFactory := func(ctx context.Context) (operatorConfig, error) {
		return operatorConfig{
			Profile:   profile,
			Plan:      plan,
			Prepare:   func(context.Context) (bigquery.ReadPlan, string, error) { return plan, "policy", nil },
			Provider:  testProvider{},
			Transport: noTransport{},
			Execution: bigquery.Execution{JobProject: "job-project", Principal: bigquery.Principal{Kind: "workload", Subject: "s", Generation: "1"}, MaximumBytesBilled: "1000", SessionBudgetBytes: "3000"},
			Bounds:    bigquery.DefaultBounds(),
		}, nil
	}

	e = run(context.Background(), []string{"--ledger", filepath.Join(t.TempDir(), "s"), "--origin", "http://localhost:4200"}, validFactory, func(context.Context, string, http.Handler) error { return nil })
	if e == nil || !strings.Contains(e.Error(), "simulated handler error") {
		t.Fatalf("expected simulated handler error, got %v", e)
	}
}
