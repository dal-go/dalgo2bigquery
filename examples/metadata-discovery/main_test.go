package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	bigquery "github.com/dal-go/dalgo2bigquery"
)

func TestFixtureJourney(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	if e := run(context.Background(), []string{"--fixture", "--source", "fixture-source"}, nil, &out); e != nil {
		t.Fatal(e)
	}
	var got bigquery.PublicDiscovery
	if e := json.Unmarshal(out.Bytes(), &got); e != nil {
		t.Fatal(e)
	}
	if got.Provenance.Kind != "synthetic-fixture" || got.SHA256 == "" || got.Schema[0].Mode != "NULLABLE" || strings.Contains(out.String(), "PRIVATE") {
		t.Fatal("invalid or private fixture output", out.String())
	}
}

func TestNoImplicitOperatorOrUnknownSource(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{{"--source", "fixture-source"}, {"--fixture", "--source", "unlisted"}, {"--fixture"}} {
		var out bytes.Buffer
		if e := run(context.Background(), args, nil, &out); e == nil || out.Len() != 0 {
			t.Fatal("unconfigured operation produced output", e, out.String())
		}
	}
	var out bytes.Buffer
	if e := run(context.Background(), []string{"--source", "fixture-source"}, nil, &out); !errors.Is(e, errProviderMissing) {
		t.Fatal(e)
	}
}

func TestRun_ErrorBranches(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	// Extra positional argument
	if e := run(context.Background(), []string{"--fixture", "--source", "fixture-source", "unexpected"}, nil, &out); e == nil {
		t.Fatal("expected error with positional argument")
	}

	// Factory returns error
	errFactory := func(context.Context) (bigquery.DiscoveryConfig, error) {
		return bigquery.DiscoveryConfig{}, errors.New("simulated setup error")
	}
	if e := run(context.Background(), []string{"--source", "fixture-source"}, errFactory, &out); e == nil || !strings.Contains(e.Error(), "operator setup failed") {
		t.Fatalf("expected operator setup failed, got %v", e)
	}

	// NewDiscoveryClient error (invalid config / empty allowlist)
	emptyCfgFactory := func(context.Context) (bigquery.DiscoveryConfig, error) {
		return bigquery.DiscoveryConfig{}, nil
	}
	if e := run(context.Background(), []string{"--source", "fixture-source"}, emptyCfgFactory, &out); e == nil {
		t.Fatal("expected error with empty discovery config")
	}
}

// Not parallel: mutates readFixture seam.
func TestFixtureTransport_ReadError(t *testing.T) {
	oldRead := readFixture
	defer func() { readFixture = oldRead }()

	readFixture = func(string) ([]byte, error) {
		return nil, errors.New("simulated read error")
	}

	var out bytes.Buffer
	e := run(context.Background(), []string{"--fixture", "--source", "fixture-source"}, nil, &out)
	if e == nil {
		t.Fatal("expected error on read failure")
	}
}

// Not parallel: mutates os.Args and exit seam.
func TestMain_SuccessAndFailure(t *testing.T) {
	oldArgs := os.Args
	oldExit := exit
	defer func() {
		os.Args = oldArgs
		exit = oldExit
	}()

	// Successful run with --fixture and --source
	os.Args = []string{"cmd", "--fixture", "--source", "fixture-source"}
	exitCalled := false
	exit = func(code int) {
		exitCalled = true
	}
	main()
	if exitCalled {
		t.Fatal("exit called on successful main()")
	}

	// Failing run with invalid arguments
	os.Args = []string{"cmd", "--invalid-flag"}
	exitCode := 0
	exit = func(code int) {
		exitCalled = true
		exitCode = code
	}
	main()
	if !exitCalled || exitCode != 1 {
		t.Fatalf("expected exit(1), got exitCalled=%v code=%d", exitCalled, exitCode)
	}
}
