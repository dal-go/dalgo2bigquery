// Command operator-server composes the approved loopback harness. The operator
// supplies a trusted provider/policy factory at build time; this scaffold never
// discovers credentials, fabricates identity or chooses a source/cost principal.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"time"

	bigquery "github.com/dal-go/dalgo2bigquery"
	"github.com/dal-go/dalgo2bigquery/examples/server"
)

type operatorConfig struct {
	Profile   bigquery.SourceProfile
	Plan      bigquery.ReadPlan
	Prepare   bigquery.Prepare
	Provider  bigquery.Provider
	Transport http.RoundTripper
	Execution bigquery.Execution
	Bounds    bigquery.Bounds
}
type operatorFactory func(context.Context) (operatorConfig, error)

// An operator-owned, build-tagged file sets this factory in init(). The factory
// must return the independently verified identity bridge and protected plan;
// bearer-token presence or configuration labels do not attest a principal.
var providerFactory operatorFactory
var errProviderMissing = errors.New("operator provider not configured; inject an operator-owned providerFactory before serving")

func run(ctx context.Context, args []string, factory operatorFactory, serve func(context.Context, string, http.Handler) error) error {
	flags := flag.NewFlagSet("operator-server", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	ledgerDir := flags.String("ledger", "", "absolute private persistent session directory")
	listen := flags.String("listen", "127.0.0.1:8080", "operator-owned loopback listener")
	origin := flags.String("origin", "", "exact trusted browser origin")
	if e := flags.Parse(args); e != nil || flags.NArg() != 0 {
		return errors.New("invalid operator flags")
	}
	if factory == nil {
		return errProviderMissing
	}
	if *ledgerDir == "" || *origin == "" {
		return errors.New("--ledger and --origin are required")
	}
	// Provider/policy construction gets one finite control-preparation context.
	// It must honor cancellation; no listener exists during operator setup.
	setup, cancel := context.WithTimeout(ctx, 15*time.Second)
	cfg, e := factory(setup)
	cancel()
	if e != nil {
		return errors.New("operator setup failed")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	ledger, e := bigquery.NewFileLedger(*ledgerDir)
	if e != nil {
		return e
	}
	client, e := bigquery.NewClient(bigquery.Config{Profiles: []bigquery.SourceProfile{cfg.Profile}, Provider: cfg.Provider, Transport: cfg.Transport, Ledger: ledger, Prepare: cfg.Prepare})
	if e != nil {
		return e
	}
	handler, e := server.NewHandler(server.Config{Client: client, Plan: cfg.Plan, Execution: cfg.Execution, Bounds: cfg.Bounds, BrowserOrigin: *origin})
	if e != nil {
		return e
	}
	return serve(ctx, *listen, handler)
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if e := run(ctx, os.Args[1:], providerFactory, server.Serve); e != nil {
		fmt.Fprintln(os.Stderr, "operator-server:", e)
		os.Exit(1)
	}
}
