// Command metadata-discovery emits one bounded public projection. Without
// --fixture it requires an operator-owned provider factory at build time.
package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	bigquery "github.com/dal-go/dalgo2bigquery"
)

//go:embed testdata/dataset.json testdata/table.json
var fixtures embed.FS

type operatorFactory func(context.Context) (bigquery.DiscoveryConfig, error)

// The operator injects this in a build-tagged file. It must supply a verified
// provider and a protected, synchronous consent/source/owner binding reader.
// The command neither loads credentials nor attests identity from token presence.
var providerFactory operatorFactory

var errProviderMissing = errors.New("operator provider not configured; use --fixture or inject an operator-owned providerFactory")

func run(ctx context.Context, args []string, factory operatorFactory, output io.Writer) error {
	flags := flag.NewFlagSet("metadata-discovery", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	source := flags.String("source", "", "exact source ID from the protected factory allowlist")
	fixture := flags.Bool("fixture", false, "offline synthetic fixture; never contacts BigQuery")
	if e := flags.Parse(args); e != nil || flags.NArg() != 0 || *source == "" {
		return errors.New("invalid discovery flags; --source is required")
	}
	if *fixture {
		factory = fixtureFactory
	}
	if factory == nil {
		return errProviderMissing
	}
	setup, cancel := context.WithTimeout(ctx, 15*time.Second)
	cfg, e := factory(setup)
	cancel()
	if e != nil {
		return errors.New("operator setup failed")
	}
	client, e := bigquery.NewDiscoveryClient(cfg)
	if e != nil {
		return e
	}
	bounds := bigquery.DefaultBounds()
	bounds.ResponseBytes = 1 << 20
	bounds.TotalResponseBytes = 2 << 20
	bounds.WallMs = 30000
	observation, e := client.Discover(ctx, *source, bounds)
	if e != nil {
		return e
	}
	return json.NewEncoder(output).Encode(observation)
}

type fixtureProvider struct{ identity bigquery.Identity }

func (p fixtureProvider) Authorize(_ context.Context, gate http.RoundTripper) (bigquery.Identity, http.RoundTripper, error) {
	return p.identity, gate, nil
}

type fixtureTransport struct{}

func (fixtureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	file := "dataset.json"
	if strings.HasSuffix(req.URL.Path, "/tables/sample") {
		file = "table.json"
	}
	raw, e := fixtures.ReadFile("testdata/" + file)
	if e != nil {
		return nil, e
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
}

func fixtureFactory(context.Context) (bigquery.DiscoveryConfig, error) {
	p := bigquery.Principal{Kind: "workload", Subject: "synthetic-fixture", Generation: "1"}
	return bigquery.DiscoveryConfig{
		Allowlist: []bigquery.DiscoveryLocator{{SourceID: "fixture-source", SourceProject: "fixture-project", DatasetID: "fixture_dataset", TableID: "sample"}},
		Authorize: func(context.Context, bigquery.DiscoveryLocator) (bigquery.DiscoveryBinding, error) {
			return bigquery.DiscoveryBinding{Principal: p, PolicyRevision: "fixture-only-v1"}, nil
		},
		Provider:  fixtureProvider{bigquery.Identity{Principal: p, ExpiresAt: time.Now().Add(time.Minute), Read: true}},
		Transport: fixtureTransport{}, EvidenceKind: "synthetic-fixture",
	}, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if e := run(ctx, os.Args[1:], providerFactory, os.Stdout); e != nil {
		fmt.Fprintln(os.Stderr, "metadata-discovery:", e)
		os.Exit(1)
	}
}
