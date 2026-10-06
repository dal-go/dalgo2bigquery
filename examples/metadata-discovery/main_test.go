package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	bigquery "github.com/dal-go/dalgo2bigquery"
)

func TestFixtureJourney(t *testing.T) {
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
