package bigquery

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

type dummySourceProvider struct{}

func (dummySourceProvider) Authorize(context.Context, http.RoundTripper) (Identity, http.RoundTripper, error) {
	return Identity{}, nil, nil
}

type dummySourceTransport struct{}

func (dummySourceTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestSourceBackend_ValidationAndMethods(t *testing.T) {
	t.Parallel()

	// Invalid input configs
	for _, bad := range []SourceConfig{
		{}, // empty
		{ProjectID: "bad project", DatasetID: "ds", Provider: dummySourceProvider{}, Transport: dummySourceTransport{}},
		{ProjectID: "proj-1", DatasetID: "bad-ds-id!", Provider: dummySourceProvider{}, Transport: dummySourceTransport{}},
		{ProjectID: "proj-1", DatasetID: "ds", Provider: nil, Transport: dummySourceTransport{}},
		{ProjectID: "proj-1", DatasetID: "ds", Provider: dummySourceProvider{}, Transport: nil},
	} {
		if _, err := NewReadOnlyDatabase(bad); errorCode(err) != "invalid_input" {
			t.Fatalf("expected invalid_input for config %+v, got %v", bad, err)
		}
	}

	// Valid config with nil Clock (triggers cfg.Clock = realClock{})
	cfg := SourceConfig{
		ProjectID: "valid-project",
		DatasetID: "valid_dataset",
		Provider:  dummySourceProvider{},
		Transport: dummySourceTransport{},
	}
	db, err := NewReadOnlyDatabase(cfg)
	if err != nil {
		t.Fatalf("unexpected error creating ReadOnlyDatabase: %v", err)
	}

	// ID and Adapter
	if id := db.source.ID(); id != "valid-project.valid_dataset" {
		t.Fatalf("expected valid-project.valid_dataset, got %s", id)
	}
	if a := db.source.Adapter(); a.Name() != "dalgo2bigquery" {
		t.Fatalf("expected dalgo2bigquery adapter, got %s", a.Name())
	}

	// Schema callbacks
	s := db.source.Schema()
	if s == nil {
		t.Fatal("expected non-nil Schema")
	}
	if _, err := s.KeyToFields(nil, nil); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got %v", err)
	}
	if _, err := s.DataToKey(nil, nil); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got %v", err)
	}

	// Unsupported DAL operations
	ctx := context.Background()
	if err := db.source.RunReadonlyTransaction(ctx, nil); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got %v", err)
	}
	if err := db.source.RunReadwriteTransaction(ctx, nil); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got %v", err)
	}
	if err := db.source.Get(ctx, nil); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got %v", err)
	}
	if err := db.source.GetMulti(ctx, nil); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got %v", err)
	}
	key := &record.Key{}
	if _, err := db.source.Exists(ctx, key); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got %v", err)
	}
	if _, err := db.source.ExecuteQueryToRecordsReader(ctx, nil); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got %v", err)
	}
	if _, err := db.source.ExecuteQueryToRecordsetReader(ctx, nil); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got %v", err)
	}
}
