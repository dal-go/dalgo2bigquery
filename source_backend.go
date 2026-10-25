package bigquery

import (
	"context"
	"net/http"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

// SourceLimits bound a physical table read. Exceeding a bound returns an error;
// it never returns a successful, truncated source. Zero values use defaults.
type SourceLimits struct {
	PageSize      int
	MaxPages      int
	MaxRows       int64
	MaxBytes      int64
	ResponseBytes int
	WallTime      time.Duration
}

func (l SourceLimits) resolved() (SourceLimits, error) {
	if l.PageSize == 0 {
		l.PageSize = 1000
	}
	if l.MaxPages == 0 {
		l.MaxPages = 100000
	}
	if l.MaxRows == 0 {
		l.MaxRows = 100000000
	}
	if l.MaxBytes == 0 {
		l.MaxBytes = 1 << 40
	}
	if l.ResponseBytes == 0 {
		l.ResponseBytes = 4 << 20
	}
	if l.WallTime == 0 {
		l.WallTime = time.Hour
	}
	if l.PageSize < 1 || l.PageSize > 1000 || l.MaxPages < 1 || l.MaxPages > 1000000 || l.MaxRows < 1 || l.MaxRows > 1000000000 || l.MaxBytes < 1 || l.MaxBytes > 1<<42 || l.ResponseBytes < 1 || l.ResponseBytes > MaxResponseBytes || l.WallTime < time.Millisecond || l.WallTime > 24*time.Hour {
		return l, fail("invalid_input")
	}
	return l, nil
}

// SourceConfig selects exactly one BigQuery dataset and supplies an existing
// credential provider. The adapter does not discover or persist credentials.
type SourceConfig struct {
	ProjectID string
	DatasetID string
	Provider  Provider
	Transport http.RoundTripper
	Clock     Clock
	Limits    SourceLimits
}

// ReadOnlyDatabase is a DALgo DB exposing schema and physical row read
// capabilities. Generic query execution and every write operation are refused.
type ReadOnlyDatabase struct {
	dal.DB
	source *sourceDatabase
}

var _ dbschema.SchemaReader = (*ReadOnlyDatabase)(nil)
var _ dbschema.SourceRowsReader = (*ReadOnlyDatabase)(nil)
var _ dbschema.SourceViewReader = (*ReadOnlyDatabase)(nil)

func NewReadOnlyDatabase(cfg SourceConfig) (*ReadOnlyDatabase, error) {
	if !projectID.MatchString(cfg.ProjectID) || !identifier.MatchString(cfg.DatasetID) || cfg.Provider == nil || cfg.Transport == nil {
		return nil, fail("invalid_input")
	}
	limits, err := cfg.Limits.resolved()
	if err != nil {
		return nil, err
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}
	source := &sourceDatabase{project: cfg.ProjectID, dataset: cfg.DatasetID, provider: cfg.Provider, transport: cfg.Transport, clock: cfg.Clock, limits: limits}
	return &ReadOnlyDatabase{DB: dal.NewDB(source), source: source}, nil
}

type sourceDatabase struct {
	dal.NoConcurrency
	project, dataset string
	provider         Provider
	transport        http.RoundTripper
	clock            Clock
	limits           SourceLimits
}

func (d *sourceDatabase) ID() string         { return d.project + "." + d.dataset }
func (*sourceDatabase) Adapter() dal.Adapter { return dal.NewAdapter("dalgo2bigquery", "source-v1") }
func (*sourceDatabase) Schema() dal.Schema {
	return dal.NewSchema(
		func(*record.Key, any) ([]dal.ExtraField, error) { return nil, dal.ErrNotSupported },
		func(*record.Key, any) (*record.Key, error) { return nil, dal.ErrNotSupported },
	)
}
func (*sourceDatabase) RunReadonlyTransaction(context.Context, dal.ROTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (*sourceDatabase) RunReadwriteTransaction(context.Context, dal.RWTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (*sourceDatabase) Get(context.Context, record.Record) error        { return dal.ErrNotSupported }
func (*sourceDatabase) GetMulti(context.Context, []record.Record) error { return dal.ErrNotSupported }
func (*sourceDatabase) Exists(context.Context, *record.Key) (bool, error) {
	return false, dal.ErrNotSupported
}
func (*sourceDatabase) ExecuteQueryToRecordsReader(context.Context, dal.Query) (dal.RecordsReader, error) {
	return nil, dal.ErrNotSupported
}
func (*sourceDatabase) ExecuteQueryToRecordsetReader(context.Context, dal.Query, ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, dal.ErrNotSupported
}
