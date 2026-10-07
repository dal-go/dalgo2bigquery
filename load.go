package bigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	bq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

var (
	ErrDatasetLocationMismatch = errors.New("BigQuery dataset location does not match the requested location")
	ErrTableExists             = errors.New("BigQuery destination table already exists")
	ErrLoadOutcomeUnknown      = errors.New("BigQuery load outcome is unknown; inspect the job before retrying")
)

// BigqueryScope is the OAuth scope required for dataset and load-job writes.
// The caller must obtain consent or configured local credentials for it.
const BigqueryScope = "https://www.googleapis.com/auth/bigquery"

var loadIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validLoadIdentifier(value string) bool {
	return len(value) <= 1024 && loadIdentifier.MatchString(value)
}

// LoadConfig selects a destination and an authenticated HTTP client. The caller
// supplies credentials; this package does not discover credentials or request
// broader scopes on its own.
type LoadConfig struct {
	ProjectID  string
	DatasetID  string
	Location   string
	HTTPClient *http.Client
}

// LoadWriter creates schema-bearing BigQuery tables and loads NDJSON data.
// It is separate from Client, whose approval and bounded-job rules are for
// reading public BigQuery sources.
type LoadWriter struct {
	service   *bq.Service
	projectID string
	datasetID string
	location  string
}

func (w *LoadWriter) ProjectID() string { return w.projectID }
func (w *LoadWriter) DatasetID() string { return w.datasetID }
func (w *LoadWriter) Location() string  { return w.location }

type LoadReceipt struct {
	JobID     string
	ProjectID string
	DatasetID string
	TableID   string
	Location  string
	Rows      uint64
}

// NewLoadWriter builds a writer pinned to the BigQuery API origin. Redirects
// are refused so credentials cannot be forwarded to another host.
func NewLoadWriter(ctx context.Context, cfg LoadConfig) (*LoadWriter, error) {
	if !projectID.MatchString(cfg.ProjectID) || !validLoadIdentifier(cfg.DatasetID) || !locationID.MatchString(cfg.Location) || cfg.HTTPClient == nil {
		return nil, fail("invalid_input")
	}
	client := *cfg.HTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	service, err := bq.NewService(ctx, option.WithHTTPClient(&client))
	if err != nil {
		return nil, fail("invalid_input")
	}
	return &LoadWriter{service: service, projectID: cfg.ProjectID, datasetID: cfg.DatasetID, location: cfg.Location}, nil
}

// EnsureDataset creates a missing dataset at the explicit location. An
// existing dataset is never modified and must already have that location.
func (w *LoadWriter) EnsureDataset(ctx context.Context) error {
	dataset, err := w.service.Datasets.Get(w.projectID, w.datasetID).Context(ctx).Do()
	if err == nil {
		if !strings.EqualFold(dataset.Location, w.location) {
			return ErrDatasetLocationMismatch
		}
		return nil
	}
	if !isNotFound(err) {
		return safeLoadError("get dataset", err)
	}
	_, err = w.service.Datasets.Insert(w.projectID, &bq.Dataset{
		DatasetReference: &bq.DatasetReference{ProjectId: w.projectID, DatasetId: w.datasetID},
		Location:         w.location,
	}).Context(ctx).Do()
	if err == nil {
		return nil
	}
	// A concurrent creator may win. Re-read and validate instead of replacing.
	if !isConflict(err) {
		return safeLoadError("create dataset", err)
	}
	dataset, getErr := w.service.Datasets.Get(w.projectID, w.datasetID).Context(ctx).Do()
	if getErr != nil {
		return safeLoadError("reconcile dataset creation", getErr)
	}
	if !strings.EqualFold(dataset.Location, w.location) {
		return ErrDatasetLocationMismatch
	}
	return nil
}

// CreateTable refuses every existing table, including empty tables. Schema and
// optional primary/foreign-key declarations are applied once, without a
// truncate or replace path. BigQuery records constraints as NOT ENFORCED.
func (w *LoadWriter) CreateTable(ctx context.Context, tableID string, schema []Field, constraints *bq.TableConstraints) error {
	if !validLoadIdentifier(tableID) || validateLoadSchema(schema) != nil {
		return fail("invalid_input")
	}
	_, err := w.service.Tables.Get(w.projectID, w.datasetID, tableID).Context(ctx).Do()
	if err == nil {
		return ErrTableExists
	}
	if !isNotFound(err) {
		return safeLoadError("check destination table", err)
	}
	_, err = w.service.Tables.Insert(w.projectID, w.datasetID, &bq.Table{
		TableReference:   &bq.TableReference{ProjectId: w.projectID, DatasetId: w.datasetID, TableId: tableID},
		Schema:           &bq.TableSchema{Fields: apiSchema(schema)},
		TableConstraints: constraints,
	}).Context(ctx).Do()
	if isConflict(err) {
		return ErrTableExists
	}
	if err != nil {
		return safeLoadError("create destination table", err)
	}
	return nil
}

// SetConstraints updates only the primary/foreign-key metadata of a table that
// was created by this transfer. It is separate from CreateTable so cross-table
// references can be declared after every target primary key exists.
func (w *LoadWriter) SetConstraints(ctx context.Context, tableID string, constraints *bq.TableConstraints) error {
	if !validLoadIdentifier(tableID) || constraints == nil {
		return fail("invalid_input")
	}
	if _, err := w.service.Tables.Get(w.projectID, w.datasetID, tableID).Context(ctx).Do(); err != nil {
		return safeLoadError("get table constraints target", err)
	}
	_, err := w.service.Tables.Patch(w.projectID, w.datasetID, tableID, &bq.Table{TableConstraints: constraints}).Context(ctx).Do()
	if err != nil {
		return safeLoadError("set table constraints", err)
	}
	return nil
}

// CheckTablesAbsent checks every target identifier before the caller creates
// any of them. It protects existing tables and prevents a later collision from
// leaving a partially prepared destination schema.
func (w *LoadWriter) CheckTablesAbsent(ctx context.Context, tableIDs []string) error {
	seen := map[string]bool{}
	for _, tableID := range tableIDs {
		if !validLoadIdentifier(tableID) || seen[strings.ToLower(tableID)] {
			return fail("invalid_input")
		}
		seen[strings.ToLower(tableID)] = true
		_, err := w.service.Tables.Get(w.projectID, w.datasetID, tableID).Context(ctx).Do()
		if err == nil {
			return ErrTableExists
		}
		if !isNotFound(err) {
			return safeLoadError("check destination tables", err)
		}
	}
	return nil
}

// LoadTable submits one atomic NEWLINE_DELIMITED_JSON load job. The table must
// have been created by CreateTable. It never appends to or truncates a table.
func (w *LoadWriter) LoadTable(ctx context.Context, tableID string, schema []Field, ndjson io.Reader) (LoadReceipt, error) {
	if !validLoadIdentifier(tableID) || validateLoadSchema(schema) != nil || ndjson == nil {
		return LoadReceipt{}, fail("invalid_input")
	}
	table, err := w.service.Tables.Get(w.projectID, w.datasetID, tableID).Context(ctx).Do()
	if err != nil {
		return LoadReceipt{}, safeLoadError("get destination table", err)
	}
	if !schemaEqualAPI(schema, table.Schema) {
		return LoadReceipt{}, fail("destination_schema_changed")
	}
	jobID := opaqueID()
	job := &bq.Job{
		JobReference: &bq.JobReference{ProjectId: w.projectID, JobId: jobID, Location: w.location},
		Configuration: &bq.JobConfiguration{Load: &bq.JobConfigurationLoad{
			DestinationTable:    &bq.TableReference{ProjectId: w.projectID, DatasetId: w.datasetID, TableId: tableID},
			SourceFormat:        "NEWLINE_DELIMITED_JSON",
			CreateDisposition:   "CREATE_NEVER",
			WriteDisposition:    "WRITE_EMPTY",
			MaxBadRecords:       0,
			IgnoreUnknownValues: false,
			Autodetect:          false,
			ForceSendFields:     []string{"MaxBadRecords", "IgnoreUnknownValues", "Autodetect"},
			Schema:              &bq.TableSchema{Fields: apiSchema(schema)},
		}},
	}
	jobReference, err := w.service.Jobs.Insert(w.projectID, job).Media(ndjson).Context(ctx).Do()
	if err != nil {
		return w.reconcileLoad(ctx, jobID, tableID, err)
	}
	if jobReference.JobReference == nil || jobReference.JobReference.ProjectId != w.projectID || jobReference.JobReference.JobId != jobID || !strings.EqualFold(jobReference.JobReference.Location, w.location) {
		return LoadReceipt{}, ErrLoadOutcomeUnknown
	}
	return w.waitForLoad(ctx, jobID, tableID)
}

func (w *LoadWriter) reconcileLoad(ctx context.Context, jobID, tableID string, cause error) (LoadReceipt, error) {
	job, err := w.service.Jobs.Get(w.projectID, jobID).Location(w.location).Context(ctx).Do()
	if err != nil {
		if ctx.Err() != nil || !isNotFound(err) {
			return LoadReceipt{}, ErrLoadOutcomeUnknown
		}
		return LoadReceipt{}, safeLoadError("submit load job", cause)
	}
	return w.receiptFromJob(job, jobID, tableID)
}

func (w *LoadWriter) waitForLoad(ctx context.Context, jobID, tableID string) (LoadReceipt, error) {
	for {
		job, err := w.service.Jobs.Get(w.projectID, jobID).Location(w.location).Context(ctx).Do()
		if err != nil {
			return LoadReceipt{}, ErrLoadOutcomeUnknown
		}
		if job.Status != nil && job.Status.State == "DONE" {
			return w.receiptFromJob(job, jobID, tableID)
		}
		select {
		case <-ctx.Done():
			return LoadReceipt{}, ErrLoadOutcomeUnknown
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (w *LoadWriter) receiptFromJob(job *bq.Job, expectedJobID, tableID string) (LoadReceipt, error) {
	if job == nil || job.JobReference == nil || job.JobReference.ProjectId != w.projectID || job.JobReference.JobId != expectedJobID || !strings.EqualFold(job.JobReference.Location, w.location) || job.Status == nil {
		return LoadReceipt{}, ErrLoadOutcomeUnknown
	}
	if job.Configuration == nil || job.Configuration.Load == nil || job.Configuration.Load.DestinationTable == nil {
		return LoadReceipt{}, ErrLoadOutcomeUnknown
	}
	destination := job.Configuration.Load.DestinationTable
	if destination.ProjectId != w.projectID || destination.DatasetId != w.datasetID || destination.TableId != tableID {
		return LoadReceipt{}, ErrLoadOutcomeUnknown
	}
	if job.Status.State != "DONE" {
		return LoadReceipt{}, ErrLoadOutcomeUnknown
	}
	if job.Status.ErrorResult != nil {
		return LoadReceipt{}, fail("load_job_failed")
	}
	rows := uint64(0)
	if job.Statistics != nil && job.Statistics.Load != nil {
		if job.Statistics.Load.OutputRows < 0 {
			return LoadReceipt{}, ErrLoadOutcomeUnknown
		}
		rows = uint64(job.Statistics.Load.OutputRows)
	}
	return LoadReceipt{JobID: job.JobReference.JobId, ProjectID: w.projectID, DatasetID: w.datasetID, TableID: tableID, Location: w.location, Rows: rows}, nil
}

func apiSchema(schema []Field) []*bq.TableFieldSchema {
	fields := make([]*bq.TableFieldSchema, len(schema))
	for i, field := range schema {
		mode := field.Mode
		if mode == "" {
			mode = "NULLABLE"
		}
		apiField := &bq.TableFieldSchema{Name: field.Name, Type: canonicalType(field.Type), Mode: mode, Description: field.Description}
		if field.Precision != "" {
			apiField.Precision, _ = strconv.ParseInt(field.Precision, 10, 64)
		}
		if field.Scale != "" {
			apiField.Scale, _ = strconv.ParseInt(field.Scale, 10, 64)
		}
		fields[i] = apiField
	}
	return fields
}

func schemaEqualAPI(want []Field, got *bq.TableSchema) bool {
	if got == nil || len(want) != len(got.Fields) {
		return false
	}
	for i, field := range want {
		actual := got.Fields[i]
		mode := field.Mode
		if mode == "" {
			mode = "NULLABLE"
		}
		precision, _ := strconv.ParseInt(field.Precision, 10, 64)
		scale, _ := strconv.ParseInt(field.Scale, 10, 64)
		if actual == nil || actual.Name != field.Name || canonicalType(actual.Type) != canonicalType(field.Type) || effectiveMode(actual.Mode) != mode || actual.Description != field.Description || actual.Precision != precision || actual.Scale != scale {
			return false
		}
	}
	return true
}

func effectiveMode(mode string) string {
	if mode == "" {
		return "NULLABLE"
	}
	return mode
}

func validateLoadSchema(schema []Field) error {
	if validateFields(schema) != nil {
		return fail("unsupported_type")
	}
	for _, field := range schema {
		if len(field.Name) > 300 || effectiveMode(field.Mode) == "REPEATED" || len(field.Fields) > 0 {
			return fail("unsupported_type")
		}
		switch canonicalType(field.Type) {
		case "BOOL", "INT64", "FLOAT64", "STRING", "BYTES", "DATE", "TIME", "DATETIME", "TIMESTAMP", "NUMERIC", "BIGNUMERIC", "JSON":
		default:
			return fail("unsupported_type")
		}
	}
	return nil
}

func isNotFound(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound
}

func isConflict(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == http.StatusConflict
}

func safeLoadError(operation string, err error) error {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return fmt.Errorf("BigQuery %s failed with HTTP %d", operation, apiErr.Code)
	}
	return fmt.Errorf("BigQuery %s failed", operation)
}
