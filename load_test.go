package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"testing"

	bq "google.golang.org/api/bigquery/v2"
)

type loadRoundTripper func(*http.Request) (*http.Response, error)

func (f loadRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type loadSyntheticAuthTransport struct{ base http.RoundTripper }

func (t loadSyntheticAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header = req.Header.Clone()
	clone.Header.Set("Authorization", "Bearer synthetic-test-token")
	return t.base.RoundTrip(clone)
}

func loadResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}
}

func TestLoadWriterCreatesLocationPinnedDatasetAndAtomicallyLoadsTypedNDJSON(t *testing.T) {
	var tableGets, datasetPosts, tablePosts, jobPosts, jobGets int
	var uploadBody string
	var jobID string
	var ownerLabel string
	transport := loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		var body []byte
		if req.Body != nil {
			body, _ = io.ReadAll(req.Body)
		}
		path := req.URL.Path
		switch {
		case req.Method == http.MethodGet && strings.HasSuffix(path, "/datasets/demo"):
			return loadResponse(req, http.StatusNotFound, `{"error":{"code":404}}`), nil
		case req.Method == http.MethodPost && strings.HasSuffix(path, "/datasets"):
			datasetPosts++
			if !strings.Contains(string(body), `"location":"US"`) {
				t.Errorf("dataset location missing from request: %s", body)
			}
			return loadResponse(req, http.StatusOK, `{"datasetReference":{"projectId":"demodb","datasetId":"demo"},"location":"US"}`), nil
		case req.Method == http.MethodGet && strings.HasSuffix(path, "/tables/People"):
			tableGets++
			if tableGets == 1 {
				return loadResponse(req, http.StatusNotFound, `{"error":{"code":404}}`), nil
			}
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"demodb","datasetId":"demo","tableId":"People"},"etag":"etag-1","labels":{"%s":%q},"schema":{"fields":[{"name":"id","type":"INT64","mode":"REQUIRED"},{"name":"parent_id","type":"INT64","mode":"NULLABLE"},{"name":"amount","type":"NUMERIC","mode":"NULLABLE"},{"name":"payload","type":"BYTES","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel, ownerLabel)), nil
		case req.Method == http.MethodPost && strings.HasSuffix(path, "/tables"):
			tablePosts++
			var created bq.Table
			if err := json.Unmarshal(body, &created); err != nil {
				t.Errorf("decode table insert body: %v", err)
			}
			ownerLabel = created.Labels[loadWriterOwnerLabel]
			if ownerLabel == "" {
				t.Errorf("created table has no ownership label: %s", body)
			}
			if !strings.Contains(string(body), `"tableConstraints"`) || !strings.Contains(string(body), `"referencingColumn":"parent_id"`) {
				t.Errorf("table constraints missing: %s", body)
			}
			return loadResponse(req, http.StatusOK, `{"tableReference":{"projectId":"demodb","datasetId":"demo","tableId":"People"}}`), nil
		case req.Method == http.MethodPost && strings.Contains(path, "/upload/bigquery/v2/projects/demodb/jobs"):
			jobPosts++
			uploadBody = string(body)
			jobID = regexp.MustCompile(`"jobId":"([^"]+)"`).FindStringSubmatch(uploadBody)[1]
			if !strings.Contains(uploadBody, "WRITE_EMPTY") || !strings.Contains(uploadBody, "CREATE_NEVER") || !strings.Contains(uploadBody, `"maxBadRecords":0`) || !strings.Contains(uploadBody, `"ignoreUnknownValues":false`) {
				t.Errorf("load policy missing from multipart payload")
			}
			if !strings.Contains(uploadBody, `{"id":"9007199254740993123","amount":"12345678901234567890.1200","payload":"AAEC"}`) {
				t.Errorf("typed NDJSON payload was changed: %s", uploadBody)
			}
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"jobReference":{"projectId":"demodb","jobId":%q,"location":"US"},"status":{"state":"DONE"},"statistics":{"load":{"outputRows":"1"}}}`, jobID)), nil
		case req.Method == http.MethodGet && strings.Contains(path, "/jobs/"):
			jobGets++
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"jobReference":{"projectId":"demodb","jobId":%q,"location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"demodb","datasetId":"demo","tableId":"People"}}},"status":{"state":"DONE"},"statistics":{"load":{"outputRows":"1"}}}`, jobID)), nil
		default:
			t.Errorf("unexpected BigQuery request: %s %s", req.Method, req.URL)
			return loadResponse(req, http.StatusInternalServerError, `{}`), nil
		}
	})
	writer, err := NewLoadWriter(context.Background(), LoadConfig{ProjectID: "demodb", DatasetID: "demo", Location: "US", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.EnsureDataset(context.Background()); err != nil {
		t.Fatal(err)
	}
	schema := []Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}, {Name: "parent_id", Type: "INT64", Mode: "NULLABLE"}, {Name: "amount", Type: "NUMERIC", Mode: "NULLABLE"}, {Name: "payload", Type: "BYTES", Mode: "NULLABLE"}}
	constraints := &bq.TableConstraints{PrimaryKey: &bq.TableConstraintsPrimaryKey{Columns: []string{"id"}}, ForeignKeys: []*bq.TableConstraintsForeignKeys{{Name: "fk_parent", ReferencedTable: &bq.TableConstraintsForeignKeysReferencedTable{ProjectId: "demodb", DatasetId: "demo", TableId: "Parents"}, ColumnReferences: []*bq.TableConstraintsForeignKeysColumnReferences{{ReferencingColumn: "parent_id", ReferencedColumn: "id"}}}}}
	if err := writer.CreateTable(context.Background(), "People", schema, constraints); err != nil {
		t.Fatal(err)
	}
	receipt, err := writer.LoadTable(context.Background(), "People", schema, strings.NewReader("{\"id\":\"9007199254740993123\",\"amount\":\"12345678901234567890.1200\",\"payload\":\"AAEC\"}\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := LoadReceipt{JobID: jobID, ProjectID: "demodb", DatasetID: "demo", TableID: "People", Location: "US", Rows: 1}
	if !reflect.DeepEqual(want, receipt) {
		t.Fatalf("receipt = %#v, want %#v", receipt, want)
	}
	if datasetPosts != 1 || tablePosts != 1 || jobPosts != 1 || jobGets != 1 || uploadBody == "" {
		t.Fatalf("request counts dataset=%d table=%d jobs=%d jobGets=%d uploaded=%t", datasetPosts, tablePosts, jobPosts, jobGets, uploadBody != "")
	}
}

func TestLoadWriterRejectsExistingTablesAndMismatchedDatasetLocation(t *testing.T) {
	transport := loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/datasets/demo/tables/People") {
			return loadResponse(req, http.StatusOK, `{"tableReference":{"projectId":"demodb","datasetId":"demo","tableId":"People"}}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/datasets/demo") {
			return loadResponse(req, http.StatusOK, `{"datasetReference":{"projectId":"demodb","datasetId":"demo"},"location":"EU"}`), nil
		}
		return loadResponse(req, http.StatusInternalServerError, `{}`), nil
	})
	writer, err := NewLoadWriter(context.Background(), LoadConfig{ProjectID: "demodb", DatasetID: "demo", Location: "US", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.EnsureDataset(context.Background()); !errors.Is(err, ErrDatasetLocationMismatch) {
		t.Fatalf("EnsureDataset() error = %v, want location mismatch", err)
	}
	if err := writer.CreateTable(context.Background(), "People", []Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}}, nil); !errors.Is(err, ErrTableExists) {
		t.Fatalf("CreateTable() error = %v, want table exists", err)
	}
}

func TestReceiptFromJobRequiresExactJobAndDestinationIdentity(t *testing.T) {
	writer := &LoadWriter{projectID: "demodb", datasetID: "demo", location: "US"}
	valid := &bq.Job{
		JobReference: &bq.JobReference{ProjectId: "demodb", JobId: "job-1", Location: "US"},
		Configuration: &bq.JobConfiguration{Load: &bq.JobConfigurationLoad{
			DestinationTable: &bq.TableReference{ProjectId: "demodb", DatasetId: "demo", TableId: "People"},
		}},
		Status:     &bq.JobStatus{State: "DONE"},
		Statistics: &bq.JobStatistics{Load: &bq.JobStatistics3{OutputRows: 0}},
	}
	ref := LoadJobRef{JobID: "job-1", ProjectID: "demodb", DatasetID: "demo", TableID: "People", Location: "US"}
	if _, err := writer.receiptFromJob(valid, ref); err != nil {
		t.Fatalf("valid job receipt failed: %v", err)
	}

	wrongJob := *valid
	wrongJob.JobReference = &bq.JobReference{ProjectId: "demodb", JobId: "other-job", Location: "US"}
	if _, err := writer.receiptFromJob(&wrongJob, ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("wrong job identity error = %v", err)
	}

	wrongTable := *valid
	wrongTable.Configuration = &bq.JobConfiguration{Load: &bq.JobConfigurationLoad{
		DestinationTable: &bq.TableReference{ProjectId: "demodb", DatasetId: "demo", TableId: "Other"},
	}}
	if _, err := writer.receiptFromJob(&wrongTable, ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("wrong destination error = %v", err)
	}

	missingStats := *valid
	missingStats.Statistics = nil
	if _, err := writer.receiptFromJob(&missingStats, ref); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("missing load statistics error = %v, want unknown outcome", err)
	}
}

func TestLoadWriterRefusesTablesItDidNotCreate(t *testing.T) {
	requests := 0
	writer, err := NewLoadWriter(context.Background(), LoadConfig{ProjectID: "demodb", DatasetID: "demo", Location: "US", HTTPClient: &http.Client{Transport: loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		requests++
		return loadResponse(req, http.StatusInternalServerError, `{}`), nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	schema := []Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}}
	if err := writer.SetConstraints(context.Background(), "People", &bq.TableConstraints{}); !errors.Is(err, ErrTableNotOwned) {
		t.Fatalf("SetConstraints on an unowned empty table = %v", err)
	}
	if _, err := writer.LoadTable(context.Background(), "People", schema, strings.NewReader("{}\n")); !errors.Is(err, ErrTableNotOwned) {
		t.Fatalf("LoadTable on an unowned empty table = %v", err)
	}
	if requests != 0 {
		t.Fatalf("unowned table operations made %d API requests", requests)
	}
}

func TestLoadWriterBlocksResumableLocationBeforeSendingCredentialsOrRows(t *testing.T) {
	var writer *LoadWriter
	var calls []string
	var authenticatedCalls []string
	transport := loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.Host+" "+req.URL.Path)
		if req.Header.Get("Authorization") == "Bearer synthetic-test-token" {
			authenticatedCalls = append(authenticatedCalls, req.URL.Host+" "+req.URL.Path)
		}
		if req.URL.Host != "bigquery.googleapis.com" {
			t.Errorf("unapproved upload reached base transport: %s", req.URL)
		}
		if req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/datasets/demo/tables/Large") {
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"demodb","datasetId":"demo","tableId":"Large"},"etag":"etag-1","labels":{"%s":%q},"schema":{"fields":[{"name":"payload","type":"STRING","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel, writer.ownerTag)), nil
		}
		if req.Method == http.MethodPost && strings.Contains(req.URL.Path, "/upload/bigquery/v2/projects/demodb/jobs") {
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: http.Header{"Location": []string{"https://evil.example/resumable?upload_id=secret"}}, Body: io.NopCloser(strings.NewReader(`{}`)), Request: req}, nil
		}
		if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/jobs/") {
			return loadResponse(req, http.StatusNotFound, `{"error":{"code":404}}`), nil
		}
		return loadResponse(req, http.StatusInternalServerError, `{}`), nil
	})
	writer, _ = NewLoadWriter(context.Background(), LoadConfig{ProjectID: "demodb", DatasetID: "demo", Location: "US", HTTPClient: &http.Client{Transport: loadSyntheticAuthTransport{base: transport}}})
	if writer == nil {
		t.Fatal("NewLoadWriter returned nil")
	}
	schema := []Field{{Name: "payload", Type: "STRING", Mode: "NULLABLE"}}
	writer.created["Large"] = &createdLoadTable{etag: "etag-1", schema: schema}
	_, err := writer.LoadTable(context.Background(), "Large", schema, strings.NewReader(strings.Repeat("x", 16*1024*1024+1)))
	var uncertain *LoadOutcomeUnknownError
	if !errors.As(err, &uncertain) || uncertain.Job.JobID == "" {
		t.Fatalf("large upload error = %#v, want recoverable unknown outcome", err)
	}
	if len(calls) != 3 || strings.Contains(strings.Join(calls, " "), "evil.example") || len(authenticatedCalls) != 3 {
		t.Fatalf("unexpected dispatches: calls=%v authenticated=%v", calls, authenticatedCalls)
	}
}

func TestRecoverLoadUsesSameJobAfterCanceledUploadAndPollsRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var writer *LoadWriter
	var insertedJobID string
	jobGets := 0
	jobPosts := 0
	transport := loadRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/datasets/demo/tables/People") {
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"tableReference":{"projectId":"demodb","datasetId":"demo","tableId":"People"},"etag":"etag-1","labels":{"%s":%q},"schema":{"fields":[{"name":"id","type":"INT64","mode":"NULLABLE"}]}}`, loadWriterOwnerLabel, writer.ownerTag)), nil
		}
		if req.Method == http.MethodPost && strings.Contains(req.URL.Path, "/upload/bigquery/v2/projects/demodb/jobs") {
			jobPosts++
			payload, _ := io.ReadAll(req.Body)
			ids := regexp.MustCompile(`"jobId":"([^"]+)"`).FindStringSubmatch(string(payload))
			if len(ids) > 1 {
				insertedJobID = ids[1]
			}
			cancel()
			return nil, context.Canceled
		}
		if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/jobs/") {
			jobGets++
			state := "RUNNING"
			stats := ""
			if jobGets > 1 {
				state = "DONE"
				stats = `,"statistics":{"load":{"outputRows":"0"}}`
			}
			return loadResponse(req, http.StatusOK, fmt.Sprintf(`{"jobReference":{"projectId":"demodb","jobId":%q,"location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"demodb","datasetId":"demo","tableId":"People"}}},"status":{"state":%q}%s}`, insertedJobID, state, stats)), nil
		}
		return loadResponse(req, http.StatusInternalServerError, `{}`), nil
	})
	writer, _ = NewLoadWriter(context.Background(), LoadConfig{ProjectID: "demodb", DatasetID: "demo", Location: "US", HTTPClient: &http.Client{Transport: transport}})
	if writer == nil {
		t.Fatal("NewLoadWriter returned nil")
	}
	schema := []Field{{Name: "id", Type: "INT64", Mode: "NULLABLE"}}
	writer.created["People"] = &createdLoadTable{etag: "etag-1", schema: schema}
	_, err := writer.LoadTable(ctx, "People", schema, strings.NewReader("{}\n"))
	var uncertain *LoadOutcomeUnknownError
	if !errors.As(err, &uncertain) || uncertain.Job.JobID != insertedJobID {
		t.Fatalf("canceled load error = %#v, expected durable job %q", err, insertedJobID)
	}
	if jobPosts != 1 {
		t.Fatalf("load submitted %d times before recovery", jobPosts)
	}
	receipt, err := writer.RecoverLoad(context.Background(), uncertain.Job)
	if err != nil {
		t.Fatalf("RecoverLoad(): %v", err)
	}
	if receipt.JobID != insertedJobID || receipt.Rows != 0 || jobPosts != 1 || jobGets != 2 {
		t.Fatalf("receipt=%#v jobPosts=%d jobGets=%d", receipt, jobPosts, jobGets)
	}
}
