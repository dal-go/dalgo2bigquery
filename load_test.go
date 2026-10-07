package bigquery

import (
	"context"
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

func loadResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}
}

func TestLoadWriterCreatesLocationPinnedDatasetAndAtomicallyLoadsTypedNDJSON(t *testing.T) {
	var tableGets, datasetPosts, tablePosts, jobPosts, jobGets int
	var uploadBody string
	var jobID string
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
			return loadResponse(req, http.StatusOK, `{"tableReference":{"projectId":"demodb","datasetId":"demo","tableId":"People"},"schema":{"fields":[{"name":"id","type":"INT64","mode":"REQUIRED"},{"name":"parent_id","type":"INT64","mode":"NULLABLE"},{"name":"amount","type":"NUMERIC","mode":"NULLABLE"},{"name":"payload","type":"BYTES","mode":"NULLABLE"}]}}`), nil
		case req.Method == http.MethodPost && strings.HasSuffix(path, "/tables"):
			tablePosts++
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
		Status: &bq.JobStatus{State: "DONE"},
	}
	if _, err := writer.receiptFromJob(valid, "job-1", "People"); err != nil {
		t.Fatalf("valid job receipt failed: %v", err)
	}

	wrongJob := *valid
	wrongJob.JobReference = &bq.JobReference{ProjectId: "demodb", JobId: "other-job", Location: "US"}
	if _, err := writer.receiptFromJob(&wrongJob, "job-1", "People"); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("wrong job identity error = %v", err)
	}

	wrongTable := *valid
	wrongTable.Configuration = &bq.JobConfiguration{Load: &bq.JobConfigurationLoad{
		DestinationTable: &bq.TableReference{ProjectId: "demodb", DatasetId: "demo", TableId: "Other"},
	}}
	if _, err := writer.receiptFromJob(&wrongTable, "job-1", "People"); !errors.Is(err, ErrLoadOutcomeUnknown) {
		t.Fatalf("wrong destination error = %v", err)
	}
}
