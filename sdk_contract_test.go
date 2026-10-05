package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dtql"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
	bq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

type rt func(*http.Request) (*http.Response, error)

func (f rt) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(s string) *http.Response {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(s))}
}
func svc(t *testing.T, f rt) *bq.Service {
	t.Helper()
	s, e := bq.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: f, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}), option.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func TestSDKSerializationAndMethods(t *testing.T) {
	calls := 0
	s := svc(t, func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Host != "bigquery.googleapis.com" {
			t.Fatal(r.URL.Host)
		}
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/queries") {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["useLegacySql"] != false || body["maximumBytesBilled"] != "9223372036854775807" || body["jobCreationMode"] != "JOB_CREATION_REQUIRED" {
				t.Fatalf("incorrect request: %v", body)
			}
			return response(`{"jobComplete":true,"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"schema":{"fields":[{"name":"n","type":"INTEGER"}]},"rows":[{"f":[{"v":"9223372036854775807"}]}]}`), nil
		}
		return response(`{}`), nil
	})
	no := false
	req := &bq.QueryRequest{Query: "SELECT n FROM `source-project.ds.tbl` LIMIT 2", UseLegacySql: &no, MaximumBytesBilled: 9223372036854775807, Location: "EU", DryRun: true, JobCreationMode: "JOB_CREATION_REQUIRED", ParameterMode: "NAMED", TimeoutMs: 1000, MaxResults: 1, FormatOptions: &bq.DataFormatOptions{UseInt64Timestamp: true}}
	got, err := s.Jobs.Query("job-project", req).Context(context.Background()).Do()
	if err != nil {
		t.Fatal(err)
	}
	if got.Rows[0].F[0].V != "9223372036854775807" {
		t.Fatal("lost exact integer")
	}
	_, _ = s.Datasets.Get("source-project", "ds").Context(context.Background()).Do()
	_, _ = s.Tables.Get("source-project", "ds", "tbl").Context(context.Background()).Do()
	_, _ = s.Jobs.GetQueryResults("job-project", "j").Location("EU").PageToken("p").MaxResults(1).FormatOptionsUseInt64Timestamp(true).Context(context.Background()).Do()
	_, _ = s.Jobs.Get("job-project", "j").Location("EU").Context(context.Background()).Do()
	_, _ = s.Jobs.Cancel("job-project", "j").Location("EU").Context(context.Background()).Do()
	if calls != 6 {
		t.Fatal(calls)
	}
}
func TestSubmissionNotRetriedByGeneratedClient(t *testing.T) {
	calls := 0
	s := svc(t, func(*http.Request) (*http.Response, error) { calls++; return nil, io.ErrUnexpectedEOF })
	_, err := s.Jobs.Query("job-project", &bq.QueryRequest{Query: "SELECT 1"}).Context(context.Background()).Do()
	if err == nil || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}
func TestGeneratedCellPresenceCollapse(t *testing.T) {
	for _, raw := range []string{`{}`, `{"v":null}`} {
		var cell bq.TableCell
		if err := json.Unmarshal([]byte(raw), &cell); err != nil || cell.V != nil {
			t.Fatalf("%s: %v", raw, err)
		}
	}
}

type capture struct {
	query dal.Query
	calls int
}

var _ dal.ReadSession = (*capture)(nil)

func (*capture) Get(context.Context, record.Record) error          { return dal.ErrNotSupported }
func (*capture) GetMulti(context.Context, []record.Record) error   { return dal.ErrNotSupported }
func (*capture) Exists(context.Context, *record.Key) (bool, error) { return false, dal.ErrNotSupported }
func (*capture) ExecuteQueryToRecordsReader(context.Context, dal.Query) (dal.RecordsReader, error) {
	return nil, dal.ErrNotSupported
}
func (c *capture) ExecuteQueryToRecordsetReader(_ context.Context, q dal.Query, _ ...recordset.Option) (dal.RecordsetReader, error) {
	c.calls++
	c.query = q
	return nil, nil
}
func TestPolicyCaptureNoNetwork(t *testing.T) {
	p, err := access.UnmarshalAccessPolicyYAML([]byte(`apiVersion: dalgo.io/access/v1
kind: AccessPolicy
metadata: {name: fixture}
default: deny
scopes:
- path: /customers
  rules:
  - id: own
    effect: allow
    operations: [query]
    where:
      op: "=="
      left: {field: ownerID}
      right: {param: currentUser}
    fields: [name]
`))
	if err != nil {
		t.Fatal(err)
	}
	q, err := dtql.Deserialize([]byte(`from: {name: customers}
columns: [{field: name}]
limit: 2
`))
	if err != nil {
		t.Fatal(err)
	}
	c := &capture{}
	ctx := access.WithPrincipal(context.Background(), access.Principal{ID: "alice"})
	_, err = access.SecureReadSession(c, p).ExecuteQueryToRecordsetReader(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if c.calls != 1 || c.query.(dal.StructuredQuery).Where() == nil {
		t.Fatal("policy residual not captured")
	}
	denied := &capture{}
	_, err = access.SecureReadSession(denied).ExecuteQueryToRecordsetReader(ctx, q)
	if !errors.Is(err, access.ErrAccessDenied) || denied.calls != 0 {
		t.Fatalf("denial: %d %v", denied.calls, err)
	}
}

func TestProposedDTQLExampleParses(t *testing.T) {
	_, err := dtql.Deserialize([]byte(`from: {name: sample}
columns: [{field: word}, {field: word_count}]
where: {op: ">=", left: {field: word_count}, right: {value: "10"}}
orderBy: [{field: word}]
limit: 100
`))
	if err != nil {
		t.Fatal(err)
	}
}
