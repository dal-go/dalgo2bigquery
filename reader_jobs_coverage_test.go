package bigquery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	bq "google.golang.org/api/bigquery/v2"
)

func TestJobs_ParseJob_Exhaustive(t *testing.T) {
	t.Parallel()

	// 1. Not an object
	if _, err := parseJob("not-an-object"); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}

	// 2. Unknown field
	if _, err := parseJob(map[string]any{"projectId": "proj", "jobId": "j", "location": "US", "extra": 1}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on extra field, got %v", err)
	}

	// 3. Missing or bad projectId
	if _, err := parseJob(map[string]any{"jobId": "j", "location": "US"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on missing projectId, got %v", err)
	}
	if _, err := parseJob(map[string]any{"projectId": 123, "jobId": "j", "location": "US"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on int projectId, got %v", err)
	}

	// 4. Missing or bad jobId
	if _, err := parseJob(map[string]any{"projectId": "proj", "location": "US"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on missing jobId, got %v", err)
	}
	if _, err := parseJob(map[string]any{"projectId": "proj", "jobId": 123, "location": "US"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on int jobId, got %v", err)
	}

	// 5. Missing or bad location
	if _, err := parseJob(map[string]any{"projectId": "proj", "jobId": "j"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on missing location, got %v", err)
	}
	if _, err := parseJob(map[string]any{"projectId": "proj", "jobId": "j", "location": 123}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on int location, got %v", err)
	}

	// 6. Regex or length validation failures
	// Bad projectId regex
	if _, err := parseJob(map[string]any{"projectId": "invalid project!", "jobId": "j", "location": "US"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on bad projectId regex, got %v", err)
	}
	// jobId too long (> 1024)
	if _, err := parseJob(map[string]any{"projectId": "valid-proj", "jobId": strings.Repeat("x", 1025), "location": "US"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on long jobId, got %v", err)
	}
	// Bad location regex
	if _, err := parseJob(map[string]any{"projectId": "valid-proj", "jobId": "j", "location": "!bad-loc"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on bad location regex, got %v", err)
	}

	// 7. Success
	job, err := parseJob(map[string]any{"projectId": "valid-proj", "jobId": "valid-job", "location": "US"})
	if err != nil || job.ProjectID != "valid-proj" || job.JobID != "valid-job" || job.Location != "US" {
		t.Fatalf("expected success, got job=%v, err=%v", job, err)
	}
}

func TestJobs_ProviderWarnings_And_SanitizeReason(t *testing.T) {
	t.Parallel()

	// Not a slice
	if len(providerWarnings(nil)) != 0 || len(providerWarnings("not-slice")) != 0 {
		t.Fatalf("expected empty warnings for non-slice")
	}

	// Slice with non-map and maps with known and unknown reasons
	raw := []any{
		"not-a-map",
		map[string]any{"reason": "accessDenied"},
		map[string]any{"reason": "backendError"},
		map[string]any{"reason": "billingTierLimitExceeded"},
		map[string]any{"reason": "invalidQuery"},
		map[string]any{"reason": "notFound"},
		map[string]any{"reason": "rateLimitExceeded"},
		map[string]any{"reason": "resourcesExceeded"},
		map[string]any{"reason": "stopped"},
		map[string]any{"reason": "cancelled"},
		map[string]any{"reason": "unknownReasonHere"},
	}
	w := providerWarnings(raw)
	if len(w) != 10 {
		t.Fatalf("expected 10 warnings, got %d: %v", len(w), w)
	}
	if w[0] != "accessDenied" || w[8] != "cancelled" || w[9] != "provider_warning" {
		t.Fatalf("unexpected warning sanitization: %v", w)
	}

	// Direct sanitizeReason tests
	for _, reason := range []string{"accessDenied", "backendError", "billingTierLimitExceeded", "invalidQuery", "notFound", "rateLimitExceeded", "resourcesExceeded", "stopped", "cancelled"} {
		if got := sanitizeReason(reason); got != reason {
			t.Fatalf("expected %s, got %s", reason, got)
		}
	}
	if got := sanitizeReason("unrecognized"); got != "provider_warning" {
		t.Fatalf("expected provider_warning, got %s", got)
	}
}

func TestJobs_CodeOf_And_ReasonOf(t *testing.T) {
	t.Parallel()

	// Generic error
	stdErr := errors.New("plain error")
	if codeOf(stdErr) != "local_stopped" {
		t.Fatalf("expected local_stopped for generic error, got %s", codeOf(stdErr))
	}
	if reasonOf(stdErr) != "local_stopped" {
		t.Fatalf("expected local_stopped for generic error, got %s", reasonOf(stdErr))
	}

	// *Error with reason
	customErrWithReason := &Error{Code: "custom_code", Reason: "custom_reason"}
	if codeOf(customErrWithReason) != "custom_code" {
		t.Fatalf("expected custom_code, got %s", codeOf(customErrWithReason))
	}
	if reasonOf(customErrWithReason) != "custom_reason" {
		t.Fatalf("expected custom_reason, got %s", reasonOf(customErrWithReason))
	}

	// *Error without reason
	customErrNoReason := &Error{Code: "custom_code"}
	if reasonOf(customErrNoReason) != "custom_code" {
		t.Fatalf("expected custom_code as reason, got %s", reasonOf(customErrNoReason))
	}
}

func TestJobs_Executor_ExecuteQueryToRecordsReader(t *testing.T) {
	t.Parallel()

	exec := Executor{}
	if _, err := exec.ExecuteQueryToRecordsReader(context.Background(), nil); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got %v", err)
	}
}

func TestJobs_ReceiptMatches_Exhaustive(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Second)
	deadline := now.Add(time.Minute)
	job := JobRef{ProjectID: "proj", JobID: "j", Location: "US"}
	r1 := Receipt{
		Version:           1,
		RunID:             "run1",
		ApprovalDigest:    "app1",
		SourceDigest:      "src1",
		ObservationDigest: "obs1",
		SchemaDigest:      "sch1",
		Principal:         Principal{Kind: "k", Subject: "s", Generation: "1"},
		Job:               &job,
		Bounds:            DefaultBounds(),
		RunStartedAt:      now,
		ExecutionDeadline: deadline,
	}

	// Match
	if !receiptMatches(r1, r1) {
		t.Fatal("expected receiptMatches to return true for identical receipt")
	}

	// Mismatches
	diffVersion := r1
	diffVersion.Version = 2
	if receiptMatches(r1, diffVersion) {
		t.Fatal("expected mismatch on Version")
	}

	diffRunID := r1
	diffRunID.RunID = "diff"
	if receiptMatches(r1, diffRunID) {
		t.Fatal("expected mismatch on RunID")
	}

	diffApproval := r1
	diffApproval.ApprovalDigest = "diff"
	if receiptMatches(r1, diffApproval) {
		t.Fatal("expected mismatch on ApprovalDigest")
	}

	diffSource := r1
	diffSource.SourceDigest = "diff"
	if receiptMatches(r1, diffSource) {
		t.Fatal("expected mismatch on SourceDigest")
	}

	diffObs := r1
	diffObs.ObservationDigest = "diff"
	if receiptMatches(r1, diffObs) {
		t.Fatal("expected mismatch on ObservationDigest")
	}

	diffSchema := r1
	diffSchema.SchemaDigest = "diff"
	if receiptMatches(r1, diffSchema) {
		t.Fatal("expected mismatch on SchemaDigest")
	}

	diffPrincipal := r1
	diffPrincipal.Principal.Subject = "other"
	if receiptMatches(r1, diffPrincipal) {
		t.Fatal("expected mismatch on Principal")
	}

	diffJob := r1
	diffJob.Job = nil
	if receiptMatches(r1, diffJob) {
		t.Fatal("expected mismatch on Job")
	}

	diffBounds := r1
	diffBounds.Bounds.WallMs = 9999
	if receiptMatches(r1, diffBounds) {
		t.Fatal("expected mismatch on Bounds")
	}

	diffStarted := r1
	diffStarted.RunStartedAt = now.Add(time.Second)
	if receiptMatches(r1, diffStarted) {
		t.Fatal("expected mismatch on RunStartedAt")
	}

	diffDeadline := r1
	diffDeadline.ExecutionDeadline = deadline.Add(time.Second)
	if receiptMatches(r1, diffDeadline) {
		t.Fatal("expected mismatch on ExecutionDeadline")
	}
}

func TestJobs_ExecuteQueryToRecordsetReader(t *testing.T) {
	t.Parallel()

	profile := testProfile(t)
	plan, err := Compile(profile, testQuery())
	if err != nil {
		t.Fatal(err)
	}

	l, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	c, _, _, _ := dynamicClient(t, l, "1000")
	prev := previewFor(t, c, "1000")
	app, _ := c.Approve(prev, prev.ApprovalDigest)

	// Case 1: Compile error (invalid query)
	badQuery := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "", nil))).SelectIntoRecordset()
	exec := Executor{
		Client:   c,
		Profile:  profile,
		Approval: app,
		Plan:     plan,
	}
	if _, err := exec.ExecuteQueryToRecordsetReader(context.Background(), badQuery); errorCode(err) != "unsupported_query" {
		t.Fatalf("expected unsupported_query, got %v", err)
	}

	// Case 2: Plan digest mismatch
	otherQuery := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("sample", "", nil))).Limit(50).SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "n")})
	if _, err := exec.ExecuteQueryToRecordsetReader(context.Background(), otherQuery); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed on digest mismatch, got %v", err)
	}

	// Case 3: Success
	reader, err := exec.ExecuteQueryToRecordsetReader(context.Background(), testQuery())
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	defer reader.Close()
}

func TestJobs_FindJob_And_CaptureJob(t *testing.T) {
	t.Parallel()

	l, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	c, _, _, _ := dynamicClient(t, l, "1000")

	// 1. findJob when not found
	job := JobRef{ProjectID: "job-project", JobID: "non-existent", Location: "EU"}
	if _, err := c.findJob(job); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid for non-existent job, got %v", err)
	}

	// Create a real run to test captureJob
	prev := previewFor(t, c, "1000")
	app, _ := c.Approve(prev, prev.ApprovalDigest)
	run, err := c.Execute(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()

	// 2. captureJob with invalid job reference
	if err := c.captureJob(context.Background(), run.id, map[string]any{"jobReference": "not-a-map"}); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on bad jobReference, got %v", err)
	}

	// 3. captureJob with project or location mismatch
	badProjJob := map[string]any{"jobReference": map[string]any{"projectId": "other-proj", "jobId": "j1", "location": "EU"}}
	if err := c.captureJob(context.Background(), run.id, badProjJob); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on jobProject mismatch, got %v", err)
	}
	badLocJob := map[string]any{"jobReference": map[string]any{"projectId": "job-project", "jobId": "j1", "location": "US"}}
	if err := c.captureJob(context.Background(), run.id, badLocJob); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on location mismatch, got %v", err)
	}

	// 4. captureJob with job mismatch when job is already set
	otherJob := map[string]any{"jobReference": map[string]any{"projectId": "job-project", "jobId": "other-job", "location": "EU"}}
	if err := c.captureJob(context.Background(), run.id, otherJob); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on existing job mismatch, got %v", err)
	}

	// 5. findJob success
	existingJob := JobRef{ProjectID: "job-project", JobID: "j", Location: "EU"}
	rec, err := c.findJob(existingJob)
	if err != nil || rec.Receipt.RunID != run.id {
		t.Fatalf("expected findJob to return runRecord, got rec=%v, err=%v", rec, err)
	}
}

func TestJobs_Execute_ValidationAndErrors(t *testing.T) {
	t.Parallel()

	l, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	c, _, clock, _ := dynamicClient(t, l, "1000")
	prev := previewFor(t, c, "1000")
	app, _ := c.Approve(prev, prev.ApprovalDigest)

	// 1. Client mismatch
	cOther, _, _, _ := dynamicClient(t, l, "1000")
	if _, err := cOther.Execute(context.Background(), app); errorCode(err) != "approval_required" {
		t.Fatalf("expected approval_required for client mismatch, got %v", err)
	}

	// 2. Empty nonce or digest
	badApp := app
	badApp.nonce = ""
	if _, err := c.Execute(context.Background(), badApp); errorCode(err) != "approval_required" {
		t.Fatalf("expected approval_required on empty nonce, got %v", err)
	}
	badApp = app
	badApp.digest = ""
	if _, err := c.Execute(context.Background(), badApp); errorCode(err) != "approval_required" {
		t.Fatalf("expected approval_required on empty digest, got %v", err)
	}

	// 3. Invalid bounds
	badBoundsApp := app
	badBoundsApp.bounds = Bounds{WallMs: -1}
	if _, err := c.Execute(context.Background(), badBoundsApp); errorCode(err) != "approval_required" {
		t.Fatalf("expected approval_required on invalid bounds, got %v", err)
	}

	// 4. Nonce not in ledger
	missingNonceApp := app
	missingNonceApp.nonce = "missing-nonce"
	if _, err := c.Execute(context.Background(), missingNonceApp); errorCode(err) != "approval_required" {
		t.Fatalf("expected approval_required on missing nonce, got %v", err)
	}

	// 5. Bounds changed between preview and approval
	prev2 := previewFor(t, c, "1000")
	app2, _ := c.Approve(prev2, prev2.ApprovalDigest)
	app2.bounds.WallMs = 9999
	if _, err := c.Execute(context.Background(), app2); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed on bounds mismatch, got %v", err)
	}

	// 6. Preview already used
	prevUsed := previewFor(t, c, "1000")
	appUsed, _ := c.Approve(prevUsed, prevUsed.ApprovalDigest)
	rUsed, err := c.Execute(context.Background(), appUsed)
	if err != nil {
		t.Fatal(err)
	}
	rUsed.Close()
	if _, err := c.Execute(context.Background(), appUsed); errorCode(err) != "approval_required" {
		t.Fatalf("expected approval_required on already used preview, got %v", err)
	}

	// 7. Preview expired
	prevExpired := previewFor(t, c, "1000")
	appExpired, _ := c.Approve(prevExpired, prevExpired.ApprovalDigest)
	clock.now = clock.now.Add(2 * time.Hour) // expires in 1 hour
	if _, err := c.Execute(context.Background(), appExpired); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed on expired preview, got %v", err)
	}
	clock.now = clock.now.Add(-2 * time.Hour) // reset clock

	// 8. Digest mismatch
	prevDigest := previewFor(t, c, "1000")
	appDigest, _ := c.Approve(prevDigest, prevDigest.ApprovalDigest)
	appDigest.digest = "altered-digest"
	if _, err := c.Execute(context.Background(), appDigest); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed on digest mismatch, got %v", err)
	}

	// 9. Budget exhausted checks
	lBudget, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger-budget"))
	cBudget, _, _, _ := dynamicClient(t, lBudget, "1000")
	prevBudget1 := previewFor(t, cBudget, "1000")
	appBudget1, _ := cBudget.Approve(prevBudget1, prevBudget1.ApprovalDigest)
	rB1, err := cBudget.Execute(context.Background(), appBudget1)
	if err != nil {
		t.Fatal(err)
	}
	defer rB1.Close()

	// In the ledger, modify rB1 reservation to exhaust budget
	_ = cBudget.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		runRec := s.Runs[rB1.id]
		runRec.Reservation = 1000 // entire budget
		s.Runs[rB1.id] = runRec
		return nil
	})

	prevExhausted := previewFor(t, cBudget, "1000")
	appExhausted, _ := cBudget.Approve(prevExhausted, prevExhausted.ApprovalDigest)
	if _, err := cBudget.Execute(context.Background(), appExhausted); errorCode(err) != "budget_exhausted" {
		t.Fatalf("expected budget_exhausted, got %v", err)
	}

	// Inconsistent session budget bytes across runs
	_ = cBudget.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		runRec := s.Runs[rB1.id]
		runRec.Preview.Execution.SessionBudgetBytes = "2000"
		runRec.Reservation = 100
		s.Runs[rB1.id] = runRec
		return nil
	})
	prevInconsistent := previewFor(t, cBudget, "1000")
	appInconsistent, _ := cBudget.Approve(prevInconsistent, prevInconsistent.ApprovalDigest)
	if _, err := cBudget.Execute(context.Background(), appInconsistent); errorCode(err) != "budget_exhausted" {
		t.Fatalf("expected budget_exhausted on inconsistent session budget, got %v", err)
	}

	// r.Reservation > budget - reserved
	_ = cBudget.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		runRec := s.Runs[rB1.id]
		runRec.Preview.Execution.SessionBudgetBytes = "1000"
		runRec.Reservation = 1500 // > budget
		s.Runs[rB1.id] = runRec
		return nil
	})
	prevOverReserved := previewFor(t, cBudget, "1000")
	appOverReserved, _ := cBudget.Approve(prevOverReserved, prevOverReserved.ApprovalDigest)
	if _, err := cBudget.Execute(context.Background(), appOverReserved); errorCode(err) != "budget_exhausted" {
		t.Fatalf("expected budget_exhausted when reservation exceeds budget, got %v", err)
	}
}

func TestJobs_Execute_BeforeSubmitErrors(t *testing.T) {
	t.Parallel()

	// 1. Source changed between preview and execute
	l, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	tblCalls := 0
	transport := rt(func(req *http.Request) (*http.Response, error) {
		t.Logf("REQ: %s %s", req.Method, req.URL.Path)
		if strings.HasSuffix(req.URL.Path, "/datasets/ds") {
			return response(`{"datasetReference":{"projectId":"source-project","datasetId":"ds"},"location":"EU"}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
			tblCalls++
			extra := ""
			if tblCalls == 3 {
				extra = `,"requirePartitionFilter":true`
			}
			return response(fmt.Sprintf(`{"tableReference":{"projectId":"source-project","datasetId":"ds","tableId":"tbl"},"type":"TABLE"%s,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`, extra)), nil
		}
		if req.Method == "POST" {
			var m map[string]any
			_ = json.NewDecoder(req.Body).Decode(&m)
			if m["dryRun"] == true {
				return response(`{"totalBytesProcessed":"100"}`), nil
			}
			return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"rows":[]}`), nil
		}
		return response(`{}`), nil
	})

	p := testProfile(t)
	plan, _ := Compile(p, testQuery())
	clock := &fakeClock{now: time.Now()}
	identity := &testProvider{identity: Identity{Principal{"workload", "operator:fixture", "1"}, clock.Now().Add(time.Hour), true, true}}
	c, _ := NewClient(Config{[]SourceProfile{p}, identity, transport, l, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})

	prev := previewFor(t, c, "1000")
	app, _ := c.Approve(prev, prev.ApprovalDigest)

	if _, err := c.Execute(context.Background(), app); errorCode(err) != "source_changed" {
		t.Fatalf("expected source_changed, got %v", err)
	}

	// 2. Query execution call error: dispatched vs non-dispatched
	l2, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	failPost := false
	transport2 := rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/ds") {
			return response(`{"datasetReference":{"projectId":"source-project","datasetId":"ds"},"location":"EU"}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
			return response(`{"tableReference":{"projectId":"source-project","datasetId":"ds","tableId":"tbl"},"type":"TABLE","etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`), nil
		}
		if req.Method == "POST" {
			var m map[string]any
			_ = json.NewDecoder(req.Body).Decode(&m)
			if m["dryRun"] == true {
				return response(`{"totalBytesProcessed":"100"}`), nil
			}
			if failPost {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":500,"message":"query failed"}}`)),
					Header:     http.Header{"Content-Type": {"application/json"}},
				}, nil
			}
			return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"rows":[]}`), nil
		}
		return response(`{}`), nil
	})
	c2, _ := NewClient(Config{[]SourceProfile{p}, identity, transport2, l2, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	prev2 := previewFor(t, c2, "1000")
	app2, _ := c2.Approve(prev2, prev2.ApprovalDigest)
	failPost = true
	if _, err := c2.Execute(context.Background(), app2); err == nil {
		t.Fatal("expected error on failed POST jobs/query")
	}

	// 3. Query execution returns malformed wire (captureJob error)
	l3, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	malformedCapture := false
	transport3 := rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/ds") {
			return response(`{"datasetReference":{"projectId":"source-project","datasetId":"ds"},"location":"EU"}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
			return response(`{"tableReference":{"projectId":"source-project","datasetId":"ds","tableId":"tbl"},"type":"TABLE","etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`), nil
		}
		if req.Method == "POST" {
			var m map[string]any
			_ = json.NewDecoder(req.Body).Decode(&m)
			if m["dryRun"] == true {
				return response(`{"totalBytesProcessed":"100"}`), nil
			}
			if malformedCapture {
				// Missing jobReference
				return response(`{"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"rows":[]}`), nil
			}
			return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"rows":[]}`), nil
		}
		return response(`{}`), nil
	})
	c3, _ := NewClient(Config{[]SourceProfile{p}, identity, transport3, l3, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	prev3 := previewFor(t, c3, "1000")
	app3, _ := c3.Approve(prev3, prev3.ApprovalDigest)
	malformedCapture = true
	if _, err := c3.Execute(context.Background(), app3); errorCode(err) != "submission_unknown" {
		t.Fatalf("expected submission_unknown on bad job capture, got %v", err)
	}

	// 4. Query execution returns loadPage error
	l4, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	badLoadPage := false
	transport4 := rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/ds") {
			return response(`{"datasetReference":{"projectId":"source-project","datasetId":"ds"},"location":"EU"}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
			return response(`{"tableReference":{"projectId":"source-project","datasetId":"ds","tableId":"tbl"},"type":"TABLE","etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`), nil
		}
		if req.Method == "POST" {
			var m map[string]any
			_ = json.NewDecoder(req.Body).Decode(&m)
			if m["dryRun"] == true {
				return response(`{"totalBytesProcessed":"100"}`), nil
			}
			if badLoadPage {
				// jobComplete is not a boolean -> loadPage fails with malformed_wire
				return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":"not-a-bool"}`), nil
			}
			return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"rows":[]}`), nil
		}
		return response(`{}`), nil
	})
	c4, _ := NewClient(Config{[]SourceProfile{p}, identity, transport4, l4, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	prev4 := previewFor(t, c4, "1000")
	app4, _ := c4.Approve(prev4, prev4.ApprovalDigest)
	badLoadPage = true
	if _, err := c4.Execute(context.Background(), app4); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on bad jobComplete in loadPage, got %v", err)
	}
}

func TestJobs_Status_Exhaustive(t *testing.T) {
	t.Parallel()

	l, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	statusJSON := `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"RUNNING"}}`
	transport := rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/ds") {
			return response(`{"datasetReference":{"projectId":"source-project","datasetId":"ds"},"location":"EU"}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
			return response(`{"tableReference":{"projectId":"source-project","datasetId":"ds","tableId":"tbl"},"type":"TABLE","etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`), nil
		}
		if req.Method == "POST" {
			var m map[string]any
			_ = json.NewDecoder(req.Body).Decode(&m)
			if m["dryRun"] == true {
				return response(`{"totalBytesProcessed":"100"}`), nil
			}
			return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"rows":[]}`), nil
		}
		if req.Method == "GET" && strings.Contains(req.URL.Path, "/jobs/") {
			return response(statusJSON), nil
		}
		return response(`{}`), nil
	})

	p := testProfile(t)
	plan, _ := Compile(p, testQuery())
	clock := &fakeClock{now: time.Now()}
	identity := &testProvider{identity: Identity{Principal{"workload", "operator:fixture", "1"}, clock.Now().Add(time.Hour), true, true}}
	c, _ := NewClient(Config{[]SourceProfile{p}, identity, transport, l, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})

	prev := previewFor(t, c, "1000")
	app, _ := c.Approve(prev, prev.ApprovalDigest)
	run, err := c.Execute(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()

	job := JobRef{ProjectID: "job-project", JobID: "j", Location: "EU"}

	// 1. RUNNING state
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"RUNNING"}}`
	st, err := c.Status(context.Background(), job)
	if err != nil || st.State != "running" {
		t.Fatalf("expected running, got st=%v, err=%v", st, err)
	}

	// 2. PENDING state
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"PENDING"}}`
	st, err = c.Status(context.Background(), job)
	if err != nil || st.State != "running" {
		t.Fatalf("expected running for pending, got st=%v, err=%v", st, err)
	}

	// 3. DONE with errorResult cancelled
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"DONE","errorResult":{"reason":"cancelled"}}}`
	st, err = c.Status(context.Background(), job)
	if err != nil || st.State != "cancelled" {
		t.Fatalf("expected cancelled, got st=%v, err=%v", st, err)
	}

	// 4. DONE with errorResult stopped
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"DONE","errorResult":{"reason":"stopped"}}}`
	st, err = c.Status(context.Background(), job)
	if err != nil || st.State != "failed" {
		t.Fatalf("expected failed, got st=%v, err=%v", st, err)
	}

	// 5. DONE with statistics query totalBytesBilled
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"DONE"},"statistics":{"query":{"totalBytesBilled":"123"}}}`
	st, err = c.Status(context.Background(), job)
	if err != nil || st.State != "completed" || st.BilledBytes == nil || *st.BilledBytes != "123" {
		t.Fatalf("expected completed with 123 billed bytes, got st=%v, err=%v", st, err)
	}

	// 6. DONE when cancel_requested -> warning completed_before_cancel
	_ = c.mutateRunContext(context.Background(), run.id, func(rr *runRecord) error {
		rr.Receipt.State = "cancel_requested"
		return nil
	})
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"DONE"}}`
	st, err = c.Status(context.Background(), job)
	if err != nil || len(st.Warnings) == 0 || st.Warnings[0] != "completed_before_cancel" {
		t.Fatalf("expected completed_before_cancel warning, got st=%v, err=%v", st, err)
	}

	// 7. Malformed wire errors in Status:
	// Job mismatch
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"different-job","location":"EU"},"status":{"state":"RUNNING"}}`
	if _, err := c.Status(context.Background(), job); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on job mismatch, got %v", err)
	}

	// Status not an object
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":"not-an-object"}`
	if _, err := c.Status(context.Background(), job); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on non-object status, got %v", err)
	}

	// Status unknown field
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"RUNNING","extra":1}}`
	if _, err := c.Status(context.Background(), job); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on extra status field, got %v", err)
	}

	// Status bad state
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"UNKNOWN_STATE"}}`
	if _, err := c.Status(context.Background(), job); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on bad state, got %v", err)
	}

	// Status errorResult not an object
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"DONE","errorResult":"not-obj"}}`
	if _, err := c.Status(context.Background(), job); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on non-object errorResult, got %v", err)
	}

	// Status errorResult missing reason
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"DONE","errorResult":{}}}`
	if _, err := c.Status(context.Background(), job); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on missing reason, got %v", err)
	}

	// Statistics not an object
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"DONE"},"statistics":"not-obj"}`
	if _, err := c.Status(context.Background(), job); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on non-object statistics, got %v", err)
	}

	// Statistics query not an object
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"DONE"},"statistics":{"query":"not-obj"}}`
	if _, err := c.Status(context.Background(), job); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on non-object query, got %v", err)
	}

	// Statistics query totalBytesBilled not decimalString
	statusJSON = `{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"status":{"state":"DONE"},"statistics":{"query":{"totalBytesBilled":"-1"}}}`
	if _, err := c.Status(context.Background(), job); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on bad totalBytesBilled, got %v", err)
	}
}

func TestJobs_CancelJob_Exhaustive(t *testing.T) {
	t.Parallel()

	l, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	cancelResp := `{"job":{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"}}}`
	cancelStatus := http.StatusOK
	transport := rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/ds") {
			return response(`{"datasetReference":{"projectId":"source-project","datasetId":"ds"},"location":"EU"}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
			return response(`{"tableReference":{"projectId":"source-project","datasetId":"ds","tableId":"tbl"},"type":"TABLE","etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`), nil
		}
		if req.Method == "POST" && strings.Contains(req.URL.Path, "/cancel") {
			if cancelStatus != http.StatusOK {
				return &http.Response{
					StatusCode: cancelStatus,
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":401,"message":"auth required"}}`)),
					Header:     http.Header{"Content-Type": {"application/json"}},
				}, nil
			}
			return response(cancelResp), nil
		}
		if req.Method == "POST" {
			var m map[string]any
			_ = json.NewDecoder(req.Body).Decode(&m)
			if m["dryRun"] == true {
				return response(`{"totalBytesProcessed":"100"}`), nil
			}
			return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"rows":[]}`), nil
		}
		return response(`{}`), nil
	})

	p := testProfile(t)
	plan, _ := Compile(p, testQuery())
	clock := &fakeClock{now: time.Now()}
	identity := &testProvider{identity: Identity{Principal{"workload", "operator:fixture", "1"}, clock.Now().Add(time.Hour), true, true}}
	c, _ := NewClient(Config{[]SourceProfile{p}, identity, transport, l, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})

	prev := previewFor(t, c, "1000")
	app, _ := c.Approve(prev, prev.ApprovalDigest)
	run, err := c.Execute(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()

	job := JobRef{ProjectID: "job-project", JobID: "j", Location: "EU"}

	// 1. Success
	res, err := c.CancelJob(context.Background(), job)
	if err != nil || res.State != "cancel_requested" {
		t.Fatalf("expected cancel_requested, got res=%v, err=%v", res, err)
	}

	// 2. Call error with auth_expired
	cancelStatus = http.StatusUnauthorized
	if _, err := c.CancelJob(context.Background(), job); errorCode(err) != "auth_expired" {
		t.Fatalf("expected auth_expired, got %v", err)
	}
	cancelStatus = http.StatusOK

	// 3. Call error with 500 -> cancellation_unknown
	cancelStatus = http.StatusInternalServerError
	if _, err := c.CancelJob(context.Background(), job); errorCode(err) != "cancellation_unknown" {
		t.Fatalf("expected cancellation_unknown on 500, got %v", err)
	}
	cancelStatus = http.StatusOK

	// 4. Bad payload (job not an object)
	cancelResp = `{"job":"not-obj"}`
	if _, err := c.CancelJob(context.Background(), job); errorCode(err) != "cancellation_unknown" {
		t.Fatalf("expected cancellation_unknown on bad job object, got %v", err)
	}

	// 5. Job reference mismatch
	cancelResp = `{"job":{"jobReference":{"projectId":"job-project","jobId":"diff-job","location":"EU"}}}`
	if _, err := c.CancelJob(context.Background(), job); errorCode(err) != "cancellation_unknown" {
		t.Fatalf("expected cancellation_unknown on job mismatch, got %v", err)
	}
}

func TestReader_MakeRecordset_And_RunMethods(t *testing.T) {
	t.Parallel()

	fs := []Field{
		{Name: "id", Type: "INT64"},
		{Name: "val", Type: "STRING"},
	}
	cells := [][]Cell{
		{{Value: int64(1)}, {Value: "hello"}},
		{{Value: int64(2)}, {Value: "world"}},
	}
	rs := makeRecordset(fs, cells)
	if rs == nil || rs.GetRow(0) == nil || rs.GetRow(1) == nil {
		t.Fatalf("expected 2 valid rows in recordset")
	}

	// Test Run methods on unpersisted run ID
	l, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	c, _, _, _ := dynamicClient(t, l, "1000")
	ghostRun := newRun(c, context.Background(), "ghost-id")
	receipt := ghostRun.Receipt()
	if receipt.RunID != "ghost-id" || receipt.Reason == "" {
		t.Fatalf("expected receipt with reason for missing run, got %v", receipt)
	}
	if schema := ghostRun.Schema(); schema != nil {
		t.Fatalf("expected nil schema for missing run, got %v", schema)
	}
	if _, err := ghostRun.Cursor(); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid for missing run, got %v", err)
	}
	_ = ghostRun.Close()

	// Test Recordset on Run
	ghostRun.fields = fs
	ghostRun.delivered = cells
	rsRun := ghostRun.Recordset()
	if rsRun == nil || rsRun.GetRow(0) == nil {
		t.Fatalf("expected valid Recordset from Run.Recordset()")
	}
}

func TestReader_LoadPage_Exhaustive(t *testing.T) {
	t.Parallel()

	l, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	c, _, _, _ := dynamicClient(t, l, "1000")
	prev := previewFor(t, c, "1000")
	app, _ := c.Approve(prev, prev.ApprovalDigest)
	run, err := c.Execute(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()

	// 1. Unknown fields in response
	badFields := map[string]any{"jobReference": map[string]any{"projectId": "job-project", "jobId": "j", "location": "EU"}, "jobComplete": true, "unapproved": true}
	if err := run.loadPage(context.Background(), badFields, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on unapproved field, got %v", err)
	}

	// 2. Job reference mismatch
	badJob := map[string]any{"jobReference": map[string]any{"projectId": "job-project", "jobId": "different", "location": "EU"}, "jobComplete": true}
	if err := run.loadPage(context.Background(), badJob, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on job reference mismatch, got %v", err)
	}

	// 3. jobComplete not bool
	badComplete := map[string]any{"jobComplete": 123}
	if err := run.loadPage(context.Background(), badComplete, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on non-bool jobComplete, got %v", err)
	}

	// 4. schema changed: incompatible schema
	badSchema := map[string]any{
		"jobComplete": true,
		"schema": map[string]any{
			"fields": []any{
				map[string]any{"name": "different_col", "type": "STRING", "mode": "NULLABLE"},
			},
		},
	}
	if err := run.loadPage(context.Background(), badSchema, nil, false); errorCode(err) != "schema_changed" {
		t.Fatalf("expected schema_changed on schema mismatch, got %v", err)
	}

	// 5. rows present and is nil
	nilRows := map[string]any{"jobComplete": true, "rows": nil}
	if err := run.loadPage(context.Background(), nilRows, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on nil rows, got %v", err)
	}

	// 6. pageToken invalid (e.g. empty or too long)
	badToken := map[string]any{"jobComplete": true, "pageToken": ""}
	if err := run.loadPage(context.Background(), badToken, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on empty pageToken, got %v", err)
	}
	longToken := map[string]any{"jobComplete": true, "pageToken": strings.Repeat("a", 16385)}
	if err := run.loadPage(context.Background(), longToken, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on long pageToken, got %v", err)
	}

	// 7. cacheHit not a bool
	badCache := map[string]any{"jobComplete": true, "cacheHit": "not-bool"}
	if err := run.loadPage(context.Background(), badCache, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on non-bool cacheHit, got %v", err)
	}

	// 8. totalBytesProcessed not a decimal string
	badProcessed := map[string]any{"jobComplete": true, "totalBytesProcessed": "abc"}
	if err := run.loadPage(context.Background(), badProcessed, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on non-decimal totalBytesProcessed, got %v", err)
	}

	// 9. totalBytesBilled not a decimal string
	badBilled := map[string]any{"jobComplete": true, "totalBytesBilled": "abc"}
	if err := run.loadPage(context.Background(), badBilled, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on non-decimal totalBytesBilled, got %v", err)
	}

	// 10. Refetch cursor_invalid tests:
	// Refetch with different page digest
	validPage := map[string]any{
		"jobComplete": true,
		"rows":        []any{map[string]any{"f": []any{map[string]any{"v": "999"}}}},
	}
	if err := run.loadPage(context.Background(), validPage, nil, true); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on refetch digest mismatch, got %v", err)
	}

	// 11. Repeated / seen token
	seenTok := "token-seen"
	_ = c.mutateRunContext(context.Background(), run.id, func(rr *runRecord) error {
		rr.SeenTokens[seenTok] = true
		return nil
	})
	seenPage := map[string]any{
		"jobComplete": false,
		"pageToken":   seenTok,
		"rows":        []any{},
	}
	if err := run.loadPage(context.Background(), seenPage, nil, false); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on seen token, got %v", err)
	}

	// 12. Token equals current token
	currTok := "curr-tok"
	sameTokPage := map[string]any{
		"jobComplete": false,
		"pageToken":   currTok,
		"rows":        []any{},
	}
	if err := run.loadPage(context.Background(), sameTokPage, &currTok, false); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid when pageToken == current token, got %v", err)
	}
}

func TestReader_Ensure_And_CommitDelivery(t *testing.T) {
	t.Parallel()

	l, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	c, _, clock, _ := dynamicClient(t, l, "1000")
	prev := previewFor(t, c, "1000")
	app, _ := c.Approve(prev, prev.ApprovalDigest)
	run, err := c.Execute(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()

	// 1. ensure() when context canceled -> local_stopped
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	run.ctx = canceledCtx
	if err := run.ensure(); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on canceled ctx, got %v", err)
	}
	run.ctx = context.Background()

	// 2. ensure() when execution deadline passed -> deadlineError
	clock.now = clock.now.Add(2 * time.Hour)
	if err := run.ensure(); errorCode(err) != "local_stopped" || reasonOf(err) != "deadline" {
		t.Fatalf("expected deadline error on expired deadline, got %v", err)
	}
	clock.now = clock.now.Add(-2 * time.Hour)

	// 3. ensure() when access != rr.ActivePrincipal -> approval_changed
	origAccess := run.access
	run.access = Principal{Kind: "tampered", Subject: "bad", Generation: "1"}
	if err := run.ensure(); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed on tampered principal, got %v", err)
	}
	run.access = origAccess

	// 4. ensure() when row limit reached -> response_limit
	_ = c.mutateRunContext(context.Background(), run.id, func(rr *runRecord) error {
		rr.Receipt.Counters.Rows = rr.Receipt.Bounds.MaxRows
		return nil
	})
	if err := run.ensure(); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit when max rows reached, got %v", err)
	}
	_ = c.mutateRunContext(context.Background(), run.id, func(rr *runRecord) error {
		rr.Receipt.Counters.Rows = 0
		return nil
	})

	// 5. ensure() when offset mismatch -> cursor_invalid
	run.loaded = true
	run.index = 10
	_ = c.mutateRunContext(context.Background(), run.id, func(rr *runRecord) error {
		rr.Offset = 5
		return nil
	})
	if err := run.ensure(); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on offset mismatch, got %v", err)
	}
	run.index = 0
	_ = c.mutateRunContext(context.Background(), run.id, func(rr *runRecord) error {
		rr.Offset = 0
		return nil
	})

	// 6. commitDelivery tests:
	// Invalid delivery count (> MaxRows - Rows) -> response_limit
	if _, err := run.commitDelivery(999999); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit on over-delivery, got %v", err)
	}
	// Negative delivery count -> response_limit
	if _, err := run.commitDelivery(-1); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit on negative delivery, got %v", err)
	}

	// 7. commitDelivery when offset == len(rows) and NextToken != nil
	tok := "next-page-tok"
	run.rows = [][]Cell{{{Value: int64(1)}}}
	run.index = 0
	_ = c.mutateRunContext(context.Background(), run.id, func(rr *runRecord) error {
		rr.NextToken = &tok
		return nil
	})
	cursorStr, err := run.commitDelivery(1)
	if err != nil || cursorStr == "" {
		t.Fatalf("expected successful cursor creation, got cursor=%s, err=%v", cursorStr, err)
	}

	// Verify Cursor() returns matching string
	cGot, err := run.Cursor()
	if err != nil || cGot != cursorStr {
		t.Fatalf("expected Cursor() to match, got %s != %s", cGot, cursorStr)
	}
}

func TestReader_Next_And_NextPage_Exhaustive(t *testing.T) {
	t.Parallel()

	// 1. Mode collision: Next() then NextPage() and vice versa
	l, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	c, _, _, _ := dynamicClient(t, l, "1000")
	prev := previewFor(t, c, "1000")
	app, _ := c.Approve(prev, prev.ApprovalDigest)
	run, err := c.Execute(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()

	// Next() sets mode = "row"
	_, _, err = run.Next()
	if err != nil {
		t.Fatalf("Next() failed: %v", err)
	}
	// NextPage() on row mode -> invalid_input
	if _, err := run.NextPage(); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on NextPage after Next, got %v", err)
	}

	// Next() on page mode -> invalid_input
	l2, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger-run2"))
	c2, _, _, _ := dynamicClient(t, l2, "1000")
	prev2 := previewFor(t, c2, "1000")
	app2, _ := c2.Approve(prev2, prev2.ApprovalDigest)
	run2, err := c2.Execute(context.Background(), app2)
	if err != nil {
		t.Fatal(err)
	}
	defer run2.Close()

	// NextPage() sets mode = "page"
	_, err = run2.NextPage()
	if err != nil {
		t.Fatalf("NextPage() failed: %v", err)
	}
	if _, _, err := run2.Next(); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on Next after NextPage, got %v", err)
	}

	// 2. Empty page delivery when rows == 0
	l3, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	transport3 := rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/ds") {
			return response(`{"datasetReference":{"projectId":"source-project","datasetId":"ds"},"location":"EU"}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
			return response(`{"tableReference":{"projectId":"source-project","datasetId":"ds","tableId":"tbl"},"type":"TABLE","etag":"etag-1","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`), nil
		}
		if req.Method == "POST" {
			var m map[string]any
			_ = json.NewDecoder(req.Body).Decode(&m)
			if m["dryRun"] == true {
				return response(`{"totalBytesProcessed":"100"}`), nil
			}
			// Empty rows
			return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"rows":[]}`), nil
		}
		return response(`{}`), nil
	})
	p := testProfile(t)
	plan, _ := Compile(p, testQuery())
	clock := &fakeClock{now: time.Now()}
	identity := &testProvider{identity: Identity{Principal{"workload", "operator:fixture", "1"}, clock.Now().Add(time.Hour), true, true}}
	c3, _ := NewClient(Config{[]SourceProfile{p}, identity, transport3, l3, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	prev3 := previewFor(t, c3, "1000")
	app3, _ := c3.Approve(prev3, prev3.ApprovalDigest)
	run3, err := c3.Execute(context.Background(), app3)
	if err != nil {
		t.Fatal(err)
	}
	defer run3.Close()

	// First NextPage on empty dataset delivers empty page
	page, err := run3.NextPage()
	if err != nil || len(page.Rows) != 0 {
		t.Fatalf("expected empty page delivery, got page=%v, err=%v", page, err)
	}
	// Second NextPage returns ErrNoMoreRecords
	if _, err := run3.NextPage(); !errors.Is(err, dal.ErrNoMoreRecords) {
		t.Fatalf("expected ErrNoMoreRecords after empty page delivery, got %v", err)
	}
}

func TestReader_Resume_Exhaustive(t *testing.T) {
	t.Parallel()

	l, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	c, _, clock, _ := dynamicClient(t, l, "1000")
	prev := previewFor(t, c, "1000")
	app, _ := c.Approve(prev, prev.ApprovalDigest)
	run, err := c.Execute(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()

	// Get a valid page to establish cursor
	page, err := run.NextPage()
	if err != nil {
		t.Fatal(err)
	}
	cursorEncoded := page.Cursor
	receipt := page.Receipt

	// 1. Invalid base64 or length > 32768
	if _, err := c.Resume(context.Background(), receipt, "invalid-base64!!!"); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on bad base64, got %v", err)
	}
	if _, err := c.Resume(context.Background(), receipt, strings.Repeat("A", 45000)); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on oversized cursor, got %v", err)
	}

	// 2. Not valid JSON
	badJSON := base64.RawURLEncoding.EncodeToString([]byte("not-json"))
	if _, err := c.Resume(context.Background(), receipt, badJSON); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on non-json, got %v", err)
	}

	// 3. Unknown field in cursor JSON
	var cursorMap map[string]any
	rawBytes, _ := base64.RawURLEncoding.DecodeString(cursorEncoded)
	_ = json.Unmarshal(rawBytes, &cursorMap)
	cursorMap["extraField"] = "unexpected"
	tamperedJSON, _ := json.Marshal(cursorMap)
	tamperedEncoded := base64.RawURLEncoding.EncodeToString(tamperedJSON)
	if _, err := c.Resume(context.Background(), receipt, tamperedEncoded); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on unknown field, got %v", err)
	}

	// 4. RunID mismatch
	diffReceipt := receipt
	diffReceipt.RunID = "diff-run"
	if _, err := c.Resume(context.Background(), diffReceipt, cursorEncoded); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on runID mismatch, got %v", err)
	}

	// 5. Receipt mismatch
	tamperedReceipt := receipt
	tamperedReceipt.ApprovalDigest = "diff"
	if _, err := c.Resume(context.Background(), tamperedReceipt, cursorEncoded); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on receipt mismatch, got %v", err)
	}

	// 6. Deadline exceeded on Resume
	clock.now = clock.now.Add(2 * time.Hour)
	if _, err := c.Resume(context.Background(), receipt, cursorEncoded); errorCode(err) != "local_stopped" || reasonOf(err) != "deadline" {
		t.Fatalf("expected deadline error on expired deadline in resume, got %v", err)
	}
	clock.now = clock.now.Add(-2 * time.Hour)

	// 7. Success with refetch = false (PageDigest == "")
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		runRec := s.Runs[run.id]
		if runRec.Cursor != nil {
			runRec.Cursor.PageDigest = ""
		}
		s.Runs[run.id] = runRec
		return nil
	})
	// decode, clear PageDigest, re-encode
	var cs cursorState
	_ = json.Unmarshal(rawBytes, &cs)
	cs.PageDigest = ""
	csBytes, _ := json.Marshal(cs)
	csEncoded := base64.RawURLEncoding.EncodeToString(csBytes)
	resumedRun, err := c.Resume(context.Background(), receipt, csEncoded)
	if err != nil {
		t.Fatalf("Resume failed without page digest: %v", err)
	}
	defer resumedRun.Close()
}

// Not parallel: temporarily replaces package-level hooks (loadRunFn, mutateRunContextFn, ledgerLeaseFn, canonicalJSONFn).
func TestJobs_Reader_Isolated_Hooks(t *testing.T) {
	origLoad := loadRunFn
	origMutate := mutateRunContextFn
	origLease := ledgerLeaseFn
	origCanonical := canonicalJSONFn
	origPreparation := preparationFn
	origReauthorize := reauthorizeFn
	origCall := callFn
	origQueryRequest := queryRequestFn
	defer func() {
		loadRunFn = origLoad
		mutateRunContextFn = origMutate
		ledgerLeaseFn = origLease
		canonicalJSONFn = origCanonical
		preparationFn = origPreparation
		reauthorizeFn = origReauthorize
		callFn = origCall
		queryRequestFn = origQueryRequest
	}()

	l, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	c, _, _, _ := dynamicClient(t, l, "1000")
	prev := previewFor(t, c, "1000")
	app, _ := c.Approve(prev, prev.ApprovalDigest)
	run, err := c.Execute(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()

	job := *run.Receipt().Job

	// 1. Cover default hook closure bodies
	_, _ = origLoad(c, run.id)
	_ = origMutate(c, context.Background(), run.id, func(r *runRecord) error { return nil })
	rel, err := origLease(c.ledger, context.Background(), run.id)
	if err == nil && rel != nil {
		rel()
	}
	_, _, _ = origPreparation(c, context.Background(), &operationScope{}, false)
	_ = origReauthorize(c, context.Background(), ReadPlan{}, "")
	_, _ = origCall(c, context.Background(), &operationScope{}, nil, false, func(api sdkAPI) error { return nil })
	_, _ = origQueryRequest(ReadPlan{}, Execution{"proj", Principal{}, "100", "100"}, Bounds{}, "US", false)

	// 2. Test ledgerLeaseFn error paths
	pLease := previewFor(t, c, "1000")
	appLease, _ := c.Approve(pLease, pLease.ApprovalDigest)
	ledgerLeaseFn = func(l Ledger, ctx context.Context, id string) (func(), error) {
		return nil, errors.New("injected-lease-error")
	}
	// Execute lease error
	if _, err := c.Execute(context.Background(), appLease); err == nil {
		t.Fatal("expected error from ledgerLeaseFn in Execute")
	}
	// Status lease error
	if _, err := c.Status(context.Background(), job); err == nil {
		t.Fatal("expected error from ledgerLeaseFn in Status")
	}
	// CancelJob lease error
	if _, err := c.CancelJob(context.Background(), job); err == nil {
		t.Fatal("expected error from ledgerLeaseFn in CancelJob")
	}
	// Next lease error
	if _, _, err := run.Next(); err == nil {
		t.Fatal("expected error from ledgerLeaseFn in Next")
	}
	// NextPage lease error
	if _, err := run.NextPage(); err == nil {
		t.Fatal("expected error from ledgerLeaseFn in NextPage")
	}
	// Resume lease error
	ledgerLeaseFn = origLease
	cursor, _ := run.Cursor()
	ledgerLeaseFn = func(l Ledger, ctx context.Context, id string) (func(), error) {
		return nil, errors.New("injected-lease-error")
	}
	if _, err := c.Resume(context.Background(), run.Receipt(), cursor); err == nil {
		t.Fatal("expected error from ledgerLeaseFn in Resume")
	}
	ledgerLeaseFn = origLease

	// 3. Test loadRunFn error paths
	loadRunFn = func(c *Client, id string) (runRecord, error) {
		return runRecord{}, errors.New("injected-load-error")
	}
	// Status loadRun error
	if _, err := c.Status(context.Background(), job); err == nil {
		t.Fatal("expected error from loadRunFn in Status")
	}
	// CancelJob loadRun error
	if _, err := c.CancelJob(context.Background(), job); err == nil {
		t.Fatal("expected error from loadRunFn in CancelJob")
	}
	// Cursor loadRun error
	if _, err := run.Cursor(); err == nil {
		t.Fatal("expected error from loadRunFn in Cursor")
	}
	// Next loadRun error
	if _, _, err := run.Next(); err == nil {
		t.Fatal("expected error from loadRunFn in Next")
	}
	// NextPage loadRun error
	if _, err := run.NextPage(); err == nil {
		t.Fatal("expected error from loadRunFn in NextPage")
	}
	// Resume loadRun error
	if _, err := c.Resume(context.Background(), run.Receipt(), cursor); err == nil {
		t.Fatal("expected error from loadRunFn in Resume")
	}
	// Schema loadRun error
	if sch := run.Schema(); sch != nil {
		t.Fatalf("expected nil Schema on load failure, got %v", sch)
	}
	// Receipt loadRun error
	if rec := run.Receipt(); rec.RunID != run.id {
		t.Fatalf("expected RunID on Receipt fallback, got %v", rec)
	}
	// loadPage line 106 error
	if err := run.loadPage(context.Background(), map[string]any{}, nil, false); err == nil {
		t.Fatal("expected error from loadRunFn in loadPage line 106")
	}
	// fetch line 255 error
	if err := run.fetch(nil, false); err == nil {
		t.Fatal("expected error from loadRunFn in fetch line 255")
	}
	// ensure line 294 error
	if err := run.ensure(); err == nil {
		t.Fatal("expected error from loadRunFn in ensure line 294")
	}
	loadRunFn = origLoad

	// Second loadRunFn call errors:
	// loadPage line 236
	loadCount := 0
	loadRunFn = func(c *Client, id string) (runRecord, error) {
		loadCount++
		if loadCount == 2 {
			return runRecord{}, errors.New("second-load-failed")
		}
		return origLoad(c, id)
	}
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"rows":        []any{},
	}, nil, false); err == nil {
		t.Fatal("expected error from second loadRunFn in loadPage line 236")
	}
	loadRunFn = origLoad

	// NextPage line 469
	loadCount = 0
	loadRunFn = func(c *Client, id string) (runRecord, error) {
		loadCount++
		if loadCount >= 3 {
			return runRecord{}, errors.New("subsequent-load-failed")
		}
		return origLoad(c, id)
	}
	run.mode = ""
	run.loaded = true
	run.rows = [][]Cell{{{Type: "INT64", Value: int64(1)}}}
	run.done = false
	run.emptyDelivered = true
	if _, err := run.NextPage(); err == nil {
		t.Fatal("expected error from subsequent loadRunFn in NextPage line 469")
	}
	loadRunFn = origLoad

	// Resume line 516 (second loadRunFn call in Resume)
	loadCount = 0
	loadRunFn = func(c *Client, id string) (runRecord, error) {
		loadCount++
		if loadCount == 2 {
			return runRecord{}, errors.New("second-load-failed")
		}
		return origLoad(c, id)
	}
	if _, err := c.Resume(context.Background(), run.Receipt(), cursor); err == nil {
		t.Fatal("expected error from second loadRunFn in Resume line 516")
	}
	loadRunFn = origLoad

	// Resume line 519 (cursor altered after lease)
	loadCount = 0
	loadRunFn = func(c *Client, id string) (runRecord, error) {
		rec, err := origLoad(c, id)
		loadCount++
		if loadCount == 2 {
			rec.Cursor = nil
		}
		return rec, err
	}
	if _, err := c.Resume(context.Background(), run.Receipt(), cursor); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on changed cursor in Resume, got %v", err)
	}
	loadRunFn = origLoad

	// 4. Test mutateRunContextFn error paths
	pMut := previewFor(t, c, "1000")
	appMut, _ := c.Approve(pMut, pMut.ApprovalDigest)
	mutateRunContextFn = func(c *Client, ctx context.Context, id string, fn func(*runRecord) error) error {
		return errors.New("injected-mutate-error")
	}
	// Execute mutate error
	if _, err := c.Execute(context.Background(), appMut); err == nil {
		t.Fatal("expected error from mutateRunContextFn in Execute")
	}
	// Status mutate error
	if _, err := c.Status(context.Background(), job); err == nil {
		t.Fatal("expected error from mutateRunContextFn in Status")
	}
	// CancelJob mutate error
	if _, err := c.CancelJob(context.Background(), job); err == nil {
		t.Fatal("expected error from mutateRunContextFn in CancelJob")
	}
	// loadPage mutate error
	if err := run.loadPage(context.Background(), map[string]any{"jobComplete": true, "rows": []any{}}, nil, false); err == nil {
		t.Fatal("expected error from mutateRunContextFn in loadPage")
	}
	// commitDelivery in Next line 426
	runNextFail := newRun(c, context.Background(), run.id)
	runNextFail.loaded = true
	runNextFail.rows = [][]Cell{{{Type: "INT64", Value: int64(1)}}}
	runNextFail.index = 0
	if _, _, err := runNextFail.Next(); err == nil {
		t.Fatal("expected commitDelivery error in Next line 426")
	}
	// commitDelivery in NextPage line 478
	runNextPageFail := newRun(c, context.Background(), run.id)
	runNextPageFail.loaded = true
	runNextPageFail.rows = [][]Cell{{{Type: "INT64", Value: int64(1)}}}
	runNextPageFail.index = 0
	if _, err := runNextPageFail.NextPage(); err == nil {
		t.Fatal("expected commitDelivery error in NextPage line 478")
	}
	// commitDelivery / Resume mutate error line 536
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		runRec := s.Runs[run.id]
		if runRec.Cursor != nil {
			runRec.Cursor.PageDigest = ""
		}
		s.Runs[run.id] = runRec
		return nil
	})
	rawBytes, _ := base64.RawURLEncoding.DecodeString(cursor)
	var cs cursorState
	_ = json.Unmarshal(rawBytes, &cs)
	cs.PageDigest = ""
	csBytes, _ := json.Marshal(cs)
	csEncoded := base64.RawURLEncoding.EncodeToString(csBytes)
	if _, err := c.Resume(context.Background(), run.Receipt(), csEncoded); err == nil {
		t.Fatal("expected error from mutateRunContextFn in Resume line 536")
	}
	mutateRunContextFn = origMutate

	// 5. Test canonicalJSONFn error path in loadPage
	canonicalJSONFn = func(b []byte) ([]byte, error) {
		return nil, errors.New("injected-canonical-error")
	}
	if err := run.loadPage(context.Background(), map[string]any{"jobComplete": true, "rows": []any{}}, nil, false); err == nil {
		t.Fatal("expected error from canonicalJSONFn in loadPage")
	}
	canonicalJSONFn = origCanonical

	// 6. NextPage preparation error (line 443)
	cancellableCtx, cancelNextPage := context.WithCancel(context.Background())
	cancelNextPage()
	runNextPageCanceled := newRun(c, cancellableCtx, run.id)
	if _, err := runNextPageCanceled.NextPage(); err == nil {
		t.Fatal("expected preparation error in NextPage line 443")
	}

	// 7. NextPage ensure error (line 453)
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Receipt.ExecutionDeadline = time.Now().Add(-time.Hour)
		s.Runs[run.id] = rec
		return nil
	})
	runNextPageEnsureFail := newRun(c, context.Background(), run.id)
	if _, err := runNextPageEnsureFail.NextPage(); err == nil {
		t.Fatal("expected ensure error in NextPage line 453")
	}
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Receipt.ExecutionDeadline = time.Now().Add(time.Hour)
		s.Runs[run.id] = rec
		return nil
	})

	// 8. Resume preparation error (line 523)
	cancellableCtxResume, cancelResume := context.WithCancel(context.Background())
	cancelResume()
	if _, err := c.Resume(cancellableCtxResume, run.Receipt(), cursor); err == nil {
		t.Fatal("expected preparation error in Resume line 523")
	}

	freshClient := func() (*Client, Approval) {
		lf, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
		cf, _, _, _ := dynamicClient(t, lf, "1000000")
		pf := previewFor(t, cf, "1000")
		af, _ := cf.Approve(pf, pf.ApprovalDigest)
		return cf, af
	}

	// 9. Execute preparationFn error (first call line 147, second call line 178)
	prepCount := 0
	preparationFn = func(c *Client, ctx context.Context, scope *operationScope, control bool) (context.Context, context.CancelFunc, error) {
		prepCount++
		if prepCount == 1 {
			return nil, nil, errors.New("prep1-err")
		}
		return origPreparation(c, ctx, scope, control)
	}
	cPrep1, appPrep1 := freshClient()
	if _, err := cPrep1.Execute(context.Background(), appPrep1); err == nil || err.Error() != "prep1-err" {
		t.Fatalf("expected prep1-err, got %v", err)
	}

	prepCount = 0
	preparationFn = func(c *Client, ctx context.Context, scope *operationScope, control bool) (context.Context, context.CancelFunc, error) {
		prepCount++
		if prepCount == 2 {
			return nil, nil, errors.New("prep2-err")
		}
		return origPreparation(c, ctx, scope, control)
	}
	cPrep2, appPrep2 := freshClient()
	if _, err := cPrep2.Execute(context.Background(), appPrep2); err == nil || err.Error() != "prep2-err" {
		t.Fatalf("expected prep2-err, got %v", err)
	}
	preparationFn = origPreparation

	// 10. Execute reauthorizeFn error on second call (line 183)
	reauthCount := 0
	reauthorizeFn = func(c *Client, ctx context.Context, plan ReadPlan, policy string) error {
		reauthCount++
		if reauthCount == 2 {
			return errors.New("reauth2-err")
		}
		return origReauthorize(c, ctx, plan, policy)
	}
	cReauth, appReauth := freshClient()
	if _, err := cReauth.Execute(context.Background(), appReauth); err == nil || err.Error() != "reauth2-err" {
		t.Fatalf("expected reauth2-err, got %v", err)
	}
	reauthorizeFn = origReauthorize

	// 11. Execute queryRequestFn error (line 187)
	queryRequestFn = func(plan ReadPlan, execution Execution, bounds Bounds, location string, dry bool) (*bq.QueryRequest, error) {
		return nil, errors.New("query-req-err")
	}
	cQuery, appQuery := freshClient()
	if _, err := cQuery.Execute(context.Background(), appQuery); err == nil || err.Error() != "query-req-err" {
		t.Fatalf("expected query-req-err, got %v", err)
	}
	queryRequestFn = origQueryRequest

	// 12. Execute mutateRunContextFn error on submitting (line 191)
	mutateRunContextFn = func(c *Client, ctx context.Context, id string, fn func(*runRecord) error) error {
		var rec runRecord
		_ = fn(&rec)
		if rec.Receipt.State == "submitting" {
			return errors.New("mutate-submitting-err")
		}
		return origMutate(c, ctx, id, fn)
	}
	cMutSub, appMutSub := freshClient()
	if _, err := cMutSub.Execute(context.Background(), appMutSub); err == nil || err.Error() != "mutate-submitting-err" {
		t.Fatalf("expected mutate-submitting-err, got %v", err)
	}
	mutateRunContextFn = origMutate

	// 13. Execute callFn error before dispatch (lines 204-206)
	callFn = func(c *Client, ctx context.Context, s *operationScope, principal *Principal, control bool, fn func(sdkAPI) error) (any, error) {
		return nil, errors.New("call-before-dispatch-err")
	}
	cCallFail, appCallFail := freshClient()
	runCallFail, err := cCallFail.Execute(context.Background(), appCallFail)
	if err == nil || err.Error() != "call-before-dispatch-err" {
		t.Fatalf("expected call-before-dispatch-err, got %v", err)
	}
	recCallFail, _ := cCallFail.loadRun(runCallFail.id)
	if recCallFail.Receipt.State != "failed" || recCallFail.Reservation != 0 {
		t.Fatalf("expected state=failed, reservation=0, got state=%s reservation=%d", recCallFail.Receipt.State, recCallFail.Reservation)
	}
	callFn = origCall

	// 14. Execute object(raw) error (line 216)
	callFn = func(c *Client, ctx context.Context, s *operationScope, principal *Principal, control bool, fn func(sdkAPI) error) (any, error) {
		return 12345, nil
	}
	cRawBad, appRawBad := freshClient()
	if _, err := cRawBad.Execute(context.Background(), appRawBad); errorCode(err) != "malformed_wire" && errorCode(err) != "invalid_input" {
		t.Fatalf("expected malformed_wire or invalid_input, got %v", err)
	}
	callFn = origCall

	// 15. Status object(raw) error (line 286)
	callFn = func(c *Client, ctx context.Context, s *operationScope, principal *Principal, control bool, fn func(sdkAPI) error) (any, error) {
		if s.controlDeadline != (time.Time{}) {
			return 12345, nil
		}
		return origCall(c, ctx, s, principal, control, fn)
	}
	if _, err := c.Status(context.Background(), job); errorCode(err) != "malformed_wire" && errorCode(err) != "invalid_input" {
		t.Fatalf("expected malformed_wire or invalid_input, got %v", err)
	}
	callFn = origCall

	// 16. Status object(x) error for errorResult (line 309)
	callFn = func(c *Client, ctx context.Context, s *operationScope, principal *Principal, control bool, fn func(sdkAPI) error) (any, error) {
		return map[string]any{
			"jobReference": map[string]any{"projectId": job.ProjectID, "jobId": job.JobID, "location": job.Location},
			"status": map[string]any{
				"state":       "DONE",
				"errorResult": "not-a-map",
			},
		}, nil
	}
	if _, err := c.Status(context.Background(), job); errorCode(err) != "malformed_wire" && errorCode(err) != "invalid_input" {
		t.Fatalf("expected malformed_wire or invalid_input, got %v", err)
	}
	callFn = origCall

	// 17. Status object(statistics) error (line 326)
	callFn = func(c *Client, ctx context.Context, s *operationScope, principal *Principal, control bool, fn func(sdkAPI) error) (any, error) {
		return map[string]any{
			"jobReference": map[string]any{"projectId": job.ProjectID, "jobId": job.JobID, "location": job.Location},
			"status":       map[string]any{"state": "DONE"},
			"statistics":   "not-a-map",
		}, nil
	}
	if _, err := c.Status(context.Background(), job); errorCode(err) != "malformed_wire" && errorCode(err) != "invalid_input" {
		t.Fatalf("expected malformed_wire or invalid_input, got %v", err)
	}
	callFn = origCall

	// 18. Status object(query) error in statistics (line 331)
	callFn = func(c *Client, ctx context.Context, s *operationScope, principal *Principal, control bool, fn func(sdkAPI) error) (any, error) {
		return map[string]any{
			"jobReference": map[string]any{"projectId": job.ProjectID, "jobId": job.JobID, "location": job.Location},
			"status":       map[string]any{"state": "DONE"},
			"statistics": map[string]any{
				"query": "not-a-map",
			},
		}, nil
	}
	if _, err := c.Status(context.Background(), job); errorCode(err) != "malformed_wire" && errorCode(err) != "invalid_input" {
		t.Fatalf("expected malformed_wire or invalid_input, got %v", err)
	}
	callFn = origCall

	// 19. CancelJob preparationFn error (line 374)
	preparationFn = func(c *Client, ctx context.Context, scope *operationScope, control bool) (context.Context, context.CancelFunc, error) {
		if control {
			return nil, nil, errors.New("cancel-prep-err")
		}
		return origPreparation(c, ctx, scope, control)
	}
	if _, err := c.CancelJob(context.Background(), job); err == nil || err.Error() != "cancel-prep-err" {
		t.Fatalf("expected cancel-prep-err, got %v", err)
	}
	preparationFn = origPreparation

	// 20. CancelJob object(raw) error (line 394)
	callFn = func(c *Client, ctx context.Context, s *operationScope, principal *Principal, control bool, fn func(sdkAPI) error) (any, error) {
		if control {
			return 12345, nil
		}
		return origCall(c, ctx, s, principal, control, fn)
	}
	resCancelRaw, err := c.CancelJob(context.Background(), job)
	if errorCode(err) != "cancellation_unknown" || resCancelRaw.State != "unknown" {
		t.Fatalf("expected cancellation_unknown, got res=%v, err=%v", resCancelRaw, err)
	}
	callFn = origCall

	// 21. reader fetch preparationFn error (reader.go:260)
	preparationFn = func(c *Client, ctx context.Context, scope *operationScope, control bool) (context.Context, context.CancelFunc, error) {
		if scope.runID != "" && !control {
			return nil, nil, errors.New("fetch-prep-err")
		}
		return origPreparation(c, ctx, scope, control)
	}
	if err := run.fetch(nil, false); err == nil || err.Error() != "fetch-prep-err" {
		t.Fatalf("expected fetch-prep-err, got %v", err)
	}
	preparationFn = origPreparation

	// 22. reader fetch object(raw) error (reader.go:286)
	callFn = func(c *Client, ctx context.Context, s *operationScope, principal *Principal, control bool, fn func(sdkAPI) error) (any, error) {
		if s.runID != "" && s.result {
			return 12345, nil
		}
		return origCall(c, ctx, s, principal, control, fn)
	}
	if err := run.fetch(nil, false); errorCode(err) != "malformed_wire" && errorCode(err) != "invalid_input" {
		t.Fatalf("expected malformed_wire or invalid_input, got %v", err)
	}
	callFn = origCall

	// 23. reader ensure preparationFn error (reader.go:313)
	preparationFn = func(c *Client, ctx context.Context, scope *operationScope, control bool) (context.Context, context.CancelFunc, error) {
		return nil, nil, errors.New("ensure-prep-err")
	}
	if err := run.ensure(); err == nil || err.Error() != "ensure-prep-err" {
		t.Fatalf("expected ensure-prep-err, got %v", err)
	}
	preparationFn = origPreparation

	// 24. reader ensure deadline expired after reauthorize (reader.go:324)
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Receipt.ExecutionDeadline = c.clock.Now().Add(10 * time.Millisecond)
		s.Runs[run.id] = rec
		return nil
	})
	preparationFn = func(c *Client, ctx context.Context, scope *operationScope, control bool) (context.Context, context.CancelFunc, error) {
		bounded, cancel := context.WithTimeout(ctx, time.Hour)
		bounded = context.WithValue(bounded, clockBoundKey{}, c.clock.Now().Add(time.Hour))
		return bounded, cancel, nil
	}
	reauthorizeFn = func(c *Client, ctx context.Context, plan ReadPlan, policy string) error {
		if fc, ok := c.clock.(*fakeClock); ok {
			fc.advance(20 * time.Millisecond)
		}
		return nil
	}
	runDeadline := newRun(c, context.Background(), run.id)
	runDeadline.access = run.access
	if err := runDeadline.ensure(); errorCode(err) != "deadline_exceeded" && errorCode(err) != "local_stopped" {
		t.Fatalf("expected deadline_exceeded or local_stopped, got %v", err)
	}
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Receipt.ExecutionDeadline = c.clock.Now().Add(time.Hour)
		s.Runs[run.id] = rec
		return nil
	})
	reauthorizeFn = origReauthorize
	preparationFn = origPreparation

	// 25. reader ensure sleep error when !loaded (reader.go:343-345)
	_ = c.mutateRunContext(context.Background(), run.id, func(rr *runRecord) error {
		rr.Schema = nil
		return nil
	})
	callFn = func(c *Client, ctx context.Context, s *operationScope, principal *Principal, control bool, fn func(sdkAPI) error) (any, error) {
		return map[string]any{
			"jobReference": map[string]any{"projectId": job.ProjectID, "jobId": job.JobID, "location": job.Location},
			"jobComplete":  false,
			"rows":         []any{},
		}, nil
	}
	cancelEnsureCtx, cancelEnsure := context.WithCancel(context.Background())
	cancelEnsure()
	runEnsureCanceled := newRun(c, cancelEnsureCtx, run.id)
	if err := runEnsureCanceled.ensure(); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on canceled sleep in ensure, got %v", err)
	}
	callFn = origCall
}

func TestJobs_Reader_RemainingEdgeCases(t *testing.T) {
	t.Parallel()

	l, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	c, _, clock, _ := dynamicClient(t, l, "1000")
	prev := previewFor(t, c, "1000")
	app, _ := c.Approve(prev, prev.ApprovalDigest)
	run, err := c.Execute(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()

	// 1. loadPage when record.Receipt.Job == nil
	noJobRun := newRun(c, context.Background(), "no-job-run")
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		s.Runs["no-job-run"] = runRecord{Receipt: Receipt{RunID: "no-job-run"}}
		return nil
	})
	if err := noJobRun.loadPage(context.Background(), map[string]any{"jobComplete": true}, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on nil job, got %v", err)
	}

	// 2. loadPage decodeSchema error
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"schema":      "not-a-map",
	}, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on bad schema, got %v", err)
	}

	// 3. loadPage schema_changed (unexpected field)
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"schema": map[string]any{
			"fields": []any{
				map[string]any{"name": "unexpected_field", "type": "STRING", "mode": "NULLABLE"},
			},
		},
	}, nil, false); errorCode(err) != "schema_changed" {
		t.Fatalf("expected schema_changed, got %v", err)
	}

	// 4. loadPage with len(fs) == 0 edge cases
	noSchemaRun := newRun(c, context.Background(), "no-schema-run")
	job := *run.Receipt().Job
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		s.Runs["no-schema-run"] = runRecord{
			Receipt: Receipt{RunID: "no-schema-run", Job: &job},
			Schema:  nil,
		}
		return nil
	})
	// 4a. rawRows not a slice
	if err := noSchemaRun.loadPage(context.Background(), map[string]any{
		"jobComplete": false,
		"rows":        "not-a-slice",
	}, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}
	// 4b. rawRows has len > 0
	if err := noSchemaRun.loadPage(context.Background(), map[string]any{
		"jobComplete": false,
		"rows":        []any{map[string]any{"f": []any{map[string]any{"v": "1"}}}},
	}, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}
	// 4c. complete == true with empty schema
	if err := noSchemaRun.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"rows":        []any{},
	}, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire, got %v", err)
	}
	// 4d. empty slice and complete == false -> sets r.loaded = false, returns nil
	if err := noSchemaRun.loadPage(context.Background(), map[string]any{
		"jobComplete": false,
		"rows":        []any{},
	}, nil, false); err != nil {
		t.Fatalf("expected nil on empty rows and not complete, got %v", err)
	}
	if noSchemaRun.loaded || noSchemaRun.done {
		t.Fatal("expected loaded and done to be false")
	}

	// 5. loadPage json.Marshal(rawRows) error
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"rows":        []any{math.NaN()},
	}, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on NaN in rawRows, got %v", err)
	}

	// 6. loadPage DecodeRows error (cell type mismatch)
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"rows": []any{
			map[string]any{"f": []any{map[string]any{"v": "not-an-integer"}}},
		},
	}, nil, false); errorCode(err) != "unsupported_value" {
		t.Fatalf("expected unsupported_value on DecodeRows type mismatch, got %v", err)
	}

	// 7. loadPage response_limit on rows > Bounds.PageSize
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Receipt.Bounds.PageSize = 1
		s.Runs[run.id] = rec
		return nil
	})
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"rows": []any{
			map[string]any{"f": []any{map[string]any{"v": "1"}}},
			map[string]any{"f": []any{map[string]any{"v": "2"}}},
		},
	}, nil, false); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit on exceeding PageSize, got %v", err)
	}
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Receipt.Bounds.PageSize = 100
		s.Runs[run.id] = rec
		return nil
	})

	// 8. pageToken invalid string
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"pageToken":   "",
	}, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on empty pageToken, got %v", err)
	}
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"pageToken":   strings.Repeat("p", 16385),
	}, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on oversized pageToken, got %v", err)
	}

	// 9. cacheHit invalid type
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"cacheHit":    "not-a-bool",
	}, nil, false); errorCode(err) != "malformed_wire" {
		t.Fatalf("expected malformed_wire on invalid cacheHit, got %v", err)
	}

	// 10. refetch validation
	// First load valid page to set PageDigest
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"rows":        []any{},
	}, nil, false); err != nil {
		t.Fatal(err)
	}
	// 10a. refetch digest mismatch
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.PageDigest = "different-digest"
		s.Runs[run.id] = rec
		return nil
	})
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"rows":        []any{},
	}, nil, true); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on refetch digest mismatch, got %v", err)
	}
	// 10b. refetch nextToken mismatch or complete mismatch
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.PageDigest = hashBytes([]byte("[]"))
		tok := "tok1"
		rec.NextToken = &tok
		rec.PageDone = true
		s.Runs[run.id] = rec
		return nil
	})
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"pageToken":   "tok2", // mismatch
		"rows":        []any{},
	}, nil, true); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on refetch nextToken mismatch, got %v", err)
	}

	// 11. SeenTokens duplicate
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.SeenTokens = map[string]bool{"seen-tok": true}
		s.Runs[run.id] = rec
		return nil
	})
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"pageToken":   "seen-tok",
		"rows":        []any{},
	}, nil, false); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on seen token, got %v", err)
	}
	// nextToken == currentToken
	curTok := "same-tok"
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"pageToken":   curTok,
		"rows":        []any{},
	}, &curTok, false); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on next token equals current token, got %v", err)
	}

	// 12. record.Receipt.Counters.Pages > Bounds.MaxPages
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Receipt.Counters.Pages = 10
		rec.Receipt.Bounds.MaxPages = 5
		rec.SeenTokens = map[string]bool{}
		s.Runs[run.id] = rec
		return nil
	})
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"rows":        []any{},
	}, nil, false); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit on Pages > MaxPages, got %v", err)
	}

	// 13. r.index > len(rows) in loadPage
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Receipt.Counters.Pages = 1
		rec.Receipt.Bounds.MaxPages = 10
		rec.Offset = 5
		rec.PageDigest = hashBytes([]byte("[]"))
		rec.NextToken = nil
		rec.PageDone = true
		s.Runs[run.id] = rec
		return nil
	})
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"rows":        []any{},
	}, nil, true); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on offset > len(rows), got %v", err)
	}

	// 14. fetch: rr.Receipt.Job == nil
	noJobRun2 := newRun(c, context.Background(), "no-job-fetch")
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		s.Runs["no-job-fetch"] = runRecord{
			Receipt: Receipt{
				RunID:             "no-job-fetch",
				Bounds:            DefaultBounds(),
				ExecutionDeadline: clock.Now().Add(time.Hour),
				Principal:         run.access,
			},
			ActivePrincipal: run.access,
			Preview:         prev,
		}
		return nil
	})
	if err := noJobRun2.fetch(nil, false); errorCode(err) != "submission_unknown" {
		t.Fatalf("expected submission_unknown on nil job in fetch, got %v", err)
	}

	// 15. ensure: Counters.Rows >= Bounds.MaxRows
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Receipt.Counters.Rows = 1000
		rec.Receipt.Bounds.MaxRows = 1000
		s.Runs[run.id] = rec
		return nil
	})
	if err := run.ensure(); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit on Rows >= MaxRows, got %v", err)
	}

	// 16. ensure: r.access != rr.ActivePrincipal
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Receipt.Counters.Rows = 0
		rec.ActivePrincipal = Principal{Kind: "changed", Subject: "different", Generation: "9"}
		s.Runs[run.id] = rec
		return nil
	})
	if err := run.ensure(); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed on principal mismatch in ensure, got %v", err)
	}

	// 17. ensure: r.loaded && r.index != rr.Offset
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.ActivePrincipal = run.access
		rec.Offset = 99
		s.Runs[run.id] = rec
		return nil
	})
	run.loaded = true
	run.index = 0
	run.rows = [][]Cell{{{Type: "STRING", Value: "x"}}}
	if err := run.ensure(); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on r.index != Offset, got %v", err)
	}

	// 18. commitDelivery: r.access != rr.ActivePrincipal
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.ActivePrincipal = Principal{Kind: "changed"}
		s.Runs[run.id] = rec
		return nil
	})
	if _, err := run.commitDelivery(1); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed in commitDelivery, got %v", err)
	}

	// 19. commitDelivery: n < 0 or n > MaxRows - Rows
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.ActivePrincipal = run.access
		rec.Receipt.Counters.Rows = 99
		rec.Receipt.Bounds.MaxRows = 100
		s.Runs[run.id] = rec
		return nil
	})
	if _, err := run.commitDelivery(-1); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit on negative n in commitDelivery, got %v", err)
	}
	if _, err := run.commitDelivery(5); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit on exceeding remaining rows in commitDelivery, got %v", err)
	}

	// 20. Next mode == "page", NextPage mode == "row"
	run.mode = "page"
	if _, _, err := run.Next(); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input calling Next in page mode, got %v", err)
	}
	run.mode = "row"
	if _, err := run.NextPage(); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input calling NextPage in row mode, got %v", err)
	}

	// 21. NextPage empty results delivery
	run.mode = ""
	run.loaded = true
	run.rows = [][]Cell{}
	run.emptyDelivered = false
	run.done = true
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Offset = 0
		rec.Receipt.Counters.Rows = 0
		s.Runs[run.id] = rec
		return nil
	})
	p, err := run.NextPage()
	if err != nil {
		t.Fatalf("NextPage empty results failed: %v", err)
	}
	if len(p.Rows) != 0 || !run.emptyDelivered {
		t.Fatal("expected empty delivered page")
	}

	// 22. NextPage n clamped to remaining rows (n > rem)
	run.mode = ""
	run.loaded = true
	run.rows = [][]Cell{{{Type: "INT64", Value: int64(1)}}, {{Type: "INT64", Value: int64(2)}}, {{Type: "INT64", Value: int64(3)}}}
	run.index = 0
	run.done = false
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Offset = 0
		rec.Receipt.Counters.Rows = 9
		rec.Receipt.Bounds.MaxRows = 10 // remaining is 1
		s.Runs[run.id] = rec
		return nil
	})
	pClamped, err := run.NextPage()
	if err != nil {
		t.Fatalf("NextPage clamped failed: %v", err)
	}
	if len(pClamped.Rows) != 1 {
		t.Fatalf("expected 1 clamped row, got %d", len(pClamped.Rows))
	}

	// 23. Status error handling
	var profile SourceProfile
	for _, p := range c.profiles {
		profile = p
		break
	}
	plan, _, _ := c.prepare(context.Background())
	identity := &testProvider{identity: Identity{Principal{"workload", "operator:fixture", "1"}, clock.Now().Add(time.Hour), true, true}}

	// 23a. Non-object errorResult
	transportStatus1 := rt(func(req *http.Request) (*http.Response, error) {
		if req.Method == "GET" && strings.Contains(req.URL.Path, "/jobs/") {
			return response(`{
				"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},
				"status":{"state":"DONE","errorResult":123}
			}`), nil
		}
		return response(`{}`), nil
	})
	cStatus1, _ := NewClient(Config{[]SourceProfile{profile}, identity, transportStatus1, l, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	if _, err := cStatus1.Status(context.Background(), job); err == nil {
		t.Fatalf("expected error on non-object errorResult, got nil")
	} else if errorCode(err) != "invalid_input" && errorCode(err) != "malformed_wire" {
		t.Fatalf("23a got err=%v (code=%s)", err, errorCode(err))
	}

	// 23b. Non-object statistics
	transportStatus2 := rt(func(req *http.Request) (*http.Response, error) {
		if req.Method == "GET" && strings.Contains(req.URL.Path, "/jobs/") {
			return response(`{
				"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},
				"status":{"state":"DONE"},
				"statistics":"not-an-object"
			}`), nil
		}
		return response(`{}`), nil
	})
	cStatus2, _ := NewClient(Config{[]SourceProfile{profile}, identity, transportStatus2, l, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	if _, err := cStatus2.Status(context.Background(), job); err == nil {
		t.Fatalf("expected error on non-object statistics, got nil")
	} else if errorCode(err) != "invalid_input" && errorCode(err) != "malformed_wire" {
		t.Fatalf("expected invalid_input or malformed_wire on non-object statistics, got %v", err)
	}

	// 23c. Non-object query
	transportStatus3 := rt(func(req *http.Request) (*http.Response, error) {
		if req.Method == "GET" && strings.Contains(req.URL.Path, "/jobs/") {
			return response(`{
				"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},
				"status":{"state":"DONE"},
				"statistics":{"query":"not-an-object"}
			}`), nil
		}
		return response(`{}`), nil
	})
	cStatus3, _ := NewClient(Config{[]SourceProfile{profile}, identity, transportStatus3, l, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	if _, err := cStatus3.Status(context.Background(), job); err == nil {
		t.Fatalf("expected error on non-object query, got nil")
	} else if errorCode(err) != "invalid_input" && errorCode(err) != "malformed_wire" {
		t.Fatalf("expected invalid_input or malformed_wire on non-object query, got %v", err)
	}

	// 23d. Invalid totalBytesBilled
	transportStatus4 := rt(func(req *http.Request) (*http.Response, error) {
		if req.Method == "GET" && strings.Contains(req.URL.Path, "/jobs/") {
			return response(`{
				"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},
				"status":{"state":"DONE"},
				"statistics":{"query":{"totalBytesBilled":"not-a-number"}}
			}`), nil
		}
		return response(`{}`), nil
	})
	cStatus4, _ := NewClient(Config{[]SourceProfile{profile}, identity, transportStatus4, l, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	if _, err := cStatus4.Status(context.Background(), job); err == nil {
		t.Fatalf("expected error on non-decimal totalBytesBilled, got nil")
	} else if errorCode(err) != "malformed_wire" && errorCode(err) != "invalid_input" {
		t.Fatalf("expected malformed_wire or invalid_input on non-decimal totalBytesBilled, got %v", err)
	}

	// 24. CancelJob error handling
	// 24a. Non-object job in cancel response
	transportCancel1 := rt(func(req *http.Request) (*http.Response, error) {
		if req.Method == "POST" && strings.Contains(req.URL.Path, "/cancel") {
			return response(`{"job":"not-an-object"}`), nil
		}
		return response(`{}`), nil
	})
	cCancel1, _ := NewClient(Config{[]SourceProfile{profile}, identity, transportCancel1, l, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	if _, err := cCancel1.CancelJob(context.Background(), job); err == nil {
		t.Fatalf("expected error on non-object job, got nil")
	} else if errorCode(err) != "cancellation_unknown" && errorCode(err) != "invalid_input" {
		t.Fatalf("expected cancellation_unknown or invalid_input on non-object job, got err=%v (%#v)", err, err)
	}

	// 24b. Invalid jobReference in cancel response
	transportCancel2 := rt(func(req *http.Request) (*http.Response, error) {
		if req.Method == "POST" && strings.Contains(req.URL.Path, "/cancel") {
			return response(`{"job":{"jobReference":"not-a-map"}}`), nil
		}
		return response(`{}`), nil
	})
	cCancel2, _ := NewClient(Config{[]SourceProfile{profile}, identity, transportCancel2, l, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	if _, err := cCancel2.CancelJob(context.Background(), job); err == nil {
		t.Fatalf("expected error on bad jobReference in cancel, got nil")
	} else if errorCode(err) != "cancellation_unknown" && errorCode(err) != "invalid_input" {
		t.Fatalf("expected cancellation_unknown or invalid_input on bad jobReference in cancel, got %v", err)
	}

	// 25. Cursor when rr.Cursor == nil
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Cursor = nil
		s.Runs[run.id] = rec
		return nil
	})
	if _, err := run.Cursor(); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid when Cursor is nil, got %v", err)
	}

	// 26. Sleep error when !r.loaded in ensure
	canceledCtx, cancelFn := context.WithCancel(context.Background())
	cancelFn() // immediately canceled
	runSleep := newRun(c, canceledCtx, run.id)
	runSleep.loaded = false
	if err := runSleep.ensure(); err == nil {
		t.Fatal("expected error on canceled context in ensure sleep")
	}

	// 27. loadPage cacheHit: true
	if err := run.loadPage(context.Background(), map[string]any{
		"jobComplete": true,
		"rows":        []any{},
		"cacheHit":    true,
	}, nil, false); err != nil {
		t.Fatal(err)
	}

	// 28. fetch preparation error (line 260)
	runFetchCanceled := newRun(c, canceledCtx, run.id)
	if err := runFetchCanceled.fetch(nil, false); err == nil {
		t.Fatal("expected preparation error in fetch")
	}

	// 29. fetch object(raw) error (line 286)
	tFetchBad := rt(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/queries/") {
			return response(`123`), nil
		}
		return response(`{}`), nil
	})
	cFetchBad, _ := NewClient(Config{[]SourceProfile{profile}, identity, tFetchBad, l, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	runFetchBad := newRun(cFetchBad, context.Background(), run.id)
	if err := runFetchBad.fetch(nil, false); err == nil {
		t.Fatal("expected error on non-object in fetch line 286")
	}

	// 30. ensure r.ctx.Err() != nil (line 294)
	runEnsureCanceled := newRun(c, canceledCtx, run.id)
	if err := runEnsureCanceled.ensure(); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on canceled ctx in ensure, got %v", err)
	}

	// 31. ensure deadline expired (line 324) and commitDelivery deadline expired (line 353)
	runClock := newRun(c, context.Background(), run.id)
	runClock.loaded = true
	runClock.index = 0
	runClock.rows = [][]Cell{{{Type: "INT64", Value: int64(1)}}}
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Offset = 0
		rec.Receipt.ExecutionDeadline = time.Now().Add(-time.Hour)
		s.Runs[run.id] = rec
		return nil
	})
	if err := runClock.ensure(); errorCode(err) != "local_stopped" || reasonOf(err) != "deadline" {
		t.Fatalf("expected deadline in ensure line 324, got %v", err)
	}
	if _, err := runClock.commitDelivery(1); errorCode(err) != "local_stopped" || reasonOf(err) != "deadline" {
		t.Fatalf("expected deadline in commitDelivery line 353, got %v", err)
	}
	_ = c.ledger.updateContext(context.Background(), func(s *ledgerState) error {
		rec := s.Runs[run.id]
		rec.Receipt.ExecutionDeadline = time.Now().Add(time.Hour)
		s.Runs[run.id] = rec
		return nil
	})

	// 32. Status findJob error (line 250)
	if _, err := c.Status(context.Background(), JobRef{ProjectID: "proj", JobID: "not-found", Location: "EU"}); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on unknown job in Status, got %v", err)
	}

	// 33. CancelJob findJob error (line 353)
	if _, err := c.CancelJob(context.Background(), JobRef{ProjectID: "proj", JobID: "not-found", Location: "EU"}); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid on unknown job in CancelJob, got %v", err)
	}

	// 34. CancelJob preparation error (line 367)
	if _, err := c.CancelJob(canceledCtx, job); err == nil {
		t.Fatal("expected preparation error on canceled context in CancelJob line 367")
	}

	// 35. CancelJob object(raw) error (line 387)
	transportCancelRawBad := rt(func(req *http.Request) (*http.Response, error) {
		if req.Method == "POST" && strings.Contains(req.URL.Path, "/cancel") {
			return response(`123`), nil
		}
		return response(`{}`), nil
	})
	cCancelRawBad, _ := NewClient(Config{[]SourceProfile{profile}, identity, transportCancelRawBad, l, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	if _, err := cCancelRawBad.CancelJob(context.Background(), job); errorCode(err) != "cancellation_unknown" && errorCode(err) != "invalid_input" {
		t.Fatalf("expected cancellation_unknown or invalid_input in CancelJob line 387, got %v", err)
	}

	// 36. Status object(raw) error (line 279)
	transportStatusRawBad := rt(func(req *http.Request) (*http.Response, error) {
		if req.Method == "GET" && strings.Contains(req.URL.Path, "/jobs/") {
			return response(`123`), nil
		}
		return response(`{}`), nil
	})
	cStatusRawBad, _ := NewClient(Config{[]SourceProfile{profile}, identity, transportStatusRawBad, l, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	if _, err := cStatusRawBad.Status(context.Background(), job); err == nil {
		t.Fatalf("expected error on non-object raw in Status, got nil")
	} else if errorCode(err) != "malformed_wire" && errorCode(err) != "invalid_input" {
		t.Fatalf("expected malformed_wire or invalid_input in Status line 279, got %v", err)
	}

	// 37. Execute preparation error (line 140)
	lPrep, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	cPrep, _, _, _ := dynamicClient(t, lPrep, "1000")
	pPrep := previewFor(t, cPrep, "1000")
	appPrep, _ := cPrep.Approve(pPrep, pPrep.ApprovalDigest)
	if _, err := cPrep.Execute(canceledCtx, appPrep); err == nil {
		t.Fatal("expected preparation error on canceled ctx in Execute line 140")
	}

	// 38. Execute estimate error (line 153)
	failEstimate := false
	tEst := rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/ds") {
			return response(`{"datasetReference":{"projectId":"source-project","datasetId":"ds"},"location":"EU"}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
			return response(`{"tableReference":{"projectId":"source-project","datasetId":"ds","tableId":"tbl"},"type":"TABLE","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`), nil
		}
		if req.Method == "POST" {
			var m map[string]any
			if req.Body != nil {
				_ = json.NewDecoder(req.Body).Decode(&m)
			}
			if m["dryRun"] == true {
				if failEstimate {
					return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader(`{"error":{"code":500,"message":"dryrun failed"}}`)), Header: http.Header{"Content-Type": {"application/json"}}}, nil
				}
				return response(`{"totalBytesProcessed":"100"}`), nil
			}
			return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"rows":[]}`), nil
		}
		return response(`{}`), nil
	})
	lEst, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	cEst, _ := NewClient(Config{[]SourceProfile{profile}, identity, tEst, lEst, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	pEst := previewFor(t, cEst, "1000")
	appEst, _ := cEst.Approve(pEst, pEst.ApprovalDigest)
	failEstimate = true
	if _, err := cEst.Execute(context.Background(), appEst); err == nil {
		t.Fatal("expected estimate error in Execute line 153")
	}

	// 39. Execute approval_changed (line 160)
	differentEstimate := false
	tDiff := rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/ds") {
			return response(`{"datasetReference":{"projectId":"source-project","datasetId":"ds"},"location":"EU"}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
			return response(`{"tableReference":{"projectId":"source-project","datasetId":"ds","tableId":"tbl"},"type":"TABLE","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`), nil
		}
		if req.Method == "POST" {
			var m map[string]any
			if req.Body != nil {
				_ = json.NewDecoder(req.Body).Decode(&m)
			}
			if m["dryRun"] == true {
				if differentEstimate {
					return response(`{"totalBytesProcessed":"200"}`), nil
				}
				return response(`{"totalBytesProcessed":"100"}`), nil
			}
			return response(`{"jobReference":{"projectId":"job-project","jobId":"j","location":"EU"},"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"rows":[]}`), nil
		}
		return response(`{}`), nil
	})
	lDiff, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	cDiff, _ := NewClient(Config{[]SourceProfile{profile}, identity, tDiff, lDiff, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	pDiff := previewFor(t, cDiff, "1000")
	appDiff, _ := cDiff.Approve(pDiff, pDiff.ApprovalDigest)
	differentEstimate = true
	if _, err := cDiff.Execute(context.Background(), appDiff); errorCode(err) != "approval_changed" {
		t.Fatalf("expected approval_changed in Execute line 160, got %v", err)
	}

	// 40. Execute object(raw) error (line 209)
	tExecRawBad := rt(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/datasets/ds") {
			return response(`{"datasetReference":{"projectId":"source-project","datasetId":"ds"},"location":"EU"}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/tables/tbl") {
			return response(`{"tableReference":{"projectId":"source-project","datasetId":"ds","tableId":"tbl"},"type":"TABLE","schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]}}`), nil
		}
		if req.Method == "POST" {
			var m map[string]any
			if req.Body != nil {
				_ = json.NewDecoder(req.Body).Decode(&m)
			}
			if m["dryRun"] == true {
				return response(`{"totalBytesProcessed":"100"}`), nil
			}
			return response(`123`), nil
		}
		return response(`{}`), nil
	})
	lExecBad, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	cExecRawBad, _ := NewClient(Config{[]SourceProfile{profile}, identity, tExecRawBad, lExecBad, func(context.Context) (ReadPlan, string, error) { return plan, "explicit-no-policies", nil }, clock})
	pBad := previewFor(t, cExecRawBad, "1000")
	appBad, _ := cExecRawBad.Approve(pBad, pBad.ApprovalDigest)
	if _, err := cExecRawBad.Execute(context.Background(), appBad); err == nil {
		t.Fatalf("expected error on non-object raw in Execute, got nil")
	} else if errorCode(err) != "malformed_wire" && errorCode(err) != "invalid_input" {
		t.Fatalf("expected malformed_wire or invalid_input in Execute line 209, got %v", err)
	}

	// 41. Execute beforeDispatch failure (lines 197-199)
	lDead, _ := NewFileLedger(filepath.Join(t.TempDir(), "ledger"))
	cDead, _, clockDead, _ := dynamicClient(t, lDead, "1000")
	pDead := previewFor(t, cDead, "1000")
	appDead, _ := cDead.Approve(pDead, pDead.ApprovalDigest)
	clockDead.now = clockDead.now.Add(2 * time.Hour)
	if _, err := cDead.Execute(context.Background(), appDead); err == nil {
		t.Fatal("expected deadline error in Execute line 197-199")
	}
}
