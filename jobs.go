package bigquery

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"strconv"
	"time"
)

func parseJob(v any) (JobRef, error) {
	m, e := object(v)
	if e != nil {
		return JobRef{}, e
	}
	if e = known(m, []string{"projectId", "jobId", "location"}); e != nil {
		return JobRef{}, fail("malformed_wire")
	}
	p, e := requiredString(m, "projectId")
	if e != nil {
		return JobRef{}, e
	}
	j, e := requiredString(m, "jobId")
	if e != nil {
		return JobRef{}, e
	}
	l, e := requiredString(m, "location")
	if e != nil {
		return JobRef{}, e
	}
	if !projectID.MatchString(p) || len(j) > 1024 || !locationID.MatchString(l) {
		return JobRef{}, fail("malformed_wire")
	}
	return JobRef{p, j, l}, nil
}
func (c *Client) captureJob(id string, m map[string]any) error {
	job, e := parseJob(m["jobReference"])
	if e != nil {
		return e
	}
	return c.mutateRun(id, func(r *runRecord) error {
		if job.ProjectID != r.Preview.Execution.JobProject || job.Location != r.Preview.Observation.Location {
			return fail("malformed_wire")
		}
		if r.Receipt.Job != nil && *r.Receipt.Job != job {
			return fail("malformed_wire")
		}
		r.Receipt.Job = &job
		r.Receipt.State = "running"
		return nil
	})
}
func (c *Client) Execute(ctx context.Context, a Approval) (*Run, error) {
	if a.client != c || a.nonce == "" || a.digest == "" {
		return nil, fail("approval_required")
	}
	id := opaqueID()
	var record runRecord
	e := c.ledger.update(func(s *ledgerState) error {
		p, ok := s.Previews[a.nonce]
		if !ok || p.Used {
			return fail("approval_required")
		}
		if !c.clock.Now().Before(p.Preview.ExpiresAt) || p.Preview.ApprovalDigest != a.digest {
			return fail("approval_changed")
		}
		cap, _ := positiveBytes(p.Preview.Execution.MaximumBytesBilled)
		budget, _ := positiveBytes(p.Preview.Execution.SessionBudgetBytes)
		reserved := int64(0)
		for _, r := range s.Runs {
			if budgetKey(r.Preview) == budgetKey(p.Preview) {
				if r.Preview.Execution.SessionBudgetBytes != p.Preview.Execution.SessionBudgetBytes {
					return fail("budget_exhausted")
				}
				if r.Reservation > budget-reserved {
					return fail("budget_exhausted")
				}
				reserved += r.Reservation
			}
		}
		if cap > budget-reserved {
			return fail("budget_exhausted")
		}
		now := c.clock.Now()
		p.Used = true
		s.Previews[a.nonce] = p
		receipt := Receipt{Version: 1, RunID: id, ApprovalDigest: a.digest, SourceDigest: p.Preview.Plan.SourceDigest, ObservationDigest: p.Preview.Observation.Digest, Principal: p.Preview.Execution.Principal, State: "reserved", RunStartedAt: now, ExecutionDeadline: now.Add(time.Duration(p.Preview.Bounds.WallMs) * time.Millisecond), Bounds: p.Preview.Bounds, Warnings: []string{}, ResidualSourceReplacementRace: true}
		record = runRecord{ActivePrincipal: receipt.Principal, Receipt: receipt, Preview: p.Preview, Reservation: cap, SeenTokens: map[string]bool{}}
		s.Runs[id] = record
		return nil
	})
	if e != nil {
		return nil, e
	}
	run := newRun(c, ctx, id)
	release, e := c.ledger.lease(ctx, id)
	if e != nil {
		return run, e
	}
	defer release()
	scope := runScope(record)
	profile := c.profiles[record.Preview.Plan.SourceDigest]
	beforeSubmit := func(e error) (*Run, error) {
		_ = c.mutateRun(id, func(r *runRecord) error {
			r.Receipt.State = "failed"
			r.Receipt.Reason = reasonOf(e)
			r.Reservation = 0
			return nil
		})
		return run, e
	}
	if e = c.reauthorize(ctx, record.Preview.Plan, record.Preview.PolicyDigest); e != nil {
		return beforeSubmit(e)
	}
	obs, e := c.observe(ctx, profile, scope, &record.Receipt.Principal)
	if e != nil {
		return beforeSubmit(e)
	}
	estimate, e := c.estimate(ctx, record.Preview.Plan, record.Preview.Execution, obs.Location, scope)
	if e != nil {
		return beforeSubmit(e)
	}
	check := record.Preview
	check.Observation = obs
	check.EstimatedBytes = estimate
	binding, e := approvalDigest(check)
	if e != nil || binding != a.digest {
		return beforeSubmit(fail("approval_changed"))
	}
	obs, e = c.observe(ctx, profile, scope, &record.Receipt.Principal)
	if e != nil {
		return beforeSubmit(e)
	}
	if obs.Digest != record.Preview.Observation.Digest {
		return beforeSubmit(fail("source_changed"))
	}
	if e = c.reauthorize(ctx, record.Preview.Plan, record.Preview.PolicyDigest); e != nil {
		return beforeSubmit(e)
	}
	req, e := queryRequest(record.Preview.Plan, record.Preview.Execution, record.Preview.Bounds, obs.Location, false)
	if e != nil {
		return beforeSubmit(e)
	}
	e = c.mutateRun(id, func(r *runRecord) error { r.Receipt.State = "submitting"; return nil })
	if e != nil {
		return run, e
	}
	scope.submit = true
	scope.result = true
	raw, e := c.call(run.ctx, scope, &record.Receipt.Principal, false, func(api sdkAPI) error {
		_, e := api.service.Jobs.Query(record.Preview.Execution.JobProject, req).Context(api.ctx).Do()
		return e
	})
	if e != nil {
		_ = c.mutateRun(id, func(r *runRecord) error {
			if r.Receipt.Job == nil && scope.dispatched {
				r.Receipt.State = "submission_unknown"
			} else if r.Receipt.Job == nil {
				r.Receipt.State = "failed"
				r.Reservation = 0
			} else {
				r.Receipt.LocalStopped = true
			}
			r.Receipt.Reason = reasonOf(e)
			return nil
		})
		return run, e
	}
	m, e := object(raw)
	if e != nil {
		return run, e
	}
	if e = c.captureJob(id, m); e != nil {
		_ = c.mutateRun(id, func(r *runRecord) error {
			r.Receipt.State = "submission_unknown"
			r.Receipt.Reason = "malformed_wire"
			return nil
		})
		return run, fail("submission_unknown")
	}
	if e = run.loadPage(m, nil, false); e != nil {
		run.stopped(e)
		return run, e
	}
	return run, nil
}
func runScope(r runRecord) *operationScope {
	return &operationScope{bounds: r.Receipt.Bounds, deadline: r.Receipt.ExecutionDeadline, runID: r.Receipt.RunID}
}
func codeOf(e error) string {
	if err, ok := e.(*Error); ok {
		return err.Code
	}
	return "local_stopped"
}
func (c *Client) findJob(job JobRef) (runRecord, error) {
	var run runRecord
	e := c.ledger.update(func(s *ledgerState) error {
		for _, r := range s.Runs {
			if r.Receipt.Job != nil && *r.Receipt.Job == job {
				run = r
				return nil
			}
		}
		return fail("cursor_invalid")
	})
	return run, e
}
func (c *Client) Status(ctx context.Context, job JobRef) (JobStatus, error) {
	r, e := c.findJob(job)
	if e != nil {
		return JobStatus{}, e
	}
	release, e := c.ledger.lease(ctx, r.Receipt.RunID)
	if e != nil {
		return JobStatus{}, e
	}
	defer release()
	r, e = c.loadRun(r.Receipt.RunID)
	if e != nil {
		return JobStatus{}, e
	}
	scope := runScope(r)
	ctx, finish, e := c.preparation(ctx, scope, true)
	if e != nil {
		return JobStatus{}, e
	}
	defer finish()
	if e = c.reauthorize(ctx, r.Preview.Plan, r.Preview.PolicyDigest); e != nil {
		return JobStatus{}, e
	}
	raw, e := c.call(ctx, scope, &r.ActivePrincipal, true, func(api sdkAPI) error {
		_, e := api.service.Jobs.Get(job.ProjectID, job.JobID).Location(job.Location).Context(api.ctx).Do()
		return e
	})
	if e != nil {
		return JobStatus{}, e
	}
	m, e := object(raw)
	if e != nil {
		return JobStatus{}, e
	}
	ref, e := parseJob(m["jobReference"])
	if e != nil || ref != job {
		return JobStatus{}, fail("malformed_wire")
	}
	status, e := object(m["status"])
	if e != nil {
		return JobStatus{}, e
	}
	if e = known(status, []string{"state", "errorResult", "errors"}); e != nil {
		return JobStatus{}, fail("malformed_wire")
	}
	state, e := requiredString(status, "state")
	if e != nil || (state != "PENDING" && state != "RUNNING" && state != "DONE") {
		return JobStatus{}, fail("malformed_wire")
	}
	result := JobStatus{Job: job, State: "running", Warnings: providerWarnings(status["errors"])}
	if state == "DONE" {
		result.State = "completed"
		if x, has := status["errorResult"]; has && x != nil {
			err, e := object(x)
			if e != nil {
				return JobStatus{}, e
			}
			reason, e := requiredString(err, "reason")
			if e != nil {
				return JobStatus{}, e
			}
			result.State = "failed"
			result.Warnings = append(result.Warnings, sanitizeReason(reason))
			// "stopped" also represents timeout and other stops; do not invent a
			// confirmed cancellation from that ambiguous provider reason.
			if reason == "cancelled" {
				result.State = "cancelled"
			}
		}
		if statistics, ok := m["statistics"]; ok {
			stats, e := object(statistics)
			if e != nil {
				return JobStatus{}, e
			}
			if query, ok := stats["query"]; ok {
				q, e := object(query)
				if e != nil {
					return JobStatus{}, e
				}
				result.BilledBytes, e = decimalString(q, "totalBytesBilled")
				if e != nil {
					return JobStatus{}, e
				}
			}
		}
	}
	e = c.mutateRun(r.Receipt.RunID, func(rr *runRecord) error {
		if state == "DONE" {
			if rr.Receipt.State == "cancel_requested" && result.State == "completed" {
				result.Warnings = append(result.Warnings, "completed_before_cancel")
			}
			rr.Receipt.State = result.State
			rr.Receipt.BilledBytes = result.BilledBytes
			if result.BilledBytes != nil {
				n, _ := strconv.ParseInt(*result.BilledBytes, 10, 64)
				rr.Reservation = n
			}
		}
		rr.Receipt.Warnings = append(rr.Receipt.Warnings, result.Warnings...)
		return nil
	})
	return result, e
}
func (c *Client) CancelJob(ctx context.Context, job JobRef) (CancelResult, error) {
	r, e := c.findJob(job)
	if e != nil {
		return CancelResult{}, e
	}
	release, e := c.ledger.lease(ctx, r.Receipt.RunID)
	if e != nil {
		return CancelResult{}, e
	}
	defer release()
	r, e = c.loadRun(r.Receipt.RunID)
	if e != nil {
		return CancelResult{}, e
	}
	scope := runScope(r)
	ctx, finish, e := c.preparation(ctx, scope, true)
	if e != nil {
		return CancelResult{}, e
	}
	defer finish()
	if e = c.reauthorize(ctx, r.Preview.Plan, r.Preview.PolicyDigest); e != nil {
		return CancelResult{}, e
	}
	scope.submit = true
	raw, e := c.call(ctx, scope, &r.ActivePrincipal, true, func(api sdkAPI) error {
		_, e := api.service.Jobs.Cancel(job.ProjectID, job.JobID).Location(job.Location).Context(api.ctx).Do()
		return e
	})
	if e != nil {
		switch codeOf(e) {
		case "auth_required", "auth_expired", "scope_missing", "response_limit", "approval_changed":
			return CancelResult{}, e
		}
		return CancelResult{job, "unknown"}, fail("cancellation_unknown")
	}
	m, e := object(raw)
	if e != nil {
		return CancelResult{job, "unknown"}, fail("cancellation_unknown")
	}
	j, e := object(m["job"])
	if e != nil {
		return CancelResult{job, "unknown"}, fail("cancellation_unknown")
	}
	ref, e := parseJob(j["jobReference"])
	if e != nil || ref != job {
		return CancelResult{job, "unknown"}, fail("cancellation_unknown")
	}
	e = c.mutateRun(r.Receipt.RunID, func(rr *runRecord) error {
		if rr.Receipt.State != "completed" && rr.Receipt.State != "failed" && rr.Receipt.State != "cancelled" {
			rr.Receipt.State = "cancel_requested"
		}
		return nil
	})
	return CancelResult{job, "cancel_requested"}, e
}
func providerWarnings(v any) []string {
	a, ok := v.([]any)
	if !ok {
		return []string{}
	}
	warnings := []string{}
	for _, x := range a {
		if m, ok := x.(map[string]any); ok {
			warnings = append(warnings, sanitizeReason(asString(m["reason"])))
		}
	}
	return warnings
}
func sanitizeReason(s string) string {
	switch s {
	case "accessDenied", "backendError", "billingTierLimitExceeded", "invalidQuery", "notFound", "rateLimitExceeded", "resourcesExceeded", "stopped", "cancelled":
		return s
	}
	return "provider_warning"
}

// Executor is a one-approved-plan DALgo bridge, not a DB or generic fallback.
type Executor struct {
	Client   *Client
	Profile  SourceProfile
	Approval Approval
	Plan     ReadPlan
}

func (e Executor) ExecuteQueryToRecordsReader(context.Context, dal.Query) (dal.RecordsReader, error) {
	return nil, dal.ErrNotSupported
}

// recordset interface method is in reader.go; no keyed projection is fabricated.

func reasonOf(e error) string {
	if err, ok := e.(*Error); ok && err.Reason != "" {
		return err.Reason
	}
	return codeOf(e)
}
