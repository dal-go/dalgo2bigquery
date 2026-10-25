package bigquery

import "time"

// OperationDeadline computes a bound from the original trusted-ledger deadline.
// It never starts or renews a run. Control means explicit status/cancel only;
// callers must still debit the unchanged cumulative byte ledger.
func OperationDeadline(now, executionDeadline, callerDeadline time.Time, httpLimit time.Duration, control bool, bytesRemaining int64) (time.Time, error) {
	if bytesRemaining <= 0 {
		return time.Time{}, fail("response_limit")
	}
	if httpLimit <= 0 || httpLimit > 15*time.Second || executionDeadline.IsZero() {
		return time.Time{}, fail("invalid_input")
	}
	deadline := now.Add(httpLimit)
	if !control {
		if !now.Before(executionDeadline) {
			return time.Time{}, deadlineError()
		}
		if executionDeadline.Before(deadline) {
			deadline = executionDeadline
		}
	}
	if !callerDeadline.IsZero() && callerDeadline.Before(deadline) {
		deadline = callerDeadline
	}
	if !now.Before(deadline) {
		return time.Time{}, fail("local_stopped")
	}
	return deadline, nil
}

func deadlineError() error { return &Error{Code: "local_stopped", Reason: "deadline"} }
