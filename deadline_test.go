package bigquery

import (
	"testing"
	"time"
)

func TestOperationDeadline_ValidationAndEdgeCases(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	validExec := now.Add(60 * time.Second)

	// bytesRemaining <= 0
	if _, err := OperationDeadline(now, validExec, time.Time{}, 5*time.Second, false, 0); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit, got %v", err)
	}

	// httpLimit <= 0
	if _, err := OperationDeadline(now, validExec, time.Time{}, 0, false, 100); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for 0 httpLimit, got %v", err)
	}

	// httpLimit > 15s
	if _, err := OperationDeadline(now, validExec, time.Time{}, 20*time.Second, false, 100); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for >15s httpLimit, got %v", err)
	}

	// executionDeadline.IsZero()
	if _, err := OperationDeadline(now, time.Time{}, time.Time{}, 5*time.Second, false, 100); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for zero executionDeadline, got %v", err)
	}

	// !now.Before(executionDeadline) in non-control mode
	pastExec := now.Add(-time.Second)
	if _, err := OperationDeadline(now, pastExec, time.Time{}, 5*time.Second, false, 100); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped for past executionDeadline, got %v", err)
	}

	// executionDeadline before now + httpLimit
	shortExec := now.Add(2 * time.Second)
	d, err := OperationDeadline(now, shortExec, time.Time{}, 5*time.Second, false, 100)
	if err != nil || !d.Equal(shortExec) {
		t.Fatalf("expected shortExec deadline, got %v, %v", d, err)
	}

	// callerDeadline before executionDeadline
	callerDeadline := now.Add(1 * time.Second)
	d, err = OperationDeadline(now, shortExec, callerDeadline, 5*time.Second, false, 100)
	if err != nil || !d.Equal(callerDeadline) {
		t.Fatalf("expected callerDeadline, got %v, %v", d, err)
	}

	// !now.Before(deadline) where callerDeadline is in the past
	pastCaller := now.Add(-time.Second)
	if _, err := OperationDeadline(now, validExec, pastCaller, 5*time.Second, true, 100); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped for past caller deadline, got %v", err)
	}

	// Control mode ignores expired executionDeadline
	d, err = OperationDeadline(now, pastExec, time.Time{}, 5*time.Second, true, 100)
	if err != nil || !d.Equal(now.Add(5*time.Second)) {
		t.Fatalf("expected now+httpLimit in control mode, got %v, %v", d, err)
	}
}
