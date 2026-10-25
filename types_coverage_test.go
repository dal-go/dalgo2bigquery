package bigquery

import (
	"context"
	"testing"
	"time"
)

func TestTypes_ClockAndJSONHelpers(t *testing.T) {
	t.Parallel()
	clock := realClock{}

	// Now()
	now := clock.Now()
	if now.IsZero() {
		t.Fatal("expected non-zero time")
	}

	// Sleep() normal completion
	if err := clock.Sleep(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("expected nil error on sleep, got %v", err)
	}

	// Sleep() with canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := clock.Sleep(canceledCtx, time.Hour); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on canceled sleep, got %v", err)
	}

	// jsonCopy on unmarshalable value
	ch := make(chan int)
	if _, err := jsonCopy(ch); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input from jsonCopy, got %v", err)
	}

	// digestObject on unmarshalable value
	if _, err := digestObject("test", ch); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input from digestObject, got %v", err)
	}
}
