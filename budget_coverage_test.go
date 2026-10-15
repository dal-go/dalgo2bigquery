package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Not parallel: mutates package-level randRead seam.
func TestBudget_OpaqueIDPanic(t *testing.T) {
	orig := randRead
	defer func() { randRead = orig }()

	randRead = func([]byte) (int, error) {
		return 0, errors.New("entropy source failed")
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic from opaqueID when randRead fails")
		}
		if r != "secure random unavailable" {
			t.Fatalf("unexpected panic value: %v", r)
		}
	}()

	_ = opaqueID()
}

func TestBudget_PositiveBytes(t *testing.T) {
	t.Parallel()

	// Invalid strings not matching unsigned regex
	for _, bad := range []string{"", "-1", "abc", "12a", " ", "+5"} {
		if _, err := positiveBytes(bad); errorCode(err) != "invalid_input" {
			t.Fatalf("expected invalid_input for %q, got %v", bad, err)
		}
	}

	// Strings matching regex but n < 1 (zero) or overflowing int64
	for _, bad := range []string{"0", "9999999999999999999999999999999999999999"} {
		if _, err := positiveBytes(bad); errorCode(err) != "invalid_input" {
			t.Fatalf("expected invalid_input for %q, got %v", bad, err)
		}
	}

	// Valid positive integers
	if n, err := positiveBytes("42"); err != nil || n != 42 {
		t.Fatalf("expected 42, got %d, err: %v", n, err)
	}
}

func TestBudget_MemoryLedger_Branches(t *testing.T) {
	t.Parallel()

	m := NewMemoryLedger()

	// lease with canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.lease(canceledCtx, "run-1"); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on lease with canceled context, got %v", err)
	}

	// lease lock collision
	unlock, err := m.lease(context.Background(), "run-conflict")
	if err != nil {
		t.Fatalf("unexpected error on first lease: %v", err)
	}
	defer unlock()
	if _, err := m.lease(context.Background(), "run-conflict"); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on lease conflict, got %v", err)
	}

	// updateContext with canceled context
	if err := m.updateContext(canceledCtx, func(*ledgerState) error { return nil }); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected local_stopped/deadline on updateContext with canceled context, got %v", err)
	}

	// updateContext with callback error
	customErr := errors.New("custom error")
	if err := m.update(func(*ledgerState) error { return customErr }); !errors.Is(err, customErr) {
		t.Fatalf("expected custom error from update, got %v", err)
	}

	// updateContext where context is canceled inside callback
	innerCtx, innerCancel := context.WithCancel(context.Background())
	if err := m.updateContext(innerCtx, func(*ledgerState) error {
		innerCancel()
		return nil
	}); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected local_stopped/deadline on inner cancel, got %v", err)
	}
}

func TestBudget_FileLedger_Branches(t *testing.T) {
	t.Parallel()

	// NewFileLedger with non-absolute path
	if _, err := NewFileLedger("relative/path"); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for relative path, got %v", err)
	}

	// NewFileLedger on a file instead of dir
	tmpFile, err := os.CreateTemp("", "ledger-not-dir-*")
	if err != nil {
		t.Fatal(err)
	}
	tmpFileName := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpFileName)

	if _, err := NewFileLedger(tmpFileName); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input when ledger path is a file, got %v", err)
	}

	// Valid FileLedger in temp dir
	tmpDir, err := os.MkdirTemp("", "ledger-valid-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	fl, err := NewFileLedger(tmpDir)
	if err != nil {
		t.Fatalf("unexpected error creating FileLedger: %v", err)
	}

	// lease with invalid hex ID
	if _, err := fl.lease(context.Background(), "short-id"); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for bad lease id, got %v", err)
	}

	// lease with canceled context
	validID := strings.Repeat("a", 48)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fl.lease(canceledCtx, validID); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on lease with canceled context, got %v", err)
	}

	// valid lease
	unlock, err := fl.lease(context.Background(), validID)
	if err != nil {
		t.Fatalf("unexpected error acquiring lease: %v", err)
	}
	unlock()

	// lock failure when file cannot be created/opened as private
	badLockDir, err := os.MkdirTemp("", "bad-lock-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(badLockDir)
	// Create directory with name session.lock so opening session.lock as file fails or fails regular file check
	if err := os.Mkdir(filepath.Join(badLockDir, "session.lock"), 0700); err != nil {
		t.Fatal(err)
	}
	badFL, err := NewFileLedger(badLockDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := badFL.update(func(*ledgerState) error { return nil }); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input when session.lock is a directory, got %v", err)
	}

	// Corrupt session.json: unknown fields rejected
	corruptDir, err := os.MkdirTemp("", "corrupt-ledger-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(corruptDir)
	corruptFL, err := NewFileLedger(corruptDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(corruptDir, "session.json"), []byte(`{"version":1,"unknown":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := corruptFL.update(func(*ledgerState) error { return nil }); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on corrupt session.json, got %v", err)
	}

	// readState with context canceled
	if _, err := fl.readState(canceledCtx); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected local_stopped/deadline on readState with canceled context, got %v", err)
	}

	// readContext with error from readState
	if err := corruptFL.readContext(context.Background(), func(*ledgerState) error { return nil }); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input from readContext on corrupt ledger, got %v", err)
	}

	// update with fn returning error
	customErr := errors.New("fn fail")
	if err := fl.update(func(*ledgerState) error { return customErr }); !errors.Is(err, customErr) {
		t.Fatalf("expected fn fail, got %v", err)
	}

	// updateContext with context canceled before replace
	cancellableCtx, cancelCall := context.WithCancel(context.Background())
	if err := fl.updateContext(cancellableCtx, func(*ledgerState) error {
		cancelCall()
		return nil
	}); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected local_stopped/deadline when context canceled during update, got %v", err)
	}

	// normal update and readContext
	if err := fl.update(func(s *ledgerState) error {
		s.Runs["test-run"] = runRecord{}
		return nil
	}); err != nil {
		t.Fatalf("unexpected error on valid update: %v", err)
	}
	var found bool
	if err := fl.readContext(context.Background(), func(s *ledgerState) error {
		_, found = s.Runs["test-run"]
		return nil
	}); err != nil || !found {
		t.Fatalf("expected to read run from ledger, err: %v", err)
	}
}

func TestBudget_ClientRunMethods(t *testing.T) {
	t.Parallel()

	c := &Client{ledger: NewMemoryLedger()}

	// loadRun with non-existent id
	if _, err := c.loadRun("missing-id"); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid from loadRun, got %v", err)
	}

	// mutateRun with non-existent id
	if err := c.mutateRun("missing-id", func(*runRecord) error { return nil }); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid from mutateRun, got %v", err)
	}

	// mutateRunContext with non-existent id
	if err := c.mutateRunContext(context.Background(), "missing-id", func(*runRecord) error { return nil }); errorCode(err) != "cursor_invalid" {
		t.Fatalf("expected cursor_invalid from mutateRunContext, got %v", err)
	}

	// Populate a run
	runID := strings.Repeat("b", 48)
	_ = c.ledger.update(func(s *ledgerState) error {
		s.Runs[runID] = runRecord{ActivePrincipal: Principal{Subject: "test"}}
		return nil
	})

	// loadRun success
	r, err := c.loadRun(runID)
	if err != nil || r.ActivePrincipal.Subject != "test" {
		t.Fatalf("unexpected result from loadRun: %+v, err: %v", r, err)
	}

	// mutateRun with failing callback
	customErr := errors.New("mutate fail")
	if err := c.mutateRun(runID, func(*runRecord) error { return customErr }); !errors.Is(err, customErr) {
		t.Fatalf("expected custom error from mutateRun, got %v", err)
	}

	// mutateRunContext with canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.mutateRunContext(canceledCtx, runID, func(*runRecord) error { return nil }); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected local_stopped/deadline from mutateRunContext, got %v", err)
	}

	// mutateRunContext with failing callback
	if err := c.mutateRunContext(context.Background(), runID, func(*runRecord) error { return customErr }); !errors.Is(err, customErr) {
		t.Fatalf("expected custom error from mutateRunContext callback, got %v", err)
	}

	// mutateRunContext success
	if err := c.mutateRunContext(context.Background(), runID, func(rec *runRecord) error {
		rec.ActivePrincipal.Subject = "updated"
		return nil
	}); err != nil {
		t.Fatalf("unexpected error mutating run context: %v", err)
	}

	// ledgerLockContext with canceled context
	tmpFile, err := os.CreateTemp("", "lock-ctx-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if err := ledgerLockContext(canceledCtx, tmpFile); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected local_stopped/deadline from ledgerLockContext on canceled context, got %v", err)
	}

	// ledgerLockContext canceled while waiting on lock (line 311)
	lockPath := filepath.Join(t.TempDir(), "held.lock")
	heldFile, err := ledgerOpen(lockPath, true)
	if err != nil {
		t.Fatal(err)
	}
	defer heldFile.Close()
	if err := ledgerLock(heldFile, true); err != nil {
		t.Fatal(err)
	}

	secondFile, err := ledgerOpen(lockPath, false)
	if err != nil {
		t.Fatal(err)
	}
	defer secondFile.Close()

	waitCtx, waitCancel := context.WithCancel(context.Background())
	time.AfterFunc(time.Millisecond, waitCancel)
	if err := ledgerLockContext(waitCtx, secondFile); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected local_stopped/deadline when canceled in select, got %v", err)
	}
}

type stepContext struct {
	context.Context
	checkCount *int
	cancelAt   int
}

func (s stepContext) Err() error {
	*s.checkCount++
	if *s.checkCount >= s.cancelAt {
		return context.Canceled
	}
	return s.Context.Err()
}

// Not parallel: modifies jsonMarshal seam.
func TestBudget_MemoryLedger_JSONErrors(t *testing.T) {
	orig := jsonMarshal
	defer func() { jsonMarshal = orig }()

	m := NewMemoryLedger()

	// Fail jsonCopy(m.state) in updateContext
	jsonMarshal = func(v any) ([]byte, error) {
		return nil, errors.New("marshal error")
	}
	if err := m.update(func(*ledgerState) error { return nil }); err == nil {
		t.Fatal("expected error when jsonMarshal fails")
	}

	// Fail stored, e := jsonCopy(next) in updateContext (2nd call)
	callCount := 0
	jsonMarshal = func(v any) ([]byte, error) {
		callCount++
		if callCount == 2 {
			return nil, errors.New("second marshal error")
		}
		return json.Marshal(v)
	}
	if err := m.update(func(*ledgerState) error { return nil }); err == nil {
		t.Fatal("expected error on second jsonCopy")
	}

	// Fail jsonCopy(*s) in readContext
	callCount = 0
	jsonMarshal = func(v any) ([]byte, error) {
		callCount++
		if callCount == 2 {
			return nil, errors.New("read marshal error")
		}
		return json.Marshal(v)
	}
	if err := m.readContext(context.Background(), func(*ledgerState) error { return nil }); err == nil {
		t.Fatal("expected error on readContext jsonCopy")
	}
}

// Not parallel: modifies ledgerSecureTemp seam.
func TestBudget_FileLedger_SecureTempFail(t *testing.T) {
	orig := ledgerSecureTemp
	defer func() { ledgerSecureTemp = orig }()

	dir, err := os.MkdirTemp("", "ledger-sectemp-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	_ = os.Chmod(dir, 0700)

	fl, err := NewFileLedger(dir)
	if err != nil {
		t.Fatal(err)
	}

	ledgerSecureTemp = func(f *os.File) error {
		return errors.New("chmod failed")
	}

	if err := fl.update(func(s *ledgerState) error { return nil }); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input when ledgerSecureTemp fails, got %v", err)
	}
}

func TestBudget_FileLedger_AdditionalBranches(t *testing.T) {
	t.Parallel()

	// NewFileLedger when dir is a file
	tmp, err := os.CreateTemp("", "not-a-dir-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp.Name())
	tmp.Close()
	if _, err := NewFileLedger(tmp.Name()); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for non-directory, got %v", err)
	}

	// NewFileLedger when dir is not 0700
	tmpDir, err := os.MkdirTemp("", "not-0700-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	_ = os.Chmod(tmpDir, 0755)
	if _, err := NewFileLedger(tmpDir); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for 0755 directory, got %v", err)
	}

	dir := filepath.Join(t.TempDir(), "fl-more")
	fl, err := NewFileLedger(dir)
	if err != nil {
		t.Fatal(err)
	}

	// lock with invalid file permissions (not 0600)
	badLock := filepath.Join(dir, "bad-perm.lock")
	if err := os.WriteFile(badLock, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(badLock, 0644)
	if _, err := fl.lock("bad-perm.lock"); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on non-0600 lock, got %v", err)
	}

	// lease when l.lock fails
	runID := strings.Repeat("c", 48)
	badLease := filepath.Join(dir, "run-"+runID+".lock")
	if err := os.WriteFile(badLease, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(badLease, 0644)
	if _, err := fl.lease(context.Background(), runID); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on lease when lock fails, got %v", err)
	}

	// lease when lock is held by another handle
	runID2 := strings.Repeat("e", 48)
	lockFd, err := ledgerOpen(filepath.Join(dir, "run-"+runID2+".lock"), true)
	if err != nil {
		t.Fatal(err)
	}
	defer lockFd.Close()
	if err := ledgerLock(lockFd, true); err != nil {
		t.Fatal(err)
	}
	if _, err := fl.lease(context.Background(), runID2); errorCode(err) != "local_stopped" {
		t.Fatalf("expected local_stopped on lease collision, got %v", err)
	}

	// updateContext with canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := fl.updateContext(canceledCtx, func(s *ledgerState) error { return nil }); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected local_stopped/deadline on updateContext with canceled ctx, got %v", err)
	}

	// updateContext with data > 64<<20
	hugeDir := filepath.Join(t.TempDir(), "fl-huge")
	flHuge, err := NewFileLedger(hugeDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := flHuge.update(func(s *ledgerState) error {
		s.Runs["huge"] = runRecord{PageDigest: strings.Repeat("a", 65<<20)}
		return nil
	}); errorCode(err) != "response_limit" {
		t.Fatalf("expected response_limit on huge state, got %v", err)
	}

	// updateContext when ledgerTemp fails (dir made readonly during update)
	if runtime.GOOS != "windows" {
		roDir := filepath.Join(t.TempDir(), "fl-ro")
		flRO, err := NewFileLedger(roDir)
		if err != nil {
			t.Fatal(err)
		}
		if err := flRO.update(func(s *ledgerState) error {
			_ = os.Chmod(roDir, 0500)
			return nil
		}); errorCode(err) != "invalid_input" {
			t.Fatalf("expected invalid_input when ledgerTemp fails, got %v", err)
		}
		_ = os.Chmod(roDir, 0700)
	}

	// updateContext when ledgerReplace fails (target is a directory)
	replDir := filepath.Join(t.TempDir(), "fl-repl")
	flRepl, err := NewFileLedger(replDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := flRepl.update(func(s *ledgerState) error {
		return os.Mkdir(filepath.Join(replDir, "session.json"), 0700)
	}); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input when ledgerReplace fails, got %v", err)
	}

	// readState when session.json > 64<<20
	sizeDir := filepath.Join(t.TempDir(), "fl-size")
	flSize, err := NewFileLedger(sizeDir)
	if err != nil {
		t.Fatal(err)
	}
	sessFile, err := ledgerOpen(filepath.Join(sizeDir, "session.json"), true)
	if err != nil {
		t.Fatal(err)
	}
	_ = sessFile.Truncate(65 << 20)
	_ = sessFile.Close()
	if _, err := flSize.readState(context.Background()); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on >64MB session.json, got %v", err)
	}

	// readState when session.json has non-not-exist error (e.g. symlink rejected by O_NOFOLLOW)
	if runtime.GOOS != "windows" {
		symDir := filepath.Join(t.TempDir(), "fl-sym")
		flSym, err := NewFileLedger(symDir)
		if err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(t.TempDir(), "target.json")
		_ = os.WriteFile(target, []byte("{}"), 0600)
		_ = os.Symlink(target, filepath.Join(symDir, "session.json"))
		if _, err := flSym.readState(context.Background()); errorCode(err) != "invalid_input" {
			t.Fatalf("expected invalid_input on symlink session.json, got %v", err)
		}
	}
}

func TestBudget_ContextCanceledSteps(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "fl-ctx")
	fl, err := NewFileLedger(dir)
	if err != nil {
		t.Fatal(err)
	}

	// updateContext canceling at check 3 (line 194)
	count1 := 0
	ctx1 := stepContext{Context: context.Background(), checkCount: &count1, cancelAt: 3}
	if err := fl.updateContext(ctx1, func(s *ledgerState) error { return nil }); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected local_stopped/deadline at step 3, got %v", err)
	}

	// updateContext canceling at check 4 (line 220)
	count2 := 0
	ctx2 := stepContext{Context: context.Background(), checkCount: &count2, cancelAt: 4}
	if err := fl.updateContext(ctx2, func(s *ledgerState) error { return nil }); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected local_stopped/deadline at step 4, got %v", err)
	}

	// mutateRunContext canceling at line 322
	c := &Client{ledger: NewMemoryLedger()}
	runID := strings.Repeat("d", 48)
	_ = c.ledger.update(func(s *ledgerState) error {
		s.Runs[runID] = runRecord{}
		return nil
	})
	count3 := 0
	ctx3 := stepContext{Context: context.Background(), checkCount: &count3, cancelAt: 2}
	if err := c.mutateRunContext(ctx3, runID, func(*runRecord) error { return nil }); errorCode(err) != "local_stopped" || err.(*Error).Reason != "deadline" {
		t.Fatalf("expected local_stopped/deadline at mutateRunContext line 322, got %v", err)
	}
}
