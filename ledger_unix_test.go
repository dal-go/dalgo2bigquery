//go:build linux || darwin

package bigquery

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLedgerUnix_PrivatePathAndOperations(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "unix-ledger")
	if err := ledgerMkdir(dir); err != nil {
		t.Fatal(err)
	}

	// ledgerPrivatePath for dir
	if !ledgerPrivatePath(dir, true) {
		t.Fatal("expected dir to be private 0700")
	}

	// Non-existent path
	if ledgerPrivatePath(filepath.Join(dir, "absent"), false) {
		t.Fatal("non-existent path reported private")
	}

	// Create a private file
	filePath := filepath.Join(dir, "session.json")
	f, err := ledgerOpen(filePath, true)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if !ledgerPrivateFile(f) {
		t.Fatal("expected file to be private 0600")
	}
	if !ledgerPrivatePath(filePath, false) {
		t.Fatal("expected path to be private 0600")
	}

	// Lock, unlock, temp, replace, secure
	if err := ledgerLock(f, true); err != nil {
		t.Fatal(err)
	}
	if err := ledgerUnlock(f); err != nil {
		t.Fatal(err)
	}
	if err := ledgerSecureTemp(f); err != nil {
		t.Fatal(err)
	}

	temp, err := ledgerTemp(dir)
	if err != nil {
		t.Fatal(err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	temp.Close()

	dest := filepath.Join(dir, "session-renamed.json")
	if err := ledgerReplace(tempName, dest); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(dest)

	// Sync non-existent dir
	if err := ledgerSyncDir(filepath.Join(dir, "absent-dir")); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input for absent dir, got %v", err)
	}
}

// Not parallel: mutates ledgerSync seam.
func TestLedgerUnix_SyncDirError(t *testing.T) {
	oldSync := ledgerSync
	defer func() { ledgerSync = oldSync }()

	ledgerSync = func(f *os.File) error {
		return errors.New("simulated sync error")
	}

	dir := t.TempDir()
	if err := ledgerSyncDir(dir); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input on sync error, got %v", err)
	}
}
