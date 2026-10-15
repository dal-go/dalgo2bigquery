//go:build windows

package bigquery

import (
	"errors"
	"golang.org/x/sys/windows"
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsLedgerRefusesForeignDACL(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "private-ledger")
	ledger, e := NewFileLedger(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = ledger.update(func(*ledgerState) error { return nil }); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "session.json")
	if !ledgerPrivatePath(path, false) {
		t.Fatal("private DACL missing")
	}
	sd, e := windows.SecurityDescriptorFromString("D:P(A;;FA;;;WD)")
	if e != nil {
		t.Fatal(e)
	}
	acl, _, e := sd.DACL()
	if e != nil {
		t.Fatal(e)
	}
	if e = windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, acl, nil); e != nil {
		t.Fatal(e)
	}
	if ledgerPrivatePath(path, false) {
		t.Fatal("world-readable ledger accepted")
	}
	if e = ledger.update(func(*ledgerState) error { return nil }); errorCode(e) != "invalid_input" {
		t.Fatal(e)
	}
	// Restore private inheritance only for normal test cleanup; no real credentials.
	private, _, e := ledgerSecurity()
	if e != nil {
		t.Fatal(e)
	}
	acl, _, e = private.DACL()
	if e != nil {
		t.Fatal(e)
	}
	if e = windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, acl, nil); e != nil {
		t.Fatal(e)
	}
	if e = os.Remove(path); e != nil {
		t.Fatal(e)
	}
}

// Explicit file security must match the token user even when Windows' default
// token owner is an administrator group. Keep exact owner and DACL validation.
func TestWindowsLedgerCreatedFilesHaveExplicitOwner(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "private-ledger")
	l, e := NewFileLedger(dir)
	if e != nil {
		t.Fatal("private directory", e)
	}
	lock, e := l.lock("session.lock")
	if e != nil {
		t.Fatal("explicit lock owner/DACL", e)
	}
	if !ledgerPrivateFile(lock) {
		t.Fatal("lock privacy")
	}
	lock.Close()
	temp, e := ledgerTemp(dir)
	if e != nil {
		t.Fatal("explicit temp creation", e)
	}
	name := temp.Name()
	defer os.Remove(name)
	if !ledgerPrivateFile(temp) {
		temp.Close()
		t.Fatal("temp privacy")
	}
	temp.Close()
	if e = l.update(func(*ledgerState) error { return nil }); e != nil {
		t.Fatal("persist explicit owner", e)
	}
	if !ledgerPrivatePath(filepath.Join(dir, "session.json"), false) {
		t.Fatal("snapshot privacy")
	}
}

// Not parallel: modifies ledgerSecurity seam.
func TestWindowsLedgerSecurityFail(t *testing.T) {
	orig := ledgerSecurity
	defer func() { ledgerSecurity = orig }()
	ledgerSecurity = func() (*windows.SECURITY_DESCRIPTOR, *windows.SID, error) {
		return nil, nil, errors.New("simulated security fail")
	}

	dir := filepath.Join(t.TempDir(), "sec-fail")
	if err := ledgerMkdir(dir); err == nil {
		t.Fatal("expected error from ledgerMkdir when ledgerSecurity fails")
	}
	if _, err := ledgerOpen(filepath.Join(dir, "f.lock"), true); err == nil {
		t.Fatal("expected error from ledgerOpen when ledgerSecurity fails")
	}
	if _, err := ledgerTemp(dir); err == nil {
		t.Fatal("expected error from ledgerTemp when ledgerSecurity fails")
	}
}

func TestWindowsLedgerInvalidPaths(t *testing.T) {
	t.Parallel()
	nullPath := "foo\x00bar"
	if err := ledgerMkdir(nullPath); err == nil {
		t.Fatal("expected error for null byte in ledgerMkdir")
	}
	if ledgerPrivatePath(nullPath, false) {
		t.Fatal("expected false for null byte in ledgerPrivatePath")
	}
	if _, err := ledgerOpen(nullPath, false); err == nil {
		t.Fatal("expected error for null byte in ledgerOpen")
	}
	if _, err := ledgerTemp(nullPath); err == nil {
		t.Fatal("expected error for null byte in ledgerTemp")
	}
	if err := ledgerReplace(nullPath, "target"); err == nil {
		t.Fatal("expected error for null byte in ledgerReplace from")
	}
	if err := ledgerReplace("target", nullPath); err == nil {
		t.Fatal("expected error for null byte in ledgerReplace to")
	}
}

func TestWindowsLedgerOperations(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ops-ledger")
	if err := ledgerMkdir(dir); err != nil {
		t.Fatal(err)
	}
	// Calling ledgerMkdir on existing dir
	if err := ledgerMkdir(dir); err != nil {
		t.Fatal(err)
	}

	// ledgerPrivatePath for missing file
	if ledgerPrivatePath(filepath.Join(dir, "absent"), false) {
		t.Fatal("expected false for absent file in ledgerPrivatePath")
	}
	// ledgerPrivatePath for dir when dir=false
	if ledgerPrivatePath(dir, false) {
		t.Fatal("expected false when dir passed but dir=false")
	}

	// ledgerOpen without create for missing file
	if _, err := ledgerOpen(filepath.Join(dir, "missing.lock"), false); err == nil {
		t.Fatal("expected error opening missing file without create")
	}

	// ledgerOpen with create
	lockPath := filepath.Join(dir, "test.lock")
	f, err := ledgerOpen(lockPath, true)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// ledgerLock non-blocking
	if err := ledgerLock(f, true); err != nil {
		t.Fatal(err)
	}
	// ledgerUnlock
	if err := ledgerUnlock(f); err != nil {
		t.Fatal(err)
	}
	// ledgerLock blocking
	if err := ledgerLock(f, false); err != nil {
		t.Fatal(err)
	}
	_ = ledgerUnlock(f)

	// ledgerReplace
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	if err := os.WriteFile(src, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ledgerReplace(src, dst); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatal(err)
	}

	// ledgerSyncDir
	if err := ledgerSyncDir(dir); err != nil {
		t.Fatal(err)
	}

	// ledgerSecureTemp
	tempFile, err := ledgerTemp(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()
	if err := ledgerSecureTemp(tempFile); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsLedgerSecureTempRefusesForeign(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "foreign-sec")
	if err := ledgerMkdir(dir); err != nil {
		t.Fatal(err)
	}
	tempFile, err := ledgerTemp(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	sd, err := windows.SecurityDescriptorFromString("D:P(A;;FA;;;WD)")
	if err != nil {
		t.Fatal(err)
	}
	acl, _, err := sd.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(tempFile.Name(), windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, acl, nil); err != nil {
		t.Fatal(err)
	}
	if err := ledgerSecureTemp(tempFile); errorCode(err) != "invalid_input" {
		t.Fatalf("expected invalid_input when ledgerSecureTemp has foreign DACL, got %v", err)
	}
}
