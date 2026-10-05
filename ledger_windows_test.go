//go:build windows

package bigquery

import (
	"golang.org/x/sys/windows"
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsLedgerRefusesForeignDACL(t *testing.T) {
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
