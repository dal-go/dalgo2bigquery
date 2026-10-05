//go:build windows

package bigquery

import (
	"golang.org/x/sys/windows"
	"os"
	"path/filepath"
	"unsafe"
)

func ledgerSecurity() (*windows.SECURITY_DESCRIPTOR, *windows.SID, error) {
	user, e := windows.GetCurrentProcessToken().GetTokenUser()
	if e != nil {
		return nil, nil, e
	}
	sid := user.User.Sid
	sd, e := windows.SecurityDescriptorFromString("O:" + sid.String() + "D:P(A;OICI;FA;;;" + sid.String() + ")")
	return sd, sid, e
}
func ledgerMkdir(dir string) error {
	if _, e := os.Lstat(dir); e == nil {
		return nil
	} else if !os.IsNotExist(e) {
		return e
	}
	if e := os.MkdirAll(filepath.Dir(dir), 0700); e != nil {
		return e
	}
	sd, _, e := ledgerSecurity()
	if e != nil {
		return e
	}
	name, e := windows.UTF16PtrFromString(dir)
	if e != nil {
		return e
	}
	return windows.CreateDirectory(name, &windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd})
}
func ledgerPrivateHandle(f *os.File) bool {
	_, sid, e := ledgerSecurity()
	if e != nil {
		return false
	}
	sd, e := windows.GetSecurityInfo(windows.Handle(f.Fd()), windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if e != nil {
		return false
	}
	owner, _, e := sd.Owner()
	if e != nil || owner == nil || !owner.Equals(sid) {
		return false
	}
	acl, _, e := sd.DACL()
	if e != nil || acl == nil || acl.AceCount != 1 {
		return false
	}
	var ace *windows.ACCESS_ALLOWED_ACE
	if e = windows.GetAce(acl, 0, &ace); e != nil || ace == nil || ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE || ace.Mask != 0x001f01ff {
		return false
	}
	allowed := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	return allowed.Equals(sid)
}
func ledgerPrivateFile(f *os.File) bool {
	s, e := f.Stat()
	return e == nil && s.Mode().IsRegular() && ledgerPrivateHandle(f)
}
func ledgerPrivatePath(path string, dir bool) bool {
	name, e := windows.UTF16PtrFromString(path)
	if e != nil {
		return false
	}
	flags := uint32(windows.FILE_FLAG_OPEN_REPARSE_POINT)
	if dir {
		flags |= windows.FILE_FLAG_BACKUP_SEMANTICS
	}
	h, e := windows.CreateFile(name, windows.READ_CONTROL, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, flags, 0)
	if e != nil {
		return false
	}
	f := os.NewFile(uintptr(h), path)
	defer f.Close()
	var info windows.ByHandleFileInformation
	if windows.GetFileInformationByHandle(h, &info) != nil || info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return false
	}
	return (info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0) == dir && ledgerPrivateHandle(f)
}
func ledgerOpen(path string, create bool) (*os.File, error) {
	name, e := windows.UTF16PtrFromString(path)
	if e != nil {
		return nil, e
	}
	access, disposition := uint32(windows.GENERIC_READ|windows.READ_CONTROL), uint32(windows.OPEN_EXISTING)
	if create {
		access |= windows.GENERIC_WRITE
		disposition = windows.OPEN_ALWAYS
	}
	h, e := windows.CreateFile(name, access, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, disposition, windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(h), path)
	var info windows.ByHandleFileInformation
	if windows.GetFileInformationByHandle(h, &info) != nil || info.FileAttributes&(windows.FILE_ATTRIBUTE_REPARSE_POINT|windows.FILE_ATTRIBUTE_DIRECTORY) != 0 || !ledgerPrivateHandle(f) {
		f.Close()
		return nil, fail("invalid_input")
	}
	return f, nil
}
func ledgerLock(f *os.File, nonblocking bool) error {
	flags := uint32(windows.LOCKFILE_EXCLUSIVE_LOCK)
	if nonblocking {
		flags |= windows.LOCKFILE_FAIL_IMMEDIATELY
	}
	return windows.LockFileEx(windows.Handle(f.Fd()), flags, 0, 1, 0, &windows.Overlapped{})
}
func ledgerUnlock(f *os.File) error {
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &windows.Overlapped{})
}
func ledgerSecureTemp(f *os.File) error {
	if !ledgerPrivateHandle(f) {
		return fail("invalid_input")
	}
	return nil
}
func ledgerReplace(from, to string) error {
	a, e := windows.UTF16PtrFromString(from)
	if e != nil {
		return e
	}
	b, e := windows.UTF16PtrFromString(to)
	if e != nil {
		return e
	}
	return windows.MoveFileEx(a, b, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}

// MoveFileEx WRITE_THROUGH persists the replacement; directory FlushFileBuffers
// is unsupported by Windows. The new file was separately FlushFileBuffers'd by Sync.
func ledgerSyncDir(string) error { return nil }
