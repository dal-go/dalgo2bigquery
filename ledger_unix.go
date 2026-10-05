//go:build linux || darwin

package bigquery

import (
	"os"
	"syscall"
)

func ledgerMkdir(dir string) error { return os.MkdirAll(dir, 0700) }
func ledgerPrivatePath(path string, dir bool) bool {
	s, e := os.Lstat(path)
	if e != nil {
		return false
	}
	if dir {
		return s.IsDir() && s.Mode().Perm() == 0700
	}
	return s.Mode().IsRegular() && s.Mode().Perm() == 0600
}
func ledgerPrivateFile(f *os.File) bool {
	s, e := f.Stat()
	return e == nil && s.Mode().IsRegular() && s.Mode().Perm() == 0600
}
func ledgerOpen(path string, create bool) (*os.File, error) {
	flags := os.O_RDONLY | syscall.O_NOFOLLOW
	mode := os.FileMode(0)
	if create {
		flags = os.O_CREATE | os.O_RDWR | syscall.O_NOFOLLOW
		mode = 0600
	}
	return os.OpenFile(path, flags, mode)
}
func ledgerLock(f *os.File, nonblocking bool) error {
	flags := syscall.LOCK_EX
	if nonblocking {
		flags |= syscall.LOCK_NB
	}
	return syscall.Flock(int(f.Fd()), flags)
}
func ledgerUnlock(f *os.File) error       { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }
func ledgerSecureTemp(f *os.File) error   { return f.Chmod(0600) }
func ledgerReplace(from, to string) error { return os.Rename(from, to) }
func ledgerSyncDir(dir string) error {
	f, e := os.Open(dir)
	if e != nil {
		return fail("invalid_input")
	}
	defer f.Close()
	if e = f.Sync(); e != nil {
		return fail("invalid_input")
	}
	return nil
}
