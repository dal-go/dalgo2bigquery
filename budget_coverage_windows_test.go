//go:build windows

package bigquery

import "golang.org/x/sys/windows"

func init() {
	makeFileNonPrivate = func(path string) error {
		sd, err := windows.SecurityDescriptorFromString("D:P(A;;FA;;;WD)")
		if err != nil {
			return err
		}
		acl, _, err := sd.DACL()
		if err != nil {
			return err
		}
		return windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, acl, nil)
	}
}
