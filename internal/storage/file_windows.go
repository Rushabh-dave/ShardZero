package storage

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var lockFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("LockFileEx")

func lockFile(f *os.File) error {
	var overlapped syscall.Overlapped
	r, _, err := lockFileEx.Call(f.Fd(), 3, 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if r == 0 {
		return fmt.Errorf("LockFileEx: %w", err)
	}
	return nil // The OS releases the lock on close or process exit.
}

// Windows FlushFileBuffers is invoked by File.Sync. Directory fsync is not
// available here; power-loss guarantees for newly created paths are not claimed.
func syncDirectory(string) error { return nil }
