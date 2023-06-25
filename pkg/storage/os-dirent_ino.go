//go:build (linux || darwin) && !appengine
// +build linux darwin
// +build !appengine

package storage

import "syscall"

func direntInode(dirent *syscall.Dirent) uint64 {
	return dirent.Ino
}
