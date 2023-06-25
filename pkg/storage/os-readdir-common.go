package storage

// Options for ReadDir function call
type readDirOpts struct {
	// The maximum number of entries to return
	count int
	// Follow directory symlink
	followDirSymlink bool
}

// Return all the entries at the directory dirPath.
func ReadDir(dirPath string) (entries []string, err error) {
	return readDirWithOpts(dirPath, readDirOpts{count: -1})
}

// Return up to count entries at the directory dirPath.
func readDirN(dirPath string, count int) (entries []string, err error) {
	return readDirWithOpts(dirPath, readDirOpts{count: count})
}
