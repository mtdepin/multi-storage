package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	pathutil "path"
	"runtime"
	"strings"

	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/pkg/logger"
	error2 "mtcloud.com/mtstorage/pkg/storageerror"
	"mtcloud.com/mtstorage/util"
)

// FsRemoveFile Removes only the file at given path does not remove
// any parent directories, handles long paths for
// windows automatically.
func FsRemoveFile(ctx context.Context, filePath string) (err error) {
	if filePath == "" {
		logger.Error(ctx, error2.ErrInvalidArgument)
		return error2.ErrInvalidArgument
	}

	if err = checkPathLength(filePath); err != nil {
		logger.Error(err)
		return err
	}

	if err = os.Remove(filePath); err != nil {
		if err = OsErrToFileErr(err); err != ErrFileNotFound {
			logger.Error(err)
		}
	}

	return err
}

// Removes all files and folders at a given path, handles
// long paths for windows automatically.
func FsRemoveAll(ctx context.Context, dirPath string) (err error) {
	if dirPath == "" {
		logger.Error(error2.ErrInvalidArgument)
		return error2.ErrInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		logger.Error(err)
		return err
	}

	if err = removeAll(dirPath); err != nil {
		if osIsPermission(err) {
			logger.Error(errVolumeAccessDenied)
			return errVolumeAccessDenied
		} else if isSysErrNotEmpty(err) {
			logger.Error(errVolumeNotEmpty)
			return errVolumeNotEmpty
		}
		logger.Error(err)
		return err
	}

	return nil
}

// Removes a directory only if its empty, handles long
// paths for windows automatically.
func fsRemoveDir(ctx context.Context, dirPath string) (err error) {
	if dirPath == "" {
		logger.Error(error2.ErrInvalidArgument)
		return error2.ErrInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		logger.Error(err)
		return err
	}

	if err = os.Remove((dirPath)); err != nil {
		if OsIsNotExist(err) {
			return ErrVolumeNotFound
		} else if isSysErrNotEmpty(err) {
			return errVolumeNotEmpty
		}
		logger.Error(err)
		return err
	}

	return nil
}

// Creates a new directory, parent dir should exist
// otherwise returns an storageerror. If directory already
// exists returns an storageerror. Windows long paths
// are handled automatically.
func fsMkdir(ctx context.Context, dirPath string) (err error) {
	if dirPath == "" {
		logger.Error(error2.ErrInvalidArgument)
		return error2.ErrInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		logger.Error(err)
		return err
	}

	if err = os.Mkdir((dirPath), 0777); err != nil {
		switch {
		case osIsExist(err):
			return errVolumeExists
		case osIsPermission(err):
			logger.Error(errDiskAccessDenied)
			return errDiskAccessDenied
		case isSysErrNotDir(err):
			// File path cannot be verified since
			// one of the parents is a file.
			logger.Error(errDiskAccessDenied)
			return errDiskAccessDenied
		case isSysErrPathNotFound(err):
			// Add specific case for windows.
			logger.Error(errDiskAccessDenied)
			return errDiskAccessDenied
		default:
			logger.Error(err)
			return err
		}
	}

	return nil
}

// checkPathLength - returns storageerror if given path name length more than 255
func checkPathLength(pathName string) error {
	// Apple OS X path length is limited to 1016
	if runtime.GOOS == "darwin" && len(pathName) > 1016 {
		return errFileNameTooLong
	}

	// Disallow more than 1024 characters on windows, there
	// are no known name_max limits on Windows.
	if runtime.GOOS == "windows" && len(pathName) > 1024 {
		return errFileNameTooLong
	}

	// On Unix we reject paths if they are just '.', '..' or '/'
	if pathName == "." || pathName == ".." || pathName == slashSeparator {
		return ErrFileAccessDenied
	}

	// Check each path segment length is > 255 on all Unix
	// platforms, look for this value as NAME_MAX in
	// /usr/include/linux/limits.h
	var count int64
	for _, p := range pathName {
		switch p {
		case '/':
			count = 0 // Reset
		case '\\':
			//if runtime.GOOS == globalWindowsOSName {
			//	count = 0
			//}
		default:
			count++
			if count > 255 {
				return errFileNameTooLong
			}
		}
	} // Success.
	return nil
}

// fsStat is a low level call which validates input arguments
// and checks input length upto supported maximum. Does
// not perform any higher layer interpretation of files v/s
// directories. For higher level interpretation look at
// fsStatFileDir, FsStatFile, fsStatDir.
func fsStat(ctx context.Context, statLoc string) (os.FileInfo, error) {
	if statLoc == "" {
		logger.Error(ctx, error2.ErrInvalidArgument)
		return nil, error2.ErrInvalidArgument
	}
	if err := checkPathLength(statLoc); err != nil {
		logger.Error(err)
		return nil, err
	}
	fi, err := os.Stat(statLoc)
	if err != nil {
		return nil, err
	}

	return fi, nil
}

// Lookup if volume exists, returns volume attributes upon success.
func fsStatVolume(ctx context.Context, volume string) (os.FileInfo, error) {
	fi, err := fsStat(ctx, volume)
	if err != nil {
		if OsIsNotExist(err) {
			return nil, ErrVolumeNotFound
		} else if osIsPermission(err) {
			return nil, errVolumeAccessDenied
		}
		return nil, err
	}

	if !fi.IsDir() {
		return nil, errVolumeAccessDenied
	}

	return fi, nil
}

// Lookup if directory exists, returns directory attributes upon success.
func fsStatDir(ctx context.Context, statDir string) (os.FileInfo, error) {
	fi, err := fsStat(ctx, statDir)
	if err != nil {
		err = OsErrToFileErr(err)
		if err != ErrFileNotFound {
			logger.Error(err)
		}
		return nil, err
	}
	if !fi.IsDir() {
		return nil, ErrFileNotFound
	}
	return fi, nil
}

// Lookup if file exists, returns file attributes upon success.
func FsStatFile(ctx context.Context, statFile string) (os.FileInfo, error) {
	ctx, span := trace.StartSpan(ctx, "FsStatFile")
	defer span.End()
	fi, err := fsStat(ctx, statFile)
	if err != nil {
		err = OsErrToFileErr(err)
		if err != ErrFileNotFound {
			logger.Error(err)
		}
		return nil, err
	}
	if fi.IsDir() {
		return nil, ErrFileNotFound
	}
	return fi, nil
}

// Returns if the filePath is a regular file.
func fsIsFile(ctx context.Context, filePath string) bool {
	fi, err := fsStat(ctx, filePath)
	if err != nil {
		return false
	}
	return fi.Mode().IsRegular()
}

// Opens the file at given path, optionally from an offset. Upon success returns
// a readable stream and the size of the readable stream.
func fsOpenFile(ctx context.Context, readPath string, offset int64) (io.ReadCloser, int64, error) {
	if readPath == "" || offset < 0 {
		logger.Error(error2.ErrInvalidArgument)
		return nil, 0, error2.ErrInvalidArgument
	}
	if err := checkPathLength(readPath); err != nil {
		logger.Error(err)
		return nil, 0, err
	}

	fr, err := os.Open(readPath)
	if err != nil {
		return nil, 0, OsErrToFileErr(err)
	}

	// Stat to get the size of the file at path.
	st, err := fr.Stat()
	if err != nil {
		err = OsErrToFileErr(err)
		if err != ErrFileNotFound {
			logger.Error(err)
		}
		return nil, 0, err
	}

	// Verify if its not a regular file, since subsequent Seek is undefined.
	if !st.Mode().IsRegular() {
		return nil, 0, errIsNotRegular
	}

	// Seek to the requested offset.
	if offset > 0 {
		_, err = fr.Seek(offset, io.SeekStart)
		if err != nil {
			logger.Error(err)
			return nil, 0, err
		}
	}

	// Success.
	return fr, st.Size(), nil
}

// FsCreateFile Creates a file and copies data from incoming reader.
func FsCreateFile(ctx context.Context, filePath string, reader io.Reader, fallocSize int64) (int64, error) {
	if filePath == "" || reader == nil {
		logger.Error(error2.ErrInvalidArgument)
		return 0, error2.ErrInvalidArgument
	}

	if err := checkPathLength(filePath); err != nil {
		logger.Error(err)
		return 0, err
	}

	if err := mkdirAll(pathutil.Dir(filePath), 0777); err != nil {
		switch {
		case osIsPermission(err):
			return 0, ErrFileAccessDenied
		case osIsExist(err):
			return 0, ErrFileAccessDenied
		case isSysErrIO(err):
			return 0, errFaultyDisk
		case isSysErrInvalidArg(err):
			return 0, errUnsupportedDisk
		case isSysErrNoSpace(err):
			return 0, errDiskFull
		}
		return 0, err
	}

	flags := os.O_CREATE | os.O_WRONLY
	//if globalFSOSync {
	//	flags = flags | os.O_SYNC
	//}
	writer, err := os.OpenFile(filePath, flags, 0666)
	if err != nil {
		return 0, OsErrToFileErr(err)
	}
	defer writer.Close()

	bytesWritten, err := io.Copy(writer, reader)
	if err != nil {
		logger.Error(err)
		return 0, err
	}

	return bytesWritten, nil
}

// Renames source path to destination path, creates all the
// missing parents if they don't exist.
func fsRenameFile(ctx context.Context, sourcePath, destPath string) error {
	if err := checkPathLength(sourcePath); err != nil {
		logger.Error(err)
		return err
	}
	if err := checkPathLength(destPath); err != nil {
		logger.Error(err)
		return err
	}

	if err := renameAll(sourcePath, destPath); err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

func deleteFile(basePath, deletePath string, recursive bool) error {
	if basePath == "" || deletePath == "" {
		return nil
	}
	isObjectDir := util.HasSuffix(deletePath, slashSeparator)
	basePath = pathutil.Clean(basePath)
	deletePath = pathutil.Clean(deletePath)
	if !strings.HasPrefix(deletePath, basePath) || deletePath == basePath {
		return nil
	}

	var err error
	if recursive {
		os.RemoveAll(deletePath)
	} else {
		err = os.Remove(deletePath)
	}
	if err != nil {
		switch {
		case isSysErrNotEmpty(err):
			// if object is a directory, but if its not empty
			// return FileNotFound to indicate its an empty prefix.
			if isObjectDir {
				return ErrFileNotFound
			}
			// Ignore errors if the directory is not empty. The server relies on
			// this functionality, and sometimes uses recursion that should not
			// storageerror on parent directories.
			return nil
		case OsIsNotExist(err):
			return ErrFileNotFound
		case osIsPermission(err):
			return ErrFileAccessDenied
		case isSysErrIO(err):
			return errFaultyDisk
		default:
			return err
		}
	}

	deletePath = pathutil.Dir(deletePath)

	// Delete parent directory obviously not recursively. Errors for
	// parent directories shouldn't trickle down.
	deleteFile(basePath, deletePath, false)

	return nil
}

// fsDeleteFile is a wrapper for deleteFile(), after checking the path length.
func fsDeleteFile(ctx context.Context, basePath, deletePath string) error {
	if err := checkPathLength(basePath); err != nil {
		logger.Error(err)
		return err
	}

	if err := checkPathLength(deletePath); err != nil {
		logger.Error(err)
		return err
	}

	if err := deleteFile(basePath, deletePath, false); err != nil {
		if err != ErrFileNotFound {
			logger.Error(err)
		}
		return err
	}
	return nil
}

// fsRemoveMeta safely removes a locked file and takes care of Windows special case
func fsRemoveMeta(ctx context.Context, basePath, deletePath, tmpDir string) error {
	// Special case for windows please read through.
	return fsDeleteFile(ctx, basePath, deletePath)
}

// PathJoin - like path.Join() but retains trailing slashSeparator of the last element
func PathJoin(elem ...string) string {
	trailingSlash := ""
	if len(elem) > 0 {
		if util.HasSuffix(elem[len(elem)-1], slashSeparator) {
			trailingSlash = slashSeparator
		}
	}
	return path.Join(elem...) + trailingSlash
}

func ParseObject(object string) (prefix, objName string) {
	if !strings.HasSuffix(object, slashSeparator) {
		object = fmt.Sprintf("%s%s", slashSeparator, object)
	}
	prefix = path.Dir(object)
	objName = path.Base(object)
	return
}
