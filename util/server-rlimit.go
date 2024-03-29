package util

import (
	"runtime/debug"

	"github.com/minio/pkg/sys"
)

func SetMaxResources() (err error) {
	// Set the Go runtime max threads threshold to 90% of kernel setting.
	sysMaxThreads, mErr := sys.GetMaxThreads()
	if mErr == nil {
		mtStorageMaxThreads := (sysMaxThreads * 90) / 100
		// Only set max threads if it is greater than the default one
		if mtStorageMaxThreads > 10000 {
			debug.SetMaxThreads(mtStorageMaxThreads)
		}
	}

	var maxLimit uint64

	// Set open files limit to maximum.
	if _, maxLimit, err = sys.GetMaxOpenFileLimit(); err != nil {
		return err
	}

	if err = sys.SetMaxOpenFileLimit(maxLimit, maxLimit); err != nil {
		return err
	}

	// Set max memory limit as current memory limit.
	if _, maxLimit, err = sys.GetMaxMemoryLimit(); err != nil {
		return err
	}

	err = sys.SetMaxMemoryLimit(maxLimit, maxLimit)
	return err
}
