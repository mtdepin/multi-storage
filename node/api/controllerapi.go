package api

import (
	"context"
	"mtcloud.com/mtstorage/cmd/controller/app/informers/core/util"
)

// ControllerNode  is a low-level interface to the distribute network call
type ControllerNode interface {
	Version(ctx context.Context) string
	Event(context.Context, util.EventType, []byte) error
}
