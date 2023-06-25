package api

import (
	"context"
	"mtcloud.com/mtstorage/node/util"
)

// ChunkerNode API is a low-level interface to the distribute network call
type ChunkerNode interface {
	Version(context.Context, string) (string, error)
	Pin(context.Context) error
	UnPin(context.Context) error
	Health(context.Context) (util.ChunkerNodeInfo, error)
}
