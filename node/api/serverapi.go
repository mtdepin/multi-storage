package api

import (
	"context"
	"mtcloud.com/mtstorage/node/util"
)

// ServerNode API is a low-level interface to the distribute network call
type ServerNode interface {
	TestNetwork(ctx context.Context) (bool, error)
	Version(context.Context, string) (string, error)
	Heartbeat(context.Context, util.ChunkerNodeInfo) error
	SaveObjectMeta(ctx context.Context, info util.ReWriteObjectInfo) error
}
