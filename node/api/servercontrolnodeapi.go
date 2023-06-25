package api

import (
	"context"

	"mtcloud.com/mtstorage/cmd/nameserver/metadata"
	"mtcloud.com/mtstorage/node/util"
)

// ServerControlNode  is a low-level interface to the distribute network call
type ServerControlNode interface {
	GetChunkerNode(context.Context) (util.ChunkerNodeInfo, error)

	GetBucketsLogging(context.Context) ([]metadata.BucketExternal, error)

	GetChunkerNodes(context.Context) ([]util.ChunkerNodeInfo, error)

	PutObjectCidInfo(context.Context, metadata.ObjectChunkInfo) error

	GetObjectCidInfos(context.Context) ([]metadata.ObjectChunkInfo, error)
}
