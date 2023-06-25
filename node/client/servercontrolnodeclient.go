package client

import (
	"context"

	"mtcloud.com/mtstorage/cmd/nameserver/metadata"
	"mtcloud.com/mtstorage/node/util"
)

type ServerControlNodeClient struct {
	Internal struct {
		GetChunkerNode    func(context.Context) (util.ChunkerNodeInfo, error)
		GetBucketsLogging func(ctx context.Context) ([]metadata.BucketExternal, error)
		GetChunkerNodes   func(ctx context.Context) ([]util.ChunkerNodeInfo, error)
		PutObjectCidInfo  func(context.Context, metadata.ObjectChunkInfo) error
		GetObjectCidInfos func(context.Context) ([]metadata.ObjectChunkInfo, error)
	}
}

func (c *ServerControlNodeClient) GetBucketsLogging(ctx context.Context) ([]metadata.BucketExternal, error) {
	return c.Internal.GetBucketsLogging(ctx)
}

func (c *ServerControlNodeClient) GetChunkerNode(ctx context.Context) (util.ChunkerNodeInfo, error) {
	return c.Internal.GetChunkerNode(ctx)
}

func (c *ServerControlNodeClient) GetChunkerNodes(ctx context.Context) ([]util.ChunkerNodeInfo, error) {
	return c.Internal.GetChunkerNodes(ctx)
}

func (c *ServerControlNodeClient) PutObjectCidInfo(ctx context.Context, obj metadata.ObjectChunkInfo) error {
	return c.Internal.PutObjectCidInfo(ctx, obj)
}

func (c *ServerControlNodeClient) GetObjectCidInfos(ctx context.Context) ([]metadata.ObjectChunkInfo, error) {
	return c.Internal.GetObjectCidInfos(ctx)
}
