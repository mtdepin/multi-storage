package nodeimpl

import (
	"context"
	"errors"
	"mtcloud.com/mtstorage/cmd/nameserver/backend"

	"mtcloud.com/mtstorage/cmd/nameserver/metadata"
	"mtcloud.com/mtstorage/node/api"
	"mtcloud.com/mtstorage/node/util"
)

type ControlNodeImpl struct {
	backend *backend.NameServer
}

func NewControlNodeImpl(ns *backend.NameServer) api.ServerControlNode {
	node := ControlNodeImpl{
		backend: ns,
	}
	return &node
}

func (n *ControlNodeImpl) GetChunkerNode(ctx context.Context) (util.ChunkerNodeInfo, error) {
	l := n.backend.GetStorageInfoFromNameServer(ctx)
	if len(l) > 0 {
		return l[0], nil
	}
	return util.ChunkerNodeInfo{}, errors.New("not health node found")
}

func (n *ControlNodeImpl) GetBucketsLogging(ctx context.Context) ([]metadata.BucketExternal, error) {
	return metadata.GetBucketsLogging()
}

func (n *ControlNodeImpl) GetChunkerNodes(ctx context.Context) ([]util.ChunkerNodeInfo, error) {
	l := n.backend.GetStorageInfoFromNameServer(ctx)
	if len(l) > 0 {
		return l, nil
	}
	return []util.ChunkerNodeInfo{}, errors.New("not health nodes found")
}

func (n *ControlNodeImpl) PutObjectCidInfo(ctx context.Context, obj metadata.ObjectChunkInfo) error {
	return metadata.PutObjectCidInfo(ctx, obj)
}

func (n *ControlNodeImpl) GetObjectCidInfos(ctx context.Context) ([]metadata.ObjectChunkInfo, error) {
	return metadata.GetObjectCidInfos(ctx)
}
