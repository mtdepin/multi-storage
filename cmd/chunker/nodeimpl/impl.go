package nodeimpl

import (
	"context"
	"mtcloud.com/mtstorage/cmd/chunker/services"
	"mtcloud.com/mtstorage/node/api"
	node_util "mtcloud.com/mtstorage/node/util"
)

type NodeImpl struct {
	backend *services.Chunker
}

func NewChunkerNodeImpl(backend *services.Chunker) api.ChunkerNode {
	cn := NodeImpl{
		backend: backend,
	}
	return &cn
}

func (n *NodeImpl) Version(ctx context.Context, v string) (string, error) {
	return "1.0", nil
}

func (n *NodeImpl) Health(ctx context.Context) (node_util.ChunkerNodeInfo, error) {
	hi := n.backend.GetHeartbeatInfo()
	return hi, nil
}

func (n *NodeImpl) Pin(ctx context.Context) error {
	return nil
}

func (n *NodeImpl) UnPin(ctx context.Context) error {
	return nil
}
