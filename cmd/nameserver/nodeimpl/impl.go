package nodeimpl

import (
	"context"

	"mtcloud.com/mtstorage/cmd/nameserver/backend"

	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/cmd/nameserver/metadata"
	"mtcloud.com/mtstorage/node/api"
	"mtcloud.com/mtstorage/node/util"
	"mtcloud.com/mtstorage/pkg/logger"
)

type NodeImpl struct {
	backend *backend.NameServer
}

func NewServerNodeImpl(ns *backend.NameServer) api.ServerNode {
	node := NodeImpl{
		backend: ns,
	}
	return &node
}

// TestNetwork 用来测试交互网络是否畅通
func (n *NodeImpl) TestNetwork(ctx context.Context) (bool, error) {
	return true, nil
}

func (n *NodeImpl) Version(ctx context.Context, v string) (string, error) {
	return "1.0", nil
}

func (n *NodeImpl) Heartbeat(ctx context.Context, info util.ChunkerNodeInfo) error {
	logger.Info("receive heartbeat")
	logger.Infof("url := %s", info.Endpoint)
	n.backend.AddOrUpdate(&info)
	return nil
}
func (n *NodeImpl) SaveObjectMeta(ctx context.Context, d util.ReWriteObjectInfo) error {
	ctx, span := trace.StartSpan(ctx, "SaveObjectMeta")
	defer span.End()

	o := &metadata.ObjectInfo{}
	o.Name = d.Name
	o.Cid = d.Cid
	o.Bucket = d.Bucket
	o.Dirname = d.DirName
	o.Content_length = d.ContenLength
	o.Etag = d.Etag
	o.Isdir = d.IsDir
	o.StorageClass = d.StorageClass
	o.Content_type = d.ContentType
	o.Acl = d.ACL
	o.CipherTextSize = d.CipherTextSize

	err := metadata.PutObjectInfo(ctx, o)
	if err != nil {
		return err
	}

	// 实际CID
	//o.Cid = d.ActualCid
	//if err := SendControllerEvent(util2.EVENTTYPE_ADD_OBJECT, o); err != nil {
	//	logger.Error(err)
	//}

	return nil
}
