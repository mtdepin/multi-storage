package client

import (
	"context"
	"mtcloud.com/mtstorage/node/util"
)

type ServerClient struct {
	Internal struct {
		TestNetwork    func(ctx context.Context) (bool, error)
		Version        func(ctx context.Context, v string) (string, error)
		Heartbeat      func(ctx context.Context, info util.ChunkerNodeInfo) error
		SaveObjectMeta func(ctx context.Context, d util.ReWriteObjectInfo) error
	}
}

func (c *ServerClient) Version(ctx context.Context, v string) (string, error) {
	return c.Internal.Version(ctx, v)
}

func (c *ServerClient) TestNetwork(ctx context.Context) (bool, error) {
	return c.Internal.TestNetwork(ctx)
}

func (c *ServerClient) Heartbeat(ctx context.Context, info util.ChunkerNodeInfo) error {
	ctx = WithBroadcast(ctx)
	return c.Internal.Heartbeat(ctx, info)
}

func (c *ServerClient) SaveObjectMeta(ctx context.Context, d util.ReWriteObjectInfo) error {
	return c.Internal.SaveObjectMeta(ctx, d)
}
