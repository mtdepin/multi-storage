package client

import (
	"context"
	"mtcloud.com/mtstorage/node/util"
)

type ChunkerClient struct {
	Internal struct {
		TestNetwork func(ctx context.Context) (bool, error)
		Version     func(ctx context.Context, v string) (string, error)
		Health      func(ctx context.Context) (util.ChunkerNodeInfo, error)
		Pin         func(ctx context.Context) error
		UnPin       func(ctx context.Context) error
	}
}

func (c *ChunkerClient) Version(ctx context.Context, v string) (string, error) {
	return c.Internal.Version(ctx, v)
}

func (c *ChunkerClient) Pin(ctx context.Context) error {
	return c.Internal.Pin(ctx)
}

func (c *ChunkerClient) UnPin(ctx context.Context) error {
	return c.Internal.Pin(ctx)
}

func (c *ChunkerClient) Health(ctx context.Context) (util.ChunkerNodeInfo, error) {
	return c.Internal.Health(ctx)
}
