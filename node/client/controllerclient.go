package client

import (
	"context"
	"mtcloud.com/mtstorage/cmd/controller/app/informers/core/util"
)

type ControllerClient struct {
	Internal struct {
		Version func(ctx context.Context) string
		Event   func(ctx context.Context, et util.EventType, payload []byte) error
	}
}

func (c *ControllerClient) Version(ctx context.Context) string {
	return c.Internal.Version(ctx)
}

func (c *ControllerClient) Event(ctx context.Context, et util.EventType, payload []byte) error {
	return c.Internal.Event(ctx, et, payload)
}
