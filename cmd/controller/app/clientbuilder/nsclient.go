package clientbuilder

import (
	"context"
	"mtcloud.com/mtstorage/node/api"
	"mtcloud.com/mtstorage/node/client"
	"mtcloud.com/mtstorage/pkg/logger"
)

// NameserverClient name server client
type NameserverClient struct {
	api.ServerControlNode
}

func CreateServerControlNodeClient(ctx context.Context, to string) (*NameserverClient, error) {
	c, err := client.CreateServerControlNodeClient(ctx, to)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	return &NameserverClient{
		c,
	}, nil
}

//func (c *NameserverClient) ListAllBucket() interface{} {
//	re, _ := c.ServerControlNode.ListAllBucket(context.TODO())
//	return re
//}

func (c *NameserverClient) List() interface{} {
	return nil
}

func (c *NameserverClient) Watch() interface{} {

	return nil
}
