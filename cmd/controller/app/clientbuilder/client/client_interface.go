package client

import "mtcloud.com/mtstorage/node/api"

type ClientInterface interface {
	api.ServerControlNode
	List() interface{}
	Watch() interface{}
}
