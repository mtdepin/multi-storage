package clientbuilder

import (
	"fmt"

	"mtcloud.com/mtstorage/pkg/config"
	"mtcloud.com/mtstorage/pkg/logger"
)

type SimpleControllerClientBuilder struct {
	nscn *NameserverClient //name server controlnode client
}

func CreateControllerClientBuilder() SimpleControllerClientBuilder {
	tp := config.GetString("mq.topic")
	sg := config.GetString("node.server_group")
	nsTopic := fmt.Sprintf("%s#%s", tp, sg)
	client, err := CreateServerControlNodeClient(nil, nsTopic)
	//client, err := CreateServerControlNodeClient(nil, "rpc_lyc#cluster")
	if err != nil {
		logger.Error(err)
	}

	return SimpleControllerClientBuilder{
		nscn: client,
	}
}

// NameserverClient nameServer client
func (ccb *SimpleControllerClientBuilder) NameserverClient() *NameserverClient {
	return ccb.nscn
}
