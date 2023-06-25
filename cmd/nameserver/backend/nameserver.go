package backend

import (
	"context"
	"encoding/json"
	"fmt"
	config2 "mtcloud.com/mtstorage/cmd/nameserver/config"
	"mtcloud.com/mtstorage/pkg/logger"
	"sync"

	"go.opencensus.io/trace"
	util2 "mtcloud.com/mtstorage/cmd/controller/app/informers/core/util"
	"mtcloud.com/mtstorage/node/api"
	node_util "mtcloud.com/mtstorage/node/util"
)

type NameServer struct {
	Id        string
	Endpoint  string
	Name      string
	NodeGroup string
	Region    string
	//the nodes keep heartbeat to nameserver
	chunkerNodes map[string]*node_util.ChunkerNodeInfo
	Controller   api.ControllerNode

	lock sync.RWMutex
}

func NewNameServer(c *config2.NameServerConfig) *NameServer {
	ns := &NameServer{
		chunkerNodes: make(map[string]*node_util.ChunkerNodeInfo),
		lock:         sync.RWMutex{},
	}
	topic := c.Mq.Topic
	// 格式: topic#node_group
	ns.NodeGroup = fmt.Sprintf("%s#%s", topic, c.Node.Node_group)
	ns.Name = c.Node.Name
	ns.Endpoint = c.Node.Api
	ns.Region = c.Node.Region
	//格式: NAMESERVER-redion#nodeGroup#id  NAMESERVER-区域#主题#组#节点id-随机值
	node_id := fmt.Sprintf("NS-%s#%s#%s-%s", ns.Region, ns.NodeGroup, c.Node.Id, "")
	ns.Id = node_id

	return ns
}

func (ns *NameServer) Start() {
	ns.checkCk()

}

func (ns *NameServer) GetStorageInfoFromNameServer(ctx context.Context) []node_util.ChunkerNodeInfo {
	_, span := trace.StartSpan(ctx, "GetStorageInfoFromNameServer")
	defer span.End()

	ret := make([]node_util.ChunkerNodeInfo, 0)
	ns.lock.RLock()
	defer ns.lock.RUnlock()
	for _, v := range ns.chunkerNodes {
		if v.State == node_util.State_Health {
			ret = append(ret, *v)
		}
	}
	return ret
}

// SendControllerEvent 向controller发送一个事件
func (ns *NameServer) SendControllerEvent(et util2.EventType, entity interface{}) error {
	//todo; in a new goroutine??
	jsonbyte, _ := json.Marshal(entity)
	//track it
	//err := ns.Controller.Event(client.WithTrack(nil), et, jsonbyte)
	//untrack it
	err := ns.Controller.Event(context.TODO(), et, jsonbyte)
	if err != nil {
		logger.Error(err)
		return err
	}
	return nil
}
