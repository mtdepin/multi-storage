package backend

import (
	"context"
	"fmt"
	"mtcloud.com/mtstorage/node/client"
	node_util "mtcloud.com/mtstorage/node/util"
	"mtcloud.com/mtstorage/pkg/logger"
	"time"
)

const (
	HealthCheckInterval = 30
	AliveInterval       = 300
)

// 定期检查chunker心跳
func (ns *NameServer) checkCk() {
	ticker := time.NewTicker(time.Second * HealthCheckInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				ns.update()
			}
		}
	}()
}

func (ns *NameServer) update() {
	now := time.Now()
	var keepNodes []*node_util.ChunkerNodeInfo
	for id, _ := range ns.chunkerNodes {
		if ns.chunkerNodes[id].State != node_util.State_Offline && now.After(ns.chunkerNodes[id].Time.Add(time.Second*HealthCheckInterval*2)) {
			//如果超过两次心跳周期，则把状态设置为 State_keep， 同时会主动向 chunker 节点发起健康询问
			ns.lock.Lock()
			ns.chunkerNodes[id].State = node_util.State_keep
			ns.lock.Unlock()
			keepNodes = append(keepNodes, ns.chunkerNodes[id])
		}
		if ns.chunkerNodes[id].State != node_util.State_Offline && now.After(ns.chunkerNodes[id].Time.Add(time.Second*AliveInterval)) {
			//5分钟检查不到心跳，则设置为离线
			ns.lock.Lock()
			ns.chunkerNodes[id].State = node_util.State_Offline
			ns.lock.Unlock()
		}
	}
	for _, v := range keepNodes {
		logger.Warnf("keep alive node: %s, url: %s ", v.Id, v.Endpoint)
		//主动探测节点健康状态
		chunker, err := client.CreateChunkerClient(context.Background(), v.Id)
		if err != nil {
			logger.Error(err)
			continue
		}
		ctx := client.WithTrack(nil)
		ctx = client.WithTimeout(ctx, 10)
		info, err := chunker.Health(ctx)
		if err != nil {
			logger.Error(err)
			continue
		}
		ns.AddOrUpdate(&info)
	}
}

func (ns *NameServer) AddOrUpdate(info *node_util.ChunkerNodeInfo) {
	ns.lock.Lock()
	defer ns.lock.Unlock()
	now := time.Now()
	key := fmt.Sprintf("%s_%d", info.Id, info.Region.RegionId)
	if now.After(info.Time.Add(time.Second * HealthCheckInterval)) {
		info.State = node_util.State_keep
	}
	ns.chunkerNodes[key] = info
}
