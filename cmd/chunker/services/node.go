package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	config2 "mtcloud.com/mtstorage/cmd/chunker/config"
	"mtcloud.com/mtstorage/cmd/chunker/engine"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/node/api"
	"mtcloud.com/mtstorage/node/client"
	node_util "mtcloud.com/mtstorage/node/util"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/util"
)

type Chunker struct {
	Id              string
	Endpoint        string
	Name            string
	NodeGroup       string
	NameServerGroup string
	Region          string
	NameServer      api.ServerNode
	storageEngine   *engine.Engine

	netSpeedCollect *util.NetSpeed
	master          []string
	fixNode         []string
	storageType     string // 选择上传的策略

	TempDir string
}

func NewChunkerNode(c *config2.ChunkerConfig) *Chunker {
	var node = &Chunker{}
	logger.Info("init chunker node")

	topic := c.Mq.Topic
	if topic == "" {
		panic("name is empty")
	}

	//node namegroup: topic#node_group
	ng := fmt.Sprintf("%s#%s", topic, c.Node.Node_group)
	node.NodeGroup = ng

	name := c.Node.Name
	if name == "" {
		panic("name is empty")
	}
	node.Name = name

	addr := c.Node.Api
	if addr == "" {
		panic("api is empty")
	}
	node.Endpoint = addr

	region := c.Node.Region
	if region == "" {
		panic("region is empty")
	}
	node.Region = region

	node_id := c.Node.Id
	if node_id == "" {
		panic("id is empty")
	}

	//format: CHUNKER-region_nodeGroup_id
	//node_id = fmt.Sprintf("CHUNKER-%s#%s#%s-%s", region, ng, node_id, util.GetRandString(5))
	node_id = fmt.Sprintf("Ck-%s#%s#%s-%s", region, ng, node_id, "")
	node.Id = node_id

	nsGroup := c.Node.NameServer_group
	if ng == "" {
		panic("name is empty")
	}
	node.NameServerGroup = fmt.Sprintf("%s#%s", topic, nsGroup)

	node.TempDir = c.TempDir

	//init storage backend
	engine, err := engine.NewEngine(c.Storage)
	if err != nil {
		logger.Error("init storage engine error: ", err)
		panic(err)
	}
	node.storageEngine = engine

	return node
}

func (ck *Chunker) Start(c *config2.ChunkerConfig) {
	logger.Info("init chunker node")

	//init netSpeedCollect
	ck.netSpeedCollect = util.NewNetSpeed(context.Background())

	ck.storageEngine.Start()
	//start hearbeat
	go ck.startHeartbeat()
}

func (ck *Chunker) GetData(ctx context.Context, cid string) (io.Reader, error) {
	return ck.storageEngine.Read(ctx, cid)
}

func (ck *Chunker) CIDExist(ctx context.Context, cid string) bool {
	return ck.storageEngine.Stat(ctx, cid) == nil
}

func (ck *Chunker) DeleteDataFromIPFS(ctx context.Context, cid string) error {
	return ck.storageEngine.Delete(ctx, cid)
}

func (ck *Chunker) WriteData(ctx context.Context, file io.Reader) (cid string, err error) {
	ctx, span := trace.StartSpan(ctx, "WriteData")
	defer span.End()

	return ck.storageEngine.Write(ctx, file)
}

func (ck *Chunker) CallBackNS(ctx context.Context, d node_util.ReWriteObjectInfo) error {
	ctx, span := trace.StartSpan(ctx, "CallBackNS")
	defer span.End()
	return ck.NameServer.SaveObjectMeta(client.WithTraceSpan(ctx, span), d)
}

func (ck *Chunker) startHeartbeat() {
	ctx := context.Background()
	if err := ck.NameServer.Heartbeat(ctx, ck.GetHeartbeatInfo()); err != nil {
		logger.Errorf("send heartbeat storageerror: ", err)
	}

	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			logger.Info("send heartbeat")
			hi := ck.GetHeartbeatInfo()
			if err := ck.NameServer.Heartbeat(ctx, hi); err != nil {
				logger.Error("send heartbeat storageerror: ", err)
			}
		}
	}
}

func (ck *Chunker) GetHeartbeatInfo() node_util.ChunkerNodeInfo {
	info := node_util.ChunkerNodeInfo{
		Id:       ck.Id,
		Endpoint: ck.Endpoint,
		Tcp:      "",
		State:    node_util.State_Health,
		Time:     time.Now(),
		Region: &node_util.Region{
			RegionId: 111,
			Name:     ck.Region,
		},
	}
	info.LatestNetSpeed = ck.netSpeedCollect.KBSpeed()
	var available, usage uint64

	stat, err := ck.storageEngine.RepoStat()

	if err == nil {
		available = stat.StorageMax - stat.RepoSize
		usage = stat.RepoSize
	}
	info.AvailableSpace = available
	info.UsedSpace = usage
	return info

}

func (ck *Chunker) GetDagTree(ctx context.Context, cid string) (interface{}, error) {
	ctx, span := trace.StartSpan(ctx, "GetDagTree")
	defer span.End()

	return ck.storageEngine.DagTree(ctx, cid)

}

func (ck *Chunker) GetDagSize(ctx context.Context, cid string) string {
	ctx, span := trace.StartSpan(ctx, "GetDagSize")
	defer span.End()

	dz, err := ck.storageEngine.DagSize(ctx, cid)
	if err != nil {
		logger.Error(err)
		return ""
	}
	return dz
}

// FixCid 下载出错时候修复文件
func (ck *Chunker) FixCid(cid string) {
	for _, v := range ck.fixNode {
		for i := 0; i < 3; i++ {
			url := v
			payload := fmt.Sprintf("cid=%s", cid)
			c := &http.Client{}
			req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(payload))
			if err != nil {
				logger.Error("fixCid", err)
				continue
			}
			req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
			_, err = c.Do(req)
			if err != nil {
				logger.Error("fixCid", err)
				continue
			}
			break
		}
	}
}

func (ck *Chunker) GetObjectDagTree(ctx context.Context, cid string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	result["Hash"] = cid

	size, err := strconv.Atoi(ck.GetDagSize(ctx, cid))
	if err != nil {
		logger.Error("dag size Atoi fail: ", err)
	}
	result["Tsize"] = size

	// todo 终止条件,ipfs分片大小 45623854
	if size <= 45623854 {
		return result, nil
	}

	lsLinks, err := ck.storageEngine.List(cid)
	if err != nil {
		return result, err
	}

	links := make([]map[string]interface{}, 0)
	for _, lsLink := range lsLinks {
		link, err := ck.GetObjectDagTree(ctx, lsLink.Hash)
		if err != nil {
			logger.Error("GetObjectDagTree fail: ", err)
		}
		links = append(links, link)
	}

	result["Links"] = links

	return result, nil
}

func (ck *Chunker) AddObjectCid(cids []string) ([]map[string]interface{}, error) {
	if len(cids) <= 0 {
		return nil, errors.New("the cids is empty")
	}

	//nodes := ck.ipfsNodes
	//if len(nodes) < 1 {
	//	return nil, errors.New("on ipfs node exist")
	//}

	// 记录分片的执行状态
	exeStatus := make([]map[string]interface{}, 0)

	// 只存在一个ipfs node
	//if len(nodes) == 1 {
	//	for _, cid := range cids {
	//		exeStatus = append(exeStatus, map[string]interface{}{
	//			"cid":      cid,
	//			"ipfsNode": ck.master[0],
	//			"status":   true,
	//		})
	//	}
	//	return exeStatus, nil
	//}

	for flag, cid := range cids {
		//node := nodes[flag%len(nodes)]
		//exeStatus = append(exeStatus, map[string]interface{}{
		//	"cid":      cid,
		//	"ipfsNode": ck.master[flag%len(nodes)],
		//	"status":   true,
		//})
		// todo 执行 pin add 命令，传入不存在cid时不会返回
		if err := ck.storageEngine.Pin(cid); err != nil {
			exeStatus[flag]["status"] = false
			logger.Errorf("%s ipfs pin add fail: %s", cid, err)
		}
	}

	return exeStatus, nil
}
