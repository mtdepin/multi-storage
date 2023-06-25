package ipfs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"mtcloud.com/mtstorage/cmd/nameserver/metadata"

	"mtcloud.com/mtstorage/node/util"

	"github.com/tidwall/gjson"

	"mtcloud.com/mtstorage/cmd/controller/app/api/core"
	coreinformers "mtcloud.com/mtstorage/cmd/controller/app/informers/core"
	"mtcloud.com/mtstorage/cmd/controller/app/informers/tools/cache"
	"mtcloud.com/mtstorage/cmd/controller/app/util/workqueue"

	"mtcloud.com/mtstorage/cmd/controller/app/clientbuilder"
	"mtcloud.com/mtstorage/node/client"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/pkg/runtime"
)

type Controller struct {
	queue workqueue.RateLimitingInterface

	nameserverClient *clientbuilder.NameserverClient
}

func NewIpfsCidAnalysisController(objectInformer coreinformers.Informer, nscli *clientbuilder.NameserverClient) *Controller {

	c := &Controller{
		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "object"),

		nameserverClient: nscli,
	}

	objectInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			logger.Info("on add object")
			c.queue.AddAfter(obj, time.Second)
		},
		//UpdateFunc: e.updateBucket,
		DeleteFunc: func(obj interface{}) {
			logger.Info("on delete object")
			//todo: delete object
		},
	})

	return c
}

func (c *Controller) Run(workers int, stopCh <-chan struct{}) {
	go func() {
		defer runtime.HandleCrash()
	}()

	for i := 0; i < workers; i++ {
		go c.worker()
	}

	<-stopCh
}

func (c *Controller) worker() {
	for c.processNextWorkItem() {
	}

}

func (c *Controller) processNextWorkItem() bool {
	obj, quit := c.queue.Get()
	if quit {
		return false
	}
	o := obj.(*core.Object)

	logger.Infof("new object name: %s", o.ObjectInfo.Name)
	if o.Cid == "" {
		logger.Errorf("%s cid is empty", o.ObjectInfo.Name)
		return true
	}

	// 获取object的分片
	objectDagTree, err := c.getObjectDagTree(o.Cid)
	if err != nil {
		logger.Errorf("%s get object tree fail: %s", o.Cid, err)
	}
	var objectCids []string
	objectCids = c.objectSlice(objectDagTree, objectCids)

	// 获取全部cs实例
	CSNodes, err := c.nameserverClient.GetChunkerNodes(client.WithTrack(nil))
	if err != nil {
		logger.Error("get chunkerNodes err: ", err)
	}
	CSNodeCount := len(CSNodes)

	CSNodeMaps := make([]map[string]interface{}, 0)

	// 只有一个cs节点
	if CSNodeCount <= 1 {
		ipfsMaps, err := c.addObjectCid(objectCids, CSNodes[0])
		if err != nil {
			logger.Errorf("%s cs node AddObjectCid fail: %s", CSNodes[0].Id, err)
		}
		CSNodeMap := make(map[string]interface{})
		CSNodeMap["CSNodeInfo"] = CSNodes[0]
		CSNodeMap["ipfsMaps"] = ipfsMaps
		CSNodeMaps = append(CSNodeMaps, CSNodeMap)
	} else { // 存在多个cs节点，将分片信息进行分配
		CSNodeCids := make(map[int][]string)
		for i := 0; i < CSNodeCount; i++ {
			CSNodeCids[i] = []string{}
		}

		for index, cid := range objectCids {
			CSNodeCids[index%CSNodeCount] = append(CSNodeCids[index%CSNodeCount], cid)
		}

		for nodeNum, nodeData := range CSNodeCids {
			if len(nodeData) > 0 {
				ipfsMaps, err := c.addObjectCid(nodeData, CSNodes[nodeNum])
				if err != nil {
					logger.Errorf("%s cs node AddObjectCid fail: %s", CSNodes[nodeNum].Id, err)
				}

				CSNodeMap := make(map[string]interface{})
				CSNodeMap["CSNodeInfo"] = CSNodes[nodeNum]
				CSNodeMap["ipfsMaps"] = ipfsMaps
				CSNodeMaps = append(CSNodeMaps, CSNodeMap)
			}
		}
	}

	// 所有分片执行状态
	status := 1

LOOP:
	for _, csNode := range CSNodeMaps {
		for _, ipfsNode := range csNode["ipfsMaps"].([]map[string]interface{}) {
			if !ipfsNode["status"].(bool) {
				status = 0
				break LOOP
			}
		}
	}

	ipfsTreeMap := make(map[string]interface{})
	ipfsTreeMap["CSNodeMaps"] = CSNodeMaps
	ipfsTreeMap["cid"] = o.ObjectInfo.Cid
	ipfsTreeMap["bucket"] = o.ObjectInfo.Bucket
	if o.ObjectInfo.Dirname == "/" {
		ipfsTreeMap["name"] = fmt.Sprintf("%s%s", o.ObjectInfo.Dirname, o.ObjectInfo.Name)
	} else {
		ipfsTreeMap["name"] = fmt.Sprintf("%s/%s", o.ObjectInfo.Dirname, o.ObjectInfo.Name)
	}
	ipfsTreeMap["status"] = status

	objectCidInfo := metadata.ObjectChunkInfo{}
	objectCidInfo.Cid = o.ObjectInfo.Cid
	objectCidInfo.Name = ipfsTreeMap["name"].(string)
	objectCidInfo.Bucket = o.ObjectInfo.Bucket
	// objectCidInfo.Status = status
	objectCidInfo.IpIDTree = objectDagTree
	ipfsTreeMapToJson, err := json.Marshal(ipfsTreeMap)
	if err != nil {
		logger.Error("ipfsTreeMap Marshal fail: ", err)
	}

	objectCidInfo.IpIDMap = string(ipfsTreeMapToJson)

	go c.putObjectCidInfo(objectCidInfo)

	return true
}

func (c *Controller) getObjectDagTree(cid string) (string, error) {
	node, err := c.nameserverClient.GetChunkerNode(client.WithTrack(nil))
	if err != nil {
		logger.Error("get chunkerNode err: ", err)
		return "", err
	}

	url := fmt.Sprintf("http://%s/cs/v1/getObjectDagTree", node.Endpoint)
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		logger.Error("make request err: ", err)
		return "", err
	}

	q := request.URL.Query()
	q.Add("cid", cid)
	request.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		logger.Error("get object cid err: ", err)
		return "", err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Error("read response data fail: ", err)
	}

	return string(data), nil
}

func (c *Controller) objectSlice(objectDagTree string, objectCids []string) []string {
	links := gjson.Get(objectDagTree, "Links").Array()
	if len(links) <= 0 {
		objectCids = append(objectCids, gjson.Get(objectDagTree, "Hash").String())
		return objectCids
	}

	for i := 0; i < len(links); i++ {
		objectCids = c.objectSlice(links[i].String(), objectCids)
	}

	return objectCids
}

func (c *Controller) addObjectCid(cid []string, node util.ChunkerNodeInfo) ([]map[string]interface{}, error) {
	logger.Infof("%s cs node executing", node.Endpoint)
	url := fmt.Sprintf("http://%s/cs/v1/addObjectCid", node.Endpoint)

	reqBody, err := json.Marshal(cid)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		logger.Error("make request err: ", err)
		return nil, err
	}

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		logger.Error("addObjectCid fail: ", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Errorf("cid: %s addObjectCid fail", cid)
		return nil, errors.New(fmt.Sprintf("cid: %s addObjectCid fail", cid))
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Error("addObjectCid fail: ", err)
		return nil, err
	}

	exeStatus := make([]map[string]interface{}, 0)
	if err = json.Unmarshal(respBody, &exeStatus); err != nil {
		logger.Error("respBody Unmarshal fail: ", err)
		return nil, err
	}

	return exeStatus, nil
}

func (c *Controller) putObjectCidInfo(obj metadata.ObjectChunkInfo) error {
	if err := c.nameserverClient.PutObjectCidInfo(client.WithTrack(nil), obj); err != nil {
		logger.Errorf("%s PutObjectCidInfo err: %s", obj, err)
	}

	return nil
}

func (c *Controller) getObjectCidInfos() ([]metadata.ObjectChunkInfo, error) {
	return c.nameserverClient.GetObjectCidInfos(client.WithTrack(nil))
}
