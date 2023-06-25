package bucket

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"mtcloud.com/mtstorage/cmd/controller/app/api/core"
	"mtcloud.com/mtstorage/cmd/controller/app/clientbuilder"
	coreinformers "mtcloud.com/mtstorage/cmd/controller/app/informers/core"
	"mtcloud.com/mtstorage/cmd/controller/app/informers/tools/cache"
	"mtcloud.com/mtstorage/cmd/controller/app/util/workqueue"
	"mtcloud.com/mtstorage/node/client"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/pkg/runtime"
)

// Controller manages selector-based service bucket.
type Controller struct {
	nameserverClient *clientbuilder.NameserverClient

	queue workqueue.RateLimitingInterface

	// workerLoopPeriod is the time between worker runs. The workers process the queue of service and pod changes.
	workerLoopPeriod time.Duration
}

// NewBucketController returns a new *Controller.
func NewBucketController(buckerInformer coreinformers.Informer, nscli *clientbuilder.NameserverClient, bucketUpdatesBatchPeriod time.Duration) *Controller {

	c := &Controller{
		nameserverClient: nscli,
		queue:            workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "bucket"),
		workerLoopPeriod: time.Second,
	}

	buckerInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.addBucket,
		//UpdateFunc: e.updateBucket,
		DeleteFunc: c.deleteBucket,
	})

	return c
}

func (c *Controller) addBucket(obj interface{}) {
	logger.Info("on add bucket")
	c.queue.AddAfter(obj, time.Second)
}

func (c *Controller) deleteBucket(obj interface{}) {
	logger.Info("on delete bucket")
	//todo: delete bucket
}

func (c *Controller) Run(workers int, stopCh <-chan struct{}) {

	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, c.workerLoopPeriod, stopCh)
	}

	go func() {
		defer runtime.HandleCrash()
	}()

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
	bucket := obj.(*core.Bucket)

	//todo:  just test
	result, err := c.nameserverClient.GetBucketsLogging(client.WithTrack(nil))
	if err != nil && result == nil {

	}

	logger.Infof("bucket name: %s", bucket.BucketExternal.Name)

	//get random chunker node
	cn, err := c.nameserverClient.GetChunkerNode(context.TODO())
	if err != nil && result == nil {

	}
	logger.Infof("chunker endpoint: %s", cn.Endpoint)

	//
	//主动探测节点健康状态
	chunker, err := client.CreateChunkerClient(context.Background(), cn.Id)
	if err != nil {
		logger.Error(err)
		return false
	}
	_, _ = chunker.Version(context.TODO(), "")
	return true
}
