package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	corev1 "mtcloud.com/mtstorage/cmd/controller/app/api/core"
	"mtcloud.com/mtstorage/cmd/controller/app/clientbuilder/client"
	"mtcloud.com/mtstorage/cmd/controller/app/informers/core/util"
	"mtcloud.com/mtstorage/cmd/controller/app/informers/internalinterfaces"
	"mtcloud.com/mtstorage/cmd/controller/app/informers/tools/cache"
	v1 "mtcloud.com/mtstorage/cmd/controller/app/lister/core"
	"mtcloud.com/mtstorage/cmd/nameserver/metadata"
	client2 "mtcloud.com/mtstorage/node/client"
	"mtcloud.com/mtstorage/pkg/logger"
)

type bucketInformer struct {
	factory   internalinterfaces.SharedInformerFactory
	namespace string
}

// NewBucketInformer constructs a new informer for Pod type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewBucketInformer(client client.ClientInterface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredBucketInformer(client, namespace, resyncPeriod, indexers)
}

// NewFilteredBucketInformer constructs a new informer for Pod type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredBucketInformer(client client.ClientInterface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				list, err := client.GetBucketsLogging(client2.WithTrack(context.TODO()))
				if err != nil {
					logger.Error(err)
					return nil, err
				}

				result := coverToBucketList(list)
				return result, nil
				//return client.CoreV1().Buckets(namespace).List(context.TODO(), options)
				//return &corev1.BucketList{}, nil
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				//return client.CoreV1().Buckets(namespace).Watch(context.TODO(), options)

				w := &bucketWatch{
					resultChan: make(chan watch.Event),
					eventType:  util.EVENTTYPE_ADD_BUCKET,
				}
				MqW.regist(w)
				return w, nil
			},
		},
		&corev1.Bucket{},
		resyncPeriod,
		indexers,
	)
}

func coverToBucketList(list []metadata.BucketExternal) *corev1.BucketList {
	bucketList := &corev1.BucketList{
		Items: make([]corev1.Bucket, 0),
	}
	if list == nil {
		return bucketList
	}
	for _, info := range list {

		item := coverToBucket(info)
		bucketList.Items = append(bucketList.Items, item)
	}
	return bucketList
}

func coverToBucket(b metadata.BucketExternal) corev1.Bucket {
	bucket := corev1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      b.Name,
		},
		BucketExternal: b,
	}

	return bucket

}

func (f *bucketInformer) defaultInformer(client client.ClientInterface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredBucketInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
}

func (f *bucketInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&corev1.Bucket{}, f.defaultInformer)
}

func (f *bucketInformer) Lister() v1.BucketLister {
	return v1.NewPodLister(f.Informer().GetIndexer())
}

//return a nameserver client

func (f *bucketInformer) Client() interface{} {
	return f.Client()
}

// WrapWatch wrap weather
type bucketWatch struct {
	resultChan chan watch.Event
	eventType  util.EventType
	obj        interface{}
}

func (w *bucketWatch) GetEventType() util.EventType {
	return w.eventType
}

func (w *bucketWatch) OnEvent(et util.EventType, payload []byte) error {
	var bucket corev1.Bucket
	if err := json.Unmarshal(payload, &bucket); err != nil {
		logger.Error(err)
		return err
	}

	bucket.ObjectMeta = metav1.ObjectMeta{
		Namespace: "default",
		Name:      bucket.BucketExternal.Name,
	}

	ev := watch.Event{
		Type:   watch.Added,
		Object: &bucket,
	}

	switch et {
	case util.EVENTTYPE_ADD_BUCKET:
		ev.Type = watch.Added
	case util.EVENTTYPE_DELETE_BUCKET:
		ev.Type = watch.Deleted
	default:
		logger.Error("unsupported type!!")
		return fmt.Errorf("unsupported type!!")
	}

	w.resultChan <- ev
	return nil
}

func (w *bucketWatch) Stop() {
	MqW.unRegist(w)
	close(w.resultChan)
}

func (w *bucketWatch) ResultChan() <-chan watch.Event {
	return w.resultChan
}
