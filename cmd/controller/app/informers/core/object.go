package core

import (
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
	"mtcloud.com/mtstorage/pkg/logger"
)

type objectInformer struct {
	factory   internalinterfaces.SharedInformerFactory
	namespace string
}

// NewObjectInformer constructs a new informer for object
func NewObjectInformer(client client.ClientInterface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredObjectInformer(client, namespace, resyncPeriod, indexers)
}

// NewFilteredObjectInformer constructs a new informer for object.
func NewFilteredObjectInformer(client client.ClientInterface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				result := &corev1.ObjectList{
					Items: make([]corev1.Object, 0),
				}
				return result, nil
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				w := &objectWatch{
					resultChan: make(chan watch.Event),
					eventType:  util.EVENTTYPE_ADD_OBJECT,
				}
				MqW.regist(w)
				return w, nil
			},
		},
		&corev1.Object{},
		resyncPeriod,
		indexers,
	)
}

func coverToObject(b metadata.ObjectInfo) corev1.Object {
	object := corev1.Object{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      b.Name,
		},
		ObjectInfo: b,
	}

	return object

}

func (f *objectInformer) defaultInformer(client client.ClientInterface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredObjectInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
}

func (f *objectInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&corev1.Object{}, f.defaultInformer)
}

func (f *objectInformer) Lister() v1.BucketLister {
	//todo , return a lister
	return v1.NewPodLister(f.Informer().GetIndexer())
}

//return a nameserver client

func (f *objectInformer) Client() interface{} {
	return f.Client()
}

// =========================== object watcher ================================
// WrapWatch wrap weather
type objectWatch struct {
	resultChan chan watch.Event
	eventType  util.EventType
	obj        interface{}
}

func (w *objectWatch) GetEventType() util.EventType {
	return w.eventType
}

func (w *objectWatch) OnEvent(et util.EventType, payload []byte) error {
	var obj corev1.Object
	if err := json.Unmarshal(payload, &obj); err != nil {
		logger.Error(err)
		return err
	}

	obj.ObjectMeta = metav1.ObjectMeta{
		Namespace: "default",
		Name:      obj.ObjectInfo.Etag,
	}

	ev := watch.Event{
		Type:   watch.Added,
		Object: &obj,
	}

	switch et {
	case util.EVENTTYPE_ADD_OBJECT:
		ev.Type = watch.Added
	case util.EVENTTYPE_DELETE_OBJECT:
		ev.Type = watch.Deleted
	default:
		logger.Error("unsupported type!!")
		return fmt.Errorf("unsupported type!!")
	}

	w.resultChan <- ev
	return nil
}

func (w *objectWatch) Stop() {
	MqW.unRegist(w)
	close(w.resultChan)
}

func (w *objectWatch) ResultChan() <-chan watch.Event {
	return w.resultChan
}
