package internalinterfaces

import (
	"mtcloud.com/mtstorage/cmd/controller/app/clientbuilder/client"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	cache "mtcloud.com/mtstorage/cmd/controller/app/informers/tools/cache"
)

// NewInformerFunc takes kubernetes.Interface and time.Duration to return a SharedIndexInformer.
type NewInformerFunc func(client.ClientInterface, time.Duration) cache.SharedIndexInformer

// SharedInformerFactory a small interface to allow for adding an informer without an import cycle
type SharedInformerFactory interface {
	Start(stopCh <-chan struct{})
	InformerFor(obj runtime.Object, newFunc NewInformerFunc) cache.SharedIndexInformer
}

// TweakListOptionsFunc is a function that transforms a v1.ListOptions.
type TweakListOptionsFunc func(*v1.ListOptions)
