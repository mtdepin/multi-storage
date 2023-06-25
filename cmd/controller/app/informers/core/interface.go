package core

import (
	internalinterfaces "mtcloud.com/mtstorage/cmd/controller/app/informers/internalinterfaces"
	"mtcloud.com/mtstorage/cmd/controller/app/informers/tools/cache"
	v1 "mtcloud.com/mtstorage/cmd/controller/app/lister/core"
)

// Informer provides access to a shared informer and lister for
// Buckets.
type Informer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1.BucketLister
	Client() interface{}
}

// Interface provides access to all the informers in this group version.
type Interface interface {
	// bucket returns a Informer.
	Buckets() Informer
	Objects() Informer
}

type version struct {
	factory          internalinterfaces.SharedInformerFactory
	namespace        string
	tweakListOptions internalinterfaces.TweakListOptionsFunc
}

// New returns a new Interface.
func New(f internalinterfaces.SharedInformerFactory, namespace string) Interface {
	return &version{factory: f, namespace: namespace}
}

func (v *version) Buckets() Informer {
	return &bucketInformer{factory: v.factory, namespace: v.namespace}
}

func (v *version) Objects() Informer {
	return &objectInformer{factory: v.factory, namespace: v.namespace}
}
