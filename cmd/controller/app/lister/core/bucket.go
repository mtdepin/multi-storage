package v1

import (
	"k8s.io/apimachinery/pkg/labels"
	"mtcloud.com/mtstorage/cmd/controller/app/api/core"
	"mtcloud.com/mtstorage/cmd/controller/app/informers/tools/cache"
)

// BucketLister helps list Pods.
// All objects returned here must be treated as read-only.
type BucketLister interface {
	// List lists all Pods in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*core.Bucket, err error)
	// Pods returns an object that can list and get Pods.
	Buckets(namespace string) PodNamespaceLister
	//PodListerExpansion
}

// bucketLister implements the BucketLister interface.
type bucketLister struct {
	indexer cache.Indexer
}

// NewPodLister returns a new BucketLister.
func NewPodLister(indexer cache.Indexer) BucketLister {
	return &bucketLister{indexer: indexer}
}

// List lists all Pods in the indexer.
func (s *bucketLister) List(selector labels.Selector) (ret []*core.Bucket, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*core.Bucket))
	})
	return ret, err
}

// Pods returns an object that can list and get Pods.
func (s *bucketLister) Buckets(namespace string) PodNamespaceLister {
	return bucketNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// PodNamespaceLister helps list and get Buckets.
// All objects returned here must be treated as read-only.
type PodNamespaceLister interface {
	// List lists all Buckets in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*core.Bucket, err error)
	// Get retrieves the Pod from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*core.Bucket, error)
	//PodNamespaceListerExpansion
}

// bucketNamespaceLister implements the PodNamespaceLister
// interface.
type bucketNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all Buckets in the indexer for a given namespace.
func (s bucketNamespaceLister) List(selector labels.Selector) (ret []*core.Bucket, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*core.Bucket))
	})
	return ret, err
}

// Get retrieves the Pod from the indexer for a given namespace and name.
func (s bucketNamespaceLister) Get(name string) (*core.Bucket, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		//return nil, errors.NewNotFound(core.Resource("pod"), name)
		return nil, nil
	}
	return obj.(*core.Bucket), nil
}
