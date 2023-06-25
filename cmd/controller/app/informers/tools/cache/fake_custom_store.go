package cache

// FakeCustomStore lets you define custom functions for store operations.
type FakeCustomStore struct {
	AddFunc      func(obj interface{}) error
	UpdateFunc   func(obj interface{}) error
	DeleteFunc   func(obj interface{}) error
	ListFunc     func() []interface{}
	ListKeysFunc func() []string
	GetFunc      func(obj interface{}) (item interface{}, exists bool, err error)
	GetByKeyFunc func(key string) (item interface{}, exists bool, err error)
	ReplaceFunc  func(list []interface{}, resourceVersion string) error
	ResyncFunc   func() error
}

// Add calls the custom Add function if defined
func (f *FakeCustomStore) Add(obj interface{}) error {
	if f.AddFunc != nil {
		return f.AddFunc(obj)
	}
	return nil
}

// Update calls the custom Update function if defined
func (f *FakeCustomStore) Update(obj interface{}) error {
	if f.UpdateFunc != nil {
		return f.UpdateFunc(obj)
	}
	return nil
}

// Delete calls the custom Delete function if defined
func (f *FakeCustomStore) Delete(obj interface{}) error {
	if f.DeleteFunc != nil {
		return f.DeleteFunc(obj)
	}
	return nil
}

// List calls the custom List function if defined
func (f *FakeCustomStore) List() []interface{} {
	if f.ListFunc != nil {
		return f.ListFunc()
	}
	return nil
}

// ListKeys calls the custom ListKeys function if defined
func (f *FakeCustomStore) ListKeys() []string {
	if f.ListKeysFunc != nil {
		return f.ListKeysFunc()
	}
	return nil
}

// Get calls the custom Get function if defined
func (f *FakeCustomStore) Get(obj interface{}) (item interface{}, exists bool, err error) {
	if f.GetFunc != nil {
		return f.GetFunc(obj)
	}
	return nil, false, nil
}

// GetByKey calls the custom GetByKey function if defined
func (f *FakeCustomStore) GetByKey(key string) (item interface{}, exists bool, err error) {
	if f.GetByKeyFunc != nil {
		return f.GetByKeyFunc(key)
	}
	return nil, false, nil
}

// Replace calls the custom Replace function if defined
func (f *FakeCustomStore) Replace(list []interface{}, resourceVersion string) error {
	if f.ReplaceFunc != nil {
		return f.ReplaceFunc(list, resourceVersion)
	}
	return nil
}

// Resync calls the custom Resync function if defined
func (f *FakeCustomStore) Resync() error {
	if f.ResyncFunc != nil {
		return f.ResyncFunc()
	}
	return nil
}
