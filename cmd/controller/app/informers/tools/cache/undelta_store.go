package cache

// UndeltaStore listens to incremental updates and sends complete state on every change.
// It implements the Store interface so that it can receive a stream of mirrored objects
// from Reflector.  Whenever it receives any complete (Store.Replace) or incremental change
// (Store.Add, Store.Update, Store.Delete), it sends the complete state by calling PushFunc.
// It is thread-safe.  It guarantees that every change (Add, Update, Replace, Delete) results
// in one call to PushFunc, but sometimes PushFunc may be called twice with the same values.
// PushFunc should be thread safe.
type UndeltaStore struct {
	Store
	PushFunc func([]interface{})
}

// Assert that it implements the Store interface.
var _ Store = &UndeltaStore{}

// Add inserts an object into the store and sends complete state by calling PushFunc.
// Note about thread safety.  The Store implementation (cache.cache) uses a lock for all methods.
// In the functions below, the lock gets released and reacquired betweend the {Add,Delete,etc}
// and the List.  So, the following can happen, resulting in two identical calls to PushFunc.
// time            thread 1                  thread 2
// 0               UndeltaStore.Add(a)
// 1                                         UndeltaStore.Add(b)
// 2               Store.Add(a)
// 3                                         Store.Add(b)
// 4               Store.List() -> [a,b]
// 5                                         Store.List() -> [a,b]
func (u *UndeltaStore) Add(obj interface{}) error {
	if err := u.Store.Add(obj); err != nil {
		return err
	}
	u.PushFunc(u.Store.List())
	return nil
}

// Update sets an item in the cache to its updated state and sends complete state by calling PushFunc.
func (u *UndeltaStore) Update(obj interface{}) error {
	if err := u.Store.Update(obj); err != nil {
		return err
	}
	u.PushFunc(u.Store.List())
	return nil
}

// Delete removes an item from the cache and sends complete state by calling PushFunc.
func (u *UndeltaStore) Delete(obj interface{}) error {
	if err := u.Store.Delete(obj); err != nil {
		return err
	}
	u.PushFunc(u.Store.List())
	return nil
}

// Replace will delete the contents of current store, using instead the given list.
// 'u' takes ownership of the list, you should not reference the list again
// after calling this function.
// The new contents complete state will be sent by calling PushFunc after replacement.
func (u *UndeltaStore) Replace(list []interface{}, resourceVersion string) error {
	if err := u.Store.Replace(list, resourceVersion); err != nil {
		return err
	}
	u.PushFunc(u.Store.List())
	return nil
}

// NewUndeltaStore returns an UndeltaStore implemented with a Store.
func NewUndeltaStore(pushFunc func([]interface{}), keyFunc KeyFunc) *UndeltaStore {
	return &UndeltaStore{
		Store:    NewStore(keyFunc),
		PushFunc: pushFunc,
	}
}
