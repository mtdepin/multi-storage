package core

import (
	"mtcloud.com/mtstorage/cmd/controller/app/informers/core/util"
	"sync"
)

var MqW *MqWatcher

type WatchInterface interface {
	GetEventType() util.EventType
	OnEvent(et util.EventType, payload []byte) error
}

type MqWatcher struct {
	lk          sync.RWMutex
	registedMap map[util.EventType][]WatchInterface
}

func init() {
	MqW = &MqWatcher{
		lk:          sync.RWMutex{},
		registedMap: make(map[util.EventType][]WatchInterface),
	}
	//resultChan = make(chan watch.Event)
	//go run(resultChan)
}

func (w *MqWatcher) regist(e WatchInterface) {
	w.lk.Lock()
	defer w.lk.Unlock()
	t := e.GetEventType()
	arr, ok := w.registedMap[t]
	if !ok {
		w.registedMap[t] = make([]WatchInterface, 0)
	} else {
		for _, v := range arr {
			if e == v {
				return
			}
		}
	}
	w.registedMap[t] = append(w.registedMap[t], e)
}

func (w *MqWatcher) unRegist(e WatchInterface) {
	w.lk.Lock()
	defer w.lk.Unlock()
	t := e.GetEventType()
	arr, ok := w.registedMap[t]
	if ok {
		for index, v := range arr {
			if e == v {
				arr = append(arr[:index], arr[index+1:]...)
				w.registedMap[t] = arr
				return
			}
		}
	}
}

//func run(resultChan chan watch.Event) {
//	fmt.Println("run a test loop")
//	for {
//		fmt.Println("add a new bucket")
//		time.Sleep(8 * time.Second)
//
//		bucketname := "testbucket" + "-" + util.GetRandString(5)
//		bi := &core.Bucket{
//			ObjectMeta: metav1.ObjectMeta{
//				Namespace: "chengdu",
//				Name:      bucketname,
//			},
//			Name:       bucketname,
//			Location:   "chengdu",
//			CreateTime: time.Now(),
//		}
//
//		ev := watch.Event{
//			Type:   watch.Added,
//			Object: bi,
//		}
//
//		resultChan <- ev
//	}
//}
