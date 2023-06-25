package mq

import (
	"errors"
	"fmt"
	result2 "mtcloud.com/mtstorage/pkg/discall/result"
	"mtcloud.com/mtstorage/pkg/logger"
	"sync"
	"time"
)

type PubSubConn struct {
	IsSubscribe int32
}

type CallTracker struct {
	mutex      *sync.Mutex
	ConnPubSub *PubSubConn
	trackMap   map[string]TrackID
	status     int32 // 0: stop , 1:running
	ch         chan CallPayload
	Done       chan interface{}
	Channels   []string
}

type TrackID struct {
	From       string
	To         string
	Id         string
	respCh     chan CallPayload
	callback   TrackCallback
	msgPayload *CallPayload
}
type TrackCallback func(*result2.RpcResult, error)

var Tracker = &CallTracker{}

func init() {
	Tracker.mutex = new(sync.Mutex)
	Tracker.trackMap = make(map[string]TrackID)
	Tracker.status = 0
	Tracker.ch = make(chan CallPayload)
	Tracker.Done = make(chan interface{}, 1)
	//go Tracker.setupTracker()
}

func (t *CallTracker) SendRouterWithTracker(msg *CallPayload, callback func(), opts ...SendOption) (*result2.RpcResult, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.status == 0 {
		//start tracker loop
		//time.Sleep(2 * time.Second)
	}
	//result, err := SendRouter(msg)
	//if err != nil {
	//	return result, err
	//}
	callback()

	trackId := t.newTrackID(msg, opts...)
	if tracked, ok := t.trackMap[trackId.Id]; !ok {
		t.trackMap[trackId.Id] = trackId
	} else {
		logger.Warn("msg is on tracking...")
		trackId = tracked
	}

	re := &result2.RpcResult{
		Code:    result2.STATE_CODE_SUCCESS,
		Data:    trackId,
		Message: "处理中",
		Success: true,
	}
	return re, nil
}

func (t *CallTracker) deleteFromTrackMap(trackId *TrackID) {
	Tracker.mutex.Lock()
	defer Tracker.mutex.Unlock()
	delete(Tracker.trackMap, trackId.Id)
	defer close(trackId.respCh)

}

func (t *CallTracker) newTrackID(payload *CallPayload, opts ...SendOption) TrackID {
	options := newSendOptions(opts...)
	return TrackID{
		From: payload.From,
		To:   payload.To,
		Id:   payload.RequestId,
		//msgPayload: payload,
		callback: options.TrackCallback,
		respCh:   make(chan CallPayload),
	}
}
func (t *CallTracker) RespHandler(msg *CallPayload) {
	logger.Info("RespHandler handler response message ... ")
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if trackId, ok := t.trackMap[msg.RequestId]; ok {
		if trackId.respCh != nil {
			trackId.respCh <- *msg
			//delete from tracker map
			delete(t.trackMap, trackId.Id)
			defer close(trackId.respCh)
		}
	}
}

func (trackId *TrackID) Track(deadline int) (CallPayload, error) {
	select {
	case m := <-trackId.respCh:
		logger.Infof("track %s complete. response from: %s, to: %s", trackId.Id, m.From, m.To)
		return m, nil
	case <-time.After(time.Duration(deadline) * time.Second):
		info := fmt.Sprintf("track %s timeout, from: %s, to: %s, deadline: %d", trackId.Id, trackId.From, trackId.To, deadline)
		logger.Warnf(info)
		//release trackId
		Tracker.deleteFromTrackMap(trackId)
		return CallPayload{}, errors.New(info)
	}
}
