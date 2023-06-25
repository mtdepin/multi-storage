package core

import (
	"context"
	"mtcloud.com/mtstorage/cmd/controller/app/informers/core/util"
	"mtcloud.com/mtstorage/pkg/logger"
)

func (w *MqWatcher) Version(ctx context.Context) string {
	return "1.0"
}

func (w *MqWatcher) Event(ctx context.Context, et util.EventType, payload []byte) error {
	w.lk.RLock()
	defer w.lk.RUnlock()
	arr, ok := w.registedMap[et]
	if ok {
		for _, e := range arr {
			if err := e.OnEvent(et, payload); err != nil {
				logger.Error(err)
				return err
			}
		}
	}
	return nil
}
