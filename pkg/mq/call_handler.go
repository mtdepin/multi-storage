package mq

import (
	"context"
	"encoding/json"
	"go.opencensus.io/trace"
	discall2 "mtcloud.com/mtstorage/pkg/discall"
	metaContext "mtcloud.com/mtstorage/pkg/discall/context"
	metadata2 "mtcloud.com/mtstorage/pkg/discall/context/metadata"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/pkg/tracing"
	"strconv"
	"time"
)

// handl Call  处理各种方法调用
func (ps *MqPubSub) handlerCall(msgPayload *CallPayload) {
	if msgPayload == nil {
		logger.Error("unexpect storageerror: msgPayload is nil")
		return
	}
	discall2.DoLog(" <--- handle call request: %s ", string(msgPayload.Payload))
	wf := func(resp discall2.Response) {
		if msgPayload.Extends == nil || msgPayload.Extends["TRACK"] == "" {
			return
		}
		jsonPayload, err := json.Marshal(&resp)
		if err != nil {
			logger.Error(err)
			return
		}
		respPayload := &CallPayload{
			RequestId: msgPayload.RequestId,
			From:      msgPayload.To,
			To:        msgPayload.From,
			MegType:   MegType_CALL_RESPONSE,
			Payload:   jsonPayload,
			Digest:    "response",
		}
		jsonbyte, err := json.Marshal(respPayload)
		if err != nil {
			logger.Error(err)
			return
		}
		if _, err := ps.Send(context.Background(), respPayload.To, jsonbyte); err != nil {
			logger.Errorf("handleChannelResponse:%s, status:%v", msgPayload.RequestId, err)
		}
		discall2.DoLog(" ---> response send: %v", string(jsonPayload))

	}

	ctx := context.Background()
	if msgPayload.Extends != nil {
		ctx = metadata2.NewContext(ctx, msgPayload.Extends)
		if !ps.checkValid(msgPayload.Extends) {
			logger.Warnf("receive a expired message: %v", msgPayload.RequestId)
			return
		}

		//trace span
		if v, ok := msgPayload.Extends[metaContext.META_SPANCTX]; ok {
			if sc, ok := tracing.SpanContextFromStr(v); ok {
				c, span := trace.StartSpanWithRemoteParent(ctx, "handlerCall", sc)
				defer span.End()
				ctx = c
			}
		} else {
			c, span := trace.StartSpan(ctx, "handlerCall")
			defer span.End()
			ctx = c
		}
	}

	discall2.CallServer.HandlerCall(ctx, msgPayload.Payload, wf)

}

func (ps *MqPubSub) checkValid(extends map[string]string) bool {
	_, ok := extends["TRACK"]
	if ok {
		timeout, ok := extends["TIMEOUT"]
		if ok {
			expire, err := strconv.Atoi(timeout)
			if err != nil {
				logger.Error(err)
				return true
			}

			timestampStr, ok := extends["TIMESTAMP"]
			if !ok {
				return true
			}
			timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
			if err != nil {
				logger.Error(err)
				return true
			}
			t := time.Unix(timestamp/1000, 0)
			now := time.Now()
			if now.After(t.Add(time.Duration(expire) * time.Second)) {
				return false
			}
		}
	}
	return true
}

func (ps *MqPubSub) handlerCallResponse(msgPayload *CallPayload) {
	Tracker.RespHandler(msgPayload)
}
