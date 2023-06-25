package mq

import (
	"encoding/json"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"mtcloud.com/mtstorage/pkg/logger"
)

var (
	MegType_CALL          MegType = 999
	MegType_CALL_RESPONSE MegType = 1000
)

type MegType int
type InviteMethod string

type CallPayload struct {
	RequestId string            `json:"requestId,omitempty"`
	From      string            `json:"from,omitempty"`
	To        string            `json:"To,omitempty"`
	MegType   MegType           `json:"megType,omitempty"`
	Payload   []byte            `json:"payload,omitempty"`
	Digest    string            `json:"digest,omitempty"`
	Version   string            `json:"version,omitempty"`
	Extends   map[string]string `json:"extends,omitempty"`
}

var sender = make(chan primitive.Message, 100)
var receiver = make(chan primitive.MessageExt, 100)

func (ps *MqPubSub) handleMQmessage(msg *primitive.MessageExt) {
	logger.Info("[MQ] handle message ...")
	var msgPayload CallPayload
	if err := json.Unmarshal(msg.Body, &msgPayload); err != nil {
		logger.Error(err)
		return
	}
	switch msgPayload.MegType {
	case MegType_CALL:
		go ps.handlerCall(&msgPayload)

	case MegType_CALL_RESPONSE:
		go ps.handlerCallResponse(&msgPayload)
	}

}
