package mq

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"mtcloud.com/mtstorage/pkg/config"
	metaContext "mtcloud.com/mtstorage/pkg/discall/context"
	"mtcloud.com/mtstorage/pkg/discall/result"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/pkg/mq/rocketmq"
)

type MqPubSub struct {
	rocketMqBus    *rocketmq.RocketMqBus
	defaultTopic   string
	heartbeatTopic string
	defaultTag     string
}

var Pubsub *MqPubSub

func init() {
	Pubsub = &MqPubSub{
		rocketMqBus: &rocketmq.RocketMqBus{},
	}
}

func StartMQ(c config.MqConfig, nodeGroup, p2pTag string) {
	logger.Info("[MQ] starting ")
	Pubsub.defaultTopic = c.Topic
	Pubsub.defaultTag = nodeGroup
	hbTopic := c.Topic_heartBeat
	Pubsub.heartbeatTopic = hbTopic

	if err := Pubsub.StartPubsub(c, nodeGroup, p2pTag, sender, receiver); err != nil {
		panic(err)
		return
	}

	//loop for mq task
	go func() {
		for {
			select {
			case msg := <-receiver:
				logger.Infof("[MQ] receive message,topic: %s,  tag: %s", msg.Topic, msg.GetTags())
				Pubsub.handleMQmessage(&msg)
			}
		}
	}()
}

func (ps *MqPubSub) StartPubsub(c config.MqConfig, nodeGroup, p2pTag string, sender <-chan primitive.Message, receiver chan<- primitive.MessageExt) error {
	if err := ps.rocketMqBus.StartProducer(sender, GetProducerOptions(c.Server, nodeGroup)); err != nil {
		panic(err)
		return err
	}
	if err := ps.rocketMqBus.StartClusterConsumer(c.Topic, nodeGroup, receiver, GetConsumerOptions(c.Server, nodeGroup)); err != nil {
		panic(err)
		return err
	}

	if err := ps.rocketMqBus.StartP2pConsumer(c.Topic, p2pTag, receiver, GetP2pConsumerOptions(c.Server, p2pTag)); err != nil {
		panic(err)
		return err
	}

	//broadcastGroupId := config.GetString("mq.broadcast_group_id")
	broadcastGroupId := c.Broadcast_group_id
	if broadcastGroupId != "" {
		hbgId := fmt.Sprintf("%s#%s", c.Topic_heartBeat, c.Broadcast_group_id)
		if err := ps.rocketMqBus.StartBroadcastConsumer(ps.heartbeatTopic, hbgId, "heartbeat", receiver, GetBroadcastConsumerOptions(c.Server, hbgId)); err != nil {
			panic(err)
			return err
		}
	}

	return nil
}

// Send send Remote Message
func (ps *MqPubSub) Send(ctx context.Context, dist string, msgData []byte, opts ...SendOption) (*result.RpcResult, error) {
	opt := sendOptions{}
	for _, o := range opts {
		o(&opt)
	}
	topic := ps.defaultTopic
	tag := dist
	if len(opt.topic) > 0 {
		topic = opt.topic
	}
	if len(opt.tag) > 0 {
		tag = opt.tag
	}

	if _, ok := metaContext.GetMetadata(ctx, metaContext.META_BROADCAST); ok {
		topic = ps.heartbeatTopic
		tag = "heartbeat"
		if opt.Track {
			return nil, errors.New("BROADCAST 和 TRACK互斥")
		}
	}

	key := ""
	if kv, ok := metaContext.GetMetadata(ctx, metaContext.META_KEY); ok {
		key = kv
	} else {
		h := sha1.New()
		h.Write(msgData)
		bs := h.Sum(nil)
		key = fmt.Sprintf("%x", bs)
	}
	m := primitive.Message{
		Topic: topic,
		Body:  msgData,
	}
	m.WithTag(tag)
	m.WithKeys([]string{key})

	if opt.Track {
		var callpayload CallPayload
		_ = json.Unmarshal(msgData, &callpayload)
		return ps.SendWithTracker(&callpayload, func() {
			sender <- m
		})
	} else {
		sender <- m

	}
	return nil, nil
}

// SendWithTracker Send Remote Message
func (ps *MqPubSub) SendWithTracker(message *CallPayload, callback func(), opts ...SendOption) (*result.RpcResult, error) {
	return Tracker.SendRouterWithTracker(message, callback, opts...)
}

func GetProducerOptions(mqserver string, groupId string) []producer.Option {
	//groupId := config.GetNodeGroup()

	var endPoint = []string{mqserver}
	return []producer.Option{producer.WithGroupName(groupId),
		producer.WithNameServer(endPoint),
		producer.WithTrace(&primitive.TraceConfig{
			GroupName:    groupId,
			Access:       primitive.Local,
			NamesrvAddrs: endPoint,
		})}
}

func GetConsumerOptions(mqserver string, groupId string) []consumer.Option {
	var endPoint = []string{mqserver}
	return []consumer.Option{consumer.WithGroupName(groupId),
		consumer.WithNameServer(endPoint),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithTrace(&primitive.TraceConfig{
			GroupName:    groupId,
			Access:       primitive.Local,
			NamesrvAddrs: endPoint,
		})}
}

func GetBroadcastConsumerOptions(mqserver string, groupId string) []consumer.Option {
	broadcastGroupId := groupId
	var endPoint = []string{mqserver}
	return []consumer.Option{
		consumer.WithGroupName(broadcastGroupId),
		consumer.WithNameServer(endPoint),
		consumer.WithConsumerModel(consumer.BroadCasting),
		consumer.WithTrace(&primitive.TraceConfig{
			GroupName:    broadcastGroupId,
			Access:       primitive.Local,
			NamesrvAddrs: endPoint,
		})}
}

func GetP2pConsumerOptions(mqserver string, p2pgroup string) []consumer.Option {
	var endPoint = []string{mqserver}
	return []consumer.Option{
		consumer.WithGroupName(p2pgroup),
		consumer.WithNameServer(endPoint),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithTrace(&primitive.TraceConfig{
			GroupName:    p2pgroup,
			Access:       primitive.Local,
			NamesrvAddrs: endPoint,
		})}
}
