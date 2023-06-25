package rocketmq

import (
	"context"
	"mtcloud.com/mtstorage/pkg/logger"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

const MsgTypeProperty = "MsgTypeProperty"

type RocketMqBus struct {
	producer          rocketmq.Producer
	clusterConsumer   rocketmq.PushConsumer
	p2pConsumer       rocketmq.PushConsumer
	broadcastConsumer rocketmq.PushConsumer
	lock              sync.Mutex
}

//var RocketBus = &RocketMqBus{}

func (bus *RocketMqBus) StartProducer(sender <-chan primitive.Message, proOptions []producer.Option) error {
	if bus.producer == nil {
		pro, err := rocketmq.NewProducer(proOptions...)
		if err != nil {
			return err
		}
		bus.producer = pro
	}

	err := bus.producer.Start()
	if err != nil {
		logger.Error(err)
		return err
	}

	//publish
	go func() {
		defer func() {
			logger.Error("producer exit unexpect!!!!")
		}()
		for {
			select {
			case msg := <-sender:
				res, err := bus.send(&msg, 20, time.Second*2)
				if err != nil {
					logger.Errorf("send on storageerror message: %s", err.Error())
				} else {
					logger.Debugf("send msg topic: %s, tags: %s id: %s, content: %s", msg.Topic, msg.GetTags(), res.MsgID, string(msg.Body))
				}
			}
		}
	}()
	return nil
}

func (bus *RocketMqBus) StartClusterConsumer(topic, tag string, receiver chan<- primitive.MessageExt, conOptions []consumer.Option) error {
	if bus.clusterConsumer == nil {
		con, err := rocketmq.NewPushConsumer(conOptions...)
		if err != nil {
			return err
		}
		bus.clusterConsumer = con
	}
	//subscribe
	if bus.clusterConsumer != nil {
		logger.Infof("Subscribe to topic: %s, group: %s,  tag: %s", topic, tag, tag)
		err := bus.clusterConsumer.Subscribe(topic, consumer.MessageSelector{
			Type:       consumer.TAG,
			Expression: tag,
		}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			bus.lock.Lock()
			defer bus.lock.Unlock()
			//RocketBus.resolveMsgs(msgs)
			for _, msg := range msgs {
				msg := msg
				logger.Infof("msg type %s", msg.GetProperty(MsgTypeProperty))
				receiver <- *msg
			}
			return consumer.ConsumeSuccess, nil
		})
		if err != nil {
			return err
		}
		if err := bus.clusterConsumer.Start(); err != nil {
			return err
		}
	}
	return nil
}

func (bus *RocketMqBus) StartP2pConsumer(topic, tag string, receiver chan<- primitive.MessageExt, conOptions []consumer.Option) error {

	if bus.p2pConsumer == nil {
		con, err := rocketmq.NewPushConsumer(conOptions...)
		if err != nil {
			return err
		}
		bus.p2pConsumer = con
	}
	//subscribe
	if bus.p2pConsumer != nil {
		logger.Infof("Subscribe to topic: %s, group: %s, tag: %s", topic, tag, tag)
		err := bus.p2pConsumer.Subscribe(topic, consumer.MessageSelector{
			Type:       consumer.TAG,
			Expression: tag,
		}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			bus.lock.Lock()
			defer bus.lock.Unlock()
			//RocketBus.resolveMsgs(msgs)
			for _, msg := range msgs {
				msg := msg
				logger.Infof("msg key %s", msg.GetKeys())
				receiver <- *msg
			}
			return consumer.ConsumeSuccess, nil
		})
		if err != nil {
			return err
		}
		if err := bus.p2pConsumer.Start(); err != nil {
			return err
		}
	}
	return nil
}

func (bus *RocketMqBus) StartBroadcastConsumer(topic, group, tag string, receiver chan<- primitive.MessageExt, conOptions []consumer.Option) error {

	if bus.broadcastConsumer == nil {
		con, err := rocketmq.NewPushConsumer(conOptions...)
		if err != nil {
			return err
		}
		bus.broadcastConsumer = con
	}

	if bus.broadcastConsumer != nil {
		logger.Infof("Subscribe to topic: %s, group: %s,  tag: %s", topic, group, tag)
		err := bus.broadcastConsumer.Subscribe(topic, consumer.MessageSelector{
			Type:       consumer.TAG,
			Expression: tag,
		}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			bus.lock.Lock()
			defer bus.lock.Unlock()
			for _, msg := range msgs {
				msg := msg
				logger.Debugf("msg type %s", msg.GetProperty(MsgTypeProperty))
				receiver <- *msg
			}
			return consumer.ConsumeSuccess, nil
		})
		if err != nil {
			return err
		}
		if err := bus.broadcastConsumer.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (bus *RocketMqBus) PowerOn() error {
	return nil
}

func (bus *RocketMqBus) PowerOff() error {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	return nil
}

func (bus *RocketMqBus) Send() error {
	//m := &primitive.Message{
	//	Topic: bus.topic,
	//	Body:  msg.Data(),
	//}
	//m.WithTag(msg.Operator())
	//m.WithProperty(types.MsgTypeProperty, msg.Type())
	//res, err := bus.send(m, 20, time.Second*2)
	//if err != nil {
	//	log.Errorf("send on storageerror message: %s", err.Error())
	//} else {
	//	log.Infof("send msg id: %s, content: %s", res.MsgID, string(msg.Data()))
	//}
	//return err
	return nil
}

func (bus *RocketMqBus) send(msg *primitive.Message, attempts int, sleep time.Duration) (*primitive.SendResult, error) {
	res, err := bus.producer.SendSync(context.Background(), msg)
	if err != nil {
		if attempts--; attempts > 0 {
			logger.Warnf("retry send storageerror: %s. attemps #%d after %s.", err.Error(), attempts, sleep)
			time.Sleep(sleep)
			return bus.send(msg, attempts, sleep*2)
		}
		return res, err
	}
	return res, err
}
