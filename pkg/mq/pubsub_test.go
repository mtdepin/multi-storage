package mq

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"testing"
	"time"
)

var addr = "192.168.1.181:9876"
var endPoint = []string{addr}

func TestRocketMq(t *testing.T) {
	p, err := rocketmq.NewProducer(
		producer.WithNameServer(endPoint),
		//producer.WithNsResolver(primitive.NewPassthroughResolver(endPoint)),
		producer.WithRetry(2),
		producer.WithGroupName("GID_xxxxxx"),
	)
	if err != nil {
		t.Fatal(err)
	}

	err = p.Start()

	if err != nil {
		t.Fatal(err)
	}

	result, err := p.SendSync(context.Background(), &primitive.Message{
		Topic: "test",
		Body:  []byte("Hello RocketMQ Go Client!"),
	})

	if err != nil {
		t.Fatal(err)
	}
	if result == nil {

	}
	time.Sleep(1000 * time.Second)
}
