package client

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"

	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/node/api"
	"mtcloud.com/mtstorage/pkg/discall"
	metaContext "mtcloud.com/mtstorage/pkg/discall/context"
	"mtcloud.com/mtstorage/pkg/discall/result"
	"mtcloud.com/mtstorage/pkg/logger"
	mq2 "mtcloud.com/mtstorage/pkg/mq"
)

// default definition
var DISCALL_DEFAULT_TIMEOUT = "300"
var API_VERSION = "1.0"

type NodeInfo struct {
	Name   string
	NodeId string
	Domain string
}

var nodeInfo *NodeInfo

func InitClient(nid, name, domain string) {
	nodeInfo = &NodeInfo{
		Name:   name,
		NodeId: nid,
		Domain: domain,
	}
}

func CreateServerClient(ctx context.Context, to string) (api.ServerNode, error) {
	if ctx == nil {
		ctx = GetContext()
	}
	from := nodeInfo.NodeId
	ctx = metaContext.SetMetadata(ctx, metaContext.META_TIMEOUT, DISCALL_DEFAULT_TIMEOUT)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_VERSION, API_VERSION)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_FROM, nodeInfo.NodeId)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_NAME, nodeInfo.Name)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_DOMAIN, nodeInfo.Domain)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_TO, to)
	var client ServerClient
	err := discall.NewClient(ctx, from, to, "nameserver",
		[]interface{}{
			&client.Internal,
		}, send)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	return &client, nil
}

func CreateControllerlient(ctx context.Context, to string) (api.ControllerNode, error) {
	if ctx == nil {
		ctx = GetContext()
	}
	from := nodeInfo.NodeId
	ctx = metaContext.SetMetadata(ctx, metaContext.META_TIMEOUT, DISCALL_DEFAULT_TIMEOUT)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_VERSION, API_VERSION)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_FROM, nodeInfo.NodeId)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_NAME, nodeInfo.Name)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_DOMAIN, nodeInfo.Domain)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_TO, to)
	var client ControllerClient
	err := discall.NewClient(ctx, from, to, "controller",
		[]interface{}{
			&client.Internal,
		}, send)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	return &client, nil
}

func CreateServerControlNodeClient(ctx context.Context, to string) (api.ServerControlNode, error) {
	if ctx == nil {
		ctx = GetContext()
	}
	from := nodeInfo.NodeId
	ctx = metaContext.SetMetadata(ctx, metaContext.META_TIMEOUT, DISCALL_DEFAULT_TIMEOUT)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_VERSION, API_VERSION)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_FROM, nodeInfo.NodeId)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_NAME, nodeInfo.Name)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_DOMAIN, nodeInfo.Domain)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_TO, to)
	var client ServerControlNodeClient
	err := discall.NewClient(ctx, from, to, "control-node",
		[]interface{}{
			&client.Internal,
		}, send)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	return &client, nil
}

func CreateChunkerClient(ctx context.Context, to string) (api.ChunkerNode, error) {
	if ctx == nil {
		ctx = GetContext()
	}
	from := nodeInfo.NodeId
	ctx = metaContext.SetMetadata(ctx, metaContext.META_TIMEOUT, DISCALL_DEFAULT_TIMEOUT)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_VERSION, API_VERSION)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_FROM, nodeInfo.NodeId)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_NAME, nodeInfo.Name)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_DOMAIN, nodeInfo.Domain)
	ctx = metaContext.SetMetadata(ctx, metaContext.META_TO, to)
	//nodeClient, err := NewServerClient(ctx, from, to)
	var client ChunkerClient
	err := discall.NewClient(ctx, from, to, "chunker",
		[]interface{}{
			&client.Internal,
		}, send)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	return &client, nil
}

func send(ctx context.Context, dist string, data []byte) ([]byte, error) {
	rpcResult := &result.RpcResult{}
	var err error
	if _, ok := metaContext.GetMetadata(ctx, "TRACK"); ok {
		rpcResult, err = mq2.Pubsub.Send(ctx, dist, data, mq2.WithTrack())

	} else {
		rpcResult, err = mq2.Pubsub.Send(ctx, dist, data)
	}
	//rpcResult, err := mq.Pubsub.Send(dist, data, mq.WithTrack())
	if rpcResult != nil && rpcResult.Data != nil {
		trackId, ok := rpcResult.Data.(mq2.TrackID)

		defaultTimeout := 600
		if ok {
			timeout := defaultTimeout
			if ti, ok := metaContext.GetMetadata(ctx, "TIMEOUT"); ok {
				timeout, err = strconv.Atoi(ti)
				if err != nil {
					timeout = defaultTimeout
				}
			}
			respMqMsgPayload, err := trackId.Track(timeout)
			if err != nil {
				return nil, err
			}
			return respMqMsgPayload.Payload, nil
		}
	}
	return nil, nil

}

func GetContext() context.Context {
	ctx := context.Background()
	return ctx
}

func WithTimeout(ctx context.Context, value int) context.Context {
	if ctx == nil {
		ctx = GetContext()
	}
	return metaContext.SetMetadata(ctx, metaContext.META_TIMEOUT, fmt.Sprintf("%d", value))
}

func WithTrack(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = GetContext()
	}
	return metaContext.SetMetadata(ctx, metaContext.META_TRACK, "true")
}

func WithTraceSpan(ctx context.Context, span *trace.Span) context.Context {
	if ctx == nil {
		ctx = GetContext()
	}
	sc := span.SpanContext()
	sid := binary.BigEndian.Uint64(sc.SpanID[:])
	spanCtx := fmt.Sprintf("%s/%d;o=%d", hex.EncodeToString(sc.TraceID[:]), sid, int64(sc.TraceOptions))
	return metaContext.SetMetadata(ctx, metaContext.META_SPANCTX, spanCtx)
}

func WithBroadcast(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = GetContext()
	}
	return metaContext.SetMetadata(ctx, metaContext.META_BROADCAST, "true")
}

func WithKey(ctx context.Context, value string) context.Context {
	if ctx == nil {
		ctx = GetContext()
	}
	return metaContext.SetMetadata(ctx, metaContext.META_KEY, value)
}

func SetConetxt(ctx context.Context, key, value string) context.Context {
	return metaContext.SetMetadata(ctx, key, value)
}

func GetFromContext(ctx context.Context, k string) (string, bool) {
	return metaContext.GetMetadata(ctx, k)
}
