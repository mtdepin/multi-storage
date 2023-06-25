package discall

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/xerrors"
	metadata2 "mtcloud.com/mtstorage/pkg/discall/context/metadata"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/util"
	"reflect"
	"strings"
	"sync/atomic"
	"time"
)

type client struct {
	namespace     string
	paramEncoders map[reflect.Type]ParamEncoder

	doRequest func(context.Context, clientRequest) (clientResponse, error)
	exiting   <-chan struct{}
	idCtr     int64

	// chanHandlers is a map of client-side channel handlers
	chanHandlers map[uint64]func(m []byte, ok bool)
}

type clientResponse struct {
	Jsonrpc string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Method  string          `json:"method"`
	ID      int64           `json:"id"`
	Error   *respError      `json:"storageerror,omitempty"`
}

type makeChanSink func() (context.Context, func([]byte, bool))

type clientRequest struct {
	req   Request
	ready chan clientResponse

	// retCh provides a context and sink for handling incoming channel messages
	retCh makeChanSink
}

const (
	methodMinRetryDelay = 100 * time.Millisecond
	methodMaxRetryDelay = 10 * time.Minute
	defaultTimeout      = 10 //second
)

var (
	errorType   = reflect.TypeOf(new(error)).Elem()
	contextType = reflect.TypeOf(new(context.Context)).Elem()
)

func NewClient(ctx context.Context, sourceAddr string, addr string, namespace string, outs []interface{}, sf func(ctx context.Context, dist string, data []byte) ([]byte, error), opts ...Option) error {
	config := defaultConfig()
	for _, o := range opts {
		o(&config)
	}
	return callClient(ctx, sourceAddr, addr, namespace, outs, config, sf)
}

func callClient(clientCtx context.Context, source string, addr string, namespace string, outs []interface{}, config Config, sf func(ctx context.Context, dist string, data []byte) ([]byte, error)) error {
	c := client{
		namespace:     namespace,
		paramEncoders: config.paramEncoders,
		chanHandlers:  map[uint64]func(m []byte, ok bool){},
	}

	stop := make(chan struct{})
	c.exiting = stop

	c.doRequest = func(ctx context.Context, cr clientRequest) (clientResponse, error) {
		jsonbytes, err := json.Marshal(&cr.req)
		if err != nil {
			return clientResponse{}, xerrors.Errorf("marshaling request: %w", err)
		}

		//merge context from clientCtx
		if mt, ok := metadata2.FromContext(clientCtx); ok {
			ctx = metadata2.MergeContext(ctx, mt, false)
			ctx = metadata2.Set(ctx, "TIMESTAMP", fmt.Sprintf("%v", time.Now().UnixNano()/1e6))
		}

		requestId := util.GetRequestId()
		mqMessagePayload := &struct {
			RequestId string            `json:"requestId,omitempty"`
			From      string            `json:"from,omitempty"`
			To        string            `json:"To,omitempty"`
			MegType   int               `json:"megType,omitempty"`
			Payload   []byte            `json:"payload,omitempty"`
			Digest    string            `json:"digest,omitempty"`
			Version   string            `json:"version,omitempty"`
			Extends   map[string]string `json:"extends,omitempty"`
		}{
			RequestId: requestId,
			From:      source,
			To:        addr,
			MegType:   999,
			Payload:   jsonbytes,
		}
		metadata, ok := metadata2.FromContext(ctx)
		if ok {
			mqMessagePayload.Extends = metadata
		}

		var msgPayload, _ = json.Marshal(mqMessagePayload)

		DoLog("---> [discall] send: %s", string(jsonbytes))
		if sf == nil {
			logger.Error("sf is nil")
			return clientResponse{}, errors.New("sf not found")
		}
		respPayload, err := sf(ctx, addr, msgPayload)
		if err != nil {
			logger.Errorf("SendWithTracker storageerror: %v", err)
			return clientResponse{}, err
		}

		if respPayload == nil {
			return clientResponse{}, nil
		}

		DoLog(" <--- [discall] receive: %s", string(respPayload))
		var resp = clientResponse{}
		if err := json.Unmarshal(respPayload, &resp); err != nil {
			return clientResponse{}, xerrors.Errorf("unmarshaling response: %w", err)
		}

		if resp.ID != *cr.req.ID {
			return clientResponse{}, xerrors.New("request and response id didn't match")
		}
		return resp, nil
	}

	if err := c.provide(outs); err != nil {
		return err
	}

	return nil
}

func DoLog(format string, content string) {
	if strings.Contains(content, "Heartbeat") {
		return
	}
	//tmp := ""
	//maxLen := len(content)
	//if maxLen > 500 {
	//	maxLen = 500
	//	tmp = "..."
	//}
	//logger.Infof(format, content[:maxLen]+tmp)
	logger.Infof(format, content)

}

func (c *client) provide(outs []interface{}) error {
	for _, handler := range outs {
		htyp := reflect.TypeOf(handler)
		if htyp.Kind() != reflect.Ptr {
			return xerrors.New("expected handler to be a pointer")
		}
		typ := htyp.Elem()
		if typ.Kind() != reflect.Struct {
			return xerrors.New("handler should be a struct")
		}

		val := reflect.ValueOf(handler)

		for i := 0; i < typ.NumField(); i++ {
			fn, err := c.makeRpcFunc(typ.Field(i))
			if err != nil {
				return err
			}

			val.Elem().Field(i).Set(fn)
		}
	}

	return nil
}

func (c *client) makeRpcFunc(f reflect.StructField) (reflect.Value, error) {
	ftyp := f.Type
	if ftyp.Kind() != reflect.Func {
		return reflect.Value{}, xerrors.New("handler field not a func")
	}

	fun := &rpcFunc{
		client: c,
		ftyp:   ftyp,
		name:   f.Name,
		retry:  f.Tag.Get("retry") == "true",
	}
	fun.valOut, fun.errOut, fun.nout = processFuncOut(ftyp)

	if ftyp.NumIn() > 0 && ftyp.In(0) == contextType {
		fun.hasCtx = 1
	}
	fun.returnValueIsChannel = fun.valOut != -1 && ftyp.Out(fun.valOut).Kind() == reflect.Chan

	return reflect.MakeFunc(ftyp, fun.handleRpcCall), nil
}

func (c *client) sendRequest(ctx context.Context, req Request, chCtor makeChanSink) (clientResponse, error) {
	creq := clientRequest{
		req:   req,
		ready: make(chan clientResponse, 1),

		retCh: chCtor,
	}

	return c.doRequest(ctx, creq)
}

type rpcFunc struct {
	client *client

	ftyp reflect.Type
	name string

	nout   int
	valOut int
	errOut int

	hasCtx               int
	returnValueIsChannel bool

	retry bool
}

func (fn *rpcFunc) processResponse(resp clientResponse, rval reflect.Value) []reflect.Value {
	out := make([]reflect.Value, fn.nout)

	if fn.valOut != -1 {
		out[fn.valOut] = rval
	}
	if fn.errOut != -1 {
		out[fn.errOut] = reflect.New(errorType).Elem()
		if resp.Error != nil {
			out[fn.errOut].Set(reflect.ValueOf(resp.Error))
		}
	}

	return out
}

func (fn *rpcFunc) processError(err error) []reflect.Value {
	out := make([]reflect.Value, fn.nout)

	if fn.valOut != -1 {
		out[fn.valOut] = reflect.New(fn.ftyp.Out(fn.valOut)).Elem()
	}
	if fn.errOut != -1 {
		out[fn.errOut] = reflect.New(errorType).Elem()
		out[fn.errOut].Set(reflect.ValueOf(&ErrClient{err}))
	}

	return out
}

func (fn *rpcFunc) handleRpcCall(args []reflect.Value) (results []reflect.Value) {
	id := atomic.AddInt64(&fn.client.idCtr, 1)
	params := make([]param, len(args)-fn.hasCtx)
	for i, arg := range args[fn.hasCtx:] {
		enc, found := fn.client.paramEncoders[arg.Type()]
		if found {
			// custom param encoder
			var err error
			arg, err = enc(arg)
			if err != nil {
				return fn.processError(fmt.Errorf("sendRequest failed: %w", err))
			}
		}

		params[i] = param{
			v: arg,
		}
	}

	var ctx context.Context
	if fn.hasCtx == 1 {
		ctx = args[0].Interface().(context.Context)
	}

	retVal := func() reflect.Value { return reflect.Value{} }

	// if the function returns a channel, we need to provide a sink for the
	// messages
	var chCtor makeChanSink
	if fn.returnValueIsChannel {
		retVal, chCtor = fn.client.makeOutChan(ctx, fn.ftyp, fn.valOut)
	}

	req := Request{
		Jsonrpc: "2.0",
		ID:      &id,
		Method:  fn.client.namespace + "." + fn.name,
		Params:  params,
	}

	//if span != nil {
	//	span.AddAttributes(trace.StringAttribute("method", req.Method))
	//
	//	eSC := base64.StdEncoding.EncodeToString(
	//		propagation.Binary(span.SpanContext()))
	//	req.Meta = map[string]string{
	//		"SpanContext": eSC,
	//	}
	//}

	b := backoff{
		maxDelay: methodMaxRetryDelay,
		minDelay: methodMinRetryDelay,
	}

	var resp clientResponse
	var err error
	// keep retrying if got a forced closed websocket conn and calling method
	// has retry annotation
	for attempt := 0; true; attempt++ {
		resp, err = fn.client.sendRequest(ctx, req, chCtor)
		if err != nil {
			return fn.processError(fmt.Errorf("sendRequest failed: %w", err))
		}

		if resp.ID != *req.ID && resp.ID != 0 {
			return fn.processError(xerrors.New("request and response id didn't match"))
		}

		if fn.valOut != -1 && !fn.returnValueIsChannel {
			val := reflect.New(fn.ftyp.Out(fn.valOut))

			if resp.Result != nil {
				logger.Debugf("rpc result", "type", fn.ftyp.Out(fn.valOut))
				if err := json.Unmarshal(resp.Result, val.Interface()); err != nil {
					logger.Warnf("unmarshaling failed", "message", string(resp.Result))
					return fn.processError(xerrors.Errorf("unmarshaling result: %w", err))
				}
			}

			retVal = func() reflect.Value { return val.Elem() }
		}
		retry := resp.Error != nil && resp.Error.Code == 2 && fn.retry
		if !retry {
			break
		}

		time.Sleep(b.next(attempt))
	}

	return fn.processResponse(resp, retVal())
}

func (c *client) makeOutChan(ctx context.Context, ftyp reflect.Type, valOut int) (func() reflect.Value, makeChanSink) {
	retVal := reflect.Zero(ftyp.Out(valOut))

	chCtor := func() (context.Context, func([]byte, bool)) {
		// unpack chan type to make sure it's reflect.BothDir
		ctyp := reflect.ChanOf(reflect.BothDir, ftyp.Out(valOut).Elem())
		ch := reflect.MakeChan(ctyp, 0) // todo: buffer?
		retVal = ch.Convert(ftyp.Out(valOut))

		incoming := make(chan reflect.Value, 32)

		// gorotuine to handle buffering of items
		go func() {
			buf := (&list.List{}).Init()

			for {
				front := buf.Front()

				cases := []reflect.SelectCase{
					{
						Dir:  reflect.SelectRecv,
						Chan: reflect.ValueOf(ctx.Done()),
					},
					{
						Dir:  reflect.SelectRecv,
						Chan: reflect.ValueOf(incoming),
					},
				}

				if front != nil {
					cases = append(cases, reflect.SelectCase{
						Dir:  reflect.SelectSend,
						Chan: ch,
						Send: front.Value.(reflect.Value).Elem(),
					})
				}

				chosen, val, ok := reflect.Select(cases)

				switch chosen {
				case 0:
					ch.Close()
					return
				case 1:
					if ok {
						vvval := val.Interface().(reflect.Value)
						buf.PushBack(vvval)
						if buf.Len() > 1 {
							if buf.Len() > 10 {
								logger.Warnf("rpc output message buffer", "n", buf.Len())
							} else {
								logger.Debugf("rpc output message buffer", "n", buf.Len())
							}
						}
					} else {
						incoming = nil
					}

				case 2:
					buf.Remove(front)
				}

				if incoming == nil && buf.Len() == 0 {
					ch.Close()
					return
				}
			}
		}()

		return ctx, func(result []byte, ok bool) {
			if !ok {
				close(incoming)
				return
			}

			val := reflect.New(ftyp.Out(valOut).Elem())
			if err := json.Unmarshal(result, val.Interface()); err != nil {
				logger.Errorf("storageerror unmarshaling chan response: %s", err)
				return
			}

			if ctx.Err() != nil {
				logger.Errorf("got rpc message with cancelled context: %s", ctx.Err())
				return
			}

			select {
			case incoming <- val:
			case <-ctx.Done():
			}
		}
	}

	return func() reflect.Value { return retVal }, chCtor
}

//func (c *client) handleChanRet(cr clientRequest, trackId common.TrackID, timeout int) (clientResponse, storageerror) {
//	respMqMsgPayload, err := trackId.Track(timeout)
//	if err != nil {
//		return clientResponse{}, err
//	}
//	if respMqMsgPayload.Payload == nil {
//		return clientResponse{}, xerrors.Errorf("response Payload is nil")
//	}
//
//	DoLog(" <--- [discall] receive: %s", string(respMqMsgPayload.Payload))
//	var resp = clientResponse{}
//	if err := json.Unmarshal(respMqMsgPayload.Payload, &resp); err != nil {
//		return clientResponse{}, xerrors.Errorf("unmarshaling response: %w", err)
//	}
//
//	if resp.ID != *cr.req.ID {
//		return clientResponse{}, xerrors.New("request and response id didn't match")
//	}
//
//	// output is channel
//	var chid uint64
//	if err := json.Unmarshal(resp.Result, &chid); err != nil {
//		logger.Errorf("failed to unmarshal channel id response: %s, data '%s'", err, string(resp.Result))
//		return clientResponse{}, err
//	}
//
//	var chanCtx context.Context
//	//chanCtx, c.chanHandlers[chid] = cr.retCh()
//	chanCtx, c.chanHandlers[chid] = cr.retCh()
//	//go c.handleCtxAsync(chanCtx, *cr.req.ID)
//	if chanCtx != nil {
//
//	}
//
//	//LOOP:
//	//	for  {
//	//		hnd, ok := c.chanHandlers[chid]
//	//		if !ok {
//	//			logger.Errorf("xrpc.ch.val: handler %d not found", chid)
//	//			return clientResponse{}, err
//	//		}
//	//
//	//		hnd(resp.Result, true)
//	//	}
//
//	//cr.ready <- clientResponse{
//	//	Jsonrpc: resp.Jsonrpc,
//	//	Result:  resp.Result,
//	//	ID:      resp.ID,
//	//	Error:   resp.Error,
//	//}
//	return resp, nil
//}

// ErrClient is an storageerror which occurred on the chunker side the library
type ErrClient struct {
	err error
}

func (e *ErrClient) Error() string {
	return fmt.Sprintf("RPC chunker storageerror: %s", e.err)
}

// Unwrap unwraps the actual storageerror
func (e *ErrClient) Unwrap(err error) error {
	return e.err
}
