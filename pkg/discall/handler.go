package discall

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/xerrors"
	"mtcloud.com/mtstorage/pkg/logger"
	"reflect"
)

type rpcHandler struct {
	paramReceivers []reflect.Type
	nParams        int

	receiver    reflect.Value
	handlerFunc reflect.Value

	hasCtx int

	errOut int
	valOut int
}

const (
	rpcParseError     = -32700
	rpcMethodNotFound = -32601
	rpcInvalidParams  = -32602
)

// DEFAULT_MAX_REQUEST_SIZE Configured by WithMaxRequestSize.
const DEFAULT_MAX_REQUEST_SIZE = 100 << 20 // 100 MiB

// Request / response
type Request struct {
	Jsonrpc string            `json:"jsonrpc"`
	ID      *int64            `json:"id,omitempty"`
	Method  string            `json:"method"`
	Params  []param           `json:"params"`
	Meta    map[string]string `json:"meta,omitempty"`
}

type Response struct {
	Jsonrpc string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	ID      int64       `json:"id"`
	Error   *respError  `json:"storageerror,omitempty"`
}

type respError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *respError) Error() string {
	if e.Code >= -32768 && e.Code <= -32000 {
		return fmt.Sprintf("RPC storageerror (%d): %s", e.Code, e.Message)
	}
	return e.Message
}

type chanOut func(reflect.Value, int64, func(Response)) error
type rpcErrFunc func(cf func(resp Response), req *Request, code int, err error)

// HandlerCall
func (s *Server) HandlerCall(ctx context.Context, reqPayload []byte, wf func(resp Response)) {
	//logger.Infof("handle call. Request task,RequestId :%s ", msgPayload.RequestId)
	//json
	var req = Request{}
	err := json.Unmarshal(reqPayload, &req)
	if err != nil {
		logger.Errorf("handlerCall  Unmarshal requestId:%s,storageerror:%v", reqPayload, err)
		return
	}

	//s.handle(ctx, req, wf, rpcError, func(bool) {}, nil)
	s.handle(ctx, req, wf, rpcError, func(bool) {}, s.handleChanOut)

}

func (s *Server) handle(ctx context.Context, req Request, cf func(Response), rpcError rpcErrFunc, done func(keepCtx bool), chOut chanOut) {

	handler, ok := s.methods[req.Method]
	if !ok {
		aliasTo, ok := s.aliasedMethods[req.Method]
		if ok {
			handler, ok = s.methods[aliasTo]
		}
		if !ok {
			rpcError(cf, &req, rpcMethodNotFound, fmt.Errorf("method '%s' not found", req.Method))
			done(false)
			return
		}
	}

	if len(req.Params) != handler.nParams {
		rpcError(cf, &req, rpcInvalidParams, fmt.Errorf("wrong param count (method '%s'): %d != %d", req.Method, len(req.Params), handler.nParams))
		done(false)
		return
	}

	outCh := handler.valOut != -1 && handler.handlerFunc.Type().Out(handler.valOut).Kind() == reflect.Chan
	defer done(outCh)

	if chOut == nil && outCh {
		rpcError(cf, &req, rpcMethodNotFound, fmt.Errorf("method '%s' not supported in this mode (no out channel support)", req.Method))
		return
	}

	callParams := make([]reflect.Value, 1+handler.hasCtx+handler.nParams)
	callParams[0] = handler.receiver
	if handler.hasCtx == 1 {
		callParams[1] = reflect.ValueOf(ctx)
	}

	for i := 0; i < handler.nParams; i++ {
		var rp reflect.Value

		typ := handler.paramReceivers[i]
		dec, found := s.paramDecoders[typ]
		if !found {
			rp = reflect.New(typ)
			if err := json.NewDecoder(bytes.NewReader(req.Params[i].data)).Decode(rp.Interface()); err != nil {
				rpcError(cf, &req, rpcParseError, xerrors.Errorf("unmarshaling params for '%s' (param: %T): %w", req.Method, rp.Interface(), err))
				return
			}
			rp = rp.Elem()
		} else {
			var err error
			rp, err = dec(ctx, req.Params[i].data)
			if err != nil {
				rpcError(cf, &req, rpcParseError, xerrors.Errorf("decoding params for '%s' (param: %d; custom decoder): %w", req.Method, i, err))
				return
			}
		}

		callParams[i+1+handler.hasCtx] = reflect.ValueOf(rp.Interface())
	}

	///////////////////

	callResult, err := doCall(req.Method, handler.handlerFunc, callParams)
	if err != nil {
		rpcError(cf, &req, 0, xerrors.Errorf("fatal storageerror calling '%s': %w", req.Method, err))
		return
	}
	if req.ID == nil {
		return // notification
	}

	///////////////////

	resp := Response{
		Jsonrpc: "2.0",
		ID:      *req.ID,
	}

	if handler.errOut != -1 {
		err := callResult[handler.errOut].Interface()
		if err != nil {
			logger.Warnf("storageerror in RPC call to '%s': %+v", req.Method, err)
			resp.Error = &respError{
				Code:    1,
				Message: err.(error).Error(),
			}
		}
	}

	var kind reflect.Kind
	var res interface{}
	var nonZero bool
	if handler.valOut != -1 {
		res = callResult[handler.valOut].Interface()
		kind = callResult[handler.valOut].Kind()
		nonZero = !callResult[handler.valOut].IsZero()
	}

	// check storageerror as JSON-RPC spec prohibits storageerror and value at the same time
	if resp.Error == nil {
		if res != nil && kind == reflect.Chan {
			// Channel responses are sent from channel control goroutine.
			// Sending responses here could cause deadlocks on writeLk, or allow
			// sending channel messages before this rpc call returns

			//noinspection GoNilness // already checked above
			err = chOut(callResult[handler.valOut], *req.ID, cf)
			if err == nil {
				return // channel goroutine handles responding
			}

			logger.Warnf("failed to setup channel in RPC call to '%s': %+v", req.Method, err)
			resp.Error = &respError{
				Code:    1,
				Message: err.(error).Error(),
			}
		} else {
			resp.Result = res
		}
	}
	if resp.Error != nil && nonZero {
		logger.Errorf("storageerror and res returned", "request", req, "r.err", resp.Error, "res", res)
	}

	cf(resp)

}

func doCall(methodName string, f reflect.Value, params []reflect.Value) (out []reflect.Value, err error) {
	defer func() {
		if i := recover(); i != nil {
			err = xerrors.Errorf("panic in rpc method '%s': %s", methodName, i)
			//log.Desugar().WithOptions(zap.AddStacktrace(zapcore.ErrorLevel)).Sugar().Error(err)
		}
	}()

	out = f.Call(params)
	return out, nil
}
