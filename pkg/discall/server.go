package discall

import (
	"mtcloud.com/mtstorage/pkg/logger"
	"reflect"
	"sync"
)

var CallServer *Server

func init() {
	CallServer = NewServer()
}

// Server provides a jsonrpc 2.0 http server handler
type Server struct {
	methods map[string]rpcHandler

	// aliasedMethods contains a map of alias:original method names.
	// These are used as fallbacks if a method is not found by the given method name.
	aliasedMethods map[string]string

	paramDecoders map[reflect.Type]ParamDecoder

	maxRequestSize int64

	exiting                 chan struct{}
	spawnOutChanHandlerOnce sync.Once

	// chanCtr is a counter used for identifying output channels on the server side
	chanCtr uint64

	registerCh chan outChanReg
}

// NewServer creates new RPCServer instance
func NewServer(opts ...ServerOption) *Server {
	config := defaultServerConfig()
	for _, o := range opts {
		o(&config)
	}

	return &Server{
		methods:        map[string]rpcHandler{},
		aliasedMethods: map[string]string{},
		paramDecoders:  config.paramDecoders,
		maxRequestSize: config.maxRequestSize,
	}
}

func rpcError(cf func(resp Response), req *Request, code int, err error) {
	logger.Errorf("RPC Error: %s", err)
	logger.Warnf("rpc storageerror: %s", err)

	if req.ID == nil { // notification
		return
	}

	resp := Response{
		Jsonrpc: "2.0",
		ID:      *req.ID,
		Error: &respError{
			Code:    code,
			Message: err.Error(),
		},
	}

	cf(resp)
}

// Register registers new RPC handler
//
// Handler is any value with methods defined
func (s *Server) Register(namespace string, handler interface{}) {
	s.register(namespace, handler)
}

func (s *Server) register(namespace string, r interface{}) {
	val := reflect.ValueOf(r)
	//TODO: expect ptr

	for i := 0; i < val.NumMethod(); i++ {
		method := val.Type().Method(i)

		funcType := method.Func.Type()
		hasCtx := 0
		if funcType.NumIn() >= 2 && funcType.In(1) == contextType {
			hasCtx = 1
		}

		ins := funcType.NumIn() - 1 - hasCtx
		recvs := make([]reflect.Type, ins)
		for i := 0; i < ins; i++ {
			recvs[i] = method.Type.In(i + 1 + hasCtx)
		}

		valOut, errOut, _ := processFuncOut(funcType)

		s.methods[namespace+"."+method.Name] = rpcHandler{
			paramReceivers: recvs,
			nParams:        ins,

			handlerFunc: method.Func,
			receiver:    val,

			hasCtx: hasCtx,

			errOut: errOut,
			valOut: valOut,
		}
	}
}

func (s *Server) AliasMethod(alias, original string) {
	s.aliasedMethods[alias] = original
}

var _ error = &respError{}
