package discall

import (
	"golang.org/x/xerrors"
	"mtcloud.com/mtstorage/pkg/logger"
	"reflect"
	"sync/atomic"
)

const chValue = "discall.ch.val"
const chClose = "discall.ch.close"

type outChanReg struct {
	reqID int64

	chID uint64
	ch   reflect.Value
}

// handleChanOut registers output channel for forwarding to client
func (s *Server) handleChanOut(ch reflect.Value, req int64, cf func(Response)) error {
	s.spawnOutChanHandlerOnce.Do(func() {
		go s.handleOutChans(cf)
	})
	id := atomic.AddUint64(&s.chanCtr, 1)

	select {
	case s.registerCh <- outChanReg{
		reqID: req,

		chID: id,
		ch:   ch,
	}:
		return nil
	case <-s.exiting:
		return xerrors.New("connection closing")
	}
}

//                 //
// Output channels //
//                 //

// handleOutChans handles channel communication on the server side
// (forwards channel messages to client)
func (s *Server) handleOutChans(cf func(Response)) {
	regV := reflect.ValueOf(s.registerCh)
	exitV := reflect.ValueOf(s.exiting)

	cases := []reflect.SelectCase{
		{ // registration chan always 0
			Dir:  reflect.SelectRecv,
			Chan: regV,
		},
		{ // exit chan always 1
			Dir:  reflect.SelectRecv,
			Chan: exitV,
		},
	}
	internal := len(cases)
	var caseToID []uint64

	for {
		chosen, val, ok := reflect.Select(cases)

		switch chosen {
		case 0: // registration channel
			if !ok {
				// control channel closed - signals closed connection
				// This shouldn't happen, instead the exiting channel should get closed
				logger.Warn("control channel closed")
				return
			}

			registration := val.Interface().(outChanReg)

			caseToID = append(caseToID, registration.chID)
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: registration.ch,
			})

			//send response
			resp := Response{
				Jsonrpc: "2.0",
				ID:      registration.reqID,
				Result:  registration.chID,
			}

			cf(resp)

			continue
		case 1: // exiting channel
			if !ok {
				// exiting channel closed - signals closed connection
				//
				// We're not closing any channels as we're on receiving end.
				// Also, context cancellation below should take care of any running
				// requests
				return
			}
			logger.Warn("exiting channel received a message")
			continue
		}

		if !ok {
			// Output channel closed, cleanup, and tell remote that this happened

			id := caseToID[chosen-internal]

			n := len(cases) - 1
			if n > 0 {
				cases[chosen] = cases[n]
				caseToID[chosen-internal] = caseToID[n-internal]
			}

			cases = cases[:n]
			caseToID = caseToID[:n-internal]

			//todo: send
			if id == 0 {

			}
			//if err := c.sendRequest(request{
			//	Jsonrpc: "2.0",
			//	ID:      nil, // notification
			//	Method:  chClose,
			//	Params:  []param{{v: reflect.ValueOf(id)}},
			//}); err != nil {
			//	log.Warnf("closed out channel sendRequest failed: %s", err)
			//}
			continue
		}

		// todo: forward message
		//if err := c.sendRequest(request{
		//	Jsonrpc: "2.0",
		//	ID:      nil, // notification
		//	Method:  chValue,
		//	Params:  []param{{v: reflect.ValueOf(caseToID[chosen-internal])}, {v: val}},
		//}); err != nil {
		//	log.Warnf("sendRequest failed: %s", err)
		//	return
		//}
	}
}
