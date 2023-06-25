package tracing

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"go.opencensus.io/trace"
	"strconv"
	"strings"
)

const (
	httpHeaderMaxSize = 200
	httpHeader        = `X-Cloud-Trace-Context`
)

// SpanContextToRequest modifies the given request to include a Stackdriver Trace header.
func SpanContextToRequest(sc trace.SpanContext, ctx context.Context) {
	sid := binary.BigEndian.Uint64(sc.SpanID[:])
	header := fmt.Sprintf("%s/%d;o=%d", hex.EncodeToString(sc.TraceID[:]), sid, int64(sc.TraceOptions))
	context.WithValue(ctx, "httpHeader", header)
}

// SpanContextFromRequest extracts a Stackdriver Trace span context from incoming requests.
func SpanContextFromStr(h string) (sc trace.SpanContext, ok bool) {
	//h := req.Header.Get(httpHeader)
	// See https://cloud.google.com/trace/docs/faq for the header HTTPFormat.
	// Return if the header is empty or missing, or if the header is unreasonably
	// large, to avoid making unnecessary copies of a large string.
	if h == "" || len(h) > httpHeaderMaxSize {
		return trace.SpanContext{}, false
	}

	// Parse the trace id field.
	slash := strings.Index(h, `/`)
	if slash == -1 {
		return trace.SpanContext{}, false
	}
	tid, h := h[:slash], h[slash+1:]

	buf, err := hex.DecodeString(tid)
	if err != nil {
		return trace.SpanContext{}, false
	}
	copy(sc.TraceID[:], buf)

	// Parse the span id field.
	spanstr := h
	semicolon := strings.Index(h, `;`)
	if semicolon != -1 {
		spanstr, h = h[:semicolon], h[semicolon+1:]
	}
	sid, err := strconv.ParseUint(spanstr, 10, 64)
	if err != nil {
		return trace.SpanContext{}, false
	}
	binary.BigEndian.PutUint64(sc.SpanID[:], sid)

	// Parse the options field, options field is optional.
	if !strings.HasPrefix(h, "o=") {
		return sc, true
	}
	o, err := strconv.ParseUint(h[2:], 10, 64)
	if err != nil {
		return trace.SpanContext{}, false
	}
	sc.TraceOptions = trace.TraceOptions(o)
	return sc, true
}
