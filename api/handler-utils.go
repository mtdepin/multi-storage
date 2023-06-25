package api

import (
	"encoding/json"
	"go.opencensus.io/exporter/stackdriver/propagation"
	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/pkg/logger"
	utilruntime "mtcloud.com/mtstorage/pkg/runtime"
	"net/http"
)

const (
	copyDirective    = "COPY"
	replaceDirective = "REPLACE"
)

// HttpTraceAll Log headers and body.
func HttpTraceAll(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer utilruntime.HandleCrash()
		format := &propagation.HTTPFormat{}
		sc, ok := format.SpanContextFromRequest(r)
		if ok {
			ctx, span := trace.StartSpanWithRemoteParent(r.Context(), "HttpTraceAll", sc)
			span.AddAttributes(trace.StringAttribute("Host", r.Host))
			vars := r.URL.Query()
			args, _ := json.Marshal(vars)
			span.AddAttributes(trace.StringAttribute("args", string(args)))
			span.AddAttributes(trace.StringAttribute("method", r.Method))
			span.AddAttributes(trace.StringAttribute("uri", r.RequestURI))

			logger.Debug("------------------------------------------------")
			logger.Debugf("request: %s, method: %s", r.RequestURI, r.Method)
			logger.Debugf("body length: %d", r.ContentLength)
			logger.Debugf("query args: %s", args)
			//for k, _ := range vars {
			//	logger.Debugf("args :%s, %s", k, vars.Get(k))
			//}
			logger.Debug()
			defer span.End()
			r = r.WithContext(ctx)
		}

		//todo : do something
		f.ServeHTTP(w, r)
	}
}
