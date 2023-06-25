package tracing

import (
	"mtcloud.com/mtstorage/pkg/config"
	"mtcloud.com/mtstorage/pkg/logger"
	"os"
	"strings"

	"contrib.go.opencensus.io/exporter/jaeger"
	"go.opencensus.io/trace"
)

const (
	// environment variable names
	envCollectorEndpoint = "JAEGER_COLLECTOR_ENDPOINT"
	envAgentEndpoint     = "JAEGER_AGENT_ENDPOINT"
	envAgentHost         = "JAEGER_AGENT_HOST"
	envAgentPort         = "JAEGER_AGENT_PORT"
	envJaegerUser        = "JAEGER_USERNAME"
	envJaegerCred        = "JAEGER_PASSWORD"
)

// When sending directly to the collector, agent options are ignored.
// The collector endpoint is an HTTP or HTTPs URL.
// The agent endpoint is a thrift/udp protocol and should be given
// as a string like "hostname:port". The agent can also be configured
// with separate host and port variables.
func jaegerOptsFromEnv(opts *jaeger.Options, c config.JaegerConfig) bool {
	var e string
	var ok bool
	if e, ok = os.LookupEnv(envJaegerUser); ok {
		if p, ok := os.LookupEnv(envJaegerCred); ok {
			opts.Username = e
			opts.Password = p
		} else {
			logger.Warn("jaeger username supplied with no password. authentication will not be used.")
		}
	}
	if e, ok = os.LookupEnv(envCollectorEndpoint); ok {
		opts.CollectorEndpoint = e
		logger.Infof("jaeger tracess will send to collector %s", e)
		return true
	}
	if e, ok = os.LookupEnv(envAgentEndpoint); ok {
		logger.Infof("jaeger traces will be sent to agent %s", e)
		opts.AgentEndpoint = e
		return true
	}

	if c.Url != "" {
		logger.Infof("jaeger traces will be sent to agent %s", c.Url)
		opts.AgentEndpoint = c.Url
		return true
	}

	if e, ok = os.LookupEnv(envAgentHost); ok {
		if p, ok := os.LookupEnv(envAgentPort); ok {
			opts.AgentEndpoint = strings.Join([]string{e, p}, ":")
		} else {
			opts.AgentEndpoint = strings.Join([]string{e, "6831"}, ":")
		}
		logger.Infof("jaeger traces will be sent to agent %s", opts.AgentEndpoint)
		return true
	}
	return false
}

func SetupJaegerTracing(serviceName string, c config.JaegerConfig) *jaeger.Exporter {
	logger.Info("jaeger url: ", c.Url)
	opts := jaeger.Options{}
	if !jaegerOptsFromEnv(&opts, c) {
		return nil
	}

	opts.ServiceName = serviceName
	je, err := jaeger.NewExporter(opts)
	if err != nil {
		logger.Error("failed to create the jaeger exporter", "storageerror", err)
		return nil
	}

	trace.RegisterExporter(je)
	trace.ApplyConfig(trace.Config{
		DefaultSampler: trace.AlwaysSample(),
	})
	return je
}
