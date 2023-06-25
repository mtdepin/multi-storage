package discall

import (
	"reflect"
	"time"

	"github.com/gorilla/websocket"
)

type ParamEncoder func(reflect.Value) (reflect.Value, error)

type Config struct {
	reconnectBackoff backoff
	pingInterval     time.Duration
	timeout          time.Duration

	paramEncoders map[reflect.Type]ParamEncoder

	noReconnect      bool
	proxyConnFactory func(func() (*websocket.Conn, error)) func() (*websocket.Conn, error) // for testing
}

func defaultConfig() Config {
	return Config{
		reconnectBackoff: backoff{
			minDelay: 100 * time.Millisecond,
			maxDelay: 5 * time.Second,
		},
		pingInterval: 5 * time.Second,
		timeout:      30 * time.Second,

		paramEncoders: map[reflect.Type]ParamEncoder{},
	}
}

type Option func(c *Config)

func WithTimeout(d time.Duration) func(c *Config) {
	return func(c *Config) {
		c.timeout = d
	}
}
