package app

import (
	"net/http"
)

// Interface defines the base of a controller managed by a controller app
type Interface interface {
	// Name returns the canonical name of the controller.
	Name() string
}

// Debuggable defines a controller that allows the controller app
// to expose a debugging handler for the controller
//
// If a controller implements Debuggable, and the returned handler is
// not nil, the controller app can mount the handler during startup.
type Debuggable interface {
	// DebuggingHandler returns a Handler that expose debugging information
	// for the controller, or nil if a debugging handler is not desired.
	//
	// The handler will be accessible at "/debug/controllers/{controllerName}/".
	DebuggingHandler() http.Handler
}

// HealthCheckable defines a controller that allows the controller app
// to include it in the health checks.
//
// If a controller implements HealthCheckable, and the returned check
// is not nil, the controller app can expose the check to the
// /healthz endpoint.
type HealthCheckable interface {
	// HealthChecker returns a UnnamedHealthChecker that the controller app
	// can choose to mount on the /healthz endpoint, or nil if no custom
	// health check is desired.
	//HealthChecker() healthz.UnnamedHealthChecker
}
