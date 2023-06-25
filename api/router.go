package api

import "github.com/gorilla/mux"

// List of some generic handlers which are applied for all incoming requests.
var GlobalHandlers = []mux.MiddlewareFunc{
	// filters HTTP headers which are treated as metadata and are reserved
	// for internal use only.
	filterReservedMetadata,
	// Auth handler verifies incoming authorization headers and
	// routes them accordingly. Client receives a HTTP storageerror for
	// invalid/unsupported signatures.
	setAuthHandler,
	// Validates all incoming requests to have a valid date header.
	setTimeValidityHandler,
}
