package api

import (
	"net/http"
)

// ServeHTTP fails if the request contains at least one reserved header which
// would be treated as metadata.
func filterReservedMetadata(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//todo : do something
		h.ServeHTTP(w, r)
	})
}

func setAuthHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//todo : do something
		h.ServeHTTP(w, r)
	})
}

func setTimeValidityHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//todo : do something
		h.ServeHTTP(w, r)
	})
}
