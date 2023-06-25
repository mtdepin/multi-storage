package http

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/pkg/sys"
)

type requestConfig struct {
	mu sync.RWMutex

	requestsDeadline time.Duration
	requestsPool     chan struct{}

	requestsInQueue int32
}

func (t *requestConfig) init(maxRequests int, deadline int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	stats, err := sys.GetStats()
	if err != nil {
		// Default to 8 GiB, not critical.
		stats.TotalRAM = 8 << 30
	}

	if cap(t.requestsPool) < maxRequests {
		// Only replace if needed.
		// Existing requests will use the previous limit,
		// but new requests will use the new limit.
		// There will be a short overlap window,
		// but this shouldn't last long.
		t.requestsPool = make(chan struct{}, maxRequests)
	}
	t.requestsDeadline = time.Duration(deadline) * time.Second
}

func (t *requestConfig) addRequestsInQueue(i int32) {
	atomic.AddInt32(&t.requestsInQueue, i)
}

func (t *requestConfig) getRequestsPool() (chan struct{}, time.Duration) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.requestsPool == nil {
		return nil, time.Duration(0)
	}

	return t.requestsPool, t.requestsDeadline
}

var globalRequestConfig = requestConfig{}

func ReqConfigInit(maxRequests int, deadline int) {
	if maxRequests == 0 {
		maxRequests = 1024
	}

	if deadline == 0 {
		deadline = 120
	}
	globalRequestConfig.init(maxRequests, deadline)
}

// maxClients throttles the S3 API calls
func MaxClients(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pool, deadline := globalRequestConfig.getRequestsPool()
		if pool == nil {
			f.ServeHTTP(w, r)
			return
		}
		globalRequestConfig.addRequestsInQueue(1)
		deadlineTimer := time.NewTimer(deadline)
		defer deadlineTimer.Stop()

		select {
		case pool <- struct{}{}:
			defer func() { <-pool }()
			globalRequestConfig.addRequestsInQueue(-1)
			f.ServeHTTP(w, r)
		case <-deadlineTimer.C:
			// Send a http timeout message
			w.WriteHeader(http.StatusRequestTimeout)
			globalRequestConfig.addRequestsInQueue(-1)
			return
		case <-r.Context().Done():
			globalRequestConfig.addRequestsInQueue(-1)
			return
		}
	}
}
