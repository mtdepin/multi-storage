package stats

import (
	"io"
	"net/http"
	"sync/atomic"
)

// IncomingTrafficMeter counts the incoming bytes from the underlying request.Body.
type IncomingTrafficMeter struct {
	countBytes int64
	io.ReadCloser
}

// Read calls the underlying Read and counts the transferred bytes.
func (r *IncomingTrafficMeter) Read(p []byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	atomic.AddInt64(&r.countBytes, int64(n))

	return n, err
}

// BytesCount returns the number of transferred bytes
func (r *IncomingTrafficMeter) BytesCount() int64 {
	return atomic.LoadInt64(&r.countBytes)
}

// OutgoingTrafficMeter counts the outgoing bytes through the responseWriter.
type OutgoingTrafficMeter struct {
	countBytes int64
	// wrapper for underlying http.ResponseWriter.
	http.ResponseWriter
}

// Write calls the underlying write and counts the output bytes
func (w *OutgoingTrafficMeter) Write(p []byte) (n int, err error) {
	n, err = w.ResponseWriter.Write(p)
	atomic.AddInt64(&w.countBytes, int64(n))
	return n, err
}

// Flush calls the underlying Flush.
func (w *OutgoingTrafficMeter) Flush() {
	w.ResponseWriter.(http.Flusher).Flush()
}

// BytesCount returns the number of transferred bytes
func (w *OutgoingTrafficMeter) BytesCount() int64 {
	return atomic.LoadInt64(&w.countBytes)
}
