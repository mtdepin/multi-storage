package api

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	xhttp "mtcloud.com/mtstorage/pkg/http"
	"net/http"
	"time"
)

// Returns a hexadecimal representation of time at the
// time response is sent to the client.
func mustGetRequestID(t time.Time) string {
	return fmt.Sprintf("%X", t.UnixNano())
}

// setEventStreamHeaders to allow proxies to avoid buffering proxy responses
func setEventStreamHeaders(w http.ResponseWriter) {
	w.Header().Set(xhttp.ContentType, "text/event-stream")
	w.Header().Set(xhttp.CacheControl, "no-cache") // nginx to turn off buffering
	w.Header().Set("X-Accel-Buffering", "no")      // nginx to turn off buffering
}

// Write http common headers
func setCommonHeaders(w http.ResponseWriter) {
	// Set the "Server" http header.
	w.Header().Set(xhttp.ServerInfo, "mtoss")

	// Set `x-amz-bucket-region` only if region is set on the server
	// by default minio uses an empty region.
	//w.Header().Set(xhttp.AmzBucketRegion, region)
	w.Header().Set(xhttp.AcceptRanges, "bytes")

	// Remove sensitive information
	//crypto.RemoveSensitiveHeaders(w.Header())
}

// Encodes the response headers into XML format.
func EncodeResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	e := xml.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}

// Encodes the response headers into JSON format.
func encodeResponseJSON(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	e := json.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}
