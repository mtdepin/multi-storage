package api

import (
	"compress/gzip"
	"mtcloud.com/mtstorage/cmd/chunker/services"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/klauspost/compress/gzhttp"
	"mtcloud.com/mtstorage/api"
	xhttp "mtcloud.com/mtstorage/pkg/http"
	"mtcloud.com/mtstorage/pkg/logger"
)

const (
	chunkerPIVersion = "v1"
)

type chunkerAPIHandlers struct {
	appendFileMap   map[string]*fsAppendFile
	appendFileMapMu sync.Mutex
	backend         *services.Chunker
}

// Represents the background append file.
type fsAppendFile struct {
	sync.Mutex
	parts    []PartInfo // List of parts appended.
	filePath string     // Absolute path of the file in the temp location.
}

// PartInfo - represents individual part metadata.
type PartInfo struct {
	// Part number that identifies the part. This is a positive integer between
	// 1 and 10,000.
	PartNumber int

	// Date and time at which the part was uploaded.
	LastModified time.Time

	// Entity tag returned when the part was initially uploaded.
	ETag string

	// Size in bytes of the part.
	Size int64

	// Decompressed Size.
	ActualSize int64
}

func RegisterHealthCheckRouter(router *mux.Router) {
	//todo
}

func RegisterMetricsRouter(router *mux.Router) {
	//todo
}

func RegisterAPIRouter(router *mux.Router, ck *services.Chunker) {
	chunkerAPI := chunkerAPIHandlers{
		appendFileMap: make(map[string]*fsAppendFile),
		backend:       ck,
	}

	// API Router
	apiRouter := router.PathPrefix("/cs/" + chunkerPIVersion).Subrouter()

	gz, err := gzhttp.NewWrapper(gzhttp.MinSize(1000), gzhttp.CompressionLevel(gzip.BestSpeed))
	if err != nil {
		// Static params, so this is very unlikely.
		logger.Fatal(err, "Unable to initialize server")
	}
	maxClients := xhttp.MaxClients
	//register router handler
	// /cs/v1/object [post]
	apiRouter.Methods(http.MethodPost).Path("/object").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(chunkerAPI.PutObjectHandler))))
	// /cs/v1/object? [post]
	apiRouter.Methods(http.MethodPost).Path("/postObject").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(chunkerAPI.PostObjectHandler))))
	// /cs/v1/object/xxxx [get]
	apiRouter.Methods(http.MethodGet).Path("/object/{cid:.+}").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(chunkerAPI.GetObjectHandler))))

	// /cs/v1/object/xxxx [delete]
	apiRouter.Methods(http.MethodDelete).Path("/object/{cid:.+}").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(chunkerAPI.DelObjectHandler))))

	// /cs/v1/newMultipart [post]
	apiRouter.Methods(http.MethodPost).Path("/newMultipart").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(chunkerAPI.NewMultipart))))

	// /cs/v1/putObjectPart [post]
	apiRouter.Methods(http.MethodPost).Path("/putObjectPart").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(chunkerAPI.PutObjectPart))))

	// /cs/v1/completeMultipart [post]
	apiRouter.Methods(http.MethodPost).Path("/completeMultipart").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(chunkerAPI.CompleteMultipart))))

	// /cs/v1/abortMultipartUpload [post]
	apiRouter.Methods(http.MethodPost).Path("/abortMultipartUpload").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(chunkerAPI.AbortMultipartUpload))))
	// /cs/v1/listObjectParts [get]
	apiRouter.Methods(http.MethodGet).Path("/listObjectParts").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(chunkerAPI.ListObjectParts))))
	// /cs/v1/getObjectDagTree [get]
	apiRouter.Methods(http.MethodGet).Path("/getObjectDagTree").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(chunkerAPI.GetObjectDagTree))))
	// /cs/v1/AddObjectCid [post]
	apiRouter.Methods(http.MethodPost).Path("/addObjectCid").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(chunkerAPI.AddObjectCid))))
}
