package httpapi

import (
	"compress/gzip"
	"mtcloud.com/mtstorage/cmd/nameserver/backend"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/klauspost/compress/gzhttp"
	"mtcloud.com/mtstorage/api"

	xhttp "mtcloud.com/mtstorage/pkg/http"
	"mtcloud.com/mtstorage/pkg/logger"
)

const (
	nameserverPIVersion = "v1"
)

type NameserverAPIHandlers struct {
	backend *backend.NameServer
}

func RegisterHealthCheckRouter(router *mux.Router) {
	//todo
}

func RegisterMetricsRouter(router *mux.Router) {
	//todo
}

// @title RegisterAPIRouter API
// @version 1.0
// @description This is maintain api router
// @BasePath /ns
func RegisterAPIRouter(router *mux.Router, ns *backend.NameServer) {
	nsAPI := NameserverAPIHandlers{
		backend: ns,
	}

	maxClients := xhttp.MaxClients

	// API Router
	apiRouter := router.PathPrefix("/ns/" + nameserverPIVersion).Subrouter()

	gz, err := gzhttp.NewWrapper(gzhttp.MinSize(1000), gzhttp.CompressionLevel(gzip.BestSpeed))
	if err != nil {
		// Static params, so this is very unlikely.
		logger.Fatal(err, "Unable to initialize server")
	}

	//xxx:8000/ns/v1/backend [get]
	apiRouter.Methods(http.MethodGet).Path("/backend").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetBackendInfo))))

	//xxx:8000/ns/v1/storage [get]
	apiRouter.Methods(http.MethodGet).Path("/storage").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetStorageInfo))))

	//xxx:8000/ns/v1/bucketinfo [get]
	apiRouter.Methods(http.MethodGet).Path("/bucketinfo").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetBucketInfoDetail))))

	// /ns/v1/object?bucket=xxx&&object=xxx  [get]
	apiRouter.Methods(http.MethodGet).Path("/object").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetObjectInfoHandler))))

	// /ns/v1/headobject?bucket=xxx&&object=xxx  [get]
	apiRouter.Methods(http.MethodHead).Path("/headobject").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.HeadObjectHandler))))

	// /ns/v1/object/list?xxx  [get] listobject
	apiRouter.Methods(http.MethodGet).Path("/object/list").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.ListObjectsHandler))))

	// /ns/v1/object/listversions?xxx  [get]
	apiRouter.Methods(http.MethodGet).Path("/object/versions").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.ListObjectVersionsHandler))))

	// /ns/v1/object/get/xxxx  [get]
	apiRouter.Methods(http.MethodGet).Path("/object/get/{object:.+}").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetObjectData))))

	// /ns/v1/object/delete?bucket=xxxx&object=xxx [delete]
	apiRouter.Methods(http.MethodDelete).Path("/object/delete").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.DeleteObject))))

	// /ns/v1/object/check?bucket=xxx&object=xxx [get]
	apiRouter.Methods(http.MethodGet).Path("/object/check").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.ObjectExistHandler))))

	// /ns/v1/object/cid?bucket=xxx&object=xxx [get]
	apiRouter.Methods(http.MethodGet).Path("/object/cid").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetObjectCidHandler))))

	// /ns/v1/chunker/address [get]
	apiRouter.Methods(http.MethodGet).Path("/chunker/address").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetChunkerAddress))))

	// /ns/v1/chunker/address/all [get]
	apiRouter.Methods(http.MethodGet).Path("/chunker/address/all").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetAllChunkerAddresses))))

	// /ns/v1/object/tag   [delete]
	apiRouter.Methods(http.MethodDelete).Path("/object/tag").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.DeleteObjectTagsHandler))))
	// /ns/v1/object/tag  [get]
	apiRouter.Methods(http.MethodGet).Path("/object/tag").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetObjectTagsHandler))))
	// /ns/v1/object/tag  [put]
	apiRouter.Methods(http.MethodPut).Path("/object/tag").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.PutObjectTagsHandler))))
	// /ns/v1/object/acl?bucket=xx&object=xx   [delete]
	apiRouter.Methods(http.MethodDelete).Path("/object/acl").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.DeleteObjectAclHandler))))
	// /ns/v1/object/acl?bucket=xx&object=xx  [get]
	apiRouter.Methods(http.MethodGet).Path("/object/acl").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetObjectAclHandler))))
	// /ns/v1/object/acl?bucket=xx&object=xxacl=xx  [put]
	apiRouter.Methods(http.MethodPut).Path("/object/acl").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.PutObjectAclHandler))))

	// /ns/v1/object/metadata  [put]
	apiRouter.Methods(http.MethodPost).Path("/object/metadata").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.PutObjectMetadata))))

	// /ns/v1/bucket [post]
	apiRouter.Methods(http.MethodPost).Path("/bucket").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.MakeBucketWithLocationHandler))))
	// /ns/v1/bucket?name=xxxx [get]
	apiRouter.Methods(http.MethodGet).Path("/bucket").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.ListBucketHandler))))
	// /ns/v1/bucket?name=xxxx [head]
	apiRouter.Methods(http.MethodHead).Path("/headbucket").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.HeadBucketHandler))))
	// /ns/v1/bucket [put]
	apiRouter.Methods(http.MethodPut).Path("/bucket").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.UpdateBucketHandler))))
	// /ns/v1/bucket/{bucket} [delete]
	apiRouter.Methods(http.MethodDelete).Path("/bucket/{bucket:.+}").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.DeleteBucketHandler))))
	// /ns/v1/versioning?bucket=xx?status=xx [put]
	apiRouter.Methods(http.MethodPut).Path("/versioning").HandlerFunc(
		gz(api.HttpTraceAll(nsAPI.PutBucketVersioningHandler)))
	// /ns/v1/versioning?bucket=xx [get]
	apiRouter.Methods(http.MethodGet).Path("/versioning").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetBucketVersioningHandler))))

	// /ns/v1/logging?bucket=xx&logging=xx [put]
	apiRouter.Methods(http.MethodPut).Path("/logging").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.PutBucketLoggingHandler))))
	// /ns/v1/logging?bucket=xx [get]
	apiRouter.Methods(http.MethodGet).Path("/logging").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetBucketLoggingHandler))))
	// /ns/v1/logging?bucket=xx [delete]
	apiRouter.Methods(http.MethodDelete).Path("/logging").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.DeleteBucketLoggingHandler))))
	// /ns/v1/policy?bucket=xx&policy=xx [put]
	apiRouter.Methods(http.MethodPut).Path("/policy").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.PutBucketPolicyHandler))))
	// /ns/v1/policy?bucket=xx [get]
	apiRouter.Methods(http.MethodGet).Path("/policy").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetBucketPolicyHandler))))
	// /ns/v1/policy?bucket=xx [delete]
	apiRouter.Methods(http.MethodDelete).Path("/policy").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.DeleteBucketPolicyHandler))))
	// /ns/v1/lifecycle?bucket=xx&lifecycle=xx [put]
	apiRouter.Methods(http.MethodPut).Path("/lifecycle").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.PutBucketLifecycleHandler))))
	// /ns/v1/lifecycle?bucket=xx [get]
	apiRouter.Methods(http.MethodGet).Path("/lifecycle").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetBucketLifecycleHandler))))
	// /ns/v1/lifecycle?bucket=xx [delete]
	apiRouter.Methods(http.MethodDelete).Path("/lifecycle").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.DeleteBucketLifecycleHandler))))
	// /ns/v1/acl?bucket=xx&acl=xx [put]
	apiRouter.Methods(http.MethodPut).Path("/acl").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.PutBucketAclHandler))))
	// /ns/v1/acl?bucket=xx [get]
	apiRouter.Methods(http.MethodGet).Path("/acl").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetBucketAclHandler))))
	// /ns/v1/acl?bucket=xx [delete]
	apiRouter.Methods(http.MethodDelete).Path("/acl").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.DeleteBucketAclHandler))))
	// /ns/v1/tag?bucket=xx&tag=xx [put]
	apiRouter.Methods(http.MethodPut).Path("/tag").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.PutBucketTagsHandler))))
	// /ns/v1/tag?bucket=xx [get]
	apiRouter.Methods(http.MethodGet).Path("/tag").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetBucketTagsHandler))))
	// /ns/v1/tag?bucket=xx [delete]
	apiRouter.Methods(http.MethodDelete).Path("/tag").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.DeleteBucketTagsHandler))))
	// /ns/v1/encryption?bucket=xx [delete]
	apiRouter.Methods(http.MethodGet).Path("/getEncryption").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.GetBucketEncryptionHandler))))
	apiRouter.Methods(http.MethodPost).Path("/putEncryption").HandlerFunc(
		maxClients(gz(api.HttpTraceAll(nsAPI.PutBucketEncryptionHandler))))
}
