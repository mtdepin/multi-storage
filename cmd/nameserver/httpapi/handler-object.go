package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"

	"go.opencensus.io/trace"
	node_util "mtcloud.com/mtstorage/node/util"

	"mtcloud.com/mtstorage/api"
	"mtcloud.com/mtstorage/cmd/nameserver/metadata"
	error2 "mtcloud.com/mtstorage/pkg/storageerror"
	"mtcloud.com/mtstorage/util"
)

type parsedOpts struct {
	ObjOptions metadata.ObjectOptions
	Err        error
	HttpCode   int
}

func (h *NameserverAPIHandlers) GetChunkerAddress(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetAllChunkerAddresses")
	defer span.End()
	info, _ := metadata.QueryBucketInfo(ctx, r.URL.Query().Get("bucket"))
	cs := h.backend.GetStorageInfoFromNameServer(ctx)
	bucketLocalCs := make([]node_util.ChunkerNodeInfo, 0)
	sort.Slice(cs, func(i, j int) bool {
		return cs[i].AvailableSpace > cs[j].AvailableSpace
	})
	for i := range cs {
		if cs[i].Region.Name == info.Location {
			bucketLocalCs = append(bucketLocalCs, cs[i])
		}
	}

	if len(cs) > 0 {
		r := util.GetRandInt(len(cs))
		util.WriteJsonQuiet(w, http.StatusOK, cs[r])
		//bucketLocalCs = append(bucketLocalCs, cs[0])
		//util.WriteJsonQuiet(w, http.StatusOK, bucketLocalCs[0])
		return
	}
	util.WriteJsonQuiet(w, http.StatusNotFound, "")
}

func (h *NameserverAPIHandlers) GetObjectData(w http.ResponseWriter, r *http.Request) {

}

func (h *NameserverAPIHandlers) GetAllChunkerAddresses(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetAllChunkerAddresses")
	defer span.End()

	var allAddress = make([]string, 0)
	cs := h.backend.GetStorageInfoFromNameServer(ctx)
	for _, ele := range cs {
		allAddress = append(allAddress, strings.Trim(ele.Endpoint, " "))
	}
	util.WriteJsonQuiet(w, http.StatusOK, strings.Join(allAddress, ","))
}

func parseObjectOptions(ctx context.Context, r *http.Request) (opt parsedOpts) {
	ctx, span := trace.StartSpan(ctx, "parseObjectArgs")
	defer span.End()

	vars := r.URL.Query()

	objopts := &opt.ObjOptions
	objopts.Bucket = vars.Get("bucket")
	if objopts.Bucket == "" {
		opt.Err = error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}
		opt.HttpCode = http.StatusBadRequest
		return
	} else if !metadata.CheckBucketExist(ctx, objopts.Bucket) {
		opt.Err = error2.BucketNotFound{Bucket: objopts.Bucket}
		opt.HttpCode = http.StatusNotFound
		return
	}

	object := vars.Get("object")
	if object == "" {
		opt.Err = error2.InvalidArgument{Err: fmt.Errorf("object name empty")}
		opt.HttpCode = http.StatusBadRequest
		return
	}

	if !strings.HasPrefix(object, "/") {
		object = fmt.Sprintf("/%s", object)
	}

	objopts.Prefix = path.Dir(object)
	objopts.Object = path.Base(object)
	if objopts.Prefix == "." || objopts.Prefix == "" {
		objopts.Prefix = "/"
	}

	oi, _ := metadata.QueryObjectInfo(ctx, objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID)
	if len(oi.Name) > 0 {
		objopts.CurrentVersion = true
	}

	ohi, _ := metadata.QueryObjectHistoryInfo(ctx, objopts.Bucket,
		objopts.Prefix, objopts.Object, objopts.VersionID)
	if len(ohi.Name) > 0 {
		objopts.HistoryVersion = true
	}

	if !objopts.CurrentVersion && !objopts.HistoryVersion {
		opt.Err = error2.ObjectNotFound{Bucket: objopts.Bucket, Object: object}
		opt.HttpCode = http.StatusNotFound
		return
	}

	opt.ObjOptions.IsDir = oi.Isdir || ohi.Isdir
	if opt.ObjOptions.IsDir {
		objopts.VersionID = metadata.Defaultversionid
	} else {
		objopts.VersionID = vars.Get("versionId")
	}

	return
}

func (h *NameserverAPIHandlers) DeleteObject(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "DeleteObject")
	defer span.End()

	opt := parseObjectOptions(ctx, r)
	if opt.Err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, opt.Err), r.URL)
		return
	}
	var deleted metadata.DeletedObjects
	var err error
	if r.URL.Query().Get("fetch-delete") == "true" {
		// todo 删除所有的对象
		deleted, err = metadata.DeleteObjectFetchDelete(ctx, opt.ObjOptions, true)
	} else {
		deleted, err = metadata.DeleteObjectInfo(ctx, opt.ObjOptions)
	}
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	data, err := json.Marshal(deleted)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("marshal data error: %s", err)}), r.URL)
		return
	}
	w.Write(data)
	w.WriteHeader(http.StatusOK)

}

// check object exist
func (h *NameserverAPIHandlers) ObjectExistHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "ObjectExistHandler")
	defer span.End()

	opt := parseObjectOptions(ctx, r)
	if opt.Err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, opt.Err), r.URL)
		return
	}
	util.WriteJsonQuiet(w, http.StatusOK, "success")
}

// delete object tags
func (h *NameserverAPIHandlers) DeleteObjectTagsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "DeleteObjectTagsHandler")
	defer span.End()

	opt := parseObjectOptions(ctx, r)
	if opt.Err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, opt.Err), r.URL)
		return
	}

	// empty string means delete, this sames as put handler
	err := metadata.PutObjectTags(ctx, opt.ObjOptions, "")
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util.WriteJsonQuiet(w, http.StatusOK, "success")
}

// get object tags
func (h *NameserverAPIHandlers) GetObjectTagsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetObjectTagsHandler")
	defer span.End()

	opt := parseObjectOptions(ctx, r)
	if opt.Err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, opt.Err), r.URL)
		return
	}

	tags, err := metadata.QueryObjectTags(ctx, opt.ObjOptions)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	api.WriteSuccessResponseJSON(w, []byte(tags))
}

// put object tags
func (h *NameserverAPIHandlers) PutObjectTagsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "PutObjectTagsHandler")
	defer span.End()

	opt := parseObjectOptions(ctx, r)
	if opt.Err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, opt.Err), r.URL)
		return
	}
	tags, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.InvalidArgument{Err: err}), r.URL)
		return
	}

	err = metadata.PutObjectTags(ctx, opt.ObjOptions, string(tags))
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util.WriteJsonQuiet(w, http.StatusOK, "success")
}

// delete object acl
func (h *NameserverAPIHandlers) DeleteObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "DeleteObjectAclHandler")
	defer span.End()

	opt := parseObjectOptions(ctx, r)
	if opt.Err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, opt.Err), r.URL)
		return
	}

	// empty string means delete, this sames as put handler
	err := metadata.PutObjectAcl(ctx, opt.ObjOptions, "")
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util.WriteJsonQuiet(w, http.StatusOK, "success")
}

// get object acl
func (h *NameserverAPIHandlers) GetObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetObjectAclHandler")
	defer span.End()

	opt := parseObjectOptions(ctx, r)
	if opt.Err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, opt.Err), r.URL)
	}

	acl, err := metadata.QueryObjectAcl(ctx, opt.ObjOptions)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	api.WriteSuccessResponseJSON(w, []byte(acl))
}

// put object acl
func (h *NameserverAPIHandlers) PutObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "PutObjectAclHandler")
	defer span.End()

	opt := parseObjectOptions(ctx, r)
	if opt.Err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, opt.Err), r.URL)
	}

	acl, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.InvalidArgument{Err: err}), r.URL)
		return
	}

	err = metadata.PutObjectAcl(ctx, opt.ObjOptions, string(acl))
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util.WriteJsonQuiet(w, http.StatusOK, "success")
}

// get object info
func (h *NameserverAPIHandlers) GetObjectInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetObjectInfoHandler")
	defer span.End()

	opt := parseObjectOptions(ctx, r)
	if opt.Err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, opt.Err), r.URL)
	}

	bi, _ := metadata.QueryBucketInfo(ctx, opt.ObjOptions.Bucket)
	versioned := (bi.Versioning == metadata.VersioningEnabled)

	o, _ := metadata.QueryObjectInfo(ctx, opt.ObjOptions.Bucket, opt.ObjOptions.Prefix, opt.ObjOptions.Object, opt.ObjOptions.VersionID)

	// found in object table
	if len(o.Name) > 0 {
		if !versioned {
			o.Version = ""
		}
		util.WriteJsonQuiet(w, http.StatusOK, o)
		return
	}

	oh, err := metadata.QueryObjectHistoryInfo(ctx, opt.ObjOptions.Bucket, opt.ObjOptions.Prefix, opt.ObjOptions.Object, opt.ObjOptions.VersionID)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	// found in history table
	if len(oh.Name) > 0 {
		if !versioned {
			oh.Version = ""
		}
		util.WriteJsonQuiet(w, http.StatusOK, oh)
		return
	}

	api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
}

func (h *NameserverAPIHandlers) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "HeadObjectHandler")
	defer span.End()

	opt := parseObjectOptions(ctx, r)
	if opt.Err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, opt.Err), r.URL)
	}

	// simple implementent, check object only
	if metadata.CheckObjectExist(ctx, opt.ObjOptions) {
		api.WriteSuccessResponseObject(w, "success")
	} else {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketNotFound{Bucket: opt.ObjOptions.Bucket}), r.URL)
	}
}

// bucket=%s&prefix=%s&marker=%s&delimiter=%s&max-keys=%d
func (h *NameserverAPIHandlers) ListObjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "ObjectInfoListHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}
	prefix := vars.Get("prefix")
	if prefix == "" {
		prefix = "/"
	}

	//marker := vars.Get("marker")
	//offset, err := strconv.Atoi(marker)

	offset, _ := strconv.Atoi(vars.Get("marker"))
	//delimiter := vars.Get("delimiter")
	maxKeysValue := vars.Get("max-keys")
	maxKeys, err := strconv.Atoi(maxKeysValue)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.InvalidArgument{Err: err}), r.URL)
		return
	}

	fetchDel := vars.Get("fetch-delete") == "true"
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.InvalidArgument{Err: err}), r.URL)
		return
	}

	ret, _, err := metadata.QueryObjectInfosByPrefix(ctx, bucket, prefix, offset, maxKeys, fetchDel)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	count, err := metadata.QueryObjectCountByPrefix(ctx, bucket, prefix)
	if err != nil {
		api.WriteErrorResponseJSON(w,
			error2.ToAPIError(ctx, error2.ObjectNotFound{Bucket: bucket, Object: prefix}),
			r.URL)
		return
	}

	util.WriteJsonQuiet(w, http.StatusOK, struct {
		Res   []metadata.ObjectInfo
		Total int
	}{ret, count})
}

func (h *NameserverAPIHandlers) ListObjectVersionsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "ObjectVersionsListHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}
	prefix := vars.Get("prefix")

	var object string
	// get all objects, if prefix is '/' and object is empty
	if prefix != "/" {
		object = path.Base(prefix)
		prefix = path.Dir(prefix)
	}

	maxkeys, _ := strconv.Atoi(vars.Get("maxkeys"))
	marker := vars.Get("marker")
	versionmarker := vars.Get("versionmarker")
	fetchDel := vars.Get("fetch-delete") == "true"
	ret, err := metadata.QueryObjectInfoAll(ctx, bucket, prefix, object, marker, versionmarker, maxkeys, fetchDel)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util.WriteJsonQuiet(w, http.StatusOK, ret)
}

func (h *NameserverAPIHandlers) PutObjectMetadata(w http.ResponseWriter, r *http.Request) {

}

func (h *NameserverAPIHandlers) GetObjectCidHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetObjectCidHandler")
	defer span.End()
	opt := parseObjectOptions(ctx, r)
	if opt.Err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, opt.Err), r.URL)
		return
	}
	if opt.ObjOptions.VersionID != "" {
		info, err := metadata.QueryObjectHistoryInfo(ctx, opt.ObjOptions.Bucket, opt.ObjOptions.Prefix, opt.ObjOptions.Object, opt.ObjOptions.VersionID)
		if err != nil {
			api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
				error2.InvalidArgument{Err: fmt.Errorf("get object fail")}),
				r.URL)
			return
		}
		util.WriteJsonQuiet(w, http.StatusOK, info.Cid)
		return
	}
	info, err := metadata.QueryObjectInfo(ctx, opt.ObjOptions.Bucket, opt.ObjOptions.Prefix, opt.ObjOptions.Object, opt.ObjOptions.VersionID)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("get object fail")}),
			r.URL)
		return
	}
	res := map[string]string{
		"cid": info.Cid,
	}
	util.WriteJsonQuiet(w, http.StatusOK, res)
	return
}
