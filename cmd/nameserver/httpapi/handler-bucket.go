package httpapi

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	util3 "mtcloud.com/mtstorage/cmd/controller/app/informers/core/util"
	"mtcloud.com/mtstorage/pkg/logger"
	error2 "mtcloud.com/mtstorage/pkg/storageerror"

	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/api"
	"mtcloud.com/mtstorage/cmd/nameserver/metadata"
	"mtcloud.com/mtstorage/node/util"
	util2 "mtcloud.com/mtstorage/util"
)

func (h *NameserverAPIHandlers) GetBackendInfo(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetBackendInfo")
	defer span.End()
	si, err := metadata.GetStorageInfo(ctx)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	util2.WriteJsonQuiet(w, http.StatusOK, si)
}

func (h *NameserverAPIHandlers) GetStorageInfo(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetStorageInfo")
	defer span.End()

	ss := h.backend.GetStorageInfoFromNameServer(ctx)
	disks := make([]util.Disk, 0)
	for _, ele := range ss {
		disks = append(disks, util.Disk{
			Endpoint:       ele.Endpoint,
			UUID:           ele.UUID,
			TotalSpace:     ele.TotalSpace,
			UsedSpace:      ele.UsedSpace,
			AvailableSpace: ele.AvailableSpace,
			State:          ele.State,
		})
	}

	util2.WriteJsonQuiet(w, http.StatusOK, disks)
}

// get bucket information as detailed as possible.
func (h *NameserverAPIHandlers) GetBucketInfoDetail(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetBucketInfoDetail")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}
	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketNotFound{Bucket: bucket}), r.URL)
		return
	}

	// get bucket info
	bi, err := metadata.QueryBucketInfo(ctx, bucket)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util2.WriteJsonQuiet(w, http.StatusOK, map[string]interface{}{
		"Name":         bi.Name,
		"Owner":        bi.Owner,
		"Size":         bi.Size,
		"CreateTime":   bi.CreatedAt,
		"StorageClass": bi.StorageClass,
		"Location":     bi.Location,
		"encryption":   bi.Encryption,
	})
}

type AddParams struct {
	UserId   int64   `param:"userid"`
	NodeIp   string  `param:"nodeip"`
	FilePath string  `param:"filepath"`
	FileSize int64   `param:"filesize"`
	Regions  []int64 `param:"regions"`
	UploadId int64   `param:"uploadid"`
	Expire   int64   `param:"expire"`
	// Cid      string  `param:"cid,optional"`
}

// ListBucketHandler ListBucket godoc
// @Summary Get bucket by name
// @Description Get bucket by name
// @Tags api
// @Accept string
// @Produce  json
// @Success 200
// @Router /ns/v1/bucket?name=xxxx [get]
func (h *NameserverAPIHandlers) ListBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "ListBucketHandler")
	defer span.End()
	vars := r.URL.Query()
	u_id := vars.Get("userid")

	bk := vars.Get("bucket")
	parseUID, err := strconv.Atoi(u_id)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.InvalidArgument{Err: err}), r.URL)
		return
	}

	var bs []metadata.BucketInfo
	if parseUID == 0x7fffffff {
		bs, err = metadata.QueryAllBucketInfos(ctx)
		if err != nil {
			api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
			return
		}
	} else {
		bs, err = metadata.QueryBucketInfoByOwner(ctx, uint32(parseUID))
		if err != nil {
			api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
			return
		}
	}
	retM := make([]map[string]interface{}, 0)
	for _, ele := range bs {
		m := map[string]interface{}{
			"Name":         ele.Name,
			"CreateTime":   ele.CreatedAt,
			"Size":         ele.Size,
			"Count":        ele.Count,
			"Location":     ele.Location,
			"StorageClass": ele.StorageClass,
			"encryption":   ele.Encryption,
		}
		if ele.Name == bk {
			util2.WriteJsonQuiet(w, http.StatusOK, []map[string]interface{}{m})
			return
		}
		if bk == "" {
			retM = append(retM, m)
		}

	}
	util2.WriteJsonQuiet(w, http.StatusOK, retM)
}

// MakeBucketWithLocationHandler MakeBucketWithLocation godoc
// @Summary Create bucket with location
// @Description Create bucket
// @Tags api
// @Accept json
// @Produce  json
// @Success 200
// @Router /ns/v1/bucket [post]
func (h *NameserverAPIHandlers) MakeBucketWithLocationHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "MakeBucketWithLocationHandler")
	defer span.End()
	obj := make(map[string]interface{})
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.InvalidArgument{Err: err}), r.URL)
		return
	}
	json.Unmarshal(body, &obj)
	u_id, ok := obj["user_id"].(float64)
	if !ok {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("user_id error")}),
			r.URL)
	}
	var bi = metadata.BucketInfo{
		Name:         obj["bucket"].(string),
		Owner:        uint32(u_id),
		StorageClass: obj["storageclass"].(string),
		Location:     obj["location"].(string),
	}
	bi.Bucketid = fmt.Sprintf("%x", md5.Sum([]byte(bi.Name)))
	bi.Versioning = metadata.VersioningUnset
	if len(bi.Location) == 0 {
		bi.Location = "mos-huadong-hangzhou"
	}

	if err = metadata.PutBucketInfo(ctx, &bi); err != nil {
		logger.Error(err)
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	if err = metadata.PutBucketAcl(ctx, bi.Name, obj["acl"].(string)); err != nil {
		logger.Error(err)
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	//send bucket event to controller
	if err = h.backend.SendControllerEvent(util3.EVENTTYPE_ADD_BUCKET, bi); err != nil {
		logger.Error(err)
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
	}

	util2.WriteJsonQuiet(w, http.StatusOK, "success")
}

// UpdateBucketHandler godoc
// @Summary Update bucket
// @Description Update bucket
// @Tags api
// @Accept json
// @Produce  json
// @Success 204
// @Router /ns/v1/bucket [put]
func (h *NameserverAPIHandlers) UpdateBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "UpdateBucketHandler")
	defer span.End()
	var ubParams = struct {
		Bucket     string `json:"bucket"`
		Bucketid   string `json:"bucketid"`
		Count      uint64 `json:"count"`
		Size       uint64 `json:"size"`
		Owner      uint32 `json:"owner"`
		Tenant     string `json:"tenant"`
		Policy     string `json:"policy"`
		Versioning string `json:"versioning"`
	}{}

	if err := api.GetValidator().ReadJsonObject(r, &ubParams); err != nil {
		fmt.Println(ubParams)
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: err}),
			r.URL)
		return
	}

	var bi = metadata.BucketInfo{
		Name:       ubParams.Bucket,
		Bucketid:   ubParams.Bucketid,
		Count:      ubParams.Count,
		Size:       ubParams.Size,
		Owner:      ubParams.Owner,
		Tenant:     ubParams.Tenant,
		Policy:     ubParams.Policy,
		Versioning: ubParams.Versioning,
	}

	if err := metadata.UpdateBucketInfo(ctx, &bi); err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	api.WriteSuccessResponseObject(w, "success")
}

// DeleteBucketHandler godoc
// @Summary delete bucket
// @Description delete bucket
// @Tags api
// @Accept
// @Produce  json
// @Success 204
// @Router /ns/v1/bucket/{bucket} [delete]
func (h *NameserverAPIHandlers) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "DeleteBucketHandler")
	defer span.End()
	bucket := strings.Split(r.URL.EscapedPath(), "/")[4]
	//vars := mux.Vars(r)
	if err := metadata.DeleteBucketInfo(ctx, bucket); err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	util2.WriteJsonQuiet(w, http.StatusOK, "success")
}

// HeadBucketHandler godoc
// @Summary head bucket
// @Description head bucket
// @Tags api
// @Accept
// @Produce  json
// @Success 204
// @Router /ns/v1/headbucket?bucket [head]
func (h *NameserverAPIHandlers) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "HeadBucketHandler")
	defer span.End()
	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	// simple implementent, check bucket only
	if metadata.CheckBucketExist(ctx, bucket) {
		api.WriteSuccessResponseObject(w, "success")
	} else {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketNotFound{Bucket: bucket}), r.URL)
	}
}

// put bucket versioning
func (h NameserverAPIHandlers) PutBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "PutBucketVersioningHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	status := vars.Get("status")
	if bucket == "" || status == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.InvalidArgument{}), r.URL)
		return
	}

	if status != metadata.VersioningEnabled && status != metadata.VersioningSuspended {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("versioning status param %s error", status)}), r.URL)
		return
	}

	bi, err := metadata.QueryBucketInfo(ctx, bucket)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	bi.Versioning = status
	err = metadata.UpdateBucketInfo(ctx, &bi)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	api.WriteSuccessResponseObject(w, "success")
}

// get bucket versioning
func (h NameserverAPIHandlers) GetBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetBucketVersioningHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	bi, err := metadata.QueryBucketInfo(ctx, bucket)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	w.Write([]byte(bi.Versioning))
}

// put bucket logging
func (h NameserverAPIHandlers) PutBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "PutBucketLoggingHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	target := vars.Get("target")
	prefix := vars.Get("prefix")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketNotFound{Bucket: bucket}), r.URL)

		return
	}
	if !metadata.CheckBucketExist(ctx, target) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketNotFound{Bucket: target}), r.URL)
		return
	}

	var data []byte
	var err error
	// should delete record if target and prefix are both empty
	if target != "" || prefix != "" {
		logging := struct {
			Target string `json:"target"`
			Prefix string `json:"prefix"`
		}{
			Target: target,
			Prefix: prefix,
		}

		data, err = json.Marshal(logging)
		if err != nil {
			api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
				error2.InvalidArgument{Err: fmt.Errorf("marshal data error: %s", err)}), r.URL)

			return
		}
	}

	err = metadata.PutBucketLogging(ctx, bucket, string(data))
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util2.WriteJsonQuiet(w, http.StatusOK, "success")
}

// get bucket logging
func (h NameserverAPIHandlers) GetBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetBucketLoggingHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketNotFound{Bucket: bucket}), r.URL)
		return
	}

	logging, err := metadata.QueryBucketLogging(ctx, bucket)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	api.WriteSuccessResponseJSON(w, []byte(logging))
}

// delete bucket logging
func (h NameserverAPIHandlers) DeleteBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "DeleteBucketLoggingHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketNotFound{Bucket: bucket}), r.URL)
		return
	}

	// empty string means delete, this sames as put handler
	err := metadata.PutBucketLogging(ctx, bucket, "")
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	util2.WriteJsonQuiet(w, http.StatusOK, "success")
}

// put bucket Policy
func (h NameserverAPIHandlers) PutBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "PutBucketPolicyHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketPolicyNotFound{Bucket: bucket}), r.URL)
		return
	}

	policy, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.InvalidArgument{Err: err}), r.URL)
		return
	}

	err = metadata.PutBucketPolicy(ctx, bucket, string(policy))
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util2.WriteJsonQuiet(w, http.StatusOK, "success")
}

// get bucket Policy
func (h NameserverAPIHandlers) GetBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetBucketPolicyHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.BucketPolicyNotFound{Bucket: bucket, Err: fmt.Errorf("bucket %s not exist", bucket)}),
			r.URL)
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketPolicyNotFound{Bucket: bucket}), r.URL)
		return
	}

	policy, err := metadata.QueryBucketPolicy(ctx, bucket)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	api.WriteSuccessResponseJSON(w, []byte(policy))
}

// delete bucket Policy
func (h NameserverAPIHandlers) DeleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "DeleteBucketPolicyHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketPolicyNotFound{Bucket: bucket}), r.URL)
		return
	}

	// empty string means delete, this sames as put handler
	err := metadata.PutBucketPolicy(ctx, bucket, "")
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util2.WriteJsonQuiet(w, http.StatusOK, "success")
}

// put bucket lifecycle
func (h NameserverAPIHandlers) PutBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "PutBucketLifecycleHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketLifecycleNotFound{Bucket: bucket}), r.URL)
		return
	}

	lifecycle, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.InvalidArgument{Err: err}), r.URL)
		return
	}

	err = metadata.PutBucketLifecycle(ctx, bucket, string(lifecycle))
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util2.WriteJsonQuiet(w, http.StatusOK, "success")
}

// get bucket lifecycle
func (h NameserverAPIHandlers) GetBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetBucketLifecycleHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketLifecycleNotFound{Bucket: bucket}), r.URL)
		return
	}

	lifecycle, err := metadata.QueryBucketLifecycle(ctx, bucket)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	api.WriteSuccessResponseJSON(w, []byte(lifecycle))
}

// delete bucket lifecycle
func (h NameserverAPIHandlers) DeleteBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "DeleteBucketLifecycleHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketLifecycleNotFound{Bucket: bucket}), r.URL)
		return
	}

	// empty string means delete, this sames as put handler
	err := metadata.PutBucketLifecycle(ctx, bucket, "")
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util2.WriteJsonQuiet(w, http.StatusOK, "success")
}

// put bucket acl
func (h NameserverAPIHandlers) PutBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "PutBucketAclHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketACLNotFound{Bucket: bucket}), r.URL)
		return
	}

	acl, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.InvalidArgument{Err: err}), r.URL)
		return
	}

	err = metadata.PutBucketAcl(ctx, bucket, string(acl))
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util2.WriteJsonQuiet(w, http.StatusOK, "success")
}

// get bucket acl
func (h NameserverAPIHandlers) GetBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetBucketAclHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketACLNotFound{Bucket: bucket}), r.URL)
		return
	}

	acl, err := metadata.QueryBucketAcl(ctx, bucket)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	api.WriteSuccessResponseJSON(w, []byte(acl))
}

// delete bucket acl
func (h NameserverAPIHandlers) DeleteBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "DeleteBucketAclHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketACLNotFound{Bucket: bucket}), r.URL)
		return
	}

	// empty string means delete, this sames as put handler
	err := metadata.PutBucketAcl(ctx, bucket, "")
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util2.WriteJsonQuiet(w, http.StatusOK, "success")
}

// put bucket tag
func (h NameserverAPIHandlers) PutBucketTagsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "PutBucketTagsHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketTaggingNotFound{Bucket: bucket}), r.URL)
		return
	}

	tags, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.InvalidArgument{Err: err}), r.URL)
		return
	}
	unescapeTags, err := url.QueryUnescape(string(tags))
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.InvalidArgument{Err: err}), r.URL)
		return
	}
	err = metadata.PutBucketTags(ctx, bucket, unescapeTags)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util2.WriteJsonQuiet(w, http.StatusOK, "success")
}

// get bucket tag
func (h NameserverAPIHandlers) GetBucketTagsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetBucketTagsHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketTaggingNotFound{Bucket: bucket}), r.URL)
		return
	}

	tags, err := metadata.QueryBucketTags(ctx, bucket)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	api.WriteSuccessResponseJSON(w, []byte(tags))
}

// delete bucket tag
func (h NameserverAPIHandlers) DeleteBucketTagsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "DeleteBucketTagsHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}

	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketTaggingNotFound{Bucket: bucket}), r.URL)
		return
	}

	// empty string means delete, this sames as put handler
	err := metadata.PutBucketTags(ctx, bucket, "")
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util2.WriteJsonQuiet(w, http.StatusOK, "success")
}

// GetBucketEncryptionHandler 获取桶的加密信息
func (h NameserverAPIHandlers) GetBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetBucketEncryptionHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	if bucket == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx,
			error2.InvalidArgument{Err: fmt.Errorf("bucket name empty")}),
			r.URL)
		return
	}
	if !metadata.CheckBucketExist(ctx, bucket) {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.BucketTaggingNotFound{Bucket: bucket}), r.URL)
		return
	}
	// 获取桶的加密信息
	bucketInfo, err := metadata.QueryBucketInfo(ctx, bucket)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	//util2.WriteJsonQuiet(w, http.StatusOK, bucketInfo.Encryption)
	api.WriteSuccessResponseJSON(w, []byte(bucketInfo.Encryption))
}
func (h NameserverAPIHandlers) PutBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "PutBucketEncryptionHandler")
	defer span.End()
	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	all, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	status := string(all)
	if bucket == "" || status == "" {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.InvalidArgument{}), r.URL)
		return
	}
	bi, err := metadata.QueryBucketInfo(ctx, bucket)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	bi.Encryption = status
	err = metadata.UpdateBucketInfo(ctx, &bi)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	api.WriteSuccessResponseObject(w, "success")
}
