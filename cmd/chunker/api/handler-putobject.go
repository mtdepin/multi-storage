package api

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"path"
	"strings"

	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/node/client"
	node_util "mtcloud.com/mtstorage/node/util"
	"mtcloud.com/mtstorage/pkg/crypto"
	"mtcloud.com/mtstorage/pkg/hash"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/util"
)

// PutObjectHandler 弃用改接口
// 使用PostObjectHandler
func (h *chunkerAPIHandlers) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	h.PostObjectHandler(w, r)
}

// PostObjectHandler -上传文件接口
func (h *chunkerAPIHandlers) PostObjectHandler(w http.ResponseWriter, r *http.Request) {
	logger.Info("===> PostObjectHandler")
	ctx, span := trace.StartSpan(r.Context(), "PostObjectHandler")
	defer span.End()

	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	object := vars.Get("object")
	encMd5Sum := vars.Get("encMd5Sum")
	sc := vars.Get("storageClass")
	acl := vars.Get("acl")
	ct := r.Header.Get("Content-Type")
	ck := r.Header.Get("crypto-key") // 加密秘钥
	decodeString, _ := base64.URLEncoding.DecodeString(ck)
	var objectEncryptionKey hash.ObjectKey
	copy(objectEncryptionKey[:len(decodeString)], decodeString)
	var (
		cid        string // 文件cid
		err        error
		size       int64  //文件大小
		cipherSize int64  // 加密文件大大小
		etagHash   string // 加密文件的md5
		etags      string // 文件原始内容的md5
		isDir      bool   // 是否是目录
		dirName    string // 目录名称
		hReader    io.Reader
	)

	hashReader, err := hash.NewReader(r.Body, r.ContentLength, encMd5Sum, "", r.ContentLength)
	if err != nil {
		logger.Error("json Marshal fail", err)
		util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
		return
	}

	// 文件前面添加2M加密信息
	if ck != "" {
		var head HeadInfo
		head.Version = "1.0.0"
		marshal, err := json.Marshal(head)
		if err != nil {
			logger.Error("json Marshal fail", err)
			util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
			return
		}
		hashReader, err = crypto.GetEncryptReader(hashReader, objectEncryptionKey, encMd5Sum, -1)
		if err != nil {
			logger.Error("Encrypt Reader fail", err)
			util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
			return
		}
		hReader, err = crypto.NewReader(hashReader, marshal)
		if err != nil {
			logger.Error("crypto NewReader  fail", err)
			util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
			return
		}
	} else {
		hReader = hashReader
	}

	logger.Info("object:", object)
	if strings.HasSuffix(object, "/") {
		isDir = true
		dirName = path.Dir(path.Dir(object))
	} else {
		isDir = false
		dirName = path.Dir(object)
	}
	// poc测试取消冷备
	//if sc == "CA" && !isDir { //cold archive
	//	cid, err = SendToPowerGate(h.powerGateHost, h.powerGateToken, hReader)
	//	if err != nil {
	//		logger.Error("write powergate failed", err)
	//		util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
	//		return
	//	}
	//} else if !isDir {
	//	cid, err = nodeimpl.WriteData(ctx, hReader)
	//	if err != nil {
	//		logger.Error("write ipfs failed", err)
	//		util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
	//		return
	//	}
	//}
	cid, err = h.backend.WriteData(ctx, hReader)
	logger.Infof("object: %s, cid: %s", object, cid)
	if err != nil {
		logger.Error("write ipfs failed", err)
		util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
		return
	}
	actualCid := cid
	// 计算文件md5
	etagHash = hashReader.MD5CurrentHexString() // 不加头文件的md5
	size = hashReader.Size()
	if size < 0 {
		size = hashReader.BytesRead()
	}

	if dirName == "" || dirName == "." {
		dirName = "/"
	}
	logger.Info("dirName:", dirName)
	if ck != "" {
		cid, err = crypto.Base64Encrypt(ck, cid)
		if err != nil {
			logger.Error("encrypt failed:", err)
			util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
			return
		}
		cipherSize = size
		orSize, err := crypto.OrSize(size)
		if err != nil {
			logger.Error("encrypt failed:", err)
			util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
			return
		}
		size = int64(orSize)
		etags, err = hash.DecryptETag(objectEncryptionKey, etagHash)
		if err != nil {
			logger.Error("encrypt failed:", err)
			util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	objectName := path.Base(object)
	if etags == "" {
		etags = etagHash
	}
	if err = h.backend.CallBackNS(client.WithTrack(ctx), node_util.ReWriteObjectInfo{
		Bucket:  bucket,
		Name:    objectName,
		Cid:     cid,
		Size:    size,
		DirName: dirName,
		//Etag:         fmt.Sprintf("%x", md5.Sum([]byte(cid))),
		Etag:           etags,
		ContenLength:   uint64(size),
		CipherTextSize: uint64(cipherSize),
		IsDir:          isDir,
		StorageClass:   sc,
		ACL:            acl,
		ContentType:    ct,
		ActualCid:      actualCid,
	}); err != nil {
		logger.Error("Rewrite DB failed:", err)
		util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJsonQuiet(w, http.StatusOK, map[string]interface{}{
		"cid":          cid,
		"size":         size,
		"bucket":       bucket,
		"object":       object,
		"storageclass": sc,
		"etag":         etagHash,
	})
	return
}
