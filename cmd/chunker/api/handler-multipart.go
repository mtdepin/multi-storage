package api

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	sysioutil "io/ioutil"
	"net/http"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"mtcloud.com/mtstorage/pkg/crypto"
	utilruntime "mtcloud.com/mtstorage/pkg/runtime"

	"github.com/minio/pkg/trie"
	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/api"
	"mtcloud.com/mtstorage/node/client"
	node_util "mtcloud.com/mtstorage/node/util"
	"mtcloud.com/mtstorage/pkg/hash"
	"mtcloud.com/mtstorage/pkg/ioutil"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/pkg/storage"
	error2 "mtcloud.com/mtstorage/pkg/storageerror"
	"mtcloud.com/mtstorage/util"
)

// CompletePart - represents the part that was completed, this is sent by the client
// during CompleteMultipartUpload request.
type CompletePart struct {
	// Part number identifying the part. This is a positive integer between 1 and
	// 10,000
	PartNumber int

	// Entity tag returned when the part was uploaded.
	ETag string
}

// HeadInfo 文件头存储 分片信息
type HeadInfo struct {
	Version string      `json:"version"`
	EnSize  int64       `json:"EnSize"`
	Parts   []PartsInfo `json:"parts"`
}

// PartsInfo 分片信息
type PartsInfo struct {
	Number     int   `json:"number"`
	Size       int64 `json:"size"`
	ActualSize int   `json:"actualSize"`
}

var etagRegex = regexp.MustCompile("\"*?([^\"]*?)\"*?$")

// Returns EXPORT/.minio.sys/multipart/SHA256/UPLOADID
func (h *chunkerAPIHandlers) getUploadIDDir(bucket, object, uploadID string) string {
	if len(uploadID) < 10 {
		uploadID = util.GetRandString(10)
	}
	//return storage.PathJoin(h.backend.TempDir, hash.GetSHA256Hash([]byte(storage.PathJoin(bucket, object, "-", util.GetRandString(2)))), uploadID)
	return storage.PathJoin(h.backend.TempDir, uploadID[:5])

}

func (h *chunkerAPIHandlers) NewMultipart(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "NewMultipart")
	defer span.End()

	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	storageClass := vars.Get("storageClass")
	_ = vars.Get("content-type")
	object := vars.Get("object")
	uploadID := vars.Get("uploadID")

	uploadPath := h.getUploadIDDir(bucket, object, uploadID)

	logger.Infof("NewMultipart bucket: %s object: %s, uploadPath: %s uploadId: %s ", bucket, object, uploadPath, uploadID)

	if bucket == "" || storageClass == "" || object == "" {
		logger.Errorf("invalid arguments bucket: %s object: %s, storageClass: %s", bucket, object, storageClass)
		api.WriteErrorResponseJSON(w, error2.ErrorCodes.ToAPIErr(error2.ErrInvalidArguments), r.URL)
		return
	}

	//uploadPath := fmt.Sprintf("%s/%s/%s", h.multiDir, bucket, uploadID, "parts")
	_, err := os.Stat(uploadPath)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(uploadPath, os.ModePerm); err != nil {
			logger.Error("create multipart dir failed: ", err)
			api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.FileSystemError{}), r.URL)
			return
		}
	}

	api.WriteSuccessResponseJSON(w, []byte("success"))

}

func (h *chunkerAPIHandlers) PutObjectPart(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "PutObjectPart")
	defer span.End()

	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	object := vars.Get("object")
	uploadID := vars.Get("uploadID")
	partID := vars.Get("partID")
	//size := vars.Get("size")
	encMd5Sum := vars.Get("encMd5Sum")
	rawMD5Sum := vars.Get("rawMD5sum")
	ck := r.Header.Get("crypto-key") // 加密秘钥
	decodeString, _ := base64.URLEncoding.DecodeString(ck)
	var objectEncryptionKey hash.ObjectKey
	copy(objectEncryptionKey[:len(decodeString)], decodeString)
	//clientETag, err := etag.FromContentMD5(r.Header)
	//if err != nil {

	//	api.WriteErrorResponseJSON(w, error2.ErrorCodes.ToAPIErr(error2.ErrInvalidDigest), r.URL)
	//	return
	//}
	//md5hex := clientETag.String()
	reader, err := hash.NewReader(r.Body, r.ContentLength, encMd5Sum, "", r.ContentLength)
	if ck != "" {
		reader, err = crypto.GetEncryptReader(reader, objectEncryptionKey, encMd5Sum, -1)
		if err != nil {
			return
		}
	}
	uploadIDDir := h.getUploadIDDir(bucket, object, uploadID)
	multipartsPath := storage.PathJoin(uploadIDDir, "parts")

	tmpPartFile := storage.PathJoin(multipartsPath, uploadID+"."+partID)
	bytesWritten, err := storage.FsCreateFile(ctx, tmpPartFile, reader, r.ContentLength)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	defer storage.FsRemoveFile(ctx, tmpPartFile)

	if bytesWritten < r.ContentLength {
		logger.Error("receive in complete body! bucket: %s, object: %s", bucket, object)
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.IncompleteBody{}), r.URL)
		return
	}

	//check etag
	etagS3 := reader.MD5CurrentHexString()
	if etagS3 == "" {
		etagS3 = hash.GenETag()
	}

	pid, err := strconv.Atoi(partID)
	if err != nil {
		logger.Error(err)
		api.WriteErrorResponseJSON(w, error2.ErrorCodes.ToAPIErr(error2.ErrInvalidArguments), r.URL)
		return
	}
	partFile := storage.PathJoin(multipartsPath, h.encodePartFile(pid, rawMD5Sum, reader.BytesRead()))
	if err := os.Rename(tmpPartFile, partFile); err != nil {
		logger.Error(err)
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.FileSystemError{}), r.URL)
		return
	}

	_, err = storage.FsStatFile(ctx, partFile)
	if err != nil {
		logger.Error(err)
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.FileSystemError{}), r.URL)
		return
	}

	//merge on background
	go h.backgroundAppend(ctx, bucket, object, uploadID)

	util.WriteJsonQuiet(w, http.StatusOK, etagS3)

}

func (h *chunkerAPIHandlers) CompleteMultipart(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "CompleteMultipart")
	defer span.End()
	//sc := r.FormValue("storageclass")

	var param = struct {
		Bucket       string
		Object       string
		UploadID     string
		StorageClass string
		CryptoKey    string
		ACL          string
		Parts        []CompletePart
	}{}

	jsonBytes, err := sysioutil.ReadAll(r.Body)
	err = json.Unmarshal(jsonBytes, &param)
	if err != nil {
		logger.Error(err)
		api.WriteErrorResponseJSON(w, error2.ErrorCodes.ToAPIErr(error2.ErrInvalidArguments), r.URL)
		return
	}
	//vars := r.URL.Query()
	bucket := param.Bucket
	object := param.Object
	uploadID := param.UploadID
	//partID := vars.Get("partID")
	//size := vars.Get("size")
	cryptoKey := param.CryptoKey
	logger.Infof("=====>CompleteMultipart: %s , %s ,%s ", bucket, object, uploadID)
	sc := param.StorageClass
	acl := param.ACL
	if bucket == "" || sc == "" || object == "" || acl == "" {
		logger.Errorf("invalid arguments bucket: %s object: %s, storageClass: %s, acl: %s.", bucket, object, sc, acl)
		api.WriteErrorResponseJSON(w, error2.ErrorCodes.ToAPIErr(error2.ErrInvalidArguments), r.URL)
		return
	}

	logger.Debugf("CompleteMultipart bucket: %s, object: %s, uploadID: %s", bucket, object, uploadID)

	uploadIDDir := h.getUploadIDDir(bucket, object, uploadID)
	multipartDir := storage.PathJoin(uploadIDDir, "parts")
	logger.Debug("multipartDir: ", multipartDir)
	if _, err := os.Stat(multipartDir); err != nil {
		if os.IsNotExist(err) {
			logger.Error("multipartDir not exist: ", multipartDir)
			api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.FileSystemError{}), r.URL)
			return
		}
	}

	defer func() {
		if err := storage.FsRemoveAll(ctx, uploadIDDir); err != nil {
			logger.Error(err)
		}
	}()
	//read multipart dir
	entries, err := storage.ReadDir(multipartDir)
	if err != nil {
		logger.Error(err, multipartDir)
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.FileSystemError{}), r.URL)
		return
	}

	// Create entries trie structure for prefix match
	entriesTrie := trie.NewTrie()
	for _, entry := range entries {
		entriesTrie.Insert(entry)
	}

	var head HeadInfo
	head.Version = "1.0.0"
	partsInfo := make([]PartsInfo, len(param.Parts), len(param.Parts))
	// Save consolidated actual size.
	var objectActualSize int64
	var cipherSize uint64
	// Validate all parts and then commit to disk.
	for i, part := range param.Parts {
		param.Parts[i].ETag = canonicalizeETag(part.ETag)
		partFile := getPartFile(entriesTrie, part.PartNumber, part.ETag)
		if partFile == "" {
			logger.Error("invalid part! PartNumber: %d", part.PartNumber)
			api.WriteErrorResponseJSON(w,
				error2.ToAPIError(ctx,
					error2.InvalidPart{
						PartNumber: part.PartNumber,
						GotETag:    part.ETag,
					}),
				r.URL)
			return
		}

		// Read the actualSize from the pathFileName.
		subParts := strings.Split(partFile, ".")
		actualSize, err := strconv.ParseInt(subParts[len(subParts)-1], 10, 64)
		if err != nil {
			logger.Errorf("error: %v PartNumber: %d", err, part.PartNumber)
			api.WriteErrorResponseJSON(w,
				error2.ToAPIError(ctx,
					error2.InvalidPart{
						PartNumber: part.PartNumber,
						GotETag:    part.ETag,
					}),
				r.URL)
			return
		}
		// 记录文件分片信息
		partsInfo[i].Size = actualSize        // 加密后的大小
		partsInfo[i].Number = part.PartNumber // 分片编号

		partPath := storage.PathJoin(multipartDir, partFile)

		_, err = storage.FsStatFile(ctx, partPath)
		if err != nil {
			if err == storage.ErrFileNotFound || err == storage.ErrFileAccessDenied {
				api.WriteErrorResponseJSON(w,
					error2.ToAPIError(ctx,
						error2.InvalidPart{
							PartNumber: part.PartNumber,
							GotETag:    part.ETag,
						}),
					r.URL)
			}
			api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
			return
		}

		// Consolidate the actual size.
		objectActualSize += actualSize

		if i == len(param.Parts)-1 {
			break
		}

		// All parts except the last part has to be atleast 5MB.
		//if !isMinAllowedPartSize(actualSize) {
		//	return oi, PartTooSmall{
		//		PartNumber: part.PartNumber,
		//		PartSize:   actualSize,
		//		PartETag:   part.ETag,
		//	}
		//}
	}

	_, objName := storage.ParseObject(object)
	appendFallback := true // In case background-append did not append the required parts.
	appendFilePath := storage.PathJoin(uploadIDDir, objName)

	h.backgroundAppend(ctx, bucket, object, uploadID)

	h.appendFileMapMu.Lock()
	file := h.appendFileMap[uploadID]
	delete(h.appendFileMap, uploadID)
	h.appendFileMapMu.Unlock()

	parts := param.Parts
	if file != nil {
		file.Lock()
		defer file.Unlock()
		// Verify that appendFile has all the parts.
		if len(file.parts) == len(parts) {
			for i := range parts {
				if parts[i].ETag != file.parts[i].ETag {
					break
				}
				if parts[i].PartNumber != file.parts[i].PartNumber {
					break
				}
				if i == len(parts)-1 {
					appendFilePath = file.filePath
					appendFallback = false
				}
			}
		}
	}

	if appendFallback {
		if file != nil {
			storage.FsRemoveFile(ctx, file.filePath)
		}
		for _, part := range parts {
			partFile := getPartFile(entriesTrie, part.PartNumber, part.ETag)
			if partFile == "" {
				logger.Error(fmt.Errorf("%.5d.%s missing will not proceed", part.PartNumber, part.ETag))
				api.WriteErrorResponseJSON(w,
					error2.ToAPIError(ctx,
						error2.InvalidPart{
							PartNumber: part.PartNumber,
							GotETag:    part.ETag,
						}),
					r.URL)
				return
			}
			// 合并分片
			if err = ioutil.AppendFile(appendFilePath, storage.PathJoin(uploadIDDir, partFile), true); err != nil {
				logger.Error(err)
				api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
				return
			}
		}
	}

	//check file again
	logger.Debugf("FsStatFile bucket: %s, object: %s ", bucket, object)
	_, err = storage.FsStatFile(ctx, appendFilePath)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return

	}
	dirName, objectName := storage.ParseObject(object)
	targetPath := path.Join(uploadIDDir, path.Base(object))

	//marshalHead := crypto.AddHeader(marshal)

	//headPath := path.Join(uploadIDDir, path.Base("head"))
	///err = ioutil.AppendHead(headPath, marshalHead[:])
	//fmt.Println(string(marshalHead[:]))
	//if err != nil {
	//	api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
	//	return
	//}
	// 合并
	//if err = ioutil.AppendFile(headPath, targetPath, true); err != nil {
	//	logger.Error(err)
	//	api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
	//	return
	//}
	targetFile, err := os.Open(targetPath)
	if err != nil {
		logger.Errorf("open targetFile err %s", err)
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
	}
	defer os.Remove(targetPath)
	var reader io.Reader

	cipherSize = uint64(objectActualSize)
	// 增加2M加密头
	if cryptoKey != "" {
		objectActualSize += crypto.Size
		// 增加文件头
		head.Parts = partsInfo
		head.EnSize = objectActualSize
		marshal, err := json.Marshal(head)
		if err != nil {
			logger.Error("序列化失败---")
			api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
			return
		}
		reader, err = crypto.NewReader(targetFile, marshal)
		if err != nil {
			logger.Errorf("open targetFile err %s", err)
			api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
			return
		}
	} else {

		reader = targetFile
	}

	logger.Info(" start ipfs")
	var dataCid string
	//switch sc {
	//case "CA":
	//	dataCid, err = SendToPowerGate(h.powerGateHost, h.powerGateToken, reader)
	//	logger.Info(" start  ca")
	//case "STANDARD":
	//	dataCid, err = nodeimpl.WriteData(ctx, reader)
	//	logger.Info(" start  STANDARD")
	//default:
	//	err = errors.New("不支持的存储类型")
	//}
	logger.Debugf("start WriteData bucket: %s, object: %s ", bucket, object)
	dataCid, err = h.backend.WriteData(ctx, reader)
	// 上传ipfs结束
	if err != nil {
		logger.Error("write ipfs failed", err)
		util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err != nil || dataCid == "" {
		logger.Errorf("open targetFile err %s", err)
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	var actualSize uint64

	if cryptoKey != "" {
		dataCid, err = crypto.Base64Encrypt(cryptoKey, dataCid)
		if err != nil {
			logger.Error("encrypt failed:", err)
			util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
			return
		}
		objectActualSize -= crypto.Size
		actualSize, err = crypto.OrSize(objectActualSize)
		if err != nil {
			logger.Error("encrypt failed:", err)
			util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
			return
		}
	} else {
		actualSize = uint64(objectActualSize)
	}

	// 确定文件的类型
	err = targetFile.Close()
	if err != nil {
		logger.Errorf("File stream close failed", err)
	}
	contentType, err := GetFileContentType(targetPath)
	if err != nil {
		logger.Errorf("GetFileContentType failed", err)
	}

	logger.Info("start  CallBackNS bucket: %s, object: %s ", bucket, object)
	if err = h.backend.CallBackNS(client.WithTrack(ctx), node_util.ReWriteObjectInfo{
		Bucket:         bucket,
		Name:           objectName,
		Cid:            dataCid,
		Size:           objectActualSize,
		DirName:        dirName,
		Etag:           getCompleteMultipartMD5(param.Parts),
		ContenLength:   actualSize,
		CipherTextSize: uint64(cipherSize),
		IsDir:          false,
		StorageClass:   sc,
		ACL:            acl,
		ContentType:    contentType,
	}); err != nil {
		fmt.Println("rewrite db failed on multipart:", err)
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	logger.Info(" end CallBackNS  bucket: %s, object: %s ", bucket, object)
	// Purge multipart folders 749db986dd50b5d96c17a94f57ed029a-110
	{
		fsTmpObjPath := uploadIDDir
		defer storage.FsRemoveAll(ctx, fsTmpObjPath) // remove multipart temporary files in background.
	}

	result := map[string]string{
		"desc":     "all success",
		"complete": "true",
	}
	util.WriteJsonQuiet(w, http.StatusOK, result)
}

// Returns the part file name which matches the partNumber and etag.
func getPartFile(entriesTrie *trie.Trie, partNumber int, etag string) (partFile string) {
	for _, match := range entriesTrie.PrefixMatch(fmt.Sprintf("%.5d.%s.", partNumber, etag)) {
		partFile = match
		break
	}
	return partFile
}

var count int64 = 0

// Appends parts to an appendFile sequentially.
func (h *chunkerAPIHandlers) backgroundAppend(ctx context.Context, bucket, object, uploadID string) {
	defer utilruntime.HandleCrash()

	h.appendFileMapMu.Lock()
	file := h.appendFileMap[uploadID]
	if file == nil {
		uploadIDDir := h.getUploadIDDir(bucket, object, uploadID)
		fileName := path.Base(object)
		mergeFilePath := path.Join(uploadIDDir, fileName)
		file = &fsAppendFile{
			//filePath: storage.PathJoin(h.multiDir, minioMetaTmpBucket, h.fsUUID, fmt.Sprintf("%s.%s", uploadID, mustGetUUID())),
			filePath: mergeFilePath,
		}
		h.appendFileMap[uploadID] = file
	}
	h.appendFileMapMu.Unlock()

	file.Lock()
	defer file.Unlock()

	// Since we append sequentially nextPartNumber will always be len(file.parts)+1
	nextPartNumber := len(file.parts) + 1
	uploadIDDir := h.getUploadIDDir(bucket, object, uploadID)
	multiPartpath := storage.PathJoin(uploadIDDir, "parts")

	entries, err := storage.ReadDir(multiPartpath)
	if err != nil {
		logger.Errorf("error: %v multiPartpath: %s uploadIDDir: %s", err, multiPartpath, uploadIDDir)
		return
	}
	sort.Strings(entries)

	for i, entry := range entries {
		partNumber, etag, actualSize, err := h.decodePartFile(entry)
		if err != nil {
			// Skip part files whose name don't match expected format. These could be backend filesystem specific files.
			continue
		}
		if partNumber < nextPartNumber {
			// Part already appended.
			continue
		}
		if partNumber > nextPartNumber {
			// Required part number is not yet uploaded.
			return
		}

		partPath := storage.PathJoin(multiPartpath, entry)
		start := time.Now()
		err = ioutil.AppendFile(file.filePath, partPath, true)
		mi := time.Now().Sub(start).Milliseconds()
		atomic.AddInt64(&count, mi)
		logger.Infof("======>合并上传分片，第 %d 个分片，耗时 %d ms 共耗时 %d ms", i, mi, count)
		if err != nil {
			logger.Error(err)
			return
		}

		file.parts = append(file.parts, PartInfo{PartNumber: partNumber, ETag: etag, ActualSize: actualSize})
		nextPartNumber++
	}
}

// Returns partNumber.etag
func (h *chunkerAPIHandlers) encodePartFile(partNumber int, etag string, actualSize int64) string {
	//return fmt.Sprintf("%.5d.%s.%d", partNumber, etag, actualSize)
	return fmt.Sprintf("%.5d.%s.%d", partNumber, etag, actualSize)
}

// Returns partNumber and etag
func (h *chunkerAPIHandlers) decodePartFile(name string) (partNumber int, etag string, actualSize int64, err error) {
	result := strings.Split(name, ".")
	if len(result) != 3 {
		return 0, "", 0, storage.ErrUnexpected
	}
	partNumber, err = strconv.Atoi(result[0])
	if err != nil {
		return 0, "", 0, storage.ErrUnexpected
	}
	actualSize, err = strconv.ParseInt(result[2], 10, 64)
	if err != nil {
		return 0, "", 0, storage.ErrUnexpected
	}
	return partNumber, result[1], actualSize, nil
}

// AbortMultipartUpload 取消分片上传，删除存放分片的目录
func (h *chunkerAPIHandlers) AbortMultipartUpload(w http.ResponseWriter, r *http.Request) {

	ctx, span := trace.StartSpan(r.Context(), "AbortMultipartUpload")
	defer span.End()
	var param = struct {
		Bucket   string
		Object   string
		UploadID string
	}{}
	jsonBytes, err := sysioutil.ReadAll(r.Body)
	err = json.Unmarshal(jsonBytes, &param)
	if err != nil {
		logger.Error(err)
		api.WriteErrorResponseJSON(w, error2.ErrorCodes.ToAPIErr(error2.ErrInvalidArguments), r.URL)
		return
	}
	logger.Infof("=====>AbortMultipartUpload: json:", string(jsonBytes))

	bucket := param.Bucket
	object := param.Object
	uploadID := param.UploadID
	if bucket == "" || object == "" {
		logger.Errorf("invalid arguments bucket: %s object: %s", bucket, object)
		api.WriteErrorResponseJSON(w, error2.ErrorCodes.ToAPIErr(error2.ErrInvalidArguments), r.URL)
		return
	}
	logger.Debugf("AbortMultipartUpload bucket: %s, object: %s, uploadID: %s", bucket, object, uploadID)
	uploadIDDir := h.getUploadIDDir(bucket, object, uploadID)
	fsTmpObjPath := uploadIDDir
	defer storage.FsRemoveAll(ctx, fsTmpObjPath) // remove multipart temporary files in background.
	util.WriteJsonQuiet(w, http.StatusOK, "")
	return
}
func (h *chunkerAPIHandlers) ListObjectParts(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "ListObjectParts")
	defer span.End()

	vars := r.URL.Query()
	bucket := vars.Get("bucket")
	object := vars.Get("object")
	uploadID := vars.Get("uploadID")

	uploadIDDir := h.getUploadIDDir(bucket, object, uploadID)
	multipartPath := storage.PathJoin(uploadIDDir, "parts") // parts目录

	entries, err := storage.ReadDir(multipartPath) // 遍历所有文件
	if err != nil {
		logger.Error(err, multipartPath)
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, error2.FileSystemError{}), r.URL)
		return
	}
	type PartInfo struct {
		PartNumber   int
		LastModified time.Time
		ETag         string
		Size         int64
		ActualSize   int64
	}
	var result []PartInfo
	//
	for _, partName := range entries {
		subParts := strings.Split(partName, ".")
		if len(subParts) < 3 {
			// 如果文件名不是  00001.14b17234e237505421b6492b8d757507.15728640
			// 跳过该文件
			continue
		}
		partSize, err := strconv.ParseInt(subParts[2], 10, 64) // 切片大小
		if err != nil {
			continue
		}
		partNumber, err := strconv.Atoi(subParts[0]) // 切片编号
		if err != nil {
			continue
		}
		result = append(result, PartInfo{
			PartNumber:   partNumber,
			LastModified: util.GetFileModTime(storage.PathJoin(multipartPath, partName)), // 获取文件的修改时间
			ETag:         subParts[1],                                                    //文件md5
			Size:         partSize,
			ActualSize:   partSize, //文件真实大小，s3没有该字段，minio有
		})
	}
	util.WriteJsonQuiet(w, http.StatusOK, result)
	return
}

func (h *chunkerAPIHandlers) GetObjectDagTree1(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetObjectDagTree")
	defer span.End()

	vars := r.URL.Query()
	cid := vars.Get("cid")
	result, err := h.backend.GetObjectDagTree(ctx, cid)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	//size := float64(0)
	//links := result.(map[string]interface{})["Links"].([]interface{})
	//for i := range links {
	//	size += links[i].(map[string]interface{})["Tsize"].(float64)
	//}
	//result.(map[string]interface{})["size"] = nodeimpl.GetDagSize(cid)
	util.WriteJsonQuiet(w, http.StatusOK, result)
	return
}

func (h *chunkerAPIHandlers) GetObjectDagTree(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "GetObjectDagTree")
	defer span.End()

	vars := r.URL.Query()
	cid := vars.Get("cid")
	result, err := h.backend.GetDagTree(ctx, cid)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}
	result.(map[string]interface{})["size"] = h.backend.GetDagSize(ctx, cid)
	util.WriteJsonQuiet(w, http.StatusOK, result)
	return
}

func (h *chunkerAPIHandlers) AddObjectCid(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "AddObjectCid")
	defer span.End()

	body, err := sysioutil.ReadAll(r.Body)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	var cids []string
	if err = json.Unmarshal(body, &cids); err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	exeStatus, err := h.backend.AddObjectCid(cids)
	if err != nil {
		api.WriteErrorResponseJSON(w, error2.ToAPIError(ctx, err), r.URL)
		return
	}

	util.WriteJsonQuiet(w, http.StatusOK, exeStatus)
	return
}

func GetFileContentType(path string) (string, error) {

	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	// Only the first 512 bytes are used to sniff the content type.
	buffer := make([]byte, 512)

	_, err = file.Read(buffer)
	if err != nil {
		return "", err
	}

	// Use the net/http package's handy DectectContentType function. Always returns a valid
	// content-type by returning "application/octet-stream" if no others seemed to match.
	contentType := http.DetectContentType(buffer)

	return contentType, nil
}

func canonicalizeETag(etag string) string {
	return etagRegex.ReplaceAllString(etag, "$1")
}

func getCompleteMultipartMD5(parts []CompletePart) string {
	var finalMD5Bytes []byte
	for _, part := range parts {
		md5Bytes, err := hex.DecodeString(canonicalizeETag(part.ETag))
		if err != nil {
			finalMD5Bytes = append(finalMD5Bytes, []byte(part.ETag)...)
		} else {
			finalMD5Bytes = append(finalMD5Bytes, md5Bytes...)
		}
	}
	s3MD5 := fmt.Sprintf("%s-%d", getMD5Hash(finalMD5Bytes), len(parts))
	return s3MD5
}

func getMD5Sum(data []byte) []byte {
	hash := md5.New()
	hash.Write(data)
	return hash.Sum(nil)
}

func getMD5Hash(data []byte) string {
	return hex.EncodeToString(getMD5Sum(data))
}
