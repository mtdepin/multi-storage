package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/minio/sio"
	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/pkg/crypto"
	"mtcloud.com/mtstorage/pkg/fips"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/util"
)

// GetObjectHandler
func (h *chunkerAPIHandlers) GetObjectHandler(w http.ResponseWriter, r *http.Request) {

	ctx, span := trace.StartSpan(r.Context(), "PostObjectHandler")
	defer span.End()

	vars := r.URL.Query()
	path := strings.Split(r.URL.EscapedPath(), "/")
	cid := strings.Join(path[4:], "/")
	//cid := vars.Get("cid")
	offset := vars.Get("offset")
	length := vars.Get("length")
	logger.Infof("===========================>开始下载", length, offset)
	sc := vars.Get("storageclass")
	ck := vars.Get("crypto-key")
	var rd io.Reader
	var err error
	var isCrypto bool
	logger.Info("====>GetObjectHandler,sc", sc)
	if ck != "" {
		var c string
		c, err = crypto.Base64Decrypt(ck, cid)
		if err == nil {
			isCrypto = true
			cid = c
		} else {
			if cid[:2] != "Qm" {
				util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
				return
			} else {
				cid = strings.Join(path[4:], "/")
			}
		}
	}
	//if sc == "CA" {
	//	rd, err = GetDataFromCold(h.powerGateHost, h.powerGateToken, cid)
	//	if err != nil {
	//		util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
	//		return
	//	}
	//} else {
	//	//if offset == "" {
	//	//	util.WriteJsonQuiet(w, http.StatusBadRequest, "Unknown bucket param")
	//	//	return
	//	//}
	//	//i_offset, _ := strconv.ParseInt(offset, 10, 64)
	//
	//	//length := vars.Get("length")
	//	//if length == "" {
	//	//	util.WriteJsonQuiet(w, http.StatusBadRequest, "Unknown bucket param")
	//	//	return
	//	//}
	//	//i_length, _ := strconv.ParseInt(length, 10, 64)
	//

	if !h.backend.CIDExist(ctx, cid) {
		util.WriteJsonQuiet(w, http.StatusNotFound, "cid not found")
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Hour) // ctx有效期
	defer cancel()
	//rd, err = nodeimpl.GetData(ctx, cid, i_offset, i_length)
	rd, err = h.backend.GetData(ctx, cid)
	if err != nil {
		util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if isCrypto {
		all, err := io.ReadAll(io.LimitReader(rd, crypto.Size))
		if err != nil {
			return
		}
		var buf [crypto.Size]byte
		copy(buf[:], all)
		// 2、检验头部是否是我们预留的加密信息
		b, t := crypto.CheckHeader(buf)
		buffSize := int64(4)
		var info HeadInfo
		isPart := false
		if t { // 如果是预留的加密信息，进行解密
			objectEncryptionKey, err := base64.URLEncoding.DecodeString(ck)
			if err != nil {
				return
			}
			index := bytes.IndexByte(b, 0)         // 忽略0字节byte
			err = json.Unmarshal(b[:index], &info) // 获取 文件里面存储的加密信息
			if err != nil {
				return
			}
			fileSize := info.EnSize //  加密文件+头部秘钥的大小
			if fileSize == 0 {
				fileSize = int64(len(buf)) + 1
			}
			//objInfo.Size = objInfo.Size - crypto.Size // 算出文件原本的加密大小   加密后大小
			if info.Parts != nil || len(info.Parts) != 0 {
				for _, v := range info.Parts {
					if v.Number == 1 {
						isPart = true
						buffSize = v.Size // 获取分块的大小，默认除了最后一片，都是等大小的分片。所以取第一片的大小
						break
					}
				}
			}
			for i := int64(len(buf)); i < fileSize; {
				if buffSize <= 0 || !isPart {
					buffSize = 1024 * 1024 * 1024 * 6 //  最大单文件5GB 加密后大小也不会超过6GB
				}
				deRead, err := sio.DecryptReader(io.LimitReader(rd, buffSize), sio.Config{Key: objectEncryptionKey[:], MinVersion: sio.Version20, CipherSuites: fips.CipherSuitesDARE()})
				if err != nil {
					logger.Error("下载文件失败 DecryptReader---》", err)
					return
				}
				n, e := io.Copy(w, deRead)
				if e != nil {
					logger.Error("下载文件失败 copy ---》", e, n)
					return
				}
				i += buffSize
				if !isPart { // 不是分片上传直接break
					break
				}
			}
		}
	} else {
		_, err = io.Copy(w, rd)
		if err != nil {
			h.backend.FixCid(cid)
			logger.Error(err)
			util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
			return
		}
		// util.WriteJsonQuiet(w, , "success")
	}

	return
}
