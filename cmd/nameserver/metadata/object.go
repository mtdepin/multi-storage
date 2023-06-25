package metadata

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/jinzhu/gorm"
	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/pkg/cache"
	"mtcloud.com/mtstorage/pkg/lock"
	"mtcloud.com/mtstorage/pkg/logger"
	error2 "mtcloud.com/mtstorage/pkg/storageerror"
)

func genObjectCacheKey(bucket, prefix, name, version string) string {
	if version == "" {
		version = "null"
	}

	return fmt.Sprintf("ns:%s:%s:%s:%s", bucket, prefix, name, version)
}

func QueryObjectCountByPrefix(ctx context.Context, bucket, objPrefix string) (count int, err error) {
	_, span := trace.StartSpan(ctx, "QueryObjectCountByPrefix")
	defer span.End()
	return queryObjectCountByPrefix(bucket, objPrefix)
}

func doCheckObjectExist(ctx context.Context, bucket, prefix, object string) bool {
	_, span := trace.StartSpan(ctx, "doCheckObjectExist")
	defer span.End()

	cacheKey := genObjectCacheKey(bucket, prefix, object, "")
	res, _ := cache.Read(ctx, cacheKey, &ObjectInfo{}, func() (interface{}, error) {
		return queryObjectInfo(bucket, prefix, object, "")
	}, 0)

	if res != nil {
		return len(res.(*ObjectInfo).Name) != 0
	}
	return false
}

func CheckObjectExist(ctx context.Context, objopts ObjectOptions) bool {
	_, span := trace.StartSpan(ctx, "CheckObjectExist")
	defer span.End()
	return doCheckObjectExist(ctx, objopts.Bucket, objopts.Prefix, objopts.Object)
}

func doCheckObjectHistoryExist(ctx context.Context, bucket, prefix, object, version string) bool {
	_, span := trace.StartSpan(ctx, "doCheckObjectHistoryExist")
	defer span.End()

	cacheKey := genObjectCacheKey(bucket, prefix, object, version)

	var o ObjectHistoryInfo
	cache.Read(ctx, cacheKey, &o, func() (interface{}, error) {
		return queryObjectHistoryInfo(bucket, prefix, object, version)
	}, 0)

	return len(o.Name) != 0
}

func CheckObjectHistoryExist(ctx context.Context, objopts ObjectOptions) bool {
	_, span := trace.StartSpan(ctx, "CheckObjectHistoryExist")
	defer span.End()
	return doCheckObjectHistoryExist(ctx, objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID)
}

func QueryObjectInfo(ctx context.Context, bucket, prefix, object string, versionId string) (ObjectInfo, error) {
	_, span := trace.StartSpan(ctx, "QueryObjectInfo")
	defer span.End()

	logger.Infof("query object:[%s,%s,%s]", bucket, prefix, object)

	var oi ObjectInfo
	cacheKey := genObjectCacheKey(bucket, prefix, object, versionId)
	res, err := cache.Read(ctx, cacheKey, &ObjectInfo{}, func() (interface{}, error) {
		return queryObjectInfo(bucket, prefix, object, versionId)
	}, 0)

	if res != nil {
		oi = *res.(*ObjectInfo)
	}
	if err != nil {
		logger.Warnf("query object storageerror: %s", err)
		return oi, error2.ObjectNotFound{Bucket: bucket, Object: prefix + "/" + object}
	}

	return oi, err
}

func QueryObjectHistoryInfo(ctx context.Context, bucket, prefix, object, version string) (ObjectHistoryInfo, error) {
	_, span := trace.StartSpan(ctx, "QueryObjectHistoryInfo")
	defer span.End()

	logger.Infof("query history object:[%s,%s,%s,%s]", bucket, prefix, object, version)
	var ohi ObjectHistoryInfo
	cacheKey := genObjectCacheKey(bucket, prefix, object, version)
	res, err := cache.Read(ctx, cacheKey, &ObjectHistoryInfo{}, func() (interface{}, error) {
		return queryObjectHistoryInfo(bucket, prefix, object, version)
	}, 0)

	if res != nil {
		ohi = *res.(*ObjectHistoryInfo)
	}
	if err != nil {
		logger.Warnf("query history object storageerror: %s", err)
		return ohi, error2.ObjectNotFound{Bucket: bucket, Object: prefix + "/" + object}
	}

	return ohi, err
}

func QueryObjectInfosByPrefix(ctx context.Context, bucket, objectPrefix string, offset, maxkeys int, fetchDel bool) (ois []ObjectInfo, total int, err error) {
	_, span := trace.StartSpan(ctx, "QueryObjectInfosByPrefix")
	defer span.End()

	ois, err = queryObjectInfoByPrefix(bucket, objectPrefix, offset, maxkeys, fetchDel)
	if err != nil {
		logger.Errorf("query object info by prefix storageerror: %s", err)
		return []ObjectInfo{}, total, error2.ObjectNotFound{Bucket: bucket, Object: objectPrefix}
	}
	return ois, total, nil
}

func PutObjectInfo(ctx context.Context, obj *ObjectInfo) error {
	_, span := trace.StartSpan(ctx, "PutObjectInfo")
	defer span.End()

	logger.Infof("put object:[%s, %s, %s]", obj.Bucket, obj.Dirname, obj.Name)

	tx := mtMetadata.db.DB.Begin()
	bi := new(BucketInfo)
	if err := tx.Raw("SELECT * FROM "+BucketTable+" WHERE  name=? LIMIT 1", obj.Bucket).Scan(bi).Error; err != nil {
		tx.Rollback()
		return err
	}

	update := true

	oi, err := queryObjectInfoStmp(ctx, tx, obj.Bucket, obj.Dirname, obj.Name, "")
	if err != nil && err != gorm.ErrRecordNotFound {
		tx.Rollback()
		return err
	}
	if len(oi.Name) == 0 {
		logger.Warnf("find object [%s,%s,%s] failed: %s", obj.Bucket, obj.Dirname, obj.Name, err)
		update = false
	}

	// if obj is dir or bucket not enable version, version id is 'null'
	obj.Version = genVersionId(obj.Isdir || (bi.Versioning != VersioningEnabled))
	if obj.Isdir {
		obj.Cid = DefaultCid
		obj.Etag = DefaultEtag
	}
	ohi := ObjectHistoryInfo{
		Bucket:         obj.Bucket,
		Dirname:        obj.Dirname,
		Name:           obj.Name,
		Cid:            obj.Cid,
		Content_length: obj.Content_length,
		Content_type:   obj.Content_type,
		Etag:           obj.Etag,
		Isdir:          obj.Isdir,
		Version:        obj.Version,
		StorageClass:   obj.StorageClass,
		IsMarker:       false,
		Acl:            obj.Acl,
		CipherTextSize: obj.CipherTextSize,
	}

	freshCache := func() {
		// remove cache records
		freshBucketCache(ctx, bi.Name, bi.Owner)

		for _, vid := range []string{obj.Version, Defaultversionid} {
			cacheKey := genObjectCacheKey(obj.Bucket, obj.Dirname, obj.Name, vid)
			err = cache.Delete(ctx, cacheKey)
			if err != nil {
				logger.Errorf("delete object in cache: %s", err)
			}
		}
	}

	freshCache()
	// The second run, based on cache-aside mode.
	defer freshCache()
	var incrCount, incrSize int64
	incrSize = int64(obj.Content_length) - int64(oi.Content_length)
	if update {
		//cover an old one
		logger.Infof("update existed object[%s,%s,%s]", obj.Bucket, obj.Dirname, obj.Name)

		// update object table
		err = updateObjectInfo(ctx, tx, obj)
		if err != nil {
			tx.Rollback()
			return error2.WriteDataBaseFailed{Err: err}
		}
		if bi.Versioning == VersioningEnabled {
			// update object history table
			incrSize = int64(obj.Content_length)
			//  开启多版本情况下，如果上一个版本是非多版本且不是mark，这将上一个版本inster到历史表
			// marker 的version永远不可能是null 所以oi版本好如果是null这不可能是marker
			if oi.Version == Defaultversionid && !oi.Isdir && !oi.IsMarker {
				oi.ID = 0
				tx.Table(ObjectHistoryTable).Create(oi)
			}
			if err := putObjectHistoryInfo(ctx, tx, &ohi, &incrCount); err != nil {
				tx.Rollback()
				logger.Warnf("update history table failed:%s", err)
			}
		}
		if (oi.Version != Defaultversionid && bi.Versioning == VersioningSuspended) || (oi.Isdir && oi.IsMarker) {
			incrCount = 1
			incrSize = int64(obj.Content_length)
		}
		// todo 如果上传的不是根目录下的文件，则需要恢复改目录上没删除的文件加
		if oi.Dirname != "/" && oi.IsMarker {
			number, err := RecoverDir(ctx, tx, oi.Dirname, bi.Name)
			if err != nil {
				tx.Rollback()
				return err
			}
			incrCount += int64(number)
		}

		if err := updateBucketDate(tx, bi, incrCount, incrSize); err != nil {
			tx.Rollback()
			return err
		}
	} else {
		incrSize = int64(obj.Content_length)

		logger.Infof("put new object[%s,%s,%s]", obj.Bucket, obj.Dirname, obj.Name)
		err = putObjectInfo(ctx, tx, bi, obj, &ohi, incrCount, incrSize)
		if err != nil {
			tx.Rollback()
			logger.Errorf("put object failed: %s", err)
			return error2.WriteDataBaseFailed{Err: err}
		}
	}
	//  历史表中最多只能存在一条非多版本的对象信息
	if bi.Versioning == VersioningSuspended && !ohi.Isdir {
		if err := tx.Unscoped().Table(ObjectHistoryTable).
			Where("name=? and dirname=? and bucket=? and version= 'null'", oi.Name, oi.Dirname, oi.Bucket).
			Delete(ObjectHistoryInfo{}).Error; err != nil {
			return err
		}
	}
	tx.Commit()
	lock.Unlock(ctx, bi.Name+strings.Split(obj.Dirname, "/")[1], bi.Name)
	return nil
}

func DeleteObjectFetchDelete(ctx context.Context, opt ObjectOptions, fetchDelete bool) (DeletedObjects, error) {
	ctx, span := trace.StartSpan(ctx, "deleteObjectFetchDelete")
	defer span.End()
	bi, err := queryBucketInfoByName(opt.Bucket)
	if err != nil {
		return DeletedObjects{}, err
	}

	freshCache := func() {
		// remove cache records
		freshBucketCache(ctx, opt.Bucket, bi.Owner)

		for _, vid := range []string{opt.VersionID, Defaultversionid} {
			cacheKey := genObjectCacheKey(opt.Bucket, opt.Prefix, opt.Object, vid)
			err = cache.Delete(ctx, cacheKey)
			if err != nil {
				logger.Errorf("delete object in cache: %s", err)
			}
		}
	}

	freshCache()
	tx := mtMetadata.db.DB.Begin()
	objectH := make([]ObjectHistoryInfo, 0)
	object := make([]ObjectInfo, 0)
	total := DeletedObjects{}

	if opt.IsDir {
		// delete from t_ns_object where bucket = ? and name = ? and dirname = ? and ismarker = 1 and isdir = 0
		// delete from t_ns_object_history where bucket = ? and name = ? and dirname = ? and ismarker = 1 and isdir = 0
		// todo 删除文件夹下的对象
		dirName := path.Join(opt.Prefix, opt.Object)

		if err := tx.Table(ObjectHistoryTable).
			Where("bucket = ? and (dirname LIKE ? or (dirname = ? and name = ?) or (dirname = ?))", opt.Bucket, dirName+"/%", opt.Prefix, opt.Object, dirName).
			Order("id desc").
			Find(&objectH).Error; err != nil {
			tx.Rollback()
			return total, err
		}
		objectHMap := make(map[string][]ObjectHistoryInfo, 0)
		for i := range objectH {
			if _, ok := objectHMap[path.Join(objectH[i].Dirname+"/"+objectH[i].Name)]; !ok {
				objectHMap[path.Join(objectH[i].Dirname+"/"+objectH[i].Name)] = []ObjectHistoryInfo{objectH[i]}
			} else {

				objectHMap[path.Join(objectH[i].Dirname+"/"+objectH[i].Name)] = append(objectHMap[path.Join(objectH[i].Dirname+"/"+objectH[i].Name)], objectH[i])
			}
		}
		if err := tx.Table(ObjectTable).
			Where("bucket = ? and ismarker = 1 and (dirname LIKE ? or (dirname = ? and name = ?) or (dirname = ?))", opt.Bucket, dirName+"/%", opt.Prefix, opt.Object, dirName).
			Find(&object).Error; err != nil {
			tx.Rollback()
			return total, err
		}
		deleteObjectId := make([]int64, 0)
		deleteObjectHId := make([]int64, 0)
		for i := range object {
			if value, ok := objectHMap[path.Join(object[i].Dirname+"/"+object[i].Name)]; ok {
				switch {
				case object[i].IsMarker:
					deleteObjectId = append(deleteObjectId, int64(object[i].ID))
					for i2 := range value {
						if !value[i2].IsMarker && !value[i2].Isdir {
							total.Count++
						}
						total.Size += value[i2].Content_length
						deleteObjectHId = append(deleteObjectHId, int64(value[i2].ID))
					}
				default:
					delete(objectHMap, path.Join(object[i].Dirname+object[i].Name))
				}
			}
		}
		if err := tx.Unscoped().Table(ObjectTable).
			Where("id in (?)", deleteObjectId).
			Delete(&ObjectInfo{}).Error; err != nil {
			tx.Rollback()
			return total, err
		}

		if err := tx.Unscoped().Table(ObjectHistoryTable).
			Where("id in (?)", deleteObjectHId).
			Delete(&ObjectHistoryInfo{}).Error; err != nil {
			tx.Rollback()
			return total, err
		}
	} else {
		// todo 删除对象
		deleteDB := tx.Where("bucket = ? and name = ? and dirname = ? ", opt.Bucket, opt.Object, opt.Prefix)
		if err := deleteDB.Table(ObjectHistoryTable).
			Order("id desc").
			Find(&objectH).Error; err != nil {
			tx.Rollback()
			return total, err
		}
		if len(objectH) == 0 || !objectH[0].IsMarker {
			tx.Rollback()
			return total, errors.New("删除对象个数为0")
		}
		for i := range objectH {
			if !objectH[i].IsMarker {
				total.Count++
				total.Size += objectH[i].Content_length
			}
		}

		if err := deleteDB.Unscoped().Table(ObjectTable).
			Where("ismarker = 1").
			Delete(ObjectInfo{}).Error; err != nil {
			tx.Rollback()
			return total, err
		}
		if err := deleteDB.Unscoped().Table(ObjectHistoryTable).
			Delete(ObjectHistoryInfo{}).Error; err != nil {
			tx.Rollback()
			return total, err
		}
	}

	// todo 更新桶的对象和容量大小
	if err := UpdateBucketCount(ctx, tx, opt.Bucket, int64(total.Count), int64(total.Size)); err != nil {
		tx.Rollback()
		return total, err
	}
	tx.Commit()
	return total, nil
}

func DeleteObjectInfo(ctx context.Context, opt ObjectOptions) (DeletedObjects, error) {
	ctx, span := trace.StartSpan(ctx, "DeleteObjectInfo")
	defer span.End()

	isdir := opt.IsDir
	bucket, dir, name, version := opt.Bucket, opt.Prefix, opt.Object, opt.VersionID
	logger.Infof("delete object: [%s,%s,%s,%s]", bucket, dir, name, version)
	bi, err := queryBucketInfoByName(bucket)
	if err != nil {
		return DeletedObjects{}, err
	}

	freshCache := func() {
		// remove cache records
		freshBucketCache(ctx, bucket, bi.Owner)

		for _, vid := range []string{version, Defaultversionid} {
			cacheKey := genObjectCacheKey(bucket, dir, name, vid)
			err = cache.Delete(ctx, cacheKey)
			if err != nil {
				logger.Errorf("delete object in cache: %s", err)
			}
		}
	}

	freshCache()
	// The second run, based on cache-aside mode.
	defer freshCache()
	// todo 删除文件
	info, _ := queryObjectInfo(bi.Name, dir, name, version)
	total := DeletedObjects{}
	if isdir && !info.IsMarker {
		total, err = deleteDirPipeline(ctx, bi, dir, name)
	} else {
		total, err = deleteObjectPipeline(ctx, bi, info, version)
	}
	return total, err

}

// return all versions
func QueryObjectInfoAll(ctx context.Context, bucket, prefix, object, marker, versionmarker string, maxkeys int, fetchDel bool) ([]ObjectHistoryInfo, error) {
	_, span := trace.StartSpan(ctx, "QueryObjectInfoAll")
	defer span.End()
	logger.Info("query all objects by name in history table")
	bi, err := queryBucketInfoByName(bucket)
	if err != nil {
		return []ObjectHistoryInfo{}, error2.ObjectNotFound{Bucket: bucket, Object: prefix + "/" + object}
	}
	if bi.Versioning != VersioningEnabled && bi.Versioning != VersioningSuspended {
		logger.Errorf("bucket [%s] not enable versioning", bucket)
		return []ObjectHistoryInfo{}, nil
	}
	if fetchDel {
		return queryObjectMarkers(bucket, path.Join(prefix, object), marker, maxkeys)
	}

	ohis, err := queryObjectInfoAllVersionsByName(bucket, prefix, object, marker, versionmarker, maxkeys)
	if err != nil {
		logger.Errorf("query history table failed: %s", err)
		return []ObjectHistoryInfo{}, error2.ObjectNotFound{Bucket: bucket, Object: prefix + "/" + object}
	}

	return *ohis, nil
}

func QueryObjectTags(ctx context.Context, objopts ObjectOptions) (string, error) {
	_, span := trace.StartSpan(ctx, "QueryObjectTags")
	defer span.End()

	var tags string
	if objopts.CurrentVersion {
		cacheKey := genObjectCacheKey(objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID)
		var oi ObjectInfo
		res, err := cache.Read(ctx, cacheKey, &ObjectInfo{}, func() (interface{}, error) {
			return queryObjectInfo(objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID)
		}, 0)

		if res != nil {
			oi = *res.(*ObjectInfo)
		}
		if err == nil || strings.Contains(err.Error(), "record not found") {
			tags = oi.Tags
		}
	} else {
		var ohi ObjectHistoryInfo
		cacheKey := genObjectCacheKey(objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID)
		res, err := cache.Read(ctx, cacheKey, &ObjectInfo{}, func() (interface{}, error) {
			return queryObjectHistoryInfo(objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID)
		}, 0)

		if res != nil {
			ohi = *res.(*ObjectHistoryInfo)
		}
		if err == nil || strings.Contains(err.Error(), "record not found") {
			tags = ohi.Tags
		}
	}

	rawTags, err := base64.StdEncoding.DecodeString(tags)
	if err != nil {
		return "", error2.ObjectTaggingNotFound{Bucket: objopts.Bucket, Object: objopts.Prefix + "/" + objopts.Object}
	} else {
		return string(rawTags), nil
	}
}

func PutObjectTags(ctx context.Context, objopts ObjectOptions, rawTags string) error {
	_, span := trace.StartSpan(ctx, "PutObjectTags")
	defer span.End()

	tags := base64.StdEncoding.EncodeToString([]byte(rawTags))

	if objopts.CurrentVersion {
		cacheKey := genObjectCacheKey(objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID)
		err := cache.Write(ctx, cacheKey, tags, func() error {
			return updateObjectTag(objopts.Bucket, objopts.Prefix, objopts.Object, tags)
		})
		if err != nil {
			logger.Errorf("update bucket tags failed: ", err)
			return error2.WriteDataBaseFailed{Err: err}
		}

		info, _ := queryObjectInfo(objopts.Bucket, objopts.Prefix, objopts.Object, "")
		if info.Version != Defaultversionid {
			if err := updateObjectHistoryTag(objopts.Bucket, objopts.Prefix, objopts.Object, info.Version, tags); err != nil {
				logger.Errorf("update bucket tags failed: ", err)
				return error2.WriteDataBaseFailed{Err: err}
			}
		}

	}
	if objopts.HistoryVersion {
		cacheKey := genObjectCacheKey(objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID)
		err := cache.Write(ctx, cacheKey, tags, func() error {
			return updateObjectHistoryTag(objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID, tags)
		})
		if err != nil {
			logger.Errorf("update bucket tags failed: ", err)
			return error2.WriteDataBaseFailed{Err: err}
		}
	}
	return nil
}

func QueryObjectAcl(ctx context.Context, objopts ObjectOptions) (string, error) {
	_, span := trace.StartSpan(ctx, "QueryObjectAcl")
	defer span.End()

	if objopts.CurrentVersion {
		var oi ObjectInfo
		cacheKey := genObjectCacheKey(objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID)
		res, err := cache.Read(ctx, cacheKey, &ObjectInfo{}, func() (interface{}, error) {
			return queryObjectInfo(objopts.Bucket, objopts.Prefix, objopts.Object, "")
		}, 0)

		if res != nil {
			oi = *res.(*ObjectInfo)
		}
		if err == nil || strings.Contains(err.Error(), "record not found") {
			return oi.Acl, nil
		}
		return oi.Acl, error2.ObjectACLNotFound{Bucket: objopts.Bucket, Object: objopts.Prefix + "/" + objopts.Object}
	}
	if objopts.HistoryVersion {
		var ohi ObjectInfo
		cacheKey := genObjectCacheKey(objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID)
		res, err := cache.Read(ctx, cacheKey, &ObjectInfo{}, func() (interface{}, error) {
			return queryObjectHistoryInfo(objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID)
		}, 0)

		if res != nil {
			ohi = *res.(*ObjectInfo)
		}
		if err == nil || strings.Contains(err.Error(), "record not found") {
			return ohi.Acl, nil
		}
		return ohi.Acl, error2.ObjectACLNotFound{Bucket: objopts.Bucket, Object: objopts.Prefix + "/" + objopts.Object}
	}

	return "", error2.ObjectACLNotFound{Bucket: objopts.Bucket, Object: objopts.Prefix + "/" + objopts.Object}
}

func PutObjectAcl(ctx context.Context, objopts ObjectOptions, acl string) error {
	_, span := trace.StartSpan(ctx, "PutObjectAcl")
	defer span.End()

	if objopts.CurrentVersion {
		cacheKey := genObjectCacheKey(objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID)
		err := cache.Write(ctx, cacheKey, acl, func() error {
			return updateObjectAcl(objopts.Bucket, objopts.Prefix, objopts.Object, acl)
		})
		if err != nil {
			logger.Errorf("update bucket acl failed: ", err)
			return error2.WriteDataBaseFailed{Err: err}
		}
		info, _ := queryObjectInfo(objopts.Bucket, objopts.Prefix, objopts.Object, "")
		if info.Version != Defaultversionid {
			err := updateObjectHistoryAcl(objopts.Bucket, objopts.Prefix, objopts.Object, info.Version, acl)
			if err != nil {
				logger.Errorf("update bucket acl failed: ", err)
				return error2.WriteDataBaseFailed{Err: err}
			}
		}

	}
	if objopts.HistoryVersion {
		cacheKey := genObjectCacheKey(objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID)
		err := cache.Write(ctx, cacheKey, acl, func() error {
			return updateObjectHistoryAcl(objopts.Bucket, objopts.Prefix, objopts.Object, objopts.VersionID, acl)
		})
		if err != nil {
			logger.Errorf("update bucket acl failed: ", err)
			return error2.WriteDataBaseFailed{Err: err}
		}
	}
	return nil
}

func PutObjectCidInfo(ctx context.Context, obj ObjectChunkInfo) error {
	_, span := trace.StartSpan(ctx, "PutObjectCidInfo")
	defer span.End()

	return insertObjectCid(obj)
}

func GetObjectCidInfos(ctx context.Context) ([]ObjectChunkInfo, error) {
	_, span := trace.StartSpan(ctx, "GetObjectCidInfos")
	defer span.End()

	return getObjectCidInfos()
}
