package metadata

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/pkg/lock"
	"mtcloud.com/mtstorage/pkg/logger"
	error2 "mtcloud.com/mtstorage/pkg/storageerror"
)

// bucket methods

type status int

const (
	null status = iota
	inster
	update
)

var RootError = errors.New("根目录下的文件或文件夹")

func queryBucketInfoByName(bucket string) (bi *BucketInfo, err error) {
	bi = new(BucketInfo)
	err = mtMetadata.db.DB.Unscoped().
		Raw("SELECT * FROM "+BucketTable+" WHERE  name=? LIMIT 1", bucket).
		Find(bi).Error
	return
}

func queryBucketInfoByOwner(owner uint32) (bis *[]BucketInfo, err error) {
	bis = new([]BucketInfo)
	err = mtMetadata.db.DB.Unscoped().
		Raw("SELECT * FROM "+BucketTable+" WHERE  owner=?", owner).
		Find(bis).Error
	return
}

func queryAllBucketInfos() (bis *[]BucketInfo, err error) {
	bis = new([]BucketInfo)
	err = mtMetadata.db.DB.Limit(MaxBucketNum).Unscoped().
		Raw("SELECT * FROM " + BucketTable).
		Find(bis).Error
	return
}

func putBucketInfo(bi *BucketInfo) error {
	return mtMetadata.db.DB.Exec("INSERT INTO "+BucketTable+
		" (name, bucketid, count, size, owner, tenant, profile, policy, versioning, storageclass, location, encryption, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		bi.Name, bi.Bucketid, bi.Count, bi.Size, bi.Owner, bi.Tenant, bi.Profile, bi.Policy, bi.Versioning, bi.StorageClass, bi.Location, bi.Encryption, now(), now()).Error
}

func updateBucketInfo(bi *BucketInfo) error {
	return mtMetadata.db.DB.Exec("UPDATE "+BucketTable+
		" SET bucketid=?, count=?, size=?, owner=?, tenant=?, profile=?, policy=?, versioning=?, storageclass=?, location=?, encryption=?, updated_at=? WHERE  name=?",
		bi.Bucketid, bi.Count, bi.Size, bi.Owner, bi.Tenant, bi.Profile, bi.Policy, bi.Versioning, bi.StorageClass, bi.Location, bi.Encryption, now(), bi.Name).Error
}

func deleteBucketInfo(bucket string) error {
	return mtMetadata.db.DB.Transaction(
		func(tx *gorm.DB) error {
			if err := tx.Exec("DELETE FROM "+BucketExtTable+
				" WHERE  name=?", bucket).Error; err != nil {
				logger.Errorf("delete bucket ext info storageerror:%s", err)
				return err
			}
			if err := tx.Exec("DELETE FROM "+BucketTable+
				" WHERE  name=?", bucket).Error; err != nil {
				logger.Errorf("delete bucket info storageerror:%s", err)
				return err
			}
			return nil
		})
}

func queryBucketExternalInfo(bucket string) (be *BucketExternal, err error) {
	be = new(BucketExternal)
	err = mtMetadata.db.DB.Unscoped().
		Raw("SELECT * FROM "+BucketExtTable+" WHERE  name=? LIMIT 1", bucket).
		Find(be).Error
	return
}

func updateBucketLogging(bucket, logging string) error {
	return mtMetadata.db.DB.
		Exec("UPDATE "+BucketExtTable+
			" SET log=?, updated_at=? WHERE  name=?", logging, now(), bucket).Error
}

func insertBucketLogging(bucket, logging string) error {
	return mtMetadata.db.DB.
		Exec("INSERT INTO "+BucketExtTable+
			" (name, log, created_at, updated_at) VALUES (?,?,?,?)", bucket, logging, now(), now()).Error
}

func updateBucketPolicy(bucket, policy string) error {
	return mtMetadata.db.DB.
		Exec("UPDATE "+BucketExtTable+
			" SET policy=?, updated_at=? WHERE  name=?", policy, now(), bucket).Error
}

func insertBucketPolicy(bucket, policy string) error {
	return mtMetadata.db.DB.
		Exec("INSERT INTO "+BucketExtTable+
			" (name, policy, created_at, updated_at) VALUES (?,?,?,?)", bucket, policy, now(), now()).Error
}

func updateBucketLifecycle(bucket, lifecycle string) error {
	return mtMetadata.db.DB.
		Exec("UPDATE "+BucketExtTable+
			" SET lifecycle=?, updated_at=? WHERE  name=?", lifecycle, now(), bucket).Error
}

func insertBucketLifecycle(bucket, lifecycle string) error {
	return mtMetadata.db.DB.
		Exec("INSERT INTO "+BucketExtTable+
			" (name, lifecycle, created_at, updated_at) VALUES (?,?,?,?)", bucket, lifecycle, now(), now()).Error
}

func updateBucketAcl(bucket, acl string) error {
	return mtMetadata.db.DB.
		Exec("UPDATE "+BucketExtTable+
			" SET acl=?, updated_at=? WHERE  name=?", acl, now(), bucket).Error
}

func insertBucketAcl(bucket, acl string) error {
	return mtMetadata.db.DB.
		Exec("INSERT INTO "+BucketExtTable+
			" (name, acl, created_at, updated_at) VALUES (?,?,?,?)", bucket, acl, now(), now()).Error
}

func updateBucketTag(bucket, tag string) error {
	return mtMetadata.db.DB.
		Exec("UPDATE "+BucketExtTable+
			" SET tag=?, updated_at=? WHERE  name=?", tag, now(), bucket).Error
}

func insertBucketTag(bucket, tag string) error {
	return mtMetadata.db.DB.
		Exec("INSERT INTO "+BucketExtTable+
			" (name, tag, created_at, updated_at) VALUES (?,?,?,?)", bucket, tag, now(), now()).Error
}

// object methods

const (
	insertHistoryObjectSQL     = "INSERT INTO " + ObjectHistoryTable + " (bucket, dirname, name, cid, etag, isdir, content_length,ciphertext_size, content_type, version, storageclass, acl, ismarker, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	insertObjectSQL            = "INSERT INTO " + ObjectTable + " (bucket, dirname, name, cid, etag, isdir, content_length, content_type, version, storageclass, ismarker, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
	deleteObjectSQL            = "DELETE FROM " + ObjectTable + " WHERE  bucket=? and  dirname=? and  name=?"
	deleteObjectWithVersionSQL = "DELETE FROM " + ObjectTable + " WHERE  bucket=? and  dirname=? and  name=? and version=?"
	deletehistoryObjectSQL     = "DELETE FROM " + ObjectHistoryTable + " WHERE  bucket=? and  dirname=? and  name=? and version=?"
	updateObjectSQL            = "UPDATE " + ObjectTable + " SET cid=?, etag=?, content_length=?, ciphertext_size = ?, content_type=?, version=?, storageclass=?, acl=?, ismarker=?, updated_at=? WHERE  bucket=? and  dirname=? and  name=?"
	updateObjectHistorySQL     = "UPDATE " + ObjectHistoryTable + " SET cid=?, etag=?, content_length=?, content_type=?, version=?, storageclass=?, ismarker=?, updated_at=? WHERE  bucket=? and  dirname=? and  name=?"

	updateBucketByIDSQL      = "UPDATE " + BucketTable + " SET name=?, bucketid=?, count=count+?, size=size+?, owner=?, tenant=?, profile=?, policy=?, versioning=?, storageclass=?, location=?, updated_at=? WHERE id=?"
	updateBucketCapSQL       = "UPDATE " + BucketTable + " SET count=?, size=?, updated_at=? WHERE  name=?"
	updateBucketTimeStampSQL = "UPDATE " + BucketExtTable + " SET updated_at=? WHERE  name=?"

	queryObjectBatchSQL             = "SELECT * FROM " + ObjectTable + " WHERE ( dirname LIKE ? OR ( dirname=? AND  name=?)) AND  bucket=?"
	queryObjectSizeSQL              = "SELECT COUNT(*) AS count,SUM(content_length) AS size FROM ( SELECT name,content_length,version FROM " + ObjectTable + " WHERE ( dirname LIKE ? OR ( dirname=? AND  name=?)) AND  bucket=? AND ismarker=false UNION SELECT name,content_length,version FROM " + ObjectHistoryTable + " WHERE ( dirname LIKE ? OR ( dirname=? AND  name=?)) AND  bucket=? AND ismarker=false AND version=?) AS c"
	queryObjectSizeNoVersionsSQL    = "SELECT COUNT(*) AS count,SUM(content_length) AS size FROM ( SELECT name,content_length FROM " + ObjectTable + " WHERE ( dirname LIKE ? OR ( dirname=? AND  name=?)) AND  bucket=? AND ismarker=false) AS c"
	queryDirectorySizeSQL           = "SELECT COUNT(*) AS count,SUM(content_length) AS size FROM ( SELECT name,content_length,version FROM " + ObjectTable + " WHERE ( dirname LIKE ? OR ( dirname=? AND  name=?)) AND  bucket=? AND ismarker=false  UNION SELECT name,content_length,version FROM " + ObjectHistoryTable + " WHERE ( dirname LIKE ? OR ( dirname=? AND  name=?)) AND  bucket=? AND ismarker=false) AS c"
	queryDirectorySizeNoVersionsSQL = "SELECT COUNT(*) AS count,SUM(content_length) AS size FROM ( SELECT name,content_length,version FROM " + ObjectTable + " WHERE ( dirname LIKE ? OR ( dirname=? AND  name=?)) AND  bucket=? AND ismarker=false) AS c"
	deleteDirectoryBatchSQL         = "DELETE FROM " + ObjectTable + " WHERE ( dirname LIKE ? OR ( dirname=? AND  name=?)) AND bucket=?"
	deleteDirectoryHisotryBatchSQL  = "DELETE FROM " + ObjectHistoryTable + " WHERE ( dirname LIKE ? OR ( dirname=? AND  name=?)) AND bucket=?"
)

func queryObjectInfo(bucket, prefix, object string, version string) (oi *ObjectInfo, err error) {
	oi = new(ObjectInfo)
	if version != "" && version != "null" {
		err = mtMetadata.db.DB.Unscoped().
			Raw("SELECT * FROM "+ObjectHistoryTable+
				" WHERE  name=? and  dirname=? and  bucket=? and version=? ORDER BY updated_at DESC LIMIT 1", object, prefix, bucket, version).
			Find(oi).Error
		return
	}
	err = mtMetadata.db.DB.Unscoped().
		Raw("SELECT * FROM "+ObjectTable+
			" WHERE  name=? and  dirname=? and  bucket=? ORDER BY updated_at DESC LIMIT 1", object, prefix, bucket).
		Find(oi).Error
	return
}

func queryObjectInfoStmp(ctx context.Context, tx *gorm.DB, bucket, prefix, object string, version string) (oi *ObjectInfo, err error) {
	_, span := trace.StartSpan(ctx, "QueryObjectInfosByPrefix")
	defer span.End()
	oi = new(ObjectInfo)
	if version != "" {
		err = tx.Unscoped().
			Raw("SELECT * FROM "+ObjectHistoryTable+
				" WHERE  name=? and  dirname=? and  bucket=? and version=? ORDER BY updated_at DESC LIMIT 1", object, prefix, bucket, version).
			Find(oi).Error
		return
	}
	err = tx.Unscoped().
		Raw("SELECT * FROM "+ObjectTable+
			" WHERE  name=? and  dirname=? and  bucket=? ORDER BY updated_at DESC LIMIT 1", object, prefix, bucket).
		Find(oi).Error
	return
}

func queryObjectHistoryInfo(bucket, prefix, object, version string) (ohi *ObjectHistoryInfo, err error) {
	ohi = new(ObjectHistoryInfo)
	if version != "" && version != Defaultversionid {
		err = mtMetadata.db.DB.Unscoped().
			Raw("SELECT * FROM "+ObjectHistoryTable+
				" WHERE  name=? and  dirname=? and  bucket=? and version=? ORDER BY updated_at DESC LIMIT 1", object, prefix, bucket, version).
			Find(ohi).Error
		return
	}
	err = mtMetadata.db.DB.Unscoped().
		Raw("SELECT * FROM "+ObjectTable+
			" WHERE  name=? and  dirname=? and  bucket=? ORDER BY updated_at DESC LIMIT 1", object, prefix, bucket).
		Find(ohi).Error
	return ohi, err
}

func isObjectHistoryExist(bucket, prefix, object string, version string) bool {
	o, _ := queryObjectHistoryInfo(bucket, prefix, object, version)
	return len(o.Name) != 0
}

func queryObjectMarkers(bucket, prefix, marker string, maxkeys int) (ohis []ObjectHistoryInfo, err error) {
	nPrefix := prefix
	if prefix == "/" {
		nPrefix = ""
	}
	var dirmarker, objmarker string
	var o = new(ObjectHistoryInfo)
	if marker != "" {
		o, err = queryObjectHistoryInfo(bucket, prefix, marker, "")
		if err != nil {
			return ohis, err
		}
		if o.Isdir {
			dirmarker = marker
		} else {
			objmarker = marker
		}
	}
	tmpDirs := []ObjectHistoryInfo{}
	if objmarker == "" {
		err = mtMetadata.db.DB.Limit(maxkeys).Unscoped().
			Raw(`SELECT DISTINCT SUBSTRING_INDEX(SUBSTR(dirname, CHAR_LENGTH(?), CHAR_LENGTH(dirname)), '/', 2) dirname FROM
					(
						SELECT CONCAT(?,name) as dirname FROM `+ObjectTable+` WHERE  bucket=? and dirname=? and name>=? and ismarker=true and isdir=true
						UNION
						SELECT dirname FROM `+ObjectTable+` WHERE  bucket=? and dirname REGEXP ? and  dirname>=? and ismarker=true
					) objs WHERE objs.dirname != '/' ORDER BY dirname`, nPrefix+"/", nPrefix+"/", bucket, prefix, dirmarker /*nPrefix+"/", bucket, prefix, dirmarker,*/, bucket, "^("+nPrefix+"/)", "/"+dirmarker /*bucket, "^("+nPrefix+"/)", "/"+dirmarker*/).
			Find(&tmpDirs).Error
	}

	for _, dir := range tmpDirs {
		obj, err := queryObjectHistoryInfo(bucket, prefix, path.Base(dir.Dirname), Defaultversionid)
		if err != nil && !strings.Contains(err.Error(), "record not found") {
			return ohis, err
		}
		if obj.Name != "" {
			ohis = append(ohis, *obj)
		}
	}

	objCnt := maxkeys - len(tmpDirs)
	if objCnt > 0 {
		tmpObjs := []ObjectHistoryInfo{}
		err = mtMetadata.db.DB.Limit(objCnt).Unscoped().
			Raw(`SELECT name,dirname,bucket,version,updated_at FROM
				(
					SELECT * FROM
					(
						SELECT ROW_NUMBER() over(PARTITION by name ORDER BY updated_at desc) row_num, name,dirname,bucket,version,updated_at FROM `+ObjectTable+` WHERE  bucket=? and  dirname=? and  name>=? and ismarker=true and isdir=false
					)objs WHERE objs.row_num=1
				)AS res`,
				bucket, prefix, objmarker).
			Find(&tmpObjs).Error
		if err != nil {
			return []ObjectHistoryInfo{}, err
		}
		ohis = append(ohis, tmpObjs...)
	}
	return
}

func queryObjectInfoAllVersionsByName(bucket, prefix, object, marker, versionmarker string, maxkeys int) (ohis *[]ObjectHistoryInfo, err error) {
	ohis = new([]ObjectHistoryInfo)
	// query marker update time and sort object by time
	if object != "" {
		ts := time.Time{}
		if versionmarker != "" {
			o, err := queryObjectHistoryInfo(bucket, prefix, marker, versionmarker)
			if err != nil {
				return ohis, err
			}
			ts = o.UpdatedAt
		} else {
			ts = now()
		}

		return ohis, mtMetadata.db.DB.Limit(maxkeys+1).Unscoped().Raw("SELECT * FROM "+ObjectHistoryTable+
			" WHERE  bucket=? and  dirname=? and  name=? and updated_at<=? ORDER BY updated_at DESC",
			bucket, prefix, object, ts).Find(ohis).Error
	}
	if versionmarker == "" {
		return ohis, mtMetadata.db.DB.Limit(maxkeys+1).Unscoped().
			Raw("SELECT * FROM "+ObjectHistoryTable+
				" WHERE  bucket=? and  dirname=? and  name>=? ORDER BY name",
				bucket, prefix, marker).
			Find(ohis).Error
	}
	o, e := queryObjectHistoryInfo(bucket, prefix, marker, versionmarker)
	if e != nil {
		return ohis, e
	}
	return ohis, mtMetadata.db.DB.Limit(maxkeys+1).Unscoped().
		Raw("SELECT * FROM "+ObjectHistoryTable+
			" WHERE  bucket=? and  dirname=? and  name>=? and updated_at>=? ORDER BY name",
			bucket, prefix, marker, o.UpdatedAt).
		Find(ohis).Error
}

func queryObjectInfoByPrefix(bucket, objPrefix string, offset, maxkeys int, fetchDel bool) (ois []ObjectInfo, err error) {
	var prefix, obj string
	var oi *ObjectInfo
	if objPrefix != "/" {
		prefix = path.Dir(objPrefix)
		obj = path.Base(objPrefix)
		oi, err = queryObjectInfo(bucket, prefix, obj, "")
		if err == nil && !oi.Isdir {
			ois = append(ois, *oi)
			return ois, err
		}
	}

	err = mtMetadata.db.DB.Offset(offset).Limit(maxkeys).Unscoped().Raw("SELECT  ROW_NUMBER() over(PARTITION by isdir) row_num, id, name, dirname, isdir, bucket, cid, etag, ismarker, storageclass, tags, acl, content_length, content_type, version, updated_at, created_at FROM "+ObjectTable+
		" WHERE  bucket=? AND  dirname=? AND ismarker = ? order BY isdir DESC,updated_at DESC", bucket, objPrefix, fetchDel).
		Find(&ois).Error
	return ois, err
}

func queryObjectCountByPrefix(bucket, objPrefix string) (count int, err error) {
	res := struct{ Count int }{}
	err = mtMetadata.db.DB.Unscoped().Raw("SELECT COUNT(*) AS count FROM "+ObjectTable+
		" WHERE  bucket=? AND  dirname=? AND ismarker = FALSE", bucket, objPrefix).
		Find(&res).Error

	return res.Count, err
}

func getDirObjects(dir, bucket string) (map[status][]string, error) {
	if dir == "/" {
		return nil, RootError
	}
	dirs := buildDirName(context.TODO(), dir)
	names := strings.Split(dir, "/")
	objects := make([]ObjectInfo, 0)
	if err := mtMetadata.db.DB.Table(ObjectTable).
		Where("bucket = ? and dirname in (?) and name in (?) and isdir = 1", bucket, dirs, names[1:]).
		Find(&objects).Error; err != nil {
		return nil, err
	}
	return getNotJoinDir(dirs[:len(dirs)-1], objects), nil
}

func getNotJoinDir(dirs []string, objects []ObjectInfo) map[status][]string {
	objectDirMap := make(map[string]ObjectInfo, len(objects))
	for i := range objects {
		objectDirMap[path.Join(objects[i].Dirname, objects[i].Name)] = objects[i]
	}
	insterOrUpdateDir := make(map[status][]string)
	for i := range dirs {
		if _, ok := objectDirMap[dirs[i]]; ok {
			if objectDirMap[dirs[i]].IsMarker {
				insterOrUpdateDir[update] = append(insterOrUpdateDir[update], dirs[i])
			}
		} else {
			insterOrUpdateDir[inster] = append(insterOrUpdateDir[inster], dirs[i])
		}
	}
	return insterOrUpdateDir
}

func updateDir(tx *gorm.DB, dirs []string, bucket string) error {
	if len(dirs) == 0 {
		return nil
	}
	sqlbuf := strings.Builder{}
	sqlbuf.WriteString("bucket = ? and ( ")
	param := make([]interface{}, 0)
	param = append(param, bucket)
	for i := range dirs {
		if i != 0 {
			sqlbuf.WriteString(" or ")
		}
		sqlbuf.WriteString(" (dirname = ? and name = ? ) ")
		param = append(param, path.Dir(dirs[i]), path.Base(dirs[i]))
	}
	sqlbuf.WriteString(")")
	sql := sqlbuf.String()
	now := time.Now()
	if err := tx.Table(ObjectTable).Where(sql, param...).
		Updates(map[string]interface{}{
			"updated_at": now,
		}).Error; err != nil {
		return err
	}
	return tx.Table(ObjectHistoryTable).Where(sql, param...).
		Updates(map[string]interface{}{
			"updated_at": now,
		}).Error
}

func makeInsertSql(dirs []string, bucket string) (string, []interface{}) {
	if len(dirs) == 0 {
		return "", nil
	}
	now := time.Now()
	sqlBuffer := strings.Builder{}
	// 这里不需要管加密后的文件大小，因为这个构建出的是文件夹的sql，文件夹大小为0
	sqlBuffer.WriteString("INSERT INTO " + ObjectTable + " (bucket, dirname, name, cid, etag, isdir, content_length, content_type, version, storageclass, acl, ismarker, created_at, updated_at) VALUES")
	param := make([]interface{}, 0)
	for i := range dirs {
		sqlBuffer.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?,?),")
		param = append(param, []interface{}{bucket, path.Dir(dirs[i]), path.Base(dirs[i]), "-", "-", 1, 0, DirContentType, Defaultversionid, "STANDARD", "", 0, now, now}...)
	}
	sql := sqlBuffer.String()
	return sql[:len(sql)-1], param
}

func putObjectInfo(ctx context.Context, tx *gorm.DB, bi *BucketInfo, obj *ObjectInfo, ohi *ObjectHistoryInfo, incrCount, incrSize int64) error {
	_, span := trace.StartSpan(ctx, "putObjectInfo")
	defer span.End()
	insterOrUpdateDir, err := getDirObjects(obj.Dirname, bi.Name)
	if err != nil && err != RootError {
		return err
	}
	if err := updateDir(tx, insterOrUpdateDir[update], bi.Name); err != nil {
		return err
	}
	if len(insterOrUpdateDir[inster]) != 0 {
		lock.Lock(ctx, bi.Name+"_"+strings.Split(obj.Dirname, "/")[1], bi.Name)
		insterOrUpdateDir, _ = getDirObjects(obj.Dirname, bi.Name)
		if len(insterOrUpdateDir[inster]) == 0 {
			// 如果没有需要插入的文件夹。立即释放锁
			lock.Unlock(ctx, bi.Name+strings.Split(obj.Dirname, "/")[1], bi.Name)
		}
	}
	sql, insertParam := makeInsertSql(insterOrUpdateDir[inster], bi.Name)
	if sql == "" {
		sql = "INSERT INTO " + ObjectTable + " (bucket, dirname, name, cid, etag, isdir, content_length,ciphertext_size, content_type, version, storageclass, acl, ismarker, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	} else {
		sql += ",(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	}
	insertParam = append(insertParam, obj.Bucket, obj.Dirname, obj.Name, obj.Cid, obj.Etag, obj.Isdir, obj.Content_length, obj.CipherTextSize, obj.Content_type, obj.Version, obj.StorageClass, obj.Acl, obj.IsMarker, now(), now())
	if bi.Versioning == VersioningEnabled && ohi.Name != "" {
		hsql := strings.Replace(sql, ObjectTable, ObjectHistoryTable, 1)
		if err := tx.Exec(hsql, insertParam...).Error; err != nil {
			logger.Errorf("insert object to history table [%s,%s] failed", ohi.Bucket, ohi.Name)
			return err
		}
		logger.Infof("inserted object to history table.")
	}
	if err := tx.Exec(sql, insertParam...).Error; err != nil {
		logger.Errorf("insert object [%s,%s] failed", obj.Bucket, obj.Name)
		return err
	}
	incrCount += int64(len(insterOrUpdateDir[inster])) + 1
	logger.Infof("insert object to table.")
	param := make(map[string]interface{})
	if incrCount > 0 {
		param["count"] = gorm.Expr("count + ?", incrCount)
	}
	if incrSize != 0 {
		param["size"] = gorm.Expr("size + ?", incrSize)
	}
	param["updated_at"] = now()
	if err := tx.Table(BucketTable).Where("id = ?", bi.ID).Updates(param).Error; err != nil {
		return err
	}
	logger.Infof("updated bucket info:%d, %d.", bi.Count, bi.Size)
	return nil
}

func updateObjectInfo(ctx context.Context, tx *gorm.DB, obj *ObjectInfo) error {
	_, span := trace.StartSpan(ctx, "PutObjectInfo")
	defer span.End()
	if err := tx.Exec(updateObjectSQL,
		obj.Cid, obj.Etag, obj.Content_length, obj.CipherTextSize, obj.Content_type, obj.Version, obj.StorageClass, obj.Acl, obj.IsMarker, now(), obj.Bucket, obj.Dirname, obj.Name).Error; err != nil {
		logger.Errorf("update object info storageerror:%s", err)
		return err
	}
	return nil
}

func putObjectHistoryInfo(ctx context.Context, tx *gorm.DB, ohi *ObjectHistoryInfo, incrCount *int64) error {
	_, span := trace.StartSpan(ctx, "PutObjectInfo")
	defer span.End()
	if !(ohi.Isdir && isObjectHistoryExist(ohi.Bucket, ohi.Dirname, ohi.Name, ohi.Version)) {
		if err := tx.Exec(insertHistoryObjectSQL,
			ohi.Bucket, ohi.Dirname, ohi.Name, ohi.Cid, ohi.Etag, ohi.Isdir,
			ohi.Content_length, ohi.CipherTextSize, ohi.Content_type, ohi.Version, ohi.StorageClass,
			ohi.Acl, ohi.IsMarker, now(), now()).Error; err != nil {
			logger.Errorf("insert object history info storageerror:%s", err)
			return err
		}
		*incrCount++
	}
	return nil
}

func updateBucketDate(tx *gorm.DB, bi *BucketInfo, incrCount, incrSize int64) error {
	param := make(map[string]interface{})
	if incrCount > 0 {
		param["count"] = gorm.Expr("count + ?", incrCount)
	}
	if incrSize != 0 {
		param["size"] = gorm.Expr("size + ?", incrSize)
	}
	param["updated_at"] = now()
	return tx.Table(BucketTable).Where("id = ?", bi.ID).Updates(param).Error
}

func deleteDirPipeline(ctx context.Context, bi *BucketInfo, dir, name string) (DeletedObjects, error) {
	_, span := trace.StartSpan(ctx, "deleteDirPipeline")
	defer span.End()
	total := DeletedObjects{}
	tx := mtMetadata.db.DB.Begin()
	objets, err := getObjectsInDir(ctx, tx, bi, dir, name)
	if err != nil {
		tx.Rollback()
		return total, err
	}
	// 构建insert t_ns_object 表的sql
	sql, param, id, unVersionObject := buildInsertDeleteMarkSql(ctx, objets, &total, ObjectTable)
	if sql[0] == "" {
		return total, errors.New("构建sql失败，sql:" + sql[0])
	}
	if err := tx.Unscoped().Table(ObjectTable).Where("id in (?)", id).Delete(ObjectInfo{}).Error; err != nil {
		tx.Rollback()
		return total, err
	}
	if bi.Versioning == VersioningEnabled {
		for i := range unVersionObject {
			if err := tx.Table(ObjectHistoryTable).Create(&unVersionObject[i]).Error; err != nil {
				tx.Rollback()
				return total, err
			}
		}
		for i := range sql {
			if err := tx.Exec(sql[i], param[i]...).Error; err != nil {
				tx.Rollback()
				return total, err
			}
			if err := tx.Exec(strings.Replace(sql[i], ObjectTable, ObjectHistoryTable, 1), param[i]...).Error; err != nil {
				tx.Rollback()
				return total, err
			}
		}

	} else {
		total.Count = 0
		// 关闭多版本的情况下，删除的对象如果有版本号则需要put一个mark
		for i := range objets {
			// 删除对不是marker的对象，两张表都需要inster marker 并且非多版本和文件夹要减1
			if err := putObjectMark(ctx, tx, objets[i]); err != nil {
				tx.Rollback()
				return total, err
			}
			if objets[i].Version == Defaultversionid || objets[i].Isdir {
				total.Count++
				total.Size += objets[i].Content_length
			}
		}
	}
	// 不管是不是多版本，删除文件夹。文件夹的记录都会被删除
	// todo 这里还是有待讨论，如果之后删除文件夹就只是删除文件夹辣条记录和文件夹里的文件没有关系，那么这里就不符合业务了
	//_, err = DelHistoryDirs(ctx, tx, bi, dir, name)
	//if err != nil {
	//	return DeletedObjects{}, err
	//}
	// todo 删除object 表的文件夹
	// todo 删除历史表的文件夹
	if err := UpdateBucketCount(ctx, tx, bi.Name, int64(total.Count), int64(total.Size)); err != nil {
		tx.Rollback()
		return total, err
	}
	tx.Commit()
	return total, nil
}

func getObjectsInDir(ctx context.Context, db *gorm.DB, bi *BucketInfo, dir, name string) ([]ObjectInfo, error) {
	_, span := trace.StartSpan(ctx, "getObjectsInDir")
	defer span.End()
	dirName := path.Join(dir, name)
	objets := make([]ObjectInfo, 0)
	if err := db.Table(ObjectTable).
		Where("ismarker = 0 and bucket = ? and (dirname LIKE ? or (dirname = ? and name = ?) or (dirname = ?))", bi.Name, dirName+"/%", dir, name, dirName).
		Find(&objets).Error; err != nil {
		return nil, err
	}

	return objets, nil
}

func DelHistoryDirs(ctx context.Context, db *gorm.DB, bi *BucketInfo, dir, name string) (int64, error) {
	_, span := trace.StartSpan(ctx, "DelDirs")
	defer span.End()
	dirName := path.Join(dir, name)
	db = db.Unscoped().Table(ObjectHistoryTable).
		Where("ismarker = 0 and bucket = ? and isdir  = 1 and (dirname LIKE ? or (dirname = ? and name = ?) or (dirname = ?))", bi.Name, dirName+"/%", dir, name, dirName).
		Delete(ObjectHistoryInfo{})
	if db.Error != nil {
		return 0, nil
	}

	return db.RowsAffected, nil
}

func buildInsertDeleteMarkSql(ctx context.Context, objects []ObjectInfo, total *DeletedObjects, table string) ([]string, [][]interface{}, []int64, []ObjectInfo) {
	_, span := trace.StartSpan(ctx, "buildInsertDeleteMarkSql")
	defer span.End()
	sqls := make([]string, 0)
	if len(objects) == 0 {
		return nil, nil, nil, nil
	}
	unVersionObject := make([]ObjectInfo, 0)
	sqlBuffer := strings.Builder{}
	sqlBuffer.WriteString("INSERT INTO " + table + " (bucket, dirname, name, cid, etag, isdir, content_length, content_type, version, storageclass, ismarker,created_at, updated_at) VALUES")
	param := make([]interface{}, 0)
	params := make([][]interface{}, 0, len(objects)/300+1)
	id := make([]int64, 0, len(objects))
	now := time.Now()
	insterCount := 0
	for i := range objects {
		objectId := objects[i].ID
		if objects[i].Version == Defaultversionid && !objects[i].Isdir {
			objects[i].ID = 0
			objects[i].CreatedAt = time.Now().Add(-time.Second)
			objects[i].UpdatedAt = time.Now().Add(-time.Second)
			unVersionObject = append(unVersionObject, objects[i])
		}
		if objects[i].Isdir {
			total.Count++
		}
		id = append(id, int64(objectId))
		if insterCount%300 == 0 && len(param) != 0 {
			sql := sqlBuffer.String()
			sqls = append(sqls, sql[:len(sql)-1])
			stmpParams := make([]interface{}, len(param))
			copy(stmpParams, param)
			params = append(params, stmpParams)
			param = param[:0]
			sqlBuffer.Reset()
			sqlBuffer.WriteString("INSERT INTO " + table + " (bucket, dirname, name, cid, etag, isdir, content_length, content_type, version, storageclass, ismarker,created_at, updated_at) VALUES")
			sqlBuffer.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?),")
			param = append(param, objects[i].Bucket, objects[i].Dirname, objects[i].Name, "-", "-", objects[i].Isdir, 0, objects[i].Content_type, genVersionId(objects[i].Isdir), objects[i].StorageClass, 1, now, now)
			insterCount = 1
			continue
		}
		sqlBuffer.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?),")
		param = append(param, objects[i].Bucket, objects[i].Dirname, objects[i].Name, "-", "-", objects[i].Isdir, 0, objects[i].Content_type, genVersionId(objects[i].Isdir), objects[i].StorageClass, 1, now, now)

		insterCount++
	}
	sql := sqlBuffer.String()
	sqls = append(sqls, sql[:len(sql)-1])
	params = append(params, param)
	return sqls, params, id, unVersionObject
}

func deleteObjectPipeline(ctx context.Context, bi *BucketInfo, info *ObjectInfo, reqVerrsion string) (DeletedObjects, error) {
	_, span := trace.StartSpan(ctx, "deleteObjectPipeline")
	defer span.End()
	total := DeletedObjects{}
	// 恢复的文件夹个数
	recoverDir := 0
	tx := mtMetadata.db.DB.Begin()
	if err := deleteObject(ctx, tx, bi, info, reqVerrsion, &recoverDir); err != nil {
		tx.Rollback()
		return total, err
	}
	if bi.Versioning == VersioningEnabled {
		if !info.IsMarker && reqVerrsion != "" {
			total.Size = info.Content_length
			total.Count = 1
		}
	} else {
		if !info.IsMarker && (info.Version == "null" || reqVerrsion != "") {
			total.Size = info.Content_length
			total.Count = 1
		}
	}
	if err := UpdateBucketCount(ctx, tx, bi.Name, int64(total.Count), int64(total.Size)); err != nil {
		tx.Rollback()
		return total, err
	}
	if err := tx.Table(BucketTable).Where("name=? ", bi.Name).Updates(map[string]interface{}{
		"count": gorm.Expr("count + ?", recoverDir),
	}).Error; err != nil {
		return total, err
	}
	tx.Commit()
	return total, nil
}

func deleteObject(ctx context.Context, tx *gorm.DB, bi *BucketInfo, info *ObjectInfo, reqVersion string, total *int) error {
	_, span := trace.StartSpan(ctx, "deleteObject")
	defer span.End()
	odb := tx
	if info.Name == "" || info.Dirname == "" {
		return errors.New("删除对象没有指定对象名和文件夹")
	}
	if reqVersion != "" {
		odb = tx.Where("version = ?", info.Version)
	}
	rows := odb.Table(ObjectTable).Unscoped().
		Where("bucket=? AND dirname=? AND name=?", bi.Name, info.Dirname, info.Name).
		Delete(ObjectInfo{}).RowsAffected
	if bi.Versioning == VersioningEnabled {
		if err := tx.Table(ObjectHistoryTable).Unscoped().
			Where("bucket=? AND dirname=? AND name=? and version = ?", bi.Name, info.Dirname, info.Name, reqVersion).
			Delete(ObjectInfo{}).Error; err != nil {
			return err
		}
		switch {
		// 开启多版本的情况下version= ""，删除t_ns_object成功，object不是mark说明删除的是最新的版本，需要insertMark
		case reqVersion == "" && rows == 1 && !info.IsMarker:
			if info.Version == Defaultversionid && !info.Isdir {
				if err := tx.Unscoped().Table(ObjectHistoryTable).Where("name=? and dirname=? and bucket=? and version= 'null'", info.Name, info.Dirname, info.Bucket).Delete(ObjectHistoryInfo{}).Error; err != nil {
					return err
				}
				info.ID = 0
				info.CreatedAt = time.Now().Add(-time.Second)
				info.UpdatedAt = time.Now().Add(-time.Second)
				if err := tx.Table(ObjectHistoryTable).Create(info).Error; err != nil {
					return err
				}
			}
			if err := putObjectMark(ctx, tx, *info); err != nil {
				return err
			}

			// 开启多版本的情况下version != ""，删除t_ns_object成功，说明删除的是最新的版本且带了版本号，需要insertObject
			// 开启多版本的情况下version = ""，删除的对象是mark 说明删除的最新的mark，需要insertObject
		case (reqVersion != "" && rows == 1) || (reqVersion == "" && info.IsMarker):
			ohi := make([]ObjectHistoryInfo, 0)
			if err := tx.Table(ObjectHistoryTable).
				Where("bucket=? AND dirname=? AND name=?", bi.Name, info.Dirname, info.Name).
				Order("id desc").
				First(&ohi).Error; err != nil && err != gorm.ErrRecordNotFound {
				return err
			}
			if len(ohi) > 0 {
				// 如果删除mark 标记的是非多版本，删除历史表的非多版本
				if info.Version == Defaultversionid && info.IsMarker {
					if err := tx.Table(ObjectHistoryTable).Unscoped().
						Where("id in (?)", ohi[0].ID).
						Delete(ObjectInfo{}).Error; err != nil {
						return err
					}
				}
				ohi[0].ID = 0
				if err := tx.Table(ObjectTable).Create(&ohi[0]).Error; err != nil {
					return err
				}
			}

		}
	} else {
		switch {
		case rows == 1 && !info.IsMarker:
			// 这里不应该由有没有指定版本号来判断
			// 没有指定版本号的情况下删除一个多版本对象，不会真正删除
			if reqVersion == "" {
				switch info.Version == Defaultversionid {
				case true:
					if err := putObjectMarkNull(ctx, tx, *info, ObjectHistoryTable); err != nil {
						return err
					}
					if err := putObjectMarkNull(ctx, tx, *info, ObjectTable); err != nil {
						return err
					}
				case false:
					if err := putObjectMark(ctx, tx, *info); err != nil {
						return err
					}
				}
				return nil
			} else {
				if err := tx.Table(ObjectHistoryTable).Unscoped().
					Where("bucket=? AND dirname=? AND name=? and version = ?", bi.Name, info.Dirname, info.Name, reqVersion).
					Delete(ObjectInfo{}).Error; err != nil {
					return err
				}
				ohi := make([]ObjectHistoryInfo, 0)
				if err := tx.Table(ObjectHistoryTable).
					Where("bucket=? AND dirname=? AND name=?", bi.Name, info.Dirname, info.Name).
					Order("id desc").
					Limit(1).
					Find(&ohi).Error; err != nil && err != gorm.ErrRecordNotFound {
					return err
				}
				ohi[0].ID = 0
				if err := tx.Table(ObjectTable).Create(&ohi[0]).Error; err != nil {
					return err
				}
				return nil

			}

		case rows == 1 && info.IsMarker:
			// 如果删除的是一个marker 而且删除object表成功
			// 删除的最新的mark
			// 需要删除历史表最新的mark，将历史表第二新的记录恢复到object表
			ohi := make([]ObjectHistoryInfo, 0)
			if err := tx.Table(ObjectHistoryTable).
				Where("bucket=? AND dirname=? AND name=?", bi.Name, info.Dirname, info.Name).
				Order("id desc").
				Limit(2).
				Find(&ohi).Error; err != nil && err != gorm.ErrRecordNotFound {
				return err
			}
			ids := make([]uint, 0, len(ohi))
			for i := range ohi {
				if i == 0 {
					ids = append(ids, ohi[i].ID)
				}
				if i == 1 {
					ohi[i].ID = 0
					if err := tx.Table(ObjectTable).Create(&ohi[i]).Error; err != nil {
						return err
					}
				}
			}
			if err := tx.Table(ObjectHistoryTable).Unscoped().
				Where("id in (?)", ids).
				Delete(ObjectInfo{}).Error; err != nil {
				return err
			}
		case rows == 0 && reqVersion != "":
			if err := tx.Table(ObjectHistoryTable).Unscoped().
				Where("bucket=? AND dirname=? AND name=? and version = ?", bi.Name, info.Dirname, info.Name, info.Version).
				Delete(ObjectInfo{}).Error; err != nil {
				return err
			}

		}
	}

	// 文件夹在object表，不再history表，将object中的数据inster到history
	if info.Dirname != "/" && !info.Isdir {
		objectDir := make([]ObjectInfo, 0)
		param, sql := getchildDirSql(ctx, info.Dirname, bi.Name)
		if err := tx.Table(ObjectTable).
			Where(sql, param...).
			Find(&objectDir).Error; err != nil {
			return err
		}
		for i := range objectDir {
			objectHDir := make([]ObjectInfo, 0)
			if err := tx.Table(ObjectHistoryTable).
				Where("bucket = ? and  isdir = 1 and name = ? and dirname = ? ", bi.Name, objectDir[i].Name, objectDir[i].Dirname).
				Find(&objectHDir).Error; err != nil {
				return err
			}
			if len(objectHDir) == 0 {
				objectDir[i].ID = 0
				if err := tx.Table(ObjectHistoryTable).Create(&objectDir[i]).Error; err != nil {
					return err
				}
			}
		}
	}
	//  删除的对象如果不是在桶的根文件夹下这则需要判断改文件夹是否被删除，如果被删除就需要恢复该文件夹
	if info.Dirname != "/" && info.IsMarker && rows == 1 && !info.Isdir {
		number, err := RecoverDir(ctx, tx, info.Dirname, bi.Name)
		if err != nil {
			return err
		}
		*total = number

	}
	return nil
}

func RecoverDir(ctx context.Context, tx *gorm.DB, dir, bucket string) (int, error) {
	_, span := trace.StartSpan(ctx, "RecoverDir")
	defer span.End()
	type ID struct {
		Id int64 `json:"id"`
	}
	ids := make([]ID, 0)
	param, sql := getchildDirSql(ctx, dir, bucket)
	if err := tx.Table(ObjectTable).
		Where(sql, param...).
		Select("id").
		Scan(&ids).Error; err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	dirIds := make([]int64, 0)
	for i := range ids {
		dirIds = append(dirIds, ids[i].Id)
	}
	ids = ids[:0]

	if err := tx.Table(ObjectHistoryTable).
		Where(sql, param...).
		Select("id").
		Scan(&ids).Error; err != nil {
		return 0, err
	}
	dirHIds := make([]int64, 0)
	for i := range ids {
		dirHIds = append(dirHIds, ids[i].Id)
	}
	if err := tx.Unscoped().Table(ObjectTable).Where("id in (?)", dirIds).Updates(map[string]interface{}{"ismarker": 0, "version": Defaultversionid}).Error; err != nil {
		return 0, err
	}
	if err := tx.Unscoped().Table(ObjectHistoryTable).Where("id in (?)", dirHIds).Delete(ObjectHistoryInfo{}).Error; err != nil {
		return 0, err
	}
	return len(dirIds), nil
}

// 获取组成目录dir的子目录的sql和参数
func getchildDirSql(ctx context.Context, dir string, bucket string) ([]interface{}, string) {
	dirs := buildDirName(ctx, dir)
	dirs = dirs[:len(dirs)-1]
	sqlbuf := strings.Builder{}
	sqlbuf.WriteString("bucket = ? and   isdir = 1  and ismarker = 1 and ((dirname = '/' and name = ? ) ")
	param := make([]interface{}, 0)
	param = append(param, bucket, dirs[0][1:])
	for i := range dirs {
		if i == 0 {
			continue
		}
		sqlbuf.WriteString("or (dirname = ? and name = ?) ")
		param = append(param, path.Dir(dirs[i]), path.Base(dirs[i]))
	}
	sqlbuf.WriteString(")")
	sql := sqlbuf.String()
	return param, sql
}

func buildDirName(ctx context.Context, dirName string) []string {
	_, span := trace.StartSpan(ctx, "buildDirName")
	defer span.End()
	dir := make([]string, 0)
	dirComponents := strings.Split(dirName, "/")
	dirTemp := strings.Builder{}
	for i := range dirComponents {
		if dirComponents[i] == "" {
			continue
		}
		dirTemp.WriteString("/")
		dirTemp.WriteString(dirComponents[i])
		dir = append(dir, dirTemp.String())
	}
	dir = append(dir, "/")
	return dir
}

func putObjectMarkNull(ctx context.Context, db *gorm.DB, info ObjectInfo, tableName string) error {
	_, span := trace.StartSpan(ctx, "putObjectMarkNull")
	defer span.End()
	info.ID = 0
	info.Cid = "-"
	info.Version = genVersionId(true)
	info.IsMarker = true
	info.CreatedAt = time.Now()
	info.UpdatedAt = time.Now()
	info.Content_length = 0
	info.Tags = ""
	// info.Acl = ""
	info.Etag = "-"
	if err := db.Table(tableName).Create(&info).Error; err != nil {
		return err
	}
	return nil
}

func putObjectMark(ctx context.Context, db *gorm.DB, info ObjectInfo) error {
	_, span := trace.StartSpan(ctx, "putObjectMark")
	defer span.End()
	info.ID = 0
	info.Cid = "-"
	info.Version = genVersionId(false)
	info.IsMarker = true
	info.CreatedAt = time.Now()
	info.UpdatedAt = time.Now()
	info.Content_length = 0
	info.Tags = ""
	info.Acl = ""
	info.Etag = "-"
	if err := db.Table(ObjectTable).Create(&info).Error; err != nil {
		return err
	}
	info.ID = 0
	if err := db.Table(ObjectHistoryTable).Create(&info).Error; err != nil {
		return err
	}
	return nil
}

func UpdateBucketCount(ctx context.Context, db *gorm.DB, bucketName string, incrCount, incrSize int64) error {
	_, span := trace.StartSpan(ctx, "UpdateBucketCount")
	defer span.End()
	param := make(map[string]interface{})
	if incrCount != 0 {
		param["count"] = gorm.Expr("count - ?", incrCount)
	}
	if incrSize != 0 {
		param["size"] = gorm.Expr("size - ?", incrSize)
	}
	if len(param) == 0 {
		return nil
	}
	db = db.Table(BucketTable).Where("name=? and count >= ? and size >= ?", bucketName, incrCount, incrSize).Updates(param)
	if err := db.Error; err != nil {
		return err
	}
	affected := db.RowsAffected
	if affected == 0 {
		logger.Error("桶对象数量和大小更新失败")
	}
	return nil
}

// If object is a direcotry, recursive delete all files under it
// Not add marker or new version to bucket size.
func deleteObjectInfo(ctx context.Context, bi *BucketInfo, dir, name, version string, isdir bool) (total DeletedObjects, err error) {
	_, span := trace.StartSpan(ctx, "deleteObjectInfo")
	defer span.End()

	bucket := bi.Name
	object := path.Join(dir, name)
	if !isdir {
		if bi.Versioning != VersioningEnabled {
			err = mtMetadata.db.DB.Unscoped().
				Raw(queryObjectSizeNoVersionsSQL, object+"%", dir, name, bucket).
				Find(&total).Error
		} else {
			err = mtMetadata.db.DB.Unscoped().
				Raw(queryObjectSizeSQL, object+"%", dir, name, bucket,
					object+"%", dir, name, bucket, version).
				Find(&total).Error
		}
		if err != nil {
			return DeletedObjects{}, error2.WriteDataBaseFailed{Err: err}
		}
		logger.Info("query deleted object size:", total)
	} else {
		if bi.Versioning != VersioningEnabled {
			err = mtMetadata.db.DB.Unscoped().Raw(queryDirectorySizeNoVersionsSQL,
				object+"%", dir, name, bucket).
				Find(&total).Error
			if err != nil {
				return DeletedObjects{}, error2.WriteDataBaseFailed{Err: err}
			}
		} else {
			err = mtMetadata.db.DB.Unscoped().Raw(queryDirectorySizeSQL,
				object+"%", dir, name, bucket, object+"%", dir, name, bucket).
				Find(&total).Error
			if err != nil {
				return DeletedObjects{}, error2.WriteDataBaseFailed{Err: err}
			}
		}
		logger.Info("query deleted object size:", total)
	}

	err = mtMetadata.db.DB.Transaction(
		func(tx *gorm.DB) error {
			if isdir {
				err := tx.Exec(deleteDirectoryBatchSQL, object+"%", dir, name, bucket).Error
				if err != nil {
					logger.Errorf("delete [%s,%s] failed", dir, name)
					return error2.WriteDataBaseFailed{Err: err}
				}
			} else {
				// fixme 删除历史版本的时候会误删掉t_ns_object表的内容
				err := tx.Exec(deleteObjectSQL, bucket, dir, name).Error
				if err != nil {
					logger.Errorf("delete [%s,%s,%s] failed", dir, name)
					return error2.WriteDataBaseFailed{Err: err}
				}
			}
			if bi.Versioning == VersioningEnabled {
				if isdir {
					err := tx.Exec(deleteDirectoryHisotryBatchSQL, object+"%", dir, name, bucket).Error
					if err != nil {
						logger.Errorf("delete [%s,%s,%s] failed", dir, name, version)
						return error2.WriteDataBaseFailed{Err: err}
					}
				} else {
					err := tx.Exec(deletehistoryObjectSQL, bucket, dir, name, version).Error
					if err != nil {
						logger.Errorf("delete [%s,%s,%s] failed", dir, name, version)
						return error2.WriteDataBaseFailed{Err: err}
					}
				}
			}
			err := tx.Exec(updateBucketCapSQL,
				bi.Count-total.Count, bi.Size-total.Size, now(), bi.Name).Error
			if err != nil {
				return error2.WriteDataBaseFailed{Err: err}
			}
			return nil
		})
	return total, nil
}

func genVersionId(isDefault bool) string {
	if isDefault {
		return Defaultversionid
	}
	return fmt.Sprintf("%x", md5.Sum([]byte(now().String())))
}

// insert delete marker
func putObjDelMarker(ctx context.Context, bi BucketInfo, dir, name string) error {
	return mtMetadata.db.DB.Transaction(
		func(tx *gorm.DB) error {
			object := path.Join(dir, name)
			// Query all object from object table
			var ois []ObjectInfo
			err := mtMetadata.db.DB.Unscoped().Raw(queryObjectBatchSQL,
				object+"%", dir, name, bi.Name).Find(&ois).Error
			if err != nil {
				logger.Errorf("get object info storageerror:%s", err)
				return err
			}
			ts := now()
			for _, oi := range ois {
				if oi.IsMarker {
					continue
				}
				vid := genVersionId(oi.Isdir)
				err := tx.Exec(updateObjectSQL, DefaultCid, DefaultEtag, 0,
					oi.Content_type, vid, oi.StorageClass, oi.Acl,
					true, ts, oi.Bucket, oi.Dirname, oi.Name).Error
				if err != nil {
					logger.Errorf("update object as marker storageerror:%s", err)
					return err
				}
				err = tx.Exec(insertHistoryObjectSQL,
					oi.Bucket, oi.Dirname, oi.Name, DefaultCid, DefaultEtag,
					oi.Isdir, 0, oi.Content_type, vid, oi.StorageClass, oi.Acl,
					true, ts, ts).Error
				if err != nil {
					logger.Errorf("insert object marker storageerror:%s", err)
					return err
				}
			}
			if dir != "/" {
				err = tx.Exec(insertHistoryObjectSQL,
					bi.Name, path.Dir(dir), path.Base(dir), DefaultCid, DefaultEtag,
					true, 0, 0, Defaultversionid, bi.StorageClass, DefaultOjbectACL,
					true, ts, ts).Error
				if err != nil {
					logger.Errorf("insert object dir marker storageerror:%s", err)
					return err
				}
			}
			err = tx.Exec(updateBucketTimeStampSQL, ts, bi.Name).Error
			if err != nil {
				logger.Errorf("update bucket info storageerror:%s", err)
				return err
			}
			return nil
		})
}

func deleteObjDelMarker(ctx context.Context, bi *BucketInfo, dir, name, version string, isdir bool) error {
	_, err := deleteObjectInfo(ctx, bi, dir, name, version, isdir)
	if err != nil {
		logger.Errorf("delete object in db: %s", err)
		return err
	}

	pdir := dir
	var pname string
	if pdir != "/" {
		pdir = path.Dir(dir)
		pname = path.Base(dir)
	}
	return mtMetadata.db.DB.Transaction(
		func(tx *gorm.DB) error {
			if err := tx.Exec(`DELETE FROM `+ObjectHistoryTable+
				` WHERE  bucket=? AND  dirname=? AND name=? AND ismarker=true and not exists (SELECT * from(SELECT * FROM `+
				ObjectHistoryTable+` WHERE  bucket=? AND  dirname=? AND ismarker=true)as c)`,
				bi.Name, pdir, pname, bi.Name, dir).Error; err != nil {
				logger.Errorf("delete object marker storageerror:%s", err)
				return err
			}
			return nil
		})
}

func updateObjectHistoryTag(bucket, prefix, name, version, tags string) error {
	return mtMetadata.db.DB.Exec("UPDATE "+ObjectHistoryTable+
		" SET tags=?, updated_at=? WHERE  bucket=? and  dirname=? and  name=? and version=?",
		tags, now(), bucket, prefix, name, version).Error
}

func updateObjectTag(bucket, prefix, name, tags string) error {
	return mtMetadata.db.DB.Exec("UPDATE "+ObjectTable+
		" SET tags=?, updated_at=? WHERE  bucket=? and  dirname=? and  name=?",
		tags, now(), bucket, prefix, name).Error
}

func updateObjectHistoryAcl(bucket, prefix, name, version, acl string) error {
	return mtMetadata.db.DB.Exec("UPDATE "+ObjectHistoryTable+
		" SET acl=?, updated_at=? WHERE  bucket=? and  dirname=? and  name=? and version=?",
		acl, now(), bucket, prefix, name, version).Error
}

func updateObjectAcl(bucket, prefix, name, acl string) error {
	return mtMetadata.db.DB.Exec("UPDATE "+ObjectTable+
		" SET acl=?, updated_at=? WHERE  bucket=? and  dirname=? and name=?",
		acl, now(), bucket, prefix, name).Error
}

func getBucketsLogging() ([]BucketExternal, error) {
	// todo 获取所有开启logging的桶列表
	buckets := make([]BucketExternal, 0)

	if err := mtMetadata.db.DB.Table(BucketExtTable).Where("log != ''").Find(&buckets).Error; err != nil {
		return nil, err
	}
	return buckets, nil
}

func insertObjectCid(obj ObjectChunkInfo) error {
	if err := mtMetadata.db.DB.Create(&obj).Error; err != nil {
		return err
	}

	return nil
}

func getObjectCidInfos() ([]ObjectChunkInfo, error) {
	objectCidInfos := make([]ObjectChunkInfo, 0)

	if err := mtMetadata.db.DB.Where("status = 0").Find(&objectCidInfos).Error; err != nil {
		return nil, err
	}

	return objectCidInfos, nil
}

func GetObjectTag(bucket, name, dirName string) string {

	return ""
}
