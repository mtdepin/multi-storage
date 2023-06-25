package metadata

import (
	"context"
	"fmt"
	"strings"

	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/pkg/cache"
	"mtcloud.com/mtstorage/pkg/logger"
	error2 "mtcloud.com/mtstorage/pkg/storageerror"
)

func CheckBucketExist(ctx context.Context, bucket string) bool {
	_, span := trace.StartSpan(ctx, "CheckBucketExist")
	defer span.End()

	var bi BucketInfo
	res, _ := cache.Read(ctx, fmt.Sprintf("ns:%s", bucket), &BucketInfo{}, func() (interface{}, error) {
		return queryBucketInfoByName(bucket)
	}, 0)

	if res != nil {
		bi = *res.(*BucketInfo)
	}

	return len(bi.Name) != 0
}

func QueryBucketInfo(ctx context.Context, bucket string) (BucketInfo, error) {
	_, span := trace.StartSpan(ctx, "QueryBucketInfo")
	defer span.End()
	logger.Info("query bucket info")
	if len(bucket) == 0 {
		logger.Error("arg storageerror")
		return BucketInfo{}, error2.BucketNameInvalid{}
	}
	var bi BucketInfo
	res, err := cache.Read(ctx, fmt.Sprintf("ns:%s", bucket), &BucketInfo{}, func() (interface{}, error) {
		return queryBucketInfoByName(bucket)
	}, 0)

	if res != nil {
		bi = *res.(*BucketInfo)
	}
	if err != nil {
		logger.Errorf("query bucket failed:%s", err)
		return BucketInfo{}, error2.BucketNotFound{Bucket: bucket}
	}
	return bi, err
}

func QueryAllBucketInfos(ctx context.Context) ([]BucketInfo, error) {
	_, span := trace.StartSpan(ctx, "QueryAllBucketInfos")
	defer span.End()
	logger.Info("query all buckets")
	bs, err := queryAllBucketInfos()
	if err != nil {
		logger.Errorf("querr all bucketinfo storageerror:%s", err)
		return *bs, error2.BucketNotFound{}
	}
	return *bs, nil
}

func QueryBucketInfoByOwner(ctx context.Context, owner uint32) ([]BucketInfo, error) {
	_, span := trace.StartSpan(ctx, "QueryBucketInfoByOwner")
	defer span.End()
	logger.Info("query bucket info by owner")

	var bis []BucketInfo
	res, err := cache.Read(ctx, fmt.Sprintf("ns:users:%d:bucket", owner), &[]BucketInfo{}, func() (interface{}, error) {
		return queryBucketInfoByOwner(owner)
	}, 0)

	if res != nil {
		bis = *res.(*[]BucketInfo)
	}
	if err != nil {
		logger.Errorf("query bucket infos storageerror:%s", err)
		return []BucketInfo{}, error2.BucketNotFound{}
	}

	return bis, nil
}

func freshBucketCache(ctx context.Context, bucket string, owner uint32) {
	for _, cacheKey := range []string{
		"ns:" + bucket,
		fmt.Sprintf("ns:users:%d:bucket", owner),
	} {
		err := cache.Delete(ctx, cacheKey)
		if err != nil {
			logger.Errorf("delete bucket in cache: %s", err)
		}
	}
}

func PutBucketInfo(ctx context.Context, b *BucketInfo) error {
	ctx, span := trace.StartSpan(ctx, "PutBucketInfo")
	defer span.End()
	if b == nil {
		logger.Error("arg storageerror")
		return error2.ErrInvalidArgument
	}

	logger.Infof("put bucket[%s]", b.Name)
	if !CheckBucketExist(ctx, b.Name) {
		err := putBucketInfo(b)
		if err != nil {
			logger.Errorf("put bucket failed: %s", err)
			return error2.WriteDataBaseFailed{Err: err}
		}
		freshBucketCache(ctx, b.Name, b.Owner)
		return nil
	}

	logger.Errorf("bucket [%s] exists", b.Name)
	return error2.BucketAlreadyOwnedByYou{Bucket: b.Name}
}

func UpdateBucketInfo(ctx context.Context, b *BucketInfo) error {
	_, span := trace.StartSpan(ctx, "UpdateBucketInfo")
	defer span.End()
	if b == nil {
		logger.Error("arg storageerror")
		return error2.ErrInvalidArgument
	}

	logger.Info("update bucket: ", b.Name)

	err := updateBucketInfo(b)
	if err != nil {
		logger.Errorf("update bucket failed: %s", err)
		return error2.WriteDataBaseFailed{Err: err}
	}

	freshBucketCache(ctx, b.Name, b.Owner)
	return nil
}

func DeleteBucketInfo(ctx context.Context, bucket string) error {
	_, span := trace.StartSpan(ctx, "DeleteBucketInfo")
	defer span.End()
	if len(bucket) == 0 {
		logger.Error("bucket name empty")
		return error2.BucketNameInvalid{}
	}

	logger.Info("delete bucket: ", bucket)
	bi, err := queryBucketInfoByName(bucket)
	if err != nil {
		logger.Errorf("query bucket failed:%s", err)
		return error2.BucketNotFound{Bucket: bucket}
	}
	if bi.Count != 0 {
		logger.Errorf("%s not empty", bucket)
		return error2.BucketNotEmpty{Bucket: bucket}
	}
	err = deleteBucketInfo(bucket)
	if err != nil {
		logger.Errorf("delete bucket failed: %s", err)
		return error2.WriteDataBaseFailed{Err: err}
	}

	freshBucketCache(ctx, bucket, bi.Owner)
	return nil
}

func GetStorageInfo(ctx context.Context) (StorageInfo, error) {
	_, span := trace.StartSpan(ctx, "GetStorageInfo")
	defer span.End()
	bis, err := queryAllBucketInfos()
	if err != nil {
		logger.Errorf("query buckets storageerror: %s", err)
		return StorageInfo{}, error2.BucketNotFound{}
	}
	var count, size uint64
	for _, bi := range *bis {
		count += bi.Count
		size += bi.Size
	}

	return StorageInfo{
		BucketsNum: len(*bis),
		ObjectNum:  count,
		TotalSzie:  size,
	}, nil
}

func QueryBucketLogging(ctx context.Context, bucket string) (string, error) {
	_, span := trace.StartSpan(ctx, "QueryBucketLogging")
	defer span.End()

	var be BucketExternal
	res, err := cache.Read(ctx, fmt.Sprintf("ns:extral:%s", bucket), &BucketExternal{}, func() (interface{}, error) {
		return queryBucketExternalInfo(bucket)
	}, 0)

	if res != nil {
		be = *res.(*BucketExternal)
	}
	if err == nil || strings.Contains(err.Error(), "record not found") {
		return be.Log, nil
	}
	return be.Log, error2.BucketLoggingNotFound{Bucket: bucket}
}

func PutBucketLogging(ctx context.Context, bucket, logging string) error {
	_, span := trace.StartSpan(ctx, "PutBucketLogging")
	defer span.End()

	var be BucketExternal
	res, _ := cache.Read(ctx, fmt.Sprintf("ns:extral:%s", bucket), &BucketExternal{}, func() (interface{}, error) {
		return queryBucketExternalInfo(bucket)
	}, 0)

	if res != nil {
		be = *res.(*BucketExternal)
	}
	if be.Name == "" {
		logger.Infof("put bucket logging [%s, %s]", bucket, logging)
		err := cache.Write(ctx, fmt.Sprintf("ns:extral:%s", bucket), logging, func() error {
			return insertBucketLogging(bucket, logging)
		})
		if err != nil {
			logger.Errorf("insert bucket logging failed: ", err)
			return error2.WriteDataBaseFailed{Err: err}
		}
		return nil
	}
	err := cache.Write(ctx, fmt.Sprintf("ns:extral:%s", bucket), logging, func() error {
		return updateBucketLogging(bucket, logging)
	})
	if err != nil {
		logger.Errorf("update bucket logging failed: ", err)
		return error2.WriteDataBaseFailed{Err: err}
	}
	return nil
}

func QueryBucketPolicy(ctx context.Context, bucket string) (string, error) {
	_, span := trace.StartSpan(ctx, "QueryBucketPolicy")
	defer span.End()

	var be BucketExternal
	res, err := cache.Read(ctx, fmt.Sprintf("ns:extral:%s", bucket), &BucketExternal{}, func() (interface{}, error) {
		return queryBucketExternalInfo(bucket)
	}, 0)

	if res != nil {
		be = *res.(*BucketExternal)
	}
	if err == nil || strings.Contains(err.Error(), "record not found") {
		return be.Policy, nil
	}
	return be.Policy, error2.BucketPolicyNotFound{Bucket: bucket}
}

func PutBucketPolicy(ctx context.Context, bucket, policy string) error {
	_, span := trace.StartSpan(ctx, "PutBucketPolicy")
	defer span.End()

	var be BucketExternal
	res, _ := cache.Read(ctx, fmt.Sprintf("ns:extral:%s", bucket), &BucketExternal{}, func() (interface{}, error) {
		return queryBucketExternalInfo(bucket)
	}, 0)

	if res != nil {
		be = *res.(*BucketExternal)
	}
	if be.Name == "" {
		logger.Infof("put bucket policy [%s, %s]", bucket, policy)
		err := cache.Write(ctx, fmt.Sprintf("ns:extral:%s", bucket), policy, func() error {
			return insertBucketPolicy(bucket, policy)
		})
		if err != nil {
			logger.Errorf("insert bucket policy failed: ", err)
			return error2.WriteDataBaseFailed{Err: err}
		}
		return nil
	}
	err := cache.Write(ctx, fmt.Sprintf("ns:extral:%s", bucket), policy, func() error {
		return updateBucketPolicy(bucket, policy)
	})
	if err != nil {
		logger.Errorf("update bucket policy failed: ", err)
		return error2.WriteDataBaseFailed{Err: err}
	}
	return nil
}

func QueryBucketLifecycle(ctx context.Context, bucket string) (string, error) {
	_, span := trace.StartSpan(ctx, "QueryBucketLifecycle")
	defer span.End()

	var be BucketExternal
	res, err := cache.Read(ctx, fmt.Sprintf("ns:extral:%s", bucket), &BucketExternal{}, func() (interface{}, error) {
		return queryBucketExternalInfo(bucket)
	}, 0)

	if res != nil {
		be = *res.(*BucketExternal)
	}
	if err == nil || strings.Contains(err.Error(), "record not found") {
		return be.Lifecycle, nil
	}
	return be.Lifecycle, error2.BucketLifecycleNotFound{Bucket: bucket}
}

func PutBucketLifecycle(ctx context.Context, bucket, lifecycle string) error {
	_, span := trace.StartSpan(ctx, "PutBucketLifecycle")
	defer span.End()

	var be BucketExternal
	res, _ := cache.Read(ctx, fmt.Sprintf("ns:extral:%s", bucket), &BucketExternal{}, func() (interface{}, error) {
		return queryBucketExternalInfo(bucket)
	}, 0)

	if res != nil {
		be = *res.(*BucketExternal)
	}
	if be.Name == "" {
		logger.Infof("put bucket lifecycle [%s, %s]", bucket, lifecycle)
		err := cache.Write(ctx, fmt.Sprintf("ns:extral:%s", bucket), lifecycle, func() error {
			return insertBucketLifecycle(bucket, lifecycle)
		})
		if err != nil {
			logger.Errorf("insert bucket lifecycle failed: ", err)
			return error2.WriteDataBaseFailed{Err: err}
		}
		return nil
	}
	err := cache.Write(ctx, fmt.Sprintf("ns:extral:%s", bucket), lifecycle, func() error {
		return updateBucketLifecycle(bucket, lifecycle)
	})
	if err != nil {
		logger.Errorf("update bucket lifecycle failed: ", err)
		return error2.WriteDataBaseFailed{Err: err}
	}
	return nil
}

func QueryBucketAcl(ctx context.Context, bucket string) (string, error) {
	_, span := trace.StartSpan(ctx, "QueryBucketAcl")
	defer span.End()

	var be BucketExternal
	res, err := cache.Read(ctx, fmt.Sprintf("ns:extral:%s", bucket), &BucketExternal{}, func() (interface{}, error) {
		return queryBucketExternalInfo(bucket)
	}, 0)

	if res != nil {
		be = *res.(*BucketExternal)
	}
	if err == nil || strings.Contains(err.Error(), "record not found") {
		return be.Acl, nil
	}
	return be.Acl, error2.BucketACLNotFound{Bucket: bucket}
}

func PutBucketAcl(ctx context.Context, bucket, acl string) error {
	_, span := trace.StartSpan(ctx, "PutBucketAcl")
	defer span.End()

	var be BucketExternal
	res, _ := cache.Read(ctx, fmt.Sprintf("ns:extral:%s", bucket), &BucketExternal{}, func() (interface{}, error) {
		return queryBucketExternalInfo(bucket)
	}, 0)

	if res != nil {
		be = *res.(*BucketExternal)
	}
	if be.Name == "" {
		logger.Infof("put bucket acl [%s, %s]", bucket, acl)
		err := cache.Write(ctx, fmt.Sprintf("ns:extral:%s", bucket), acl, func() error {
			return insertBucketAcl(bucket, acl)
		})
		if err != nil {
			logger.Errorf("insert bucket acl failed: ", err)
			return error2.WriteDataBaseFailed{Err: err}
		}
		return nil
	}
	err := cache.Write(ctx, fmt.Sprintf("ns:extral:%s", bucket), acl, func() error {
		return updateBucketAcl(bucket, acl)
	})
	if err != nil {
		logger.Errorf("update bucket acl failed: ", err)
		return error2.WriteDataBaseFailed{Err: err}
	}
	return nil
}

func QueryBucketTags(ctx context.Context, bucket string) (string, error) {
	_, span := trace.StartSpan(ctx, "QueryBucketTags")
	defer span.End()

	var be BucketExternal
	res, err := cache.Read(ctx, fmt.Sprintf("ns:extral:%s", bucket), &BucketExternal{}, func() (interface{}, error) {
		return queryBucketExternalInfo(bucket)
	}, 0)

	if res != nil {
		be = *res.(*BucketExternal)
	}
	if err == nil || strings.Contains(err.Error(), "record not found") {
		return be.Tag, nil
	}
	return be.Tag, error2.BucketTaggingNotFound{Bucket: bucket}
}

func PutBucketTags(ctx context.Context, bucket, tags string) error {
	_, span := trace.StartSpan(ctx, "PutBucketTags")
	defer span.End()

	var be BucketExternal
	res, _ := cache.Read(ctx, fmt.Sprintf("ns:extral:%s", bucket), &BucketExternal{}, func() (interface{}, error) {
		return queryBucketExternalInfo(bucket)
	}, 0)

	if res != nil {
		be = *res.(*BucketExternal)
	}
	if be.Name == "" {
		logger.Infof("put bucket tags [%s, %s]", bucket, tags)
		err := cache.Write(ctx, fmt.Sprintf("ns:extral:%s", bucket), tags, func() error {
			return insertBucketTag(bucket, tags)
		})
		if err != nil {
			logger.Errorf("insert bucket tags failed: ", err)
			return error2.WriteDataBaseFailed{Err: err}
		}
		return nil
	}
	err := cache.Write(ctx, fmt.Sprintf("ns:extral:%s", bucket), tags, func() error {
		return updateBucketTag(bucket, tags)
	})
	if err != nil {
		logger.Errorf("update bucket tag failed: ", err)
		return error2.WriteDataBaseFailed{Err: err}
	}
	return nil
}

func GetBucketsLogging() ([]BucketExternal, error) {
	return getBucketsLogging()
}
