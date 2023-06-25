package cache

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/pkg/logger"
)

type RedisHandler struct {
	Cli            *redis.Client
	NsCache        *cache.Cache
	NsCacheEnabled bool
}

var globalRedisHandler = &RedisHandler{}

func Init(addr, pwd string) {
	if addr == "" {
		logger.Warn("Cache NOT enabled.")
		return
	}

	globalRedisHandler.Cli = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pwd,
		DB:       0,
	})
	if err := globalRedisHandler.Cli.Ping(context.TODO()).Err(); err != nil {
		return
	}
	globalRedisHandler.NsCache = cache.New(&cache.Options{
		Redis: globalRedisHandler.Cli,
	})

	globalRedisHandler.NsCacheEnabled = false
	logger.Info("Cache enabled.")
}

type readCallback func() (interface{}, error)
type writeCallback func() error

func Read(ctx context.Context, key string, val interface{}, call readCallback, ttl time.Duration) (interface{}, error) {
	ctx, span := trace.StartSpan(ctx, "Read")
	defer span.End()

	if !globalRedisHandler.NsCacheEnabled {
		return call()
	}

	cli := globalRedisHandler.NsCache
	for i := 0; i < 3; i++ {
		err := cli.Get(ctx, key, val)
		if err != nil {
			logger.Error("cache read: ", err)
		} else {
			logger.Infof("cache read %s success", key)
			return val, nil
		}

		// run callback
		logger.Infof("cache read [%s] missed, run callback", key)
		if call != nil {
			res, err := call()
			if err != nil {
				if strings.Contains(key, "b4991bb7207fbba30511ab3e270db21b") {
					fmt.Println("copyObject===>", err.Error())
				}
				logger.Error("cache read callback:", key, err)
				return nil, err
			}

			if ttl == 0 {
				ttl = 30 * time.Minute
			}
			err = cli.Set(&cache.Item{
				Ctx:   ctx,
				Key:   key,
				Value: &res,
				TTL:   ttl,
			})
			if err != nil {
				logger.Error("cache read:", key, res, err)
				return nil, err
			}
			continue
		}
		break
	}
	return nil, nil
}

func Write(ctx context.Context, key string, val interface{}, call writeCallback) error {
	ctx, span := trace.StartSpan(ctx, "Write")
	ttl := 30 * time.Minute
	defer span.End()

	if !globalRedisHandler.NsCacheEnabled {
		return call()
	}

	cli := globalRedisHandler.NsCache
	err := cli.Set(&cache.Item{
		Ctx:   ctx,
		Key:   key,
		Value: &val,
		TTL:   ttl,
	})
	if err != nil {
		logger.Error("cache write:", key, err, val)
	}

	err = Delete(ctx, key)
	if err != nil {
		logger.Error("cache write:", key, err)
	}

	// run callback here
	if call != nil {
		err = call()
		if err != nil {
			logger.Error("cache write callback:", err)
			return err
		}
	}

	// delete again, we implement it like optimistic lock
	time.Sleep(2 * time.Microsecond)
	err = Delete(ctx, key)
	if err != nil {
		logger.Error("cache write:", key, err)
		return err
	}

	return nil
}

func Delete(ctx context.Context, key string) error {
	ctx, span := trace.StartSpan(ctx, "Delete")
	defer span.End()

	if !globalRedisHandler.NsCacheEnabled {
		return nil
	}

	cli := globalRedisHandler.NsCache
	err := cli.Delete(ctx, key)
	if err != nil {
		logger.Error("cache delete:", err)
	}
	logger.Infof("cache delete:%s success", key)

	return err
}

func GetBucketObjectCountKey(bucketName string) string {
	return fmt.Sprintf("ns:%s:count", bucketName)
}

func IncrBucketObjectCount(ctx context.Context, bucketName string) *redis.IntCmd {
	return globalRedisHandler.Cli.Incr(ctx, GetBucketObjectCountKey(bucketName))
}

func IncrByBucketObjectCount(ctx context.Context, bucketName string, incr int64) *redis.IntCmd {
	return globalRedisHandler.Cli.IncrBy(ctx, GetBucketObjectCountKey(bucketName), incr)
}
