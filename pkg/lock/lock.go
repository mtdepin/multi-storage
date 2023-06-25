package lock

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"mtcloud.com/mtstorage/pkg/logger"
)

type RedisHandler struct {
	Cli     *redis.Client
	delScpt *redis.Script
}

var GlobalRedisHandler = &RedisHandler{}

func Init(addr, pwd string) {
	GlobalRedisHandler.Cli = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pwd,
		DB:       0,
	})
	GlobalRedisHandler.delScpt = redis.NewScript(`local v = redis.call("get",KEYS[1])
		if v == ARGV[1] then
			return redis.call("del",KEYS[1])
		else
			return v
		end`)
}

func Lock(ctx context.Context, locker, value string) (bool, error) {

	ttl := 2 * time.Second

	cli := GlobalRedisHandler.Cli
	for {
		res, err := cli.SetNX(ctx, "locker:"+locker, value, ttl).Result()
		if err != nil {
			logger.Errorf("got locker failed:", err)
			return res, err
		}

		if res {
			logger.Infof("got locker: %s, %s.", locker, value)
			return res, err
		}

		//time.Sleep(time.Millisecond * 1)
	}
}

func Unlock(ctx context.Context, locker, value string) (bool, error) {

	cli := GlobalRedisHandler.Cli

	sha, err := GlobalRedisHandler.delScpt.Load(ctx, cli).Result()
	if err != nil {
		logger.Errorf("unlock failed:", err)
		return false, err
	}

	res, err := cli.EvalSha(ctx, sha, []string{"locker:" + locker}, value).Result()
	if err == nil {
		switch res.(int64) {
		case 1:
			logger.Infof("unlocked. locker:%s, %s.", locker, value)
			return true, nil
		case 0:
			logger.Errorf("unlock failed. locker:%s, %s.", locker, value)
			return false, nil
		default:
			logger.Warnf("not held by me. locker:%s, %d.", locker, res)
			return true, nil
		}
	}

	logger.Error("unlock failed:", err)
	return false, err
}
