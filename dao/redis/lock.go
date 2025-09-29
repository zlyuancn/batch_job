package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/zly-app/component/redis"
	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"
)

var lockErr = errors.New("redis Lock TooMany")

// 加锁
func Lock(ctx context.Context, client redis.UniversalClient, lockKey string, lockTime time.Duration) (unlock func(), err error) {
	startTime := time.Now().UnixNano()
	ok, err := client.SetNX(ctx, lockKey, "1", lockTime).Result()
	if err != nil {
		logger.Error(ctx, "Lock set lock fail.", zap.String("key", lockKey), zap.Error(err))
		return nil, err
	}
	if !ok {
		logger.Error(ctx, "Lock set lock fail. TooMany", zap.String("key", lockKey))
		return nil, lockErr
	}

	oneUnlock := int32(0)
	unlock = func() {
		// 一次性解锁
		if atomic.AddInt32(&oneUnlock, 1) != 1 {
			return
		}

		latency := time.Duration(time.Now().UnixNano() - startTime)
		if latency < lockTime/2 { // 处理时间小于加锁时间的一半则主动解锁
			err := client.Del(ctx, lockKey).Err()
			if err != nil {
				logger.Error(ctx, "Unlock fail.", zap.String("key", lockKey), zap.Error(err))
			}
		}
	}
	return unlock, nil
}
