package redis

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/client/db"
)

var LockManyErr = errors.New("redis Lock TooMany")

// 解锁
type KeyUnlock func() error

// 秘钥ttl更新
type KeyTtlRenew func(ctx context.Context, ttl time.Duration) error

// 加锁
func Lock(ctx context.Context, lockKey string, lockTime time.Duration) (unlock KeyUnlock, renew KeyTtlRenew, err error,
) {
	value := strconv.Itoa(int(time.Now().UnixNano()))
	ok, err := db.GetRedis().SetNX(ctx, lockKey, value, lockTime).Result()
	if err != nil {
		logger.Error(ctx, "Lock set lock fail.", zap.String("key", lockKey), zap.Error(err))
		return nil, nil, err
	}
	if !ok {
		logger.Error(ctx, "Lock set lock fail. TooMany", zap.String("key", lockKey))
		return nil, nil, LockManyErr
	}

	oneUnlock := int32(0)
	unlock = func() error {
		// 一次性解锁
		if atomic.AddInt32(&oneUnlock, 1) != 1 {
			return nil
		}

		ok, err := CompareAndDel(ctx, lockKey, value)
		if err != nil {
			logger.Error(ctx, "Unlock fail.", zap.String("key", lockKey), zap.Error(err))
			return err
		}
		if !ok {
			err = errors.New("The possible value has changed")
			logger.Error(ctx, "Unlock fail.", zap.String("key", lockKey), zap.String("ExpectedValue", value))
			return err
		}
		return nil
	}
	renew = func(ctx context.Context, ttl time.Duration) error {
		ok, err := CompareAndExpire(ctx, lockKey, value, ttl)
		if err != nil {
			logger.Error(ctx, "Renew fail.", zap.String("key", lockKey), zap.Error(err))
			return err
		}
		if !ok {
			err := errors.New("The possible value has changed")
			logger.Error(ctx, "Renew fail.", zap.String("key", lockKey), zap.String("ExpectedValue", value), zap.Error(err))
			return err
		}
		return nil
	}
	return unlock, renew, nil
}
