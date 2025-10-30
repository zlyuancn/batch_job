package redis

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/client/db"
)

var (
	LockManyErr           = errors.New("redis Lock TooMany")              // 加锁太频繁
	LockAuthCodeIsChanged = errors.New("redis Lock Auth Code is Changed") // 授权码已变更
)

// 解锁
type KeyUnlock func() error

// 秘钥ttl更新
type KeyTtlRenew func(ctx context.Context, ttl time.Duration) error

// 加锁, 返回可用于解锁和续期的函数
func AutoLock(ctx context.Context, lockKey string, ttl time.Duration) (unlock KeyUnlock, renew KeyTtlRenew, err error,
) {
	authCode := strconv.FormatInt(time.Now().Unix(), 32) + strconv.FormatInt(rand.Int63n(1e9), 32) // 生成随机授权码
	ok, err := db.GetRedis().SetNX(ctx, lockKey, authCode, ttl).Result()
	if err != nil {
		logger.Error(ctx, "AutoLock set lock fail.", zap.String("key", lockKey), zap.Error(err))
		return nil, nil, err
	}
	if !ok {
		logger.Error(ctx, "AutoLock set lock fail. TooMany", zap.String("key", lockKey))
		return nil, nil, LockManyErr
	}

	oneUnlock := int32(0)
	unlock = func() error {
		// 一次性解锁
		if atomic.AddInt32(&oneUnlock, 1) != 1 {
			return nil
		}

		ok, err := CompareAndDel(ctx, lockKey, authCode)
		if err != nil {
			logger.Error(ctx, "Unlock fail.", zap.String("key", lockKey), zap.Error(err))
			return err
		}
		if !ok {
			err = LockAuthCodeIsChanged
			logger.Error(ctx, "Unlock fail.", zap.String("key", lockKey))
			return err
		}
		return nil
	}
	renew = func(ctx context.Context, ttl time.Duration) error {
		ok, err := CompareAndExpire(ctx, lockKey, authCode, ttl)
		if err != nil {
			logger.Error(ctx, "Renew fail.", zap.String("key", lockKey), zap.Error(err))
			return err
		}
		if !ok {
			err := LockAuthCodeIsChanged
			logger.Error(ctx, "Renew fail.", zap.String("key", lockKey), zap.Error(err))
			return err
		}
		return nil
	}
	return unlock, renew, nil
}

// 加锁, 返回授权码, 授权码用于解锁和续期
func Lock(ctx context.Context, lockKey string, lockTime time.Duration) (string, error) {
	authCode := strconv.FormatInt(time.Now().Unix(), 32) + strconv.FormatInt(rand.Int63n(1e9), 32) // 生成随机授权码
	ok, err := db.GetRedis().SetNX(ctx, lockKey, authCode, lockTime).Result()
	if err != nil {
		logger.Error(ctx, "Lock set lock fail.", zap.String("key", lockKey), zap.Error(err))
		return "", err
	}
	if !ok {
		logger.Error(ctx, "Lock set lock fail. TooMany", zap.String("key", lockKey))
		return "", LockManyErr
	}

	return authCode, nil
}

// 解锁
func UnLock(ctx context.Context, lockKey, authCode string) error {
	ok, err := CompareAndDel(ctx, lockKey, authCode)
	if err != nil {
		logger.Error(ctx, "Unlock fail.", zap.String("key", lockKey), zap.Error(err))
		return err
	}
	if !ok {
		err = LockAuthCodeIsChanged
		logger.Error(ctx, "Unlock fail.", zap.String("key", lockKey))
		return err
	}
	return nil
}

// 续期
func RenewLock(ctx context.Context, lockKey, authCode string, ttl time.Duration) error {
	ok, err := CompareAndExpire(ctx, lockKey, authCode, ttl)
	if err != nil {
		logger.Error(ctx, "RenewLock fail.", zap.String("key", lockKey), zap.Error(err))
		return err
	}
	if !ok {
		err := LockAuthCodeIsChanged
		logger.Error(ctx, "RenewLock fail.", zap.String("key", lockKey), zap.Error(err))
		return err
	}
	return nil
}

// 检查授权码, key不存在也会返回err
func CheckLockAuthCode(ctx context.Context, lockKey, authCode string) error {
	v, err := db.GetRedis().Get(ctx, lockKey).Result()
	if err != nil {
		logger.Error(ctx, "CheckLockAuthCode call Get fail.", zap.Error(err))
		return err
	}
	if authCode != v {
		err = LockAuthCodeIsChanged
		logger.Error(ctx, "CheckLockAuthCode fail.", zap.String("key", lockKey), zap.Error(err))
		return err
	}
	return nil
}
