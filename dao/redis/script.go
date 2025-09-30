package redis

import (
	"context"
	"time"

	"github.com/zly-app/zapp"
	"github.com/zly-app/zapp/core"
	"github.com/zly-app/zapp/handler"
	"github.com/zly-app/zapp/logger"
	"github.com/zly-app/zapp/pkg/utils"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/client/db"
)

var (
	redisLua_CAS_sha1 = ""
	redisLua_CAD_sha1 = ""
	redisLua_CAE_sha1 = ""
)

const (
	// 原子交换, 如果key的值等于v1, 则设为v2; KEYS=[key] ARGV=[v1, v2]; 成功返回1
	redisLua_CAS = `
local v = redis.call("get", KEYS[1])

if (v == ARGV[1]) then
    redis.call("set", KEYS[1], ARGV[2])
    return 1
end

return 0
`
	// 原子删除, 如果key的值等于value则删除, 如果删除成功或者key不存在则返回1; KEYS=[key] ARGV=[value]
	redisLua_CAD = `
local v = redis.call("get", KEYS[1])

if (v == ARGV[1]) then
    redis.call("del", KEYS[1])
    return 1
end
if (v == false) then
    return 1
end

return 0
`
	// 原子续期, 如果key的值等于value则续期, 续期成功返回1; KEYS=[key] ARGV=[value, ttl(秒)]
	redisLua_CAE = `
local v = redis.call("get", KEYS[1])

if (v == ARGV[1]) then
    redis.call("expire", KEYS[1], tonumber(ARGV[2]))
    return 1
end

return 0
`
)

func init() {
	zapp.AddHandler(zapp.AfterInitializeHandler, func(app core.IApp, handlerType handler.HandlerType) {
		tryInjectScript()
	})
}

// 尝试注入脚本
func tryInjectScript() {
	ctx := utils.Otel.CtxStart(context.Background(), "TryInjectScript")
	defer utils.Otel.CtxEnd(ctx)

	sha1, err := db.GetRedis().ScriptLoad(ctx, redisLua_CAS).Result()
	if err != nil {
		logger.Error(ctx, "TryInjectScript redisLua_CAS fail", zap.Error(err))
		return
	}
	redisLua_CAS_sha1 = sha1

	sha1, err = db.GetRedis().ScriptLoad(ctx, redisLua_CAD).Result()
	if err != nil {
		logger.Error(ctx, "TryInjectScript redisLua_CAD fail", zap.Error(err))
		return
	}
	redisLua_CAD_sha1 = sha1

	sha1, err = db.GetRedis().ScriptLoad(ctx, redisLua_CAE).Result()
	if err != nil {
		logger.Error(ctx, "TryInjectScript redisLua_CAE fail", zap.Error(err))
		return
	}
	redisLua_CAE_sha1 = sha1

	logger.Info(ctx, "TryInjectScript ok")
}

// 原子交换, 如果key的值等于v1, 则设为v2, 成功返回 true
func CompareAndSwap(ctx context.Context, key, v1, v2 string) (bool, error) {
	if redisLua_CAS_sha1 != "" {
		v, err := db.GetRedis().EvalSha(ctx, redisLua_CAS_sha1, []string{key}, v1, v2).Result()
		if err != nil {
			return false, err
		}
		return v == "1", nil
	}

	v, err := db.GetRedis().Eval(ctx, redisLua_CAS, []string{key}, v1, v2).Result()
	if err != nil {
		return false, err
	}
	return v == "1", nil
}

// 原子删除, 如果key的值等于v1则删除, 如果删除成功或者key不存在则返回 true
func CompareAndDel(ctx context.Context, key, value string) (bool, error) {
	if redisLua_CAD_sha1 != "" {
		v, err := db.GetRedis().EvalSha(ctx, redisLua_CAD_sha1, []string{key}, value).Result()
		if err != nil {
			return false, err
		}
		return v == "1", nil
	}

	v, err := db.GetRedis().Eval(ctx, redisLua_CAD, []string{key}, value).Result()
	if err != nil {
		return false, err
	}
	return v == "1", nil
}

// 原子续期, 如果key的值等于value则续期, 续期成功返回1; 参数顺序 key value ttl
func CompareAndExpire(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if redisLua_CAE_sha1 != "" {
		v, err := db.GetRedis().EvalSha(ctx, redisLua_CAE_sha1, []string{key}, value, int(ttl/time.Second)).Result()
		if err != nil {
			return false, err
		}
		return v == "1", nil
	}

	v, err := db.GetRedis().Eval(ctx, redisLua_CAE, []string{key}, value, int(ttl/time.Second)).Result()
	if err != nil {
		return false, err
	}
	return v == "1", nil
}
