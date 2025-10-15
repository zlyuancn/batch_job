package module

import (
	"context"
	"strconv"
	"time"

	"github.com/zly-app/component/redis"
	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/client/db"
	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_log"
	"github.com/zlyuancn/batch_job/pb"
)

var Job = &jobCli{}

type jobCli struct{}

// 写入停止标记
func (*jobCli) SetStopFlag(ctx context.Context, jobId int, flag bool) error {
	key := conf.Conf.JobStopFlagPrefix + strconv.Itoa(jobId)
	var err error
	if flag {
		err = db.GetRedis().Set(ctx, key, "1", time.Duration(conf.Conf.JobStopFlagTtl)*time.Second).Err()
	} else {
		err = db.GetRedis().Del(ctx, key).Err()
	}
	return err
}

// 获取停止标记
func (*jobCli) GetStopFlag(ctx context.Context, jobId int) (bool, error) {
	key := conf.Conf.JobStopFlagPrefix + strconv.Itoa(jobId)
	v, err := db.GetRedis().Get(ctx, key).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		logger.Error(ctx, "GetStopFlag fail.", zap.Error(err))
		return false, err
	}
	return v == "1", nil
}

// 从redis加载进度, 对于服务突然宕机, 进度是不会写入到db中, 而运行中的任务的实际进度都应该以redis为准
func (j *jobCli) LoadCacheProgress(ctx context.Context, jobId int) (int64, bool, error) {
	key := j.GenProgressCacheKey(jobId)
	p, err := db.GetRedis().Get(ctx, key).Int64()
	if err == redis.Nil { // redis没有记录数据, 以db数据为准
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	return p, true, nil
}

// 从redis加载错误计数, 对于服务突然宕机, 进度是不会写入到db中, 而运行中的任务的实际进度都应该以redis为准
func (j *jobCli) LoadCacheErrCount(ctx context.Context, jobId int) (int64, bool, error) {
	key := j.GenErrCountCacheKey(jobId)
	p, err := db.GetRedis().Get(ctx, key).Int64()
	if err == redis.Nil { // redis没有记录数据, 以db数据为准
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	return p, true, nil
}

// 获取错误数
func (*jobCli) GetErrCount(ctx context.Context, jobId int) (int64, error) {
	where := map[string]interface{}{
		"job_id":   jobId,
		"log_type": int(pb.DataLogType_DataLogType_ErrData),
	}
	total, err := batch_job_log.Count(ctx, where)
	if err != nil {
		logger.Error(ctx, "GetErrCount call batch_job_log.Count", zap.Error(err))
		return 0, err
	}
	return total, nil
}

// 生成进度缓存key
func (*jobCli) GenProgressCacheKey(jobId int) string {
	return conf.Conf.JobProcessedCountKeyPrefix + strconv.Itoa(jobId)
}

// 生成错误数缓存key
func (*jobCli) GenErrCountCacheKey(jobId int) string {
	return conf.Conf.JobErrLogCountKeyPrefix + strconv.Itoa(jobId)
}
