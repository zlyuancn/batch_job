package module

import (
	"context"
	"time"

	"github.com/spf13/cast"
	"github.com/zly-app/cache/v2"
	"github.com/zly-app/component/redis"
	"github.com/zly-app/component/sqlx"
	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/log"
	"github.com/zly-app/zapp/pkg/utils"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/model"

	"github.com/zlyuancn/batch_job/client/db"
	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/batch_job_log"
	"github.com/zlyuancn/batch_job/pb"
)

var Job = &jobCli{}

type jobCli struct{}

// 写入或删除停止标记
func (*jobCli) SetStopFlag(ctx context.Context, jobId int, flag model.StopFlag) error {
	key := CacheKey.GetStopFlag(jobId)
	rdb, err := db.GetRedis()
	if err != nil {
		return err
	}
	if flag == model.StopFlag_None {
		err = rdb.Del(ctx, key).Err()
	} else {
		err = rdb.Set(ctx, key, "1", time.Duration(conf.Conf.JobStopFlagTtl)*time.Second).Err()
	}
	return err
}

// 获取停止标记
func (*jobCli) GetStopFlag(ctx context.Context, jobId int) (model.StopFlag, error) {
	key := CacheKey.GetStopFlag(jobId)
	rdb, err := db.GetRedis()
	if err != nil {
		return model.StopFlag_None, err
	}
	v, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return model.StopFlag_None, nil
	}
	if err != nil {
		log.Error(ctx, "GetStopFlag fail.", zap.Error(err))
		return model.StopFlag_None, err
	}
	return model.StopFlag(cast.ToInt(v)), nil
}

// 从redis加载进度, 对于服务突然宕机, 进度是不会写入到db中, 而运行中的任务的实际进度都应该以redis为准
func (j *jobCli) LoadCacheProgress(ctx context.Context, jobId int) (int64, bool, error) {
	key := CacheKey.GetProcessedCount(jobId)
	rdb, err := db.GetRedis()
	if err != nil {
		return 0, false, err
	}
	p, err := rdb.Get(ctx, key).Int64()
	if err == redis.Nil { // redis没有记录数据, 以db数据为准
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	return p, true, nil
}

// 从redis加载错误计数, 对于服务突然宕机, 进度是不会写入到db中, 而运行中的任务的实际进度都应该以redis为准
func (*jobCli) LoadCacheErrCount(ctx context.Context, jobId int) (int64, bool, error) {
	key := CacheKey.GetErrCount(jobId)
	rdb, err := db.GetRedis()
	if err != nil {
		return 0, false, err
	}
	p, err := rdb.Get(ctx, key).Int64()
	if err == redis.Nil { // redis没有记录数据, 以db数据为准
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	return p, true, nil
}

// 增加错误计数到redis中
func (*jobCli) IncrCacheErrCount(ctx context.Context, jobId int, num int64) (int64, error) {
	key := CacheKey.GetErrCount(jobId)
	rdb, err := db.GetRedis()
	if err != nil {
		return 0, err
	}
	p, err := rdb.IncrBy(ctx, key, num).Result()
	return p, err
}

// 获取错误数
func (*jobCli) GetErrCount(ctx context.Context, jobId int) (int64, error) {
	where := map[string]interface{}{
		"job_id":   jobId,
		"log_type": int(pb.DataLogType_DataLogType_ErrorAndAbandon),
	}
	total, err := batch_job_log.Count(ctx, where)
	if err != nil {
		log.Error(ctx, "GetErrCount call batch_job_log.Count", zap.Error(err))
		return 0, err
	}
	return total, nil
}

// 获取任务信息, 使用缓存
func (*jobCli) GetJobInfoByCache(ctx context.Context, jobId uint) (*batch_job_list.Model, error) {
	key := CacheKey.GetJobInfo(int(jobId))
	ret := &batch_job_list.Model{}
	err := cache.GetDefCache().Get(ctx, key, ret, cache.WithLoadFn(func(ctx context.Context, key string) (interface{}, error) {
		v, err := batch_job_list.GetOneByJobId(ctx, jobId)
		if err == sqlx.ErrNoRows {
			return nil, nil
		}
		return v, err
	}), cache.WithExpire(conf.Conf.JobInfoCacheTtl))
	return ret, err
}

// 批量获取任务信息, 使用缓存
func (j *jobCli) BatchGetJobInfoByCache(ctx context.Context, jobId []uint) ([]*batch_job_list.Model, error) {
	// 批量获取数据
	lines, err := utils.GoQuery(jobId, func(id uint) (*batch_job_list.Model, error) {
		line, err := j.GetJobInfoByCache(ctx, id)
		if err != nil {
			log.Error(ctx, "BatchGetJobInfoByCache call GetJobInfoByCache fail.", zap.Uint("id", id), zap.Error(err))
			return nil, err
		}
		return line, nil
	}, true)
	if err != nil {
		log.Error(ctx, "GetJobInfoByCache call query fail.", zap.Error(err))
		return nil, err
	}
	return lines, nil
}

func (j *jobCli) AddDataLog(ctx context.Context, jobId uint, dataLog []*pb.DataLogQ) error {
	if len(dataLog) == 0 {
		return nil
	}

	errNum := int64(0) // 错误计数
	lines := make([]*batch_job_log.Model, len(dataLog))
	for i, a := range dataLog {
		lines[i] = &batch_job_log.Model{
			JobID:     jobId,
			DataIndex: a.GetDataIndex(),
			Remark:    a.GetRemark(),
			Extend:    a.GetExtend(),
			LogType:   byte(a.GetLogType()),
		}
		if a.GetLogType() == pb.DataLogType_DataLogType_ErrorAndAbandon {
			errNum++
		}
	}

	_, err := batch_job_log.MultiSave(ctx, lines)
	if err != nil {
		log.Error(ctx, "AddDataLog call MultiSave fail.", zap.Error(err))
		return err
	}

	// 添加错误日志数
	if errNum > 0 {
		cloneCtx := utils.Ctx.CloneContext(ctx)
		gpool.GetDefGPool().Go(func() error {
			_, err := Job.IncrCacheErrCount(cloneCtx, int(jobId), errNum)
			if err != nil {
				log.Error(cloneCtx, "AddDataLog call IncrCacheErrCount fail.", zap.Error(err))
				// return err
			}
			return nil
		}, nil)
	}
	return nil
}
