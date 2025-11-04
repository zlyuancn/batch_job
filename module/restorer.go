package module

import (
	"context"
	"time"

	"github.com/zly-app/cache/v2"
	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/logger"
	"github.com/zly-app/zapp/pkg/utils"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/redis"
	"github.com/zlyuancn/batch_job/pb"
)

// 查询一次活跃任务limit数量
const oneQueryActivateJobLimit = 30

var Restorer = newRestorer()

type restorerCli struct {
}

func newRestorer() *restorerCli {
	return &restorerCli{}
}

func (r *restorerCli) Restorer(ctx context.Context) error {
	// 扫描活跃的任务
	queryTime := time.Now().Add(-time.Duration(conf.Conf.RecoverJobLastActivateDay*24) * time.Hour)
	for {
		// 检查节点速率上限
		if RateLimit.CheckIsMaxRace() {
			logger.Warn(ctx, "node is max race")
			return nil
		}

		// 查询活动任务
		jobInfos, err := batch_job_list.QueryActivateJob(ctx, queryTime, oneQueryActivateJobLimit)
		if err != nil {
			logger.Error(ctx, "Restorer call batch_job_list.QueryActivateJob fail.", zap.Error(err))
			return err
		}

		// 扫描完成
		if len(jobInfos) == 0 {
			return nil
		}

		// 恢复任务, 并设置下一个查询时间
		queryTime, err = r.restorerJob(ctx, jobInfos)
		if err != nil {
			logger.Error(ctx, "Restorer call restorerJob fail.", zap.Error(err))
			return err
		}
	}
}

// 恢复任务
func (r *restorerCli) restorerJob(ctx context.Context, jobInfos []*batch_job_list.Model) (time.Time, error) {
	t := time.Now()
	for i, jobInfo := range jobInfos {
		// 重设活动时间
		if !jobInfo.ActivateTime.IsZero() && (i == 0 || t.Before(jobInfo.ActivateTime)) {
			t = jobInfo.ActivateTime
		}

		// 对于停止中的任务单独处理, 这里不会占用节点速率
		if pb.JobStatus(jobInfo.Status) == pb.JobStatus_JobStatus_Stopping {
			jobId := jobInfo.JobID
			cloneCtx := utils.Ctx.CloneContext(ctx)
			gpool.GetDefGPool().Go(func() error {
				r.restorerJobStopping(cloneCtx, jobId)
				return nil
			}, nil)
			continue
		}

		// 速率检查
		if !RateLimit.TryRunJobCheckRate(int32(jobInfo.RateSec)) {
			continue
		}

		// 获取真实任务信息
		realJobInfo, err := batch_job_list.GetOneByJobId(ctx, jobInfo.JobID)
		if err != nil {
			logger.Error(ctx, "restorerJobStopping call batch_job_list.GetOneByJobId fail.", zap.Error(err))
			return t, err
		}

		// 获取真实业务信息
		realBizInfo, err := batch_job_biz.GetOneByBizId(ctx, int(realJobInfo.BizId))
		if err != nil {
			logger.Error(ctx, "restorerJobStopping call batch_job_biz.GetOneByBizId fail.", zap.Error(err))
			return t, err
		}

		// 恢复任务
		Job.CreateLauncherByData(ctx, realBizInfo, realJobInfo)
	}
	return t, nil
}

// 扭转任务的 Stopping 状态
func (r *restorerCli) restorerJobStopping(ctx context.Context, jobId uint) {
	// 尝试获取运行锁
	runLockKey := CacheKey.GetRunLockKey(int(jobId))
	unlock, _, err := redis.AutoLock(ctx, runLockKey, time.Duration(conf.Conf.JobRunLockExtraTtl)*time.Second)
	if err == redis.LockManyErr { // 有别的线程处理
		return
	}
	if err != nil { // 加锁异常
		logger.Error(ctx, "restorerJobStopping call set run lock fail.", zap.Error(err))
		return
	}
	defer unlock()

	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, jobId)
	if err != nil {
		logger.Error(ctx, "restorerJobStopping call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return
	}

	// 更新任务状态
	updateData := map[string]interface{}{
		"status":      pb.JobStatus_JobStatus_Stopped,
		"status_info": "restorerJobStopping",
	}
	err = batch_job_list.UpdateOne(ctx, int(jobId), updateData, byte(pb.JobStatus_JobStatus_Stopping))
	if err != nil {
		logger.Error(ctx, "stopSideEffect call UpdateOne fail.", zap.Error(err))
		return
	}
	// 清除缓存
	err = cache.GetDefCache().Del(ctx, CacheKey.GetJobInfo(int(jobId)))
	if err != nil {
		logger.Error(ctx, "stopSideEffect call clear Cache fail.", zap.Error(err))
		// return err
	}

	// 获取业务信息
	bizInfo, err := Biz.GetBizInfoByCache(ctx, int(jobInfo.BizId))
	if err != nil {
		logger.Error(ctx, "restorerJobStopping call GetBizInfoByCache fail.", zap.Error(err))
		return
	}

	// 获取业务
	b, err := Biz.GetBiz(ctx, bizInfo)
	if err != nil {
		logger.Error(ctx, "restorerJobStopping call biz.GetBiz fail.", zap.Error(err))
		return
	}
	// 回调
	err = b.ProcessStop(ctx, jobInfo, false)
	if err != nil {
		logger.Error(ctx, "restorerJobStopping call ProcessStop fail.", zap.Error(err))
		return
	}
}
