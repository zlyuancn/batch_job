package module

import (
	"context"
	"time"

	"github.com/zly-app/cache/v2"
	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/log"
	"github.com/zly-app/zapp/pkg/utils"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/redis"
	"github.com/zlyuancn/batch_job/handler"
	"github.com/zlyuancn/batch_job/model"
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
	if conf.Conf.DoNotRunJob {
		log.Warn(ctx, "DoNotRunJob")
		return nil
	}

	// 扫描活跃的任务
	queryTime := time.Now().Add(-time.Duration(conf.Conf.RecoverJobLastActivateDay*24) * time.Hour)
	for {
		// 检查节点速率上限
		if RateLimit.CheckIsMaxRace() {
			log.Warn(ctx, "node is max race")
			return nil
		}

		// 查询活动任务
		jobInfos, err := batch_job_list.QueryActivateJob(ctx, queryTime, oneQueryActivateJobLimit)
		if err != nil {
			log.Error(ctx, "Restorer call batch_job_list.QueryActivateJob fail.", zap.Error(err))
			return err
		}

		// 扫描完成
		if len(jobInfos) == 0 {
			return nil
		}

		// 恢复任务, 并设置下一个查询时间
		queryTime, err = r.restorerJob(ctx, jobInfos)
		if err != nil {
			log.Error(ctx, "Restorer call restorerJob fail.", zap.Error(err))
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

		// 获取停止标记
		stopFlag, err := Job.GetStopFlag(ctx, int(jobInfo.JobID))
		if err != nil {
			log.Error(ctx, "restorerJob call GetStopFlag fail.", zap.Uint("jobId", jobInfo.JobID), zap.Error(err))
		}
		if stopFlag {
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

		// 恢复任务
		err = r.restorerJobRunning(ctx, jobInfo.JobID, jobInfo.Status)
		if err != nil {
			log.Error(ctx, "restorerJob call restorerJobRunning fail.", zap.Uint("jobId", jobInfo.JobID), zap.Error(err))
			return t, err
		}

		// 检查节点速率上限
		if RateLimit.CheckIsMaxRace() {
			log.Warn(ctx, "node is max race")
			return t, nil
		}
	}
	return t, nil
}

func (r *restorerCli) restorerJobRunning(ctx context.Context, jobId uint, status byte) error {
	isRecovered := false

	// 尝试获取运行锁
	runLockKey := CacheKey.GetRunLockKey(int(jobId))
	authCode, err := redis.Lock(ctx, runLockKey, time.Duration(conf.Conf.JobRunLockExtraTtl)*time.Second)
	if err == redis.LockManyErr { // 有别的线程处理
		return nil
	}
	if err != nil { // 加锁异常
		log.Error(ctx, "restorerJobRunning call set run lock fail.", zap.Uint("jobId", jobId), zap.Error(err))
		return err
	}
	defer func() {
		// 如果没有恢复则解锁
		if !isRecovered {
			_ = redis.UnLock(ctx, runLockKey, authCode)
		}
	}()

	// 更新任务信息
	err = batch_job_list.UpdateOne(ctx, int(jobId), map[string]interface{}{
		"status_info": model.StatusInfo_RestorerJob,
	}, status)
	if err != nil {
		log.Error(ctx, "restorerJobRunning call UpdateOne fail.", zap.Uint("jobId", jobId), zap.Error(err))
		return err
	}

	// 清除缓存
	err = cache.GetDefCache().Del(ctx, CacheKey.GetJobInfo(int(jobId)))
	if err != nil {
		log.Error(ctx, "restorerJobRunning call clear Cache fail.", zap.Uint("jobId", jobId), zap.Error(err))
		// return err
	}

	// 获取真实任务信息
	realJobInfo, err := batch_job_list.GetOneByJobId(ctx, jobId)
	if err != nil {
		log.Error(ctx, "restorerJobRunning call batch_job_list.GetOneByJobId fail.", zap.Uint("jobId", jobId), zap.Error(err))
		return err
	}

	handler.Trigger(ctx, handler.JobRestorer, realJobInfo)

	// 恢复任务
	Job.CreateLauncherByRestorer(ctx, realJobInfo, authCode)
	isRecovered = true
	return nil
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
		log.Error(ctx, "restorerJobStopping call set run lock fail.", zap.Uint("jobId", jobId), zap.Error(err))
		return
	}
	defer unlock()

	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, jobId)
	if err != nil {
		log.Error(ctx, "restorerJobStopping call batch_job_list.GetOneByJobId fail.", zap.Uint("jobId", jobId), zap.Error(err))
		return
	}

	// 更新任务状态
	updateData := map[string]interface{}{
		"status":      pb.JobStatus_JobStatus_Stopped,
		"status_info": "restorerJobStopping",
	}
	err = batch_job_list.UpdateOne(ctx, int(jobId), updateData, byte(pb.JobStatus_JobStatus_Stopping))
	if err != nil {
		log.Error(ctx, "stopSideEffect call UpdateOne fail.", zap.Uint("jobId", jobId), zap.Error(err))
		return
	}
	jobInfo.Status = byte(pb.JobStatus_JobStatus_Stopped)
	jobInfo.StatusInfo = "restorerJobStopping"

	handler.Trigger(ctx, handler.AfterJobStopped, jobInfo)

	// 清除缓存
	err = cache.GetDefCache().Del(ctx, CacheKey.GetJobInfo(int(jobId)))
	if err != nil {
		log.Error(ctx, "stopSideEffect call clear Cache fail.", zap.Uint("jobId", jobId), zap.Error(err))
		// return err
	}

	// 获取业务信息
	bizInfo, err := Biz.GetBizInfoByCache(ctx, int(jobInfo.BizId))
	if err != nil {
		log.Error(ctx, "restorerJobStopping call GetBizInfoByCache fail.", zap.Uint("jobId", jobId), zap.Error(err))
		return
	}

	// 获取业务
	b, err := Biz.GetBiz(ctx, bizInfo)
	if err != nil {
		log.Error(ctx, "restorerJobStopping call biz.GetBiz fail.", zap.Uint("jobId", jobId), zap.Error(err))
		return
	}

	// 回调
	err = b.ProcessStop(ctx, jobInfo, false)
	if err != nil {
		log.Error(ctx, "restorerJobStopping call ProcessStop fail.", zap.Uint("jobId", jobId), zap.Error(err))
		return
	}
}
