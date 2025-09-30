package module

import (
	"context"
	"strconv"
	"time"

	"github.com/zly-app/zapp/logger"
	"github.com/zly-app/zapp/pkg/utils"
	"github.com/zlyuancn/sliding_window"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/batch_job_type"
	"github.com/zlyuancn/batch_job/dao/redis"
	"github.com/zlyuancn/batch_job/pb"
)

// 创建启动器
func (j *jobCli) CreateLauncherByData(ctx context.Context, bizInfo *batch_job_type.Model, jobInfo *batch_job_list.Model) {
	ctx = utils.Ctx.CloneContext(ctx)
	go j.createLauncher(ctx, bizInfo, jobInfo)
}

func (*jobCli) createLauncher(ctx context.Context, bizInfo *batch_job_type.Model, jobInfo *batch_job_list.Model) {
	// 获取业务
	b, err := GetBizByDbModel(ctx, bizInfo)
	if err != nil {
		logger.Error("createLauncher call GetBizByDbModel fail.", zap.Error(err))
		return
	}

	// 如果有启动前回调. 则交给业务处理
	if b.HasBeforeRunCallback() {
		ttl := time.Duration(int(bizInfo.CbBeforeRunTimeout)+conf.Conf.JobBeforeRunLockExtraTtl) * time.Second
		lockKey := conf.Conf.JobBeforeRunLockKeyPrefix + strconv.Itoa(int(jobInfo.JobID))
		unlock, _, err := redis.Lock(ctx, lockKey, ttl)
		if err == redis.LockManyErr { // 加锁失败
			return
		}
		if err != nil { // 加锁异常
			logger.Error("createLauncher call Lock fail.", zap.Error(err))
			return
		}
		defer unlock()

		b.BeforeRun(ctx, bizInfo, jobInfo)
		return
	}

	// 由启动器启动任务
	lockKey := conf.Conf.JobRunLockKeyPrefix + strconv.Itoa(int(jobInfo.JobID))
	unlock, renew, err := redis.Lock(ctx, lockKey, time.Duration(conf.Conf.JobRunLockExtraTtl)*time.Second)
	if err == redis.LockManyErr { // 加锁失败
		return
	}
	if err != nil { // 加锁异常
		logger.Error("createLauncher call Lock fail.", zap.Error(err))
		return
	}

	jl := newJobLauncher(bizInfo, jobInfo, unlock, renew)
	jl.Run()
}

type jobLauncher struct {
	ctx    context.Context
	cancel context.CancelFunc

	bizInfo       *batch_job_type.Model
	jobInfo       *batch_job_list.Model
	lockKeyUnlock redis.KeyUnlock   // 任务运行锁解锁方法
	lockKeyRenew  redis.KeyTtlRenew // 任务运行锁续期方法

	threadLock chan struct{}                 // 线程锁
	limiter    *rate.Limiter                 // 限速器
	sw         *sliding_window.SlidingWindow // 滑动窗口. 注意. 数据编号从0开始
}

func newJobLauncher(bizInfo *batch_job_type.Model, jobInfo *batch_job_list.Model,
	unlock redis.KeyUnlock, renew redis.KeyTtlRenew) *jobLauncher {

	j := &jobLauncher{
		bizInfo:       bizInfo,
		jobInfo:       jobInfo,
		lockKeyUnlock: unlock,
		lockKeyRenew:  renew,
	}
	j.ctx, j.cancel = context.WithCancel(context.Background())

	switch pb.RateType(bizInfo.RateType) {
	case pb.RateType_Serialization: // 串行化
		j.threadLock = make(chan struct{}, 1) // 只能用一个线程
	default:
		j.threadLock = make(chan struct{}, conf.Conf.JobRunThreadCount) // 可以使用多个线程
	}

	if bizInfo.RateSec > 0 { // 限速
		j.limiter = rate.NewLimiter(rate.Limit(bizInfo.RateSec), int(bizInfo.RateSec)) // 每秒限速
	}

	// 滑动窗口
	j.sw = sliding_window.NewSlidingWindow(conf.Conf.JobSlidingWindowSize, int64(jobInfo.ProcessedCount))

	return j
}

// todo 循环对运行锁续期
func (j *jobLauncher) loopLockKeyRenew() {
}

// todo 循环写入当前进度和错误数到redis
func (j *jobLauncher) loopWriteProgress() {
}

// todo 循环检查停止flag
func (j *jobLauncher) loopCheckStopFlag() {

}

func (j *jobLauncher) Run() {
	go j.loopLockKeyRenew()  // 循环续期
	go j.loopWriteProgress() // 循环写入进度
	go j.loopCheckStopFlag() // 循环检查停止flag

	for {
		// 占用一个线程
		j.threadLock <- struct{}{}
		sn, err := j.sw.Next(j.ctx)
		if err != nil {
			logger.Error("job Run get sn fail.", zap.Error(err))
			break
		}

		// todo 检查进度是否达标

		// todo 限速器
		if j.limiter != nil {
			err := j.limiter.Wait(j.ctx)
			if err != nil {
				logger.Error("job Run call limiter.Wait fail.", zap.Error(err))
				break
			}
		}

		// todo 检查停止 flag

		// todo 执行一个数据, 注意重试
		go func(sn int64) {
			defer func() {
				<-j.threadLock // 释放一个线程
			}()
			// todo 业务调用处理回调. 注意错误累计检查
			// 滑动窗口确认当前数据已完成
			j.sw.Ack(sn)
		}(sn)
	}

	// todo 停止时更新进度数据
	// todo 停止时业务调用停止回调

}
