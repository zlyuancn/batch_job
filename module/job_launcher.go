package module

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/zly-app/zapp/logger"
	"github.com/zly-app/zapp/pkg/utils"
	"github.com/zlyuancn/sliding_window"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/redis"
	"github.com/zlyuancn/batch_job/pb"
)

// 创建启动器
func (j *jobCli) CreateLauncherByData(ctx context.Context, bizInfo *batch_job_biz.Model, jobInfo *batch_job_list.Model) {
	ctx = utils.Ctx.CloneContext(ctx)
	go j.createLauncher(ctx, bizInfo, jobInfo)
}

func (*jobCli) createLauncher(ctx context.Context, bizInfo *batch_job_biz.Model, jobInfo *batch_job_list.Model) {
	// 获取业务
	b, err := Biz.GetBizByDbModel(ctx, bizInfo)
	if err != nil {
		logger.Error("createLauncher call GetBizByDbModel fail.", zap.Error(err))
		return
	}

	// 如果有启动前回调. 则交给业务处理
	if b.HasBeforeRunCallback() {
		// 加等待业务主动启动锁, 这个时间比较长, 防止自动扫描运行中的任务时其它线程抢锁启动
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

		cloneCtx := utils.Ctx.CloneContext(ctx) // 不超时
		b.BeforeRun(cloneCtx, jobInfo)
		return
	}

	// 由启动器启动任务
	// 加任务运行锁, 防止自动扫描运行中的任务时其它线程抢锁启动, 下一步流程中会定时对锁续期
	lockKey := conf.Conf.JobRunLockKeyPrefix + strconv.Itoa(int(jobInfo.JobID))
	unlock, renew, err := redis.Lock(ctx, lockKey, time.Duration(conf.Conf.JobRunLockExtraTtl)*time.Second)
	if err == redis.LockManyErr { // 加锁失败
		return
	}
	if err != nil { // 加锁异常
		logger.Error("createLauncher call Lock fail.", zap.Error(err))
		return
	}

	jl, err := newJobLauncher(b, bizInfo, jobInfo, unlock, renew)
	if err != nil {
		logger.Error("createLauncher call newJobLauncher fail.", zap.Error(err))
		return
	}
	jl.Run()
}

type jobLauncher struct {
	ctx    context.Context
	cancel context.CancelFunc
	b      Business

	bizInfo       *batch_job_biz.Model
	jobInfo       *batch_job_list.Model
	lockKeyUnlock redis.KeyUnlock   // 任务运行锁解锁方法
	lockKeyRenew  redis.KeyTtlRenew // 任务运行锁续期方法

	errLogCount int64 // 错误日志数缓存

	threadLock chan struct{}                 // 线程锁
	limiter    *rate.Limiter                 // 限速器
	errLimiter *rate.Limiter                 // 错误限速器
	sw         *sliding_window.SlidingWindow // 滑动窗口. 注意. 数据编号从0开始

	stopChan chan struct{} // 停止信号
	onceStop int32         // 只调用一次stop
}

func newJobLauncher(b Business, bizInfo *batch_job_biz.Model, jobInfo *batch_job_list.Model,
	unlock redis.KeyUnlock, renew redis.KeyTtlRenew) (*jobLauncher, error) {

	j := &jobLauncher{
		b:             b,
		bizInfo:       bizInfo,
		jobInfo:       jobInfo,
		lockKeyUnlock: unlock,
		lockKeyRenew:  renew,

		errLimiter: rate.NewLimiter(rate.Limit(conf.Conf.JobProcessCumulativeErrorRate), conf.Conf.JobProcessCumulativeErrorRate),

		stopChan: make(chan struct{}),
	}
	j.ctx, j.cancel = context.WithCancel(context.Background())

	switch pb.RateType(jobInfo.RateType) {
	case pb.RateType_RateType_RateSec: // 可以使用多个线程
		j.threadLock = make(chan struct{}, conf.Conf.JobRunThreadCount) // 可以使用多个线程
	case pb.RateType_RateType_Serialization: // 串行化
		j.threadLock = make(chan struct{}, 1) // 只能用一个线程
	default:
		return nil, fmt.Errorf("rateType %d nonsupport", int(jobInfo.RateType))
	}

	if jobInfo.RateSec > 0 { // 限速
		j.limiter = rate.NewLimiter(rate.Limit(jobInfo.RateSec), int(jobInfo.RateSec)) // 每秒限速
	}

	// 滑动窗口
	j.sw = sliding_window.NewSlidingWindow(conf.Conf.JobSlidingWindowSize, int64(jobInfo.ProcessedCount))

	return j, nil
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

// todo 从redis加载进度和错误数, 对于服务突然宕机, 进度和错误数是不会写入到db中, 而运行中的任务的实际进度和错误数都应该以redis为准
func (j *jobLauncher) loadProgress() {

}

func (j *jobLauncher) Run() {
	j.loadProgress()

	go j.loopLockKeyRenew()  // 循环续期
	go j.loopWriteProgress() // 循环写入进度和错误数
	go j.loopCheckStopFlag() // 循环检查停止flag

	defer func() {
		j.submitStopFlag()
		j.stopSideEffect() // 处理停止后副作用
	}()

	maxDataSn := int64(j.jobInfo.ProcessDataTotal - 1)
	for {
		// 占用一个线程
		select {
		case j.threadLock <- struct{}{}:
		case <-j.stopChan:
			return
		}

		// 检查进度是否达标
		if j.sw.GetProgress() >= maxDataSn {
			return
		}

		// 限速器
		if j.limiter != nil {
			err := j.limiter.Wait(j.ctx)
			if err == context.Canceled { // 已停止直接退出
				return
			}
			if err != nil {
				logger.Error("job Run call limiter.Wait fail.", zap.Error(err))
				return
			}
		}

		// 获取下一个要处理的数据编号
		sn, err := j.sw.Next(j.ctx)
		if err == sliding_window.ErrIsStop || err == context.Canceled { // 已停止直接退出
			return
		}
		if err != nil {
			logger.Error("job Run get sn fail.", zap.Error(err))
			return
		}

		// 处理一个数据
		go j.processData(sn)

		// 所有待处理数据已发送, 退出循环
		if sn == maxDataSn {
			break
		}
	}

	// 等待任务完成
	err := j.sw.Wait(j.ctx, int64(j.jobInfo.ProcessDataTotal))
	if err == sliding_window.ErrIsStop || err == context.Canceled { // 已停止直接退出
		return
	}
	if err != nil {
		logger.Error("job Run wait fail.", zap.Error(err))
		return
	}
}

func (j *jobLauncher) processData(sn int64) {
	defer func() {
		<-j.threadLock // 释放一个线程
	}()

	// 多次尝试处理任务
	attemptCount := 0 // 已尝试次数
	for {
		attemptCount++
		err := j.b.Process(j.ctx, j.jobInfo, sn, attemptCount)
		if err == nil {
			break
		}

		logger.Error("job Run process fail.", zap.Error(err))

		// 检查是否达到错误限速
		if !j.errLimiter.Allow() { // 表示无法再添加错误了
			j.submitStopFlag()
			return
		}

		// 达到最大尝试次数
		if attemptCount >= conf.Conf.JobProcessOneDataMaxAttemptCount {
			j.submitStopFlag()
			return
		}
	}

	// 滑动窗口确认当前数据已完成
	j.sw.Ack(sn)
}

// 内部发起停止信号
func (j *jobLauncher) submitStopFlag() {
	if atomic.AddInt32(&j.onceStop, 1) == 1 {
		close(j.stopChan)
		j.cancel()
		j.sw.Stop()
	}
}

// 停止后置逻辑
func (j *jobLauncher) stopSideEffect() {
	// todo 立即写入当前进度和错误日志计数到redis
	// todo 立即写入当前进度和错误日志计数到db, 对于已完成任务刷新任务状态
	// todo 删除redis进度
	// todo 任务停止回调
}
