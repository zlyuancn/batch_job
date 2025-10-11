package module

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	zRedis "github.com/zly-app/component/redis"
	"github.com/zly-app/zapp/logger"
	"github.com/zly-app/zapp/pkg/utils"
	"github.com/zlyuancn/sliding_window"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/zlyuancn/batch_job/client/db"
	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/redis"
	"github.com/zlyuancn/batch_job/pb"
)

// 创建启动器
func (j *jobCli) CreateLauncherByData(ctx context.Context, bizInfo *batch_job_biz.Model, jobInfo *batch_job_list.Model) {
	cloneCtx := utils.Ctx.CloneContext(ctx)
	go j.createLauncher(cloneCtx, bizInfo, jobInfo)
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

		b.BeforeRun(ctx, jobInfo)
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

	jl, err := newJobLauncher(ctx, b, bizInfo, jobInfo, unlock, renew)
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

	threadLock chan struct{}                 // 线程锁
	limiter    *rate.Limiter                 // 限速器
	errLimiter *rate.Limiter                 // 错误限速器
	sw         *sliding_window.SlidingWindow // 滑动窗口. 注意. 数据编号从0开始

	stopChan         chan struct{} // 停止信号
	renewKeyStopChan chan struct{} // key自动续期具有独立的停止信号
	onceStop         int32         // 只调用一次stop

	isGotStopFlag bool   // 是否因为收到停止标记而停止
	statusInfo    string // 停止时的状态信息传递
}

func newJobLauncher(ctx context.Context, b Business, bizInfo *batch_job_biz.Model, jobInfo *batch_job_list.Model,
	unlock redis.KeyUnlock, renew redis.KeyTtlRenew) (*jobLauncher, error) {

	j := &jobLauncher{
		b:             b,
		bizInfo:       bizInfo,
		jobInfo:       jobInfo,
		lockKeyUnlock: unlock,
		lockKeyRenew:  renew,

		errLimiter: rate.NewLimiter(rate.Limit(conf.Conf.JobProcessCumulativeErrorRate), conf.Conf.JobProcessCumulativeErrorRate),

		stopChan:         make(chan struct{}),
		renewKeyStopChan: make(chan struct{}),
	}
	j.ctx, j.cancel = context.WithCancel(ctx)

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

	// 加载进度
	err := j.loadProgress()
	if err != nil {
		logger.Error(ctx, "newJobLauncher call loadProgress fail.", zap.Error(err))
		return nil, err
	}

	// 从db读取错误数并写入到cache中, 这一步表示, 在运行过程中, 数据查询错误数去缓存获取, 此时业务新增的错误数也会记录到缓存中, 降低mysql负载
	err = j.writeErrCount2Cache()
	if err != nil {
		logger.Error(ctx, "newJobLauncher call loadErrCount fail.", zap.Error(err))
		return nil, err
	}

	// 滑动窗口
	j.sw = sliding_window.NewSlidingWindow(conf.Conf.JobSlidingWindowSize, int64(jobInfo.ProcessedCount))

	return j, nil
}

// 循环对运行锁续期
func (j *jobLauncher) loopLockKeyRenew() {
	t := time.NewTicker(time.Duration(conf.Conf.JobRunLockRenewInterval) * time.Second)
	defer t.Stop()

	errCount := 0
	for {
		select {
		case <-j.renewKeyStopChan:
			return
		case <-t.C:
			err := j.lockKeyRenew(j.ctx, time.Duration(conf.Conf.JobRunLockExtraTtl)*time.Second)
			if err != nil {
				logger.Error(j.ctx, "lockKeyRenew fail.", zap.Uint("jobId", j.jobInfo.JobID), zap.Error(err))
				errCount++
			} else {
				errCount = 0
			}
			if errCount >= conf.Conf.JobRunLockRenewMaxContinuousErrCount {
				logger.Error(j.ctx, "lockKeyRenew Continuous fail.", zap.Uint("jobId", j.jobInfo.JobID), zap.Error(err))
				j.submitStopFlag("lockKeyRenew Continuous fail")
			}
		}
	}
}

// 循环写入当前进度数到redis
func (j *jobLauncher) loopWriteProgress() {
	t := time.NewTicker(time.Duration(conf.Conf.JobFlushProcessedCountInterval) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-j.stopChan:
			return
		case <-t.C:
			j.writeProcess2Cache()
		}
	}
}

// 循环检查停止flag
func (j *jobLauncher) loopCheckStopFlag() {
	t := time.NewTicker(time.Duration(conf.Conf.JobFlushCheckStopFlagInterval) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-j.stopChan:
			return
		case <-t.C:
			flag, _ := Job.GetStopFlag(j.ctx, int(j.jobInfo.JobID))
			if flag {
				logger.Warn(j.ctx, "loopCheckStopFlag got stop flag")
				j.isGotStopFlag = true
				j.submitStopFlag("got stop flag")
			}
		}
	}
}

// 从redis加载进度, 对于服务突然宕机, 进度是不会写入到db中, 而运行中的任务的实际进度都应该以redis为准
func (j *jobLauncher) loadProgress() error {
	p, ok, err := Job.LoadCacheProgress(j.ctx, int(j.jobInfo.JobID))
	if err != nil {
		return err
	}
	if ok {
		j.jobInfo.ProcessedCount = uint64(p)
	}
	return nil
}

// 写入错误数到缓存. 错误数最终是以db为准的
func (j *jobLauncher) writeErrCount2Cache() error {
	// 从db加载错误数
	count, err := Job.GetErrCount(j.ctx, int(j.jobInfo.JobID))
	if err != nil {
		return err
	}

	// 写入到缓存
	key := Job.GenErrCountCacheKey(int(j.jobInfo.JobID))
	err = db.GetRedis().Set(j.ctx, key, count, 0).Err()
	if err != nil {
		logger.Error(j.ctx, "writeErrCount2Cache fail.", zap.Error(err))
		return err
	}
	return nil
}

// 写入进度到缓存
func (j *jobLauncher) writeProcess2Cache() {
	key := Job.GenProgressCacheKey(int(j.jobInfo.JobID))
	finishedCount := j.sw.GetProgress() + 1
	err := db.GetRedis().Set(j.ctx, key, finishedCount, 0).Err()
	if err != nil {
		logger.Error(j.ctx, "writeProcess2Cache fail.", zap.Error(err))
		return
	}
}

func (j *jobLauncher) Run() {
	go j.loopLockKeyRenew()  // 循环续期
	go j.loopWriteProgress() // 循环写入进度
	go j.loopCheckStopFlag() // 循环检查停止flag

	defer j.stopSideEffect() // 处理停止后副作用

	maxDataSn := int64(j.jobInfo.ProcessDataTotal - 1)
	for {
		// 占用一个线程
		select {
		case j.threadLock <- struct{}{}:
		case <-j.stopChan:
			j.submitStopFlag("got stop signal")
			return
		}

		// 检查进度是否达标
		if j.sw.GetProgress() >= maxDataSn {
			j.submitStopFlag("status is finished")
			return
		}

		// 限速器
		if j.limiter != nil {
			err := j.limiter.Wait(j.ctx)
			if err == context.Canceled { // 已停止直接退出
				j.submitStopFlag("context cancel")
				return
			}
			if err != nil {
				j.submitStopFlag("limiter.Wait fail. " + err.Error())
				logger.Error("job Run call limiter.Wait fail.", zap.Error(err))
				return
			}
		}

		// 获取下一个要处理的数据编号
		sn, err := j.sw.Next(j.ctx)
		if err == sliding_window.ErrIsStop || err == context.Canceled { // 已停止直接退出
			j.submitStopFlag("sliding_window stop")
			return
		}
		if err != nil {
			j.submitStopFlag("sliding_window get next fail. " + err.Error())
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
		j.submitStopFlag("sliding_window stop")
		return
	}
	if err != nil {
		j.submitStopFlag("sliding_window wait fail. " + err.Error())
		logger.Error("job Run wait fail.", zap.Error(err))
		return
	}

	j.submitStopFlag("sliding_window finished")
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
			j.submitStopFlag("many err." + err.Error())
			return
		}

		// 达到最大尝试次数
		if attemptCount >= conf.Conf.JobProcessOneDataMaxAttemptCount {
			j.submitStopFlag("multi attempt err." + err.Error())
			return
		}
	}

	// 滑动窗口确认当前数据已完成
	j.sw.Ack(sn)
}

// 内部发起停止信号
func (j *jobLauncher) submitStopFlag(statusInfo string) {
	if atomic.AddInt32(&j.onceStop, 1) == 1 {
		close(j.stopChan)
		j.cancel()
		j.sw.Stop()
		j.statusInfo = statusInfo
	}
}

// 停止后置逻辑
func (j *jobLauncher) stopSideEffect() {
	defer func() {
		close(j.renewKeyStopChan) // 停止续期
		_ = j.lockKeyUnlock()     // 主动解锁
	}()

	// 替换ctx
	j.ctx = utils.Ctx.CloneContext(j.ctx)

	// 立即写入当前进度日志计数到redis, 失败的后果是重跑部分数据
	j.writeProcess2Cache()

	// 立即写入当前进度日志计数到db, 对于已完成任务刷新任务状态
	finishedCount := j.sw.GetProgress() + 1 // 已完成数
	isFinished := finishedCount >= int64(j.jobInfo.ProcessDataTotal)
	updateData := map[string]interface{}{
		"processed_count": finishedCount,
		"status_info":     j.statusInfo,
	}
	if j.isGotStopFlag {
		updateData["status"] = int(pb.JobStatus_JobStatus_Stopped)
	}
	if isFinished {
		updateData["status_info"] = "finished"
		updateData["status"] = int(pb.JobStatus_JobStatus_Finished)

		errLogCount, err := Job.GetErrCount(j.ctx, int(j.jobInfo.JobID))
		if err != nil {
			logger.Error(j.ctx, "stopSideEffect GetErrCount.", zap.Error(err))
			return // 这里不再更新db了, 等重试
		}
		updateData["err_log_count"] = errLogCount
	}
	err := batch_job_list.UpdateOne(j.ctx, int(j.jobInfo.JobID), updateData)
	if err != nil {
		logger.Error(j.ctx, "stopSideEffect UpdateOne fail.", zap.Error(err))
		return // 这里失败等重试
	}

	// 删除redis进度和错误数
	key1, key2 := Job.GenProgressCacheKey(int(j.jobInfo.JobID)), Job.GenErrCountCacheKey(int(j.jobInfo.ErrLogCount))
	err = db.GetRedis().Del(j.ctx, key1, key2).Err()
	if err != nil && err != zRedis.Nil {
		logger.Error(j.ctx, "stopSideEffect Del cacheKey fail.", zap.Error(err))
		// return // 这里不影响主流程
	}

	// 任务停止回调
	err = j.b.ProcessStop(j.ctx, j.jobInfo, isFinished)
	if err != nil {
		logger.Error(j.ctx, "stopSideEffect ProcessStop fail.", zap.Error(err))
		// return // 这里不影响主流程
	}
}
