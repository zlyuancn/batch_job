package module

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/conf"
)

var RateLimit = newRateLimit()

type rateLimitCli struct {
	nowRate    int32 // 当前速率
	jobRateSec map[int]int32
	mx         sync.Mutex
}

func newRateLimit() *rateLimitCli {
	return &rateLimitCli{
		jobRateSec: make(map[int]int32),
	}
}

// 尝试运行任务检查速率
func (r *rateLimitCli) TryRunJobCheckRate(rateSec int32) bool {
	if rateSec < 1 {
		rateSec = conf.Conf.NoRateLimitJobMappingRate
	}

	// 检查是否超出速率上限
	nowRate := atomic.LoadInt32(&r.nowRate)
	if nowRate+rateSec > conf.Conf.NodeMaxRate {
		return false
	}

	return true
}

// 运行任务, 会增加节点当前速率
func (r *rateLimitCli) RunJob(ctx context.Context, jobId int, rateSec int32) {
	r.mx.Lock()
	oldRateSec := r.jobRateSec[jobId]
	r.jobRateSec[jobId] = rateSec
	r.mx.Unlock()

	newRate := atomic.AddInt32(&r.nowRate, rateSec-oldRateSec)
	logger.Warn(ctx, "node rate change", zap.Int32("rate", newRate))
}

// 停止任务, 会减少节点速率
func (r *rateLimitCli) StopJob(ctx context.Context, jobId int) {
	r.mx.Lock()

	rateSec, ok := r.jobRateSec[jobId]
	if !ok {
		r.mx.Unlock()
		return
	}

	delete(r.jobRateSec, jobId)
	r.mx.Unlock()

	if rateSec > 0 {
		newRate := atomic.AddInt32(&r.nowRate, -rateSec)
		logger.Warn(ctx, "node rate change", zap.Int32("rate", newRate))
	}
}

// 检查是否达到了速率上限
func (r *rateLimitCli) CheckIsMaxRace() bool {
	return atomic.LoadInt32(&r.nowRate) >= conf.Conf.NodeMaxRate
}
