package module

import (
	"strconv"

	"github.com/zlyuancn/batch_job/conf"
)

var CacheKey = cacheKeyCli{}

type cacheKeyCli struct{}

// 所有业务名缓存key
func (cacheKeyCli) QueryAllBizName() string {
	return conf.Conf.AllBizNameCacheKey
}

// 业务信息缓存key
func (cacheKeyCli) GetBizInfo(bizId int) string {
	return conf.Conf.BizInfoCacheKeyPrefix + strconv.Itoa(bizId)
}

// 任务信息缓存key
func (cacheKeyCli) GetJobInfo(jobId int) string {
	return conf.Conf.JobInfoKeyPrefix + strconv.Itoa(jobId)
}

// 错误数缓存key
func (*cacheKeyCli) GetErrCount(jobId int) string {
	return conf.Conf.JobErrLogCountKeyPrefix + strconv.Itoa(jobId)
}

// 已完成数缓存key
func (*cacheKeyCli) GetProcessedCount(jobId int) string {
	return conf.Conf.JobProcessedCountKeyPrefix + strconv.Itoa(jobId)
}

// 停止标记key
func (*cacheKeyCli) GetStopFlag(jobId int) string {
	return conf.Conf.JobStopFlagPrefix + strconv.Itoa(jobId)
}

// 任务启动前回调锁key
func (*cacheKeyCli) GetJobBeforeRunLock(jobId int) string {
	return conf.Conf.JobBeforeRunLockKeyPrefix + strconv.Itoa(jobId)
}
