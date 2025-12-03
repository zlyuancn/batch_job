package conf

const ConfigKey = "batch_job"

const (
	defSqlxName  = "batch_job"
	defRedisName = "batch_job"

	defJobIdGenKey                          = "batch_job:job_id_gen"
	defJobOpLockKeyPrefix                   = "batch_job:op_lock:"
	defJobStopFlagPrefix                    = "batch_job:stop:"
	defJobStopFlagTtl                       = 86400 // 1天
	defJobBeforeRunLockAppendTtl            = 60    // 60秒
	defJobRunLockKeyPrefix                  = "batch_job:run_lock:"
	defJobRunLockExtraTtl                   = 600 // 10分钟
	defJobRunLockRenewInterval              = 120 // 2分钟
	defJobRunLockRenewMaxContinuousErrCount = 3   // 续期最大连续错误次数
	defJobProcessedCountKeyPrefix           = "batch_job:processed_count:"
	defJobErrLogCountKeyPrefix              = "batch_job:err_log_count:"
	defAllBizNameCacheKey                   = "batch_job:all_biz_name"
	defBizInfoCacheKeyPrefix                = "batch_job:biz_info:"
	defBizInfoCacheTtl                      = 3600
	defJobInfoKeyPrefix                     = "batch_job:job_info:"
	defJobInfoCacheTtl                      = 3600

	defJobRunThreadCount                = 20
	defJobSlidingWindowSize             = 40
	defJobFlushProcessedCountInterval   = 1
	defJobFlushCheckStopFlagInterval    = 1
	defJobProcessCumulativeErrorRate    = 10
	defJobProcessOneDataMaxAttemptCount = 5
	defJobProcessErrWaitRetryTimeSec    = 3
	defAllowCreateNoRateLimitJob        = false

	defNodeMaxRate               = 10000
	defNoRateLimitJobMappingRate = 1000

	defRecoverJobLastActivateDay = 30
)

var Conf = Config{
	SqlxName:  defSqlxName,
	RedisName: defRedisName,

	JobIdGenKey:                          defJobIdGenKey,
	JobOpLockKeyPrefix:                   defJobOpLockKeyPrefix,
	JobStopFlagPrefix:                    defJobStopFlagPrefix,
	JobStopFlagTtl:                       defJobStopFlagTtl,
	JobBeforeRunLockAppendTtl:            defJobBeforeRunLockAppendTtl,
	JobRunLockKeyPrefix:                  defJobRunLockKeyPrefix,
	JobRunLockExtraTtl:                   defJobRunLockExtraTtl,
	JobRunLockRenewInterval:              defJobRunLockRenewInterval,
	JobRunLockRenewMaxContinuousErrCount: defJobRunLockRenewMaxContinuousErrCount,
	JobProcessedCountKeyPrefix:           defJobProcessedCountKeyPrefix,
	JobErrLogCountKeyPrefix:              defJobErrLogCountKeyPrefix,
	AllBizNameCacheKey:                   defAllBizNameCacheKey,
	BizInfoCacheKeyPrefix:                defBizInfoCacheKeyPrefix,
	BizInfoCacheTtl:                      defBizInfoCacheTtl,
	JobInfoKeyPrefix:                     defJobInfoKeyPrefix,
	JobInfoCacheTtl:                      defJobInfoCacheTtl,

	JobRunThreadCount:                defJobRunThreadCount,
	JobSlidingWindowSize:             defJobSlidingWindowSize,
	JobFlushProcessedCountInterval:   defJobFlushProcessedCountInterval,
	JobFlushCheckStopFlagInterval:    defJobFlushCheckStopFlagInterval,
	JobProcessCumulativeErrorRate:    defJobProcessCumulativeErrorRate,
	JobProcessOneDataMaxAttemptCount: defJobProcessOneDataMaxAttemptCount,
	JobProcessErrWaitRetryTimeSec:    defJobProcessErrWaitRetryTimeSec,
	AllowCreateNoRateLimitJob:        defAllowCreateNoRateLimitJob,

	NodeMaxRate:               defNodeMaxRate,
	NoRateLimitJobMappingRate: defNoRateLimitJobMappingRate,
	RecoverJobLastActivateDay: defRecoverJobLastActivateDay,
}

type Config struct {
	// 组件名

	SqlxName  string // sqlx组件名
	RedisName string // redis组件名

	// redisKey

	JobIdGenKey                          string // 任务id生成key前缀
	JobOpLockKeyPrefix                   string // 任务操作锁前缀
	JobStopFlagPrefix                    string // 任务停止flag前缀
	JobStopFlagTtl                       int    // 任务停止标记有效时间, 单位秒
	JobBeforeRunLockAppendTtl            int    // 任务启动前回调加运行锁追加的ttl, 单位秒, 在回调超时时间上再加多长时间的ttl, 用于防止重复创建启动器
	JobRunLockKeyPrefix                  string // 任务运行锁
	JobRunLockExtraTtl                   int    // 任务运行锁的ttl, 单位秒, 用于防止重复启动器
	JobRunLockRenewInterval              int    // 任务运行锁续期间隔时间, 单位秒
	JobRunLockRenewMaxContinuousErrCount int    // 续期最大连续错误次数, 达到该次数后将自动停止运行防止多线程抢到锁
	JobProcessedCountKeyPrefix           string // 缓存的已完成数key前缀
	JobErrLogCountKeyPrefix              string // 错误日志数key前缀
	AllBizNameCacheKey                   string // 所有业务名缓存key
	BizInfoCacheKeyPrefix                string // 缓存业务信息的key前缀
	BizInfoCacheTtl                      int    // 业务信息缓存ttl秒数
	JobInfoKeyPrefix                     string // 缓存任务信息的key前缀
	JobInfoCacheTtl                      int    // 任务信息的缓存ttl秒数

	// 任务控制

	JobRunThreadCount                int  // 任务运行时使用多少线程
	JobSlidingWindowSize             int  // 任务运行时使用的滑动窗口大小. 一般设置为线程的2倍, 为了避免性能浪费不应该小于线程数
	JobFlushProcessedCountInterval   int  // 将任务进度刷新到redis缓存间隔时间, 单位秒
	JobFlushCheckStopFlagInterval    int  // 检查停止标记间隔时间, 单位秒
	JobProcessCumulativeErrorRate    int  // 一秒内允许累计处理错误的数量, 超出后会停止任务等待重新启动
	JobProcessOneDataMaxAttemptCount int  // 一条数据最大尝试处理次数, 多次失败会导致任务停止等待重新启动
	JobProcessErrWaitRetryTimeSec    int  // 任务处理失败等待重试时间, 单位秒
	AllowCreateNoRateLimitJob        bool // 是否允许创建不限速任务

	// 节点速率控制

	DoNotRunJob               bool  // 禁止运行任务, 如果设为true, 将不会创建任务启动器
	NodeMaxRate               int32 // 节点允许最大速率
	AllowNoRateLimitJob       bool  // 是否允许执行不限速任务
	NoRateLimitJobMappingRate int32 // 对不限速任务映射为指定速率

	// 恢复器
	RecoverJobLastActivateDay int // 恢复最后多少天处于活跃的任务

}

func (conf *Config) Check() {
	if conf.SqlxName == "" {
		conf.SqlxName = defSqlxName
	}
	if conf.RedisName == "" {
		conf.RedisName = defRedisName
	}

	if conf.JobIdGenKey == "" {
		conf.JobIdGenKey = defJobIdGenKey
	}
	if conf.JobOpLockKeyPrefix == "" {
		conf.JobOpLockKeyPrefix = defJobOpLockKeyPrefix
	}
	if conf.JobStopFlagPrefix == "" {
		conf.JobStopFlagPrefix = defJobStopFlagPrefix
	}
	if conf.JobStopFlagTtl < 1 {
		conf.JobStopFlagTtl = defJobStopFlagTtl
	}
	if conf.JobBeforeRunLockAppendTtl < 1 {
		conf.JobBeforeRunLockAppendTtl = defJobBeforeRunLockAppendTtl
	}
	if conf.JobRunLockKeyPrefix == "" {
		conf.JobRunLockKeyPrefix = defJobRunLockKeyPrefix
	}
	if conf.JobRunLockExtraTtl < 1 {
		conf.JobRunLockExtraTtl = defJobRunLockExtraTtl
	}
	if conf.JobRunLockRenewInterval < 1 {
		conf.JobRunLockRenewInterval = defJobRunLockRenewInterval
	}
	if conf.JobRunLockRenewMaxContinuousErrCount < 1 {
		conf.JobRunLockRenewMaxContinuousErrCount = defJobRunLockRenewMaxContinuousErrCount
	}
	if conf.JobProcessedCountKeyPrefix == "" {
		conf.JobProcessedCountKeyPrefix = defJobProcessedCountKeyPrefix
	}
	if conf.JobErrLogCountKeyPrefix == "" {
		conf.JobErrLogCountKeyPrefix = defJobErrLogCountKeyPrefix
	}
	if conf.BizInfoCacheKeyPrefix == "" {
		conf.BizInfoCacheKeyPrefix = defBizInfoCacheKeyPrefix
	}
	if conf.BizInfoCacheTtl < 1 {
		conf.BizInfoCacheTtl = defBizInfoCacheTtl
	}
	if conf.JobInfoKeyPrefix == "" {
		conf.JobInfoKeyPrefix = defJobInfoKeyPrefix
	}
	if conf.JobInfoCacheTtl < 1 {
		conf.JobInfoCacheTtl = defJobInfoCacheTtl
	}

	if conf.JobRunThreadCount < 1 {
		conf.JobRunThreadCount = defJobRunThreadCount
	}
	if conf.JobSlidingWindowSize < 1 {
		conf.JobSlidingWindowSize = defJobSlidingWindowSize
	}
	if conf.JobSlidingWindowSize < conf.JobRunThreadCount {
		conf.JobSlidingWindowSize = conf.JobRunThreadCount
	}
	if conf.JobFlushProcessedCountInterval < 1 {
		conf.JobFlushProcessedCountInterval = defJobFlushProcessedCountInterval
	}
	if conf.JobFlushCheckStopFlagInterval < 1 {
		conf.JobFlushCheckStopFlagInterval = defJobFlushCheckStopFlagInterval
	}
	if conf.JobProcessErrWaitRetryTimeSec < 1 {
		conf.JobProcessErrWaitRetryTimeSec = defJobProcessErrWaitRetryTimeSec
	}
	if conf.JobProcessOneDataMaxAttemptCount < 1 {
		conf.JobProcessOneDataMaxAttemptCount = defJobProcessOneDataMaxAttemptCount
	}

	if conf.NodeMaxRate < 1 {
		conf.NodeMaxRate = defNodeMaxRate
	}
	if conf.NoRateLimitJobMappingRate < 1 {
		conf.NoRateLimitJobMappingRate = defNoRateLimitJobMappingRate
	}

	if conf.RecoverJobLastActivateDay < 1 {
		conf.RecoverJobLastActivateDay = defRecoverJobLastActivateDay
	}
}
