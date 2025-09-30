package conf

const ConfigKey = "batch_job"

const (
	defSqlxName  = "batch_job"
	defRedisName = "batch_job"

	defJobIdGenKeyPrefix         = "batch_job:id_gen:"
	defJobOpLockKeyPrefix        = "batch_job:op_lock:"
	defJobStopFlagPrefix         = "batch_job:stop:"
	defJobStopFlagTtl            = 86400 // 1天
	defJobBeforeRunLockKeyPrefix = "batch_job:before_run_lock:"
	defJobBeforeRunLockExtraTtl  = 60 // 60秒
	defJobRunLockKeyPrefix       = "batch_job:run_lock:"
	defJobRunLockExtraTtl        = 600 // 10分钟
	defJobRunLockRenewInterval   = 120 // 2分钟

	defJobRunThreadCount    = 10
	defJobSlidingWindowSize = 20
)

var Conf = Config{
	SqlxName:  defSqlxName,
	RedisName: defRedisName,

	JobIdGenKeyPrefix:         defJobIdGenKeyPrefix,
	JobOpLockKeyPrefix:        defJobOpLockKeyPrefix,
	JobStopFlagPrefix:         defJobStopFlagPrefix,
	JobStopFlagTtl:            defJobStopFlagTtl,
	JobBeforeRunLockKeyPrefix: defJobBeforeRunLockKeyPrefix,
	JobBeforeRunLockExtraTtl:  defJobBeforeRunLockExtraTtl,
	JobRunLockKeyPrefix:       defJobRunLockKeyPrefix,
	JobRunLockExtraTtl:        defJobRunLockExtraTtl,
	JobRunLockRenewInterval:   defJobRunLockRenewInterval,

	JobRunThreadCount:    defJobRunThreadCount,
	JobSlidingWindowSize: defJobSlidingWindowSize,
}

type Config struct {
	SqlxName  string // sqlx组件名
	RedisName string // redis组件名

	// redisKey
	JobIdGenKeyPrefix         string // 任务id生成key前缀
	JobOpLockKeyPrefix        string // 任务操作锁前缀
	JobStopFlagPrefix         string // 任务停止flag前缀
	JobStopFlagTtl            int    // 任务停止标记有效时间, 单位秒
	JobBeforeRunLockKeyPrefix string // 任务启动前回调锁
	JobBeforeRunLockExtraTtl  int    // 任务启动前回调锁额外的ttl, 单位秒, 在回调超时时间上再加多长时间的ttl, 用于防止重复创建启动器
	JobRunLockKeyPrefix       string // 任务启动锁
	JobRunLockExtraTtl        int    // 任务启动的ttl, 单位秒, 用于防止重复启动器
	JobRunLockRenewInterval   int    // 任务启动锁续期间隔时间, 单位秒

	JobRunThreadCount    int // 任务运行时使用多少线程
	JobSlidingWindowSize int // 任务运行时使用的滑动窗口大小. 一般设置为线程的2倍, 为了避免性能浪费不应该小于线程数
}

func (conf *Config) Check() {
	if conf.SqlxName == "" {
		conf.SqlxName = defSqlxName
	}
	if conf.RedisName == "" {
		conf.RedisName = defRedisName
	}

	if conf.JobIdGenKeyPrefix == "" {
		conf.JobIdGenKeyPrefix = defJobIdGenKeyPrefix
	}
	if conf.JobOpLockKeyPrefix == "" {
		conf.JobOpLockKeyPrefix = conf.JobIdGenKeyPrefix
	}
	if conf.JobStopFlagPrefix == "" {
		conf.JobStopFlagPrefix = defJobStopFlagPrefix
	}
	if conf.JobStopFlagTtl < 1 {
		conf.JobStopFlagTtl = defJobStopFlagTtl
	}
	if conf.JobBeforeRunLockKeyPrefix == "" {
		conf.JobBeforeRunLockKeyPrefix = defJobBeforeRunLockKeyPrefix
	}
	if conf.JobBeforeRunLockExtraTtl < 1 {
		conf.JobBeforeRunLockExtraTtl = defJobBeforeRunLockExtraTtl
	}
	if conf.JobRunLockKeyPrefix == "" {
		conf.JobRunLockKeyPrefix = conf.JobIdGenKeyPrefix
	}
	if conf.JobRunLockExtraTtl < 1 {
		conf.JobRunLockExtraTtl = defJobRunLockExtraTtl
	}
	if conf.JobRunLockRenewInterval < 1 {
		conf.JobRunLockRenewInterval = defJobRunLockRenewInterval
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
}
