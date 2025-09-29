package conf

const ConfigKey = "batch_job"

const (
	defSqlxName  = "batch_job"
	defRedisName = "batch_job"

	defJobIdGenKeyPrefix  = "batch_job:id_gen:"
	defJobOpLockKeyPrefix = "batch_job:op_lock:"
	defJobStopFlagPrefix  = "batch_job:stop:"
	defJobStopFlagTtl     = 86400
)

var Conf = Config{
	SqlxName:  defSqlxName,
	RedisName: defRedisName,

	JobIdGenKeyPrefix:  defJobIdGenKeyPrefix,
	JobOpLockKeyPrefix: defJobOpLockKeyPrefix,
	JobStopFlagPrefix:  defJobStopFlagPrefix,
	JobStopFlagTtl:     defJobStopFlagTtl,
}

type Config struct {
	SqlxName  string // sqlx组件名
	RedisName string // redis组件名

	// redisKey
	JobIdGenKeyPrefix  string // 任务id生成key前缀
	JobOpLockKeyPrefix string // 任务操作锁前缀
	JobStopFlagPrefix  string // 任务停止flag前缀
	JobStopFlagTtl     int    // 任务停止标记有效时间, 单位秒
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
}
