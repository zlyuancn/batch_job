package conf

const ConfigKey = "batch_job"

const (
	defSqlxName = "batch_job"
	defRedisName = "batch_job"

	defJobIdGenKeyPrefix = "batch_job_id_gen_"
)

var Conf = Config{
	SqlxName: defSqlxName,
	RedisName: defRedisName,

	JobIdGenKeyPrefix: defJobIdGenKeyPrefix,
}

type Config struct {
	SqlxName string // sqlx组件名
	RedisName string // redis组件名

	JobIdGenKeyPrefix string // 任务id生成key前缀
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
}
