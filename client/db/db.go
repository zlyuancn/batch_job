package db

import (
	"github.com/zly-app/component/redis"
	"github.com/zly-app/component/sqlx"

	"github.com/zlyuancn/batch_job/conf"
)

func GetSqlx() sqlx.Client {
	return sqlx.GetClient(conf.Conf.SqlxName)
}
func GetRedis() redis.UniversalClient {
	return redis.GetClient(conf.Conf.RedisName)
}
