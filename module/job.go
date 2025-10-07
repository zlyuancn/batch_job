package module

import (
	"context"
	"strconv"
	"time"

	"github.com/zlyuancn/batch_job/client/db"
	"github.com/zlyuancn/batch_job/conf"
)

var Job = &jobCli{}

type jobCli struct{}

// 写入停止标记
func (*jobCli) SetStopFlag(ctx context.Context, jobId int64, flag bool) error {
	key := conf.Conf.JobStopFlagPrefix + strconv.Itoa(int(jobId))
	var err error
	if flag {
		err = db.GetRedis().Set(ctx, key, "1", time.Duration(conf.Conf.JobStopFlagTtl)*time.Second).Err()
	} else {
		err = db.GetRedis().Del(ctx, key).Err()
	}
	return err
}
