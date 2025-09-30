package module

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/client/db"
	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/model"
	"github.com/zlyuancn/batch_job/pb"
)

var Job = &jobCli{}

type jobCli struct{}

// 更新任务状态
func (*jobCli) UpdateJobStatus(ctx context.Context, jobId int64, oldStatus, status pb.JobStatus, opInfo *model.HistoryOpInfo) error {
	historyOpInfoText, err := sonic.MarshalString(opInfo)
	if err != nil {
		logger.Error(ctx, "UpdateJobStatus call MarshalString opInfo fail.", zap.Error(err))
		return err
	}

	count, err := batch_job_list.UpdateStatus(ctx, jobId, byte(oldStatus), byte(status), opInfo.OpSource, opInfo.OpUserId,
		opInfo.OpUserName, opInfo.Remark, historyOpInfoText)
	if err != nil {
		logger.Error(ctx, "UpdateJobStatus call batch_job_list.UpdateStatus fail.", zap.Error(err))
		return err
	}
	if count != 1 {
		err = fmt.Errorf("update job status fail. update count != 1. is %d", count)
		logger.Error(ctx, "UpdateJobStatus call batch_job_list.UpdateStatus fail.", zap.Error(err))
		return err
	}
	return err
}

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
