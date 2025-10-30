package logic

import (
	"context"
	"errors"
	"fmt"

	"github.com/bytedance/gopkg/util/logger"
	"github.com/zly-app/cache/v2"
	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/pkg/utils"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/batch_job_list_history"
	"github.com/zlyuancn/batch_job/dao/batch_job_log"
	"github.com/zlyuancn/batch_job/dao/redis"
	"github.com/zlyuancn/batch_job/model"
	"github.com/zlyuancn/batch_job/module"
	"github.com/zlyuancn/batch_job/pb"
)

// 业务启动. 要求任务必须处于 JobStatus.WaitBizRun 状态
func (b *BatchJob) BizStartJob(ctx context.Context, req *pb.BizStartJobReq) (*pb.BizStartJobRsp, error) {
	// 检查授权码
	key := module.CacheKey.GetJobBeforeRunLock(int(req.GetJobId()))
	err := redis.CheckLockAuthCode(ctx, key, req.GetAuthCode())
	if err != nil {
		logger.Error(ctx, "BizStartJob call CheckLockAuthCode fail.", zap.Error(err))
		return nil, err
	}

	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, int(req.GetJobId()))
	if err != nil {
		logger.Error(ctx, "BizStartJob call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}

	// 检查任务状态
	switch pb.JobStatus(jobInfo.Status) {
	case pb.JobStatus_JobStatus_WaitBizRun:
	default:
		logger.Info(ctx, "BizStartJob fail. status is not WaitBizRun", zap.Int64("jobId", req.GetJobId()))
		return nil, errors.New("status is not WaitBizRun")
	}

	// 更新状态和操作人
	v := &batch_job_list.Model{
		JobID:      uint(req.GetJobId()),
		Status:     byte(pb.JobStatus_JobStatus_Running),
		OpSource:   model.Op_Biz,
		OpUserID:   model.Op_Biz,
		OpUserName: model.Op_Biz,
		OpRemark:   req.GetRemark(),
		StatusInfo: model.StatusInfo_BizChangeStatus,
	}
	count, err := batch_job_list.UpdateStatus(ctx, v, jobInfo.Status)
	if err != nil {
		logger.Error(ctx, "BizStartJob call UpdateStatus fail.", zap.Error(err))
		return nil, err
	}
	if count != 1 {
		err = fmt.Errorf("update job status fail. update count != 1. is %d", count)
		logger.Error(ctx, "BizStartJob call batch_job_biz.UpdateStatus fail.", zap.Error(err))
		return nil, err
	}

	// 更新jobInfo状态
	jobInfo.Status = v.Status

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:      uint(req.GetJobId()),
		BizId:      jobInfo.BizId,
		Status:     byte(pb.JobStatus_JobStatus_Running),
		OpSource:   model.Op_Biz,
		OpUserID:   model.Op_Biz,
		OpUserName: model.Op_Biz,
		OpRemark:   req.GetRemark(),
		StatusInfo: model.StatusInfo_BizChangeStatus,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			logger.Error(cloneCtx, "BizStartJob call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(req.GetJobId())))
		if err != nil {
			logger.Error(cloneCtx, "BizStartJob call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	// 启动
	gpool.GetDefGPool().Go(func() error {
		// 删除停止标记
		err = module.Job.SetStopFlag(cloneCtx, int(jobInfo.JobID), false)
		if err != nil {
			logger.Error(cloneCtx, "BizStartJob call DelStopFlag fail.", zap.Error(err))
			return err
		}

		// 获取业务信息
		bizInfo, err := batch_job_biz.GetOneByBizId(cloneCtx, int(jobInfo.BizId))
		if err != nil {
			logger.Error(cloneCtx, "BizStartJob call batch_job_biz.GetOneByBizId fail.", zap.Error(err))
			return err
		}

		// 创建启动器
		module.Job.CreateLauncherByData(cloneCtx, bizInfo, jobInfo)
		return nil
	}, nil)

	return nil, nil
}

// 更新任务数据. 要求任务必须处于 JobStatus.WaitBizRun 状态可以使用
func (b *BatchJob) BizUpdateJobData(ctx context.Context, req *pb.BizUpdateJobDataReq) (*pb.BizUpdateJobDataRsp, error) {
	// 检查授权码
	key := module.CacheKey.GetJobBeforeRunLock(int(req.GetJobId()))
	err := redis.CheckLockAuthCode(ctx, key, req.GetAuthCode())
	if err != nil {
		logger.Error(ctx, "BizStartJob call CheckLockAuthCode fail.", zap.Error(err))
		return nil, err
	}

	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, int(req.GetJobId()))
	if err != nil {
		logger.Error(ctx, "BizUpdateJobData call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}

	// 检查任务状态
	switch pb.JobStatus(jobInfo.Status) {
	case pb.JobStatus_JobStatus_WaitBizRun:
	default:
		logger.Info(ctx, "BizUpdateJobData fail. status is not WaitBizRun", zap.Int64("jobId", req.GetJobId()))
		return nil, errors.New("status is not WaitBizRun")
	}

	// 写入数据库
	v := &batch_job_list.Model{
		JobID:            uint(req.GetJobId()),
		JobData:          req.GetJobData(),
		ProcessDataTotal: uint64(req.GetProcessDataTotal()),
		ProcessedCount:   uint64(req.GetProcessedCount()),
		OpSource:         model.Op_Biz,
		OpUserID:         model.Op_Biz,
		OpUserName:       model.Op_Biz,
		OpRemark:         req.GetRemark(),
		StatusInfo:       model.StatusInfo_BizUpdateJob,
	}
	_, err = batch_job_list.BizUpdateJob(ctx, v, jobInfo.Status)
	if err != nil {
		logger.Error(ctx, "BizUpdateJobData call UpdateOneModelWhereStatus fail.", zap.Error(err))
		return nil, err
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:            uint(req.GetJobId()),
		BizId:            jobInfo.BizId,
		JobData:          req.GetJobData(),
		ProcessDataTotal: uint64(req.GetProcessDataTotal()),
		ProcessedCount:   uint64(req.GetProcessedCount()),
		Status:           jobInfo.Status,
		OpSource:         model.Op_Biz,
		OpUserID:         model.Op_Biz,
		OpUserName:       model.Op_Biz,
		OpRemark:         req.GetRemark(),
		StatusInfo:       model.StatusInfo_BizUpdateJob,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			logger.Error(cloneCtx, "BizUpdateJobData call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(req.GetJobId())))
		if err != nil {
			logger.Error(cloneCtx, "BizUpdateJobData call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)
	return &pb.BizUpdateJobDataRsp{}, nil
}

// 要求业务停止运行. 一般为业务判断任务无法继续的时候
func (b *BatchJob) BizStopJob(ctx context.Context, req *pb.BizStopJobReq) (*pb.BizStopJobRsp, error) {
	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, int(req.GetJobId()))
	if err != nil {
		logger.Error(ctx, "BizStopJob call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}

	// 检查任务状态
	switch pb.JobStatus(jobInfo.Status) {
	case pb.JobStatus_JobStatus_WaitBizRun, pb.JobStatus_JobStatus_Running:
	case pb.JobStatus_JobStatus_Stopping, pb.JobStatus_JobStatus_Stopped:
		logger.Info(ctx, "AdminStartJob fail. status is stopped", zap.Int64("jobId", req.GetJobId()))
		return &pb.BizStopJobRsp{}, nil
	default:
		logger.Info(ctx, "BizStopJob fail. status is not running", zap.Int64("jobId", req.GetJobId()))
		return &pb.BizStopJobRsp{}, nil
	}

	// 写入停止标记
	err = module.Job.SetStopFlag(ctx, int(req.GetJobId()), true)
	if err != nil {
		logger.Error(ctx, "BizStopJob call SetStopFlag fail.", zap.Error(err))
		return nil, err
	}

	// 更新状态和操作人
	v := &batch_job_list.Model{
		JobID:      uint(req.GetJobId()),
		Status:     byte(pb.JobStatus_JobStatus_Stopping),
		OpSource:   model.Op_Biz,
		OpUserID:   model.Op_Biz,
		OpUserName: model.Op_Biz,
		OpRemark:   req.GetRemark(),
		StatusInfo: model.StatusInfo_BizChangeStatus,
	}
	count, err := batch_job_list.UpdateStatus(ctx, v, jobInfo.Status)
	if err != nil {
		logger.Error(ctx, "BizStopJob call UpdateStatus fail.", zap.Error(err))
		return nil, err
	}
	if count != 1 {
		err = fmt.Errorf("update job status fail. update count != 1. is %d", count)
		logger.Error(ctx, "BizStopJob call batch_job_biz.UpdateStatus fail.", zap.Error(err))
		return nil, err
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:      uint(req.GetJobId()),
		BizId:      jobInfo.BizId,
		Status:     byte(pb.JobStatus_JobStatus_Stopped),
		OpSource:   model.Op_Biz,
		OpUserID:   model.Op_Biz,
		OpUserName: model.Op_Biz,
		OpRemark:   req.GetRemark(),
		StatusInfo: model.StatusInfo_BizChangeStatus,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			logger.Error(cloneCtx, "BizStopJob call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(req.GetJobId())))
		if err != nil {
			logger.Error(cloneCtx, "BizStopJob call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)
	return &pb.BizStopJobRsp{}, nil
}

// 增加数据日志
func (b *BatchJob) BizAddDataLog(ctx context.Context, req *pb.BizAddDataLogReq) (*pb.BizAddDataLogRsp, error) {
	if len(req.GetLog()) == 0 {
		return &pb.BizAddDataLogRsp{}, nil
	}

	errNum := int64(0) // 错误计数
	lines := make([]*batch_job_log.Model, len(req.GetLog()))
	for i, a := range req.GetLog() {
		lines[i] = &batch_job_log.Model{
			JobID:   uint(req.GetJobId()),
			DataID:  a.GetDataId(),
			Remark:  a.GetRemark(),
			Extend:  a.GetExtend(),
			LogType: byte(a.GetLogType()),
		}
		if a.GetLogType() == pb.DataLogType_DataLogType_ErrData {
			errNum++
		}
	}

	_, err := batch_job_log.MultiSave(ctx, lines)
	if err != nil {
		logger.Error(ctx, "BizAddDataLog call MultiSave fail.", zap.Error(err))
		return nil, err
	}

	// 添加错误日志数
	if errNum > 0 {
		cloneCtx := utils.Ctx.CloneContext(ctx)
		gpool.GetDefGPool().Go(func() error {
			_, err := module.Job.IncrCacheErrCount(ctx, int(req.GetJobId()), errNum)
			if err != nil {
				logger.Error(cloneCtx, "BizAddDataLog call IncrCacheErrCount fail.", zap.Error(err))
				// return err
			}
			return nil
		}, nil)
	}
	return &pb.BizAddDataLogRsp{}, nil
}
