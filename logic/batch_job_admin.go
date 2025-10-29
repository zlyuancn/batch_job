package logic

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
	"github.com/zly-app/cache/v2"
	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/logger"
	"github.com/zly-app/zapp/pkg/utils"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/client/db"
	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_biz_history"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/batch_job_list_history"
	"github.com/zlyuancn/batch_job/dao/redis"
	"github.com/zlyuancn/batch_job/model"
	"github.com/zlyuancn/batch_job/module"
	"github.com/zlyuancn/batch_job/pb"
)

// 业务注册
func (*BatchJob) AdminRegistryBiz(ctx context.Context, req *pb.AdminRegistryBizReq) (*pb.AdminRegistryBizRsp, error) {
	eed, err := sonic.MarshalString(req.GetExecExtendData())
	if err != nil {
		logger.Error(ctx, "AdminRegistryBiz call MarshalString ExecExtendData fail.", zap.Error(err))
		return nil, err
	}

	v := &batch_job_biz.Model{
		BizName:        req.GetBizName(),
		ExecType:       byte(req.GetExecType()),
		ExecExtendData: eed,
		Remark:         req.GetRemark(),

		OpSource:   req.GetOp().GetOpSource(),
		OpUserID:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		Status:     0,
	}
	bizId, err := batch_job_biz.CreateOneModel(ctx, v)
	if err != nil {
		logger.Error(ctx, "AdminRegistryBiz call CreateOneModel fail.", zap.Error(err))
		return nil, err
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_biz_history.Model{
		BizId:          uint(bizId),
		BizName:        req.GetBizName(),
		ExecType:       byte(req.GetExecType()),
		ExecExtendData: eed,
		Remark:         req.GetRemark(),
		OpSource:       req.GetOp().GetOpSource(),
		OpUserID:       req.GetOp().GetOpUserid(),
		OpUserName:     req.GetOp().GetOpUserName(),
		OpRemark:       req.GetOp().GetOpRemark(),
		Status:         0,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err := batch_job_biz_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			logger.Error(cloneCtx, "AdminRegistryBiz call batch_job_biz_history.CreateOneModel fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.QueryAllBizName(), module.CacheKey.GetBizInfo(int(bizId)))
		if err != nil {
			logger.Error(cloneCtx, "AdminRegistryBiz call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	return &pb.AdminRegistryBizRsp{BizId: int32(bizId)}, nil
}

// 修改业务
func (*BatchJob) AdminChangeBiz(ctx context.Context, req *pb.AdminChangeBizReq) (*pb.AdminChangeBizRsp, error) {
	eed, err := sonic.MarshalString(req.GetExecExtendData())
	if err != nil {
		logger.Error(ctx, "AdminRegistryBiz call MarshalString ExecExtendData fail.", zap.Error(err))
		return nil, err
	}

	v := &batch_job_biz.Model{
		BizId:          uint(req.GetBizId()),
		BizName:        req.GetBizName(),
		ExecType:       byte(req.GetExecType()),
		ExecExtendData: eed,
		Remark:         req.GetRemark(),
		OpSource:       req.GetOp().GetOpSource(),
		OpUserID:       req.GetOp().GetOpUserid(),
		OpUserName:     req.GetOp().GetOpUserName(),
		OpRemark:       req.GetOp().GetOpRemark(),
		Status:         byte(req.GetStatus()),
	}
	count, err := batch_job_biz.UpdateOneModel(ctx, v)
	if err != nil {
		logger.Error(ctx, "AdminChangeBiz call UpdateOneModel fail.", zap.Error(err))
		return nil, err
	}
	if count != 1 {
		err = fmt.Errorf("update biz fail. update count != 1. is %d", count)
		logger.Error(ctx, "AdminChangeBiz call batch_job_biz.UpdateOneModel fail.", zap.Error(err))
		return nil, err
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_biz_history.Model{
		BizId:          uint(req.GetBizId()),
		BizName:        req.GetBizName(),
		ExecType:       byte(req.GetExecType()),
		ExecExtendData: eed,
		Remark:         req.GetRemark(),
		OpSource:       req.GetOp().GetOpSource(),
		OpUserID:       req.GetOp().GetOpUserid(),
		OpUserName:     req.GetOp().GetOpUserName(),
		OpRemark:       req.GetOp().GetOpRemark(),
		Status:         byte(req.GetStatus()),
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_biz_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			logger.Error(cloneCtx, "AdminChangeBiz call batch_job_biz_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.QueryAllBizName(), module.CacheKey.GetBizInfo(int(req.GetBizId())))
		if err != nil {
			logger.Error(cloneCtx, "AdminChangeBiz call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	return &pb.AdminChangeBizRsp{}, nil
}

// 创建任务
func (*BatchJob) AdminCreateJob(ctx context.Context, req *pb.AdminCreateJobReq) (*pb.AdminCreateJobRsp, error) {
	// 获取业务信息
	bizInfo, err := batch_job_biz.GetOneByBizId(ctx, int(req.GetBizId()))
	if err != nil {
		logger.Error(ctx, "AdminStartJob call batch_job_biz.GetOneByBizId fail.", zap.Error(err))
		return nil, err
	}

	// 获取业务
	b, err := module.Biz.GetBizByDbModel(ctx, bizInfo)
	if err != nil {
		logger.Error(ctx, "AdminCreateJob call GetBiz fail.", zap.Int32("bizId", req.GetBizId()), zap.Error(err))
		return nil, err
	}

	// 生成任务id
	jobIdKey := conf.Conf.JobIdGenKey
	jobId, err := db.GetRedis().Incr(ctx, jobIdKey).Result()
	if err != nil {
		logger.Error(ctx, "AdminCreateJob call Incr fail.", zap.Error(err))
		return nil, err
	}

	// 创建前回调
	args := &pb.JobBeforeCreateAndChangeReq{
		JobName:          req.GetJobName(),
		BizId:            req.GetBizId(),
		JobData:          req.GetJobData(),
		ProcessDataTotal: req.GetProcessDataTotal(),
		ProcessedCount:   req.GetProcessedCount(),
		RateType:         req.GetRateType(),
		RateSec:          req.GetRateSec(),
		JobId:            jobId,
		IsCreate:         true,
	}
	err = b.BeforeCreateAndChange(ctx, args)
	if err != nil {
		logger.Error(ctx, "AdminCreateJob call biz.BeforeCreateAndChange fail.", zap.Error(err))
		return nil, err
	}

	// 写入数据库
	v := &batch_job_list.Model{
		JobID:            uint(jobId),
		JobName:          req.GetJobName(),
		BizId:            uint(req.GetBizId()),
		JobData:          req.GetJobData(),
		ProcessDataTotal: uint64(req.GetProcessDataTotal()),
		ProcessedCount:   uint64(req.GetProcessedCount()),
		Status:           byte(pb.JobStatus_JobStatus_Created),
		OpSource:         req.GetOp().GetOpSource(),
		OpUserID:         req.GetOp().GetOpUserid(),
		OpUserName:       req.GetOp().GetOpUserName(),
		OpRemark:         req.GetOp().GetOpRemark(),
		RateSec:          uint(req.GetRateSec()),
		RateType:         byte(req.GetRateType()),
		StatusInfo:       model.StatusInfo_UserOp,
	}
	if req.GetStartNow() {
		v.Status = byte(pb.JobStatus_JobStatus_Running)
		v.StatusInfo = model.StatusInfo_UserCreateAndRun
	}
	if req.GetStartNow() {
		if b.HasBeforeRunCallback() {
			v.Status = byte(pb.JobStatus_JobStatus_WaitBizRun)
		}
	}
	_, err = batch_job_list.CreateOneModel(ctx, v)
	if err != nil {
		logger.Error(ctx, "AdminCreateJob call CreateOneModel fail.", zap.Error(err))
		return nil, err
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:            uint(jobId),
		JobName:          req.GetJobName(),
		BizId:            uint(req.GetBizId()),
		JobData:          req.GetJobData(),
		ProcessDataTotal: uint64(req.GetProcessDataTotal()),
		ProcessedCount:   uint64(req.GetProcessedCount()),
		Status:           byte(pb.JobStatus_JobStatus_Created),
		OpSource:         req.GetOp().GetOpSource(),
		OpUserID:         req.GetOp().GetOpUserid(),
		OpUserName:       req.GetOp().GetOpUserName(),
		OpRemark:         req.GetOp().GetOpRemark(),
		RateSec:          uint(req.GetRateSec()),
		RateType:         byte(req.GetRateType()),
		StatusInfo:       v.StatusInfo,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			logger.Error(cloneCtx, "AdminCreateJob call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(jobId)))
		if err != nil {
			logger.Error(cloneCtx, "AdminCreateJob call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	// 立即启动
	if req.StartNow {
		gpool.GetDefGPool().Go(func() error {
			module.Job.CreateLauncherByData(cloneCtx, bizInfo, v)
			return nil
		}, nil)
	}

	return &pb.AdminCreateJobRsp{JobId: jobId}, nil
}

// 修改任务
func (*BatchJob) AdminChangeJob(ctx context.Context, req *pb.AdminChangeJobReq) (*pb.AdminChangeJobRsp, error) {
	// 加锁
	lockKey := conf.Conf.JobOpLockKeyPrefix + strconv.Itoa(int(req.GetJobId()))
	unlock, _, err := redis.Lock(ctx, lockKey, time.Second*10)
	if err != nil {
		logger.Error(ctx, "AdminStartJob call Lock fail.", zap.Error(err))
		return nil, err
	}
	defer unlock()

	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, int(req.GetJobId()))
	if err != nil {
		logger.Error(ctx, "AdminChangeJob call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}

	// 对于运行中的任务禁止修改
	switch pb.JobStatus(jobInfo.Status) {
	case pb.JobStatus_JobStatus_Running, pb.JobStatus_JobStatus_WaitBizRun, pb.JobStatus_JobStatus_Stopping:
		logger.Info(ctx, "AdminChangeJob fail. status is running", zap.Int64("jobId", req.GetJobId()))
		return &pb.AdminChangeJobRsp{}, nil
	}

	// 获取业务信息
	bizInfo, err := batch_job_biz.GetOneByBizId(ctx, int(jobInfo.BizId))
	if err != nil {
		logger.Error(ctx, "AdminStartJob call batch_job_biz.GetOneByBizId fail.", zap.Error(err))
		return nil, err
	}

	// 获取业务
	b, err := module.Biz.GetBizByDbModel(ctx, bizInfo)
	if err != nil {
		logger.Error(ctx, "AdminCreateJob call GetBiz fail.", zap.Uint("bizId", jobInfo.BizId), zap.Error(err))
		return nil, err
	}

	// 修改前回调
	args := &pb.JobBeforeCreateAndChangeReq{
		JobName:          req.GetJobName(),
		BizId:            int32(jobInfo.BizId),
		JobData:          req.GetJobData(),
		ProcessDataTotal: req.GetProcessDataTotal(),
		ProcessedCount:   req.GetProcessedCount(),
		RateType:         req.GetRateType(),
		RateSec:          req.GetRateSec(),
		JobId:            req.GetJobId(),
		IsCreate:         false,
	}
	err = b.BeforeCreateAndChange(ctx, args)
	if err != nil {
		logger.Error(ctx, "AdminChangeJob call biz.BeforeCreateAndChange fail.", zap.Error(err))
		return nil, err
	}

	// 写入数据库
	v := &batch_job_list.Model{
		JobID:            uint(req.GetJobId()),
		BizId:            jobInfo.BizId,
		JobName:          req.GetJobName(),
		JobData:          req.GetJobData(),
		ProcessDataTotal: uint64(req.GetProcessDataTotal()),
		ProcessedCount:   uint64(req.GetProcessedCount()),
		OpSource:         req.GetOp().GetOpSource(),
		OpUserID:         req.GetOp().GetOpUserid(),
		OpUserName:       req.GetOp().GetOpUserName(),
		OpRemark:         req.GetOp().GetOpRemark(),
		RateSec:          uint(req.GetRateSec()),
		RateType:         byte(req.GetRateType()),
		StatusInfo:       model.StatusInfo_UserOp,
	}
	_, err = batch_job_list.ChangeJob(ctx, v, jobInfo.Status)
	if err != nil {
		logger.Error(ctx, "AdminChangeJob call UpdateOneModelWhereStatus fail.", zap.Error(err))
		return nil, err
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:            uint(req.GetJobId()),
		JobName:          req.GetJobName(),
		BizId:            jobInfo.BizId,
		JobData:          req.GetJobData(),
		ProcessDataTotal: uint64(req.GetProcessDataTotal()),
		ProcessedCount:   uint64(req.GetProcessedCount()),
		Status:           jobInfo.Status,
		OpSource:         req.GetOp().GetOpSource(),
		OpUserID:         req.GetOp().GetOpUserid(),
		OpUserName:       req.GetOp().GetOpUserName(),
		OpRemark:         req.GetOp().GetOpRemark(),
		RateSec:          uint(req.GetRateSec()),
		RateType:         byte(req.GetRateType()),
		StatusInfo:       v.StatusInfo,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			logger.Error(cloneCtx, "AdminChangeJob call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(req.GetJobId())))
		if err != nil {
			logger.Error(cloneCtx, "AdminChangeJob call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	return &pb.AdminChangeJobRsp{}, nil
}

// 启动任务
func (*BatchJob) AdminStartJob(ctx context.Context, req *pb.AdminStartJobReq) (*pb.AdminStartJobRsp, error) {
	// 加锁
	lockKey := conf.Conf.JobOpLockKeyPrefix + strconv.Itoa(int(req.GetJobId()))
	unlock, _, err := redis.Lock(ctx, lockKey, time.Second*10)
	if err != nil {
		logger.Error(ctx, "AdminStartJob call Lock fail.", zap.Error(err))
		return nil, err
	}
	defer unlock()

	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, int(req.GetJobId()))
	if err != nil {
		logger.Error(ctx, "AdminStartJob call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}

	// 检查任务状态
	switch pb.JobStatus(jobInfo.Status) {
	case pb.JobStatus_JobStatus_Running, pb.JobStatus_JobStatus_WaitBizRun:
		logger.Info(ctx, "AdminStartJob fail. status is running", zap.Int64("jobId", req.GetJobId()))
		return &pb.AdminStartJobRsp{}, nil
	case pb.JobStatus_JobStatus_Stopping, pb.JobStatus_JobStatus_Finished:
		logger.Error(ctx, "AdminStartJob fail. status is finished or stopping", zap.Int64("jobId", req.GetJobId()))
		return nil, errors.New("Job is finished or stopping")
	}

	// 获取业务信息
	bizInfo, err := batch_job_biz.GetOneByBizId(ctx, int(jobInfo.BizId))
	if err != nil {
		logger.Error(ctx, "AdminStartJob call batch_job_biz.GetOneByBizId fail.", zap.Error(err))
		return nil, err
	}

	// 获取业务
	b, err := module.Biz.GetBizByDbModel(ctx, bizInfo)
	if err != nil {
		logger.Error(ctx, "AdminStartJob call biz.GetBiz fail.", zap.Error(err))
		return nil, err
	}

	// 更新状态和操作人
	v := &batch_job_list.Model{
		JobID:      uint(req.GetJobId()),
		Status:     byte(pb.JobStatus_JobStatus_Running),
		OpSource:   req.GetOp().GetOpSource(),
		OpUserID:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		StatusInfo: model.StatusInfo_UserChangeStatus,
	}
	if b.HasBeforeRunCallback() {
		v.Status = byte(pb.JobStatus_JobStatus_WaitBizRun)
	}
	count, err := batch_job_list.UpdateStatus(ctx, v)
	if err != nil {
		logger.Error(ctx, "AdminStartJob call UpdateStatus fail.", zap.Error(err))
		return nil, err
	}
	if count != 1 {
		err = fmt.Errorf("update job status fail. update count != 1. is %d", count)
		logger.Error(ctx, "AdminStartJob call batch_job_biz.UpdateStatus fail.", zap.Error(err))
		return nil, err
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:      uint(req.GetJobId()),
		BizId:      jobInfo.BizId,
		Status:     byte(pb.JobStatus_JobStatus_Running),
		OpSource:   req.GetOp().GetOpSource(),
		OpUserID:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		StatusInfo: model.StatusInfo_UserChangeStatus,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			logger.Error(cloneCtx, "AdminStartJob call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(req.GetJobId())))
		if err != nil {
			logger.Error(cloneCtx, "AdminStartJob call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	// 启动
	gpool.GetDefGPool().Go(func() error {
		module.Job.CreateLauncherByData(ctx, bizInfo, jobInfo)
		return nil
	}, nil)

	return &pb.AdminStartJobRsp{}, nil
}

// 停止任务
func (*BatchJob) AdminStopJob(ctx context.Context, req *pb.AdminStopJobReq) (*pb.AdminStopJobRsp, error) {
	// 加锁
	lockKey := conf.Conf.JobOpLockKeyPrefix + strconv.Itoa(int(req.GetJobId()))
	unlock, _, err := redis.Lock(ctx, lockKey, time.Second*10)
	if err != nil {
		logger.Error(ctx, "AdminStopJob call Lock fail.", zap.Error(err))
		return nil, err
	}
	defer unlock()

	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, int(req.GetJobId()))
	if err != nil {
		logger.Error(ctx, "AdminStopJob call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}

	// 检查任务状态
	switch pb.JobStatus(jobInfo.Status) {
	case pb.JobStatus_JobStatus_Created, pb.JobStatus_JobStatus_Finished, pb.JobStatus_JobStatus_Stopping, pb.JobStatus_JobStatus_Stopped:
		logger.Info(ctx, "AdminStopJob fail. status is stopped", zap.Int64("jobId", req.GetJobId()))
		return &pb.AdminStopJobRsp{}, nil
	}

	// 写入停止标记
	err = module.Job.SetStopFlag(ctx, int(req.GetJobId()), true)
	if err != nil {
		logger.Error(ctx, "AdminStopJob call SetStopFlag fail.", zap.Error(err))
		return nil, err
	}

	// 更新状态和操作人
	v := &batch_job_list.Model{
		JobID:      uint(req.GetJobId()),
		Status:     byte(pb.JobStatus_JobStatus_Stopping),
		OpSource:   req.GetOp().GetOpSource(),
		OpUserID:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		StatusInfo: model.StatusInfo_UserChangeStatus,
	}
	count, err := batch_job_list.UpdateStatus(ctx, v)
	if err != nil {
		logger.Error(ctx, "AdminStopJob call UpdateStatus fail.", zap.Error(err))
		return nil, err
	}
	if count != 1 {
		err = fmt.Errorf("update job status fail. update count != 1. is %d", count)
		logger.Error(ctx, "AdminStopJob call batch_job_biz.UpdateStatus fail.", zap.Error(err))
		return nil, err
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:      uint(req.GetJobId()),
		BizId:      jobInfo.BizId,
		Status:     byte(pb.JobStatus_JobStatus_Stopping),
		OpSource:   req.GetOp().GetOpSource(),
		OpUserID:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		StatusInfo: model.StatusInfo_UserChangeStatus,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			logger.Error(cloneCtx, "AdminStopJob call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(req.GetJobId())))
		if err != nil {
			logger.Error(cloneCtx, "AdminStopJob call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	return &pb.AdminStopJobRsp{}, nil
}
