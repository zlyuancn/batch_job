package logic

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/client/db"
	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/redis"
	"github.com/zlyuancn/batch_job/model"
	"github.com/zlyuancn/batch_job/module"
	"github.com/zlyuancn/batch_job/pb"
)

// 业务注册
func (*BatchJob) AdminRegistryBiz(ctx context.Context, req *pb.AdminRegistryBizReq) (*pb.AdminRegistryBizRsp, error) {
	v := &batch_job_biz.Model{
		BizType:               uint(req.GetBizType()),
		BizName:               req.GetBizName(),
		ExecType:              byte(req.GetExecType()),
		Remark:                req.GetRemark(),
		CbBeforeCreate:        req.GetCbBeforeCreate(),
		CbBeforeRun:           req.GetCbBeforeRun(),
		CbProcess:             req.GetCbProcess(),
		CbProcessStop:         req.GetCbProcessStop(),
		CbBeforeCreateTimeout: uint(req.GetCbBeforeCreateTimeout()),
		CbBeforeRunTimeout:    uint(req.GetCbBeforeRunTimeout()),
		CbProcessTimeout:      uint(req.GetCbProcessTimeout()),
		CbProcessStopTimeout:  uint(req.GetCbProcessStopTimeout()),
		LastOpSource:          req.GetOp().GetOpSource(),
		LastOpUserID:          req.GetOp().GetOpUserid(),
		LastOpUserName:        req.GetOp().GetOpUserName(),
		LastOpRemark:          req.GetOp().GetOpRemark(),
		OpHistory:             "",
		Status:                0,
	}

	hop := model.BizHistoryOpInfo{
		OpTime: time.Now().Unix(),
		Model:  v,
	}
	history := model.BizHistoryOpInfos{hop}
	historyText, err := sonic.MarshalString(history)
	if err != nil {
		logger.Error(ctx, "AdminCreateJob call MarshalString history fail.", zap.Error(err))
		return nil, err
	}

	v.OpHistory = historyText

	_, err = batch_job_biz.CreateOneModel(ctx, v)
	if err != nil {
		logger.Error(ctx, "AdminRegistryBiz call CreateOneModel fail.", zap.Error(err))
		return nil, err
	}
	return &pb.AdminRegistryBizRsp{}, nil
}

// 修改业务
func (*BatchJob) AdminChangeBiz(ctx context.Context, req *pb.AdminRegistryBizReq) (*pb.AdminRegistryBizRsp, error) {
	v := &batch_job_biz.Model{
		BizType:               uint(req.GetBizType()),
		BizName:               req.GetBizName(),
		ExecType:              byte(req.GetExecType()),
		Remark:                req.GetRemark(),
		CbBeforeCreate:        req.GetCbBeforeCreate(),
		CbBeforeRun:           req.GetCbBeforeRun(),
		CbProcess:             req.GetCbProcess(),
		CbProcessStop:         req.GetCbProcessStop(),
		CbBeforeCreateTimeout: uint(req.GetCbBeforeCreateTimeout()),
		CbBeforeRunTimeout:    uint(req.GetCbBeforeRunTimeout()),
		CbProcessTimeout:      uint(req.GetCbProcessTimeout()),
		CbProcessStopTimeout:  uint(req.GetCbProcessStopTimeout()),
		LastOpSource:          req.GetOp().GetOpSource(),
		LastOpUserID:          req.GetOp().GetOpUserid(),
		LastOpUserName:        req.GetOp().GetOpUserName(),
		LastOpRemark:          req.GetOp().GetOpRemark(),
		Status:                byte(req.GetStatus()),
	}
	hop := &model.BizHistoryOpInfo{
		OpTime: time.Now().Unix(),
		Model:  v,
	}
	historyText, err := sonic.MarshalString(hop)
	if err != nil {
		logger.Error(ctx, "AdminChangeBiz call MarshalString history fail.", zap.Error(err))
		return nil, err
	}

	v.OpHistory = historyText

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
	return &pb.AdminRegistryBizRsp{}, nil
}

// 创建任务
func (*BatchJob) AdminCreateJob(ctx context.Context, req *pb.AdminCreateJobReq) (*pb.AdminCreateJobRsp, error) {
	// 获取业务信息
	bizInfo, err := batch_job_biz.GetOneBaseInfoByBizType(ctx, req.GetBizType())
	if err != nil {
		logger.Error(ctx, "AdminStartJob call batch_job_biz.GetOneByBizType fail.", zap.Error(err))
		return nil, err
	}

	// 获取业务
	b, err := module.Biz.GetBizByDbModel(ctx, bizInfo)
	if err != nil {
		logger.Error(ctx, "AdminCreateJob call GetBiz fail.", zap.Int32("bizType", req.GetBizType()), zap.Error(err))
		return nil, err
	}

	// 生成任务id
	jobIdKey := conf.Conf.JobIdGenKeyPrefix + strconv.Itoa(int(req.BizType))
	jobId, err := db.GetRedis().Incr(ctx, jobIdKey).Result()
	if err != nil {
		logger.Error(ctx, "AdminCreateJob call Incr fail.", zap.Error(err))
		return nil, err
	}

	// 创建前回调
	req, err = b.BeforeCreate(ctx, req, jobId)
	if err != nil {
		logger.Error(ctx, "AdminCreateJob call biz.BeforeCreate fail.", zap.Error(err))
		return nil, err
	}

	// 写入数据库
	v := &batch_job_list.Model{
		JobID:            uint64(jobId),
		BizType:          uint(req.GetBizType()),
		BizData:          req.GetBizData(),
		ProcessDataTotal: uint64(req.GetProcessDataTotal()),
		Status:           byte(pb.JobStatus_JobStatus_Created),
		LastOpSource:     req.GetOp().GetOpSource(),
		LastOpUserID:     req.GetOp().GetOpUserid(),
		LastOpUserName:   req.GetOp().GetOpUserName(),
		LastOpRemark:     req.GetOp().GetOpRemark(),
		RateSec:          uint(req.GetRateSec()),
		RateType:         byte(req.GetRateType()),
		StatusInfo:       model.StatusInfo_UserOp,
	}
	if req.GetStartNow() {
		v.Status = byte(pb.JobStatus_JobStatus_Running)
		v.StatusInfo = model.StatusInfo_UserCreateAndRun
	}

	hop := model.JobHistoryOpInfo{
		OpTime: time.Now().Unix(),
		Model:  v,
	}
	history := model.JobHistoryOpInfos{hop}
	historyText, err := sonic.MarshalString(history)
	if err != nil {
		logger.Error(ctx, "AdminCreateJob call MarshalString history fail.", zap.Error(err))
		return nil, err
	}

	v.OpHistory = historyText

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

	// 立即启动
	if req.StartNow {
		module.Job.CreateLauncherByData(ctx, bizInfo, v)
	}

	return &pb.AdminCreateJobRsp{JobId: jobId}, nil
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
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, req.GetJobId())
	if err != nil {
		logger.Error(ctx, "AdminStartJob call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}

	// 获取业务信息
	bizInfo, err := batch_job_biz.GetOneBaseInfoByBizType(ctx, int32(jobInfo.BizType))
	if err != nil {
		logger.Error(ctx, "AdminStartJob call batch_job_biz.GetOneByBizType fail.", zap.Error(err))
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

	// 获取业务
	b, err := module.Biz.GetBizByDbModel(ctx, bizInfo)
	if err != nil {
		logger.Error(ctx, "AdminStartJob call biz.GetBiz fail.", zap.Error(err))
		return nil, err
	}

	// 更新状态和操作人
	v := &batch_job_list.Model{
		JobID:          uint64(req.GetJobId()),
		Status:         byte(pb.JobStatus_JobStatus_Running),
		LastOpSource:   req.GetOp().GetOpSource(),
		LastOpUserID:   req.GetOp().GetOpUserid(),
		LastOpUserName: req.GetOp().GetOpUserName(),
		LastOpRemark:   req.GetOp().GetOpRemark(),
		StatusInfo:     model.StatusInfo_UserOp,
	}
	hop := &model.JobHistoryOpInfo{
		OpTime: time.Now().Unix(),
		Model:  v,
	}
	historyText, err := sonic.MarshalString(hop)
	if err != nil {
		logger.Error(ctx, "AdminStartJob call MarshalString history fail.", zap.Error(err))
		return nil, err
	}

	v.OpHistory = historyText

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

	// 启动
	module.Job.CreateLauncherByData(ctx, bizInfo, jobInfo)

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
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, req.GetJobId())
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
	err = module.Job.SetStopFlag(ctx, req.GetJobId(), true)
	if err != nil {
		logger.Error(ctx, "AdminStopJob call SetStopFlag fail.", zap.Error(err))
		return nil, err
	}

	// 更新状态和操作人
	v := &batch_job_list.Model{
		JobID:          uint64(req.GetJobId()),
		Status:         byte(pb.JobStatus_JobStatus_Stopping),
		LastOpSource:   req.GetOp().GetOpSource(),
		LastOpUserID:   req.GetOp().GetOpUserid(),
		LastOpUserName: req.GetOp().GetOpUserName(),
		LastOpRemark:   req.GetOp().GetOpRemark(),
		StatusInfo:     model.StatusInfo_UserOp,
	}
	hop := &model.JobHistoryOpInfo{
		OpTime: time.Now().Unix(),
		Model:  v,
	}
	historyText, err := sonic.MarshalString(hop)
	if err != nil {
		logger.Error(ctx, "AdminStopJob call MarshalString history fail.", zap.Error(err))
		return nil, err
	}

	v.OpHistory = historyText

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
	return &pb.AdminStopJobRsp{}, nil
}
