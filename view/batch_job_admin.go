package view

import (
	"context"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/client/db"
	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/batch_job_type"
	"github.com/zlyuancn/batch_job/model"
	"github.com/zlyuancn/batch_job/module/biz"
	"github.com/zlyuancn/batch_job/pb"
)

// 业务注册
func (*BatchJob) AdminRegistryBiz(ctx context.Context, req *pb.AdminRegistryBizReq) (*pb.AdminRegistryBizRsp, error) {
	v := &batch_job_type.Model{
		BizType:                uint(req.GetBizType()),
		BizName:                req.GetBizName(),
		RateSec:                uint(req.GetRateSec()),
		RateType:               byte(req.GetRateType()),
		ExecType:               byte(req.GetExecType()),
		Remark:                 req.GetRemark(),
		CbBeforeCreate:         req.GetCbBeforeCreate(),
		CbBeforeRun:            req.GetCbBeforeRun(),
		CbProcess:              req.GetCbProcess(),
		CbProcessFinish:        req.GetCbProcessFinish(),
		CbBeforeCreateTimeout:  uint(req.GetCbBeforeCreateTimeout()),
		CbBeforeRunTimeout:     uint(req.GetCbBeforeRunTimeout()),
		CbProcessTimeout:       uint(req.GetCbProcessTimeout()),
		CbProcessFinishTimeout: uint(req.GetCbProcessFinishTimeout()),
	}
	_, err := batch_job_type.CreateOneModel(ctx, v)
	if err != nil {
		logger.Error(ctx, "AdminRegistryBiz call CreateOneModel fail.", zap.Error(err))
		return nil, err
	}
	return &pb.AdminRegistryBizRsp{}, nil
}

// 创建任务
func (*BatchJob) AdminCreateJob(ctx context.Context, req *pb.AdminCreateJobReq) (*pb.AdminCreateJobRsp, error) {
	// 获取业务
	b, err := biz.GetBiz(ctx, req.GetBizType())
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
	jobInfo := &model.JobInfo{
		JobId: jobId,
	}
	req, err = b.BeforeCreate(ctx, req, jobInfo)
	if err != nil {
		logger.Error(ctx, "AdminCreateJob call biz.BeforeCreate fail.", zap.Error(err))
		return nil, err
	}

	hop := model.HistoryOpInfo{
		OpSource:   req.GetOp().GetOpSource(),
		OpUserId:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpStatus:   int32(pb.JobStatus_Created),
		OldStatus:  int32(pb.JobStatus_Created),
		OpTime:     time.Now().Unix(),
		Remark:     model.StatusInfo_UserOp,
	}
	if req.GetStartNow() {
		hop.OpStatus = int32(pb.JobStatus_Running)
		hop.Remark = "create and start"
	}

	history := model.HistoryOpInfos{hop}
	historyText, err := sonic.MarshalString(history)
	if err != nil {
		logger.Error(ctx, "AdminCreateJob call MarshalString history fail.", zap.Error(err))
		return nil, err
	}

	// 写入数据库
	v := &batch_job_list.Model{
		ID:               0,
		JobID:            uint64(jobId),
		BizType:          uint(req.GetBizType()),
		BizData:          req.GetBizData(),
		ProcessDataTotal: uint64(req.GetProcessDataTotal()),
		Status:           byte(pb.JobStatus_Created),
		UpdateTime:       time.Time{},
		LastOpSource:     req.GetOp().GetOpSource(),
		LastOpUserID:     req.GetOp().GetOpUserid(),
		LastOpUserName:   req.GetOp().GetOpUserName(),
		StatusInfo:       model.StatusInfo_UserOp,
		OpHistory:        historyText,
		BizProcessData:   req.GetBizProcessData(),
	}
	_, err = batch_job_list.CreateOneModel(ctx, v)
	if err != nil {
		logger.Error(ctx, "AdminCreateJob call CreateOneModel fail.", zap.Error(err))
		return nil, err
	}

	// 立即启动
	if req.StartNow {
		// todo 创建时立即启动
	}

	return &pb.AdminCreateJobRsp{JobId: jobId}, nil
}
