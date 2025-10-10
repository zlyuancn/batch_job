package module

import (
	"context"

	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/pb"
)

type httpCallbackBiz struct {
	v *batch_job_biz.Model
}

func (h *httpCallbackBiz) HasBeforeRunCallback() bool {
	return h.v.CbBeforeRun != ""
}

func (h *httpCallbackBiz) BeforeCreateAndChange(ctx context.Context, args *pb.JobBeforeCreateAndChangeReq) error {
	if h.v.CbBeforeCreate == "" {
		return nil
	}

	// todo 创建前回调
	logger.Debug(ctx, "BeforeCreateAndChange", zap.Any("args", args))
	return nil
}

func (h *httpCallbackBiz) BeforeRun(ctx context.Context, jobInfo *batch_job_list.Model) {
	if h.v.CbBeforeRun == "" {
		return
	}

	args := &pb.JobBeforeRunReq{
		JobId:            int64(jobInfo.JobID),
		JobName:          jobInfo.JobName,
		BizType:          int32(jobInfo.BizType),
		BizName:          h.v.BizName,
		JobData:          jobInfo.JobData,
		ProcessDataTotal: int64(jobInfo.ProcessDataTotal),
		ProcessedCount:   int64(jobInfo.ProcessedCount),
	}
	_ = args

	// todo 运行前回调
	logger.Debug(ctx, "BeforeRun", zap.Any("args", args))
	return
}

func (h *httpCallbackBiz) Process(ctx context.Context, jobInfo *batch_job_list.Model, dataIndex int64, attemptCount int) error {
	args := &pb.JobProcessReq{
		JobId:     int64(jobInfo.JobID),
		DataIndex: dataIndex,
	}
	_ = args

	// todo 处理数据
	logger.Debug(ctx, "Process", zap.Any("args", args))
	return nil
}

func (h *httpCallbackBiz) ProcessStop(ctx context.Context, jobInfo *batch_job_list.Model, isFinished bool) error {
	if h.v.CbProcessStop == "" {
		return nil
	}

	args := &pb.JobProcessStopReq{
		JobId:            int64(jobInfo.JobID),
		JobName:          jobInfo.JobName,
		BizType:          int32(jobInfo.BizType),
		BizName:          h.v.BizName,
		JobData:          jobInfo.JobData,
		ProcessDataTotal: int64(jobInfo.ProcessDataTotal),
		ProcessedCount:   int64(jobInfo.ProcessedCount),
		IsFinished:       isFinished,
	}
	_ = args

	// todo 停止前回调
	logger.Debug(ctx, "ProcessStop", zap.Any("args", args))
	return nil
}

func newHttpCallbackBiz(ctx context.Context, v *batch_job_biz.Model) (Business, error) {
	h := &httpCallbackBiz{v: v}
	return h, nil
}
