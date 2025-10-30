package module

import (
	"context"
	"time"

	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/pb"
)

type httpCallbackBiz struct {
	v              *batch_job_biz.Model
	ExecExtendData *pb.ExecExtendDataA
}

func (h *httpCallbackBiz) GetBizInfo() *batch_job_biz.Model {
	return h.v
}

func (h *httpCallbackBiz) GetExecExtendData() *pb.ExecExtendDataA {
	return h.ExecExtendData
}

func (h *httpCallbackBiz) HasBeforeRunCallback() bool {
	return h.ExecExtendData.GetHttpCallback().GetCbBeforeRun() != ""
}

func (h *httpCallbackBiz) GetCbBeforeRunTimeout() time.Duration {
	return time.Duration(h.ExecExtendData.GetHttpCallback().GetCbBeforeRunTimeout()) * time.Second
}

func (h *httpCallbackBiz) BeforeCreateAndChange(ctx context.Context, args *pb.JobBeforeCreateAndChangeReq) error {
	if h.ExecExtendData.GetHttpCallback().GetCbBeforeCreate() == "" {
		return nil
	}

	// todo 创建前回调
	logger.Debug(ctx, "BeforeCreateAndChange", zap.Any("args", args))
	return nil
}

func (h *httpCallbackBiz) BeforeRun(ctx context.Context, jobInfo *batch_job_list.Model, authCode string) {
	if h.ExecExtendData.GetHttpCallback().GetCbBeforeRun() == "" {
		return
	}

	args := &pb.JobBeforeRunReq{
		JobId:            int64(jobInfo.JobID),
		JobName:          jobInfo.JobName,
		BizId:            int32(jobInfo.BizId),
		BizName:          h.v.BizName,
		JobData:          jobInfo.JobData,
		ProcessDataTotal: int64(jobInfo.ProcessDataTotal),
		ProcessedCount:   int64(jobInfo.ProcessedCount),
		AuthCode:         authCode,
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
	if h.ExecExtendData.GetHttpCallback().GetCbProcessStop() == "" {
		return nil
	}

	args := &pb.JobProcessStopReq{
		JobId:            int64(jobInfo.JobID),
		JobName:          jobInfo.JobName,
		BizId:            int32(jobInfo.BizId),
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

func newHttpCallbackBiz(ctx context.Context, v *batch_job_biz.Model, eed *pb.ExecExtendDataA) (Business, error) {
	h := &httpCallbackBiz{
		v:              v,
		ExecExtendData: eed,
	}
	return h, nil
}
