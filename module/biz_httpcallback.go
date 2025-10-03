package module

import (
	"context"

	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/batch_job_type"
	"github.com/zlyuancn/batch_job/pb"
)

type httpCallbackBiz struct {
	v *batch_job_type.Model
}

func (h *httpCallbackBiz) HasBeforeRunCallback() bool {
	return h.v.CbBeforeRun != ""
}

func (h *httpCallbackBiz) BeforeCreate(ctx context.Context, req *pb.AdminCreateJobReq, jobId int64) (*pb.AdminCreateJobReq, error) {
	if h.v.CbBeforeCreate == "" {
		return req, nil
	}

	// todo 创建前回调
	return req, nil
}

func (h *httpCallbackBiz) BeforeRun(ctx context.Context, bizInfo *batch_job_type.Model, jobInfo *batch_job_list.Model) {
	if h.v.CbBeforeRun == "" {
		return
	}

	// todo 运行前回调
	return
}

func newHttpCallbackBiz(ctx context.Context, v *batch_job_type.Model) (Biz, error) {
	h := &httpCallbackBiz{v: v}
	return h, nil
}
