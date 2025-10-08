package module

import (
	"context"

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

func (h *httpCallbackBiz) BeforeCreateAndChange(ctx context.Context, args *pb.BeforeCreateAndChangeReq) error {
	if h.v.CbBeforeCreate == "" {
		return nil
	}

	// todo 创建前回调
	return nil
}

func (h *httpCallbackBiz) BeforeRun(ctx context.Context, bizInfo *batch_job_biz.Model, jobInfo *batch_job_list.Model) {
	if h.v.CbBeforeRun == "" {
		return
	}

	// todo 运行前回调
	return
}

func newHttpCallbackBiz(ctx context.Context, v *batch_job_biz.Model) (Business, error) {
	h := &httpCallbackBiz{v: v}
	return h, nil
}
