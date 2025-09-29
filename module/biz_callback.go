package module

import (
	"context"

	"github.com/zlyuancn/batch_job/dao/batch_job_type"
	"github.com/zlyuancn/batch_job/pb"
)

type callbackBiz struct {
	v *batch_job_type.Model
}

func (c *callbackBiz) BeforeCreate(ctx context.Context, req *pb.AdminCreateJobReq, jobId int64) (*pb.AdminCreateJobReq, error) {
	if c.v.CbBeforeCreate == "" {
		return req, nil
	}

	// todo 创建前回调
	return req, nil
}

func (c *callbackBiz) HasBeforeRunCallback() bool {
	return c.v.CbBeforeRun != ""
}

func newCallbackBiz(ctx context.Context, v *batch_job_type.Model) (Biz, error) {
	c := &callbackBiz{v: v}
	return c, nil
}
