package biz

import (
	"context"

	"github.com/zlyuancn/batch_job/dao/batch_job_type"
	"github.com/zlyuancn/batch_job/model"
	"github.com/zlyuancn/batch_job/pb"
)

type callbackBiz struct {
	v *batch_job_type.Model
}

func (c *callbackBiz) BeforeCreate(ctx context.Context, req *pb.AdminCreateJobReq, jobInfo *model.JobInfo) (*pb.AdminCreateJobReq, error) {
	if c.v.CbBeforeCreate == "" {
		return req, nil
	}

	// todo 创建前回调
	return req, nil
}

func createCallbackBiz(ctx context.Context, v *batch_job_type.Model) (Biz, error) {
	c := &callbackBiz{v: v}
	return c, nil
}
