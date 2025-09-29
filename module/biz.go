package module

import (
	"context"
	"fmt"

	"github.com/zly-app/zapp/logger"

	"github.com/zlyuancn/batch_job/dao/batch_job_type"
	"github.com/zlyuancn/batch_job/pb"
)

type Biz interface {
	// 创建业务回调
	BeforeCreate(ctx context.Context, req *pb.AdminCreateJobReq, jobId int64) (*pb.AdminCreateJobReq, error)
	// 是否存在启动前回调
	HasBeforeRunCallback() bool
}

// 获取业务
func GetBizByBizType(ctx context.Context, bizType int32) (Biz, error) {
	// 从db加载biz信息
	v, err := batch_job_type.GetOneByBizType(ctx, bizType)
	if err != nil {
		logger.Error(ctx, "GetBiz error: %v", err)
		return nil, err
	}

	return GetBizByDbModel(ctx, v)
}

// 获取业务
func GetBizByDbModel(ctx context.Context, v *batch_job_type.Model) (Biz, error) {
	switch v.ExecType {
	case byte(pb.ExecType_Callback): // 回调
		return newCallbackBiz(ctx, v)
	}

	return nil, fmt.Errorf("biz type %d not support", v.ExecType)
}
