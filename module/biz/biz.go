package biz

import (
	"context"
	"fmt"

	"github.com/zly-app/zapp/logger"

	"github.com/zlyuancn/batch_job/dao/batch_job_type"
	"github.com/zlyuancn/batch_job/model"
	"github.com/zlyuancn/batch_job/pb"
)

type Biz interface {
	// 创建业务回调
	BeforeCreate(ctx context.Context, req *pb.AdminCreateJobReq, jobInfo *model.JobInfo) (*pb.AdminCreateJobReq, error)
}

// 获取业务
func GetBiz(ctx context.Context, bizType int32) (Biz, error) {
	// 从db加载biz信息
	where := map[string]interface{}{
		"biz_type": bizType,
	}
	v, err := batch_job_type.GetOne(ctx, where)
	if err != nil {
		logger.Error(ctx, "GetBiz error: %v", err)
		return nil, err
	}

	switch v.ExecType {
	case byte(pb.ExecType_Callback): // 回调
		return createCallbackBiz(ctx, v)
	}

	return nil, fmt.Errorf("biz type %d not support", v.ExecType)
}
