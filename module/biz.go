package module

import (
	"context"
	"fmt"
	"time"

	"github.com/bytedance/sonic"
	"github.com/zly-app/zapp/logger"

	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/pb"
)

type Business interface {
	// 获取业务信息
	GetBizInfo() *batch_job_biz.Model
	// 获取执行器扩展数据
	GetExecExtendData() *pb.ExecExtendDataA

	// 是否存在启动前回调
	HasBeforeRunCallback() bool
	// 获取启动前回调超时时间
	GetCbBeforeRunTimeout() time.Duration

	// 创建业务回调
	BeforeCreateAndChange(ctx context.Context, args *pb.JobBeforeCreateAndChangeReq) error
	// 业务启动前回调
	BeforeRun(ctx context.Context, jobInfo *batch_job_list.Model, authCode string)
	/*处理任务回调

	jobInfo 任务信息
	dataIndex 数据序号, 从0开始

	return err表示处理失败
	*/
	Process(ctx context.Context, jobInfo *batch_job_list.Model, dataIndex int64, attemptCount int) error
	// 任务停止回调
	ProcessStop(ctx context.Context, jobInfo *batch_job_list.Model, isFinished bool) error
}

var Biz = &bizCli{}

type bizCli struct{}

// 获取业务
func (b *bizCli) GetBizByBizId(ctx context.Context, bizId int) (Business, error) {
	// 从db加载biz信息
	v, err := batch_job_biz.GetOneByBizId(ctx, bizId)
	if err != nil {
		logger.Error(ctx, "GetBiz error: %v", err)
		return nil, err
	}

	return b.GetBizByDbModel(ctx, v)
}

// 获取业务
func (*bizCli) GetBizByDbModel(ctx context.Context, v *batch_job_biz.Model) (Business, error) {
	eed := &pb.ExecExtendDataA{}
	err := sonic.UnmarshalString(v.ExecExtendData, eed)
	if err != nil {
		err = fmt.Errorf("GetBizByDbModel call UnmarshalString ExecExtendData fail. err=%s", err)
		return nil, err
	}

	switch v.ExecType {
	case byte(pb.ExecType_ExecType_HttpCallback): // http回调
		return newHttpCallbackBiz(ctx, v, eed)
	}

	return nil, fmt.Errorf("biz type %d not support", v.ExecType)
}
