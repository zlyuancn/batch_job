package module

import (
	"context"
	"fmt"
	"time"

	"github.com/bytedance/sonic"
	"github.com/zly-app/cache/v2"
	"github.com/zly-app/component/sqlx"

	"github.com/zlyuancn/batch_job/conf"
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
	GetBeforeRunTimeout() time.Duration

	// 创建业务回调
	BeforeCreateAndChange(ctx context.Context, args *pb.JobBeforeCreateAndChangeReq) error
	// 业务启动前回调
	BeforeRun(ctx context.Context, args *pb.JobBeforeRunReq)
	/*处理任务回调

	jobInfo 任务信息
	dataIndex 数据序号, 从0开始

	return err表示处理失败
	*/
	Process(ctx context.Context, jobInfo *batch_job_list.Model, dataIndex int64, attemptCount int) (*pb.JobProcessRsp, error)
	// 任务停止回调
	ProcessStop(ctx context.Context, jobInfo *batch_job_list.Model, isFinished bool) error
}

var Biz = &bizCli{}

type bizCli struct{}

// 获取业务
func (*bizCli) GetBiz(ctx context.Context, biz *batch_job_biz.Model) (Business, error) {
	eed := &pb.ExecExtendDataA{}
	err := sonic.UnmarshalString(biz.ExecExtendData, eed)
	if err != nil {
		err = fmt.Errorf("GetBiz call UnmarshalString ExecExtendData fail. err=%s", err)
		return nil, err
	}

	switch biz.ExecType {
	case byte(pb.ExecType_ExecType_HttpCallback): // http回调
		return newHttpCallbackBiz(ctx, biz, eed)
	}

	return nil, fmt.Errorf("biz type %d not support", biz.ExecType)
}

// 获取业务数据, 使用缓存
func (*bizCli) GetBizInfoByCache(ctx context.Context, bizId int) (*batch_job_biz.Model, error) {
	key := CacheKey.GetBizInfo(bizId)
	ret := &batch_job_biz.Model{}
	err := cache.GetDefCache().Get(ctx, key, ret, cache.WithLoadFn(func(ctx context.Context, key string) (interface{}, error) {
		v, err := batch_job_biz.GetOneByBizId(ctx, bizId)
		if err == sqlx.ErrNoRows {
			return nil, nil
		}
		return v, err
	}), cache.WithExpire(conf.Conf.BizInfoCacheTtl))
	return ret, err
}
