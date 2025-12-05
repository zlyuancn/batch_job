package interceptor

import (
	"context"

	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/pb"
)

type Interceptor func(ctx context.Context, handlerType InterceptorType, op *pb.OpInfoQ, jobInfo *batch_job_list.Model) error

var interceptorList = map[InterceptorType][]Interceptor{}

type InterceptorType int

const (
	// 创建任务前
	BeforeCreateJob InterceptorType = iota

	// 更新任务前
	BeforeUpdateJob

	// 启动任务前
	BeforeRunJob
)

// 添加拦截器
func AddInterceptor(t InterceptorType, hs ...Interceptor) {
	interceptorList[t] = append(interceptorList[t], hs...)
}

// 触发
func Trigger(ctx context.Context, t InterceptorType, op *pb.OpInfoQ, jobInfo *batch_job_list.Model) error {
	for _, h := range interceptorList[t] {
		err := h(ctx, t, op, jobInfo)
		if err != nil {
			return err
		}
	}
	return nil
}
