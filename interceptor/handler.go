package interceptor

import (
	"context"

	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
)

type Info struct {
	BizInfo *batch_job_biz.Model  // 业务信息
	JobInfo *batch_job_list.Model // 任务信息
}

type Interceptor func(ctx context.Context, handlerType InterceptorType, info *Info) error

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
func Trigger(ctx context.Context, t InterceptorType, info *Info) error {
	for _, h := range interceptorList[t] {
		err := h(ctx, t, info)
		if err != nil {
			return err
		}
	}
	return nil
}
