package handler

import (
	"context"

	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/pkg/utils"

	"github.com/zlyuancn/batch_job/dao/batch_job_list"
)

type Handler func(ctx context.Context, handlerType HandlerType, jobInfo *batch_job_list.Model)

var handlers = map[HandlerType][]Handler{}

type HandlerType int

const (
	// 创建任务后
	AfterCreateJob HandlerType = iota

	// 更新任务后
	AfterUpdateJob

	// 启动任务后
	AfterRunJob

	// 任务停止后
	AfterJobStopped

	// 任务故障退出
	JobRunFailureExit
	// 任务恢复运行
	JobRestorer

	// 任务完成后
	JobFinished
)

// 添加handler
func AddHandler(t HandlerType, hs ...Handler) {
	handlers[t] = append(handlers[t], hs...)
}

// 触发
func Trigger(ctx context.Context, t HandlerType, jobInfo *batch_job_list.Model) {
	cloneCtx := utils.Ctx.CloneContext(ctx)
	gpool.GetDefGPool().Go(func() error {
		for _, h := range handlers[t] {
			h(cloneCtx, t, jobInfo)
		}
		return nil
	}, nil)
}
