package main

import (
	"context"

	"github.com/zly-app/grpc"
	"github.com/zly-app/service/cron"
	"github.com/zly-app/uapp"
	"github.com/zly-app/zapp/config"
	"github.com/zly-app/zapp/log"
	"github.com/zlyuancn/batch_job/module"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/handler"
	"github.com/zlyuancn/batch_job/logic"
	"github.com/zlyuancn/batch_job/pb"
)

func main() {
	config.RegistryApolloNeedParseNamespace(conf.ConfigKey)

	app := uapp.NewApp("batch_job",
		grpc.WithService(),        // 启用 grpc 服务
		grpc.WithGatewayService(), // 启用网关服务
		cron.WithService(),        // 启用定时服务
	)
	defer app.Exit()

	// rpc服务
	pb.RegisterBatchJobServiceServer(grpc.Server("batch_job"), logic.NewServer())

	// rpc网关
	client := pb.NewBatchJobServiceClient(grpc.GetGatewayClientConn("batch_job"))
	_ = pb.RegisterBatchJobServiceHandlerClient(context.Background(), grpc.GetGatewayMux(), client)

	// 定时器
	cron.RegistryHandler("recover", "@every 10m", true, func(ctx cron.IContext) error {
		return module.Restorer.Restorer(ctx)
	})

	handler.AddHandler(handler.AfterCreateJob, Handler)
	handler.AddHandler(handler.AfterUpdateJob, Handler)
	handler.AddHandler(handler.AfterRunJob, Handler)
	handler.AddHandler(handler.AfterJobStopped, Handler)
	handler.AddHandler(handler.JobRunFailureExit, Handler)
	handler.AddHandler(handler.JobRestorer, Handler)
	handler.AddHandler(handler.JobFinished, Handler)

	app.Run()
}

func Handler(ctx context.Context, handlerType handler.HandlerType, jobInfo *batch_job_list.Model) {
	switch handlerType {
	case handler.AfterCreateJob:
		log.Info(ctx, "Handler AfterCreateJob", zap.Any("jobInfo", jobInfo))
	case handler.AfterUpdateJob:
		log.Info(ctx, "Handler AfterUpdateJob", zap.Any("jobInfo", jobInfo))
	case handler.AfterRunJob:
		log.Info(ctx, "Handler AfterRunJob", zap.Any("jobInfo", jobInfo))
	case handler.AfterJobStopped:
		log.Info(ctx, "Handler AfterJobStopped", zap.Any("jobInfo", jobInfo))
	case handler.JobRunFailureExit:
		log.Info(ctx, "Handler JobRunFailureExit", zap.Any("jobInfo", jobInfo))
	case handler.JobRestorer:
		log.Info(ctx, "Handler JobRestorer", zap.Any("jobInfo", jobInfo))
	case handler.JobFinished:
		log.Info(ctx, "Handler JobFinished", zap.Any("jobInfo", jobInfo))
	}
}
