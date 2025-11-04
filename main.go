package main

import (
	"context"

	"github.com/zly-app/grpc"
	"github.com/zly-app/service/cron"
	"github.com/zly-app/uapp"
	"github.com/zly-app/zapp/config"

	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/logic"
	"github.com/zlyuancn/batch_job/module"
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
	cron.RegistryHandler("recover", "@every 5m", true, func(ctx cron.IContext) error {
		return module.Restorer.Restorer(ctx)
	})

	app.Run()
}
