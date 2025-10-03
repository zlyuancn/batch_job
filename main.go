package main

import (
	"context"

	"github.com/zly-app/grpc"
	"github.com/zly-app/uapp"
	"github.com/zly-app/zapp/config"

	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/logic"
	"github.com/zlyuancn/batch_job/pb"
)

func main() {
	config.RegistryApolloNeedParseNamespace(conf.ConfigKey)

	app := uapp.NewApp("batch_job",
		grpc.WithService(),        // 启用 grpc 服务
		grpc.WithGatewayService(), // 启用网关服务
	)
	defer app.Exit()

	client := pb.NewBatchJobServiceClient(grpc.GetGatewayClientConn("batch_job"))
	_ = pb.RegisterBatchJobServiceHandlerClient(context.Background(), grpc.GetGatewayMux(), client)

	pb.RegisterBatchJobServiceServer(grpc.Server("batch_job"), logic.NewServer())

	app.Run()
}
