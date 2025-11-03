package main

import (
	"context"

	"github.com/zly-app/grpc"
	"github.com/zly-app/uapp"

	"github.com/zlyuancn/batch_job/example/hello/logic"
	"github.com/zlyuancn/batch_job/example/hello/pb"
)

func main() {
	app := uapp.NewApp("batch_job.hello",
		grpc.WithService(),        // 启用 grpc 服务
		grpc.WithGatewayService(), // 启用网关服务
	)
	defer app.Exit()

	client := pb.NewHelloServiceClient(grpc.GetGatewayClientConn("hello"))
	_ = pb.RegisterHelloServiceHandlerClient(context.Background(), grpc.GetGatewayMux(), client)

	pb.RegisterHelloServiceServer(grpc.Server("hello"), logic.NewServer())

	app.Run()
}
