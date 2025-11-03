package logic

import (
	"context"

	"github.com/zly-app/grpc"
	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/example/hello/pb"
	pb_0 "github.com/zlyuancn/batch_job/pb"
)

type Hello struct {
	pb.UnimplementedHelloServiceServer
}

func NewServer() pb.HelloServiceServer {
	return &Hello{}
}

func (h *Hello) CreateCheck(ctx context.Context, req *pb_0.JobBeforeCreateAndChangeReq) (*pb_0.JobBeforeCreateAndChangeRsp, error) {
	// return nil, errors.New("故意")
	return &pb_0.JobBeforeCreateAndChangeRsp{}, nil
}

func (h *Hello) Start(ctx context.Context, req *pb_0.JobBeforeRunReq) (*pb_0.JobBeforeRunRsp, error) {
	client := pb_0.NewBatchJobServiceClient(grpc.GetClientConn("batch_job"))

	// 更新数据
	// _, err := client.BizUpdateJobData(ctx, &pb_0.BizUpdateJobDataReq{
	// 	AuthCode:         req.GetAuthCode(),
	// 	JobId:            req.GetJobInfo().GetJobId(),
	// 	JobData:          req.GetJobInfo().GetJobData(),
	// 	ProcessDataTotal: 3,
	// 	ProcessedCount:   2,
	// 	Remark:           "hello业务主动变更",
	// })
	// if err != nil {
	// 	logger.Error(ctx, "Start call BizUpdateJobData fail.", zap.Error(err))
	// 	return nil, err
	// }

	// 测试停止
	// _, err = client.BizStopJob(ctx, &pb_0.BizStopJobReq{
	// 	JobId:  req.GetJobInfo().GetJobId(),
	// 	Remark: "hello业务主动停止",
	// })
	// if err != nil {
	// 	logger.Error(ctx, "Start call BizStopJob fail.", zap.Error(err))
	// 	return nil, err
	// }
	// return &pb_0.JobBeforeRunRsp{}, nil

	_, err := client.BizStartJob(ctx, &pb_0.BizStartJobReq{
		AuthCode: req.GetAuthCode(),
		JobId:    req.GetJobInfo().GetJobId(),
		Remark:   "hello业务主动启动",
	})
	if err != nil {
		logger.Error(ctx, "Start call BizStartJob fail.", zap.Error(err))
		return nil, err
	}

	return &pb_0.JobBeforeRunRsp{}, nil
}

func (h *Hello) Process(ctx context.Context, req *pb_0.JobProcessReq) (*pb_0.JobProcessRsp, error) {
	// client := pb_0.NewBatchJobServiceClient(grpc.GetClientConn("batch_job"))
	// _, err := client.BizAddDataLog(ctx, &pb_0.BizAddDataLogReq{
	// 	JobId: req.GetJobId(),
	// 	Log: []*pb_0.DataLogQ{
	// 		{
	// 			DataIndex: req.GetDataIndex(),
	// 			Remark:    "测试添加日志",
	// 			Extend:    "描述",
	// 			LogType:   pb_0.DataLogType_DataLogType_Err,
	// 		},
	// 	},
	// })
	// if err != nil {
	// 	logger.Error(ctx, "Start call BizStartJob fail.", zap.Error(err))
	// 	return nil, err
	// }
	return &pb_0.JobProcessRsp{}, nil
}

func (h *Hello) Stop(ctx context.Context, req *pb_0.JobProcessStopReq) (*pb_0.JobProcessStopRsp, error) {
	return &pb_0.JobProcessStopRsp{}, nil
}
