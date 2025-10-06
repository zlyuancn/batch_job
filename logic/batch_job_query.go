package logic

import (
	"context"

	"github.com/bytedance/sonic"
	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/model"
	"github.com/zlyuancn/batch_job/pb"
)

// 查询业务信息
func (*BatchJob) QueryBizInfo(ctx context.Context, req *pb.QueryBizInfoReq) (*pb.QueryBizInfoRsp, error) {
	line, err := batch_job_biz.GetOneByBizType(ctx, req.GetBizType())
	if err != nil {
		logger.Error(ctx, "QueryBizInfo call batch_job_biz.GetOneByBizType fail.", zap.Error(err))
		return nil, err
	}

	remark := ""
	hop := model.BizHistoryOpInfos{}
	_ = sonic.UnmarshalString(line.OpHistory, &hop)
	if len(hop) > 0 {
		op := hop[0]
		remark = op.OpRemark
	}

	ret := &pb.BizInfoA{
		BizType:               int32(line.BizType),
		BizName:               line.BizName,
		Remark:                line.Remark,
		ExecType:              pb.ExecType(line.ExecType),
		CbBeforeCreate:        line.CbBeforeCreate,
		CbBeforeRun:           line.CbBeforeRun,
		CbProcess:             line.CbProcess,
		CbProcessStop:         line.CbProcessStop,
		CbBeforeCreateTimeout: int32(line.CbBeforeCreateTimeout),
		CbBeforeRunTimeout:    int32(line.CbBeforeRunTimeout),
		CbProcessTimeout:      int32(line.CbProcessTimeout),
		CbProcessStopTimeout:  int32(line.CbProcessStopTimeout),
		RateType:              pb.RateType(line.RateType),
		RateSec:               int32(line.RateSec),
		Op: &pb.OpInfoA{
			OpSource:   line.LastOpSource,
			OpUserid:   line.LastOpUserID,
			OpUserName: line.LastOpUserName,
			OpRemark:   remark,
			OpTime:     line.UpdateTime.Unix(),
		},
	}
	return &pb.QueryBizInfoRsp{Line: ret}, nil
}
