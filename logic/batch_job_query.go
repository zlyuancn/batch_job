package logic

import (
	"context"
	"math"

	"github.com/bytedance/sonic"
	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/model"
	"github.com/zlyuancn/batch_job/pb"
)

// 查询业务信息
func (b *BatchJob) QueryBizInfo(ctx context.Context, req *pb.QueryBizInfoReq) (*pb.QueryBizInfoRsp, error) {
	line, err := batch_job_biz.GetOneByBizType(ctx, req.GetBizType())
	if err != nil {
		logger.Error(ctx, "QueryBizInfo call batch_job_biz.GetOneByBizType fail.", zap.Error(err))
		return nil, err
	}

	ret := b.bizDbModel2Pb(line)
	return &pb.QueryBizInfoRsp{Line: ret}, nil
}

// 查询业务列表
func (b *BatchJob) QueryBizList(ctx context.Context, req *pb.QueryBizListReq) (*pb.QueryBizListRsp, error) {
	where := map[string]interface{}{}

	total, err := batch_job_biz.Count(ctx, where)
	if err != nil {
		logger.Error(ctx, "QueryBizList call batch_job_biz.Count", zap.Error(err))
		return nil, err
	}

	page, pageSize := req.GetPage(), req.GetPageSize()
	page = int32(math.Max(float64(page), 1))
	pageSize = int32(math.Max(float64(pageSize), 20))
	where["_orderby"] = "id desc"
	where["_limit"] = []uint{uint(page-1) * uint(pageSize), uint(pageSize)}

	lines, err := batch_job_biz.MultiGet(ctx, where)
	if err != nil {
		logger.Error(ctx, "QueryBizList call batch_job_biz.MultiGet", zap.Error(err))
		return nil, err
	}

	ret := make([]*pb.BizInfoA, 0, len(lines))
	for _, line := range lines {
		ret = append(ret, b.bizDbModel2Pb(line))
	}
	return &pb.QueryBizListRsp{
		Total:    int32(total),
		PageSize: pageSize,
		Line:     ret,
	}, nil
}

func (*BatchJob) bizDbModel2Pb(line *batch_job_biz.Model) *pb.BizInfoA {
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
			OpTime:     line.UpdateTime.Unix(),
		},
	}

	hop := model.BizHistoryOpInfos{}
	_ = sonic.UnmarshalString(line.OpHistory, &hop)
	if len(hop) > 0 {
		op := hop[0]
		ret.Op.OpRemark = op.OpRemark
	}

	return ret
}
