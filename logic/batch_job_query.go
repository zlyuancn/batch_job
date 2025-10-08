package logic

import (
	"context"
	"math"

	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/pb"
)

// 查询业务信息
func (b *BatchJob) QueryBizInfo(ctx context.Context, req *pb.QueryBizInfoReq) (*pb.QueryBizInfoRsp, error) {
	var line *batch_job_biz.Model
	var err error

	if req.GetNeedOpHistory() {
		line, err = batch_job_biz.GetOneByBizType(ctx, int(req.GetBizType()))
		if err != nil {
			logger.Error(ctx, "QueryBizInfo call batch_job_biz.GetOneByBizType fail.", zap.Error(err))
			return nil, err
		}
	} else {
		line, err = batch_job_biz.GetOneBaseInfoByBizType(ctx, int(req.GetBizType()))
		if err != nil {
			logger.Error(ctx, "QueryBizInfo call batch_job_biz.GetOneBaseInfoByBizType fail.", zap.Error(err))
			return nil, err
		}
	}

	ret := b.bizDbModel2Pb(line)
	return &pb.QueryBizInfoRsp{Line: ret}, nil
}

// 查询业务列表
func (b *BatchJob) QueryBizList(ctx context.Context, req *pb.QueryBizListReq) (*pb.QueryBizListRsp, error) {
	where := map[string]interface{}{
		"status": int(req.GetStatus()),
	}
	if req.GetOpUser() != "" {
		where["_or"] = []map[string]interface{}{
			{
				"last_op_user_id like": req.GetOpUser() + "%",
			},
			{
				"last_op_user_name like": req.GetOpUser() + "%",
			},
		}
	}

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

	ret := make([]*pb.BizInfoByListA, 0, len(lines))
	for _, line := range lines {
		ret = append(ret, b.bizDbModel2ListPb(line))
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
		Op: &pb.OpInfoA{
			OpSource:   line.LastOpSource,
			OpUserid:   line.LastOpUserID,
			OpUserName: line.LastOpUserName,
			OpRemark:   line.LastOpRemark,
			OpTime:     line.UpdateTime.Unix(),
		},
		Status:     pb.BizStatus(line.Status),
		CreateTime: line.CreateTime.Unix(),
	}
	return ret
}

func (*BatchJob) bizDbModel2ListPb(line *batch_job_biz.Model) *pb.BizInfoByListA {
	ret := &pb.BizInfoByListA{
		BizType:  int32(line.BizType),
		BizName:  line.BizName,
		Remark:   line.Remark,
		ExecType: pb.ExecType(line.ExecType),
		Op: &pb.OpInfoA{
			OpSource:   line.LastOpSource,
			OpUserid:   line.LastOpUserID,
			OpUserName: line.LastOpUserName,
			OpRemark:   line.LastOpRemark,
			OpTime:     line.UpdateTime.Unix(),
		},
		Status: pb.BizStatus(line.Status),
	}
	return ret
}

// 查询任务基本信息
func (b *BatchJob) QueryJobInfo(ctx context.Context, req *pb.QueryJobInfoReq) (*pb.QueryJobInfoRsp, error) {
	var line *batch_job_list.Model
	var err error

	if req.GetNeedOpHistory() {
		line, err = batch_job_list.GetOneByJobId(ctx, int(req.GetJobId()))
		if err != nil {
			logger.Error(ctx, "QueryJobBaseInfo call batch_job_list.GetOneByJobId fail.", zap.Error(err))
			return nil, err
		}
	} else {
		line, err = batch_job_list.GetOneBaseInfoByJobId(ctx, int(req.GetJobId()))
		if err != nil {
			logger.Error(ctx, "QueryJobBaseInfo call batch_job_list.GetOneBaseInfoByJobId fail.", zap.Error(err))
			return nil, err
		}
	}

	ret := b.jobDbModel2Pb(line)
	return &pb.QueryJobInfoRsp{Line: ret}, nil
}

// 查询任务列表
func (b *BatchJob) QueryJobList(ctx context.Context, req *pb.QueryJobListReq) (*pb.QueryJobListRsp, error) {
	where := map[string]interface{}{}
	if req.GetBizType() > 0 {
		where["biz_type"] = req.GetBizType()
	}
	switch req.GetStatus() {
	case pb.JobStatusQ_JobStatusQ_Running:
		where["status in"] = []int{int(pb.JobStatus_JobStatus_Running), int(pb.JobStatus_JobStatus_WaitBizRun), int(pb.JobStatus_JobStatus_Stopping)}
	case pb.JobStatusQ_JobStatusQ_Finished:
		where["status in"] = []int{int(pb.JobStatus_JobStatus_Finished)}
	default:
		where["status in"] = []int{int(pb.JobStatus_JobStatus_Created), int(pb.JobStatus_JobStatus_Stopped)}
	}
	if req.GetStartTime() > 0 {
		where["_or_start_time"] = []map[string]interface{}{
			{
				"create_time >=": req.GetStartTime(),
			},
			{
				"update_time >=": req.GetStartTime(),
			},
		}
	}
	if req.GetEndTime() > 0 {
		where["_or_end_time"] = []map[string]interface{}{
			{
				"create_time <=": req.GetEndTime(),
			},
			{
				"update_time <=": req.GetEndTime(),
			},
		}
	}
	if req.GetOpUser() != "" {
		where["_or_user"] = []map[string]interface{}{
			{
				"last_op_user_id like": req.GetOpUser() + "%",
			},
			{
				"last_op_user_name like": req.GetOpUser() + "%",
			},
		}
	}

	total, err := batch_job_list.Count(ctx, where)
	if err != nil {
		logger.Error(ctx, "QueryJobList call batch_job_list.Count", zap.Error(err))
		return nil, err
	}

	page, pageSize := req.GetPage(), req.GetPageSize()
	page = int32(math.Max(float64(page), 1))
	pageSize = int32(math.Max(float64(pageSize), 20))
	where["_orderby"] = "update_time desc"
	where["_limit"] = []uint{uint(page-1) * uint(pageSize), uint(pageSize)}

	lines, err := batch_job_list.MultiGet(ctx, where)
	if err != nil {
		logger.Error(ctx, "QueryJobList call batch_job_list.MultiGet", zap.Error(err))
		return nil, err
	}

	// todo 填充 业务名
	// todo 对于运行中的任务, 对进度和错误数需要从redis获取

	ret := make([]*pb.JobInfoByListA, 0, len(lines))
	for _, line := range lines {
		ret = append(ret, b.jobDbModel2ListPb(line))
	}
	return &pb.QueryJobListRsp{
		Total:    int32(total),
		PageSize: pageSize,
		Line:     ret,
	}, nil
}

func (*BatchJob) jobDbModel2Pb(line *batch_job_list.Model) *pb.JobInfoA {
	ret := &pb.JobInfoA{
		JobId:            int64(line.JobID),
		JobName:          line.JobName,
		BizType:          int32(line.BizType),
		BizData:          line.BizData,
		ProcessDataTotal: int64(line.ProcessDataTotal),
		ProcessedCount:   int64(line.ProcessedCount),
		ErrLogCount:      int64(line.ErrLogCount),
		Status:           pb.JobStatus(line.Status),
		CreateTime:       line.CreateTime.Unix(),
		RateType:         pb.RateType(line.RateType),
		RateSec:          int32(line.RateSec),
		Op: &pb.OpInfoA{
			OpSource:   line.LastOpSource,
			OpUserid:   line.LastOpUserID,
			OpUserName: line.LastOpUserName,
			OpRemark:   line.LastOpRemark,
			OpTime:     line.UpdateTime.Unix(),
		},
		StatusInfo: line.StatusInfo,
	}
	return ret
}

func (*BatchJob) jobDbModel2ListPb(line *batch_job_list.Model) *pb.JobInfoByListA {
	ret := &pb.JobInfoByListA{
		JobId:            int64(line.JobID),
		BizType:          int32(line.BizType),
		ProcessDataTotal: int64(line.ProcessDataTotal),
		ProcessedCount:   int64(line.ProcessedCount),
		ErrLogCount:      int64(line.ErrLogCount),
		Status:           pb.JobStatus(line.Status),
		CreateTime:       line.CreateTime.Unix(),
		RateType:         pb.RateType(line.RateType),
		RateSec:          int32(line.RateSec),
		Op: &pb.OpInfoA{
			OpSource:   line.LastOpSource,
			OpUserid:   line.LastOpUserID,
			OpUserName: line.LastOpUserName,
			OpRemark:   line.LastOpRemark,
			OpTime:     line.UpdateTime.Unix(),
		},
		StatusInfo: line.StatusInfo,
	}
	return ret
}
