package logic

import (
	"context"
	"math"

	"github.com/bytedance/sonic"
	"github.com/zly-app/cache/v2"
	"github.com/zly-app/component/redis"
	"github.com/zly-app/zapp/log"
	"github.com/zly-app/zapp/pkg/utils"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/client/db"
	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/batch_job_log"
	"github.com/zlyuancn/batch_job/module"
	"github.com/zlyuancn/batch_job/pb"
)

// 查询所有业务名
func (b *BatchJob) QueryAllBizName(ctx context.Context, req *pb.QueryAllBizNameReq) (*pb.QueryAllBizNameRsp, error) {
	key := module.CacheKey.QueryAllBizName()

	var ret []*pb.QueryAllBizNameRsp_LineA
	err := cache.GetDefCache().Get(ctx, key, &ret, cache.WithLoadFn(func(ctx context.Context, key string) (interface{}, error) {
		return b.queryAllBizName(ctx)
	}))
	if err != nil {
		log.Error(ctx, "QueryAllBizName call fail", zap.Error(err))
		return nil, err
	}

	return &pb.QueryAllBizNameRsp{Line: ret}, nil
}
func (b *BatchJob) queryAllBizName(ctx context.Context) ([]*pb.QueryAllBizNameRsp_LineA, error) {
	where := map[string]interface{}{
		"_orderby": "biz_id asc",
	}
	selectField := []string{"biz_id", "biz_name", "status"}
	lines, err := batch_job_biz.MultiGetBySelect(ctx, where, selectField)
	if err != nil {
		log.Error(ctx, "QueryAllBizName call batch_job_biz.MultiGetBySelect", zap.Error(err))
		return nil, err
	}

	ret := make([]*pb.QueryAllBizNameRsp_LineA, 0, len(lines))
	for _, line := range lines {
		ret = append(ret, &pb.QueryAllBizNameRsp_LineA{
			BizId:   int32(line.BizId),
			BizName: line.BizName,
			Status:  pb.BizStatus(line.Status),
		})
	}
	return ret, nil
}

// 查询业务信息
func (b *BatchJob) QueryBizInfo(ctx context.Context, req *pb.QueryBizInfoReq) (*pb.QueryBizInfoRsp, error) {
	line, err := module.Biz.GetBizInfoByCache(ctx, int(req.GetBizId()))
	if err != nil {
		log.Error(ctx, "QueryBizInfo call GetBizInfoByCache fail.", zap.Error(err))
		return nil, err
	}
	ret := b.bizDbModel2Pb(line)
	return &pb.QueryBizInfoRsp{Line: ret}, nil
}

// 查询业务列表
func (b *BatchJob) QueryBizList(ctx context.Context, req *pb.QueryBizListReq) (*pb.QueryBizListRsp, error) {
	where := map[string]interface{}{
		"status": int(req.GetStatus()),
	}
	if req.GetBizId() > 0 {
		where["biz_id"] = req.GetBizId()
	}
	if req.GetOpUser() != "" {
		where["_or"] = []map[string]interface{}{
			{
				"op_user_id like": req.GetOpUser() + "%",
			},
			{
				"op_user_name like": req.GetOpUser() + "%",
			},
		}
	}

	total, err := batch_job_biz.Count(ctx, where)
	if err != nil {
		log.Error(ctx, "QueryBizList call batch_job_biz.Count", zap.Error(err))
		return nil, err
	}

	page, pageSize := req.GetPage(), req.GetPageSize()
	page = int32(math.Max(float64(page), 1))
	pageSize = int32(math.Max(float64(pageSize), 20))
	where["_orderby"] = "biz_id desc"
	where["_limit"] = []uint{uint(page-1) * uint(pageSize), uint(pageSize)}

	// 获取id列表
	ids, err := batch_job_biz.MultiGetBizId(ctx, where)
	if err != nil {
		log.Error(ctx, "QueryBizList call batch_job_biz.MultiGetBizId", zap.Error(err))
		return nil, err
	}

	// 批量获取数据
	lines, err := utils.GoQuery(ids, func(id uint) (*batch_job_biz.Model, error) {
		line, err := module.Biz.GetBizInfoByCache(ctx, int(id))
		log.Info(ctx, "QueryBizList call GetBizInfoByCache result", zap.Any("line", line), zap.Error(err))
		if err != nil {
			log.Error(ctx, "QueryBizList call GetBizInfoByCache fail.", zap.Uint("id", id), zap.Error(err))
			return nil, err
		}
		return line, nil
	}, true)
	if err != nil {
		log.Error(ctx, "QueryBizList call query fail.", zap.Error(err))
		return nil, err
	}

	// 数据转换
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
	eed := &pb.ExecExtendDataA{}
	_ = sonic.UnmarshalString(line.ExecExtendData, eed)
	ret := &pb.BizInfoA{
		BizId:          int32(line.BizId),
		BizName:        line.BizName,
		Remark:         line.Remark,
		ExecType:       pb.ExecType(line.ExecType),
		ExecExtendData: eed,
		Op: &pb.OpInfoA{
			OpSource:   line.OpSource,
			OpUserid:   line.OpUserID,
			OpUserName: line.OpUserName,
			OpRemark:   line.OpRemark,
			OpTime:     line.UpdateTime.Unix(),
		},
		Status:     pb.BizStatus(line.Status),
		CreateTime: line.CreateTime.Unix(),
	}
	return ret
}

func (*BatchJob) bizDbModel2ListPb(line *batch_job_biz.Model) *pb.BizInfoByListA {
	ret := &pb.BizInfoByListA{
		BizId:    int32(line.BizId),
		BizName:  line.BizName,
		Remark:   line.Remark,
		ExecType: pb.ExecType(line.ExecType),
		Op: &pb.OpInfoA{
			OpSource:   line.OpSource,
			OpUserid:   line.OpUserID,
			OpUserName: line.OpUserName,
			OpRemark:   line.OpRemark,
			OpTime:     line.UpdateTime.Unix(),
		},
		Status: pb.BizStatus(line.Status),
	}
	return ret
}

// 查询任务基本信息
func (b *BatchJob) QueryJobInfo(ctx context.Context, req *pb.QueryJobInfoReq) (*pb.QueryJobInfoRsp, error) {
	line, err := module.Job.GetJobInfoByCache(ctx, uint(req.GetJobId()))
	if err != nil {
		log.Error(ctx, "QueryJobInfo call queryJobInfoByCache fail.", zap.Error(err))
		return nil, err
	}
	ret := b.jobDbModel2Pb(line)
	return &pb.QueryJobInfoRsp{Line: ret}, nil
}

// 查询任务列表
func (b *BatchJob) QueryJobList(ctx context.Context, req *pb.QueryJobListReq) (*pb.QueryJobListRsp, error) {
	where := map[string]interface{}{}
	if req.GetBizId() > 0 {
		where["biz_id"] = req.GetBizId()
	}
	switch req.GetStatus() {
	case pb.JobStatusQ_JobStatusQ_Created:
		where["status in"] = []int{int(pb.JobStatus_JobStatus_Created), int(pb.JobStatus_JobStatus_Stopped)}
	case pb.JobStatusQ_JobStatusQ_Running:
		where["status in"] = []int{int(pb.JobStatus_JobStatus_Running), int(pb.JobStatus_JobStatus_WaitBizRun), int(pb.JobStatus_JobStatus_Stopping)}
	case pb.JobStatusQ_JobStatusQ_Finished:
		where["status in"] = []int{int(pb.JobStatus_JobStatus_Finished)}
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
				"op_user_id like": req.GetOpUser() + "%",
			},
			{
				"op_user_name like": req.GetOpUser() + "%",
			},
		}
	}

	total, err := batch_job_list.Count(ctx, where)
	if err != nil {
		log.Error(ctx, "QueryJobList call batch_job_list.Count", zap.Error(err))
		return nil, err
	}

	page, pageSize := req.GetPage(), req.GetPageSize()
	page = int32(math.Max(float64(page), 1))
	pageSize = int32(math.Max(float64(pageSize), 20))
	where["_orderby"] = "update_time desc"
	where["_limit"] = []uint{uint(page-1) * uint(pageSize), uint(pageSize)}

	// 获取id列表
	ids, err := batch_job_list.MultiGetJobId(ctx, where)
	if err != nil {
		log.Error(ctx, "QueryJobList call batch_job_list.MultiGetJobId", zap.Error(err))
		return nil, err
	}

	// 批量获取数据
	lines, err := module.Job.BatchGetJobInfoByCache(ctx, ids)
	if err != nil {
		log.Error(ctx, "QueryJobList call BatchGetJobInfoByCache fail.", zap.Error(err))
		return nil, err
	}

	// 数据转换
	ret := make([]*pb.JobInfoByListA, 0, len(lines))
	for _, line := range lines {
		ret = append(ret, b.jobDbModel2ListPb(line))
	}

	// 对于运行中的任务, 对进度和错误数需要从redis获取
	_ = b.batchRenderRunningJobProcess(ctx, ret)

	return &pb.QueryJobListRsp{
		Total:    int32(total),
		PageSize: pageSize,
		Line:     ret,
	}, nil
}

// 批量渲染运行中任务进度
func (*BatchJob) batchRenderRunningJobProcess(ctx context.Context, ret []*pb.JobInfoByListA) error {
	lines := make([]*pb.JobInfoByListA, 0, len(ret))
	ps := make([]*redis.StringCmd, 0, len(ret)) // 任务进度
	es := make([]*redis.StringCmd, 0, len(ret)) // 错误数

	rdb, err := db.GetRedis()
	if err != nil {
		return err
	}
	pipe := rdb.Pipeline()
	for _, l := range ret {
		switch l.Status {
		case pb.JobStatus_JobStatus_Running, pb.JobStatus_JobStatus_Stopping:
		default:
			continue
		}

		lines = append(lines, l)
		ps = append(ps, pipe.Get(ctx, module.CacheKey.GetProcessedCount(int(l.JobId))))
		es = append(es, pipe.Get(ctx, module.CacheKey.GetErrCount(int(l.JobId))))
	}
	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		log.Error(ctx, "batchRenderRunningJobProcess call pipe.Exec", zap.Error(err))
		return err
	}

	for i, line := range lines {
		p, e := ps[i], es[i]
		if p.Err() == nil {
			line.ProcessedCount, _ = p.Int64()
		}
		if e.Err() == nil {
			line.ErrLogCount, _ = e.Int64()
		}
	}
	return nil
}

func (*BatchJob) jobDbModel2Pb(line *batch_job_list.Model) *pb.JobInfoA {
	ret := &pb.JobInfoA{
		JobId:            int64(line.JobID),
		JobName:          line.JobName,
		BizId:            int32(line.BizId),
		JobData:          line.JobData,
		ProcessDataTotal: int64(line.ProcessDataTotal),
		ProcessedCount:   int64(line.ProcessedCount),
		ErrLogCount:      int64(line.ErrLogCount),
		Status:           pb.JobStatus(line.Status),
		CreateTime:       line.CreateTime.Unix(),
		ConcType:         pb.ConcType(line.ConcType),
		RateSec:          int32(line.RateSec),
		Op: &pb.OpInfoA{
			OpSource:   line.OpSource,
			OpUserid:   line.OpUserID,
			OpUserName: line.OpUserName,
			OpRemark:   line.OpRemark,
			OpTime:     line.UpdateTime.Unix(),
		},
		StatusInfo: line.StatusInfo,
	}
	return ret
}

func (*BatchJob) jobDbModel2ListPb(line *batch_job_list.Model) *pb.JobInfoByListA {
	ret := &pb.JobInfoByListA{
		JobId:            int64(line.JobID),
		BizId:            int32(line.BizId),
		ProcessDataTotal: int64(line.ProcessDataTotal),
		ProcessedCount:   int64(line.ProcessedCount),
		ErrLogCount:      int64(line.ErrLogCount),
		Status:           pb.JobStatus(line.Status),
		CreateTime:       line.CreateTime.Unix(),
		ConcType:         pb.ConcType(line.ConcType),
		RateSec:          int32(line.RateSec),
		Op: &pb.OpInfoA{
			OpSource:   line.OpSource,
			OpUserid:   line.OpUserID,
			OpUserName: line.OpUserName,
			OpRemark:   line.OpRemark,
			OpTime:     line.UpdateTime.Unix(),
		},
		StatusInfo: line.StatusInfo,
	}
	return ret
}

// 查询任务状态信息, 用于获取运行中的任务的变化数据
func (b *BatchJob) QueryJobStateInfo(ctx context.Context, req *pb.QueryJobStateInfoReq) (*pb.QueryJobStateInfoRsp, error) {
	// 批量获取数据
	ids := make([]uint, len(req.GetJobIds()))
	for i, id := range req.GetJobIds() {
		ids[i] = uint(id)
	}
	lines, err := module.Job.BatchGetJobInfoByCache(ctx, ids)
	if err != nil {
		log.Error(ctx, "QueryJobStateInfo call BatchGetJobInfoByCache fail.", zap.Error(err))
		return nil, err
	}

	// 数据转换
	ret := make([]*pb.JobInfoByListA, 0, len(lines))
	for _, line := range lines {
		ret = append(ret, b.jobDbModel2ListPb(line))
	}

	// 对于运行中的任务, 对进度和错误数需要从redis获取
	_ = b.batchRenderRunningJobProcess(ctx, ret)

	// 输出转换
	out := make([]*pb.JobStateInfo, 0, len(ret))
	for _, r := range ret {
		out = append(out, &pb.JobStateInfo{
			JobId:            r.GetJobId(),
			ProcessDataTotal: r.GetProcessDataTotal(),
			ProcessedCount:   r.GetProcessedCount(),
			ErrLogCount:      r.GetErrLogCount(),
			Status:           r.GetStatus(),
			Op:               r.GetOp(),
			StatusInfo:       r.GetStatusInfo(),
		})
	}
	return &pb.QueryJobStateInfoRsp{JobStateInfos: out}, nil
}

// 查询任务的数据日志
func (b *BatchJob) QueryJobDataLog(ctx context.Context, req *pb.QueryJobDataLogReq) (*pb.QueryJobDataLogRsp, error) {
	where := map[string]interface{}{}
	if req.GetJobId() > 0 {
		where["job_id"] = req.GetJobId()
	}
	if req.GetNextCursor() > 0 {
		where["id <"] = req.GetNextCursor()
	}
	if req.GetStartTime() > 0 {
		where["create_time >="] = req.GetStartTime()
	}
	if req.GetEndTime() > 0 {
		where["create_time <="] = req.GetEndTime()
	}
	if len(req.GetLogType()) == 1 {
		where["log_type"] = int(req.GetLogType()[0])
	}
	if len(req.GetLogType()) > 1 {
		types := make([]int32, len(req.GetLogType()))
		for i := range req.GetLogType() {
			types[i] = int32(req.GetLogType()[i])
		}
		where["log_type in"] = types
	}

	pageSize := int32(math.Max(float64(req.GetPageSize()), 20))
	where["_orderby"] = "create_time desc"
	where["_limit"] = []uint{0, uint(pageSize)}

	lines, err := batch_job_log.MultiGet(ctx, where)
	if err != nil {
		log.Error(ctx, "QueryJobDataLog call batch_job_log.MultiGet", zap.Error(err))
		return nil, err
	}

	ret := make([]*pb.LogInfoByListA, 0, len(lines))
	for _, line := range lines {
		ret = append(ret, b.logDbModel2ListPb(line))
	}
	nextCursor := int64(0)
	if len(lines) > 0 {
		nextCursor = int64(lines[len(lines)-1].ID)
	}
	return &pb.QueryJobDataLogRsp{
		NextCursor: nextCursor,
		PageSize:   pageSize,
		Line:       ret,
	}, nil
}

func (*BatchJob) logDbModel2ListPb(line *batch_job_log.Model) *pb.LogInfoByListA {
	ret := &pb.LogInfoByListA{
		DataIndex:  line.DataIndex,
		Remark:     line.Remark,
		Extend:     line.Extend,
		LogType:    pb.DataLogType(line.LogType),
		CreateTime: line.CreateTime.Unix(),
	}
	return ret
}
