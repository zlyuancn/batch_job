package logic

import (
	"context"
	"errors"

	"github.com/zly-app/cache/v2"
	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/log"
	"github.com/zly-app/zapp/pkg/utils"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/batch_job_list_history"
	"github.com/zlyuancn/batch_job/dao/batch_job_log"
	"github.com/zlyuancn/batch_job/dao/redis"
	"github.com/zlyuancn/batch_job/handler"
	"github.com/zlyuancn/batch_job/model"
	"github.com/zlyuancn/batch_job/module"
	"github.com/zlyuancn/batch_job/pb"
)

// 业务启动. 要求任务必须处于 JobStatus.WaitBizRun 状态
func (b *BatchJob) BizStartJob(ctx context.Context, req *pb.BizStartJobReq) (*pb.BizStartJobRsp, error) {
	// 检查授权码
	key := module.CacheKey.GetRunLockKey(int(req.GetJobId()))
	err := redis.CheckLockAuthCode(ctx, key, req.GetAuthCode())
	if err != nil {
		log.Error(ctx, "BizStartJob call CheckLockAuthCode fail.", zap.Error(err))
		return nil, err
	}

	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, uint(req.GetJobId()))
	if err != nil {
		log.Error(ctx, "BizStartJob call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}

	// 检查任务状态
	switch pb.JobStatus(jobInfo.Status) {
	case pb.JobStatus_JobStatus_WaitBizRun:
	default:
		log.Info(ctx, "BizStartJob fail. status is not WaitBizRun", zap.Int64("jobId", req.GetJobId()))
		return nil, errors.New("status is not WaitBizRun")
	}

	// 更新状态和操作人
	err = batch_job_list.UpdateOne(ctx, int(req.GetJobId()), map[string]interface{}{
		"status":      byte(pb.JobStatus_JobStatus_Running),
		"status_info": model.StatusInfo_BizChangeStatus,
	}, byte(pb.JobStatus_JobStatus_WaitBizRun))
	if err != nil {
		log.Error(ctx, "BizStartJob call UpdateOne fail.", zap.Error(err))
		return nil, err
	}

	// 更新jobInfo状态
	jobInfo.Status = byte(pb.JobStatus_JobStatus_Running)

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:      uint(req.GetJobId()),
		BizId:      jobInfo.BizId,
		Status:     byte(pb.JobStatus_JobStatus_Running),
		OpSource:   model.Op_Biz,
		OpUserID:   model.Op_Biz,
		OpUserName: model.Op_Biz,
		OpRemark:   req.GetRemark(),
		StatusInfo: model.StatusInfo_BizChangeStatus,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "BizStartJob call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(req.GetJobId())))
		if err != nil {
			log.Error(cloneCtx, "BizStartJob call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	// 启动
	gpool.GetDefGPool().Go(func() error {
		// 删除停止标记
		err = module.Job.SetStopFlag(cloneCtx, int(jobInfo.JobID), false)
		if err != nil {
			log.Error(cloneCtx, "BizStartJob call DelStopFlag fail.", zap.Error(err))
			return err
		}

		// 创建启动器
		module.Job.CreateLauncherByBizStart(cloneCtx, jobInfo, req.GetAuthCode())
		return nil
	}, nil)

	return nil, nil
}

// 更新任务数据. 要求任务必须处于 JobStatus.WaitBizRun 状态可以使用
func (b *BatchJob) BizUpdateJobData(ctx context.Context, req *pb.BizUpdateJobDataReq) (*pb.BizUpdateJobDataRsp, error) {
	// 检查授权码
	key := module.CacheKey.GetRunLockKey(int(req.GetJobId()))
	err := redis.CheckLockAuthCode(ctx, key, req.GetAuthCode())
	if err != nil {
		log.Error(ctx, "BizStartJob call CheckLockAuthCode fail.", zap.Error(err))
		return nil, err
	}

	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, uint(req.GetJobId()))
	if err != nil {
		log.Error(ctx, "BizUpdateJobData call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}

	// 检查任务状态
	switch pb.JobStatus(jobInfo.Status) {
	case pb.JobStatus_JobStatus_WaitBizRun:
	default:
		log.Info(ctx, "BizUpdateJobData fail. status is not WaitBizRun", zap.Int64("jobId", req.GetJobId()))
		return nil, errors.New("status is not WaitBizRun")
	}

	// 写入数据库
	err = batch_job_list.UpdateOne(ctx, int(req.GetJobId()), map[string]interface{}{
		"job_data":           req.GetJobData(),
		"process_data_total": uint64(req.GetProcessDataTotal()),
		"processed_count":    uint64(req.GetProcessedCount()),
		"status_info":        model.StatusInfo_BizChangeStatus,
	}, byte(pb.JobStatus_JobStatus_WaitBizRun))
	if err != nil {
		log.Error(ctx, "BizUpdateJobData call UpdateOneModelWhereStatus fail.", zap.Error(err))
		return nil, err
	}
	jobInfo.JobData = req.GetJobData()
	jobInfo.ProcessDataTotal = uint64(req.GetProcessDataTotal())
	jobInfo.ProcessedCount = uint64(req.GetProcessedCount())
	jobInfo.StatusInfo = model.StatusInfo_BizChangeStatus

	handler.Trigger(ctx, handler.AfterUpdateJob, jobInfo)

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:            uint(req.GetJobId()),
		BizId:            jobInfo.BizId,
		JobData:          req.GetJobData(),
		ProcessDataTotal: uint64(req.GetProcessDataTotal()),
		ProcessedCount:   uint64(req.GetProcessedCount()),
		Status:           jobInfo.Status,
		OpSource:         model.Op_Biz,
		OpUserID:         model.Op_Biz,
		OpUserName:       model.Op_Biz,
		OpRemark:         req.GetRemark(),
		StatusInfo:       model.StatusInfo_BizUpdateJob,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "BizUpdateJobData call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(req.GetJobId())))
		if err != nil {
			log.Error(cloneCtx, "BizUpdateJobData call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)
	return &pb.BizUpdateJobDataRsp{}, nil
}

// 要求业务停止运行. 一般为业务判断任务无法继续的时候
func (b *BatchJob) BizStopJob(ctx context.Context, req *pb.BizStopJobReq) (*pb.BizStopJobRsp, error) {
	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, uint(req.GetJobId()))
	if err != nil {
		log.Error(ctx, "BizStopJob call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}

	// 检查任务状态
	switch pb.JobStatus(jobInfo.Status) {
	case pb.JobStatus_JobStatus_WaitBizRun, pb.JobStatus_JobStatus_Running:
	case pb.JobStatus_JobStatus_Stopping, pb.JobStatus_JobStatus_Stopped:
		log.Info(ctx, "AdminStartJob fail. status is stopped", zap.Int64("jobId", req.GetJobId()))
		return &pb.BizStopJobRsp{}, nil
	default:
		log.Info(ctx, "BizStopJob fail. status is not running", zap.Int64("jobId", req.GetJobId()))
		return &pb.BizStopJobRsp{}, nil
	}

	// 写入停止标记
	if pb.JobStatus(jobInfo.Status) == pb.JobStatus_JobStatus_Running {
		err = module.Job.SetStopFlag(ctx, int(req.GetJobId()), true)
		if err != nil {
			log.Error(ctx, "BizStopJob call SetStopFlag fail.", zap.Error(err))
			return nil, err
		}
	}

	// 更新状态. 如果任务处于 WaitBizRun 状态则立即停止
	status := pb.JobStatus_JobStatus_Stopping
	if pb.JobStatus(jobInfo.Status) == pb.JobStatus_JobStatus_WaitBizRun {
		status = pb.JobStatus_JobStatus_Stopped
	}
	err = batch_job_list.UpdateOne(ctx, int(req.GetJobId()), map[string]interface{}{
		"status":      byte(status),
		"status_info": model.StatusInfo_BizChangeStatus,
	}, jobInfo.Status)
	if err != nil {
		log.Error(ctx, "BizStopJob call UpdateOne fail.", zap.Error(err))
		return nil, err
	}
	jobInfo.Status = byte(status)
	jobInfo.StatusInfo = model.StatusInfo_BizChangeStatus

	if status == pb.JobStatus_JobStatus_Stopped{
		handler.Trigger(ctx, handler.AfterJobStopped, jobInfo)
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:      uint(req.GetJobId()),
		BizId:      jobInfo.BizId,
		Status:     byte(pb.JobStatus_JobStatus_Stopped),
		OpSource:   model.Op_Biz,
		OpUserID:   model.Op_Biz,
		OpUserName: model.Op_Biz,
		OpRemark:   req.GetRemark(),
		StatusInfo: model.StatusInfo_BizChangeStatus,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "BizStopJob call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(req.GetJobId())))
		if err != nil {
			log.Error(cloneCtx, "BizStopJob call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)
	return &pb.BizStopJobRsp{}, nil
}

// 增加数据日志
func (b *BatchJob) BizAddDataLog(ctx context.Context, req *pb.BizAddDataLogReq) (*pb.BizAddDataLogRsp, error) {
	if len(req.GetLog()) == 0 {
		return &pb.BizAddDataLogRsp{}, nil
	}

	errNum := int64(0) // 错误计数
	lines := make([]*batch_job_log.Model, len(req.GetLog()))
	for i, a := range req.GetLog() {
		lines[i] = &batch_job_log.Model{
			JobID:     uint(req.GetJobId()),
			DataIndex: a.GetDataIndex(),
			Remark:    a.GetRemark(),
			Extend:    a.GetExtend(),
			LogType:   byte(a.GetLogType()),
		}
		if a.GetLogType() == pb.DataLogType_DataLogType_ErrData {
			errNum++
		}
	}

	_, err := batch_job_log.MultiSave(ctx, lines)
	if err != nil {
		log.Error(ctx, "BizAddDataLog call MultiSave fail.", zap.Error(err))
		return nil, err
	}

	// 添加错误日志数
	if errNum > 0 {
		cloneCtx := utils.Ctx.CloneContext(ctx)
		gpool.GetDefGPool().Go(func() error {
			_, err := module.Job.IncrCacheErrCount(cloneCtx, int(req.GetJobId()), errNum)
			if err != nil {
				log.Error(cloneCtx, "BizAddDataLog call IncrCacheErrCount fail.", zap.Error(err))
				// return err
			}
			return nil
		}, nil)
	}
	return &pb.BizAddDataLogRsp{}, nil
}
