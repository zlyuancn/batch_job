package logic

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
	"github.com/zly-app/cache/v2"
	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/log"
	"github.com/zly-app/zapp/pkg/utils"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/client/db"
	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_biz_history"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/dao/batch_job_list_history"
	"github.com/zlyuancn/batch_job/dao/redis"
	"github.com/zlyuancn/batch_job/handler"
	"github.com/zlyuancn/batch_job/model"
	"github.com/zlyuancn/batch_job/module"
	"github.com/zlyuancn/batch_job/pb"
)

// 业务注册
func (*BatchJob) AdminRegistryBiz(ctx context.Context, req *pb.AdminRegistryBizReq) (*pb.AdminRegistryBizRsp, error) {
	eed, err := sonic.MarshalString(req.GetExecExtendData())
	if err != nil {
		log.Error(ctx, "AdminRegistryBiz call MarshalString eed fail.", zap.Error(err))
		return nil, err
	}

	v := &batch_job_biz.Model{
		BizName:        req.GetBizName(),
		ExecType:       byte(req.GetExecType()),
		ExecExtendData: eed,
		Remark:         req.GetRemark(),

		OpSource:   req.GetOp().GetOpSource(),
		OpUserID:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		Status:     0,
	}
	bizId, err := batch_job_biz.CreateOneModel(ctx, v)
	if err != nil {
		log.Error(ctx, "AdminRegistryBiz call CreateOneModel fail.", zap.Error(err))
		return nil, err
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_biz_history.Model{
		BizId:          uint(bizId),
		BizName:        req.GetBizName(),
		ExecType:       byte(req.GetExecType()),
		ExecExtendData: eed,
		Remark:         req.GetRemark(),
		OpSource:       req.GetOp().GetOpSource(),
		OpUserID:       req.GetOp().GetOpUserid(),
		OpUserName:     req.GetOp().GetOpUserName(),
		OpRemark:       req.GetOp().GetOpRemark(),
		Status:         0,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err := batch_job_biz_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "AdminRegistryBiz call batch_job_biz_history.CreateOneModel fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.QueryAllBizName(), module.CacheKey.GetBizInfo(int(bizId)))
		if err != nil {
			log.Error(cloneCtx, "AdminRegistryBiz call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	return &pb.AdminRegistryBizRsp{BizId: int32(bizId)}, nil
}

// 修改业务
func (*BatchJob) AdminUpdateBiz(ctx context.Context, req *pb.AdminUpdateBizReq) (*pb.AdminUpdateBizRsp, error) {
	eed, err := sonic.MarshalString(req.GetExecExtendData())
	if err != nil {
		log.Error(ctx, "AdminRegistryBiz call MarshalString eed fail.", zap.Error(err))
		return nil, err
	}

	v := &batch_job_biz.Model{
		BizId:          uint(req.GetBizId()),
		BizName:        req.GetBizName(),
		ExecType:       byte(req.GetExecType()),
		ExecExtendData: eed,
		Remark:         req.GetRemark(),
		OpSource:       req.GetOp().GetOpSource(),
		OpUserID:       req.GetOp().GetOpUserid(),
		OpUserName:     req.GetOp().GetOpUserName(),
		OpRemark:       req.GetOp().GetOpRemark(),
		Status:         byte(req.GetStatus()),
	}
	count, err := batch_job_biz.UpdateOneModel(ctx, v)
	if err != nil {
		log.Error(ctx, "AdminUpdateBiz call UpdateOneModel fail.", zap.Error(err))
		return nil, err
	}
	if count != 1 {
		err = fmt.Errorf("update biz fail. update count != 1. is %d", count)
		log.Error(ctx, "AdminUpdateBiz call batch_job_biz.UpdateOneModel fail.", zap.Error(err))
		return nil, err
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_biz_history.Model{
		BizId:          uint(req.GetBizId()),
		BizName:        req.GetBizName(),
		ExecType:       byte(req.GetExecType()),
		ExecExtendData: eed,
		Remark:         req.GetRemark(),
		OpSource:       req.GetOp().GetOpSource(),
		OpUserID:       req.GetOp().GetOpUserid(),
		OpUserName:     req.GetOp().GetOpUserName(),
		OpRemark:       req.GetOp().GetOpRemark(),
		Status:         byte(req.GetStatus()),
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_biz_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "AdminUpdateBiz call batch_job_biz_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.QueryAllBizName(), module.CacheKey.GetBizInfo(int(req.GetBizId())))
		if err != nil {
			log.Error(cloneCtx, "AdminUpdateBiz call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	return &pb.AdminUpdateBizRsp{}, nil
}

// 创建任务
func (*BatchJob) AdminCreateJob(ctx context.Context, req *pb.AdminCreateJobReq) (*pb.AdminCreateJobRsp, error) {
	// 不限速检查
	if !conf.Conf.AllowCreateNoRateLimitJob && req.GetRateSec() < 1 {
		return nil, errors.New("create tasks with no rate limit")
	}

	// 获取业务信息
	bizInfo, err := batch_job_biz.GetOneByBizId(ctx, int(req.GetBizId()))
	if err != nil {
		log.Error(ctx, "AdminStartJob call batch_job_biz.GetOneByBizId fail.", zap.Error(err))
		return nil, err
	}

	// 获取业务
	b, err := module.Biz.GetBiz(ctx, bizInfo)
	if err != nil {
		log.Error(ctx, "AdminCreateJob call GetBiz fail.", zap.Int32("bizId", req.GetBizId()), zap.Error(err))
		return nil, err
	}

	// 生成任务id
	jobIdKey := conf.Conf.JobIdGenKey
	rdb, err := db.GetRedis()
	if err != nil {
		return nil, err
	}
	jobId, err := rdb.Incr(ctx, jobIdKey).Result()
	if err != nil {
		log.Error(ctx, "AdminCreateJob call Incr fail.", zap.Error(err))
		return nil, err
	}

	// 构造jobInfo
	jobInfo := &batch_job_list.Model{
		JobID:            uint(jobId),
		JobName:          req.GetJobName(),
		BizId:            uint(req.GetBizId()),
		JobData:          req.GetJobData(),
		ProcessDataTotal: uint64(req.GetProcessDataTotal()),
		ProcessedCount:   uint64(req.GetProcessedCount()),
		Status:           byte(pb.JobStatus_JobStatus_Created),
		CreateTime:       time.Now(),
		UpdateTime:       time.Now(),
		OpSource:         req.GetOp().GetOpSource(),
		OpUserID:         req.GetOp().GetOpUserid(),
		OpUserName:       req.GetOp().GetOpUserName(),
		OpRemark:         req.GetOp().GetOpRemark(),
		RateSec:          uint(req.GetRateSec()),
		ConcType:         byte(req.GetConcType()),
		StatusInfo:       model.StatusInfo_UserOp,
	}
	if req.GetStartNow() {
		jobInfo.Status = byte(pb.JobStatus_JobStatus_Running)
		jobInfo.StatusInfo = model.StatusInfo_UserCreateAndRun
		if b.HasBeforeRunCallback() {
			jobInfo.Status = byte(pb.JobStatus_JobStatus_WaitBizRun)
		}
		jobInfo.ActivateTime = time.Now()
	}

	// 创建前回调
	args := &pb.JobBeforeCreateAndChangeReq{
		JobInfo: &pb.JobCBInfo{
			JobId:            jobId,
			JobName:          req.GetJobName(),
			BizId:            req.GetBizId(),
			BizName:          bizInfo.BizName,
			JobData:          req.GetJobData(),
			ProcessDataTotal: req.GetProcessDataTotal(),
			ProcessedCount:   req.GetProcessedCount(),
			ConcType:         req.GetConcType(),
			RateSec:          req.GetRateSec(),
		},
		IsCreate: true,
	}
	err = b.BeforeCreateAndChange(ctx, args)
	if err != nil {
		log.Error(ctx, "AdminCreateJob call biz.BeforeCreateAndChange fail.", zap.Error(err))
		return nil, err
	}

	// 写入数据库
	_, err = batch_job_list.CreateOneModel(ctx, jobInfo)
	if err != nil {
		log.Error(ctx, "AdminCreateJob call CreateOneModel fail.", zap.Error(err))
		return nil, err
	}

	handler.Trigger(ctx, handler.AfterCreateJob, jobInfo)

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:            uint(jobId),
		JobName:          req.GetJobName(),
		BizId:            uint(req.GetBizId()),
		JobData:          req.GetJobData(),
		ProcessDataTotal: uint64(req.GetProcessDataTotal()),
		ProcessedCount:   uint64(req.GetProcessedCount()),
		Status:           byte(pb.JobStatus_JobStatus_Created),
		OpSource:         req.GetOp().GetOpSource(),
		OpUserID:         req.GetOp().GetOpUserid(),
		OpUserName:       req.GetOp().GetOpUserName(),
		OpRemark:         req.GetOp().GetOpRemark(),
		RateSec:          uint(req.GetRateSec()),
		ConcType:         byte(req.GetConcType()),
		StatusInfo:       jobInfo.StatusInfo,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "AdminCreateJob call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(jobId)))
		if err != nil {
			log.Error(cloneCtx, "AdminCreateJob call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	// 立即启动
	if req.StartNow {
		gpool.GetDefGPool().Go(func() error {
			module.Job.CreateLauncherByData(cloneCtx, bizInfo, jobInfo)
			return nil
		}, nil)
	}

	return &pb.AdminCreateJobRsp{JobId: jobId}, nil
}

// 修改任务
func (*BatchJob) AdminUpdateJob(ctx context.Context, req *pb.AdminUpdateJobReq) (*pb.AdminUpdateJobRsp, error) {
	// 不限速检查
	if !conf.Conf.AllowCreateNoRateLimitJob && req.GetRateSec() < 1 {
		return nil, errors.New("create tasks with no rate limit")
	}

	// 加锁
	lockKey := conf.Conf.JobOpLockKeyPrefix + strconv.Itoa(int(req.GetJobId()))
	unlock, _, err := redis.AutoLock(ctx, lockKey, time.Second*10)
	if err != nil {
		log.Error(ctx, "AdminStartJob call AutoLock fail.", zap.Error(err))
		return nil, err
	}
	defer unlock()

	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, uint(req.GetJobId()))
	if err != nil {
		log.Error(ctx, "AdminUpdateJob call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}

	// 对于运行中的任务禁止修改
	switch pb.JobStatus(jobInfo.Status) {
	case pb.JobStatus_JobStatus_Created, pb.JobStatus_JobStatus_Stopped:
	default:
		log.Info(ctx, "AdminUpdateJob fail. status is not stopped", zap.Int64("jobId", req.GetJobId()))
		return &pb.AdminUpdateJobRsp{}, nil
	}

	// 获取业务信息
	bizInfo, err := batch_job_biz.GetOneByBizId(ctx, int(jobInfo.BizId))
	if err != nil {
		log.Error(ctx, "AdminStartJob call batch_job_biz.GetOneByBizId fail.", zap.Error(err))
		return nil, err
	}

	// 获取业务
	b, err := module.Biz.GetBiz(ctx, bizInfo)
	if err != nil {
		log.Error(ctx, "AdminCreateJob call GetBiz fail.", zap.Uint("bizId", jobInfo.BizId), zap.Error(err))
		return nil, err
	}

	// 修改前回调
	args := &pb.JobBeforeCreateAndChangeReq{
		JobInfo: &pb.JobCBInfo{
			JobId:            req.GetJobId(),
			JobName:          req.GetJobName(),
			BizId:            int32(jobInfo.BizId),
			BizName:          bizInfo.BizName,
			JobData:          req.GetJobData(),
			ProcessDataTotal: req.GetProcessDataTotal(),
			ProcessedCount:   req.GetProcessedCount(),
			ErrLogCount:      int64(jobInfo.ErrLogCount),
			ConcType:         req.GetConcType(),
			RateSec:          req.GetRateSec(),
		},
		IsCreate: false,
	}
	err = b.BeforeCreateAndChange(ctx, args)
	if err != nil {
		log.Error(ctx, "AdminUpdateJob call biz.BeforeCreateAndChange fail.", zap.Error(err))
		return nil, err
	}

	// 写入数据库
	v := &batch_job_list.Model{
		JobID:            uint(req.GetJobId()),
		JobName:          req.GetJobName(),
		BizId:            jobInfo.BizId,
		JobData:          req.GetJobData(),
		ProcessDataTotal: uint64(req.GetProcessDataTotal()),
		ProcessedCount:   uint64(req.GetProcessedCount()),
		ErrLogCount:      jobInfo.ErrLogCount,
		Status:           jobInfo.Status,
		CreateTime:       jobInfo.CreateTime,
		UpdateTime:       jobInfo.UpdateTime,
		OpSource:         req.GetOp().GetOpSource(),
		OpUserID:         req.GetOp().GetOpUserid(),
		OpUserName:       req.GetOp().GetOpUserName(),
		OpRemark:         req.GetOp().GetOpRemark(),
		RateSec:          uint(req.GetRateSec()),
		ConcType:         byte(req.GetConcType()),
		StatusInfo:       model.StatusInfo_UserOp,
		ActivateTime:     jobInfo.ActivateTime,
	}
	_, err = batch_job_list.AdminUpdateJob(ctx, v, jobInfo.Status)
	if err != nil {
		log.Error(ctx, "AdminUpdateJob call UpdateOneModelWhereStatus fail.", zap.Error(err))
		return nil, err
	}

	handler.Trigger(ctx, handler.AfterUpdateJob, v)

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:            uint(req.GetJobId()),
		JobName:          req.GetJobName(),
		BizId:            jobInfo.BizId,
		JobData:          req.GetJobData(),
		ProcessDataTotal: uint64(req.GetProcessDataTotal()),
		ProcessedCount:   uint64(req.GetProcessedCount()),
		Status:           jobInfo.Status,
		OpSource:         req.GetOp().GetOpSource(),
		OpUserID:         req.GetOp().GetOpUserid(),
		OpUserName:       req.GetOp().GetOpUserName(),
		OpRemark:         req.GetOp().GetOpRemark(),
		RateSec:          uint(req.GetRateSec()),
		ConcType:         byte(req.GetConcType()),
		StatusInfo:       model.StatusInfo_UserOp,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "AdminUpdateJob call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(req.GetJobId())))
		if err != nil {
			log.Error(cloneCtx, "AdminUpdateJob call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	return &pb.AdminUpdateJobRsp{}, nil
}

// 启动任务
func (*BatchJob) AdminStartJob(ctx context.Context, req *pb.AdminStartJobReq) (*pb.AdminStartJobRsp, error) {
	// 加锁
	lockKey := conf.Conf.JobOpLockKeyPrefix + strconv.Itoa(int(req.GetJobId()))
	unlock, _, err := redis.AutoLock(ctx, lockKey, time.Second*10)
	if err != nil {
		log.Error(ctx, "AdminStartJob call AutoLock fail.", zap.Error(err))
		return nil, err
	}
	defer unlock()

	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, uint(req.GetJobId()))
	if err != nil {
		log.Error(ctx, "AdminStartJob call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}

	// 检查任务状态
	switch pb.JobStatus(jobInfo.Status) {
	case pb.JobStatus_JobStatus_Running, pb.JobStatus_JobStatus_WaitBizRun:
		log.Info(ctx, "AdminStartJob fail. status is running", zap.Int64("jobId", req.GetJobId()))
		return &pb.AdminStartJobRsp{}, nil
	case pb.JobStatus_JobStatus_Stopping, pb.JobStatus_JobStatus_Finished:
		log.Error(ctx, "AdminStartJob fail. status is finished or stopping", zap.Int64("jobId", req.GetJobId()))
		return nil, errors.New("Job is finished or stopping")
	case pb.JobStatus_JobStatus_Created, pb.JobStatus_JobStatus_Stopped:
	default:
		log.Error(ctx, "AdminStartJob fail. status is unknown", zap.Int64("jobId", req.GetJobId()))
		return nil, errors.New("Job status is unknown")
	}

	// 获取业务信息
	bizInfo, err := batch_job_biz.GetOneByBizId(ctx, int(jobInfo.BizId))
	if err != nil {
		log.Error(ctx, "AdminStartJob call batch_job_biz.GetOneByBizId fail.", zap.Error(err))
		return nil, err
	}

	// 获取业务
	b, err := module.Biz.GetBiz(ctx, bizInfo)
	if err != nil {
		log.Error(ctx, "AdminStartJob call biz.GetBiz fail.", zap.Error(err))
		return nil, err
	}

	// 更新状态和操作人
	v := &batch_job_list.Model{
		JobID:        uint(req.GetJobId()),
		Status:       byte(pb.JobStatus_JobStatus_Running),
		OpSource:     req.GetOp().GetOpSource(),
		OpUserID:     req.GetOp().GetOpUserid(),
		OpUserName:   req.GetOp().GetOpUserName(),
		OpRemark:     req.GetOp().GetOpRemark(),
		StatusInfo:   model.StatusInfo_UserChangeStatus,
		ActivateTime: time.Now(),
	}
	if b.HasBeforeRunCallback() {
		v.Status = byte(pb.JobStatus_JobStatus_WaitBizRun)
	}
	count, err := batch_job_list.AdminUpdateStatus(ctx, v, jobInfo.Status)
	if err != nil {
		log.Error(ctx, "AdminStartJob call AdminUpdateStatus fail.", zap.Error(err))
		return nil, err
	}
	if count != 1 {
		err = fmt.Errorf("update job status fail. update count != 1. is %d", count)
		log.Error(ctx, "AdminStartJob call batch_job_biz.AdminUpdateStatus fail.", zap.Error(err))
		return nil, err
	}

	// 更新jobInfo数据
	jobInfo.Status = v.Status
	jobInfo.OpSource = req.GetOp().GetOpSource()
	jobInfo.OpUserID = req.GetOp().GetOpUserid()
	jobInfo.OpUserName = req.GetOp().GetOpUserName()
	jobInfo.OpRemark = req.GetOp().GetOpRemark()
	jobInfo.StatusInfo = model.StatusInfo_UserChangeStatus
	jobInfo.ActivateTime = time.Now()

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:      uint(req.GetJobId()),
		BizId:      jobInfo.BizId,
		Status:     byte(pb.JobStatus_JobStatus_Running),
		OpSource:   req.GetOp().GetOpSource(),
		OpUserID:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		StatusInfo: model.StatusInfo_UserChangeStatus,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "AdminStartJob call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(req.GetJobId())))
		if err != nil {
			log.Error(cloneCtx, "AdminStartJob call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	// 启动
	gpool.GetDefGPool().Go(func() error {
		// 删除停止标记
		err = module.Job.SetStopFlag(cloneCtx, int(jobInfo.JobID), false)
		if err != nil {
			log.Error(cloneCtx, "AdminStartJob call DelStopFlag fail.", zap.Error(err))
			return err
		}
		// 创建启动器
		module.Job.CreateLauncherByData(cloneCtx, bizInfo, jobInfo)
		return nil
	}, nil)

	return &pb.AdminStartJobRsp{}, nil
}

// 停止任务
func (b *BatchJob) AdminStopJob(ctx context.Context, req *pb.AdminStopJobReq) (*pb.AdminStopJobRsp, error) {
	// 加锁
	lockKey := conf.Conf.JobOpLockKeyPrefix + strconv.Itoa(int(req.GetJobId()))
	unlock, _, err := redis.AutoLock(ctx, lockKey, time.Second*10)
	if err != nil {
		log.Error(ctx, "AdminStopJob call AutoLock fail.", zap.Error(err))
		return nil, err
	}
	defer unlock()

	// 获取任务信息
	jobInfo, err := batch_job_list.GetOneByJobId(ctx, uint(req.GetJobId()))
	if err != nil {
		log.Error(ctx, "AdminStopJob call batch_job_list.GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}

	// 检查任务状态
	switch pb.JobStatus(jobInfo.Status) {
	case pb.JobStatus_JobStatus_WaitBizRun, pb.JobStatus_JobStatus_Running:
	default:
		log.Info(ctx, "AdminStopJob fail. status is stopped", zap.Int64("jobId", req.GetJobId()))
		return &pb.AdminStopJobRsp{}, nil
	}

	// 写入停止标记
	err = module.Job.SetStopFlag(ctx, int(req.GetJobId()), true)
	if err != nil {
		log.Error(ctx, "AdminStopJob call SetStopFlag fail.", zap.Error(err))
		return nil, err
	}

	// 更新状态和操作人
	v := &batch_job_list.Model{
		JobID:        uint(req.GetJobId()),
		Status:       byte(pb.JobStatus_JobStatus_Stopping),
		OpSource:     req.GetOp().GetOpSource(),
		OpUserID:     req.GetOp().GetOpUserid(),
		OpUserName:   req.GetOp().GetOpUserName(),
		OpRemark:     req.GetOp().GetOpRemark(),
		StatusInfo:   model.StatusInfo_UserChangeStatus,
		ActivateTime: time.Now(),
	}
	// 尝试获取运行锁
	runLockKey := module.CacheKey.GetRunLockKey(int(jobInfo.JobID))
	runLockAuthCode, err := redis.Lock(ctx, runLockKey, time.Duration(conf.Conf.JobRunLockExtraTtl)*time.Second)
	if err == nil { // 加锁成功
		v.Status = byte(pb.JobStatus_JobStatus_Stopped)
		defer b.adminStopJobCB(ctx, jobInfo, runLockKey, runLockAuthCode)
	} else if err != redis.LockManyErr { // 加锁异常
		log.Error(ctx, "AdminStopJob call set run lock fail.", zap.Error(err))
		return nil, err
	}

	count, err := batch_job_list.AdminUpdateStatus(ctx, v, jobInfo.Status)
	if err != nil {
		log.Error(ctx, "AdminStopJob call AdminUpdateStatus fail.", zap.Error(err))
		return nil, err
	}
	if count != 1 {
		err = fmt.Errorf("update job status fail. update count != 1. is %d", count)
		log.Error(ctx, "AdminStopJob call batch_job_biz.AdminUpdateStatus fail.", zap.Error(err))
		return nil, err
	}

	// 更新jobInfo数据
	jobInfo.Status = v.Status
	jobInfo.OpSource = req.GetOp().GetOpSource()
	jobInfo.OpUserID = req.GetOp().GetOpUserid()
	jobInfo.OpUserName = req.GetOp().GetOpUserName()
	jobInfo.OpRemark = req.GetOp().GetOpRemark()
	jobInfo.StatusInfo = model.StatusInfo_UserChangeStatus
	jobInfo.ActivateTime = time.Now()

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &batch_job_list_history.Model{
		JobID:      uint(req.GetJobId()),
		BizId:      jobInfo.BizId,
		Status:     byte(pb.JobStatus_JobStatus_Stopped),
		OpSource:   req.GetOp().GetOpSource(),
		OpUserID:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		StatusInfo: model.StatusInfo_UserChangeStatus,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = batch_job_list_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "AdminStopJob call batch_job_list_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetJobInfo(int(req.GetJobId())))
		if err != nil {
			log.Error(cloneCtx, "AdminStopJob call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	return &pb.AdminStopJobRsp{}, nil
}

func (*BatchJob) adminStopJobCB(ctx context.Context, jobInfo *batch_job_list.Model, lockKey, authCode string) {
	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 对于加锁成功, 主动停止时注意业务回调
	gpool.GetDefGPool().Go(func() error {
		_ = redis.UnLock(cloneCtx, lockKey, authCode)
		// 获取业务信息
		bizInfo, err := module.Biz.GetBizInfoByCache(cloneCtx, int(jobInfo.BizId))
		if err != nil {
			log.Error(cloneCtx, "AdminStopJob call GetBizInfoByCache fail.", zap.Error(err))
			return err
		}

		// 获取业务
		b, err := module.Biz.GetBiz(cloneCtx, bizInfo)
		if err != nil {
			log.Error(cloneCtx, "AdminStopJob call biz.GetBiz fail.", zap.Error(err))
			return err
		}
		// 回调
		err = b.ProcessStop(cloneCtx, jobInfo, false)
		if err != nil {
			log.Error(cloneCtx, "AdminStopJob call ProcessStop fail.", zap.Error(err))
			return err
		}

		handler.Trigger(ctx, handler.AfterJobStopped, jobInfo)

		return nil
	}, nil)
}
