package module

import (
	"context"
	"fmt"
	rawHttp "net/http"
	"strconv"
	"time"

	"github.com/zly-app/component/http"
	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
	"github.com/zlyuancn/batch_job/pb"
)

type httpCallbackBiz struct {
	biz     *batch_job_biz.Model
	eed     *pb.ExecExtendDataA
	headers http.Header
}

func (h *httpCallbackBiz) GetBizInfo() *batch_job_biz.Model {
	return h.biz
}

func (h *httpCallbackBiz) GetExecExtendData() *pb.ExecExtendDataA {
	return h.eed
}

func (h *httpCallbackBiz) HasBeforeRunCallback() bool {
	return h.eed.GetHttpCallback().GetBeforeRun() != ""
}

func (h *httpCallbackBiz) GetBeforeRunTimeout() time.Duration {
	return time.Duration(h.eed.GetHttpCallback().GetBeforeRunTimeout()) * time.Second
}

func (h *httpCallbackBiz) BeforeCreateAndChange(ctx context.Context, args *pb.JobBeforeCreateAndChangeReq) error {
	if h.eed.GetHttpCallback().GetBeforeCreate() == "" {
		return nil
	}

	rsp := &pb.JobBeforeCreateAndChangeRsp{}

	timeout := time.Duration(h.eed.GetHttpCallback().GetBeforeCreateTimeout()) * time.Second
	opts := []http.Option{http.WithInJson(args), http.WithOutJson(rsp), http.WithTimeout(timeout)}
	if h.eed.GetHttpCallback().InsecureSkipVerify {
		opts = append(opts, http.WithInsecureSkipVerify())
	}
	if len(h.headers) > 0 {
		opts = append(opts, http.WithInHeader(h.headers))
	}

	// 创建/修改前回调
	c := http.NewClient("job" + strconv.Itoa(int(args.JobId)))
	sp, err := c.Post(ctx, h.eed.GetHttpCallback().GetBeforeCreate(), nil, opts...)
	if err != nil {
		logger.Error(ctx, "BeforeCreateAndChange call http fail.", zap.Error(err))
		return err
	}

	// 检查状态码
	if sp.StatusCode != rawHttp.StatusOK {
		err = fmt.Errorf("http StatusCode not ok. is %d", sp.StatusCode)
		logger.Error(ctx, "BeforeCreateAndChange call http fail.", zap.Error(err))
		return err
	}
	return nil
}

func (h *httpCallbackBiz) BeforeRun(ctx context.Context, args *pb.JobBeforeRunReq) {
	if h.eed.GetHttpCallback().GetBeforeRun() == "" {
		return
	}

	rsp := &pb.JobBeforeRunRsp{}

	timeout := time.Duration(h.eed.GetHttpCallback().GetBeforeRunTimeout()) * time.Second
	opts := []http.Option{http.WithInJson(args), http.WithOutJson(rsp), http.WithTimeout(timeout)}
	if h.eed.GetHttpCallback().InsecureSkipVerify {
		opts = append(opts, http.WithInsecureSkipVerify())
	}
	if len(h.headers) > 0 {
		opts = append(opts, http.WithInHeader(h.headers))
	}

	// 运行前回调
	c := http.NewClient("job" + strconv.Itoa(int(args.JobId)))
	sp, err := c.Post(ctx, h.eed.GetHttpCallback().GetBeforeRun(), nil, opts...)
	if err != nil {
		logger.Error(ctx, "BeforeRun call http fail.", zap.Error(err))
		return
	}

	// 检查状态码
	if sp.StatusCode != rawHttp.StatusOK {
		err = fmt.Errorf("http StatusCode not ok. is %d", sp.StatusCode)
		logger.Error(ctx, "BeforeRun call http fail.", zap.Error(err))
		return
	}
	return
}

func (h *httpCallbackBiz) Process(ctx context.Context, jobInfo *batch_job_list.Model, dataIndex int64, attemptCount int) error {
	args := &pb.JobProcessReq{
		JobId:        int64(jobInfo.JobID),
		DataIndex:    dataIndex,
		AttemptCount: int32(attemptCount),
	}
	rsp := &pb.JobProcessRsp{}

	timeout := time.Duration(h.eed.GetHttpCallback().GetProcessTimeout()) * time.Second
	opts := []http.Option{http.WithInJson(args), http.WithOutJson(rsp), http.WithTimeout(timeout)}
	if h.eed.GetHttpCallback().InsecureSkipVerify {
		opts = append(opts, http.WithInsecureSkipVerify())
	}
	if len(h.headers) > 0 {
		opts = append(opts, http.WithInHeader(h.headers))
	}

	// 处理数据
	c := http.NewClient("job" + strconv.Itoa(int(jobInfo.JobID)))
	sp, err := c.Post(ctx, h.eed.GetHttpCallback().GetProcess(), nil, opts...)
	if err != nil {
		logger.Error(ctx, "Process call http fail.", zap.Error(err))
		return err
	}

	// 检查状态码
	if sp.StatusCode != rawHttp.StatusOK {
		err = fmt.Errorf("http StatusCode not ok. is %d", sp.StatusCode)
		logger.Error(ctx, "Process call http fail.", zap.Error(err))
		return err
	}
	return nil
}

func (h *httpCallbackBiz) ProcessStop(ctx context.Context, jobInfo *batch_job_list.Model, isFinished bool) error {
	if h.eed.GetHttpCallback().GetProcessStop() == "" {
		return nil
	}

	args := &pb.JobProcessStopReq{
		JobId:            int64(jobInfo.JobID),
		JobName:          jobInfo.JobName,
		BizId:            int32(jobInfo.BizId),
		BizName:          h.biz.BizName,
		JobData:          jobInfo.JobData,
		ProcessDataTotal: int64(jobInfo.ProcessDataTotal),
		ProcessedCount:   int64(jobInfo.ProcessedCount),
		IsFinished:       isFinished,
	}
	rsp := &pb.JobProcessStopRsp{}

	timeout := time.Duration(h.eed.GetHttpCallback().GetProcessStopTimeout()) * time.Second
	opts := []http.Option{http.WithInJson(args), http.WithOutJson(rsp), http.WithTimeout(timeout)}
	if h.eed.GetHttpCallback().InsecureSkipVerify {
		opts = append(opts, http.WithInsecureSkipVerify())
	}
	if len(h.headers) > 0 {
		opts = append(opts, http.WithInHeader(h.headers))
	}

	// 停止时回调
	c := http.NewClient("job" + strconv.Itoa(int(args.JobId)))
	sp, err := c.Post(ctx, h.eed.GetHttpCallback().GetProcessStop(), nil, opts...)
	if err != nil {
		logger.Error(ctx, "ProcessStop call http fail.", zap.Error(err))
		return err
	}

	// 检查状态码
	if sp.StatusCode != rawHttp.StatusOK {
		err = fmt.Errorf("http StatusCode not ok. is %d", sp.StatusCode)
		logger.Error(ctx, "ProcessStop call http fail.", zap.Error(err))
		return err
	}
	return nil
}

func newHttpCallbackBiz(ctx context.Context, biz *batch_job_biz.Model, eed *pb.ExecExtendDataA) (Business, error) {
	h := &httpCallbackBiz{
		biz: biz,
		eed: eed,
	}

	// 预构建header
	if len(eed.GetHttpCallback().GetHeaders()) > 0 {
		h.headers = make(http.Header, len(eed.GetHttpCallback().GetHeaders()))
		for _, kv := range eed.GetHttpCallback().GetHeaders() {
			h.headers.Add(kv.GetV(), kv.GetV())
		}
	}
	return h, nil
}
