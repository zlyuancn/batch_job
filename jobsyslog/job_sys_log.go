package jobsyslog

import (
	"context"
	"strings"
	"time"

	"github.com/zly-app/zapp"
	"github.com/zly-app/zapp/core"
	"github.com/zly-app/zapp/handler"
	"github.com/zly-app/zapp/log"
	"github.com/zlyuancn/rotate"
	"github.com/zlyuancn/zretry"

	"github.com/zlyuancn/batch_job/conf"
	"github.com/zlyuancn/batch_job/dao/batch_job_log"
	"github.com/zlyuancn/batch_job/pb"
)

var jsl *jobSysLog

type jobSysLog struct {
	r     rotate.IRotator[*batch_job_log.Model]
	level byte
	write bool
}

func Init() {
	jsl = &jobSysLog{}

	if !conf.Conf.JobSysLogWriteDatabase {
		return
	}

	jsl.write = true

	c := &rotate.RotateConf{
		BatchSize:       conf.Conf.JobSysLogBatchSize,
		AutoRotateTime:  time.Duration(conf.Conf.JobSysLogAutoRotateTimeSec) * time.Second,
		CloseAutoRotate: conf.Conf.JobSysLogCloseAutoRotate,
	}
	jsl.r = rotate.NewRotateWithConf(c, func(values []*batch_job_log.Model) {
		_ = zretry.DoRetry(conf.Conf.JobSysLogFlushAttemptCount, time.Duration(conf.Conf.JobSysLogFlushErrIntervalSec)*time.Second,
			func() error {
				return flush(values)
			}, func(nowAttemptCount, remainCount int, err error) {
				if remainCount == 0 {
					log.Warn("jobSysLog write fail. wait retry. nowAttemptCount=%d err:", nowAttemptCount, err)
				} else {
					log.Error("jobSysLog write fail. nowAttemptCount=%d err:", nowAttemptCount, err)
				}
			})
	})

	// app退出前主动轮转
	zapp.AddHandler(zapp.AfterCloseService, func(app core.IApp, handlerType handler.HandlerType) {
		jsl.r.Rotate()
	})

	// 等级限制
	level := byte(pb.DataLogType_DataLogType_SysInfo)
	switch strings.ToLower(conf.Conf.JobSysLogWriteLevel) {
	case "sysinfo":
		level = byte(pb.DataLogType_DataLogType_SysInfo)
	case "syswarn":
		level = byte(pb.DataLogType_DataLogType_SysWarn)
	case "syserr":
		level = byte(pb.DataLogType_DataLogType_SysErr)
	}
	jsl.level = level
}

func flush(values []*batch_job_log.Model) error {
	_, err := batch_job_log.MultiSave(context.Background(), values)
	return err
}

func Log(v *batch_job_log.Model) {
	if !jsl.write || v.LogType >= jsl.level {
		return
	}
	jsl.r.Add(v)
}
