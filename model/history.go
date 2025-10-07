package model

import (
	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
	"github.com/zlyuancn/batch_job/dao/batch_job_list"
)

// 历史操作信息
type BizHistoryOpInfo struct {
	OpTime               int64 // 操作时间. 秒级时间戳
	*batch_job_biz.Model       // 数据
}

// 历史操作记录
type BizHistoryOpInfos []BizHistoryOpInfo

// 历史操作信息
type JobHistoryOpInfo struct {
	OpTime                int64 // 操作时间. 秒级时间戳
	*batch_job_list.Model       // 数据
}

// 历史操作记录
type JobHistoryOpInfos []JobHistoryOpInfo

// 操作信息
const (
	StatusInfo_UserOp           = "user op"
	StatusInfo_UserCreateAndRun = "user create and run"
)
