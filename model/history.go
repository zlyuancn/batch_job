package model

import (
	"github.com/zlyuancn/batch_job/dao/batch_job_biz"
)

// 历史操作信息
type BizHistoryOpInfo struct {
	OpSource   string               // 操作来源
	OpUserId   string               // 操作用户id
	OpUserName string               // 操作用户名
	OpTime     int64                // 操作时间. 秒级时间戳
	OpRemark   string               // 操作备注
	NewData    *batch_job_biz.Model // 新数据
}

// 历史操作记录
type BizHistoryOpInfos []BizHistoryOpInfo

// 历史操作信息
type JobHistoryOpInfo struct {
	OpSource   string // 操作来源
	OpUserId   string // 操作用户id
	OpUserName string // 操作用户名
	OpTime     int64  // 操作时间. 秒级时间戳
	OpRemark   string // 操作备注
	NewStatus  byte   // 新状态
}

// 历史操作记录
type JobHistoryOpInfos []JobHistoryOpInfo

// 操作信息
const (
	StatusInfo_UserOp           = "user op"
	StatusInfo_UserCreateAndRun = "user create and run"
)
