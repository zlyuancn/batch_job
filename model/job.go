package model

type JobInfo struct {
	JobId int64
}

// 历史操作信息
type HistoryOpInfo struct {
	OpSource   string    // 操作来源
	OpUserId   string // 操作用户id
	OpUserName string // 操作用户名
	OpStatus   int32 // 操作状态
	OldStatus  int32 // 旧状态
	OpTime     int64  // 操作时间. 秒级时间戳
	Remark     string // 备注
}

// 历史操作记录
type HistoryOpInfos []HistoryOpInfo

// 操作信息
const(
	StatusInfo_UserOp = "user op"
)
