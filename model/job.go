package model

import (
	"github.com/zlyuancn/batch_job/pb"
)

type JobInfo struct {
	JobId            int64        // "任务号"
	BizId          int32        // "业务id"
	JobData          string       // "任务数据, 让业务知道应该做什么"
	ProcessDataTotal int          // "业务中需要处理数据总数"
	ProcessedNum     int          // "已处理过的数据量."
	ErrLogNum        int          // "错误日志数"
	Status           pb.JobStatus // "任务状态 0=未进行 1=进行中 2=已完成 3=已取消"
	CreateTime       int64        // 创建秒级时间戳
	UpdateTime       int64        // 更新秒级时间戳
	StatusInfo       string       // "状态信息"
}
