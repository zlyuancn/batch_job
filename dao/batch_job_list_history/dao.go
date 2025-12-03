package batch_job_list_history

import (
	"context"
	"errors"
	"time"

	"github.com/didi/gendry/builder"
	"github.com/zly-app/zapp/log"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/client/db"
)

const (
	tableName = "batch_job_list_history"
)

type Model struct {
	JobID            uint      `db:"job_id"`             // "任务号"
	JobName          string    `db:"job_name"`           // "任务名称"
	BizId            uint      `db:"biz_id"`             // "业务id"
	JobData          string    `db:"job_data"`           // "任务数据, 让业务知道应该做什么"
	ProcessDataTotal uint64    `db:"process_data_total"` // "需要处理数据总数"
	ProcessedCount   uint64    `db:"processed_count"`    // "已处理过的数据量, 无论成功还是失败. 如果任务在运行中, 则真实进度存在于redis"
	Status           byte      `db:"status"`             // "任务状态 0=已创建 1=等待业务主动启动 2=运行中 3=已完成 4=正在停止 5=已停止"
	UpdateTime       time.Time `db:"update_time"`
	OpSource         string    `db:"op_source"`    // "最后操作来源"
	OpUserID         string    `db:"op_user_id"`   // "最后操作用户id"
	OpUserName       string    `db:"op_user_name"` // "最后操作用户名"
	OpRemark         string    `db:"op_remark"`    // "最后操作备注"
	RateSec          uint      `db:"rate_sec"`     // "每秒处理速率. 0表示不限制"
	ConcType         byte      `db:"conc_type"`    // "并发类型. 0=并行, 1=串行化"
	StatusInfo       string    `db:"status_info"`  // "状态信息"
}

func CreateOneModel(ctx context.Context, v *Model) (int64, error) {
	if v == nil {
		return 0, errors.New("CreateOneModel v is empty")
	}

	var data []map[string]any
	data = append(data, map[string]any{
		"job_id":             v.JobID,
		"job_name":           v.JobName,
		"biz_id":             v.BizId,
		"job_data":           v.JobData,
		"process_data_total": v.ProcessDataTotal,
		"processed_count":    v.ProcessedCount,
		"status":             v.Status,
		"op_source":          v.OpSource,
		"op_user_id":         v.OpUserID,
		"op_user_name":       v.OpUserName,
		"op_remark":          v.OpRemark,
		"status_info":        v.StatusInfo,
		"rate_sec":           v.RateSec,
		"conc_type":          v.ConcType,
	})
	cond, vals, err := builder.BuildInsert(tableName, data)
	if err != nil {
		log.Error(ctx, "CreateOneModel BuildSelect err",
			zap.Any("data", data),
			zap.Error(err),
		)
		return 0, err
	}

	result, err := db.GetSqlx().Exec(ctx, cond, vals...)
	if err != nil {
		log.Error(ctx, "CreateOneModel fail.", zap.Any("data", data), zap.Error(err))
		return 0, err
	}
	return result.LastInsertId()
}
