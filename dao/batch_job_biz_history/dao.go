package batch_job_biz_history

import (
	"context"
	"errors"
	"time"

	"github.com/didi/gendry/builder"
	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/client/db"
)

const (
	tableName = "batch_job_biz_history"
)

type Model struct {
	BizId          uint      `db:"biz_id"`           // "业务id"
	BizName        string    `db:"biz_name"`         // "业务名"
	ExecType       byte      `db:"exec_type"`        // "执行类型"
	ExecExtendData string    `db:"exec_extend_data"` // "执行器扩展数据"
	Remark         string    `db:"remark"`           // "备注"
	UpdateTime     time.Time `db:"update_time"`
	OpSource       string    `db:"op_source"`    // "最后操作来源"
	OpUserID       string    `db:"op_user_id"`   // "最后操作用户id"
	OpUserName     string    `db:"op_user_name"` // "最后操作用户名"
	OpRemark       string    `db:"op_remark"`    // "最后操作备注"
	Status         byte      `db:"status"`       // "状态 0=正常 1=隐藏"
}

func CreateOneModel(ctx context.Context, v *Model) (int64, error) {
	if v == nil {
		return 0, errors.New("CreateOneModel v is empty")
	}

	var data []map[string]any
	data = append(data, map[string]any{
		"biz_id":           v.BizId,
		"biz_name":         v.BizName,
		"exec_type":        v.ExecType,
		"exec_extend_data": v.ExecExtendData,
		"remark":           v.Remark,
		"op_source":        v.OpSource,
		"op_user_id":       v.OpUserID,
		"op_user_name":     v.OpUserName,
		"op_remark":        v.OpRemark,
	})
	cond, vals, err := builder.BuildInsert(tableName, data)
	if err != nil {
		logger.Log.Error(ctx, "CreateOneModel BuildSelect err",
			zap.Any("data", data),
			zap.Error(err),
		)
		return 0, err
	}

	result, err := db.GetSqlx().Exec(ctx, cond, vals...)
	if err != nil {
		logger.Error(ctx, "CreateOneModel fail.", zap.Any("data", data), zap.Error(err))
		return 0, err
	}
	return result.LastInsertId()
}
