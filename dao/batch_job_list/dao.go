package batch_job_list

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/didi/gendry/builder"
	"github.com/zly-app/zapp/logger"
	"go.uber.org/zap"

	"github.com/zlyuancn/batch_job/client/db"
)

var (
	// 默认select当前表所有字段；
	// 如果在select时候传nil，会采用select * 执行
	// 这种方式会在库表新增字段的时候，由于两边结构字段未对齐而抛错！
	selectField = func() []string {
		var selectAllFields []string
		for _, field := range reflect.
			VisibleFields(reflect.TypeOf(Model{})) {
			// 拿到所有db字段
			if field.Tag.Get("db") != "" {
				selectAllFields = append(selectAllFields, field.Tag.Get("db"))
			}
		}
		return selectAllFields
	}()
	// selectField = []string{
	//	"id",
	//	"job_id",
	//	"biz_type",
	//	"biz_data",
	//	"process_data_total",
	//	"processed_count",
	//	"err_log_count",
	//	"status",
	//	"create_time",
	//	"update_time",
	//	"last_op_source",
	//	"last_op_user_id",
	//	"last_op_user_name",
	//	"status_info",
	//	"op_history",
	//	"biz_process_data",
	// }
)

const (
	tableName = "batch_job_list"
)

type Model struct {
	ID               uint64    `db:"id"`
	JobID            uint64    `db:"job_id"`             // "任务号"
	BizType          uint      `db:"biz_type"`           // "业务类型"
	BizData          string    `db:"biz_data"`           // "业务任务数据, 让业务知道应该做什么"
	ProcessDataTotal uint64    `db:"process_data_total"` // "需要处理数据总数"
	ProcessedCount   uint64    `db:"processed_count"`    // "已处理过的数据量, 无论成功还是失败. 如果任务在运行中, 则真实进度存在于redis"
	ErrLogCount      uint64    `db:"err_log_count"`      // "错误日志数"
	Status           byte      `db:"status"`             // "任务状态 0=已创建 1=等待业务主动启动 2=运行中 3=已完成 4=正在停止 5=已停止"
	CreateTime       time.Time `db:"create_time"`
	UpdateTime       time.Time `db:"update_time"`
	LastOpSource     string    `db:"last_op_source"`    // "最后操作来源"
	LastOpUserID     string    `db:"last_op_user_id"`   // "最后操作用户id"
	LastOpUserName   string    `db:"last_op_user_name"` // "最后操作用户名"
	StatusInfo       string    `db:"status_info"`       // "状态信息"
	OpHistory        string    `db:"op_history"`        // "操作历史信息"
	BizProcessData   string    `db:"biz_process_data"`  // "业务中需要处理的批量数据"
}

func CreateOneModel(ctx context.Context, v *Model) (int64, error) {
	if v == nil {
		return 0, errors.New("CreateOneModel v is empty")
	}

	var data []map[string]any
	data = append(data, map[string]any{
		"job_id":             v.JobID,
		"biz_type":           v.BizType,
		"biz_data":           v.BizData,
		"process_data_total": v.ProcessDataTotal,
		"processed_count":    v.ProcessedCount,
		"err_log_count":      v.ErrLogCount,
		"status":             v.Status,
		"last_op_source":     v.LastOpSource,
		"last_op_user_id":    v.LastOpUserID,
		"last_op_user_name":  v.LastOpUserName,
		"status_info":        v.StatusInfo,
		"op_history":         v.OpHistory,
		"biz_process_data":   v.BizProcessData,
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

func GetOne(ctx context.Context, where map[string]any) (*Model, error) {
	if where == nil {
		where = map[string]any{}
	}
	where["_limit"] = []uint{1} // 限制只查询一条记录
	cond, vals, err := builder.BuildSelect(tableName, where, selectField)
	if err != nil {
		logger.Error(ctx, "GetOne BuildSelect fail.", zap.Any("where", where), zap.Error(err))
		return nil, err
	}
	ret := Model{}
	err = db.GetSqlx().FindOne(ctx, &ret, cond, vals...)
	if err != nil {
		logger.Error(ctx, "GetOne FindOne fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return nil, err
	}
	return &ret, nil
}

func GetOneByJobId(ctx context.Context, jobId int64) (*Model, error) {
	where := map[string]interface{}{
		"job_id": jobId,
	}
	v, err := GetOne(ctx, where)
	if err != nil {
		logger.Error(ctx, "GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}
	return v, nil
}

// 更新状态和操作人
func UpdateStatus(ctx context.Context, jobId int64, oldStatus, status byte, opSource, opUserId, opUserName, statusInfo, historyOpInfo string) (int64, error) {
	const cond = `
update batch_job_list
set status=?,
    last_op_source=?,
    last_op_user_id=?,
    last_op_user_name=?,
    update_time=now(),
    history_op_info=json_array_insert(history_op_info, '$[0]', json_extract(?, '$')),
    status_info=?
where job_id = ?
    and oldStatus = ?
limit 1;`
	vals := []interface{}{
		status,
		opSource,
		opUserId,
		opUserName,
		historyOpInfo,
		statusInfo,
		jobId,
		oldStatus,
	}
	result, err := db.GetSqlx().Exec(ctx, cond, vals...)
	if nil != err {
		logger.Error(ctx, "UpdateStatus fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return 0, err
	}
	return result.RowsAffected()
}
