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
	//	"last_op_remark",
	//	"status_info",
	//	"op_history",
	//	"rate_sec",
	//	"rate_type",
	// }
)

const (
	tableName = "batch_job_list"
)

type Model struct {
	ID               uint64    `db:"id" json:"-"`
	JobID            uint64    `db:"job_id" json:"-"`        // "任务号"
	BizType          uint      `db:"biz_type" json:"-"`      // "业务类型"
	BizData          string    `db:"biz_data"`               // "业务任务数据, 让业务知道应该做什么"
	ProcessDataTotal uint64    `db:"process_data_total"`     // "需要处理数据总数"
	ProcessedCount   uint64    `db:"processed_count"`        // "已处理过的数据量, 无论成功还是失败. 如果任务在运行中, 则真实进度存在于redis"
	ErrLogCount      uint64    `db:"err_log_count" json:"-"` // "错误日志数"
	Status           byte      `db:"status"`                 // "任务状态 0=已创建 1=等待业务主动启动 2=运行中 3=已完成 4=正在停止 5=已停止"
	CreateTime       time.Time `db:"create_time" json:"-"`
	UpdateTime       time.Time `db:"update_time" json:"-"`
	LastOpSource     string    `db:"last_op_source" json:"-"`    // "最后操作来源"
	LastOpUserID     string    `db:"last_op_user_id" json:"-"`   // "最后操作用户id"
	LastOpUserName   string    `db:"last_op_user_name" json:"-"` // "最后操作用户名"
	LastOpRemark     string    `db:"last_op_remark" json:"-"`    // "最后操作备注"
	OpHistory        string    `db:"op_history" json:"-"`        // "操作历史信息"
	RateSec          uint      `db:"rate_sec"`                   // "每秒处理速率. 0表示不限制"
	RateType         byte      `db:"rate_type"`                  // "速率类型. 0=通过rate_sec限速, 1=串行化"
	StatusInfo       string    `db:"status_info"`                // "状态信息"
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
		"rate_sec":           v.RateSec,
		"rate_type":          v.RateType,
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

func UpdateOneModel(ctx context.Context, v *Model) (int64, error) {
	if v == nil {
		return 0, errors.New("UpdateOneModel v is empty")
	}
	if v.BizType == 0 {
		return 0, errors.New("UpdateOneModel BizType is empty")
	}
	const cond = `
update batch_job_list
set 
    biz_data=?,
    process_data_total=?,
    processed_count=?,
    status=?,
    update_time=now(),
    last_op_source=?,
    last_op_user_id=?,
    last_op_user_name=?,
    last_op_remark=?,
    status_info=?,
    op_history=json_array_insert(op_history, '$[0]', json_extract(?, '$')),
    rate_sec=?,
    rate_type=?
where job_id = ?
limit 1;`
	vals := []interface{}{
		v.BizData,
		v.ProcessDataTotal,
		v.ProcessedCount,
		v.Status,
		v.LastOpSource,
		v.LastOpUserID,
		v.LastOpUserName,
		v.LastOpRemark,
		v.StatusInfo,
		v.OpHistory,
		v.RateSec,
		v.RateType,
		v.JobID,
	}
	result, err := db.GetSqlx().Exec(ctx, cond, vals...)
	if nil != err {
		logger.Error(ctx, "UpdateOneModel fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return 0, err
	}
	return result.RowsAffected()
}

// 仅更新状态和操作人相关信息
func UpdateStatus(ctx context.Context, v *Model) (int64, error) {
	if v == nil {
		return 0, errors.New("UpdateStatus v is empty")
	}
	if v.JobID == 0 {
		return 0, errors.New("UpdateStatus JobID is empty")
	}
	const cond = `
update batch_job_list
set 
    status=?,
    update_time=now(),
    last_op_source=?,
    last_op_user_id=?,
    last_op_user_name=?,
    last_op_remark=?,
    status_info=?,
    op_history=json_array_insert(op_history, '$[0]', json_extract(?, '$'))
where job_id = ?
limit 1;`
	vals := []interface{}{
		v.Status,
		v.LastOpSource,
		v.LastOpUserID,
		v.LastOpUserName,
		v.LastOpRemark,
		v.StatusInfo,
		v.OpHistory,
		v.JobID,
	}
	result, err := db.GetSqlx().Exec(ctx, cond, vals...)
	if nil != err {
		logger.Error(ctx, "UpdateStatus fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return 0, err
	}
	return result.RowsAffected()
}
