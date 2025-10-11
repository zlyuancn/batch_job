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

	selectBaseField = []string{
		"job_id",
		"biz_type",
		"job_name",
		"job_data",
		"process_data_total",
		"processed_count",
		"err_log_count",
		"status",
		"create_time",
		"update_time",
		"last_op_source",
		"last_op_user_id",
		"last_op_user_name",
		"last_op_remark",
		"status_info",
		"rate_sec",
		"rate_type",
	}

	selectFieldByQueryList = []string{
		"job_id",
		"biz_type",
		"job_name",
		"process_data_total",
		"processed_count",
		"err_log_count",
		"status",
		"create_time",
		"update_time",
		"last_op_source",
		"last_op_user_id",
		"last_op_user_name",
		"last_op_remark",
		"status_info",
		"rate_sec",
		"rate_type",
	}
)

const (
	tableName = "batch_job_list"
)

type Model struct {
	ID               uint      `db:"id" json:"-"`
	JobID            uint      `db:"job_id" json:"-"`        // "任务号"
	JobName          string    `db:"job_name"`               // "任务名称"
	BizType          uint      `db:"biz_type" json:"-"`      // "业务类型"
	JobData          string    `db:"job_data"`               // "任务数据, 让业务知道应该做什么"
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
		"job_name":           v.JobName,
		"biz_type":           v.BizType,
		"job_data":           v.JobData,
		"process_data_total": v.ProcessDataTotal,
		"processed_count":    v.ProcessedCount,
		"err_log_count":      v.ErrLogCount,
		"status":             v.Status,
		"last_op_source":     v.LastOpSource,
		"last_op_user_id":    v.LastOpUserID,
		"last_op_user_name":  v.LastOpUserName,
		"last_op_remark":     v.LastOpRemark,
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

func GetOne(ctx context.Context, where map[string]any, selectField []string) (*Model, error) {
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

func GetOneByJobId(ctx context.Context, jobId int) (*Model, error) {
	where := map[string]interface{}{
		"job_id": jobId,
	}
	v, err := GetOne(ctx, where, selectField)
	if err != nil {
		logger.Error(ctx, "GetOneByJobId fail.", zap.Error(err))
		return nil, err
	}
	return v, nil
}

func GetOneBaseInfoByJobId(ctx context.Context, jobId int) (*Model, error) {
	where := map[string]interface{}{
		"job_id": jobId,
	}
	v, err := GetOne(ctx, where, selectBaseField)
	if err != nil {
		logger.Error(ctx, "GetOneBaseInfoByJobId fail.", zap.Error(err))
		return nil, err
	}
	return v, nil
}

func MultiGet(ctx context.Context, where map[string]any) ([]*Model, error) {
	cond, vals, err := builder.BuildSelect(tableName, where, selectFieldByQueryList)
	if err != nil {
		logger.Log.Error(ctx, "MultiGet BuildSelect err",
			zap.Any("where", where),
			zap.Error(err),
		)
		return nil, err
	}

	ret := []*Model{}
	err = db.GetSqlx().Find(ctx, &ret, cond, vals...)
	if err != nil {
		logger.Error(ctx, "MultiGet Find fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return nil, err
	}
	return ret, nil
}

func Count(ctx context.Context, where map[string]any) (int64, error) {
	cond, vals, err := builder.BuildSelect(tableName, where, []string{"count(1)"})
	if err != nil {
		logger.Log.Error(ctx, "Count BuildSelect err",
			zap.Any("where", where),
			zap.Error(err),
		)
		return 0, err
	}

	var ret int64
	err = db.GetSqlx().FindOne(ctx, &ret, cond, vals...)
	if err != nil {
		logger.Error(ctx, "Count FindOne fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return 0, err
	}
	return ret, nil
}

func ChangeJob(ctx context.Context, v *Model, whereStatus byte) (int64, error) {
	if v == nil {
		return 0, errors.New("UpdateOneModel v is empty")
	}
	if v.BizType == 0 {
		return 0, errors.New("UpdateOneModel BizType is empty")
	}
	const cond = `
update batch_job_list
set 
    job_name=?,
    job_data=?,
    process_data_total=?,
    processed_count=?,
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
    and status = ?
limit 1;`
	vals := []interface{}{
		v.JobName,
		v.JobData,
		v.ProcessDataTotal,
		v.ProcessedCount,
		v.LastOpSource,
		v.LastOpUserID,
		v.LastOpUserName,
		v.LastOpRemark,
		v.StatusInfo,
		v.OpHistory,
		v.RateSec,
		v.RateType,
		v.JobID,
		whereStatus,
	}
	result, err := db.GetSqlx().Exec(ctx, cond, vals...)
	if nil != err {
		logger.Error(ctx, "UpdateOneModelWhereStatus fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
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

func UpdateOne(ctx context.Context, jobId int, updateData map[string]interface{}) error {
	where := map[string]any{
		"job_id": jobId,
		"_limit": 1,
	}
	cond, vals, err := builder.BuildUpdate(tableName, where, updateData)
	if err != nil {
		logger.Log.Error(ctx, "UpdateOne BuildUpdate err",
			zap.Int("jobId", jobId),
			zap.Any("updateData", updateData),
			zap.Error(err),
		)
		return err
	}

	_, err = db.GetSqlx().Exec(ctx, cond, vals...)
	if err != nil {
		logger.Error(ctx, "UpdateOne fail.", zap.Int("jobId", jobId), zap.Any("updateData", updateData), zap.Error(err))
		return err
	}
	return nil
}
