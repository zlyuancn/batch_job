package batch_job_biz

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
		"biz_type",
		"biz_name",
		"rate_sec",
		"rate_type",
		"exec_type",
		"remark",
		"cb_before_create",
		"cb_before_run",
		"cb_process",
		"cb_process_stop",
		"cb_before_create_timeout",
		"cb_before_run_timeout",
		"cb_process_timeout",
		"cb_process_stop_timeout",
		"update_time",
		"last_op_source",
		"last_op_user_id",
		"last_op_user_name",
		"last_op_remark",
		"status",
	}
)

const (
	tableName = "batch_job_biz"
)

type Model struct {
	ID                    uint      `db:"id" json:"-"`
	BizType               uint      `db:"biz_type"`                 // "业务类型"
	BizName               string    `db:"biz_name"`                 // "业务名"
	RateSec               uint      `db:"rate_sec"`                 // "每秒处理速率. 0表示不限制"
	RateType              byte      `db:"rate_type"`                // "速率类型. 0=通过rate_sec限速, 1=串行化"
	ExecType              byte      `db:"exec_type"`                // "执行类型"
	Remark                string    `db:"remark"`                   // "备注"
	CbBeforeCreate        string    `db:"cb_before_create"`         // "创建任务回调url"
	CbBeforeRun           string    `db:"cb_before_run"`            // "启动前回调. 一旦配置, 则任务必须由业务主动调用 BizStartJob 执行任务. 否则任务将一直处于 JobStatus.WaitBizRun 状态"
	CbProcess             string    `db:"cb_process"`               // "处理任务回调. 必填"
	CbProcessStop         string    `db:"cb_process_stop"`          // "处理任务完成回调. 用于业务方做一些清理. 选填"
	CbBeforeCreateTimeout uint      `db:"cb_before_create_timeout"` // "启动前回调超时秒数"
	CbBeforeRunTimeout    uint      `db:"cb_before_run_timeout"`    // "启动前回调超时秒数"
	CbProcessTimeout      uint      `db:"cb_process_timeout"`       // "处理任务回调超时秒数"
	CbProcessStopTimeout  uint      `db:"cb_process_stop_timeout"`  // "处理任务完成回调超时秒数"
	CreateTime            time.Time `db:"create_time" json:"-"`
	UpdateTime            time.Time `db:"update_time" json:"-"`
	LastOpSource          string    `db:"last_op_source" json:"-"`    // "最后操作来源"
	LastOpUserID          string    `db:"last_op_user_id" json:"-"`   // "最后操作用户id"
	LastOpUserName        string    `db:"last_op_user_name" json:"-"` // "最后操作用户名"
	LastOpRemark          string    `db:"last_op_remark" json:"-"`    // "最后操作备注"
	OpHistory             string    `db:"op_history" json:"-"`        // "操作历史信息"
	Status                byte      `db:"status"`                     // "状态 0=正常 1=隐藏"
}

func CreateOneModel(ctx context.Context, v *Model) (int64, error) {
	if v == nil {
		return 0, errors.New("CreateOneModel v is empty")
	}

	var data []map[string]any
	data = append(data, map[string]any{
		"biz_type":                 v.BizType,
		"biz_name":                 v.BizName,
		"rate_sec":                 v.RateSec,
		"rate_type":                v.RateType,
		"exec_type":                v.ExecType,
		"remark":                   v.Remark,
		"cb_before_create":         v.CbBeforeCreate,
		"cb_before_run":            v.CbBeforeRun,
		"cb_process":               v.CbProcess,
		"cb_process_stop":          v.CbProcessStop,
		"cb_before_create_timeout": v.CbBeforeCreateTimeout,
		"cb_before_run_timeout":    v.CbBeforeRunTimeout,
		"cb_process_timeout":       v.CbProcessTimeout,
		"cb_process_stop_timeout":  v.CbProcessStopTimeout,
		"last_op_source":           v.LastOpSource,
		"last_op_user_id":          v.LastOpUserID,
		"last_op_user_name":        v.LastOpUserName,
		"last_op_remark":           v.LastOpRemark,
		"op_history":               v.OpHistory,
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

func UpdateOneModel(ctx context.Context, v *Model) (int64, error) {
	if v == nil {
		return 0, errors.New("UpdateOneModel v is empty")
	}
	if v.BizType == 0 {
		return 0, errors.New("UpdateOneModel BizType is empty")
	}
	const cond = `
update batch_job_biz
set 
    biz_name=?,
    rate_sec=?,
    rate_type=?,
    exec_type=?,
    remark=?,
    cb_before_create=?,
    cb_before_run=?,
    cb_process=?,
    cb_process_stop=?,
    cb_before_create_timeout=?,
    cb_before_run_timeout=?,
    cb_process_timeout=?,
    cb_process_stop_timeout=?,
    last_op_source=?,
    last_op_user_id=?,
    last_op_user_name=?,
    last_op_remark=?,
    op_history=json_array_insert(op_history, '$[0]', json_extract(?, '$')),
    status=?
where biz_type = ?
limit 1;`
	vals := []interface{}{
		v.BizName,
		v.RateSec,
		v.RateType,
		v.ExecType,
		v.Remark,
		v.CbBeforeCreate,
		v.CbBeforeRun,
		v.CbProcess,
		v.CbProcessStop,
		v.CbBeforeCreateTimeout,
		v.CbBeforeRunTimeout,
		v.CbProcessTimeout,
		v.CbProcessStopTimeout,
		v.LastOpSource,
		v.LastOpUserID,
		v.LastOpUserName,
		v.LastOpRemark,
		v.OpHistory,
		v.Status,
		v.BizType,
	}
	result, err := db.GetSqlx().Exec(ctx, cond, vals...)
	if nil != err {
		logger.Error(ctx, "UpdateStatus fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return 0, err
	}
	return result.RowsAffected()
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

func MultiGet(ctx context.Context, where map[string]any) ([]*Model, error) {
	cond, vals, err := builder.BuildSelect(tableName, where, selectBaseField)
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
		logger.Error(ctx, "MultiGet FindOne fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
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

func GetOneByBizType(ctx context.Context, bizType int32) (*Model, error) {
	where := map[string]interface{}{
		"biz_type": bizType,
	}
	v, err := GetOne(ctx, where, selectField)
	if err != nil {
		logger.Error(ctx, "GetOneByBizType fail.", zap.Error(err))
		return nil, err
	}
	return v, nil
}

func GetOneBaseInfoByBizType(ctx context.Context, bizType int32) (*Model, error) {
	where := map[string]interface{}{
		"biz_type": bizType,
	}
	v, err := GetOne(ctx, where, selectBaseField)
	if err != nil {
		logger.Error(ctx, "GetOneBaseInfoByBizType fail.", zap.Error(err))
		return nil, err
	}
	return v, nil
}
