package batch_job_log

import (
	"context"
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
	//	"data_id",
	//	"remark",
	//	"extend",
	//	"log_type",
	//	"create_time",
	// }
)

const (
	tableName = "batch_job_log"
)

type Model struct {
	ID         uint64    `db:"id"`
	JobID      uint      `db:"job_id"`   // "任务号"
	DataID     string    `db:"data_id"`  // "数据id"
	Remark     string    `db:"remark"`   // "备注"
	Extend     string    `db:"extend"`   // "扩展数据"
	LogType    byte      `db:"log_type"` // "日志类型 0=调试Debug 1=信息Info 2=警告Warn 3=错误Err"
	CreateTime time.Time `db:"create_time"`
}

func MultiGet(ctx context.Context, where map[string]any) ([]*Model, error) {
	cond, vals, err := builder.BuildSelect(tableName, where, selectField)
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

func MultiSave(ctx context.Context, list []*Model) (int64, error) {
	if len(list) == 0 {
		return 0, nil
	}
	var data []map[string]any
	for _, v := range list {
		data = append(data, map[string]any{
			"job_id":   v.JobID,
			"data_id":  v.DataID,
			"remark":   v.Remark,
			"extend":   v.Extend,
			"log_type": v.LogType,
		})
	}
	cond, vals, err := builder.BuildInsert(tableName, data)
	if err != nil {
		logger.Log.Error(ctx, "MultiSave call BuildInsert err", zap.Any("data", data), zap.Error(err))
		return 0, err
	}

	result, err := db.GetSqlx().Exec(ctx, cond, vals...)
	if err != nil {
		logger.Error(ctx, "MultiSave call Exec fail.", zap.Error(err))
		return 0, err
	}
	return result.LastInsertId()
}
