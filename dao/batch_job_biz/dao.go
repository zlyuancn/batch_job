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
		"biz_id",
		"biz_name",
		"exec_type",
		"exec_extend_data",
		"remark",
		"create_time",
		"update_time",
		"op_source",
		"op_user_id",
		"op_user_name",
		"op_remark",
		"status",
	}

	selectFieldByQueryList = []string{
		"biz_id",
		"biz_name",
		"exec_type",
		"remark",
		"update_time",
		"op_source",
		"op_user_id",
		"op_user_name",
		"op_remark",
		"status",
	}
)

const (
	tableName = "batch_job_biz"
)

type Model struct {
	BizId          uint      `db:"biz_id"`           // "业务id"
	BizName        string    `db:"biz_name"`         // "业务名"
	ExecType       byte      `db:"exec_type"`        // "执行类型"
	ExecExtendData string    `db:"exec_extend_data"` // "执行器扩展数据"
	Remark         string    `db:"remark"`           // "备注"
	CreateTime     time.Time `db:"create_time"`
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

func UpdateOneModel(ctx context.Context, v *Model) (int64, error) {
	if v == nil {
		return 0, errors.New("UpdateOneModel v is empty")
	}
	if v.BizId == 0 {
		return 0, errors.New("UpdateOneModel BizId is empty")
	}
	const cond = `
update batch_job_biz
set 
    biz_name=?,
    exec_type=?,
    exec_extend_data=?,
    remark=?,
    update_time=now(),
    op_source=?,
    op_user_id=?,
    op_user_name=?,
    op_remark=?,
    status=?
where biz_id = ?
limit 1;`
	vals := []interface{}{
		v.BizName,
		v.ExecType,
		v.ExecExtendData,
		v.Remark,
		v.OpSource,
		v.OpUserID,
		v.OpUserName,
		v.OpRemark,
		v.Status,
		v.BizId,
	}
	result, err := db.GetSqlx().Exec(ctx, cond, vals...)
	if nil != err {
		logger.Error(ctx, "UpdateOneModel fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
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

func MultiGetBySelect(ctx context.Context, where map[string]any, selectField []string) ([]*Model, error) {
	cond, vals, err := builder.BuildSelect(tableName, where, selectField)
	if err != nil {
		logger.Log.Error(ctx, "MultiGetBySelect BuildSelect err",
			zap.Any("where", where),
			zap.Error(err),
		)
		return nil, err
	}

	ret := []*Model{}
	err = db.GetSqlx().Find(ctx, &ret, cond, vals...)
	if err != nil {
		logger.Error(ctx, "MultiGetBySelect Find fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
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

func GetOneByBizId(ctx context.Context, bizId int) (*Model, error) {
	where := map[string]interface{}{
		"biz_id": bizId,
	}
	v, err := GetOne(ctx, where, selectField)
	if err != nil {
		logger.Error(ctx, "GetOneByBizId fail.", zap.Error(err))
		return nil, err
	}
	return v, nil
}

func GetOneBaseInfoByBizId(ctx context.Context, bizId int) (*Model, error) {
	where := map[string]interface{}{
		"biz_id": bizId,
	}
	v, err := GetOne(ctx, where, selectBaseField)
	if err != nil {
		logger.Error(ctx, "GetOneBaseInfoByBizId fail.", zap.Error(err))
		return nil, err
	}
	return v, nil
}
