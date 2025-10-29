package utils

import (
	"sort"

	"github.com/zly-app/zapp/pkg/utils"
)

// GoQuery 的排序结构
type goQueryOrderStruct[T any] struct {
	Data  T
	Index int
}

/*
并发查询并返回有序切片

ignoreErr: 如果设为true,当查询函数返回err时会忽略错误,且返回的数据会少一条
*/
func GoQuery[In any, Out any](ids []In, fns func(id In) (Out, error), ignoreErr bool) ([]Out, error) {
	fnList := make([]func() error, 0, len(ids))
	ch := make(chan *goQueryOrderStruct[Out], len(ids))

	// 构造函数列表
	for index, id := range ids {
		i, in := index, id
		fnList = append(fnList, func() error {
			out, err := fns(in)
			if err == nil {
				ch <- &goQueryOrderStruct[Out]{
					Data:  out,
					Index: i,
				}
				return nil
			}
			if !ignoreErr {
				return err
			}
			return nil
		})
	}
	err := utils.Go.GoAndWait(fnList...)
	close(ch)
	if err != nil {
		return nil, err
	}

	// 收集结果
	var resOut []*goQueryOrderStruct[Out]
	for s := range ch {
		resOut = append(resOut, s)
	}

	// 对结果排序
	sort.SliceStable(resOut, func(i, j int) bool {
		return resOut[i].Index < resOut[j].Index
	})

	// 取出实际需要的结果数据
	outResult := make([]Out, 0, len(resOut))
	for _, s := range resOut {
		outResult = append(outResult, s.Data)
	}
	return outResult, nil
}
