package view

import (
	"github.com/zlyuancn/batch_job/pb"
)

type BatchJob struct {
	pb.UnimplementedBatchJobServiceServer
}

func NewServer() pb.BatchJobServiceServer {
	return &BatchJob{}
}
