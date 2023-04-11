package mocks

import (
	"context"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"
	"github.com/stretchr/testify/mock"
)

type MockQLDBDriver struct {
	Txn MockTransaction
}

func (mqd MockQLDBDriver) Execute(ctx context.Context, fn func(txn qldbdriver.Transaction) (interface{}, error)) (interface{}, error) {
	result, err := fn(mqd.Txn)

	return result, err
}

func (mqd MockQLDBDriver) SetRetryPolicy(retryPolicy qldbdriver.RetryPolicy) {
	panic("not used")
}

func (mqd MockQLDBDriver) GetTableNames(ctx context.Context) ([]string, error) {
	panic("not used")
}

func (mqd MockQLDBDriver) Shutdown(ctx context.Context) {
	panic("not used")
}

type MockTransaction struct {
	mock.Mock
}

func (mt MockTransaction) Execute(statement string, parameters ...interface{}) (qldbdriver.Result, error) {
	args := mt.Called(statement, parameters)
	return args.Get(0).(MockResult), args.Error(1)
}

func (mt MockTransaction) BufferResult(res qldbdriver.Result) (qldbdriver.BufferedResult, error) {
	panic("not used")
}

func (mt MockTransaction) Abort() error {
	panic("not used")
}

func (mt MockTransaction) ID() string {
	panic("not used")
}

type MockResult struct {
	mock.Mock
}

func (mr MockResult) Next(txn qldbdriver.Transaction) bool {
	args := mr.Called(txn)
	return args.Get(0).(bool)
}

func (mr MockResult) GetCurrentData() []byte {
	args := mr.Called()
	return args.Get(0).([]byte)
}

func (mr MockResult) Err() error {
	panic("not used")
}

func (mr MockResult) GetConsumedIOs() *qldbdriver.IOUsage {
	panic("not used")
}

func (mr MockResult) GetTimingInformation() *qldbdriver.TimingInformation {
	panic("not used")
}
