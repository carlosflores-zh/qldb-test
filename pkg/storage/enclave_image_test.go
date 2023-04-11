package storage

import (
	"fmt"
	"github.com/amzn/ion-go/ion"
	"github.com/carflores-zh/qldb-go/pkg/model/metadata"
	"github.com/carflores-zh/qldb-go/pkg/storage/mocks"
	"github.com/stretchr/testify/mock"
	"testing"

	"github.com/carflores-zh/qldb-go/pkg/model"
	"github.com/stretchr/testify/assert"
)

var errInsertImage = fmt.Errorf("error inserting image")

func TestDB_InsertImage(t *testing.T) {
	type args struct {
		image *model.Image
	}

	tests := []struct {
		name    string
		newDB   func() *DB
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{"success-insert-image",
			func() *DB {
				// create mock driver
				mDriver := mocks.MockQLDBDriver{}

				// first create the result of the insert
				result := mocks.MockResult{}

				// operations executed on the result
				result.On("Next", mock.Anything).Return(true)
				metadataResult := metadata.Result{
					DocumentID: "xc1221",
				}
				resultIon, _ := ion.MarshalText(metadataResult)
				result.On("GetCurrentData", mock.Anything).Return(resultIon)

				// operations executed on the transaction
				mDriver.Txn.On("Execute", "INSERT INTO Image ?", mock.Anything).Return(result, nil).Times(1)
				mDriver.Txn.On("Execute", "UPDATE Image AS i SET i.id = ? WHERE i.imageId = ?", []interface{}{metadataResult.DocumentID, "0001"}).Return(mocks.MockResult{}, nil).Times(1)

				return &DB{
					Driver:     mDriver,
					LedgerName: "test",
				}
			},
			args{&model.Image{
				ImageID: "0001",
				ID:      "0123456789",
			}},
			assert.NoError,
		},
		{"error-insert-image",
			func() *DB {
				// create mock driver
				mDriver := mocks.MockQLDBDriver{}
				mDriver.Txn.On("Execute", "INSERT INTO Image ?", mock.Anything).Return(mocks.MockResult{}, errInsertImage).Times(1)

				return &DB{
					Driver:     mDriver,
					LedgerName: "test",
				}
			},
			args{&model.Image{
				ImageID: "0001",
				ID:      "0123456789",
			}},
			assert.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.newDB()

			tt.wantErr(t, db.InsertImage(tt.args.image), fmt.Sprintf("InsertImage(%v)", tt.args.image))
		})
	}
}
