package storage

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	type args struct {
		cfg        aws.Config
		ledgerName string
	}

	tests := []struct {
		name    string
		args    args
		wantDs  *DBMigrator
		wantErr bool
	}{
		{"success", args{aws.Config{}, "ledger"}, &DBMigrator{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDs, err := New(tt.args.cfg, tt.args.ledgerName)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.IsType(t, gotDs, tt.wantDs)
		})
	}
}
