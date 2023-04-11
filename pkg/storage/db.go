package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/qldb"
	"github.com/aws/aws-sdk-go-v2/service/qldbsession"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"

	"github.com/carflores-zh/qldb-go/pkg/model"
	"github.com/carflores-zh/qldb-go/pkg/model/metadata"
)

const waitForTables = 1 * time.Second
const upMigration = "up"
const downMigration = "down"

type Store interface {
	InsertTx(tx *model.TransactionLog) error
	SelectContractVersion(id string) ([]metadata.HistoryMetadata, error)
	HasDataRedaction(tableName string) (bool, error)
	SelectContractInstance(id string, version int) ([]model.Contract, error)
}

type QLDBDriver interface {
	SetRetryPolicy(rp qldbdriver.RetryPolicy)
	Execute(ctx context.Context, fn func(txn qldbdriver.Transaction) (interface{}, error)) (interface{}, error)
	GetTableNames(ctx context.Context) ([]string, error)
	Shutdown(ctx context.Context)
}

//nolint:lll // ignore long line length
type QLDBClient interface {
	ListJournalS3Exports(ctx context.Context, params *qldb.ListJournalS3ExportsInput, optFns ...func(*qldb.Options)) (*qldb.ListJournalS3ExportsOutput, error)
	ListJournalKinesisStreamsForLedger(ctx context.Context, params *qldb.ListJournalKinesisStreamsForLedgerInput, optFns ...func(*qldb.Options)) (*qldb.ListJournalKinesisStreamsForLedgerOutput, error)
	TagResource(ctx context.Context, params *qldb.TagResourceInput, optFns ...func(*qldb.Options)) (*qldb.TagResourceOutput, error)
	DescribeJournalKinesisStream(ctx context.Context, params *qldb.DescribeJournalKinesisStreamInput, optFns ...func(*qldb.Options)) (*qldb.DescribeJournalKinesisStreamOutput, error)
	DescribeLedger(ctx context.Context, params *qldb.DescribeLedgerInput, optFns ...func(*qldb.Options)) (*qldb.DescribeLedgerOutput, error)
	GetBlock(ctx context.Context, params *qldb.GetBlockInput, optFns ...func(*qldb.Options)) (*qldb.GetBlockOutput, error)
	ListTagsForResource(ctx context.Context, params *qldb.ListTagsForResourceInput, optFns ...func(*qldb.Options)) (*qldb.ListTagsForResourceOutput, error)
	UntagResource(ctx context.Context, params *qldb.UntagResourceInput, optFns ...func(*qldb.Options)) (*qldb.UntagResourceOutput, error)
	GetDigest(ctx context.Context, params *qldb.GetDigestInput, optFns ...func(*qldb.Options)) (*qldb.GetDigestOutput, error)
	CreateLedger(ctx context.Context, params *qldb.CreateLedgerInput, optFns ...func(*qldb.Options)) (*qldb.CreateLedgerOutput, error)
	DeleteLedger(ctx context.Context, params *qldb.DeleteLedgerInput, optFns ...func(*qldb.Options)) (*qldb.DeleteLedgerOutput, error)
	UpdateLedger(ctx context.Context, params *qldb.UpdateLedgerInput, optFns ...func(*qldb.Options)) (*qldb.UpdateLedgerOutput, error)
	ListJournalS3ExportsForLedger(ctx context.Context, params *qldb.ListJournalS3ExportsForLedgerInput, optFns ...func(*qldb.Options)) (*qldb.ListJournalS3ExportsForLedgerOutput, error)
	GetRevision(ctx context.Context, params *qldb.GetRevisionInput, optFns ...func(*qldb.Options)) (*qldb.GetRevisionOutput, error)
	ListLedgers(ctx context.Context, params *qldb.ListLedgersInput, optFns ...func(*qldb.Options)) (*qldb.ListLedgersOutput, error)
	UpdateLedgerPermissionsMode(ctx context.Context, params *qldb.UpdateLedgerPermissionsModeInput, optFns ...func(*qldb.Options)) (*qldb.UpdateLedgerPermissionsModeOutput, error)
	ExportJournalToS3(ctx context.Context, params *qldb.ExportJournalToS3Input, optFns ...func(*qldb.Options)) (*qldb.ExportJournalToS3Output, error)
	StreamJournalToKinesis(ctx context.Context, params *qldb.StreamJournalToKinesisInput, optFns ...func(*qldb.Options)) (*qldb.StreamJournalToKinesisOutput, error)
	DescribeJournalS3Export(ctx context.Context, params *qldb.DescribeJournalS3ExportInput, optFns ...func(*qldb.Options)) (*qldb.DescribeJournalS3ExportOutput, error)
	CancelJournalKinesisStream(ctx context.Context, params *qldb.CancelJournalKinesisStreamInput, optFns ...func(*qldb.Options)) (*qldb.CancelJournalKinesisStreamOutput, error)
}

type DB struct {
	Driver     QLDBDriver
	LedgerName string
}

type DBMigrator struct {
	*DB
	Client *qldb.Client
}

func New(cfg aws.Config, ledgerName string) (ds *DB, err error) {
	qldbSession := qldbsession.NewFromConfig(cfg, func(options *qldbsession.Options) {
		options.Region = cfg.Region
		options.RetryMaxAttempts = 3
	})

	qldbDriver, err := qldbdriver.New(
		ledgerName,
		qldbSession,
		func(options *qldbdriver.DriverOptions) {
			options.LoggerVerbosity = qldbdriver.LogInfo
		})

	store := &DB{
		Driver: qldbDriver,
	}

	return store, err
}

func NewMigrator(cfg aws.Config, ledgerName string) (ds *DBMigrator, err error) {
	qldbClient := qldb.NewFromConfig(cfg, func(options *qldb.Options) {
		options.Region = cfg.Region
		options.RetryMaxAttempts = 3
	})

	store, err := New(cfg, ledgerName)
	if err != nil {
		return nil, err
	}

	storeMigrator := &DBMigrator{
		DB:     store,
		Client: qldbClient,
	}

	return storeMigrator, err
}

func (db *DB) InsertTx(tx *model.TransactionLog) error {
	_, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("INSERT INTO TransactionLog ?", tx)
	})
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) SelectContractVersion(id string) (resultMetadata []metadata.HistoryMetadata, err error) {
	c, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, errTxn := txn.Execute("SELECT metadata.version from history(Contract) where data.id = ?", id)
		if errTxn != nil {
			return nil, errTxn
		}

		var versions []metadata.HistoryMetadata
		for result.Next(txn) {
			temp := new(metadata.HistoryMetadata)
			err = ion.Unmarshal(result.GetCurrentData(), temp)
			if err != nil {
				return nil, err
			}

			versions = append(versions, *temp)
		}
		if result.Err() != nil {
			return nil, result.Err()
		}

		return versions, nil
	})
	if err != nil {
		return nil, err
	}

	resultMetadata = c.([]metadata.HistoryMetadata)

	return resultMetadata, nil
}

// HasDataRedaction If we get datahashes this means someone has redacted the data
func (db *DB) HasDataRedaction(tableName string) (bool, error) {
	result, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		countSelect, err := txn.Execute(fmt.Sprintf("SELECT count(dataHash) as countHashes from history(%s)", tableName))
		if err != nil {
			return nil, err
		}

		countSelect.Next(txn)

		temp := new(metadata.ResponseHasDataRedaction)
		err = ion.Unmarshal(countSelect.GetCurrentData(), temp)
		if err != nil {
			return nil, err
		}

		if countSelect.Err() != nil {
			return nil, countSelect.Err()
		}

		return temp, nil
	})
	if err != nil {
		return false, err
	}

	resultRedaction := result.(*metadata.ResponseHasDataRedaction)

	if resultRedaction.CountHashes > 0 {
		return true, nil
	}

	return false, nil
}

func (db *DB) SelectContractInstance(id string, version int) ([]model.Contract, error) {
	var contracts []model.Contract

	c, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT data.* from history(Contract) where data.id = ? AND metadata.version = ?", id, version)
		if err != nil {
			return nil, err
		}

		var cs []model.Contract
		for result.Next(txn) {
			temp := new(model.Contract)
			err = ion.Unmarshal(result.GetCurrentData(), temp)
			if err != nil {
				return nil, err
			}

			cs = append(cs, *temp)
		}
		if result.Err() != nil {
			return nil, result.Err()
		}

		return cs, nil
	})
	if err != nil {
		return nil, err
	}

	contracts = c.([]model.Contract)

	return contracts, nil
}

func (db *DB) SelectContractActive(id string) ([]model.Contract, error) {
	var contracts []model.Contract

	c, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT id FROM Contract AS c WHERE c.id = ?", id)
		if err != nil {
			return nil, err
		}

		var cs []model.Contract
		for result.Next(txn) {
			temp := new(model.Contract)
			err = ion.Unmarshal(result.GetCurrentData(), temp)
			if err != nil {
				return nil, err
			}

			cs = append(cs, *temp)
		}
		if result.Err() != nil {
			return nil, result.Err()
		}

		return cs, nil
	})

	if err != nil {
		return nil, err
	}

	contracts = c.([]model.Contract)

	return contracts, nil
}

func (db *DB) InsertContractTx(contract *model.Contract) (id string, err error) {
	_, err = db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		temp := new(metadata.Result)
		contract.ID = ""

		resultContract, errContract := txn.Execute("INSERT INTO Contract ?", contract)
		if errContract != nil {
			return nil, err
		}

		resultContract.Next(txn)
		err = ion.Unmarshal(resultContract.GetCurrentData(), temp)
		if err != nil {
			return nil, err
		}

		contract.ID = temp.DocumentID

		_, err = txn.Execute(
			"UPDATE Contract AS c SET c.id = ? WHERE c.address = ? AND c.network = ?",
			temp.DocumentID, contract.Address, contract.Network)
		if err != nil {
			return nil, err
		}

		controlRecord := model.Control{
			Table:      "Contract",
			DocumentID: temp.DocumentID,
			Version:    0,
		}

		resultControl, errControl := txn.Execute("INSERT INTO ControlRecord ?", controlRecord)
		if errControl != nil {
			return nil, err
		}

		resultControl.Next(txn)
		err = ion.Unmarshal(resultControl.GetCurrentData(), temp)
		if err != nil {
			return nil, err
		}

		_, err = txn.Execute(
			"UPDATE ControlRecord AS c SET c.id = ? WHERE c.documentId = ? AND c.version = ?",
			temp.DocumentID, controlRecord.DocumentID, controlRecord.Version)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})

	return contract.ID, err
}

func (db *DB) UpdateContract(contract *model.Contract) error {
	_, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("UPDATE Contract AS c SET c = ? where c.id = ?", contract, contract.ID)
	})
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) QueryTransactions() (int, []model.TransactionLog, error) {
	p, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT * FROM TransactionLog")
		if err != nil {
			return nil, err
		}

		var txs []model.TransactionLog
		for result.Next(txn) {
			temp := new(model.TransactionLog)
			err = ion.Unmarshal(result.GetCurrentData(), temp)
			if err != nil {
				return nil, err
			}

			txs = append(txs, *temp)
		}
		if result.Err() != nil {
			return nil, result.Err()
		}

		return txs, nil
	})
	if err != nil {
		return 0, nil, err
	}

	people := p.([]model.TransactionLog)

	return len(people), people, err
}
