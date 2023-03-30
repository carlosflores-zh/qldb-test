package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/qldb"
	"github.com/aws/aws-sdk-go-v2/service/qldbsession"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"
	"github.com/carflores-zh/qldb-go/pkg/model"
	"github.com/carflores-zh/qldb-go/pkg/model/metadata"
)

const s5 = 5 * time.Second
const upMigration = "up"
const downMigration = "down"

type Store struct {
	Driver     *qldbdriver.QLDBDriver
	Client     *qldb.Client
	LedgerName string
}

func Connect(awsEndpoint string, region string, ledgerName string) (driver *qldbdriver.QLDBDriver, client *qldb.Client, err error) {
	awsRegion := "us-east-1"
	/*
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if awsEndpoint != "" {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           awsEndpoint,
					SigningRegion: awsRegion,
				}, nil
			}

			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})*/

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
	)
	if err != nil {
		return nil, nil, err
	}

	qldbSession := qldbsession.NewFromConfig(cfg, func(options *qldbsession.Options) {
		options.Region = region
		options.RetryMaxAttempts = 3
	})

	qldbClient := qldb.NewFromConfig(cfg, func(options *qldb.Options) {
		options.Region = region
		options.RetryMaxAttempts = 3
	})

	driver, err = qldbdriver.New(
		ledgerName,
		qldbSession,
		func(options *qldbdriver.DriverOptions) {
			options.LoggerVerbosity = qldbdriver.LogInfo
		})

	return driver, qldbClient, err
}

func (s *Store) InsertTx(tx *model.TransactionLog) error {
	_, err := s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("INSERT INTO transactionLog ?", tx)
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) SelectContractVersion(id string) (resultMetadata []metadata.HistoryMetadata, err error) {
	c, err := s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
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
func (s *Store) HasDataRedaction(tableName string) (bool, error) {
	result, err := s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
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

func (s *Store) SelectContractInstance(id string, version int) ([]model.Contract, error) {
	var contracts []model.Contract

	c, err := s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
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

func (s *Store) SelectContractActive(id string) ([]model.Contract, error) {
	var contracts []model.Contract

	c, err := s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
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

func (s *Store) InsertContractTx(contract *model.Contract) (id string, err error) {
	_, err = s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
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

func (s *Store) UpdateContract(contract *model.Contract) error {
	_, err := s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("UPDATE Contract AS c SET c = ? where c.id = ?", contract, contract.ID)
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) QueryTransactions() (int, []model.TransactionLog, error) {
	p, err := s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
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
