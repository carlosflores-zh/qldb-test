package storage

import (
	"bufio"
	"context"
	"fmt"
	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/qldbsession"
	"github.com/carflores-zh/qldb-go/pkg/model"
	"os"

	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"
	log "github.com/sirupsen/logrus"
)

type DB struct {
	Driver     *qldbdriver.QLDBDriver
	LedgerName string
}

func Connect(ctx context.Context, region string, ledgerName string) (driver *qldbdriver.QLDBDriver, err error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Errorf("error loading config: %v", err)
	}

	qldbSession := qldbsession.NewFromConfig(cfg, func(options *qldbsession.Options) {
		options.Region = region
		options.RetryMaxAttempts = 3
	})

	driver, err = qldbdriver.New(
		ledgerName,
		qldbSession,
		func(options *qldbdriver.DriverOptions) {
			options.LoggerVerbosity = qldbdriver.LogInfo
		})
	if err != nil {
		log.Printf("error creating Driver: %v", err)
	}

	return driver, nil
}

func (db *DB) InsertTx(tx *model.TransactionLog) error {
	_, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("INSERT INTO transactionLog ?", tx)
	})
	if err != nil {
		log.Errorf("Error inserting transaction: %v", err)
		return err
	}

	return nil
}

type metadata struct {
	ID       string `ion:"id"`
	Version  int    `ion:"version"`
	DataHash []byte `ion:"dataHash"`
}

func (db *DB) SelectContractVersion(id string) ([]metadata, error) {
	var resultMetadata []metadata
	c, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT metadata.version from history(Contract) where data.id = ?", id)
		if err != nil {
			return nil, err
		}

		var versions []metadata
		for result.Next(txn) {
			ionBinary := result.GetCurrentData()

			temp := new(metadata)
			err = ion.Unmarshal(ionBinary, temp)
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

	resultMetadata = c.([]metadata)

	return resultMetadata, nil
}

type ResponseHasDataRedaction struct {
	CountHashes int `ion:"countHashes"`
}

// HasDataRedaction If we get datahashes this means someone has redacted the data
func (db *DB) HasDataRedaction(tableName string) (bool, error) {
	result, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		countSelect, err := txn.Execute(fmt.Sprintf("SELECT count(dataHash) as countHashes from history(%s)", tableName))
		if err != nil {
			return nil, err
		}

		countSelect.Next(txn)
		ionBinary := countSelect.GetCurrentData()

		temp := new(ResponseHasDataRedaction)
		err = ion.Unmarshal(ionBinary, temp)
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

	resultRedaction := result.(*ResponseHasDataRedaction)

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
			ionBinary := result.GetCurrentData()
			log.Printf("ionBinary: %v", ionBinary)
			temp := new(model.Contract)
			err = ion.Unmarshal(ionBinary, temp)
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

func (db *DB) SelectContract(id string) ([]model.Contract, error) {
	var contracts []model.Contract
	c, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT id FROM Contract AS c WHERE c.id = ?", id)
		if err != nil {
			return nil, err
		}

		var cs []model.Contract
		for result.Next(txn) {
			ionBinary := result.GetCurrentData()

			temp := new(model.Contract)
			err = ion.Unmarshal(ionBinary, temp)
			if err != nil {
				return nil, err
			}

			log.Printf("Contract: %v", temp)

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

func (db *DB) InsertContract(contract *model.Contract) error {
	_, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("INSERT INTO Contract ?", contract)
	})
	if err != nil {
		log.Errorf("Error inserting contract: %v", err)
		return err
	}

	return nil
}

func (db *DB) UpdateContract(contract *model.Contract) error {
	_, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("UPDATE Contract AS c SET c = ? where c.id = ?", contract, contract.Id)
	})
	if err != nil {
		log.Errorf("Error updating contract: %v", err)
		return err
	}

	return nil
}

func (db *DB) MigrateQLDB(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// creates a transaction and executes statements
	_, err = db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		for scanner.Scan() {
			_, errScan := txn.Execute(scanner.Text())
			if errScan != nil {
				log.Errorf("Error creating table {%s}: %v", scanner.Text(), err)
			}
		}
		return nil, nil
	})
	if err != nil {
		log.Errorf("Error creating tables: %v", err)
	}

	if err := scanner.Err(); err != nil {
		log.Errorf("Error reading file: %v", err)
	}

	return nil
}

/*
func QueryTX(Driver *qldbdriver.QLDBDriver, query string) (int, []Transaction, error) {
	p, err := Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute(query)
		if err != nil {
			return nil, err
		}

		var txs []Transaction
		for result.Next(txn) {
			ionBinary := result.GetCurrentData()

			temp := new(Transaction)
			err = ion.Unmarshal(ionBinary, temp)
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

	var people []Transaction
	people = p.([]Transaction)

	return len(people), people, err
}
*/
