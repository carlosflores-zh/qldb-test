package storage

import (
	"bufio"
	"context"
	"github.com/amzn/ion-go/ion"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/qldbsession"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"
	log "github.com/sirupsen/logrus"

	"qldb-test/model"
)

type DB struct {
	Driver *qldbdriver.QLDBDriver
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
	var versions []metadata
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

	versions = c.([]metadata)

	return versions, nil
}

// SelectContractDataHash If we get datahashes this means someone has redacted the data
func (db *DB) SelectContractDataHash() ([]metadata, error) {
	var versions []metadata
	c, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT dataHash,metadata.version from history(Contract)")
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

	versions = c.([]metadata)

	return versions, nil
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

func Connect(region string, ledgerName string) (driver *qldbdriver.QLDBDriver, err error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Errorf("Error loading config: %v", err)
	}

	qldbSession := qldbsession.NewFromConfig(cfg, func(options *qldbsession.Options) {
		options.Region = region
	})

	driver, err = qldbdriver.New(
		ledgerName,
		qldbSession,
		func(options *qldbdriver.DriverOptions) {
			options.LoggerVerbosity = qldbdriver.LogInfo
		})
	if err != nil {
		log.Printf("Error creating Driver: %v", err)
	}

	return driver, nil
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
			_, err := txn.Execute(scanner.Text())
			if err != nil {
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
