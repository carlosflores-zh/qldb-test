package main

import (
	"context"
	"os"
	"qldb-test/model"
	"qldb-test/storage"

	log "github.com/sirupsen/logrus"
)

const accessID = "-"
const secretKey = "-"

const ledgerName = "ledger"
const region = "us-east-2"

// TODO: making change to a policy two signers to accept that, propagate change to all nodes
// TODO: store private key to DB

func main() {
	os.Setenv("AWS_ACCESS_KEY_ID", accessID)
	os.Setenv("AWS_SECRET_KEY", secretKey)

	driver, err := storage.Connect(region, ledgerName)
	if err != nil {
		log.Errorf("error connecting/creating: %v", err)
	}

	dbStorage := &storage.DB{
		Driver: driver,
	}

	err = dbStorage.MigrateQLDB("sql/migration.sql")
	if err != nil {
		log.Printf("Error migrating QLDB: %v", err)
	}

	contract := &model.Contract{
		Id:        "123",
		Address:   "11",
		Input:     "0x123",
		Output:    "0x123",
		Network:   "0x123",
		SendFunds: true,
		Execution: false,
	}

	/*
		c2, err := dbStorage.SelectContract(contract.Id)
		if err != nil {
			log.Errorf("Error selecting contract: %v", err)
		}


			if c2 == nil {
				err = dbStorage.InsertContract(contract)
				if err != nil {
					log.Errorf("Error inserting contract: %v", err)
				}
			}

			log.Printf("Contract: %v", c2)

			contract.Execution = true
			contract.Network = "ethereum"
			err = dbStorage.UpdateContract(contract)
			if err != nil {
				log.Errorf("Error updating contract: %v", err)
			}
	*/

	versions, err := dbStorage.SelectContractVersion(contract.Id)
	if err != nil {
		log.Errorf("Error selecting contract version: %v", err)
	}

	log.Printf("Versions of my ID:%s : %+v", contract.Id, versions)

	if len(versions) > 1 {
		wantedVersion := versions[1]
		c3, err := dbStorage.SelectContractInstance(contract.Id, wantedVersion.Version)
		if err != nil {
			log.Errorf("Error selecting contract instance: %v", err)
		}

		log.Printf("Contract instance: %v", c3)
	}

	datahashes, err := dbStorage.SelectContractDataHash()
	if err != nil {
		log.Errorf("Error selecting contract datahash: %v", err)
	}

	redactions := 0
	for _, v := range datahashes {
		if v.DataHash != nil {
			redactions++
			log.Printf("Found datahash on revision  %d: %v", v.Version, v.DataHash)
		}
	}

	log.Printf("Found %d redactions", redactions)
	defer driver.Shutdown(context.Background())

}
