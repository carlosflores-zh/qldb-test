package main

import (
	"context"
	"github.com/carflores-zh/qldb-go/pkg/model"
	log "github.com/sirupsen/logrus"

	"github.com/carflores-zh/qldb-go/storage"
)

const ledgerName = "ledger"
const region = "us-east-2"

// TODO: making change to a policy two signers to accept that, propagate change to all nodes
// TODO: store private key to DB

func main() {
	ctx := context.Background()

	driver, err := storage.Connect(ctx, region, ledgerName)
	if err != nil {
		log.Errorf("error connecting/creating: %v", err)
	}

	defer driver.Shutdown(ctx)

	dbStorage := &storage.DB{
		Driver: driver,
	}

	testContract := &model.Contract{
		Id:        "123",
		Address:   "11",
		Input:     "0x123",
		Output:    "0x123",
		Network:   "0x123",
		SendFunds: true,
		Execution: false,
	}

	// get contract by ID if there's an active revision
	contractResult, err := dbStorage.SelectContract(testContract.Id)
	if err != nil {
		log.Errorf("Error selecting contract: %v", err)
	}

	// insert contract
	if contractResult == nil {
		err = dbStorage.InsertContract(testContract)
		if err != nil {
			log.Errorf("Error inserting contract: %v", err)
		}
	} else {
		log.Printf("Contract: %v", contractResult)
	}

	// update an existing record to create a new revision
	testContract.Execution = true
	testContract.Network = "ethereum"
	err = dbStorage.UpdateContract(testContract)
	if err != nil {
		log.Errorf("Error updating contract: %v", err)
	}

	// select all existing version for an ID
	versions, err := dbStorage.SelectContractVersion(testContract.Id)
	if err != nil {
		log.Errorf("Error selecting contract version: %v", err)
	}

	log.Printf("Versions of my ID:%s : %+v", testContract.Id, versions)

	// get an specific version of that ID
	if len(versions) > 1 {
		wantedVersion := versions[1]
		c3, err := dbStorage.SelectContractInstance(testContract.Id, wantedVersion.Version)
		if err != nil {
			log.Errorf("Error selecting contract instance: %v", err)
		}

		log.Printf("Contract instance: %v", c3)
	}

	// Validate if a table has redactions
	hasRedactions, err := dbStorage.HasDataRedaction("Contract")
	if err != nil {
		log.Errorf("Error selecting contract datahash: %v", err)
	}

	log.Printf("Found %v", hasRedactions)
}
