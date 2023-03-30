package main

import (
	"context"

	log "github.com/sirupsen/logrus"

	"github.com/carflores-zh/qldb-go/pkg/model"
	"github.com/carflores-zh/qldb-go/pkg/storage"
)

const ledgerName = "ledger"
const region = "us-east-2"

// TODO: making change to a policy two signers to accept that, propagate change to all nodes
// TODO: store private key to Store

func main() {
	ctx := context.Background()

	driver, client, err := storage.Connect("", region, ledgerName)
	if err != nil {
		log.Errorf("error connecting/creating: %v", err)
	}

	defer driver.Shutdown(ctx)

	dbStorage := &storage.Store{
		Driver: driver,
		Client: client,
	}

	testContract := &model.Contract{
		ID:        "123",
		Address:   "11",
		Input:     "0x123",
		Output:    "0x123",
		Network:   "0x123",
		SendFunds: true,
		Execution: false,
	}

	// get contract by ID if there's an active revision
	contractResult, err := dbStorage.SelectContractActive(testContract.ID)
	if err != nil {
		log.Errorf("Error selecting contract: %v", err)
	}

	// insert contract
	if contractResult == nil {
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
	versions, err := dbStorage.SelectContractVersion(testContract.ID)
	if err != nil {
		log.Errorf("Error selecting contract version: %v", err)
	}

	log.Printf("Versions of my ID:%s : %+v", testContract.ID, versions)

	// get a specific version of that ID
	if len(versions) > 1 {
		wantedVersion := versions[1]

		c3, errSelect := dbStorage.SelectContractInstance(testContract.ID, wantedVersion.Version)
		if errSelect != nil {
			log.Errorf("Error selecting contract instance: %v", errSelect)
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
