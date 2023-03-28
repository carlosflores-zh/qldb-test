package main

import (
	"context"
	"github.com/carflores-zh/qldb-go/pkg/model"
	log "github.com/sirupsen/logrus"

	"github.com/carflores-zh/qldb-go/storage"
)

const ledgerName = "ledger"
const region = "us-east-2"

func main() {
	ctx := context.Background()

	driver, client, err := storage.Connect(ctx, region, ledgerName)
	if err != nil {
		log.Errorf("error connecting/creating: %v", err)
	}

	defer driver.Shutdown(ctx)

	dbStorage := &storage.DB{
		Driver: driver,
		Client: client,
	}

	testContract := &model.Contract{
		Id:        "4444",
		Address:   "11",
		Input:     "0x123",
		Output:    "0x123",
		Network:   "0x123",
		SendFunds: true,
		Execution: false,
	}

	err = dbStorage.InsertContractTx(testContract)
	if err != nil {
		log.Errorf("Error inserting contract: %v", err)
	}
}
