package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/qldb"
	"github.com/aws/aws-sdk-go-v2/service/qldb/types"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/carflores-zh/qldb-go/storage"
)

const inputParams = 3

func main() {
	params := os.Args[1:]

	if len(params) < inputParams {
		log.Fatalf("missing parameters")
	}

	region := params[0]
	ledgerName := params[1]
	migrationFile := params[2]

	ctx := context.Background()

	log.Printf("starting migration for region: %s, ledger: %s, migrationFile: %s", region, ledgerName, migrationFile)

	driver, client, err := storage.Connect(ctx, region, ledgerName)
	if err != nil {
		log.Errorf("error connecting/creating: %v", err)
	}

	defer driver.Shutdown(context.Background())

	dbStorage := &storage.DB{
		Driver: driver,
		Client: client,
	}

	ledgerInput := qldb.CreateLedgerInput{
		Name:            &ledgerName,
		PermissionsMode: types.PermissionsModeStandard,
	}

	_, err = dbStorage.Client.CreateLedger(ctx, &ledgerInput)
	if err != nil {
		log.Errorf("error creating ledger: %v", err)
		return
	}

	// validate and continue if ledger already exists
	// or it is being created, because it takes a while
	list, err := dbStorage.Client.ListLedgers(ctx, &qldb.ListLedgersInput{})
	if err != nil {
		log.Errorf("error listing ledgers: %+v", err)
		return
	}

	log.Printf("listing available ledgers: %v", list)

	time.Sleep(60 * time.Second)

	err = dbStorage.MigrateQLDB(migrationFile)
	if err != nil {
		log.Errorf("error migrating QLDB: %v", err)
		return
	}
}
