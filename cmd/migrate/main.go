package main

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/qldb"
	"github.com/aws/aws-sdk-go-v2/service/qldb/types"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"

	"github.com/carflores-zh/qldb-go/pkg/storage"
)

const minutes3 = 180
const inputParams = 3
const awsEndpoint = "http://localhost:4566"

// PARAM 0: region
// PARAM 1: ledger name
// PARAM 2: version

func main() {
	params := os.Args[1:]

	if len(params) < inputParams {
		log.Fatalf("missing parameters")
	}

	region := params[0]
	ledgerName := params[1]
	version := cast.ToInt(params[2])

	log.Printf("starting migration for region: %s, ledger: %s, version: %d", region, ledgerName, version)

	driver, client, err := storage.Connect(awsEndpoint, region, ledgerName)
	if err != nil {
		log.Errorf("error connecting/creating: %v", err)
	}

	defer driver.Shutdown(context.Background())

	dbStorage := &storage.Store{
		Driver: driver,
		Client: client,
	}

	ledgerInput := qldb.CreateLedgerInput{
		Name:            &ledgerName,
		PermissionsMode: types.PermissionsModeStandard,
	}

	ctx := context.Background()

	// create ledger and wait for it to be active
	for {
		// getting the list of ledgers
		list, errList := dbStorage.Client.ListLedgers(ctx, nil)
		if errList != nil {
			log.Errorf("error listing ledgers: %+v", errList)
			return
		}

		index := -1

		for i, ledger := range list.Ledgers {
			// check if ledger exists
			if *ledger.Name == ledgerName {
				index = i
			}
		}

		if index == -1 {
			log.Printf("ledger not found: %s, creating it...", ledgerName)

			_, err = dbStorage.Client.CreateLedger(ctx, &ledgerInput)
			if err != nil {
				log.Errorf("error creating ledger: %v", errList)
			}

			continue
		}

		if list.Ledgers[index].State == types.LedgerStateActive {
			break
		} else {
			log.Printf("ledger not active yet: %s", list.Ledgers[index].State)
			time.Sleep(minutes3 * time.Second)
		}
	}

	err = dbStorage.MigrateQLDB("sql/", version)
	if err != nil {
		log.Errorf("migration failed: %v \n canceling migration", err)
		return
	}
}
