package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/qldb"
	"github.com/aws/aws-sdk-go-v2/service/qldb/types"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"os"
	"time"

	"github.com/carflores-zh/qldb-go/storage"
)

const inputParams = 3

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

	ctx := context.Background()

	log.Printf("starting migration for region: %s, ledger: %s, version: %d", region, ledgerName, version)

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

	// create ledger and wait for it to be active
	for {
		list, err := dbStorage.Client.ListLedgers(ctx, &qldb.ListLedgersInput{})
		if err != nil {
			log.Errorf("error listing ledgers: %+v", err)
			return
		}

		index := -1
		for i, ledger := range list.Ledgers {
			if *ledger.Name == ledgerName {
				index = i
			}
		}

		if index == -1 {
			log.Errorf("ledger not found: %s, creating it...", ledgerName)
			_, err = dbStorage.Client.CreateLedger(ctx, &ledgerInput)
			if err != nil {
				log.Errorf("error creating ledger: %v", err)
			}

			continue
		}

		if list.Ledgers[index].State == types.LedgerStateActive {
			break
		} else {
			log.Printf("ledger not active yet: %s", list.Ledgers[index].State)
			time.Sleep(50 * time.Second)
		}
	}

	err = dbStorage.MigrateQLDB("sql/", version)
	if err != nil {
		log.Errorf("error migrating QLDB: %v", err)
		return
	}
}
