package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/qldb"
	"github.com/carflores-zh/qldb-go/storage"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"os"
)

// PARAM 0: region
// PARAM 1: ledger name

func main() {
	params := os.Args[1:]

	region := params[0]

	ctx := context.Background()

	driver, client, err := storage.Connect(ctx, region, "ledger")
	if err != nil {
		log.Errorf("error connecting/creating: %v", err)
	}

	defer driver.Shutdown(context.Background())

	dbStorage := &storage.DB{
		Driver: driver,
		Client: client,
	}

	list, err := dbStorage.Client.ListLedgers(ctx, &qldb.ListLedgersInput{})
	if err != nil {
		log.Errorf("error listing ledgers: %+v", err)
		return
	}

	for _, ledger := range list.Ledgers {
		ledgerName := cast.ToString(ledger.Name)

		log.Printf("deleting ledger: %s", ledgerName)

		deleteProtection := false
		_, err := dbStorage.Client.UpdateLedger(ctx, &qldb.UpdateLedgerInput{
			Name:               &ledgerName,
			DeletionProtection: &deleteProtection,
		})
		if err != nil {
			log.Errorf("error updating ledger: %+v", err)
		}

		_, err = dbStorage.Client.DeleteLedger(ctx, &qldb.DeleteLedgerInput{
			Name: &ledgerName,
		})
		if err != nil {
			log.Errorf("error deleting ledger: %+v", err)
		}
	}

	log.Printf("done")
}
