package main

import (
	"context"
	"os"

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

	driver, err := storage.Connect(ctx, region, ledgerName)
	if err != nil {
		log.Errorf("error connecting/creating: %v", err)
	}

	defer driver.Shutdown(context.Background())

	dbStorage := &storage.DB{
		Driver: driver,
	}

	err = dbStorage.MigrateQLDB(migrationFile)
	if err != nil {
		log.Errorf("error migrating QLDB: %v", err)
		return
	}
}
