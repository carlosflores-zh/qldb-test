package main

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/qldb"
	"github.com/aws/aws-sdk-go-v2/service/qldb/types"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cast"

	"github.com/carflores-zh/qldb-go/pkg/storage"
)

const time3Minutes = 3 * time.Minute
const inputParams = 3

// PARAM 0: region
// PARAM 1: ledger name
// PARAM 2: version

func main() {
	params := os.Args[1:]

	if len(params) < inputParams {
		log.Fatal().Msg("not enough params")
	}

	region := params[0]
	ledgerName := params[1]
	version := cast.ToInt(params[2])

	log.Info().Str("region", region).Str("ledger", ledgerName).Int("version", version).Msg("starting migration")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		log.Error().Err(err).Msg("error loading config")
		return
	}

	db, err := storage.NewMigrator(cfg, ledgerName)
	if err != nil {
		log.Error().Err(err).Msg("error connecting/creating")
		return
	}

	defer db.Driver.Shutdown(context.TODO())

	ledgerInput := qldb.CreateLedgerInput{
		Name:            &ledgerName,
		PermissionsMode: types.PermissionsModeStandard,
	}

	ctx := context.Background()

	// create ledger and wait for it to be active
	for {
		// getting the list of ledgers
		list, errList := db.Client.ListLedgers(ctx, nil)
		if errList != nil {
			log.Error().Err(errList).Msg("error listing ledgers")
			return
		}

		index := -1

		for i, ledger := range list.Ledgers {
			if *ledger.Name == ledgerName {
				index = i
			}
		}

		if index == -1 {
			log.Info().Str("ledger", ledgerName).Msg("ledger not found, creating it")

			_, err = db.Client.CreateLedger(ctx, &ledgerInput)
			if err != nil {
				log.Error().Err(err).Msg("error creating ledger")
			}

			continue
		}

		if list.Ledgers[index].State == types.LedgerStateActive {
			break
		} else {
			log.Info().Str("ledger", ledgerName).Str("state", string(list.Ledgers[index].State)).Msg("waiting 3 minutes for ledger to be active")
			time.Sleep(time3Minutes)
		}
	}

	err = db.MigrateQLDB("sql/", version)
	if err != nil {
		log.Error().Err(err).Msg("migration failed")
		return
	}
}
