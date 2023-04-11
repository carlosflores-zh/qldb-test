package main

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/qldb"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cast"

	"github.com/carflores-zh/qldb-go/pkg/storage"
)

// PARAM 0: region
// PARAM 1: ledger name

func main() {
	params := os.Args[1:]

	region := params[0]

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		log.Error().Err(err).Msg("error loading config")
		return
	}

	db, err := storage.NewMigrator(cfg, "")
	if err != nil {
		log.Error().Err(err).Msg("error connecting/creating")
		return
	}

	defer db.Driver.Shutdown(ctx)

	list, err := db.Client.ListLedgers(ctx, &qldb.ListLedgersInput{})
	if err != nil {
		log.Error().Err(err).Msg("error listing ledgers")
		return
	}

	for _, ledger := range list.Ledgers {
		ledgerName := cast.ToString(ledger.Name)

		log.Info().Str("ledger", ledgerName).Msg("deleting ledger")

		deleteProtection := false

		_, err := db.Client.UpdateLedger(ctx, &qldb.UpdateLedgerInput{
			Name:               &ledgerName,
			DeletionProtection: &deleteProtection,
		})
		if err != nil {
			log.Error().Err(err).Msg("error updating ledger")
		}

		_, err = db.Client.DeleteLedger(ctx, &qldb.DeleteLedgerInput{
			Name: &ledgerName,
		})
		if err != nil {
			log.Error().Err(err).Msg("error deleting ledger")
		}
	}
}
