package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/rs/zerolog/log"

	"github.com/carflores-zh/qldb-go/pkg/model"
	"github.com/carflores-zh/qldb-go/pkg/storage"
)

const ledgerName = "ledger"
const region = "us-east-2"

// TODO: making change to a policy two signers to accept that, propagate change to all nodes
// TODO: store private key to Store

func main() {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		log.Error().Err(err).Msg("error loading config")
		return
	}

	db, err := storage.New(cfg, ledgerName)
	if err != nil {
		log.Error().Err(err).Msg("error connecting/creating")
		return
	}

	defer db.Driver.Shutdown(ctx)

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
	contractResult, err := db.SelectContractActive(testContract.ID)
	if err != nil {
		log.Error().Err(err).Msg("error selecting contract")
	}

	log.Error().Msgf("Contract: %+v", contractResult)

	// update an existing record to create a new revision
	testContract.Execution = true
	testContract.Network = "ethereum"

	err = db.UpdateContract(testContract)
	if err != nil {
		log.Error().Err(err).Msg("error updating contract")
	}

	// select all existing version for an ID
	versions, err := db.SelectContractVersion(testContract.ID)
	if err != nil {
		log.Error().Err(err).Msg("error selecting contract version")
	}

	// get a specific version of that ID
	if len(versions) > 1 {
		wantedVersion := versions[1]

		c3, errSelect := db.SelectContractInstance(testContract.ID, wantedVersion.Version)
		if errSelect != nil {
			log.Error().Err(errSelect).Msg("error selecting contract instance")
		}

		log.Printf("Contract instance: %v", c3)
	}

	// Validate if a table has redactions
	hasRedactions, err := db.HasDataRedaction("Contract")
	if err != nil {
		log.Error().Err(err).Msg("error checking redactions")
	}

	log.Info().Msgf("Has redactions: %v", hasRedactions)
}
