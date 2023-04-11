package main

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/rs/zerolog/log"

	"github.com/carflores-zh/qldb-go/pkg/model"
	"github.com/carflores-zh/qldb-go/pkg/storage"
)

const ledgerName = "ledger"
const region = "us-east-2"

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
	}

	defer db.Driver.Shutdown(ctx)

	testContract := &model.Contract{
		Address:   "12x3124",
		Input:     "0x123",
		Output:    "0x123",
		Network:   "0x123",
		SendFunds: true,
		Execution: false,
	}

	id, err := db.InsertContractTx(testContract)
	if err != nil {
		log.Error().Err(err).Msg("error inserting contract")
	}

	log.Printf("Contract: %+v", testContract)

	testContract.Network = "ethereum"
	testContract.ID = id

	err = db.UpdateContract(testContract)
	if err != nil {
		log.Error().Err(err).Msg("error updating contract")
	}

	imageDocument := &model.Image{
		ImageID: "123",
	}

	document, err := json.Marshal(imageDocument)
	if err != nil {
		log.Error().Err(err).Msg("error marshalling image document")
		return
	}

	image := &model.Image{
		ImageID:  "123",
		Document: document,
	}

	err = db.InsertImage(image)
	if err != nil {
		log.Error().Err(err).Msg("error inserting image")
	}

	images, err := db.GetAllImages()
	if err != nil {
		log.Error().Err(err).Msg("error getting all images")
	}

	if len(images) == 0 {
		log.Info().Msg("no images found")
	}

	for _, image := range images {
		log.Printf("Image: %+v", image)

		res := &model.Image{}

		err := json.Unmarshal(image.Document, &res)
		if err != nil {
			log.Error().Err(err).Msg("error unmarshalling image document")
			return
		}

		log.Printf("Image unmarshalled: %+v", res)
	}
}
