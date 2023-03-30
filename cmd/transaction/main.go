package main

import (
	"context"
	"encoding/json"

	log "github.com/sirupsen/logrus"

	"github.com/carflores-zh/qldb-go/pkg/model"
	"github.com/carflores-zh/qldb-go/pkg/storage"
)

const ledgerName = "ledger"
const region = "us-east-2"

func main() {
	ctx := context.Background()

	driver, client, err := storage.Connect("", region, ledgerName)
	if err != nil {
		log.Errorf("error connecting/creating: %v", err)
	}

	defer driver.Shutdown(ctx)

	dbStorage := &storage.Store{
		Driver: driver,
		Client: client,
	}

	testContract := &model.Contract{
		Address:   "12x3124",
		Input:     "0x123",
		Output:    "0x123",
		Network:   "0x123",
		SendFunds: true,
		Execution: false,
	}

	id, err := dbStorage.InsertContractTx(testContract)
	if err != nil {
		log.Errorf("Error inserting contract tx: %v", err)
	}

	log.Printf("Contract: %+v", testContract)

	testContract.Network = "ethereum"
	testContract.ID = id

	err = dbStorage.UpdateContract(testContract)
	if err != nil {
		log.Errorf("Error updating contract: %v", err)
	}

	imageDocument := &model.Image{
		ImageID: "123",
	}

	document, err := json.Marshal(imageDocument)
	if err != nil {
		log.Errorf("Error marshaling image document: %v", err)
		return
	}

	image := &model.Image{
		ImageID:    "123",
		Document:   document,
		Signature1: "signature1",
	}

	err = dbStorage.InsertImage(image)
	if err != nil {
		log.Errorf("Error inserting image: %v", err)
	}

	images, err := dbStorage.GetAllImages()
	if err != nil {
		log.Errorf("Error getting all images: %v", err)
	}

	if len(images) == 0 {
		log.Errorf("No images found")
	}

	for _, image := range images {
		log.Printf("Image: %+v", image)

		res := &model.Image{}

		err := json.Unmarshal(image.Document, &res)
		if err != nil {
			log.Errorf("Error unmarshalling image document: %v", err)
			return
		}

		log.Printf("Image unmarshalled: %+v", res)
	}
}
