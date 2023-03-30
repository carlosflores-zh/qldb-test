package storage

import (
	"context"
	log "github.com/sirupsen/logrus"

	"github.com/amzn/ion-go/ion"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"

	"github.com/carflores-zh/qldb-go/pkg/model"
	"github.com/carflores-zh/qldb-go/pkg/model/metadata"
)

func (s *Store) InsertImage(image *model.Image) error {
	_, err := s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		temp := new(metadata.Result)
		image.ID = ""

		result, err := txn.Execute("INSERT INTO Image ?", image)
		if err != nil {
			return nil, err
		}

		result.Next(txn)
		err = ion.Unmarshal(result.GetCurrentData(), temp)
		if err != nil {
			return nil, err
		}

		image.ID = temp.DocumentID

		log.Println("Image ID: ", image.ID, temp)

		result, err = txn.Execute("UPDATE Image AS i SET i.id = ? WHERE i.imageId = ?", temp.DocumentID, image.ImageID)
		if err != nil {
			log.Printf("Error updating image: %v", err)
			return nil, err
		}

		result.Next(txn)
		log.Printf("Result: %v", string(result.GetCurrentData()))

		return nil, nil
	})

	return err
}

func (s *Store) GetAllImages() ([]model.Image, error) {
	var images []model.Image

	c, err := s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT id,document,signature1,signature2 FROM Image")
		if err != nil {
			return nil, err
		}

		var versions []model.Image
		for result.Next(txn) {
			ionBinary := result.GetCurrentData()

			temp := new(model.Image)
			err = ion.Unmarshal(ionBinary, temp)
			if err != nil {
				return nil, err
			}

			versions = append(versions, *temp)
		}
		if result.Err() != nil {
			return nil, result.Err()
		}

		return versions, nil
	})

	if err != nil {
		return nil, err
	}

	images = c.([]model.Image)

	return images, nil
}
