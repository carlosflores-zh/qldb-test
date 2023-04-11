package storage

import (
	"context"

	"github.com/amzn/ion-go/ion"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"

	"github.com/carflores-zh/qldb-go/pkg/model"
	"github.com/carflores-zh/qldb-go/pkg/model/metadata"
)

func (db *DB) InsertImage(image *model.Image) error {
	_, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
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

		_, err = txn.Execute("UPDATE Image AS i SET i.id = ? WHERE i.imageId = ?", temp.DocumentID, image.ImageID)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})

	return err
}

func (db *DB) GetAllImages() ([]model.Image, error) {
	var images []model.Image

	c, err := db.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
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
