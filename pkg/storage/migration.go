package storage

import (
	"context"
	"errors"
	"time"

	"github.com/amzn/ion-go/ion"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"
	log "github.com/sirupsen/logrus"

	"github.com/carflores-zh/qldb-go/pkg/model"
)

// InsertMigration inserts a migration into the database
func (s *Store) InsertMigration(migration model.Migration) error {
	migration.UpdatedAt = time.Now()

	_, err := s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("INSERT INTO Migration ?", migration)
	})
	if err != nil {
		log.Errorf("Error inserting mig: %v", err)
		return err
	}

	return nil
}

// GetMigrations returns all migrations from the database
func (s *Store) GetMigrations() ([]model.Migration, error) {
	var resultMigrations []model.Migration

	c, err := s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT version, updatedAt, active FROM Migration")
		if err != nil {
			return nil, err
		}

		var versions []model.Migration
		for result.Next(txn) {
			ionBinary := result.GetCurrentData()

			temp := new(model.Migration)
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

	resultMigrations = c.([]model.Migration)

	return resultMigrations, nil
}

func (s *Store) MigrateDown(mostRecent model.Migration, version int, path string, migrationType string) error {
	for i := mostRecent.Version; i > version; i-- {
		fileLines, file, err := getFileScanner(path, migrationType, i)
		defer closeFile(file)

		// creates a transaction and executes statements
		_, err = s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			for fileLines.Scan() {
				_, errScan := txn.Execute(fileLines.Text())
				if errScan != nil {
					log.Errorf("Error creating table {%s}: %v", fileLines.Text(), err)
				}
			}
			return nil, nil
		})
		if err != nil {
			log.Errorf("Error creating tables: %v", err)
		}

		migration := model.Migration{
			Version:   i - 1,
			UpdatedAt: time.Now(),
			Active:    true,
		}

		time.Sleep(s5)

		err = s.InsertMigration(migration)
		if err != nil {
			log.Errorf("Error inserting migration: %v", err)
		}

		log.Printf("migration %d-%s executed", i, migrationType)
	}

	return nil
}

func (s *Store) MigrateUp(mostRecent model.Migration, version int, path string, migrationType string) error {
	for i := mostRecent.Version + 1; i <= version; i++ {
		fileLines, file, err := getFileScanner(path, migrationType, i)
		if err != nil {
			log.Printf("error getting file scanner: %v", err)
		}

		defer closeFile(file)

		// creates a transaction and executes statements
		_, err = s.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			var errScan error

			for fileLines.Scan() {
				log.Printf("sql: %s", fileLines.Text())
				isValid := isSQLValid(fileLines.Text())
				if !isValid {
					log.Printf("invalid sql: %s", fileLines.Text())
					return nil, errors.New("invalid sql")
				}

				_, errScan = txn.Execute(fileLines.Text())
				if errScan != nil {
					return nil, errScan
				}
			}

			return nil, errScan
		})
		if err != nil {
			log.Errorf("Error creating tables: %v", err)
			return err
		}

		migration := model.Migration{
			Version:   i,
			UpdatedAt: time.Now(),
			Active:    true,
		}

		time.Sleep(s5)

		err = s.InsertMigration(migration)
		if err != nil {
			log.Errorf("Error inserting migration: %v", err)
		}

		log.Printf("migration %d-%s executed", i, migrationType)
	}

	return nil
}

func (s *Store) MigrateQLDB(path string, version int) error {
	migrations, err := s.GetMigrations()
	if err != nil {
		log.Printf("no migrations found")
	}

	mostRecent := getMostRecentVersion(migrations)

	if (mostRecent.Version == version) && mostRecent.Active {
		log.Printf("Migration %d already executed", version)
		return nil
	}

	migrationType := getMigrationDirection(mostRecent, version)

	log.Printf("migrations from %d to %d", mostRecent.Version, version)

	if migrationType == "up" {
		err = s.MigrateUp(mostRecent, version, path, migrationType)
		if err != nil {
			return err
		}
	} else {
		err = s.MigrateDown(mostRecent, version, path, migrationType)
		if err != nil {
			return err
		}
	}

	return nil
}
