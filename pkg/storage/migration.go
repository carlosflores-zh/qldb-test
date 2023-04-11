package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/amzn/ion-go/ion"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"
	"github.com/rs/zerolog/log"

	"github.com/carflores-zh/qldb-go/pkg/model"
)

// InsertMigration inserts a migration into the database
func (dbm *DBMigrator) InsertMigration(migration model.Migration) error {
	migration.MigratedAt = time.Now()

	_, err := dbm.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("INSERT INTO Migration ?", migration)
	})
	if err != nil {
		return err
	}

	return nil
}

// GetMigrations returns all migrations from the database
func (dbm *DBMigrator) GetMigrations() ([]model.Migration, error) {
	var resultMigrations []model.Migration

	c, err := dbm.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT version, migratedAt, active FROM Migration")
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

func (dbm *DBMigrator) MigrateDown(mostRecent model.Migration, version int, path string, migrationType string) error {
	for i := mostRecent.Version; i > version; i-- {
		fileLines, file, err := getFileScanner(path, migrationType, i)
		defer closeFile(file)

		// creates a transaction and executes statements
		_, err = dbm.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			for fileLines.Scan() {
				_, errScan := txn.Execute(fileLines.Text())
				if errScan != nil {
					log.Error().Msgf("Error creating table {%s}: %v", fileLines.Text(), err)
				}
			}
			return nil, nil
		})
		if err != nil {
			log.Error().Err(err).Msg("Error creating tables")
		}

		migration := model.Migration{
			Version:    i - 1,
			MigratedAt: time.Now(),
		}

		time.Sleep(waitForTables)

		err = dbm.InsertMigration(migration)
		if err != nil {
			log.Error().Err(err).Msg("Error inserting migration")
		}

		log.Printf("migration %d-%s executed", i, migrationType)
	}

	return nil
}

func (dbm *DBMigrator) MigrateUp(mostRecent model.Migration, version int, path string, migrationType string) error {
	for i := mostRecent.Version + 1; i <= version; i++ {
		fileLines, file, err := getFileScanner(path, migrationType, i)
		if err != nil {
			log.Printf("error getting file scanner: %v", err)
		}

		defer closeFile(file)

		// creates a transaction and executes statements
		_, err = dbm.Driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			var errScan error

			for fileLines.Scan() {
				log.Info().Msgf("sql: %s", fileLines.Text())
				isValid := isSQLValid(fileLines.Text())
				if !isValid {
					log.Error().Msgf("invalid sql: %s", fileLines.Text())
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
			log.Error().Err(err).Msg("Error creating tables")
			return err
		}

		migration := model.Migration{
			Version:    i,
			MigratedAt: time.Now(),
		}

		// wait for creation of tables
		time.Sleep(waitForTables)

		err = dbm.InsertMigration(migration)
		if err != nil {
			log.Error().Err(err).Msg("Error inserting migration")
		}

		log.Info().Msgf("migration %d-%s executed", i, migrationType)
	}

	return nil
}

func (dbm *DBMigrator) MigrateQLDB(path string, version int) error {
	migrations, err := dbm.GetMigrations()
	if err != nil {
		log.Info().Msg("no migrations found")
	}

	fmt.Printf("migrations: %v", migrations)

	mostRecent := getMostRecentVersion(migrations)

	if mostRecent.Version == version {
		log.Info().Msgf("database is already at version %d", version)
		return nil
	}

	migrationType := getMigrationDirection(mostRecent, version)

	log.Info().Msgf("migrations from %d to %d", mostRecent.Version, version)

	if migrationType == "up" {
		err = dbm.MigrateUp(mostRecent, version, path, migrationType)
		if err != nil {
			return err
		}
	} else {
		err = dbm.MigrateDown(mostRecent, version, path, migrationType)
		if err != nil {
			return err
		}
	}

	return nil
}
