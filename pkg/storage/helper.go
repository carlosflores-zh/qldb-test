package storage

import (
	"bufio"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cast"

	"github.com/carflores-zh/qldb-go/pkg/model"
)

func closeFile(file *os.File) {
	if err := file.Close(); err != nil {
		log.Error().Err(err).Msg("error closing file")
	}
}

func getFileScanner(path string, migrationType string, index int) (*bufio.Scanner, *os.File, error) {
	fullPath := path + migrationType + "/" + cast.ToString(index) + "-migration.sql"

	file, err := os.Open(fullPath)
	if err != nil {
		return nil, nil, err
	}

	scanner := bufio.NewScanner(file)

	return scanner, file, nil
}

func isSQLValid(fileLine string) bool {
	if strings.Contains(fileLine, "CREATE") ||
		strings.Contains(fileLine, "INSERT") ||
		strings.Contains(fileLine, "UPDATE") ||
		strings.Contains(fileLine, "DELETE") ||
		strings.Contains(fileLine, "DROP") {
		return true
	}

	return false
}

func getMigrationDirection(mostRecent model.Migration, version int) string {
	if mostRecent.Version > version {
		return downMigration
	}

	return upMigration
}

func getMostRecentVersion(migrations []model.Migration) model.Migration {
	mostRecent := model.Migration{
		Version:    0,
		MigratedAt: time.Time{},
	}

	for _, migration := range migrations {
		if migration.MigratedAt.After(mostRecent.MigratedAt) {
			mostRecent = migration
		}
	}

	log.Info().Int("version", mostRecent.Version).Msg("most recent version")

	return mostRecent
}
