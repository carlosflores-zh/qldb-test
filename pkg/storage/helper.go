package storage

import (
	"bufio"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"

	"github.com/carflores-zh/qldb-go/pkg/model"
)

func closeFile(file *os.File) {
	if err := file.Close(); err != nil {
		log.Errorf("Error closing file: %v", err)
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
		Version:   -1,
		UpdatedAt: time.Time{},
	}

	for _, migration := range migrations {
		if migration.UpdatedAt.After(mostRecent.UpdatedAt) {
			mostRecent = migration
		}
	}

	log.Printf("most recent version: %d", mostRecent.Version)

	return mostRecent
}
