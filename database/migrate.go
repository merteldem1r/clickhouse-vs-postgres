package database

import (
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/clickhouse"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func RunMigrations(migrationDB string, dbDSN string, migrationPath string) error {
	source := "file://" + migrationPath
	m, err := migrate.New(source, dbDSN)
	if err != nil {
		return err
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}

	version, dirty, err := m.Version()
	if err != nil {
		return fmt.Errorf("migrate: get version: %w", err)
	}
	m.Close()

	fmt.Printf("%s Migrations: version: %d, dirty: %v\n", migrationDB, version, dirty)
	return nil
}
