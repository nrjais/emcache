package migrations

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"log"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed postgres/*.sql
var MigrationsFS embed.FS

func RunMigrations(databaseURL string) error {
	log.Println("Running database migrations from embedded files...")

	sourceInstance, err := iofs.New(MigrationsFS, "postgres")
	if err != nil {
		return fmt.Errorf("failed to create iofs source driver: %w", err)
	}
	defer func() {
		if cerr := sourceInstance.Close(); cerr != nil {
			log.Printf("Warning: error closing migration source instance: %v", cerr)
		}
	}()

	migrateDB, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return fmt.Errorf("failed to open database connection for migration: %w", err)
	}
	defer func() {
		if cerr := migrateDB.Close(); cerr != nil {
			log.Printf("Warning: error closing migration db connection: %v", cerr)
		}
	}()

	if err = migrateDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping database for migration: %w", err)
	}

	dbDriver, err := postgres.WithInstance(migrateDB, &postgres.Config{
		MigrationsTable: postgres.DefaultMigrationsTable,
	})
	if err != nil {
		return fmt.Errorf("could not create postgres driver instance: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", sourceInstance, "postgres", dbDriver)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	m.Log = &migrateLogAdapter{}

	err = m.Up()
	srcErr, dbErr := m.Close()

	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		if srcErr != nil {
			log.Printf("Warning: error closing migration source after migration failure: %v", srcErr)
		}
		if dbErr != nil {
			log.Printf("Warning: error closing migration database connection after migration failure: %v", dbErr)
		}
		return fmt.Errorf("migration failed: %w", err)
	}

	if srcErr != nil {
		log.Printf("Warning: error closing migration source: %v", srcErr)
	}
	if dbErr != nil {
		log.Printf("Warning: error closing migration database connection: %v", dbErr)
	}

	if errors.Is(err, migrate.ErrNoChange) {
		log.Println("No database schema changes to apply.")
	} else {
		log.Println("Database migrations completed successfully.")
	}

	return nil
}

type migrateLogAdapter struct{}

func (l *migrateLogAdapter) Printf(format string, v ...any) {
	log.Printf(format, v...)
}

func (l *migrateLogAdapter) Verbose() bool {
	return true
}
