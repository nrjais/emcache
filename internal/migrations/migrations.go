package migrations

import (
	"embed"
	"errors"
	"fmt"
	"log"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

//go:embed postgres/*.sql
var MigrationsFS embed.FS

func RunMigrations(pool *pgxpool.Pool) error {
	log.Println("Running database migrations from embedded files...")

	sourceInstance, err := iofs.New(MigrationsFS, "postgres")
	if err != nil {
		return fmt.Errorf("failed to create iofs source driver: %w", err)
	}

	db := stdlib.OpenDBFromPool(pool)
	dbDriver, err := pgx.WithInstance(db, &pgx.Config{
		MigrationsTable: pgx.DefaultMigrationsTable,
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
