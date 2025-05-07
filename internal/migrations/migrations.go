package migrations

import (
	"embed"
	"errors"
	"fmt"
	"log/slog"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

//go:embed postgres/*.sql
var MigrationsFS embed.FS

func RunMigrations(pool *pgxpool.Pool) error {
	slog.Info("Running database migrations from embedded files")

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
			slog.Warn("Error closing migration source after migration failure", "error", srcErr)
		}
		if dbErr != nil {
			slog.Warn("Error closing migration database connection after migration failure", "error", dbErr)
		}
		return fmt.Errorf("migration failed: %w", err)
	}

	if srcErr != nil {
		slog.Warn("Error closing migration source", "error", srcErr)
	}
	if dbErr != nil {
		slog.Warn("Error closing migration database connection", "error", dbErr)
	}

	if errors.Is(err, migrate.ErrNoChange) {
		slog.Info("No database schema changes to apply")
	} else {
		slog.Info("Database migrations completed successfully")
	}

	return nil
}

type migrateLogAdapter struct{}

func (l *migrateLogAdapter) Printf(format string, v ...any) {
	slog.Info(fmt.Sprintf(format, v...))
}

func (l *migrateLogAdapter) Verbose() bool {
	return true
}
