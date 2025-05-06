package client

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	pb "github.com/nrjais/emcache/pkg/protos"
	"google.golang.org/protobuf/types/known/structpb"
)

type sqliteCollection struct {
	db      *sql.DB
	version int32
	details *pb.Collection
}

func openSQLiteDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite db (%s): %w", dbPath, err)
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func (sc *sqliteCollection) close() error {
	if sc.db != nil {
		return sc.db.Close()
	}
	return nil
}

func (sc *sqliteCollection) applyOplogEntries(ctx context.Context, entries []*pb.OplogEntry) error {
	if sc.db == nil {
		return fmt.Errorf("database connection is not available")
	}

	tx, err := sc.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	const dataTableName = "data"

	for _, entry := range entries {
		const pkColumn = "id"
		pkValue := entry.Id
		if sc.version != entry.Version {
			// Skip entries with a version that is different from the current collection's version
			// TODO: Add realtime swap of the database file here to latest version
			continue
		}

		switch entry.Operation {
		case pb.OplogEntry_UPSERT:
			columns := []string{quoteIdentifier(pkColumn)}
			values := []any{pkValue}
			placeholders := []string{"?"}

			if sc.details == nil || sc.details.Shape == nil {
				return fmt.Errorf("collection details are not available")
			}

			for _, column := range sc.details.Shape.Columns {
				colName := column.Name
				if entry.Data == nil || entry.Data.Fields == nil {
					return fmt.Errorf("missing data for column %s", colName)
				}
				valProto := entry.Data.Fields[colName]
				columns = append(columns, quoteIdentifier(colName))
				values = append(values, valueFromProto(valProto))
				placeholders = append(placeholders, "?")
			}

			sql := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
				quoteIdentifier(dataTableName),
				strings.Join(columns, ", "),
				strings.Join(placeholders, ", "))

			_, err = tx.ExecContext(ctx, sql, values...)
			if err != nil {
				return fmt.Errorf("failed to execute UPSERT oplog entry index %d for ID %s (%s): %w", entry.Index, pkValue, sql, err)
			}

		case pb.OplogEntry_DELETE:
			sql := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdentifier(dataTableName), quoteIdentifier(pkColumn))

			_, err = tx.ExecContext(ctx, sql, pkValue)
			if err != nil {
				return fmt.Errorf("failed to execute DELETE oplog entry index %d for ID %s (%s): %w", entry.Index, pkValue, sql, err)
			}
		default:
			// Ignore unknown operation types
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction after applying oplog entries: %w", err)
	}

	return nil
}

func (sc *sqliteCollection) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if sc.db == nil {
		return nil, fmt.Errorf("database connection is not available")
	}

	rows, err := sc.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return rows, nil
}

func valueFromProto(v *structpb.Value) any {
	switch k := v.Kind.(type) {
	case *structpb.Value_NullValue:
		return nil
	case *structpb.Value_NumberValue:
		return k.NumberValue
	case *structpb.Value_StringValue:
		return k.StringValue
	case *structpb.Value_BoolValue:
		return k.BoolValue
	case *structpb.Value_StructValue:
		jsonBytes, err := k.StructValue.MarshalJSON()
		if err != nil {
			// TODO: Log or handle error
			return nil
		}
		return string(jsonBytes)
	case *structpb.Value_ListValue:
		jsonBytes, err := k.ListValue.MarshalJSON()
		if err != nil {
			// TODO: Log or handle error
			return nil
		}
		return string(jsonBytes)
	default:
		return nil
	}
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
