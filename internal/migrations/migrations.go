package migrations

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const advisoryLockID int64 = 0x5253495343414E

var migrationFilePattern = regexp.MustCompile(`^([0-9]{6,})_([a-z0-9_]+)\.(up|down)\.sql$`)

type migration struct {
	version int64
	name    string
	up      string
	down    string
}

// Up applies every migration that has not yet been recorded in PostgreSQL.
func Up(ctx context.Context, pool *pgxpool.Pool, directory string) error {
	return run(ctx, pool, directory, true)
}

// Down rolls back the most recently applied migration by one step.
func Down(ctx context.Context, pool *pgxpool.Pool, directory string) error {
	return run(ctx, pool, directory, false)
}

func run(ctx context.Context, pool *pgxpool.Pool, directory string, up bool) error {
	if pool == nil {
		return fmt.Errorf("migration pool must not be nil")
	}
	items, err := load(directory)
	if err != nil {
		return err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version BIGINT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockID); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryLockID)
	}()

	applied, err := readApplied(ctx, conn)
	if err != nil {
		return err
	}
	byVersion := make(map[int64]migration, len(items))
	for _, item := range items {
		byVersion[item.version] = item
	}
	for version := range applied {
		if _, ok := byVersion[version]; !ok {
			return fmt.Errorf("database contains migration %d which is missing from %s", version, directory)
		}
	}

	if up {
		for _, item := range items {
			if _, ok := applied[item.version]; ok {
				continue
			}
			if err := execute(ctx, conn, item.version, item.up, true); err != nil {
				return fmt.Errorf("apply migration %06d_%s: %w", item.version, item.name, err)
			}
		}
		return nil
	}

	for i := len(items) - 1; i >= 0; i-- {
		item := items[i]
		if _, ok := applied[item.version]; !ok {
			continue
		}
		if err := execute(ctx, conn, item.version, item.down, false); err != nil {
			return fmt.Errorf("rollback migration %06d_%s: %w", item.version, item.name, err)
		}
		break
	}
	return nil
}

func execute(ctx context.Context, conn *pgxpool.Conn, version int64, script string, up bool) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.Background())
		}
	}()

	if _, err := tx.Exec(ctx, script, pgx.QueryExecModeSimpleProtocol); err != nil {
		return fmt.Errorf("execute SQL: %w", err)
	}
	if up {
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, version); err != nil {
			return fmt.Errorf("record migration: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, version); err != nil {
			return fmt.Errorf("remove migration record: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	committed = true
	return nil
}

func readApplied(ctx context.Context, conn *pgxpool.Conn) (map[int64]struct{}, error) {
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("read applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int64]struct{})
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		applied[version] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}
	return applied, nil
}

func load(directory string) ([]migration, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("read migrations directory %q: %w", directory, err)
	}

	byVersion := make(map[int64]*migration)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		matches := migrationFilePattern.FindStringSubmatch(entry.Name())
		if matches == nil {
			continue
		}
		version, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse migration version in %q: %w", entry.Name(), err)
		}
		item := byVersion[version]
		if item == nil {
			item = &migration{version: version, name: matches[2]}
			byVersion[version] = item
		}
		if item.name != matches[2] {
			return nil, fmt.Errorf("migration version %d has multiple names", version)
		}
		content, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}
		if matches[3] == "up" {
			if item.up != "" {
				return nil, fmt.Errorf("duplicate up migration for version %d", version)
			}
			item.up = string(content)
		} else {
			if item.down != "" {
				return nil, fmt.Errorf("duplicate down migration for version %d", version)
			}
			item.down = string(content)
		}
	}

	result := make([]migration, 0, len(byVersion))
	for _, item := range byVersion {
		if item.up == "" || item.down == "" {
			return nil, fmt.Errorf("migration %06d_%s must have both .up.sql and .down.sql files", item.version, item.name)
		}
		result = append(result, *item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].version < result[j].version })
	return result, nil
}
