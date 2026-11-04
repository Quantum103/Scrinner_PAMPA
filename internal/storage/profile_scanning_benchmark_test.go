package storage

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

// benchmarkRepository requires a dedicated database with this exact name,
// because the benchmarks truncate its tables before seeding synthetic data.
func benchmarkRepository(b *testing.B) (*Repository, *pgxpool.Pool) {
	b.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_BENCH_URL"))
	if databaseURL == "" {
		b.Skip("DATABASE_BENCH_URL is not set; PostgreSQL benchmarks skipped")
	}
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		b.Fatalf("parse DATABASE_BENCH_URL: %v", err)
	}
	if strings.Trim(parsed.Path, "/") != "rsi_scanner_bench" {
		b.Fatal("DATABASE_BENCH_URL must point to the dedicated rsi_scanner_bench database; this benchmark truncates its tables")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		b.Fatalf("create benchmark PostgreSQL pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		b.Fatalf("connect to benchmark PostgreSQL: %v", err)
	}
	if err := migrations.Up(ctx, pool, filepath.Join("..", "..", "migrations")); err != nil {
		pool.Close()
		b.Fatalf("apply benchmark migrations: %v", err)
	}
	repository, err := New(pool)
	if err != nil {
		pool.Close()
		b.Fatalf("create benchmark repository: %v", err)
	}
	b.Cleanup(pool.Close)
	return repository, pool
}

func seedWorkloadBenchmarkDB(b *testing.B, pool *pgxpool.Pool, instruments, profiles int) {
	b.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE TABLE telegram_users, instruments RESTART IDENTITY CASCADE`); err != nil {
		b.Fatalf("truncate benchmark tables: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO instruments (
			symbol, base_coin, quote_coin, settle_coin, contract_type, status,
			is_active, turnover_24h, last_price
		)
		SELECT 'LOAD' || lpad(sequence_number::text, 5, '0') || 'USDT', 'LOAD', 'USDT', 'USDT',
		       'LinearPerpetual', 'Trading', TRUE, (1000000 - sequence_number)::numeric, 1
		FROM generate_series(1, $1) AS generated(sequence_number)
	`, instruments); err != nil {
		b.Fatalf("seed benchmark instruments: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO telegram_users (telegram_id)
		SELECT 8000000000000 + sequence_number
		FROM generate_series(1, $1) AS generated(sequence_number)
	`, profiles); err != nil {
		b.Fatalf("seed benchmark Telegram users: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_settings (
			user_id, rsi_length, ema_length, instrument_limit, scan_all_instruments,
			notify_long, notify_short, notify_confluence
		)
		SELECT id,
		       2 + ((telegram_id - 8000000000000) % $2::bigint)::integer,
		       3, $1, FALSE, TRUE, TRUE, TRUE
		FROM telegram_users
	`, instruments, profiles); err != nil {
		b.Fatalf("seed benchmark user settings: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_timeframes (user_id, timeframe)
		SELECT users.id, frames.timeframe
		FROM telegram_users users
		CROSS JOIN (VALUES ('5'), ('15'), ('60')) AS frames(timeframe)
	`); err != nil {
		b.Fatalf("seed benchmark user timeframes: %v", err)
	}
}

func BenchmarkListScannerWorkloads(b *testing.B) {
	repository, pool := benchmarkRepository(b)
	for _, instruments := range []int{100, 500, 1000} {
		name := fmt.Sprintf("instruments=%d/profiles=30", instruments)
		b.Run(name, func(b *testing.B) {
			b.StopTimer()
			seedWorkloadBenchmarkDB(b, pool, instruments, 30)
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				workloads, err := repository.ListScannerWorkloads(context.Background())
				if err != nil {
					b.Fatalf("list scanner workloads: %v", err)
				}
				if len(workloads) != 90 {
					b.Fatalf("workload count = %d, want 90", len(workloads))
				}
				for _, workload := range workloads {
					if len(workload.Symbols) != instruments {
						b.Fatalf("profile %d/%d timeframe %s has %d symbols, want %d",
							workload.RSILength, workload.EMALength, workload.Timeframe, len(workload.Symbols), instruments)
					}
				}
			}
			b.ReportMetric(float64(instruments), "instruments")
			b.ReportMetric(30, "profiles")
			b.ReportMetric(float64(instruments*30*3), "profile-pairs/op")
		})
	}
}

func BenchmarkCommitScannerProfileCandleWithoutSignal(b *testing.B) {
	repository, pool := benchmarkRepository(b)
	seedWorkloadBenchmarkDB(b, pool, 1, 1)

	state := ScannerProfileState{
		Symbol: "LOAD00001USDT", Timeframe: "5", RSILength: 2, EMALength: 3,
		LastClose: 100, LastRSI: 50, LastEMARSI: 50, Initialized: true,
	}
	start := time.Now().UTC().Truncate(5 * time.Minute)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		state.LastProcessedCandleStart = start.Add(time.Duration(iteration+1) * 5 * time.Minute)
		id, inserted, queued, err := repository.CommitScannerProfileCandle(context.Background(), state, nil)
		if err != nil || id != 0 || inserted || queued != 0 {
			b.Fatalf("commit no-signal profile candle: id=%d inserted=%t queued=%d err=%v", id, inserted, queued, err)
		}
	}
	b.ReportMetric(1, "sql-execs/op")
}
