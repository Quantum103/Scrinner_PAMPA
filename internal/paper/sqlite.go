package paper

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteRepository хранит все данные Paper Trading в одной общей базе SQLite.
type SQLiteRepository struct {
	db *sql.DB
}

func OpenSQLiteRepository(path string, busyTimeout time.Duration) (*SQLiteRepository, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("APP_DB_PATH не должен быть пустым")
	}
	if busyTimeout <= 0 {
		return nil, errors.New("APP_DB_BUSY_TIMEOUT должен быть положительным")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("получить абсолютный путь SQLite: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
		return nil, fmt.Errorf("создать каталог SQLite: %w", err)
	}
	sqlitePath := filepath.ToSlash(absolute)
	if len(sqlitePath) >= 2 && sqlitePath[1] == ':' {
		sqlitePath = "/" + sqlitePath // file:///C:/... на Windows и WSL
	}
	databaseURL := url.URL{Scheme: "file", Path: sqlitePath}
	query := databaseURL.Query()
	query.Set("_busy_timeout", strconv.FormatInt(busyTimeout.Milliseconds(), 10))
	query.Set("_journal_mode", "WAL")
	query.Set("_synchronous", "FULL")
	query.Set("_foreign_keys", "on")
	databaseURL.RawQuery = query.Encode()

	db, err := sql.Open("sqlite3", databaseURL.String())
	if err != nil {
		return nil, fmt.Errorf("открыть SQLite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	repository := &SQLiteRepository{db: db}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("проверить соединение SQLite: %w", err)
	}
	if err := repository.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := repository.IntegrityCheck(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return repository, nil
}

func (r *SQLiteRepository) migrate(ctx context.Context) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать миграцию SQLite: %w", err)
	}
	defer tx.Rollback()
	statements := []string{
		"CREATE TABLE IF NOT EXISTS paper_account (id INTEGER PRIMARY KEY CHECK(id=1), initial_balance_usdt REAL NOT NULL, balance REAL NOT NULL, realized_pnl REAL NOT NULL, total_fees REAL NOT NULL, net_funding REAL NOT NULL, updated_at_ns INTEGER NOT NULL)",
		"CREATE TABLE IF NOT EXISTS paper_open_positions (symbol TEXT PRIMARY KEY, trade_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL)",
		"CREATE TABLE IF NOT EXISTS paper_trades (trade_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, opened_at_ns INTEGER NOT NULL, closed_at_ns INTEGER NOT NULL, net_pnl REAL NOT NULL, close_reason TEXT NOT NULL, payload TEXT NOT NULL)",
		"CREATE INDEX IF NOT EXISTS idx_paper_trades_symbol_closed ON paper_trades(symbol, closed_at_ns)",
		"CREATE TABLE IF NOT EXISTS paper_equity_history (recorded_at_ns INTEGER PRIMARY KEY, balance REAL NOT NULL, equity REAL NOT NULL, realized_pnl REAL NOT NULL, unrealized_pnl REAL NOT NULL)",
		"CREATE TABLE IF NOT EXISTS paper_processed_signals (signal_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, signal_at_ns INTEGER NOT NULL)",
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("создать схему SQLite: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, "PRAGMA user_version = 1"); err != nil {
		return fmt.Errorf("обновить версию схемы SQLite: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать миграцию SQLite: %w", err)
	}
	return nil
}

func (r *SQLiteRepository) Load(ctx context.Context) (PersistentState, error) {
	state := PersistentState{}
	var updatedAt int64
	err := r.db.QueryRowContext(ctx,
		"SELECT initial_balance_usdt, balance, realized_pnl, total_fees, net_funding, updated_at_ns FROM paper_account WHERE id=1",
	).Scan(&state.InitialBalanceUSDT, &state.Balance, &state.RealizedPnL, &state.TotalFees, &state.NetFunding, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return PersistentState{}, fmt.Errorf("загрузить Paper Trading account: %w", err)
	}
	state.Found = true
	state.UpdatedAt = time.Unix(0, updatedAt).UTC()

	positionRows, err := r.db.QueryContext(ctx, "SELECT payload FROM paper_open_positions ORDER BY symbol")
	if err != nil {
		return PersistentState{}, fmt.Errorf("загрузить открытые позиции: %w", err)
	}
	for positionRows.Next() {
		var payload string
		var position Position
		if err := positionRows.Scan(&payload); err != nil {
			positionRows.Close()
			return PersistentState{}, fmt.Errorf("прочитать открытую позицию: %w", err)
		}
		if err := json.Unmarshal([]byte(payload), &position); err != nil {
			positionRows.Close()
			return PersistentState{}, fmt.Errorf("декодировать открытую позицию: %w", err)
		}
		state.OpenPositions = append(state.OpenPositions, position)
	}
	if err := positionRows.Err(); err != nil {
		positionRows.Close()
		return PersistentState{}, fmt.Errorf("обойти открытые позиции: %w", err)
	}
	if err := positionRows.Close(); err != nil {
		return PersistentState{}, err
	}

	tradeRows, err := r.db.QueryContext(ctx, "SELECT payload FROM paper_trades ORDER BY closed_at_ns, trade_id")
	if err != nil {
		return PersistentState{}, fmt.Errorf("загрузить историю сделок: %w", err)
	}
	for tradeRows.Next() {
		var payload string
		var trade Position
		if err := tradeRows.Scan(&payload); err != nil {
			tradeRows.Close()
			return PersistentState{}, fmt.Errorf("прочитать закрытую сделку: %w", err)
		}
		if err := json.Unmarshal([]byte(payload), &trade); err != nil {
			tradeRows.Close()
			return PersistentState{}, fmt.Errorf("декодировать закрытую сделку: %w", err)
		}
		state.ClosedTrades = append(state.ClosedTrades, trade)
	}
	if err := tradeRows.Err(); err != nil {
		tradeRows.Close()
		return PersistentState{}, fmt.Errorf("обойти историю сделок: %w", err)
	}
	if err := tradeRows.Close(); err != nil {
		return PersistentState{}, err
	}

	equityRows, err := r.db.QueryContext(ctx, "SELECT recorded_at_ns, balance, equity, realized_pnl, unrealized_pnl FROM paper_equity_history ORDER BY recorded_at_ns")
	if err != nil {
		return PersistentState{}, fmt.Errorf("загрузить историю капитала: %w", err)
	}
	for equityRows.Next() {
		var at int64
		var point EquityPoint
		if err := equityRows.Scan(&at, &point.Balance, &point.Equity, &point.RealizedPnL, &point.UnrealizedPnL); err != nil {
			equityRows.Close()
			return PersistentState{}, fmt.Errorf("прочитать точку капитала: %w", err)
		}
		point.At = time.Unix(0, at).UTC()
		state.EquityHistory = append(state.EquityHistory, point)
	}
	if err := equityRows.Err(); err != nil {
		equityRows.Close()
		return PersistentState{}, fmt.Errorf("обойти историю капитала: %w", err)
	}
	if err := equityRows.Close(); err != nil {
		return PersistentState{}, err
	}

	signalRows, err := r.db.QueryContext(ctx, "SELECT signal_id FROM paper_processed_signals ORDER BY signal_at_ns, signal_id")
	if err != nil {
		return PersistentState{}, fmt.Errorf("загрузить обработанные сигналы: %w", err)
	}
	for signalRows.Next() {
		var id string
		if err := signalRows.Scan(&id); err != nil {
			signalRows.Close()
			return PersistentState{}, fmt.Errorf("прочитать ID сигнала: %w", err)
		}
		state.ProcessedSignalIDs = append(state.ProcessedSignalIDs, id)
	}
	if err := signalRows.Err(); err != nil {
		signalRows.Close()
		return PersistentState{}, fmt.Errorf("обойти обработанные сигналы: %w", err)
	}
	if err := signalRows.Close(); err != nil {
		return PersistentState{}, err
	}
	return state, nil
}

// Save фиксирует счёт, позицию, сделки, equity curve и обработанные сигналы
// в одном COMMIT. Закрытие не может оставить баланс и историю в разных состояниях.
func (r *SQLiteRepository) Save(ctx context.Context, state PersistentState) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать транзакцию Paper Trading: %w", err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx,
		"INSERT INTO paper_account(id, initial_balance_usdt, balance, realized_pnl, total_fees, net_funding, updated_at_ns) VALUES(1,?,?,?,?,?,?) "+
			"ON CONFLICT(id) DO UPDATE SET initial_balance_usdt=excluded.initial_balance_usdt, balance=excluded.balance, realized_pnl=excluded.realized_pnl, total_fees=excluded.total_fees, net_funding=excluded.net_funding, updated_at_ns=excluded.updated_at_ns",
		state.InitialBalanceUSDT, state.Balance, state.RealizedPnL, state.TotalFees, state.NetFunding, state.UpdatedAt.UnixNano())
	if err != nil {
		return fmt.Errorf("сохранить Paper Trading account: %w", err)
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM paper_open_positions"); err != nil {
		return fmt.Errorf("обновить открытые позиции: %w", err)
	}
	for _, position := range state.OpenPositions {
		payload, err := json.Marshal(position)
		if err != nil {
			return fmt.Errorf("сериализовать позицию %s: %w", position.Symbol, err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO paper_open_positions(symbol, trade_id, payload) VALUES(?,?,?)", position.Symbol, position.TradeID, string(payload)); err != nil {
			return fmt.Errorf("сохранить позицию %s: %w", position.Symbol, err)
		}
	}
	for _, trade := range state.ClosedTrades {
		payload, err := json.Marshal(trade)
		if err != nil {
			return fmt.Errorf("сериализовать сделку %s: %w", trade.TradeID, err)
		}
		_, err = tx.ExecContext(ctx,
			"INSERT INTO paper_trades(trade_id, symbol, opened_at_ns, closed_at_ns, net_pnl, close_reason, payload) VALUES(?,?,?,?,?,?,?) "+
				"ON CONFLICT(trade_id) DO UPDATE SET symbol=excluded.symbol, opened_at_ns=excluded.opened_at_ns, closed_at_ns=excluded.closed_at_ns, net_pnl=excluded.net_pnl, close_reason=excluded.close_reason, payload=excluded.payload",
			trade.TradeID, trade.Symbol, trade.OpenedAt.UnixNano(), trade.ClosedAt.UnixNano(), trade.RealizedPnL, trade.CloseReason, string(payload))
		if err != nil {
			return fmt.Errorf("сохранить сделку %s: %w", trade.TradeID, err)
		}
	}
	for _, point := range state.EquityHistory {
		if point.At.IsZero() {
			continue
		}
		_, err := tx.ExecContext(ctx,
			"INSERT INTO paper_equity_history(recorded_at_ns, balance, equity, realized_pnl, unrealized_pnl) VALUES(?,?,?,?,?) "+
				"ON CONFLICT(recorded_at_ns) DO UPDATE SET balance=excluded.balance, equity=excluded.equity, realized_pnl=excluded.realized_pnl, unrealized_pnl=excluded.unrealized_pnl",
			point.At.UnixNano(), point.Balance, point.Equity, point.RealizedPnL, point.UnrealizedPnL)
		if err != nil {
			return fmt.Errorf("сохранить историю капитала: %w", err)
		}
	}
	for _, signal := range state.ProcessedSignalIDs {
		symbol := signalSymbol(signal)
		at := signalTimestamp(signal)
		_, err := tx.ExecContext(ctx,
			"INSERT OR IGNORE INTO paper_processed_signals(signal_id, symbol, signal_at_ns) VALUES(?,?,?)",
			signal, symbol, at)
		if err != nil {
			return fmt.Errorf("сохранить ID обработанного сигнала: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать транзакцию Paper Trading: %w", err)
	}
	return nil
}

func signalSymbol(id string) string {
	parts := strings.SplitN(id, "|", 4)
	if len(parts) < 3 {
		return ""
	}
	return parts[0]
}

func signalTimestamp(id string) int64 {
	parts := strings.SplitN(id, "|", 4)
	if len(parts) < 3 {
		return 0
	}
	value, _ := strconv.ParseInt(parts[2], 10, 64)
	return value
}

func (r *SQLiteRepository) IntegrityCheck(ctx context.Context) error {
	var result string
	if err := r.db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil {
		return fmt.Errorf("выполнить PRAGMA integrity_check: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("SQLite integrity_check обнаружил проблему: %s", result)
	}
	return nil
}

func (r *SQLiteRepository) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}
