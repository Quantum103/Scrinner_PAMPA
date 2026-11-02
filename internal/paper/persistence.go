package paper

import (
	"context"
	"time"
)

// Repository — единое хранилище состояния Paper Trading.
// Реализация SQLite использует одну БД приложения и сохраняет полный снимок
// в одной транзакции, чтобы счёт, позиция, сделки и equity curve не расходились.
type Repository interface {
	Load(ctx context.Context) (PersistentState, error)
	Save(ctx context.Context, state PersistentState) error
	IntegrityCheck(ctx context.Context) error
	Close() error
}

type PersistentState struct {
	Found              bool
	InitialBalanceUSDT float64
	Balance            float64
	RealizedPnL        float64
	TotalFees          float64
	NetFunding         float64
	OpenPositions      []Position
	ClosedTrades       []Position
	EquityHistory      []EquityPoint
	ProcessedSignalIDs []string
	UpdatedAt          time.Time
}

type EquityPoint struct {
	At           time.Time `json:"at"`
	Balance      float64   `json:"balance"`
	Equity       float64   `json:"equity"`
	RealizedPnL  float64   `json:"realized_pnl"`
	UnrealizedPnL float64  `json:"unrealized_pnl"`
}

type PaperStatistics struct {
	Mode                    string                  `json:"mode"`
	TradeCount              int                     `json:"trade_count"`
	WinRatePercent          float64                 `json:"win_rate_percent"`
	NetPnL                  float64                 `json:"net_pnl"`
	ProfitableTrades        int                     `json:"profitable_trades"`
	LosingTrades            int                     `json:"losing_trades"`
	AverageProfit           float64                 `json:"average_profit"`
	AverageLoss             float64                 `json:"average_loss"`
	ProfitFactor             *float64                `json:"profit_factor,omitempty"`
	InfiniteProfitFactor    bool                    `json:"infinite_profit_factor"`
	MaxDrawdownUSDT         float64                 `json:"max_drawdown_usdt"`
	MaxDrawdownPercent      float64                 `json:"max_drawdown_percent"`
	AverageRealizedRR       float64                 `json:"average_realized_rr"`
	MaxRealizedRR           float64                 `json:"max_realized_rr"`
	TotalFees               float64                 `json:"total_fees"`
	TotalSlippageUSDT       float64                 `json:"total_slippage_usdt"`
	NetFunding              float64                 `json:"net_funding"`
	BySymbol                map[string]SymbolStats  `json:"by_symbol"`
	ByCloseReason           map[string]int          `json:"by_close_reason"`
}

type SymbolStats struct {
	TradeCount       int     `json:"trade_count"`
	WinRatePercent   float64 `json:"win_rate_percent"`
	NetPnL           float64 `json:"net_pnl"`
	ProfitableTrades int     `json:"profitable_trades"`
	LosingTrades     int     `json:"losing_trades"`
	TotalFees        float64 `json:"total_fees"`
	SlippageUSDT     float64 `json:"slippage_usdt"`
	NetFunding       float64 `json:"net_funding"`
}
