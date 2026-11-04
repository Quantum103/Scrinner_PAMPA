package storage

import "time"

// TelegramUser is a Telegram account stored in the application database.
type TelegramUser struct {
	ID           int64
	TelegramID   int64
	Username     string
	FirstName    string
	LastName     string
	LanguageCode string
	IsActive     bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
	LastSeenAt   time.Time
}

// UserSettings contains scanner options that belong to one Telegram user.
type UserSettings struct {
	UserID             int64
	RSILength          int
	EMALength          int
	InstrumentLimit    int
	ScanAllInstruments bool
	NotifyLong         bool
	NotifyShort        bool
	NotifyConfluence   bool
	Timeframes         []string
}

// Instrument is an entry from the Bybit Linear USDT perpetual catalogue.
type Instrument struct {
	Symbol              string
	BaseCoin            string
	QuoteCoin           string
	SettleCoin          string
	ContractType        string
	Status              string
	IsActive            bool
	Turnover24h         string
	LastPrice           string
	InstrumentUpdatedAt time.Time
	TickerUpdatedAt     *time.Time
}

// InstrumentPreference is a user-specific explicit include or exclude rule.
type InstrumentPreference struct {
	Symbol     string
	Preference string
	UpdatedAt  time.Time
}

// Signal represents one confirmed RSI/EMA crossover on a closed candle.
type Signal struct {
	ID          int64
	Symbol      string
	Timeframe   string
	Direction   string
	CandleStart time.Time
	CandleClose time.Time
	ClosePrice  float64
	RSI         float64
	EMARSI      float64
	DetectedAt  time.Time
}

// ScannerState is the durable processing cursor and the last calculated values.
type ScannerState struct {
	Symbol                   string
	Timeframe                string
	LastProcessedCandleStart time.Time
	LastClose                float64
	LastRSI                  float64
	LastEMARSI               float64
	LastCrossDirection       string
	LastCrossAt              *time.Time
	Initialized              bool
	UpdatedAt                time.Time
}

// Delivery is one signal notification assigned to a Telegram user.
type Delivery struct {
	UserID       int64
	TelegramID   int64
	SignalID     int64
	Status       string
	AttemptCount int
	Symbol       string
	Timeframe    string
	Direction    string
	CandleStart  time.Time
	CandleClose  time.Time
	ClosePrice   float64
	RSI          float64
	EMARSI       float64
	DetectedAt   time.Time
}

// SignalFilters limits a user's signal history.
type SignalFilters struct {
	Symbol    string
	Direction string
	Timeframe string
	Since     *time.Time
	Limit     int
	Offset    int
}
