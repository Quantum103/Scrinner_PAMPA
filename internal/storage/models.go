package storage

import "time"

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

type SpotMomentumAlert struct {
	ID             int64
	Symbol         string
	DetectedAt     time.Time
	ReferenceAt    time.Time
	Price          float64
	ReferencePrice float64
	ChangePercent  float64
	WindowSeconds  int
}

type SpotAlertDelivery struct {
	UserID         int64
	TelegramID     int64
	AlertID        int64
	AttemptCount   int
	Symbol         string
	DetectedAt     time.Time
	ReferenceAt    time.Time
	Price          float64
	ReferencePrice float64
	ChangePercent  float64
	WindowSeconds  int
}

type SpotAlertHistoryItem struct {
	ID             int64
	Symbol         string
	DetectedAt     time.Time
	ReferenceAt    time.Time
	Price          float64
	ReferencePrice float64
	ChangePercent  float64
	WindowSeconds  int
}
