// Package paper реализует виртуальное исполнение LONG по публичному стакану Bybit.
// Пакет не содержит HTTP-клиента, WebSocket-клиента или методов отправки биржевых ордеров.
package paper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	"github.com/Quantum103/Scrinner_PAMPA/internal/strategy"
)

type Config struct {
	Enabled             bool
	InitialBalanceUSDT  float64
	TargetRiskPercent   float64
	MaxRiskPercent      float64
	MaxLeverage         float64
	MaxMarginPercent    float64
	MaxOpenPositions    int
	TrailingStopPercent float64
	FeeRate             float64
	SlippageBPS         float64
	MaxBookAge                       time.Duration
	OrderFlowExitEnabled              bool
	OrderFlowMinSignals                int
	OrderFlowConsecutiveIntervals      int
	OrderFlowObservationInterval       time.Duration
	OrderFlowMaxObservationGap         time.Duration
	OrderFlowMaxTakerBuySharePercent   float64
	OrderFlowTakerBuyShareDropPP       float64
	OrderFlowMinTakerSellSharePercent  float64
	OrderFlowCVDMinDeclinePercent      float64
	OrderFlowPriceStallMaxRisePercent  float64
}

func DefaultConfig() Config {
	return Config{
		Enabled:             true,
		InitialBalanceUSDT:  10,
		TargetRiskPercent:   2.5,
		MaxRiskPercent:      3,
		MaxLeverage:         2,
		MaxMarginPercent:    50,
		MaxOpenPositions:    1,
		TrailingStopPercent: 2.5,
		FeeRate:             0.00055, // Расчётное допущение, не ставка конкретного аккаунта Bybit.
		SlippageBPS:         5,
		MaxBookAge:                       5 * time.Second,
		OrderFlowExitEnabled:              true,
		OrderFlowMinSignals:                3,
		OrderFlowConsecutiveIntervals:      2,
		OrderFlowObservationInterval:       time.Second,
		OrderFlowMaxObservationGap:         3 * time.Second,
		OrderFlowMaxTakerBuySharePercent:   45,
		OrderFlowTakerBuyShareDropPP:       10,
		OrderFlowMinTakerSellSharePercent:  55,
		OrderFlowCVDMinDeclinePercent:      0.5,
		OrderFlowPriceStallMaxRisePercent:  0.02,
	}
}

func (c Config) Validate() error {
	if !finitePositive(c.InitialBalanceUSDT) {
		return errors.New("виртуальный начальный баланс должен быть положительным")
	}
	if !finitePositive(c.TargetRiskPercent) || c.TargetRiskPercent > 100 {
		return errors.New("целевой риск должен быть в диапазоне (0, 100]")
	}
	if !finitePositive(c.MaxRiskPercent) || c.MaxRiskPercent > 100 || c.MaxRiskPercent < c.TargetRiskPercent {
		return errors.New("предельный риск должен быть не меньше целевого и не больше 100%")
	}
	if !finitePositive(c.MaxLeverage) || c.MaxLeverage > 2 {
		return errors.New("максимальное плечо должно быть в диапазоне (0, 2]")
	}
	if !finitePositive(c.MaxMarginPercent) || c.MaxMarginPercent > 50 {
		return errors.New("доля маржи должна быть в диапазоне (0, 50]")
	}
	if c.MaxOpenPositions != 1 {
		return errors.New("paper trading поддерживает не более одной открытой позиции")
	}
	if !finitePositive(c.TrailingStopPercent) || c.TrailingStopPercent >= 100 {
		return errors.New("Trailing Stop должен быть в диапазоне (0, 100)")
	}
	if c.FeeRate < 0 || !finite(c.FeeRate) || c.FeeRate >= 1 {
		return errors.New("ставка комиссии должна быть в диапазоне [0, 1)")
	}
	if c.SlippageBPS < 0 || !finite(c.SlippageBPS) || c.SlippageBPS > 10000 {
		return errors.New("проскальзывание должно быть в диапазоне [0, 10000] bps")
	}
	if c.MaxBookAge <= 0 {
		return errors.New("максимальный возраст стакана должен быть положительным")
	}
	if c.OrderFlowMinSignals < 1 || c.OrderFlowMinSignals > 4 {
		return errors.New("для Order Flow Exit требуется от 1 до 4 групп сигналов")
	}
	if c.OrderFlowConsecutiveIntervals < 1 || c.OrderFlowConsecutiveIntervals > 20 {
		return errors.New("число последовательных интервалов Order Flow должно быть в диапазоне 1..20")
	}
	if c.OrderFlowObservationInterval <= 0 || c.OrderFlowMaxObservationGap < c.OrderFlowObservationInterval {
		return errors.New("интервалы наблюдения Order Flow должны быть положительными, а максимальный разрыв не меньше интервала")
	}
	if !finite(c.OrderFlowMaxTakerBuySharePercent) || c.OrderFlowMaxTakerBuySharePercent < 0 || c.OrderFlowMaxTakerBuySharePercent > 100 {
		return errors.New("максимальная доля Taker Buy должна быть в диапазоне 0..100%")
	}
	if !finite(c.OrderFlowTakerBuyShareDropPP) || c.OrderFlowTakerBuyShareDropPP < 0 || c.OrderFlowTakerBuyShareDropPP > 100 {
		return errors.New("снижение доли Taker Buy должно быть в диапазоне 0..100 процентных пунктов")
	}
	if !finite(c.OrderFlowMinTakerSellSharePercent) || c.OrderFlowMinTakerSellSharePercent < 50 || c.OrderFlowMinTakerSellSharePercent > 100 {
		return errors.New("минимальная доля Taker Sell должна быть в диапазоне 50..100%")
	}
	if !finite(c.OrderFlowCVDMinDeclinePercent) || c.OrderFlowCVDMinDeclinePercent < 0 || c.OrderFlowCVDMinDeclinePercent > 100 {
		return errors.New("минимальное снижение CVD должно быть в диапазоне 0..100%")
	}
	if !finite(c.OrderFlowPriceStallMaxRisePercent) || c.OrderFlowPriceStallMaxRisePercent < 0 || c.OrderFlowPriceStallMaxRisePercent > 5 {
		return errors.New("допуск остановки цены должен быть в диапазоне 0..5%")
	}
	return nil
}

type Level struct {
	Price    float64
	Quantity float64
}

type Book struct {
	Bids       []Level
	Asks       []Level
	ReceivedAt time.Time
	Valid      bool
	Fresh      bool
}

type Contract struct {
	Symbol           string
	MinOrderQty      float64
	QtyStep          float64
	MinNotionalValue float64
	TickSize         float64
	MaxMarketQty     float64
	FundingInterval  time.Duration
}

type Funding struct {
	Rate       float64
	MarkPrice  float64
	NextTime   time.Time
	Interval   time.Duration
	UpdatedAt  time.Time
	Available  bool
}

type Position struct {
	TradeID            string
	SignalID           string
	EntryReason        string
	EntrySignal        strategy.Signal
	Symbol             string
	Direction          string
	EntryPrice         float64
	ExitPrice          float64
	EntrySlippageUSDT  float64
	ExitSlippageUSDT   float64
	InitialRiskUSDT    float64
	MFEUSDT            float64
	MAEUSDT            float64
	MFEPercent         float64
	MAEPercent         float64
	LowWaterPrice      float64
	ExitNotional       float64
	RealizedRR         float64
	InitialQuantity    float64
	RemainingQuantity  float64
	ClosedQuantity     float64
	EntryNotional      float64
	InitialMargin      float64
	ReservedMargin     float64
	Leverage           float64
	EntryFee           float64
	ExitFee            float64
	FundingPaid        float64
	RealizedPnL        float64
	UnrealizedPnL      float64
	MarkPrice          float64
	FundingMarkPrice   float64
	FundingMarkUpdatedAt time.Time
	HighWaterPrice     float64
	StopPrice          float64
	StopTriggered      bool
	OpenedAt           time.Time
	UpdatedAt          time.Time
	ClosedAt           time.Time
	CloseReason        string
	LastFundingAt      time.Time
	NextFundingTime    time.Time
	FundingRate        float64
	FundingInterval    time.Duration
	Contract                     Contract
	PreviousTakerBuySharePercent float64
	PreviousCVD                  float64
	PreviousLastPrice            float64
	HasOrderFlowObservation      bool
	LastOrderFlowObservationAt   time.Time
	OrderFlowConfirmations       int
	LastOrderFlowSignalCount     int
	OrderFlowExitTriggered      bool
	ExitReason                   string
}

type Event struct {
	Type              string
	Symbol            string
	At                time.Time
	Reason            string
	Direction         string
	Price             float64
	Quantity          float64
	RequestedQuantity float64
	Notional          float64
	Margin            float64
	Leverage          float64
	Fee               float64
	Funding           float64
	RiskUSDT          float64
	RiskPercent       float64
	GrossPnL          float64
	NetPnL            float64
	RemainingQuantity float64
	SlippageUSDT      float64
	FlowSignalCount   int
	FlowConfirmations int
}

type AccountSnapshot struct {
	Mode             string
	Statistics       PaperStatistics
	EquityHistory    []EquityPoint
	Enabled          bool
	Currency         string
	Balance          float64
	Equity           float64
	FreeMargin       float64
	ReservedMargin   float64
	UnrealizedPnL    float64
	RealizedPnL      float64
	TotalFees        float64
	NetFunding       float64
	OpenPositions    []Position
	ClosedTrades     []Position
	UpdatedAt        time.Time
}

type Engine struct {
	mu            sync.RWMutex
	repository    Repository
	initialBalance float64
	equityHistory []EquityPoint
	processedSignals map[string]struct{}
	storageErr    error
	lastEquityRecordAt time.Time
	cfg           Config
	balance       float64
	realizedPnL   float64
	totalFees     float64
	netFunding    float64
	contracts     map[string]Contract
	funding       map[string]Funding
	positions     map[string]*Position
	closedTrades  []Position
	pendingEvents []Event
	updatedAt     time.Time
}

func New(cfg Config) (*Engine, error) {
	return NewWithRepository(cfg, nil)
}

// NewWithRepository восстанавливает единый Paper Trading account, открытые позиции,
// закрытые сделки и обработанные сигналы до начала приёма рыночных обновлений.
func NewWithRepository(cfg Config, repository Repository) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	e := &Engine{
		cfg: cfg, repository: repository,
		balance: cfg.InitialBalanceUSDT, initialBalance: cfg.InitialBalanceUSDT,
		contracts: make(map[string]Contract), funding: make(map[string]Funding),
		positions: make(map[string]*Position), processedSignals: make(map[string]struct{}),
	}
	if repository == nil {
		return e, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	state, err := repository.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("восстановить Paper Trading из SQLite: %w", err)
	}
	if !state.Found {
		now := time.Now().UTC()
		e.recordEquityLocked(now, true)
		if err := e.persistLocked(now); err != nil {
			return nil, fmt.Errorf("инициализировать Paper Trading account в SQLite: %w", err)
		}
		return e, nil
	}
	if !finitePositive(state.InitialBalanceUSDT) || !finite(state.Balance) {
		return nil, errors.New("SQLite содержит некорректный Paper Trading account")
	}
	if len(state.OpenPositions) > cfg.MaxOpenPositions {
		return nil, fmt.Errorf("SQLite содержит %d открытых позиций, конфигурация разрешает %d", len(state.OpenPositions), cfg.MaxOpenPositions)
	}
	e.initialBalance = state.InitialBalanceUSDT
	e.balance = state.Balance
	e.realizedPnL = state.RealizedPnL
	e.totalFees = state.TotalFees
	e.netFunding = state.NetFunding
	e.closedTrades = append([]Position(nil), state.ClosedTrades...)
	e.equityHistory = append([]EquityPoint(nil), state.EquityHistory...)
	if len(e.equityHistory) > 0 {
		e.lastEquityRecordAt = e.equityHistory[len(e.equityHistory)-1].At
	}
	for i := range state.OpenPositions {
		position := state.OpenPositions[i]
		if position.Symbol == "" || !finitePositive(position.RemainingQuantity) || position.TradeID == "" {
			return nil, errors.New("SQLite содержит некорректную открытую Paper Trading позицию")
		}
		copy := position
		e.positions[position.Symbol] = &copy
	}
	for _, id := range state.ProcessedSignalIDs {
		if id != "" {
			e.processedSignals[id] = struct{}{}
		}
	}
	e.updatedAt = state.UpdatedAt
	return e, nil
}

func (e *Engine) persistentStateLocked(now time.Time) PersistentState {
	positions := make([]Position, 0, len(e.positions))
	for _, position := range e.positions {
		positions = append(positions, *position)
	}
	closed := append([]Position(nil), e.closedTrades...)
	equity := append([]EquityPoint(nil), e.equityHistory...)
	signals := make([]string, 0, len(e.processedSignals))
	for id := range e.processedSignals {
		signals = append(signals, id)
	}
	sort.Strings(signals)
	return PersistentState{
		Found: true, InitialBalanceUSDT: e.initialBalance, Balance: e.balance,
		RealizedPnL: e.realizedPnL, TotalFees: e.totalFees, NetFunding: e.netFunding,
		OpenPositions: positions, ClosedTrades: closed, EquityHistory: equity,
		ProcessedSignalIDs: signals, UpdatedAt: now,
	}
}

func (e *Engine) persistLocked(now time.Time) error {
	if e.repository == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.repository.Save(ctx, e.persistentStateLocked(now)); err != nil {
		e.storageErr = err
		e.addEvent(Event{Type: "persistence_error", At: now, Reason: "не удалось атомарно сохранить Paper Trading в SQLite: " + err.Error()})
		return err
	}
	e.storageErr = nil
	return nil
}

func (e *Engine) recordEquityLocked(now time.Time, force bool) {
	if now.IsZero() {
		return
	}
	if !force && !e.lastEquityRecordAt.IsZero() && now.Sub(e.lastEquityRecordAt) < time.Second {
		return
	}
	point := EquityPoint{
		At: now.UTC(), Balance: e.balance, Equity: e.balance + e.unrealizedPnLLocked(),
		RealizedPnL: e.realizedPnL, UnrealizedPnL: e.unrealizedPnLLocked(),
	}
	if len(e.equityHistory) > 0 && !now.After(e.lastEquityRecordAt) {
		e.equityHistory[len(e.equityHistory)-1] = point
	} else {
		e.equityHistory = append(e.equityHistory, point)
	}
	e.lastEquityRecordAt = point.At
}

func signalID(signal strategy.Signal) string {
	return fmt.Sprintf("%s|%s|%d|%s", signal.Symbol, signal.Direction,
		signal.Time.UTC().UnixNano(), strconv.FormatFloat(signal.Price, 'g', -1, 64))
}

func tradeIDForSignal(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

// UpdateMarketData сохраняет только публичные спецификации контрактов и funding-данные REST-тикеров.
func (e *Engine) UpdateMarketData(items []bybit.MarketData, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, item := range items {
		symbol := item.Instrument.Symbol
		if symbol == "" {
			continue
		}
		if position := e.positions[symbol]; position != nil {
			e.applyDueFunding(position, now)
		}

		contract, err := contractFromInstrument(item.Instrument)
		if err != nil {
			e.addEvent(Event{Type: "market_data_rejected", Symbol: symbol, At: now, Reason: err.Error()})
			continue
		}
		e.contracts[symbol] = contract

		rate, rateErr := strconv.ParseFloat(item.Ticker.FundingRate, 64)
		markPrice, markErr := strconv.ParseFloat(item.Ticker.MarkPrice, 64)
		nextTime := item.Ticker.NextFundingTime
		if rateErr != nil || markErr != nil || !finite(rate) || !finitePositive(markPrice) || nextTime.IsZero() || contract.FundingInterval <= 0 {
			e.funding[symbol] = Funding{UpdatedAt: now}
			e.addEvent(Event{Type: "funding_data_unavailable", Symbol: symbol, At: now, Reason: "нет корректной ставки, следующего времени или интервала funding; новые позиции по инструменту заблокированы"})
			continue
		}
		funding := Funding{
			Rate: rate, MarkPrice: markPrice, NextTime: nextTime, Interval: contract.FundingInterval,
			UpdatedAt: now, Available: true,
		}
		e.funding[symbol] = funding
		if position := e.positions[symbol]; position != nil {
			position.FundingRate = rate
			position.FundingMarkPrice = markPrice
			position.FundingMarkUpdatedAt = now
			position.FundingInterval = funding.Interval
			if nextTime.After(now) {
				position.NextFundingTime = nextTime
			}
		}
	}
	e.updatedAt = now
	if len(e.positions) > 0 {
		e.recordEquityLocked(now, false)
		_ = e.persistLocked(now)
	}
}

func contractFromInstrument(item bybit.Instrument) (Contract, error) {
	parse := func(name, value string) (float64, error) {
		result, err := strconv.ParseFloat(value, 64)
		if err != nil || !finitePositive(result) {
			return 0, fmt.Errorf("контракт %s: отсутствует или некорректен %s=%q", item.Symbol, name, value)
		}
		return result, nil
	}
	minQty, err := parse("minOrderQty", item.MinOrderQty)
	if err != nil {
		return Contract{}, err
	}
	step, err := parse("qtyStep", item.QtyStep)
	if err != nil {
		return Contract{}, err
	}
	minNotional, err := parse("minNotionalValue", item.MinNotionalValue)
	if err != nil {
		return Contract{}, err
	}
	tick, err := parse("tickSize", item.TickSize)
	if err != nil {
		return Contract{}, err
	}
	maxQty, err := parse("maxMktOrderQty", item.MaxMktOrderQty)
	if err != nil {
		return Contract{}, err
	}
	if item.FundingInterval <= 0 {
		return Contract{}, fmt.Errorf("контракт %s: некорректный fundingInterval=%d", item.Symbol, item.FundingInterval)
	}
	if minQty > maxQty {
		return Contract{}, fmt.Errorf("контракт %s: minOrderQty больше maxMktOrderQty", item.Symbol)
	}
	return Contract{
		Symbol: item.Symbol, MinOrderQty: minQty, QtyStep: step,
		MinNotionalValue: minNotional, TickSize: tick, MaxMarketQty: maxQty,
		FundingInterval: time.Duration(item.FundingInterval) * time.Minute,
	}, nil
}

// OpenLong моделирует рыночную покупку проходом по Ask. Функция не отправляет запросы на биржу.
func (e *Engine) OpenLong(signal strategy.Signal, book Book, now time.Time) Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	reject := func(reason string) Event {
		event := Event{Type: "entry_rejected", Symbol: signal.Symbol, At: now, Direction: "LONG", Reason: reason}
		e.addEvent(event)
		return event
	}
	if !e.cfg.Enabled {
		return reject("виртуальная торговля отключена")
	}
	if signal.Direction != "LONG" {
		return reject("поддерживается только LONG")
	}
	if signal.Time.IsZero() || now.Before(signal.Time) || now.Sub(signal.Time) > e.cfg.MaxBookAge {
		return reject("сигнал устарел")
	}
	if e.storageErr != nil {
		return reject("вход заблокирован: последнее сохранение Paper Trading в SQLite завершилось ошибкой")
	}
	id := signalID(signal)
	if _, processed := e.processedSignals[id]; processed {
		return reject("сигнал уже был обработан; повторный вход по нему запрещён")
	}
	if _, exists := e.positions[signal.Symbol]; exists {
		return reject("по инструменту уже есть открытая позиция; усреднение запрещено")
	}
	if len(e.positions) >= e.cfg.MaxOpenPositions {
		return reject("достигнут лимит открытых позиций")
	}
	if !bookUsable(book, now, e.cfg.MaxBookAge) {
		return reject("стакан отсутствует, неполон или устарел")
	}
	contract, exists := e.contracts[signal.Symbol]
	if !exists {
		return reject("нет актуальных спецификаций контракта Bybit")
	}
	funding, exists := e.funding[signal.Symbol]
	if !exists || !funding.Available || funding.UpdatedAt.IsZero() ||
		now.Before(funding.UpdatedAt) || now.Sub(funding.UpdatedAt) > 2*e.cfg.MaxBookAge ||
		funding.NextTime.IsZero() || !funding.NextTime.After(now) || !finitePositive(funding.MarkPrice) || funding.Interval <= 0 {
		return reject("нет актуальных данных о funding; открытие позиции заблокировано")
	}

	bestAsk := book.Asks[0].Price
	equity := e.balance + e.unrealizedPnLLocked()
	if equity <= 0 {
		return reject("виртуальный капитал исчерпан")
	}
	freeMargin := equity - e.reservedMarginLocked()
	if freeMargin <= 0 {
		return reject("недостаточно свободной маржи")
	}

	slippage := e.cfg.SlippageBPS / 10000
	estimatedEntry := bestAsk * (1 + slippage)
	estimatedStop := estimatedEntry * (1 - e.cfg.TrailingStopPercent/100)
	estimatedStopFill := estimatedStop * (1 - slippage)
	entryFeePerUnit := estimatedEntry * e.cfg.FeeRate
	exitFeePerUnit := estimatedStopFill * e.cfg.FeeRate
	riskPerUnit := estimatedEntry - estimatedStopFill + entryFeePerUnit + exitFeePerUnit
	if !finitePositive(riskPerUnit) {
		return reject("не удалось оценить риск на единицу количества")
	}

	riskBudget := equity * e.cfg.TargetRiskPercent / 100
	qtyByRisk := riskBudget / riskPerUnit
	marginBudget := math.Min(equity*e.cfg.MaxMarginPercent/100, freeMargin)
	qtyByMargin := marginBudget * e.cfg.MaxLeverage / estimatedEntry
	requestedQty := math.Min(qtyByRisk, qtyByMargin)

	// Минимальный объём учитывает одновременно minOrderQty и minNotionalValue.
	minimumQty := math.Max(contract.MinOrderQty, contract.MinNotionalValue/estimatedEntry)
	minimumQty = ceilToStep(minimumQty, contract.QtyStep)
	if minimumQty > contract.MaxMarketQty {
		return reject("минимальный допустимый ордер превышает maxMktOrderQty")
	}
	if requestedQty < minimumQty {
		minimumRisk := minimumQty * riskPerUnit
		minimumRiskPercent := minimumRisk / equity * 100
		if minimumRiskPercent > e.cfg.MaxRiskPercent+1e-9 {
			return reject(fmt.Sprintf("минимальный ордер требует риск %.4f%% капитала, выше лимита %.4f%%", minimumRiskPercent, e.cfg.MaxRiskPercent))
		}
		if minimumQty > qtyByMargin {
			return reject("минимальный ордер требует маржу выше доступного лимита")
		}
		requestedQty = minimumQty
	}
	requestedQty = math.Min(requestedQty, contract.MaxMarketQty)
	requestedQty = floorToStep(requestedQty, contract.QtyStep)
	if !finitePositive(requestedQty) {
		return reject("расчётное количество после округления по qtyStep равно нулю")
	}

	fill := walkBook(book.Asks, requestedQty, true, e.cfg.SlippageBPS)
	if !finitePositive(fill.Quantity) || !finitePositive(fill.Price) {
		return reject("в Ask недостаточно ликвидности для исполнения")
	}
	filledQty := floorToStep(fill.Quantity, contract.QtyStep)
	if filledQty > requestedQty {
		filledQty = requestedQty
	}
	if filledQty < contract.MinOrderQty {
		return reject(fmt.Sprintf("доступное исполнение %.12g меньше minOrderQty %.12g", filledQty, contract.MinOrderQty))
	}
	fill = walkBook(book.Asks, filledQty, true, e.cfg.SlippageBPS)
	notional := fill.Price * fill.Quantity
	if notional < contract.MinNotionalValue {
		return reject(fmt.Sprintf("номинал %.8f USDT меньше minNotionalValue %.8f USDT", notional, contract.MinNotionalValue))
	}
	if fill.Quantity > contract.MaxMarketQty+contract.QtyStep*1e-9 {
		return reject("исполнение превышает maxMktOrderQty")
	}

	stopPrice := floorToStep(fill.Price*(1-e.cfg.TrailingStopPercent/100), contract.TickSize)
	stopFill := stopPrice * (1 - slippage)
	entryFee := notional * e.cfg.FeeRate
	expectedExitNotional := stopFill * fill.Quantity
	expectedExitFee := expectedExitNotional * e.cfg.FeeRate
	risk := math.Max(0, (fill.Price-stopFill)*fill.Quantity) + entryFee + expectedExitFee
	riskPercent := risk / equity * 100
	if riskPercent > e.cfg.MaxRiskPercent+1e-9 {
		return reject(fmt.Sprintf("минимальный фактически исполнимый объём создаёт риск %.4f%%, лимит %.4f%%", riskPercent, e.cfg.MaxRiskPercent))
	}

	margin := notional / e.cfg.MaxLeverage
	if margin > marginBudget+1e-8 || margin > equity*e.cfg.MaxMarginPercent/100+1e-8 {
		return reject("фактическая маржа после проскальзывания превышает доступный лимит")
	}
	if entryFee+margin > freeMargin+1e-8 {
		return reject("недостаточно свободной маржи для позиции и комиссии входа")
	}

	position := &Position{
		TradeID: tradeIDForSignal(id), SignalID: id, EntryReason: signal.Reason, EntrySignal: signal,
		Symbol: signal.Symbol, Direction: "LONG",
		EntryPrice: fill.Price, EntrySlippageUSDT: fill.SlippageUSDT, InitialRiskUSDT: risk,
		InitialQuantity: fill.Quantity, RemainingQuantity: fill.Quantity,
		EntryNotional: notional, InitialMargin: margin, ReservedMargin: margin,
		Leverage: e.cfg.MaxLeverage, EntryFee: entryFee, RealizedPnL: -entryFee,
		MarkPrice: book.Bids[0].Price, HighWaterPrice: fill.Price, LowWaterPrice: fill.Price, StopPrice: stopPrice,
		OpenedAt: now, UpdatedAt: now, NextFundingTime: funding.NextTime,
		FundingRate: funding.Rate, FundingMarkPrice: funding.MarkPrice, FundingMarkUpdatedAt: funding.UpdatedAt,
		FundingInterval: funding.Interval, Contract: contract,
	}
	e.balance -= entryFee
	e.realizedPnL -= entryFee
	e.totalFees += entryFee
	e.positions[signal.Symbol] = position
	e.processedSignals[id] = struct{}{}
	e.updatedAt = now

	eventType := "position_opened"
	reason := "виртуальная LONG-позиция исполнена по Ask с учётом глубины стакана, VWAP и заданного проскальзывания"
	if fill.Quantity+contract.QtyStep*1e-9 < requestedQty {
		eventType = "position_partially_opened"
		reason = "виртуальная LONG-позиция исполнена частично: доступного объёма Ask не хватило на весь расчётный размер"
	}
	event := Event{
		Type: eventType, Symbol: signal.Symbol, At: now, Direction: "LONG", Reason: reason,
		Price: fill.Price, Quantity: fill.Quantity, RequestedQuantity: requestedQty,
		Notional: notional, Margin: margin, Leverage: e.cfg.MaxLeverage,
		Fee: entryFee, RiskUSDT: risk, RiskPercent: riskPercent,
		RemainingQuantity: position.RemainingQuantity,
	}
	e.addEvent(event)
	e.recordEquityLocked(now, true)
	_ = e.persistLocked(now)
	return event
}

// UpdateBook переоценивает позицию по лучшему Bid, двигает trailing stop только вверх
// и пытается виртуально закрыть позицию при срабатывании стопа.
func (e *Engine) UpdateBook(symbol string, book Book, now time.Time) []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	position := e.positions[symbol]
	if position == nil {
		return nil
	}
	if !bookUsable(book, now, e.cfg.MaxBookAge) {
		return nil
	}

	bestBid := book.Bids[0].Price
	position.MarkPrice = bestBid
	e.applyDueFunding(position, now)
	if bestBid > position.HighWaterPrice {
		position.HighWaterPrice = bestBid
	}
	if position.LowWaterPrice <= 0 || bestBid < position.LowWaterPrice {
		position.LowWaterPrice = bestBid
	}
	position.MFEUSDT = math.Max(0, position.HighWaterPrice-position.EntryPrice) * position.InitialQuantity
	position.MAEUSDT = math.Max(0, position.EntryPrice-position.LowWaterPrice) * position.InitialQuantity
	position.MFEPercent = math.Max(0, (position.HighWaterPrice-position.EntryPrice)/position.EntryPrice*100)
	position.MAEPercent = math.Max(0, (position.EntryPrice-position.LowWaterPrice)/position.EntryPrice*100)
	position.UnrealizedPnL = (bestBid - position.EntryPrice) * position.RemainingQuantity
	position.UpdatedAt = now

	if position.ExitReason == "" && !position.StopTriggered {
		if bestBid > position.HighWaterPrice {
			position.HighWaterPrice = bestBid
			candidate := floorToStep(bestBid*(1-e.cfg.TrailingStopPercent/100), position.Contract.TickSize)
			if candidate > position.StopPrice {
				position.StopPrice = candidate
			}
		}
		if bestBid <= position.StopPrice {
			position.StopTriggered = true
			position.ExitReason = "сработал виртуальный Trailing Stop"
		}
	}
	// После частичного исполнения любой причины выхода продолжаем закрывать остаток
	// по последующим актуальным Bid, не теряя первоначальную причину.
	closed := position.ExitReason != ""
	if closed {
		e.closeLocked(position, book, now, position.ExitReason)
	}
	e.updatedAt = now
	e.recordEquityLocked(now, closed)
	_ = e.persistLocked(now)
	return nil
}

// CloseLong позволяет закрыть позицию вручную в симуляции, используя доступные Bid.
func (e *Engine) CloseLong(symbol string, book Book, now time.Time, reason string) Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	position := e.positions[symbol]
	if position == nil {
		event := Event{Type: "close_rejected", Symbol: symbol, At: now, Reason: "нет открытой позиции"}
		e.addEvent(event)
		return event
	}
	e.applyDueFunding(position, now)
	if !bookUsable(book, now, e.cfg.MaxBookAge) {
		event := Event{Type: "close_rejected", Symbol: symbol, At: now, Reason: "невозможно закрыть позицию: стакан отсутствует, неполон или устарел"}
		e.addEvent(event)
		return event
	}
	if reason == "" {
		reason = "ручное виртуальное закрытие"
	}
	position.ExitReason = reason
	event := e.closeLocked(position, book, now, reason)
	e.updatedAt = now
	e.recordEquityLocked(now, true)
	_ = e.persistLocked(now)
	return event
}

func (e *Engine) closeLocked(position *Position, book Book, now time.Time, reason string) Event {
	fill := walkBook(book.Bids, position.RemainingQuantity, false, e.cfg.SlippageBPS)
	if !finitePositive(fill.Quantity) || !finitePositive(fill.Price) {
		event := Event{Type: "close_pending", Symbol: position.Symbol, At: now, Reason: "в Bid нет доступного объёма для виртуального закрытия"}
		e.addEvent(event)
		return event
	}
	closedQty := math.Min(fill.Quantity, position.RemainingQuantity)
	exitNotional := closedQty * fill.Price
	slippageRatio := closedQty / fill.Quantity
	position.ExitNotional += exitNotional
	position.ExitPrice = position.ExitNotional / (position.ClosedQuantity + closedQty)
	position.ExitSlippageUSDT += fill.SlippageUSDT * slippageRatio
	exitFee := exitNotional * e.cfg.FeeRate
	grossPnL := (fill.Price - position.EntryPrice) * closedQty
	netIncrement := grossPnL - exitFee

	e.balance += netIncrement
	e.realizedPnL += netIncrement
	e.totalFees += exitFee
	position.ExitFee += exitFee
	position.RealizedPnL += netIncrement
	position.RemainingQuantity -= closedQty
	position.ClosedQuantity += closedQty
	position.ReservedMargin = position.InitialMargin * position.RemainingQuantity / position.InitialQuantity
	position.UnrealizedPnL = (position.MarkPrice - position.EntryPrice) * position.RemainingQuantity
	position.UpdatedAt = now

	eventType := "position_partially_closed"
	if position.RemainingQuantity <= position.Contract.QtyStep*1e-8 || position.RemainingQuantity <= 1e-12 {
		position.RemainingQuantity = 0
		position.ReservedMargin = 0
		position.UnrealizedPnL = 0
		position.ClosedAt = now
		position.CloseReason = reason
		position.ExitReason = reason
		if position.InitialRiskUSDT > 0 {
			position.RealizedRR = position.RealizedPnL / position.InitialRiskUSDT
		}
		eventType = "position_closed"
		e.closedTrades = append(e.closedTrades, *position)
		delete(e.positions, position.Symbol)
	}
	eventNetPnL := netIncrement
	if eventType == "position_closed" {
		eventNetPnL = position.RealizedPnL
	}
	event := Event{
		Type: eventType, Symbol: position.Symbol, At: now, Direction: "LONG", Reason: reason,
		Price: fill.Price, Quantity: closedQty, Notional: exitNotional,
		Fee: exitFee, SlippageUSDT: fill.SlippageUSDT * slippageRatio,
		GrossPnL: grossPnL, NetPnL: eventNetPnL,
		RemainingQuantity: position.RemainingQuantity,
	}
	if eventType == "position_partially_closed" {
		event.Reason += "; часть количества остаётся открытой, выход будет продолжен по следующим актуальным Bid"
	}
	e.addEvent(event)
	return event
}

func (e *Engine) applyDueFunding(position *Position, now time.Time) {
	if position.NextFundingTime.IsZero() || position.FundingInterval <= 0 || now.Before(position.NextFundingTime) {
		return
	}
	for count := 0; !now.Before(position.NextFundingTime) && count < 100; count++ {
		mark := position.FundingMarkPrice
		if !finitePositive(mark) || position.FundingMarkUpdatedAt.IsZero() || now.Before(position.FundingMarkUpdatedAt) || now.Sub(position.FundingMarkUpdatedAt) > 2*e.cfg.MaxBookAge {
			e.addEvent(Event{Type: "funding_skipped", Symbol: position.Symbol, At: position.NextFundingTime, Reason: "пропущен расчёт funding: Mark Price отсутствует или устарел"})
			position.LastFundingAt = position.NextFundingTime
			position.NextFundingTime = position.NextFundingTime.Add(position.FundingInterval)
			continue
		}
		amount := mark * position.RemainingQuantity * position.FundingRate
		cashFlow := -amount // Положительная ставка означает выплату LONG-позицией.
		e.balance += cashFlow
		e.realizedPnL += cashFlow
		e.netFunding += cashFlow
		position.FundingPaid += amount
		position.RealizedPnL += cashFlow
		position.LastFundingAt = position.NextFundingTime
		position.NextFundingTime = position.NextFundingTime.Add(position.FundingInterval)
		e.addEvent(Event{
			Type: "funding_applied", Symbol: position.Symbol, At: position.LastFundingAt,
			Price: mark, Quantity: position.RemainingQuantity, Funding: cashFlow,
			Reason: fmt.Sprintf("виртуальный funding по ставке %.8f; положительная ставка оплачивается LONG", position.FundingRate),
		})
	}
	if !now.Before(position.NextFundingTime) {
		e.addEvent(Event{Type: "funding_backlog", Symbol: position.Symbol, At: now, Reason: "funding backlog превышает 100 интервалов; требуется проверить расписание"})
	}
}

func (e *Engine) Snapshot(now time.Time) AccountSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	unrealized := e.unrealizedPnLLocked()
	reserved := e.reservedMarginLocked()
	equity := e.balance + unrealized
	positions := make([]Position, 0, len(e.positions))
	for _, position := range e.positions {
		copy := *position
		positions = append(positions, copy)
	}
	closedTrades := append([]Position(nil), e.closedTrades...)
	return AccountSnapshot{
		Mode: "Paper Trading", Statistics: calculateStatistics(e.closedTrades, e.equityHistory, e.totalFees, e.netFunding),
		EquityHistory: append([]EquityPoint(nil), e.equityHistory...),
		Enabled: e.cfg.Enabled, Currency: "USDT",
		Balance: e.balance, Equity: equity, FreeMargin: equity - reserved,
		ReservedMargin: reserved, UnrealizedPnL: unrealized,
		RealizedPnL: e.realizedPnL, TotalFees: e.totalFees, NetFunding: e.netFunding,
		OpenPositions: positions, ClosedTrades: closedTrades, UpdatedAt: now,
	}
}

func (e *Engine) DrainEvents() []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := append([]Event(nil), e.pendingEvents...)
	e.pendingEvents = nil
	return result
}

func (e *Engine) unrealizedPnLLocked() float64 {
	var result float64
	for _, position := range e.positions {
		result += (position.MarkPrice - position.EntryPrice) * position.RemainingQuantity
	}
	return result
}

func (e *Engine) reservedMarginLocked() float64 {
	var result float64
	for _, position := range e.positions {
		result += position.ReservedMargin
	}
	return result
}

func (e *Engine) addEvent(event Event) {
	e.pendingEvents = append(e.pendingEvents, event)
	if len(e.pendingEvents) > 1000 {
		e.pendingEvents = append([]Event(nil), e.pendingEvents[len(e.pendingEvents)-1000:]...)
	}
}

type fillResult struct {
	Quantity      float64
	Price         float64
	BookVWAP      float64
	SlippageUSDT  float64
}

func walkBook(levels []Level, requested float64, buy bool, slippageBPS float64) fillResult {
	if !finitePositive(requested) || len(levels) == 0 {
		return fillResult{}
	}
	remaining := requested
	quote := 0.0
	filled := 0.0
	for _, level := range levels {
		if !finitePositive(level.Price) || !finitePositive(level.Quantity) {
			continue
		}
		take := math.Min(remaining, level.Quantity)
		filled += take
		quote += take * level.Price
		remaining -= take
		if remaining <= requested*1e-12 {
			break
		}
	}
	if filled <= 0 {
		return fillResult{}
	}
	vwap := quote / filled
	slippage := slippageBPS / 10000
	if buy {
		vwap *= 1 + slippage
	} else {
		vwap *= 1 - slippage
	}
	return fillResult{
		Quantity: filled, Price: vwap, BookVWAP: quote / filled,
		SlippageUSDT: math.Abs(vwap-(quote/filled))*filled,
	}
}

func bookUsable(book Book, now time.Time, maxAge time.Duration) bool {
	if !book.Valid || !book.Fresh || book.ReceivedAt.IsZero() ||
		now.Before(book.ReceivedAt) || now.Sub(book.ReceivedAt) > maxAge ||
		len(book.Bids) == 0 || len(book.Asks) == 0 {
		return false
	}
	return finitePositive(book.Bids[0].Price) && finitePositive(book.Bids[0].Quantity) &&
		finitePositive(book.Asks[0].Price) && finitePositive(book.Asks[0].Quantity) &&
		book.Asks[0].Price > book.Bids[0].Price
}

func ceilToStep(value, step float64) float64 {
	if !finitePositive(value) || !finitePositive(step) {
		return 0
	}
	valueRat, ok := new(big.Rat).SetString(strconv.FormatFloat(value, 'f', -1, 64))
	if !ok {
		return 0
	}
	stepRat, ok := new(big.Rat).SetString(strconv.FormatFloat(step, 'f', -1, 64))
	if !ok {
		return 0
	}
	ratio := new(big.Rat).Quo(valueRat, stepRat)
	units, remainder := new(big.Int).QuoRem(ratio.Num(), ratio.Denom(), new(big.Int))
	if remainder.Sign() > 0 {
		units.Add(units, big.NewInt(1))
	}
	resultRat := new(big.Rat).Mul(new(big.Rat).SetInt(units), stepRat)
	result, _ := resultRat.Float64()
	return result
}

func floorToStep(value, step float64) float64 {
	if !finitePositive(value) || !finitePositive(step) {
		return 0
	}
	valueRat, ok := new(big.Rat).SetString(strconv.FormatFloat(value, 'f', -1, 64))
	if !ok {
		return 0
	}
	stepRat, ok := new(big.Rat).SetString(strconv.FormatFloat(step, 'f', -1, 64))
	if !ok {
		return 0
	}
	ratio := new(big.Rat).Quo(valueRat, stepRat)
	units := new(big.Int).Quo(ratio.Num(), ratio.Denom())
	resultRat := new(big.Rat).Mul(new(big.Rat).SetInt(units), stepRat)
	result, _ := resultRat.Float64()
	return result
}

func finitePositive(value float64) bool {
	return value > 0 && finite(value)
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
