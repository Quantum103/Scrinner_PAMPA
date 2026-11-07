package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSyncSpotCatalogApplies24HourOverLimitHold(t *testing.T) {
	repository, pool := testRepository(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "TRUNCATE TABLE spot_instruments CASCADE"); err != nil {
		t.Fatalf("reset Spot market tables: %v", err)
	}

	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	transitionSymbol := "TR" + uniqueSuffix() + "USDT"
	lowSymbol := "LO" + uniqueSuffix() + "USDT"
	initialHighSymbol := "HI" + uniqueSuffix() + "USDT"
	makeSnapshot := func(at time.Time, transitionTurnover string) []SpotInstrument {
		return []SpotInstrument{
			{Symbol: transitionSymbol, BaseCoin: transitionSymbol[:len(transitionSymbol)-4], QuoteCoin: "USDT", Status: "Trading", Turnover24h: transitionTurnover, LastPrice: "0.00125", TickerUpdatedAt: at},
			{Symbol: lowSymbol, BaseCoin: lowSymbol[:len(lowSymbol)-4], QuoteCoin: "USDT", Status: "Trading", Turnover24h: "1000000", LastPrice: "0.25", TickerUpdatedAt: at},
			{Symbol: initialHighSymbol, BaseCoin: initialHighSymbol[:len(initialHighSymbol)-4], QuoteCoin: "USDT", Status: "Trading", Turnover24h: "1500000", LastPrice: "2.5", TickerUpdatedAt: at},
		}
	}

	// Pairs that begin above the liquidity ceiling are excluded immediately:
	// major coins should not occupy the low-liquidity Watchlist on first launch.
	if err := repository.SyncSpotCatalog(ctx, makeSnapshot(now, "750000"), 1_000_000, 24*time.Hour, now); err != nil {
		t.Fatalf("initial Spot sync: %v", err)
	}
	watchlist, err := repository.ListSpotWatchlist(ctx)
	if err != nil {
		t.Fatalf("list initial Watchlist: %v", err)
	}
	if len(watchlist) != 2 {
		t.Fatalf("initial Watchlist should contain only pairs at or below $1m: %+v", watchlist)
	}
	var initialHighState string
	if err := pool.QueryRow(ctx, "SELECT state FROM spot_watchlist WHERE symbol = $1", initialHighSymbol).Scan(&initialHighState); err != nil {
		t.Fatalf("read initially high-turnover state: %v", err)
	}
	if initialHighState != SpotWatchlistExcluded {
		t.Fatalf("new pair already above $1m state = %q, want excluded", initialHighState)
	}

	// A pair already being watched gets a full 24-hour grace period after it
	// crosses the ceiling, even if it remains above the ceiling on refreshes.
	crossedAt := now.Add(time.Minute)
	if err := repository.SyncSpotCatalog(ctx, makeSnapshot(crossedAt, "1500000"), 1_000_000, 24*time.Hour, crossedAt); err != nil {
		t.Fatalf("Spot sync at liquidity crossing: %v", err)
	}
	watchlist, err = repository.ListSpotWatchlist(ctx)
	if err != nil {
		t.Fatalf("list Watchlist after crossing: %v", err)
	}
	if len(watchlist) != 2 {
		t.Fatalf("pair should remain in Watchlist during grace period: %+v", watchlist)
	}
	var initialHold *SpotWatchlistEntry
	for i := range watchlist {
		if watchlist[i].Symbol == transitionSymbol {
			initialHold = &watchlist[i]
		}
	}
	if initialHold == nil || initialHold.State != SpotWatchlistOverLimitHold ||
		initialHold.OverLimitSince == nil || !initialHold.OverLimitSince.Equal(crossedAt) {
		t.Fatalf("crossed pair was not retained with crossing timestamp: %+v", initialHold)
	}

	beforeDeadline := crossedAt.Add(24*time.Hour - time.Second)
	if err := repository.SyncSpotCatalog(ctx, makeSnapshot(beforeDeadline, "1750000"), 1_000_000, 24*time.Hour, beforeDeadline); err != nil {
		t.Fatalf("Spot sync before hold deadline: %v", err)
	}
	watchlist, err = repository.ListSpotWatchlist(ctx)
	if err != nil {
		t.Fatalf("list Watchlist before deadline: %v", err)
	}
	if len(watchlist) != 2 {
		t.Fatalf("pair was removed before the 24-hour deadline: %+v", watchlist)
	}

	atDeadline := crossedAt.Add(24 * time.Hour)
	if err := repository.SyncSpotCatalog(ctx, makeSnapshot(atDeadline, "1750000"), 1_000_000, 24*time.Hour, atDeadline); err != nil {
		t.Fatalf("Spot sync at hold deadline: %v", err)
	}
	watchlist, err = repository.ListSpotWatchlist(ctx)
	if err != nil {
		t.Fatalf("list Watchlist after deadline: %v", err)
	}
	if len(watchlist) != 1 || watchlist[0].Symbol != lowSymbol {
		t.Fatalf("crossed pair should be excluded after 24 hours: %+v", watchlist)
	}

	var state string
	var excludedAt time.Time
	if err := pool.QueryRow(ctx, "SELECT state, excluded_at FROM spot_watchlist WHERE symbol = $1", transitionSymbol).Scan(&state, &excludedAt); err != nil {
		t.Fatalf("read excluded Watchlist row: %v", err)
	}
	if state != SpotWatchlistExcluded || !excludedAt.Equal(atDeadline) {
		t.Fatalf("excluded state=%q excluded_at=%s", state, excludedAt)
	}

	// Exactly $1,000,000 is eligible again; the old grace timestamp is cleared.
	reenteredAt := atDeadline.Add(time.Minute)
	if err := repository.SyncSpotCatalog(ctx, makeSnapshot(reenteredAt, "1000000"), 1_000_000, 24*time.Hour, reenteredAt); err != nil {
		t.Fatalf("Spot sync after turnover falls: %v", err)
	}
	watchlist, err = repository.ListSpotWatchlist(ctx)
	if err != nil {
		t.Fatalf("list Watchlist after re-entry: %v", err)
	}
	if len(watchlist) != 2 {
		t.Fatalf("pair at the turnover limit should re-enter: %+v", watchlist)
	}
	var reentered *SpotWatchlistEntry
	for i := range watchlist {
		if watchlist[i].Symbol == transitionSymbol {
			reentered = &watchlist[i]
		}
	}
	if reentered == nil || reentered.State != SpotWatchlistWatching || reentered.OverLimitSince != nil {
		t.Fatalf("re-entered pair retained stale hold state: %+v", reentered)
	}
}

func TestSyncSpotCatalogRejectsInvalidSnapshot(t *testing.T) {
	repository, _ := testRepository(t)
	now := time.Now().UTC()
	tests := []struct {
		name string
		item SpotInstrument
	}{
		{name: "invalid symbol", item: SpotInstrument{Symbol: "BAD_SYMBOL", BaseCoin: "BAD", QuoteCoin: "USDT", Status: "Trading", Turnover24h: "10", LastPrice: "1", TickerUpdatedAt: now}},
		{name: "wrong quote", item: SpotInstrument{Symbol: "ABCUSDC", BaseCoin: "ABC", QuoteCoin: "USDC", Status: "Trading", Turnover24h: "10", LastPrice: "1", TickerUpdatedAt: now}},
		{name: "zero price", item: SpotInstrument{Symbol: "ABCUSDT", BaseCoin: "ABC", QuoteCoin: "USDT", Status: "Trading", Turnover24h: "10", LastPrice: "0", TickerUpdatedAt: now}},
		{name: "negative turnover", item: SpotInstrument{Symbol: "ABCUSDT", BaseCoin: "ABC", QuoteCoin: "USDT", Status: "Trading", Turnover24h: "-1", LastPrice: "1", TickerUpdatedAt: now}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := repository.SyncSpotCatalog(context.Background(), []SpotInstrument{test.item}, 1_000_000, 24*time.Hour, now); err == nil {
				t.Fatal(fmt.Sprintf("expected invalid Spot snapshot to fail: %+v", test.item))
			}
		})
	}
}
