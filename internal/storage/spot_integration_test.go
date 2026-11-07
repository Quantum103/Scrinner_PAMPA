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
	highSymbol := "HI" + uniqueSuffix() + "USDT"
	lowSymbol := "LO" + uniqueSuffix() + "USDT"
	makeSnapshot := func(at time.Time, highTurnover string) []SpotInstrument {
		return []SpotInstrument{
			{Symbol: highSymbol, BaseCoin: highSymbol[:len(highSymbol)-4], QuoteCoin: "USDT", Status: "Trading", Turnover24h: highTurnover, LastPrice: "0.00125", TickerUpdatedAt: at},
			{Symbol: lowSymbol, BaseCoin: lowSymbol[:len(lowSymbol)-4], QuoteCoin: "USDT", Status: "Trading", Turnover24h: "1000000", LastPrice: "0.25", TickerUpdatedAt: at},
		}
	}

	if err := repository.SyncSpotCatalog(ctx, makeSnapshot(now, "1500000"), 1_000_000, 24*time.Hour, now); err != nil {
		t.Fatalf("initial Spot sync: %v", err)
	}
	watchlist, err := repository.ListSpotWatchlist(ctx)
	if err != nil {
		t.Fatalf("list initial Watchlist: %v", err)
	}
	if len(watchlist) != 2 {
		t.Fatalf("initial Watchlist length = %d, want 2: %+v", len(watchlist), watchlist)
	}
	var initialHold *SpotWatchlistEntry
	for i := range watchlist {
		if watchlist[i].Symbol == highSymbol {
			initialHold = &watchlist[i]
		}
	}
	if initialHold == nil || initialHold.State != SpotWatchlistOverLimitHold || initialHold.OverLimitSince == nil || !initialHold.OverLimitSince.Equal(now) {
		t.Fatalf("high-turnover symbol was not retained with a timestamp: %+v", initialHold)
	}

	beforeDeadline := now.Add(24*time.Hour - time.Second)
	if err := repository.SyncSpotCatalog(ctx, makeSnapshot(beforeDeadline, "1750000"), 1_000_000, 24*time.Hour, beforeDeadline); err != nil {
		t.Fatalf("Spot sync before hold deadline: %v", err)
	}
	watchlist, err = repository.ListSpotWatchlist(ctx)
	if err != nil {
		t.Fatalf("list Watchlist before deadline: %v", err)
	}
	if len(watchlist) != 2 {
		t.Fatalf("high-turnover symbol was removed before 24 hours: %+v", watchlist)
	}

	atDeadline := now.Add(24 * time.Hour)
	if err := repository.SyncSpotCatalog(ctx, makeSnapshot(atDeadline, "1750000"), 1_000_000, 24*time.Hour, atDeadline); err != nil {
		t.Fatalf("Spot sync at hold deadline: %v", err)
	}
	watchlist, err = repository.ListSpotWatchlist(ctx)
	if err != nil {
		t.Fatalf("list Watchlist after deadline: %v", err)
	}
	if len(watchlist) != 1 || watchlist[0].Symbol != lowSymbol {
		t.Fatalf("high-turnover symbol should be excluded after 24 hours: %+v", watchlist)
	}

	var state string
	var excludedAt time.Time
	if err := pool.QueryRow(ctx, "SELECT state, excluded_at FROM spot_watchlist WHERE symbol = $1", highSymbol).Scan(&state, &excludedAt); err != nil {
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
		t.Fatalf("symbol at the turnover limit should re-enter: %+v", watchlist)
	}
	var reentered *SpotWatchlistEntry
	for i := range watchlist {
		if watchlist[i].Symbol == highSymbol {
			reentered = &watchlist[i]
		}
	}
	if reentered == nil || reentered.State != SpotWatchlistWatching || reentered.OverLimitSince != nil {
		t.Fatalf("re-entered symbol retained stale hold state: %+v", reentered)
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
