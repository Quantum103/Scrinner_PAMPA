package scanner

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

// BenchmarkPersonalizedScan measures scanner CPU and allocation costs with
// increasing instrument/profile counts. Market data and persistence are
// deliberately in-memory: use the storage benchmarks for PostgreSQL latency.
func BenchmarkPersonalizedScan(b *testing.B) {
	cases := []struct {
		instruments int
		profiles    int
	}{
		{instruments: 100, profiles: 10},
		{instruments: 500, profiles: 20},
		{instruments: 1000, profiles: 30},
	}

	for _, benchmarkCase := range cases {
		name := fmt.Sprintf("instruments=%d/profiles=%d", benchmarkCase.instruments, benchmarkCase.profiles)
		b.Run(name, func(b *testing.B) {
			const candleCount = 64
			start := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
			serverTime := start.Add(candleCount * 5 * time.Minute)
			closes := make([]float64, candleCount)
			for index := range closes {
				closes[index] = 100 + 5*math.Sin(float64(index)*0.31) + float64(index)*0.03
			}

			symbols := make([]string, benchmarkCase.instruments)
			candlesByPair := make(map[string][]bybit.Candle, benchmarkCase.instruments)
			for index := range symbols {
				symbol := fmt.Sprintf("LOAD%05dUSDT", index+1)
				symbols[index] = symbol
				candlesByPair[pairKey(symbol, "5")] = makeCandles(symbol, "5", closes, start, serverTime)
			}

			workloads := make([]storage.ScannerWorkload, 0, benchmarkCase.profiles)
			for profileIndex := 0; profileIndex < benchmarkCase.profiles; profileIndex++ {
				workloads = append(workloads, storage.ScannerWorkload{
					RSILength: 2 + profileIndex,
					EMALength: 2,
					Timeframe: "5",
					Symbols:   symbols,
				})
			}

			source := &fakeSource{serverTime: serverTime, candles: candlesByPair}
			repository := newProfileFakeRepository(workloads)
			service, err := New(source, repository, Settings{
				Timeframes: []string{"5"}, RSILength: 2, EMALength: 2,
				InstrumentLimit: benchmarkCase.instruments, HistoryCandles: candleCount,
			})
			if err != nil {
				b.Fatalf("create scanner: %v", err)
			}

			// Establish durable-equivalent profile cursors outside the timed loop.
			if _, err := service.ScanOnce(context.Background()); err != nil {
				b.Fatalf("establish benchmark baselines: %v", err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(benchmarkCase.instruments * benchmarkCase.profiles * candleCount * 8))
			b.ResetTimer()

			for iteration := 0; iteration < b.N; iteration++ {
				report, err := service.ScanOnce(context.Background())
				if err != nil {
					b.Fatalf("scan benchmark workload: %v", err)
				}
				if report.PairsScanned != benchmarkCase.instruments {
					b.Fatalf("pairs scanned = %d, want %d", report.PairsScanned, benchmarkCase.instruments)
				}
			}
			b.ReportMetric(float64(benchmarkCase.instruments), "instruments")
			b.ReportMetric(float64(benchmarkCase.profiles), "profiles")
			b.ReportMetric(float64(benchmarkCase.instruments*benchmarkCase.profiles), "profile-pairs/op")
			b.ReportMetric(float64(benchmarkCase.instruments*benchmarkCase.profiles*candleCount), "indicator-points/op")
		})
	}
}

// BenchmarkProfileRoutingCache measures the cost of building the hot-path
// routing map from distinct workload rows, without market-data or SQL costs.
func BenchmarkProfileRoutingCache(b *testing.B) {
	const instrumentCount = 1000
	const profileCount = 30

	symbols := make([]string, instrumentCount)
	for index := range symbols {
		symbols[index] = fmt.Sprintf("LOAD%05dUSDT", index+1)
	}
	workloads := make([]storage.ScannerWorkload, 0, profileCount*3)
	for profileIndex := 0; profileIndex < profileCount; profileIndex++ {
		for _, timeframe := range []string{"5", "15", "60"} {
			workloads = append(workloads, storage.ScannerWorkload{
				RSILength: 2 + profileIndex, EMALength: 2,
				Timeframe: timeframe, Symbols: symbols,
			})
		}
	}
	service := &Scanner{profilePairs: make(map[string][]storage.ScannerProfile)}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := service.refreshProfileCacheLocked(workloads, true); err != nil {
			b.Fatalf("refresh profile cache: %v", err)
		}
	}
	b.ReportMetric(float64(instrumentCount), "instruments")
	b.ReportMetric(float64(profileCount), "profiles")
	b.ReportMetric(float64(instrumentCount*profileCount*3), "profile-pairs/op")
}
