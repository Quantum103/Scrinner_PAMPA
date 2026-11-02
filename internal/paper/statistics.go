package paper

import "math"

func calculateStatistics(trades, openPositions []Position, equity []EquityPoint, totalFees, netFunding float64) PaperStatistics {
	stats := PaperStatistics{
		Mode: "Paper Trading",
		TradeCount: len(trades),
		TotalFees: totalFees,
		NetFunding: netFunding,
		BySymbol: make(map[string]SymbolStats),
		ByCloseReason: make(map[string]int),
	}
	var grossProfit, grossLoss, profitSum, lossSum, rrSum float64
	var rrCount int
	for _, position := range openPositions {
		stats.TotalSlippageUSDT += position.EntrySlippageUSDT + position.ExitSlippageUSDT
	}
	for _, trade := range trades {
		pnl := trade.RealizedPnL
		stats.NetPnL += pnl
		stats.TotalSlippageUSDT += trade.EntrySlippageUSDT + trade.ExitSlippageUSDT
		stats.ByCloseReason[trade.CloseReason]++

		symbol := stats.BySymbol[trade.Symbol]
		symbol.TradeCount++
		symbol.NetPnL += pnl
		symbol.TotalFees += trade.EntryFee + trade.ExitFee
		symbol.SlippageUSDT += trade.EntrySlippageUSDT + trade.ExitSlippageUSDT
		symbol.NetFunding -= trade.FundingPaid
		if pnl > 0 {
			stats.ProfitableTrades++
			grossProfit += pnl
			profitSum += pnl
			symbol.ProfitableTrades++
		} else if pnl < 0 {
			stats.LosingTrades++
			grossLoss += -pnl
			lossSum += pnl
			symbol.LosingTrades++
		}
		if trade.InitialRiskUSDT > 0 {
			rr := pnl / trade.InitialRiskUSDT
			rrSum += rr
			rrCount++
			if rr > stats.MaxRealizedRR || rrCount == 1 {
				stats.MaxRealizedRR = rr
			}
		}
		stats.BySymbol[trade.Symbol] = symbol
	}

	if stats.TradeCount > 0 {
		stats.WinRatePercent = float64(stats.ProfitableTrades) / float64(stats.TradeCount) * 100
	}
	if stats.ProfitableTrades > 0 {
		stats.AverageProfit = profitSum / float64(stats.ProfitableTrades)
	}
	if stats.LosingTrades > 0 {
		stats.AverageLoss = lossSum / float64(stats.LosingTrades)
	}
	if grossLoss > 0 {
		factor := grossProfit / grossLoss
		stats.ProfitFactor = &factor
	} else if grossProfit > 0 {
		stats.InfiniteProfitFactor = true
	}
	if rrCount > 0 {
		stats.AverageRealizedRR = rrSum / float64(rrCount)
	}
	for symbol, item := range stats.BySymbol {
		if item.TradeCount > 0 {
			item.WinRatePercent = float64(item.ProfitableTrades) / float64(item.TradeCount) * 100
		}
		stats.BySymbol[symbol] = item
	}

	peak := 0.0
	for _, point := range equity {
		if !finite(point.Equity) {
			continue
		}
		if point.Equity > peak {
			peak = point.Equity
		}
		if peak <= 0 {
			continue
		}
		drawdown := math.Max(0, peak-point.Equity)
		if drawdown > stats.MaxDrawdownUSDT {
			stats.MaxDrawdownUSDT = drawdown
			stats.MaxDrawdownPercent = drawdown / peak * 100
		}
	}
	return stats
}
