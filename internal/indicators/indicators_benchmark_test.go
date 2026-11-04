package indicators

import (
	"fmt"
	"math"
	"testing"
)

func BenchmarkCalculate(b *testing.B) {
	for _, candleCount := range []int{64, 300, 1000} {
		b.Run(fmt.Sprintf("candles=%d", candleCount), func(b *testing.B) {
			closes := make([]float64, candleCount)
			for index := range closes {
				closes[index] = 100 + 7*math.Sin(float64(index)*0.17) + float64(index)*0.002
			}
			b.ReportAllocs()
			b.SetBytes(int64(candleCount * 8))
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err := Calculate(closes, 14, 9); err != nil {
					b.Fatalf("calculate RSI/EMA: %v", err)
				}
			}
		})
	}
}
