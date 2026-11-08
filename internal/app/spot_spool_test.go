package app

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

func TestSpotAlertSpoolPersistsAndAcknowledgesRecords(t *testing.T) {
	directory := t.TempDir()
	spool, err := newSpotAlertSpool(directory)
	if err != nil {
		t.Fatal(err)
	}
	first := storage.SpotMomentumAlert{
		Symbol: "ABCUSDT", DetectedAt: time.Date(2026, 11, 8, 12, 15, 1, 0, time.UTC),
		ReferenceAt: time.Date(2026, 11, 8, 12, 0, 1, 0, time.UTC),
		Price:       1.08, ReferencePrice: 1, ChangePercent: 8, ThresholdPercent: 5, WindowSeconds: 900,
	}
	second := first
	second.Symbol = "XYZUSDT"
	second.DetectedAt = second.DetectedAt.Add(time.Second)
	if err := spool.Append(first); err != nil {
		t.Fatalf("append first alert: %v", err)
	}
	if err := spool.Append(second); err != nil {
		t.Fatalf("append second alert: %v", err)
	}

	// Reopening simulates application restart while the first record is pending.
	spool, err = newSpotAlertSpool(directory)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := spool.Peek()
	if err != nil || !ok {
		t.Fatalf("peek first alert: ok=%t err=%v", ok, err)
	}
	if got.Symbol != first.Symbol || !got.DetectedAt.Equal(first.DetectedAt) {
		t.Fatalf("first spool record = %+v, want %+v", got, first)
	}
	if err := spool.Ack(first); err != nil {
		t.Fatalf("acknowledge first alert: %v", err)
	}
	got, ok, err = spool.Peek()
	if err != nil || !ok || got.Symbol != second.Symbol {
		t.Fatalf("second spool record = %+v ok=%t err=%v", got, ok, err)
	}
	if err := spool.Ack(second); err != nil {
		t.Fatalf("acknowledge second alert: %v", err)
	}
	if _, ok, err := spool.Peek(); err != nil || ok {
		t.Fatalf("spool should be empty after acknowledgements: ok=%t err=%v", ok, err)
	}
}

func TestSpotAlertSpoolRecoversIncompleteTail(t *testing.T) {
	directory := t.TempDir()
	spool, err := newSpotAlertSpool(directory)
	if err != nil {
		t.Fatal(err)
	}
	alert := storage.SpotMomentumAlert{
		Symbol: "ABCUSDT", DetectedAt: time.Date(2026, 11, 8, 12, 15, 1, 0, time.UTC),
		ReferenceAt: time.Date(2026, 11, 8, 12, 0, 1, 0, time.UTC),
		Price:       1.08, ReferencePrice: 1, ChangePercent: 8, WindowSeconds: 900,
	}
	if err := spool.Append(alert); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "spot-alerts.jsonl")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("{incomplete"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	spool, err = newSpotAlertSpool(directory)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := spool.Peek()
	if err != nil || !ok || got.Symbol != alert.Symbol {
		t.Fatalf("recovered record = %+v ok=%t err=%v", got, ok, err)
	}
	if err := spool.Ack(alert); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := spool.Peek(); err != nil || ok {
		t.Fatalf("incomplete tail was not discarded: ok=%t err=%v", ok, err)
	}
}

func TestSpotAlertSpoolAcknowledgesRulesIndependently(t *testing.T) {
	spool, err := newSpotAlertSpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	detected := time.Date(2026, 11, 8, 12, 15, 1, 0, time.UTC)
	reference := detected.Add(-5 * time.Minute)
	first := storage.SpotMomentumAlert{
		Symbol: "ABCUSDT", DetectedAt: detected, ReferenceAt: reference,
		Price: 1.04, ReferencePrice: 1, ChangePercent: 4,
		ThresholdPercent: 3, WindowSeconds: 300,
	}
	second := first
	second.ThresholdPercent = 1
	second.WindowSeconds = 60
	if err := spool.Append(first); err != nil {
		t.Fatal(err)
	}
	if err := spool.Append(second); err != nil {
		t.Fatal(err)
	}
	if err := spool.Ack(first); err != nil {
		t.Fatal(err)
	}
	got, ok, err := spool.Peek()
	if err != nil || !ok || got.ThresholdPercent != second.ThresholdPercent ||
		got.WindowSeconds != second.WindowSeconds {
		t.Fatalf("second rule-specific event was lost: %+v ok=%t err=%v", got, ok, err)
	}
	if err := spool.Ack(second); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := spool.Peek(); err != nil || ok {
		t.Fatalf("spool should be empty after both rule acknowledgements: ok=%t err=%v", ok, err)
	}
}
