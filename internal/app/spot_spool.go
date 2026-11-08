package app

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

// spotAlertSpool persists detected events before the WebSocket callback returns.
// The single-process application serializes append, peek and acknowledge operations.
type spotAlertSpool struct {
	mu   sync.Mutex
	path string
}

func newSpotAlertSpool(directory string) (*spotAlertSpool, error) {
	if directory == "" {
		return nil, errors.New("Spot alert data directory is required")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create Spot alert data directory: %w", err)
	}
	spool := &spotAlertSpool{path: filepath.Join(directory, "spot-alerts.jsonl")}
	if err := spool.recoverIncompleteTail(); err != nil {
		return nil, err
	}
	return spool, nil
}

func (s *spotAlertSpool) recoverIncompleteTail() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open Spot alert spool: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("read Spot alert spool: %w", err)
	}
	if len(data) == 0 || data[len(data)-1] == '\n' {
		return nil
	}
	lastComplete := bytes.LastIndexByte(data, '\n') + 1
	if err := file.Truncate(int64(lastComplete)); err != nil {
		return fmt.Errorf("truncate incomplete Spot alert spool tail: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek Spot alert spool after recovery: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync recovered Spot alert spool: %w", err)
	}
	return nil
}

// Append writes one complete record and fsyncs it before returning.
func (s *spotAlertSpool) Append(alert storage.SpotMomentumAlert) error {
	payload, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("encode Spot alert spool record: %w", err)
	}
	payload = append(payload, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open Spot alert spool for append: %w", err)
	}
	written, writeErr := file.Write(payload)
	if writeErr == nil && written != len(payload) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("append Spot alert spool record: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close Spot alert spool after append: %w", closeErr)
	}
	return nil
}

// Peek returns the oldest pending record without removing it.
func (s *spotAlertSpool) Peek() (storage.SpotMomentumAlert, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return storage.SpotMomentumAlert{}, false, nil
	}
	if err != nil {
		return storage.SpotMomentumAlert{}, false, fmt.Errorf("open Spot alert spool for reading: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	line, err := reader.ReadBytes('\n')
	if errors.Is(err, io.EOF) && len(line) == 0 {
		return storage.SpotMomentumAlert{}, false, nil
	}
	if err != nil {
		return storage.SpotMomentumAlert{}, false, fmt.Errorf("read Spot alert spool record: %w", err)
	}
	var alert storage.SpotMomentumAlert
	if err := json.Unmarshal(bytes.TrimSpace(line), &alert); err != nil {
		return storage.SpotMomentumAlert{}, false, fmt.Errorf("decode Spot alert spool record: %w", err)
	}
	// Events written by the previous version did not persist the threshold.
	if alert.ThresholdPercent == 0 {
		alert.ThresholdPercent = storage.DefaultSpotAlertThreshold
	}
	return alert, true, nil
}

// Ack removes the oldest record using an atomic file replacement.
func (s *spotAlertSpool) Ack(expected storage.SpotMomentumAlert) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		return fmt.Errorf("read Spot alert spool before acknowledge: %w", err)
	}
	newline := bytes.IndexByte(data, '\n')
	if newline < 0 {
		return errors.New("Spot alert spool has no complete record to acknowledge")
	}
	var first storage.SpotMomentumAlert
	if err := json.Unmarshal(bytes.TrimSpace(data[:newline]), &first); err != nil {
		return fmt.Errorf("decode oldest Spot alert before acknowledge: %w", err)
	}
	if first.ThresholdPercent == 0 {
		first.ThresholdPercent = storage.DefaultSpotAlertThreshold
	}
	if first.Symbol != expected.Symbol || !first.DetectedAt.Equal(expected.DetectedAt) ||
		!first.ReferenceAt.Equal(expected.ReferenceAt) ||
		first.WindowSeconds != expected.WindowSeconds ||
		first.ThresholdPercent != expected.ThresholdPercent {
		// A previous acknowledge may have completed its atomic rename even if
		// syncing the directory then reported an error. Never remove a new head.
		return nil
	}
	remaining := data[newline+1:]
	temp, err := os.CreateTemp(filepath.Dir(s.path), ".spot-alerts-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary Spot alert spool: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)

	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("set temporary Spot alert spool permissions: %w", err)
	}
	if _, err := temp.Write(remaining); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write temporary Spot alert spool: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync temporary Spot alert spool: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close temporary Spot alert spool: %w", err)
	}
	if err := os.Rename(tempName, s.path); err != nil {
		return fmt.Errorf("replace Spot alert spool after acknowledge: %w", err)
	}
	directory, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		return fmt.Errorf("open Spot alert spool directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync Spot alert spool directory: %w", err)
	}
	return nil
}
