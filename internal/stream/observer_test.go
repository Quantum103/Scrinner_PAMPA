package stream

import (
	"errors"
	"testing"

	marketpaper "github.com/Quantum103/Scrinner_PAMPA/internal/paper"
	"github.com/Quantum103/Scrinner_PAMPA/internal/strategy"
)

type observerRecorder struct {
	signals       int
	paperEvents   int
	connection    []ConnectionStatus
	marketIssues  []string
}

func (o *observerRecorder) OnSignal(strategy.Signal) {
	o.signals++
}

func (o *observerRecorder) OnPaperEvent(marketpaper.Event, marketpaper.AccountSnapshot) {
	o.paperEvents++
}

func (o *observerRecorder) OnConnectionStatus(status ConnectionStatus) {
	o.connection = append(o.connection, status)
}

func (o *observerRecorder) OnMarketDataIssue(message string) {
	o.marketIssues = append(o.marketIssues, message)
}

func TestObserverReceivesInitialConnectFailureAndConnectionTransitions(t *testing.T) {
	manager := testManager(t)
	recorder := &observerRecorder{}
	manager.SetObserver(recorder)
	manager.mu.Lock()
	manager.totalShards = 1
	manager.mu.Unlock()

	manager.setShardConnection([]string{"BTCUSDT"}, false, errors.New("connection refused"))
	if len(recorder.marketIssues) != 1 {
		t.Fatalf("initial connection failure alerts = %d, want 1", len(recorder.marketIssues))
	}
	if len(recorder.connection) != 0 {
		t.Fatalf("initial failure should not be reported as a connected-state transition: %+v", recorder.connection)
	}

	manager.setShardConnection([]string{"BTCUSDT"}, true, nil)
	manager.setShardConnection([]string{"BTCUSDT"}, false, errors.New("connection lost"))
	if len(recorder.connection) != 2 {
		t.Fatalf("connection state transitions = %d, want 2", len(recorder.connection))
	}
	if recorder.connection[0].ConnectedShards != 1 || recorder.connection[1].ConnectedShards != 0 {
		t.Fatalf("unexpected connection counts: %+v", recorder.connection)
	}
}
