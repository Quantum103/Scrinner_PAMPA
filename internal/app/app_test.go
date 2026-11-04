package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthLive(t *testing.T) {
	handler := healthHandler(func(context.Context) error { return errors.New("database must not be checked for liveness") })
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
}

func TestHealthReady(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		handler := healthHandler(func(context.Context) error { return nil })
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
		}
	})

	t.Run("database unavailable", func(t *testing.T) {
		handler := healthHandler(func(context.Context) error { return errors.New("database unavailable") })
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}
	})
}

func TestWaitForSubscriptionRefresh(t *testing.T) {
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if waitForSubscriptionRefresh(ctx, make(chan struct{}), make(chan time.Time)) {
			t.Fatal("waitForSubscriptionRefresh() = true, want false for cancelled context")
		}
	})

	t.Run("market changed", func(t *testing.T) {
		changed := make(chan struct{}, 1)
		changed <- struct{}{}
		if !waitForSubscriptionRefresh(context.Background(), changed, make(chan time.Time)) {
			t.Fatal("waitForSubscriptionRefresh() = false, want true after market change")
		}
	})

	t.Run("periodic refresh", func(t *testing.T) {
		refresh := make(chan time.Time, 1)
		refresh <- time.Now()
		if !waitForSubscriptionRefresh(context.Background(), make(chan struct{}), refresh) {
			t.Fatal("waitForSubscriptionRefresh() = false, want true after refresh tick")
		}
	})
}
