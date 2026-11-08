package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthLive(t *testing.T) {
	handler := healthHandler(func(context.Context) error {
		return errors.New("database must not be checked for liveness")
	})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
}

func TestHealthReady(t *testing.T) {
	tests := []struct {
		name string
		ping func(context.Context) error
		want int
	}{
		{name: "ready", ping: func(context.Context) error { return nil }, want: http.StatusOK},
		{name: "database unavailable", ping: func(context.Context) error { return errors.New("offline") }, want: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			healthHandler(test.ping).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
			if recorder.Code != test.want {
				t.Fatalf("status = %d, want %d", recorder.Code, test.want)
			}
		})
	}
}
