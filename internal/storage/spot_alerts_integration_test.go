package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSpotAlertSubscriptionsAndDurableDelivery(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	subscriber := createTestUser(t, repository)
	other := createTestUser(t, repository)
	if err := repository.SubscribeSpotAlerts(ctx, subscriber.ID); err != nil {
		t.Fatalf("subscribe first user: %v", err)
	}
	if err := repository.SubscribeSpotAlerts(ctx, subscriber.ID); err != nil {
		t.Fatalf("repeat subscription: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	alert := SpotMomentumAlert{
		Symbol: "TST" + uniqueSuffix() + "USDT", DetectedAt: now, ReferenceAt: now.Add(-15 * time.Minute),
		Price: 1.08, ReferencePrice: 1, ChangePercent: 8, ThresholdPercent: 5,
		WindowSeconds: 900,
	}
	alertID, inserted, queued, err := repository.SaveSpotMomentumAlert(ctx, alert)
	if err != nil {
		t.Fatalf("save Spot alert: %v", err)
	}
	if !inserted || alertID <= 0 || queued != 1 {
		t.Fatalf("unexpected saved Spot alert: id=%d inserted=%t queued=%d", alertID, inserted, queued)
	}
	duplicateID, inserted, queued, err := repository.SaveSpotMomentumAlert(ctx, alert)
	if err != nil {
		t.Fatalf("save duplicate Spot alert: %v", err)
	}
	if inserted || duplicateID != alertID || queued != 0 {
		t.Fatalf("duplicate Spot alert was not deduplicated: id=%d inserted=%t queued=%d", duplicateID, inserted, queued)
	}

	claimed, err := repository.ClaimPendingSpotAlertDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("claim Spot alert: %v", err)
	}
	if len(claimed) != 1 || claimed[0].TelegramID != subscriber.TelegramID ||
		claimed[0].AlertID != alertID || claimed[0].AttemptCount != 1 {
		t.Fatalf("unexpected Spot alert delivery: %+v", claimed)
	}
	if claimed[0].Symbol != alert.Symbol || claimed[0].Price != 1.08 ||
		claimed[0].ReferencePrice != 1 || claimed[0].ChangePercent != 8 ||
		claimed[0].WindowSeconds != 900 {
		t.Fatalf("Spot alert fields were not preserved: %+v", claimed[0])
	}

	if err := repository.MarkSpotAlertDeliveryFailed(ctx, subscriber.ID, alertID, time.Now().Add(-time.Second), errors.New("temporary Telegram error")); err != nil {
		t.Fatalf("mark Spot delivery failed: %v", err)
	}
	retried, err := repository.ClaimPendingSpotAlertDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("claim Spot retry: %v", err)
	}
	if len(retried) != 1 || retried[0].AttemptCount != 2 {
		t.Fatalf("Spot delivery retry was not scheduled: %+v", retried)
	}
	if err := repository.MarkSpotAlertDeliverySent(ctx, subscriber.ID, alertID); err != nil {
		t.Fatalf("mark Spot delivery sent: %v", err)
	}
	remaining, err := repository.ClaimPendingSpotAlertDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("check sent Spot deliveries: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("sent Spot delivery was claimed again: %+v", remaining)
	}

	if err := repository.SubscribeSpotAlerts(ctx, other.ID); err != nil {
		t.Fatalf("subscribe second user: %v", err)
	}
	next := alert
	next.DetectedAt = now.Add(time.Second)
	next.ReferenceAt = next.DetectedAt.Add(-15 * time.Minute)
	next.Price = 1.1
	next.ChangePercent = 10
	nextID, inserted, queued, err := repository.SaveSpotMomentumAlert(ctx, next)
	if err != nil || !inserted || queued != 2 {
		t.Fatalf("enqueue second alert: id=%d inserted=%t queued=%d err=%v", nextID, inserted, queued, err)
	}
	if err := repository.UnsubscribeSpotAlerts(ctx, subscriber.ID); err != nil {
		t.Fatalf("unsubscribe first user: %v", err)
	}
	pending, err := repository.ClaimPendingSpotAlertDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("claim after unsubscribe: %v", err)
	}
	if len(pending) != 1 || pending[0].TelegramID != other.TelegramID {
		t.Fatalf("unsubscribed user still has pending deliveries: %+v", pending)
	}
	if err := repository.UnsubscribeSpotAlerts(ctx, subscriber.ID); err != nil {
		t.Fatalf("repeat unsubscription: %v", err)
	}
}

func TestSpotAlertRejectsInvalidDataAndInactiveSubscriber(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	user := createTestUser(t, repository)

	if err := repository.SubscribeSpotAlerts(ctx, 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("subscribe with invalid ID error = %v", err)
	}
	if err := repository.SubscribeSpotAlerts(ctx, user.ID+99999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("subscribe missing user error = %v", err)
	}
	if _, err := repository.pool.Exec(ctx, `UPDATE telegram_users SET is_active = FALSE WHERE id = $1`, user.ID); err != nil {
		t.Fatalf("deactivate Telegram test user: %v", err)
	}
	if err := repository.SubscribeSpotAlerts(ctx, user.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("subscribe inactive user error = %v", err)
	}

	now := time.Now().UTC()
	_, _, _, err := repository.SaveSpotMomentumAlert(ctx, SpotMomentumAlert{
		Symbol: "BAD SYMBOL", DetectedAt: now, ReferenceAt: now.Add(-time.Minute),
		Price: 2, ReferencePrice: 1, ChangePercent: 100, ThresholdPercent: 5, WindowSeconds: 60,
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid Spot alert error = %v", err)
	}
}

func TestSpotAlertDeliveryLeaseCanBeRecoveredAfterWorkerRestart(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	subscriber := createTestUser(t, repository)
	if err := repository.SubscribeSpotAlerts(ctx, subscriber.ID); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	alert := SpotMomentumAlert{
		Symbol:     "LEASE" + uniqueSuffix() + "USDT",
		DetectedAt: now, ReferenceAt: now.Add(-15 * time.Minute),
		Price: 1.1, ReferencePrice: 1, ChangePercent: 10, ThresholdPercent: 5, WindowSeconds: 900,
	}
	alertID, inserted, _, err := repository.SaveSpotMomentumAlert(ctx, alert)
	if err != nil || !inserted {
		t.Fatalf("save Spot event: id=%d inserted=%t err=%v", alertID, inserted, err)
	}
	first, err := repository.ClaimPendingSpotAlertDeliveries(ctx, 10, time.Millisecond)
	if err != nil || len(first) != 1 || first[0].AttemptCount != 1 {
		t.Fatalf("first lease: deliveries=%+v err=%v", first, err)
	}
	time.Sleep(10 * time.Millisecond)
	recovered, err := repository.ClaimPendingSpotAlertDeliveries(ctx, 10, time.Minute)
	if err != nil || len(recovered) != 1 || recovered[0].AttemptCount != 2 {
		t.Fatalf("stale lease recovery: deliveries=%+v err=%v", recovered, err)
	}
	if err := repository.MarkSpotAlertDeliverySent(ctx, subscriber.ID, alertID); err != nil {
		t.Fatalf("mark recovered delivery sent: %v", err)
	}
}
