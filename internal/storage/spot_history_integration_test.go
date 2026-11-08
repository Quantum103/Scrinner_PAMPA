package storage

import (
	"context"
	"testing"
	"time"
)

func TestClearUserSpotHistoryPreservesOtherUsersAndSubscriptions(t *testing.T) {
	repository, pool := testRepository(t)
	ctx := context.Background()
	first := createTestUser(t, repository)
	second := createTestUser(t, repository)
	for _, user := range []TelegramUser{first, second} {
		if err := repository.SubscribeSpotAlerts(ctx, user.ID); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	alert := SpotMomentumAlert{
		Symbol:     "CLEAR" + uniqueSuffix() + "USDT",
		DetectedAt: now, ReferenceAt: now.Add(-15 * time.Minute),
		Price: 1.1, ReferencePrice: 1, ChangePercent: 10, WindowSeconds: 900,
	}
	alertID, inserted, queued, err := repository.SaveSpotMomentumAlert(ctx, alert)
	if err != nil || !inserted || queued != 2 {
		t.Fatalf("save shared alert: id=%d inserted=%t queued=%d err=%v", alertID, inserted, queued, err)
	}
	for _, user := range []TelegramUser{first, second} {
		if err := repository.MarkSpotAlertDeliverySent(ctx, user.ID, alertID); err != nil {
			t.Fatal(err)
		}
	}

	deleted, err := repository.ClearUserSpotAlertHistory(ctx, first.ID)
	if err != nil || deleted != 1 {
		t.Fatalf("clear first user's history: deleted=%d err=%v", deleted, err)
	}
	firstHistory, err := repository.ListUserSpotAlertHistory(ctx, first.ID, 10, 0)
	if err != nil || len(firstHistory) != 0 {
		t.Fatalf("first user's history remains: %+v err=%v", firstHistory, err)
	}
	secondHistory, err := repository.ListUserSpotAlertHistory(ctx, second.ID, 10, 0)
	if err != nil || len(secondHistory) != 1 || secondHistory[0].ID != alertID {
		t.Fatalf("other user's history changed: %+v err=%v", secondHistory, err)
	}
	var subscriptions int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM spot_alert_subscriptions WHERE user_id IN ($1, $2)",
		first.ID, second.ID).Scan(&subscriptions); err != nil {
		t.Fatal(err)
	}
	if subscriptions != 2 {
		t.Fatalf("subscriptions after cleanup = %d, want 2", subscriptions)
	}
}
