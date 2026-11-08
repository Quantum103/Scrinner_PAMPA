package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSpotAlertRulesArePersonalAndTargetOnlyMatchingUsers(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	first := createTestUser(t, repository)
	second := createTestUser(t, repository)
	for _, user := range []TelegramUser{first, second} {
		if err := repository.SubscribeSpotAlerts(ctx, user.ID); err != nil {
			t.Fatal(err)
		}
	}

	firstDefault, err := repository.ListSpotAlertRules(ctx, first.ID)
	if err != nil || len(firstDefault) != 1 ||
		firstDefault[0].ThresholdPercent != DefaultSpotAlertThreshold ||
		firstDefault[0].WindowSeconds != DefaultSpotAlertWindow {
		t.Fatalf("first subscriber default rule = %+v err=%v", firstDefault, err)
	}
	secondDefault, err := repository.ListSpotAlertRules(ctx, second.ID)
	if err != nil || len(secondDefault) != 1 {
		t.Fatalf("second subscriber default rule = %+v err=%v", secondDefault, err)
	}

	custom, created, err := repository.AddSpotAlertRule(ctx, first.ID, 3, 5*60)
	if err != nil || !created || custom.ThresholdPercent != 3 || custom.WindowSeconds != 300 {
		t.Fatalf("add first user's custom rule: rule=%+v created=%t err=%v", custom, created, err)
	}
	duplicate, created, err := repository.AddSpotAlertRule(ctx, first.ID, 3, 5*60)
	if err != nil || created || duplicate.ID != custom.ID {
		t.Fatalf("duplicate rule was not idempotent: rule=%+v created=%t err=%v", duplicate, created, err)
	}
	secondCustom, created, err := repository.AddSpotAlertRule(ctx, second.ID, 10, 10*60)
	if err != nil || !created || secondCustom.ThresholdPercent != 10 || secondCustom.WindowSeconds != 600 {
		t.Fatalf("add second user's custom rule: rule=%+v created=%t err=%v", secondCustom, created, err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	alert := SpotMomentumAlert{
		Symbol: "RULE" + uniqueSuffix() + "USDT",
		DetectedAt: now, ReferenceAt: now.Add(-5 * time.Minute),
		Price: 1.035, ReferencePrice: 1, ChangePercent: 3.5,
		ThresholdPercent: 3, WindowSeconds: 5 * 60,
	}
	alertID, inserted, queued, err := repository.SaveSpotMomentumAlert(ctx, alert)
	if err != nil || !inserted || queued != 1 {
		t.Fatalf("save rule-specific alert: id=%d inserted=%t queued=%d err=%v", alertID, inserted, queued, err)
	}
	deliveries, err := repository.ClaimPendingSpotAlertDeliveries(ctx, 10, time.Minute)
	if err != nil || len(deliveries) != 1 || deliveries[0].UserID != first.ID ||
		deliveries[0].ThresholdPercent != 3 || deliveries[0].WindowSeconds != 300 {
		t.Fatalf("alert should only be delivered to user with matching rule: %+v err=%v", deliveries, err)
	}
	if err := repository.MarkSpotAlertDeliverySent(ctx, first.ID, alertID); err != nil {
		t.Fatal(err)
	}

	firstRules, err := repository.ListSpotAlertRules(ctx, first.ID)
	if err != nil || len(firstRules) != 2 {
		t.Fatalf("first user's rules: %+v err=%v", firstRules, err)
	}
	secondRules, err := repository.ListSpotAlertRules(ctx, second.ID)
	if err != nil || len(secondRules) != 2 {
		t.Fatalf("second user's rules: %+v err=%v", secondRules, err)
	}
	active, err := repository.ListActiveSpotAlertRules(ctx)
	if err != nil || len(active) != 3 {
		t.Fatalf("active unique rule configurations: %+v err=%v", active, err)
	}

	if err := repository.DeleteSpotAlertRule(ctx, first.ID, firstDefault[0].ID); err != nil {
		t.Fatalf("delete first user's old default rule: %v", err)
	}
	if err := repository.DeleteSpotAlertRule(ctx, first.ID, custom.ID); !errors.Is(err, ErrLastSpotAlertRule) {
		t.Fatalf("deleting the last rule error = %v, want ErrLastSpotAlertRule", err)
	}
	otherRules, err := repository.ListSpotAlertRules(ctx, second.ID)
	if err != nil || len(otherRules) != 2 {
		t.Fatalf("changing first user's rules changed another user: %+v err=%v", otherRules, err)
	}
}

func TestSpotAlertRuleValidationAndLimit(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	user := createTestUser(t, repository)
	for _, invalid := range []struct {
		threshold float64
		window    int
	}{
		{threshold: 0, window: 60},
		{threshold: 1001, window: 60},
		{threshold: 1, window: 30},
		{threshold: 1, window: 3660},
		{threshold: 1, window: 61},
	} {
		if _, _, err := repository.AddSpotAlertRule(ctx, user.ID, invalid.threshold, invalid.window); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("invalid rule %+v error = %v, want ErrInvalidInput", invalid, err)
		}
	}
	for i := 0; i < MaxSpotAlertRulesPerUser; i++ {
		threshold := 1 + float64(i)/100
		if _, _, err := repository.AddSpotAlertRule(ctx, user.ID, threshold, 60); err != nil {
			t.Fatalf("add rule %d: %v", i, err)
		}
	}
	if _, _, err := repository.AddSpotAlertRule(ctx, user.ID, 9, 120); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("adding rule above limit error = %v, want ErrInvalidInput", err)
	}
}

func TestDeletingSpotRuleCancelsItsPendingDeliveries(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	user := createTestUser(t, repository)
	if err := repository.SubscribeSpotAlerts(ctx, user.ID); err != nil {
		t.Fatal(err)
	}
	rule, created, err := repository.AddSpotAlertRule(ctx, user.ID, 3, 5*60)
	if err != nil || !created {
		t.Fatalf("add custom rule: rule=%+v created=%t err=%v", rule, created, err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	alert := SpotMomentumAlert{
		Symbol: "CANCEL" + uniqueSuffix() + "USDT",
		DetectedAt: now, ReferenceAt: now.Add(-5 * time.Minute),
		Price: 1.04, ReferencePrice: 1, ChangePercent: 4,
		ThresholdPercent: 3, WindowSeconds: 300,
	}
	if _, inserted, queued, err := repository.SaveSpotMomentumAlert(ctx, alert); err != nil || !inserted || queued != 1 {
		t.Fatalf("save pending rule event: inserted=%t queued=%d err=%v", inserted, queued, err)
	}
	if err := repository.DeleteSpotAlertRule(ctx, user.ID, rule.ID); err != nil {
		t.Fatalf("delete custom rule: %v", err)
	}
	deliveries, err := repository.ClaimPendingSpotAlertDeliveries(ctx, 10, time.Minute)
	if err != nil || len(deliveries) != 0 {
		t.Fatalf("removed rule still has claimable deliveries: %+v err=%v", deliveries, err)
	}
	rules, err := repository.ListSpotAlertRules(ctx, user.ID)
	if err != nil || len(rules) != 1 ||
		rules[0].ThresholdPercent != DefaultSpotAlertThreshold ||
		rules[0].WindowSeconds != DefaultSpotAlertWindow {
		t.Fatalf("default rule should remain after deleting custom rule: %+v err=%v", rules, err)
	}
}
