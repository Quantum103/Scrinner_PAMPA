# Database migrations

Migrations are applied automatically at application startup and tracked in schema_migrations. Every migration has paired up and down SQL files.

Migration 000009 removes the retired Linear/RSI scanner schema and historical signal data. It is intentionally irreversible because deleted records cannot be reconstructed.

Migration 000010 introduces personal Spot alert rules and extends event identity with its threshold and time window. Existing Spot subscribers receive a default 5% / 15-minute rule. The migration is intentionally irreversible after new rule-specific events have been stored, because the previous event uniqueness key cannot represent multiple thresholds for the same symbol and timestamps.

Telegram account records, Spot Watchlist tables, Spot alert history, subscriptions, and delivery queues remain.
