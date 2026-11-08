# Database migrations

Migrations are applied automatically at application startup and tracked in schema_migrations. Every migration has paired up and down SQL files.

Migration 000009 removes the retired Linear/RSI scanner schema and historical signal data. It is intentionally irreversible because deleted records cannot be reconstructed. Telegram account records, Spot Watchlist tables, Spot alert history, subscriptions, and delivery queues are retained.
