-- The previous Telegram allowlist is being replaced with password authentication.
-- Force existing accounts to authenticate once with the shared access password.
UPDATE telegram_users
SET is_active = FALSE, updated_at = now()
WHERE is_active = TRUE;
