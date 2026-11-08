-- Retire the Linear/RSI scanner and permanently remove its data.
-- Telegram accounts and all Spot tables remain untouched.
DROP TABLE IF EXISTS signal_deliveries;
DROP TABLE IF EXISTS scanner_profile_states;
DROP TABLE IF EXISTS scanner_states;
DROP TABLE IF EXISTS signals;
DROP TABLE IF EXISTS user_instrument_preferences;
DROP TABLE IF EXISTS user_timeframes;
DROP TABLE IF EXISTS user_settings;
DROP TABLE IF EXISTS instruments;
