-- Add Bybit's 30-minute linear kline interval without rewriting existing data.
ALTER TABLE user_timeframes
    DROP CONSTRAINT user_timeframes_timeframe_check,
    ADD CONSTRAINT user_timeframes_timeframe_check
        CHECK (timeframe IN ('5', '15', '30', '60'));

ALTER TABLE signals
    DROP CONSTRAINT signals_timeframe_check,
    ADD CONSTRAINT signals_timeframe_check
        CHECK (timeframe IN ('5', '15', '30', '60'));

ALTER TABLE scanner_states
    DROP CONSTRAINT scanner_states_timeframe_check,
    ADD CONSTRAINT scanner_states_timeframe_check
        CHECK (timeframe IN ('5', '15', '30', '60'));

ALTER TABLE scanner_profile_states
    DROP CONSTRAINT scanner_profile_states_timeframe_check,
    ADD CONSTRAINT scanner_profile_states_timeframe_check
        CHECK (timeframe IN ('5', '15', '30', '60'));
