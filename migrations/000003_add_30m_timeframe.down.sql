-- Refuse to roll back while 30-minute data exists; never silently delete user data.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM user_timeframes WHERE timeframe = '30')
       OR EXISTS (SELECT 1 FROM signals WHERE timeframe = '30')
       OR EXISTS (SELECT 1 FROM scanner_states WHERE timeframe = '30')
       OR EXISTS (SELECT 1 FROM scanner_profile_states WHERE timeframe = '30') THEN
        RAISE EXCEPTION
            'Cannot roll back 30-minute timeframe support while 30-minute data exists';
    END IF;
END
$$;

ALTER TABLE scanner_profile_states
    DROP CONSTRAINT scanner_profile_states_timeframe_check,
    ADD CONSTRAINT scanner_profile_states_timeframe_check
        CHECK (timeframe IN ('5', '15', '60'));

ALTER TABLE scanner_states
    DROP CONSTRAINT scanner_states_timeframe_check,
    ADD CONSTRAINT scanner_states_timeframe_check
        CHECK (timeframe IN ('5', '15', '60'));

ALTER TABLE signals
    DROP CONSTRAINT signals_timeframe_check,
    ADD CONSTRAINT signals_timeframe_check
        CHECK (timeframe IN ('5', '15', '60'));

ALTER TABLE user_timeframes
    DROP CONSTRAINT user_timeframes_timeframe_check,
    ADD CONSTRAINT user_timeframes_timeframe_check
        CHECK (timeframe IN ('5', '15', '60'));
