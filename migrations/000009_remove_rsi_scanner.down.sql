-- Removed RSI history and settings cannot be reconstructed.
DO $$
BEGIN
    RAISE EXCEPTION 'migration 000009 is irreversible: RSI history and settings were deleted';
END
$$;
