-- A rule may create multiple events with the same symbol and timestamps,
-- so the old unique key cannot be restored without potentially losing history.
DO $$
BEGIN
    RAISE EXCEPTION 'migration 000010 is irreversible after configurable Spot rules have generated events';
END
$$;
