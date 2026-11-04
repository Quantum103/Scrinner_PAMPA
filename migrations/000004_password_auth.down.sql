-- Access status cannot be reconstructed: the former allowlist was stored only in .env.
-- Leave accounts inactive rather than silently granting access on rollback.
SELECT 1;
