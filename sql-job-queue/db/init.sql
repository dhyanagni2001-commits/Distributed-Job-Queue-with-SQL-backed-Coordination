CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$ BEGIN
  CREATE TYPE job_status AS ENUM ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'DEAD', 'CANCELED');
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status job_status NOT NULL DEFAULT 'PENDING',

  priority INT NOT NULL DEFAULT 0,

  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 5,

  run_after TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  locked_by TEXT NULL,
  locked_at TIMESTAMPTZ NULL,

  last_error TEXT NULL,

  idempotency_key TEXT NULL UNIQUE,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_pick
  ON jobs (status, run_after, priority DESC, created_at);

CREATE INDEX IF NOT EXISTS idx_jobs_running_lease
  ON jobs (status, locked_at);

CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_touch_updated ON jobs;
CREATE TRIGGER trg_touch_updated
BEFORE UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();


-- ================================
-- Priority Queue Support
-- ================================

ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS queue TEXT NOT NULL DEFAULT 'default';

-- Optimized pick index for distributed workers
DROP INDEX IF EXISTS idx_jobs_pick;

CREATE INDEX IF NOT EXISTS idx_jobs_pick
ON jobs (status, queue, run_after, priority DESC, created_at);


-- ================================
-- Distributed Rate Limiter Table
-- ================================

CREATE TABLE IF NOT EXISTS rate_limits (
  key TEXT PRIMARY KEY,
  window_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  count INT NOT NULL
);

