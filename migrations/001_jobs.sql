CREATE TABLE jobs (
  id              bigserial PRIMARY KEY,
  queue           text        NOT NULL DEFAULT 'default',
  type            text        NOT NULL,                 -- handler name
  payload         jsonb       NOT NULL DEFAULT '{}',
  idempotency_key text,
  priority        int         NOT NULL DEFAULT 0,
  run_at          timestamptz NOT NULL DEFAULT now(),
  status          text        NOT NULL DEFAULT 'queued'
                  CHECK (status IN ('queued','running','done','dead')),
  attempts        int         NOT NULL DEFAULT 0,
  max_attempts    int         NOT NULL DEFAULT 5,
  locked_by       text,
  locked_until    timestamptz,                          -- visibility timeout
  last_error      text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  started_at      timestamptz,                          -- first claim time
  finished_at     timestamptz,
  UNIQUE (queue, idempotency_key)
);

-- Partial index: only ready-to-claim rows, in the exact order the claim query wants them.
CREATE INDEX jobs_ready_idx ON jobs (queue, priority DESC, run_at) WHERE status = 'queued';

-- Lets the reaper find expired locks without scanning finished jobs.
CREATE INDEX jobs_running_idx ON jobs (locked_until) WHERE status = 'running';

-- Used by the crash test and the "write-effect" handler to detect duplicate side effects.
CREATE TABLE job_effects (
  job_id     bigint PRIMARY KEY,
  worker_id  text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
