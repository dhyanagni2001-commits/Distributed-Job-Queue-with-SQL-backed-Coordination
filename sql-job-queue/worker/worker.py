import os
import time
import json
import signal
import socket
from datetime import datetime, timezone, timedelta

from psycopg import OperationalError
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, push_to_gateway

from db import get_conn

# -----------------------------
# Config
# -----------------------------
WORKER_ID = os.getenv("WORKER_ID", socket.gethostname())
QUEUE = os.getenv("QUEUE", "default")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))
POLL_INTERVAL_MS = int(os.getenv("POLL_INTERVAL_MS", "500"))
LEASE_SECONDS = int(os.getenv("LEASE_SECONDS", "60"))

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL")  # e.g. http://pushgateway:9091 (optional)

# -----------------------------
# Prometheus (pushgateway)
# -----------------------------
REGISTRY = CollectorRegistry()

W_POLL_LOOPS = Counter("worker_poll_loops_total", "Worker poll loops", ["worker_id"], registry=REGISTRY)
W_JOBS_LOCKED = Counter("worker_jobs_locked_total", "Jobs locked by worker", ["worker_id", "queue"], registry=REGISTRY)
W_JOBS_DONE = Counter("worker_jobs_completed_total", "Jobs completed by worker", ["worker_id", "queue"], registry=REGISTRY)
W_JOBS_FAIL = Counter("worker_jobs_failed_total", "Jobs failed by worker", ["worker_id", "queue"], registry=REGISTRY)
W_INFLIGHT = Gauge("worker_inflight", "Jobs in-flight", ["worker_id", "queue"], registry=REGISTRY)
W_JOB_TIME = Histogram("worker_job_duration_seconds", "Job execution duration", ["job_type"], registry=REGISTRY)
W_DB_ERRORS = Counter("worker_db_errors_total", "DB errors", ["worker_id"], registry=REGISTRY)

def push_metrics():
    if not PUSHGATEWAY_URL:
        return
    try:
        push_to_gateway(PUSHGATEWAY_URL, job="sql_job_queue_worker", registry=REGISTRY)
    except Exception:
        # metrics must never crash worker
        pass

# -----------------------------
# Graceful shutdown
# -----------------------------
STOP = False

def handle_signal(signum, frame):
    global STOP
    STOP = True
    print(f"[worker] received signal {signum}, shutting down...")

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# -----------------------------
# Job Handlers
# -----------------------------
def handle_job(job: dict):
    """
    Implement job types here.
    This is where you add real tasks: email send, video encode, scraping, ML inference, etc.
    """
    job_type = job["type"]
    payload = job.get("payload") or {}

    if job_type == "demo.sleep":
        seconds = int(payload.get("seconds", 1))
        time.sleep(seconds)
        return

    # Unknown job type -> fail (worker marks FAILED/DEAD accordingly)
    raise RuntimeError(f"Unknown job type: {job_type}")

# -----------------------------
# DB Operations
# -----------------------------
def now_utc():
    return datetime.now(timezone.utc)

def reclaim_stale_leases(conn):
    """
    If a worker died while holding a lock, reclaim jobs whose locked_at is too old.
    """
    lease_cutoff = now_utc() - timedelta(seconds=LEASE_SECONDS)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status='PENDING', locked_by=NULL, locked_at=NULL
            WHERE status='RUNNING'
              AND locked_at IS NOT NULL
              AND locked_at < %s
            """,
            (lease_cutoff,),
        )

def pick_jobs(conn):
    """
    Pick jobs safely using row locks:
    - only PENDING
    - only our QUEUE
    - only due jobs
    - priority DESC, FIFO
    - SKIP LOCKED for multi-worker concurrency
    """
    with conn.cursor() as cur:
        cur.execute("BEGIN;")

        # Reclaim stale locks occasionally (cheap and safe)
        reclaim_stale_leases(conn)

        cur.execute(
            """
            SELECT *
            FROM jobs
            WHERE status='PENDING'
              AND queue = %s
              AND run_after <= NOW()
            ORDER BY priority DESC, created_at
            FOR UPDATE SKIP LOCKED
            LIMIT %s
            """,
            (QUEUE, BATCH_SIZE),
        )
        rows = cur.fetchall()

        if not rows:
            cur.execute("COMMIT;")
            return []

        job_ids = [r["id"] for r in rows]

        cur.execute(
            """
            UPDATE jobs
            SET status='RUNNING',
                locked_by=%s,
                locked_at=NOW()
            WHERE id = ANY(%s)
            """,
            (WORKER_ID, job_ids),
        )

        cur.execute("COMMIT;")
        return [dict(r) for r in rows]

def mark_completed(conn, job_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status='COMPLETED', locked_by=NULL, locked_at=NULL
            WHERE id=%s
            """,
            (job_id,),
        )

def backoff_seconds(attempts: int) -> int:
    # exponential backoff with cap
    return min(2 ** attempts, 60)

def mark_failed(conn, job: dict, err: str):
    """
    If attempts < max_attempts -> FAILED and schedule retry
    Else -> DEAD
    """
    attempts = int(job["attempts"])
    max_attempts = int(job["max_attempts"])
    new_attempts = attempts + 1

    with conn.cursor() as cur:
        if new_attempts >= max_attempts:
            cur.execute(
                """
                UPDATE jobs
                SET status='DEAD',
                    attempts=%s,
                    last_error=%s,
                    locked_by=NULL,
                    locked_at=NULL
                WHERE id=%s
                """,
                (new_attempts, err, job["id"]),
            )
        else:
            delay = backoff_seconds(new_attempts)
            run_after = now_utc() + timedelta(seconds=delay)
            cur.execute(
                """
                UPDATE jobs
                SET status='FAILED',
                    attempts=%s,
                    last_error=%s,
                    run_after=%s,
                    locked_by=NULL,
                    locked_at=NULL
                WHERE id=%s
                """,
                (new_attempts, err, run_after, job["id"]),
            )

def is_cancelled(conn, job_id) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM jobs WHERE id=%s", (job_id,))
        row = cur.fetchone()
        return row and row["status"] == "CANCELLED"

# -----------------------------
# Main Loop
# -----------------------------
def main():
    print(f"[worker] started worker_id={WORKER_ID} queue={QUEUE} batch={BATCH_SIZE}")

    while not STOP:
        W_POLL_LOOPS.labels(worker_id=WORKER_ID).inc()

        try:
            with get_conn() as conn:
                jobs = pick_jobs(conn)
                if not jobs:
                    push_metrics()
                    time.sleep(POLL_INTERVAL_MS / 1000.0)
                    continue

                W_JOBS_LOCKED.labels(worker_id=WORKER_ID, queue=QUEUE).inc(len(jobs))

                for job in jobs:
                    # Skip if job cancelled after lock (rare but possible)
                    if is_cancelled(conn, job["id"]):
                        continue

                    W_INFLIGHT.labels(worker_id=WORKER_ID, queue=QUEUE).inc()
                    t0 = time.time()

                    try:
                        handle_job(job)
                        mark_completed(conn, job["id"])
                        W_JOBS_DONE.labels(worker_id=WORKER_ID, queue=QUEUE).inc()
                    except Exception as e:
                        err = str(e)
                        mark_failed(conn, job, err)
                        W_JOBS_FAIL.labels(worker_id=WORKER_ID, queue=QUEUE).inc()
                    finally:
                        W_JOB_TIME.labels(job_type=job["type"]).observe(time.time() - t0)
                        W_INFLIGHT.labels(worker_id=WORKER_ID, queue=QUEUE).dec()
                        push_metrics()

        except OperationalError as e:
            W_DB_ERRORS.labels(worker_id=WORKER_ID).inc()
            print("[worker] db error:", e)
            time.sleep(1)
        except Exception as e:
            print("[worker] unexpected error:", e)
            time.sleep(1)

    print("[worker] stopped cleanly")

if __name__ == "__main__":
    main()
