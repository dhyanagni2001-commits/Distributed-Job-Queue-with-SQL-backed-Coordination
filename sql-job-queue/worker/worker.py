import os
import time
import random
from datetime import datetime, timedelta, timezone
from db import get_conn

# ================= CONFIG =================

WORKER_ID = os.getenv("WORKER_ID", "worker-1")
POLL_INTERVAL_MS = int(os.getenv("POLL_INTERVAL_MS", "500"))
LEASE_SECONDS = int(os.getenv("LEASE_SECONDS", "60"))
MAX_ATTEMPTS_DEFAULT = int(os.getenv("MAX_ATTEMPTS", "5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))

# ================= UTILS =================

def utcnow():
    return datetime.now(timezone.utc)

# ================= CRASH RECOVERY =================

def reclaim_stale_locks(conn):
    """
    If a worker crashes while holding a job lock,
    reclaim it after lease timeout.
    """
    lease_deadline = utcnow() - timedelta(seconds=LEASE_SECONDS)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status='PENDING', locked_by=NULL, locked_at=NULL
            WHERE status='RUNNING' AND locked_at < %s
            """,
            (lease_deadline,),
        )

# ================= JOB FINALIZATION =================

def complete_job(conn, job_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status='COMPLETED',
                locked_by=NULL,
                locked_at=NULL,
                last_error=NULL
            WHERE id=%s
            """,
            (job_id,),
        )

def fail_job(conn, job_id, err, attempts, max_attempts):
    """
    Retry with exponential backoff.
    After max attempts → DEAD queue.
    """
    backoff = min(300, (2 ** min(attempts, 8))) + random.randint(0, 3)
    run_after = utcnow() + timedelta(seconds=backoff)

    new_status = "PENDING" if attempts < max_attempts else "DEAD"

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status=%s,
                attempts=%s,
                run_after=%s,
                locked_by=NULL,
                locked_at=NULL,
                last_error=%s
            WHERE id=%s
            """,
            (new_status, attempts, run_after, err[:2000], job_id),
        )

# ================= JOB HANDLERS =================

def handle_job(job):
    """
    Plug your real business logic here.
    """

    job_type = job["type"]
    payload = job["payload"]

    # Demo: sleep job
    if job_type == "demo.sleep":
        seconds = int(payload.get("seconds", 1))
        time.sleep(seconds)
        return

    # Demo: flaky job (random failure)
    if job_type == "demo.flaky":
        if random.random() < 0.6:
            raise RuntimeError("Random failure (demo.flaky)")
        return

    raise RuntimeError(f"Unknown job type: {job_type}")

# ================= WORKER LOOP =================

def main():
    print(f"[{WORKER_ID}] worker started, polling for jobs...")

    while True:
        try:
            print(f"[{WORKER_ID}] polling for jobs...")

            with get_conn() as conn:
                with conn.cursor() as cur:

                    # Recover jobs from crashed workers
                    reclaim_stale_locks(conn)

                    # Start transaction
                    cur.execute("BEGIN;")

                    # Pick jobs with row locking
                    cur.execute(
                        """
                        SELECT id
                        FROM jobs
                        WHERE status='PENDING'
                          AND run_after <= NOW()
                        ORDER BY priority DESC, created_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT %s
                        """,
                        (BATCH_SIZE,),
                    )

                    rows = cur.fetchall()

                    if not rows:
                        cur.execute("COMMIT;")
                        time.sleep(POLL_INTERVAL_MS / 1000)
                        continue

                    ids = [r["id"] for r in rows]

                    # Mark jobs as RUNNING
                    cur.execute(
                        """
                        UPDATE jobs
                        SET status='RUNNING',
                            locked_by=%s,
                            locked_at=NOW()
                        WHERE id = ANY(%s)
                        RETURNING *;
                        """,
                        (WORKER_ID, ids),
                    )

                    jobs = cur.fetchall()

                    # Commit transaction
                    cur.execute("COMMIT;")

            # Execute jobs outside DB transaction
            for job in jobs:
                job_id = job["id"]
                attempts = int(job["attempts"])
                max_attempts = int(job.get("max_attempts") or MAX_ATTEMPTS_DEFAULT)

                try:
                    print(f"[{WORKER_ID}] RUN {job_id} type={job['type']} attempts={attempts}")

                    handle_job(job)

                    with get_conn() as conn2:
                        complete_job(conn2, job_id)

                    print(f"[{WORKER_ID}] DONE {job_id}")

                except Exception as e:
                    attempts += 1

                    with get_conn() as conn2:
                        fail_job(conn2, job_id, str(e), attempts, max_attempts)

                    print(f"[{WORKER_ID}] FAIL {job_id} attempts={attempts} err={e}")

        except Exception as outer:
            print(f"[{WORKER_ID}] WORKER LOOP ERROR: {outer}")
            time.sleep(2)

# ================= ENTRYPOINT =================

if __name__ == "__main__":
    main()
