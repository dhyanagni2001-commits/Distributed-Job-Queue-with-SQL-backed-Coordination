import os
import time
from datetime import datetime, timezone
from croniter import croniter
from db import get_conn

POLL_SECONDS = int(os.getenv("SCHEDULER_POLL_SECONDS", "2"))

def utcnow():
    return datetime.now(timezone.utc)

def compute_next(cron_expr: str, base: datetime) -> datetime:
    it = croniter(cron_expr, base)
    nxt = it.get_next(datetime)
    if nxt.tzinfo is None:
        nxt = nxt.replace(tzinfo=timezone.utc)
    return nxt

def main():
    print("[scheduler] started")
    while True:
        try:
            now = utcnow()
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("BEGIN;")

                    cur.execute(
                        """
                        SELECT *
                        FROM schedules
                        WHERE enabled=TRUE AND next_run_at <= NOW()
                        FOR UPDATE SKIP LOCKED
                        LIMIT 50
                        """
                    )
                    scheds = cur.fetchall()

                    for s in scheds:
                        # enqueue job
                        cur.execute(
                            """
                            INSERT INTO jobs(type, payload, queue, priority, status, run_after)
                            VALUES (%s, %s, %s, %s, 'PENDING', NOW())
                            """,
                            (s["type"], s["payload"], s["queue"], s["priority"]),
                        )

                        # update next_run
                        nxt = compute_next(s["cron"], now)
                        cur.execute(
                            """
                            UPDATE schedules
                            SET last_run_at=NOW(), next_run_at=%s
                            WHERE id=%s
                            """,
                            (nxt, s["id"]),
                        )

                    cur.execute("COMMIT;")

            time.sleep(POLL_SECONDS)
        except Exception as e:
            print("[scheduler] error:", e)
            time.sleep(2)

if __name__ == "__main__":
    main()
