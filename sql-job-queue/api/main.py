from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta, timezone
import json
import time
import uuid

from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response

from db import get_conn

app = FastAPI(title="SQL Job Queue API")

# =========================
# Prometheus Metrics
# =========================
JOBS_ENQUEUED = Counter("jobs_enqueued_total", "Total jobs enqueued")
ENQUEUE_LATENCY = Histogram("enqueue_latency_seconds", "Latency of enqueue endpoint")

JOBS_PENDING = Gauge("jobs_pending", "Number of pending jobs")
JOBS_RUNNING = Gauge("jobs_running", "Number of running jobs")
JOBS_COMPLETED = Gauge("jobs_completed", "Number of completed jobs")
JOBS_DEAD = Gauge("jobs_dead", "Number of dead-letter jobs")
JOBS_CANCELLED = Gauge("jobs_cancelled", "Number of cancelled jobs")

# =========================
# Rate Limiting (DB-based, distributed-safe)
# =========================
RATE_WINDOW_SECONDS = int(__import__("os").getenv("RATE_WINDOW_SECONDS", "60"))
RATE_LIMIT = int(__import__("os").getenv("RATE_LIMIT", "60"))  # 60 requests/min per IP by default

def enforce_rate_limit(conn, key: str):
    """
    Distributed-safe rate limit using Postgres.
    Table:
      rate_limits(key TEXT PRIMARY KEY, window_start TIMESTAMPTZ, count INT)
    """
    now = datetime.now(timezone.utc)
    window_start_cutoff = now - timedelta(seconds=RATE_WINDOW_SECONDS)

    with conn.cursor() as cur:
        cur.execute("SELECT window_start, count FROM rate_limits WHERE key=%s", (key,))
        row = cur.fetchone()

        if row is None or row["window_start"] < window_start_cutoff:
            cur.execute(
                """
                INSERT INTO rate_limits(key, window_start, count)
                VALUES (%s, %s, 1)
                ON CONFLICT (key) DO UPDATE
                SET window_start=EXCLUDED.window_start, count=EXCLUDED.count
                """,
                (key, now),
            )
            return

        if row["count"] >= RATE_LIMIT:
            raise HTTPException(status_code=429, detail="Rate limit exceeded. Try again later.")

        cur.execute("UPDATE rate_limits SET count=count+1 WHERE key=%s", (key,))

# =========================
# Models
# =========================
class EnqueueRequest(BaseModel):
    type: str
    payload: Dict[str, Any] = {}
    queue: str = "default"
    priority: int = 0
    run_after_seconds: int = 0
    max_attempts: int = 5

class JobOut(BaseModel):
    id: str
    type: str
    payload: Dict[str, Any]
    status: str
    queue: str
    priority: int
    attempts: int
    max_attempts: int
    run_after: datetime
    locked_by: Optional[str]
    locked_at: Optional[datetime]
    last_error: Optional[str]
    created_at: datetime
    updated_at: datetime

# =========================
# Helpers
# =========================
def serialize_job(job):
    job = dict(job)
    job["id"] = str(job["id"])
    # psycopg returns JSONB as dict already in many setups, but keep safe:
    if isinstance(job.get("payload"), str):
        job["payload"] = json.loads(job["payload"])
    return job

def job_exists(cur, job_id: str):
    cur.execute("SELECT 1 FROM jobs WHERE id=%s", (job_id,))
    return cur.fetchone() is not None

# =========================
# Routes
# =========================
@app.get("/health")
def health():
    return {"ok": True}

# -------------------------
# Dashboard UI
# -------------------------
@app.get("/", response_class=HTMLResponse)
def dashboard():
    return """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>SQL Job Queue Dashboard</title>
  <style>
    body { font-family: system-ui, Arial; padding: 16px; }
    .row { display: flex; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; }
    input, select { padding: 8px; }
    button { padding: 8px 10px; cursor: pointer; }
    pre { background: #f6f6f6; padding: 12px; border-radius: 10px; overflow: auto; }
    a { color: #0b66ff; text-decoration: none; }
  </style>
</head>
<body>
  <h2>SQL Job Queue Dashboard</h2>
  <div class="row">
    <button onclick="loadJobs()">Refresh</button>
    <label>Limit <input id="limit" value="50" style="width:80px"/></label>
    <label>Status
      <select id="status">
        <option value="">(all)</option>
        <option value="PENDING">PENDING</option>
        <option value="RUNNING">RUNNING</option>
        <option value="COMPLETED">COMPLETED</option>
        <option value="DEAD">DEAD</option>
        <option value="CANCELLED">CANCELLED</option>
      </select>
    </label>
    <label>Queue <input id="queue" placeholder="default"/></label>
  </div>

  <div class="row">
    <input id="jobId" placeholder="job id" style="width:420px"/>
    <button onclick="cancelJob()">Cancel</button>
    <button onclick="retryJob()">Retry</button>
    <button onclick="deleteJob()">Delete</button>
  </div>

  <p>
    Metrics: <a href="/metrics" target="_blank">/metrics</a>
  </p>

  <pre id="out">Loading...</pre>

<script>
async function loadJobs(){
  const limit = document.getElementById('limit').value.trim() || "50";
  const status = document.getElementById('status').value.trim();
  const queue = document.getElementById('queue').value.trim();

  const params = new URLSearchParams({limit});
  if(status) params.set("status", status);
  if(queue) params.set("queue", queue);

  const r = await fetch('/jobs?' + params.toString());
  const j = await r.json();
  document.getElementById('out').textContent = JSON.stringify(j, null, 2);
}

async function cancelJob(){
  const id = document.getElementById('jobId').value.trim();
  if(!id) return alert('Enter job id');
  const r = await fetch(`/jobs/${id}/cancel`, {method:'POST'});
  alert(await r.text());
  loadJobs();
}

async function retryJob(){
  const id = document.getElementById('jobId').value.trim();
  if(!id) return alert('Enter job id');
  const r = await fetch(`/jobs/${id}/retry`, {method:'POST'});
  alert(await r.text());
  loadJobs();
}

async function deleteJob(){
  const id = document.getElementById('jobId').value.trim();
  if(!id) return alert('Enter job id');
  const r = await fetch(`/jobs/${id}`, {method:'DELETE'});
  alert(await r.text());
  loadJobs();
}

loadJobs();
</script>
</body>
</html>
"""

# -------------------------
# Enqueue Job
# -------------------------
@app.post("/jobs", response_model=JobOut)
def enqueue_job(req: EnqueueRequest, request: Request, idempotency_key: Optional[str] = Header(default=None)):
    """
    Enqueue a job with:
    - idempotency_key: returns existing job if already enqueued
    - queue: named queue support
    - priority: bigger = earlier
    - run_after_seconds: schedule for later
    - rate limiting per client IP (DB-backed)
    """
    t0 = time.time()
    run_after = datetime.now(timezone.utc) + timedelta(seconds=req.run_after_seconds)

    client_ip = request.client.host if request.client else "unknown"

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Rate limiting
            enforce_rate_limit(conn, f"ip:{client_ip}")

            # Idempotency check
            if idempotency_key:
                cur.execute("SELECT * FROM jobs WHERE idempotency_key = %s", (idempotency_key,))
                existing = cur.fetchone()
                if existing:
                    return serialize_job(existing)

            cur.execute(
                """
                INSERT INTO jobs(type, payload, queue, priority, run_after, max_attempts, idempotency_key)
                VALUES (%s, %s::jsonb, %s, %s, %s, %s, %s)
                RETURNING *;
                """,
                (req.type, json.dumps(req.payload), req.queue, req.priority, run_after, req.max_attempts, idempotency_key),
            )

            job = cur.fetchone()

    JOBS_ENQUEUED.inc()
    ENQUEUE_LATENCY.observe(time.time() - t0)

    return serialize_job(job)

# -------------------------
# List Jobs (filters)
# -------------------------
@app.get("/jobs", response_model=List[JobOut])
def list_jobs(limit: int = 50, status: Optional[str] = None, queue: Optional[str] = None):
    limit = min(max(limit, 1), 200)
    allowed_status = {"PENDING", "RUNNING", "COMPLETED", "DEAD", "FAILED", "CANCELLED"}

    if status and status not in allowed_status:
        raise HTTPException(status_code=400, detail=f"Invalid status. Allowed: {sorted(allowed_status)}")

    with get_conn() as conn:
        with conn.cursor() as cur:
            q = "SELECT * FROM jobs"
            conds = []
            params = []

            if status:
                conds.append("status = %s")
                params.append(status)
            if queue:
                conds.append("queue = %s")
                params.append(queue)

            if conds:
                q += " WHERE " + " AND ".join(conds)

            q += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)

            cur.execute(q, tuple(params))
            jobs = cur.fetchall()
            return [serialize_job(j) for j in jobs]

# -------------------------
# Get Single Job
# -------------------------
@app.get("/jobs/{job_id}", response_model=JobOut)
def get_job(job_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
            job = cur.fetchone()

            if not job:
                raise HTTPException(status_code=404, detail="Job not found")

            return serialize_job(job)

# -------------------------
# Cancel Job (only PENDING)
# -------------------------
@app.post("/jobs/{job_id}/cancel", response_model=JobOut)
def cancel_job(job_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status='CANCELLED', locked_by=NULL, locked_at=NULL, updated_at=NOW()
                WHERE id=%s AND status IN ('PENDING')
                RETURNING *;
                """,
                (job_id,),
            )
            job = cur.fetchone()

    if not job:
        raise HTTPException(status_code=400, detail="Job cannot be cancelled (only PENDING jobs can be cancelled)")

    return serialize_job(job)

# -------------------------
# Retry Job (DEAD or FAILED)
# -------------------------
@app.post("/jobs/{job_id}/retry", response_model=JobOut)
def retry_job(job_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status='PENDING',
                    attempts=0,
                    run_after=NOW(),
                    locked_by=NULL,
                    locked_at=NULL,
                    last_error=NULL,
                    updated_at=NOW()
                WHERE id=%s AND status IN ('DEAD','FAILED')
                RETURNING *;
                """,
                (job_id,),
            )
            job = cur.fetchone()

    if not job:
        raise HTTPException(status_code=400, detail="Job cannot be retried (must be DEAD or FAILED)")

    return serialize_job(job)

# -------------------------
# Delete Job (optional admin)
# -------------------------
@app.delete("/jobs/{job_id}")
def delete_job(job_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM jobs WHERE id=%s RETURNING id", (job_id,))
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    return "deleted"

# -------------------------
# Metrics endpoint
# -------------------------
@app.get("/metrics")
def metrics():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) c FROM jobs WHERE status='PENDING'")
            JOBS_PENDING.set(cur.fetchone()["c"])
            cur.execute("SELECT count(*) c FROM jobs WHERE status='RUNNING'")
            JOBS_RUNNING.set(cur.fetchone()["c"])
            cur.execute("SELECT count(*) c FROM jobs WHERE status='COMPLETED'")
            JOBS_COMPLETED.set(cur.fetchone()["c"])
            cur.execute("SELECT count(*) c FROM jobs WHERE status='DEAD'")
            JOBS_DEAD.set(cur.fetchone()["c"])
            cur.execute("SELECT count(*) c FROM jobs WHERE status='CANCELLED'")
            JOBS_CANCELLED.set(cur.fetchone()["c"])

    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
