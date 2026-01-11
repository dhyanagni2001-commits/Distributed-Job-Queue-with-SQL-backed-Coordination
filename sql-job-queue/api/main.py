from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta, timezone
import json

from db import get_conn

app = FastAPI(title="SQL Job Queue API")

# ---------- Models ----------

class EnqueueRequest(BaseModel):
    type: str
    payload: Dict[str, Any] = {}
    priority: int = 0
    run_after_seconds: int = 0
    max_attempts: int = 5

class JobOut(BaseModel):
    id: str
    type: str
    payload: Dict[str, Any]
    status: str
    priority: int
    attempts: int
    max_attempts: int
    run_after: datetime
    locked_by: Optional[str]
    locked_at: Optional[datetime]
    last_error: Optional[str]
    created_at: datetime
    updated_at: datetime


# ---------- Helpers ----------

def serialize_job(job):
    job["id"] = str(job["id"])
    return job


# ---------- Routes ----------

@app.get("/health")
def health():
    return {"ok": True}


@app.post("/jobs", response_model=JobOut)
def enqueue_job(req: EnqueueRequest, idempotency_key: Optional[str] = Header(default=None)):
    run_after = datetime.now(timezone.utc) + timedelta(seconds=req.run_after_seconds)

    with get_conn() as conn:
        with conn.cursor() as cur:

            # Idempotency check
            if idempotency_key:
                cur.execute("SELECT * FROM jobs WHERE idempotency_key = %s", (idempotency_key,))
                existing = cur.fetchone()
                if existing:
                    return serialize_job(existing)

            cur.execute(
                """
                INSERT INTO jobs(type, payload, priority, run_after, max_attempts, idempotency_key)
                VALUES (%s, %s::jsonb, %s, %s, %s, %s)
                RETURNING *;
                """,
                (req.type, json.dumps(req.payload), req.priority, run_after, req.max_attempts, idempotency_key),
            )

            job = cur.fetchone()
            return serialize_job(job)


@app.get("/jobs", response_model=List[JobOut])
def list_jobs(limit: int = 50):
    limit = min(max(limit, 1), 200)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC LIMIT %s",
                (limit,)
            )
            jobs = cur.fetchall()
            return [serialize_job(j) for j in jobs]


@app.get("/jobs/{job_id}", response_model=JobOut)
def get_job(job_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
            job = cur.fetchone()

            if not job:
                raise HTTPException(status_code=404, detail="Job not found")

            return serialize_job(job)


@app.post("/jobs/{job_id}/cancel", response_model=JobOut)
def cancel_job(job_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status='CANCELED', locked_by=NULL, locked_at=NULL
                WHERE id=%s AND status IN ('PENDING','FAILED')
                RETURNING *;
                """,
                (job_id,),
            )

            job = cur.fetchone()
            if not job:
                raise HTTPException(status_code=400, detail="Job cannot be canceled")

            return serialize_job(job)
