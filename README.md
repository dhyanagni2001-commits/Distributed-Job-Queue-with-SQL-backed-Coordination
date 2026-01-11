## 🚀 Distributed Job Queue with SQL-Backed Coordination
A production-grade distributed background job processing system built on PostgreSQL with exactly-once execution, fault tolerance, retries, scheduling, metrics, and Kubernetes deployment.

This system is inspired by architectures used at Amazon, Stripe, Uber, and Airbnb for asynchronous workflows.

# ✨ Features
**Core System**

SQL-backed distributed job queue (PostgreSQL)

Exactly-once execution using row-level locking

Distributed worker pool

Crash recovery with lease reclaim

Automatic retries with exponential backoff

Dead-letter queue

Idempotent job enqueue API

**Observability**

Prometheus metrics (API + worker)

Grafana dashboards

Job latency, throughput, failures, queue depth

**Operations**

Web dashboard UI

Job cancellation & retry

Priority queues

Named queues (email, video, default, etc.)

Rate limiting

Cron scheduler service

**Cloud Native**

Dockerized services

Kubernetes deployment manifests

Horizontal autoscaling ready

ConfigMaps & Secrets

# 📦 Tech Stack

Python 3.11

FastAPI

PostgreSQL 16

psycopg3

Docker & Docker Compose

Prometheus + Grafana

# Kubernetes

croniter

🧠 Job Lifecycle
PENDING → RUNNING → COMPLETED
        ↘ FAILED → RETRY → DEAD

# 🗃 Database Schema
jobs table
CREATE TABLE jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'PENDING',
  queue TEXT NOT NULL DEFAULT 'default',
  priority INT NOT NULL DEFAULT 0,
  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 5,
  run_after TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  locked_by TEXT,
  locked_at TIMESTAMPTZ,
  last_error TEXT,
  idempotency_key TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX jobs_pick_idx
ON jobs (status, queue, run_after, priority DESC, created_at);  

# ▶ Running Locally (Docker)
Start everything
docker compose up --build -d

Enqueue a job
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"type":"demo.sleep","payload":{"seconds":2}}'

List jobs
http://localhost:8000/jobs

Dashboard UI
http://localhost:8000/

# 📊 Metrics (Prometheus + Grafana)
Start observability stack
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d

Prometheus
http://localhost:9090

Grafana
http://localhost:3000


Login:

admin / admin


Metrics:

jobs_enqueued_total

jobs_completed_total

worker_jobs_completed_total

worker_job_duration_seconds

jobs_pending

# ⏱ Cron Scheduler

Create recurring jobs using cron syntax.

Example schedule:

{
  "type": "demo.sleep",
  "payload": {"seconds": 1},
  "cron": "*/5 * * * *"
}


Scheduler service:

polls schedules

computes next_run_at

enqueues jobs automatically

# 🚦 Rate Limiting

API supports per-client rate limiting:

token bucket

DB-based distributed limiter

configurable limits

Protects against abuse and job flooding.

# 🎯 Priority Queues

Jobs support:

named queues (default, email, video)

per-queue worker pools

priority ordering

starvation protection

Run dedicated workers per queue:

QUEUE=email docker compose up -d worker

# ☸ Kubernetes Deployment
Apply manifests
kubectl apply -f k8s/

Components

PostgreSQL StatefulSet

API Deployment (replicas=2)

Worker Deployment (replicas=3+)

Scheduler Deployment

ConfigMap + Secrets

Horizontal Pod Autoscaler

Prometheus + Grafana (Helm)

Autoscaling

Workers scale automatically based on:

CPU usage

Queue depth (via Prometheus adapter)