# Email Marketing System -- Design Document

Multi-tenant email marketing platform built on Resend for delivery. Supports
transactional emails (login codes, order confirmations), promotional emails
(marketing blasts), and file-based bulk campaigns with scheduling. Transactional
emails are never blocked by promotional volume -- separate queues, separate
workers, separate scaling.

**Key properties:**

- 6 services: API, Worker (transactional), Worker (promotional), Campaign Worker, Webhook Service, Analytics Worker
- PostgreSQL for durable state, Redis for job queues
- GKE deployment with HPA autoscaling on queue depth
- Dry-run mode for load testing without email costs or Resend API calls

---

## Architecture

```
                                POST /send-email
                                POST /schedule-email
                                POST /campaigns
  Tenants ──────────────────────────────────►  API Service (:8080)
  (booking, marketing, friending)                  │
                                                   │ INSERT emails/campaigns
                                                   │ LPUSH
                                                   ▼
                                          ┌────────────────────────────┐
                                          │         Redis              │
                                          │                            │
                                          │  queue:transactional ──────┼──► Worker (transactional)
                                          │  queue:promotional   ──────┼──► Worker (promotional, x2)
                                          │  queue:campaigns     ──────┼──► Campaign Worker
                                          │  queue:analytics     ──────┼──► Analytics Worker
                                          └────────────────────────────┘
                                                                              │
                                               ┌──────────────────────────────┘
                                               │
                                               ▼
                                          Resend API
                                          (https://api.resend.com/emails)
                                               │
                                               │ webhook callbacks
                                               ▼
                                          Webhook Service (:8080)
                                               │
                                               ├── INSERT delivery_events
                                               ├── INSERT unsubscribes (on unsub)
                                               └── LPUSH queue:analytics
                                                          │
                                                          ▼
                                                   Analytics Worker
                                                          │
                                                          ▼
                                                   UPSERT campaign_metrics
                                                   (hourly buckets)
```

**Data flow summary:**

```
Client → API → Redis Queues → Workers → Resend API
                                 ↑
Resend Webhooks → Webhook Service → delivery_events + queue:analytics
                                                       ↓
                                                 Analytics Worker → campaign_metrics
```

---

### Services

| Service | Binary | Role | Port | Scaling Strategy |
|---------|--------|------|------|------------------|
| API | `cmd/api` | HTTP ingestion -- validate, persist, enqueue | 8080 | HPA on CPU/RPS |
| Worker (transactional) | `cmd/worker` | BRPOP `queue:transactional`, send via Resend | -- | Fixed 1 replica (low latency) |
| Worker (promotional) | `cmd/worker` | BRPOP `queue:promotional`, send via Resend | -- | HPA on `LLEN queue:promotional` |
| Campaign Worker | `cmd/campaign` | BRPOP `queue:campaigns`, fan-out file to `queue:promotional` | -- | HPA on `LLEN queue:campaigns` |
| Webhook Service | `cmd/webhook` | Receives Resend webhook events, writes delivery_events | 8080 | HPA on CPU |
| Analytics Worker | `cmd/analytics` | BRPOP `queue:analytics`, batch-upsert campaign_metrics | -- | HPA on `LLEN queue:analytics` |

The Worker binary (`cmd/worker`) is deployed twice -- once for transactional and
once for promotional -- consuming different Redis queues. Same binary, different
queue assignment via the BRPOP keys. Both are started as goroutines within the
same process for local development; in production they are separate Deployments.

---

### Queue Topology

| Queue | Producer | Consumer | Purpose |
|-------|----------|----------|---------|
| `queue:transactional` | API (on `POST /send-email` with `category=TRANSACTIONAL`), Scheduler sweep | Worker (transactional) | High-priority email delivery -- login codes, order confirmations |
| `queue:promotional` | API (on `POST /send-email` with `category=PROMOTIONAL`), Campaign Worker fan-out | Worker (promotional) | Bulk/marketing email delivery, isolated from transactional |
| `queue:campaigns` | API (on `POST /campaigns`), Campaign Sweeper | Campaign Worker | Triggers file-streaming fan-out for bulk campaigns |
| `queue:analytics` | Webhook Service (on every Resend callback) | Analytics Worker | Batched metrics aggregation into `campaign_metrics` |

All queues use Redis lists with LPUSH (producer) / BRPOP (consumer). BRPOP uses
a 2-second timeout so workers can check for context cancellation and shut down
cleanly on SIGINT/SIGTERM.

---

## Data Schema

### Tables

**tenants** -- Multi-tenant isolation. Each email and campaign belongs to a tenant.

```sql
tenants (
  id         UUID PK DEFAULT gen_random_uuid(),
  name       TEXT UNIQUE NOT NULL,       -- "booking-service", "marketing-service"
  created_at TIMESTAMPTZ DEFAULT NOW()
)
```

**messages** -- Reusable template content keyed by (template_type, locale).

```sql
messages (
  id             UUID PK DEFAULT gen_random_uuid(),
  template_type  TEXT NOT NULL,       -- LOGIN_MSG, WELCOME, PROMO_OFFER, ORDER_CONFIRM
  locale         TEXT DEFAULT 'en',
  content        TEXT NOT NULL,
  created_at     TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (template_type, locale)
)
```

**emails** -- Every email job, whether transactional or part of a campaign.

```sql
emails (
  id                  UUID PK DEFAULT gen_random_uuid(),
  tenant_id           UUID FK → tenants(id),
  recipient_user_id   TEXT NOT NULL,
  recipient_address   TEXT NOT NULL,
  category            TEXT CHECK (IN 'TRANSACTIONAL','PROMOTIONAL'),
  template_type       TEXT NOT NULL,
  template_attributes JSONB DEFAULT '{}',
  locale              TEXT DEFAULT 'en',
  status              TEXT CHECK (IN 'PENDING','QUEUED','SENT','FAILED'),
  scheduled_at        TIMESTAMPTZ,         -- NULL = send immediately
  sent_at             TIMESTAMPTZ,
  failure_reason      TEXT,
  campaign_id         UUID FK → campaigns(id),  -- NULL for non-campaign emails
  resend_id           TEXT,                      -- Resend's message ID after send
  created_at          TIMESTAMPTZ DEFAULT NOW()
)
-- Indexes: (status), (tenant_id), (scheduled_at WHERE NOT NULL), (campaign_id WHERE NOT NULL), (resend_id)
```

**delivery_events** -- Append-only event log for every state transition.

```sql
delivery_events (
  id          UUID PK DEFAULT gen_random_uuid(),
  email_id    UUID FK → emails(id),
  event_type  TEXT NOT NULL,    -- QUEUED, SENT, DELIVERED, OPENED, CLICKED, BOUNCED, FAILED, UNSUBSCRIBED
  occurred_at TIMESTAMPTZ DEFAULT NOW(),
  metadata    JSONB DEFAULT '{}'
)
```

**campaigns** -- Bulk promotional sends driven by a file of user IDs.

```sql
campaigns (
  id                  UUID PK DEFAULT gen_random_uuid(),
  tenant_id           UUID FK → tenants(id),
  template_type       TEXT NOT NULL,
  template_attributes JSONB DEFAULT '{}',
  locale              TEXT DEFAULT 'en',
  status              TEXT CHECK (IN 'PENDING','RUNNING','DONE','FAILED'),
  total_recipients    INT DEFAULT 0,
  queued_count        INT DEFAULT 0,
  sent_count          INT DEFAULT 0,
  failed_count        INT DEFAULT 0,
  file_path           TEXT DEFAULT '',    -- /uploads/campaign-xxx.txt or S3 key
  scheduled_at        TIMESTAMPTZ,        -- NULL = run immediately
  completed_at        TIMESTAMPTZ,
  created_at          TIMESTAMPTZ DEFAULT NOW()
)
-- Indexes: (tenant_id), (scheduled_at WHERE NOT NULL)
```

**campaign_metrics** -- Time-bucketed aggregates per campaign, updated by the Analytics Worker.

```sql
campaign_metrics (
  campaign_id  UUID FK → campaigns(id),
  bucket       TIMESTAMPTZ,              -- truncated to hour
  sent         INT DEFAULT 0,
  delivered    INT DEFAULT 0,
  opened       INT DEFAULT 0,
  clicked      INT DEFAULT 0,
  bounced      INT DEFAULT 0,
  unsubscribed INT DEFAULT 0,
  failed       INT DEFAULT 0,
  PRIMARY KEY (campaign_id, bucket)
)
-- Index: (bucket)
```

**unsubscribes** -- Global per-tenant suppression list. Checked before every send.

```sql
unsubscribes (
  id         UUID PK DEFAULT gen_random_uuid(),
  email      TEXT NOT NULL,
  tenant_id  UUID FK → tenants(id),
  reason     TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (email, tenant_id)
)
-- Index: (email)
```

---

### Email State Machine

```
PENDING ──► QUEUED ──► SENT ──► DELIVERED
                  ╲               ╱  ╲
                   ╲             ╱    ╲
                    ► FAILED    ► OPENED ──► CLICKED
                                ╲
                                 ► BOUNCED
```

- `PENDING` -- created in DB; scheduled emails stay here until `scheduled_at <= NOW()`
- `QUEUED` -- pushed to Redis queue; worker has picked it up
- `SENT` -- Resend accepted the message (stored `resend_id`)
- `FAILED` -- Resend rejected, recipient unsubscribed, or max retries exhausted
- `DELIVERED` / `OPENED` / `CLICKED` / `BOUNCED` -- set by Resend webhooks via the Webhook Service

State transitions `PENDING → QUEUED → SENT` are managed by the API + Worker.
Transitions after `SENT` are driven by Resend webhook callbacks.

### Campaign State Machine

```
PENDING ──► RUNNING ──► DONE
                 ╲
                  ► FAILED
```

- `PENDING` -- campaign created; scheduled campaigns wait for sweeper
- `RUNNING` -- fan-out in progress; Campaign Worker is streaming the file
- `DONE` -- all recipients queued; `completed_at` set
- `FAILED` -- file not found, DB error, or context cancelled

---

## API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/send-email` | Validate, persist email, enqueue immediately |
| `POST` | `/schedule-email` | Validate, persist with `scheduled_at`; sweeper enqueues later |
| `GET` | `/delivery-stats` | Count by `(category, status)`, filterable by `?tenant_id=` |
| `POST` | `/campaigns` | Create campaign from file; enqueue or schedule |
| `GET` | `/campaigns/{id}` | Poll campaign progress (queued/sent/failed counts, delivery rate) |

**`POST /send-email` request:**
```json
{
  "tenant_id": "00000000-0000-0000-0000-000000000001",
  "user_id": "user-42",
  "category": "TRANSACTIONAL",
  "template_type": "LOGIN_MSG",
  "template_attributes": {"code": "987654"},
  "locale": "en"
}
```

**`POST /campaigns` request:**
```json
{
  "tenant_id": "00000000-0000-0000-0000-000000000002",
  "template_type": "PROMO_OFFER",
  "template_attributes": {"offer": "Weekend sale: 40% off"},
  "locale": "en",
  "file_path": "/uploads/campaign-123.txt",
  "scheduled_at": "2026-06-12T09:00:00Z"
}
```

---

## Resend Integration

### ResendClient

The `ResendClient` struct in `cmd/worker/main.go` wraps the Resend HTTP API:

- **Send(job, body)** -- POST to `https://api.resend.com/emails` with Bearer auth
- Returns Resend's message ID, stored as `resend_id` on the email row
- Uses a 10-second HTTP client timeout

### Dry-Run Mode

When `--dry-run` flag or `DRY_RUN=true` env var is set:

- `Send()` returns `"dry-run-{email_id}"` immediately -- no HTTP call to Resend
- Email is still marked SENT in the database with a synthetic `resend_id`
- All queue processing, DB writes, and metrics work identically
- Enables full load testing of the pipeline without email costs or rate limits

### Webhook Verification

The Webhook Service at `POST /webhooks/resend` handles Resend callbacks:

- If `RESEND_WEBHOOK_SECRET` is set, requires `svix-signature` header presence
- Full Svix HMAC verification is stubbed for MVP (header presence check only)
- Unknown event types are acknowledged (200 OK) to prevent Resend retries

### Supported Webhook Events

| Resend Event | Internal Type | Action |
|---|---|---|
| `email.delivered` | `DELIVERED` | Insert delivery_event, push to queue:analytics |
| `email.opened` | `OPENED` | Insert delivery_event, push to queue:analytics |
| `email.clicked` | `CLICKED` | Insert delivery_event, push to queue:analytics |
| `email.bounced` | `BOUNCED` | Insert delivery_event, push to queue:analytics |
| `email.unsubscribed` | `UNSUBSCRIBED` | Insert delivery_event, push to queue:analytics, insert into unsubscribes table |

The webhook handler looks up the internal `email_id` by `resend_id`, so emails
must have been sent (and `resend_id` stored) before webhooks can be processed.

---

## Campaign Fan-Out

The Campaign Worker streams a newline-delimited file of user IDs and fans out
individual `EmailJob` records to `queue:promotional` in batches of 1,000.

**Flow:**

1. Marketing service uploads `/uploads/campaign-123.txt` (one user_id per line)
2. `POST /campaigns` with `file_path` creates a campaign record, pushes to `queue:campaigns`
3. Campaign Worker BRPOPs, opens the file, scans line-by-line
4. Every 1,000 lines: batch INSERT 1,000 email rows + LPUSH 1,000 jobs to `queue:promotional`
5. Counters (`total_recipients`, `queued_count`, `failed_count`) updated atomically per batch
6. On completion: `status=DONE`, `completed_at=NOW()`

**Memory profile:** Peak memory is one 1,000-line batch in RAM, never the full file.
A 1M-user campaign uses ~1MB of memory, not ~100MB.

**Scheduling:** Campaigns with `scheduled_at` stay `PENDING`. A sweeper goroutine
(30-second interval) runs `UPDATE ... RETURNING` to atomically claim due campaigns
and push them to `queue:campaigns`. Double-dispatch is prevented by the
`PENDING → RUNNING` status transition being atomic.

**Sent-count sync:** A separate goroutine polls every 10 seconds, updating
`sent_count` and `failed_count` on running campaigns from actual email statuses.

---

## Analytics Worker

The Analytics Worker consumes `queue:analytics` and batch-upserts into
`campaign_metrics` using time-bucketed (hourly) counters.

**Batching strategy:**

- Accumulates up to 100 events or waits 5 seconds (whichever comes first)
- Groups events by `(campaign_id, bucket_hour)`
- Uses `INSERT ... ON CONFLICT DO UPDATE SET col = col + EXCLUDED.col` for idempotent counter increments
- Non-campaign events (individual transactional sends) are skipped

**Graceful shutdown:** On SIGINT/SIGTERM, drains any remaining events in the
batch before exiting.

---

## Autoscaler (GKE HPA)

| Deployment | Metric | Min Replicas | Max Replicas | Target Value |
|---|---|---|---|---|
| api | CPU utilization | 2 | 10 | 70% |
| worker-transactional | `LLEN queue:transactional` | 1 | 5 | 100 |
| worker-promotional | `LLEN queue:promotional` | 1 | 20 | 500 |
| worker-campaign | `LLEN queue:campaigns` | 1 | 3 | 1 |
| webhook | CPU utilization | 2 | 10 | 70% |
| analytics | `LLEN queue:analytics` | 1 | 5 | 200 |

**Custom metrics pipeline:**

```
Redis LLEN → redis-exporter → Prometheus → prometheus-adapter → HPA
```

- `redis-exporter` scrapes Redis and exposes queue lengths as Prometheus metrics
- `prometheus-adapter` translates Prometheus metrics into the Kubernetes custom metrics API
- HPA queries `custom.metrics.k8s.io` and scales Deployments based on queue depth

---

## Failure Modes

| Component | Failure Mode | When It Breaks | Impact | Mitigation |
|---|---|---|---|---|
| Redis | Memory exhaustion | Queue depth > available RAM | All enqueues fail, API returns 500 | Monitor `used_memory` + HPA to drain queues faster |
| PostgreSQL | Connection exhaustion | > 100 concurrent connections | All DB operations fail | PgBouncer + connection pooling (`sql.SetMaxOpenConns`) |
| PostgreSQL | `delivery_events` bloat | > 100M rows (no partitioning) | Query slowdown on event lookups | Partition by month, archive to cold storage |
| Worker | Resend rate limit | Free tier: 100 emails/day | 429 errors, emails marked FAILED | Exponential backoff + token bucket rate limiter |
| Campaign Worker | Large file (>10M lines) | Fan-out takes hours | Campaign stuck in RUNNING | Shard files across multiple campaign jobs |
| Webhook Service | Burst after recovery | Resend retries queued webhooks | CPU spike, potential OOM | HPA on CPU + buffering via queue:analytics |
| Analytics Worker | Crash mid-batch | Loses up to 100 events | Dashboard metrics drift | Backfill from delivery_events table |
| HPA | Scale-up lag | Sudden burst (e.g., campaign fan-out) | ~30s of queue growth before new pods ready | Pre-scale with `minReplicas` + pod warm-up |
| Network | Resend API timeout | API unreachable or slow | Emails stuck in QUEUED | 10s context timeout + retry with backoff |
| Resend Webhooks | Out-of-order delivery | No ordering guarantee from Resend | Event ordering wrong (e.g., OPENED before DELIVERED) | Idempotent counters in campaign_metrics (additive, not state-dependent) |

---

## Observability

### Prometheus Metrics

**API Service:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `email_enqueue_total` | Counter | `category`, `tenant_id` | Emails enqueued |
| `campaign_created_total` | Counter | `tenant_id` | Campaigns created |
| `api_request_duration_seconds` | Histogram | `endpoint`, `status` | Request latency |

**Worker:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `email_send_total` | Counter | `category`, `status` | Emails sent (success/fail) |
| `email_send_duration_seconds` | Histogram | `category` | Resend API call latency |
| `email_queue_depth` | Gauge | `queue` | Current LLEN of each queue |

**Webhook Service:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `webhook_received_total` | Counter | `event_type` | Webhooks received by type |
| `webhook_processing_errors_total` | Counter | `error_type` | DB/Redis errors during processing |

**Analytics Worker:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `analytics_events_processed_total` | Counter | `event_type` | Events flushed to campaign_metrics |
| `analytics_batch_size` | Histogram | -- | Events per flush batch |
| `analytics_flush_duration_seconds` | Histogram | -- | Time to flush a batch |

### Grafana Dashboards

**1. Campaign Performance**

- Open rate: `opened / delivered * 100` per campaign
- Click rate: `clicked / delivered * 100` per campaign
- Bounce rate: `bounced / sent * 100` per campaign
- Delivery funnel: sent -> delivered -> opened -> clicked (stacked bar)
- Unsubscribe rate over time (hourly buckets from `campaign_metrics`)

**2. System Health**

- Queue depths: `LLEN` for all 4 queues (line chart, alert on sustained growth)
- Worker replica count vs. queue depth (correlation overlay)
- Send latency: p50/p95/p99 of Resend API call duration
- Error rates: failed sends, webhook processing errors, analytics flush failures
- Campaign progress: total_recipients vs. queued vs. sent vs. failed (per running campaign)

---

## Deployment

### Local Development

```bash
# Start all services
docker-compose up --build

# Services available:
#   API:       http://localhost:8083  (mapped from container :8080)
#   Postgres:  localhost:5435
#   Redis:     localhost:6380

# Run integration tests
./scripts/test.sh
```

`docker-compose.yml` runs 5 containers:

| Container | Image | Replicas | Notes |
|---|---|---|---|
| `api` | `Dockerfile.api` | 1 | Port 8083:8080 |
| `worker-transactional` | `Dockerfile.worker` | 1 | Consumes both queues (goroutines) |
| `worker-promotional` | `Dockerfile.worker` | 2 | `deploy.replicas: 2` |
| `worker-campaign` | `Dockerfile.campaign` | 1 | Shared `/uploads` volume with API |
| `postgres` | `postgres:16-alpine` | 1 | Schema auto-loaded from `db/schema.sql` |
| `redis` | `redis:7-alpine` | 1 | -- |

Webhook Service and Analytics Worker are omitted from docker-compose for local
development (they require Resend callbacks). Add them for integration testing.

### GKE Production

```
k8s/
  namespace.yaml          # email-marketing namespace
  configmap.yaml          # DATABASE_URL, REDIS_URL, DRY_RUN
  secret.yaml             # RESEND_API_KEY, RESEND_WEBHOOK_SECRET (from GCP Secret Manager)
  redis.yaml              # StatefulSet + Service (5Gi PVC, liveness/readiness probes)
  deployments/            # Deployment + HPA per service (one YAML each)
  prometheus/             # redis-exporter, prometheus-adapter, ServiceMonitor
```

**Production path for managed services:**

- Redis: GCP Memorystore (Redis 7.x) instead of self-managed StatefulSet
- PostgreSQL: Cloud SQL for PostgreSQL with automated backups, HA
- Secrets: GCP Secret Manager with External Secrets Operator syncing to k8s Secrets

### Dry-Run Load Testing

```bash
# Set DRY_RUN=true in ConfigMap or env var
export DRY_RUN=true

# Generate a large campaign file
seq 1 1000000 | sed 's/^/user-/' > /uploads/load-test-1m.txt

# Create the campaign
curl -X POST http://localhost:8083/campaigns \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "00000000-0000-0000-0000-000000000002",
    "template_type": "PROMO_OFFER",
    "template_attributes": {"offer": "Load test"},
    "file_path": "/uploads/load-test-1m.txt"
  }'

# Monitor queue depths
watch -n1 'redis-cli -p 6380 LLEN queue:promotional'

# All emails will be "sent" instantly (no Resend API calls).
# DB writes, queue throughput, and campaign metrics all exercise the real path.
```

Dry-run mode tests the full pipeline: API validation, Redis enqueue, worker
dequeue, DB state transitions, campaign fan-out, and analytics aggregation --
everything except the actual Resend HTTP call.

---

## Scale Analysis

### 10K emails/day

Everything is fine. Single replicas for all services. Redis memory usage
negligible. PostgreSQL handles the load without indexes being stressed.
`delivery_events` table stays small.

### 100K emails/day

HPA starts scaling promotional workers. Queue depths may spike during campaign
fan-out but drain quickly with 2-3 worker replicas. No infrastructure changes
needed.

### 1M emails/day

- ~12 emails/sec sustained, ~120/sec during bursts
- Need Redis memory monitoring (queue backlog during bursts)
- `delivery_events` table hits ~3M rows/month (3 events/email avg) -- add monthly partitioning
- Campaign fan-out for 1M-user files takes ~15 minutes with batch size 1,000
- Consider increasing fan-out batch size to 5,000-10,000

### 10M emails/day

- ~115 emails/sec sustained, ~1,150/sec peak
- Need queue sharding: `queue:promotional:{0..N}` with workers round-robin
- Template rendering becomes a hot path -- add Redis caching by `(template_type, locale, hash(attributes))`
- PgBouncer required for connection pooling across 20+ worker pods
- `delivery_events` at 30M rows/month -- partitioning mandatory, consider archival to BigQuery
- Campaign files should be on GCS/S3, not shared volumes

### 100M+ emails/day

- ~1,150 emails/sec sustained, ~11,500/sec peak
- Dedicated Redis Cluster (16+ shards) for queue throughput
- PostgreSQL read replicas for `delivery-stats` and campaign status queries
- CDN for tracking pixels (open/click tracking)
- Multi-provider delivery: Resend for transactional, SES for bulk promotional (cost optimization)
- Kafka replaces Redis queues: per-tenant partitioning, infinite replay, consumer groups
- `delivery_events` moves to a columnar store (ClickHouse) -- keep only 7 days in PostgreSQL
- Template rendering as a separate service with aggressive caching

---

## Key Design Decisions

**Why separate queues instead of a priority queue?**
Redis has no native priority queue. Two separate list keys + separate worker pools
gives hard isolation. A promotional burst (10M marketing emails) cannot cause
head-of-line blocking for a transactional login code. Workers are separate
containers -- promotional replicas scale independently.

**Why file-based campaigns instead of inline user IDs?**
A `POST /campaigns` with 1M user IDs in the request body would be a ~20MB JSON
payload. Instead, the marketing service uploads a file to a shared volume (or S3),
and the campaign worker streams it line-by-line. Peak memory is one batch (1,000
records), not the full file.

**Why batch analytics instead of real-time?**
The Analytics Worker batches up to 100 events or 5 seconds because the
`campaign_metrics` table uses `INSERT ... ON CONFLICT DO UPDATE` -- each upsert
takes a row-level lock. Batching reduces lock contention and write amplification.
A single flush groups events by `(campaign_id, bucket)` so each upsert covers
many events.

**Why Resend instead of SendGrid?**
The prototype originally used SendGrid. Resend offers a simpler API, webhook
support via Svix, and a developer-friendly free tier for prototyping. The
`ResendClient` abstraction makes swapping providers straightforward.

**Why Redis lists instead of Kafka?**
At the current scale (prototype to ~10M emails/day), Redis lists are simpler to
operate, have sub-millisecond latency for BRPOP, and handle 100K+ ops/sec
easily. The trade-off is no replay and no consumer groups -- at 100M+/day,
Kafka becomes the right choice for its partitioning, offset tracking, and
replay capabilities.
