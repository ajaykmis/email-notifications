# Email Notification System

Multi-tenant email delivery platform supporting transactional and promotional emails
at 1B emails/day (10M users × 100 emails). Transactional emails must never be blocked
by promotional volume. Delivery rate is tracked per email.

---

## Email State Machine

```
PENDING → QUEUED → SENT
                 → FAILED
```

- `PENDING` — created in DB; scheduled emails stay here until `scheduled_at <= NOW()`
- `QUEUED` — pushed to Redis queue; worker has picked it up for delivery
- `SENT` — SendGrid accepted the message
- `FAILED` — SendGrid rejected, or max retries exhausted

---

## Schema

```sql
tenants (
  id UUID PK,
  name TEXT UNIQUE        -- "booking-service", "marketing-service", ...
)

messages (
  id UUID PK,
  template_type TEXT,     -- LOGIN_MSG, WELCOME, PROMO_OFFER, ORDER_CONFIRM
  locale TEXT,            -- "en", "fr", "es"
  content TEXT,           -- raw template body
  UNIQUE (template_type, locale)
)

emails (
  id UUID PK,
  tenant_id UUID FK,
  recipient_user_id TEXT,
  recipient_address TEXT,
  category TEXT,          -- TRANSACTIONAL | PROMOTIONAL
  template_type TEXT,
  template_attributes JSONB,  -- {code: "123456", order_id: "ORD-001", ...}
  locale TEXT,
  status TEXT,            -- PENDING | QUEUED | SENT | FAILED
  scheduled_at TIMESTAMPTZ,   -- NULL = send immediately
  sent_at TIMESTAMPTZ,
  failure_reason TEXT
)

delivery_events (
  id UUID PK,
  email_id UUID FK,
  event_type TEXT,        -- QUEUED | SENT | DELIVERED | BOUNCED | FAILED
  occurred_at TIMESTAMPTZ,
  metadata JSONB
)
```

Indexes: `(status)`, `(tenant_id)`, `(scheduled_at) WHERE scheduled_at IS NOT NULL`

---

## API

| Endpoint | Purpose |
|---|---|
| `POST /send-email` | Validate, persist, enqueue immediately |
| `POST /schedule-email` | Validate, persist with `scheduled_at`; sweeper enqueues later |
| `GET /delivery-stats` | Count by `(category, status)`, filterable by `tenant_id` |

**`POST /send-email` request:**
```json
{
  "tenant_id": "uuid",
  "user_id": "user-42",
  "category": "TRANSACTIONAL",
  "template_type": "LOGIN_MSG",
  "template_attributes": { "code": "987654" },
  "locale": "en"
}
```

**Validation:**
- `category` must be `TRANSACTIONAL` or `PROMOTIONAL`
- `template_type` must be a known template
- `user_id` must resolve via Users service (`userExists()`)
- `tenant_id` must exist

---

## Request Flow

```
Client (booking / marketing / friending svc)
        │
        │ POST /send-email
        ▼
  ┌─────────────────┐
  │  Load Balancer  │
  └────────┬────────┘
           │
           ▼
  ┌─────────────────────────────────────────────┐
  │          Email Ingestion Service             │
  │                                             │
  │  1. Validate request (category, template)   │
  │  2. userExists() → Users Service            │
  │  3. INSERT emails (status=PENDING)           │
  │  4. LPUSH → queue:{transactional|promo}     │
  │  5. UPDATE emails (status=QUEUED)            │
  │  6. INSERT delivery_events (QUEUED)          │
  │                                             │
  │  Returns: { email_id, status: "QUEUED" }    │
  └─────────────────────────────────────────────┘
           │
           │ Redis LPUSH
           ▼
  ┌─────────────────────────────────────────────┐
  │              Redis Queues                   │
  │                                             │
  │   queue:transactional  (high priority)      │
  │   queue:promotional    (isolated)           │
  └───────┬─────────────────────┬───────────────┘
          │                     │
          ▼                     ▼
  ┌───────────────┐   ┌───────────────────────┐
  │  Worker       │   │  Worker (×2 replicas) │
  │  Transact.    │   │  Promotional          │
  └───────┬───────┘   └──────────┬────────────┘
          │                      │
          └──────────┬───────────┘
                     │ BRPOP
                     ▼
           renderTemplate(job)
                     │
                     ▼
             SendGrid API
                     │
          ┌──────────┴──────────┐
          │ success             │ failure
          ▼                     ▼
   UPDATE status=SENT    UPDATE status=FAILED
   INSERT delivery_event  INSERT delivery_event
   (SENT)                 (FAILED, reason)
```

---

## Worker Design

```
BRPOP from queue (2s timeout — allows clean shutdown)
  → renderTemplate(job)       -- substitute TemplateAttributes into template body
  → SendGrid.Send(job, body)  -- POST to api.sendgrid.com/v3/mail/send
  → on success: UPDATE emails SET status=SENT, sent_at=NOW()
               INSERT delivery_events(SENT)
  → on failure: UPDATE emails SET status=FAILED, failure_reason=...
               INSERT delivery_events(FAILED)
```

**Key isolation property:** transactional and promotional workers are separate goroutines
consuming separate Redis keys. A promotional burst (10M marketing emails) cannot cause
head-of-line blocking for a transactional `LOGIN_MSG`. Workers are also separate
Docker containers — promotional replicas scale independently of transactional.

**Template rendering:** `(template_type, locale)` → fetch body from `messages` table,
substitute `template_attributes` with `text/template`. Prototype uses an in-process switch.

---

## Scheduled Email Flow

```
POST /schedule-email
  → INSERT emails (status=PENDING, scheduled_at=T)
  → Returns immediately — no queue push yet

Every 30s — Scheduler goroutine (inside worker):
  SELECT * FROM emails
  WHERE status='PENDING' AND scheduled_at <= NOW()
  → UPDATE status=QUEUED
  → LPUSH queue:{category}
  → INSERT delivery_events(QUEUED)
```

The scheduler is an UPDATE...RETURNING — atomic read-modify, no race between
multiple worker instances. Only rows that transition from PENDING→QUEUED get pushed.

---

## Architecture

```
  Tenants (booking / marketing / friending)
        │
        │ POST /send-email   POST /schedule-email
        ▼
  ┌─────────────┐
  │Load Balancer│
  └──────┬──────┘
         │
         ▼
  ┌───────────────────┐   userExists()    ┌─────────────────┐
  │ Email Ingestion   │ ─────────────────►│  Users Service  │
  │     Service       │                   └─────────────────┘
  │   (stateless)     │   INSERT/UPDATE   ┌─────────────────┐
  │                   │ ─────────────────►│   PostgreSQL     │
  └────────┬──────────┘                   │  (primary DB)   │
           │                              │  emails         │
           │ LPUSH                        │  messages       │
           ▼                              │  delivery_events│
  ┌────────────────────────┐              └────────┬────────┘
  │       Redis            │                       ▲
  │                        │                       │ UPDATE status
  │  queue:transactional ──┼──► Worker (×1)        │ INSERT events
  │  queue:promotional   ──┼──► Worker (×2) ───────┘
  └────────────────────────┘         │
                                     │
                              ┌──────▼──────┐
                              │   SendGrid  │
                              └─────────────┘
```

---

## Scale

| Layer | Approach |
|---|---|
| API servers | Stateless, horizontal — scale on CPU/RPS |
| Transactional workers | Dedicated pool — never share with promotional |
| Promotional workers | Scale on `LLEN queue:promotional`; k8s HPA or KEDA |
| Redis queues | Two queues enforce priority isolation; Redis Cluster for throughput |
| PostgreSQL | Read replicas for `delivery-stats`; primary for writes |
| SendGrid | Rate-limit per API key; shard across multiple keys if needed |

**1B emails/day math:**
- 1B / 86400s ≈ 11,600 emails/sec peak
- 10× burst headroom → ~115,000/sec
- At 50ms/email per worker goroutine → 50 goroutines per worker pod → ~20 worker pods
- Redis easily handles 100K+ ops/sec — not a bottleneck

---

## Staff-Level Follow-ups

### Correctness & Edge Cases

**What if userExists() is down?**
Fail fast — return 503. Do not enqueue without a valid recipient address. Alternatively,
accept the request, store `recipient_user_id` without resolving, and resolve lazily in the worker
before sending (trades strict validation for higher ingestion availability).

**Duplicate sends on worker crash?**
Worker crashes after SendGrid accepts but before updating DB → email is re-queued
by a timeout reaper and sent again. Fix: store SendGrid's `X-Message-Id` in the emails
row before calling Send, check for it on retry. Or use SendGrid's `x-unique-args` for
server-side deduplication by `email_id`.

**Promotional email to unsubscribed user?**
Validate against a suppression list before enqueuing (not after). Failing to do so wastes
worker cycles and risks sending to opted-out users. Check `suppressions` table or call
a dedicated Preferences service in the ingestion layer.

**Scheduled email and user deletes their account between creation and delivery?**
Worker calls userExists() again at send time, not just at ingestion. If user no longer
exists, mark email FAILED with `reason=USER_DELETED` rather than sending to a stale address.

---

### Scale & Performance

**Separate queues are not enough at massive promotional scale.**
At 800M promotional emails/day, even with isolated workers, a single Redis key becomes
a hot write. Solution: fan out to `queue:promotional:{shard_0..N}`. Workers round-robin
across shards. Kafka is a better fit at this scale — topics partition naturally.

**Template rendering is synchronous and per-email.**
At 11K emails/sec, rendering the same `LOGIN_MSG` template thousands of times/sec is
wasteful. Cache rendered templates by `(template_type, locale, hash(attributes))` in
Redis. Invalidate on template update. Reduces DB reads for the `messages` table to near zero.

**Delivery-stats query is a full table scan.**
`GROUP BY category, status` without a time bound scans all emails. Add `created_at`
index and require callers to supply a time window. For high-frequency dashboards, maintain
a materialized counters table updated by triggers or a separate aggregation process.

**Localization lookup on every send.**
Worker currently embeds templates in code. Production version should fetch from `messages`
table by `(template_type, locale)` with Redis caching. Missing locale falls back to `en`
before failing — never drop a transactional email because `fr` isn't translated yet.

---

### Operational & Failure Modes

**SendGrid rate limit hit.**
SendGrid returns 429. Worker should implement exponential backoff (1s, 2s, 4s)
and re-push to the back of the queue on exhaustion rather than marking FAILED.
Maintain a per-API-key token bucket at the worker level to avoid even sending requests
that will be rejected.

**Redis goes down.**
Ingestion service cannot enqueue — return 503. In-flight BRPOP calls fail — workers idle
until Redis recovers. No emails are lost (emails table has PENDING/QUEUED records).
On recovery, a reconciliation job can re-enqueue all QUEUED emails that have no `sent_at`.

**How to track actual delivery (not just SENT)?**
SENT = SendGrid accepted. DELIVERED = recipient mail server confirmed.
Implement a SendGrid webhook handler (`POST /sendgrid/webhook`) that receives
`delivered`, `bounce`, `open`, `click` events and writes them to `delivery_events`.
This gives true delivery rate vs. acceptance rate.

**Dead-letter handling.**
Emails that fail after N retries should move to a dead-letter queue (separate Redis key
or DB table). Expose `GET /emails/{id}/retry` to allow ops to manually replay specific
emails after the underlying issue (bad template, invalid address) is fixed.

---

### Design Tradeoffs

**Redis vs Kafka for the job queue?**
Redis: simpler ops, sub-millisecond BRPOP, adequate for 100K ops/sec.
Kafka: ordered delivery per partition, infinite replay, consumer group semantics, built-in
offset tracking. At 1B emails/day Kafka is worth it — you get per-tenant partitioning,
the ability to replay failed promotional batches, and separation of fast transactional
consumers from slow promotional ones with consumer groups rather than separate topics.

**Two queues vs priority queue within one queue?**
Redis has no native priority queue. Two separate keys + separate worker pools is simpler,
more debuggable, and gives hard isolation. A min-heap priority queue (Redis Sorted Set
with score = priority × timestamp) is an alternative but workers still compete on
the same CPU — a promotional burst still impacts transactional latency.

**Synchronous userExists() call at ingestion vs. async resolution at send time?**
Synchronous (current): fail fast, user gets immediate 404, no wasted queue slot.
Async: higher ingestion throughput, but emails can reach the worker after user deletion.
Use synchronous for transactional (latency-sensitive, correctness matters), async for
promotional (bulk send where some attrition is acceptable).

**Scheduled emails: DB polling vs. delayed queue (Redis ZSET)?**
Polling (current): simple, 30s granularity, handles restarts cleanly.
Redis ZSET with score=scheduled_at + ZRANGEBYSCORE: sub-second granularity,
no DB reads at sweep time. Trade-off: ZSET is in-memory only — scheduled emails
survive a Redis restart only if you also write to DB (as we do), so the ZSET is just
an optimization not the source of truth.

---

### Cross-System Thinking

**How does this interact with the Users service at scale?**
`userExists()` is a synchronous RPC on every ingest. At 11K emails/sec that's 11K
RPC calls/sec to the Users service. Cache positive results in an in-process LRU
(TTL=60s) keyed by `user_id`. For promotional sends where `user_id` lists are known
at job-creation time, validate the entire batch up front via a bulk `usersExist([]id)`
endpoint instead of per-email point lookups.

**Multi-region deployment.**
Transactional emails need low latency → ingest in the user's home region, send from
the same region's workers. Promotional emails are bulk/async → one global queue is fine,
workers can be in the cheapest region. Use a geolocation header or tenant config to
route `POST /send-email` to the nearest ingestion shard.

**Compliance — GDPR right-to-erasure.**
When a user requests deletion, the `delivery_events` and `emails` tables contain their
address. Options: (1) hard delete rows — loses delivery history. (2) replace address with
a hash or NULL — preserves aggregate delivery stats. (3) store address only in encrypted
form with a per-user key — delete the key on erasure. Option 3 is ideal but operationally
complex. At minimum, `recipient_address` and `recipient_user_id` should be in a separate
table that can be wiped without touching the event log.

**Where does the system break at 10× scale (10B emails/day)?**
1. PostgreSQL `emails` table at 10B rows/day → terabyte-scale fast. Partition by
   `created_at` (monthly partitions), archive to S3 via pg_partman.
2. Single Redis instance → Redis Cluster with 16 shards.
3. SendGrid API limit → multi-provider (SendGrid + SES + Mailgun) with a provider
   abstraction layer; route by category (transactional → SendGrid, promotional → SES for cost).
4. `delivery_events` at 3 events/email = 30B rows/day → move to a columnar store
   (ClickHouse, BigQuery) for analytics; keep only last 7 days in PostgreSQL.
