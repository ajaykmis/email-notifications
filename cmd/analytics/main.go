package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

var (
	db  *sql.DB
	rdb *redis.Client
)

const (
	queueAnalytics = "queue:analytics"
	batchMaxSize   = 100
	batchTimeout   = 5 * time.Second
)

// ── Event types ──────────────────────────────────────────────────────────────

type AnalyticsEvent struct {
	EmailID    string `json:"email_id"`
	CampaignID string `json:"campaign_id"`
	EventType  string `json:"event_type"` // DELIVERED, OPENED, CLICKED, BOUNCED, UNSUBSCRIBED, SENT
	Timestamp  string `json:"timestamp"`
}

// ── Batch accumulator ────────────────────────────────────────────────────────

type eventBatch struct {
	mu     sync.Mutex
	events []AnalyticsEvent
	timer  *time.Timer
}

func newEventBatch() *eventBatch {
	return &eventBatch{}
}

// add appends an event and returns true if the batch should be flushed (hit max size).
func (b *eventBatch) add(ev AnalyticsEvent) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, ev)
	return len(b.events) >= batchMaxSize
}

// drain returns all accumulated events and resets the batch.
func (b *eventBatch) drain() []AnalyticsEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.events
	b.events = nil
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	return out
}

// ── Flush: group by (campaign_id, bucket) and upsert ─────────────────────────

type metricsKey struct {
	CampaignID string
	Bucket     time.Time
}

type metricsCounters struct {
	Sent         int
	Delivered    int
	Opened       int
	Clicked      int
	Bounced      int
	Unsubscribed int
	Failed       int
}

func flush(ctx context.Context, events []AnalyticsEvent) {
	if len(events) == 0 {
		return
	}

	// Group events by (campaign_id, bucket_hour).
	groups := make(map[metricsKey]*metricsCounters)
	for _, ev := range events {
		if ev.CampaignID == "" {
			continue // skip non-campaign individual sends
		}

		ts, err := time.Parse(time.RFC3339, ev.Timestamp)
		if err != nil {
			log.Printf("[analytics] bad timestamp %q for email_id=%s: %v", ev.Timestamp, ev.EmailID, err)
			continue
		}
		bucket := ts.Truncate(time.Hour)

		key := metricsKey{CampaignID: ev.CampaignID, Bucket: bucket}
		c := groups[key]
		if c == nil {
			c = &metricsCounters{}
			groups[key] = c
		}

		switch ev.EventType {
		case "SENT":
			c.Sent++
		case "DELIVERED":
			c.Delivered++
		case "OPENED":
			c.Opened++
		case "CLICKED":
			c.Clicked++
		case "BOUNCED":
			c.Bounced++
		case "UNSUBSCRIBED":
			c.Unsubscribed++
		case "FAILED":
			c.Failed++
		default:
			log.Printf("[analytics] unknown event_type=%s for email_id=%s", ev.EventType, ev.EmailID)
		}
	}

	if len(groups) == 0 {
		return
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("[analytics] begin tx: %v", err)
		return
	}

	for key, c := range groups {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO campaign_metrics (campaign_id, bucket, sent, delivered, opened, clicked, bounced, unsubscribed, failed)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (campaign_id, bucket) DO UPDATE SET
				sent         = campaign_metrics.sent         + EXCLUDED.sent,
				delivered    = campaign_metrics.delivered    + EXCLUDED.delivered,
				opened       = campaign_metrics.opened       + EXCLUDED.opened,
				clicked      = campaign_metrics.clicked      + EXCLUDED.clicked,
				bounced      = campaign_metrics.bounced       + EXCLUDED.bounced,
				unsubscribed = campaign_metrics.unsubscribed + EXCLUDED.unsubscribed,
				failed       = campaign_metrics.failed       + EXCLUDED.failed`,
			key.CampaignID, key.Bucket,
			c.Sent, c.Delivered, c.Opened, c.Clicked, c.Bounced, c.Unsubscribed, c.Failed,
		)
		if err != nil {
			log.Printf("[analytics] upsert campaign_id=%s bucket=%s: %v", key.CampaignID, key.Bucket, err)
			tx.Rollback()
			return
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[analytics] commit: %v", err)
		return
	}

	log.Printf("[analytics] flushed %d events across %d metric groups", len(events), len(groups))
}

// ── Consumer loop ────────────────────────────────────────────────────────────

func consumeLoop(ctx context.Context, batch *eventBatch, flushCh chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		result, err := rdb.BRPop(ctx, 2*time.Second, queueAnalytics).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[analytics] brpop error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var ev AnalyticsEvent
		if err := json.Unmarshal([]byte(result[1]), &ev); err != nil {
			log.Printf("[analytics] unmarshal error: %v", err)
			continue
		}

		// Start timer on first event in a fresh batch.
		batch.mu.Lock()
		firstEvent := len(batch.events) == 0
		batch.mu.Unlock()

		if firstEvent {
			batch.mu.Lock()
			batch.timer = time.AfterFunc(batchTimeout, func() {
				select {
				case flushCh <- struct{}{}:
				default:
				}
			})
			batch.mu.Unlock()
		}

		if batch.add(ev) {
			// Hit max batch size — flush immediately.
			select {
			case flushCh <- struct{}{}:
			default:
			}
		}
	}
}

// ── main ─────────────────────────────────────────────────────────────────────

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/emaildb?sslmode=disable"
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}

	var err error
	db, err = sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err = db.Ping(); err == nil {
			break
		}
		log.Printf("waiting for db... (%d/10)", i+1)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("db ping failed: %v", err)
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("redis parse: %v", err)
	}
	rdb = redis.NewClient(opt)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	batch := newEventBatch()
	flushCh := make(chan struct{}, 1)

	// Consumer goroutine: BRPOP loop that accumulates events.
	go consumeLoop(ctx, batch, flushCh)

	log.Println("Analytics worker started — consuming queue:analytics")

	// Main goroutine: waits for flush signals or shutdown.
	for {
		select {
		case <-flushCh:
			events := batch.drain()
			flush(ctx, events)

		case <-ctx.Done():
			log.Println("Analytics worker shutting down...")
			// Flush remaining events before exit.
			events := batch.drain()
			flush(context.Background(), events)
			log.Println("Analytics worker stopped.")
			return
		}
	}
}
