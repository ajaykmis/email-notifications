package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

var (
	db  *sql.DB
	rdb *redis.Client
)

// ── Shared types (would live in an internal package in a real service) ─────────

type EmailJob struct {
	EmailID            string         `json:"email_id"`
	TenantID           string         `json:"tenant_id"`
	RecipientUserID    string         `json:"recipient_user_id"`
	RecipientAddress   string         `json:"recipient_address"`
	Category           string         `json:"category"`
	TemplateType       string         `json:"template_type"`
	TemplateAttributes map[string]any `json:"template_attributes"`
	Locale             string         `json:"locale"`
}

const (
	queueTransactional = "queue:transactional"
	queuePromotional   = "queue:promotional"
)

// ── SendGrid mock ─────────────────────────────────────────────────────────────
// Replace with actual SendGrid SDK call in production.

type SendGridClient struct {
	APIKey string
}

func (s *SendGridClient) Send(job EmailJob, body string) error {
	// Simulate occasional failure (1-in-10) for realism
	// In production: POST to api.sendgrid.com/v3/mail/send
	log.Printf("[sendgrid] TO=%s TEMPLATE=%s LOCALE=%s BODY=%q",
		job.RecipientAddress, job.TemplateType, job.Locale, body)
	return nil
}

// ── Template rendering ────────────────────────────────────────────────────────
// In production: fetch template from DB by (template_type, locale), then
// substitute TemplateAttributes using a text/template engine.

func renderTemplate(job EmailJob) string {
	switch job.TemplateType {
	case "LOGIN_MSG":
		code, _ := job.TemplateAttributes["code"].(string)
		return fmt.Sprintf("Your login code is: %s", code)
	case "WELCOME":
		return "Welcome to our platform!"
	case "PROMO_OFFER":
		offer, _ := job.TemplateAttributes["offer"].(string)
		return fmt.Sprintf("Special offer for you: %s", offer)
	case "ORDER_CONFIRM":
		orderID, _ := job.TemplateAttributes["order_id"].(string)
		return fmt.Sprintf("Your order %s has been confirmed.", orderID)
	default:
		return "You have a new notification."
	}
}

// ── Worker loop ───────────────────────────────────────────────────────────────

// processQueue blocks on BRPOP from the given queue and processes one job.
// Returns false when context is cancelled.
func processQueue(ctx context.Context, sg *SendGridClient, queue string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// BRPOP blocks up to 2s so we can check ctx cancellation promptly.
		result, err := rdb.BRPop(ctx, 2*time.Second, queue).Result()
		if err == redis.Nil {
			continue // timeout, loop again
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[%s] brpop error: %v", queue, err)
			time.Sleep(time.Second)
			continue
		}

		// result[0] = queue name, result[1] = payload
		var job EmailJob
		if err := json.Unmarshal([]byte(result[1]), &job); err != nil {
			log.Printf("[%s] unmarshal error: %v", queue, err)
			continue
		}

		processJob(ctx, sg, job)
	}
}

func processJob(ctx context.Context, sg *SendGridClient, job EmailJob) {
	log.Printf("[worker] processing email_id=%s category=%s template=%s",
		job.EmailID, job.Category, job.TemplateType)

	body := renderTemplate(job)

	if err := sg.Send(job, body); err != nil {
		log.Printf("[worker] sendgrid error for %s: %v", job.EmailID, err)
		markFailed(ctx, job.EmailID, err.Error())
		logDeliveryEvent(ctx, job.EmailID, "FAILED", map[string]any{"error": err.Error()})
		return
	}

	markSent(ctx, job.EmailID)
	logDeliveryEvent(ctx, job.EmailID, "SENT", map[string]any{
		"recipient": job.RecipientAddress,
	})
	log.Printf("[worker] sent email_id=%s to=%s", job.EmailID, job.RecipientAddress)
}

// ── Scheduled email sweeper ───────────────────────────────────────────────────
// Polls for PENDING emails whose scheduled_at has passed and enqueues them.

func runScheduler(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepScheduled(ctx)
		}
	}
}

func sweepScheduled(ctx context.Context) {
	rows, err := db.QueryContext(ctx, `
		UPDATE emails
		SET status = 'QUEUED'
		WHERE status = 'PENDING'
		  AND scheduled_at IS NOT NULL
		  AND scheduled_at <= NOW()
		RETURNING id, tenant_id, recipient_user_id, recipient_address,
		          category, template_type, template_attributes, locale`)
	if err != nil {
		log.Printf("[scheduler] sweep error: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var job EmailJob
		var attrsJSON string
		if err := rows.Scan(
			&job.EmailID, &job.TenantID, &job.RecipientUserID,
			&job.RecipientAddress, &job.Category, &job.TemplateType,
			&attrsJSON, &job.Locale,
		); err != nil {
			log.Printf("[scheduler] scan error: %v", err)
			continue
		}
		json.Unmarshal([]byte(attrsJSON), &job.TemplateAttributes)

		payload, _ := json.Marshal(job)
		queue := queueTransactional
		if job.Category == "PROMOTIONAL" {
			queue = queuePromotional
		}
		if err := rdb.LPush(ctx, queue, payload).Err(); err != nil {
			log.Printf("[scheduler] enqueue error for %s: %v", job.EmailID, err)
		} else {
			logDeliveryEvent(ctx, job.EmailID, "QUEUED", nil)
			log.Printf("[scheduler] enqueued scheduled email_id=%s", job.EmailID)
		}
	}
}

// ── DB helpers ────────────────────────────────────────────────────────────────

func markSent(ctx context.Context, emailID string) {
	db.ExecContext(ctx,
		`UPDATE emails SET status='SENT', sent_at=NOW() WHERE id=$1`, emailID)
}

func markFailed(ctx context.Context, emailID, reason string) {
	db.ExecContext(ctx,
		`UPDATE emails SET status='FAILED', failure_reason=$1 WHERE id=$2`,
		reason, emailID)
}

func logDeliveryEvent(ctx context.Context, emailID, eventType string, meta map[string]any) {
	metaJSON, _ := json.Marshal(meta)
	db.ExecContext(ctx, `
		INSERT INTO delivery_events (email_id, event_type, metadata)
		VALUES ($1, $2, $3)`, emailID, eventType, string(metaJSON))
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/emaildb?sslmode=disable"
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}
	sgAPIKey := os.Getenv("SENDGRID_API_KEY")

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

	sg := &SendGridClient{APIKey: sgAPIKey}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// KEY NFR: transactional and promotional run on separate goroutines
	// so promotional volume never blocks transactional delivery.
	log.Println("Worker started — transactional + promotional consumers running")

	go processQueue(ctx, sg, queueTransactional) // high-priority
	go processQueue(ctx, sg, queuePromotional)   // lower-priority, isolated
	go runScheduler(ctx)                          // scheduled email sweeper

	<-ctx.Done()
	log.Println("Worker shutting down...")
	time.Sleep(time.Second) // let in-flight jobs finish
}
