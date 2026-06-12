package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// ── Prometheus metrics ───────────────────────────────────────────────────────

var (
	workerSendTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "worker_email_send_total",
		Help: "Emails processed by worker",
	}, []string{"category", "status"}) // status: sent, failed, dry_run, unsubscribed

	workerSendDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "worker_email_send_duration_seconds",
		Help:    "Time to send email via Resend API",
		Buckets: prometheus.DefBuckets,
	}, []string{"category"})

	workerUnsubSkip = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_unsubscribe_skip_total",
		Help: "Emails skipped due to unsubscribe list",
	})
)

func init() {
	prometheus.MustRegister(workerSendTotal, workerSendDuration, workerUnsubSkip)
}

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

// ── Resend client ─────────────────────────────────────────────────────────────

type ResendClient struct {
	APIKey string
	DryRun bool
	http   *http.Client
}

func (r *ResendClient) Send(job EmailJob, body string) (string, error) {
	if r.DryRun {
		return "dry-run-" + job.EmailID, nil
	}

	subject := "Notification"
	switch job.TemplateType {
	case "LOGIN_MSG":
		subject = "Your login code"
	case "WELCOME":
		subject = "Welcome!"
	case "PROMO_OFFER":
		subject = "Special offer"
	case "ORDER_CONFIRM":
		subject = "Order confirmation"
	}

	reqBody, _ := json.Marshal(map[string]any{
		"from":    "noreply@mercor.com",
		"to":      []string{job.RecipientAddress},
		"subject": subject,
		"html":    body,
	})

	req, err := http.NewRequest("POST", "https://api.resend.com/emails", bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+r.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("resend request: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("resend returned %d: %s", resp.StatusCode, string(respBytes))
	}

	var result struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return "", fmt.Errorf("parse response: %w", err)
	}
	return result.ID, nil
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
func processQueue(ctx context.Context, resend *ResendClient, queue string) {
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

		processJob(ctx, resend, job)
	}
}

func processJob(ctx context.Context, resend *ResendClient, job EmailJob) {
	log.Printf("[worker] processing email_id=%s category=%s template=%s",
		job.EmailID, job.Category, job.TemplateType)

	// Check unsubscribe list before sending (tenant-scoped)
	var exists int
	err := db.QueryRowContext(ctx,
		`SELECT 1 FROM unsubscribes WHERE email = $1 AND tenant_id = $2 LIMIT 1`,
		job.RecipientAddress, job.TenantID).Scan(&exists)
	if err == nil {
		// recipient is unsubscribed — skip send
		markFailed(ctx, job.EmailID, "recipient unsubscribed")
		logDeliveryEvent(ctx, job.EmailID, "FAILED", map[string]any{"reason": "unsubscribed"})
		workerUnsubSkip.Inc()
		workerSendTotal.WithLabelValues(job.Category, "unsubscribed").Inc()
		log.Printf("[worker] skipped email_id=%s — recipient %s is unsubscribed",
			job.EmailID, job.RecipientAddress)
		return
	}

	body := renderTemplate(job)

	sendStart := time.Now()
	resendID, err := resend.Send(job, body)
	sendDuration := time.Since(sendStart).Seconds()
	workerSendDuration.WithLabelValues(job.Category).Observe(sendDuration)

	if err != nil {
		log.Printf("[worker] resend error for %s: %v", job.EmailID, err)
		markFailed(ctx, job.EmailID, err.Error())
		logDeliveryEvent(ctx, job.EmailID, "FAILED", map[string]any{"error": err.Error()})
		workerSendTotal.WithLabelValues(job.Category, "failed").Inc()
		return
	}

	markSent(ctx, job.EmailID, resendID)
	logDeliveryEvent(ctx, job.EmailID, "SENT", map[string]any{
		"recipient": job.RecipientAddress,
		"resend_id": resendID,
	})
	if resend.DryRun {
		workerSendTotal.WithLabelValues(job.Category, "dry_run").Inc()
		log.Printf("[worker] dry-run email_id=%s to=%s resend_id=%s",
			job.EmailID, job.RecipientAddress, resendID)
	} else {
		workerSendTotal.WithLabelValues(job.Category, "sent").Inc()
		log.Printf("[worker] sent email_id=%s to=%s resend_id=%s",
			job.EmailID, job.RecipientAddress, resendID)
	}
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

func markSent(ctx context.Context, emailID, resendID string) {
	db.ExecContext(ctx,
		`UPDATE emails SET status='SENT', sent_at=NOW(), resend_id=$1 WHERE id=$2`,
		resendID, emailID)
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
	resendAPIKey := os.Getenv("RESEND_API_KEY")
	dryRun, _ := strconv.ParseBool(os.Getenv("DRY_RUN"))

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

	resend := &ResendClient{
		APIKey: resendAPIKey,
		DryRun: dryRun,
		http:   &http.Client{Timeout: 10 * time.Second},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// KEY NFR: transactional and promotional run on separate goroutines
	// so promotional volume never blocks transactional delivery.
	log.Println("Worker started — transactional + promotional consumers running")

	// Prometheus metrics server
	go func() {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server on :9091")
		http.ListenAndServe(":9091", metricsMux)
	}()

	go processQueue(ctx, resend, queueTransactional) // high-priority
	go processQueue(ctx, resend, queuePromotional)   // lower-priority, isolated
	go runScheduler(ctx)                          // scheduled email sweeper

	<-ctx.Done()
	log.Println("Worker shutting down...")
	time.Sleep(time.Second) // let in-flight jobs finish
}
