package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// ── Prometheus metrics ───────────────────────────────────────────────────────

var (
	webhookTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_received_total",
		Help: "Webhook events received from Resend",
	}, []string{"event_type"})

	webhookDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhook_process_duration_seconds",
		Help:    "Time to process a webhook event",
		Buckets: prometheus.DefBuckets,
	})

	webhookVerifyFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "webhook_verification_failed_total",
		Help: "Webhook signature verification failures",
	})
)

func init() {
	prometheus.MustRegister(webhookTotal, webhookDuration, webhookVerifyFailed)
}

// ── DB + Redis globals ──────────────────────────────────────────────────────

var (
	db  *sql.DB
	rdb *redis.Client
)

// ── Resend webhook types ────────────────────────────────────────────────────

type ResendWebhookEvent struct {
	Type string         `json:"type"`
	Data ResendEventData `json:"data"`
}

type ResendEventData struct {
	EmailID   string   `json:"email_id"`
	To        []string `json:"to"`
	CreatedAt string   `json:"created_at"`
}

// ── Analytics event (pushed to queue:analytics) ─────────────────────────────

type AnalyticsEvent struct {
	EmailID    string `json:"email_id"`
	CampaignID string `json:"campaign_id"` // may be empty for non-campaign emails
	EventType  string `json:"event_type"`  // DELIVERED, OPENED, CLICKED, BOUNCED, UNSUBSCRIBED
	Timestamp  string `json:"timestamp"`   // ISO 8601
}

// ── Event type mapping ──────────────────────────────────────────────────────

var resendEventMap = map[string]string{
	"email.delivered":     "DELIVERED",
	"email.opened":        "OPENED",
	"email.clicked":       "CLICKED",
	"email.bounced":       "BOUNCED",
	"email.unsubscribed":  "UNSUBSCRIBED",
}

const queueAnalytics = "queue:analytics"

// ── Handlers ────────────────────────────────────────────────────────────────

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func handleResendWebhook(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		webhookDuration.Observe(time.Since(start).Seconds())
	}()

	// Read body for signature verification and parsing
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	// HMAC-SHA256 signature verification
	webhookSecret := os.Getenv("RESEND_WEBHOOK_SECRET")
	if webhookSecret != "" {
		sig := r.Header.Get("svix-signature")
		if sig == "" {
			webhookVerifyFailed.Inc()
			http.Error(w, "missing signature", http.StatusUnauthorized)
			return
		}
		msgID := r.Header.Get("svix-id")
		timestamp := r.Header.Get("svix-timestamp")
		signedContent := msgID + "." + timestamp + "." + string(bodyBytes)
		mac := hmac.New(sha256.New, []byte(webhookSecret))
		mac.Write([]byte(signedContent))
		expectedSig := hex.EncodeToString(mac.Sum(nil))

		// svix-signature is "v1,<base64sig>" — check each comma-separated entry
		valid := false
		for _, part := range strings.Split(sig, " ") {
			if strings.TrimPrefix(part, "v1,") == expectedSig {
				valid = true
				break
			}
		}
		if !valid {
			webhookVerifyFailed.Inc()
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
	}

	var event ResendWebhookEvent
	if err := json.Unmarshal(bodyBytes, &event); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	internalType, supported := resendEventMap[event.Type]
	if !supported {
		// Unknown event type — acknowledge so Resend doesn't retry.
		log.Printf("[webhook] ignoring unsupported event type: %s", event.Type)
		w.WriteHeader(http.StatusOK)
		return
	}

	webhookTotal.WithLabelValues(internalType).Inc()

	ctx := r.Context()

	// Look up our internal email record by Resend's email ID.
	var emailID string
	var campaignID sql.NullString
	var tenantID string
	err = db.QueryRowContext(ctx,
		`SELECT id, campaign_id, tenant_id FROM emails WHERE resend_id = $1`,
		event.Data.EmailID,
	).Scan(&emailID, &campaignID, &tenantID)
	if err == sql.ErrNoRows {
		// Not found — likely a dry-run email or one we don't track.
		log.Printf("[webhook] no email found for resend_id=%s (type=%s), skipping",
			event.Data.EmailID, event.Type)
		w.WriteHeader(http.StatusOK)
		return
	}
	if err != nil {
		log.Printf("[webhook] db lookup error for resend_id=%s: %v", event.Data.EmailID, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// INSERT into delivery_events.
	meta := map[string]any{
		"resend_event_type": event.Type,
		"resend_email_id":   event.Data.EmailID,
	}
	metaJSON, _ := json.Marshal(meta)
	_, err = db.ExecContext(ctx, `
		INSERT INTO delivery_events (email_id, event_type, metadata)
		VALUES ($1, $2, $3)`, emailID, internalType, string(metaJSON))
	if err != nil {
		log.Printf("[webhook] delivery_events insert error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// LPUSH analytics event to Redis.
	campID := ""
	if campaignID.Valid {
		campID = campaignID.String
	}
	analyticsEvt := AnalyticsEvent{
		EmailID:    emailID,
		CampaignID: campID,
		EventType:  internalType,
		Timestamp:  event.Data.CreatedAt,
	}
	analyticsJSON, _ := json.Marshal(analyticsEvt)
	if err := rdb.LPush(ctx, queueAnalytics, analyticsJSON).Err(); err != nil {
		log.Printf("[webhook] redis lpush error: %v", err)
		// Non-fatal: we already persisted the delivery event.
	}

	// If unsubscribed, record in unsubscribes table.
	if internalType == "UNSUBSCRIBED" && len(event.Data.To) > 0 {
		recipientEmail := event.Data.To[0]
		_, err := db.ExecContext(ctx, `
			INSERT INTO unsubscribes (email, tenant_id, reason)
			VALUES ($1, $2, 'webhook_unsubscribe')
			ON CONFLICT DO NOTHING`, recipientEmail, tenantID)
		if err != nil {
			log.Printf("[webhook] unsubscribes insert error: %v", err)
			// Non-fatal: acknowledge the webhook anyway.
		} else {
			log.Printf("[webhook] recorded unsubscribe for %s (tenant=%s)", recipientEmail, tenantID)
		}
	}

	log.Printf("[webhook] processed %s for email_id=%s (resend_id=%s)",
		internalType, emailID, event.Data.EmailID)
	w.WriteHeader(http.StatusOK)
}

// ── main ────────────────────────────────────────────────────────────────────

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
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)
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

	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhooks/resend", handleResendWebhook)
	mux.HandleFunc("GET /health", handleHealth)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Prometheus metrics server
	go func() {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server on :9091")
		http.ListenAndServe(":9091", metricsMux)
	}()

	go func() {
		log.Printf("Webhook service listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Webhook service shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("Webhook service stopped")
}
