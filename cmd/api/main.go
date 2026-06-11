package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// ── DB + Redis globals ────────────────────────────────────────────────────────

var (
	db  *sql.DB
	rdb *redis.Client
)

// ── Request / Response types ──────────────────────────────────────────────────

type SendEmailRequest struct {
	TenantID           string         `json:"tenant_id"`
	UserID             string         `json:"user_id"`
	Category           string         `json:"category"`            // TRANSACTIONAL | PROMOTIONAL
	TemplateType       string         `json:"template_type"`       // e.g. LOGIN_MSG
	TemplateAttributes map[string]any `json:"template_attributes"` // {code: "123456"}
	Locale             string         `json:"locale"`              // default "en"
}

type ScheduleEmailRequest struct {
	SendEmailRequest
	ScheduledAt time.Time `json:"scheduled_at"`
}

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

// ── Redis queue names ─────────────────────────────────────────────────────────

const (
	queueTransactional = "queue:transactional"
	queuePromotional   = "queue:promotional"
)

func queueFor(category string) string {
	if category == "TRANSACTIONAL" {
		return queueTransactional
	}
	return queuePromotional
}

// ── Users service stub ────────────────────────────────────────────────────────
// In production this would be an RPC/HTTP call to the Users service.

type UserInfo struct {
	UserID string
	Email  string
}

func userExists(ctx context.Context, userID string) (UserInfo, bool) {
	// Stub: any non-empty userID is "valid"; derive a fake email.
	if userID == "" {
		return UserInfo{}, false
	}
	return UserInfo{UserID: userID, Email: userID + "@example.com"}, true
}

// ── Validation ────────────────────────────────────────────────────────────────

var validCategories = map[string]bool{
	"TRANSACTIONAL": true,
	"PROMOTIONAL":   true,
}

var validTemplates = map[string]bool{
	"LOGIN_MSG":    true,
	"WELCOME":      true,
	"PROMO_OFFER":  true,
	"ORDER_CONFIRM": true,
}

func validate(req SendEmailRequest) error {
	if req.TenantID == "" {
		return fmt.Errorf("tenant_id is required")
	}
	if req.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	if !validCategories[req.Category] {
		return fmt.Errorf("category must be TRANSACTIONAL or PROMOTIONAL")
	}
	if !validTemplates[req.TemplateType] {
		return fmt.Errorf("unknown template_type: %s", req.TemplateType)
	}
	return nil
}

// ── Handlers ──────────────────────────────────────────────────────────────────

func handleSendEmail(w http.ResponseWriter, r *http.Request) {
	var req SendEmailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Locale == "" {
		req.Locale = "en"
	}

	if err := validate(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// 1. Verify user exists
	userInfo, ok := userExists(ctx, req.UserID)
	if !ok {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	// 2. Persist email record
	attrsJSON, _ := json.Marshal(req.TemplateAttributes)
	var emailID string
	err := db.QueryRowContext(ctx, `
		INSERT INTO emails
			(tenant_id, recipient_user_id, recipient_address, category,
			 template_type, template_attributes, locale, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'PENDING')
		RETURNING id`,
		req.TenantID, req.UserID, userInfo.Email,
		req.Category, req.TemplateType, string(attrsJSON), req.Locale,
	).Scan(&emailID)
	if err != nil {
		log.Printf("db insert error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// 3. Enqueue into the correct Redis queue
	job := EmailJob{
		EmailID:            emailID,
		TenantID:           req.TenantID,
		RecipientUserID:    req.UserID,
		RecipientAddress:   userInfo.Email,
		Category:           req.Category,
		TemplateType:       req.TemplateType,
		TemplateAttributes: req.TemplateAttributes,
		Locale:             req.Locale,
	}
	jobBytes, _ := json.Marshal(job)
	if err := rdb.LPush(ctx, queueFor(req.Category), jobBytes).Err(); err != nil {
		log.Printf("redis enqueue error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// 4. Mark QUEUED
	db.ExecContext(ctx, `UPDATE emails SET status='QUEUED' WHERE id=$1`, emailID)
	logDeliveryEvent(ctx, emailID, "QUEUED", nil)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"email_id": emailID, "status": "QUEUED"})
}

func handleScheduleEmail(w http.ResponseWriter, r *http.Request) {
	var req ScheduleEmailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Locale == "" {
		req.Locale = "en"
	}
	if req.ScheduledAt.IsZero() {
		http.Error(w, "scheduled_at is required", http.StatusBadRequest)
		return
	}
	if err := validate(req.SendEmailRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	userInfo, ok := userExists(ctx, req.UserID)
	if !ok {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	attrsJSON, _ := json.Marshal(req.TemplateAttributes)
	var emailID string
	err := db.QueryRowContext(ctx, `
		INSERT INTO emails
			(tenant_id, recipient_user_id, recipient_address, category,
			 template_type, template_attributes, locale, status, scheduled_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'PENDING',$8)
		RETURNING id`,
		req.TenantID, req.UserID, userInfo.Email,
		req.Category, req.TemplateType, string(attrsJSON), req.Locale,
		req.ScheduledAt,
	).Scan(&emailID)
	if err != nil {
		log.Printf("db insert error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Scheduled emails are picked up by a separate cron/scheduler process.
	// For this prototype we just store them; a scheduler loop would enqueue
	// them when scheduled_at <= NOW().

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"email_id":     emailID,
		"status":       "PENDING",
		"scheduled_at": req.ScheduledAt.Format(time.RFC3339),
	})
}

func handleDeliveryStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantID := r.URL.Query().Get("tenant_id")

	query := `
		SELECT
			category,
			status,
			COUNT(*) AS count
		FROM emails
		WHERE ($1 = '' OR tenant_id::text = $1)
		GROUP BY category, status
		ORDER BY category, status`

	rows, err := db.QueryContext(ctx, query, tenantID)
	if err != nil {
		log.Printf("stats query error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type row struct {
		Category string `json:"category"`
		Status   string `json:"status"`
		Count    int    `json:"count"`
	}
	var results []row
	for rows.Next() {
		var r row
		rows.Scan(&r.Category, &r.Status, &r.Count)
		results = append(results, r)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// ── Campaign handlers ─────────────────────────────────────────────────────────

// CampaignRequest is what the marketing service POSTs.
// file_path points to a newline-delimited file of user_ids on the shared
// /uploads volume (in production this would be an S3 key). The campaign worker
// streams it line-by-line — never loads 1M IDs into memory.
type CampaignRequest struct {
	TenantID           string         `json:"tenant_id"`
	TemplateType       string         `json:"template_type"`
	TemplateAttributes map[string]any `json:"template_attributes"`
	Locale             string         `json:"locale"`
	FilePath           string         `json:"file_path"`   // e.g. /uploads/campaign-123.txt
	ScheduledAt        *time.Time     `json:"scheduled_at"` // nil = send immediately
}

type CampaignJob struct {
	CampaignID         string         `json:"campaign_id"`
	TenantID           string         `json:"tenant_id"`
	TemplateType       string         `json:"template_type"`
	TemplateAttributes map[string]any `json:"template_attributes"`
	Locale             string         `json:"locale"`
	FilePath           string         `json:"file_path"`
}

func handleCreateCampaign(w http.ResponseWriter, r *http.Request) {
	var req CampaignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Locale == "" {
		req.Locale = "en"
	}
	if req.TenantID == "" || req.TemplateType == "" || req.FilePath == "" {
		http.Error(w, "tenant_id, template_type, and file_path are required", http.StatusBadRequest)
		return
	}
	if !validTemplates[req.TemplateType] {
		http.Error(w, "unknown template_type: "+req.TemplateType, http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// 1. Persist campaign record.
	attrsJSON, _ := json.Marshal(req.TemplateAttributes)
	var campaignID string
	err := db.QueryRowContext(ctx, `
		INSERT INTO campaigns
			(tenant_id, template_type, template_attributes, locale, file_path, status, scheduled_at)
		VALUES ($1, $2, $3, $4, $5, 'PENDING', $6)
		RETURNING id`,
		req.TenantID, req.TemplateType, string(attrsJSON), req.Locale, req.FilePath, req.ScheduledAt,
	).Scan(&campaignID)
	if err != nil {
		log.Printf("campaign insert error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	status := "PENDING"

	// 2. If no scheduled_at, enqueue immediately. Otherwise the campaign sweeper
	//    will pick it up when scheduled_at <= NOW().
	if req.ScheduledAt == nil {
		job := CampaignJob{
			CampaignID:         campaignID,
			TenantID:           req.TenantID,
			TemplateType:       req.TemplateType,
			TemplateAttributes: req.TemplateAttributes,
			Locale:             req.Locale,
			FilePath:           req.FilePath,
		}
		payload, _ := json.Marshal(job)
		if err := rdb.LPush(ctx, "queue:campaigns", payload).Err(); err != nil {
			log.Printf("campaign enqueue error: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		db.ExecContext(ctx, `UPDATE campaigns SET status='RUNNING' WHERE id=$1`, campaignID)
		status = "RUNNING"
	}

	resp := map[string]any{
		"campaign_id": campaignID,
		"status":      status,
		"file_path":   req.FilePath,
	}
	if req.ScheduledAt != nil {
		resp["scheduled_at"] = req.ScheduledAt.Format(time.RFC3339)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(resp)
}

func handleGetCampaign(w http.ResponseWriter, r *http.Request) {
	campaignID := r.PathValue("id")
	ctx := r.Context()

	var (
		id              string
		status          string
		total, queued   int
		sent, failed    int
		templateType    string
		scheduledAt     sql.NullTime
	)
	err := db.QueryRowContext(ctx, `
		SELECT id, status, total_recipients, queued_count, sent_count, failed_count,
		       template_type, scheduled_at
		FROM campaigns WHERE id = $1`, campaignID,
	).Scan(&id, &status, &total, &queued, &sent, &failed, &templateType, &scheduledAt)
	if err == sql.ErrNoRows {
		http.Error(w, "campaign not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	var deliveryRate float64
	if total > 0 {
		deliveryRate = float64(sent) / float64(total) * 100
	}

	resp := map[string]any{
		"campaign_id":       id,
		"template_type":     templateType,
		"status":            status,
		"total_recipients":  total,
		"queued":            queued,
		"sent":              sent,
		"failed":            failed,
		"delivery_rate_pct": deliveryRate,
	}
	if scheduledAt.Valid {
		resp["scheduled_at"] = scheduledAt.Time.Format(time.RFC3339)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

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

	mux := http.NewServeMux()
	mux.HandleFunc("POST /send-email", handleSendEmail)
	mux.HandleFunc("POST /schedule-email", handleScheduleEmail)
	mux.HandleFunc("GET /delivery-stats", handleDeliveryStats)
	mux.HandleFunc("POST /campaigns", handleCreateCampaign)
	mux.HandleFunc("GET /campaigns/{id}", handleGetCampaign)

	addr := ":8080"
	log.Printf("Email Ingestion Service listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
