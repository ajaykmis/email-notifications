// Campaign worker: dequeues CampaignJobs and fans out individual EmailJobs
// to queue:promotional by STREAMING through a user-id file line by line.
//
// The file is newline-delimited user_ids uploaded to /uploads (shared volume).
// In production this would be an S3 key read via GetObject streaming.
//
// At 1M users:
//   - marketing service writes /uploads/campaign-xyz.txt (1M lines)
//   - POST /campaigns with file_path → 1 tiny job on queue:campaigns
//   - campaign worker opens file, scans line by line, batches every 1000 lines
//   - each batch → INSERT 1000 emails + LPUSH 1000 to queue:promotional
//   - promotional workers drain the queue and deliver via SendGrid
//   - peak memory: one 1000-line batch in RAM, never the full 1M
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

var (
	db  *sql.DB
	rdb *redis.Client
)

// ── Shared types ──────────────────────────────────────────────────────────────

type CampaignJob struct {
	CampaignID         string         `json:"campaign_id"`
	TenantID           string         `json:"tenant_id"`
	TemplateType       string         `json:"template_type"`
	TemplateAttributes map[string]any `json:"template_attributes"`
	Locale             string         `json:"locale"`
	FilePath           string         `json:"file_path"` // /uploads/campaign-xxx.txt
}

type EmailJob struct {
	EmailID            string         `json:"email_id"`
	CampaignID         string         `json:"campaign_id"`
	TenantID           string         `json:"tenant_id"`
	RecipientUserID    string         `json:"recipient_user_id"`
	RecipientAddress   string         `json:"recipient_address"`
	Category           string         `json:"category"`
	TemplateType       string         `json:"template_type"`
	TemplateAttributes map[string]any `json:"template_attributes"`
	Locale             string         `json:"locale"`
}

const (
	queueCampaigns   = "queue:campaigns"
	queuePromotional = "queue:promotional"
	fanOutBatchSize  = 1000
)

// ── Users service stub ────────────────────────────────────────────────────────

func resolveEmail(userID string) string {
	return userID + "@example.com"
}

// ── File-streaming fan-out ────────────────────────────────────────────────────

func fanOut(ctx context.Context, job CampaignJob) {
	log.Printf("[campaign] opening file %s campaign_id=%s", job.FilePath, job.CampaignID)

	f, err := os.Open(job.FilePath)
	if err != nil {
		log.Printf("[campaign] cannot open file %s: %v", job.FilePath, err)
		db.ExecContext(ctx, `UPDATE campaigns SET status='FAILED' WHERE id=$1`, job.CampaignID)
		return
	}
	defer f.Close()

	attrsJSON, _ := json.Marshal(job.TemplateAttributes)

	var (
		batch        []string
		totalLines   int
		totalQueued  int
		totalFailed  int
	)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		q, f := fanOutBatch(ctx, job, batch, string(attrsJSON))
		totalQueued += q
		totalFailed += f

		// Atomically update counters after every batch flush.
		db.ExecContext(ctx, `
			UPDATE campaigns
			SET total_recipients = total_recipients + $1,
			    queued_count     = queued_count     + $2,
			    failed_count     = failed_count     + $3
			WHERE id = $4`,
			len(batch), q, f, job.CampaignID)

		log.Printf("[campaign] flushed batch lines=%d queued=%d failed=%d total_so_far=%d",
			len(batch), q, f, totalLines)

		batch = batch[:0] // reset slice, keep capacity
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if ctx.Err() != nil {
			log.Printf("[campaign] context cancelled at line %d", totalLines)
			break
		}

		uid := strings.TrimSpace(scanner.Text())
		if uid == "" || strings.HasPrefix(uid, "#") {
			continue // skip blank lines and comments
		}

		batch = append(batch, uid)
		totalLines++

		if len(batch) >= fanOutBatchSize {
			flush()
		}
	}
	flush() // final partial batch

	if err := scanner.Err(); err != nil {
		log.Printf("[campaign] scanner error: %v", err)
	}

	db.ExecContext(ctx, `
		UPDATE campaigns SET status='DONE', completed_at=NOW() WHERE id=$1`,
		job.CampaignID)

	log.Printf("[campaign] done campaign_id=%s lines=%d queued=%d failed=%d",
		job.CampaignID, totalLines, totalQueued, totalFailed)
}

// fanOutBatch inserts a batch of email rows and pushes jobs to Redis.
func fanOutBatch(
	ctx context.Context,
	job CampaignJob,
	userIDs []string,
	attrsJSON string,
) (queued, failed int) {

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("[campaign] begin tx: %v", err)
		return 0, len(userIDs)
	}
	defer tx.Rollback()

	var redisPayloads []any

	for _, uid := range userIDs {
		addr := resolveEmail(uid)

		var emailID string
		err := tx.QueryRowContext(ctx, `
			INSERT INTO emails
				(tenant_id, recipient_user_id, recipient_address, category,
				 template_type, template_attributes, locale, status, campaign_id)
			VALUES ($1,$2,$3,'PROMOTIONAL',$4,$5,$6,'QUEUED',$7)
			RETURNING id`,
			job.TenantID, uid, addr,
			job.TemplateType, attrsJSON, job.Locale, job.CampaignID,
		).Scan(&emailID)
		if err != nil {
			log.Printf("[campaign] insert error uid=%s: %v", uid, err)
			failed++
			continue
		}

		ej := EmailJob{
			EmailID:            emailID,
			CampaignID:         job.CampaignID,
			TenantID:           job.TenantID,
			RecipientUserID:    uid,
			RecipientAddress:   addr,
			Category:           "PROMOTIONAL",
			TemplateType:       job.TemplateType,
			TemplateAttributes: job.TemplateAttributes,
			Locale:             job.Locale,
		}
		payload, _ := json.Marshal(ej)
		redisPayloads = append(redisPayloads, payload)
		queued++
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[campaign] commit: %v", err)
		return 0, len(userIDs)
	}

	if len(redisPayloads) > 0 {
		if err := rdb.LPush(ctx, queuePromotional, redisPayloads...).Err(); err != nil {
			log.Printf("[campaign] redis pipeline error: %v", err)
			// emails are in DB as QUEUED; a reconciliation sweep can re-enqueue them
		}
	}

	return queued, failed
}

// ── Campaign consumer loop ────────────────────────────────────────────────────

func runCampaignConsumer(ctx context.Context) {
	log.Println("[campaign] consumer started — waiting on queue:campaigns")
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		result, err := rdb.BRPop(ctx, 2*time.Second, queueCampaigns).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[campaign] brpop error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var job CampaignJob
		if err := json.Unmarshal([]byte(result[1]), &job); err != nil {
			log.Printf("[campaign] unmarshal error: %v", err)
			continue
		}

		fanOut(ctx, job)
	}
}

// ── Scheduled campaign sweeper ────────────────────────────────────────────────
// Every 30s: find PENDING campaigns whose scheduled_at has passed, push them
// onto queue:campaigns so the consumer picks them up and starts fan-out.

func runCampaignSweeper(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepScheduledCampaigns(ctx)
		}
	}
}

func sweepScheduledCampaigns(ctx context.Context) {
	// Atomically claim all due campaigns in one UPDATE ... RETURNING.
	// Only rows that transition PENDING→RUNNING are returned — no double-dispatch.
	rows, err := db.QueryContext(ctx, `
		UPDATE campaigns
		SET status = 'RUNNING'
		WHERE status       = 'PENDING'
		  AND scheduled_at IS NOT NULL
		  AND scheduled_at <= NOW()
		RETURNING id, tenant_id, template_type, template_attributes, locale, file_path`)
	if err != nil {
		log.Printf("[sweeper] query error: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var job CampaignJob
		var attrsJSON string
		if err := rows.Scan(
			&job.CampaignID, &job.TenantID, &job.TemplateType,
			&attrsJSON, &job.Locale, &job.FilePath,
		); err != nil {
			log.Printf("[sweeper] scan error: %v", err)
			continue
		}
		json.Unmarshal([]byte(attrsJSON), &job.TemplateAttributes)

		payload, _ := json.Marshal(job)
		if err := rdb.LPush(ctx, queueCampaigns, payload).Err(); err != nil {
			log.Printf("[sweeper] enqueue error campaign_id=%s: %v", job.CampaignID, err)
			db.ExecContext(ctx, `UPDATE campaigns SET status='PENDING' WHERE id=$1`, job.CampaignID)
			continue
		}
		log.Printf("[sweeper] enqueued scheduled campaign_id=%s", job.CampaignID)
	}
}

// ── Sent-count updater ────────────────────────────────────────────────────────

func runSentCountUpdater(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			db.ExecContext(ctx, `
				UPDATE campaigns c
				SET sent_count = (
					SELECT COUNT(*) FROM emails e
					WHERE e.campaign_id = c.id AND e.status = 'SENT'
				),
				failed_count = (
					SELECT COUNT(*) FROM emails e
					WHERE e.campaign_id = c.id AND e.status = 'FAILED'
				)
				WHERE c.status IN ('RUNNING','DONE')`)
		}
	}
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go runSentCountUpdater(ctx)
	go runCampaignSweeper(ctx)
	runCampaignConsumer(ctx)

	log.Println("[campaign] worker shut down")
}
