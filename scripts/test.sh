#!/usr/bin/env bash
set -euo pipefail

API="http://localhost:8083"
WEBHOOK="http://localhost:8084"
TENANT_ID="00000000-0000-0000-0000-000000000002"

echo "=== Email Marketing System — Smoke Test ==="

# 1. Health check
echo "[1/6] Waiting for API..."
for i in $(seq 1 30); do
    if curl -sf "$API/delivery-stats" > /dev/null 2>&1; then break; fi
    sleep 1
done

# 2. Send a transactional email
echo "[2/6] Sending transactional email..."
RESP=$(curl -sf -X POST "$API/send-email" \
    -H "Content-Type: application/json" \
    -d "{
        \"tenant_id\": \"$TENANT_ID\",
        \"user_id\": \"test-user-1\",
        \"category\": \"TRANSACTIONAL\",
        \"template_type\": \"LOGIN_MSG\",
        \"template_attributes\": {\"code\": \"123456\"}
    }")
EMAIL_ID=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['email_id'])")
echo "  email_id=$EMAIL_ID"

# 3. Send a promotional email
echo "[3/6] Sending promotional email..."
curl -sf -X POST "$API/send-email" \
    -H "Content-Type: application/json" \
    -d "{
        \"tenant_id\": \"$TENANT_ID\",
        \"user_id\": \"test-user-2\",
        \"category\": \"PROMOTIONAL\",
        \"template_type\": \"PROMO_OFFER\",
        \"template_attributes\": {\"offer\": \"50% off\"}
    }" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  email_id={d[\"email_id\"]} status={d[\"status\"]}')"

# 4. Wait for workers to process
echo "[4/6] Waiting for workers to process (3s)..."
sleep 3

# 5. Check delivery stats
echo "[5/6] Checking delivery stats..."
curl -sf "$API/delivery-stats?tenant_id=$TENANT_ID" | python3 -m json.tool

# 6. Send a mock webhook event (simulate Resend callback)
echo "[6/6] Sending mock webhook (email.delivered)..."
curl -sf -X POST "$WEBHOOK/webhooks/resend" \
    -H "Content-Type: application/json" \
    -d "{
        \"type\": \"email.delivered\",
        \"data\": {
            \"email_id\": \"dry-run-$EMAIL_ID\",
            \"to\": [\"test-user-1@example.com\"],
            \"created_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
        }
    }" && echo "  Webhook accepted"

echo ""
echo "=== Smoke test complete ==="
