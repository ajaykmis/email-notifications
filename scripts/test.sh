#!/usr/bin/env bash
set -e
BASE="http://localhost:8080"

echo "=== Send TRANSACTIONAL email (LOGIN_MSG) ==="
curl -s -X POST "$BASE/send-email" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "00000000-0000-0000-0000-000000000001",
    "user_id": "user-42",
    "category": "TRANSACTIONAL",
    "template_type": "LOGIN_MSG",
    "template_attributes": {"code": "987654"},
    "locale": "en"
  }' | jq .

echo ""
echo "=== Send PROMOTIONAL email ==="
curl -s -X POST "$BASE/send-email" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "00000000-0000-0000-0000-000000000002",
    "user_id": "user-99",
    "category": "PROMOTIONAL",
    "template_type": "PROMO_OFFER",
    "template_attributes": {"offer": "50% off this weekend"},
    "locale": "en"
  }' | jq .

echo ""
echo "=== Schedule a future email ==="
FUTURE=$(date -u -v+1H "+%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -d "+1 hour" "+%Y-%m-%dT%H:%M:%SZ")
curl -s -X POST "$BASE/schedule-email" \
  -H "Content-Type: application/json" \
  -d "{
    \"tenant_id\": \"00000000-0000-0000-0000-000000000001\",
    \"user_id\": \"user-77\",
    \"category\": \"TRANSACTIONAL\",
    \"template_type\": \"ORDER_CONFIRM\",
    \"template_attributes\": {\"order_id\": \"ORD-2026-001\"},
    \"locale\": \"en\",
    \"scheduled_at\": \"$FUTURE\"
  }" | jq .

echo ""
echo "=== Delivery stats ==="
curl -s "$BASE/delivery-stats" | jq .

echo ""
echo "=== Delivery stats for booking-service tenant ==="
curl -s "$BASE/delivery-stats?tenant_id=00000000-0000-0000-0000-000000000001" | jq .

echo ""
echo "=== Create a campaign for 10 users (simulating 1M) ==="
# Generate 10 user IDs to simulate a bulk send
USER_IDS=$(python3 -c "import json; print(json.dumps(['user-' + str(i) for i in range(1,11)]))")
CAMPAIGN=$(curl -s -X POST "$BASE/campaigns" \
  -H "Content-Type: application/json" \
  -d "{
    \"tenant_id\": \"00000000-0000-0000-0000-000000000002\",
    \"template_type\": \"PROMO_OFFER\",
    \"template_attributes\": {\"offer\": \"Weekend sale: 40% off\"},
    \"locale\": \"en\",
    \"user_ids\": $USER_IDS
  }")
echo "$CAMPAIGN" | jq .

CAMPAIGN_ID=$(echo "$CAMPAIGN" | jq -r '.campaign_id')

echo ""
echo "=== Poll campaign status (after 3s) ==="
sleep 3
curl -s "$BASE/campaigns/$CAMPAIGN_ID" | jq .

echo ""
echo "=== Validation: bad category ==="
curl -s -X POST "$BASE/send-email" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "00000000-0000-0000-0000-000000000001",
    "user_id": "user-1",
    "category": "SPAM",
    "template_type": "LOGIN_MSG",
    "template_attributes": {}
  }'
echo ""
