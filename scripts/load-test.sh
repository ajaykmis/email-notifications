#!/usr/bin/env bash
set -euo pipefail

API="http://localhost:8083"
TENANT_ID="00000000-0000-0000-0000-000000000002"
COUNT=${1:-1000}
CONCURRENCY=${2:-10}
CATEGORY=${3:-PROMOTIONAL}

echo "=== Load Test: $COUNT emails, concurrency=$CONCURRENCY ==="
echo "Category: $CATEGORY"
echo "Make sure DRY_RUN=true in docker-compose!"
echo ""

START=$(date +%s)

send_email() {
    local i=$1
    curl -sf -X POST "$API/send-email" \
        -H "Content-Type: application/json" \
        -d "{
            \"tenant_id\": \"$TENANT_ID\",
            \"user_id\": \"loadtest-user-$i\",
            \"category\": \"$CATEGORY\",
            \"template_type\": \"PROMO_OFFER\",
            \"template_attributes\": {\"offer\": \"test-$i\"}
        }" > /dev/null 2>&1
}

export -f send_email
export API TENANT_ID

# Use xargs for parallel sends
seq 1 "$COUNT" | xargs -P "$CONCURRENCY" -I {} bash -c "send_email {}"

END=$(date +%s)
ELAPSED=$((END - START))
RPS=$((COUNT / (ELAPSED > 0 ? ELAPSED : 1)))

echo ""
echo "=== Results ==="
echo "Sent: $COUNT emails in ${ELAPSED}s (~${RPS} req/s)"
echo ""
echo "Queue depths:"
redis-cli -p 6380 LLEN queue:transactional 2>/dev/null || echo "  (connect to redis on port 6380 to check)"
redis-cli -p 6380 LLEN queue:promotional 2>/dev/null || echo "  (connect to redis on port 6380 to check)"
echo ""
echo "Check Grafana at http://localhost:3000 for real-time metrics"
echo "Check delivery stats: curl $API/delivery-stats"
