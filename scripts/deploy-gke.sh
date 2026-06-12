#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID=${GCP_PROJECT_ID:-""}
REGION=${GCP_REGION:-"us-central1"}
CLUSTER=${GKE_CLUSTER:-"email-marketing"}
TAG=${IMAGE_TAG:-"latest"}

if [ -z "$PROJECT_ID" ]; then
    echo "Error: Set GCP_PROJECT_ID environment variable"
    exit 1
fi

REGISTRY="gcr.io/$PROJECT_ID"

echo "=== Deploying Email Marketing System to GKE ==="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Cluster: $CLUSTER"
echo "Tag: $TAG"
echo ""

# 1. Build and push images
SERVICES=(api worker campaign webhook analytics)
for svc in "${SERVICES[@]}"; do
    echo "[build] $svc..."
    docker build -t "$REGISTRY/email-notifications-$svc:$TAG" -f "Dockerfile.$svc" .
    docker push "$REGISTRY/email-notifications-$svc:$TAG"
done

# 2. Get cluster credentials
echo "[gke] Getting cluster credentials..."
gcloud container clusters get-credentials "$CLUSTER" --region "$REGION" --project "$PROJECT_ID"

# 3. Update image tags in manifests
echo "[k8s] Updating image tags..."
find k8s/ -name '*.yaml' -exec sed -i.bak "s|gcr.io/PROJECT_ID|$REGISTRY|g" {} \;
find k8s/ -name '*.yaml' -exec sed -i.bak "s|:latest|:$TAG|g" {} \;
find k8s/ -name '*.bak' -delete

# 4. Apply manifests
echo "[k8s] Applying manifests..."
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/redis.yaml

echo "[k8s] Waiting for postgres and redis..."
kubectl -n email-marketing wait --for=condition=ready pod -l component=postgres --timeout=120s
kubectl -n email-marketing wait --for=condition=ready pod -l component=redis --timeout=120s

kubectl apply -f k8s/deployments/
kubectl apply -f k8s/prometheus/
kubectl apply -f k8s/ingress.yaml

# 5. Wait for rollout
echo "[k8s] Waiting for deployments..."
for deploy in api worker-transactional worker-promotional campaign-worker webhook analytics-worker; do
    kubectl -n email-marketing rollout status deployment/$deploy --timeout=120s 2>/dev/null || true
done

echo ""
echo "=== Deployment complete ==="
echo "Get ingress IP: kubectl -n email-marketing get ingress"
echo "Port-forward Grafana: kubectl -n email-marketing port-forward svc/grafana 3000:3000"
