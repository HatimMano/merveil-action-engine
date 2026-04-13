#!/bin/bash
# deploy.sh — Build & déploie merveil-action-engine sur Cloud Run Jobs
# Deux jobs : merveil-action-engine (FREQ=4h) + merveil-action-engine-daily (FREQ=daily)
# Usage: ./deploy.sh

set -e

PROJECT="merveil-data-warehouse"
REGION="europe-west1"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/merveil-docker/merveil-action-engine:latest"
SA="action-engine-sa@${PROJECT}.iam.gserviceaccount.com"

echo "🔨 Build de l'image..."
gcloud builds submit \
  --tag "$IMAGE" \
  --region "$REGION" \
  --project "$PROJECT"

COMMON_ARGS="--image $IMAGE \
  --region $REGION \
  --memory 512Mi \
  --cpu 1 \
  --task-timeout 300 \
  --max-retries 1 \
  --set-secrets BREEZEWAY_CLIENT_ID=breezeway-client-id:latest,BREEZEWAY_CLIENT_SECRET=breezeway-client-secret:latest \
  --service-account $SA \
  --project $PROJECT"

echo "🚀 Déploiement du job 4h..."
gcloud run jobs deploy merveil-action-engine \
  $COMMON_ARGS \
  --set-env-vars GCP_PROJECT_ID="$PROJECT",GMAIL_SENDER="noreply@archides.fr",GMAIL_TO="alerte_ventes@archides.fr",FREQ="4h"

echo "🚀 Déploiement du job daily..."
gcloud run jobs deploy merveil-action-engine-daily \
  $COMMON_ARGS \
  --set-env-vars GCP_PROJECT_ID="$PROJECT",GMAIL_SENDER="noreply@archides.fr",GMAIL_TO="alerte_ventes@archides.fr",FREQ="daily"

echo ""
echo "✅ Jobs déployés : merveil-action-engine (4h) + merveil-action-engine-daily"
