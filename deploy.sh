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

echo "🚀 Déploiement du job cancellations-brief (11h Paris)..."
gcloud run jobs deploy merveil-action-engine-cancellations-brief \
  $COMMON_ARGS \
  --set-env-vars GCP_PROJECT_ID="$PROJECT",GMAIL_SENDER="noreply@archides.fr",CANCELLATIONS_TO="alerte_ventes@archides.fr",FREQ="cancellations_brief"

echo "🚀 Déploiement du job iseo-orchestrator (recreate PINs Sofia à J-7)..."
# Notes :
#   - ISEO_SHADOW_MODE=true au début → log "would POST" sans appeler Sofia
#   - ISEO_ALLOWED_PROPERTY_IDS=c12a7244-… = whitelist P02-DAL40-1D
#   - Secrets ISEO ajoutés en plus des Breezeway (les jobs partagent COMMON_ARGS,
#     donc on override --set-secrets ici)
gcloud run jobs deploy merveil-action-engine-iseo \
  --image $IMAGE \
  --region $REGION \
  --memory 512Mi --cpu 1 --task-timeout 300 --max-retries 1 \
  --set-secrets BREEZEWAY_CLIENT_ID=breezeway-client-id:latest,BREEZEWAY_CLIENT_SECRET=breezeway-client-secret:latest,ISEO_MANAGER_USERNAME=iseo-manager-username:latest,ISEO_MANAGER_PASSWORD=iseo-manager-password:latest \
  --service-account $SA \
  --project $PROJECT \
  --set-env-vars GCP_PROJECT_ID="$PROJECT",FREQ="iseo_orchestrator",ISEO_SHADOW_MODE="true",ISEO_ALLOWED_PROPERTY_IDS="c12a7244-f97b-4633-b6a7-b16f0079821c"

echo ""
echo "✅ Jobs déployés : 4h + daily + cancellations-brief (11h) + iseo (J-7)"
