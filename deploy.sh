#!/bin/bash
# deploy.sh — Build & déploie merveil-action-engine sur Cloud Run Jobs
# Usage: ./deploy.sh

set -e

PROJECT="merveil-data-warehouse"
REGION="europe-west1"
JOB="merveil-action-engine"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/merveil-docker/${JOB}:latest"
SA="action-engine-sa@${PROJECT}.iam.gserviceaccount.com"

echo "🔨 Build de l'image..."
gcloud builds submit \
  --tag "$IMAGE" \
  --region "$REGION" \
  --project "$PROJECT"

echo "🚀 Déploiement du Cloud Run Job..."
gcloud run jobs deploy "$JOB" \
  --image "$IMAGE" \
  --region "$REGION" \
  --memory 512Mi \
  --cpu 1 \
  --task-timeout 300 \
  --max-retries 1 \
  --set-env-vars GCP_PROJECT_ID="$PROJECT",GMAIL_SENDER="noreply@archides.fr",GMAIL_TO="alerte_ventes@archides.fr" \
  --set-secrets BREEZEWAY_CLIENT_ID=breezeway-client-id:latest,BREEZEWAY_CLIENT_SECRET=breezeway-client-secret:latest \
  --service-account "$SA" \
  --project "$PROJECT"

echo ""
echo "✅ Job déployé : $JOB"
echo ""
echo "Pour exécuter manuellement :"
echo "  gcloud run jobs execute $JOB --region $REGION --project $PROJECT"
