#!/bin/bash
# deploy.sh — Build & déploie merveil-action-engine sur Cloud Run Jobs
# Deux jobs : merveil-action-engine (FREQ=4h) + merveil-action-engine-daily (FREQ=daily)
# Usage: ./deploy.sh

set -e

# ⚠️ Build depuis le dossier DU SERVICE, jamais le CWD courant : `gcloud builds submit`
# sans source utilise le CWD → lancé depuis un autre repo (ex. dbt), il buildait l'image
# de CE repo et l'écrasait sur l'action-engine (incident 2026-07-06 : jobs iseo/2h/daily
# se sont mis à exécuter dbt). Le cd garantit le bon contexte de build.
cd "$(dirname "$0")"

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

echo "🚀 Déploiement du job 2h (alertes serrures ISEO)..."
gcloud run jobs deploy merveil-action-engine-2h \
  $COMMON_ARGS \
  --set-env-vars GCP_PROJECT_ID="$PROJECT",GMAIL_SENDER="noreply@archides.fr",GMAIL_TO="alerte_ventes@archides.fr",FREQ="2h"

echo "🚀 Déploiement du job cancellations-brief (11h Paris)..."
# CANCELLATIONS_TO = 2 destinataires (virgule) → délimiteur ^;^ pour que gcloud
# ne splitte pas sur la virgule. Pas ^@^ (les valeurs contiennent des @ d'emails).
gcloud run jobs deploy merveil-action-engine-cancellations-brief \
  $COMMON_ARGS \
  --set-env-vars "^;^GCP_PROJECT_ID=$PROJECT;GMAIL_SENDER=noreply@archides.fr;CANCELLATIONS_TO=alerte_ventes@archides.fr,emilia@archides.fr;FREQ=cancellations_brief"

echo "🚀 Déploiement du job iseo-orchestrator (pipeline V3 100% DWH — natif coupé)..."
# Notes :
#   - Pipeline V3 : provision J-7 (device + invitation + push Duve) / archive au checkout.
#   - ISEO_SHADOW_MODE=false → live. Mettre =true pour un dry-run (log "would provision").
#   - DUVE_CONNECT_TOKEN (secret) + DUVE_CONNECT_PID (intégration entrante Duve dédiée DWH).
#   - Guest tag : 1 user Sofia DÉDIÉ par résa, au vrai nom du guest (firstname/lastname),
#     créé AVEC un password aléatoire (sinon enabled=False → GC / perd son tag → PIN cassé).
#     Son tag 'user' auto-créé = guestTagId du PIN → l'UI Luckey affiche le vrai nom. Le
#     user est supprimé à l'archive (cf. iseo_orchestrator.py). Plus de tag générique
#     partagé (ancien ISEO_DEFAULT_GUEST_TAG_ID : 184106 system KO, 184181 GC'd).
#   - ⭐ WHITELIST pilotée par le SEED dbt `iseo_whitelisted_apartments` (source unique,
#     lue au run par _load_whitelist + par les 2 modèles dbt). Élargir = 1 ligne dans le
#     seed CSV, sans redeploy. L'ISEO_ALLOWED_PROPERTY_IDS ci-dessous n'est plus qu'un
#     FALLBACK (si le seed est vide/inaccessible). Liste complète des 13 apparts (ref) :
#       c12a7244-f97b-4633-b6a7-b16f0079821c,1068206f-58c2-4ff8-8d71-b16f0079821c,
#       22785cb3-555b-4020-92d6-b16f0079821c,ef51211e-3550-456a-9410-b16f0079821c,
#       aa37778e-7257-40ad-9b5c-b16f0079821c,70edbca0-6abb-4bae-bd89-b16f0079821c,
#       e8474d43-8f8f-4b87-9e20-b16f0079821c,ed0d0ccd-d5a0-4cbf-9f6f-b1d20103b89f,
#       847cac7d-4030-4c3d-84fa-b1d201078a1f,3cc98d6e-294c-43df-848b-b16f0079821c,
#       fb06038d-3d4a-4910-b7f7-b16f0079821c,f88ab4e0-16ed-4f5c-965f-b16f0079821c,
#       db56b3ca-1462-46ec-aaee-b16f0079821c
gcloud run jobs deploy merveil-action-engine-iseo \
  --image $IMAGE \
  --region $REGION \
  --memory 512Mi --cpu 1 --task-timeout 300 --max-retries 1 \
  --set-secrets BREEZEWAY_CLIENT_ID=breezeway-client-id:latest,BREEZEWAY_CLIENT_SECRET=breezeway-client-secret:latest,ISEO_MANAGER_USERNAME=iseo-manager-username:latest,ISEO_MANAGER_PASSWORD=iseo-manager-password:latest,DUVE_CONNECT_TOKEN=duve-connect-token:latest \
  --service-account $SA \
  --project $PROJECT \
  --set-env-vars "^;^GCP_PROJECT_ID=$PROJECT;FREQ=iseo_orchestrator;ISEO_SHADOW_MODE=false;DUVE_CONNECT_PID=6a357cbd2e45c374a9a9fd18;GMAIL_SENDER=noreply@archides.fr;ISEO_ALERT_TO=hatim@archides.fr;ISEO_ALLOWED_PROPERTY_IDS=e8474d43-8f8f-4b87-9e20-b16f0079821c,847cac7d-4030-4c3d-84fa-b1d201078a1f,ed0d0ccd-d5a0-4cbf-9f6f-b1d20103b89f,70edbca0-6abb-4bae-bd89-b16f0079821c,aa37778e-7257-40ad-9b5c-b16f0079821c,3cc98d6e-294c-43df-848b-b16f0079821c,880c9419-8e25-4740-b8c3-b1c200b95203"

echo ""
echo "✅ Jobs déployés : 4h + daily + 2h (serrures) + cancellations-brief (11h) + iseo (J-7)"
