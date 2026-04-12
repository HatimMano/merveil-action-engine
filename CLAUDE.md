# merveil-action-engine — Rules Engine

## Overview
Python 3.12 + **Cloud Run Job** (runs once and terminates — not an HTTP server).
3 Cloud Schedulers : **4H et daily ENABLED** (prod), weekly PAUSED.
Reads pending actions (Breezeway) + rule tables (digest) produced by dbt.

## Execution Flow
```
dbt run (merveil-dbt-schedule : 06:00, 09:00, 12:00, 15:00, 18:00, 21:00 UTC)
    ↓  [30 min plus tard]
action-engine (merveil-action-engine-4h : 06:30, 09:30, 12:30, 15:30, 18:30, 21:30 UTC)
    Phase 1 — Breezeway
      1. resolve_completed_triggers (Breezeway webhook task-completed)
      2. Lit pending_actions → crée les tâches Breezeway
    Phase 2 — Digest (si FREQ défini)
      1. resolve_digest_triggers(FREQ) — purge les triggers email_digest expirés (TTL)
      2. Lit rule_{FREQ} depuis BigQuery
      3. Filtre les nouvelles alertes (non déjà dans action_triggers)
      4. Envoie 1 digest email (EmailDigestHandler.execute_batch)
      5. Logue heartbeat + alertes dans action_triggers
```

## TTL — Purge automatique des triggers digest
`resolve_digest_triggers(freq)` s'exécute au début de chaque phase 2.
Résout tous les triggers `destination='email_digest'` + `status='open'` expirés :

| FREQ | TTL |
|---|---|
| `4h` | 4 heures |
| `daily` | 24 heures |
| `weekly` | 168 heures |
| `monthly` | 720 heures |

Sans cette purge, un trigger `open` permanent bloquerait les ré-alertes sur le même `property_id`.

## BigQuery Datasets
| Dataset | Tables | Role |
|---|---|---|
| `action_engine` | `pending_actions`, `rule_*`, `action_triggers` | Action-engine state and history |
| `dashboard_alerts` | `dash_alerts` | Alerts computed by dbt (source for the email digest) |

## Adding a Rule — 3 Steps
1. **Create the SQL** in `dbt/models/rules/rule_<name>.sql`
   - Required schema: `rule_name, property_id, context (JSON), detected_at`
   - `property_id` = deduplication key (e.g. reservation_id, CURRENT_DATE...)
2. **Add it** to `dbt/models/rules/pending_actions.sql` (UNION ALL)
3. **Declare destinations** in `config/rules.yaml`:
```yaml
rules:
  my_new_rule:
    enabled: true
    frequency: daily        # 4h | daily | weekly | monthly | (absent = toujours exécuté)
    destinations:
      - type: breezeway_task
        params:
          name_template: "Task — {apartment_name}"
          department: housekeeping
          priority: normal
      - type: email_digest
        params:
          to: "alertes@archides.fr"   # destinataire par règle (override GMAIL_TO)
```

## Frequency Filtering
The runner reads `FREQ` env var at startup. If set, only rules with a matching `frequency` are executed (rules without `frequency` always run). If `FREQ` is unset, all enabled rules run.

| FREQ value | Triggered by |
|---|---|
| `4h` | Cloud Scheduler every 4 hours |
| `daily` | Cloud Scheduler daily at 07:00 |
| `weekly` | Cloud Scheduler every Monday at 08:00 |
| `monthly` | Cloud Scheduler 1st of month at 08:00 |
| (not set) | Manual execution — runs all rules |

### Cloud Scheduler setup
```bash
# 4H — aligné sur dbt (30 min après chaque run dbt)
gcloud scheduler jobs create http merveil-action-engine-4h \
  --schedule="30 6,9,12,15,18,21 * * *" \
  --uri="https://run.googleapis.com/v2/projects/merveil-data-warehouse/locations/europe-west1/jobs/merveil-action-engine:run" \
  --message-body='{"overrides":{"containerOverrides":[{"env":[{"name":"FREQ","value":"4h"}]}]}}' \
  --oauth-service-account-email="action-engine-sa@merveil-data-warehouse.iam.gserviceaccount.com" \
  --location=europe-west1

# Daily
gcloud scheduler jobs create http merveil-action-engine-daily \
  --schedule="0 7 * * *" \
  --uri="https://run.googleapis.com/v2/projects/merveil-data-warehouse/locations/europe-west1/jobs/merveil-action-engine:run" \
  --message-body='{"overrides":{"containerOverrides":[{"env":[{"name":"FREQ","value":"daily"}]}]}}' \
  --oauth-service-account-email="action-engine-sa@merveil-data-warehouse.iam.gserviceaccount.com" \
  --location=europe-west1

# Weekly (lundi 08:00)
gcloud scheduler jobs create http merveil-action-engine-weekly \
  --schedule="0 8 * * 1" \
  --uri="https://run.googleapis.com/v2/projects/merveil-data-warehouse/locations/europe-west1/jobs/merveil-action-engine:run" \
  --message-body='{"overrides":{"containerOverrides":[{"env":[{"name":"FREQ","value":"weekly"}]}]}}' \
  --oauth-service-account-email="action-engine-sa@merveil-data-warehouse.iam.gserviceaccount.com" \
  --location=europe-west1
```

## Adding a Destination
1. Create `src/handlers/<destination>.py` with `class XxxHandler` and method `execute(action, params) -> str`
2. Add one line in `HANDLER_REGISTRY` in `src/core/runner.py`

## Existing Handlers
| Handler | Type | Description |
|---|---|---|
| `breezeway_task` | `BreezewayTasksHandler` | Creates Breezeway tasks (cleaning, inspection, logistics) |
| `email_digest` | `EmailDigestHandler` | Sends the daily alert digest via Gmail API (DWD) |

## Email Digest — Architecture Gmail API + DWD
**Sender**: `noreply@archides.fr` (dedicated Workspace account, never connected as a human)
**Recipient**: configurable via `GMAIL_TO` (currently `alerte_ventes@archides.fr` for testing — change to `alertes@archides.fr` for prod)
**Auth**: Domain-Wide Delegation — the SA `alerts-gmail-sender` impersonates `noreply@archides.fr`

Required secret in Secret Manager: `alerts-gmail-sa-key` (SA JSON key)
Env vars (defined in `deploy.sh`):
```
GMAIL_SENDER=noreply@archides.fr
GMAIL_TO=alerte_ventes@archides.fr   ← change to alertes@archides.fr for prod
```

The SA `alerts-gmail-sender@merveil-data-warehouse.iam.gserviceaccount.com` must have access to `alerts-gmail-sa-key` in Secret Manager, and the Cloud Run Job must mount this secret.

## Deduplication
`rule_daily_alert_digest` emits `property_id = CURRENT_DATE` → max 1 email per day.
All rules: if an `open` trigger already exists for `(rule_name, property_id)` in
`action_engine.action_triggers`, the action is skipped.
If no alerts in `dash_alerts`, the handler raises `SkipAction` (not logged as an error).

The email digest triggers are never auto-resolved (no associated Breezeway task) —
this is expected, as the property_id changes each day so the cooldown doesn't block subsequent runs.

## Streaming buffer — BigQuery limitation
Inserts into `action_triggers` are done via `insert_rows_json` (streaming).
Rows inserted via streaming cannot be UPDATE/DELETE for ~90 min.
In production this is not a problem. For testing, to force a re-trigger:
```bash
# Wait 90 min OR use an INSERT INTO ... SELECT query instead of UPDATE
bq query --use_legacy_sql=false "UPDATE \`...\` SET status='resolved' WHERE ..."
```

## Debug — Cloud Run Job Logs
```bash
gcloud run jobs executions list --job merveil-action-engine --region europe-west1 --project merveil-data-warehouse
gcloud beta run jobs executions logs read <execution-name> --region europe-west1 --project merveil-data-warehouse
```

## Deploy
```bash
bash deploy.sh
```

## IAM — Prérequis
`action-engine-sa` doit avoir les rôles suivants :
- `roles/bigquery.dataEditor` — lire/écrire action_triggers
- `roles/bigquery.jobUser` — exécuter les queries BQ
- `roles/secretmanager.secretAccessor` — lire alerts-gmail-sa-key
- `roles/run.invoker` — **requis pour que Cloud Scheduler puisse déclencher le job**

```bash
gcloud projects add-iam-policy-binding merveil-data-warehouse \
  --member="serviceAccount:action-engine-sa@merveil-data-warehouse.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```

## Triggering manuel (gcloud run jobs execute --update-env-vars est cassé)
Utiliser l'API REST directement :
```bash
curl -s -X POST \
  "https://run.googleapis.com/v2/projects/merveil-data-warehouse/locations/europe-west1/jobs/merveil-action-engine:run" \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  -d '{"overrides":{"containerOverrides":[{"env":[{"name":"FREQ","value":"4h"}]}]}}'
```

## Known Errors
| Error | Cause | Fix |
|---|---|---|
| `pending_actions` stale date | dbt image not rebuilt after modifying `dbt_project.yml` | Rebuild dbt image: `cd DWH/dbt && bash redeploy.sh` |
| `pending_actions` empty | dbt rules produced no actions | Verify dbt ran and rules are enabled in `rules.yaml` |
| Duplicate action skipped | Trigger already `open` for the same entity | Normal — cooldown active |
| `KeyError` in a handler | Missing field in `pending_actions` | Check the SQL of the corresponding dbt model |
| Gmail API 401 | DWD not enabled or wrong Client ID | Check admin.google.com → Domain delegation |
| UPDATE streaming buffer error | Row too recent in action_triggers | Wait ~90 min |
| Scheduler PERMISSION_DENIED (code 7) | `action-engine-sa` manque `roles/run.invoker` | Voir section IAM ci-dessus |
