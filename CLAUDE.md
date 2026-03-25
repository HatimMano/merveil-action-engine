# merveil-action-engine — Rules Engine

## Overview
Python 3.12 + **Cloud Run Job** (runs once and terminates — not an HTTP server).
Triggered by Cloud Scheduler **after each dbt run** (6x/day).
Reads pending actions produced by dbt and executes them against external systems.

## Execution Flow
```
dbt run
    ↓
dbt models/rules/ → BigQuery sync_logs.pending_actions
    ↓
action-engine (Cloud Run Job)
    1. Resolve completed triggers (e.g., task-completed in Breezeway)
    2. Read pending_actions
    3. For each action: check no duplicate → route to handler → log in action_triggers
```

## Adding a Rule — 3 Steps
1. **Create the SQL** in `dbt/models/rules/rule_<name>.sql` (produces rows in pending_actions)
2. **Add it** to `dbt/models/rules/pending_actions.sql` (UNION ALL)
3. **Declare destinations** in `config/rules.yaml`:
```yaml
rules:
  my_new_rule:
    enabled: true
    destinations:
      - handler: breezeway_tasks   # filename in src/handlers/
        params:
          task_type: "cleaning"
```

## Adding a Destination
1. Create `src/handlers/<destination>.py`
2. Import it in `src/core/runner.py`

## Existing Handlers
- `breezeway_tasks`: creates Breezeway tasks (cleaning, inspection, maintenance)
- `email_digest`: sends daily alert digest via SMTP to alertes@archides.fr

## Email Digest — Setup requis (Cloud Run Job env vars)
```
SMTP_FROM=noreply@archides.fr    # compte Gmail expéditeur
SMTP_USER=noreply@archides.fr
SMTP_PASSWORD=<app_password>     # depuis Secret Manager : smtp-app-password
SMTP_TO=alertes@archides.fr      # Google Group destinataire (défaut)
```
Créer le secret : `gcloud secrets create smtp-app-password --data-file=-`
Monter dans Cloud Run Job : `--set-secrets SMTP_PASSWORD=smtp-app-password:latest`

## Déduplication email
`rule_daily_alert_digest` émet `property_id = CURRENT_DATE`. Le runner skip si un trigger
`open` existe déjà pour ce (rule_name, property_id) → 1 email max par jour.
Si aucune alerte dans `dash_alerts`, le handler lève `SkipAction` (non loggé comme erreur).

## In Progress / Backlog
- Customer.io (automated emails) — handler to create

## Debug — Cloud Run Job Logs
```bash
# List recent executions
gcloud run jobs executions list --job action-engine --region europe-west1

# Logs for a specific execution
gcloud run jobs executions describe <execution-name> --region europe-west1
gcloud run jobs executions logs <execution-name> --region europe-west1
```

## Local Environment Variables
```bash
export GCP_PROJECT_ID=merveil-data-warehouse
# Authentication: gcloud auth application-default login
```

## Known Errors
| Error | Cause | Fix |
|---|---|---|
| `pending_actions` empty | dbt rules produced no actions | Verify dbt ran and rules are enabled in `rules.yaml` |
| Duplicate action | Trigger already `open` for the same entity | Normal — cooldown prevents re-triggering (wait for resolution) |
| `KeyError` in a handler | Missing field in `pending_actions` | Check the SQL of the corresponding dbt model |

## Deploy
```bash
bash deploy.sh
# or
gcloud builds submit --config cloudbuild.yaml
```
