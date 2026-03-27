# merveil-action-engine — Rules Engine

## Overview
Python 3.12 + **Cloud Run Job** (runs once and terminates — not an HTTP server).
**No scheduler configured** — triggered manually or to be integrated after the dbt scheduler.
Reads pending actions produced by dbt and executes them against external systems.

## Execution Flow
```
dbt run (scheduler merveil-dbt-schedule, 6x/day)
    ↓
dbt models/rules/ → BigQuery action_engine.pending_actions
    ↓
action-engine (Cloud Run Job) — trigger manually or via scheduler to be created
    1. Resolve completed triggers (webhook task-completed Breezeway)
    2. Read pending_actions
    3. For each action: check no duplicate → route to handler → log in action_triggers
```

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
    destinations:
      - type: breezeway_task
        params:
          name_template: "Task — {apartment_name}"
          department: housekeeping
          priority: normal
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

## Known Errors
| Error | Cause | Fix |
|---|---|---|
| `pending_actions` stale date | dbt image not rebuilt after modifying `dbt_project.yml` | Rebuild dbt image: `cd DWH/dbt && bash redeploy.sh` |
| `pending_actions` empty | dbt rules produced no actions | Verify dbt ran and rules are enabled in `rules.yaml` |
| Duplicate action skipped | Trigger already `open` for the same entity | Normal — cooldown active |
| `KeyError` in a handler | Missing field in `pending_actions` | Check the SQL of the corresponding dbt model |
| Gmail API 401 | DWD not enabled or wrong Client ID | Check admin.google.com → Domain delegation |
| UPDATE streaming buffer error | Row too recent in action_triggers | Wait ~90 min |
