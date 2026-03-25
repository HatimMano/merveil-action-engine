# merveil-action-engine — Rules Engine

## Overview
Python 3.12 + **Cloud Run Job** (runs once and terminates — not an HTTP server).
Triggered by Cloud Scheduler **after each dbt run** (6x/day).
Reads pending actions produced by dbt and executes them against external systems.

## Execution Flow
```
dbt run
    ↓
dbt models/rules/ → BigQuery action_engine.pending_actions
    ↓
action-engine (Cloud Run Job)
    1. Resolve completed triggers (e.g., task-completed in Breezeway)
    2. Read pending_actions
    3. For each action: check no duplicate → route to handler → log in action_triggers
```

## BigQuery Datasets
| Dataset | Tables | Rôle |
|---|---|---|
| `action_engine` | `pending_actions`, `rule_*`, `action_triggers` | État et historique de l'action-engine |
| `dashboard_alerts` | `dash_alerts` | Alertes calculées par dbt (source du digest email) |

## Adding a Rule — 3 Steps
1. **Create the SQL** in `dbt/models/rules/rule_<name>.sql`
   - Schema obligatoire : `rule_name, property_id, context (JSON), detected_at`
   - `property_id` = clé de déduplication (ex: reservation_id, CURRENT_DATE...)
2. **Add it** to `dbt/models/rules/pending_actions.sql` (UNION ALL)
3. **Declare destinations** in `config/rules.yaml`:
```yaml
rules:
  my_new_rule:
    enabled: true
    destinations:
      - type: breezeway_task
        params:
          name_template: "Tâche — {apartment_name}"
          department: housekeeping
          priority: normal
```

## Adding a Destination
1. Create `src/handlers/<destination>.py` with `class XxxHandler` et méthode `execute(action, params) -> str`
2. Add one line in `HANDLER_REGISTRY` dans `src/core/runner.py`

## Existing Handlers
| Handler | Type | Description |
|---|---|---|
| `breezeway_task` | `BreezewayTasksHandler` | Crée des tasks Breezeway (ménage, inspection, logistique) |
| `email_digest` | `EmailDigestHandler` | Envoie le digest quotidien d'alertes via Gmail API (DWD) |

## Email Digest — Architecture Gmail API + DWD
**Expéditeur** : `noreply@merveil.fr` (compte Workspace dédié, jamais connecté en humain)
**Destinataire** : `alertes@archides.fr` (Google Group)
**Auth** : Domain-Wide Delegation — le SA `alerts-gmail-sender` usurpe `noreply@merveil.fr`

Secret requis dans Secret Manager : `alerts-gmail-sa-key` (clé JSON du SA)
Env vars optionnels (défauts suffisants) :
```
GMAIL_SENDER=noreply@merveil.fr
GMAIL_TO=alertes@archides.fr
```

Le SA `alerts-gmail-sender@merveil-data-warehouse.iam.gserviceaccount.com` doit avoir accès à `alerts-gmail-sa-key` dans Secret Manager, et le Cloud Run Job doit monter ce secret.

## Déduplication
`rule_daily_alert_digest` émet `property_id = CURRENT_DATE` → 1 email max par jour.
Toutes les règles : si un trigger `open` existe déjà pour `(rule_name, property_id)` dans
`action_engine.action_triggers`, l'action est skippée.
Si aucune alerte dans `dash_alerts`, le handler lève `SkipAction` (non loggé comme erreur).

## Debug — Cloud Run Job Logs
```bash
gcloud run jobs executions list --job action-engine --region europe-west1
gcloud run jobs executions logs <execution-name> --region europe-west1
```

## Deploy
```bash
bash deploy.sh
```

## Known Errors
| Error | Cause | Fix |
|---|---|---|
| `pending_actions` empty | dbt rules produced no actions | Verify dbt ran and rules are enabled in `rules.yaml` |
| Duplicate action skipped | Trigger already `open` for the same entity | Normal — cooldown actif |
| `KeyError` in a handler | Missing field in `pending_actions` | Check the SQL of the corresponding dbt model |
| Gmail API 401 | DWD non activé ou mauvais Client ID | Vérifier admin.google.com → Délégation domaine |
