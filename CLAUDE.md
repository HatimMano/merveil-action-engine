# merveil-action-engine — Rules Engine

## Overview
Python 3.12 + **Cloud Run Job** (runs once and terminates — not an HTTP server).
**Pas de scheduler configuré** — se déclenche manuellement ou à intégrer après le dbt scheduler.
Reads pending actions produced by dbt and executes them against external systems.

## Execution Flow
```
dbt run (scheduler merveil-dbt-schedule, 6x/jour)
    ↓
dbt models/rules/ → BigQuery action_engine.pending_actions
    ↓
action-engine (Cloud Run Job) — déclencher manuellement ou via scheduler à créer
    1. Resolve completed triggers (webhook task-completed Breezeway)
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
**Expéditeur** : `noreply@archides.fr` (compte Workspace dédié, jamais connecté en humain)
**Destinataire** : configurable via `GMAIL_TO` (actuellement `hatim@archides.fr` pour test — changer en `alertes@archides.fr` pour prod)
**Auth** : Domain-Wide Delegation — le SA `alerts-gmail-sender` usurpe `noreply@archides.fr`

Secret requis dans Secret Manager : `alerts-gmail-sa-key` (clé JSON du SA)
Env vars (définis dans `deploy.sh`) :
```
GMAIL_SENDER=noreply@archides.fr
GMAIL_TO=hatim@archides.fr   ← changer en alertes@archides.fr pour prod
```

Le SA `alerts-gmail-sender@merveil-data-warehouse.iam.gserviceaccount.com` doit avoir accès à `alerts-gmail-sa-key` dans Secret Manager, et le Cloud Run Job doit monter ce secret.

## Déduplication
`rule_daily_alert_digest` émet `property_id = CURRENT_DATE` → 1 email max par jour.
Toutes les règles : si un trigger `open` existe déjà pour `(rule_name, property_id)` dans
`action_engine.action_triggers`, l'action est skippée.
Si aucune alerte dans `dash_alerts`, le handler lève `SkipAction` (non loggé comme erreur).

Les triggers email digest ne sont jamais auto-résolus (pas de Breezeway task associée) —
c'est normal, le property_id change chaque jour donc le cooldown ne bloque pas les runs suivants.

## Streaming buffer — limitation BigQuery
Les inserts dans `action_triggers` se font via `insert_rows_json` (streaming).
Les rows insérées en streaming ne peuvent pas être UPDATE/DELETE pendant ~90 min.
En production ce n'est pas un problème. En test, pour forcer un re-déclenchement :
```bash
# Attendre 90 min OU utiliser une requête INSERT INTO ... SELECT au lieu de UPDATE
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
| `pending_actions` stale date | dbt image pas rebuildée après modif `dbt_project.yml` | Rebuild image dbt : `cd DWH/dbt && bash redeploy.sh` |
| `pending_actions` empty | dbt rules produced no actions | Verify dbt ran and rules are enabled in `rules.yaml` |
| Duplicate action skipped | Trigger already `open` for the same entity | Normal — cooldown actif |
| `KeyError` in a handler | Missing field in `pending_actions` | Check the SQL of the corresponding dbt model |
| Gmail API 401 | DWD non activé ou mauvais Client ID | Vérifier admin.google.com → Délégation domaine |
| UPDATE streaming buffer error | Row trop récente dans action_triggers | Attendre ~90 min |
