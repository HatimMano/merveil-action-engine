# merveil-action-engine — Rules Engine

## Overview
Python 3.12 + **Cloud Run Job** (runs once and terminates — not an HTTP server).
**4 Cloud Run Jobs distincts** (1 par fréquence, FREQ hardcodé dans chaque job) :
- `merveil-action-engine` → FREQ=4h (dispatcher trigger/action) — **scheduler PAUSED depuis 2026-05-30**
- `merveil-action-engine-daily` → FREQ=daily (dispatcher trigger/action) — englobe maintenant tous les triggers
- `merveil-action-engine-cancellations-brief` → FREQ=cancellations_brief (standalone, court-circuite le dispatcher pour envoyer un rapport mail annulations 24h à 11h Paris ; cf. `src/handlers/cancellations_brief.py`)
- `merveil-action-engine-iseo` → FREQ=iseo_orchestrator (standalone, pipeline V3 ISEO — recreate PINs Sofia à J-7 du CI avec MÊME deviceId capturé au pre-checkin done par webhook-gateway ; cf. `src/handlers/iseo_orchestrator.py`). Scheduler **`merveil-action-engine-iseo-2h` toutes les 2h à :45** Europe/Paris (`45 */2 * * *`) — passé de quotidien (7h30) à 2h le **2026-06-18** pour fermer le lockout near-CI + la latence d'annulation. Calé sur la cascade : ETL `:00` → dbt `:15` → dashboard `:30` → **orchestrateur `:45`** (fct_reservations frais, zéro chevauchement). Cf. ADR 2026-06-18.
- `merveil-action-engine-2h` → FREQ=2h (dispatcher, bucket `2h`) — **alertes serrures ISEO** (`iseo_pin_missing`, `iseo_etl_stale`, **`iseo_reconciliation`** depuis 2026-06-18 = écarts critical cache↔Sofia). Scheduler toutes les 2h à :00. Bucket `2h` TTL 4h (dédup). Destinataires : `digest_buckets.2h.default_recipients` dans `routing.yaml` (⚠ le flush n'utilise PAS `params.recipients` per-trigger). Cf. ADR 2026-06-13 + 2026-06-18.

5 Cloud Schedulers : **daily 7h + 2h serrures (:00) + iseo 2h (:45) + 11h cancellations ENABLED** (prod), 4H + weekly PAUSED.
Reads pending actions (Breezeway) + rule tables (digest) produced by dbt.

### Bascule 4h → daily (2026-05-30)
TTL du bucket 4h = 4h = fréquence du job → dédup neutralisée → ~60 alertes renvoyées à chaque cycle.
Fix : les 5 triggers anciennement bucket `4h` (cancellation_vip, satisfaction_low_review, last_minute_checkin, double_booking, checkin_no_apartment) ont été basculés en `bucket: daily` dans `config/routing.yaml`. Le scheduler `merveil-action-engine-4h` a été pausé. Pas de perte d'info : annulations VIP restent dans le mail RC 11h `cancellations-brief`.

### Fix explosion alertes daily — borne 24h sur `_load_triggers` (2026-06-17)
`action_engine.triggers` est append-only (dedup par jour). Le dispatcher faisait
`SELECT * FROM triggers` sans borne → rechargeait tout l'historique ; les triggers
anciens, dont le `dispatched_action` avait dépassé son TTL, étaient renvoyés à chaque
run (~1000 alertes). Fix : `_load_triggers` filtre `detected_at >= now - 24h`. Un trigger
persistant génère une ligne fraîche par jour donc reste capté ; borne ≤ TTL daily (24h)
→ pas de double envoi inter-jour. Uniforme pour tous les buckets (daily, 2h serrures, manuel).

### Mode `cancellations_brief` (2026-05-23)
Mail récap quotidien envoyé à 11h Paris à `alerte_ventes@archides.fr` + `emilia@archides.fr` (override via env `CANCELLATIONS_TO`). Ne passe **pas** par le dispatcher trigger/action — c'est un rapport, pas un événement déclencheur. Query directe sur `dashboard_ops.dash_ops_cancellations_recent` filtré sur `cancelled_at >= NOW() - 24h`. HTML minimaliste (KPIs + table compacte) avec bouton `Voir le détail dans le dashboard →` qui pointe vers `https://direction.archides.fr/ops-front?tab=cancellations&preset=24h`. Reuse infra Gmail API (secret `alerts-gmail-sa-key` + Domain-Wide Delegation existante).

### Mode `iseo_orchestrator` — Pipeline V3 100% DWH (refonte 2026-06-20, natif coupé)
Cf. [[project_iseo_integration_2026]] + `Archides/to_do_20_06.md`.

⚠️ **Refonte majeure 2026-06-20** : l'intégration native Duve↔Sofia est **coupée dans les 2 sens** (création Duve→Sofia + livraison Sofia→Duve). Le DWH est **seul maître** du cycle PIN. **Le cacher `webhook-gateway/iseo_pin_cacher.py` est retiré** (plus de `DUVE_PIN` à capturer). L'ancienne logique capture/recreate + PHANTOM/DRIFT est **supprimée**.

**Provision (J-7)** — `_resa_to_provision()` : résas Mews non annulées, payées, `checkin_date ∈ [today, today+7j]`, CO futur, **pas déjà couvertes par une row de cache active** (`archived_at IS NULL`). Pour chacune (`_provision`) :
- **A.** génère un code PIN **4 chiffres** unique account-wide (retry sur `already present`).
- **B.** `POST /api/v2/standardDevices` (extId `MERVEIL_RESA - <duve_id>`) — **réutilise le guest tag + lock tag de l'appart** (PAS de création user/tag par résa). Window = heures de politique appart (floor 7h / ceil 23h59), tz Paris.
- **C.** `POST /api/v2/invitations` (extId `MERVEIL_INV - <duve_id>`, `smartLockIds=[lock_id]`, `numberOfDevices:0`) → `code` → lien `https://archides.jago.cloud/remoteOpen?code=<code>` (⚠️ host `archides`, PAS le `api-archides` renvoyé par l'API). Lien gated sur la window (OK pendant le séjour).
- **D.** `POST` intégration entrante Duve (`DUVE_CONNECT_URL?pid=DUVE_CONNECT_PID`, secret `duve-connect-token`) : `primaryCode` (code clavier) + champ `merveil_paris_iseo_access_link_eIhhEnlspM` (lien). N'émet aucun message (messages auto Duve lisent le champ).
- **E.** INSERT état dans `iseo_raw.merveil_pin_cache` (colonnes `iseo_device_id`, `iseo_invitation_id`, `invitation_code/link`, `provisioned_at`, `duve_pushed_at`). Device + invitation sont **get-or-create par extId** → idempotent sur retry partiel.

**Résolution ids appart (JOIN BQ, pas de seed)** : `duve property_id (GUID) == Mews resource_id == nom du lock tag`. → `lock_id` + `lock_tag_id` via `stg_iseo__smart_locks` ; `guest_tag_id` = guest tag le plus fréquent des PINs existants de l'appart (`stg_iseo__standard_devices.rule_guest_tags_ids`). Si pas de guest tag (appart neuf) → skip (master code couvre).

**Archive** — `_resa_to_archive()` : rows actives où CO passé OU résa annulée (cross-check Mews). `_archive` : `DELETE /standardDevices/{id}` (par extId) + `DELETE /invitations/{id}`. **Pas de user à supprimer** (on réutilise le user d'appart partagé → pas de fuite quota).

**Mapping Duve↔Mews** : payload checkin Duve embarque `guestProfiles[isPrimary].externalId = customer_id` ; JOIN `fct_reservations` sur `customer_id + resource_id` (résa au CI le plus proche). Skip annulées au provision + archive.

**Mode shadow** (`ISEO_SHADOW_MODE=true`) : log "would provision" sans appel Sofia/Duve ni écriture d'état.

**Whitelist** (`ISEO_ALLOWED_PROPERTY_IDS=csv de GUID`) : **cutover = DAL40 seul** (`c12a7244…`). Liste des 13 apparts à ré-activer en commentaire dans `deploy.sh`.

**Secrets requis** : `ISEO_MANAGER_USERNAME` + `ISEO_MANAGER_PASSWORD` + `DUVE_CONNECT_TOKEN` (=`duve-connect-token`). Env : `DUVE_CONNECT_PID=6a357cbd2e45c374a9a9fd18`. SA `action-engine-sa` a `secretAccessor` project-wide.

**Test local** : `gcloud run jobs execute merveil-action-engine-iseo --region=europe-west1 --project=merveil-data-warehouse --wait`. Forcer une résa >7j : `--update-env-vars ISEO_LOOKAHEAD_DAYS=400`.

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
⚠️ Utiliser l'API v1 régionale (pas v2) — seule compatible avec l'auth OAuth du scheduler.
⚠️ Utiliser `scheduler-invoker@` comme SA OAuth (pas `action-engine-sa@`).
⚠️ `gcloud run jobs execute --update-env-vars` est cassé (bug gcloud) — FREQ est hardcodé dans chaque job.

```bash
# 4H — pointe sur merveil-action-engine (FREQ=4h dans le job)
gcloud scheduler jobs create http merveil-action-engine-4h \
  --schedule="30 6,9,12,15,18,21 * * *" \
  --uri="https://europe-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/merveil-data-warehouse/jobs/merveil-action-engine:run" \
  --message-body='{}' \
  --headers="Content-Type=application/json" \
  --oauth-service-account-email="scheduler-invoker@merveil-data-warehouse.iam.gserviceaccount.com" \
  --location=europe-west1

# Daily — pointe sur merveil-action-engine-daily (FREQ=daily dans le job)
gcloud scheduler jobs create http merveil-action-engine-daily \
  --schedule="0 7 * * *" \
  --uri="https://europe-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/merveil-data-warehouse/jobs/merveil-action-engine-daily:run" \
  --message-body='{}' \
  --headers="Content-Type=application/json" \
  --oauth-service-account-email="scheduler-invoker@merveil-data-warehouse.iam.gserviceaccount.com" \
  --location=europe-west1
```

## ⭐ Refonte Trigger / Action / Routing (2026-05-21)

**Statut** : terminé. Code déployé, prod sur le nouveau dispatcher depuis 2026-05-21. Legacy `ActionRunner` + `rules.yaml` + `action_logger.py` **supprimés** le 2026-05-21 (Phase E). Tables BQ legacy (`action_engine.rule_*`, `pending_actions`, `action_triggers`) conservées en archive — non actualisées par dbt — à droper manuellement plus tard si besoin.

**Concepts** :
- `trigger` (dbt → `action_engine.triggers`) : 1 ligne = 1 occurrence métier détectée. Schema unifié `(trigger_name, entity_type, entity_id, property_id, severity, category, message, action_url, context, detected_at, expires_at)`. 1 fichier `models/triggers/trigger_<name>.sql` par condition.
- `action` (Python handler) : réaction typée (asana_task, breezeway_task, email_digest). Un trigger peut déclencher N actions.
- `routing` (`config/routing.yaml`) : mapping `trigger_name → [actions]`. Déclaratif.
- `dispatched_actions` (BQ table append-only) : log par (trigger, property_id, action_type, status). Cooldown via `status='open'`.

**Flux exécution** (`src/core/dispatcher.py::TriggerDispatcher.run`) :
1. Résoudre les Breezeway tasks completed (webhook) → `dispatched_actions` status=resolved
2. Résoudre les digests expirés (TTL 4h/24h/168h selon bucket) → resolved
3. Charger triggers + dispatched_actions ouverts (1 query chacun)
4. Pour chaque trigger × routing.actions : skip si déjà open, sinon (a) buffer pour email_digest, (b) handler.execute pour asana/breezeway
5. Flush 1 mail par bucket avec les triggers bufferisés

**Feature flag** `USE_NEW_DISPATCHER` : supprimé en Phase E. Le `deploy.sh` ne le set plus dans les env vars Cloud Run (utilise `--set-env-vars` qui réinitialise). Rollback rapide impossible désormais — git revert + redeploy si régression.

**Inventaire triggers** (cf. `dbt/models/triggers/`) — 18 actifs :
| trigger_name | bucket / action | enabled | volume actuel |
|---|---|---|---|
| cancellation_vip | email_digest@4h | ✅ | ~37 |
| satisfaction_low_review | email_digest@4h | ✅ | ~3 |
| last_minute_checkin | email_digest@4h | ✅ | ~3 |
| double_booking | email_digest@4h | ✅ | 0 |
| checkin_no_apartment | email_digest@4h | ✅ | 0 |
| abandoned_cart | email_digest@daily | ✅ | 0 |
| revenue_anomaly_adr | email_digest@daily | ✅ | 0 |
| revenue_anomaly_adr_per_room | email_digest@daily | ✅ | 0 |
| revenue_apt_zero | email_digest@daily | ✅ | 0 |
| low_occupation | email_digest@daily | ✅ | 0 |
| high_cancellations_daily | email_digest@daily | ✅ | 0 |
| cancellation_large_apt | email_digest@daily | ✅ | ~10 |
| superhost_risk | email_digest@daily | ✅ | ~25 |
| dispo_daily_summary | email_digest@daily | ✅ | ~2 |
| gap_pricing_summary | email_digest@daily | ✅ | ~3 |
| champagne_direct | breezeway_task | ❌ disabled | ~5 |
| low_review_cleanliness | breezeway_task | ✅ (placeholder) | 0 |
| client_risk | email_digest@daily | ❌ disabled | ~18 |
| inspection_overdue | asana_task | ❌ disabled (POC) | ~41 |

**Ajouter un trigger** :
1. Créer `dbt/models/triggers/trigger_<name>.sql` (schema unifié, 1 ligne par occurrence)
2. Ajouter `UNION ALL SELECT * FROM {{ ref('trigger_<name>') }}` dans `dbt/models/triggers/triggers.sql`
3. Ajouter mapping dans `config/routing.yaml` (1 entrée sous `triggers:`)
4. `bash redeploy.sh` (dbt) + `bash deploy.sh` (action-engine) si routing.yaml modifié

**Backlog** :
- Externalisation `routing.yaml` → DWH Feed Sheets (cf. Archides/CLAUDE.md backlog)
- Drop manuel des tables BQ `action_engine.rule_*` + `pending_actions` + `action_triggers` quand l'archive ne sert plus

## Adding a Destination (handler)
1. Create `src/handlers/<destination>.py` with `class XxxHandler` and method `execute(action, params) -> str`
2. Add one line in `HANDLER_REGISTRY` in `src/core/dispatcher.py` (et `src/core/runner.py` pour le legacy si nécessaire)

## Existing Handlers
| Handler | Type | Description |
|---|---|---|
| `breezeway_task` | `BreezewayTasksHandler` | Creates Breezeway tasks (cleaning, inspection, logistics) |
| `email_digest` | `EmailDigestHandler` | Sends the daily alert digest via Gmail API (DWD) |
| `asana_task` | `AsanaTaskHandler` | **POC** — Creates Asana tasks (idempotence via `source_key:` in notes — workaround Asana free tier qui n'a pas de custom fields) |

### Asana — POC (mai 2026)
**État** : handler en place, rule `inspection_overdue` configurée `enabled: false` dans `rules.yaml`. Workspace MERVEIL `1199370302253551`, projet Test `1214996499081074`.

**Idempotence sans custom fields (Asana free tier)** : le `source_key = "{rule_name}:{property_id}"` est encodé dans les notes au format `source_key: rule:property`. Avant POST, le handler liste les tâches ouvertes du projet (1 GET caché par run) et skip si match. Cache mémoire par projet pour ne pas refaire 50 GET dans un même run.

**Test local** (sans déployer) :
```bash
cd merveil-action-engine
ASANA_PAT=<token> python -m utils.test_asana
```
Vérifie : (1) création de tâche, (2) idempotence (run 2 = SKIP), (3) listing.

**Pour activer en prod** :
1. Créer secret : `gcloud secrets create asana-pat --replication-policy=automatic` puis `echo -n "<PAT>" | gcloud secrets versions add asana-pat --data-file=-`
2. Grant accès au SA action-engine sur le secret
3. Monter dans `deploy.sh` : `--update-secrets=ASANA_PAT=asana-pat:latest`
4. Créer la SQL dbt `rule_inspection_overdue.sql` (insertion dans `action_engine.pending_actions`)
5. Mettre `enabled: true` dans `rules.yaml` + `assignee_email` correct
6. Redéployer

**Limites free tier à connaître** :
- Pas de custom fields → idempotence par parsing notes (fragile si quelqu'un édite les notes à la main et casse la ligne `source_key:`)
- Pas de webhooks "rules" Asana → si on veut une boucle de fermeture (Asana → DWH), faut polling périodique
- Asana Rules engine non disponible → toute automatisation côté DWH

## Email Digest — Architecture Gmail API + DWD
**Sender**: `noreply@archides.fr` (dedicated Workspace account, never connected as a human)
**Recipient**: `alerte_ventes@archides.fr, hatim@archides.fr` — défini dans `config/rules.yaml` sous `digest.4h.to` et `digest.daily.to`
**Auth**: Domain-Wide Delegation — the SA `alerts-gmail-sender` impersonates `noreply@archides.fr`

Required secret in Secret Manager: `alerts-gmail-sa-key` (SA JSON key)
Env vars (defined in `deploy.sh`):
```
GMAIL_SENDER=noreply@archides.fr
GMAIL_TO=alerte_ventes@archides.fr   ← fallback si rules.yaml ne définit pas `to`
```

Pour forcer l'envoi en test sur une adresse unique : `gcloud run jobs update merveil-action-engine-daily --update-env-vars=TEST_RECIPIENT=<email>` (et `--remove-env-vars=TEST_RECIPIENT` pour repasser en prod). NE PAS utiliser `--set-env-vars` qui remplace toutes les vars (FREQ saute).

The SA `alerts-gmail-sender@merveil-data-warehouse.iam.gserviceaccount.com` must have access to `alerts-gmail-sa-key` in Secret Manager, and the Cloud Run Job must mount this secret.

### Template HTML (`src/handlers/email_digest.py::_build_html_from_rows`)

**Structure** :
- Header indigo · summary bar (`X alertes — N critique(s) · M warning(s)`)
- Table 3 colonnes (Alerte / Date / Action) groupée par `alert_category` (sections gris clair)
- Pour chaque alerte : emoji severity + `alert_message` + date + bouton "Voir →" (si `action_recommended.startswith("http")`) ou texte gris sinon

**Détection URL** : `action_recommended.startswith("http")` → bouton cliquable indigo. Sinon texte. Tous les `action_recommended` dans `dash_alerts.sql` sont désormais des URLs dashboard direct (cf. dbt CLAUDE.md "Alerting — refonte dashboard liens + multi-apparts").

### Regroupement multi-apparts

Pour les types qui touchent N apparts/résas simultanément (`superhost_risk` 29/jour, `revenue_anomaly`, `last_minute`, `cancellation_vip`, `cancellation_large_apt`, `abandoned_cart`) — si **≥ 3 occurrences** du même type dans une même catégorie, on regroupe en **1 case agrégée** au lieu de N lignes :

- Titre + count + breakdown severity (`🔴 6 critical · 🟡 23 warning`)
- Sous-blocs Critical puis Warning avec chips compacts (1 chip = `alert_message` complet)
- Bouton "Voir →" unique cliquable (URL = `action_recommended` du 1er élément)

**Tri intelligent (constante `GROUPABLE` en haut de email_digest.py)** :
- `superhost_risk` : `secondary_value` desc (= `nb_reviews_3m`, signal solide) puis `metric_value` asc (pires notes en haut)
- `revenue_anomaly` : `secondary_value` desc (= CA P-1, plus grosse chute en premier)
- Autres : par `alert_date`

**Pour ajouter un nouveau type groupable** : ajouter une entrée dans `GROUPABLE` avec `title` + lambda `sort`. Le seuil `GROUP_MIN_COUNT=3` est global.

### ⚠️ NULL CONCAT côté dbt

BigQuery `CONCAT(a, b, NULL, c)` retourne **NULL** dès qu'un seul argument est NULL → mail affiche "None". Si tu vois "None" dans la section d'une catégorie, le bug est côté `dash_alerts.sql` — wrapper tous les args potentiellement NULL en `COALESCE(col, '?')`. Cas connu corrigé 2026-05-20 : `cancellation_vip` avec `customer_name = NULL`.

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

## Triggering manuel
```bash
# 4h
curl -s -X POST \
  "https://europe-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/merveil-data-warehouse/jobs/merveil-action-engine:run" \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  -d '{}'

# daily
curl -s -X POST \
  "https://europe-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/merveil-data-warehouse/jobs/merveil-action-engine-daily:run" \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  -d '{}'
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
| Scheduler PERMISSION_DENIED (code 7) | SA OAuth du scheduler pas dans la policy du job Cloud Run, ou API v2 utilisée | Utiliser API v1 + `scheduler-invoker@` + `gcloud run jobs add-iam-policy-binding` |
