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
- **B.** crée (get-or-create par extId `MERVEIL_USER - <duve_id>`) un **user Sofia dédié à la résa, au vrai nom du guest** (firstname/lastname), **avec un password aléatoire** (jamais partagé) → le user est `enabled=True` et son tag `user` auto-créé sert de `guestTagId` → l'UI Luckey affiche le vrai nom. Puis `POST /api/v2/standardDevices` (extId `MERVEIL_RESA - <duve_id>`, credentialRule sur ce guest tag + le lock tag de l'appart). Le device ancre le user. Window = heures de politique appart (floor 7h / ceil 23h59), tz Paris.
- **C.** `POST /api/v2/invitations` (extId `MERVEIL_INV - <duve_id>`, `smartLockIds=[lock_id]`, `numberOfDevices:0`) → `code` → lien `https://archides.jago.cloud/remoteOpen?code=<code>` (⚠️ host `archides`, PAS le `api-archides` renvoyé par l'API). Lien gated sur la window (OK pendant le séjour).
- **D.** `POST` intégration entrante Duve (`DUVE_CONNECT_URL?pid=DUVE_CONNECT_PID`, secret `duve-connect-token`) : `primaryCode` (code clavier) + champ `merveil_paris_iseo_access_link_eIhhEnlspM` (lien). N'émet aucun message (messages auto Duve lisent le champ).
- **E.** INSERT état dans `iseo_raw.merveil_pin_cache` (colonnes `iseo_device_id`, `iseo_invitation_id`, `invitation_code/link`, `provisioned_at`, `duve_pushed_at`). Device + invitation sont **get-or-create par extId** → idempotent sur retry partiel.

**Résolution ids appart (JOIN BQ, pas de seed)** : `duve property_id (GUID) == Mews resource_id == nom du lock tag`. → `lock_id` + `lock_tag_id` via `stg_iseo__smart_locks`. Le `guest_tag_id` n'est **plus** dérivé de l'appart : c'est le tag `user` du user dédié de la résa (créé en B). `customer_name` vient de `fct_reservations`.

**Resync drift de dates (2026-06-20)** — `_resa_to_resync()` : rows actives dont la window cache (= dates Mews au provision) ≠ dates live `fct_reservations` (séjour étendu/raccourci/décalé après provision). `_resync` : `DELETE` device+invitation puis re-`POST` avec la window live + **le MÊME code PIN** (réutilisé via `_post_device(pin_value=…)`, libéré par le DELETE) ; le lien remote-open change (nouvelle invitation) → re-push Duve. UPDATE l'état cache (CI/CO + ids + `provisioned_at`). Sans ça, une extension de séjour = guest lockout sur les nuits ajoutées (master couvre, dégradé).

**Archive** — `_resa_to_archive()` : rows actives où CO passé OU résa annulée (cross-check Mews). `_archive` : `DELETE /standardDevices/{id}` (par extId) + `DELETE /invitations/{id}` + **`DELETE /users/{id}`** (le user dédié de la résa, par extId `MERVEIL_USER - <duve_id>` — sinon accumulation de users guest).

**Mapping Duve↔Mews** : payload checkin Duve embarque `guestProfiles[isPrimary].externalId = customer_id` ; JOIN `fct_reservations` sur `customer_id + resource_id` (résa au CI le plus proche). Skip annulées au provision + archive.

**Mode shadow** (`ISEO_SHADOW_MODE=true`) : log "would provision" sans appel Sofia/Duve ni écriture d'état.

**Whitelist** (`ISEO_ALLOWED_PROPERTY_IDS=csv de GUID`) : **cutover = DAL40 seul** (`c12a7244…`). Liste des 13 apparts à ré-activer en commentaire dans `deploy.sh`.

**Secrets requis** : `ISEO_MANAGER_USERNAME` + `ISEO_MANAGER_PASSWORD` + `DUVE_CONNECT_TOKEN` (=`duve-connect-token`). Env : `DUVE_CONNECT_PID=6a357cbd2e45c374a9a9fd18`. SA `action-engine-sa` a `secretAccessor` project-wide.

**Retry Duve** (`_resa_duve_retry`) : à chaque run, re-pousse Duve pour les rows `provisioned_at IS NOT NULL AND duve_pushed_at IS NULL` (Sofia OK mais Duve KO à un run précédent) — code + lien lus depuis la cache, sans rappel Sofia.

**Alerting** : `run()` est un wrapper qui envoie un **mail** (`ISEO_ALERT_TO`, infra Gmail `alerts-gmail-sa-key` via Secret Manager + DWD) — récap si ≥1 erreur (provision/retry/archive), CRASH + exit non-zero si exception. ⚠️ délimiteur env `^;^` dans `deploy.sh` (les emails contiennent `@`).

**État cutover (22/06)** : live sur **7 apparts**. Les 5 d'origine AVEC guest tag (OUR12-1D, TBG52-1D, TBG52-1G, SEB23-3F, SEB23-3G) + **CLE7-0D** (1er appart **via fallback guest tag 132094**, validé E2E en prod le 22/06 : device+invitation+lien+Duve OK) + **MRI16-0D** (guest tag propre). `ISEO_SHADOW_MODE=false`. Restent hors whitelist : ABO58, POC5, SEB44 (tagless → prochain paquet fallback).

**Trous connus à traiter** : (1) ~~drift de dates~~ → **FAIT 2026-06-20** (`_resa_to_resync`, cf. ci-dessus) ; (2) ~~**fallback guest tag**~~ → **OBSOLÈTE depuis 2026-06-30** : plus de tag dérivé de l'appart ni de fallback générique. Chaque résa a son user dédié au vrai nom (cf. « Label Luckey » ci-dessous) → tout appart est provisionnable sans dépendre d'un guest tag pré-existant. `ISEO_DEFAULT_GUEST_TAG_ID` supprimé de `deploy.sh`. ⚠ Pour réellement provisionner les 4 apparts ABO58/CLE7/POC5/SEB44, il reste à **ajouter leurs GUID à `ISEO_ALLOWED_PROPERTY_IDS`** (cutover live = décision séparée) ; (3) élargir whitelist aux 13 ; (4) ~~retirer le cacher~~ → **FAIT 2026-06-20** (cacher webhook + topic/sub/endpoint supprimés ; reste à droper le topic `iseo-pin-to-cache` + sub + DLQ côté infra) ; (5) ~~tab dashboard~~ → **FAIT 2026-06-20** (`dash_ops_pin_pipeline` refondu sur le nouveau vocabulaire provision/invitation/duve + drift ; ancienne réconciliation PHANTOM/DRIFT supprimée).

**Onboarding lockTags — 96 serrures taggées 2026-06-29** : le pipeline mappe une serrure→appart via un lockTag dont le `name` = `resource_id` Mews. Seules 18/130 serrures l'avaient (créés jadis par l'intégration native) → 96 sans tag = non provisionnables. `merveil-etl-v2/utils/iseo_sync_lock_tags.py --apply` les a toutes onboardées : `createLockTag {name: resource_id}` (idempotent par extId, dérivé de `raw_resources` via le nom de la serrure) + `updateSmartLock` en ré-incluant les tag ids existants (ADMIN préservé). **Prérequis levé pour élargir `ISEO_ALLOWED_PROPERTY_IDS`** au-delà des 7 actuels (le tag n'est qu'un prérequis ; le provision reste gaté par la whitelist). **+8 cas limites résolus via `--fix-edge-cases`** (map `EDGE_CASES`, resource_id vérifiés) : A = 4 serrures renommées pour matcher Mews (RIC75 O→0, VIA29 casse, BAC116 espace, FRE17→FRE17B) + 2 faux positifs regex taggés (VAL1B, DUL16) ; B = 2 préfixes décalés taggés sans rename (MTM13 lock-P02/Mews-P01, PAL7 lock-P03/Mews-P02). **Restent (décision ops/Mews)** : 3 C = unité serrure absente de Mews (ARC77-2F, MAR181-0F, POI8-1G) ; 3 D = vraiment absents (DES5-5F, DES-5G, SEB76-2F) ; 2 E = non-apparts (LOCAL HK, Test applique) ; 1 MISMATCH (BRS1-5F taggée avec le resource_id de SEB76-2D).

**Purge DUVE_PIN natifs orphelins — ajouté 2026-06-29** : l'intégration native étant coupée, plus personne ne supprime les `DUVE_PIN` à l'annulation/au départ → un guest annulé gardait un code valide (cas Bianca Aranha : résa annulée, PIN 7552 toujours actif). Nouvelle phase `_purge_native_orphan` : DELETE les `DUVE_PIN - <duve_id>` dont la résa Mews est **annulée OU checked-out**, **scopé STRICTEMENT à `ISEO_ALLOWED_PROPERTY_IDS`** (hors whitelist le DUVE_PIN natif reste l'UNIQUE code du guest → ne jamais purger ailleurs). Les résas ACTIVE ne sont pas touchées (leur DUVE_PIN double notre code mais reste l'unique code des résas >J-7 pas encore provisionnées) → nettoyées au checkout. Compteur `purged=` dans le log DONE.

**Label Luckey — user dédié au vrai nom par résa (2026-06-30, remplace le tag générique)** : l'UI Luckey affiche le **firstname/lastname du user** porteur du guest tag (pas le nom du tag, qui vaut l'email). Donc `_provision`/`_resync` créent un **user Sofia par résa au vrai nom du guest** (`firstname/lastname` depuis `fct_reservations.customer_name`), supprimé à l'archive. Fin de l'approche « tag générique partagé » du 29/06 qui était cassée.

⚠️ **Finding clé `enabled` (2026-06-30)** : un user créé via l'API est **`enabled=False`** par défaut (le schéma POST `/api/v2/users` n'a pas de champ `enabled`, et `/disable` existe mais pas `/enable` ; PUT `enabled=True` ignoré). Un user `enabled=False` est **garbage-collecté en quelques minutes s'il n'a pas de device**, et un user désactivé **perd son tag `user`** (vérifié : 7/44 users natifs `enabled=False` = 0 guest tag → PIN cassé). **Seul moyen de créer un user `enabled=True` : fournir un `password` à la création** (testé : password → `enabled=True` + tag, sans password → `enabled=False`). D'où le password aléatoire systématique dans `_get_or_create_guest_user` (le guest ne se connecte jamais, il ouvre au PIN clavier + lien remote-open).

**Historique des échecs (à ne pas refaire)** : 29/06 tag « Merveil Guest » 184106 créé en `type: system` → refusé par Sofia `HTTP 400 "guestTagId must be a user type"` (un guestTagId doit être un tag `type: user`, càd attaché à un user). 30/06 tag user 184181 d'un user guest standalone (sans device, `enabled=False`) → user GC en <6 min → `HTTP 404 "UserTag not found"`. Les deux pistes mortes ; tags/users de test purgés.

**Bug churn provision/archive — corrigé 2026-06-29** : quand une résa Duve mappe vers **2 résas Mews mêmes dates** (jumeau actif + jumeau annulé issu d'un rebooking Mews cancel+recreate), `_resa_to_archive` pouvait piocher le jumeau **annulé** (son `QUALIFY` ne filtrait pas `is_cancelled`, tie-break à égalité sur les dates) → archivait un PIN actif → `_resa_to_provision` le recréait au run suivant (cache row archivée = plus couverte) → **boucle create/delete toutes les 2h** (~40 devices/invitations Sofia créés+supprimés sur CLE7-0D, push Duve à chaque cycle). Fix : ajouter `COALESCE(m.is_cancelled, FALSE) ASC` en tête du `ORDER BY` du QUALIFY d'archive (le jumeau actif gagne) — même logique que provision. Idem dans `dash_ops_pin_pipeline.sql` (le dash affichait 1 ligne/résa via QUALIFY → churn invisible, et tie-break aléatoire). Observé sur RUIQING XU / CLE7-0D, CI 04/07.

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
