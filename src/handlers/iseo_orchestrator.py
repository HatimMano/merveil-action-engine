"""
ISEO PIN Orchestrator — pipeline V3 100% DWH (intégration native Duve↔Sofia COUPÉE).

Cf. [[project_iseo_integration_2026]] + to_do "MAJ 20/06 — pipeline complet validé".

Depuis le 20/06 le natif Duve↔Sofia est désactivé dans les 2 sens. Le DWH est
seul maître du cycle PIN. Pour chaque résa Mews non annulée, payée, dont le CI est
dans les LOOKAHEAD_DAYS prochains jours (et pas encore provisionnée) :

  A. génère un code PIN 4 chiffres (unique account-wide, retry sur collision)
  B. crée (get-or-create par extId) un user Sofia DÉDIÉ à la résa, au VRAI nom du
     guest (firstname/lastname), avec un password aléatoire jamais partagé → le user
     est `enabled=True` et porte un tag `user` auto-créé. Ce tag sert de guestTagId →
     l'UI Luckey affiche le vrai nom du guest. POST Sofia /standardDevices
     (credentialRule sur ce guest tag + le lock tag de l'appart). Le device ancre le
     user (pas de garbage-collection).
  C. POST Sofia /invitations (smartLockIds=[lock_id]) → code → lien remote-open
     `https://archides.jago.cloud/remoteOpen?code=<code>`
  D. POST intégration Duve (champ custom) : primaryCode = code clavier +
     ISEO ACCESS LINK = lien remote-open. (Aucun message déclenché — les messages
     auto Duve lisent le champ. Lien gated sur la window, OK pendant le séjour.)
  E. INSERT état dans iseo_raw.merveil_pin_cache.

Archive (CO passé OU résa annulée) : DELETE Sofia device + DELETE invitation +
DELETE le user dédié de la résa (par extId) + flag archived_at.

⚠️ enabled : un user créé via l'API est enabled=False SAUF si on fournit un
`password` à la création (le schéma create n'a pas de champ `enabled`). Un user
enabled=False finit garbage-collecté / perd son tag → PIN cassé. D'où le password
aléatoire systématique (le guest ne se connecte jamais, il ouvre au PIN clavier).

Résolution des ids appart (par JOIN BQ, pas de seed) :
  - duve property_id (GUID) == Mews resource_id == nom du lock tag Sofia
  - lock_id + lock_tag_id ← stg_iseo__smart_locks (par nom de tag = property_id)
  - guest_tag_id ← tag `user` du user dédié de la résa (créé en B)

Modes :
  - ISEO_SHADOW_MODE=true → log "would provision" sans appel Sofia/Duve
  - ISEO_ALLOWED_PROPERTY_IDS (csv de GUID) → whitelist rollout par appart

Trigger : Cloud Run Job `merveil-action-engine-iseo` (scheduler 2h à :45).
"""

import logging
import os
import secrets
import time
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from google.cloud import bigquery

from src.core.mailer import build_email, esc, send_mail

PARIS_TZ = ZoneInfo("Europe/Paris")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "merveil-data-warehouse")
PIN_CACHE_TABLE = os.environ.get(
    "ISEO_PIN_CACHE_TABLE", "merveil-data-warehouse.iseo_raw.merveil_pin_cache")
RAW_DUVE_CHECKIN_TABLE = os.environ.get(
    "RAW_DUVE_CHECKIN_TABLE", "merveil-data-warehouse.raw_duve.checkin_events")
DUVE_CHECKIN_STG_TABLE = os.environ.get(
    "DUVE_CHECKIN_STG_TABLE", "merveil-data-warehouse.staging.stg_duve__checkin_events")
MEWS_FCT_TABLE = os.environ.get(
    "MEWS_FCT_TABLE", "merveil-data-warehouse.marts.fct_reservations")
MEWS_PAYMENTS_TABLE = os.environ.get(
    "MEWS_PAYMENTS_TABLE", "merveil-data-warehouse.staging.stg_mews__payments")
SMART_LOCKS_TABLE = os.environ.get(
    "SMART_LOCKS_TABLE", "merveil-data-warehouse.staging.stg_iseo__smart_locks")
STD_DEVICES_TABLE = os.environ.get(
    "STD_DEVICES_TABLE", "merveil-data-warehouse.staging.stg_iseo__standard_devices")
WHITELIST_TABLE = os.environ.get(
    "ISEO_WHITELIST_TABLE", "merveil-data-warehouse.staging.iseo_whitelisted_apartments")
HOLD_DECISIONS_TABLE = os.environ.get(
    "ISEO_HOLD_DECISIONS_TABLE", "merveil-data-warehouse.iseo_raw.hold_decisions")

ISEO_BASE_URL = os.environ.get("ISEO_BASE_URL", "https://api-archides.jago.cloud")
ISEO_USERNAME = (os.environ.get("ISEO_MANAGER_USERNAME") or "").strip()
ISEO_PASSWORD = (os.environ.get("ISEO_MANAGER_PASSWORD") or "").strip()

# Intégration entrante Duve (write path) — cf. to_do 20/06.
DUVE_CONNECT_URL = os.environ.get(
    "DUVE_CONNECT_URL", "https://connect.duve.com/api/v1/hooks/duveconnect")
DUVE_CONNECT_PID = os.environ.get("DUVE_CONNECT_PID", "")
DUVE_CONNECT_TOKEN = (os.environ.get("DUVE_CONNECT_TOKEN") or "").strip()
DUVE_FIELD_NAME = os.environ.get(
    "DUVE_FIELD_NAME", "merveil_paris_iseo_access_link_eIhhEnlspM")
# La page web guest est sur archides.jago.cloud, PAS le host api- renvoyé par l'API.
REMOTE_OPEN_HOST = os.environ.get("ISEO_REMOTE_OPEN_HOST", "archides.jago.cloud")

ISEO_SHADOW_MODE = os.environ.get("ISEO_SHADOW_MODE", "true").lower() == "true"
# Whitelist des apparts cutover. Source de vérité = seed BQ iseo_whitelisted_apartments
# (chargé au run via _load_whitelist → élargir = 1 ligne dans le seed, sans redeploy).
# La valeur env ci-dessous = FALLBACK si le seed est vide/inaccessible (garde-fou).
ALLOWED_PROPERTY_IDS = {
    pid.strip().lower()
    for pid in (os.environ.get("ISEO_ALLOWED_PROPERTY_IDS") or "").split(",")
    if pid.strip()
}

# Alerting mail (réutilise l'infra Gmail API du service — secret alerts-gmail-sa-key
# lu via Secret Manager + Domain-Wide Delegation, comme cancellations_brief).
GMAIL_SENDER = os.getenv("GMAIL_SENDER", "noreply@archides.fr")
ISEO_ALERT_TO = os.getenv("ISEO_ALERT_TO", "hatim@archides.fr")

LOOKAHEAD_DAYS = int(os.environ.get("ISEO_LOOKAHEAD_DAYS", "3"))

# ── Porte de validation (hold) ────────────────────────────────────────────────
# Le code est CRÉÉ côté Sofia (visible au dashboard, révocable) mais N'EST PAS
# poussé à Duve : le client ne le voit pas, la RC valide puis le lui envoie.
#   off     → porte désactivée
#   observe → décision calculée et journalisée, mais on pousse quand même (défaut)
#   on      → rétention effective
# ⚠ La porte n'a d'effet RÉEL qu'une fois le code fixe retiré du champ Duve de
# l'appartement : tant qu'il y est, Duve l'affiche en repli et le client entre
# quand même. Elle mord donc progressivement, appartement par appartement, au
# rythme des suppressions côté ops — c'est voulu, ça rend l'activation sans risque.
ISEO_HOLD_MODE = os.environ.get("ISEO_HOLD_MODE", "observe").lower()
# Seuil « réservé peu avant l'arrivée », appliqué au canal DIRECT seul (cf. _evaluate_hold).
ISEO_HOLD_LEAD_HOURS = int(os.environ.get("ISEO_HOLD_LEAD_HOURS", "72"))
# Destinataire de la notification « code retenu » (défaut = alerte ISEO).
ISEO_HOLD_ALERT_TO = os.getenv("ISEO_HOLD_ALERT_TO", "") or ISEO_ALERT_TO
DEFAULT_CI_HOUR = os.environ.get("ISEO_DEFAULT_CI_HOUR", "13:00")
DEFAULT_CO_HOUR = os.environ.get("ISEO_DEFAULT_CO_HOUR", "19:00")
PIN_COLLISION_RETRIES = 8


# ── Sofia auth (singleton) ────────────────────────────────────────────────────

class _SofiaAuth:
    _token: Optional[str] = None
    _expires_at: float = 0.0

    @classmethod
    def get_token(cls) -> str:
        now = time.time()
        if cls._token and now < cls._expires_at - 600:
            return cls._token
        if not ISEO_USERNAME or not ISEO_PASSWORD:
            raise RuntimeError("ISEO credentials missing")
        resp = requests.post(
            f"{ISEO_BASE_URL}/oauth/token", auth=("client", ""),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "password", "username": ISEO_USERNAME, "password": ISEO_PASSWORD},
            timeout=30)
        resp.raise_for_status()
        p = resp.json()
        cls._token = p["access_token"]
        cls._expires_at = now + int(p.get("expires_in", 172800))
        logger.info(f"🔑 Sofia token (exp in {p.get('expires_in', '?')}s)")
        return cls._token


def _sofia(method: str, path: str, json_body=None) -> requests.Response:
    return requests.request(
        method, f"{ISEO_BASE_URL}{path}",
        headers={"Authorization": f"Bearer {_SofiaAuth.get_token()}",
                 "Accept": "application/json", "Content-Type": "application/json"},
        json=json_body, timeout=30)


def _duve_push(duve_resa_id: str, code: str, link: str) -> tuple[bool, Optional[str]]:
    """POST l'intégration entrante Duve : écrit primaryCode (code clavier) +
    le champ custom ISEO ACCESS LINK (lien remote-open). N'émet aucun message
    (les messages auto Duve lisent le champ)."""
    if not (DUVE_CONNECT_PID and DUVE_CONNECT_TOKEN):
        return False, "Duve connect config missing (PID/TOKEN)"
    r = requests.post(
        f"{DUVE_CONNECT_URL}?pid={DUVE_CONNECT_PID}",
        headers={"Authorization": f"Bearer {DUVE_CONNECT_TOKEN}",
                 "Content-Type": "application/json"},
        json={"reservation": duve_resa_id, "primaryCode": code,
              "additionalFields": [{"name": DUVE_FIELD_NAME, "value": link}]},
        timeout=30)
    if r.status_code == 200:
        return True, None
    return False, f"Duve HTTP {r.status_code}: {r.text[:200]}"


def _duve_push_all(duve_ids, code: str, link: str) -> tuple[bool, Optional[str]]:
    """Pousse le même code + lien à TOUS les duve du stay (back-to-back → chaque résa
    Duve reçoit le code, quel que soit le message auto qui se déclenche). Succès =
    tous OK ; sinon renvoie la 1re erreur (le retry re-tentera l'ensemble)."""
    ids = [d for d in (duve_ids or []) if d]
    if not ids:
        return False, "aucun duve_id à pousser"
    errs = []
    for d in ids:
        ok, err = _duve_push(d, code, link)
        if not ok:
            errs.append(f"{d}: {err}")
    return (not errs), ("; ".join(errs) if errs else None)


def _send_alert(subject: str, body: str, html: bool = False) -> None:
    """Mail d'alerte best-effort (infra commune src/core/mailer)."""
    send_mail(subject, body, ISEO_ALERT_TO, html=html, sender=GMAIL_SENDER)


# ── BigQuery ──────────────────────────────────────────────────────────────────

_bq_client: Optional[bigquery.Client] = None


def _bq() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


# CTEs partagés (mapping Duve↔Mews + résolution lock/tags par appart).
_DUVE_LATEST_CTE = f"""
    duve_latest AS (
      SELECT
        reservation_id AS duve_reservation_id,
        property_id    AS duve_property_id,
        (SELECT JSON_VALUE(g, '$.externalId')
           FROM UNNEST(JSON_QUERY_ARRAY(_raw_payload, '$.resource.guestProfiles')) g
           WHERE JSON_VALUE(g, '$.isPrimary') = 'true' LIMIT 1) AS mews_customer_id
      FROM `{RAW_DUVE_CHECKIN_TABLE}`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY reservation_id ORDER BY received_at DESC) = 1
    )"""

_LOCKS_CTE = f"""
    locks AS (
      SELECT l.lock_id, l.apartment_code,
             JSON_VALUE(t, '$.name')              AS duve_property_id,
             CAST(JSON_VALUE(t, '$.id') AS INT64) AS lock_tag_id
      FROM `{SMART_LOCKS_TABLE}` l, UNNEST(JSON_QUERY_ARRAY(l.tags)) AS t
      WHERE JSON_VALUE(t, '$.name') != 'ADMIN'
    )"""

_PAYMENTS_CTE = f"""
    payments AS (
      SELECT reservation_id,
             COUNTIF(state = 'Charged') AS n_charged,
             COUNTIF(state = 'Failed')  AS n_failed
      FROM `{MEWS_PAYMENTS_TABLE}`
      WHERE reservation_id IS NOT NULL GROUP BY reservation_id
    )"""

# CTE "stay" = unité d'accès physique = occupation CONTINUE d'un guest sur une serrure.
# Un stay regroupe les résas Mews non annulées d'un même (customer_id, resource_id) dont
# les intervalles [CI,CO] sont contigus ou chevauchants (gaps-and-islands). Résout le bug
# de collision quand un client a ≥2 résas sur le même appart (back-to-back → 1 seul code,
# fenêtre fusionnée min(CI)→max(CO) ; un TROU entre 2 périodes → 2 stays = 2 codes).
# Pivot = `canonical_duve` = duve de la résa la plus tôt du groupe. Pour une résa unique
# (99% des cas) : stay = 1 membre, canonical = son duve, fenêtre = sa fenêtre → strictement
# identique à l'ancien comportement (zéro migration). `member_duve_ids` = tous les duve du
# stay (le code est poussé à chacun côté Duve). Résolution duve↔résa par date de CI exacte
# (déterministe, ≠ ancien join (customer,property) ambigu). Réutilise _LOCKS_CTE/_PAYMENTS_CTE.
_STAYS_CTE = f"""
    {_LOCKS_CTE},
    {_PAYMENTS_CTE},
    duve_stay AS (
      SELECT duve_reservation_id, duve_property_id,
             primary_guest_external_id AS mews_customer_id, checkin_date AS duve_ci
      FROM `{DUVE_CHECKIN_STG_TABLE}`
      WHERE primary_guest_external_id IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY duve_reservation_id ORDER BY received_at DESC) = 1
    ),
    member_resas AS (
      SELECT m.reservation_id, m.reservation_number, m.customer_id, m.customer_name,
             m.resource_id, m.checkin_date, m.checkout_date,
             m.earliest_checkin_hour, m.latest_checkout_hour,
             lk.lock_id, lk.lock_tag_id, lk.apartment_code,
             (COALESCE(pay.n_failed, 0) > 0 AND COALESCE(pay.n_charged, 0) = 0) AS payment_unpaid,
             -- Signaux de la PORTE (hold). Délai réservation→arrivée en heures, calé sur
             -- 15h le jour du CI (même convention que la mesure du 15/08). Négatif =
             -- réservé après l'heure d'arrivée théorique (cas Defalque, 21h02 pour le soir).
             -- Conservé tel quel pour l'affichage du motif dans le mail de rétention.
             TIMESTAMP_DIFF(TIMESTAMP(DATETIME(m.checkin_date, TIME '15:00:00')),
                            m.created_at, HOUR)                          AS lead_hours,
             -- Les DEUX critères de la porte sont restreints au canal DIRECT (cf.
             -- _evaluate_hold) : les 4 fraudes d'août y sont, et une résa OTA est payée
             -- à l'OTA (moyen de paiement vérifié, recours possible).
             (m.ota_source = 'Site direct'
              AND TIMESTAMP_DIFF(TIMESTAMP(DATETIME(m.checkin_date, TIME '15:00:00')),
                                 m.created_at, HOUR) <= {ISEO_HOLD_LEAD_HOURS}) AS direct_last_minute,
             -- « Rien d'encaissé » n'a de sens que sur les canaux où NOUS prenons la carte :
             -- une résa Booking/Airbnb est payée à l'OTA et n'a AUCUN paiement dans Mews →
             -- appliqué à tous les canaux, ce critère retiendrait 69 % des arrivées.
             (m.ota_source = 'Site direct'
              AND COALESCE(pay.n_charged, 0) = 0)                        AS direct_unpaid
      FROM `{MEWS_FCT_TABLE}` m
      LEFT JOIN locks lk    ON lk.duve_property_id = m.resource_id
      LEFT JOIN payments pay ON pay.reservation_id = m.reservation_id
      WHERE COALESCE(m.is_cancelled, FALSE) = FALSE
        AND m.checkout_date >= CURRENT_DATE()
    ),
    prev_co AS (
      SELECT *, MAX(checkout_date) OVER (
        PARTITION BY customer_id, resource_id
        ORDER BY checkin_date, reservation_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_checkout
      FROM member_resas
    ),
    islands AS (
      SELECT *, SUM(CAST(prev_checkout IS NULL OR checkin_date > prev_checkout AS INT64)) OVER (
        PARTITION BY customer_id, resource_id
        ORDER BY checkin_date, reservation_id) AS island_id
      FROM prev_co
    ),
    -- Champs stay-level agrégés depuis les membres AVANT d'attacher les duve (l'attache
    -- multiplie les lignes). 1 ligne par (customer, resource, island).
    stay_base AS (
      SELECT customer_id, resource_id, island_id,
        ANY_VALUE(customer_name)  AS customer_name,
        ANY_VALUE(apartment_code) AS apartment_code,
        ANY_VALUE(lock_id)        AS lock_id,
        ANY_VALUE(lock_tag_id)    AS lock_tag_id,
        MIN(checkin_date)         AS stay_ci,
        MAX(checkout_date)        AS stay_co,
        ARRAY_AGG(earliest_checkin_hour ORDER BY checkin_date)[SAFE_OFFSET(0)]       AS earliest_checkin_hour,
        ARRAY_AGG(latest_checkout_hour  ORDER BY checkout_date DESC)[SAFE_OFFSET(0)] AS latest_checkout_hour,
        CAST(ARRAY_AGG(reservation_number ORDER BY checkin_date)[SAFE_OFFSET(0)] AS STRING) AS mews_reservation_number,
        ARRAY_AGG(payment_unpaid ORDER BY checkin_date)[SAFE_OFFSET(0)]              AS payment_unpaid,
        -- Porte agrégée au stay : on prend le membre le PLUS tardivement réservé et on
        -- retient si l'un des membres est impayé. Conservateur par choix — sur un stay
        -- back-to-back, une seule résa suspecte suffit à demander une validation.
        MIN(lead_hours)                                                              AS min_lead_hours,
        LOGICAL_OR(direct_last_minute)                                               AS direct_last_minute,
        LOGICAL_OR(direct_unpaid)                                                    AS direct_unpaid
      FROM islands
      GROUP BY customer_id, resource_id, island_id
    ),
    -- Attache les duve du (customer, resource) dont le CI tombe dans la fenêtre du stay,
    -- avec tolérance -14j (drift de dates post-pré-checkin : les dates Mews peuvent bouger
    -- après que le guest a rempli Duve → un match par date EXACTE raterait le duve et
    -- archiverait à tort le code actif). La tolérance ≪ écart entre 2 stays d'un même
    -- (customer, resource) → pas de contamination inter-stays.
    stay_duve AS (
      SELECT sb.*, d.duve_reservation_id, d.duve_ci
      FROM stay_base sb
      LEFT JOIN duve_stay d
        ON d.mews_customer_id  = sb.customer_id
       AND d.duve_property_id  = sb.resource_id
       AND d.duve_ci BETWEEN DATE_SUB(sb.stay_ci, INTERVAL 14 DAY) AND sb.stay_co
    ),
    stays AS (
      SELECT
        resource_id            AS duve_property_id,
        customer_name, apartment_code, lock_id, lock_tag_id,
        stay_ci, stay_co, earliest_checkin_hour, latest_checkout_hour,
        mews_reservation_number, payment_unpaid, min_lead_hours,
        direct_last_minute, direct_unpaid,
        ARRAY_AGG(duve_reservation_id IGNORE NULLS ORDER BY duve_ci)                 AS member_duve_ids,
        ARRAY_AGG(duve_reservation_id IGNORE NULLS ORDER BY duve_ci)[SAFE_OFFSET(0)] AS canonical_duve
      FROM stay_duve
      GROUP BY customer_id, resource_id, island_id, duve_property_id, customer_name,
               apartment_code, lock_id, lock_tag_id, stay_ci, stay_co,
               earliest_checkin_hour, latest_checkout_hour, mews_reservation_number,
               payment_unpaid, min_lead_hours, direct_last_minute, direct_unpaid
    )"""


def _resa_to_provision() -> list[dict]:
    """Stays à provisionner : fenêtre CI dans [today, today+LOOKAHEAD], CO futur,
    canonical_duve résolu, PAS déjà couvert par une row de cache active — ni par le
    canonical, ni par un member duve (évite un 2e device sur le même stay). Le
    `duve_reservation_id` renvoyé = le canonical (= identité Sofia du stay)."""
    q = f"""
    WITH {_STAYS_CTE},
    active AS (
      SELECT DISTINCT duve_reservation_id
      FROM `{PIN_CACHE_TABLE}` WHERE archived_at IS NULL
    )
    SELECT
      s.canonical_duve AS duve_reservation_id,
      s.duve_property_id, s.lock_id, s.lock_tag_id, s.apartment_code,
      s.customer_name, s.mews_reservation_number,
      s.stay_ci AS checkin_date, s.stay_co AS checkout_date,
      s.earliest_checkin_hour, s.latest_checkout_hour,
      s.payment_unpaid, s.member_duve_ids,
      s.min_lead_hours, s.direct_last_minute, s.direct_unpaid
    FROM stays s
    WHERE s.canonical_duve IS NOT NULL
      AND s.stay_ci <= DATE_ADD(CURRENT_DATE(), INTERVAL {LOOKAHEAD_DAYS} DAY)
      AND s.stay_co >= CURRENT_DATE()
      AND NOT EXISTS (SELECT 1 FROM active a WHERE a.duve_reservation_id = s.canonical_duve)
      AND NOT EXISTS (SELECT 1 FROM UNNEST(s.member_duve_ids) md
                      JOIN active a ON a.duve_reservation_id = md)
    ORDER BY s.stay_ci
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


def _whitelisted_gaps() -> list[dict]:
    """Trous silencieux : résas whitelistées à provisionner (CI ≤ J-lookahead, CO ≥
    today, non annulées) SANS row cache active, classées par cause :
      - lock       : serrure non résolue (anormal sur whitelist, tout horizon)
      - precheckin : pas de mapping Duve = formulaire pas rempli (bruit auto-résolu
                     avant J-1 → n'alerte qu'à CI ≤ J+1)
      - paiement   : gate volontaire — tous les paiements Failed, aucun Charged
      - autre      : provisionnable en apparence mais toujours pas de code = le vrai
                     signal « aurait dû être généré », à investiguer
    Sans ce filet une résa sans mapping Duve est effacée par le INNER JOIN de
    _resa_to_provision (ni skip ni erreur) → guest sans code sans aucun signal."""
    if not ALLOWED_PROPERTY_IDS:
        return []
    q = f"""
    WITH {_DUVE_LATEST_CTE},
    {_LOCKS_CTE},
    {_PAYMENTS_CTE},
    active_by_mews AS (
      SELECT DISTINCT mews_reservation_number
      FROM `{PIN_CACHE_TABLE}`
      WHERE archived_at IS NULL AND mews_reservation_number IS NOT NULL
    )
    SELECT m.reservation_number, m.resource_id, m.customer_name, m.checkin_date,
           m.checkout_date, lk.apartment_code,
           CASE
             WHEN lk.lock_id IS NULL THEN 'lock'
             WHEN d.duve_reservation_id IS NULL THEN 'precheckin'
             WHEN COALESCE(pay.n_failed, 0) > 0
              AND COALESCE(pay.n_charged, 0) = 0 THEN 'paiement'
             ELSE 'autre'
           END AS reason
    FROM `{MEWS_FCT_TABLE}` m
    LEFT JOIN duve_latest d ON d.mews_customer_id = m.customer_id
                           AND d.duve_property_id = m.resource_id
    LEFT JOIN locks lk       ON lk.duve_property_id = m.resource_id
    LEFT JOIN payments pay   ON pay.reservation_id = m.reservation_id
    LEFT JOIN active_by_mews am
      ON am.mews_reservation_number = CAST(m.reservation_number AS STRING)
    WHERE LOWER(m.resource_id) IN UNNEST(@wl)
      AND m.checkin_date <= DATE_ADD(CURRENT_DATE(), INTERVAL {LOOKAHEAD_DAYS} DAY)
      AND m.checkout_date >= CURRENT_DATE()
      AND COALESCE(m.is_cancelled, FALSE) = FALSE
      AND am.mews_reservation_number IS NULL
      -- precheckin avant J-1 = bruit qui se résout tout seul (vérifié 15/07) →
      -- exclu du mail. Les 3 autres causes alertent à tout horizon ≤ lookahead.
      AND NOT (lk.lock_id IS NOT NULL
               AND d.duve_reservation_id IS NULL
               AND m.checkin_date > DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY CAST(m.reservation_number AS STRING)
      ORDER BY m.checkin_date) = 1
    ORDER BY CASE reason WHEN 'autre' THEN 0 WHEN 'lock' THEN 1
                         WHEN 'paiement' THEN 2 ELSE 3 END, m.checkin_date
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("wl", "STRING", sorted(ALLOWED_PROPERTY_IDS))])
    return [dict(r.items()) for r in _bq().query(q, job_config=cfg).result()]


# Mail quotidien « résas sans code » — 1 section par cause, style aligné sur le
# brief annulations (cancellations_brief.py). Destinataire = ISEO_ALERT_TO.
_GAP_REASONS = {
    "autre": {
        "label": "⚠ Aurait dû être généré — à investiguer",
        "hint":  "Whitelisté, pre-checkin fait, paiement OK, serrure OK… mais aucun code. "
                 "Vérifier le tab 7.8 et les logs de l'orchestrateur.",
        "bg": "#fef2f2", "fg": "#dc2626",
    },
    "lock": {
        "label": "Serrure non résolue",
        "hint":  "Appart whitelisté sans lock/lockTag mappé — anormal, à corriger côté Sofia.",
        "bg": "#fef2f2", "fg": "#dc2626",
    },
    "paiement": {
        "label": "Paiement échoué — provision retenue",
        "hint":  "Tous les paiements Mews en Failed, aucun Charged (gate volontaire, "
                 "souvent VCC Expedia/VRBO pas encore chargeable). Le code fixe couvre.",
        "bg": "#fffbeb", "fg": "#b45309",
    },
    "precheckin": {
        "label": "Pre-checkin non rempli (CI ≤ J+1)",
        "hint":  "Pas de mapping Duve sans formulaire → relancer le guest. "
                 "Le code fixe couvre en attendant.",
        "bg": "#fffbeb", "fg": "#b45309",
    },
}


def _build_gaps_html(gaps: list[dict], paris_today: str) -> str:
    counts = {k: sum(1 for g in gaps if g["reason"] == k) for k in _GAP_REASONS}
    kpis = "".join(
        f'<div style="display:inline-block;background:#fff;border:1px solid #e2e8f0;'
        f'border-radius:6px;padding:10px 16px;margin:0 8px 8px 0">'
        f'<div style="font-size:11px;color:#64748b;text-transform:uppercase">{cfg["label"]}</div>'
        f'<div style="font-size:22px;font-weight:700;color:{cfg["fg"] if counts[k] else "#059669"}">{counts[k]}</div>'
        f'</div>'
        for k, cfg in _GAP_REASONS.items()
    )

    sections = ""
    for k, cfg in _GAP_REASONS.items():
        items = [g for g in gaps if g["reason"] == k]
        if not items:
            continue
        rows_html = "".join(
            f'<tr style="border-bottom:1px solid #f1f5f9">'
            f'<td style="padding:8px 12px;font-size:13px;color:#334155"><strong>{g.get("customer_name") or "—"}</strong></td>'
            f'<td style="padding:8px 12px;font-size:12px;color:#64748b;font-family:monospace">{g.get("apartment_code") or g.get("resource_id") or "—"}</td>'
            f'<td style="padding:8px 12px;font-size:12px;color:#64748b;white-space:nowrap">{g.get("checkin_date")} → {g.get("checkout_date")}</td>'
            f'<td style="padding:8px 12px;font-size:12px;color:#64748b">{g.get("reservation_number")}</td>'
            f'</tr>'
            for g in items
        )
        sections += (
            f'<div style="margin-top:20px">'
            f'<div style="display:inline-block;background:{cfg["bg"]};color:{cfg["fg"]};'
            f'padding:3px 10px;border-radius:4px;font-size:13px;font-weight:600">'
            f'{cfg["label"]} · {len(items)}</div>'
            f'<p style="font-size:12px;color:#94a3b8;margin:6px 0 8px">{cfg["hint"]}</p>'
            f'<table style="width:100%;border-collapse:collapse;background:white;'
            f'border:1px solid #e2e8f0;border-radius:6px;overflow:hidden">'
            f'<thead><tr style="background:#f8fafc;text-align:left">'
            f'<th style="padding:8px 12px;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600">Guest</th>'
            f'<th style="padding:8px 12px;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600">Appart</th>'
            f'<th style="padding:8px 12px;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600">Séjour</th>'
            f'<th style="padding:8px 12px;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600">Résa</th>'
            f'</tr></thead><tbody>{rows_html}</tbody></table>'
            f'</div>'
        )

    return f"""
<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="background:#f8fafc;padding:24px;font-family:-apple-system,Segoe UI,sans-serif;color:#0f172a">
  <div style="max-width:820px;margin:0 auto">
    <h1 style="font-size:22px;margin:0 0 4px">Codes d'accès ISEO — {paris_today}</h1>
    <p style="color:#64748b;font-size:14px;margin:0 0 20px">
      Résas whitelistées à provisionner (CI ≤ J+{LOOKAHEAD_DAYS}) toujours sans code, par cause.
    </p>
    <div style="margin-bottom:12px">{kpis}</div>
    {sections}
    <div style="margin-top:24px">
      <a href="https://direction.archides.fr/ops-front?tab=arrivals"
         style="background:#4f46e5;color:white;padding:10px 20px;border-radius:6px;
                text-decoration:none;font-size:14px;font-weight:600;display:inline-block">
        Voir les arrivées (6.1) →
      </a>
      <a href="https://direction.archides.fr/ops-back?tab=pin_pipeline"
         style="margin-left:8px;color:#4f46e5;padding:10px 12px;font-size:14px;
                text-decoration:none;font-weight:600;display:inline-block">
        Pipeline PIN (7.8)
      </a>
    </div>
    <p style="font-size:11px;color:#94a3b8;margin-top:32px">
      Mail généré automatiquement (run orchestrateur ~08:45 Paris, 1×/jour) ·
      merveil-action-engine-iseo · les états par arrivée sont aussi dans 6.1 (« Code d'accès ISEO »).
    </p>
  </div>
</body></html>
"""


def _resa_to_archive() -> list[dict]:
    """Rows actives à archiver : plus aucun stay live ne contient le duve du cache.
    `member_resas` exclut les annulées et les CO < today → un duve absent de TOUT stay
    = stay fini (CO passé) ou annulé → à archiver. DELETE device + invitation + user.
    Match par appartenance (member_duve_ids) : robuste au décalage de canonical si la
    résa la plus tôt d'un stay est annulée."""
    q = f"""
    WITH {_STAYS_CTE},
    stay_members AS (
      SELECT md AS duve_reservation_id FROM stays, UNNEST(member_duve_ids) md
    )
    SELECT c.duve_reservation_id, c.iseo_invitation_id, c.shadow_mode
    FROM `{PIN_CACHE_TABLE}` c
    LEFT JOIN stay_members sm ON sm.duve_reservation_id = c.duve_reservation_id
    WHERE c.archived_at IS NULL AND sm.duve_reservation_id IS NULL
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


def _resa_to_resync() -> list[dict]:
    """Rows actives dont la fenêtre cache ≠ fenêtre live du stay = drift post-provision
    (extension / raccourcissement / décalage OU fusion back-to-back : une nouvelle résa
    contiguë étend le stay) OU invitation manquante. Recrée device (même PIN) + invitation
    sur la fenêtre live. Match cache↔stay par canonical_duve (déterministe, 1:1)."""
    q = f"""
    WITH {_STAYS_CTE},
    cache AS (
      SELECT duve_reservation_id, pin_value, iseo_device_id, iseo_invitation_id,
             iseo_guest_tag_id, iseo_lock_id, iseo_lock_tag_id,
             mews_reservation_number, apartment_code, hold_reason, released_at,
             checkin_date AS cache_ci, checkout_date AS cache_co
      FROM `{PIN_CACHE_TABLE}`
      WHERE archived_at IS NULL AND provisioned_at IS NOT NULL
    )
    SELECT
      c.duve_reservation_id, c.pin_value, c.iseo_device_id, c.iseo_invitation_id,
      c.iseo_guest_tag_id, c.iseo_lock_id, c.iseo_lock_tag_id,
      c.cache_ci, c.cache_co, c.hold_reason AS cache_hold, c.released_at AS cache_released,
      c.mews_reservation_number, c.apartment_code,
      s.duve_property_id, s.customer_name,
      s.stay_ci AS live_ci, s.stay_co AS live_co,
      s.earliest_checkin_hour, s.latest_checkout_hour, s.member_duve_ids,
      s.min_lead_hours, s.direct_last_minute, s.direct_unpaid
    FROM cache c
    JOIN stays s ON s.canonical_duve = c.duve_reservation_id
    WHERE s.stay_ci != c.cache_ci OR s.stay_co != c.cache_co OR c.iseo_invitation_id IS NULL
    ORDER BY s.stay_ci
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


# ── State writers ───────────────────────────────────────────────────────────

def _save_provisioned(row: dict, pin_value: str, device_id: int,
                      inv_id: Optional[int], inv_code: Optional[str],
                      link: Optional[str], duve_ok: bool,
                      member_csv: Optional[str] = None,
                      hold_reason: Optional[str] = None) -> None:
    q = f"""
    INSERT INTO `{PIN_CACHE_TABLE}` (
      duve_reservation_id, mews_reservation_number, apartment_code, pin_value,
      iseo_guest_tag_id, iseo_lock_id, iseo_lock_tag_id, iseo_device_id,
      iseo_invitation_id, invitation_code, invitation_link,
      checkin_date, checkout_date, cached_at, provisioned_at, duve_pushed_at,
      shadow_mode, stay_member_duve_ids, hold_reason, held_at)
    VALUES (@duve, @num, @apt, @pin, @gtag, @lock, @ltag, @dev,
            @inv, @code, @link, @ci, @co, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
            {'CURRENT_TIMESTAMP()' if duve_ok else 'NULL'}, FALSE, @members, @hold,
            {'CURRENT_TIMESTAMP()' if hold_reason else 'NULL'})
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("duve", "STRING", row["duve_reservation_id"]),
        bigquery.ScalarQueryParameter("num", "STRING", row.get("mews_reservation_number")),
        bigquery.ScalarQueryParameter("apt", "STRING", row.get("apartment_code")),
        bigquery.ScalarQueryParameter("pin", "STRING", pin_value),
        bigquery.ScalarQueryParameter("gtag", "INT64", row.get("guest_tag_id")),
        bigquery.ScalarQueryParameter("lock", "INT64", row.get("lock_id")),
        bigquery.ScalarQueryParameter("ltag", "INT64", row.get("lock_tag_id")),
        bigquery.ScalarQueryParameter("dev", "INT64", device_id),
        bigquery.ScalarQueryParameter("inv", "INT64", inv_id),
        bigquery.ScalarQueryParameter("code", "STRING", inv_code),
        bigquery.ScalarQueryParameter("link", "STRING", link),
        bigquery.ScalarQueryParameter("ci", "DATE", str(row["checkin_date"])),
        bigquery.ScalarQueryParameter("co", "DATE", str(row["checkout_date"])),
        bigquery.ScalarQueryParameter("members", "STRING", member_csv),
        bigquery.ScalarQueryParameter("hold", "STRING", hold_reason),
    ])
    _bq().query(q, job_config=cfg).result()


def _save_resynced(duve_resa_id: str, ci: str, co: str, device_id: object,
                   inv_id: Optional[int], inv_code: Optional[str],
                   link: Optional[str], duve_ok: bool,
                   member_csv: Optional[str] = None,
                   hold_reason: Optional[str] = None) -> None:
    # `held_at` n'est posé qu'à la PREMIÈRE rétention (COALESCE) : un resync
    # successif ne doit pas rajeunir l'ancienneté d'une rétention en attente.
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET checkin_date = @ci, checkout_date = @co, iseo_device_id = @dev,
        iseo_invitation_id = @inv, invitation_code = @code, invitation_link = @link,
        provisioned_at = CURRENT_TIMESTAMP(),
        duve_pushed_at = {'CURRENT_TIMESTAMP()' if duve_ok else 'NULL'},
        stay_member_duve_ids = @members,
        hold_reason = @hold,
        held_at = {'COALESCE(held_at, CURRENT_TIMESTAMP())' if hold_reason else 'NULL'},
        last_error = NULL
    WHERE duve_reservation_id = @id AND archived_at IS NULL
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", duve_resa_id),
        bigquery.ScalarQueryParameter("ci", "DATE", ci),
        bigquery.ScalarQueryParameter("co", "DATE", co),
        bigquery.ScalarQueryParameter("dev", "INT64", device_id),
        bigquery.ScalarQueryParameter("inv", "INT64", inv_id),
        bigquery.ScalarQueryParameter("code", "STRING", inv_code),
        bigquery.ScalarQueryParameter("link", "STRING", link),
        bigquery.ScalarQueryParameter("members", "STRING", member_csv),
        bigquery.ScalarQueryParameter("hold", "STRING", hold_reason),
    ])
    _bq().query(q, job_config=cfg).result()


def _resa_duve_retry() -> list[dict]:
    """Rows provisionnées côté Sofia mais dont le push Duve a échoué
    (`duve_pushed_at IS NULL`) → à re-pousser (code + lien déjà en cache).

    ⚠ EXCLUT les rétentions volontaires. Une ligne retenue a exactement la même
    signature qu'un push raté (`provisioned_at` rempli, `duve_pushed_at` NULL) :
    sans ce filtre, le retry enverrait au client, au run suivant, le code que la
    porte vient de retenir — la porte serait silencieusement inopérante. Une
    rétention libérée (`released_at` posé) redevient éligible et part au run d'après.
    """
    q = f"""
    SELECT duve_reservation_id, pin_value, invitation_link, stay_member_duve_ids
    FROM `{PIN_CACHE_TABLE}`
    WHERE archived_at IS NULL AND provisioned_at IS NOT NULL AND duve_pushed_at IS NULL
      AND (hold_reason IS NULL OR released_at IS NOT NULL)
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


def _mark_duve_pushed(duve_resa_id: str) -> None:
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET duve_pushed_at = CURRENT_TIMESTAMP(), last_error = NULL
    WHERE duve_reservation_id = @id AND archived_at IS NULL
    """
    _bq().query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", duve_resa_id)])).result()


def _mark_archived(duve_resa_id: str, error: Optional[str] = None) -> None:
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET archived_at = CURRENT_TIMESTAMP(), last_error = @err
    WHERE duve_reservation_id = @id AND archived_at IS NULL
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", duve_resa_id),
        bigquery.ScalarQueryParameter("err", "STRING", error),
    ])
    _bq().query(q, job_config=cfg).result()


# ── Window calc (Paris tz) ────────────────────────────────────────────────────

def _hm_to_min(h: Optional[str]) -> Optional[int]:
    try:
        hh, mm = h[:5].split(":")
        return int(hh) * 60 + int(mm)
    except Exception:
        return None


WINDOW_FLOOR_CI = "07:00"
WINDOW_CEIL_CO = "23:59"


def _earliest_hour(policy: Optional[str], default: str) -> str:
    p, d = _hm_to_min(policy), _hm_to_min(default)
    chosen = default if p is None else (policy[:5] if p <= d else default)
    return WINDOW_FLOOR_CI if _hm_to_min(chosen) < _hm_to_min(WINDOW_FLOOR_CI) else chosen


def _latest_hour(policy: Optional[str], default: str) -> str:
    p, d = _hm_to_min(policy), _hm_to_min(default)
    chosen = default if p is None else (policy[:5] if p >= d else default)
    return WINDOW_CEIL_CO if _hm_to_min(chosen) > _hm_to_min(WINDOW_CEIL_CO) else chosen


def _build_window_ms(ci_date: str, co_date: str, ci_hour: str, co_hour: str) -> tuple[int, int]:
    ci_h, ci_m = (ci_hour[:5].split(":") + ["00"])[:2]
    co_h, co_m = (co_hour[:5].split(":") + ["00"])[:2]
    cy, cmo, cd = (int(x) for x in ci_date.split("-"))
    oy, omo, od = (int(x) for x in co_date.split("-"))
    ci_dt = datetime(cy, cmo, cd, int(ci_h), int(ci_m), tzinfo=PARIS_TZ)
    co_dt = datetime(oy, omo, od, int(co_h), int(co_m), tzinfo=PARIS_TZ)
    return int(ci_dt.timestamp() * 1000), int(co_dt.timestamp() * 1000)


# ── Guest user dédié (un par résa, au vrai nom) ────────────────────────────────

def _split_name(name: Optional[str]) -> tuple[str, str]:
    parts = (name or "").strip().split()
    if not parts:
        return "Merveil", "Guest"
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def _user_tag_id(user: dict) -> Optional[int]:
    """Le tag de type 'user' auto-créé avec le user (= le seul valide comme guestTagId)."""
    for t in (user.get("tags") or []):
        if t.get("type") == "user" and t.get("id"):
            return t["id"]
    return None


def _get_or_create_guest_user(duve_resa_id: str, guest_name: Optional[str]) -> tuple[Optional[int], Optional[str]]:
    """Get-or-create (par extId) un user Sofia dédié à la résa, au vrai nom du guest.
    Retourne (guest_tag_id, error). Le user est créé avec un password aléatoire pour
    être enabled=True (sinon il est GC / perd son tag). Le password n'est jamais
    partagé — le guest ouvre au PIN clavier + lien remote-open."""
    ext = f"MERVEIL_USER - {duve_resa_id}"
    g = _sofia("GET", f"/api/v2/users/extId/{ext}")
    if g.status_code == 200:
        tag = _user_tag_id(g.json())
        return (tag, None) if tag else (None, "user existant sans tag 'user'")
    if g.status_code != 404:  # 401/500… ≠ absent : ne PAS créer un doublon
        return None, f"user GET HTTP {g.status_code}"

    fn, ln = _split_name(guest_name)
    email = f"resa-{duve_resa_id}@guest.archides.fr"  # unique par résa, jamais utilisé
    r = _sofia("POST", "/api/v2/users", json_body={
        "username": email, "email": email, "password": "Mv!" + secrets.token_urlsafe(16),
        "firstname": fn, "lastname": ln, "roleIds": [5], "extId": ext})
    if r.status_code not in (200, 201):
        return None, f"user POST HTTP {r.status_code}: {r.text[:200]}"
    body = r.json()
    if body.get("enabled") is False:
        # Contrat empirique password→enabled : si Sofia le change, le user sera GC
        # en minutes (perte du tag) → PIN cassé. On le signale au lieu de subir.
        logger.warning(f"⚠️ user {ext} créé enabled=False (comportement password→enabled changé ?)")
    tag = _user_tag_id(body)
    if tag is None:  # fallback : relire le user pour récupérer son tag
        g2 = _sofia("GET", f"/api/v2/users/extId/{ext}")
        tag = _user_tag_id(g2.json()) if g2.status_code == 200 else None
    if tag is None:
        return None, "user créé sans tag 'user'"
    logger.info(f"✅ user guest créé '{(fn + ' ' + ln).strip()}' → tag {tag}")
    return tag, None


# ── Provision (A→E) ───────────────────────────────────────────────────────────

def _evaluate_hold(row: dict) -> Optional[str]:
    """Motif de rétention du code, ou None si le code peut partir chez le client.

    Deux critères, tous deux restreints au canal DIRECT (recalibrage 15/08) :
      - réservé ≤ ISEO_HOLD_LEAD_HOURS (72 h) avant l'arrivée — ~8 résas/mois, c'est
        le critère qui porte la valeur : les 4 fraudes d'août sont toutes en direct,
        3 réservées le jour même et la 4ᵉ (Bossongo) à J-2 ;
      - rien d'encaissé (cf. commentaire du CTE).

    ⚠ Le critère last-minute était initialement TOUS CANAUX (≤24 h). Restreint au
    direct le 15/08 : mesuré, 87 % des résas du jour même sont des OTA, payées à
    l'OTA (moyen de paiement vérifié, recours possible) et absentes des 4 fraudes —
    retenir leur code, c'est un client dehors le soir pour un gain de sécurité nul.

    ⚠ Volontairement PAS conditionné à « pièce d'identité scannée » : scanner une
    pièce coûte 30 secondes à un fraudeur (n'importe quel document passe l'OCR, et
    un cas de fausse pièce est avéré côté Merveil), donc en faire une condition de
    libération rendrait la porte contournable par une action que l'attaquant
    contrôle. La pièce sert au triage humain au moment de libérer.
    """
    if ISEO_HOLD_MODE == "off":
        return None
    motifs = []
    if row.get("direct_last_minute"):
        lead = row.get("min_lead_hours")
        detail = f" ({int(lead)}h avant l'arrivée)" if lead is not None else ""
        motifs.append(f"direct réservé au dernier moment{detail}")
    if row.get("direct_unpaid"):
        motifs.append("direct sans encaissement")
    return " + ".join(motifs) if motifs else None


def _log_hold_decision(row: dict, motif: str, phase: str, outcome: str) -> None:
    """Journalise UNE décision de la porte dans `iseo_raw.hold_decisions` (append-only).

    ⚠ Pourquoi une table à part et pas `hold_reason` dans le cache : en mode `observe`
    le cache ne porte PAS de `hold_reason` (« retenu » y garde un sens strict), et on ne
    peut pas l'y écrire sans casser `_resa_duve_retry`, qui filtre précisément dessus —
    un push Duve réellement raté ne serait alors plus jamais retenté. Cette table capture
    donc ce que le cache ne peut pas dire : les décisions en `observe`, et celles dont la
    résa est ensuite skippée (whitelist/lock/paiement) et qui n'envoient aucun mail.

    Best-effort : une écriture ratée ne doit jamais faire échouer un provisioning.
    """
    try:
        q = f"""
        INSERT INTO `{HOLD_DECISIONS_TABLE}` (
          evaluated_at, phase, hold_mode, outcome,
          duve_reservation_id, duve_property_id, apartment_code, customer_name,
          mews_reservation_number, checkin_date, checkout_date,
          hold_reason, direct_last_minute, direct_unpaid, min_lead_hours,
          hold_lead_hours_setting)
        VALUES (CURRENT_TIMESTAMP(), @phase, @mode, @outcome,
          @duve, @pid, @apt, @name, @resa,
          SAFE_CAST(@ci AS DATE), SAFE_CAST(@co AS DATE),
          @reason, @dlm, @du, @lead, @setting)
        """
        params = [
            bigquery.ScalarQueryParameter("phase", "STRING", phase),
            bigquery.ScalarQueryParameter("mode", "STRING", ISEO_HOLD_MODE),
            bigquery.ScalarQueryParameter("outcome", "STRING", outcome),
            bigquery.ScalarQueryParameter("duve", "STRING", row.get("duve_reservation_id")),
            bigquery.ScalarQueryParameter("pid", "STRING", row.get("duve_property_id")),
            bigquery.ScalarQueryParameter("apt", "STRING", row.get("apartment_code")),
            bigquery.ScalarQueryParameter("name", "STRING", row.get("customer_name")),
            bigquery.ScalarQueryParameter("resa", "STRING",
                                          row.get("mews_reservation_number")),
            # provision porte checkin_date/checkout_date, resync live_ci/live_co
            bigquery.ScalarQueryParameter(
                "ci", "STRING", str(row.get("checkin_date") or row.get("live_ci") or "") or None),
            bigquery.ScalarQueryParameter(
                "co", "STRING", str(row.get("checkout_date") or row.get("live_co") or "") or None),
            bigquery.ScalarQueryParameter("reason", "STRING", motif),
            bigquery.ScalarQueryParameter("dlm", "BOOL", bool(row.get("direct_last_minute"))),
            bigquery.ScalarQueryParameter("du", "BOOL", bool(row.get("direct_unpaid"))),
            bigquery.ScalarQueryParameter(
                "lead", "INT64",
                int(row["min_lead_hours"]) if row.get("min_lead_hours") is not None else None),
            bigquery.ScalarQueryParameter("setting", "INT64", ISEO_HOLD_LEAD_HOURS),
        ]
        _bq().query(q, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    except Exception as e:
        logger.warning(f"⚠️ hold_decisions : écriture échouée — {type(e).__name__}: {e}")


def _notify_hold(row: dict, motif: str, suffix: str = "") -> None:
    """Prévient la RC qu'une résa est jugée à risque par la porte.

    ⚠ Envoyé dans les modes `on` ET `observe` (décision 15/08) : toute rétention
    DOIT être doublée d'une alerte, sinon la porte transforme une fraude évitée en
    client dehors à 22 h — et en `observe`, sans ce mail, personne n'apprenait
    qu'une résa avait été jugée à risque. Le mail dit explicitement, dans chaque
    mode, si le client a le code ou non : ce sont deux gestes RC opposés.
    """
    effectif = ISEO_HOLD_MODE == "on"
    apt = row.get("apartment_code") or row.get("duve_property_id")
    # Le provision porte checkin_date/checkout_date, le resync live_ci/live_co.
    ci = row.get("checkin_date") or row.get("live_ci")
    co = row.get("checkout_date") or row.get("live_co")
    if effectif:
        titre, sujet = "Code d'accès retenu — à valider", "🔒 Code retenu à valider"
        etat = ("Le code a été <strong>créé côté serrure mais volontairement pas envoyé</strong> "
                "au client : il ne le voit pas dans son application.")
        suite = ("Après vérification de l'identité, lire le code sur le dashboard et "
                 "l'envoyer au client. En cas de doute, ne rien envoyer et faire "
                 "annuler la réservation.")
    else:
        titre, sujet = "Réservation à risque — code déjà envoyé", "⚠️ Résa à risque (code envoyé)"
        etat = ("La porte de validation est en <strong>mode observation</strong> : le code "
                "<strong>a bien été envoyé au client</strong>, il peut entrer.")
        suite = ("Vérifier l'identité du client. En cas de doute, faire annuler la "
                 "réservation et changer le code de l'appartement avant l'arrivée.")
    html = build_email(
        titre,
        subtitle=f"{row.get('customer_name')} · {apt}{suffix}",
        severity="warning",
        intro=f"{etat}<br><strong>Motif :</strong> {esc(motif)}",
        table={"headers": ["Client", "Appartement", "Séjour", "Résa Mews"],
               "rows": [[esc(row.get("customer_name")), esc(apt),
                         f"{esc(ci)} → {esc(co)}",
                         esc(row.get("mews_reservation_number"))]]},
        sections_html=('<div style="padding:0 24px 8px;font-size:14px;color:#475569">'
                       f"{suite}</div>"),
        button=("Voir les arrivées →",
                "https://direction.archides.fr/ops-front?tab=arrivees"))
    send_mail(f"{sujet} — {row.get('customer_name')} ({apt})",
              html, ISEO_HOLD_ALERT_TO, html=True, sender=GMAIL_SENDER)


def _provision(row: dict) -> tuple[bool, Optional[str]]:
    duve_resa_id = row["duve_reservation_id"]

    apt_pid = (row.get("duve_property_id") or "").lower()
    if ALLOWED_PROPERTY_IDS and apt_pid not in ALLOWED_PROPERTY_IDS:
        return False, "skipped: whitelist"
    if row.get("lock_tag_id") is None or row.get("lock_id") is None:
        return False, "skipped: lock non résolue"
    if row.get("payment_unpaid"):
        return False, "skipped: paiement non validé"

    ci_date = row.get("checkin_date")
    co_date = row.get("checkout_date")
    if ci_date is None or co_date is None:
        return False, "missing CI/CO date"
    ci_str, co_str = str(ci_date), str(co_date)
    ci_hour = _earliest_hour(row.get("earliest_checkin_hour"), DEFAULT_CI_HOUR)
    co_hour = _latest_hour(row.get("latest_checkout_hour"), DEFAULT_CO_HOUR)
    ci_ms, co_ms = _build_window_ms(ci_str, co_str, ci_hour, co_hour)
    if co_ms < int(time.time() * 1000):
        return False, "skipped: checkout déjà passé (sera archivé demain)"

    win = {"from": ci_ms, "to": co_ms}
    pin_ext = f"MERVEIL_RESA - {duve_resa_id}"
    inv_ext = f"MERVEIL_INV - {duve_resa_id}"

    logger.info(
        f"→ provision {duve_resa_id} ({row.get('apartment_code')}) "
        f"window={datetime.fromtimestamp(ci_ms/1000, timezone.utc).isoformat()} → "
        f"{datetime.fromtimestamp(co_ms/1000, timezone.utc).isoformat()}")

    if ISEO_SHADOW_MODE:
        logger.info(f"🌗 SHADOW {duve_resa_id}: would provision (skip Sofia/Duve/state)")
        return True, None

    # User dédié à la résa (vrai nom du guest) → son tag 'user' = guest tag du PIN.
    tag_id, uerr = _get_or_create_guest_user(duve_resa_id, row.get("customer_name"))
    if tag_id is None:
        return False, f"guest user creation failed: {uerr}"
    row["guest_tag_id"] = tag_id

    # A+B. device (get-or-create par extId → idempotent sur retry partiel)
    pin_value, device_id = _get_or_create_device(row, pin_ext, win)
    if pin_value is None:
        return False, f"device creation failed: {device_id}"  # device_id porte l'erreur

    # C. invitation (get-or-create)
    inv_id, inv_code = _get_or_create_invitation(row, inv_ext, win)
    link = f"https://{REMOTE_OPEN_HOST}/remoteOpen?code={inv_code}" if inv_code else None

    # D. Duve push (code clavier + lien) — à TOUS les duve du stay (back-to-back),
    #    SAUF si la porte retient : le code existe alors côté Sofia (donc lisible au
    #    dashboard et révocable) mais le client ne le voit pas.
    members = row.get("member_duve_ids") or [duve_resa_id]
    hold = row.get("hold_reason")  # posé par le caller (déjà évalué pour le log)
    if hold and ISEO_HOLD_MODE == "on":
        duve_ok, duve_err = False, None
        logger.warning(f"🔒 HOLD {duve_resa_id} ({row.get('apartment_code')}) — {hold} "
                       f"→ code créé, PAS envoyé à Duve")
    else:
        duve_ok, duve_err = _duve_push_all(members, pin_value, link or "")
        if not duve_ok:
            logger.warning(f"⚠️ Duve push failed for {duve_resa_id}: {duve_err}")

    # E. état
    _save_provisioned(row, pin_value, device_id, inv_id, inv_code, link, duve_ok,
                      member_csv=",".join(members),
                      hold_reason=hold if ISEO_HOLD_MODE == "on" else None)
    if hold:
        _notify_hold(row, hold)  # `observe` compris — cf. docstring de _notify_hold
    if hold and ISEO_HOLD_MODE == "on":
        return True, None  # rétention volontaire : ce n'est PAS une erreur de run
    if not duve_ok:
        return False, f"Sofia OK mais Duve KO: {duve_err}"
    return True, None


def _device_payload(row: dict, pin_ext: str, win: dict) -> dict:
    """Payload POST /standardDevices sans deviceId (= le code, ajouté par _post_device)."""
    return {
        "type": "ISEO_PIN", "extId": pin_ext, "notes": pin_ext,
        "validationMode": "ONE_HOUR_VALIDATION", "validationPeriod": 24,
        "additionalCredentialRules": [],
        "credentialRule": {
            "name": pin_ext, "description": "merveil_dwh_v3",
            "lockTagIds": [int(row["lock_tag_id"])], "lockTagMatchingMode": "AT_LEAST_ONE_TAG",
            "guestTagIds": [int(row["guest_tag_id"])], "guestTagMatchingMode": "EVERY_TAG",
            "daysOfTheWeek": [1, 2, 3, 4, 5, 6, 7],
            "dateInterval": win, "timeInterval": {"from": 0, "to": 86340},
            "alwaysOpen": False, "holidays": True, "openOnPrivacy": False},
    }


def _post_device(row: dict, pin_ext: str, win: dict,
                 pin_value: Optional[str] = None) -> tuple[Optional[str], object]:
    """POST un device. Si pin_value fourni (resync) on tente de réutiliser le même
    code (libéré par le DELETE qui précède) ; sinon on génère un code 4 chiffres
    unique account-wide (retry sur collision)."""
    payload = _device_payload(row, pin_ext, win)
    if pin_value is not None:
        payload["deviceId"] = pin_value
        r = _sofia("POST", "/api/v2/standardDevices", json_body=payload)
        if r.status_code in (200, 201):
            return pin_value, r.json().get("id")
        if "already present" not in r.text.lower():
            return None, f"HTTP {r.status_code}: {r.text[:200]}"
        logger.warning(f"⚠️ code {pin_value} repris entre-temps → régénération")
    last_err = None
    for _ in range(PIN_COLLISION_RETRIES):
        pv = f"{secrets.randbelow(10000):04d}"
        payload["deviceId"] = pv
        r = _sofia("POST", "/api/v2/standardDevices", json_body=payload)
        if r.status_code in (200, 201):
            logger.info(f"✅ device créé id={r.json().get('id')} PIN={pv}")
            return pv, r.json().get("id")
        if "already present" in r.text.lower():
            last_err = "code collision"
            continue
        return None, f"HTTP {r.status_code}: {r.text[:200]}"
    return None, last_err or "no free PIN"


def _get_or_create_device(row: dict, pin_ext: str, win: dict) -> tuple[Optional[str], object]:
    """Retourne (pin_value, device_id). Réutilise le device existant (même extId)
    s'il existe (retry partiel). Sinon génère un code 4 chiffres unique."""
    g = _sofia("GET", f"/api/v2/standardDevices/extId/{pin_ext}")
    if g.status_code == 200:
        d = g.json()
        return str(d.get("deviceId")), d.get("id")
    if g.status_code != 404:  # ≠ absent : ne pas créer un doublon device
        return None, f"device GET {g.status_code}: {g.text[:120]}"
    return _post_device(row, pin_ext, win)


def _get_or_create_invitation(row: dict, inv_ext: str, win: dict) -> tuple[Optional[int], Optional[str]]:
    g = _sofia("GET", f"/api/v2/invitations/extId/{inv_ext}")
    if g.status_code == 200:
        d = g.json()
        return d.get("id"), d.get("code")
    if g.status_code != 404:  # ≠ absent : ne pas créer un doublon invitation
        logger.warning(f"⚠️ invitation GET {inv_ext}: HTTP {g.status_code}")
        return None, None
    r = _sofia("POST", "/api/v2/invitations", json_body={
        "name": inv_ext, "extId": inv_ext, "smartLockIds": [int(row["lock_id"])],
        "daysOfTheWeek": [1, 2, 3, 4, 5, 6, 7],
        "dateInterval": win, "timeInterval": {"from": 0, "to": 86340},
        "numberOfDevices": 0})
    if r.status_code in (200, 201):
        d = r.json()
        logger.info(f"✅ invitation id={d.get('id')} code={d.get('code')}")
        return d.get("id"), d.get("code")
    logger.warning(f"⚠️ invitation KO {inv_ext}: HTTP {r.status_code} {r.text[:200]}")
    return None, None


def _archive(row: dict) -> tuple[bool, Optional[str]]:
    """DELETE Sofia device (par extId) + DELETE invitation (par id) + DELETE le user
    dédié de la résa (par extId)."""
    duve_resa_id = row["duve_reservation_id"]
    if bool(row.get("shadow_mode")):
        # Row provisionnée en shadow (aucun device Sofia) → mark archived OK, rien à supprimer.
        logger.info(f"🌗 SHADOW row {duve_resa_id}: archive (pas d'appel Sofia)")
        return True, None
    if ISEO_SHADOW_MODE:
        # Shadow GLOBAL sur une row LIVE : NE PAS marquer archived (sinon le PIN Sofia
        # n'est jamais supprimé mais la row est figée → code valide résiduel). On laisse
        # la row active pour qu'elle soit réellement archivée dès que shadow repasse off.
        logger.info(f"🌗 SHADOW global {duve_resa_id}: would archive (row live laissée active)")
        return False, "skipped: shadow global (row live)"

    errs = []
    # device (à supprimer avant le user — il l'ancre)
    g = _sofia("GET", f"/api/v2/standardDevices/extId/MERVEIL_RESA - {duve_resa_id}")
    if g.status_code == 200:
        sid = g.json().get("id")
        rd = _sofia("DELETE", f"/api/v2/standardDevices/{sid}")
        if rd.status_code not in (200, 204):
            errs.append(f"device DELETE {rd.status_code}")
    elif g.status_code != 404:
        errs.append(f"device GET {g.status_code}")
    # invitation
    inv_id = row.get("iseo_invitation_id")
    if inv_id:
        ri = _sofia("DELETE", f"/api/v2/invitations/{int(inv_id)}")
        if ri.status_code not in (200, 204, 404):
            errs.append(f"inv DELETE {ri.status_code}")
    # user dédié (sinon accumulation de users guest)
    gu = _sofia("GET", f"/api/v2/users/extId/MERVEIL_USER - {duve_resa_id}")
    if gu.status_code == 200:
        ru = _sofia("DELETE", f"/api/v2/users/{gu.json().get('id')}")
        if ru.status_code not in (200, 204, 404):
            errs.append(f"user DELETE {ru.status_code}")
    elif gu.status_code != 404:
        errs.append(f"user GET {gu.status_code}")
    if errs:
        return False, "; ".join(errs)
    logger.info(f"🗑️ archived {duve_resa_id} (stay terminé / annulé)")
    return True, None


def _resync(row: dict) -> tuple[bool, Optional[str]]:
    """Resync window Sofia après drift de dates : DELETE device+invitation puis
    re-POST avec la window live + le MÊME code PIN (le guest garde son code clavier ;
    le lien remote-open change car nouvelle invitation). UPDATE l'état cache."""
    duve_resa_id = row["duve_reservation_id"]
    apt_pid = (row.get("duve_property_id") or "").lower()
    if ALLOWED_PROPERTY_IDS and apt_pid not in ALLOWED_PROPERTY_IDS:
        return False, "skipped: whitelist"

    ci_str, co_str = str(row["live_ci"]), str(row["live_co"])
    ci_hour = _earliest_hour(row.get("earliest_checkin_hour"), DEFAULT_CI_HOUR)
    co_hour = _latest_hour(row.get("latest_checkout_hour"), DEFAULT_CO_HOUR)
    ci_ms, co_ms = _build_window_ms(ci_str, co_str, ci_hour, co_hour)
    if co_ms < int(time.time() * 1000):
        return False, "skipped: checkout passé (sera archivé)"
    win = {"from": ci_ms, "to": co_ms}

    if ISEO_SHADOW_MODE:
        logger.info(f"🌗 SHADOW {duve_resa_id}: would resync window → {ci_str}→{co_str}")
        return True, None

    # Adapter les clés cache → clés attendues par les helpers partagés.
    row["lock_tag_id"] = row.get("iseo_lock_tag_id")
    row["lock_id"] = row.get("iseo_lock_id")
    # Guest tag = tag 'user' du user dédié de la résa (get-or-create idempotent).
    tag_id, uerr = _get_or_create_guest_user(duve_resa_id, row.get("customer_name"))
    if tag_id is None:
        return False, f"resync guest user failed: {uerr}"
    row["guest_tag_id"] = tag_id
    if row["lock_tag_id"] is None or row["lock_id"] is None:
        return False, "resync impossible: ids appart manquants en cache"

    pin_ext = f"MERVEIL_RESA - {duve_resa_id}"
    inv_ext = f"MERVEIL_INV - {duve_resa_id}"

    # Drift de dates ? Sinon on est ici pour réparer une invitation manquante → NE PAS
    # toucher au device (éviter une fenêtre de lockout ≤2h si le re-POST échoue après
    # le DELETE, et le churn Sofia). Le device n'a besoin d'être refait que si la window
    # a changé.
    has_drift = (str(row["live_ci"]) != str(row.get("cache_ci"))
                 or str(row["live_co"]) != str(row.get("cache_co")))

    if has_drift:
        # 1. DELETE device puis re-POST avec la window live (même code PIN).
        g = _sofia("GET", f"/api/v2/standardDevices/extId/{pin_ext}")
        if g.status_code == 200:
            rd = _sofia("DELETE", f"/api/v2/standardDevices/{g.json().get('id')}")
            if rd.status_code not in (200, 204):
                return False, f"resync device DELETE {rd.status_code}"
        elif g.status_code != 404:
            return False, f"resync device GET {g.status_code}"
        pin_value, device_id = _post_device(row, pin_ext, win, pin_value=row.get("pin_value"))
        if pin_value is None:
            return False, f"resync device re-POST failed: {device_id}"
    else:
        # Invitation-only : device intact (window déjà correcte), on garde son état cache.
        pin_value, device_id = str(row.get("pin_value")), row.get("iseo_device_id")

    # Invitation : DELETE l'ancienne (si présente) + recréer (nouveau code/lien). Sans la
    # vérif du DELETE, un échec ferait réutiliser l'ancienne invitation (window périmée).
    if row.get("iseo_invitation_id"):
        ri = _sofia("DELETE", f"/api/v2/invitations/{int(row['iseo_invitation_id'])}")
        if ri.status_code not in (200, 204, 404):
            return False, f"resync invitation DELETE {ri.status_code}"
    inv_id, inv_code = _get_or_create_invitation(row, inv_ext, win)
    link = f"https://{REMOTE_OPEN_HOST}/remoteOpen?code={inv_code}" if inv_code else None

    # 3. Duve push (code identique, lien neuf) — à tous les duve du stay.
    #    ⚠ La porte est RÉ-ÉVALUÉE ici, sur les dates LIVE. Sinon : réserver à J+5
    #    (la porte laisse passer), recevoir le code à J-3, puis avancer les dates à
    #    aujourd'hui — le resync repousserait le code sans aucun contrôle. Une
    #    rétention déjà libérée à la main n'est pas re-fermée (released_at présent).
    members = row.get("member_duve_ids") or [duve_resa_id]
    hold = None
    if not row.get("cache_released"):
        hold = _evaluate_hold(row)
    if hold and ISEO_HOLD_MODE == "on":
        duve_ok, duve_err = False, None
        logger.warning(f"🔒 HOLD au resync {duve_resa_id} ({row.get('apartment_code')}) — "
                       f"{hold} → nouveau lien PAS envoyé à Duve")
    else:
        duve_ok, duve_err = _duve_push_all(members, pin_value, link or "")

    # 4. état
    _save_resynced(duve_resa_id, ci_str, co_str, device_id, inv_id, inv_code, link, duve_ok,
                   member_csv=",".join(members),
                   hold_reason=hold if ISEO_HOLD_MODE == "on" else None)
    if hold:
        _log_hold_decision(row, hold, "resync",
                           "held" if ISEO_HOLD_MODE == "on" else "pushed_observe")
    if hold and not row.get("cache_hold"):  # nouvelle rétention née du changement de dates
        _notify_hold(row, hold, suffix=" — après modification des dates")
    if hold and ISEO_HOLD_MODE == "on":
        logger.info(f"🔄 resync {duve_resa_id} window → {ci_str}→{co_str} (code retenu)")
        return True, None
    if not duve_ok:
        return False, f"resync Sofia OK mais Duve KO: {duve_err}"
    logger.info(f"🔄 resync {duve_resa_id} window → {ci_str}→{co_str}")
    return True, None


# ── Purge des DUVE_PIN natifs orphelins ────────────────────────────────────────

def _native_duve_pins_to_purge() -> list[dict]:
    """DUVE_PIN natifs encore vivants en Sofia alors que la résa est annulée ou
    déjà checked-out, SUR LES APPARTS CUTOVER UNIQUEMENT (whitelist). L'intégration
    native est coupée → plus personne ne supprime ces PIN à l'annulation/au départ,
    laissant un code valide à un guest qui ne devrait plus entrer.

    ⚠️ Scope STRICT à la whitelist : sur les ~120 apparts non cutover, le DUVE_PIN
    natif reste l'UNIQUE code du guest — ne JAMAIS purger en dehors de la whitelist.
    On ne touche pas non plus les résas ACTIVE (leur DUVE_PIN double notre code mais
    reste l'unique code des résas >J-3 pas encore provisionnées) — nettoyées au CO."""
    if not ALLOWED_PROPERTY_IDS:
        return []
    q = f"""
    WITH {_DUVE_LATEST_CTE},
    dpin AS (
      SELECT ext_id, duve_reservation_id, user_firstname, user_lastname, active_from
      FROM `{STD_DEVICES_TABLE}`
      WHERE ext_id LIKE 'DUVE_PIN - %' AND deleted = FALSE AND is_present_in_latest_snapshot
    ),
    m AS (
      SELECT customer_id, resource_id, reservation_number, is_cancelled, checkin_date, checkout_date
      FROM `{MEWS_FCT_TABLE}`
    ),
    -- Résas actives (non annulées, CO à venir) par guest×appart. Si une existe, le
    -- DUVE_PIN peut être l'unique code d'une résa JUMELLE active (rebooking Mews
    -- cancel+recreate mêmes dates) → NE PAS purger, sinon guest sans code jusqu'à J-3.
    -- Le WHERE seul ne protège pas ce cas : le jumeau actif (CO futur) est filtré,
    -- seul le jumeau annulé passe → purge à tort. D'où ce garde anti-jointure.
    active AS (
      SELECT DISTINCT customer_id, resource_id
      FROM `{MEWS_FCT_TABLE}`
      WHERE NOT COALESCE(is_cancelled, FALSE) AND checkout_date >= CURRENT_DATE()
    )
    SELECT p.ext_id, p.duve_reservation_id,
           TRIM(CONCAT(COALESCE(p.user_firstname,''),' ',COALESCE(p.user_lastname,''))) AS guest,
           m.reservation_number,
           CASE WHEN m.is_cancelled THEN 'cancelled' ELSE 'checked_out' END AS reason
    FROM dpin p
    JOIN duve_latest d ON d.duve_reservation_id = p.duve_reservation_id
    JOIN m ON m.customer_id = d.mews_customer_id AND m.resource_id = d.duve_property_id
    LEFT JOIN active a ON a.customer_id = d.mews_customer_id AND a.resource_id = d.duve_property_id
    WHERE LOWER(d.duve_property_id) IN UNNEST(@wl)
      AND (m.is_cancelled OR m.checkout_date < CURRENT_DATE())
      AND a.customer_id IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.ext_id
      ORDER BY ABS(DATE_DIFF(m.checkin_date, DATE(p.active_from), DAY))) = 1
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("wl", "STRING", sorted(ALLOWED_PROPERTY_IDS))])
    return [dict(r.items()) for r in _bq().query(q, job_config=cfg).result()]


def _purge_native_orphan(row: dict) -> tuple[bool, Optional[str]]:
    """DELETE le DUVE_PIN natif (par extId)."""
    ext = row["ext_id"]
    if ISEO_SHADOW_MODE:
        logger.info(f"🌗 SHADOW: would purge {ext} ({row.get('reason')}, {row.get('guest')})")
        return True, None
    g = _sofia("GET", f"/api/v2/standardDevices/extId/{ext}")
    if g.status_code == 404:
        return True, None  # déjà supprimé
    if g.status_code != 200:
        return False, f"GET {g.status_code}"
    rd = _sofia("DELETE", f"/api/v2/standardDevices/{g.json().get('id')}")
    if rd.status_code not in (200, 204, 404):
        return False, f"DELETE {rd.status_code}"
    logger.info(f"🧹 purged DUVE_PIN {ext} ({row.get('reason')}, résa {row.get('reservation_number')}, {row.get('guest')})")
    return True, None


# ── Entry point ───────────────────────────────────────────────────────────────

def _load_whitelist() -> set:
    """Whitelist des GUID property_id depuis le seed BQ iseo_whitelisted_apartments.
    Fallback sur ISEO_ALLOWED_PROPERTY_IDS (env) si le seed est vide OU inaccessible —
    garde-fou : jamais élargir ni vider la whitelist par accident sur un incident BQ.
    Le seed est le point unique d'élargissement (aussi lu par les 2 modèles dbt)."""
    try:
        # TRIM + exige les 2 colonnes non vides : une ligne `,<guid>` (apartment_code
        # vide) provisionnerait un appart non surveillé par les modèles dbt ; un GUID
        # avec espace parasite sortirait silencieusement du pipeline.
        rows = _bq().query(
            f"SELECT DISTINCT TRIM(LOWER(property_id)) AS pid FROM `{WHITELIST_TABLE}` "
            f"WHERE COALESCE(TRIM(property_id), '') != '' "
            f"  AND COALESCE(TRIM(apartment_code), '') != ''").result()
        pids = {r["pid"] for r in rows if r["pid"]}
        if pids:
            return pids
        logger.warning("⚠️ whitelist seed BQ vide → fallback env var")
    except Exception as e:
        logger.warning(f"⚠️ whitelist seed BQ inaccessible ({e}) → fallback env var")
    if not ALLOWED_PROPERTY_IDS:
        # Seed ET env vides : un set vide = allow-all sur _provision/_resync (l'inverse
        # de la purge, gatée elle). On refuse de provisionner tout le parc → CRASH (mail).
        raise RuntimeError("whitelist ISEO vide (seed BQ + env var) — refus de provisionner")
    return ALLOWED_PROPERTY_IDS


def run() -> None:
    """Wrapper : tout crash → alerte mail + exit non-zero (visible Cloud Run)."""
    try:
        _run_inner()
    except Exception as e:
        logger.critical(f"🔴 ISEO orchestrator CRASH: {e}")
        _send_alert(
            "🔴 ISEO orchestrator — CRASH",
            build_email(
                "ISEO orchestrator — CRASH", severity="critical",
                subtitle=datetime.now(PARIS_TZ).strftime("%d/%m/%Y %H:%M"),
                intro="Le job a planté avant la fin — provisions/archives non "
                      "traitées sur ce run (retentées au prochain run 2h).<br><br>"
                      f'<pre style="background:#f8fafc;border:1px solid #e2e8f0;'
                      f'border-radius:6px;padding:12px;font-size:12px;color:#dc2626;'
                      f'white-space:pre-wrap">{esc(e)}</pre>',
            ),
            html=True)
        raise


def _run_inner() -> None:
    global ALLOWED_PROPERTY_IDS
    ALLOWED_PROPERTY_IDS = _load_whitelist()
    logger.info("=" * 70)
    logger.info(f"🚀 ISEO Orchestrator V3 (shadow={ISEO_SHADOW_MODE}, "
                f"whitelist={len(ALLOWED_PROPERTY_IDS)} property_ids depuis le seed BQ)")
    logger.info("=" * 70)
    errors: list[str] = []

    # 1. Provision (J-3)
    to_provision = _resa_to_provision()
    logger.info(f"📋 {len(to_provision)} résa(s) à provisionner (CI dans 0-{LOOKAHEAD_DAYS}j, pas encore couvertes)")
    ok = skip = held = 0
    for row in to_provision:
        # Porte évaluée AVANT le provision : en mode `observe` on journalise sans
        # retenir, ce qui permet de mesurer le volume réel et de repérer un faux
        # positif coûteux avant de passer en `on`. La RC est alertée dans les deux
        # modes (le mail est envoyé par `_provision`, cf. `_notify_hold`).
        row["hold_reason"] = _evaluate_hold(row)
        if row["hold_reason"]:
            held += 1
            if ISEO_HOLD_MODE == "observe":
                logger.info(f"🌗 HOLD-OBSERVE {row['duve_reservation_id']} "
                            f"({row.get('apartment_code')}, {row.get('customer_name')}) — "
                            f"{row['hold_reason']} → aurait été retenu, code envoyé quand même")
        try:
            success, err = _provision(row)
        except Exception as e:
            success, err = False, f"exception: {e}"
        # Journal de la porte — APRÈS le provision, pour enregistrer ce qui est
        # réellement arrivé au code (retenu / parti quand même / skippé avant la
        # porte / erreur). C'est la seule trace des décisions qui n'envoient pas de
        # mail, cf. `_log_hold_decision`.
        if row["hold_reason"]:
            if err and str(err).startswith("skipped"):
                outcome = str(err)
            elif not success:
                outcome = f"error: {err}"
            elif ISEO_HOLD_MODE == "on":
                outcome = "held"
            else:
                outcome = "pushed_observe"
            _log_hold_decision(row, row["hold_reason"], "provision", outcome)
        if success:
            ok += 1
        elif str(err).startswith("skipped"):
            skip += 1
        else:
            logger.warning(f"⚠️ provision failed {row['duve_reservation_id']}: {err}")
            errors.append(f"provision {row['duve_reservation_id']} ({row.get('apartment_code')}): {err}")

    # 1a. Trous silencieux : résas whitelistées à provisionner toujours sans code,
    # classées par cause (lock / precheckin / paiement / autre). Loggés à CHAQUE run
    # mais mail 1×/jour seulement (run ~08:45 Paris) — un precheckin se résout souvent
    # seul quand le guest fait son formulaire, inutile de spammer toutes les 2h.
    gaps = _whitelisted_gaps()
    for g in gaps:
        logger.warning(
            f"⚠️ gap provision [{g['reason']}] résa {g.get('reservation_number')} "
            f"({g.get('customer_name')}, {g.get('apartment_code') or g.get('resource_id')}, "
            f"CI {g.get('checkin_date')})")

    # 1b. Resync drift de dates (window cache ≠ dates live Mews)
    to_resync = _resa_to_resync()
    if to_resync:
        logger.info(f"🔄 {len(to_resync)} résa(s) à resync (drift de dates)")
    resynced = 0
    for row in to_resync:
        try:
            success, err = _resync(row)
        except Exception as e:
            success, err = False, f"exception: {e}"
        if success:
            resynced += 1
        elif not str(err).startswith("skipped"):
            logger.warning(f"⚠️ resync failed {row['duve_reservation_id']}: {err}")
            errors.append(f"resync {row['duve_reservation_id']} ({row.get('duve_property_id')}): {err}")

    # 2. Retry du push Duve (Sofia OK mais Duve KO à un run précédent)
    retry = 0
    if not ISEO_SHADOW_MODE:
        for row in _resa_duve_retry():
            members = (row.get("stay_member_duve_ids") or row["duve_reservation_id"]).split(",")
            done, err = _duve_push_all(members, row.get("pin_value") or "",
                                       row.get("invitation_link") or "")
            if done:
                _mark_duve_pushed(row["duve_reservation_id"])
                retry += 1
            else:
                errors.append(f"duve-retry {row['duve_reservation_id']}: {err}")

    # 3. Archive (CO passé / annulée)
    to_archive = _resa_to_archive()
    logger.info(f"🗑️ {len(to_archive)} résa(s) à archiver (CO passé ou annulée)")
    archived = 0
    for row in to_archive:
        try:
            success, err = _archive(row)
        except Exception as e:
            success, err = False, f"exception: {e}"
        if success:
            try:
                _mark_archived(row["duve_reservation_id"])
                archived += 1
            except Exception as e:
                errors.append(f"archive state {row['duve_reservation_id']}: {e}")
        elif str(err).startswith("skipped"):
            pass  # shadow global : row live laissée active (cf. _archive)
        else:
            errors.append(f"archive {row['duve_reservation_id']}: {err}")

    # 4. Purge des DUVE_PIN natifs orphelins (résa annulée/checked-out) — whitelist only
    to_purge = _native_duve_pins_to_purge()
    if to_purge:
        logger.info(f"🧹 {len(to_purge)} DUVE_PIN natif(s) orphelin(s) à purger (whitelist)")
    purged = 0
    for row in to_purge:
        try:
            success, err = _purge_native_orphan(row)
        except Exception as e:
            success, err = False, f"exception: {e}"
        if success:
            purged += 1
        else:
            errors.append(f"purge {row.get('ext_id')}: {err}")

    logger.info("=" * 70)
    logger.info(f"DONE — provision ok={ok} skip={skip} | hold[{ISEO_HOLD_MODE}]={held} | "
                f"resync={resynced} | duve-retry={retry} | archived={archived} | "
                f"purged={purged} | erreurs={len(errors)}")
    logger.info("=" * 70)

    if errors:
        body = build_email(
            "ISEO orchestrator — erreurs",
            subtitle=datetime.now(PARIS_TZ).strftime("%d/%m/%Y %H:%M"),
            severity="critical",
            kpis=[
                {"label": "Erreurs", "value": len(errors), "color": "#dc2626"},
                {"label": "Provisions ok", "value": ok},
                {"label": "Resync", "value": resynced},
                {"label": "Archivées", "value": archived},
            ],
            intro="Chaque ligne = une résa dont le cycle PIN a échoué sur ce run "
                  "(retenté automatiquement au prochain run 2h).",
            table={"headers": ["Erreur"],
                   "rows": [[esc(e)] for e in errors[:50]]},
            button=("Ops · 7.8 Pipeline PIN",
                    "https://direction.archides.fr/ops-back?tab=pin_pipeline"),
        )
        _send_alert(f"⚠️ ISEO orchestrator — {len(errors)} erreur(s)", body, html=True)

    # Gaps : mail 1×/jour (run du matin ~08:45 Paris), séparé des erreurs dures qui,
    # elles, alertent à chaque run. Évite le spam sur un gap qui se résout tout seul.
    if gaps and datetime.now(PARIS_TZ).hour == 8:
        n_urgent = sum(1 for g in gaps if g["reason"] in ("autre", "lock"))
        subject = (f"{'🔴' if n_urgent else 'ℹ️'} ISEO — {len(gaps)} résa(s) sans code"
                   + (f" dont {n_urgent} à investiguer" if n_urgent else ""))
        paris_today = datetime.now(PARIS_TZ).strftime("%A %d %B %Y")
        _send_alert(subject, _build_gaps_html(gaps[:50], paris_today), html=True)
