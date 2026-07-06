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

import base64
import logging
import os
import random
import secrets
import time
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
from googleapiclient.discovery import build

PARIS_TZ = ZoneInfo("Europe/Paris")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "merveil-data-warehouse")
PIN_CACHE_TABLE = os.environ.get(
    "ISEO_PIN_CACHE_TABLE", "merveil-data-warehouse.iseo_raw.merveil_pin_cache")
RAW_DUVE_CHECKIN_TABLE = os.environ.get(
    "RAW_DUVE_CHECKIN_TABLE", "merveil-data-warehouse.raw_duve.checkin_events")
MEWS_FCT_TABLE = os.environ.get(
    "MEWS_FCT_TABLE", "merveil-data-warehouse.marts.fct_reservations")
MEWS_PAYMENTS_TABLE = os.environ.get(
    "MEWS_PAYMENTS_TABLE", "merveil-data-warehouse.staging.stg_mews__payments")
SMART_LOCKS_TABLE = os.environ.get(
    "SMART_LOCKS_TABLE", "merveil-data-warehouse.staging.stg_iseo__smart_locks")
STD_DEVICES_TABLE = os.environ.get(
    "STD_DEVICES_TABLE", "merveil-data-warehouse.staging.stg_iseo__standard_devices")

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
ALLOWED_PROPERTY_IDS = {
    pid.strip().lower()
    for pid in (os.environ.get("ISEO_ALLOWED_PROPERTY_IDS") or "").split(",")
    if pid.strip()
}

# Alerting mail (réutilise l'infra Gmail API du service — secret alerts-gmail-sa-key
# lu via Secret Manager + Domain-Wide Delegation, comme cancellations_brief).
GMAIL_SENDER = os.getenv("GMAIL_SENDER", "noreply@archides.fr")
ISEO_ALERT_TO = os.getenv("ISEO_ALERT_TO", "hatim@archides.fr")

LOOKAHEAD_DAYS = int(os.environ.get("ISEO_LOOKAHEAD_DAYS", "7"))
DEFAULT_CI_HOUR = os.environ.get("ISEO_DEFAULT_CI_HOUR", "13:00")
DEFAULT_CO_HOUR = os.environ.get("ISEO_DEFAULT_CO_HOUR", "12:00")
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


def _send_alert(subject: str, body: str) -> None:
    """Envoie un mail d'alerte (best-effort — ne fait jamais planter le job)."""
    try:
        name = f"projects/{PROJECT_ID}/secrets/alerts-gmail-sa-key/versions/latest"
        sa_info = secretmanager.SecretManagerServiceClient().access_secret_version(
            name=name).payload.data.decode()
        import json as _json
        creds = service_account.Credentials.from_service_account_info(
            _json.loads(sa_info), scopes=["https://www.googleapis.com/auth/gmail.send"]
        ).with_subject(GMAIL_SENDER)
        svc = build("gmail", "v1", credentials=creds, cache_discovery=False)
        msg = MIMEText(body, "plain", "utf-8")
        msg["From"], msg["To"], msg["Subject"] = GMAIL_SENDER, ISEO_ALERT_TO, subject
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        svc.users().messages().send(userId=GMAIL_SENDER, body={"raw": raw}).execute()
        logger.info(f"📧 alerte envoyée à {ISEO_ALERT_TO}: {subject}")
    except Exception as e:
        logger.error(f"⚠️ envoi alerte échoué: {e}")


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
    ),
    guest_tags AS (
      SELECT lock_tag_id, guest_tag_id FROM (
        SELECT lock_tag_id, guest_tag_id,
               ROW_NUMBER() OVER (PARTITION BY lock_tag_id ORDER BY n DESC) AS rn
        FROM (
          SELECT
            CAST(REGEXP_EXTRACT(rule_lock_tags_ids,  r'(\\d+)') AS INT64) AS lock_tag_id,
            CAST(REGEXP_EXTRACT(rule_guest_tags_ids, r'(\\d+)') AS INT64) AS guest_tag_id,
            COUNT(*) AS n
          FROM `{STD_DEVICES_TABLE}`
          WHERE is_present_in_latest_snapshot
            AND rule_lock_tags_ids IS NOT NULL AND rule_lock_tags_ids != '[]'
            AND rule_guest_tags_ids IS NOT NULL AND rule_guest_tags_ids != '[]'
          GROUP BY 1, 2
        )
      ) WHERE rn = 1
    )"""

_PAYMENTS_CTE = f"""
    payments AS (
      SELECT reservation_id,
             COUNTIF(state = 'Charged') AS n_charged,
             COUNTIF(state = 'Failed')  AS n_failed
      FROM `{MEWS_PAYMENTS_TABLE}`
      WHERE reservation_id IS NOT NULL GROUP BY reservation_id
    )"""


def _resa_to_provision() -> list[dict]:
    """Résas à provisionner : CI dans [today, today+LOOKAHEAD], CO futur, non annulée,
    PAS déjà couverte par une row de cache active (archived_at IS NULL). Résout
    lock_id / lock_tag_id / guest_tag_id de l'appart par JOIN."""
    q = f"""
    WITH {_DUVE_LATEST_CTE},
    {_LOCKS_CTE},
    {_PAYMENTS_CTE},
    mews AS (
      SELECT customer_id, resource_id, customer_name,
             reservation_id     AS mews_reservation_id,
             reservation_number AS mews_reservation_number,
             is_cancelled, checkin_date, checkout_date,
             earliest_checkin_hour, latest_checkout_hour
      FROM `{MEWS_FCT_TABLE}`
    ),
    active_state AS (
      SELECT DISTINCT duve_reservation_id
      FROM `{PIN_CACHE_TABLE}` WHERE archived_at IS NULL
    ),
    -- Résas Mews déjà couvertes par une row active (via un AUTRE duve_id). Une résa
    -- Mews (cancel+recreate) peut avoir 2 reservations Duve ; le JOIN Duve↔Mews sur
    -- (customer_id, resource_id) n'est pas unique → sans ça chaque duve_id provisionne
    -- un device = doublon sur la même serrure (cf. churn RUIQING/CLE7-0D).
    active_by_mews AS (
      SELECT DISTINCT mews_reservation_number
      FROM `{PIN_CACHE_TABLE}`
      WHERE archived_at IS NULL AND mews_reservation_number IS NOT NULL
    ),
    joined AS (
      SELECT
        d.duve_reservation_id, d.duve_property_id,
        lk.lock_id, lk.lock_tag_id, lk.apartment_code,
        gt.guest_tag_id, m.customer_name,
        m.mews_reservation_id, m.mews_reservation_number,
        m.checkin_date, m.checkout_date,
        m.earliest_checkin_hour, m.latest_checkout_hour,
        (COALESCE(pay.n_failed, 0) > 0 AND COALESCE(pay.n_charged, 0) = 0) AS payment_unpaid
      FROM mews m
      JOIN duve_latest d ON d.mews_customer_id = m.customer_id
                        AND d.duve_property_id = m.resource_id
      JOIN locks lk      ON lk.duve_property_id = m.resource_id
      LEFT JOIN guest_tags gt ON gt.lock_tag_id = lk.lock_tag_id
      LEFT JOIN payments pay  ON pay.reservation_id = m.mews_reservation_id
      LEFT JOIN active_state s ON s.duve_reservation_id = d.duve_reservation_id
      LEFT JOIN active_by_mews am
        ON am.mews_reservation_number = CAST(m.mews_reservation_number AS STRING)
      WHERE m.checkin_date <= DATE_ADD(CURRENT_DATE(), INTERVAL {LOOKAHEAD_DAYS} DAY)
        AND m.checkout_date >= CURRENT_DATE()
        AND COALESCE(m.is_cancelled, FALSE) = FALSE
        AND s.duve_reservation_id IS NULL
        AND am.mews_reservation_number IS NULL
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CAST(m.mews_reservation_number AS STRING)
        ORDER BY d.duve_reservation_id) = 1
    )
    SELECT * FROM joined ORDER BY checkin_date
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


def _resa_to_archive() -> list[dict]:
    """Rows actives à archiver : CO passé OU résa annulée (cross-check Mews).
    DELETE Sofia device (par extId) + DELETE invitation (par id stocké)."""
    q = f"""
    WITH {_DUVE_LATEST_CTE},
    cache AS (
      SELECT duve_reservation_id, iseo_invitation_id, checkout_date, shadow_mode
      FROM `{PIN_CACHE_TABLE}` WHERE archived_at IS NULL
    ),
    mews AS (
      SELECT customer_id, resource_id, is_cancelled, checkin_date AS mews_checkin_date
      FROM `{MEWS_FCT_TABLE}`
    ),
    joined AS (
      SELECT c.duve_reservation_id, c.iseo_invitation_id, c.checkout_date, c.shadow_mode,
             COALESCE(m.is_cancelled, FALSE) AS is_cancelled
      FROM cache c
      LEFT JOIN duve_latest d ON d.duve_reservation_id = c.duve_reservation_id
      LEFT JOIN mews m ON m.customer_id = d.mews_customer_id
                      AND m.resource_id = d.duve_property_id
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY c.duve_reservation_id
        ORDER BY COALESCE(m.is_cancelled, FALSE) ASC,
                 ABS(DATE_DIFF(m.mews_checkin_date, c.checkout_date, DAY)) ASC NULLS LAST) = 1
    )
    SELECT duve_reservation_id, iseo_invitation_id, shadow_mode,
           CASE WHEN is_cancelled THEN 'cancelled' ELSE 'checkout_passed' END AS archive_reason
    FROM joined
    WHERE checkout_date < CURRENT_DATE() OR is_cancelled
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


def _resa_to_resync() -> list[dict]:
    """Rows actives dont la window cache ≠ dates live Mews = drift de dates post-
    provision (extension / raccourcissement / décalage du séjour). À resync Sofia
    pour que la fenêtre du PIN colle au séjour réel. Annulations → _resa_to_archive."""
    q = f"""
    WITH {_DUVE_LATEST_CTE},
    cache AS (
      SELECT duve_reservation_id, pin_value, iseo_device_id, iseo_invitation_id,
             iseo_guest_tag_id, iseo_lock_id, iseo_lock_tag_id,
             checkin_date AS cache_ci, checkout_date AS cache_co
      FROM `{PIN_CACHE_TABLE}`
      WHERE archived_at IS NULL AND provisioned_at IS NOT NULL
    ),
    mews AS (
      SELECT customer_id, resource_id, customer_name, is_cancelled,
             checkin_date, checkout_date, earliest_checkin_hour, latest_checkout_hour
      FROM `{MEWS_FCT_TABLE}`
    ),
    joined AS (
      SELECT c.duve_reservation_id, c.pin_value, c.iseo_device_id, c.iseo_invitation_id,
             c.iseo_guest_tag_id, c.iseo_lock_id, c.iseo_lock_tag_id,
             d.duve_property_id, m.customer_name,
             m.checkin_date AS live_ci, m.checkout_date AS live_co,
             m.earliest_checkin_hour, m.latest_checkout_hour
      FROM cache c
      JOIN duve_latest d ON d.duve_reservation_id = c.duve_reservation_id
      JOIN mews m ON m.customer_id = d.mews_customer_id
                 AND m.resource_id = d.duve_property_id
      WHERE COALESCE(m.is_cancelled, FALSE) = FALSE
        AND m.checkout_date >= CURRENT_DATE()
        AND (m.checkin_date != c.cache_ci OR m.checkout_date != c.cache_co)
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY c.duve_reservation_id
        ORDER BY ABS(DATE_DIFF(m.checkin_date, c.cache_ci, DAY))) = 1
    )
    SELECT * FROM joined ORDER BY live_ci
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


# ── State writers ───────────────────────────────────────────────────────────

def _save_provisioned(row: dict, pin_value: str, device_id: int,
                      inv_id: Optional[int], inv_code: Optional[str],
                      link: Optional[str], duve_ok: bool) -> None:
    q = f"""
    INSERT INTO `{PIN_CACHE_TABLE}` (
      duve_reservation_id, mews_reservation_number, apartment_code, pin_value,
      iseo_guest_tag_id, iseo_lock_id, iseo_lock_tag_id, iseo_device_id,
      iseo_invitation_id, invitation_code, invitation_link,
      checkin_date, checkout_date, cached_at, provisioned_at, duve_pushed_at,
      shadow_mode)
    VALUES (@duve, @num, @apt, @pin, @gtag, @lock, @ltag, @dev,
            @inv, @code, @link, @ci, @co, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
            {'CURRENT_TIMESTAMP()' if duve_ok else 'NULL'}, FALSE)
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
    ])
    _bq().query(q, job_config=cfg).result()


def _save_resynced(duve_resa_id: str, ci: str, co: str, device_id: object,
                   inv_id: Optional[int], inv_code: Optional[str],
                   link: Optional[str], duve_ok: bool) -> None:
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET checkin_date = @ci, checkout_date = @co, iseo_device_id = @dev,
        iseo_invitation_id = @inv, invitation_code = @code, invitation_link = @link,
        provisioned_at = CURRENT_TIMESTAMP(),
        duve_pushed_at = {'CURRENT_TIMESTAMP()' if duve_ok else 'NULL'},
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
    ])
    _bq().query(q, job_config=cfg).result()


def _resa_duve_retry() -> list[dict]:
    """Rows provisionnées côté Sofia mais dont le push Duve a échoué
    (`duve_pushed_at IS NULL`) → à re-pousser (code + lien déjà en cache)."""
    q = f"""
    SELECT duve_reservation_id, pin_value, invitation_link
    FROM `{PIN_CACHE_TABLE}`
    WHERE archived_at IS NULL AND provisioned_at IS NOT NULL AND duve_pushed_at IS NULL
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

    fn, ln = _split_name(guest_name)
    email = f"resa-{duve_resa_id}@guest.archides.fr"  # unique par résa, jamais utilisé
    r = _sofia("POST", "/api/v2/users", json_body={
        "username": email, "email": email, "password": "Mv!" + secrets.token_urlsafe(16),
        "firstname": fn, "lastname": ln, "roleIds": [5], "extId": ext})
    if r.status_code not in (200, 201):
        return None, f"user POST HTTP {r.status_code}: {r.text[:200]}"
    tag = _user_tag_id(r.json())
    if tag is None:  # fallback : relire le user pour récupérer son tag
        g2 = _sofia("GET", f"/api/v2/users/extId/{ext}")
        tag = _user_tag_id(g2.json()) if g2.status_code == 200 else None
    if tag is None:
        return None, "user créé sans tag 'user'"
    logger.info(f"✅ user guest créé '{(fn + ' ' + ln).strip()}' → tag {tag}")
    return tag, None


# ── Provision (A→E) ───────────────────────────────────────────────────────────

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

    # D. Duve push (code clavier + lien)
    duve_ok, duve_err = _duve_push(duve_resa_id, pin_value, link or "")
    if not duve_ok:
        logger.warning(f"⚠️ Duve push failed for {duve_resa_id}: {duve_err}")

    # E. état
    _save_provisioned(row, pin_value, device_id, inv_id, inv_code, link, duve_ok)
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
        pv = f"{random.randint(0, 9999):04d}"
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
    return _post_device(row, pin_ext, win)


def _get_or_create_invitation(row: dict, inv_ext: str, win: dict) -> tuple[Optional[int], Optional[str]]:
    g = _sofia("GET", f"/api/v2/invitations/extId/{inv_ext}")
    if g.status_code == 200:
        d = g.json()
        return d.get("id"), d.get("code")
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
    if ISEO_SHADOW_MODE or bool(row.get("shadow_mode")):
        logger.info(f"🌗 SHADOW {duve_resa_id}: archive skipped")
        return True, None

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
    logger.info(f"🗑️ archived {duve_resa_id} ({row.get('archive_reason')})")
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

    # 1. DELETE device + invitation existants
    g = _sofia("GET", f"/api/v2/standardDevices/extId/{pin_ext}")
    if g.status_code == 200:
        rd = _sofia("DELETE", f"/api/v2/standardDevices/{g.json().get('id')}")
        if rd.status_code not in (200, 204):
            return False, f"resync device DELETE {rd.status_code}"
    elif g.status_code != 404:
        return False, f"resync device GET {g.status_code}"
    if row.get("iseo_invitation_id"):
        _sofia("DELETE", f"/api/v2/invitations/{int(row['iseo_invitation_id'])}")

    # 2. re-POST device (même code si possible) + invitation (nouveau code/lien)
    pin_value, device_id = _post_device(row, pin_ext, win, pin_value=row.get("pin_value"))
    if pin_value is None:
        return False, f"resync device re-POST failed: {device_id}"
    inv_id, inv_code = _get_or_create_invitation(row, inv_ext, win)
    link = f"https://{REMOTE_OPEN_HOST}/remoteOpen?code={inv_code}" if inv_code else None

    # 3. Duve push (code identique, lien neuf)
    duve_ok, duve_err = _duve_push(duve_resa_id, pin_value, link or "")

    # 4. état
    _save_resynced(duve_resa_id, ci_str, co_str, device_id, inv_id, inv_code, link, duve_ok)
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
    reste l'unique code des résas >J-7 pas encore provisionnées) — nettoyées au CO."""
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
    -- cancel+recreate mêmes dates) → NE PAS purger, sinon guest sans code jusqu'à J-7.
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

def run() -> None:
    """Wrapper : tout crash → alerte mail + exit non-zero (visible Cloud Run)."""
    try:
        _run_inner()
    except Exception as e:
        logger.critical(f"🔴 ISEO orchestrator CRASH: {e}")
        _send_alert("🔴 ISEO orchestrator — CRASH", f"Le job a planté.\n\nException : {e}")
        raise


def _run_inner() -> None:
    logger.info("=" * 70)
    logger.info(f"🚀 ISEO Orchestrator V3 (shadow={ISEO_SHADOW_MODE}, "
                f"whitelist={len(ALLOWED_PROPERTY_IDS)} property_ids)")
    logger.info("=" * 70)
    errors: list[str] = []

    # 1. Provision (J-7)
    to_provision = _resa_to_provision()
    logger.info(f"📋 {len(to_provision)} résa(s) à provisionner (CI dans 0-{LOOKAHEAD_DAYS}j, pas encore couvertes)")
    ok = skip = 0
    for row in to_provision:
        try:
            success, err = _provision(row)
        except Exception as e:
            success, err = False, f"exception: {e}"
        if success:
            ok += 1
        elif str(err).startswith("skipped"):
            skip += 1
        else:
            logger.warning(f"⚠️ provision failed {row['duve_reservation_id']}: {err}")
            errors.append(f"provision {row['duve_reservation_id']} ({row.get('apartment_code')}): {err}")

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
            done, err = _duve_push(row["duve_reservation_id"], row.get("pin_value") or "",
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
    logger.info(f"DONE — provision ok={ok} skip={skip} | resync={resynced} | "
                f"duve-retry={retry} | archived={archived} | purged={purged} | erreurs={len(errors)}")
    logger.info("=" * 70)

    if errors:
        body = (f"{len(errors)} erreur(s) sur le run ISEO orchestrator "
                f"(provision ok={ok}, resync={resynced}, duve-retry={retry}, archived={archived}, purged={purged}) :\n\n"
                + "\n".join(f"• {e}" for e in errors[:50]))
        _send_alert(f"⚠️ ISEO orchestrator — {len(errors)} erreur(s)", body)
