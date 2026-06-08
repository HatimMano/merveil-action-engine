"""
ISEO PIN Orchestrator — recreate PINs côté Sofia à J-7 du CI (pipeline V3).

Cf. project_iseo_integration_2026 section Architecture V3.

Lit `iseo_raw.merveil_pin_cache` (alimentée par webhook-gateway/iseo_pin_cacher au
pre-checkin done Duve), pour chaque résa active dont CI dans 7j :
  - JOIN raw_duve.checkin_events pour récupérer estimated_checkin_time / estimated_checkout_time
    et la dernière valeur d'apartment_code (en cas de modif)
  - POST Sofia /standardDevices avec MÊME deviceId (= pin_value capturé) + window
    [CI + estimated_ci_time Paris, CO + estimated_co_time Paris]
  - UPDATE cache.recreated_at

Logique d'archivage : pour les rows where checkout_date < today, DELETE Sofia + flag
archived_at. Permet de nettoyer après le séjour.

Modes :
  - ISEO_SHADOW_MODE=true → log "would POST" mais pas d'appel Sofia
  - ISEO_ALLOWED_PROPERTY_IDS (csv) → whitelist (sécurise le rollout par appart)

⚠ Le pin_value en cache reste valide tant que la résa est active. Pas de rotation
auto pour l'instant — c'est le code envoyé au guest par Duve au pre-checkin done.

Trigger : Cloud Run Job `merveil-action-engine-iseo` lancé par scheduler quotidien.
"""

import hashlib
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "merveil-data-warehouse")
PIN_CACHE_TABLE = os.environ.get(
    "ISEO_PIN_CACHE_TABLE",
    "merveil-data-warehouse.iseo_raw.merveil_pin_cache",
)
RAW_DUVE_CHECKIN_TABLE = os.environ.get(
    "RAW_DUVE_CHECKIN_TABLE",
    "merveil-data-warehouse.raw_duve.checkin_events",
)

ISEO_BASE_URL = os.environ.get("ISEO_BASE_URL", "https://api-archides.jago.cloud")
ISEO_USERNAME = (os.environ.get("ISEO_MANAGER_USERNAME") or "").strip()
ISEO_PASSWORD = (os.environ.get("ISEO_MANAGER_PASSWORD") or "").strip()

ISEO_SHADOW_MODE = os.environ.get("ISEO_SHADOW_MODE", "true").lower() == "true"
ALLOWED_PROPERTY_IDS = {
    pid.strip().lower()
    for pid in (os.environ.get("ISEO_ALLOWED_PROPERTY_IDS") or "").split(",")
    if pid.strip()
}

# Fenêtre temporelle des résa à traiter : CI dans les 7 prochains jours
LOOKAHEAD_DAYS = int(os.environ.get("ISEO_LOOKAHEAD_DAYS", "7"))

# Fallback heures si Duve ne pousse pas les estimated_checkin_time / etc.
DEFAULT_CI_HOUR = os.environ.get("ISEO_DEFAULT_CI_HOUR", "16:00")
DEFAULT_CO_HOUR = os.environ.get("ISEO_DEFAULT_CO_HOUR", "11:00")

PARIS_TZ_OFFSET_HOURS = 1  # CET; switche à 2 (CEST) entre derniers dimanches mars/oct
# Pour simplifier, on calcule l'offset dynamiquement au moment du POST :
# datetime aware avec zoneinfo Europe/Paris


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
            f"{ISEO_BASE_URL}/oauth/token",
            auth=("client", ""),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "password", "username": ISEO_USERNAME, "password": ISEO_PASSWORD},
            timeout=30,
        )
        resp.raise_for_status()
        p = resp.json()
        cls._token = p["access_token"]
        cls._expires_at = now + int(p.get("expires_in", 172800))
        logger.info(f"🔑 Sofia token (exp in {p.get('expires_in', '?')}s)")
        return cls._token


def _sofia(method: str, path: str, json_body=None) -> requests.Response:
    return requests.request(
        method,
        f"{ISEO_BASE_URL}{path}",
        headers={
            "Authorization": f"Bearer {_SofiaAuth.get_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        json=json_body,
        timeout=30,
    )


# ── BigQuery ──────────────────────────────────────────────────────────────────

_bq_client: Optional[bigquery.Client] = None


def _bq() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


def _resa_to_recreate() -> list[dict]:
    """Lit les rows à recréer côté Sofia : cache non-archivée, non-recréée, CI dans LOOKAHEAD_DAYS,
    et résa pas annulée (cross-check fct_reservations via duve_reservation_id n'est pas
    facile sans mapping mews — pour l'instant on confie à la cache + raw_duve)."""
    q = f"""
    WITH cache AS (
      SELECT *
      FROM `{PIN_CACHE_TABLE}`
      WHERE archived_at IS NULL
        AND recreated_at IS NULL
        AND pin_value IS NOT NULL
        AND iseo_guest_tag_id IS NOT NULL
        AND iseo_lock_tag_id IS NOT NULL
        AND checkin_date BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL {LOOKAHEAD_DAYS} DAY)
    ),
    duve_latest AS (
      SELECT
        reservation_id AS duve_reservation_id,
        DATE(SAFE_CAST(checkin_date  AS TIMESTAMP)) AS checkin_date_live,
        DATE(SAFE_CAST(checkout_date AS TIMESTAMP)) AS checkout_date_live,
        COALESCE(earliest_checkin_hour, JSON_VALUE(_raw_payload, '$.resource.earliestCheckInTime')) AS earliest_checkin_hour,
        COALESCE(latest_checkout_hour,  JSON_VALUE(_raw_payload, '$.resource.latestCheckOutTime'))  AS latest_checkout_hour,
        COALESCE(estimated_checkin_time, JSON_VALUE(_raw_payload, '$.resource.estimatedCheckInTime'))  AS estimated_ci_time,
        COALESCE(estimated_checkout_time, JSON_VALUE(_raw_payload, '$.resource.estimatedCheckOutTime')) AS estimated_co_time,
        property_id,
        received_at
      FROM `{RAW_DUVE_CHECKIN_TABLE}`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY reservation_id ORDER BY received_at DESC) = 1
    )
    SELECT
      c.* EXCEPT (cached_at, recreated_at, archived_at, last_error, shadow_mode, row_hash),
      d.checkin_date_live,
      d.checkout_date_live,
      d.estimated_ci_time,
      d.estimated_co_time,
      d.earliest_checkin_hour,
      d.latest_checkout_hour,
      d.property_id AS duve_property_id_live
    FROM cache c
    LEFT JOIN duve_latest d ON d.duve_reservation_id = c.duve_reservation_id
    ORDER BY c.checkin_date
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


def _resa_to_archive() -> list[dict]:
    """Rows à archiver : checkout passé. DELETE Sofia + set archived_at."""
    q = f"""
    SELECT duve_reservation_id, pin_value, iseo_lock_tag_id, iseo_guest_tag_id
    FROM `{PIN_CACHE_TABLE}`
    WHERE archived_at IS NULL
      AND checkout_date < CURRENT_DATE()
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


def _update_cache_recreated(duve_resa_id: str, error: Optional[str] = None) -> None:
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET recreated_at = CURRENT_TIMESTAMP(),
        last_error   = @err
    WHERE duve_reservation_id = @id AND archived_at IS NULL
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", duve_resa_id),
        bigquery.ScalarQueryParameter("err", "STRING", error),
    ])
    _bq().query(q, job_config=cfg).result()


def _update_cache_archived(duve_resa_id: str, error: Optional[str] = None) -> None:
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET archived_at = CURRENT_TIMESTAMP(),
        last_error  = @err
    WHERE duve_reservation_id = @id AND archived_at IS NULL
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", duve_resa_id),
        bigquery.ScalarQueryParameter("err", "STRING", error),
    ])
    _bq().query(q, job_config=cfg).result()


# ── Window calc (Paris timezone) ──────────────────────────────────────────────

def _paris_offset_hours(date_str: str) -> int:
    """Retourne +1 (hiver) ou +2 (été). Approximation : dernier dimanche mars→octobre."""
    try:
        d = datetime.fromisoformat(date_str)
        y = d.year
        # Dernier dimanche mars
        march = datetime(y, 3, 31)
        dst_start = march - timedelta(days=(march.weekday() + 1) % 7)
        # Dernier dimanche octobre
        oct31 = datetime(y, 10, 31)
        dst_end = oct31 - timedelta(days=(oct31.weekday() + 1) % 7)
        return 2 if dst_start <= d < dst_end else 1
    except Exception:
        return 1


def _build_window_ms(checkin_date: str, checkout_date: str,
                     ci_hour: Optional[str], co_hour: Optional[str]) -> tuple[int, int]:
    """Construit dateInterval ms epoch UTC depuis dates DATE + heures Paris HH:MM."""
    ci_h = (ci_hour or DEFAULT_CI_HOUR)[:5]   # HH:MM
    co_h = (co_hour or DEFAULT_CO_HOUR)[:5]
    ci_offset = _paris_offset_hours(checkin_date)
    co_offset = _paris_offset_hours(checkout_date)
    ci_str = f"{checkin_date}T{ci_h}:00+0{ci_offset}:00"
    co_str = f"{checkout_date}T{co_h}:00+0{co_offset}:00"
    ci_dt = datetime.fromisoformat(ci_str)
    co_dt = datetime.fromisoformat(co_str)
    return int(ci_dt.timestamp() * 1000), int(co_dt.timestamp() * 1000)


# ── Main recreate logic ───────────────────────────────────────────────────────

def _recreate_pin(row: dict) -> tuple[bool, Optional[str]]:
    """POST nouveau PIN côté Sofia avec MÊME deviceId + window calculée."""
    duve_resa_id = row["duve_reservation_id"]

    # Whitelist
    apt_pid = (row.get("duve_property_id_live") or "").lower()
    if ALLOWED_PROPERTY_IDS and apt_pid not in ALLOWED_PROPERTY_IDS:
        logger.info(f"⏭️ {duve_resa_id} hors whitelist property={apt_pid} → skip")
        return False, "skipped: whitelist"

    # Window calculation (prefer live values from raw_duve, fallback cache)
    ci_date = row.get("checkin_date_live") or row.get("checkin_date")
    co_date = row.get("checkout_date_live") or row.get("checkout_date")
    if ci_date is None or co_date is None:
        return False, "missing CI/CO date"
    ci_date_str = ci_date.isoformat() if hasattr(ci_date, "isoformat") else str(ci_date)
    co_date_str = co_date.isoformat() if hasattr(co_date, "isoformat") else str(co_date)

    # Préférence ordre : estimated > earliest/latest > default
    ci_hour = row.get("estimated_ci_time") or row.get("earliest_checkin_hour") or DEFAULT_CI_HOUR
    co_hour = row.get("estimated_co_time") or row.get("latest_checkout_hour") or DEFAULT_CO_HOUR

    try:
        ci_ms, co_ms = _build_window_ms(ci_date_str, co_date_str, ci_hour, co_hour)
    except Exception as e:
        return False, f"window calc failed: {e}"

    # ⚠ Sofia rejette les windows trop dans le passé (dont window.to < now)
    if co_ms < int(time.time() * 1000):
        return False, f"checkout already passed: {co_date_str} {co_hour}"

    ext_id = f"MERVEIL_RESA - {duve_resa_id}"
    payload = {
        "type": "ISEO_PIN",
        "deviceId": row["pin_value"],
        "extId": ext_id,
        "notes": ext_id,
        "validationMode": "ONE_HOUR_VALIDATION",
        "validationPeriod": 24,
        "additionalCredentialRules": [],
        "credentialRule": {
            "name": ext_id,
            "description": "merveil_dwh_v3",
            "lockTagIds": [int(row["iseo_lock_tag_id"])],
            "lockTagMatchingMode": "AT_LEAST_ONE_TAG",
            "guestTagIds": [int(row["iseo_guest_tag_id"])],
            "guestTagMatchingMode": "EVERY_TAG",
            "daysOfTheWeek": [1, 2, 3, 4, 5, 6, 7],
            "dateInterval": {"from": ci_ms, "to": co_ms},
            "timeInterval": {"from": 0, "to": 86340},
            "alwaysOpen": False, "holidays": True, "openOnPrivacy": False,
        },
    }

    logger.info(
        f"→ recreate {duve_resa_id} deviceId={row['pin_value']} "
        f"window={datetime.fromtimestamp(ci_ms/1000, timezone.utc).isoformat()} → "
        f"{datetime.fromtimestamp(co_ms/1000, timezone.utc).isoformat()}"
    )

    if ISEO_SHADOW_MODE:
        logger.info(f"🌗 SHADOW MODE: POST Sofia skipped for {duve_resa_id}")
        return True, None

    r = _sofia("POST", "/api/v2/standardDevices", json_body=payload)
    if r.status_code in (200, 201):
        new_pin = r.json()
        logger.info(f"✅ Recreated PIN id={new_pin.get('id')} for {duve_resa_id}")
        return True, None
    # Si "already present" : c'est un retry, on accepte
    if "already present" in r.text.lower():
        logger.info(f"ℹ️ PIN déjà présent côté Sofia pour {duve_resa_id} (retry idempotent)")
        return True, None
    return False, f"HTTP {r.status_code}: {r.text[:300]}"


def _archive_pin(row: dict) -> tuple[bool, Optional[str]]:
    """DELETE le PIN côté Sofia (search by extId = MERVEIL_RESA - <id>) puis flag archived."""
    duve_resa_id = row["duve_reservation_id"]
    ext_id = f"MERVEIL_RESA - {duve_resa_id}"

    if ISEO_SHADOW_MODE:
        logger.info(f"🌗 SHADOW MODE: DELETE Sofia skipped (archive {duve_resa_id})")
        return True, None

    # GET pour obtenir id
    r = _sofia("GET", f"/api/v2/standardDevices/extId/{ext_id}")
    if r.status_code == 404:
        logger.info(f"ℹ️ {duve_resa_id}: pas de PIN MERVEIL_RESA côté Sofia (déjà delete?) → archive direct")
        return True, None
    if r.status_code != 200:
        return False, f"GET HTTP {r.status_code}: {r.text[:200]}"
    sid = r.json().get("id")
    rdel = _sofia("DELETE", f"/api/v2/standardDevices/{sid}")
    if rdel.status_code in (200, 204):
        logger.info(f"🗑️ Archived {duve_resa_id} (deleted Sofia id={sid})")
        return True, None
    return False, f"DELETE HTTP {rdel.status_code}: {rdel.text[:200]}"


# ── Entry point ───────────────────────────────────────────────────────────────

def run() -> None:
    logger.info("=" * 70)
    logger.info(
        f"🚀 ISEO Orchestrator (shadow={ISEO_SHADOW_MODE}, whitelist={len(ALLOWED_PROPERTY_IDS)} property_ids)"
    )
    logger.info("=" * 70)

    # ── 1. Recreate à J-7 (résa à venir dans les 7 prochains jours) ─────────
    to_recreate = _resa_to_recreate()
    logger.info(f"📋 {len(to_recreate)} résa(s) à recréer (CI dans 0-{LOOKAHEAD_DAYS}j)")

    ok = fail = 0
    for row in to_recreate:
        success, err = _recreate_pin(row)
        if success:
            try:
                if not ISEO_SHADOW_MODE:
                    _update_cache_recreated(row["duve_reservation_id"])
                else:
                    logger.info(f"🌗 cache.recreated_at update skipped (shadow)")
            except Exception as e:
                logger.warning(f"⚠️ cache update failed for {row['duve_reservation_id']}: {e}")
            ok += 1
        else:
            try:
                _update_cache_recreated(row["duve_reservation_id"], error=err[:500])
            except Exception:
                pass
            logger.warning(f"⚠️ recreate failed for {row['duve_reservation_id']}: {err}")
            fail += 1

    # ── 2. Archive (checkout passé) ─────────────────────────────────────────
    to_archive = _resa_to_archive()
    logger.info(f"🗑️ {len(to_archive)} résa(s) à archiver (CO passé)")
    archived = 0
    for row in to_archive:
        success, err = _archive_pin(row)
        if success:
            try:
                if not ISEO_SHADOW_MODE:
                    _update_cache_archived(row["duve_reservation_id"])
                archived += 1
            except Exception as e:
                logger.warning(f"⚠️ archive update failed for {row['duve_reservation_id']}: {e}")
        else:
            logger.warning(f"⚠️ archive failed for {row['duve_reservation_id']}: {err}")

    logger.info("=" * 70)
    logger.info(f"DONE — recreate ok={ok} fail={fail} | archived={archived}")
    logger.info("=" * 70)
