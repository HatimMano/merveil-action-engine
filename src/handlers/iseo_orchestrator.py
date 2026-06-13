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

import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from google.cloud import bigquery

PARIS_TZ = ZoneInfo("Europe/Paris")

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
# Source de vérité Mews pour dates + statut annulation. Le webhook Duve est figé
# au pre-checkin done (cf. incident Crystal Balcom) ; Mews est pollé toutes les 2h.
MEWS_FCT_TABLE = os.environ.get(
    "MEWS_FCT_TABLE",
    "merveil-data-warehouse.marts.fct_reservations",
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
    """Rows à recréer côté Sofia : cache non-archivée, non-recréée, CI dans LOOKAHEAD_DAYS,
    et résa NON annulée (cross-check Mews via le mapping Duve↔Mews).

    Mapping Duve↔Mews (le payload Duve embarque les ids Mews) :
      - guestProfiles[isPrimary].externalId  = Mews customer_id
      - resource.externalPropertyId          = Mews resource_id (= property_id stocké)
    → on résout la résa Mews et on rafraîchit dates + statut depuis fct_reservations
    (SoT, pollé 2h), plutôt que de se fier au webhook Duve figé. Les heures de window
    utilisent les heures de POLITIQUE appart (earliest/latest), stables, et non
    l'heure estimée du form (volatile → risque de lockout à l'arrivée).
    """
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
        property_id    AS duve_property_id,   -- = Mews resource_id
        (SELECT JSON_VALUE(g, '$.externalId')
           FROM UNNEST(JSON_QUERY_ARRAY(_raw_payload, '$.resource.guestProfiles')) g
           WHERE JSON_VALUE(g, '$.isPrimary') = 'true'
           LIMIT 1)                          AS mews_customer_id,
        received_at
      FROM `{RAW_DUVE_CHECKIN_TABLE}`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY reservation_id ORDER BY received_at DESC) = 1
    ),
    mews AS (
      SELECT
        customer_id, resource_id,
        reservation_id      AS mews_reservation_id,
        reservation_number  AS mews_reservation_number,
        is_cancelled,
        checkin_date        AS mews_checkin_date,
        checkout_date       AS mews_checkout_date,
        earliest_checkin_hour,
        latest_checkout_hour
      FROM `{MEWS_FCT_TABLE}`
    ),
    joined AS (
      SELECT
        c.* EXCEPT (cached_at, recreated_at, archived_at, last_error, shadow_mode, row_hash),
        d.duve_property_id,
        m.mews_reservation_id,
        m.mews_reservation_number,
        m.is_cancelled,
        m.mews_checkin_date,
        m.mews_checkout_date,
        m.earliest_checkin_hour,
        m.latest_checkout_hour
      FROM cache c
      LEFT JOIN duve_latest d ON d.duve_reservation_id = c.duve_reservation_id
      LEFT JOIN mews m
        ON m.customer_id = d.mews_customer_id
       AND m.resource_id = d.duve_property_id
      -- Si le guest a plusieurs séjours dans le même appart, prendre la résa Mews
      -- dont le CI est le plus proche du CI capturé en cache.
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY c.duve_reservation_id
        ORDER BY ABS(DATE_DIFF(m.mews_checkin_date, c.checkin_date, DAY)) ASC NULLS LAST
      ) = 1
    )
    SELECT * FROM joined
    WHERE COALESCE(is_cancelled, FALSE) = FALSE   -- annulées gérées par l'archive
    ORDER BY mews_checkin_date, checkin_date
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


def _resa_to_archive() -> list[dict]:
    """Rows à archiver : checkout passé OU résa annulée (cross-check Mews).
    DELETE Sofia (skip en shadow) + set archived_at.

    L'annulation est le trou symétrique de l'incident Crystal : une résa annulée
    en cours de window gardait un code fonctionnel car on n'archivait que sur CO passé.
    On résout la résa Mews via le mapping Duve↔Mews et on révoque si is_cancelled.
    """
    q = f"""
    WITH cache AS (
      SELECT duve_reservation_id, pin_value, iseo_lock_tag_id, iseo_guest_tag_id,
             checkin_date, checkout_date
      FROM `{PIN_CACHE_TABLE}`
      WHERE archived_at IS NULL
    ),
    duve_latest AS (
      SELECT
        reservation_id AS duve_reservation_id,
        property_id    AS duve_property_id,
        (SELECT JSON_VALUE(g, '$.externalId')
           FROM UNNEST(JSON_QUERY_ARRAY(_raw_payload, '$.resource.guestProfiles')) g
           WHERE JSON_VALUE(g, '$.isPrimary') = 'true'
           LIMIT 1)                          AS mews_customer_id,
        received_at
      FROM `{RAW_DUVE_CHECKIN_TABLE}`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY reservation_id ORDER BY received_at DESC) = 1
    ),
    mews AS (
      SELECT customer_id, resource_id, is_cancelled, checkin_date AS mews_checkin_date
      FROM `{MEWS_FCT_TABLE}`
    ),
    joined AS (
      SELECT
        c.duve_reservation_id, c.pin_value, c.iseo_lock_tag_id, c.iseo_guest_tag_id,
        c.checkout_date,
        COALESCE(m.is_cancelled, FALSE) AS is_cancelled
      FROM cache c
      LEFT JOIN duve_latest d ON d.duve_reservation_id = c.duve_reservation_id
      LEFT JOIN mews m
        ON m.customer_id = d.mews_customer_id
       AND m.resource_id = d.duve_property_id
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY c.duve_reservation_id
        ORDER BY ABS(DATE_DIFF(m.mews_checkin_date, c.checkin_date, DAY)) ASC NULLS LAST
      ) = 1
    )
    SELECT
      duve_reservation_id, pin_value, iseo_lock_tag_id, iseo_guest_tag_id,
      CASE WHEN is_cancelled THEN 'cancelled' ELSE 'checkout_passed' END AS archive_reason
    FROM joined
    WHERE checkout_date < CURRENT_DATE()
       OR is_cancelled
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


def _mark_recreate_success(duve_resa_id: str) -> None:
    """Recreate Sofia OK → set recreated_at + clear last_error. Idempotent."""
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET recreated_at = CURRENT_TIMESTAMP(),
        last_error   = NULL
    WHERE duve_reservation_id = @id AND archived_at IS NULL
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", duve_resa_id),
    ])
    _bq().query(q, job_config=cfg).result()


def _mark_recreate_error(duve_resa_id: str, error: str) -> None:
    """Recreate Sofia FAIL → write last_error mais NE TOUCHE PAS recreated_at.
    Au prochain run, la query `WHERE recreated_at IS NULL` re-sélectionne la résa → auto-retry.
    """
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET last_error = @err
    WHERE duve_reservation_id = @id AND archived_at IS NULL
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", duve_resa_id),
        bigquery.ScalarQueryParameter("err", "STRING", error[:500]),
    ])
    _bq().query(q, job_config=cfg).result()


def _mark_archived(duve_resa_id: str, error: Optional[str] = None) -> None:
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


# ── Window calc (Paris timezone via zoneinfo) ─────────────────────────────────

def _build_window_ms(checkin_date: str, checkout_date: str,
                     ci_hour: Optional[str], co_hour: Optional[str]) -> tuple[int, int]:
    """Construit dateInterval ms epoch UTC depuis dates DATE + heures Paris HH:MM."""
    ci_h, ci_m = ((ci_hour or DEFAULT_CI_HOUR)[:5].split(":") + ["00"])[:2]
    co_h, co_m = ((co_hour or DEFAULT_CO_HOUR)[:5].split(":") + ["00"])[:2]
    ci_y, ci_mo, ci_d = (int(x) for x in checkin_date.split("-"))
    co_y, co_mo, co_d = (int(x) for x in checkout_date.split("-"))
    ci_dt = datetime(ci_y, ci_mo, ci_d, int(ci_h), int(ci_m), tzinfo=PARIS_TZ)
    co_dt = datetime(co_y, co_mo, co_d, int(co_h), int(co_m), tzinfo=PARIS_TZ)
    return int(ci_dt.timestamp() * 1000), int(co_dt.timestamp() * 1000)


# ── Main recreate logic ───────────────────────────────────────────────────────

def _recreate_pin(row: dict) -> tuple[bool, Optional[str]]:
    """POST nouveau PIN côté Sofia avec MÊME deviceId + window calculée."""
    duve_resa_id = row["duve_reservation_id"]

    # Whitelist
    apt_pid = (row.get("duve_property_id") or "").lower()
    if ALLOWED_PROPERTY_IDS and apt_pid not in ALLOWED_PROPERTY_IDS:
        logger.info(f"⏭️ {duve_resa_id} hors whitelist property={apt_pid} → skip")
        return False, "skipped: whitelist"

    # Dates : Mews (SoT, rafraîchi 2h) d'abord, fallback cache.
    ci_date = row.get("mews_checkin_date") or row.get("checkin_date")
    co_date = row.get("mews_checkout_date") or row.get("checkout_date")
    if ci_date is None or co_date is None:
        return False, "missing CI/CO date"
    ci_date_str = ci_date.isoformat() if hasattr(ci_date, "isoformat") else str(ci_date)
    co_date_str = co_date.isoformat() if hasattr(co_date, "isoformat") else str(co_date)

    # Bornes d'heures : heures de POLITIQUE appart (earliest/latest), stables.
    # On n'utilise PAS l'heure estimée du form (volatile) : un guest qui avance son
    # arrivée serait sinon bloqué dehors (cf. analyse incident Crystal — estimé 21h30
    # vs arrivée ~14h = 7h de lockout). Mieux vaut une borne large qu'un lockout.
    ci_hour = row.get("earliest_checkin_hour") or DEFAULT_CI_HOUR
    co_hour = row.get("latest_checkout_hour") or DEFAULT_CO_HOUR

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
                    _mark_recreate_success(row["duve_reservation_id"])
                else:
                    logger.info(f"🌗 cache.recreated_at update skipped (shadow)")
            except Exception as e:
                logger.warning(f"⚠️ cache update failed for {row['duve_reservation_id']}: {e}")
            ok += 1
        else:
            try:
                _mark_recreate_error(row["duve_reservation_id"], err)
            except Exception:
                pass
            logger.warning(f"⚠️ recreate failed for {row['duve_reservation_id']}: {err}")
            fail += 1

    # ── 2. Archive (checkout passé) ─────────────────────────────────────────
    to_archive = _resa_to_archive()
    logger.info(f"🗑️ {len(to_archive)} résa(s) à archiver (CO passé ou annulée)")
    archived = 0
    for row in to_archive:
        success, err = _archive_pin(row)
        if success:
            # En shadow mode, on archive aussi côté BQ pour éviter l'accumulation
            # de rows expirées (le DELETE Sofia reste skipped via _archive_pin).
            try:
                _mark_archived(row["duve_reservation_id"])
                archived += 1
            except Exception as e:
                logger.warning(f"⚠️ archive update failed for {row['duve_reservation_id']}: {e}")
        else:
            logger.warning(f"⚠️ archive failed for {row['duve_reservation_id']}: {err}")

    logger.info("=" * 70)
    logger.info(f"DONE — recreate ok={ok} fail={fail} | archived={archived}")
    logger.info("=" * 70)
