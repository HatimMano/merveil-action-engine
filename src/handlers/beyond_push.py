"""
Beyond Gap Push — job déclaratif de push des fenêtres de prix sur les gaps.

Décision call Raphael/Arnaud 07/07/2026 (remplissage gaps 1N = 2,5%) : le DWH
pousse une fenêtre de prix [point mort, ADR voisin ÷ markup] sur chaque nuit de
gap 1N des apparts whitelistés, via les seasonal-prices Beyond (override
officiellement supporté, confirmation écrite support 09/07).

Extensions call 04/08 (livrées 06/08) : gaps 2N (fenêtre start ≠ end, plancher
par nuit ÷ 2, scope 1N/2N/BOTH dans la whitelist), nuits orphelines (fin de
trou : nuit du jour libre + arrivée demain + veille passée → plancher plein
poussé pour la journée, run additionnel 6h45). Le détail des cibles vit dans
dash_beyond_push_targets — ce job ne connaît que des fenêtres (start, end).

Déclaratif : l'état VOULU vit dans dashboard_ventes.dash_beyond_push_targets
(recalculé à chaque run dbt). Pour chaque listing whitelisté :
  1. GET /listings/<id>/customizations/min-max-prices/ → liste actuelle
  2. Sépare NOS fenêtres (reconnues via beyond_raw.price_pushes_log par
     (listing, start, end), dernière action != remove — Beyond n'a pas de champ
     label) des règles ÉQUIPE (préservées telles quelles, rollover inclus)
  3. Diff état voulu vs nos fenêtres actuelles → PATCH la liste complète
     (règles équipe + fenêtres voulues) SEULEMENT si écart. Le support a
     confirmé qu'un envoi suffit (l'algo ne ré-écrase pas) → jamais de re-push
     de maintien.
  4. Log chaque action dans beyond_raw.price_pushes_log (append-only).

Le déclaratif nettoie tout seul : gap comblé → fenêtre absente de l'état voulu
→ retirée au run suivant (latence 24h max — une fenêtre stale primerait sur le
plancher annuel Beyond, cf. finding POC #3).

Garde-fous :
  - fenêtres GAP (1N/2N) uniquement sur les listings whitelistés (gate côté
    dash_beyond_push_targets) ; les nuits ORPHELINES couvrent le parc entier
    (pure protection plancher, décision 06/08). Le job visite whitelist ∪
    listings avec cible ∪ listings possédant une fenêtre (retrait J+1 garanti)
  - règle Hatim 17/07 : min = ménage + ops + coussin, JAMAIS en dessous (« mieux
    vaut ne pas vendre que vendre sous coussin »). Si une règle équipe chevauche
    avec un plancher PLUS HAUT → min relevé (on ne casse jamais un plancher
    équipe) ; un plancher équipe plus bas est bypassé. Le max suit toujours le
    min (max(max, min)) → aucune fenêtre skippée.
  - bornes de sécurité AVANT tout PATCH : PRICE_FLOOR ≤ min ≤ max ≤ PRICE_CEILING
    (défaut 50..5000 €, mêmes bornes que le test dbt ADR). Fenêtre hors bornes →
    écartée de l'état voulu (donc retirée de Beyond si possédée) + erreur mail.
    Couvre aussi bien un target dbt aberrant (coussin cassé par le référentiel
    Sheets) qu'une règle équipe extrême. min ≤ max garanti par construction.
  - dates futures, ≤ MAX_WINDOWS/listing
  - BEYOND_SHADOW_MODE=true → log "would patch" sans écrire
  - toute erreur → mail récap (infra Gmail DWD), crash → mail + exit non-zero

Trigger : Cloud Run Job `merveil-action-engine-beyond` (scheduler daily 10:45
Paris, après le run dbt de 10:15 → targets frais).
"""

import base64
import json
import logging
import os
import time
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Optional

import requests
from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "merveil-data-warehouse")
TARGETS_TABLE = os.environ.get(
    "BEYOND_TARGETS_TABLE", "merveil-data-warehouse.dashboard_ventes.dash_beyond_push_targets")
# Vue de validation sur la table ÉDITABLE dwh_inputs.beyond_push_whitelist
# (dashboard → /api/rules/*, POC rules-edition 2026-07-18) — résout le listing,
# exclut les codes invalides, fallback seed si la table est vidée par erreur.
WHITELIST_TABLE = os.environ.get(
    "BEYOND_WHITELIST_TABLE", "merveil-data-warehouse.staging.stg_inputs__beyond_push_whitelist")
LOG_TABLE = os.environ.get(
    "BEYOND_LOG_TABLE", "merveil-data-warehouse.beyond_raw.price_pushes_log")

BEYOND_BASE_URL = os.environ.get("BEYOND_BASE_URL", "https://developers.beyondpricing.com")
BEYOND_PAT = (os.environ.get("BEYOND_PAT") or "").strip()

SHADOW_MODE = os.environ.get("BEYOND_SHADOW_MODE", "false").lower() == "true"
MAX_WINDOWS_PER_LISTING = int(os.environ.get("BEYOND_MAX_WINDOWS", "20"))

# Bornes de sécurité prix (€/nuit HT) — dernier filet avant PATCH, indépendant du
# SQL amont. Alignées sur le test dbt ADR (50..5000). Fenêtre hors bornes = skip.
PRICE_FLOOR = float(os.environ.get("BEYOND_PRICE_FLOOR", "50"))
PRICE_CEILING = float(os.environ.get("BEYOND_PRICE_CEILING", "5000"))

GMAIL_SENDER = os.getenv("GMAIL_SENDER", "noreply@archides.fr")
ALERT_TO = os.getenv("BEYOND_ALERT_TO", "hatim@archides.fr")

JSONAPI_HEADERS = {
    "Content-Type": "application/vnd.api+json",
    "Accept": "application/vnd.api+json",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

_bq_client: Optional[bigquery.Client] = None


def _bq() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


def _send_alert(subject: str, body: str) -> None:
    """Mail d'alerte best-effort (même infra Gmail DWD que l'orchestrateur ISEO)."""
    try:
        name = f"projects/{PROJECT_ID}/secrets/alerts-gmail-sa-key/versions/latest"
        sa_info = secretmanager.SecretManagerServiceClient().access_secret_version(
            name=name).payload.data.decode()
        creds = service_account.Credentials.from_service_account_info(
            json.loads(sa_info), scopes=["https://www.googleapis.com/auth/gmail.send"]
        ).with_subject(GMAIL_SENDER)
        svc = build("gmail", "v1", credentials=creds, cache_discovery=False)
        msg = MIMEText(body, "plain", "utf-8")
        msg["From"], msg["To"], msg["Subject"] = GMAIL_SENDER, ALERT_TO, subject
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        svc.users().messages().send(userId=GMAIL_SENDER, body={"raw": raw}).execute()
        logger.info(f"📧 alerte envoyée à {ALERT_TO}: {subject}")
    except Exception as e:
        logger.error(f"⚠️ envoi alerte échoué: {e}")


def _beyond(method: str, path: str, payload: Optional[dict] = None) -> requests.Response:
    if not BEYOND_PAT:
        raise RuntimeError("BEYOND_PAT absent des env vars (secret beyond-pat-dwh)")
    headers = {"Authorization": f"Bearer {BEYOND_PAT}", **JSONAPI_HEADERS}
    resp = requests.request(
        method, f"{BEYOND_BASE_URL}{path}", headers=headers,
        json=payload, timeout=30)
    # rate-limit soft : l'API v1 est généreuse mais on est poli (≤6 listings/run)
    time.sleep(0.5)
    return resp


# ── État : voulu / réel / possédé ─────────────────────────────────────────────

def _load_whitelist() -> list[dict]:
    """Listings whitelistés (seed dbt). Élargir = 1 ligne CSV + dbt seed."""
    rows = _bq().query(
        f"SELECT apartment_code, CAST(beyond_listing_id AS INT64) AS listing_id "
        f"FROM `{WHITELIST_TABLE}` "
        f"WHERE COALESCE(TRIM(apartment_code), '') != '' AND beyond_listing_id IS NOT NULL"
    ).result()
    wl = [dict(r.items()) for r in rows]
    if not wl:
        raise RuntimeError("whitelist beyond_push_whitelist vide — rien à faire (seed manquant ?)")
    return wl


def _load_targets() -> dict[int, dict[tuple, dict]]:
    """État voulu par listing : {(start, end): {min, max}}.

    Fenêtres 1N (gaps 1N + nuits orphelines : start = end) et 2N
    (gaps 2N : end = start + 1, plancher par nuit ÷ 2 — décisions 04/08)."""
    rows = _bq().query(f"""
        SELECT beyond_listing_id AS listing_id, apartment_code,
               CAST(window_start AS STRING) AS s, CAST(window_end AS STRING) AS e,
               min_price, max_price
        FROM `{TARGETS_TABLE}`
        -- >= sur la FIN de fenêtre : elle tient jusqu'au soir de sa dernière
        -- nuit (retrait au run suivant) — sinon le run du matin du gap la
        -- retirait et Beyond repassait en pricing libre (min-stay 1 + prix
        -- cassé) sur les dernières heures, en contradiction avec la règle
        -- « jamais sous coussin » (décision 04/08). Un gap 2N dont la 1re nuit
        -- est passée sort de lui-même de l'état voulu au refresh dbt (la nuit
        -- restante devient orpheline, plancher plein).
        WHERE window_end >= CURRENT_DATE('Europe/Paris')
    """).result()
    targets: dict[int, dict[tuple, dict]] = {}
    for r in rows:
        key = (r["s"], r["e"])
        targets.setdefault(r["listing_id"], {})[key] = {
            # NULL/0 → -1 : recalé par le garde-fou bornes (skip + mail) au lieu
            # de crasher le run entier sur float(None).
            "min": float(r["min_price"] or -1), "max": float(r["max_price"] or -1),
            "apartment_code": r["apartment_code"],
        }
    return targets


def _load_owned() -> tuple[dict[int, set[tuple]], dict[int, str]]:
    """Fenêtres possédées par le DWH : dernière action loggée != remove/skip/error.

    Retourne aussi listing_id → apartment_code (pour visiter les listings hors
    whitelist qui portent encore une fenêtre à retirer — cas nuit orpheline
    parc entier de la veille)."""
    rows = _bq().query(f"""
        SELECT listing_id, apartment_code,
               CAST(start_date AS STRING) AS s, CAST(end_date AS STRING) AS e
        FROM `{LOG_TABLE}`
        WHERE status = 'ok'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY listing_id, start_date, end_date ORDER BY pushed_at DESC
        ) = 1 AND action IN ('add', 'update')
    """).result()
    owned: dict[int, set[tuple]] = {}
    apt_by_listing: dict[int, str] = {}
    for r in rows:
        owned.setdefault(r["listing_id"], set()).add((r["s"], r["e"]))
        if r["apartment_code"]:
            apt_by_listing.setdefault(r["listing_id"], r["apartment_code"])
    return owned, apt_by_listing


def _get_current(listing_id: int) -> tuple[Optional[list[dict]], Optional[str]]:
    """Liste seasonal-prices actuelle du listing (dasherized, telle que l'API la rend)."""
    resp = _beyond("GET", f"/api/v1/listings/{listing_id}/customizations/min-max-prices/")
    if resp.status_code != 200:
        return None, f"GET {resp.status_code}: {resp.text[:200]}"
    attrs = resp.json().get("data", {}).get("attributes", {})
    return attrs.get("seasonal-prices") or [], None


def _overlaps(start: str, end: str, rule: dict) -> bool:
    """Chevauchement de dates avec une règle équipe. Les règles rollover:true se
    répètent chaque année → comparaison sur (mois, jour) ; sinon dates pleines."""
    if rule.get("rollover"):
        md = lambda d: d[5:]  # 'MM-DD'
        rs, re_ = md(rule["start-date"]), md(rule["end-date"])
        s, e = md(start), md(end)
        if rs <= re_:
            return not (e < rs or s > re_)
        # règle à cheval sur le nouvel an (ex. 15/12 → 15/01)
        return s >= rs or e <= re_
    return not (end < rule["start-date"] or start > rule["end-date"])


def _log_rows(rows: list[dict]) -> None:
    if rows:
        errors = _bq().insert_rows_json(LOG_TABLE.replace("`", ""), rows)
        if errors:
            logger.error(f"⚠️ insert log échoué: {errors}")


# ── Cœur : réconciliation par listing ─────────────────────────────────────────

def _reconcile_listing(listing_id: int, apartment_code: str,
                       desired: dict[tuple, dict], owned_keys: set[tuple],
                       run_id: str) -> tuple[list[dict], list[str]]:
    """Retourne (log_rows, erreurs). PATCH seulement si diff."""
    now = datetime.now(timezone.utc).isoformat()
    logs: list[dict] = []
    errs: list[str] = []

    def log(action, status, start=None, end=None, mn=None, mx=None,
            http=None, error=None, ctx=None):
        logs.append({
            "run_id": run_id, "pushed_at": now, "listing_id": listing_id,
            "apartment_code": apartment_code, "start_date": start, "end_date": end,
            "min_price": mn, "max_price": mx, "action": action, "status": status,
            "http_status": http, "error": error, "context": ctx,
        })

    current, err = _get_current(listing_id)
    if err:
        errs.append(f"{apartment_code} ({listing_id}): {err}")
        log("get", "error", error=err)
        return logs, errs

    ours_current = {(w["start-date"], w["end-date"]): w
                    for w in current if (w["start-date"], w["end-date"]) in owned_keys}
    team_rules = [w for w in current if (w["start-date"], w["end-date"]) not in owned_keys]

    # Fenêtres voulues. Un plancher équipe plus HAUT relève notre min (on ne
    # casse jamais une règle équipe) ; plus bas → bypassé (règle coussin).
    # Le max suit le min → jamais de fenêtre incohérente, jamais de skip.
    final_desired: dict[tuple, dict] = {}
    for (start, end), t in sorted(desired.items()):
        mn, mx = t["min"], t["max"]
        for rule in team_rules:
            if rule.get("min-price") and _overlaps(start, end, rule):
                mn = max(mn, float(rule["min-price"]))
        final_desired[(start, end)] = {"min": mn, "max": max(mx, mn)}

    # Garde-fou bornes prix : jamais de PATCH avec une fenêtre aberrante, quelle
    # que soit son origine (target dbt corrompu, plancher équipe extrême). Fenêtre
    # hors bornes → écartée de l'état voulu (si possédée, le diff la retire de
    # Beyond : mieux vaut aucune fenêtre qu'une fenêtre fausse) + erreur mail.
    for k in list(final_desired):
        v = final_desired[k]
        if not (PRICE_FLOOR <= v["min"] <= v["max"] <= PRICE_CEILING):
            errs.append(f"{apartment_code} {k[0]}: fenêtre [{v['min']}, {v['max']}] "
                        f"hors bornes [{PRICE_FLOOR}, {PRICE_CEILING}] — écartée, pas de push")
            log("skip", "error", k[0], k[1], v["min"], v["max"],
                error=f"hors bornes [{PRICE_FLOOR}, {PRICE_CEILING}]")
            del final_desired[k]

    if len(final_desired) > MAX_WINDOWS_PER_LISTING:
        errs.append(f"{apartment_code}: {len(final_desired)} fenêtres > cap "
                    f"{MAX_WINDOWS_PER_LISTING} — run skippé pour ce listing")
        log("cap_exceeded", "error", error=f"{len(final_desired)} fenêtres")
        return logs, errs

    # Diff état voulu vs nos fenêtres actuelles
    to_add = [k for k in final_desired if k not in ours_current]
    to_remove = [k for k in ours_current if k not in final_desired]
    to_update = [k for k in final_desired if k in ours_current and (
        float(ours_current[k].get("min-price") or 0) != final_desired[k]["min"]
        or float(ours_current[k].get("max-price") or 0) != final_desired[k]["max"])]

    if not to_add and not to_remove and not to_update:
        logger.info(f"✓ {apartment_code} ({listing_id}) : {len(final_desired)} fenêtre(s), aucun écart")
        return logs, errs

    new_list = team_rules + [
        {"start-date": s, "end-date": e, "rollover": False,
         "min-price": v["min"], "max-price": v["max"]}
        for (s, e), v in sorted(final_desired.items())
    ]

    if SHADOW_MODE:
        logger.info(f"🌗 SHADOW {apartment_code}: would PATCH "
                    f"+{len(to_add)} ~{len(to_update)} -{len(to_remove)} "
                    f"(équipe préservée: {len(team_rules)})")
        for k in to_add + to_update:
            v = final_desired[k]
            log("add" if k in to_add else "update", "shadow", k[0], k[1], v["min"], v["max"])
        for k in to_remove:
            log("remove", "shadow", k[0], k[1])
        return logs, errs

    payload = {"data": {
        "type": "min-max-price-customizations",
        "id": str(listing_id),
        "attributes": {"seasonal-prices": new_list},
    }}
    resp = _beyond("PATCH", f"/api/v1/listings/{listing_id}/customizations/min-max-prices/", payload)
    ok = resp.status_code == 200
    if not ok:
        err = f"PATCH {resp.status_code}: {resp.text[:300]}"
        errs.append(f"{apartment_code} ({listing_id}): {err}")

    for k in to_add:
        v = final_desired[k]
        log("add", "ok" if ok else "error", k[0], k[1], v["min"], v["max"],
            resp.status_code, None if ok else resp.text[:300])
    for k in to_update:
        v = final_desired[k]
        log("update", "ok" if ok else "error", k[0], k[1], v["min"], v["max"],
            resp.status_code, None if ok else resp.text[:300])
    for k in to_remove:
        log("remove", "ok" if ok else "error", k[0], k[1],
            http=resp.status_code, error=None if ok else resp.text[:300])

    if ok:
        logger.info(f"🚀 {apartment_code} ({listing_id}) : PATCH ok — "
                    f"+{len(to_add)} ~{len(to_update)} -{len(to_remove)} "
                    f"(équipe préservée: {len(team_rules)})")
    return logs, errs


# ── Entry point ───────────────────────────────────────────────────────────────

def run() -> None:
    """Wrapper : tout crash → alerte mail + exit non-zero."""
    try:
        _run_inner()
    except Exception as e:
        logger.critical(f"🔴 Beyond push CRASH: {e}")
        _send_alert("🔴 Beyond gap push — CRASH", f"Le job a planté.\n\nException : {e}")
        raise


def _run_inner() -> None:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    whitelist = _load_whitelist()
    targets = _load_targets()
    owned, owned_apt = _load_owned()

    # Listings à visiter = whitelist ∪ listings avec cible ∪ listings possédant
    # encore une fenêtre. Depuis le 06/08 les nuits orphelines couvrent le PARC
    # ENTIER (cibles hors whitelist) — et leur fenêtre de la veille doit être
    # retirée même si le listing n'a plus aucune cible aujourd'hui.
    visit: dict[int, str] = {w["listing_id"]: w["apartment_code"] for w in whitelist}
    for lid, wins in targets.items():
        if lid not in visit:
            visit[lid] = next(iter(wins.values()))["apartment_code"]
    for lid in owned:
        visit.setdefault(lid, owned_apt.get(lid, "?"))

    logger.info("=" * 70)
    logger.info(f"🚀 Beyond gap push (shadow={SHADOW_MODE}, "
                f"{len(whitelist)} whitelistés, {len(visit)} listings visités, "
                f"{sum(len(v) for v in targets.values())} fenêtres voulues)")
    logger.info("=" * 70)

    all_logs: list[dict] = []
    all_errs: list[str] = []
    for lid, apt in sorted(visit.items()):
        logs, errs = _reconcile_listing(
            lid, apt, targets.get(lid, {}), owned.get(lid, set()), run_id)
        all_logs.extend(logs)
        all_errs.extend(errs)

    # Heartbeat : 1 ligne par run, même sans écart — permet au dashboard
    # d'afficher « vérifié pour la dernière fois à HH:MM » (l'absence d'action
    # au 10h45 signifie "vérifié, aucun écart", pas "pas tourné").
    all_logs.append({
        "run_id": run_id, "pushed_at": datetime.now(timezone.utc).isoformat(),
        "listing_id": None, "apartment_code": None, "start_date": None,
        "end_date": None, "min_price": None, "max_price": None,
        "action": "check", "status": "ok", "http_status": None, "error": None,
        "context": f"{len(whitelist)} listings · "
                   f"{sum(len(v) for v in targets.values())} fenêtres voulues · "
                   f"{len(all_logs)} action(s) · {len(all_errs)} erreur(s)",
    })

    _log_rows(all_logs)
    logger.info(f"DONE — {len(all_logs) - 1} action(s) loggée(s), {len(all_errs)} erreur(s)")
    if all_errs:
        _send_alert(
            f"⚠️ Beyond gap push — {len(all_errs)} erreur(s)",
            "Erreurs du run " + run_id + " :\n\n- " + "\n- ".join(all_errs)
            + f"\n\nLog : beyond_raw.price_pushes_log (run_id={run_id})")
