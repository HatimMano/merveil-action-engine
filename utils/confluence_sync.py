#!/usr/bin/env python3
"""
Sync Confluence ← état réel des règles machine (pages vivantes).

Job Cloud Run `confluence-rules-sync` (daily 7h40 Paris). La mise à jour de la
doc cesse d'être une action (oubliable) pour devenir une conséquence : ce job
régénère les pages « Règle — … » des spaces métier merveil.atlassian.net depuis
la prose déclarée ici + des BLOCS VIVANTS requêtés dans BigQuery à chaque run
(état de la whitelist, dernière modification auditée, dernière activité réelle).

Remplace le script local Archides/docs/confluence/build_rules_merveil.py
(prototype VD 10/07, prose conservée telle quelle). Éditer la prose = éditer
RULES dans `utils/confluence_rules.py` puis laisser le job tourner (ou
l'exécuter manuellement).

V1 (2026-07-21) : blocs vivants scopés à la règle « Push automatique des prix »
(whitelist Beyond éditable depuis le dash — rules-edition 18/07). Brancher une
autre règle = lui donner un champ `live` + le gérer dans collect_live().

V2 (2026-07-22) : multi-spaces. Chaque règle porte un champ `space` (défaut VD) ;
1 page racine « 🤖 Automatisations (DWH) — <domaine> » par space (VD Ventes, GDA
Opérations, TRAN Serrures & accès) + index global cross-spaces dans EN
(00. Entreprise, report CQL label=regle-dwh). Ajouter un domaine = 1 entrée
SPACES + des règles avec ce `space`. Bloc vivant serrures : collector `iseo`.

V3 (2026-08-12) : la prose part dans `utils/confluence_rules.py` (contenu pur),
ce module ne garde que le moteur. Les faits DÉRIVABLES ne s'écrivent plus à la
main : une règle qui déclare `triggers=[...]` voit son statut actif/inactif, sa
fréquence, son canal et ses destinataires lus dans `config/routing.yaml` au run
(`derive_facts`). Une règle désactivée dans le routing ne peut plus s'afficher
« Actif » sur Confluence — c'était le cas avant, en dur.

Local : `python3 -m utils.confluence_sync` (token via gcloud CLI, BQ via ADC).
"""
import base64
import html
import json
import logging
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from utils.confluence_rules import (CROSSLINK_MARK, CROSSLINK_TITRE, CROSSLINKS, FOOTER,
                                    INDEX_SPACE, INDEX_TITLE, NIVEAUX, RULES,
                                    SPACES)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("confluence_sync")

PROJECT   = os.environ.get("GCP_PROJECT_ID", "merveil-data-warehouse")
SITE      = "merveil"
EMAIL     = "hatim@archides.fr"
SECRET    = "confluence-api-token"
BASE      = f"https://{SITE}.atlassian.net"
PARIS     = ZoneInfo("Europe/Paris")

GMAIL_SENDER = os.getenv("GMAIL_SENDER", "noreply@archides.fr")
ALERT_TO     = os.getenv("CONFLUENCE_ALERT_TO", "hatim@archides.fr")


# ── Auth / clients ────────────────────────────────────────────────────────────

def _secret(name: str) -> str:
    """Secret Manager via lib (job Cloud Run), fallback gcloud CLI (local)."""
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        path = f"projects/{PROJECT}/secrets/{name}/versions/latest"
        return client.access_secret_version(name=path).payload.data.decode().strip()
    except Exception as e:
        logger.info(f"secretmanager lib KO ({e.__class__.__name__}) → fallback gcloud CLI")
        return subprocess.run(
            ["gcloud", "secrets", "versions", "access", "latest",
             f"--secret={name}", f"--project={PROJECT}"],
            capture_output=True, text=True, check=True).stdout.strip()


TOKEN = None
AUTH = None


def req(method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method, headers={
        "Authorization": "Basic " + AUTH, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {path} -> HTTP {e.code}: {e.read().decode()[:300]}")


def _bq():
    from google.cloud import bigquery
    return bigquery.Client(project=PROJECT)


def _send_alert(subject: str, body: str, html_body: bool = False) -> None:
    """Mail d'alerte best-effort (infra commune src/core/mailer)."""
    from src.core.mailer import send_mail
    send_mail(subject, body, ALERT_TO, html=html_body, sender=GMAIL_SENDER)


# ── storage XHTML ─────────────────────────────────────────────────────────────

def esc(s): return html.escape(s or "", quote=False)

def status(colour, title):
    return (f'<ac:structured-macro ac:name="status"><ac:parameter ac:name="colour">{colour}</ac:parameter>'
            f'<ac:parameter ac:name="title">{esc(title)}</ac:parameter></ac:structured-macro>')

def panel(name, body, title=None):
    t = f'<ac:parameter ac:name="title">{esc(title)}</ac:parameter>' if title else ""
    return f'<ac:structured-macro ac:name="{name}">{t}<ac:rich-text-body>{body}</ac:rich-text-body></ac:structured-macro>'

def details(rows):
    trs = "".join(f"<tr><th>{esc(k)}</th><td>{v}</td></tr>" for k, v in rows)
    return ('<ac:structured-macro ac:name="details"><ac:rich-text-body>'
            f'<table><tbody>{trs}</tbody></table></ac:rich-text-body></ac:structured-macro>')

def report(cql, headings, sort):
    return ('<ac:structured-macro ac:name="detailssummary">'
            f'<ac:parameter ac:name="cql">{esc(cql)}</ac:parameter>'
            f'<ac:parameter ac:name="headings">{esc(headings)}</ac:parameter>'
            f'<ac:parameter ac:name="sortBy">{esc(sort)}</ac:parameter></ac:structured-macro>')

def link(url, label=None):
    return f'<a href="{esc(url)}">{esc(label or url)}</a>'

def p(txt): return f"<p>{txt}</p>"
def h2(txt): return f"<h2>{esc(txt)}</h2>"

def fmt_ts(ts):
    """TIMESTAMP BQ (UTC) → « 21/07/2026 à 10h45 » heure Paris."""
    if ts is None:
        return None
    return ts.astimezone(PARIS).strftime("%d/%m/%Y à %Hh%M")

def fmt_d(d):
    return d.strftime("%d/%m/%Y") if d else None


# ── upsert ────────────────────────────────────────────────────────────────────

def find_page(title, space_id):
    # Lookup direct par titre (API v2) — la recherche CQL est indexée avec du
    # retard et rate les pages tout juste créées (doublon au re-run).
    q = urllib.request.quote(title)
    res = req("GET", f"/wiki/api/v2/pages?space-id={space_id}&title={q}&limit=1")
    return res["results"][0]["id"] if res["results"] else None


def upsert(title, parent_id, body, labels, space):
    """space = {'id': …, 'key': …} résolu au run."""
    pid = find_page(title, space["id"])
    if pid:
        cur = req("GET", f"/wiki/api/v2/pages/{pid}?body-format=storage")
        req("PUT", f"/wiki/api/v2/pages/{pid}", {
            "id": str(pid), "status": "current", "title": title,
            "body": {"representation": "storage", "value": body},
            "version": {"number": cur["version"]["number"] + 1, "message": "confluence-rules-sync"}})
        action = "maj"
    else:
        res = req("POST", "/wiki/api/v2/pages", {
            "spaceId": space["id"], "status": "current", "title": title,
            "parentId": str(parent_id),
            "body": {"representation": "storage", "value": body}})
        pid = res["id"]
        action = "créé"
    if labels:
        req("POST", f"/wiki/rest/api/content/{pid}/label",
            [{"prefix": "global", "name": l} for l in labels])
    logger.info(f"{action} : {title}  ({BASE}/wiki/spaces/{space['key']}/pages/{pid})")
    return pid


# ── blocs vivants (BigQuery) ──────────────────────────────────────────────────

def _collect_beyond_push(bq) -> dict:
    """État réel du push Beyond : whitelist, fenêtres actives, audit, activité."""
    live = {"generated_at": datetime.now(PARIS).strftime("%d/%m/%Y à %Hh%M")}

    whitelist = [r.apartment_code + ("" if (r.scope or "1N") == "1N" else f" ({r.scope})")
                 for r in bq.query(f"""
        SELECT apartment_code, scope FROM `{PROJECT}.dwh_inputs.beyond_push_whitelist`
        ORDER BY apartment_code""").result()]

    windows = [dict(apartment_code=r.apartment_code, start=r.start_date,
                    end=r.end_date, min=r.min_price, max=r.max_price)
               for r in bq.query(f"""
        WITH last_state AS (
          SELECT apartment_code, start_date, end_date, min_price, max_price, action,
                 ROW_NUMBER() OVER (PARTITION BY listing_id, start_date, end_date
                                    ORDER BY pushed_at DESC) AS rn
          FROM `{PROJECT}.beyond_raw.price_pushes_log`
          WHERE action IN ('add', 'update', 'remove') AND status = 'ok'
        )
        SELECT apartment_code, start_date, end_date, min_price, max_price
        FROM last_state
        WHERE rn = 1 AND action != 'remove' AND end_date >= CURRENT_DATE('Europe/Paris')
        ORDER BY start_date, apartment_code""").result()]

    edits = []
    for row in bq.query(f"""
        SELECT action, row_before, row_after, edited_by, edited_at
        FROM `{PROJECT}.dwh_inputs.rules_audit_log`
        WHERE rule_table = 'beyond_push_whitelist'
        ORDER BY edited_at DESC LIMIT 5""").result():
        code = None
        for raw in (row.row_after, row.row_before):
            if raw:
                try:
                    code = json.loads(raw).get("apartment_code")
                except (ValueError, AttributeError):
                    pass
            if code:
                break
        verbe = {"add": "ajout de", "remove": "retrait de",
                 "migrate": "initialisation (migration seed)"}.get(row.action, row.action)
        qui = (row.edited_by or "?").split("@")[0]
        if row.action == "migrate":
            edits.append(f"{verbe} le {fmt_ts(row.edited_at)}")
        else:
            edits.append(f"{verbe} {code or '?'} le {fmt_ts(row.edited_at)} par {qui}")

    live["last_run"] = fmt_ts(next(iter(bq.query(f"""
        SELECT MAX(pushed_at) AS ts
        FROM `{PROJECT}.beyond_raw.price_pushes_log`""").result())).ts)

    last_gap_filled = fmt_ts(next(iter(bq.query(f"""
        SELECT MAX(dispatched_at) AS ts
        FROM `{PROJECT}.action_engine.dispatched_actions`
        WHERE trigger_name = 'beyond_gap_filled'""").result())).ts)

    items = [f"<strong>Appartements du pilote ({len(whitelist)})</strong> : "
             + ", ".join(f"<code>{esc(c)}</code>" for c in whitelist)]
    if windows:
        nights = sum((w["end"] - w["start"]).days + 1 for w in windows)
        items.append(f"<strong>Fenêtres de prix actives dans Beyond</strong> : {len(windows)} "
                     f"({nights} nuit(s), de {fmt_d(min(w['start'] for w in windows))} "
                     f"à {fmt_d(max(w['end'] for w in windows))})")
    else:
        items.append("<strong>Fenêtres de prix actives dans Beyond</strong> : aucune actuellement")
    if edits:
        subs = "".join(f"<li>{esc(e)}</li>" for e in edits)
        items.append(f"<strong>Dernières modifications de la liste</strong> :<ul>{subs}</ul>")
    if live["last_run"]:
        items.append(f"<strong>Dernier run du job vérifié</strong> : {esc(live['last_run'])} ✓")
    items.append("<strong>Dernière nuit seule vendue via push</strong> : "
                 + (esc(last_gap_filled) if last_gap_filled else "aucune pour l'instant"))
    live["items"] = items
    return live


def _collect_iseo(bq) -> dict:
    """État réel de l'orchestrateur serrures : whitelist, codes actifs, activité."""
    live = {"generated_at": datetime.now(PARIS).strftime("%d/%m/%Y à %Hh%M")}

    whitelist = [r.apartment_code for r in bq.query(f"""
        SELECT apartment_code FROM `{PROJECT}.staging.iseo_whitelisted_apartments`
        ORDER BY apartment_code""").result()]

    counts = next(iter(bq.query(f"""
        SELECT
          COUNTIF(archived_at IS NULL AND provisioned_at IS NOT NULL)  AS actifs,
          COUNTIF(archived_at IS NULL AND provisioned_at IS NOT NULL
                  AND checkin_date > CURRENT_DATE('Europe/Paris'))     AS a_venir,
          COUNTIF(provisioned_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(),
                                                  INTERVAL 7 DAY))     AS sem,
          MAX(provisioned_at)                                          AS last_ts
        FROM `{PROJECT}.iseo_raw.merveil_pin_cache`""").result()))
    live["last_run"] = fmt_ts(counts.last_ts)

    recon = next(iter(bq.query(f"""
        SELECT COUNT(*) AS n, COUNTIF(severity = 'CRITICAL') AS crit
        FROM `{PROJECT}.dashboard_ops.dash_ops_pin_reconciliation`""").result()))

    items = [f"<strong>Appartements basculés ({len(whitelist)})</strong> : "
             + ", ".join(f"<code>{esc(c)}</code>" for c in whitelist)]
    items.append(f"<strong>Codes actifs posés sur les serrures</strong> : {counts.actifs} "
                 f"(dont {counts.a_venir} séjour(s) à venir)")
    items.append(f"<strong>Codes créés sur les 7 derniers jours</strong> : {counts.sem}")
    if live["last_run"]:
        items.append(f"<strong>Dernier code posé</strong> : {esc(live['last_run'])} ✓")
    items.append("<strong>Réconciliation interne ↔ serrures Sofia</strong> : "
                 + ("aucun écart détecté ✅" if recon.n == 0 else
                    f"{recon.n} écart(s) dont {recon.crit} critique(s) — voir le dashboard 7.8"))
    live["items"] = items
    return live


LIVE_COLLECTORS = {"beyond_push": _collect_beyond_push, "iseo": _collect_iseo}


def collect_live(rule: dict, bq) -> dict | None:
    collector = LIVE_COLLECTORS.get(rule.get("live") or "")
    return collector(bq) if collector else None


def live_section(live: dict) -> str:
    body = "<ul>" + "".join(f"<li>{i}</li>" for i in live["items"]) + "</ul>"
    return panel("info", body,
                 title=f"📊 État actuel — généré automatiquement le {live['generated_at']}")


# ── contenu ───────────────────────────────────────────────────────────────────

HEADINGS = "Niveau,Statut,Fréquence,Canal,Owner métier,Depuis,Dernière activité"


# ── faits dérivés de routing.yaml ─────────────────────────────────────────────
# Statut, fréquence, canal et destinataires ne s'écrivent PAS dans la prose :
# ils sont lus dans le routing au run. Une règle qui ne passe pas par le
# dispatcher (job autonome, outil externe) n'a pas de `triggers` et garde ses
# champs manuels.

ROUTING_PATH = Path(__file__).resolve().parent.parent / "config" / "routing.yaml"

# Cadence d'envoi par bucket : portée par les Cloud Schedulers, pas par le
# routing → seul mapping resté déclaratif. Ne contient que les buckets qui ONT
# leur propre scheduler ; un bucket satellite est résolu par son `flush_with`
# (cf. _cadence) — sinon tout nouveau bucket affiche son nom technique comme
# fréquence, ce qu'a fait `fraude` jusqu'au 19/08 (« Fréquence : fraude »).
BUCKET_CADENCE = {
    "daily": "Quotidien 7h",
    "2h":    "Toutes les 2 heures",
    "4h":    "Toutes les 4 heures",
}


def _cadence(routing: dict, bucket: str) -> str:
    """Cadence lisible d'un bucket, en suivant `flush_with` pour les satellites."""
    if bucket in BUCKET_CADENCE:
        return BUCKET_CADENCE[bucket]
    flush = routing.get("digest_buckets", {}).get(bucket, {}).get("flush_with")
    return BUCKET_CADENCE.get(flush, "—")


def load_routing() -> dict:
    import yaml
    with open(ROUTING_PATH) as f:
        return yaml.safe_load(f)


def derive_facts(r: dict, routing: dict) -> dict:
    """Statut / fréquence / canal d'une règle, lus dans routing.yaml.

    Retourne aussi `unknown` : les triggers cités par la prose mais absents du
    routing (prose en avance sur le code, ou trigger renommé).
    """
    names = r.get("triggers") or []
    if not names:                                    # hors dispatcher
        # Rien à dériver : le statut vaut « Actif » sauf déclaration explicite
        # (`statut=("Yellow", "Mode observation")` par ex.). Sans cette porte de
        # sortie, une règle en rodage s'affiche « Actif » — exactement le défaut
        # que la dérivation depuis routing.yaml a corrigé pour les autres.
        return dict(actif=True, statut=r.get("statut") or ("Green", "Actif"),
                    frequence=r.get("frequence", "—"), canal=r.get("canal", "—"),
                    unknown=[])

    declared = routing.get("triggers", {})
    conf = {n: declared[n] for n in names if n in declared}
    unknown = [n for n in names if n not in declared]

    on = [n for n, c in conf.items() if c.get("enabled")]
    if not conf:
        statut = ("Red", "Inconnu du routing")
    elif not on:
        statut = ("Grey", "Inactive")
    elif len(on) < len(conf):
        statut = ("Yellow", f"Partiellement active ({len(on)}/{len(conf)})")
    else:
        statut = ("Green", "Actif")

    buckets, kinds = [], []
    for n in on:
        for a in conf[n].get("actions", []):
            kinds.append(a.get("type"))
            if a.get("bucket") and a["bucket"] not in buckets:
                buckets.append(a["bucket"])

    freq = " · ".join(dict.fromkeys(_cadence(routing, b) for b in buckets))
    canaux = []
    for b in buckets:
        meta = routing.get("digest_buckets", {}).get(b, {})
        dest = ", ".join(meta.get("default_recipients", []))
        canaux.append(f"Mail {meta.get('subject_prefix', b)} → {dest}" if dest
                      else f"Mail {meta.get('subject_prefix', b)}")
    if "breezeway_task" in kinds:
        canaux.append("Création de tâche Breezeway")
    if "asana_task" in kinds:
        canaux.append("Création de tâche Asana")
    if not on:
        canaux = ["— (règle inactive)"]

    # Part réellement hors routing.yaml (job autonome greffé sur la même règle)
    if r.get("frequence_extra"):
        freq = " · ".join(x for x in (freq, r["frequence_extra"]) if x)
    if r.get("canal_extra"):
        canaux.append(r["canal_extra"])

    return dict(actif=bool(on), statut=statut, frequence=freq or "—",
                canal=" · ".join(canaux), unknown=unknown)


def coverage_gap(routing: dict) -> list:
    """Triggers actifs en prod que la prose ne documente nulle part."""
    documented = {n for r in RULES for n in (r.get("triggers") or [])}
    return sorted(n for n, c in routing.get("triggers", {}).items()
                  if c.get("enabled") and n not in documented)


def rule_body(r, live=None, facts=None):
    facts = facts or dict(statut=("Green", "Actif"), frequence=r.get("frequence", "—"),
                          canal=r.get("canal", "—"))
    rows = [
        ("Règle",           f"<strong>{esc(r['titre'])}</strong>"),
        ("Domaine",         esc(r["domaine"])),
        ("Niveau",          status("Blue", r["niveau"]) + f" — {esc(r['niveau_desc'])}"),
        ("Statut",          status(*facts["statut"])),
        ("Fréquence",       esc(facts["frequence"])),
        ("Canal",           esc(facts["canal"])),
        ("Owner métier",    esc(r["owner"])),
        ("Depuis",          esc(r["depuis"])),
        ("Source technique", f"<code>{esc(r['source'])}</code>"),
        ("Dashboard",       link(r["dashboard_url"], "Voir dans le dashboard →")),
    ]
    if live and live.get("last_run"):
        rows.insert(8, ("Dernière activité", esc(live["last_run"])))
    body = details(rows)
    if live:
        body += live_section(live)
    body += h2("Ce que fait la machine")
    body += "".join(p(x) for x in r["quoi"])
    if r.get("exemple"):
        body += h2("Exemple concret")
        body += panel("info", "".join(p(x) for x in r["exemple"]))
    body += h2("Modifier / arrêter la règle")
    if r.get("modifier"):
        body += "".join(p(x) for x in r["modifier"])
    else:
        body += p(esc("Seuils, destinataires et activation sont gérés dans le DWH (pas dans cette page). "
                      "Demande à Hatim — changement effectif en < 1 jour."))
    body += p(esc("Cette page est régénérée automatiquement chaque matin depuis l'état réel du système : "
                  "ne pas l'éditer à la main (les commentaires, eux, sont bienvenus)."))
    return body


# ── cross-links dans les pages process de l'équipe ────────────────────────────

def _norm(s: str) -> str:
    """Storage comparable : Confluence échappe les accents et ajoute ses propres
    attributs de macro (`ac:macro-id`, `ac:schema-version`) au premier
    enregistrement. Sans ça, le bloc régénéré diffère toujours du bloc stocké et
    on repousse une version par jour dans une page qui n'est pas la nôtre."""
    s = re.sub(r'\s+ac:(macro-id|schema-version)="[^"]*"', "", s)
    return re.sub(r"\s+", " ", html.unescape(s)).strip()


def _splice(body: str, mark: str, block: str):
    """Remplace le bloc marqué s'il existe, sinon l'ajoute en fin de page.

    On délimite par la macro qui PORTE le marqueur : Confluence réécrit le
    storage à chaque édition humaine (un commentaire HTML n'y survit pas).
    """
    i = body.find(mark)
    if i == -1:
        return body + block, "ajouté"
    start = body.rfind("<ac:structured-macro", 0, i)
    end = body.find("</ac:structured-macro>", i) + len("</ac:structured-macro>")
    if start == -1 or end <= i:                       # marqueur hors macro → on n'écrase rien
        return body + block, "ajouté (marqueur orphelin)"
    return body[:start] + block + body[end:], "mis à jour"


def crosslink_body(cl: dict, rule_pages: dict) -> str:
    items = []
    for slug in cl["rules"]:
        page = rule_pages.get(slug)
        if not page:
            logger.warning(f"cross-link {cl['page_id']} : règle '{slug}' sans page — ignorée")
            continue
        items.append(f'<li>{link(page["url"] + CROSSLINK_MARK, page["titre"])}</li>')
    if not items:
        return ""
    inner = (p(f"<strong>{esc(CROSSLINK_TITRE)}</strong>")
             + p(cl["intro"])
             + "<ul>" + "".join(items) + "</ul>"
             + p(esc("Ces pages sont régénérées chaque matin depuis ce qui tourne réellement. "
                     "Cet encart est posé automatiquement : le reste de la page n'est jamais modifié.")))
    return panel("info", inner)


def sync_crosslinks(rule_pages: dict, dry_run: bool = False) -> None:
    for cl in CROSSLINKS:
        block = crosslink_body(cl, rule_pages)
        if not block:
            continue
        cur = req("GET", f"/wiki/api/v2/pages/{cl['page_id']}?body-format=storage")
        body = cur["body"]["storage"]["value"]
        new, how = _splice(body, CROSSLINK_MARK, block)
        # Confluence réécrit le storage (accents → entités) : comparer brut
        # ferait un PUT à chaque run, soit une version par jour dans
        # l'historique d'une page qui n'est pas la nôtre.
        if _norm(new) == _norm(body):
            logger.info(f"cross-link inchangé : {cl['titre']}")
            continue
        if dry_run:
            logger.info(f"[dry-run] cross-link {how} → {cl['titre']} "
                        f"({len(body)} → {len(new)} chars)")
            continue
        req("PUT", f"/wiki/api/v2/pages/{cl['page_id']}", {
            "id": str(cl["page_id"]), "status": "current", "title": cur["title"],
            "body": {"representation": "storage", "value": new},
            "version": {"number": cur["version"]["number"] + 1,
                        "message": "confluence-rules-sync (encart automatisations)"}})
        logger.info(f"cross-link {how} : {cl['titre']} "
                    f"({BASE}/wiki/spaces/{cl['space']}/pages/{cl['page_id']})")


def root_body(space_key, meta):
    b = panel("info",
              p(f"Cette rubrique documente <strong>ce que la machine (DWH) fait automatiquement</strong> pour le "
                f"domaine {esc(meta['domaine'])} : surveillances, alertes, et bientôt propositions d'action. Une "
                "page par règle, toujours à jour avec ce qui tourne réellement en production — on n'y documente "
                "<strong>que l'actif</strong> (le backlog vit dans la roadmap)."))
    b += p(NIVEAUX)
    b += h2("Les règles actives")
    b += report(f'space = "{space_key}" and label = "regle-dwh"', HEADINGS, "Règle")
    b += p(FOOTER)
    return b


def index_body(roots):
    """Index global 00. Entreprise — vue CEO cross-spaces, zéro maintenance."""
    b = panel("info",
              p("Vue d'ensemble de <strong>toutes les automatisations du DWH</strong>, tous domaines confondus. "
                "Chaque règle est documentée dans le space de son équipe (une rubrique 🤖 par space) ; ce "
                "tableau est alimenté automatiquement depuis ces pages."))
    b += p(NIVEAUX)
    b += h2("Les rubriques par domaine")
    b += "<ul>" + "".join(
        f'<li>{link(f"{BASE}/wiki/spaces/{key}/pages/{pid}", SPACES[key]["root"])}</li>'
        for key, pid in roots.items()) + "</ul>"
    b += h2("Toutes les règles actives")
    b += report('label = "regle-dwh"', HEADINGS, "Règle")
    b += p(FOOTER)
    return b


def run() -> None:
    global TOKEN, AUTH
    TOKEN = _secret(SECRET)
    AUTH = base64.b64encode(f"{EMAIL}:{TOKEN}".encode()).decode()

    bq = _bq()
    routing = load_routing()
    keys = ",".join(list(SPACES) + [INDEX_SPACE])
    by_key = {s["key"]: s for s in
              req("GET", f"/wiki/api/v2/spaces?keys={keys}&limit=25")["results"]}

    roots, rule_pages = {}, {}
    for key, meta in SPACES.items():
        sp = by_key[key]
        space = {"id": sp["id"], "key": key}
        logger.info(f"Space {key} id={sp['id']} homepage={sp['homepageId']}")
        roots[key] = upsert(meta["root"], sp["homepageId"], root_body(key, meta),
                            ["regle-dwh-index", meta["label"]], space)
        for r in [r for r in RULES if r.get("space", "VD") == key]:
            live = collect_live(r, bq)
            facts = derive_facts(r, routing)
            if facts["unknown"]:
                logger.warning(f"{r['slug']} : trigger(s) absent(s) du routing → "
                               f"{', '.join(facts['unknown'])}")
            pid = upsert(f"Règle — {r['titre']}", roots[key], rule_body(r, live, facts),
                         ["regle-dwh", meta["label"], f"niveau-{r['niveau'].lower()}"], space)
            rule_pages[r["slug"]] = dict(
                titre=f"Règle — {r['titre']}",
                url=f"{BASE}/wiki/spaces/{key}/pages/{pid}")

    sp = by_key[INDEX_SPACE]
    upsert(INDEX_TITLE, sp["homepageId"], index_body(roots),
           ["regle-dwh-index"], {"id": sp["id"], "key": INDEX_SPACE})

    sync_crosslinks(rule_pages, dry_run=os.getenv("CROSSLINK_DRY_RUN") == "1")

    gap = coverage_gap(routing)
    if gap:
        logger.warning(f"{len(gap)} trigger(s) actif(s) sans page Confluence : {', '.join(gap)}")
    logger.info("Done.")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logger.exception("💥 confluence-rules-sync CRASH")
        from src.core.mailer import build_email
        _send_alert(
            "🔴 confluence-rules-sync — CRASH",
            build_email(
                "confluence-rules-sync — CRASH", severity="critical",
                subtitle=datetime.now(ZoneInfo("Europe/Paris")).strftime("%d/%m/%Y %H:%M"),
                intro="Le sync Confluence des règles a planté — pages vivantes non "
                      "régénérées (prochain essai au daily 7h40).<br><br>"
                      f'<pre style="background:#f8fafc;border:1px solid #e2e8f0;'
                      f'border-radius:6px;padding:12px;font-size:12px;color:#dc2626;'
                      f'white-space:pre-wrap">{esc(str(e))}</pre>',
            ),
            html_body=True)
        sys.exit(1)
