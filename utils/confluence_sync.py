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
RULES ici puis laisser le job tourner (ou l'exécuter manuellement).

V1 (2026-07-21) : blocs vivants scopés à la règle « Push automatique des prix »
(whitelist Beyond éditable depuis le dash — rules-edition 18/07). Brancher une
autre règle = lui donner un champ `live` + le gérer dans collect_live().

V2 (2026-07-22) : multi-spaces. Chaque règle porte un champ `space` (défaut VD) ;
1 page racine « 🤖 Automatisations (DWH) — <domaine> » par space (VD Ventes, GDA
Opérations, TRAN Serrures & accès) + index global cross-spaces dans EN
(00. Entreprise, report CQL label=regle-dwh). Ajouter un domaine = 1 entrée
SPACES + des règles avec ce `space`. Bloc vivant serrures : collector `iseo`.

Local : `python3 -m utils.confluence_sync` (token via gcloud CLI, BQ via ADC).
"""
import base64
import html
import json
import logging
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("confluence_sync")

PROJECT   = os.environ.get("GCP_PROJECT_ID", "merveil-data-warehouse")
SITE      = "merveil"
EMAIL     = "hatim@archides.fr"
SECRET    = "confluence-api-token"
BASE      = f"https://{SITE}.atlassian.net"
PARIS     = ZoneInfo("Europe/Paris")

# Spaces métier cibles — 1 page racine « 🤖 Automatisations (DWH) — <domaine> »
# par space, posée en sibling des sections existantes (on ne touche jamais au
# contenu préexistant). Chaque règle porte un champ `space` (défaut VD).
# NB : COR (0. Finance et Stratégie) est archivé → le futur lot finance ira
# dans CXSXJ (2. Comptabilité x Social x Juridique).
SPACES = {
    "VD":   dict(domaine="Ventes",
                 root="🤖 Automatisations (DWH) — Ventes",
                 label="domaine-ventes"),
    "GDA":  dict(domaine="Opérations",
                 root="🤖 Automatisations (DWH) — Opérations",
                 label="domaine-operations"),
    "TRAN": dict(domaine="Serrures & accès",
                 root="🤖 Automatisations (DWH) — Serrures & accès",
                 label="domaine-serrures"),
}
# Index global cross-spaces (vue CEO), dans 00. Entreprise.
INDEX_SPACE = "EN"
INDEX_TITLE = "🤖 Tout ce que fait la machine"

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


def rule_body(r, live=None):
    rows = [
        ("Règle",           f"<strong>{esc(r['titre'])}</strong>"),
        ("Domaine",         esc(r["domaine"])),
        ("Niveau",          status("Blue", r["niveau"]) + f" — {esc(r['niveau_desc'])}"),
        ("Statut",          status("Green", "Actif")),
        ("Fréquence",       esc(r["frequence"])),
        ("Canal",           esc(r["canal"])),
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


RULES = [
    dict(
        titre="Atterrissage budget sous cible (mensuel)",
        slug="budget-atterrissage",
        domaine="Ventes — Budget",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        frequence="Quotidien 7h (armé à partir du 10 du mois)",
        canal="Mail digest [Merveil Daily] → alerte_ventes@archides.fr",
        owner="Arnaud (à confirmer)",
        depuis="7 juillet 2026",
        source="trigger_budget_landing_gap → dash_ventes_budget (dernière édition du budget)",
        dashboard_url="https://direction.archides.fr/ventes?tab=budget&view=atterrissage",
        quoi=[
            "Chaque nuit, la machine compare l'<strong>atterrissage projeté</strong> du mois en cours "
            "(chiffre d'affaires réalisé + réservations déjà prises pour le reste du mois) au budget cible "
            "de la dernière édition.",
            "Si la projection passe <strong>sous 90 %</strong> du budget → alerte 🟡 dans le mail quotidien "
            "(sous 75 % → 🔴). L'alerte n'est armée qu'à partir du <strong>10 du mois</strong> (avant, les "
            "ventes restantes rendent le ratio peu significatif).",
            "À partir du <strong>15 du mois</strong>, elle surveille aussi le mois suivant : si les réservations "
            "déjà prises couvrent moins de <strong>50 %</strong> du budget M+1 → info dans le même mail.",
            "L'alerte donne le montant projeté, la cible, le % d'atteinte et le nombre d'appartements sous 80 %.",
        ],
        exemple=[
            "Mail du 10/07/2026 : « 🟡 Budget 2026-07 : atterrissage projeté 1 947 k€ / cible 2 222 k€ (88 %) "
            "— 34 apparts &lt; 80 % ».",
        ],
    ),
    dict(
        titre="Gaps de pricing — récap quotidien",
        slug="gaps-pricing",
        domaine="Ventes — Pricing",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        frequence="Quotidien 7h",
        canal="Mail digest [Merveil Daily] → alerte_ventes@archides.fr",
        owner="Raphael (à confirmer)",
        depuis="17 mai 2026",
        source="trigger_gap_pricing_summary → dash_ventes_gaps (flag_action)",
        dashboard_url="https://direction.archides.fr/ventes?tab=controle&view=gaps_actions",
        quoi=[
            "Chaque nuit, la machine repère les <strong>trous de calendrier</strong> (nuits isolées entre deux "
            "réservations) qui appellent une action de pricing, et les résume en 3 lignes dans le mail quotidien :",
            "• <strong>Gaps critiques</strong> : à combler sous 7 jours (dernière chance de vendre la nuit).<br/>"
            "• <strong>Gaps moyens</strong> : entre 8 et 14 jours.<br/>"
            "• <strong>Marge potentielle</strong> : ce que rapporteraient ces nuits si elles étaient vendues au "
            "prix des nuits voisines (déduction faite du ménage et du coussin de marge).",
            "Le bouton du mail ouvre la liste détaillée par appartement dans le dashboard.",
        ],
    ),
    dict(
        titre="Surcote 1 nuit inefficace sur gap à venir",
        slug="surcote-1n-inefficace",
        domaine="Ventes — Pricing Beyond",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        frequence="Quotidien 7h",
        canal="Mail digest [Merveil Daily] → alerte_ventes@archides.fr",
        owner="Raphael / Arnaud (à confirmer)",
        depuis="juin 2026",
        source="trigger_beyond_surcote_gap → dash_beyond_proposed_changes × dash_beyond_gap1n_surcote",
        dashboard_url="https://direction.archides.fr/ventes?tab=controle&view=surcote_1n&ineff=1",
        quoi=[
            "Beyond applique une <strong>surcote automatique sur les nuits seules</strong> (en moyenne +88 %, "
            "jusqu'à +160 % selon l'appartement). Sur certains appartements, cette surcote est "
            "<strong>empiriquement inefficace</strong> : historiquement, moins de 5 % de ces nuits surcotées "
            "≥ 100 % se vendent — la nuit reste vide.",
            "Chaque nuit, la machine repère les appartements qui ont <strong>une nuit seule à vendre dans les "
            "14 prochains jours</strong> ET une surcote flaggée inefficace → alerte dans le mail quotidien, "
            "<strong>avant</strong> que la nuit ne soit perdue.",
            "L'action suggérée : baisser la surcote / le prix de cette nuit. Depuis le 17/07, sur les "
            "appartements du pilote, la machine pose <strong>elle-même</strong> la fenêtre de prix — voir la "
            "règle « Push automatique des prix sur les nuits seules ». Sur ces appartements, la surcote 1 nuit "
            "Beyond a été <strong>supprimée le 21/07</strong> (elle s'appliquait après la fenêtre et la rendait "
            "inopérante).",
        ],
    ),
    dict(
        titre="Push automatique des prix sur les nuits seules (gaps 1N/2N)",
        slug="beyond-push-gaps-1n",
        live="beyond_push",
        domaine="Ventes — Pricing Beyond",
        niveau="N4", niveau_desc="la machine agit seule, l'humain audite a posteriori",
        frequence="Quotidien 6h45 + 10h45",
        canal="Mail [Merveil Beyond] à chaque nuit vendue → Hatim, Raphael, Mickael",
        owner="Raphael / Mickael",
        depuis="17 juillet 2026 (pilote 8 appartements — élargi à 31 appartements le 07/08, "
               "nuits orphelines sur tout le parc le 06/08, trous de 2 nuits activés le 10/08)",
        source="merveil-action-engine-beyond → dash_beyond_push_targets → API Beyond (seasonal-prices) → beyond_raw.price_pushes_log",
        dashboard_url="https://direction.archides.fr/ventes?tab=controle&view=push_auto",
        quoi=[
            "Sur 2026, ~1 380 « nuits seules » (une nuit vide coincée entre deux réservations) — seules 2,5 % "
            "se vendent, car la surcote 1 nuit de Beyond les affiche à ~2× le prix du marché.",
            "Chaque matin, la machine repère les nuits seules à venir (jusqu'à J+90) sur les appartements du "
            "pilote et pose <strong>directement dans Beyond</strong> une fourchette de prix : "
            "<strong>plancher</strong> = ménage + frais ops + coussin de marge (on ne brade jamais) · "
            "<strong>plafond</strong> = ce que les nuits voisines ont réellement vendu. Beyond continue son "
            "pricing normalement à l'intérieur de la fourchette.",
            "Depuis le 10/08 (décisions meeting Beyond 04/08), la même mécanique couvre les <strong>trous "
            "de 2 nuits</strong> sur les appartements marqués « 2N » (cf. bloc État actuel) : fourchette posée "
            "sur les 2 nuits, plancher "
            "par nuit divisé par 2 (les coûts fixes s'amortissent sur le séjour — Beyond impose un séjour "
            "minimum de 2 nuits sur ces trous, donc jamais de vente d'1 nuit au plancher réduit ; quand la "
            "première nuit est passée, la nuit restante repasse automatiquement au plancher plein le lendemain "
            "matin) ; et les "
            "<strong>nuits orphelines</strong> (fin de trou : la nuit de ce soir est libre, quelqu'un arrive "
            "demain, la veille est déjà passée) : plancher plein posé pour la journée dès le run de 6h45 — "
            "sur <strong>l'ensemble du parc</strong>, pas seulement la whitelist (pure protection plancher, "
            "aucune surcote à retirer : le clamp s'applique après les réglages Beyond).",
            "Si les nuits voisines vendent sous notre plancher (fréquent sur les petits appartements), la "
            "fourchette devient un <strong>prix fixe rentable</strong> — mieux que le prix surcoté, jamais à perte.",
            "Les <strong>règles saisonnières posées par l'équipe dans Beyond sont préservées</strong> (un plancher "
            "équipe plus haut gagne toujours). Nuit vendue ou gap disparu → la fenêtre est retirée le lendemain. "
            "Chaque modification est journalisée (audit complet, réversible en un clic).",
        ],
        exemple=[
            "17/07/2026 : 1er run — fenêtres posées sur 10 nuits seules des 5 premiers appartements "
            "(ex. P11-RIC75-0F, nuit du 10/09 : fourchette 394 → 419 €, ADR voisin 469 €). "
            "Quand une de ces nuits se vend : mail 🎉 automatique avec le prix vendu, le canal et la fenêtre.",
        ],
        modifier=[
            "La <strong>liste des appartements du pilote</strong> est modifiable en direct depuis le dashboard "
            "(Ventes → Contrôle → Push auto, éditeurs autorisés) : l'ajout/retrait est pris en compte au run "
            "suivant (10h45) et journalisé — le bloc « État actuel » ci-dessus reflète la liste en vigueur.",
            "⚠️ En ajoutant un appartement, demander à Beyond la <strong>suppression de sa surcote 1 nuit</strong> "
            "(elle s'applique après notre fourchette et la rendrait inopérante — réglé le 21/07 sur le pilote).",
            "Fourchettes, seuils et arrêt de la règle restent gérés dans le DWH — demande à Hatim.",
        ],
    ),

    dict(
        titre="Séquences relationnelles automatiques (customers.io)",
        slug="sequences-crm",
        domaine="Ventes — CRM",
        niveau="N4", niveau_desc="la machine agit seule, l'humain audite a posteriori",
        frequence="customers.io va chercher les données du DWH plusieurs fois par jour",
        canal="E-mails envoyés directement au client par customers.io",
        owner="à confirmer",
        depuis="2026",
        source="marts.cio_customers · marts.cio_events → intégration native customers.io (pull BigQuery)",
        dashboard_url="https://direction.archides.fr/clients",
        quoi=[
            "C'est la seule règle où la machine <strong>écrit au client</strong>, sans validation humaine "
            "préalable. Le DWH ne fait qu'exposer la donnée ; c'est customers.io qui envoie.",
            "Le DWH tient à jour une <strong>fiche client</strong> (nombre de séjours, chiffre d'affaires à "
            "vie, segment Gold/Silver/Bronze, canal de réservation, date du dernier avis satisfait, "
            "anniversaire) et publie <strong>5 événements</strong> : réservation confirmée · séjour terminé · "
            "réservation annulée · panier abandonné · avis reçu.",
            "customers.io vient lire ces deux tables plusieurs fois par jour et déclenche ses séquences. "
            "<strong>Actives aujourd'hui</strong> : séquence post-séjour (satisfaction puis relance pour une "
            "nouvelle réservation) et séquence post-annulation (récupération du client annulé).",
            "<strong>Garde-fous côté DWH</strong> : seuls les clients avec une adresse e-mail réelle sont "
            "exposés (les adresses relais des OTAs sont exclues) — un client qui n'a jamais rempli son "
            "formulaire Duve n'est donc jamais contacté. Chaque événement est envoyé <strong>une seule fois "
            "par réservation</strong>, à vie.",
            "⚠️ <strong>Point de vigilance connu</strong> : un changement d'appartement se traduit dans Mews "
            "par une annulation suivie d'une nouvelle réservation. Un événement « annulation » part donc pour "
            "un client qui vient quand même — à filtrer côté ciblage de la campagne.",
        ],
        modifier=[
            "Le <strong>contenu des e-mails et les conditions de déclenchement</strong> se règlent dans "
            "customers.io, pas dans le DWH.",
            "Ce que le DWH contrôle : les données envoyées (fiche client et événements) — demande à Hatim.",
            "⚠️ <strong>Ce qui n'est pas mesurable ici</strong> : envois, ouvertures, clics et désinscriptions "
            "restent dans customers.io. Les faire redescendre dans le DWH suppose un abonnement customers.io "
            "supérieur, que nous n'avons pas — aucun chiffre de performance CRM n'est donc disponible dans le "
            "dashboard aujourd'hui.",
        ],
    ),

    # ── GDA — 5. Opérations ──────────────────────────────────────────────────
    dict(
        space="GDA",
        titre="Digest arrivées & disponibilités du jour",
        slug="digest-dispo",
        domaine="Opérations — Front office",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        frequence="Quotidien 7h",
        canal="Mail digest [Merveil Daily] → alerte_ventes@archides.fr",
        owner="Mickael (à confirmer)",
        depuis="17 mai 2026",
        source="trigger_dispo_daily_summary → dash_ops_dispo_daily",
        dashboard_url="https://direction.archides.fr/ops-front?tab=dispo&view=matin",
        quoi=[
            "Chaque matin, la machine compte les appartements <strong>réellement disponibles</strong> et le "
            "résume en 3 lignes dans le mail quotidien :",
            "• <strong>Dès le matin</strong> : vides depuis au moins la veille (vendables immédiatement).<br/>"
            "• <strong>Cet après-midi</strong> : check-out aujourd'hui, sans late checkout.<br/>"
            "• <strong>Today + 2 jours minimum</strong> : libres sur une fenêtre d'au moins 3 nuits.",
            "Les appartements <strong>bloqués</strong> (travaux, usage interne) sont exclus automatiquement — "
            "le chiffre est directement exploitable pour pousser des ventes last-minute.",
        ],
    ),
    dict(
        space="GDA",
        titre="Annulations — brief quotidien 11h & alertes ciblées",
        slug="annulations",
        domaine="Opérations — Réservations",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        frequence="Brief à 11h · alertes ciblées dans le digest de 7h",
        canal="Mail [Merveil] → alerte_ventes@archides.fr + emilia@archides.fr",
        owner="Emilia",
        depuis="23 mai 2026",
        source="cancellations_brief → dash_ops_cancellations_recent · trigger_cancellation_vip / trigger_cancellation_large_apt / trigger_high_cancellations_daily",
        dashboard_url="https://direction.archides.fr/ops-front?tab=cancellations&preset=24h",
        quoi=[
            "<strong>Brief de 11h</strong> : toutes les annulations des dernières 24h en un mail (montant, "
            "canal, dates, client), avec le bouton vers le détail dashboard. Zéro annulation = pas de mail.",
            "<strong>Alertes ciblées</strong> (digest de 7h) : les annulations qui méritent une action "
            "immédiate — client <strong>Gold/Silver</strong> (fidèle ou gros panier) et <strong>grands "
            "appartements</strong> avec check-in proche (nuits chères difficiles à revendre à court terme).",
            "<strong>Pic d'annulations</strong> : dès qu'une journée dépasse <strong>30 annulations</strong>, "
            "une alerte le signale avec le montant total annulé. Un pic de cette ampleur est rarement un "
            "hasard (incident sur un canal, sur un appartement, ou erreur de manipulation).",
            "Filtre anti-bruit : si le client a une autre réservation active à ±7 jours (changement de dates "
            "ou d'appartement), ce n'est pas une vraie perte → pas d'alerte.",
        ],
    ),
    dict(
        space="GDA",
        titre="Alertes séjour (last-minute, double booking, sans appartement)",
        slug="alertes-sejour",
        domaine="Opérations — Front office",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        frequence="Quotidien 7h",
        canal="Mail digest [Merveil Daily] → alerte_ventes@archides.fr",
        owner="Mickael (à confirmer)",
        depuis="mai 2026",
        source="trigger_last_minute_checkin · trigger_double_booking · trigger_checkin_no_apartment",
        dashboard_url="https://direction.archides.fr/ops-front",
        quoi=[
            "Trois surveillances qui ne se manifestent <strong>que lorsqu'il y a un cas</strong> (la plupart "
            "des jours : rien) :",
            "• <strong>Check-in last-minute</strong> : réservation prise très peu de temps avant l'arrivée → "
            "vérifier que ménage, code d'accès et accueil suivent.<br/>"
            "• <strong>Double booking</strong> : deux réservations actives qui se chevauchent sur le même "
            "appartement → à résoudre avant l'arrivée.<br/>"
            "• <strong>Check-in sans appartement</strong> : arrivée imminente sans espace assigné dans Mews.",
        ],
    ),
    dict(
        space="GDA",
        titre="Suivi des avis — mauvais avis & risque Superhost",
        slug="suivi-avis",
        domaine="Opérations — Qualité",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        frequence="Quotidien 7h",
        canal="Mail digest [Merveil Daily] → alerte_ventes@archides.fr",
        owner="Emilia",
        depuis="mai 2026",
        source="trigger_satisfaction_low_review · trigger_superhost_risk → fct_reviews (Reva)",
        dashboard_url="https://direction.archides.fr/qualite?tab=appartements",
        quoi=[
            "<strong>Mauvais avis</strong> : chaque nouvel avis ≤ 3★ déclenche une alerte individuelle "
            "(appartement, note, canal) → traiter à chaud (réponse publique, geste commercial, tâche "
            "correctrice).",
            "<strong>Risque Superhost</strong> : appartements dont la note moyenne sur les <strong>3 derniers "
            "mois</strong> passe sous 4,5★ (avec au moins 3 avis) → 🟡 ; sous 4,0★ → 🔴. Regroupés en une "
            "case unique dans le mail, triés par volume d'avis puis pire note.",
            "C'est le radar avancé de la note publique : la moyenne 3 mois bouge des semaines avant la note "
            "affichée sur les OTAs.",
        ],
    ),

    # ── TRAN — 6. Opérations N2 (serrures) ───────────────────────────────────
    dict(
        space="TRAN",
        titre="Codes d'accès automatiques par séjour (serrures connectées)",
        slug="codes-acces-auto",
        live="iseo",
        domaine="Serrures & accès",
        niveau="N4", niveau_desc="la machine agit seule, l'humain audite a posteriori",
        frequence="Toutes les 2 heures (à :45)",
        canal="Silencieux quand tout va bien — mail d'erreur si un code n'a pas pu être posé",
        owner="Sylvain / Mickael (à confirmer)",
        depuis="juin 2026 (élargissement progressif du parc)",
        source="merveil-action-engine-iseo → API Sofia/ISEO + Duve → iseo_raw.merveil_pin_cache",
        dashboard_url="https://direction.archides.fr/ops-back?tab=serrures",
        quoi=[
            "Sur les appartements basculés (liste dans le bloc « État actuel »), la machine gère "
            "<strong>seule</strong> tout le cycle de vie du code de la porte — à la place du code fixe "
            "permanent partagé entre tous les clients.",
            "<strong>3 jours avant l'arrivée</strong> (réservation payée, pre-checkin Duve complété), elle "
            "génère un code 4 chiffres unique, le pose sur la serrure <strong>au nom du client</strong>, "
            "valable uniquement du check-in au check-out (aux horaires de la politique de l'appartement), "
            "crée un <strong>lien d'ouverture à distance</strong> de secours, et pousse le tout dans Duve — "
            "les messages automatiques Duve envoient donc le bon code sans aucune intervention.",
            "Séjour <strong>prolongé, raccourci ou décalé</strong> → le code est reposé sur les nouvelles "
            "dates (même code, le client ne voit rien). <strong>Départ ou annulation</strong> → le code est "
            "supprimé de la serrure.",
            "Le code fixe historique reste en place en parallèle pendant la phase pilote (décision du "
            "13/07) — filet de sécurité, à purger appartement par appartement plus tard.",
        ],
        exemple=[
            "Réservation arrivant vendredi sur un appartement basculé : mardi, la machine pose un code unique "
            "valable du vendredi 15h au lundi 11h. Le client le reçoit dans son message Duve habituel. "
            "S'il prolonge d'une nuit dans Mews, le code est étendu automatiquement au run suivant.",
        ],
        modifier=[
            "La liste des appartements basculés est un référentiel DWH : l'élargissement se fait par lots "
            "après période d'observation — demande à Hatim.",
            "⚠️ Basculer un appartement ne supprime pas son code fixe (conservés en doublon pour l'instant, "
            "décision du 13/07).",
        ],
    ),
    dict(
        space="TRAN",
        titre="Surveillance des serrures & des codes",
        slug="surveillance-serrures",
        domaine="Serrures & accès",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        frequence="Toutes les 2 heures + un récapitulatif quotidien (~8h45)",
        canal="Mail [Merveil Serrures] → alerte_ventes@archides.fr (2h) · mail quotidien « résas sans code » → hatim@archides.fr",
        owner="Sylvain / Mickael (à confirmer)",
        depuis="13 juin 2026 (récap quotidien par cause : 5 août 2026 · surveillance des HyperGates : 10 août 2026)",
        source="trigger_iseo_pin_missing · trigger_iseo_reconciliation · trigger_iseo_etl_stale · trigger_iseo_gateway_offline → dash_ops_pin_reconciliation · stg_iseo__gateways · gaps orchestrateur",
        dashboard_url="https://direction.archides.fr/ops-back?tab=serrures",
        quoi=[
            "Le filet de sécurité de la règle « Codes d'accès automatiques » — trois surveillances toutes "
            "les 2 heures :",
            "• <strong>Porte dormante</strong> : un client est censé séjourner sur un appartement basculé, "
            "aucun code n'est posé ET la porte n'a pas été ouverte depuis 48h → vrai risque d'accès.<br/>"
            "• <strong>Réconciliation</strong> : l'état interne est comparé à l'état réel des serrures Sofia. "
            "Code supprimé à la main dans l'interface, code orphelin, code encore actif après le départ → "
            "chaque écart est signalé (l'incident fondateur : un code effacé par erreur dans l'UI, client "
            "bloqué dehors).<br/>"
            "• <strong>Données en retard</strong> : si la collecte ISEO ne remonte plus, alerte — on ne "
            "surveille jamais à l'aveugle.",
            "Un même problème n'est signalé qu'une fois tant qu'il n'est pas résolu (déduplication 4h).",
            "S'y ajoute depuis le <strong>10 août 2026</strong> la surveillance des <strong>HyperGates</strong> "
            "(les boîtiers qui relient les serrures au réseau) : une passerelle qui n'a plus donné signe de vie "
            "depuis <strong>plus de 7 jours</strong> est signalée dans le mail quotidien, avec la liste des "
            "serrures qu'elle dessert. Sans passerelle, la serrure fonctionne toujours au clavier mais on ne "
            "peut plus ni poser un code à distance, ni ouvrir la porte à distance, ni voir les ouvertures — "
            "c'est une panne à traiter sur place. Cette surveillance rend <strong>redondant</strong> le rapport "
            "horaire envoyé par ISEO : la même information est désormais dans le DWH.",
            "En complément, un <strong>récapitulatif quotidien</strong> (~8h45) liste les arrivées ≤ J+3 "
            "toujours sans code, <strong>classées par cause</strong> : formulaire pre-checkin non rempli, "
            "paiement en échec (le code est volontairement retenu), serrure non résolue, ou anomalie à "
            "investiguer. Le même état est visible arrivée par arrivée sur la page Arrivées (6.1, "
            "« Code d'accès ISEO »).",
        ],
    ),
]


NIVEAUX = ("Grille de lecture des niveaux : <strong>N2</strong> = la machine surveille et alerte · "
           "<strong>N3</strong> = la machine <em>propose</em> une action, l'humain valide · "
           "<strong>N4</strong> = la machine agit, l'humain audite · <strong>N5</strong> = autonome.")

FOOTER = ('<em>Contact : Hatim (hatim@archides.fr) — pages générées automatiquement chaque matin, '
          'commentaires bienvenus, édition manuelle déconseillée.</em>')


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
    keys = ",".join(list(SPACES) + [INDEX_SPACE])
    by_key = {s["key"]: s for s in
              req("GET", f"/wiki/api/v2/spaces?keys={keys}&limit=25")["results"]}

    roots = {}
    for key, meta in SPACES.items():
        sp = by_key[key]
        space = {"id": sp["id"], "key": key}
        logger.info(f"Space {key} id={sp['id']} homepage={sp['homepageId']}")
        roots[key] = upsert(meta["root"], sp["homepageId"], root_body(key, meta),
                            ["regle-dwh-index", meta["label"]], space)
        for r in [r for r in RULES if r.get("space", "VD") == key]:
            live = collect_live(r, bq)
            upsert(f"Règle — {r['titre']}", roots[key], rule_body(r, live),
                   ["regle-dwh", meta["label"], f"niveau-{r['niveau'].lower()}"], space)

    sp = by_key[INDEX_SPACE]
    upsert(INDEX_TITLE, sp["homepageId"], index_body(roots),
           ["regle-dwh-index"], {"id": sp["id"], "key": INDEX_SPACE})
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
