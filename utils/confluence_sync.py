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
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("confluence_sync")

PROJECT   = os.environ.get("GCP_PROJECT_ID", "merveil-data-warehouse")
SITE      = "merveil"
EMAIL     = "hatim@archides.fr"
SECRET    = "confluence-api-token"
BASE      = f"https://{SITE}.atlassian.net"
SPACE_KEY = "VD"   # 4. Ventes - Distribution - Marketing
PARIS     = ZoneInfo("Europe/Paris")

GMAIL_SENDER = os.getenv("GMAIL_SENDER", "noreply@archides.fr")
ALERT_TO     = os.getenv("CONFLUENCE_ALERT_TO", "hatim@archides.fr")

SPACE_ID = None  # résolu au run


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


def _send_alert(subject: str, body: str) -> None:
    """Mail d'alerte best-effort (même infra que iseo_orchestrator)."""
    try:
        from google.cloud import secretmanager
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        name = f"projects/{PROJECT}/secrets/alerts-gmail-sa-key/versions/latest"
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

def find_page(title):
    # Lookup direct par titre (API v2) — la recherche CQL est indexée avec du
    # retard et rate les pages tout juste créées (doublon au re-run).
    q = urllib.request.quote(title)
    res = req("GET", f"/wiki/api/v2/pages?space-id={SPACE_ID}&title={q}&limit=1")
    return res["results"][0]["id"] if res["results"] else None


def upsert(title, parent_id, body, labels):
    pid = find_page(title)
    if pid:
        cur = req("GET", f"/wiki/api/v2/pages/{pid}?body-format=storage")
        req("PUT", f"/wiki/api/v2/pages/{pid}", {
            "id": str(pid), "status": "current", "title": title,
            "body": {"representation": "storage", "value": body},
            "version": {"number": cur["version"]["number"] + 1, "message": "confluence-rules-sync"}})
        action = "maj"
    else:
        res = req("POST", "/wiki/api/v2/pages", {
            "spaceId": SPACE_ID, "status": "current", "title": title,
            "parentId": str(parent_id),
            "body": {"representation": "storage", "value": body}})
        pid = res["id"]
        action = "créé"
    if labels:
        req("POST", f"/wiki/rest/api/content/{pid}/label",
            [{"prefix": "global", "name": l} for l in labels])
    logger.info(f"{action} : {title}  ({BASE}/wiki/spaces/{SPACE_KEY}/pages/{pid})")
    return pid


# ── blocs vivants (BigQuery) ──────────────────────────────────────────────────

def _collect_beyond_push(bq) -> dict:
    """État réel du push Beyond : whitelist, fenêtres actives, audit, activité."""
    live = {"generated_at": datetime.now(PARIS).strftime("%d/%m/%Y à %Hh%M")}

    live["whitelist"] = [r.apartment_code for r in bq.query(f"""
        SELECT apartment_code FROM `{PROJECT}.dwh_inputs.beyond_push_whitelist`
        ORDER BY apartment_code""").result()]

    live["windows"] = [dict(apartment_code=r.apartment_code, start=r.start_date,
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
    live["last_edits"] = edits

    live["last_run"] = fmt_ts(next(iter(bq.query(f"""
        SELECT MAX(pushed_at) AS ts
        FROM `{PROJECT}.beyond_raw.price_pushes_log`""").result())).ts)

    live["last_gap_filled"] = fmt_ts(next(iter(bq.query(f"""
        SELECT MAX(dispatched_at) AS ts
        FROM `{PROJECT}.action_engine.dispatched_actions`
        WHERE trigger_name = 'beyond_gap_filled'""").result())).ts)

    return live


LIVE_COLLECTORS = {"beyond_push": _collect_beyond_push}


def collect_live(rule: dict, bq) -> dict | None:
    collector = LIVE_COLLECTORS.get(rule.get("live") or "")
    return collector(bq) if collector else None


def live_section(live: dict) -> str:
    items = []
    wl = live.get("whitelist") or []
    items.append(f"<strong>Appartements du pilote ({len(wl)})</strong> : "
                 + ", ".join(f"<code>{esc(c)}</code>" for c in wl))
    wins = live.get("windows") or []
    if wins:
        nights = sum((w["end"] - w["start"]).days + 1 for w in wins)
        items.append(f"<strong>Fenêtres de prix actives dans Beyond</strong> : {len(wins)} "
                     f"({nights} nuit(s), de {fmt_d(min(w['start'] for w in wins))} "
                     f"à {fmt_d(max(w['end'] for w in wins))})")
    else:
        items.append("<strong>Fenêtres de prix actives dans Beyond</strong> : aucune actuellement")
    if live.get("last_edits"):
        subs = "".join(f"<li>{esc(e)}</li>" for e in live["last_edits"])
        items.append(f"<strong>Dernières modifications de la liste</strong> :<ul>{subs}</ul>")
    if live.get("last_run"):
        items.append(f"<strong>Dernier run du job vérifié</strong> : {esc(live['last_run'])} ✓")
    items.append("<strong>Dernière nuit seule vendue via push</strong> : "
                 + (esc(live["last_gap_filled"]) if live.get("last_gap_filled")
                    else "aucune pour l'instant"))
    body = "<ul>" + "".join(f"<li>{i}</li>" for i in items) + "</ul>"
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
        titre="Push automatique des prix sur les nuits seules (gaps 1N)",
        slug="beyond-push-gaps-1n",
        live="beyond_push",
        domaine="Ventes — Pricing Beyond",
        niveau="N4", niveau_desc="la machine agit seule, l'humain audite a posteriori",
        frequence="Quotidien 10h45",
        canal="Mail [Merveil Beyond] à chaque nuit vendue → Hatim, Raphael, Mickael",
        owner="Raphael / Mickael",
        depuis="17 juillet 2026 (pilote — bilan meeting Beyond du 04/08)",
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
]

ROOT_TITLE = "🤖 Automatisations (DWH) — Ventes"


def root_body():
    b = panel("info",
              p("Cette rubrique documente <strong>ce que la machine (DWH) fait automatiquement</strong> pour le "
                "domaine Ventes : surveillances, alertes, et bientôt propositions d'action. Une page par règle, "
                "toujours à jour avec ce qui tourne réellement en production — on n'y documente <strong>que "
                "l'actif</strong> (le backlog vit dans la roadmap)."))
    b += p("Grille de lecture des niveaux : <strong>N2</strong> = la machine surveille et alerte · "
           "<strong>N3</strong> = la machine <em>propose</em> une action, l'humain valide · "
           "<strong>N4</strong> = la machine agit, l'humain audite · <strong>N5</strong> = autonome.")
    b += h2("Les règles actives")
    b += report(f'space = "{SPACE_KEY}" and label = "regle-dwh"', HEADINGS, "Règle")
    b += p(f'<em>Contact : Hatim (hatim@archides.fr) — pages générées automatiquement chaque matin, '
           f'commentaires bienvenus, édition manuelle déconseillée.</em>')
    return b


def run() -> None:
    global TOKEN, AUTH, SPACE_ID
    TOKEN = _secret(SECRET)
    AUTH = base64.b64encode(f"{EMAIL}:{TOKEN}".encode()).decode()

    bq = _bq()
    space = req("GET", f"/wiki/api/v2/spaces?keys={SPACE_KEY}")["results"][0]
    SPACE_ID = space["id"]
    logger.info(f"Space {SPACE_KEY} id={SPACE_ID} homepage={space['homepageId']}")

    root_id = upsert(ROOT_TITLE, space["homepageId"], root_body(),
                     ["regle-dwh-index", "domaine-ventes"])
    for r in RULES:
        live = collect_live(r, bq)
        upsert(f"Règle — {r['titre']}", root_id, rule_body(r, live),
               ["regle-dwh", "domaine-ventes", f"niveau-{r['niveau'].lower()}"])
    logger.info("Done.")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logger.exception("💥 confluence-rules-sync CRASH")
        _send_alert("🔴 confluence-rules-sync — CRASH",
                    f"Le sync Confluence des règles a planté.\n\nException : {e}")
        sys.exit(1)
