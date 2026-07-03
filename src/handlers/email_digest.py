"""
Handler : Email Digest quotidien
=================================
Envoie un digest des alertes actives (dash_alerts) par email via Gmail API.
Expéditeur : noreply@merveil.fr (impersonné via Domain-Wide Delegation)
Destinataire : alertes@archides.fr (Google Group)

Déduplication assurée en amont par le runner via action_triggers
(property_id = CURRENT_DATE → 1 envoi max par jour).

Secret Manager requis :
    alerts-gmail-sa-key : clé JSON du SA alerts-gmail-sender
                          (Domain-Wide Delegation activée sur gmail.send)

Env vars optionnels :
    GMAIL_SENDER    : expéditeur impersonné (défaut: noreply@merveil.fr)
    GMAIL_TO        : destinataire (défaut: alertes@archides.fr)
"""

import base64
import json
import logging
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
from googleapiclient.discovery import build

from src.handlers import SkipAction

logger = logging.getLogger(__name__)

PROJECT_ID      = os.getenv("GCP_PROJECT_ID", "merveil-data-warehouse")
GMAIL_SENDER    = os.getenv("GMAIL_SENDER", "noreply@merveil.fr")
GMAIL_TO        = os.getenv("GMAIL_TO",     "alertes@archides.fr")
TEST_RECIPIENT  = os.getenv("TEST_RECIPIENT")   # si défini, override tous les destinataires

SEVERITY_EMOJI = {"CRITICAL": "🔴", "WARNING": "🟡", "INFO": "🔵"}

CATEGORY_LABELS = {
    "Clients":        "Clients à risque",
    "Paniers":        "Paniers abandonnés",
    "Satisfaction":   "Satisfaction",
    "Disponibilites": "Disponibilités",
    "Revenue":        "Revenus",
    "Operationnel":   "Opérationnel",
    "Ventes":         "Ventes",
    "Qualite":        "Qualité",
}

# Types d'alertes qui touchent plusieurs apparts/résas et qu'on regroupe en 1
# case agrégée dans le mail (au lieu de N cases individuelles).
# Min 3 occurrences pour déclencher le regroupement, sinon affichage standard.
GROUPABLE = {
    "superhost_risk": {
        "title": "Apparts en risque qualité",
        # tri : nb_reviews desc (signal solide) puis note asc (pires d'abord)
        "sort": lambda a: (-float(a.get("secondary_value") or 0), float(a.get("metric_value") or 5.0)),
    },
    "revenue_anomaly": {
        "title": "Apparts à 0 € de CA ce mois",
        # tri : CA P-1 desc (plus grosse chute en premier)
        "sort": lambda a: -float(a.get("secondary_value") or 0),
    },
    "last_minute": {
        "title": "Résas last-minute",
        "sort": lambda a: str(a.get("alert_date") or ""),
    },
    "cancellation_vip": {
        "title": "Annulations VIP (Gold / Silver)",
        "sort": lambda a: str(a.get("alert_date") or ""),
    },
    "cancellation_large_apt": {
        "title": "Annulations grands appartements",
        "sort": lambda a: str(a.get("alert_date") or ""),
    },
    "abandoned_cart": {
        "title": "Paniers abandonnés ≥ 1 500 €",
        "sort": lambda a: -float(a.get("metric_value") or 0),
    },
}
GROUP_MIN_COUNT = 3  # à partir de 3 alertes du même type, on regroupe


def _render_grouped_block(alerts: list[dict], spec: dict) -> str:
    """Rend une case agrégée pour un type d'alerte multi-apparts.

    Affiche : titre + count + sous-blocs Critical/Warning (apparts triés) + bouton lien.
    """
    sorted_alerts = sorted(alerts, key=spec["sort"])
    crit = [a for a in sorted_alerts if a.get("severity") == "CRITICAL"]
    warn = [a for a in sorted_alerts if a.get("severity") == "WARNING"]
    info = [a for a in sorted_alerts if a.get("severity") not in ("CRITICAL", "WARNING")]
    url = (alerts[0].get("action_recommended") or "").strip()

    def items_html(group: list[dict]) -> str:
        # Chaque appart : message court de dash_alerts (déjà compact)
        chips = "".join(
            f'<span style="display:inline-block;background:#f8fafc;border:1px solid #e2e8f0;'
            f'border-radius:4px;padding:2px 8px;margin:2px 4px 2px 0;font-size:11px;'
            f'color:#475569;font-family:ui-monospace,monospace">'
            f'{a.get("alert_message", "")}</span>'
            for a in group
        )
        return f'<div style="margin:4px 0 8px">{chips}</div>'

    sev_label = []
    if crit:
        sev_label.append(f'<strong style="color:#dc2626">🔴 {len(crit)} critical</strong>')
    if warn:
        sev_label.append(f'<strong style="color:#d97706">🟡 {len(warn)} warning</strong>')
    if info:
        sev_label.append(f'<strong style="color:#475569">🔵 {len(info)}</strong>')

    header = (
        f'<div style="font-size:13px;font-weight:600;color:#1e293b;margin-bottom:6px">'
        f'{spec["title"]} — {len(alerts)} concernés '
        f'<span style="font-weight:normal;color:#64748b;font-size:11px">'
        f'({" · ".join(sev_label)})</span></div>'
    )

    body = ""
    if crit:
        body += '<div style="font-size:11px;color:#dc2626;font-weight:600;margin-top:6px">🔴 Critical</div>'
        body += items_html(crit)
    if warn:
        body += '<div style="font-size:11px;color:#d97706;font-weight:600;margin-top:6px">🟡 Warning</div>'
        body += items_html(warn)
    if info:
        body += items_html(info)

    button = ""
    if url.startswith("http"):
        button = (
            f'<div style="text-align:right;margin-top:8px">'
            f'<a href="{url}" style="display:inline-block;background:#6366f1;color:white;'
            f'padding:4px 10px;border-radius:6px;text-decoration:none;font-size:11px;'
            f'font-weight:500">Voir →</a></div>'
        )

    return (
        f'<tr><td colspan="3" style="padding:12px 16px;border-bottom:1px solid #f1f5f9;'
        f'background:#fefce8/30">'
        f'{header}{body}{button}'
        f'</td></tr>'
    )


def _load_gmail_service():
    """Charge la clé SA depuis Secret Manager et construit le service Gmail."""
    sm = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/alerts-gmail-sa-key/versions/latest"
    payload = sm.access_secret_version(request={"name": name}).payload.data.decode("utf-8")
    sa_info = json.loads(payload)

    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/gmail.send"],
    ).with_subject(GMAIL_SENDER)

    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _encode_message(msg: MIMEMultipart) -> dict:
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    return {"raw": raw}


class EmailDigestHandler:

    def __init__(self):
        self.bq = bigquery.Client(project=PROJECT_ID)



    def _build_empty_html(self, label: str) -> str:
        freq = os.getenv("FREQ", "")
        freq_label = {"4h": "Toutes les 4h", "daily": "Quotidien", "weekly": "Hebdomadaire"}.get(freq, freq)

        return f"""<!DOCTYPE html>
<html><body style="font-family:system-ui,sans-serif;background:#f8fafc;margin:0;padding:0">
<div style="max-width:700px;margin:24px auto;background:white;border-radius:12px;
            overflow:hidden;border:1px solid #e2e8f0">
  <div style="background:#6366f1;padding:20px 24px">
    <h1 style="color:white;margin:0;font-size:18px">Merveil — Rapport d'alertes</h1>
    <p style="color:#e0e7ff;margin:4px 0 0;font-size:13px">{label} · {freq_label}</p>
  </div>
  <div style="padding:24px;text-align:center;color:#64748b;font-size:14px">
    ✅ Aucune alerte active.
  </div>
</div>
</body></html>"""


    def _build_html_from_rows(self, alerts: list[dict], today: str) -> str:
        """Construit le HTML depuis des lignes rule_4h / rule_daily (nouveau flux)."""
        by_category: dict[str, list] = {}
        for a in alerts:
            by_category.setdefault(a.get("alert_category", "Autre"), []).append(a)

        critical_count = sum(1 for a in alerts if a.get("severity") == "CRITICAL")
        warning_count  = sum(1 for a in alerts if a.get("severity") == "WARNING")

        summary_color = "#dc2626" if critical_count > 0 else "#d97706"
        summary_text  = f"{critical_count} critique(s)" + (
            f" · {warning_count} warning(s)" if warning_count else ""
        )

        rows_html = ""
        for cat, items in by_category.items():
            label = CATEGORY_LABELS.get(cat, cat)
            rows_html += (
                f'<tr><td colspan="3" style="background:#f1f5f9;padding:8px 12px;'
                f'font-weight:600;font-size:12px;color:#475569;text-transform:uppercase;'
                f'letter-spacing:.05em">{label}</td></tr>'
            )

            # Regroupement par alert_type pour identifier les groupes multi-apparts
            by_type: dict[str, list] = {}
            for a in items:
                by_type.setdefault(a.get("alert_type", ""), []).append(a)

            for atype, group in by_type.items():
                spec = GROUPABLE.get(atype)
                if spec and len(group) >= GROUP_MIN_COUNT:
                    # Case agrégée : 1 ligne avec liste compacte
                    rows_html += _render_grouped_block(group, spec)
                    continue

                # Sinon : N lignes individuelles (comportement standard)
                for a in group:
                    emoji = SEVERITY_EMOJI.get(a.get("severity", ""), "⚪")
                    date_display = str(a.get("alert_date", ""))
                    action      = a.get("action_recommended", "") or ""
                    if action.startswith("http"):
                        action_cell = (
                            f'<a href="{action}" style="display:inline-block;'
                            f'background:#6366f1;color:white;padding:4px 10px;'
                            f'border-radius:6px;text-decoration:none;font-size:11px;font-weight:500">'
                            f'Voir →</a>'
                        )
                    else:
                        action_cell = f'<span style="color:#64748b;font-size:12px">{action}</span>'
                    rows_html += (
                        f'<tr>'
                        f'<td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px">'
                        f'{emoji} {a.get("alert_message", "")}</td>'
                        f'<td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:12px;'
                        f'color:#64748b;white-space:nowrap">{date_display}</td>'
                        f'<td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;text-align:right">{action_cell}</td>'
                        f'</tr>'
                    )

        freq = os.getenv("FREQ", "")
        freq_label = {"4h": "Toutes les 4h", "daily": "Quotidien", "weekly": "Hebdomadaire"}.get(freq, freq)

        return f"""<!DOCTYPE html>
<html><body style="font-family:system-ui,sans-serif;background:#f8fafc;margin:0;padding:0">
<div style="max-width:760px;margin:24px auto;background:white;border-radius:12px;
            overflow:hidden;border:1px solid #e2e8f0">
  <div style="background:#6366f1;padding:20px 24px">
    <h1 style="color:white;margin:0;font-size:18px">Merveil — Rapport d'alertes</h1>
    <p style="color:#e0e7ff;margin:4px 0 0;font-size:13px">{today} · {freq_label}</p>
  </div>
  <div style="padding:14px 24px;background:#fef9c3;border-bottom:1px solid #fde68a">
    <span style="font-weight:600;color:{summary_color};font-size:14px">
      {len(alerts)} alertes actives — {summary_text}
    </span>
  </div>
  <table style="width:100%;border-collapse:collapse">
    <thead>
      <tr style="background:#f8fafc">
        <th style="padding:8px 12px;text-align:left;font-size:12px;color:#94a3b8;font-weight:500">Alerte</th>
        <th style="padding:8px 12px;text-align:left;font-size:12px;color:#94a3b8;font-weight:500">Date</th>
        <th style="padding:8px 12px;text-align:right;font-size:12px;color:#94a3b8;font-weight:500">Action</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>
  <div style="padding:16px 24px;background:#f8fafc;border-top:1px solid #e2e8f0">
    <p style="margin:0;font-size:12px;color:#94a3b8">
      Merveil Data Warehouse ·
      <a href="https://direction.archides.fr" style="color:#6366f1;text-decoration:none">
        Dashboard complet
      </a>
    </p>
  </div>
</div>
</body></html>"""

    def _log_digest_batch(self, alerts: list[dict], freq: str) -> None:
        """Insère une ligne par alerte dans digest_log (nouveau flux execute_batch)."""
        now         = datetime.now(timezone.utc)
        rule_name   = f"rule_{freq}"
        property_id = now.strftime("%Y-%m-%d")

        if not alerts:
            rows = [{
                "sent_at":       now.isoformat(),
                "rule_name":     rule_name,
                "property_id":   property_id,
                "section":       "empty",
                "alert_type":    None,
                "severity":      None,
                "entity_name":   None,
                "alert_message": None,
            }]
        else:
            rows = [
                {
                    "sent_at":       now.isoformat(),
                    "rule_name":     rule_name,
                    "property_id":   property_id,
                    "section":       a.get("alert_category"),
                    "alert_type":    a.get("alert_type"),
                    "severity":      a.get("severity"),
                    "entity_name":   a.get("entity_name"),
                    "alert_message": a.get("alert_message"),
                }
                for a in alerts
            ]

        table_ref = self.bq.dataset("action_engine").table("digest_log")
        errors = self.bq.insert_rows_json(table_ref, rows)
        if errors:
            logger.warning(f"digest_log insert errors : {errors}")

    def execute_batch(self, alerts: list[dict], params: dict) -> str:
        """Nouveau flux : reçoit les lignes de rule_{freq} directement, envoie 1 digest.

        Params optionnels :
          - to (str)             : destinataires (override GMAIL_TO)
          - send_if_empty (bool) : si True, envoie "rien à signaler" quand vide
          - subject_prefix (str) : préfixe du sujet (défaut '[Merveil]', utilisé par TriggerDispatcher pour distinguer 4H/Daily)
        """
        freq          = os.getenv("FREQ", "unknown")
        send_if_empty = params.get("send_if_empty", False)
        subject_pref  = params.get("subject_prefix", "[Merveil]")
        now_paris     = datetime.now(ZoneInfo("Europe/Paris"))
        today         = now_paris.strftime("%d/%m/%Y %H:%M")
        to_addr       = TEST_RECIPIENT or params.get("to", GMAIL_TO)

        if not alerts:
            if not send_if_empty:
                raise SkipAction("no_alerts")
            html    = self._build_empty_html(today)
            subject = f"{subject_pref} ✅ Rien à signaler · {today}"
        else:
            html           = self._build_html_from_rows(alerts, today)
            critical_count = sum(1 for a in alerts if a.get("severity") == "CRITICAL")
            subject        = f"{subject_pref} {len(alerts)} alertes · {critical_count} critique(s) · {today}"

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = GMAIL_SENDER
        msg["To"]      = to_addr
        msg.attach(MIMEText(html, "html", "utf-8"))

        service = _load_gmail_service()
        service.users().messages().send(
            userId="me",
            body=_encode_message(msg),
        ).execute()

        self._log_digest_batch(alerts, freq)

        logger.info(f"Digest {freq} envoyé → {to_addr} ({len(alerts)} alertes)")
        return f"digest_{freq}_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"

