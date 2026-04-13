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
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
from googleapiclient.discovery import build

from src.handlers import SkipAction

logger = logging.getLogger(__name__)

PROJECT_ID   = os.getenv("GCP_PROJECT_ID", "merveil-data-warehouse")
GMAIL_SENDER = os.getenv("GMAIL_SENDER", "noreply@merveil.fr")
GMAIL_TO     = os.getenv("GMAIL_TO",     "alertes@archides.fr")

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

    def _fetch_alerts(self, rule_name: str, alert_types: list[str] | None = None) -> list[dict]:
        if alert_types:
            types_str   = ", ".join(f"'{t}'" for t in alert_types)
            type_clause = f"AND alert_type IN ({types_str})"
        else:
            type_clause = "AND NOT (alert_type = 'operational' AND entity_name = 'Annulations')"
        query = f"""
            SELECT
                alert_category, severity, entity_name,
                alert_message, action_recommended, alert_date
            FROM `{PROJECT_ID}.dashboard_alerts.dash_alerts`
            WHERE 1=1 {type_clause}
            AND (alert_type, entity_name) NOT IN (
                SELECT alert_type, entity_name
                FROM `{PROJECT_ID}.action_engine.digest_log`
                WHERE rule_name = '{rule_name}'
                AND sent_at >= (
                    SELECT MAX(sent_at)
                    FROM `{PROJECT_ID}.action_engine.digest_log`
                    WHERE rule_name = '{rule_name}'
                )
            )
            ORDER BY severity_order ASC, alert_date DESC
        """
        return [dict(r) for r in self.bq.query(query).result()]

    def _build_html(self, alerts: list[dict], today: str) -> str:
        by_category: dict[str, list] = {}
        for a in alerts:
            by_category.setdefault(a.get("alert_category", "Autre"), []).append(a)

        critical_count = sum(1 for a in alerts if a["severity"] == "CRITICAL")
        warning_count  = sum(1 for a in alerts if a["severity"] == "WARNING")

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
            for a in items:
                emoji = SEVERITY_EMOJI.get(a["severity"], "⚪")
                rows_html += (
                    f'<tr>'
                    f'<td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px">'
                    f'{emoji} {a["alert_message"]}</td>'
                    f'<td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:12px;'
                    f'color:#64748b;white-space:nowrap">{a["alert_date"]}</td>'
                    f'<td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:12px;'
                    f'color:#64748b">{a.get("action_recommended", "")}</td>'
                    f'</tr>'
                )

        freq = os.getenv("FREQ", "")
        freq_label = {"4h": "Toutes les 4h", "daily": "Quotidien", "weekly": "Hebdomadaire"}.get(freq, freq)

        return f"""<!DOCTYPE html>
<html><body style="font-family:system-ui,sans-serif;background:#f8fafc;margin:0;padding:0">
<div style="max-width:700px;margin:24px auto;background:white;border-radius:12px;
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
        <th style="padding:8px 12px;text-align:left;font-size:12px;color:#94a3b8;font-weight:500">Action recommandée</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>
  <div style="padding:16px 24px;background:#f8fafc;border-top:1px solid #e2e8f0">
    <p style="margin:0;font-size:12px;color:#94a3b8">Merveil Data Warehouse</p>
  </div>
</div>
</body></html>"""

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
    ✅ Aucune alerte active — tout est nominal.
  </div>
</div>
</body></html>"""

    def _log_digest(self, action: dict, alerts: list[dict]) -> None:
        """Insère une ligne par alerte dans digest_log après chaque envoi."""
        now = datetime.now(timezone.utc)
        rule_name   = action.get("rule_name", "")
        property_id = action.get("property_id", "")

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
            for a in items:
                emoji = SEVERITY_EMOJI.get(a.get("severity", ""), "⚪")
                date_display = str(a.get("alert_date", ""))
                rows_html += (
                    f'<tr>'
                    f'<td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px">'
                    f'{emoji} {a.get("alert_message", "")}</td>'
                    f'<td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:12px;'
                    f'color:#64748b;white-space:nowrap">{date_display}</td>'
                    f'<td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:12px;'
                    f'color:#64748b">{a.get("action_recommended", "") or ""}</td>'
                    f'</tr>'
                )

        freq = os.getenv("FREQ", "")
        freq_label = {"4h": "Toutes les 4h", "daily": "Quotidien", "weekly": "Hebdomadaire"}.get(freq, freq)

        return f"""<!DOCTYPE html>
<html><body style="font-family:system-ui,sans-serif;background:#f8fafc;margin:0;padding:0">
<div style="max-width:700px;margin:24px auto;background:white;border-radius:12px;
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
        <th style="padding:8px 12px;text-align:left;font-size:12px;color:#94a3b8;font-weight:500">Action recommandée</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>
  <div style="padding:16px 24px;background:#f8fafc;border-top:1px solid #e2e8f0">
    <p style="margin:0;font-size:12px;color:#94a3b8">Merveil Data Warehouse</p>
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
        """Nouveau flux : reçoit les lignes de rule_{freq} directement, envoie 1 digest."""
        freq          = os.getenv("FREQ", "unknown")
        send_if_empty = params.get("send_if_empty", False)
        today         = datetime.now(timezone.utc).strftime("%d/%m/%Y")
        to_addr       = params.get("to", GMAIL_TO)

        if not alerts:
            if not send_if_empty:
                raise SkipAction("no_alerts")
            html    = self._build_empty_html(today)
            subject = f"[Merveil] ✅ Rien à signaler · {today}"
        else:
            html           = self._build_html_from_rows(alerts, today)
            critical_count = sum(1 for a in alerts if a.get("severity") == "CRITICAL")
            subject        = f"[Merveil] {len(alerts)} alertes · {critical_count} critique(s) · {today}"

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

    def execute(self, action: dict, params: dict) -> str:
        alerts         = self._fetch_alerts(action.get("rule_name", ""), params.get("alert_types"))
        send_if_empty  = params.get("send_if_empty", False)
        today          = datetime.now(timezone.utc).strftime("%d/%m/%Y")

        if not alerts:
            if not send_if_empty:
                logger.info("Aucune alerte active — digest non envoyé")
                raise SkipAction("no_alerts")
            html    = self._build_empty_html(today)
            subject = f"[Merveil] ✅ Rien à signaler · {today}"
        else:
            html           = self._build_html(alerts, today)
            critical_count = sum(1 for a in alerts if a["severity"] == "CRITICAL")
            subject        = f"[Merveil] {len(alerts)} alertes · {critical_count} critique(s) · {today}"

        msg = MIMEMultipart("alternative")
        to_addr = params.get("to", GMAIL_TO)

        msg["Subject"] = subject
        msg["From"]    = GMAIL_SENDER
        msg["To"]      = to_addr
        msg.attach(MIMEText(html, "html", "utf-8"))

        service = _load_gmail_service()
        service.users().messages().send(
            userId="me",
            body=_encode_message(msg),
        ).execute()

        self._log_digest(action, alerts)

        logger.info(f"Digest envoyé → {to_addr} ({len(alerts)} alertes, {sum(1 for a in alerts if a.get('severity') == 'CRITICAL')} critiques)")
        return f"digest_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
