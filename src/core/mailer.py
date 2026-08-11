"""
Mailer commun — envoi Gmail (DWD) + shell HTML des mails d'alerte.

Déduplique l'infra d'envoi qui vivait en 3 copies identiques (beyond_push,
iseo_orchestrator, confluence_sync) et donne aux alertes techniques le même
habillage que le digest daily (header coloré, cartes KPI, table, bouton
dashboard) au lieu du texte brut.

- send_mail()   : envoi best-effort par défaut (une alerte qui échoue ne doit
                  jamais faire planter le job qui la signale). Auth = SA
                  `alerts-gmail-sender` via Secret Manager `alerts-gmail-sa-key`
                  + Domain-Wide Delegation sur GMAIL_SENDER.
- build_email() : shell HTML aligné sur email_digest (mêmes couleurs slate/
                  indigo, même carte 760px). Contenu = cartes KPI + intro +
                  table + sections libres, tout optionnel.
"""

import base64
import html as _html
import json
import logging
import os
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "merveil-data-warehouse")
GMAIL_SENDER = os.getenv("GMAIL_SENDER", "noreply@archides.fr")

# Couleur du header + du bandeau résumé par sévérité (palette du digest daily)
SEVERITY_STYLE = {
    "info":     {"header": "#6366f1", "sub": "#e0e7ff"},
    "warning":  {"header": "#d97706", "sub": "#fef3c7"},
    "critical": {"header": "#dc2626", "sub": "#fee2e2"},
}


def esc(s) -> str:
    """Échappe une valeur pour insertion dans le HTML (None → '')."""
    return _html.escape(str(s), quote=False) if s is not None else ""


def send_mail(subject: str, body: str, to: str, *, html: bool = False,
              sender: str = None, raise_on_error: bool = False) -> bool:
    """Envoie un mail via Gmail API (DWD). Best-effort par défaut : logge et
    retourne False en cas d'échec, sans propager (raise_on_error=True pour
    propager — à réserver aux cas où l'appelant gère)."""
    sender = sender or GMAIL_SENDER
    try:
        # Imports paresseux : seuls les runs qui alertent en ont besoin
        from google.cloud import secretmanager
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        name = f"projects/{PROJECT_ID}/secrets/alerts-gmail-sa-key/versions/latest"
        sa_info = secretmanager.SecretManagerServiceClient().access_secret_version(
            name=name).payload.data.decode()
        creds = service_account.Credentials.from_service_account_info(
            json.loads(sa_info), scopes=["https://www.googleapis.com/auth/gmail.send"]
        ).with_subject(sender)
        svc = build("gmail", "v1", credentials=creds, cache_discovery=False)
        msg = MIMEText(body, "html" if html else "plain", "utf-8")
        msg["From"], msg["To"], msg["Subject"] = sender, to, subject
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        svc.users().messages().send(userId=sender, body={"raw": raw}).execute()
        logger.info(f"📧 mail envoyé à {to}: {subject}")
        return True
    except Exception as e:
        logger.error(f"⚠️ envoi mail échoué ({subject}): {e}")
        if raise_on_error:
            raise
        return False


def _kpi_cards(kpis: list[dict]) -> str:
    cards = "".join(
        f'<div style="display:inline-block;background:#fff;border:1px solid #e2e8f0;'
        f'border-radius:6px;padding:12px 20px;margin:0 8px 8px 0">'
        f'<div style="font-size:11px;color:#64748b;text-transform:uppercase">{esc(k["label"])}</div>'
        f'<div style="font-size:24px;font-weight:700;color:{k.get("color", "#0f172a")}">{esc(k["value"])}</div>'
        f'</div>'
        for k in kpis
    )
    return f'<div style="padding:16px 24px 8px">{cards}</div>'


def _table(table: dict) -> str:
    heads = "".join(
        f'<th style="padding:8px 12px;text-align:left;font-size:11px;color:#64748b;'
        f'text-transform:uppercase;font-weight:600">{esc(h)}</th>'
        for h in table["headers"]
    )
    body = ""
    for row in table["rows"]:
        cells = "".join(
            f'<td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;'
            f'font-size:13px;color:#334155;vertical-align:top">{c}</td>'
            for c in row
        )
        body += f"<tr>{cells}</tr>"
    return (
        f'<table style="width:100%;border-collapse:collapse">'
        f'<thead><tr style="background:#f8fafc">{heads}</tr></thead>'
        f'<tbody>{body}</tbody></table>'
    )


def mono(s) -> str:
    """Cellule en fonte mono (codes appart, plages de dates, messages API)."""
    return f'<span style="font-family:monospace;font-size:12px;color:#64748b">{esc(s)}</span>'


def build_email(title: str, *, subtitle: str = None, severity: str = "info",
                kpis: list[dict] = None, intro: str = None, table: dict = None,
                sections_html: str = None, button: tuple = None) -> str:
    """Shell HTML commun des mails d'alerte (style du digest daily).

    - kpis          : [{label, value, color?}] → cartes en tête
    - intro         : paragraphe libre (HTML autorisé)
    - table         : {"headers": [...], "rows": [[cellules HTML]]}
    - sections_html : bloc libre inséré après la table (déjà stylé)
    - button        : (label, url) → bouton indigo en pied de contenu
    """
    style = SEVERITY_STYLE.get(severity, SEVERITY_STYLE["info"])
    parts = []
    if kpis:
        parts.append(_kpi_cards(kpis))
    if intro:
        parts.append(f'<div style="padding:8px 24px;font-size:14px;color:#475569">{intro}</div>')
    if table:
        parts.append(f'<div style="padding:8px 24px 16px">{_table(table)}</div>')
    if sections_html:
        parts.append(sections_html)
    if button:
        label, url = button
        parts.append(
            f'<div style="padding:8px 24px 20px">'
            f'<a href="{url}" style="display:inline-block;background:#6366f1;color:white;'
            f'padding:8px 16px;border-radius:6px;text-decoration:none;font-size:13px;'
            f'font-weight:500">{esc(label)} →</a></div>'
        )

    return f"""<!DOCTYPE html>
<html><body style="font-family:system-ui,sans-serif;background:#f8fafc;margin:0;padding:0">
<div style="max-width:760px;margin:24px auto;background:white;border-radius:12px;
            overflow:hidden;border:1px solid #e2e8f0">
  <div style="background:{style['header']};padding:20px 24px">
    <h1 style="color:white;margin:0;font-size:18px">{esc(title)}</h1>
    {f'<p style="color:{style["sub"]};margin:4px 0 0;font-size:13px">{esc(subtitle)}</p>' if subtitle else ''}
  </div>
  {''.join(parts)}
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
