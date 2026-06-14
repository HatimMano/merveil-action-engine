"""
Handler : Brief Annulations 11h
================================
Mail quotidien envoyé à 11h Paris à l'équipe RC.

Récap minimaliste des annulations des 24 dernières heures.
Le détail (contact, historique guest, contexte) vit dans le dashboard
Ops Front > 6.5 Annulations. Le mail = trigger pour ouvrir l'app.

Source : dashboard_ops.dash_ops_cancellations_recent
Filtre : cancelled_at >= NOW() - 24h
Destinataires : alerte_ventes@archides.fr, emilia@archides.fr (override via env CANCELLATIONS_TO)
Lien dashboard : https://direction.archides.fr/ops-front?tab=cancellations&preset=24h
"""

import base64
import json
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

PROJECT_ID      = os.getenv("GCP_PROJECT_ID", "merveil-data-warehouse")
GMAIL_SENDER    = os.getenv("GMAIL_SENDER", "noreply@merveil.fr")
GMAIL_TO        = os.getenv("CANCELLATIONS_TO", "alerte_ventes@archides.fr, emilia@archides.fr")
TEST_RECIPIENT  = os.getenv("TEST_RECIPIENT")  # override pour tests
DASHBOARD_URL   = "https://direction.archides.fr/ops-front?tab=cancellations&preset=24h"

TIER_STYLE = {
    "GOLD":   ("#fef3c7", "#92400e"),
    "SILVER": ("#e2e8f0", "#475569"),
    "BRONZE": ("#fed7aa", "#9a3412"),
    "BASIC":  ("#f1f5f9", "#64748b"),
}


def _load_gmail_service():
    sm = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/alerts-gmail-sa-key/versions/latest"
    payload = sm.access_secret_version(request={"name": name}).payload.data.decode("utf-8")
    sa_info = json.loads(payload)
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/gmail.send"],
    ).with_subject(GMAIL_SENDER)
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _fetch_cancellations() -> list[dict]:
    bq = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            cancelled_at,
            customer_name,
            customer_email,
            customer_phone,
            tier_segment,
            is_proxy_booking,
            booker_name,
            booker_email,
            apartment_code,
            checkin_date,
            checkout_date,
            nights,
            ota_source,
            accommodation_revenue_gross,
            customer_total_trips,
            customer_lifetime_value,
            customer_is_repeat,
            customer_is_vip
        FROM `{PROJECT_ID}.dashboard_ops.dash_ops_cancellations_recent`
        WHERE cancelled_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        ORDER BY
            CASE tier_segment WHEN 'GOLD' THEN 0 WHEN 'SILVER' THEN 1
                              WHEN 'BRONZE' THEN 2 ELSE 3 END,
            accommodation_revenue_gross DESC
    """
    return [dict(r) for r in bq.query(query).result()]


def _build_html(rows: list[dict], paris_today: str) -> str:
    n_total  = len(rows)
    n_gold   = sum(1 for r in rows if r["tier_segment"] == "GOLD")
    n_silver = sum(1 for r in rows if r["tier_segment"] == "SILVER")
    n_repeat = sum(1 for r in rows if r.get("customer_is_repeat"))
    total_ht = sum(float(r["accommodation_revenue_gross"] or 0) for r in rows)

    def euro(v): return f"{v:,.0f} €".replace(",", " ")

    if n_total == 0:
        body = (
            '<p style="font-size:15px;color:#475569;margin:24px 0;">'
            'Aucune annulation dans les 24 dernières heures. ✅'
            '</p>'
        )
    else:
        rows_html = ""
        for r in rows:
            bg, fg = TIER_STYLE.get(r.get("tier_segment") or "BASIC", TIER_STYLE["BASIC"])
            badges = []
            if r.get("customer_is_vip"):
                badges.append('<span style="background:#f3e8ff;color:#7e22ce;padding:1px 5px;border-radius:3px;font-size:10px;font-weight:600">VIP</span>')
            elif r.get("customer_is_repeat"):
                badges.append('<span style="background:#e0e7ff;color:#4338ca;padding:1px 5px;border-radius:3px;font-size:10px;font-weight:600">Récurrent</span>')
            if r.get("is_proxy_booking"):
                badges.append('<span style="background:#f3e8ff;color:#7e22ce;padding:1px 5px;border-radius:3px;font-size:10px;font-weight:600">PROXY</span>')
            badge_html = " ".join(badges)

            contact_email = (r.get("customer_email") or r.get("booker_email") or "—")
            contact_phone = (r.get("customer_phone") or "—")

            rows_html += (
                f'<tr style="border-bottom:1px solid #f1f5f9">'
                f'<td style="padding:8px 12px;font-size:13px;color:#334155;">'
                f'<strong>{r.get("customer_name") or "—"}</strong> {badge_html}<br>'
                f'<span style="font-size:11px;color:#94a3b8">{contact_phone} · {contact_email}</span>'
                f'</td>'
                f'<td style="padding:8px 12px;font-size:12px;text-align:center">'
                f'<span style="background:{bg};color:{fg};padding:2px 6px;border-radius:3px;font-size:10px;font-weight:600">'
                f'{r.get("tier_segment") or "—"}</span></td>'
                f'<td style="padding:8px 12px;font-size:12px;color:#64748b;font-family:monospace">{r.get("apartment_code") or "—"}</td>'
                f'<td style="padding:8px 12px;font-size:12px;color:#64748b;white-space:nowrap">{r.get("checkin_date")} → {r.get("checkout_date")}</td>'
                f'<td style="padding:8px 12px;font-size:12px;color:#64748b;text-align:center">{r.get("nights") or 0}n</td>'
                f'<td style="padding:8px 12px;font-size:12px;color:#64748b">{r.get("ota_source") or "—"}</td>'
                f'<td style="padding:8px 12px;font-size:13px;color:#0f172a;font-weight:600;text-align:right">'
                f'{euro(float(r.get("accommodation_revenue_gross") or 0))}</td>'
                f'</tr>'
            )

        body = (
            f'<table style="width:100%;border-collapse:collapse;background:white;'
            f'border:1px solid #e2e8f0;border-radius:6px;overflow:hidden;margin-top:16px">'
            f'<thead><tr style="background:#f8fafc;text-align:left">'
            f'<th style="padding:8px 12px;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600">Client</th>'
            f'<th style="padding:8px 12px;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600;text-align:center">Tier</th>'
            f'<th style="padding:8px 12px;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600">Appart</th>'
            f'<th style="padding:8px 12px;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600">Séjour</th>'
            f'<th style="padding:8px 12px;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600;text-align:center">Nuits</th>'
            f'<th style="padding:8px 12px;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600">Canal</th>'
            f'<th style="padding:8px 12px;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600;text-align:right">HT</th>'
            f'</tr></thead><tbody>{rows_html}</tbody></table>'
        )

    kpis = (
        f'<div style="display:inline-block;background:#fff;border:1px solid #e2e8f0;border-radius:6px;padding:12px 20px;margin-right:8px">'
        f'<div style="font-size:11px;color:#64748b;text-transform:uppercase">Total 24h</div>'
        f'<div style="font-size:24px;font-weight:700;color:#dc2626">{n_total}</div>'
        f'</div>'
        f'<div style="display:inline-block;background:#fff;border:1px solid #e2e8f0;border-radius:6px;padding:12px 20px;margin-right:8px">'
        f'<div style="font-size:11px;color:#64748b;text-transform:uppercase">Gold / Silver</div>'
        f'<div style="font-size:24px;font-weight:700;color:#ca8a04">{n_gold} · {n_silver}</div>'
        f'</div>'
        f'<div style="display:inline-block;background:#fff;border:1px solid #e2e8f0;border-radius:6px;padding:12px 20px;margin-right:8px">'
        f'<div style="font-size:11px;color:#64748b;text-transform:uppercase">Récurrents</div>'
        f'<div style="font-size:24px;font-weight:700;color:#4338ca">{n_repeat}</div>'
        f'</div>'
        f'<div style="display:inline-block;background:#fff;border:1px solid #e2e8f0;border-radius:6px;padding:12px 20px">'
        f'<div style="font-size:11px;color:#64748b;text-transform:uppercase">CA HT perdu</div>'
        f'<div style="font-size:24px;font-weight:700;color:#0f172a">{euro(total_ht)}</div>'
        f'</div>'
    )

    return f"""
<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="background:#f8fafc;padding:24px;font-family:-apple-system,Segoe UI,sans-serif;color:#0f172a">
  <div style="max-width:920px;margin:0 auto">
    <h1 style="font-size:22px;margin:0 0 4px">Annulations — {paris_today}</h1>
    <p style="color:#64748b;font-size:14px;margin:0 0 20px">
      Workflow RC : contact des guests annulés ces 24 dernières heures.
    </p>
    <div style="margin-bottom:20px">{kpis}</div>
    {body}
    <div style="margin-top:24px">
      <a href="{DASHBOARD_URL}"
         style="background:#4f46e5;color:white;padding:10px 20px;border-radius:6px;
                text-decoration:none;font-size:14px;font-weight:600;display:inline-block">
        Voir le détail dans le dashboard →
      </a>
    </div>
    <p style="font-size:11px;color:#94a3b8;margin-top:32px">
      Mail généré automatiquement à 11h Paris · merveil-action-engine ·
      Source : dashboard_ops.dash_ops_cancellations_recent
    </p>
  </div>
</body></html>
"""


def run() -> None:
    paris_now = datetime.now(ZoneInfo("Europe/Paris"))
    paris_today = paris_now.strftime("%A %d %B %Y").lower()

    logger.info("Brief annulations 11h : fetching…")
    rows = _fetch_cancellations()
    logger.info(f"  → {len(rows)} annulations sur les 24 dernières heures")

    html = _build_html(rows, paris_today)
    subject = (
        f"[Merveil] {len(rows)} annulation{'s' if len(rows) > 1 else ''} "
        f"dans les 24h — {paris_now.strftime('%d/%m')}"
    )

    msg = MIMEMultipart("alternative")
    msg["From"] = GMAIL_SENDER
    msg["To"]   = TEST_RECIPIENT or GMAIL_TO
    msg["Subject"] = subject
    msg.attach(MIMEText(html, "html", "utf-8"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    service = _load_gmail_service()
    sent = service.users().messages().send(userId=GMAIL_SENDER, body={"raw": raw}).execute()
    logger.info(f"  → mail envoyé id={sent.get('id')} to={msg['To']}")
