"""
Action Logger
=============
Écrit et met à jour les entrées dans sync_logs.action_triggers (BigQuery).
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "merveil-data-warehouse")
TABLE_ID = f"{PROJECT_ID}.action_engine.action_triggers"


class ActionLogger:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_ID)

    def open_trigger(
        self,
        rule_name: str,
        destination: str,
        property_id: Optional[str],
        home_id: Optional[int],
        context: Optional[dict],
        breezeway_task_id: Optional[str] = None,
    ) -> str:
        """Enregistre un nouveau trigger avec status 'open'. Retourne l'id."""
        trigger_id = str(uuid.uuid4())
        row = {
            "id": trigger_id,
            "rule_name": rule_name,
            "property_id": property_id,
            "home_id": home_id,
            "destination": destination,
            "status": "open",
            "triggered_at": datetime.now(timezone.utc).isoformat(),
            "resolved_at": None,
            "breezeway_task_id": breezeway_task_id,
            "error_message": None,
            "context": json.dumps(context) if context else None,
            "retry_count": 0,
        }
        errors = self.client.insert_rows_json(TABLE_ID, [row])
        if errors:
            logger.error(f"Erreur insert action_triggers: {errors}")
        return trigger_id

    def mark_error(self, trigger_id: str, error_message: str):
        """Met à jour un trigger en status 'error'."""
        query = f"""
            UPDATE `{TABLE_ID}`
            SET status = 'error',
                error_message = @error_message,
                retry_count = retry_count + 1
            WHERE id = @trigger_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("trigger_id", "STRING", trigger_id),
                bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
            ]
        )
        self.client.query(query, job_config=job_config).result()

    def get_open_trigger(self, rule_name: str, property_id: str) -> Optional[dict]:
        """Retourne le trigger 'open' existant pour (rule_name, property_id), ou None."""
        query = f"""
            SELECT id, breezeway_task_id, triggered_at
            FROM `{TABLE_ID}`
            WHERE rule_name = @rule_name
              AND property_id = @property_id
              AND status = 'open'
            ORDER BY triggered_at DESC
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("rule_name", "STRING", rule_name),
                bigquery.ScalarQueryParameter("property_id", "STRING", property_id or ""),
            ]
        )
        rows = list(self.client.query(query, job_config=job_config).result())
        return dict(rows[0]) if rows else None

    def resolve_digest_triggers(self, freq: str):
        """
        Résout les triggers email_digest expirés selon le TTL de la fréquence.
        Évite que action_triggers accumule des lignes open bloquant les ré-alertes.
        TTL : 4h → 4h, daily → 24h, weekly → 168h, monthly → 720h.
        """
        ttl = {"4h": 4, "daily": 24, "weekly": 168, "monthly": 720}.get(freq, 24)
        query = f"""
            UPDATE `{TABLE_ID}`
            SET status = 'resolved',
                resolved_at = CURRENT_TIMESTAMP()
            WHERE destination = 'email_digest'
              AND status = 'open'
              AND triggered_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {ttl} HOUR)
        """
        self.client.query(query).result()
        logger.info(f"Triggers digest résolus (TTL={ttl}h, freq={freq})")

    def resolve_completed_triggers(self):
        """
        Ferme les triggers 'open' dont la task Breezeway est complétée.
        Lit raw_breezeway.webhook_tasks pour détecter les task-completed.
        """
        query = f"""
            UPDATE `{TABLE_ID}` t
            SET
                t.status = 'resolved',
                t.resolved_at = CAST(wt.finished_at AS TIMESTAMP)
            FROM (
                SELECT task_id, finished_at
                FROM `{PROJECT_ID}.raw_breezeway.webhook_tasks`
                WHERE event_type = 'task-completed'
                  AND finished_at IS NOT NULL
            ) wt
            WHERE t.breezeway_task_id = wt.task_id
              AND t.status = 'open'
        """
        self.client.query(query).result()
        logger.info("Triggers résolus depuis webhook task-completed")
