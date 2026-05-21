"""
Crée la table action_engine.dispatched_actions (idempotent).

Schema : log append-only des actions dispatchées par le TriggerDispatcher.
Remplace action_triggers (legacy), cohabite avec elle pendant la migration.

Usage : python -m utils.create_dispatched_actions_table

Décision : cf. decisions.md 2026-05-21 — Refacto action-engine trigger/action/routing
"""

import logging
import os

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "merveil-data-warehouse")
TABLE_FQN = f"{PROJECT_ID}.action_engine.dispatched_actions"

SCHEMA = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED", description="UUID identifiant unique de la ligne"),
    bigquery.SchemaField("trigger_name", "STRING", mode="REQUIRED", description="Identifiant de la détection (dénormalisé)"),
    bigquery.SchemaField("property_id", "STRING", mode="REQUIRED", description="Clé de dedup côté trigger (dénormalisé)"),
    bigquery.SchemaField("action_type", "STRING", mode="REQUIRED", description="asana_task | breezeway_task | email_digest"),
    bigquery.SchemaField("bucket", "STRING", mode="NULLABLE", description="Pour email_digest : 4h | daily | weekly. NULL sinon"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED", description="open | resolved | error"),
    bigquery.SchemaField("external_id", "STRING", mode="NULLABLE", description="Asana gid / Breezeway task id / Gmail message-id"),
    bigquery.SchemaField("dispatched_at", "TIMESTAMP", mode="REQUIRED", description="Horodatage du dispatch"),
    bigquery.SchemaField("resolved_at", "TIMESTAMP", mode="NULLABLE", description="Horodatage de résolution (cooldown OK)"),
    bigquery.SchemaField("context", "STRING", mode="NULLABLE", description="JSON snapshot du trigger au moment du dispatch"),
    bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("retry_count", "INTEGER", mode="NULLABLE", default_value_expression="0"),
]


def main():
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = bigquery.Table(TABLE_FQN, schema=SCHEMA)

    # Partitioning par dispatched_at (1 partition/jour) — gère bien le volume long-terme
    table_ref.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="dispatched_at",
    )
    # Clustering pour les dedup queries fréquentes (trigger_name, property_id, status)
    table_ref.clustering_fields = ["trigger_name", "property_id", "status"]

    try:
        existing = client.get_table(TABLE_FQN)
        logger.info(f"Table {TABLE_FQN} existe déjà ({existing.num_rows} lignes). Rien à faire.")
        return
    except Exception:
        pass

    table = client.create_table(table_ref)
    logger.info(f"Table créée : {TABLE_FQN}")
    logger.info(f"  Partitioning : DAY sur dispatched_at")
    logger.info(f"  Clustering   : (trigger_name, property_id, status)")
    logger.info(f"  Schema       : {len(SCHEMA)} colonnes")


if __name__ == "__main__":
    main()
