"""
Action Engine Runner
=====================
Lit sync_logs.pending_actions, déduplique via action_triggers,
route vers le bon handler, et logue le résultat.

Flux par exécution :
  1. Résoudre les triggers complétés (task-completed dans Breezeway)
  2. Lire pending_actions (produit par dbt)
  3. Pour chaque action :
       a. Vérifier qu'aucun trigger 'open' n'existe déjà
       b. Router vers le handler selon la destination
       c. Enregistrer le trigger en 'open' ou 'error'
"""

import json
import logging
import os
from typing import Optional

import yaml
from google.cloud import bigquery

from src.core.action_logger import ActionLogger
from src.handlers.breezeway_tasks import BreezewayTasksHandler

logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "merveil-data-warehouse")


# ── Registre des handlers disponibles ────────────────────────────────────────

HANDLER_REGISTRY = {
    "breezeway_task": BreezewayTasksHandler,
    # "customerio": CustomerIOHandler,   ← ajouter ici
}


class ActionRunner:
    def __init__(self, config_path: str = "config/rules.yaml"):
        self.bq = bigquery.Client(project=PROJECT_ID)
        self.logger = ActionLogger()

        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        # Instancier les handlers une seule fois
        self._handlers: dict = {}

    def _get_handler(self, handler_type: str):
        if handler_type not in self._handlers:
            cls = HANDLER_REGISTRY.get(handler_type)
            if not cls:
                raise ValueError(f"Handler inconnu : {handler_type}")
            self._handlers[handler_type] = cls()
        return self._handlers[handler_type]

    def _load_pending_actions(self) -> list[dict]:
        query = f"""
            SELECT
                rule_name,
                property_id,
                context,
                detected_at
            FROM `{PROJECT_ID}.sync_logs.pending_actions`
            WHERE detected_at IS NOT NULL
        """
        rows = list(self.bq.query(query).result())
        logger.info(f"{len(rows)} action(s) en attente dans pending_actions")
        return [dict(r) for r in rows]

    def run(self):
        """Point d'entrée principal."""
        logger.info("=== Action Engine démarré ===")

        # 1. Résoudre les triggers complétés
        try:
            self.logger.resolve_completed_triggers()
        except Exception as e:
            logger.warning(f"resolve_completed_triggers a échoué : {e}")

        # 2. Charger les actions en attente
        actions = self._load_pending_actions()
        if not actions:
            logger.info("Aucune action à traiter.")
            return

        rules_config = self.config.get("rules", {})
        triggered = 0
        skipped = 0
        errors = 0

        for action in actions:
            rule_name = action["rule_name"]
            property_id = action.get("property_id")
            rule_conf = rules_config.get(rule_name, {})

            if not rule_conf.get("enabled", False):
                logger.debug(f"Règle désactivée : {rule_name}")
                skipped += 1
                continue

            # 3. Déduplication : skip si trigger open déjà existant
            existing = self.logger.get_open_trigger(rule_name, property_id)
            if existing:
                logger.info(
                    f"Skip {rule_name} / {property_id} : trigger open depuis {existing['triggered_at']}"
                )
                skipped += 1
                continue

            # 4. Déclencher chaque destination configurée
            for dest_conf in rule_conf.get("destinations", []):
                dest_type = dest_conf["type"]
                params = dest_conf.get("params", {})
                trigger_id = None

                try:
                    handler = self._get_handler(dest_type)
                    result_id = handler.execute(action, params)

                    context_data = json.loads(action.get("context") or "{}")
                    trigger_id = self.logger.open_trigger(
                        rule_name=rule_name,
                        destination=dest_type,
                        property_id=property_id,
                        home_id=context_data.get("home_id"),
                        context=context_data,
                        breezeway_task_id=result_id if dest_type == "breezeway_task" else None,
                    )
                    logger.info(
                        f"[OK] {rule_name} → {dest_type} | task={result_id} | trigger={trigger_id}"
                    )
                    triggered += 1

                except Exception as e:
                    logger.error(f"[ERROR] {rule_name} → {dest_type} : {e}")
                    if trigger_id:
                        self.logger.mark_error(trigger_id, str(e))
                    errors += 1

        logger.info(
            f"=== Terminé : {triggered} déclenchés · {skipped} skippés · {errors} erreurs ==="
        )
