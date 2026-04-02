"""
Action Engine Runner
=====================
Deux phases par exécution :

  Phase 1 — Breezeway (pending_actions)
    Lit les règles opérationnelles (tâches Breezeway) et les exécute.

  Phase 2 — Digest (rule_{FREQ})
    Lit rule_4h / rule_daily / rule_weekly selon FREQ.
    Filtre les nouvelles lignes (non déjà dans action_triggers).
    Envoie 1 seul digest email avec toutes les nouvelles alertes.
    Le heartbeat garantit l'envoi même si aucune alerte n'existe.
"""

import json
import logging
import os

import yaml
from google.cloud import bigquery

from src.core.action_logger import ActionLogger
from src.handlers import SkipAction
from src.handlers.breezeway_tasks import BreezewayTasksHandler
from src.handlers.email_digest import EmailDigestHandler

logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "merveil-data-warehouse")
FREQ       = os.getenv("FREQ")  # 4h | daily | weekly | monthly | None

HANDLER_REGISTRY = {
    "breezeway_task": BreezewayTasksHandler,
    "email_digest":   EmailDigestHandler,
}


class ActionRunner:
    def __init__(self, config_path: str = "config/rules.yaml"):
        self.bq = bigquery.Client(project=PROJECT_ID)
        self.logger = ActionLogger()

        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self._handlers: dict = {}

    def _get_handler(self, handler_type: str):
        if handler_type not in self._handlers:
            cls = HANDLER_REGISTRY.get(handler_type)
            if not cls:
                raise ValueError(f"Handler inconnu : {handler_type}")
            self._handlers[handler_type] = cls()
        return self._handlers[handler_type]

    # ── Phase 1 : Breezeway ──────────────────────────────────────────────────

    def _load_pending_actions(self) -> list[dict]:
        query = f"""
            SELECT rule_name, property_id, context, detected_at
            FROM `{PROJECT_ID}.action_engine.pending_actions`
            WHERE detected_at IS NOT NULL
        """
        rows = list(self.bq.query(query).result())
        logger.info(f"{len(rows)} action(s) dans pending_actions")
        return [dict(r) for r in rows]

    def _run_breezeway(self):
        try:
            self.logger.resolve_completed_triggers()
        except Exception as e:
            logger.warning(f"resolve_completed_triggers a échoué : {e}")

        actions = self._load_pending_actions()
        rules_config = self.config.get("rules", {})

        for action in actions:
            rule_name   = action["rule_name"]
            property_id = action.get("property_id")
            rule_conf   = rules_config.get(rule_name, {})

            if not rule_conf.get("enabled", False):
                continue

            if self.logger.get_open_trigger(rule_name, property_id):
                logger.info(f"Skip {rule_name}/{property_id} : trigger open existant")
                continue

            for dest_conf in rule_conf.get("destinations", []):
                dest_type = dest_conf["type"]
                params    = dest_conf.get("params", {})
                try:
                    handler   = self._get_handler(dest_type)
                    result_id = handler.execute(action, params)
                    context_data = json.loads(action.get("context") or "{}")
                    self.logger.open_trigger(
                        rule_name=rule_name,
                        destination=dest_type,
                        property_id=property_id,
                        home_id=context_data.get("home_id"),
                        context=context_data,
                        breezeway_task_id=result_id if dest_type == "breezeway_task" else None,
                    )
                    logger.info(f"[OK] {rule_name} → {dest_type} | {result_id}")
                except SkipAction as e:
                    logger.info(f"[SKIP] {rule_name} → {dest_type} : {e}")
                except Exception as e:
                    logger.error(f"[ERROR] {rule_name} → {dest_type} : {e}")

    # ── Phase 2 : Digest ─────────────────────────────────────────────────────

    def _load_digest_actions(self, freq: str) -> list[dict]:
        query = f"""
            SELECT
                alert_type,
                property_id,
                severity,
                entity_name,
                alert_message,
                action_recommended,
                alert_category,
                DATE(detected_at) AS alert_date,
                detected_at
            FROM `{PROJECT_ID}.action_engine.rule_{freq}`
        """
        rows = list(self.bq.query(query).result())
        logger.info(f"{len(rows)} ligne(s) dans rule_{freq}")
        return [dict(r) for r in rows]

    def _run_digest(self):
        try:
            self.logger.resolve_digest_triggers(FREQ)
        except Exception as e:
            logger.warning(f"resolve_digest_triggers a échoué : {e}")

        actions = self._load_digest_actions(FREQ)
        if not actions:
            logger.info(f"rule_{FREQ} vide — rien à traiter.")
            return

        heartbeats = [a for a in actions if a["alert_type"] == "_heartbeat"]
        all_alerts = [a for a in actions if a["alert_type"] != "_heartbeat"]

        # Bucket déjà traité ?
        if heartbeats:
            hb = heartbeats[0]
            if self.logger.get_open_trigger("_heartbeat", hb["property_id"]):
                logger.info(f"Bucket {hb['property_id']} déjà envoyé — skip total.")
                return

        # Nouvelles alertes uniquement
        new_alerts = [
            a for a in all_alerts
            if not self.logger.get_open_trigger(a["alert_type"], a["property_id"])
        ]

        # Envoyer le digest
        digest_conf = self.config.get("digest", {}).get(FREQ, {})
        handler = self._get_handler("email_digest")

        try:
            handler.execute_batch(new_alerts, digest_conf)
        except SkipAction as e:
            logger.info(f"[SKIP] digest_{FREQ} : {e}")
            return
        except Exception as e:
            logger.error(f"[ERROR] digest_{FREQ} : {e}")
            return

        # Logger heartbeat
        if heartbeats:
            hb = heartbeats[0]
            self.logger.open_trigger(
                rule_name="_heartbeat",
                destination="email_digest",
                property_id=hb["property_id"],
                home_id=None,
                context=None,
            )

        # Logger chaque alerte
        for alert in new_alerts:
            self.logger.open_trigger(
                rule_name=alert["alert_type"],
                destination="email_digest",
                property_id=alert["property_id"],
                home_id=None,
                context=None,
            )

        logger.info(f"=== Digest {FREQ} : {len(new_alerts)} alerte(s) envoyée(s) ===")

    # ── Point d'entrée ───────────────────────────────────────────────────────

    def run(self):
        logger.info("=== Action Engine démarré ===")
        self._run_breezeway()
        if FREQ:
            self._run_digest()
        else:
            logger.info("FREQ non défini — digest ignoré (Breezeway uniquement)")
        logger.info("=== Terminé ===")
