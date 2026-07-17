"""
TriggerDispatcher — refacto trigger / action / routing (cf. decisions.md 2026-05-21).
==================================================================================

Lit `action_engine.triggers` (produit par dbt), confronte chaque trigger non
encore traité au routing.yaml, et déclenche les actions configurées.

Particularités :
  - Email digest = action "fan-in" : on accumule dans un buffer puis 1 mail par bucket.
  - Idempotence par (trigger_name, property_id, action_type) dans dispatched_actions.
  - Cooldown :
      • asana_task / breezeway_task → tant que la tâche externe n'est pas done
      • email_digest               → TTL par bucket (4h, daily=24h, weekly=168h)

Coexiste avec l'ancien ActionRunner via le feature flag USE_NEW_DISPATCHER.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import yaml
from google.cloud import bigquery

from src.handlers import SkipAction
from src.handlers.asana_task import AsanaTaskHandler
from src.handlers.breezeway_tasks import BreezewayTasksHandler
from src.handlers.email_digest import EmailDigestHandler

logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "merveil-data-warehouse")
TRIGGERS_TABLE = f"{PROJECT_ID}.action_engine.triggers"
DISPATCHED_TABLE = f"{PROJECT_ID}.action_engine.dispatched_actions"

# TTL par bucket pour les actions email_digest (en heures)
DIGEST_TTL_HOURS = {
    "2h": 4,   # serrures : run toutes les 2h, re-alerte max toutes les 4h (dédup)
    "4h": 4,
    "daily": 24,
    "weekly": 168,
    "monthly": 720,
}

# Registry des handlers
HANDLER_REGISTRY = {
    "asana_task": AsanaTaskHandler,
    "breezeway_task": BreezewayTasksHandler,
    # email_digest est géré séparément (fan-in via buffer)
}


class TriggerDispatcher:
    """Refonte action-engine : un trigger → N actions, routées via YAML."""

    def __init__(self, routing_path: str = "config/routing.yaml"):
        self.bq = bigquery.Client(project=PROJECT_ID)

        with open(routing_path) as f:
            self.routing = yaml.safe_load(f)

        self._handlers: dict[str, Any] = {}
        # Buffer email_digest : { bucket → [(trigger, action_params)] }
        self._digest_buffer: dict[str, list[tuple[dict, dict]]] = {}

    # ── Handler lazy-init ────────────────────────────────────────────────────

    def _get_handler(self, action_type: str):
        if action_type not in self._handlers:
            cls = HANDLER_REGISTRY.get(action_type)
            if not cls:
                raise ValueError(f"Handler inconnu : {action_type}")
            self._handlers[action_type] = cls()
        return self._handlers[action_type]

    # ── Étape 1 : Résolution des actions terminées côté externe ─────────────

    def _resolve_completed_breezeway(self):
        """Ferme les dispatched_actions Breezeway dont la task est completed."""
        query = f"""
            UPDATE `{DISPATCHED_TABLE}` t
            SET t.status = 'resolved',
                t.resolved_at = CAST(wt.finished_at AS TIMESTAMP)
            FROM (
                SELECT task_id, finished_at
                FROM `{PROJECT_ID}.raw_breezeway.webhook_tasks`
                WHERE event_type = 'task-completed'
                  AND finished_at IS NOT NULL
            ) wt
            WHERE t.external_id = wt.task_id
              AND t.action_type = 'breezeway_task'
              AND t.status = 'open'
        """
        try:
            self.bq.query(query).result()
            logger.info("Breezeway completed → dispatched_actions resolved")
        except Exception as e:
            logger.warning(f"resolve breezeway failed (non-fatal) : {e}")

    def _resolve_expired_digests(self, freq: str):
        """Ferme les dispatched_actions email_digest plus vieilles que TTL du bucket."""
        ttl = DIGEST_TTL_HOURS.get(freq, 24)
        query = f"""
            UPDATE `{DISPATCHED_TABLE}`
            SET status = 'resolved',
                resolved_at = CURRENT_TIMESTAMP()
            WHERE action_type = 'email_digest'
              AND bucket = @bucket
              AND status = 'open'
              AND dispatched_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {ttl} HOUR)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("bucket", "STRING", freq)]
        )
        try:
            self.bq.query(query, job_config=job_config).result()
            logger.info(f"Digest TTL ({ttl}h, bucket={freq}) → résolution OK")
        except Exception as e:
            logger.warning(f"resolve digests failed (non-fatal) : {e}")

    # ── Étape 2 : Charger triggers + dispatched_actions ouvertes ────────────

    def _load_triggers(self) -> list[dict]:
        """Charge les triggers détectés dans les dernières 24h.

        ⚠ `action_engine.triggers` est incrémental (append-only, dedup par
        trigger_id = HASH(name+property+DATE(detected_at))) → la table accumule
        TOUT l'historique. Sans borne temporelle, chaque run rechargeait des
        milliers de triggers anciens dont le dispatched_action avait expiré (TTL),
        donc renvoyés en masse à chaque digest (~1000 alertes/run).

        Fenêtre 24h : un trigger persistant produit une ligne fraîche par jour
        (dedup à la journée), donc reste capté ; la borne ≤ TTL daily (24h) évite
        de renvoyer deux fois la même occurrence d'un jour à l'autre.
        """
        query = f"""
            SELECT * FROM `{TRIGGERS_TABLE}`
            WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            ORDER BY detected_at
        """
        rows = list(self.bq.query(query).result())
        logger.info(f"{len(rows)} trigger(s) chargé(s) (24h) depuis action_engine.triggers")
        return [dict(r) for r in rows]

    def _load_open_dispatches(self) -> set[tuple[str, str, str]]:
        """Charge les (trigger_name, property_id, action_type) déjà dispatchés non resolved."""
        query = f"""
            SELECT trigger_name, property_id, action_type
            FROM `{DISPATCHED_TABLE}`
            WHERE status = 'open'
        """
        rows = list(self.bq.query(query).result())
        keys = {(r["trigger_name"], r["property_id"], r["action_type"]) for r in rows}
        logger.info(f"{len(keys)} dispatched_action(s) ouverte(s)")
        return keys

    # ── Étape 3 : Dispatch d'un trigger vers ses actions configurées ────────

    @staticmethod
    def _render_template(template: str, trigger: dict) -> str:
        """Format() le template avec les champs du context JSON + colonnes du trigger."""
        try:
            ctx = json.loads(trigger.get("context") or "{}")
        except Exception:
            ctx = {}
        # On accepte aussi les champs scalaires du trigger lui-même (apartment_code, etc.)
        merged = {**ctx, **{k: v for k, v in trigger.items() if isinstance(v, (str, int, float))}}
        try:
            return template.format(**merged)
        except KeyError as e:
            logger.warning(f"Template key manquante : {e} — template laissé brut")
            return template

    def _dispatch_trigger(self, trigger: dict, open_keys: set):
        """Pour 1 trigger, exécute toutes ses actions selon routing.yaml."""
        trigger_name = trigger["trigger_name"]
        property_id = trigger["property_id"]

        config = self.routing.get("triggers", {}).get(trigger_name)
        if not config:
            logger.info(f"Skip {trigger_name}/{property_id} : pas de routing configuré")
            return
        if not config.get("enabled", False):
            logger.debug(f"Skip {trigger_name}/{property_id} : enabled=false")
            return

        for action in config.get("actions", []):
            action_type = action["type"]
            key = (trigger_name, property_id, action_type)

            # Idempotence : déjà dispatché et non resolved → skip
            if key in open_keys:
                logger.debug(f"Skip {key} : dispatched_action ouverte existe")
                continue

            params = action.get("params", {}) or {}

            if action_type == "email_digest":
                # Buffer (flush plus tard)
                bucket = action.get("bucket")
                if not bucket:
                    logger.warning(f"{trigger_name} : email_digest sans bucket — skip")
                    continue
                self._digest_buffer.setdefault(bucket, []).append((trigger, params))
                logger.debug(f"Bufferisé pour digest[{bucket}] : {trigger_name}/{property_id}")
            else:
                # Action directe (asana, breezeway)
                try:
                    handler = self._get_handler(action_type)
                    # Adapter : les handlers existants attendent un format "action" legacy
                    # On forge un dict compatible
                    action_payload = {
                        "rule_name": trigger_name,
                        "property_id": property_id,
                        "context": trigger.get("context"),
                        "detected_at": trigger.get("detected_at"),
                    }
                    external_id = handler.execute(action_payload, params)
                    self._insert_dispatched(
                        trigger_name=trigger_name,
                        property_id=property_id,
                        action_type=action_type,
                        bucket=None,
                        external_id=str(external_id) if external_id else None,
                        context=trigger.get("context"),
                        status="open",
                    )
                    logger.info(f"[OK] {trigger_name} → {action_type} | external_id={external_id}")
                except SkipAction as e:
                    logger.info(f"[SKIP] {trigger_name} → {action_type} : {e}")
                except Exception as e:
                    logger.error(f"[ERROR] {trigger_name} → {action_type} : {e}", exc_info=True)
                    self._insert_dispatched(
                        trigger_name=trigger_name,
                        property_id=property_id,
                        action_type=action_type,
                        bucket=None,
                        external_id=None,
                        context=trigger.get("context"),
                        status="error",
                        error_message=str(e),
                    )

    # ── Étape 4 : Flush des digests par bucket ──────────────────────────────

    def _flush_digest(self, bucket: str):
        """Envoie 1 mail digest avec tous les triggers bufferisés sur ce bucket."""
        triggers_in_bucket = self._digest_buffer.get(bucket, [])
        bucket_conf = self.routing.get("digest_buckets", {}).get(bucket, {})
        send_if_empty = bucket_conf.get("send_if_empty", False)
        subject_prefix = bucket_conf.get("subject_prefix", "[Merveil]")
        default_recipients = bucket_conf.get("default_recipients", [])

        if not triggers_in_bucket and not send_if_empty:
            logger.info(f"Digest[{bucket}] vide et send_if_empty=False → skip")
            return

        # Conversion triggers → format attendu par EmailDigestHandler (legacy schema)
        alerts = [self._trigger_to_legacy_alert(t, params) for (t, params) in triggers_in_bucket]

        # Si pas d'alerte mais send_if_empty=True, on ajoute un heartbeat
        if not alerts and send_if_empty:
            alerts = []  # le handler legacy gère naturellement "rien à signaler"

        handler = EmailDigestHandler()
        digest_conf = {
            "to": ", ".join(default_recipients),
            "send_if_empty": send_if_empty,
            "subject_prefix": subject_prefix,
        }
        try:
            message_id = handler.execute_batch(alerts, digest_conf)
            # Log 1 dispatched_action par trigger envoyé
            for (trigger, _params) in triggers_in_bucket:
                self._insert_dispatched(
                    trigger_name=trigger["trigger_name"],
                    property_id=trigger["property_id"],
                    action_type="email_digest",
                    bucket=bucket,
                    external_id=str(message_id) if message_id else None,
                    context=trigger.get("context"),
                    status="open",
                )
            logger.info(f"Digest[{bucket}] envoyé : {len(triggers_in_bucket)} trigger(s)")
        except SkipAction as e:
            logger.info(f"[SKIP] digest[{bucket}] : {e}")
        except Exception as e:
            logger.error(f"[ERROR] digest[{bucket}] : {e}", exc_info=True)

    @staticmethod
    def _trigger_to_legacy_alert(trigger: dict, _params: dict) -> dict:
        """Convertit un trigger en format compatible avec EmailDigestHandler legacy."""
        detected_at = trigger.get("detected_at")
        # alert_date = juste la date (le mail digest legacy attendait une DATE, pas un TIMESTAMP)
        alert_date = detected_at.date() if hasattr(detected_at, "date") else detected_at
        return {
            "alert_type": trigger["trigger_name"],
            "property_id": trigger["property_id"],
            "severity": trigger.get("severity") or "INFO",
            "entity_name": trigger.get("entity_id"),
            "alert_message": trigger.get("message"),
            "action_recommended": trigger.get("action_url"),
            "alert_category": trigger.get("category"),
            "alert_date": alert_date,
            "detected_at": detected_at,
        }

    # ── Helpers persistance ─────────────────────────────────────────────────

    def _insert_dispatched(
        self,
        trigger_name: str,
        property_id: str,
        action_type: str,
        bucket: str | None,
        external_id: str | None,
        context: str | None,
        status: str,
        error_message: str | None = None,
    ):
        row = {
            "id": str(uuid.uuid4()),
            "trigger_name": trigger_name,
            "property_id": property_id or "",
            "action_type": action_type,
            "bucket": bucket,
            "status": status,
            "external_id": external_id,
            "dispatched_at": datetime.now(timezone.utc).isoformat(),
            "resolved_at": None,
            "context": context,
            "error_message": error_message,
            "retry_count": 0,
        }
        errors = self.bq.insert_rows_json(DISPATCHED_TABLE, [row])
        if errors:
            logger.error(f"Insert dispatched_actions échoué : {errors}")

    # ── Point d'entrée ───────────────────────────────────────────────────────

    def _buckets_for_freq(self, freq: str) -> list[str]:
        """Bucket homonyme + buckets satellites déclarés `flush_with: <freq>`.

        ⚠ Sans flush_with, un bucket custom (destinataires dédiés) est bufferisé
        puis JETÉ en fin de run — aucun job ne le flushe jamais (bug silencieux
        découvert 2026-07-17 : data_quality n'avait envoyé 0 mail depuis sa
        création, triggers gouvernance/tests dbt/finance perdus sans erreur).
        """
        satellites = [
            b for b, conf in self.routing.get("digest_buckets", {}).items()
            if (conf or {}).get("flush_with") == freq and b != freq
        ]
        return [freq] + satellites

    def run(self, freq: str | None = None):
        """
        Exécute un cycle complet de dispatch.

        Args:
            freq: '4h' | 'daily' | None
                  Détermine quels buckets de digest sont flushés à la fin :
                  le bucket homonyme + les buckets `flush_with: <freq>`.
                  Les autres actions (asana_task, breezeway_task) sont exécutées
                  indépendamment de freq.
        """
        logger.info(f"=== TriggerDispatcher start (freq={freq}) ===")
        flush_buckets = self._buckets_for_freq(freq) if freq else []

        # 1. Résolutions
        self._resolve_completed_breezeway()
        for bucket in flush_buckets:
            self._resolve_expired_digests(bucket)

        # 2. Charger l'état
        triggers = self._load_triggers()
        open_keys = self._load_open_dispatches()

        # 3. Dispatch
        for trigger in triggers:
            self._dispatch_trigger(trigger, open_keys)

        # 4. Flush digests : bucket courant + satellites flush_with
        for bucket in flush_buckets:
            self._flush_digest(bucket)

        logger.info(f"=== TriggerDispatcher done ===")
