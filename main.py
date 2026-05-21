"""
merveil-action-engine — Entry Point
=====================================
Cloud Run Job : s'exécute une fois et termine.
Déclenché par Cloud Scheduler après chaque dbt run.

Feature flag USE_NEW_DISPATCHER (refacto 2026-05-21, cf. decisions.md) :
  - "false" (défaut) → ActionRunner legacy (pending_actions + rule_4h/daily)
  - "true"           → TriggerDispatcher (table unifiée triggers + routing.yaml)
"""

import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

USE_NEW = os.getenv("USE_NEW_DISPATCHER", "false").lower() in ("true", "1", "yes")
FREQ = os.getenv("FREQ")  # 4h | daily | weekly | None


if __name__ == "__main__":
    try:
        if USE_NEW:
            from src.core.dispatcher import TriggerDispatcher
            logging.info("Démarrage TriggerDispatcher (USE_NEW_DISPATCHER=true)")
            TriggerDispatcher().run(freq=FREQ)
        else:
            from src.core.runner import ActionRunner
            logging.info("Démarrage ActionRunner legacy (USE_NEW_DISPATCHER=false)")
            ActionRunner().run()
        sys.exit(0)
    except Exception as e:
        logging.critical(f"Action engine a planté : {e}", exc_info=True)
        sys.exit(1)
