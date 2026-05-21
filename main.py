"""
merveil-action-engine — Entry Point
=====================================
Cloud Run Job : s'exécute une fois et termine.
Déclenché par Cloud Scheduler après chaque dbt run.

Refonte trigger/action/routing (2026-05-21, cf. decisions.md) :
  Lit action_engine.triggers (produit par dbt), confronte au routing.yaml,
  exécute les actions via les handlers (asana, breezeway, email_digest).
"""

import logging
import os
import sys

from src.core.dispatcher import TriggerDispatcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

FREQ = os.getenv("FREQ")  # 4h | daily | weekly | None


if __name__ == "__main__":
    try:
        TriggerDispatcher().run(freq=FREQ)
        sys.exit(0)
    except Exception as e:
        logging.critical(f"Action engine a planté : {e}", exc_info=True)
        sys.exit(1)
