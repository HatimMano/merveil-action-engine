"""
merveil-action-engine — Entry Point
=====================================
Cloud Run Job : s'exécute une fois et termine.
Déclenché par Cloud Scheduler après chaque dbt run.
"""

import logging
import sys

from src.core.runner import ActionRunner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

if __name__ == "__main__":
    try:
        runner = ActionRunner()
        runner.run()
        sys.exit(0)
    except Exception as e:
        logging.critical(f"Action engine a planté : {e}", exc_info=True)
        sys.exit(1)
