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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

FREQ = os.getenv("FREQ")  # 4h | daily | weekly | 2h | cancellations_brief | iseo_orchestrator | beyond_push


if __name__ == "__main__":
    # Fail-fast : sans FREQ, le dispatcher bufferise les digests puis les JETTE
    # (run(freq=None) ne flush jamais) → toutes les alertes mail muettes, zéro
    # erreur visible. Arrive si un deploy écrase les env vars (--set-env-vars).
    # Mieux vaut un job en échec bruyant qu'un faux succès silencieux.
    # Exécution manuelle : passer un FREQ explicite (ex: FREQ=daily).
    if not FREQ:
        logging.critical(
            "FREQ absent des env vars — les digests seraient silencieusement perdus. "
            "Vérifier le deploy (--set-env-vars doit inclure FREQ) ou passer FREQ explicitement."
        )
        sys.exit(1)

    try:
        if FREQ == "cancellations_brief":
            # Mode standalone : brief annulations 11h Paris pour l'équipe RC.
            # Pas de passage par le dispatcher trigger/action — c'est un rapport
            # quotidien, pas un événement déclencheur d'actions.
            from src.handlers.cancellations_brief import run as run_cancellations_brief
            run_cancellations_brief()
        elif FREQ == "iseo_orchestrator":
            # Mode standalone : pipeline V3 100% DWH (natif coupé). Provisionne les
            # PINs Sofia à J-3 (POST device + invitation + push Duve), resync le drift
            # de dates, archive après CO/annulation. Indépendant du dispatcher.
            from src.handlers.iseo_orchestrator import run as run_iseo_orchestrator
            run_iseo_orchestrator()
        elif FREQ == "beyond_push":
            # Mode standalone : push déclaratif des fenêtres de prix gaps 1N vers
            # Beyond (état voulu = dash_beyond_push_targets, GET → diff → PATCH,
            # log beyond_raw.price_pushes_log). Indépendant du dispatcher.
            from src.handlers.beyond_push import run as run_beyond_push
            run_beyond_push()
        else:
            from src.core.dispatcher import TriggerDispatcher
            TriggerDispatcher().run(freq=FREQ)
        sys.exit(0)
    except Exception as e:
        logging.critical(f"Action engine a planté : {e}", exc_info=True)
        sys.exit(1)
