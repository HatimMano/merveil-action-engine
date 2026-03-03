"""
Handler : Breezeway Tasks
=========================
Crée une task Breezeway à partir d'une action pending.
"""

import json
import logging
import os

import requests

logger = logging.getLogger(__name__)

BREEZEWAY_BASE_URL = "https://api.breezeway.io/public"


class BreezewayTasksHandler:
    """
    Crée des tasks dans Breezeway.

    Auth : JWT via client_id + client_secret (Secret Manager).
    """

    def __init__(self):
        self._access_token: str | None = None

    def _get_token(self) -> str:
        if self._access_token:
            return self._access_token

        client_id = os.getenv("BREEZEWAY_CLIENT_ID")
        client_secret = os.getenv("BREEZEWAY_CLIENT_SECRET")

        if not client_id or not client_secret:
            raise ValueError("BREEZEWAY_CLIENT_ID et BREEZEWAY_CLIENT_SECRET requis")

        response = requests.post(
            f"{BREEZEWAY_BASE_URL}/auth/v1/",
            json={"client_id": client_id, "client_secret": client_secret},
            timeout=10,
        )
        response.raise_for_status()
        self._access_token = response.json()["access_token"]
        return self._access_token

    def _headers(self) -> dict:
        return {
            "Authorization": f"JWT {self._get_token()}",
            "Content-Type": "application/json",
        }

    def execute(self, action: dict, params: dict) -> str:
        """
        Crée une task Breezeway.

        Args:
            action: ligne de pending_actions
                    {rule_name, property_id, home_id, context, detected_at}
            params: paramètres depuis rules.yaml
                    {name_template, department, priority, ...}

        Returns:
            breezeway_task_id (str)
        """
        home_id = action.get("home_id")
        if not home_id:
            raise ValueError(f"home_id manquant pour rule={action['rule_name']}")

        context = json.loads(action.get("context") or "{}")
        home_name = context.get("home_name", f"Propriété {home_id}")

        name_template = params.get("name_template", "Action requise — {home_name}")
        task_name = name_template.format(home_name=home_name, **context)

        payload = {
            "name": task_name,
            "home_id": int(home_id),
        }
        if params.get("department"):
            payload["type_department"] = params["department"]
        if params.get("priority"):
            payload["type_priority"] = params["priority"]
        if params.get("scheduled_date"):
            payload["scheduled_date"] = params["scheduled_date"]

        logger.info(f"Création task Breezeway : {task_name!r} (home_id={home_id})")

        response = requests.post(
            f"{BREEZEWAY_BASE_URL}/inventory/v1/task/",
            json=payload,
            headers=self._headers(),
            timeout=15,
        )
        response.raise_for_status()

        task_id = str(response.json()["id"])
        logger.info(f"Task créée : id={task_id}")
        return task_id
