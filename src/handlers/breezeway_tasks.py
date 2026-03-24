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

    def _get_company_people_id(self, name: str) -> int | None:
        """Cherche un utilisateur Breezeway par nom exact. Retourne company_people_id ou None."""
        response = requests.get(
            f"{BREEZEWAY_BASE_URL}/company-people/v1/",
            headers=self._headers(),
            params={"search": name},
            timeout=10,
        )
        response.raise_for_status()
        results = response.json().get("results", [])
        for person in results:
            if person.get("full_name", "").strip() == name:
                return person.get("id")
        return None

    def _get_cleaning_assignees(self, home_id: int, scheduled_date: str) -> list[int]:
        """
        Retourne les company_people_id assignés au ménage prévu ce jour
        pour cet appartement. Liste vide si aucun ménage trouvé.
        """
        response = requests.get(
            f"{BREEZEWAY_BASE_URL}/inventory/v1/task/",
            headers=self._headers(),
            params={
                "home_id": home_id,
                "scheduled_date": scheduled_date,
                "department": "housekeeping",
            },
            timeout=10,
        )
        response.raise_for_status()
        tasks = response.json().get("results", [])
        assignee_ids = []
        for task in tasks:
            for assignment in task.get("assignments", []):
                cid = assignment.get("company_people_id")
                if cid and cid not in assignee_ids:
                    assignee_ids.append(cid)
        return assignee_ids

    def execute(self, action: dict, params: dict) -> str:
        """
        Crée une task Breezeway.

        Args:
            action: ligne de pending_actions
                    {rule_name, property_id, context, detected_at}
                    context doit contenir : home_id, apartment_code, checkin_date, ...
            params: paramètres depuis rules.yaml
                    {name_template, department, subdepartment, priority,
                     assign_economat, assign_cleaning_team}

        Returns:
            breezeway_task_id (str)
        """
        context = json.loads(action.get("context") or "{}")
        home_id = context.get("home_id")
        if not home_id:
            raise ValueError(f"home_id manquant dans context pour rule={action['rule_name']}")

        apartment_code = context.get("apartment_code", "")
        apartment_name = context.get("apartment_name", f"Propriété {home_id}")
        checkin_date = context.get("checkin_date", "")

        name_template = params.get("name_template", "Action requise — {apartment_name}")
        task_name = name_template.format(
            apartment_name=apartment_name,
            apartment_code=apartment_code,
            checkin_date=checkin_date,
            **{k: v for k, v in context.items() if isinstance(v, str)},
        )

        payload = {
            "name": task_name,
            "home_id": int(home_id),
        }
        if params.get("department"):
            payload["type_department"] = params["department"]
        if params.get("subdepartment"):
            payload["type_subdepartment"] = params["subdepartment"]
        if params.get("priority"):
            payload["type_priority"] = params["priority"]
        if checkin_date:
            payload["scheduled_date"] = checkin_date

        # Assignation : Economat de l'appartement + équipe ménage du jour
        assignee_ids: list[int] = []

        if params.get("assign_economat") and apartment_code:
            economat_name = f"Economat {apartment_code}"
            economat_id = self._get_company_people_id(economat_name)
            if economat_id:
                assignee_ids.append(economat_id)
                logger.info(f"Economat trouvé : {economat_name} → id={economat_id}")
            else:
                logger.warning(f"Economat introuvable pour {economat_name}")

        if params.get("assign_cleaning_team") and home_id and checkin_date:
            cleaning_ids = self._get_cleaning_assignees(int(home_id), checkin_date)
            for cid in cleaning_ids:
                if cid not in assignee_ids:
                    assignee_ids.append(cid)
            logger.info(f"Équipe ménage trouvée : {cleaning_ids}")

        if assignee_ids:
            payload["assignments"] = [{"company_people_id": cid} for cid in assignee_ids]

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
