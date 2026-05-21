"""
Handler : Asana Tasks
=====================
Crée une tâche Asana à partir d'une action pending.

Idempotence (free tier — pas de custom fields) :
  source_key encodé dans les notes au format `source_key: <rule>:<property_id>`
  Avant POST, on liste les tâches ouvertes du projet et on skip si match.
"""

import json
import logging
import os
from typing import Iterable

import requests

from src.handlers import SkipAction

logger = logging.getLogger(__name__)

ASANA_BASE_URL = "https://app.asana.com/api/1.0"
SOURCE_KEY_PREFIX = "source_key:"


class AsanaTaskHandler:
    """
    Crée des tâches Asana via PAT.

    Auth : Personal Access Token via env var ASANA_PAT (Secret Manager).

    Params attendus depuis rules.yaml :
        project_id (str)        — gid du projet Asana cible (obligatoire)
        workspace_id (str)      — gid du workspace (obligatoire)
        name_template (str)     — template format() avec champs du context
        notes_template (str)    — template format() (optionnel)
        assignee_email (str)    — email Asana de l'assignee (optionnel)
        due_offset_days (int)   — délai en jours depuis détection (optionnel, défaut 3)
        section_id (str)        — gid de section (optionnel)
    """

    def __init__(self):
        self._pat: str | None = None
        # cache des tâches ouvertes par projet (1 lecture par run)
        self._open_tasks_cache: dict[str, list[dict]] = {}

    def _token(self) -> str:
        if not self._pat:
            pat = os.getenv("ASANA_PAT")
            if not pat:
                raise ValueError("ASANA_PAT manquant (env var ou Secret Manager)")
            self._pat = pat.strip()
        return self._pat

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._token()}",
            "Content-Type": "application/json",
        }

    # ── Idempotence ──────────────────────────────────────────────────────────

    def _list_open_tasks(self, project_id: str) -> list[dict]:
        if project_id in self._open_tasks_cache:
            return self._open_tasks_cache[project_id]

        r = requests.get(
            f"{ASANA_BASE_URL}/projects/{project_id}/tasks",
            headers=self._headers(),
            params={
                "completed_since": "now",  # uniquement non-terminées
                "opt_fields": "name,notes,completed,completed_at",
                "limit": 100,
            },
            timeout=15,
        )
        r.raise_for_status()
        tasks = r.json().get("data", [])
        self._open_tasks_cache[project_id] = tasks
        return tasks

    @staticmethod
    def _extract_source_key(notes: str) -> str | None:
        if not notes:
            return None
        for line in notes.splitlines():
            line = line.strip()
            if line.lower().startswith(SOURCE_KEY_PREFIX):
                return line[len(SOURCE_KEY_PREFIX):].strip()
        return None

    def _find_existing_open(self, project_id: str, source_key: str) -> dict | None:
        for t in self._list_open_tasks(project_id):
            if self._extract_source_key(t.get("notes", "")) == source_key:
                return t
        return None

    # ── Lookups ──────────────────────────────────────────────────────────────

    def _resolve_assignee_gid(self, workspace_id: str, email: str) -> str | None:
        """Résout email → user gid dans le workspace. None si introuvable."""
        if not email:
            return None
        try:
            r = requests.get(
                f"{ASANA_BASE_URL}/workspaces/{workspace_id}/users",
                headers=self._headers(),
                params={"opt_fields": "email,name", "limit": 100},
                timeout=10,
            )
            r.raise_for_status()
            for u in r.json().get("data", []):
                if (u.get("email") or "").lower() == email.lower():
                    return u["gid"]
        except Exception as e:
            logger.warning(f"Résolution assignee Asana {email!r} échouée : {e}")
        return None

    # ── Templating ───────────────────────────────────────────────────────────

    @staticmethod
    def _render(template: str, context: dict, extra: dict) -> str:
        safe_ctx = {k: v for k, v in context.items() if isinstance(v, (str, int, float))}
        try:
            return template.format(**safe_ctx, **extra)
        except KeyError as e:
            logger.warning(f"Clé manquante dans template Asana : {e} — template laissé brut")
            return template

    # ── Execute ──────────────────────────────────────────────────────────────

    def execute(self, action: dict, params: dict) -> str:
        rule_name = action["rule_name"]
        property_id = action.get("property_id") or ""
        context = json.loads(action.get("context") or "{}")

        project_id = params.get("project_id")
        workspace_id = params.get("workspace_id")
        if not project_id or not workspace_id:
            raise ValueError("project_id et workspace_id requis dans params (rules.yaml)")

        # Idempotence
        source_key = f"{rule_name}:{property_id}"
        existing = self._find_existing_open(project_id, source_key)
        if existing:
            logger.info(
                f"Skip Asana : tâche déjà ouverte source_key={source_key} "
                f"(gid={existing['gid']})"
            )
            raise SkipAction(f"Tâche Asana existante pour {source_key}")

        # Build payload
        name_template = params.get("name_template", "Action requise — {apartment_name}")
        notes_template = params.get("notes_template", "")
        extra = {
            "rule_name": rule_name,
            "property_id": property_id,
            "detected_at": str(action.get("detected_at") or ""),
        }
        name = self._render(name_template, context, extra)

        notes_body = self._render(notes_template, context, extra) if notes_template else ""
        notes_full = (notes_body + ("\n\n" if notes_body else "") + f"{SOURCE_KEY_PREFIX} {source_key}")

        payload: dict = {
            "workspace": workspace_id,
            "projects": [project_id],
            "name": name,
            "notes": notes_full,
        }

        # Due date
        due_offset = params.get("due_offset_days")
        if due_offset is not None:
            try:
                import datetime
                due = (datetime.date.today() + datetime.timedelta(days=int(due_offset))).isoformat()
                payload["due_on"] = due
            except Exception:
                pass

        # Assignee
        assignee_email = params.get("assignee_email")
        if assignee_email:
            gid = self._resolve_assignee_gid(workspace_id, assignee_email)
            if gid:
                payload["assignee"] = gid
            else:
                logger.warning(f"Assignee Asana introuvable : {assignee_email}")

        # Section
        if params.get("section_id"):
            payload["memberships"] = [{
                "project": project_id,
                "section": params["section_id"],
            }]

        logger.info(f"Création tâche Asana : {name!r} (source_key={source_key})")
        r = requests.post(
            f"{ASANA_BASE_URL}/tasks",
            headers=self._headers(),
            json={"data": payload},
            timeout=15,
        )
        r.raise_for_status()
        task_gid = r.json()["data"]["gid"]
        logger.info(f"Tâche Asana créée : gid={task_gid}")
        return task_gid

    # ── Lookups exposés (utiles pour /api/data, tests, debugging) ────────────

    def list_open_tasks(self, project_id: str) -> Iterable[dict]:
        return self._list_open_tasks(project_id)
