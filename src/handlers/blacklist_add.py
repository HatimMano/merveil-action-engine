"""
BlacklistAddHandler — auto-blacklist machine (2026-09-01, décision Hatim).

Sur un trigger `test_cartes` (≥2 refus carte motif FRAUDE, cf. le trigger dbt),
la machine crée la fiche blacklist ELLE-MÊME au lieu d'attendre le geste RC —
le fraudeur s'auto-annule en ~30 min et la fiche est ce qui prolonge la
détection au-delà de la fenêtre 48 h du signal jumelle (cas Enzo Simone :
2 profils, 2 emails jetables, retour possible sous le même nom).

Écrit directement dans `dwh_inputs.record_edits` (scope `blacklist`, le mode
« record » du framework rules-edition) — mêmes champs que l'UI 6.7, donc la
fiche machine est indistinguable d'une fiche humaine à l'écran, à ceci près
que `signale_par` affiche `machine:test_cartes`.

⚠ RÈGLES, à ne pas défaire :
- `gravite` = TOUJOURS `vigilance`, jamais `interdiction` : la machine PROPOSE,
  l'interdiction reste un verdict humain (doctrine blacklist du 29/08 —
  c'est la liste d'Emilia, la machine n'y pose que des candidats motivés).
- 1 fiche PAR PROFIL Mews (`customer:<id>`), pas par nom : chaque profil porte
  un hameçon de rapprochement différent (customer_id exact + email fort + tél
  fort). Fusionner par nom perdrait ces hameçons. Le LIEN entre profils du
  même individu se fait dans la `note` (la machine cherche les fiches au même
  nom normalisé et les référence croisées).
- Idempotent : si la fiche existe déjà (machine OU humaine), on n'écrit rien —
  on ne réécrit jamais par-dessus une édition d'Emilia.
- Best-effort : un échec d'écriture ne fait pas échouer le dispatch du mail.
"""

import json
import logging
import unicodedata
from datetime import datetime, timezone

from google.cloud import bigquery

logger = logging.getLogger(__name__)

PROJECT_ID = "merveil-data-warehouse"
RECORD_EDITS = f"{PROJECT_ID}.dwh_inputs.record_edits"
EDITED_BY = "machine:test_cartes"


def _norm_name(s: str) -> str:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.lower().split())


class BlacklistAddHandler:

    def __init__(self):
        self.bq = bigquery.Client(project=PROJECT_ID)

    def execute(self, action: dict, params: dict) -> str:
        ctx = json.loads(action.get("context") or "{}")
        customer_id = ctx.get("customer_id")
        name = ctx.get("customer_name") or "?"
        if not customer_id:
            logger.warning("blacklist_add: pas de customer_id dans le context — skip")
            return "skipped:no_customer_id"

        record_key = f"customer:{customer_id}"

        # Idempotence : la fiche existe déjà (peu importe qui l'a créée) → rien.
        exists = list(self.bq.query(
            f"SELECT 1 FROM `{RECORD_EDITS}` "
            f"WHERE scope='blacklist' AND record_key=@rk AND field='nom' LIMIT 1",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("rk", "STRING", record_key)]),
        ).result())
        if exists:
            logger.info(f"blacklist_add: fiche {record_key} déjà présente — skip")
            return "skipped:exists"

        # Lien entre profils du même individu : fiches existantes au même nom
        # normalisé (c'est le cas Enzo Simone — 2 profils, 2 emails).
        siblings = []
        try:
            rows = self.bq.query(
                f"""SELECT record_key, value FROM `{RECORD_EDITS}`
                    WHERE scope='blacklist' AND field='nom'
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY record_key ORDER BY edited_at DESC) = 1""",
            ).result()
            target = _norm_name(name)
            siblings = [r.record_key for r in rows
                        if _norm_name(r.value) == target and r.record_key != record_key]
        except Exception as e:
            logger.warning(f"blacklist_add: recherche de fiches liées échouée ({e})")

        resa = ctx.get("reservation_number")
        note = (
            f"[machine] Test de cartes : {ctx.get('n_refus_durs')} refus motif FRAUDE "
            f"sur la résa {resa} ({ctx.get('apartment_code') or 'sans appart'}, "
            f"CI {ctx.get('checkin_date')}, {ctx.get('canal')})."
        )
        if siblings:
            note += f" Même nom que fiche(s) existante(s) : {', '.join(siblings)} — probablement le même individu."
        note = note[:500]

        now = datetime.now(timezone.utc).isoformat()
        fields = {
            "nom": name,
            "email": ctx.get("customer_email") or "",
            "telephone": ctx.get("customer_phone") or "",
            "motif": "fraude_paiement",
            "gravite": "vigilance",
            "date_incident": datetime.now(timezone.utc).date().isoformat(),
            "note": note,
            "actif": "oui",
        }
        rows = [
            {"scope": "blacklist", "record_key": record_key, "field": k,
             "value": v, "edited_by": EDITED_BY, "edited_at": now}
            for k, v in fields.items() if v
        ]
        errors = self.bq.insert_rows_json(RECORD_EDITS, rows)
        if errors:
            logger.error(f"blacklist_add: insert échoué {errors}")
            return f"error:{str(errors)[:100]}"

        # Référence croisée sur les fiches sœurs (append-only : on pose une
        # nouvelle version de leur note SEULEMENT si la machine en est déjà
        # l'auteur — on ne réécrit jamais une note humaine).
        for sib in siblings:
            try:
                last = list(self.bq.query(
                    f"""SELECT value, edited_by FROM `{RECORD_EDITS}`
                        WHERE scope='blacklist' AND record_key=@rk AND field='note'
                        ORDER BY edited_at DESC LIMIT 1""",
                    job_config=bigquery.QueryJobConfig(query_parameters=[
                        bigquery.ScalarQueryParameter("rk", "STRING", sib)]),
                ).result())
                if last and last[0].edited_by == EDITED_BY and record_key not in (last[0].value or ""):
                    new_note = f"{last[0].value} Autre profil du même individu : {record_key}."[:500]
                    self.bq.insert_rows_json(RECORD_EDITS, [{
                        "scope": "blacklist", "record_key": sib, "field": "note",
                        "value": new_note, "edited_by": EDITED_BY, "edited_at": now}])
            except Exception as e:
                logger.warning(f"blacklist_add: cross-ref {sib} échouée ({e})")

        logger.info(f"blacklist_add: fiche {record_key} créée ({name}, vigilance)"
                    + (f" liée à {siblings}" if siblings else ""))
        return record_key
