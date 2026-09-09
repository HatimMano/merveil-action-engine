"""
ISEO PIN Orchestrator — pipeline V3 100% DWH (intégration native Duve↔Sofia COUPÉE).

Cf. [[project_iseo_integration_2026]] + to_do "MAJ 20/06 — pipeline complet validé".

Depuis le 20/06 le natif Duve↔Sofia est désactivé dans les 2 sens. Le DWH est
seul maître du cycle PIN. Pour chaque résa Mews non annulée, payée, dont le CI est
dans les LOOKAHEAD_DAYS prochains jours (et pas encore provisionnée) :

  A. génère un code PIN 4 chiffres (unique account-wide, retry sur collision)
  B. crée (get-or-create par extId) un user Sofia DÉDIÉ à la résa, au VRAI nom du
     guest (firstname/lastname), avec un password aléatoire jamais partagé → le user
     est `enabled=True` et porte un tag `user` auto-créé. Ce tag sert de guestTagId →
     l'UI Luckey affiche le vrai nom du guest. POST Sofia /standardDevices
     (credentialRule sur ce guest tag + le lock tag de l'appart). Le device ancre le
     user (pas de garbage-collection).
  C. POST Sofia /invitations (smartLockIds=[lock_id]) → code → lien remote-open
     `https://archides.jago.cloud/remoteOpen?code=<code>`
  D. POST intégration Duve (champ custom) : primaryCode = code clavier +
     ISEO ACCESS LINK = lien remote-open. (Aucun message déclenché — les messages
     auto Duve lisent le champ. Lien gated sur la window, OK pendant le séjour.)
  E. INSERT état dans iseo_raw.merveil_pin_cache.

Archive (CO passé OU résa annulée) : DELETE Sofia device + DELETE invitation +
DELETE le user dédié de la résa (par extId) + flag archived_at.

⚠️ enabled : un user créé via l'API est enabled=False SAUF si on fournit un
`password` à la création (le schéma create n'a pas de champ `enabled`). Un user
enabled=False finit garbage-collecté / perd son tag → PIN cassé. D'où le password
aléatoire systématique (le guest ne se connecte jamais, il ouvre au PIN clavier).

Résolution des ids appart (par JOIN BQ, pas de seed) :
  - duve property_id (GUID) == Mews resource_id == nom du lock tag Sofia
  - lock_id + lock_tag_id ← stg_iseo__smart_locks (par nom de tag = property_id)
  - guest_tag_id ← tag `user` du user dédié de la résa (créé en B)

Modes :
  - ISEO_SHADOW_MODE=true → log "would provision" sans appel Sofia/Duve
  - ISEO_ALLOWED_PROPERTY_IDS (csv de GUID) → whitelist rollout par appart

Trigger : Cloud Run Job `merveil-action-engine-iseo` (scheduler 2h à :45).
"""

import logging
import os
import secrets
import time
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from google.cloud import bigquery

from src.core.mailer import build_email, esc, send_mail

PARIS_TZ = ZoneInfo("Europe/Paris")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "merveil-data-warehouse")
PIN_CACHE_TABLE = os.environ.get(
    "ISEO_PIN_CACHE_TABLE", "merveil-data-warehouse.iseo_raw.merveil_pin_cache")
RAW_DUVE_CHECKIN_TABLE = os.environ.get(
    "RAW_DUVE_CHECKIN_TABLE", "merveil-data-warehouse.raw_duve.checkin_events")
DUVE_CHECKIN_STG_TABLE = os.environ.get(
    "DUVE_CHECKIN_STG_TABLE", "merveil-data-warehouse.staging.stg_duve__checkin_events")
MEWS_FCT_TABLE = os.environ.get(
    "MEWS_FCT_TABLE", "merveil-data-warehouse.marts.fct_reservations")
ISEO_DEVICES_STG_TABLE = os.environ.get(
    "ISEO_DEVICES_STG_TABLE", "merveil-data-warehouse.staging.stg_iseo__standard_devices")
# ⚠ `MEWS_PAYMENTS_TABLE` / `MEWS_ORDER_ITEMS_TABLE` retirés le 09/09/2026 : la logique
# paiement est passée dans la vue `int_reservations__payments` (cf. `PAYMENTS_TABLE`).
SMART_LOCKS_TABLE = os.environ.get(
    "SMART_LOCKS_TABLE", "merveil-data-warehouse.staging.stg_iseo__smart_locks")
STD_DEVICES_TABLE = os.environ.get(
    "STD_DEVICES_TABLE", "merveil-data-warehouse.staging.stg_iseo__standard_devices")
GATEWAYS_TABLE = os.environ.get(
    "GATEWAYS_TABLE", "merveil-data-warehouse.staging.stg_iseo__gateways")
GATEWAY_PUSH_HEALTH_TABLE = os.environ.get(
    "GATEWAY_PUSH_HEALTH_TABLE",
    "merveil-data-warehouse.staging.stg_iseo__gateway_push_health")
WHITELIST_TABLE = os.environ.get(
    "ISEO_WHITELIST_TABLE", "merveil-data-warehouse.staging.iseo_whitelisted_apartments")
HOLD_DECISIONS_TABLE = os.environ.get(
    "ISEO_HOLD_DECISIONS_TABLE", "merveil-data-warehouse.iseo_raw.hold_decisions")

ISEO_BASE_URL = os.environ.get("ISEO_BASE_URL", "https://api-archides.jago.cloud")
ISEO_USERNAME = (os.environ.get("ISEO_MANAGER_USERNAME") or "").strip()
ISEO_PASSWORD = (os.environ.get("ISEO_MANAGER_PASSWORD") or "").strip()

# Intégration entrante Duve (write path) — cf. to_do 20/06.
DUVE_CONNECT_URL = os.environ.get(
    "DUVE_CONNECT_URL", "https://connect.duve.com/api/v1/hooks/duveconnect")
DUVE_CONNECT_PID = os.environ.get("DUVE_CONNECT_PID", "")
DUVE_CONNECT_TOKEN = (os.environ.get("DUVE_CONNECT_TOKEN") or "").strip()
DUVE_FIELD_NAME = os.environ.get(
    "DUVE_FIELD_NAME", "merveil_paris_iseo_access_link_eIhhEnlspM")
# La page web guest est sur archides.jago.cloud, PAS le host api- renvoyé par l'API.
REMOTE_OPEN_HOST = os.environ.get("ISEO_REMOTE_OPEN_HOST", "archides.jago.cloud")

ISEO_SHADOW_MODE = os.environ.get("ISEO_SHADOW_MODE", "true").lower() == "true"
# Whitelist des apparts cutover. Source de vérité = seed BQ iseo_whitelisted_apartments
# (chargé au run via _load_whitelist → élargir = 1 ligne dans le seed, sans redeploy).
# La valeur env ci-dessous = FALLBACK si le seed est vide/inaccessible (garde-fou).
ALLOWED_PROPERTY_IDS = {
    pid.strip().lower()
    for pid in (os.environ.get("ISEO_ALLOWED_PROPERTY_IDS") or "").split(",")
    if pid.strip()
}

# Alerting mail (réutilise l'infra Gmail API du service — secret alerts-gmail-sa-key
# lu via Secret Manager + Domain-Wide Delegation, comme cancellations_brief).
GMAIL_SENDER = os.getenv("GMAIL_SENDER", "noreply@archides.fr")
ISEO_ALERT_TO = os.getenv("ISEO_ALERT_TO", "hatim@archides.fr")

LOOKAHEAD_DAYS = int(os.environ.get("ISEO_LOOKAHEAD_DAYS", "3"))

# ── Porte de validation (hold) ────────────────────────────────────────────────
# Le code est CRÉÉ côté Sofia (visible au dashboard, révocable) mais N'EST PAS
# poussé à Duve : le client ne le voit pas, la RC valide puis le lui envoie.
#   off     → porte désactivée
#   observe → décision calculée et journalisée, mais on pousse quand même (défaut)
#   on      → rétention effective
# ⚠ La porte n'a d'effet RÉEL qu'une fois le code fixe retiré du champ Duve de
# l'appartement : tant qu'il y est, Duve l'affiche en repli et le client entre
# quand même. Elle mord donc progressivement, appartement par appartement, au
# rythme des suppressions côté ops — c'est voulu, ça rend l'activation sans risque.
ISEO_HOLD_MODE = os.environ.get("ISEO_HOLD_MODE", "observe").lower()
# Seuil « réservé peu avant l'arrivée », appliqué au canal DIRECT seul (cf. _evaluate_hold).
ISEO_HOLD_LEAD_HOURS = int(os.environ.get("ISEO_HOLD_LEAD_HOURS", "72"))
# Critère « solde restant dû » (ex-« rien d'encaissé »), lui aussi canal DIRECT seul :
#   - montant minimum pour qu'un reliquat compte (un résidu de 210 € sur 4 156 € n'est
#     pas un signal de fraude) ;
#   - ancienneté MAX de la réservation : une résa posée 6 mois à l'avance et pas encore
#     soldée est un sujet de relance commerciale, pas de fraude → on n'alerte pas
#     (demande Hatim 15/08). 720 h = 30 j.
ISEO_HOLD_MIN_BALANCE = float(os.environ.get("ISEO_HOLD_MIN_BALANCE", "1"))
# … et part minimale du séjour restant à payer : mesuré le 15/08, se contenter d'un
# solde > 0 fait sonner des reliquats de 75 € sur un séjour complet (Largaespada) ou
# 210 € sur 4 156 € (Javier). Le signal utile est « la moitié du séjour n'est toujours
# pas payée », pas « il reste un résidu ».
ISEO_HOLD_MIN_BALANCE_RATIO = float(
    os.environ.get("ISEO_HOLD_MIN_BALANCE_RATIO", "0.5"))
ISEO_HOLD_BALANCE_MAX_LEAD_HOURS = int(
    os.environ.get("ISEO_HOLD_BALANCE_MAX_LEAD_HOURS", "720"))
# Destinataire de la notification « code retenu » (défaut = alerte ISEO).
ISEO_HOLD_ALERT_TO = os.getenv("ISEO_HOLD_ALERT_TO", "") or ISEO_ALERT_TO
# Fenêtre de validité du code, alignée sur les horaires CONTRACTUELS du séjour
# (décision Hatim 2026-08-17) : ouverture = min(heure annoncée par le client, 16 h),
# fermeture = max(heure annoncée, 11 h). Autrement dit le code vaut de 16 h à 11 h,
# étendu — jamais rétréci — par une arrivée anticipée ou un départ tardif négociés.
# ⚠ Deux conséquences assumées vs les valeurs précédentes (13 h / 19 h) :
#   · un client qui se présente à 15 h sans avoir annoncé d'arrivée anticipée est
#     refusé par la porte (il passait avec 13 h) → c'est un appel RC ;
#   · le jour du départ, plus de retour possible après 11 h pour récupérer des
#     bagages (la borne avait justement été relevée à 19 h le 14/08 pour ça).
# ⚠ Ne s'applique qu'aux codes NOUVELLEMENT provisionnés : `_resa_to_resync` ne
# détecte qu'un drift de DATES, pas d'heures, et le cache ne stocke pas les heures
# → le stock existant garde 13 h/19 h jusqu'à son archivage (~1 semaine).
# ⚠ 13:00, PAS 16:00 — décision Hatim 24/08. Le commit du 18/08 (« alignée sur les
# horaires contractuels ») avait basculé à 16 h : contractuel ne veut pas dire vécu —
# un client qui arrive à 14 h 30 avec un early check-in accordé oralement tapait un
# code mort. 13 h = marge d'une heure sur le CI contractuel, et l'ETA déclarée au
# pre-checkin ouvre plus tôt encore si besoin (`_earliest_hour`, plancher 07:00).
DEFAULT_CI_HOUR = os.environ.get("ISEO_DEFAULT_CI_HOUR", "13:00")
DEFAULT_CO_HOUR = os.environ.get("ISEO_DEFAULT_CO_HOUR", "11:00")
# Heure de fin de fenêtre quand un LATE CHECK-OUT a été acheté via Duve.
# ⚠ Ce n'est PAS une valeur lue dans la donnée : Duve ne transmet pas l'heure
# prolongée (`delivery_at` vaut l'heure standard 11 h sur 10 des 11 orders
# mesurés le 25/08). 18:00 = ce que promet le produit (arbitrage Hatim 21/08).
LATE_CO_HOUR = os.environ.get("ISEO_LATE_CO_HOUR", "18:00")
PIN_COLLISION_RETRIES = 8
# Retry push (04/09) : au-delà de N pushes non appliqués depuis le dernier APPLIED, on
# arrête de relancer à chaque run — c'est un cas pour le restart manuel + escalade ISEO.
# 36 = 3 jours à 1 push/2 h. OPE18 avait 17 échecs et est passé au 2ᵉ push (après restart).
ISEO_RETRY_PUSH_MAX = int(os.environ.get("ISEO_RETRY_PUSH_MAX", "36"))

# Vérification post-écriture (chantier C, 07/09) : un code « délivré » n'est pas un code
# ÉCRIT — l'accusé est `credentialRule.state = UPDATED` sur le device Sofia, que rien ne
# lisait avant le polling ETL 2 h (alerte `code_non_propage` à 12 h). On le lit en direct
# ISEO_VERIFY_AFTER_MIN après la création ; sans accusé à ISEO_VERIFY_RETRY_AFTER_MIN on
# ré-émet un CREDENTIALS_UPDATED sur SA passerelle (≤ ISEO_WRITE_RETRY_MAX fois), et
# dbt/6.1 rendent l'état `non_ecrit` (« dicter le code fixe »). ⛔ Aucun mail : la RC
# lit 6.1 chaque matin (filtre « Problème »), le geste est le même quelle que soit la
# cause (décision Hatim 07/09). Le job tourne toutes les 10 min pour ça.
ISEO_VERIFY_AFTER_MIN = int(os.environ.get("ISEO_VERIFY_AFTER_MIN", "3"))
ISEO_VERIFY_RETRY_AFTER_MIN = int(os.environ.get("ISEO_VERIFY_RETRY_AFTER_MIN", "10"))
ISEO_WRITE_RETRY_MAX = int(os.environ.get("ISEO_WRITE_RETRY_MAX", "3"))
ISEO_VERIFY_MAX_PER_RUN = int(os.environ.get("ISEO_VERIFY_MAX_PER_RUN", "200"))

# Chantier E (07/09, décision Hatim) : le code est créé à J-3 pour TOUT séjour intégré,
# pré-checkin fait ou pas. Sans résa Duve, la ligne de cache est keyée `M<n° résa Mews>`
# (extIds `MERVEIL_RESA - M<n°>`…) et le push Duve part seul à l'arrivée du formulaire.
# Garde-fou quota Luckey (2 éléments par séjour, saturation = plus AUCUNE création,
# intégrés compris) : au-dessus de ce seuil d'éléments utilisés on ne crée plus pour
# les séjours SANS formulaire (= statu quo pour eux, jamais pire).
ISEO_QUOTA_MAX_USED_NO_FORM = int(os.environ.get("ISEO_QUOTA_MAX_USED_NO_FORM", "590"))
WALLET_TABLE = os.environ.get(
    "ISEO_WALLET_TABLE", "merveil-data-warehouse.staging.stg_iseo__wallet")
# Chantier D (07/09) : 3 critères de porte de plus, lus sur les modèles dbt qui
# portent déjà le verdict (pas de recalcul ici) — combo fraude (`is_fraud_combo`,
# calibré par backtest 19/08) et blacklist confirmée (rapprochement exact/fort).
RISK_TABLE = os.environ.get(
    "RISK_TABLE", "merveil-data-warehouse.intermediate_reservations.int_reservations__risk")
BLACKLIST_MATCH_TABLE = os.environ.get(
    "BLACKLIST_MATCH_TABLE",
    "merveil-data-warehouse.intermediate_reservations.int_reservations__blacklist_match")


# ── Sofia auth (singleton) ────────────────────────────────────────────────────

class _SofiaAuth:
    _token: Optional[str] = None
    _expires_at: float = 0.0

    @classmethod
    def get_token(cls) -> str:
        now = time.time()
        if cls._token and now < cls._expires_at - 600:
            return cls._token
        if not ISEO_USERNAME or not ISEO_PASSWORD:
            raise RuntimeError("ISEO credentials missing")
        resp = requests.post(
            f"{ISEO_BASE_URL}/oauth/token", auth=("client", ""),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "password", "username": ISEO_USERNAME, "password": ISEO_PASSWORD},
            timeout=30)
        resp.raise_for_status()
        p = resp.json()
        cls._token = p["access_token"]
        cls._expires_at = now + int(p.get("expires_in", 172800))
        logger.info(f"🔑 Sofia token (exp in {p.get('expires_in', '?')}s)")
        return cls._token


def _sofia(method: str, path: str, json_body=None) -> requests.Response:
    return requests.request(
        method, f"{ISEO_BASE_URL}{path}",
        headers={"Authorization": f"Bearer {_SofiaAuth.get_token()}",
                 "Accept": "application/json", "Content-Type": "application/json"},
        json=json_body, timeout=30)


def _duve_push(duve_resa_id: str, code: str, link: str) -> tuple[bool, Optional[str]]:
    """POST l'intégration entrante Duve : écrit primaryCode (code clavier) +
    le champ custom ISEO ACCESS LINK (lien remote-open). N'émet aucun message
    (les messages auto Duve lisent le champ)."""
    if not (DUVE_CONNECT_PID and DUVE_CONNECT_TOKEN):
        return False, "Duve connect config missing (PID/TOKEN)"
    r = requests.post(
        f"{DUVE_CONNECT_URL}?pid={DUVE_CONNECT_PID}",
        headers={"Authorization": f"Bearer {DUVE_CONNECT_TOKEN}",
                 "Content-Type": "application/json"},
        json={"reservation": duve_resa_id, "primaryCode": code,
              "additionalFields": [{"name": DUVE_FIELD_NAME, "value": link}]},
        timeout=30)
    if r.status_code == 200:
        return True, None
    return False, f"Duve HTTP {r.status_code}: {r.text[:200]}"


def _duve_push_all(duve_ids, code: str, link: str) -> tuple[bool, Optional[str]]:
    """Pousse le même code + lien à TOUS les duve du stay (back-to-back → chaque résa
    Duve reçoit le code, quel que soit le message auto qui se déclenche). Succès =
    tous OK ; sinon renvoie la 1re erreur (le retry re-tentera l'ensemble)."""
    ids = [d for d in (duve_ids or []) if d]
    if not ids:
        return False, "aucun duve_id à pousser"
    errs = []
    for d in ids:
        ok, err = _duve_push(d, code, link)
        if not ok:
            errs.append(f"{d}: {err}")
    return (not errs), ("; ".join(errs) if errs else None)


def _send_alert(subject: str, body: str, html: bool = False) -> None:
    """Mail d'alerte best-effort (infra commune src/core/mailer)."""
    send_mail(subject, body, ISEO_ALERT_TO, html=html, sender=GMAIL_SENDER)


# ── BigQuery ──────────────────────────────────────────────────────────────────

_bq_client: Optional[bigquery.Client] = None


def _bq() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


# CTEs partagés (mapping Duve↔Mews + résolution lock/tags par appart).
_DUVE_LATEST_CTE = f"""
    duve_latest AS (
      SELECT
        reservation_id AS duve_reservation_id,
        property_id    AS duve_property_id,
        (SELECT JSON_VALUE(g, '$.externalId')
           FROM UNNEST(JSON_QUERY_ARRAY(_raw_payload, '$.resource.guestProfiles')) g
           WHERE JSON_VALUE(g, '$.isPrimary') = 'true' LIMIT 1) AS mews_customer_id
      FROM `{RAW_DUVE_CHECKIN_TABLE}`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY reservation_id ORDER BY received_at DESC) = 1
    )"""

_LOCKS_CTE = f"""
    locks AS (
      SELECT l.lock_id, l.apartment_code,
             JSON_VALUE(t, '$.name')              AS duve_property_id,
             CAST(JSON_VALUE(t, '$.id') AS INT64) AS lock_tag_id,
             -- ⚠⚠ Passerelle vivante ? Une serrure ISEO stocke ses codes EN LOCAL ;
             -- c'est la HyperGate qui y pousse les nouveaux. Passerelle morte = le code
             -- part bien dans Duve mais n'arrive JAMAIS dans la serrure, et le client
             -- se retrouve devant une porte qui refuse son code. Vécu sur
             -- `P15-LAO4-0G` : HyperGate hors ligne depuis le 06/07, 9 codes poussés
             -- quand même, et QUATRE clients bloqués dehors (Sardar Bilal 23/07,
             -- Sara Mavromatis 27/07, Maritza Padilla 07/08, Samantha Mamone 12/08 —
             -- verbatims dans le chat Duve). La donnée était là depuis le début, rien
             -- ne la lisait. Cf. ADR 19/08.
             -- ⚠ SEUIL 24 h, PAS 7 JOURS (resserré le 19/08 au soir). Mesuré sur les
             -- 106 HyperGates : **101 pinguent dans l'heure et AUCUNE ne se situe entre
             -- 1 h et 24 h** — les 5 restantes sont à 51 h, 70 h, 72 h, 82 h et 1 072 h.
             -- La distribution est binaire : vivante ou morte. À 7 j, 4 passerelles
             -- tombées restaient non protégées, dont `CAI31-2D` qui est intégré.
             -- L'asymétrie justifie de bloquer tôt : bloquer à tort coûte un code de
             -- séjour en moins (le client garde le code fixe, qui marche, et le
             -- provisioning repart au run suivant, 2 h après) ; pousser à tort met le
             -- client dehors. ⚠ Ne PAS aligner sur le seuil du trigger `iseo_gateway_offline`
             -- (72 h) : alerter et bloquer n'ont pas le même coût d'erreur.
             -- ⭐⭐ TROISIÈME CONDITION, ajoutée le 25/08 (ADR) : `ph.push_stuck`.
             -- Les deux tests ci-dessus lisent la CONNEXION de la passerelle, et
             -- l'incident du 08-24/08 a montré que ça ne suffit pas : les six
             -- passerelles fautives pinguaient toutes à la minute — donc vertes ici —
             -- pendant que sept séjours recevaient un code que la serrure n'a jamais
             -- appris. `P15-LAO4-0G` a tenu SEPT SEMAINES ainsi, et le garde-fou
             -- qu'on avait posé le 19/08 précisément pour ce cas l'a laissé passer.
             -- « Pingue » et « écrit encore les codes » sont deux questions
             -- différentes ; seule la seconde nous intéresse. Le statut du dernier
             -- push `CREDENTIALS_UPDATED` y répond (cf. `stg_iseo__gateway_push_health`).
             -- ⚠ COALESCE à FALSE et pas à TRUE : tant que le flux ETL n'a pas tourné,
             -- l'absence de mesure ne doit pas bloquer le parc entier — l'ancien
             -- comportement reste le repli sûr.
             (g.gateway_id IS NULL
              OR COALESCE(g.hours_since_last_connection, 1e9) >= 24
              OR COALESCE(ph.push_stuck, FALSE)) AS gateway_dead
      FROM `{SMART_LOCKS_TABLE}` l, UNNEST(JSON_QUERY_ARRAY(l.tags)) AS t
      LEFT JOIN `{GATEWAYS_TABLE}` g ON g.gateway_id = l.gateway_id
      LEFT JOIN `{GATEWAY_PUSH_HEALTH_TABLE}` ph ON ph.gateway_id = l.gateway_id
      WHERE JSON_VALUE(t, '$.name') != 'ADMIN'
    )"""

# Ce que le client doit vs ce qu'il a réellement payé — reconstitué, parce que l'API
# Mews n'expose AUCUN champ solde / « to be paid » (vérifié sur `raw_reservations`).
#
# ⚠⚠ Le piège qui a coûté deux guests sans code (15/08) : Mews attache la tentative
# REFUSÉE à la réservation, mais le paiement RÉUSSI au bill / au compte payeur, avec
# `reservation_id` NULL. Et ce compte payeur n'est même pas `customer_id` : Mews
# fabrique un profil « shadow » (mêmes 12 derniers caractères du GUID, préfixe
# différent) qui porte les order items. Compter les paiements par `reservation_id`
# faisait donc voir « impayé » une résa intégralement encaissée : 4 faux positifs sur
# 9 mesurés le 15/08, dont 2 apparts whitelistés laissés SANS code généré (Jack
# Spence, encaissé 4 672 € le 02/08 ; Ray Javier, 3 946 € le 06/08).
#
# ⭐ **Depuis le 09/09/2026 cette logique ne vit plus ici** : elle est dans la vue dbt
# `int_reservations__payments`, SEULE SOURCE du DWH — la porte, 6.1 (`payment_unpaid`),
# 6.7 (`f_solde_du`), le 360 (`encaisse_ttc`) et la simulation de porte la lisent toutes.
# Elle avait fini en 5 copies, dont une (le gate de `dash_ops_arrivals`) déjà divergente :
# la porte pouvait retenir pour non-paiement un séjour que 6.1 affichait réglé.
# La vue ajoute un TROISIÈME chemin de rattachement, le **bill mono-résa** : quand la
# carte du client est refusée et que la RC re-passe l'encaissement sur le compte de
# facturation OTA, le paiement n'a ni `reservation_id` ni compte `Customer` (cas Ariel
# Taylor 59579, 09/09 : refusée à 06:57, encaissée 48 s plus tard sur un compte
# `Company`) — mais il porte le bill des order items. Mesuré sur CI −120 j → +30 j :
# 40 des 60 résas que la porte disait « impayées » étaient intégralement réglées.
#
# ⚠ C'est une VUE sur des vues de staging : la porte voit un encaissement dès que l'ETL
# l'a écrit, sans attendre la cascade dbt de `:15`. Ne pas la matérialiser en table.
# ⚠ `amount_charged` peut couvrir plusieurs séjours (chemin par compte) → un `balance`
# négatif veut dire « rien à devoir », pas un avoir.
# ⚠ Alias `balance` conservé : `_STAYS_CTE` et `_whitelisted_gaps` lisent `pay.balance`.
PAYMENTS_TABLE = os.environ.get(
    "PAYMENTS_TABLE",
    "merveil-data-warehouse.intermediate_reservations.int_reservations__payments")

_PAYMENTS_CTE = f"""
    payments AS (
      SELECT reservation_id, amount_due, amount_charged,
             balance_due AS balance, n_failed
      FROM `{PAYMENTS_TABLE}`
    )"""


# CTE "stay" = unité d'accès physique = occupation CONTINUE d'un guest sur une serrure.
# Un stay regroupe les résas Mews non annulées d'un même (customer_id, resource_id) dont
# les intervalles [CI,CO] sont contigus ou chevauchants (gaps-and-islands). Résout le bug
# de collision quand un client a ≥2 résas sur le même appart (back-to-back → 1 seul code,
# fenêtre fusionnée min(CI)→max(CO) ; un TROU entre 2 périodes → 2 stays = 2 codes).
# Pivot = `canonical_duve` = duve de la résa la plus tôt du groupe. Pour une résa unique
# (99% des cas) : stay = 1 membre, canonical = son duve, fenêtre = sa fenêtre → strictement
# identique à l'ancien comportement (zéro migration). `member_duve_ids` = tous les duve du
# stay (le code est poussé à chacun côté Duve). Résolution duve↔résa par date de CI exacte
# (déterministe, ≠ ancien join (customer,property) ambigu). Réutilise _LOCKS_CTE/_PAYMENTS_CTE.
_STAYS_CTE = f"""
    {_LOCKS_CTE},
    {_PAYMENTS_CTE},
    duve_stay AS (
      SELECT duve_reservation_id, duve_property_id, mews_customer_id, duve_ci
      FROM (
        SELECT duve_reservation_id, duve_property_id,
               primary_guest_external_id AS mews_customer_id, checkin_date AS duve_ci,
               received_at
        FROM `{DUVE_CHECKIN_STG_TABLE}`
        WHERE primary_guest_external_id IS NOT NULL
        UNION ALL
        -- ⭐ Voie rapide du formulaire (07/09, chantier E) : le raw est écrit par le
        -- webhook à la seconde, le staging est INCRÉMENTAL (~2 h). Sans cette
        -- branche, un pré-checkin reçu à 14h ne rattachait sa résa Duve au stay —
        -- donc ne déclenchait le push du code créé à J-3 — qu'à 16h15.
        SELECT reservation_id, property_id,
               (SELECT JSON_VALUE(g, '$.externalId')
                  FROM UNNEST(JSON_QUERY_ARRAY(_raw_payload, '$.resource.guestProfiles')) g
                  WHERE JSON_VALUE(g, '$.isPrimary') = 'true' LIMIT 1),
               DATE(SAFE_CAST(checkin_date AS TIMESTAMP)),
               received_at
        FROM `{RAW_DUVE_CHECKIN_TABLE}`
        WHERE received_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
      )
      WHERE mews_customer_id IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY duve_reservation_id ORDER BY received_at DESC) = 1
    ),
    risk_flags AS (
      SELECT reservation_id,
             LOGICAL_OR(COALESCE(is_fraud_combo, FALSE))  AS fraud_combo,
             -- Groupe jeune (≥ 2 adultes, tous ≤ 25 ans, âges du pré-checkin Duve) —
             -- critère de porte depuis le 07/09 (demande Hatim). Signal FORT de 6.7.
             LOGICAL_OR(COALESCE(f_groupe_jeune, FALSE)) AS young_group
      FROM `{RISK_TABLE}` GROUP BY reservation_id
    ),
    blacklist_flags AS (
      SELECT reservation_id,
             LOGICAL_OR(COALESCE(is_blacklist_confirme, FALSE)
                        AND COALESCE(is_actionnable, FALSE)) AS blacklist_confirmed
      FROM `{BLACKLIST_MATCH_TABLE}` GROUP BY reservation_id
    ),
    member_resas AS (
      SELECT m.reservation_id, m.reservation_number, m.customer_id, m.customer_name,
             m.resource_id, m.checkin_date, m.checkout_date,
             m.earliest_checkin_hour, m.latest_checkout_hour,
             -- Services d'arrivée/départ ACHETÉS (≠ les 2 heures ci-dessus, qui
             -- sont la politique annoncée au pré-checkin).
             m.purchased_early_checkin_hour,
             COALESCE(m.has_purchased_late_checkout, FALSE) AS has_purchased_late_checkout,
             lk.lock_id, lk.lock_tag_id, lk.apartment_code,
             COALESCE(lk.gateway_dead, FALSE)                            AS gateway_dead,
             -- Gate volontaire : on ne provisionne PAS tant qu'une carte a été refusée
             -- et que RIEN n'est encaissé sur le compte (cas VCC Expedia/VRBO non
             -- chargeable avant le jour J). ⚠ Depuis le 15/08 la condition porte sur
             -- l'encaissé du COMPTE, plus sur les paiements de la réservation : un
             -- paiement réussi n'y est jamais rattaché (cf. _PAYMENTS_CTE).
             (COALESCE(pay.n_failed, 0) > 0
              AND COALESCE(pay.amount_charged, 0) <= 0
              AND COALESCE(pay.balance, 0) > 0)                          AS payment_unpaid,
             ROUND(COALESCE(pay.balance, 0), 2)                          AS balance_due,
             -- Signaux de la PORTE (hold). Délai réservation→arrivée en heures, calé sur
             -- 15h le jour du CI (même convention que la mesure du 15/08). Négatif =
             -- réservé après l'heure d'arrivée théorique (cas Defalque, 21h02 pour le soir).
             -- Conservé tel quel pour l'affichage du motif dans le mail de rétention.
             TIMESTAMP_DIFF(TIMESTAMP(DATETIME(m.checkin_date, TIME '15:00:00')),
                            m.created_at, HOUR)                          AS lead_hours,
             -- Les DEUX critères de la porte sont restreints au canal DIRECT (cf.
             -- _evaluate_hold) : les 4 fraudes d'août y sont, et une résa OTA est payée
             -- à l'OTA (moyen de paiement vérifié, recours possible).
             (m.ota_source = 'Site direct'
              AND TIMESTAMP_DIFF(TIMESTAMP(DATETIME(m.checkin_date, TIME '15:00:00')),
                                 m.created_at, HOUR) <= {ISEO_HOLD_LEAD_HOURS}) AS direct_last_minute,
             -- « Solde restant dû » (ex-« rien d'encaissé »). N'a de sens que sur les
             -- canaux où NOUS prenons la carte : une résa Booking/Airbnb est payée à
             -- l'OTA et n'a AUCUN paiement dans Mews → appliqué à tous les canaux, ce
             -- critère retiendrait 69 % des arrivées.
             -- Borné depuis le 15/08 en ancienneté (une résa posée 6 mois avant et pas
             -- encore soldée est un sujet de relance, pas de fraude) et en matérialité
             -- (un reliquat de 75 € n'est pas « sans encaissement »).
             (m.ota_source = 'Site direct'
              AND COALESCE(pay.balance, 0) > {ISEO_HOLD_MIN_BALANCE}
              AND COALESCE(pay.balance, 0)
                  >= {ISEO_HOLD_MIN_BALANCE_RATIO} * NULLIF(pay.amount_due, 0)
              AND TIMESTAMP_DIFF(TIMESTAMP(DATETIME(m.checkin_date, TIME '15:00:00')),
                                 m.created_at, HOUR)
                  <= {ISEO_HOLD_BALANCE_MAX_LEAD_HOURS})                 AS direct_unpaid,
             -- Chantier D (07/09) : 3 critères de plus, tous canaux.
             COALESCE(rk.fraud_combo, FALSE)                              AS fraud_combo,
             COALESCE(rk.young_group, FALSE)                              AS young_group,
             COALESCE(bl.blacklist_confirmed, FALSE)                      AS blacklist_confirmed,
             -- Réservé LE JOUR de l'arrivée (heure Paris), quel que soit le canal :
             -- le critère « jour J » de D. Utile parce que A+B évaluent à la minute.
             (DATE(m.created_at, 'Europe/Paris') = m.checkin_date)        AS same_day_booking
      FROM `{MEWS_FCT_TABLE}` m
      LEFT JOIN locks lk    ON lk.duve_property_id = m.resource_id
      LEFT JOIN payments pay ON pay.reservation_id = m.reservation_id
      LEFT JOIN risk_flags rk      ON rk.reservation_id = m.reservation_id
      LEFT JOIN blacklist_flags bl ON bl.reservation_id = m.reservation_id
      WHERE COALESCE(m.is_cancelled, FALSE) = FALSE
        AND m.checkout_date >= CURRENT_DATE()
    ),
    prev_co AS (
      SELECT *, MAX(checkout_date) OVER (
        PARTITION BY customer_id, resource_id
        ORDER BY checkin_date, reservation_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_checkout
      FROM member_resas
    ),
    islands AS (
      SELECT *, SUM(CAST(prev_checkout IS NULL OR checkin_date > prev_checkout AS INT64)) OVER (
        PARTITION BY customer_id, resource_id
        ORDER BY checkin_date, reservation_id) AS island_id
      FROM prev_co
    ),
    -- Champs stay-level agrégés depuis les membres AVANT d'attacher les duve (l'attache
    -- multiplie les lignes). 1 ligne par (customer, resource, island).
    stay_base AS (
      SELECT customer_id, resource_id, island_id,
        ANY_VALUE(customer_name)  AS customer_name,
        ANY_VALUE(apartment_code) AS apartment_code,
        ANY_VALUE(lock_id)        AS lock_id,
        ANY_VALUE(lock_tag_id)    AS lock_tag_id,
        LOGICAL_OR(gateway_dead)  AS gateway_dead,
        MIN(checkin_date)         AS stay_ci,
        MAX(checkout_date)        AS stay_co,
        ARRAY_AGG(earliest_checkin_hour ORDER BY checkin_date)[SAFE_OFFSET(0)]       AS earliest_checkin_hour,
        ARRAY_AGG(latest_checkout_hour  ORDER BY checkout_date DESC)[SAFE_OFFSET(0)] AS latest_checkout_hour,
        -- ⚠ MIN / LOGICAL_OR et non « la 1re / la dernière résa du stay » : sur un
        -- back-to-back l'early check-in est acheté sur la 1re résa et le late
        -- check-out sur la dernière, mais rien ne garantit que Duve rattache
        -- l'achat au bon membre. Prendre le plus favorable ne fait qu'élargir une
        -- fenêtre déjà payée ; le rater laisse un client à la porte.
        MIN(purchased_early_checkin_hour)                                            AS purchased_early_checkin_hour,
        LOGICAL_OR(has_purchased_late_checkout)                                      AS has_purchased_late_checkout,
        CAST(ARRAY_AGG(reservation_number ORDER BY checkin_date)[SAFE_OFFSET(0)] AS STRING) AS mews_reservation_number,
        ARRAY_AGG(payment_unpaid ORDER BY checkin_date)[SAFE_OFFSET(0)]              AS payment_unpaid,
        -- Pire cas du stay : chaque membre porte l'encaissé du COMPTE, donc sommer
        -- doublonnerait. MAX = le membre le moins soldé.
        MAX(balance_due)                                                             AS balance_due,
        -- Porte agrégée au stay : on prend le membre le PLUS tardivement réservé et on
        -- retient si l'un des membres est impayé. Conservateur par choix — sur un stay
        -- back-to-back, une seule résa suspecte suffit à demander une validation.
        MIN(lead_hours)                                                              AS min_lead_hours,
        LOGICAL_OR(direct_last_minute)                                               AS direct_last_minute,
        LOGICAL_OR(direct_unpaid)                                                    AS direct_unpaid,
        LOGICAL_OR(fraud_combo)                                                      AS fraud_combo,
        LOGICAL_OR(young_group)                                                      AS young_group,
        LOGICAL_OR(blacklist_confirmed)                                              AS blacklist_confirmed,
        LOGICAL_OR(same_day_booking)                                                 AS same_day_booking
      FROM islands
      GROUP BY customer_id, resource_id, island_id
    ),
    -- Attache les duve du (customer, resource) dont le CI tombe dans la fenêtre du stay,
    -- avec tolérance -14j (drift de dates post-pré-checkin : les dates Mews peuvent bouger
    -- après que le guest a rempli Duve → un match par date EXACTE raterait le duve et
    -- archiverait à tort le code actif). La tolérance ≪ écart entre 2 stays d'un même
    -- (customer, resource) → pas de contamination inter-stays.
    stay_duve AS (
      SELECT sb.*, d.duve_reservation_id, d.duve_ci
      FROM stay_base sb
      LEFT JOIN duve_stay d
        ON d.mews_customer_id  = sb.customer_id
       AND d.duve_property_id  = sb.resource_id
       AND d.duve_ci BETWEEN DATE_SUB(sb.stay_ci, INTERVAL 14 DAY) AND sb.stay_co
    ),
    stays AS (
      SELECT
        resource_id            AS duve_property_id,
        customer_name, apartment_code, lock_id, lock_tag_id, gateway_dead,
        stay_ci, stay_co, earliest_checkin_hour, latest_checkout_hour,
        purchased_early_checkin_hour, has_purchased_late_checkout,
        mews_reservation_number, payment_unpaid, balance_due, min_lead_hours,
        direct_last_minute, direct_unpaid, fraud_combo, blacklist_confirmed, same_day_booking,
        young_group,
        ARRAY_AGG(duve_reservation_id IGNORE NULLS ORDER BY duve_ci)                 AS member_duve_ids,
        ARRAY_AGG(duve_reservation_id IGNORE NULLS ORDER BY duve_ci)[SAFE_OFFSET(0)] AS canonical_duve
      FROM stay_duve
      GROUP BY customer_id, resource_id, island_id, duve_property_id, customer_name,
               apartment_code, lock_id, lock_tag_id, gateway_dead, stay_ci, stay_co,
               earliest_checkin_hour, latest_checkout_hour, purchased_early_checkin_hour,
               has_purchased_late_checkout, mews_reservation_number,
               payment_unpaid, balance_due, min_lead_hours, direct_last_minute,
               direct_unpaid, fraud_combo, blacklist_confirmed, same_day_booking, young_group
    )"""


def _resa_to_provision() -> list[dict]:
    """Stays à provisionner : fenêtre CI dans [today-1, today+LOOKAHEAD], CO futur,
    canonical_duve résolu, PAS déjà couvert par une row de cache active — ni par le
    canonical, ni par un member duve (évite un 2e device sur le même stay). Le
    `duve_reservation_id` renvoyé = le canonical (= identité Sofia du stay)."""
    q = f"""
    WITH {_STAYS_CTE},
    active AS (
      SELECT DISTINCT duve_reservation_id
      FROM `{PIN_CACHE_TABLE}`
      WHERE archived_at IS NULL
         -- ⛔ Révocation humaine (6.1 / Vision 360, 06/09, ADR) : la ligne est
         -- archivée (le code n'existe plus chez Sofia) mais le stay est toujours
         -- live — sans cette clause, le run suivant RECRÉERAIT un code à la
         -- personne qu'on vient de couper. Bornée au séjour révoqué (CO ≥ today).
         OR (revoked_at IS NOT NULL AND checkout_date >= CURRENT_DATE())
    )
    SELECT
      -- ⭐ Chantier E (07/09) : sans résa Duve, la clé du stay est `M<n° résa Mews>`
      -- (le n° de la 1re résa du stay). Même clé pour les extIds Sofia. Quand le
      -- formulaire arrive, la ligne garde sa clé M : c'est `_resa_duve_retry` qui
      -- résout les duve du stay en direct et pousse le code.
      COALESCE(s.canonical_duve, CONCAT('M', s.mews_reservation_number)) AS duve_reservation_id,
      (s.canonical_duve IS NULL) AS no_duve,
      s.duve_property_id, s.lock_id, s.lock_tag_id, s.apartment_code, s.gateway_dead,
      s.customer_name, s.mews_reservation_number,
      s.stay_ci AS checkin_date, s.stay_co AS checkout_date,
      s.earliest_checkin_hour, s.latest_checkout_hour,
      s.purchased_early_checkin_hour, s.has_purchased_late_checkout,
      s.payment_unpaid, s.member_duve_ids, s.balance_due,
      s.min_lead_hours, s.direct_last_minute, s.direct_unpaid,
      s.fraud_combo, s.blacklist_confirmed, s.same_day_booking, s.young_group
    FROM stays s
    WHERE s.mews_reservation_number IS NOT NULL
      AND s.stay_ci <= DATE_ADD(CURRENT_DATE(), INTERVAL {LOOKAHEAD_DAYS} DAY)
      -- ⭐ Borne BASSE (25-26/08, ADR) : on ne CRÉE plus un code après le lendemain
      -- de l'arrivée. Tant que le gate paiement bloquait la création, c'est
      -- justement l'absence de borne qui rattrapait les résas débloquées le jour J
      -- (cas Goldwyn 22/08) ; depuis que la création est inconditionnelle, le code
      -- existe depuis J-3 et seul le push Duve reste à décider. En fabriquer un
      -- nouveau en plein séjour n'ajoute qu'un code de plus — donc un élément de
      -- quota Luckey de plus — au client qui a déjà le sien par un autre chemin.
      -- ⚠ Ne borne QUE la création : `_resa_to_resync` part du cache et doit
      -- continuer à tourner tout le séjour (drift de dates, early CI / late CO
      -- achetés après coup).
      AND s.stay_ci >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND s.stay_co >= CURRENT_DATE()
      AND NOT EXISTS (SELECT 1 FROM active a WHERE a.duve_reservation_id = s.canonical_duve)
      AND NOT EXISTS (SELECT 1 FROM UNNEST(s.member_duve_ids) md
                      JOIN active a ON a.duve_reservation_id = md)
      -- Le stay a déjà son code créé SANS formulaire (clé M) : quand le Duve arrive,
      -- canonical_duve se remplit mais la ligne active reste keyée M — sans ce
      -- test, on fabriquerait un 2ᵉ code au même séjour.
      AND NOT EXISTS (SELECT 1 FROM active a
                      WHERE a.duve_reservation_id = CONCAT('M', s.mews_reservation_number))
    ORDER BY s.stay_ci
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


def _wallet_used() -> Optional[int]:
    """Éléments Luckey utilisés au dernier snapshot (None si illisible → pas de garde)."""
    try:
        rows = list(_bq().query(
            f"SELECT num_elements_used FROM `{WALLET_TABLE}` LIMIT 1").result())
        return int(rows[0].num_elements_used) if rows else None
    except Exception as e:
        logger.warning(f"⚠️ wallet illisible ({e}) — garde quota inactive")
        return None


def _whitelisted_gaps() -> list[dict]:
    """Trous silencieux : résas whitelistées à provisionner (CI ∈ [J-1, J+lookahead],
    CO ≥ today, non annulées) SANS row cache active, classées par cause :
      - lock       : serrure non résolue (anormal sur whitelist, tout horizon)
      - precheckin : pas de mapping Duve = formulaire pas rempli (bruit auto-résolu
                     avant J-1 → n'alerte qu'à CI ≤ J+1)
      - paiement   : gate volontaire — tous les paiements Failed, aucun Charged
      - autre      : provisionnable en apparence mais toujours pas de code = le vrai
                     signal « aurait dû être généré », à investiguer
    Sans ce filet une résa sans mapping Duve est effacée par le INNER JOIN de
    _resa_to_provision (ni skip ni erreur) → guest sans code sans aucun signal."""
    if not ALLOWED_PROPERTY_IDS:
        return []
    q = f"""
    WITH {_DUVE_LATEST_CTE},
    {_LOCKS_CTE},
    {_PAYMENTS_CTE},
    active_by_mews AS (
      SELECT DISTINCT mews_reservation_number
      FROM `{PIN_CACHE_TABLE}`
      WHERE (archived_at IS NULL
             -- Code révoqué par un humain (06/09) : l'absence de code est VOULUE,
             -- le mail « aurait dû être généré » n'a rien à en dire.
             OR (revoked_at IS NOT NULL AND checkout_date >= CURRENT_DATE()))
        AND mews_reservation_number IS NOT NULL
    )
    SELECT m.reservation_number, m.resource_id, m.customer_name, m.checkin_date,
           m.checkout_date, lk.apartment_code,
           CASE
             WHEN lk.lock_id IS NULL THEN 'lock'
             -- ⚠ AVANT 'precheckin' et 'paiement' : quand la passerelle est morte, la
             -- cause du « pas de code » est celle-là et aucune autre — le formulaire ou
             -- le paiement n'y changeraient rien. La classer plus bas la ferait passer
             -- pour un bruit qui se résout tout seul.
             WHEN COALESCE(lk.gateway_dead, FALSE) THEN 'gateway'
             WHEN d.duve_reservation_id IS NULL THEN 'precheckin'
             -- ⚠ 'paiement' ne devrait plus JAMAIS sortir depuis le 25/08 : le gate
             -- qui empêchait la création a été absorbé par la porte, donc une résa
             -- impayée reçoit désormais son code comme les autres et n'est plus un
             -- « trou ». La branche est conservée comme DÉTECTEUR DE RÉGRESSION —
             -- si elle se remet à compter, c'est qu'un blocage a été réintroduit
             -- quelque part avant la création. Ne pas la supprimer sans avoir
             -- vérifié qu'elle est bien à zéro depuis plusieurs jours.
             WHEN COALESCE(pay.n_failed, 0) > 0
              AND COALESCE(pay.amount_charged, 0) <= 0
              AND COALESCE(pay.balance, 0) > 0 THEN 'paiement'
             ELSE 'autre'
           END AS reason
    FROM `{MEWS_FCT_TABLE}` m
    LEFT JOIN duve_latest d ON d.mews_customer_id = m.customer_id
                           AND d.duve_property_id = m.resource_id
    LEFT JOIN locks lk       ON lk.duve_property_id = m.resource_id
    LEFT JOIN payments pay   ON pay.reservation_id = m.reservation_id
    LEFT JOIN active_by_mews am
      ON am.mews_reservation_number = CAST(m.reservation_number AS STRING)
    WHERE LOWER(m.resource_id) IN UNNEST(@wl)
      AND m.checkin_date <= DATE_ADD(CURRENT_DATE(), INTERVAL {LOOKAHEAD_DAYS} DAY)
      -- Même borne basse que `_resa_to_provision` (26/08) : l'alerte promet « un code
      -- aurait dû être généré », ce qui n'est vrai que dans la fenêtre où la création
      -- a lieu. Au-delà, la machine ne tentera plus rien et répéter le constat chaque
      -- jour n'apprend rien à personne — c'est le défaut « état par défaut accusateur ».
      -- Le séjour en cours sans code reste visible en 6.1 (`pin_state`, qui porte les
      -- séjours commencés depuis le 24/08). ⚠ Fenêtre de rattrapage humain = J et J+1.
      AND m.checkin_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND m.checkout_date >= CURRENT_DATE()
      AND COALESCE(m.is_cancelled, FALSE) = FALSE
      AND am.mews_reservation_number IS NULL
      -- precheckin avant J-1 = bruit qui se résout tout seul (vérifié 15/07) →
      -- exclu du mail. Les 3 autres causes alertent à tout horizon ≤ lookahead.
      AND NOT (lk.lock_id IS NOT NULL
               AND d.duve_reservation_id IS NULL
               AND m.checkin_date > DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY CAST(m.reservation_number AS STRING)
      ORDER BY m.checkin_date) = 1
    ORDER BY CASE reason WHEN 'autre' THEN 0 WHEN 'gateway' THEN 1 WHEN 'lock' THEN 2
                         WHEN 'paiement' THEN 3 ELSE 4 END, m.checkin_date
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("wl", "STRING", sorted(ALLOWED_PROPERTY_IDS))])
    return [dict(r.items()) for r in _bq().query(q, job_config=cfg).result()]


def _resa_to_archive() -> list[dict]:
    """Rows actives à archiver : plus aucun stay live ne contient le duve du cache.
    `member_resas` exclut les annulées et les CO < today → un duve absent de TOUT stay
    = stay fini (CO passé) ou annulé → à archiver. DELETE device + invitation + user.
    Match par appartenance (member_duve_ids) : robuste au décalage de canonical si la
    résa la plus tôt d'un stay est annulée."""
    q = f"""
    WITH {_STAYS_CTE},
    stay_members AS (
      SELECT md AS duve_reservation_id FROM stays, UNNEST(member_duve_ids) md
      UNION ALL
      -- Chantier E : la ligne keyée M<n°> est vivante tant que son stay Mews l'est.
      SELECT CONCAT('M', mews_reservation_number) FROM stays
      WHERE mews_reservation_number IS NOT NULL
    )
    SELECT c.duve_reservation_id, c.iseo_invitation_id, c.shadow_mode
    FROM `{PIN_CACHE_TABLE}` c
    LEFT JOIN stay_members sm ON sm.duve_reservation_id = c.duve_reservation_id
    WHERE c.archived_at IS NULL AND sm.duve_reservation_id IS NULL
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


def _resa_to_resync() -> list[dict]:
    """Rows actives dont la fenêtre cache ≠ fenêtre live du stay = drift post-provision
    (extension / raccourcissement / décalage OU fusion back-to-back : une nouvelle résa
    contiguë étend le stay) OU invitation manquante. Recrée device (même PIN) + invitation
    sur la fenêtre live. Match cache↔stay par canonical_duve (déterministe, 1:1)."""
    q = f"""
    WITH {_STAYS_CTE},
    cache AS (
      SELECT duve_reservation_id, pin_value, iseo_device_id, iseo_invitation_id,
             iseo_guest_tag_id, iseo_lock_id, iseo_lock_tag_id,
             mews_reservation_number, apartment_code, hold_reason, released_at,
             duve_pushed_at,
             checkin_date AS cache_ci, checkout_date AS cache_co
      FROM `{PIN_CACHE_TABLE}`
      WHERE archived_at IS NULL AND provisioned_at IS NOT NULL
    ),
    -- Fenêtre RÉELLEMENT posée en serrure, telle que la voit le snapshot ISEO
    -- (fraîcheur ~2 h ETL + ~2 h dbt). Sert UNIQUEMENT à repérer des candidats :
    -- `_resync` re-lit le credential en direct avant d'écrire quoi que ce soit,
    -- donc une ligne déjà corrigée mais encore périmée au snapshot ne produit
    -- qu'un GET sans effet.
    cred AS (
      SELECT duve_reservation_id, active_from, active_to
      FROM `{ISEO_DEVICES_STG_TABLE}`
      WHERE duve_reservation_id IS NOT NULL AND pin_origin = 'merveil_dwh'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY duve_reservation_id ORDER BY snapshot_at DESC) = 1
    )
    SELECT
      c.duve_reservation_id, c.pin_value, c.iseo_device_id, c.iseo_invitation_id,
      c.iseo_guest_tag_id, c.iseo_lock_id, c.iseo_lock_tag_id,
      c.cache_ci, c.cache_co, c.hold_reason AS cache_hold, c.released_at AS cache_released,
      (c.duve_pushed_at IS NOT NULL) AS cache_pushed,
      c.mews_reservation_number, c.apartment_code,
      s.duve_property_id, s.customer_name,
      s.stay_ci AS live_ci, s.stay_co AS live_co,
      s.earliest_checkin_hour, s.latest_checkout_hour,
      s.purchased_early_checkin_hour, s.has_purchased_late_checkout, s.member_duve_ids,
      s.min_lead_hours, s.direct_last_minute, s.direct_unpaid, s.balance_due,
      s.fraud_combo, s.blacklist_confirmed, s.same_day_booking, s.young_group,
      -- ⛔⛔ `payment_unpaid` MANQUAIT ICI depuis que le paiement est devenu un critère
      -- de la porte (25/08) : ce SELECT n'avait pas suivi. `_evaluate_hold` lisait donc
      -- `row.get("payment_unpaid")` = None à chaque resync, et le critère paiement
      -- DISPARAISSAIT — un simple resync levait une rétention pour impayé, sans trace.
      -- Mesuré le 09/09 : Anne Elizabeth Harris (52172), retenue pour 4 500 € impayés,
      -- a été libérée à 10h01 par le resync déclenché par son achat d'early check-in à
      -- 255 €. Acheter un service suffisait donc à contourner la porte. C'est la même
      -- faille que celle documentée pour les dates (réserver à J+5, avancer à J-0), sur
      -- un autre critère.
      s.payment_unpaid
    FROM cache c
    JOIN stays s ON s.canonical_duve = c.duve_reservation_id
                 OR c.duve_reservation_id = CONCAT('M', s.mews_reservation_number)
    LEFT JOIN cred cr ON cr.duve_reservation_id = c.duve_reservation_id
    WHERE s.stay_ci != c.cache_ci OR s.stay_co != c.cache_co OR c.iseo_invitation_id IS NULL
       -- ⭐ Drift d'HEURES : un service d'arrivée/départ acheté APRÈS le
       -- provisioning (J-3) ne déplace AUCUNE date — les 3 conditions ci-dessus
       -- sont aveugles à ce cas, qui est précisément le plus fréquent (l'achat
       -- se fait quand l'arrivée approche). On compare donc l'heure achetée à
       -- l'heure réellement posée en serrure.
       OR (s.purchased_early_checkin_hour IS NOT NULL
           AND cr.active_from IS NOT NULL
           AND FORMAT_TIME('%H:%M', TIME(cr.active_from, 'Europe/Paris'))
               > s.purchased_early_checkin_hour)
       OR (s.has_purchased_late_checkout
           AND cr.active_to IS NOT NULL
           AND FORMAT_TIME('%H:%M', TIME(cr.active_to, 'Europe/Paris')) < '{LATE_CO_HOUR}')
    ORDER BY s.stay_ci
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


# ── State writers ───────────────────────────────────────────────────────────

def _save_provisioned(row: dict, pin_value: str, device_id: int,
                      inv_id: Optional[int], inv_code: Optional[str],
                      link: Optional[str], duve_ok: bool,
                      member_csv: Optional[str] = None,
                      hold_reason: Optional[str] = None) -> None:
    q = f"""
    INSERT INTO `{PIN_CACHE_TABLE}` (
      duve_reservation_id, mews_reservation_number, apartment_code, pin_value,
      iseo_guest_tag_id, iseo_lock_id, iseo_lock_tag_id, iseo_device_id,
      iseo_invitation_id, invitation_code, invitation_link,
      checkin_date, checkout_date, cached_at, provisioned_at, duve_pushed_at,
      shadow_mode, stay_member_duve_ids, hold_reason, held_at)
    VALUES (@duve, @num, @apt, @pin, @gtag, @lock, @ltag, @dev,
            @inv, @code, @link, @ci, @co, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
            {'CURRENT_TIMESTAMP()' if duve_ok else 'NULL'}, FALSE, @members, @hold,
            {'CURRENT_TIMESTAMP()' if hold_reason else 'NULL'})
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("duve", "STRING", row["duve_reservation_id"]),
        bigquery.ScalarQueryParameter("num", "STRING", row.get("mews_reservation_number")),
        bigquery.ScalarQueryParameter("apt", "STRING", row.get("apartment_code")),
        bigquery.ScalarQueryParameter("pin", "STRING", pin_value),
        bigquery.ScalarQueryParameter("gtag", "INT64", row.get("guest_tag_id")),
        bigquery.ScalarQueryParameter("lock", "INT64", row.get("lock_id")),
        bigquery.ScalarQueryParameter("ltag", "INT64", row.get("lock_tag_id")),
        bigquery.ScalarQueryParameter("dev", "INT64", device_id),
        bigquery.ScalarQueryParameter("inv", "INT64", inv_id),
        bigquery.ScalarQueryParameter("code", "STRING", inv_code),
        bigquery.ScalarQueryParameter("link", "STRING", link),
        bigquery.ScalarQueryParameter("ci", "DATE", str(row["checkin_date"])),
        bigquery.ScalarQueryParameter("co", "DATE", str(row["checkout_date"])),
        bigquery.ScalarQueryParameter("members", "STRING", member_csv),
        bigquery.ScalarQueryParameter("hold", "STRING", hold_reason),
    ])
    _bq().query(q, job_config=cfg).result()


def _save_resynced(duve_resa_id: str, ci: str, co: str, device_id: object,
                   inv_id: Optional[int], inv_code: Optional[str],
                   link: Optional[str], duve_ok: bool,
                   member_csv: Optional[str] = None,
                   hold_reason: Optional[str] = None) -> None:
    # `held_at` n'est posé qu'à la PREMIÈRE rétention (COALESCE) : un resync
    # successif ne doit pas rajeunir l'ancienneté d'une rétention en attente.
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET checkin_date = @ci, checkout_date = @co, iseo_device_id = @dev,
        iseo_invitation_id = @inv, invitation_code = @code, invitation_link = @link,
        provisioned_at = CURRENT_TIMESTAMP(),
        duve_pushed_at = {'CURRENT_TIMESTAMP()' if duve_ok else 'NULL'},
        stay_member_duve_ids = @members,
        hold_reason = @hold,
        held_at = {'COALESCE(held_at, CURRENT_TIMESTAMP())' if hold_reason else 'NULL'},
        last_error = NULL
    WHERE duve_reservation_id = @id AND archived_at IS NULL
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", duve_resa_id),
        bigquery.ScalarQueryParameter("ci", "DATE", ci),
        bigquery.ScalarQueryParameter("co", "DATE", co),
        bigquery.ScalarQueryParameter("dev", "INT64", device_id),
        bigquery.ScalarQueryParameter("inv", "INT64", inv_id),
        bigquery.ScalarQueryParameter("code", "STRING", inv_code),
        bigquery.ScalarQueryParameter("link", "STRING", link),
        bigquery.ScalarQueryParameter("members", "STRING", member_csv),
        bigquery.ScalarQueryParameter("hold", "STRING", hold_reason),
    ])
    _bq().query(q, job_config=cfg).result()


def _resa_duve_retry() -> list[dict]:
    """Rows provisionnées côté Sofia mais dont le push Duve a échoué
    (`duve_pushed_at IS NULL`) → à re-pousser (code + lien déjà en cache).

    ⚠ EXCLUT les rétentions volontaires. Une ligne retenue a exactement la même
    signature qu'un push raté (`provisioned_at` rempli, `duve_pushed_at` NULL) :
    sans ce filtre, le retry enverrait au client, au run suivant, le code que la
    porte vient de retenir — la porte serait silencieusement inopérante. Une
    rétention libérée (`released_at` posé) redevient éligible et part au run d'après.
    """
    q = f"""
    WITH {_STAYS_CTE},
    cache AS (
      -- ⭐ Depuis le 07/09 les RETENUES sont incluses (plus de filtre hold_reason) :
      -- la porte est ré-évaluée à chaque run et une rétention dont TOUS les critères
      -- sont levés (paiement encaissé, fiche blacklist levée…) est libérée seule.
      SELECT duve_reservation_id, pin_value, invitation_link, stay_member_duve_ids,
             mews_reservation_number, hold_reason, released_at
      FROM `{PIN_CACHE_TABLE}`
      WHERE archived_at IS NULL AND provisioned_at IS NOT NULL AND duve_pushed_at IS NULL
    )
    -- ⭐ Chantier E : les duve du stay sont résolus EN DIRECT (une ligne keyée M n'en
    -- avait aucun à la création). Dès que le formulaire arrive, `stays` porte le
    -- duve → push au run suivant (≤ 10 min, ou 2-3 min via l'event-driven).
    SELECT c.duve_reservation_id, c.pin_value, c.invitation_link, c.stay_member_duve_ids,
           c.mews_reservation_number, c.hold_reason AS cache_hold, c.released_at AS cache_released,
           ANY_VALUE(s.member_duve_ids)      AS live_member_duve_ids,
           -- Signaux de la porte, pour la RÉ-ÉVALUER à l'arrivée du formulaire sur une
           -- clé M : le pré-checkin apporte des signaux (âges → groupe jeune, nom de la
           -- pièce → combo fraude) que la création à J-3 n'avait pas.
           ANY_VALUE(s.customer_name)        AS customer_name,
           ANY_VALUE(s.apartment_code)       AS apartment_code,
           ANY_VALUE(s.duve_property_id)     AS duve_property_id,
           ANY_VALUE(s.stay_ci)              AS checkin_date,
           ANY_VALUE(s.stay_co)              AS checkout_date,
           ANY_VALUE(s.payment_unpaid)       AS payment_unpaid,
           ANY_VALUE(s.balance_due)          AS balance_due,
           ANY_VALUE(s.min_lead_hours)       AS min_lead_hours,
           ANY_VALUE(s.direct_last_minute)   AS direct_last_minute,
           ANY_VALUE(s.direct_unpaid)        AS direct_unpaid,
           ANY_VALUE(s.fraud_combo)          AS fraud_combo,
           ANY_VALUE(s.blacklist_confirmed)  AS blacklist_confirmed,
           ANY_VALUE(s.same_day_booking)     AS same_day_booking,
           ANY_VALUE(s.young_group)          AS young_group
    FROM cache c
    LEFT JOIN stays s
      ON s.canonical_duve = c.duve_reservation_id
      OR c.duve_reservation_id = CONCAT('M', s.mews_reservation_number)
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    """
    return [dict(r.items()) for r in _bq().query(q).result()]


def _mark_released(duve_resa_id: str, by: str) -> None:
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET released_at = CURRENT_TIMESTAMP(), released_by = @by
    WHERE duve_reservation_id = @id AND archived_at IS NULL
    """
    _bq().query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", duve_resa_id),
        bigquery.ScalarQueryParameter("by", "STRING", by)])).result()


def _mark_held(duve_resa_id: str, hold_reason: str, member_csv: Optional[str] = None,
               reset_release: bool = False) -> None:
    """Rétention posée APRÈS la création (clé M dont le formulaire vient d'arriver).

    ⚠ `reset_release` (09/09) : efface `released_at`/`released_by` et repart sur un
    `held_at` neuf. À poser UNIQUEMENT quand le formulaire révèle un motif que la RC
    n'a pas pu acquitter en cliquant [Livrer]. Sans lui, une ligne portait à la fois
    `hold_reason` et `released_at` : l'overlay 6.1 (`held_at IS NOT NULL AND
    released_at IS NULL`) ne la voyait plus comme `retenu` mais comme `attente_form`,
    le bouton [Livrer] disparaissait du 360 (même condition), et la boucle de retry
    la re-jugeait à chaque run — un mail toutes les 10 min jusqu'au check-out.
    """
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET hold_reason = @hold,
        held_at = {'CURRENT_TIMESTAMP()' if reset_release else 'COALESCE(held_at, CURRENT_TIMESTAMP())'},
        {'released_at = NULL, released_by = NULL,' if reset_release else ''}
        stay_member_duve_ids = COALESCE(@members, stay_member_duve_ids)
    WHERE duve_reservation_id = @id AND archived_at IS NULL
    """
    _bq().query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", duve_resa_id),
        bigquery.ScalarQueryParameter("hold", "STRING", hold_reason),
        bigquery.ScalarQueryParameter("members", "STRING", member_csv)])).result()


def _mark_duve_pushed(duve_resa_id: str, member_csv: Optional[str] = None) -> None:
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET duve_pushed_at = CURRENT_TIMESTAMP(), last_error = NULL,
        stay_member_duve_ids = COALESCE(@members, stay_member_duve_ids)
    WHERE duve_reservation_id = @id AND archived_at IS NULL
    """
    _bq().query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", duve_resa_id),
        bigquery.ScalarQueryParameter("members", "STRING", member_csv)])).result()


def _mark_archived(duve_resa_id: str, error: Optional[str] = None) -> None:
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET archived_at = CURRENT_TIMESTAMP(), last_error = @err
    WHERE duve_reservation_id = @id AND archived_at IS NULL
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id", "STRING", duve_resa_id),
        bigquery.ScalarQueryParameter("err", "STRING", error),
    ])
    _bq().query(q, job_config=cfg).result()


# ── Window calc (Paris tz) ────────────────────────────────────────────────────

def _hm_to_min(h: Optional[str]) -> Optional[int]:
    try:
        hh, mm = h[:5].split(":")
        return int(hh) * 60 + int(mm)
    except Exception:
        return None


WINDOW_FLOOR_CI = "07:00"
WINDOW_CEIL_CO = "23:59"


def _min_hm(*hours: Optional[str]) -> Optional[str]:
    """La plus TÔT des heures fournies (NULL ignorés). Sert à faire entrer l'early
    check-in ACHETÉ dans le même calcul que l'heure de politique annoncée."""
    vals = [h[:5] for h in hours if h and _hm_to_min(h) is not None]
    return min(vals, key=lambda h: _hm_to_min(h)) if vals else None


def _max_hm(*hours: Optional[str]) -> Optional[str]:
    vals = [h[:5] for h in hours if h and _hm_to_min(h) is not None]
    return max(vals, key=lambda h: _hm_to_min(h)) if vals else None


def _stay_hours(row: dict) -> tuple[str, str]:
    """Fenêtre horaire du séjour : min(politique, early check-in acheté, 13:00) →
    max(politique, 18:00 si late check-out acheté, 11:00).

    ⭐ La 2e entrée est la correction du 25/08 : jusque-là seul
    `earliest_checkin_hour` était lu — une heure de POLITIQUE Duve, jamais l'heure
    NÉGOCIÉE. Un client ayant payé 140 € pour entrer à 9 h avait un code qui
    n'ouvrait qu'à 13 h (12 cas sur 15 mesurés sur les apparts intégrés).
    ⚠ La fenêtre ne peut que s'ÉLARGIR : le plancher 07:00 et le plafond 23:59
    tiennent, et un early check-in acheté APRÈS 13 h ne repousse pas l'ouverture."""
    ci = _earliest_hour(
        _min_hm(row.get("earliest_checkin_hour"), row.get("purchased_early_checkin_hour")),
        DEFAULT_CI_HOUR)
    co = _latest_hour(
        _max_hm(row.get("latest_checkout_hour"),
                LATE_CO_HOUR if row.get("has_purchased_late_checkout") else None),
        DEFAULT_CO_HOUR)
    return ci, co


def _earliest_hour(policy: Optional[str], default: str) -> str:
    p, d = _hm_to_min(policy), _hm_to_min(default)
    chosen = default if p is None else (policy[:5] if p <= d else default)
    return WINDOW_FLOOR_CI if _hm_to_min(chosen) < _hm_to_min(WINDOW_FLOOR_CI) else chosen


def _latest_hour(policy: Optional[str], default: str) -> str:
    p, d = _hm_to_min(policy), _hm_to_min(default)
    chosen = default if p is None else (policy[:5] if p >= d else default)
    return WINDOW_CEIL_CO if _hm_to_min(chosen) > _hm_to_min(WINDOW_CEIL_CO) else chosen


def _build_window_ms(ci_date: str, co_date: str, ci_hour: str, co_hour: str) -> tuple[int, int]:
    ci_h, ci_m = (ci_hour[:5].split(":") + ["00"])[:2]
    co_h, co_m = (co_hour[:5].split(":") + ["00"])[:2]
    cy, cmo, cd = (int(x) for x in ci_date.split("-"))
    oy, omo, od = (int(x) for x in co_date.split("-"))
    ci_dt = datetime(cy, cmo, cd, int(ci_h), int(ci_m), tzinfo=PARIS_TZ)
    co_dt = datetime(oy, omo, od, int(co_h), int(co_m), tzinfo=PARIS_TZ)
    return int(ci_dt.timestamp() * 1000), int(co_dt.timestamp() * 1000)


# ── Guest user dédié (un par résa, au vrai nom) ────────────────────────────────

def _split_name(name: Optional[str]) -> tuple[str, str]:
    parts = (name or "").strip().split()
    if not parts:
        return "Merveil", "Guest"
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def _user_tag_id(user: dict) -> Optional[int]:
    """Le tag de type 'user' auto-créé avec le user (= le seul valide comme guestTagId)."""
    for t in (user.get("tags") or []):
        if t.get("type") == "user" and t.get("id"):
            return t["id"]
    return None


def _get_or_create_guest_user(duve_resa_id: str, guest_name: Optional[str]) -> tuple[Optional[int], Optional[str]]:
    """Get-or-create (par extId) un user Sofia dédié à la résa, au vrai nom du guest.
    Retourne (guest_tag_id, error). Le user est créé avec un password aléatoire pour
    être enabled=True (sinon il est GC / perd son tag). Le password n'est jamais
    partagé — le guest ouvre au PIN clavier + lien remote-open."""
    ext = f"MERVEIL_USER - {duve_resa_id}"
    g = _sofia("GET", f"/api/v2/users/extId/{ext}")
    if g.status_code == 200:
        tag = _user_tag_id(g.json())
        return (tag, None) if tag else (None, "user existant sans tag 'user'")
    if g.status_code != 404:  # 401/500… ≠ absent : ne PAS créer un doublon
        return None, f"user GET HTTP {g.status_code}"

    fn, ln = _split_name(guest_name)
    email = f"resa-{duve_resa_id}@guest.archides.fr"  # unique par résa, jamais utilisé
    r = _sofia("POST", "/api/v2/users", json_body={
        "username": email, "email": email, "password": "Mv!" + secrets.token_urlsafe(16),
        "firstname": fn, "lastname": ln, "roleIds": [5], "extId": ext})
    if r.status_code not in (200, 201):
        return None, f"user POST HTTP {r.status_code}: {r.text[:200]}"
    body = r.json()
    if body.get("enabled") is False:
        # Contrat empirique password→enabled : si Sofia le change, le user sera GC
        # en minutes (perte du tag) → PIN cassé. On le signale au lieu de subir.
        logger.warning(f"⚠️ user {ext} créé enabled=False (comportement password→enabled changé ?)")
    tag = _user_tag_id(body)
    if tag is None:  # fallback : relire le user pour récupérer son tag
        g2 = _sofia("GET", f"/api/v2/users/extId/{ext}")
        tag = _user_tag_id(g2.json()) if g2.status_code == 200 else None
    if tag is None:
        return None, "user créé sans tag 'user'"
    logger.info(f"✅ user guest créé '{(fn + ' ' + ln).strip()}' → tag {tag}")
    return tag, None


# ── Provision (A→E) ───────────────────────────────────────────────────────────

def _evaluate_hold(row: dict) -> Optional[str]:
    """Motif de rétention du code, ou None si le code peut partir chez le client.

    ⭐ **UN SEUL ORGANE RETIENT** (refonte 25/08, cf. ADR). Le paiement était un
    `skip` de `_provision` évalué AVANT cette fonction : il empêchait la CRÉATION du
    code, donc la porte n'était jamais atteinte et la RC devait fabriquer un code à
    la main le jour de l'arrivée (cas Liliana Goldwyn, 22/08). Désormais le code est
    créé quoi qu'il arrive et le paiement n'est plus qu'un critère de rétention parmi
    les autres — seul le push Duve est retenu, jamais la création.

    ⚠ **Conséquence de la fusion : `ISEO_HOLD_MODE=off` désactive AUSSI le contrôle
    paiement.** C'est le prix d'un organe unique et c'est voulu — mais le passer à
    `off` ne se fait plus « juste pour couper la porte fraude ».

    Trois critères. Les deux premiers restreints au canal DIRECT (recalibrage 15/08) :
      - réservé ≤ ISEO_HOLD_LEAD_HOURS (72 h) avant l'arrivée — ~12 résas/mois, c'est
        le critère qui porte la valeur : les 4 fraudes d'août sont toutes en direct,
        3 réservées le jour même et la 4ᵉ (Bossongo) à J-2 ;
      - solde restant dû > ISEO_HOLD_MIN_BALANCE, sur une résa posée ≤
        ISEO_HOLD_BALANCE_MAX_LEAD_HOURS avant l'arrivée (~6/mois, dont 2 déjà prises
        par le critère précédent).
    Le troisième, TOUS CANAUX (ex-gate `payment_unpaid`) :
      - carte refusée ET rien d'encaissé sur le compte ET solde positif — typiquement
        une VCC Expedia/VRBO non chargeable avant le jour J.

    ⚠ Ce troisième critère est le SEUL applicable hors direct, et c'est sûr parce
    qu'il exige une **tentative refusée** (`n_failed > 0`) : une résa Booking/Airbnb
    payée à l'OTA n'a aucun paiement dans Mews, donc ne le déclenche jamais. Ne PAS
    le confondre avec `direct_unpaid`, qui se lit sur le solde seul et retiendrait
    69 % des arrivées s'il était généralisé.

    ⚠ « Retenu paiement » et « retenu fraude » sont deux gestes RC OPPOSÉS (relancer
    l'encaissement vs vérifier une identité) — d'où le motif explicite dans le mail
    et dans `hold_decisions.hold_reason`.

    ⚠ Ce second critère s'appelait « rien d'encaissé » et se lisait sur les paiements
    de la RÉSERVATION → il sonnait sur des séjours intégralement payés (cf. le piège
    documenté dans _PAYMENTS_CTE) et sur des réservations vieilles de 6 mois. Recalculé
    au compte payeur et borné en ancienneté le 15/08. À garder en tête : les 4 fraudes
    d'août avaient toutes PAYÉ (les pertes sont des chargebacks) — le paiement n'est
    pas un signal de fraude, c'est un signal de créance. Le critère qui protège
    vraiment est le premier.

    ⚠ Le critère last-minute était initialement TOUS CANAUX (≤24 h). Restreint au
    direct le 15/08 : mesuré, 87 % des résas du jour même sont des OTA, payées à
    l'OTA (moyen de paiement vérifié, recours possible) et absentes des 4 fraudes —
    retenir leur code, c'est un client dehors le soir pour un gain de sécurité nul.

    ⚠ Volontairement PAS conditionné à « pièce d'identité scannée » : scanner une
    pièce coûte 30 secondes à un fraudeur (n'importe quel document passe l'OCR, et
    un cas de fausse pièce est avéré côté Merveil), donc en faire une condition de
    libération rendrait la porte contournable par une action que l'attaquant
    contrôle. La pièce sert au triage humain au moment de libérer.
    """
    if ISEO_HOLD_MODE == "off":
        return None
    motifs = _hold_motifs(row)
    return " + ".join(label for _, label in motifs) if motifs else None


# Libellés de base des motifs. Le texte affiché y ajoute un détail variable
# (« (2570 € dus) », « (48h avant l'arrivée) ») — d'où la séparation clé / libellé :
# comparer deux rétentions se fait sur les CLÉS, jamais sur les libellés. Sans ça,
# « paiement refusé, rien d'encaissé (2570 € dus) » et le même motif à 2 400 € en
# paraîtraient deux, et une libération acquittée serait perdue au moindre centime.
_HOLD_LABELS = {
    "direct_last_minute":  "direct réservé au dernier moment",
    "direct_unpaid":       "direct avec solde restant dû",
    "payment_unpaid":      "paiement refusé, rien d'encaissé",
    "fraud_combo":         "combo fraude (≥2 signaux, cf. 6.7)",
    "blacklist_confirmed": "client blacklisté (rapprochement confirmé)",
    "same_day_booking":    "réservé le jour de l'arrivée",
    "young_group":         "groupe jeune (≥ 2 adultes ≤ 25 ans)",
}


def _hold_motifs(row: dict) -> list[tuple[str, str]]:
    """[(clé, libellé affiché)] des motifs qui retiennent CE séjour, dans l'ordre.

    Sépare l'évaluation (ici) de son rendu en chaîne (`_evaluate_hold`) : la boucle de
    retry a besoin des clés pour distinguer un motif NOUVEAU d'un motif déjà acquitté
    par la RC au moment où elle a cliqué [Livrer] (cf. `_run_inner`, 09/09).
    ⚠ Ne pas trier : l'ordre porte la hiérarchie de lecture du mail de rétention.
    """
    motifs: list[tuple[str, str]] = []
    if row.get("direct_last_minute"):
        lead = row.get("min_lead_hours")
        detail = f" ({int(lead)}h avant l'arrivée)" if lead is not None else ""
        motifs.append(("direct_last_minute",
                       f"{_HOLD_LABELS['direct_last_minute']}{detail}"))
    if row.get("direct_unpaid"):
        bal = row.get("balance_due")
        detail = f" ({bal:.0f} € restants)" if bal is not None else ""
        motifs.append(("direct_unpaid", f"{_HOLD_LABELS['direct_unpaid']}{detail}"))
    if row.get("payment_unpaid"):
        bal = row.get("balance_due")
        detail = f" ({bal:.0f} € dus)" if bal is not None else ""
        motifs.append(("payment_unpaid", f"{_HOLD_LABELS['payment_unpaid']}{detail}"))
    # Chantier D (07/09, décision Hatim) — 3 critères de plus, tous canaux. Les deux
    # premiers sont des verdicts dbt déjà calibrés (backtest 19/08 · rapprochement
    # exact/fort 29/08), le 3ᵉ est le « jour J » que A+B rendent évaluable à la minute.
    for key in ("fraud_combo", "blacklist_confirmed", "same_day_booking", "young_group"):
        if row.get(key):
            motifs.append((key, _HOLD_LABELS[key]))
    return motifs


def _motif_keys(hold_reason: Optional[str]) -> set[str]:
    """Clés des motifs portés par un `hold_reason` déjà écrit en base.

    Le cache ne stocke que la chaîne ; on retrouve les clés par préfixe de libellé —
    pas de colonne `hold_keys`, donc pas de DDL. Un libellé inconnu (motif retiré du
    code depuis) ne matche rien : il sera vu comme « nouveau » et retiendra, ce qui
    est le sens prudent.
    """
    parts = [p.strip() for p in (hold_reason or "").split(" + ") if p.strip()]
    return {key for part in parts
            for key, base in _HOLD_LABELS.items() if part.startswith(base)}


def _log_hold_decision(row: dict, motif: str, phase: str, outcome: str) -> None:
    """Journalise UNE décision de la porte dans `iseo_raw.hold_decisions` (append-only).

    ⚠ Pourquoi une table à part et pas `hold_reason` dans le cache : en mode `observe`
    le cache ne porte PAS de `hold_reason` (« retenu » y garde un sens strict), et on ne
    peut pas l'y écrire sans casser `_resa_duve_retry`, qui filtre précisément dessus —
    un push Duve réellement raté ne serait alors plus jamais retenté. Cette table capture
    donc ce que le cache ne peut pas dire : les décisions en `observe`, et celles dont la
    résa est ensuite skippée (whitelist/lock) et qui n'envoient aucun mail.

    ⚠ `skipped:paiement` a disparu des `outcome` possibles le 25/08 — le paiement est
    devenu un critère de la porte, plus un skip. Les lignes historiques le portent
    encore ; ne pas les lire comme des rétentions, c'étaient des non-créations.

    Best-effort : une écriture ratée ne doit jamais faire échouer un provisioning.
    """
    try:
        q = f"""
        INSERT INTO `{HOLD_DECISIONS_TABLE}` (
          evaluated_at, phase, hold_mode, outcome,
          duve_reservation_id, duve_property_id, apartment_code, customer_name,
          mews_reservation_number, checkin_date, checkout_date,
          hold_reason, direct_last_minute, direct_unpaid, payment_unpaid,
          min_lead_hours, hold_lead_hours_setting, balance_due)
        VALUES (CURRENT_TIMESTAMP(), @phase, @mode, @outcome,
          @duve, @pid, @apt, @name, @resa,
          SAFE_CAST(@ci AS DATE), SAFE_CAST(@co AS DATE),
          @reason, @dlm, @du, @pu, @lead, @setting, @balance)
        """
        params = [
            bigquery.ScalarQueryParameter("phase", "STRING", phase),
            bigquery.ScalarQueryParameter("mode", "STRING", ISEO_HOLD_MODE),
            bigquery.ScalarQueryParameter("outcome", "STRING", outcome),
            bigquery.ScalarQueryParameter("duve", "STRING", row.get("duve_reservation_id")),
            bigquery.ScalarQueryParameter("pid", "STRING", row.get("duve_property_id")),
            bigquery.ScalarQueryParameter("apt", "STRING", row.get("apartment_code")),
            bigquery.ScalarQueryParameter("name", "STRING", row.get("customer_name")),
            bigquery.ScalarQueryParameter("resa", "STRING",
                                          row.get("mews_reservation_number")),
            # provision porte checkin_date/checkout_date, resync live_ci/live_co
            bigquery.ScalarQueryParameter(
                "ci", "STRING", str(row.get("checkin_date") or row.get("live_ci") or "") or None),
            bigquery.ScalarQueryParameter(
                "co", "STRING", str(row.get("checkout_date") or row.get("live_co") or "") or None),
            bigquery.ScalarQueryParameter("reason", "STRING", motif),
            bigquery.ScalarQueryParameter("dlm", "BOOL", bool(row.get("direct_last_minute"))),
            bigquery.ScalarQueryParameter("du", "BOOL", bool(row.get("direct_unpaid"))),
            bigquery.ScalarQueryParameter("pu", "BOOL", bool(row.get("payment_unpaid"))),
            bigquery.ScalarQueryParameter(
                "lead", "INT64",
                int(row["min_lead_hours"]) if row.get("min_lead_hours") is not None else None),
            bigquery.ScalarQueryParameter("setting", "INT64", ISEO_HOLD_LEAD_HOURS),
            bigquery.ScalarQueryParameter(
                "balance", "FLOAT64",
                float(row["balance_due"]) if row.get("balance_due") is not None else None),
        ]
        _bq().query(q, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    except Exception as e:
        logger.warning(f"⚠️ hold_decisions : écriture échouée — {type(e).__name__}: {e}")


def _hold_already_notified(duve_resa_id: str) -> bool:
    """La RC a-t-elle DÉJÀ été prévenue d'une rétention sur cette réservation ?

    ⚠ Garde-fou anti-boucle d'alerte, posé le 25/08 avec la fusion du gate paiement.
    Le resync se garde de re-notifier via `row['cache_hold']` — mais en mode
    `observe` le cache ne porte JAMAIS de `hold_reason` (cf. `_log_hold_decision`),
    donc ce garde-fou est inopérant et chaque resync renvoyait le mail. Ça ne se
    voyait pas tant que la porte ne retenait que ~12 résas directes/mois ; en y
    versant le paiement (tous canaux), le resync devient un émetteur régulier.

    On interroge donc le journal, seule trace qui survit au mode `observe`. Coût :
    une petite query, uniquement pour les résas effectivement retenues et non déjà
    marquées en cache — quelques-unes par run au plus.

    Best-effort volontairement ASYMÉTRIQUE : en cas d'erreur on renvoie False, donc
    on notifie. Rater une alerte de rétention coûte un client devant une porte ;
    envoyer un doublon coûte un mail.
    """
    try:
        q = f"""
        SELECT 1 FROM `{HOLD_DECISIONS_TABLE}`
        WHERE duve_reservation_id = @duve AND hold_reason IS NOT NULL
        LIMIT 1
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("duve", "STRING", duve_resa_id)])
        return len(list(_bq().query(q, job_config=cfg).result())) > 0
    except Exception as e:
        logger.warning(f"⚠️ hold déjà notifié ? query KO — {type(e).__name__}: {e}")
        return False


def _notify_hold(row: dict, motif: str, suffix: str = "",
                 retenu: Optional[bool] = None) -> None:
    """Prévient la RC qu'une résa est jugée à risque par la porte.

    ⚠ Envoyé dans les modes `on` ET `observe` (décision 15/08) : toute rétention
    DOIT être doublée d'une alerte, sinon la porte transforme une fraude évitée en
    client dehors à 22 h — et en `observe`, sans ce mail, personne n'apprenait
    qu'une résa avait été jugée à risque. Le mail dit explicitement, dans chaque
    mode, si le client a le code ou non : ce sont deux gestes RC opposés.
    """
    # ⚠ `retenu` force la variante du mail quand le mode ne suffit plus à la décrire :
    # au resync d'un code DÉJÀ livré, la porte n'a rien retenu même en mode `on`
    # (cf. `_resync`) — annoncer « code retenu à valider » enverrait la RC cliquer
    # [Livrer] sur un code que le client a déjà. Le bon geste est vérifier / révoquer.
    effectif = (ISEO_HOLD_MODE == "on") if retenu is None else retenu
    apt = row.get("apartment_code") or row.get("duve_property_id")
    # Le provision porte checkin_date/checkout_date, le resync live_ci/live_co.
    ci = row.get("checkin_date") or row.get("live_ci")
    co = row.get("checkout_date") or row.get("live_co")
    if effectif:
        titre, sujet = "Code d'accès retenu — à valider", "🔒 Code retenu à valider"
        etat = ("Le code a été <strong>créé côté serrure mais volontairement pas envoyé</strong> "
                "au client : il ne le voit pas dans son application.")
        suite = ("Après vérification (identité, paiement), cliquer <strong>Livrer le code</strong> "
                 "en 6.1 : il part dans la Guest App immédiatement. En cas de doute, "
                 "<strong>Révoquer</strong> et faire annuler la réservation. Le code reste "
                 "lisible en 6.1 si le client appelle.")
    elif row.get("no_duve"):
        titre, sujet = "Réservation à risque — code créé, non transmis", "⚠️ Résa à risque (sans pré-checkin)"
        etat = ("Le client n'a <strong>pas rempli son pré-checkin</strong> : le code existe côté "
                "serrure et en 6.1, mais <strong>rien ne lui a été envoyé</strong>. Il partira "
                "seul s'il remplit le formulaire.")
        suite = ("Vérifier l'identité si le client se présente sans avoir rempli le formulaire. "
                 "En cas de doute, ne pas dicter le code et faire annuler la réservation.")
    else:
        titre, sujet = "Réservation à risque — code déjà envoyé", "⚠️ Résa à risque (code envoyé)"
        etat = ("La porte de validation est en <strong>mode observation</strong> : le code "
                "<strong>a bien été envoyé au client</strong>, il peut entrer.")
        suite = ("Vérifier l'identité du client. En cas de doute, faire annuler la "
                 "réservation et changer le code de l'appartement avant l'arrivée.")
    html = build_email(
        titre,
        subtitle=f"{row.get('customer_name')} · {apt}{suffix}",
        severity="warning",
        intro=f"{etat}<br><strong>Motif :</strong> {esc(motif)}",
        table={"headers": ["Client", "Appartement", "Séjour", "Résa Mews"],
               "rows": [[esc(row.get("customer_name")), esc(apt),
                         f"{esc(ci)} → {esc(co)}",
                         esc(row.get("mews_reservation_number"))]]},
        sections_html=('<div style="padding:0 24px 8px;font-size:14px;color:#475569">'
                       f"{suite}</div>"),
        button=("Voir les arrivées →",
                "https://direction.archides.fr/ops-front?tab=arrivees"))
    send_mail(f"{sujet} — {row.get('customer_name')} ({apt})",
              html, ISEO_HOLD_ALERT_TO, html=True, sender=GMAIL_SENDER)


def _provision(row: dict) -> tuple[bool, Optional[str]]:
    duve_resa_id = row["duve_reservation_id"]

    apt_pid = (row.get("duve_property_id") or "").lower()
    if ALLOWED_PROPERTY_IDS and apt_pid not in ALLOWED_PROPERTY_IDS:
        return False, "skipped: whitelist"
    if row.get("lock_tag_id") is None or row.get("lock_id") is None:
        return False, "skipped: lock non résolue"
    # ⚠ NE PAS RÉINTRODUIRE DE GATE DE JUGEMENT ICI (retiré le 25/08, cf. ADR).
    # Il y avait `if row.get("payment_unpaid"): return False, "skipped: paiement non
    # validé"`. Il court-circuitait la porte : aucun code n'était créé, donc rien à
    # lire au dashboard, et la RC en fabriquait un à la main le jour de l'arrivée.
    # Le paiement est désormais un critère de `_evaluate_hold` — seul le push Duve
    # est retenu. Les seuls refus qui restent ici sont des PRÉREQUIS TECHNIQUES
    # (whitelist, serrure non résolue, dates absentes, checkout passé) : sans eux
    # l'appel Sofia n'a pas de sens. Tout ce qui relève d'un jugement métier va dans
    # la porte, sinon on se retrouve avec deux organes qui retiennent et un seul qui
    # alerte.

    ci_date = row.get("checkin_date")
    co_date = row.get("checkout_date")
    if ci_date is None or co_date is None:
        return False, "missing CI/CO date"
    ci_str, co_str = str(ci_date), str(co_date)
    ci_hour, co_hour = _stay_hours(row)
    ci_ms, co_ms = _build_window_ms(ci_str, co_str, ci_hour, co_hour)
    if co_ms < int(time.time() * 1000):
        return False, "skipped: checkout déjà passé (sera archivé demain)"

    win = {"from": ci_ms, "to": co_ms}
    pin_ext = f"MERVEIL_RESA - {duve_resa_id}"
    inv_ext = f"MERVEIL_INV - {duve_resa_id}"

    logger.info(
        f"→ provision {duve_resa_id} ({row.get('apartment_code')}) "
        f"window={datetime.fromtimestamp(ci_ms/1000, timezone.utc).isoformat()} → "
        f"{datetime.fromtimestamp(co_ms/1000, timezone.utc).isoformat()}")

    if ISEO_SHADOW_MODE:
        logger.info(f"🌗 SHADOW {duve_resa_id}: would provision (skip Sofia/Duve/state)")
        return True, None

    # User dédié à la résa (vrai nom du guest) → son tag 'user' = guest tag du PIN.
    tag_id, uerr = _get_or_create_guest_user(duve_resa_id, row.get("customer_name"))
    if tag_id is None:
        return False, f"guest user creation failed: {uerr}"
    row["guest_tag_id"] = tag_id

    # A+B. device (get-or-create par extId → idempotent sur retry partiel)
    pin_value, device_id = _get_or_create_device(row, pin_ext, win)
    if pin_value is None:
        return False, f"device creation failed: {device_id}"  # device_id porte l'erreur

    # C. invitation (get-or-create)
    inv_id, inv_code = _get_or_create_invitation(row, inv_ext, win)
    link = f"https://{REMOTE_OPEN_HOST}/remoteOpen?code={inv_code}" if inv_code else None

    # D. Duve push (code clavier + lien) — à TOUS les duve du stay (back-to-back),
    #    SAUF si la porte retient : le code existe alors côté Sofia (donc lisible au
    #    dashboard et révocable) mais le client ne le voit pas.
    members = [] if row.get("no_duve") else (row.get("member_duve_ids") or [duve_resa_id])
    hold = row.get("hold_reason")  # posé par le caller (déjà évalué pour le log)
    if hold and ISEO_HOLD_MODE == "on":
        duve_ok, duve_err = False, None
        logger.warning(f"🔒 HOLD {duve_resa_id} ({row.get('apartment_code')}) — {hold} "
                       f"→ code créé, PAS envoyé à Duve")
    elif not members:
        # Chantier E : pas de pré-checkin → rien où pousser. Le code existe (6.1 le
        # porte, la RC peut le dicter) et partira seul à l'arrivée du formulaire.
        duve_ok, duve_err = False, None
        logger.info(f"📝 {duve_resa_id} ({row.get('apartment_code')}, {row.get('customer_name')}) "
                    f"— code créé SANS pré-checkin, push Duve à l'arrivée du formulaire")
    else:
        duve_ok, duve_err = _duve_push_all(members, pin_value, link or "")
        if not duve_ok:
            logger.warning(f"⚠️ Duve push failed for {duve_resa_id}: {duve_err}")

    # E. état
    _save_provisioned(row, pin_value, device_id, inv_id, inv_code, link, duve_ok,
                      member_csv=",".join(members),
                      hold_reason=hold if ISEO_HOLD_MODE == "on" else None)

    # E bis. Le code natif de CETTE résa devient un doublon à l'instant précis où le
    # nôtre existe et part chez le client. On le retire ici, et pas en purge groupée :
    # tant que nous n'avons rien poussé, le code natif est ce que la Guest App affiche,
    # donc le SEUL code du client (constat 18/08 : 12 codes natifs correspondaient à des
    # séjours réels à venir, dont 3 sur des appartements non intégrés où nous ne
    # provisionnerons jamais — les supprimer en masse mettait ces clients dehors).
    # Ici la substitution est atomique : nouveau code écrit dans Duve → ancien supprimé.
    for duve_id in members:
        _purge_native_duplicate(duve_id)
    if hold:
        _notify_hold(row, hold)  # `observe` compris — cf. docstring de _notify_hold
    if hold and ISEO_HOLD_MODE == "on":
        return True, None  # rétention volontaire : ce n'est PAS une erreur de run
    if not members:
        return True, None  # code créé sans formulaire : pas une erreur non plus
    if not duve_ok:
        return False, f"Sofia OK mais Duve KO: {duve_err}"
    return True, None


def _device_payload(row: dict, pin_ext: str, win: dict) -> dict:
    """Payload POST /standardDevices sans deviceId (= le code, ajouté par _post_device)."""
    return {
        "type": "ISEO_PIN", "extId": pin_ext, "notes": pin_ext,
        "validationMode": "ONE_HOUR_VALIDATION", "validationPeriod": 24,
        "additionalCredentialRules": [],
        "credentialRule": {
            "name": pin_ext, "description": "merveil_dwh_v3",
            "lockTagIds": [int(row["lock_tag_id"])], "lockTagMatchingMode": "AT_LEAST_ONE_TAG",
            "guestTagIds": [int(row["guest_tag_id"])], "guestTagMatchingMode": "EVERY_TAG",
            "daysOfTheWeek": [1, 2, 3, 4, 5, 6, 7],
            "dateInterval": win, "timeInterval": {"from": 0, "to": 86340},
            "alwaysOpen": False, "holidays": True, "openOnPrivacy": False},
    }


def _post_device(row: dict, pin_ext: str, win: dict,
                 pin_value: Optional[str] = None) -> tuple[Optional[str], object]:
    """POST un device. Si pin_value fourni (resync) on tente de réutiliser le même
    code (libéré par le DELETE qui précède) ; sinon on génère un code 4 chiffres
    unique account-wide (retry sur collision)."""
    payload = _device_payload(row, pin_ext, win)
    if pin_value is not None:
        payload["deviceId"] = pin_value
        r = _sofia("POST", "/api/v2/standardDevices", json_body=payload)
        if r.status_code in (200, 201):
            return pin_value, r.json().get("id")
        if "already present" not in r.text.lower():
            return None, f"HTTP {r.status_code}: {r.text[:200]}"
        logger.warning(f"⚠️ code {pin_value} repris entre-temps → régénération")
    last_err = None
    for _ in range(PIN_COLLISION_RETRIES):
        pv = f"{secrets.randbelow(10000):04d}"
        payload["deviceId"] = pv
        r = _sofia("POST", "/api/v2/standardDevices", json_body=payload)
        if r.status_code in (200, 201):
            logger.info(f"✅ device créé id={r.json().get('id')} PIN={pv}")
            return pv, r.json().get("id")
        if "already present" in r.text.lower():
            last_err = "code collision"
            continue
        return None, f"HTTP {r.status_code}: {r.text[:200]}"
    return None, last_err or "no free PIN"


def _get_or_create_device(row: dict, pin_ext: str, win: dict) -> tuple[Optional[str], object]:
    """Retourne (pin_value, device_id). Réutilise le device existant (même extId)
    s'il existe (retry partiel). Sinon génère un code 4 chiffres unique."""
    g = _sofia("GET", f"/api/v2/standardDevices/extId/{pin_ext}")
    if g.status_code == 200:
        d = g.json()
        return str(d.get("deviceId")), d.get("id")
    if g.status_code != 404:  # ≠ absent : ne pas créer un doublon device
        return None, f"device GET {g.status_code}: {g.text[:120]}"
    return _post_device(row, pin_ext, win)


def _put_device_window(dev: dict, win: dict) -> tuple[bool, Optional[str]]:
    """Déplace la fenêtre d'un device EXISTANT, en place. Validé en prod le 21/08
    (cas LaBrash) et re-vérifié le 25/08 en no-op : HTTP 201, même enregistrement,
    même PIN.

    ⭐ Pourquoi PUT plutôt que le DELETE + re-POST historique : le code ne
    disparaît jamais, même une seconde. Le delete+recreate ouvre une fenêtre de
    lockout si le POST échoue — et il n'a d'intérêt que quand le device n'existe
    pas encore.

    ⚠ Asymétrie du DTO Sofia : l'entrée attend `lockTagIds`/`guestTagIds`
    (SINGULIER « Tag »), alors que le GET renvoie les tags en objets
    `lockTags[].id` / `guestTags[].id` et laisse `lockTagsIds`/`guestTagsIds`
    (pluriel) à NULL. Reconstruire depuis les objets, pas depuis les champs ids."""
    cr = dev.get("credentialRule") or {}
    body = {
        "type": dev.get("type"), "extId": dev.get("extId"), "notes": dev.get("notes"),
        "deviceId": dev.get("deviceId"),
        "validationMode": dev.get("validationMode"),
        "validationPeriod": dev.get("validationPeriod"),
        "additionalCredentialRules": [],
        "credentialRule": {
            "name": cr.get("name"), "description": cr.get("description"),
            "lockTagIds": [t["id"] for t in (cr.get("lockTags") or [])],
            "lockTagMatchingMode": cr.get("lockTagMatchingMode"),
            "guestTagIds": [t["id"] for t in (cr.get("guestTags") or [])],
            "guestTagMatchingMode": cr.get("guestTagMatchingMode"),
            "daysOfTheWeek": cr.get("daysOfTheWeek"),
            "dateInterval": win, "timeInterval": cr.get("timeInterval"),
            "alwaysOpen": cr.get("alwaysOpen"), "holidays": cr.get("holidays"),
            "openOnPrivacy": cr.get("openOnPrivacy")},
    }
    r = _sofia("PUT", f"/api/v2/standardDevices/{dev.get('id')}", json_body=body)
    if r.status_code in (200, 201):
        return True, None
    return False, f"PUT device {r.status_code}: {r.text[:200]}"


def _put_invitation_window(inv: dict, win: dict) -> tuple[bool, Optional[str]]:
    """Déplace la fenêtre d'une invitation existante. ⭐ Le CODE est conservé
    (vérifié 25/08) — donc le lien remote-open déjà parti dans un message Duve
    reste valide. Le DELETE + recréation, lui, tuait ce lien : les messages Duve
    envoyés sont figés, le client gardait un lien mort."""
    body = {
        "name": inv.get("name"), "extId": inv.get("extId"),
        "smartLockIds": [l["id"] for l in (inv.get("smartLocks") or [])],
        "daysOfTheWeek": inv.get("daysOfTheWeek"),
        "dateInterval": win, "timeInterval": inv.get("timeInterval"),
        "numberOfDevices": inv.get("numberOfDevices") or 0,
    }
    r = _sofia("PUT", f"/api/v2/invitations/{inv.get('id')}", json_body=body)
    if r.status_code in (200, 201):
        return True, None
    return False, f"PUT invitation {r.status_code}: {r.text[:200]}"


def _get_or_create_invitation(row: dict, inv_ext: str, win: dict) -> tuple[Optional[int], Optional[str]]:
    g = _sofia("GET", f"/api/v2/invitations/extId/{inv_ext}")
    if g.status_code == 200:
        d = g.json()
        return d.get("id"), d.get("code")
    if g.status_code != 404:  # ≠ absent : ne pas créer un doublon invitation
        logger.warning(f"⚠️ invitation GET {inv_ext}: HTTP {g.status_code}")
        return None, None
    r = _sofia("POST", "/api/v2/invitations", json_body={
        "name": inv_ext, "extId": inv_ext, "smartLockIds": [int(row["lock_id"])],
        "daysOfTheWeek": [1, 2, 3, 4, 5, 6, 7],
        "dateInterval": win, "timeInterval": {"from": 0, "to": 86340},
        "numberOfDevices": 0})
    if r.status_code in (200, 201):
        d = r.json()
        logger.info(f"✅ invitation id={d.get('id')} code={d.get('code')}")
        return d.get("id"), d.get("code")
    logger.warning(f"⚠️ invitation KO {inv_ext}: HTTP {r.status_code} {r.text[:200]}")
    return None, None


def _archive(row: dict) -> tuple[bool, Optional[str]]:
    """DELETE Sofia device (par extId) + DELETE invitation (par id) + DELETE le user
    dédié de la résa (par extId)."""
    duve_resa_id = row["duve_reservation_id"]
    if bool(row.get("shadow_mode")):
        # Row provisionnée en shadow (aucun device Sofia) → mark archived OK, rien à supprimer.
        logger.info(f"🌗 SHADOW row {duve_resa_id}: archive (pas d'appel Sofia)")
        return True, None
    if ISEO_SHADOW_MODE:
        # Shadow GLOBAL sur une row LIVE : NE PAS marquer archived (sinon le PIN Sofia
        # n'est jamais supprimé mais la row est figée → code valide résiduel). On laisse
        # la row active pour qu'elle soit réellement archivée dès que shadow repasse off.
        logger.info(f"🌗 SHADOW global {duve_resa_id}: would archive (row live laissée active)")
        return False, "skipped: shadow global (row live)"

    errs = []
    # device (à supprimer avant le user — il l'ancre)
    g = _sofia("GET", f"/api/v2/standardDevices/extId/MERVEIL_RESA - {duve_resa_id}")
    if g.status_code == 200:
        sid = g.json().get("id")
        rd = _sofia("DELETE", f"/api/v2/standardDevices/{sid}")
        if rd.status_code not in (200, 204):
            errs.append(f"device DELETE {rd.status_code}")
    elif g.status_code != 404:
        errs.append(f"device GET {g.status_code}")
    # invitation
    inv_id = row.get("iseo_invitation_id")
    if inv_id:
        ri = _sofia("DELETE", f"/api/v2/invitations/{int(inv_id)}")
        if ri.status_code not in (200, 204, 404):
            errs.append(f"inv DELETE {ri.status_code}")
    # user dédié (sinon accumulation de users guest)
    gu = _sofia("GET", f"/api/v2/users/extId/MERVEIL_USER - {duve_resa_id}")
    if gu.status_code == 200:
        ru = _sofia("DELETE", f"/api/v2/users/{gu.json().get('id')}")
        if ru.status_code not in (200, 204, 404):
            errs.append(f"user DELETE {ru.status_code}")
    elif gu.status_code != 404:
        errs.append(f"user GET {gu.status_code}")
    if errs:
        return False, "; ".join(errs)
    logger.info(f"🗑️ archived {duve_resa_id} (stay terminé / annulé)")
    return True, None


def _resync(row: dict) -> tuple[bool, Optional[str]]:
    """Resync window Sofia après drift de dates : DELETE device+invitation puis
    re-POST avec la window live + le MÊME code PIN (le guest garde son code clavier ;
    le lien remote-open change car nouvelle invitation). UPDATE l'état cache."""
    duve_resa_id = row["duve_reservation_id"]
    apt_pid = (row.get("duve_property_id") or "").lower()
    if ALLOWED_PROPERTY_IDS and apt_pid not in ALLOWED_PROPERTY_IDS:
        return False, "skipped: whitelist"

    ci_str, co_str = str(row["live_ci"]), str(row["live_co"])
    ci_hour, co_hour = _stay_hours(row)
    ci_ms, co_ms = _build_window_ms(ci_str, co_str, ci_hour, co_hour)
    if co_ms < int(time.time() * 1000):
        return False, "skipped: checkout passé (sera archivé)"
    win = {"from": ci_ms, "to": co_ms}

    if ISEO_SHADOW_MODE:
        logger.info(f"🌗 SHADOW {duve_resa_id}: would resync window → {ci_str}→{co_str}")
        return True, None

    # Adapter les clés cache → clés attendues par les helpers partagés.
    row["lock_tag_id"] = row.get("iseo_lock_tag_id")
    row["lock_id"] = row.get("iseo_lock_id")
    # Guest tag = tag 'user' du user dédié de la résa (get-or-create idempotent).
    tag_id, uerr = _get_or_create_guest_user(duve_resa_id, row.get("customer_name"))
    if tag_id is None:
        return False, f"resync guest user failed: {uerr}"
    row["guest_tag_id"] = tag_id
    if row["lock_tag_id"] is None or row["lock_id"] is None:
        return False, "resync impossible: ids appart manquants en cache"

    pin_ext = f"MERVEIL_RESA - {duve_resa_id}"
    inv_ext = f"MERVEIL_INV - {duve_resa_id}"

    # ⭐ La fenêtre cible se compare à ce que Sofia porte RÉELLEMENT, pas au cache :
    # le cache ne stocke que les DATES, jamais les heures. Un early check-in acheté
    # après le provisioning (le cas LaBrash : acheté à J-2, code posé à J-3) ne
    # déplace aucune date — il ne serait jamais rattrapé par une comparaison de cache.
    g = _sofia("GET", f"/api/v2/standardDevices/extId/{pin_ext}")
    if g.status_code not in (200, 404):
        return False, f"resync device GET {g.status_code}"
    dev = g.json() if g.status_code == 200 else None
    live_win = ((dev or {}).get("credentialRule") or {}).get("dateInterval") or {}
    window_drift = dev is not None and (
        int(live_win.get("from") or 0) != ci_ms or int(live_win.get("to") or 0) != co_ms)

    if dev is None:
        # Device absent côté Sofia (supprimé à la main, ou POST jamais abouti) :
        # seul cas qui justifie un re-POST. Même code PIN si possible.
        pin_value, device_id = _post_device(row, pin_ext, win, pin_value=row.get("pin_value"))
        if pin_value is None:
            return False, f"resync device re-POST failed: {device_id}"
    elif window_drift:
        logger.info(
            f"↔️ window drift {duve_resa_id} ({row.get('apartment_code')}) → "
            f"{ci_str} {ci_hour} → {co_str} {co_hour}"
            + (f" [early check-in acheté {row.get('purchased_early_checkin_hour')}]"
               if row.get("purchased_early_checkin_hour") else "")
            + (" [late check-out acheté]" if row.get("has_purchased_late_checkout") else ""))
        pin_value, device_id = str(dev.get("deviceId")), dev.get("id")
        ok, perr = _put_device_window(dev, win)
        if not ok:
            # Repli sur l'ancien chemin (DELETE + re-POST). Il rouvre la fenêtre de
            # lockout que le PUT évite, mais mieux vaut ça qu'une fenêtre périmée.
            logger.warning(f"⚠️ PUT device KO ({perr}) → repli delete+recreate")
            rd = _sofia("DELETE", f"/api/v2/standardDevices/{dev.get('id')}")
            if rd.status_code not in (200, 204):
                return False, f"resync device DELETE {rd.status_code}"
            pin_value, device_id = _post_device(row, pin_ext, win, pin_value=row.get("pin_value"))
            if pin_value is None:
                return False, f"resync device re-POST failed: {device_id}"
    else:
        # Fenêtre déjà bonne : on est ici pour réparer une invitation manquante.
        pin_value, device_id = str(dev.get("deviceId")), dev.get("id")

    # Invitation : même logique. ⭐ Le PUT conserve le CODE, donc le lien
    # remote-open déjà parti dans un message Duve (figé) reste valide — le
    # delete+recreate le tuait à chaque resync.
    gi = _sofia("GET", f"/api/v2/invitations/extId/{inv_ext}")
    inv = gi.json() if gi.status_code == 200 else None
    inv_id = inv_code = None
    if inv is not None:
        inv_win = inv.get("dateInterval") or {}
        if int(inv_win.get("from") or 0) != ci_ms or int(inv_win.get("to") or 0) != co_ms:
            ok, ierr = _put_invitation_window(inv, win)
            if not ok:
                logger.warning(f"⚠️ PUT invitation KO ({ierr}) → repli delete+recreate")
                ri = _sofia("DELETE", f"/api/v2/invitations/{inv.get('id')}")
                if ri.status_code not in (200, 204, 404):
                    return False, f"resync invitation DELETE {ri.status_code}"
                inv = None
        if inv is not None:
            inv_id, inv_code = inv.get("id"), inv.get("code")
    if inv_id is None:
        inv_id, inv_code = _get_or_create_invitation(row, inv_ext, win)
    link = f"https://{REMOTE_OPEN_HOST}/remoteOpen?code={inv_code}" if inv_code else None

    # 3. Duve push (code identique, lien neuf) — à tous les duve du stay.
    #    ⚠ La porte est RÉ-ÉVALUÉE ici, sur les dates LIVE. Sinon : réserver à J+5
    #    (la porte laisse passer), recevoir le code à J-3, puis avancer les dates à
    #    aujourd'hui — le resync repousserait le code sans aucun contrôle. Une
    #    rétention déjà libérée à la main n'est pas re-fermée (released_at présent).
    members = [m for m in (row.get("member_duve_ids") or []) if m]
    if not members and not duve_resa_id.startswith("M"):
        members = [duve_resa_id]
    row["no_duve"] = not members
    hold = None
    if not row.get("cache_released"):
        hold = _evaluate_hold(row)

    # ⛔⛔ UNE FOIS LE CODE LIVRÉ, LA PORTE NE PEUT PLUS RIEN RETENIR (09/09).
    # La porte retient le PUSH DUVE, pas le code. Or au resync le PUT conserve le
    # MÊME PIN et le MÊME code d'invitation : ce qu'on pousserait est identique, au
    # caractère près, à ce que le champ Duve porte déjà. Retenir revient donc à ne
    # pas réécrire une valeur inchangée — zéro effet pour le client, qui garde un
    # code valide sur la fenêtre que l'étape 2 vient d'élargir côté Sofia.
    # ⚠ Ne PAS justifier ça par « le message Duve est figé » : à terme le message ne
    # portera que le lien vers la Guest App, qui lit le champ en direct (Hatim,
    # 09/09) — c'est bien l'égalité des valeurs qui rend la rétention inopérante,
    # pas l'immuabilité du message.
    # Retenir ne faisait donc que deux dégâts : (a) `_save_resynced(duve_ok=False)`
    # remet `duve_pushed_at` à NULL, donc 6.1 affiche « retenu » sur un client qui a
    # son code et l'a peut-être déjà utilisé ; (b) un mail « Livrer le code » part
    # pour un code déjà livré. Le geste qui agit sur un code sorti est **Révoquer**
    # (point 10), pas la porte.
    # ⚠ SEULE EXCEPTION connue, et elle plaide pour pousser, pas pour retenir : si le
    # PUT échoue et que `_resync` retombe sur DELETE + re-POST, l'invitation change de
    # code donc le LIEN change. Ne pas pousser laisserait alors un lien mort dans le
    # champ Duve — raison de plus pour ne pas retenir ici.
    # ⚠ Mesuré le 09/09 en ajoutant `payment_unpaid` au resync : 1 résa concernée
    # (Matilda Reaburn, 37118, SEN18-2G, Expedia VCC impayée, EN SÉJOUR jusqu'au 10/09).
    deja_livre = bool(row.get("cache_pushed"))
    # `live_ci` est la date de début du stay : le séjour a-t-il commencé ?
    en_sejour = (str(row.get("live_ci") or "9999-12-31")
                 <= datetime.now(PARIS_TZ).date().isoformat())
    if hold and deja_livre:
        # ⚠ On JOURNALISE quand même : le signal est réel (fraude, créance) et doit
        # rester mesurable — c'est seulement la rétention qui n'a plus de prise.
        _log_hold_decision(row, hold, "resync", "skipped: code déjà livré")
        if en_sejour:
            # Le client est dans l'appartement. « Vérifier avant l'arrivée » n'a plus
            # d'objet, et aucun geste de la porte ne le fera sortir → pas de mail.
            # Une fraude avérée sur un séjour en cours passe par 6.7 / la révocation.
            logger.info(f"⏭️ resync {duve_resa_id} ({row.get('apartment_code')}) — "
                        f"critères réunis ({hold}) mais code livré ET séjour commencé : "
                        f"ni rétention ni mail")
        elif not _hold_already_notified(duve_resa_id):
            # Séjour à venir : le code est parti, mais la RC doit savoir. Mail dans sa
            # variante « code déjà envoyé » — elle porte le bon geste (vérifier
            # l'identité, faire annuler, changer le code), pas « Livrer le code ».
            _notify_hold(row, hold, suffix=" — code déjà envoyé, à vérifier", retenu=False)
        hold = None

    if hold and ISEO_HOLD_MODE == "on":
        duve_ok, duve_err = False, None
        logger.warning(f"🔒 HOLD au resync {duve_resa_id} ({row.get('apartment_code')}) — "
                       f"{hold} → nouveau lien PAS envoyé à Duve")
    elif not members:
        duve_ok, duve_err = False, None  # clé M sans formulaire : rien où pousser
    else:
        duve_ok, duve_err = _duve_push_all(members, pin_value, link or "")

    # 4. état
    _save_resynced(duve_resa_id, ci_str, co_str, device_id, inv_id, inv_code, link, duve_ok,
                   member_csv=",".join(members),
                   hold_reason=hold if ISEO_HOLD_MODE == "on" else None)
    # ⚠ ORDRE IMPORTANT : la question « a-t-on déjà prévenu ? » se pose AVANT
    # d'écrire la décision de ce run, sinon la query voit la ligne qu'on vient de
    # poser et le mail ne part JAMAIS. `cache_hold` ne suffit pas en mode `observe`
    # (le cache n'y porte pas de `hold_reason`) — sans ce second garde-fou la RC
    # reçoit le même mail à chaque resync. Cf. `_hold_already_notified`.
    deja_notifie = bool(row.get("cache_hold")) or (
        bool(hold) and _hold_already_notified(duve_resa_id))
    if hold:
        _log_hold_decision(row, hold, "resync",
                           "held" if ISEO_HOLD_MODE == "on" else "pushed_observe")
    if hold and not deja_notifie:  # nouvelle rétention née du changement de dates
        _notify_hold(row, hold, suffix=" — après modification des dates")
    if hold and ISEO_HOLD_MODE == "on":
        logger.info(f"🔄 resync {duve_resa_id} window → {ci_str}→{co_str} (code retenu)")
        return True, None
    if not duve_ok and duve_err is not None:
        return False, f"resync Sofia OK mais Duve KO: {duve_err}"
    logger.info(f"🔄 resync {duve_resa_id} window → {ci_str}→{co_str}")
    return True, None


# ── Purge des DUVE_PIN natifs orphelins ────────────────────────────────────────

def _native_duve_pins_to_purge() -> list[dict]:
    """DUVE_PIN natifs encore vivants en Sofia alors que la résa est annulée ou
    déjà checked-out, SUR LES APPARTS CUTOVER UNIQUEMENT (whitelist). L'intégration
    native est coupée → plus personne ne supprime ces PIN à l'annulation/au départ,
    laissant un code valide à un guest qui ne devrait plus entrer.

    ⚠️ Scope STRICT à la whitelist : sur les ~120 apparts non cutover, le DUVE_PIN
    natif reste l'UNIQUE code du guest — ne JAMAIS purger en dehors de la whitelist.
    On ne touche pas non plus les résas ACTIVE (leur DUVE_PIN double notre code mais
    reste l'unique code des résas >J-3 pas encore provisionnées) — nettoyées au CO."""
    if not ALLOWED_PROPERTY_IDS:
        return []
    q = f"""
    WITH {_DUVE_LATEST_CTE},
    dpin AS (
      SELECT ext_id, duve_reservation_id, user_firstname, user_lastname, active_from
      FROM `{STD_DEVICES_TABLE}`
      WHERE ext_id LIKE 'DUVE_PIN - %' AND deleted = FALSE AND is_present_in_latest_snapshot
    ),
    m AS (
      SELECT customer_id, resource_id, reservation_number, is_cancelled, checkin_date, checkout_date
      FROM `{MEWS_FCT_TABLE}`
    ),
    -- Résas actives (non annulées, CO à venir) par guest×appart. Si une existe, le
    -- DUVE_PIN peut être l'unique code d'une résa JUMELLE active (rebooking Mews
    -- cancel+recreate mêmes dates) → NE PAS purger, sinon guest sans code jusqu'à J-3.
    -- Le WHERE seul ne protège pas ce cas : le jumeau actif (CO futur) est filtré,
    -- seul le jumeau annulé passe → purge à tort. D'où ce garde anti-jointure.
    active AS (
      SELECT DISTINCT customer_id, resource_id
      FROM `{MEWS_FCT_TABLE}`
      WHERE NOT COALESCE(is_cancelled, FALSE) AND checkout_date >= CURRENT_DATE()
    )
    SELECT p.ext_id, p.duve_reservation_id,
           TRIM(CONCAT(COALESCE(p.user_firstname,''),' ',COALESCE(p.user_lastname,''))) AS guest,
           m.reservation_number,
           CASE WHEN m.is_cancelled THEN 'cancelled' ELSE 'checked_out' END AS reason
    FROM dpin p
    JOIN duve_latest d ON d.duve_reservation_id = p.duve_reservation_id
    JOIN m ON m.customer_id = d.mews_customer_id AND m.resource_id = d.duve_property_id
    LEFT JOIN active a ON a.customer_id = d.mews_customer_id AND a.resource_id = d.duve_property_id
    WHERE LOWER(d.duve_property_id) IN UNNEST(@wl)
      AND (m.is_cancelled OR m.checkout_date < CURRENT_DATE())
      AND a.customer_id IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.ext_id
      ORDER BY ABS(DATE_DIFF(m.checkin_date, DATE(p.active_from), DAY))) = 1
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("wl", "STRING", sorted(ALLOWED_PROPERTY_IDS))])
    return [dict(r.items()) for r in _bq().query(q, job_config=cfg).result()]


def _purge_native_duplicate(duve_resa_id: str) -> None:
    """Supprime le code ET l'invitation créés jadis par l'intégration native Duve pour
    CETTE réservation, une fois que le nôtre a été posé (appelé en fin de `_provision`).

    L'intégration native est coupée depuis le 20/06/2026 mais ses objets survivent :
    au 18/08 il restait 19 invitations `DUVE - …` actives ou futures et 10 utilisateurs
    porteurs d'un code natif encore valide. Sur un appartement intégré, ces objets
    doublonnent le nôtre — deux codes valides pour le même séjour, dont un que nous ne
    contrôlons pas. Sur un appartement NON intégré, ce sont au contraire les seuls codes
    du client : d'où le rattachement à `_provision`, qui ne s'exécute que sur la whitelist.

    Best-effort : un échec ne fait jamais échouer un provisioning réussi (le client a son
    code, c'est ce qui compte ; le doublon repartira au run suivant).
    """
    if ISEO_SHADOW_MODE:
        logger.info(f"🌗 SHADOW: would purge natifs DUVE de {duve_resa_id}")
        return
    for kind, path in (("DUVE_PIN", "standardDevices"), ("DUVE", "invitations")):
        ext = f"{kind} - {duve_resa_id}"
        try:
            g = _sofia("GET", f"/api/v2/{path}/extId/{ext}")
            if g.status_code != 200:
                continue  # 404 = rien à faire, le cas normal
            rd = _sofia("DELETE", f"/api/v2/{path}/{g.json().get('id')}")
            if rd.status_code in (200, 204, 404):
                logger.info(f"🧹 doublon natif supprimé : {ext}")
            else:
                logger.warning(f"⚠️ suppression {ext} : HTTP {rd.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ suppression {ext} : {e}")


def _purge_native_orphan(row: dict) -> tuple[bool, Optional[str]]:
    """DELETE le DUVE_PIN natif (par extId)."""
    ext = row["ext_id"]
    if ISEO_SHADOW_MODE:
        logger.info(f"🌗 SHADOW: would purge {ext} ({row.get('reason')}, {row.get('guest')})")
        return True, None
    g = _sofia("GET", f"/api/v2/standardDevices/extId/{ext}")
    if g.status_code == 404:
        return True, None  # déjà supprimé
    if g.status_code != 200:
        return False, f"GET {g.status_code}"
    rd = _sofia("DELETE", f"/api/v2/standardDevices/{g.json().get('id')}")
    if rd.status_code not in (200, 204, 404):
        return False, f"DELETE {rd.status_code}"
    logger.info(f"🧹 purged DUVE_PIN {ext} ({row.get('reason')}, résa {row.get('reservation_number')}, {row.get('guest')})")
    return True, None


# ── Entry point ───────────────────────────────────────────────────────────────

def _load_whitelist() -> set:
    """Whitelist des GUID property_id depuis le seed BQ iseo_whitelisted_apartments.
    Fallback sur ISEO_ALLOWED_PROPERTY_IDS (env) si le seed est vide OU inaccessible —
    garde-fou : jamais élargir ni vider la whitelist par accident sur un incident BQ.
    Le seed est le point unique d'élargissement (aussi lu par les 2 modèles dbt)."""
    try:
        # TRIM + exige les 2 colonnes non vides : une ligne `,<guid>` (apartment_code
        # vide) provisionnerait un appart non surveillé par les modèles dbt ; un GUID
        # avec espace parasite sortirait silencieusement du pipeline.
        rows = _bq().query(
            f"SELECT DISTINCT TRIM(LOWER(property_id)) AS pid FROM `{WHITELIST_TABLE}` "
            f"WHERE COALESCE(TRIM(property_id), '') != '' "
            f"  AND COALESCE(TRIM(apartment_code), '') != ''").result()
        pids = {r["pid"] for r in rows if r["pid"]}
        if pids:
            return pids
        logger.warning("⚠️ whitelist seed BQ vide → fallback env var")
    except Exception as e:
        logger.warning(f"⚠️ whitelist seed BQ inaccessible ({e}) → fallback env var")
    if not ALLOWED_PROPERTY_IDS:
        # Seed ET env vides : un set vide = allow-all sur _provision/_resync (l'inverse
        # de la purge, gatée elle). On refuse de provisionner tout le parc → CRASH (mail).
        raise RuntimeError("whitelist ISEO vide (seed BQ + env var) — refus de provisionner")
    return ALLOWED_PROPERTY_IDS


def _stuck_gateways_to_retry() -> list[dict]:
    """Passerelles `push_stuck` portant au moins un appart de la whitelist (seed dbt),
    encore sous le plafond de tentatives. Même périmètre que `trigger_iseo_gateway_push_stuck`."""
    q = f"""
    SELECT ph.gateway_id, ph.gateway_name, ph.last_push_status,
           COALESCE(ph.n_pushes_since_applied, 0) AS n_pushes_since_applied,
           STRING_AGG(DISTINCT l.apartment_code ORDER BY l.apartment_code) AS apartments
    FROM `{GATEWAY_PUSH_HEALTH_TABLE}` ph
    JOIN `{SMART_LOCKS_TABLE}` l ON l.gateway_id = ph.gateway_id
    JOIN `{WHITELIST_TABLE}` w ON w.apartment_code = l.apartment_code
    WHERE ph.push_stuck
      AND COALESCE(ph.n_pushes_since_applied, 0) < @max_pushes
    GROUP BY 1, 2, 3, 4
    ORDER BY 1
    """
    job = _bq().query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("max_pushes", "INT64", ISEO_RETRY_PUSH_MAX)]))
    return [dict(r) for r in job.result()]


def _rows_to_verify() -> list[dict]:
    """Lignes de cache actives, créées depuis ≥ ISEO_VERIFY_AFTER_MIN, sans accusé
    d'écriture en serrure. La passerelle vient de la serrure du cache (pas du tag)."""
    q = f"""
    SELECT c.duve_reservation_id, c.mews_reservation_number, c.apartment_code,
           c.iseo_device_id, c.provisioned_at,
           COALESCE(c.write_retries, 0) AS write_retries,
           l.gateway_id
    FROM `{PIN_CACHE_TABLE}` c
    LEFT JOIN `{SMART_LOCKS_TABLE}` l
      ON CAST(l.lock_id AS STRING) = CAST(c.iseo_lock_id AS STRING)
    WHERE c.archived_at IS NULL
      AND c.lock_written_at IS NULL
      AND c.iseo_device_id IS NOT NULL
      AND c.provisioned_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @after MINUTE)
      AND c.checkout_date >= CURRENT_DATE('Europe/Paris')
    -- Les plus anciens d'abord ; plafond de sûreté (task-timeout 300 s). Plus aucun
    -- appel API par ligne depuis la recette du 07/09 (listing + UPDATE par lot).
    ORDER BY c.provisioned_at
    LIMIT @cap
    """
    job = _bq().query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("after", "INT64", ISEO_VERIFY_AFTER_MIN),
        bigquery.ScalarQueryParameter("cap", "INT64", ISEO_VERIFY_MAX_PER_RUN)]))
    return [dict(r) for r in job.result()]


def _device_states() -> dict[str, str]:
    """{device id: credentialRule.state} pour TOUT l'inventaire, en 2-3 appels paginés
    (Spring : page/pageSize → content/totalPages, même chemin que l'ETL `iseo.py`).
    ⚠ C'est la SEULE lecture qui porte `state` : `GET /standardDevices/{id}` répond
    400 et `/extId/{ext}` renvoie un DTO SANS `credentialRule.state` (recette 07/09 :
    98/98 « rule_state=? », tous relancés à tort)."""
    states: dict[str, str] = {}
    page = 0
    while True:
        r = _sofia("GET", f"/api/v2/standardDevices?page={page}&pageSize=200")
        if r.status_code != 200:
            raise RuntimeError(f"list devices page={page}: HTTP {r.status_code}: {r.text[:120]}")
        payload = r.json() or {}
        items = payload.get("content", []) if isinstance(payload, dict) else payload
        for it in items:
            st = ((it.get("credentialRule") or {}).get("state") or "")
            states[str(it.get("id"))] = str(st).upper()
        total_pages = payload.get("totalPages") if isinstance(payload, dict) else None
        if not items or total_pages is None or page + 1 >= total_pages:
            break
        page += 1
    return states


def _mark_lock_written(duve_resa_ids: list[str]) -> None:
    # ⚠ Un UPDATE BQ coûte ~2-3 s : par lot, jamais par ligne (98 lignes × 2 requêtes
    # = task-timeout dépassé au 1er passage du 07/09).
    if not duve_resa_ids:
        return
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET lock_written_at = CURRENT_TIMESTAMP()
    WHERE duve_reservation_id IN UNNEST(@ids) AND archived_at IS NULL
    """
    _bq().query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("ids", "STRING", duve_resa_ids)])).result()


def _bump_write_retries(duve_resa_ids: list[str]) -> None:
    if not duve_resa_ids:
        return
    q = f"""
    UPDATE `{PIN_CACHE_TABLE}`
    SET write_retries = COALESCE(write_retries, 0) + 1
    WHERE duve_reservation_id IN UNNEST(@ids) AND archived_at IS NULL
    """
    _bq().query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("ids", "STRING", duve_resa_ids)])).result()


def _verify_writes() -> tuple[int, int, int, list[str]]:
    """Phase « verify » (chantier C) : confronte chaque device créé et pas encore
    accusé à `credentialRule.state` lu sur l'inventaire Sofia (un listing par run).
    UPDATED → `lock_written_at`. Sinon, passé le délai de retry, ré-émet UN
    CREDENTIALS_UPDATED PAR PASSERELLE concernée et incrémente `write_retries` des
    résas derrière — à ISEO_WRITE_RETRY_MAX, dbt rend `non_ecrit` (6.1 : dicter le
    code fixe). On continue de VÉRIFIER après le plafond : quand la passerelle repart
    (salve manuelle, chantier G), l'accusé arrive et l'état redevient `delivre` seul.
    Retourne (écrits, relancés, en attente, erreurs)."""
    written = retried = pending = 0
    errors: list[str] = []
    rows = _rows_to_verify()
    if not rows:
        return 0, 0, 0, []
    states = _device_states()
    written_ids: list[str] = []
    bumped_ids: list[str] = []
    gateways: dict[str, list[str]] = {}
    now = datetime.now(timezone.utc)
    for row in rows:
        tag = f"{row.get('mews_reservation_number')} ({row.get('apartment_code')})"
        dev_id = str(row["iseo_device_id"])
        if dev_id not in states:
            # Device disparu côté Sofia (supprimé à la main) : `iseo_reconciliation`
            # (MISSING_IN_SOFIA) le porte, pas nous.
            pending += 1
            continue
        if states[dev_id] == "UPDATED":
            written_ids.append(row["duve_reservation_id"])
            written += 1
            continue
        age_min = (now - row["provisioned_at"]).total_seconds() / 60
        if age_min < ISEO_VERIFY_RETRY_AFTER_MIN or row["write_retries"] >= ISEO_WRITE_RETRY_MAX:
            pending += 1
            continue
        if row.get("gateway_id") is None:
            logger.warning(f"⚠️ verify {tag}: serrure sans passerelle connue — rien à relancer")
        else:
            gateways.setdefault(str(row["gateway_id"]), []).append(tag)
        bumped_ids.append(row["duve_reservation_id"])
        retried += 1
        logger.warning(f"⚠️ verify {tag}: rule_state={states[dev_id] or '?'} après {age_min:.0f} min "
                       f"→ relance {row['write_retries'] + 1}/{ISEO_WRITE_RETRY_MAX}")
    for gw_id, tags in gateways.items():
        try:
            ok, err = _retry_push({"gateway_id": gw_id})
            if not ok and not str(err).startswith("skipped"):
                errors.append(f"verify: retry push gw {gw_id} ({', '.join(tags[:3])}): {err}")
        except Exception as e:
            errors.append(f"verify: retry push gw {gw_id}: exception: {e}")
    try:
        _mark_lock_written(written_ids)
        _bump_write_retries(bumped_ids)
    except Exception as e:
        errors.append(f"verify: écriture cache échouée ({e}) — {len(written_ids)} accusé(s) perdus, revus au prochain run")
    return written, retried, pending, errors


def _retry_push(gw: dict) -> tuple[bool, Optional[str]]:
    """Ré-émet un CREDENTIALS_UPDATED — le contournement OFFICIEL donné par ISEO le 04/09
    (« forces a global credential synchronization »). Leur plateforme ne ré-émet JAMAIS un
    push FAILED (seul IN_TRANSIT est renvoyé, 10×), donc sans ce geste un code reste
    NOT_UPDATED à vie. ⚠ Pas de GATEWAY_RESTART ici : il coupe l'ouverture à distance
    ~5 min, et le job a un task-timeout de 300 s — le restart + la salve de 6 restent
    manuels (`merveil-etl-v2/utils/iseo_unstick_gateway.py`). Fire-and-forget : le
    résultat se lit au snapshot ETL suivant (`push_health`), pas ici."""
    if ISEO_SHADOW_MODE:
        return False, "skipped: shadow"
    r = _sofia("POST", f"/api/v2/gateways/{gw['gateway_id']}/notifications",
               {"type": "CONFIGURATION", "payloadType": "CREDENTIALS_UPDATED"})
    if r.status_code >= 300:
        return False, f"HTTP {r.status_code}: {r.text[:200]}"
    return True, None


def run() -> None:
    """Wrapper : tout crash → alerte mail + exit non-zero (visible Cloud Run)."""
    try:
        _run_inner()
    except Exception as e:
        logger.critical(f"🔴 ISEO orchestrator CRASH: {e}")
        _send_alert(
            "🔴 ISEO orchestrator — CRASH",
            build_email(
                "ISEO orchestrator — CRASH", severity="critical",
                subtitle=datetime.now(PARIS_TZ).strftime("%d/%m/%Y %H:%M"),
                intro="Le job a planté avant la fin — provisions/archives non "
                      "traitées sur ce run (retentées au prochain run 2h).<br><br>"
                      f'<pre style="background:#f8fafc;border:1px solid #e2e8f0;'
                      f'border-radius:6px;padding:12px;font-size:12px;color:#dc2626;'
                      f'white-space:pre-wrap">{esc(e)}</pre>',
            ),
            html=True)
        raise


def _run_inner() -> None:
    global ALLOWED_PROPERTY_IDS
    ALLOWED_PROPERTY_IDS = _load_whitelist()
    logger.info("=" * 70)
    logger.info(f"🚀 ISEO Orchestrator V3 (shadow={ISEO_SHADOW_MODE}, "
                f"whitelist={len(ALLOWED_PROPERTY_IDS)} property_ids depuis le seed BQ)")
    logger.info("=" * 70)
    errors: list[str] = []

    # 0. Vérification post-écriture (chantier C) — avant tout : c'est ce qui justifie
    # la cadence 10 min, et un code créé au run précédent doit être accusé ici.
    try:
        v_written, v_retried, v_pending, v_errors = _verify_writes()
    except Exception as e:
        v_written = v_retried = v_pending = 0
        v_errors = [f"verify: {e}"]
    errors.extend(v_errors)
    if v_written or v_retried or v_pending:
        logger.info(f"🔎 verify — écrits={v_written} relancés={v_retried} en attente={v_pending}")

    # 1. Provision (J-3)
    to_provision = _resa_to_provision()
    logger.info(f"📋 {len(to_provision)} résa(s) à provisionner (CI dans 0-{LOOKAHEAD_DAYS}j, pas encore couvertes)")
    ok = skip = held = 0
    n_no_form = sum(1 for r in to_provision if r.get("no_duve"))
    wallet_used = _wallet_used() if n_no_form else None
    if n_no_form:
        logger.info(f"📝 {n_no_form} séjour(s) sans pré-checkin à créer (chantier E) — "
                    f"wallet {wallet_used}/600, garde à {ISEO_QUOTA_MAX_USED_NO_FORM}")
    for row in to_provision:
        if row.get("no_duve") and wallet_used is not None \
                and wallet_used >= ISEO_QUOTA_MAX_USED_NO_FORM:
            skip += 1
            logger.warning(f"🚫 QUOTA {wallet_used}/600 ≥ {ISEO_QUOTA_MAX_USED_NO_FORM} — pas de "
                           f"création sans formulaire pour {row.get('customer_name')} "
                           f"({row.get('apartment_code')}, CI {row.get('checkin_date')})")
            continue
        # ⚠⚠ HyperGate morte = NE PAS PROVISIONNER. Le code partirait dans Duve sans
        # jamais atteindre la serrure (qui stocke ses codes en local et ne les reçoit
        # que par la passerelle) → le client se présente avec un code qui ne s'ouvre
        # pas. Ne rien pousser est ici l'état SÛR : Duve retombe alors sur le code
        # fixe de l'appartement, lui bien programmé dans la serrure.
        # ⚠ Ce n'est PAS un `hold` : la porte de validation retient un code VALIDE en
        # attendant un humain ; ici le code serait inutilisable quoi qu'on décide.
        # ⚠ Restreint à la WHITELIST, comme le premier skip de `_provision` : hors
        # whitelist on ne provisionne de toute façon jamais, et alerter dessus ferait
        # sonner toutes les 2 h sur des appartements sans passerelle qu'on n'a aucune
        # intention de piloter (mesuré : `ROY15-5D`, 2 séjours, alerterait à vide).
        if row.get("gateway_dead") and (
                not ALLOWED_PROPERTY_IDS
                or (row.get("duve_property_id") or "").lower() in ALLOWED_PROPERTY_IDS):
            skip += 1
            logger.warning(
                f"🚫 GATEWAY MORTE {row.get('apartment_code')} — provision annulée pour "
                f"{row.get('customer_name')} (CI {row.get('checkin_date')}) : le code "
                f"n'atteindrait pas la serrure. Duve garde le code fixe.")
            continue
        # Porte évaluée AVANT le provision : en mode `observe` on journalise sans
        # retenir, ce qui permet de mesurer le volume réel et de repérer un faux
        # positif coûteux avant de passer en `on`. La RC est alertée dans les deux
        # modes (le mail est envoyé par `_provision`, cf. `_notify_hold`).
        row["hold_reason"] = _evaluate_hold(row)
        if row["hold_reason"]:
            held += 1
            if ISEO_HOLD_MODE == "observe":
                logger.info(f"🌗 HOLD-OBSERVE {row['duve_reservation_id']} "
                            f"({row.get('apartment_code')}, {row.get('customer_name')}) — "
                            f"{row['hold_reason']} → aurait été retenu, code envoyé quand même")
        try:
            success, err = _provision(row)
        except Exception as e:
            success, err = False, f"exception: {e}"
        # Journal de la porte — APRÈS le provision, pour enregistrer ce qui est
        # réellement arrivé au code (retenu / parti quand même / skippé avant la
        # porte / erreur). C'est la seule trace des décisions qui n'envoient pas de
        # mail, cf. `_log_hold_decision`.
        if row["hold_reason"]:
            if err and str(err).startswith("skipped"):
                outcome = str(err)
            elif not success:
                outcome = f"error: {err}"
            elif ISEO_HOLD_MODE == "on":
                outcome = "held"
            else:
                outcome = "pushed_observe"
            _log_hold_decision(row, row["hold_reason"], "provision", outcome)
        if success:
            ok += 1
            if wallet_used is not None:
                wallet_used += 2  # user + device (+ invitation) par séjour créé
        elif str(err).startswith("skipped"):
            skip += 1
        else:
            logger.warning(f"⚠️ provision failed {row['duve_reservation_id']}: {err}")
            errors.append(f"provision {row['duve_reservation_id']} ({row.get('apartment_code')}): {err}")

    # 1a. Trous silencieux : résas whitelistées à provisionner toujours sans code,
    # classées par cause (lock / gateway / precheckin / paiement / autre). Loggés à
    # CHAQUE run, en warning. ⛔ Plus de mail depuis le 07/09 : le mail « 1×/jour »
    # était gardé par `hour == 8`, et depuis que le gateway lance le job à chaque
    # preCheckInDone (03/09), chaque run entre 8h et 9h le renvoyait (4 mails le
    # 07/09). Décision Hatim : rien d'urgent dedans — les états par arrivée sont en
    # 6.1 (`pin_state`), la passerelle morte a son trigger dbt, la porte dormante
    # a `iseo_pin_missing`. Le log par run suffit pour investiguer un « autre ».
    gaps = _whitelisted_gaps()
    for g in gaps:
        logger.warning(
            f"⚠️ gap provision [{g['reason']}] résa {g.get('reservation_number')} "
            f"({g.get('customer_name')}, {g.get('apartment_code') or g.get('resource_id')}, "
            f"CI {g.get('checkin_date')})")

    # 1b. Resync drift de dates (window cache ≠ dates live Mews)
    to_resync = _resa_to_resync()
    if to_resync:
        logger.info(f"🔄 {len(to_resync)} résa(s) à resync (drift de dates ou d'heures)")
    resynced = 0
    for row in to_resync:
        try:
            success, err = _resync(row)
        except Exception as e:
            success, err = False, f"exception: {e}"
        if success:
            resynced += 1
        elif not str(err).startswith("skipped"):
            logger.warning(f"⚠️ resync failed {row['duve_reservation_id']}: {err}")
            errors.append(f"resync {row['duve_reservation_id']} ({row.get('duve_property_id')}): {err}")

    # 2. Retry du push Duve (Sofia OK mais Duve KO à un run précédent)
    retry = 0
    if not ISEO_SHADOW_MODE:
        for row in _resa_duve_retry():
            key = row["duve_reservation_id"]
            members = [m for m in (row.get("live_member_duve_ids") or []) if m] \
                or [m for m in (row.get("stay_member_duve_ids") or "").split(",") if m]
            was_held = bool(row.get("cache_hold")) and not row.get("cache_released")

            # ⭐ Rétention en cours : la porte est RÉ-ÉVALUÉE à chaque run. Si plus aucun
            # critère ne tient (paiement encaissé, fiche levée, dates changées…), le code
            # part seul — sans geste RC ni mail. Sinon on attend [Livrer].
            # ⚠ ÉVALUÉ AVANT le test `members` (09/09) : une clé M sans formulaire n'a rien
            # où pousser, mais l'écran doit cesser d'accuser un paiement qui est arrivé.
            # Avant, `continue` sur `not members` sautait cette libération, et 6.1 affichait
            # « retenu · paiement refusé » jusqu'au pré-checkin — ou jusqu'au check-out.
            if was_held:
                still = _evaluate_hold(row) if ISEO_HOLD_MODE == "on" else None
                if still:
                    continue
                _mark_released(key, "auto:criteres_leves")
                _log_hold_decision(row, row["cache_hold"], "auto_release", "released")
                logger.info(f"🔓 {key} ({row.get('apartment_code')}) — critères levés "
                            f"(était : {row['cache_hold']}) → libéré")

            if not members:
                if key.startswith("M"):
                    continue  # code créé sans formulaire, toujours pas de Duve : rien où pousser
                members = [key]

            # ⭐ Le formulaire vient d'arriver sur une clé M : la porte est évaluée ICI, avec
            # les signaux qu'il apporte (groupe jeune, nom de la pièce…) — ils n'existaient
            # pas à la création. `was_held` est exclu : une rétention encore active a déjà
            # été tranchée juste au-dessus (elle a fait `continue`), et une rétention libérée
            # au même run n'a pas à être re-jugée sur les critères qu'on vient de lever.
            if key.startswith("M") and not was_held:
                hold = _evaluate_hold(row)
                # ⛔ Après une libération MANUELLE (09/09) : ne re-retenir que sur un motif
                # NOUVEAU. Sinon le formulaire ré-évaluait la porte sur les mêmes critères,
                # re-posait le hold — `_mark_held` n'effaçant pas `released_at`, l'overlay 6.1
                # basculait en `attente_form` alors que la résa était retenue — et renvoyait
                # un mail À CHAQUE RUN (10 min) jusqu'au check-out. La RC a acquitté ce
                # qu'elle a vu ; ce qu'elle n'a pas pu voir, elle doit le revoir.
                if hold and row.get("cache_released"):
                    acquittes = _motif_keys(row.get("cache_hold"))
                    nouveaux = [(k, label) for k, label in _hold_motifs(row)
                                if k not in acquittes]
                    hold = " + ".join(label for _, label in nouveaux) if nouveaux else None
                    if hold:
                        # `reset_release` : la rétention repart de zéro (released_at à NULL)
                        # → 6.1 réaffiche `retenu` et [Livrer] revient, avec le seul motif
                        # que la RC n'a pas encore tranché.
                        _mark_held(key, hold, ",".join(members), reset_release=True)
                        row["no_duve"] = False
                        _log_hold_decision(row, hold, "form_arrival_new_motif", "held")
                        _notify_hold(row, hold,
                                     suffix=" — nouveau motif au pré-checkin, après libération")
                        logger.warning(f"🔒 RE-HOLD {key} ({row.get('apartment_code')}) — motif "
                                       f"nouveau : {hold} → code PAS envoyé à Duve")
                        continue
                    logger.info(f"🔓 {key} ({row.get('apartment_code')}) — formulaire arrivé, "
                                f"aucun motif nouveau (acquittés : {row.get('cache_hold')}) → push")
                elif hold and ISEO_HOLD_MODE == "on":
                    _mark_held(key, hold, ",".join(members))
                    row["no_duve"] = False
                    _log_hold_decision(row, hold, "form_arrival", "held")
                    _notify_hold(row, hold, suffix=" — au pré-checkin")
                    logger.warning(f"🔒 HOLD au formulaire {key} ({row.get('apartment_code')}) — {hold} "
                                   f"→ code PAS envoyé à Duve")
                    continue
                elif hold:
                    _log_hold_decision(row, hold, "form_arrival", "pushed_observe")
            done, err = _duve_push_all(members, row.get("pin_value") or "",
                                       row.get("invitation_link") or "")
            if done:
                _mark_duve_pushed(key, ",".join(members))
                retry += 1
                if key.startswith("M"):
                    logger.info(f"📨 {key} — formulaire arrivé, code poussé à Duve ({', '.join(members)})")
                    for d in members:
                        _purge_native_duplicate(d)
            else:
                errors.append(f"duve-retry {key}: {err}")

    # 3. Archive (CO passé / annulée)
    to_archive = _resa_to_archive()
    logger.info(f"🗑️ {len(to_archive)} résa(s) à archiver (CO passé ou annulée)")
    archived = 0
    for row in to_archive:
        try:
            success, err = _archive(row)
        except Exception as e:
            success, err = False, f"exception: {e}"
        if success:
            try:
                _mark_archived(row["duve_reservation_id"])
                archived += 1
            except Exception as e:
                errors.append(f"archive state {row['duve_reservation_id']}: {e}")
        elif str(err).startswith("skipped"):
            pass  # shadow global : row live laissée active (cf. _archive)
        else:
            errors.append(f"archive {row['duve_reservation_id']}: {err}")

    # 4. Purge des DUVE_PIN natifs orphelins (résa annulée/checked-out) — whitelist only
    to_purge = _native_duve_pins_to_purge()
    if to_purge:
        logger.info(f"🧹 {len(to_purge)} DUVE_PIN natif(s) orphelin(s) à purger (whitelist)")
    purged = 0
    for row in to_purge:
        try:
            success, err = _purge_native_orphan(row)
        except Exception as e:
            success, err = False, f"exception: {e}"
        if success:
            purged += 1
        else:
            errors.append(f"purge {row.get('ext_id')}: {err}")

    # 5. Retry push des passerelles coincées (04/09) — 1 CREDENTIALS_UPDATED par run et
    # par passerelle `push_stuck` intégrée. Campagne manuelle du 04/09 : 8/11 débloquées,
    # toutes en ≤ 4 pushes → à 1 push/2 h la plupart repartent dans la journée, sans
    # geste humain. Le garde-fou `gateway_dead` reste tel quel : on ne pousse un code
    # qu'après un APPLIED constaté par l'ETL.
    # ⚠ Cadence 10 min depuis le 07/09 : sans garde, ce serait jusqu'à 12 pushes par
    # passerelle entre deux snapshots ETL (le compteur `n_pushes_since_applied` ne
    # bouge qu'à `:00`). On ne relance qu'à partir de :40 → :40, :45 (scheduler 2h) et
    # :50, soit ≤ 3 pushes / 2 h — la campagne du 04/09 débloquait en ≤ 4 pushes.
    to_retry = _stuck_gateways_to_retry() if datetime.now(PARIS_TZ).minute >= 40 else []
    if to_retry:
        logger.info(f"🔁 {len(to_retry)} passerelle(s) coincée(s) à relancer (< {ISEO_RETRY_PUSH_MAX} tentatives)")
    retried = 0
    for gw in to_retry:
        try:
            success, err = _retry_push(gw)
        except Exception as e:
            success, err = False, f"exception: {e}"
        if success:
            retried += 1
            logger.info(f"  🔁 gw {gw['gateway_id']} ({gw['apartments']}) — {gw['last_push_status']}, "
                        f"{gw['n_pushes_since_applied']} échec(s) → CREDENTIALS_UPDATED ré-émis")
        elif not str(err).startswith("skipped"):
            errors.append(f"retry push gw {gw['gateway_id']} ({gw['apartments']}): {err}")

    logger.info("=" * 70)
    logger.info(f"DONE — verify écrits={v_written}/relancés={v_retried} | "
                f"provision ok={ok} skip={skip} | hold[{ISEO_HOLD_MODE}]={held} | "
                f"resync={resynced} | duve-retry={retry} | archived={archived} | "
                f"purged={purged} | retry-push={retried} | erreurs={len(errors)}")
    logger.info("=" * 70)

    # ⚠ PAS de mail dédié ici (retiré le 20/08, il sonnait toutes les 2 h pour la
    # même réservation). Une passerelle morte est un état PERSISTANT : le répéter huit
    # fois par nuit n'ajoute rien et apprend au lecteur à ignorer l'expéditeur. La cause
    # est portée par `_whitelisted_gaps` (cause `gateway`, log par run) et par le
    # trigger dbt `iseo_gateway_offline` / `iseo_gateway_push_stuck` (digest 2h).
    # Les erreurs vont TOUJOURS au log (avant le 07/09 elles ne vivaient que dans le
    # mail). Le mail, lui, ne part qu'aux runs des minutes ≥ :40 — cadence 10 min
    # oblige, sinon une panne Sofia = 6 mails/h ; une erreur persistante est de
    # toute façon retentée à chaque run et ressort au :40.
    for e in errors:
        logger.warning(f"⚠️ {e}")
    if errors and datetime.now(PARIS_TZ).minute < 40:
        logger.info(f"{len(errors)} erreur(s) — mail différé au prochain run ≥ :40")
    elif errors:
        body = build_email(
            "ISEO orchestrator — erreurs",
            subtitle=datetime.now(PARIS_TZ).strftime("%d/%m/%Y %H:%M"),
            severity="critical",
            kpis=[
                {"label": "Erreurs", "value": len(errors), "color": "#dc2626"},
                {"label": "Provisions ok", "value": ok},
                {"label": "Resync", "value": resynced},
                {"label": "Archivées", "value": archived},
            ],
            intro="Chaque ligne = une résa dont le cycle PIN a échoué sur ce run "
                  "(retenté automatiquement au prochain run 2h).",
            table={"headers": ["Erreur"],
                   "rows": [[esc(e)] for e in errors[:50]]},
            button=("Ops · 7.8 Pipeline PIN",
                    "https://direction.archides.fr/ops-back?tab=pin_pipeline"),
        )
        _send_alert(f"⚠️ ISEO orchestrator — {len(errors)} erreur(s)", body, html=True)

