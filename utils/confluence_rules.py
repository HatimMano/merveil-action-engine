#!/usr/bin/env python3
"""
Prose et registre des règles machine publiées sur Confluence — contenu pur.

Séparé du moteur (`confluence_sync.py`) : ici on n'écrit QUE ce qu'un humain
doit rédiger. Tout ce qui est dérivable de l'état réel du système ne doit PAS
être écrit ici :

  • fréquence, canal, destinataires, statut actif/inactif  → dérivés de
    `config/routing.yaml` dès qu'une règle déclare `triggers=[...]` ;
  • whitelists, dernière activité, écarts                  → blocs vivants BQ
    (champ `live=` + collecteur dans `confluence_sync.LIVE_COLLECTORS`).

⚠️ CONTRAT ANTI-DÉRIVE : toute modification du COMPORTEMENT d'une règle
(handler, seuil, fenêtre, périmètre) = mise à jour de sa prose ici dans le
MÊME commit. C'est le seul maillon qui ne se corrige pas tout seul.

Champs d'une règle
------------------
Obligatoires : titre · slug · domaine · niveau · niveau_desc · owner · depuis
               · source · dashboard_url · quoi[]
Optionnels   : space (défaut VD) · triggers[] (active la dérivation)
               · live (bloc vivant BQ) · exemple[] · modifier[]
               · frequence / canal  → REQUIS seulement si pas de `triggers`
                 (règles hors dispatcher : jobs autonomes, outils externes)
               · frequence_extra / canal_extra → complément manuel concaténé
                 au dérivé, pour la part réellement hors `routing.yaml`
"""

# Spaces métier cibles — 1 page racine « 🤖 Automatisations (DWH) — <domaine> »
# par space, posée en sibling des sections existantes (on ne touche jamais au
# contenu préexistant). Chaque règle porte un champ `space` (défaut VD).
# NB : COR (0. Finance et Stratégie) est archivé → le futur lot finance ira
# dans CXSXJ (2. Comptabilité x Social x Juridique).
SPACES = {
    "VD":   dict(domaine="Ventes",
                 root="🤖 Automatisations (DWH) — Ventes",
                 label="domaine-ventes"),
    "GDA":  dict(domaine="Opérations",
                 root="🤖 Automatisations (DWH) — Opérations",
                 label="domaine-operations"),
    "TRAN": dict(domaine="Serrures & accès",
                 root="🤖 Automatisations (DWH) — Serrures & accès",
                 label="domaine-serrures"),
}
# Index global cross-spaces (vue CEO), dans 00. Entreprise.
INDEX_SPACE = "EN"
INDEX_TITLE = "🤖 Tout ce que fait la machine"

# ── Cross-links : pages process de l'équipe → règles machine ─────────────────
# La doc des règles vit dans une branche « 🤖 Automatisations (DWH) » parallèle
# aux process. Sans passerelle depuis la procédure humaine, personne ne la
# trouve. On pose donc un encart dans les pages process concernées — et on ne
# touche à RIEN d'autre : le bloc est délimité, réécrit à l'identique à chaque
# run, jamais dupliqué.
#
# ⚠️ Ne cibler qu'une page dont le contenu décrit VRAIMENT le geste automatisé.
# Un encart hors sujet dans la page d'une autre équipe = du bruit chez eux.

CROSSLINK_TITRE = "🤖 Assisté par la machine (DWH)"

# Marqueur de délimitation du bloc, cherché tel quel dans le storage au run
# suivant. ⚠️ Il DOIT être ASCII pur : Confluence réécrit le storage et échappe
# les accents (`é` → `&eacute;`), donc un marqueur accentué n'est plus
# retrouvable → l'encart serait ré-ajouté à chaque run (constaté le 12/08).
# On le planque dans l'URL des liens : invisible pour le lecteur, jamais touché.
CROSSLINK_MARK = "?src=dwh-crosslink"

CROSSLINKS = [
    dict(
        page_id="378077185",
        space="VD",
        titre="1.0. Ventes - Vérifications Quotidiennes",
        rules=["beyond-push-gaps-1n", "gaps-pricing", "surcote-1n-inefficace"],
        intro="Les étapes « gaps de 1 et 2 nuits au prix minimum » de cette procédure sont "
              "désormais <strong>faites par la machine</strong> sur les appartements du pilote : elle pose "
              "elle-même la fourchette de prix dans Beyond chaque matin, et signale dans le mail quotidien "
              "les gaps qu'elle ne couvre pas encore. La vérification manuelle reste utile hors pilote.",
    ),
    dict(
        page_id="98830694",
        space="TRAN",
        titre="2. Gestion des clés",
        rules=["codes-acces-auto", "surveillance-serrures"],
        intro="Aux trousseaux physiques décrits ci-dessus s'ajoutent les <strong>serrures connectées</strong> : "
              "sur les appartements basculés, le code de la porte est généré, posé et retiré automatiquement "
              "pour chaque séjour, et une surveillance signale les codes manquants ou les passerelles hors "
              "ligne.",
    ),
]


RULES = [
    dict(
        titre="Atterrissage budget sous cible (mensuel)",
        slug="budget-atterrissage",
        triggers=["budget_landing_gap"],
        domaine="Ventes — Budget",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        owner="Arnaud (à confirmer)",
        depuis="7 juillet 2026",
        source="trigger_budget_landing_gap → dash_ventes_budget (dernière édition du budget)",
        dashboard_url="https://direction.archides.fr/ventes?tab=budget&view=atterrissage",
        quoi=[
            "Chaque nuit, la machine compare l'<strong>atterrissage projeté</strong> du mois en cours "
            "(chiffre d'affaires réalisé + réservations déjà prises pour le reste du mois) au budget cible "
            "de la dernière édition.",
            "Si la projection passe <strong>sous 90 %</strong> du budget → alerte 🟡 dans le mail quotidien "
            "(sous 75 % → 🔴). L'alerte n'est armée qu'à partir du <strong>10 du mois</strong> (avant, les "
            "ventes restantes rendent le ratio peu significatif).",
            "À partir du <strong>15 du mois</strong>, elle surveille aussi le mois suivant : si les réservations "
            "déjà prises couvrent moins de <strong>50 %</strong> du budget M+1 → info dans le même mail.",
            "L'alerte donne le montant projeté, la cible, le % d'atteinte et le nombre d'appartements sous 80 %.",
        ],
        exemple=[
            "Mail du 10/07/2026 : « 🟡 Budget 2026-07 : atterrissage projeté 1 947 k€ / cible 2 222 k€ (88 %) "
            "— 34 apparts &lt; 80 % ».",
        ],
    ),
    dict(
        titre="Gaps de pricing — récap quotidien",
        slug="gaps-pricing",
        triggers=["gap_pricing_summary"],
        domaine="Ventes — Pricing",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        owner="Raphael (à confirmer)",
        depuis="17 mai 2026",
        source="trigger_gap_pricing_summary → dash_ventes_gaps (flag_action)",
        dashboard_url="https://direction.archides.fr/ventes?tab=controle&view=gaps_actions",
        quoi=[
            "Chaque nuit, la machine repère les <strong>trous de calendrier</strong> (nuits isolées entre deux "
            "réservations) qui appellent une action de pricing, et les résume en 3 lignes dans le mail quotidien :",
            "• <strong>Gaps critiques</strong> : à combler sous 7 jours (dernière chance de vendre la nuit).<br/>"
            "• <strong>Gaps moyens</strong> : entre 8 et 14 jours.<br/>"
            "• <strong>Marge potentielle</strong> : ce que rapporteraient ces nuits si elles étaient vendues au "
            "prix des nuits voisines (déduction faite du ménage et du coussin de marge).",
            "Le bouton du mail ouvre la liste détaillée par appartement dans le dashboard.",
        ],
    ),
    dict(
        titre="Surcote 1 nuit inefficace sur gap à venir",
        slug="surcote-1n-inefficace",
        triggers=["beyond_surcote_gap"],
        domaine="Ventes — Pricing Beyond",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        owner="Raphael / Arnaud (à confirmer)",
        depuis="juin 2026",
        source="trigger_beyond_surcote_gap → dash_beyond_proposed_changes × dash_beyond_gap1n_surcote",
        dashboard_url="https://direction.archides.fr/ventes?tab=controle&view=surcote_1n&ineff=1",
        quoi=[
            "Beyond applique une <strong>surcote automatique sur les nuits seules</strong> (en moyenne +88 %, "
            "jusqu'à +160 % selon l'appartement). Sur certains appartements, cette surcote est "
            "<strong>empiriquement inefficace</strong> : historiquement, moins de 5 % de ces nuits surcotées "
            "≥ 100 % se vendent — la nuit reste vide.",
            "Chaque nuit, la machine repère les appartements qui ont <strong>une nuit seule à vendre dans les "
            "14 prochains jours</strong> ET une surcote flaggée inefficace → alerte dans le mail quotidien, "
            "<strong>avant</strong> que la nuit ne soit perdue.",
            "L'action suggérée : baisser la surcote / le prix de cette nuit. Depuis le 17/07, sur les "
            "appartements du pilote, la machine pose <strong>elle-même</strong> la fenêtre de prix — voir la "
            "règle « Push automatique des prix sur les nuits seules ». Sur ces appartements, la surcote 1 nuit "
            "Beyond a été <strong>supprimée le 21/07</strong> (elle s'appliquait après la fenêtre et la rendait "
            "inopérante).",
        ],
    ),
    dict(
        titre="Push automatique des prix sur les nuits seules (gaps 1N/2N)",
        slug="beyond-push-gaps-1n",
        frequence_extra="Push des prix : quotidien 6h45 + 10h45",
        triggers=["beyond_gap_filled"],  # le mail 🎉 ; le push lui-même est un job autonome
        live="beyond_push",
        domaine="Ventes — Pricing Beyond",
        niveau="N4", niveau_desc="la machine agit seule, l'humain audite a posteriori",
        owner="Raphael / Mickael",
        depuis="17 juillet 2026 (pilote 8 appartements — élargi à 31 appartements le 07/08, "
               "nuits orphelines sur tout le parc le 06/08, trous de 2 nuits activés le 10/08)",
        source="merveil-action-engine-beyond → dash_beyond_push_targets → API Beyond (seasonal-prices) → beyond_raw.price_pushes_log",
        dashboard_url="https://direction.archides.fr/ventes?tab=controle&view=push_auto",
        quoi=[
            "Sur 2026, ~1 380 « nuits seules » (une nuit vide coincée entre deux réservations) — seules 2,5 % "
            "se vendent, car la surcote 1 nuit de Beyond les affiche à ~2× le prix du marché.",
            "Chaque matin, la machine repère les nuits seules à venir (jusqu'à J+90) sur les appartements du "
            "pilote et pose <strong>directement dans Beyond</strong> une fourchette de prix : "
            "<strong>plancher</strong> = ménage + frais ops + coussin de marge (on ne brade jamais) · "
            "<strong>plafond</strong> = ce que les nuits voisines ont réellement vendu. Beyond continue son "
            "pricing normalement à l'intérieur de la fourchette.",
            "Depuis le 10/08 (décisions meeting Beyond 04/08), la même mécanique couvre les <strong>trous "
            "de 2 nuits</strong> sur les appartements marqués « 2N » (cf. bloc État actuel) : fourchette posée "
            "sur les 2 nuits, plancher "
            "par nuit divisé par 2 (les coûts fixes s'amortissent sur le séjour — Beyond impose un séjour "
            "minimum de 2 nuits sur ces trous, donc jamais de vente d'1 nuit au plancher réduit ; quand la "
            "première nuit est passée, la nuit restante repasse automatiquement au plancher plein le lendemain "
            "matin) ; et les "
            "<strong>nuits orphelines</strong> (fin de trou : la nuit de ce soir est libre, quelqu'un arrive "
            "demain, la veille est déjà passée) : plancher plein posé pour la journée dès le run de 6h45 — "
            "sur <strong>l'ensemble du parc</strong>, pas seulement la whitelist (pure protection plancher, "
            "aucune surcote à retirer : le clamp s'applique après les réglages Beyond).",
            "Si les nuits voisines vendent sous notre plancher (fréquent sur les petits appartements), la "
            "fourchette devient un <strong>prix fixe rentable</strong> — mieux que le prix surcoté, jamais à perte.",
            "Les <strong>règles saisonnières posées par l'équipe dans Beyond sont préservées</strong> (un plancher "
            "équipe plus haut gagne toujours). Nuit vendue ou gap disparu → la fenêtre est retirée le lendemain. "
            "Chaque modification est journalisée (audit complet, réversible en un clic).",
        ],
        exemple=[
            "17/07/2026 : 1er run — fenêtres posées sur 10 nuits seules des 5 premiers appartements "
            "(ex. P11-RIC75-0F, nuit du 10/09 : fourchette 394 → 419 €, ADR voisin 469 €). "
            "Quand une de ces nuits se vend : mail 🎉 automatique avec le prix vendu, le canal et la fenêtre.",
        ],
        modifier=[
            "La <strong>liste des appartements du pilote</strong> est modifiable en direct depuis le dashboard "
            "(Ventes → Contrôle → Push auto, éditeurs autorisés) : l'ajout/retrait est pris en compte au run "
            "suivant (10h45) et journalisé — le bloc « État actuel » ci-dessus reflète la liste en vigueur.",
            "⚠️ En ajoutant un appartement, demander à Beyond la <strong>suppression de sa surcote 1 nuit</strong> "
            "(elle s'applique après notre fourchette et la rendrait inopérante — réglé le 21/07 sur le pilote).",
            "Fourchettes, seuils et arrêt de la règle restent gérés dans le DWH — demande à Hatim.",
        ],
    ),

    dict(
        titre="Séquences relationnelles automatiques (customers.io)",
        slug="sequences-crm",
        domaine="Ventes — CRM",
        niveau="N4", niveau_desc="la machine agit seule, l'humain audite a posteriori",
        frequence="customers.io va chercher les données du DWH plusieurs fois par jour",
        canal="E-mails envoyés directement au client par customers.io",
        owner="à confirmer",
        depuis="2026",
        source="marts.cio_customers · marts.cio_events → intégration native customers.io (pull BigQuery)",
        dashboard_url="https://direction.archides.fr/clients",
        quoi=[
            "C'est la seule règle où la machine <strong>écrit au client</strong>, sans validation humaine "
            "préalable. Le DWH ne fait qu'exposer la donnée ; c'est customers.io qui envoie.",
            "Le DWH tient à jour une <strong>fiche client</strong> (nombre de séjours, chiffre d'affaires à "
            "vie, segment Gold/Silver/Bronze, canal de réservation, date du dernier avis satisfait, "
            "anniversaire) et publie <strong>5 événements</strong> : réservation confirmée · séjour terminé · "
            "réservation annulée · panier abandonné · avis reçu.",
            "customers.io vient lire ces deux tables plusieurs fois par jour et déclenche ses séquences. "
            "<strong>Actives aujourd'hui</strong> : séquence post-séjour (satisfaction puis relance pour une "
            "nouvelle réservation) et séquence post-annulation (récupération du client annulé).",
            "<strong>Garde-fous côté DWH</strong> : seuls les clients avec une adresse e-mail réelle sont "
            "exposés (les adresses relais des OTAs sont exclues) — un client qui n'a jamais rempli son "
            "formulaire Duve n'est donc jamais contacté. Chaque événement est envoyé <strong>une seule fois "
            "par réservation</strong>, à vie.",
            "⚠️ <strong>Point de vigilance connu</strong> : un changement d'appartement se traduit dans Mews "
            "par une annulation suivie d'une nouvelle réservation. Un événement « annulation » part donc pour "
            "un client qui vient quand même — à filtrer côté ciblage de la campagne.",
        ],
        modifier=[
            "Le <strong>contenu des e-mails et les conditions de déclenchement</strong> se règlent dans "
            "customers.io, pas dans le DWH.",
            "Ce que le DWH contrôle : les données envoyées (fiche client et événements) — demande à Hatim.",
            "⚠️ <strong>Ce qui n'est pas mesurable ici</strong> : envois, ouvertures, clics et désinscriptions "
            "restent dans customers.io. Les faire redescendre dans le DWH suppose un abonnement customers.io "
            "supérieur, que nous n'avons pas — aucun chiffre de performance CRM n'est donc disponible dans le "
            "dashboard aujourd'hui.",
        ],
    ),

    # ── GDA — 5. Opérations ──────────────────────────────────────────────────
    dict(
        space="GDA",
        titre="Digest arrivées & disponibilités du jour",
        slug="digest-dispo",
        triggers=["dispo_daily_summary"],
        domaine="Opérations — Front office",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        owner="Mickael (à confirmer)",
        depuis="17 mai 2026",
        source="trigger_dispo_daily_summary → dash_ops_dispo_daily",
        dashboard_url="https://direction.archides.fr/ops-front?tab=dispo&view=matin",
        quoi=[
            "Chaque matin, la machine compte les appartements <strong>réellement disponibles</strong> et le "
            "résume en 3 lignes dans le mail quotidien :",
            "• <strong>Dès le matin</strong> : vides depuis au moins la veille (vendables immédiatement).<br/>"
            "• <strong>Cet après-midi</strong> : check-out aujourd'hui, sans late checkout.<br/>"
            "• <strong>Today + 2 jours minimum</strong> : libres sur une fenêtre d'au moins 3 nuits.",
            "Les appartements <strong>bloqués</strong> (travaux, usage interne) sont exclus automatiquement — "
            "le chiffre est directement exploitable pour pousser des ventes last-minute.",
        ],
    ),
    dict(
        space="GDA",
        titre="Annulations — brief quotidien 11h & alertes ciblées",
        slug="annulations",
        frequence_extra="Brief autonome à 11h",
        canal_extra="Brief 11h → alerte_ventes@archides.fr, emilia@archides.fr",
        triggers=[
            "cancellation_vip",
            "cancellation_large_apt",
            "high_cancellations_daily",
        ],
        domaine="Opérations — Réservations",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        owner="Emilia",
        depuis="23 mai 2026",
        source="cancellations_brief → dash_ops_cancellations_recent · trigger_cancellation_vip / trigger_cancellation_large_apt / trigger_high_cancellations_daily",
        dashboard_url="https://direction.archides.fr/ops-front?tab=cancellations&preset=24h",
        quoi=[
            "<strong>Brief de 11h</strong> : toutes les annulations des dernières 24h en un mail (montant, "
            "canal, dates, client), avec le bouton vers le détail dashboard. Zéro annulation = pas de mail.",
            "<strong>Alertes ciblées</strong> (digest de 7h) : les annulations qui méritent une action "
            "immédiate — client <strong>Gold/Silver</strong> (fidèle ou gros panier) et <strong>grands "
            "appartements</strong> avec check-in proche (nuits chères difficiles à revendre à court terme).",
            "<strong>Pic d'annulations</strong> : dès qu'une journée dépasse <strong>30 annulations</strong>, "
            "une alerte le signale avec le montant total annulé. Un pic de cette ampleur est rarement un "
            "hasard (incident sur un canal, sur un appartement, ou erreur de manipulation).",
            "Filtre anti-bruit : si le client a une autre réservation active à ±7 jours (changement de dates "
            "ou d'appartement), ce n'est pas une vraie perte → pas d'alerte.",
        ],
    ),
    dict(
        space="GDA",
        titre="Alertes séjour (last-minute, double booking, sans appartement)",
        slug="alertes-sejour",
        triggers=[
            "last_minute_checkin",
            "double_booking",
            "checkin_no_apartment",
        ],
        domaine="Opérations — Front office",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        owner="Mickael (à confirmer)",
        depuis="mai 2026",
        source="trigger_last_minute_checkin · trigger_double_booking · trigger_checkin_no_apartment",
        dashboard_url="https://direction.archides.fr/ops-front",
        quoi=[
            "Trois surveillances qui ne se manifestent <strong>que lorsqu'il y a un cas</strong> (la plupart "
            "des jours : rien) :",
            "• <strong>Check-in last-minute</strong> : réservation prise très peu de temps avant l'arrivée → "
            "vérifier que ménage, code d'accès et accueil suivent.<br/>"
            "• <strong>Double booking</strong> : deux réservations actives qui se chevauchent sur le même "
            "appartement → à résoudre avant l'arrivée.<br/>"
            "• <strong>Check-in sans appartement</strong> : arrivée imminente sans espace assigné dans Mews.",
        ],
    ),
    dict(
        space="GDA",
        titre="Suivi des avis — mauvais avis & risque Superhost",
        slug="suivi-avis",
        triggers=[
            "satisfaction_low_review",
            "superhost_risk",
        ],
        domaine="Opérations — Qualité",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        owner="Emilia",
        depuis="mai 2026",
        source="trigger_satisfaction_low_review · trigger_superhost_risk → fct_reviews (Reva)",
        dashboard_url="https://direction.archides.fr/qualite?tab=appartements",
        quoi=[
            "<strong>Mauvais avis</strong> : chaque nouvel avis ≤ 3★ déclenche une alerte individuelle "
            "(appartement, note, canal) → traiter à chaud (réponse publique, geste commercial, tâche "
            "correctrice).",
            "<strong>Risque Superhost</strong> : appartements dont la note moyenne sur les <strong>3 derniers "
            "mois</strong> passe sous 4,5★ (avec au moins 3 avis) → 🟡 ; sous 4,0★ → 🔴. Regroupés en une "
            "case unique dans le mail, triés par volume d'avis puis pire note.",
            "C'est le radar avancé de la note publique : la moyenne 3 mois bouge des semaines avant la note "
            "affichée sur les OTAs.",
        ],
    ),

    # ── TRAN — 6. Opérations N2 (serrures) ───────────────────────────────────
    dict(
        space="TRAN",
        titre="Codes d'accès automatiques par séjour (serrures connectées)",
        slug="codes-acces-auto",
        live="iseo",
        domaine="Serrures & accès",
        niveau="N4", niveau_desc="la machine agit seule, l'humain audite a posteriori",
        frequence="Toutes les 2 heures (à :45)",
        canal="Silencieux quand tout va bien — mail d'erreur si un code n'a pas pu être posé",
        owner="Sylvain / Mickael (à confirmer)",
        depuis="juin 2026 (élargissement progressif du parc)",
        source="merveil-action-engine-iseo → API Sofia/ISEO + Duve → iseo_raw.merveil_pin_cache",
        dashboard_url="https://direction.archides.fr/ops-back?tab=serrures",
        quoi=[
            "Sur les appartements basculés (liste dans le bloc « État actuel »), la machine gère "
            "<strong>seule</strong> tout le cycle de vie du code de la porte — à la place du code fixe "
            "permanent partagé entre tous les clients.",
            "<strong>3 jours avant l'arrivée</strong> (réservation payée, pre-checkin Duve complété), elle "
            "génère un code 4 chiffres unique, le pose sur la serrure <strong>au nom du client</strong>, "
            "valable uniquement du check-in au check-out — de l'heure d'arrivée jusqu'à "
            "<strong>19h le jour du départ</strong> (marge volontaire après l'heure de départ officielle, "
            "pour ne pas enfermer dehors un client qui repasse chercher ses bagages), "
            "crée un <strong>lien d'ouverture à distance</strong> de secours, et pousse le tout dans Duve — "
            "les messages automatiques Duve envoient donc le bon code sans aucune intervention.",
            "Séjour <strong>prolongé, raccourci ou décalé</strong> → le code est reposé sur les nouvelles "
            "dates (même code, le client ne voit rien). <strong>Départ ou annulation</strong> → le code est "
            "supprimé de la serrure.",
            "Le code fixe historique reste en place en parallèle pendant la phase pilote (décision du "
            "13/07) — filet de sécurité, à purger appartement par appartement plus tard.",
        ],
        exemple=[
            "Réservation arrivant vendredi sur un appartement basculé : mardi, la machine pose un code unique "
            "valable du vendredi 15h au lundi 19h. Le client le reçoit dans son message Duve habituel. "
            "S'il prolonge d'une nuit dans Mews, le code est étendu automatiquement au run suivant.",
        ],
        modifier=[
            "La liste des appartements basculés est un référentiel DWH : l'élargissement se fait par lots "
            "après période d'observation — demande à Hatim.",
            "⚠️ Basculer un appartement ne supprime pas son code fixe (conservés en doublon pour l'instant, "
            "décision du 13/07).",
        ],
    ),
    dict(
        space="TRAN",
        titre="Surveillance des serrures & des codes",
        slug="surveillance-serrures",
        frequence_extra="Récap quotidien ~8h45",
        canal_extra="Récap quotidien « résas sans code » → hatim@archides.fr",
        triggers=[
            "iseo_pin_missing",
            "iseo_reconciliation",
            "iseo_etl_stale",
            "iseo_gateway_offline",
        ],
        domaine="Serrures & accès",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        owner="Sylvain / Mickael (à confirmer)",
        depuis="13 juin 2026 (récap quotidien par cause : 5 août 2026 · surveillance des HyperGates : 10 août 2026)",
        source="trigger_iseo_pin_missing · trigger_iseo_reconciliation · trigger_iseo_etl_stale · trigger_iseo_gateway_offline → dash_ops_pin_reconciliation · stg_iseo__gateways · gaps orchestrateur",
        dashboard_url="https://direction.archides.fr/ops-back?tab=serrures",
        quoi=[
            "Le filet de sécurité de la règle « Codes d'accès automatiques » — trois surveillances toutes "
            "les 2 heures :",
            "• <strong>Porte dormante</strong> : un client est censé séjourner sur un appartement basculé, "
            "aucun code n'est posé ET la porte n'a pas été ouverte depuis 48h → vrai risque d'accès.<br/>"
            "• <strong>Réconciliation</strong> : l'état interne est comparé à l'état réel des serrures Sofia. "
            "Code supprimé à la main dans l'interface, code orphelin, code encore actif après le départ → "
            "chaque écart est signalé (l'incident fondateur : un code effacé par erreur dans l'UI, client "
            "bloqué dehors).<br/>"
            "• <strong>Données en retard</strong> : si la collecte ISEO ne remonte plus, alerte — on ne "
            "surveille jamais à l'aveugle.",
            "Un même problème n'est signalé qu'une fois tant qu'il n'est pas résolu (déduplication 4h).",
            "S'y ajoute depuis le <strong>10 août 2026</strong> la surveillance des <strong>HyperGates</strong> "
            "(les boîtiers qui relient les serrures au réseau) : une passerelle qui n'a plus donné signe de vie "
            "depuis <strong>plus de 7 jours</strong> est signalée dans le mail quotidien, avec la liste des "
            "serrures qu'elle dessert. Sans passerelle, la serrure fonctionne toujours au clavier mais on ne "
            "peut plus ni poser un code à distance, ni ouvrir la porte à distance, ni voir les ouvertures — "
            "c'est une panne à traiter sur place. Cette surveillance rend <strong>redondant</strong> le rapport "
            "horaire envoyé par ISEO : la même information est désormais dans le DWH.",
            "En complément, un <strong>récapitulatif quotidien</strong> (~8h45) liste les arrivées ≤ J+3 "
            "toujours sans code, <strong>classées par cause</strong> : formulaire pre-checkin non rempli, "
            "paiement en échec (le code est volontairement retenu), serrure non résolue, ou anomalie à "
            "investiguer. Le même état est visible arrivée par arrivée sur la page Arrivées (6.1, "
            "« Code d'accès ISEO »).",
        ],
    ),
]

NIVEAUX = ("Grille de lecture des niveaux : <strong>N2</strong> = la machine surveille et alerte · "
           "<strong>N3</strong> = la machine <em>propose</em> une action, l'humain valide · "
           "<strong>N4</strong> = la machine agit, l'humain audite · <strong>N5</strong> = autonome.")

FOOTER = ('<em>Contact : Hatim (hatim@archides.fr) — pages générées automatiquement chaque matin, '
          'commentaires bienvenus, édition manuelle déconseillée.</em>')
