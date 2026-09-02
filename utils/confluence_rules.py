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
# NB : COR (0. Finance et Stratégie) est archivé → le lot finance vit dans
# CXSXJ (2. Comptabilité x Social x Juridique), ouvert le 19/08.
SPACES = {
    "VD":    dict(domaine="Ventes",
                  root="🤖 Automatisations (DWH) — Ventes",
                  label="domaine-ventes"),
    "GDA":   dict(domaine="Opérations",
                  root="🤖 Automatisations (DWH) — Opérations",
                  label="domaine-operations"),
    "TRAN":  dict(domaine="Serrures & accès",
                  root="🤖 Automatisations (DWH) — Serrures & accès",
                  label="domaine-serrures"),
    "CXSXJ": dict(domaine="Finance & Comptabilité",
                  root="🤖 Automatisations (DWH) — Finance & Comptabilité",
                  label="domaine-finance"),
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
        rules=["beyond-push-gaps-1n", "gaps-pricing"],
        intro="Les étapes « gaps de 1 et 2 nuits au prix minimum » de cette procédure sont "
              "désormais <strong>faites par la machine</strong> sur les appartements du pilote : elle pose "
              "elle-même la fourchette de prix dans Beyond chaque matin, et signale dans le mail quotidien "
              "les gaps qu'elle ne couvre pas encore. La vérification manuelle reste utile hors pilote.",
    ),
    dict(
        page_id="98830694",
        space="TRAN",
        titre="2. Gestion des clés",
        rules=["codes-acces-auto", "surveillance-serrures", "resa-risque-acces", "intrusion-code-fixe"],
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
    # ⚠️ Cette règle et « Push automatique des prix » regardent le MÊME objet :
    # les trous de 1 et 2 nuits (`dash_ventes_gaps` ne couvre rien d'autre). La
    # ligne de partage n'est pas le périmètre, c'est le geste : ici l'humain
    # baisse un prix à la main, là-bas la machine le pose. Ne pas les refondre
    # en une page unique : un niveau (N2/N4) par page, sinon le lecteur ne sait
    # plus de quel côté est son appartement.
    dict(
        titre="Trous de 1 et 2 nuits restant à traiter à la main",
        slug="gaps-pricing",
        triggers=["gap_pricing_summary"],
        domaine="Ventes — Pricing",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        owner="Raphael (à confirmer)",
        depuis="17 mai 2026",
        source="trigger_gap_pricing_summary → dash_ventes_gaps (flag_action)",
        dashboard_url="https://direction.archides.fr/ventes?tab=calendrier&view=gaps_matrix",
        quoi=[
            "Chaque nuit, la machine repère les <strong>trous de calendrier de 1 ou 2 nuits</strong> (coincés "
            "entre deux réservations) que Beyond affiche <em>plus cher</em> que les nuits voisines alors qu'ils "
            "se vendraient en couvrant le coussin de marge, et les résume en 3 lignes dans le mail quotidien :",
            "• <strong>Gaps critiques</strong> : à combler sous 7 jours (dernière chance de vendre la nuit).<br/>"
            "• <strong>Gaps moyens</strong> : entre 8 et 14 jours.<br/>"
            "• <strong>Marge potentielle</strong> : ce que rapporteraient ces nuits si elles étaient vendues au "
            "prix des nuits voisines (déduction faite du ménage et du coussin de marge).",
            "Le décompte porte sur <strong>tout le parc</strong> : il inclut donc les appartements où la machine "
            "pose déjà elle-même le prix (voir « Push automatique des prix sur les nuits seules »), mais "
            "l'essentiel de ce qui reste est sur les appartements <strong>hors pilote</strong>, où le geste est "
            "manuel.",
            "Le bouton du mail ouvre la <strong>matrice des gaps</strong> (1.3 Calendrier), appartement par "
            "appartement et nuit par nuit.",
        ],
        exemple=[
            "Mail du 19/08/2026 : « 🔴 3 gaps critiques à baisser dans les 7 prochains jours » · "
            "« 🟡 7 gaps moyens (8-14 jours) » · « 💰 401 € de marge nette potentielle si tous les gaps ≤ 14 j "
            "étaient vendus au tarif adjacent ».",
        ],
    ),
    # ⭐ Fusion du 19/08 : l'ancienne règle « Surcote 1 nuit inefficace » (slug
    # surcote-1n-inefficace, trigger beyond_surcote_gap) a été absorbée ici. Elle
    # décrivait le même levier que le bloc « Modifier » de cette page, et depuis
    # la mesure du 16/08 la surcote n'est plus un sujet autonome : c'est le
    # mécanisme qui rend notre plafond inopérant. Sa page Confluence a été
    # archivée à la main (le sync ne supprime jamais).
    dict(
        titre="Push automatique des prix sur les nuits seules (gaps 1N/2N)",
        slug="beyond-push-gaps-1n",
        frequence_extra="Push des prix : quotidien 6h45, 10h45 et 20h45",
        # beyond_gap_filled = le mail 🎉 ; beyond_surcote_gap = le nudge surcote
        # (fusionné le 19/08) ; le push lui-même est un job autonome.
        triggers=["beyond_gap_filled", "beyond_surcote_gap"],
        live="beyond_push",
        domaine="Ventes — Pricing Beyond",
        niveau="N4", niveau_desc="la machine agit seule, l'humain audite a posteriori",
        owner="Raphael / Mickael",
        depuis="17 juillet 2026 (pilote 8 appartements — élargi à 31 appartements le 07/08, "
               "nuits orphelines sur tout le parc le 06/08, trous de 2 nuits activés le 10/08)",
        source="merveil-action-engine-beyond → dash_beyond_push_targets → API Beyond (seasonal-prices) → beyond_raw.price_pushes_log · trigger_beyond_surcote_gap → dash_beyond_gap1n_surcote",
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
            "sur <strong>l'ensemble du parc</strong>, pas seulement la whitelist. Là, c'est une protection du "
            "plancher, rien de plus : on empêche la nuit de partir à perte.",
            "Si les nuits voisines vendent sous notre plancher (fréquent sur les petits appartements), la "
            "fourchette devient un <strong>prix fixe rentable</strong> — mieux que le prix surcoté, jamais à perte.",
            "<strong>La surcote 1 nuit de Beyond, et pourquoi elle compte.</strong> Beyond majore "
            "automatiquement les nuits seules (en moyenne +88 %, jusqu'à +160 % selon l'appartement). Sur "
            "beaucoup d'appartements cette majoration est <strong>empiriquement inefficace</strong> : moins de "
            "5 % des nuits surcotées de 100 % ou plus se vendent — la nuit reste vide. Elle a donc été retirée "
            "sur les appartements du pilote (le 21/07 sur les 8 premiers, le 07/08 sur les 31 actuels), et "
            "c'est le <strong>prérequis</strong> à l'entrée d'un appartement dans le pilote.",
            "⚠️ <strong>Pourquoi ce prérequis, mesuré le 16/08</strong> : Beyond applique sa surcote "
            "<strong>après</strong> notre fourchette, pas avant. Sur le pilote, surcote retirée, le prix affiché "
            "est exactement notre fourchette. Sur les autres — ceux que couvre seulement la protection des "
            "nuits orphelines — la nuit reste publiée 1,5 à 2 fois au-dessus de notre plafond : <strong>le "
            "plancher tient, le plafond non</strong>. La nuit ne part jamais à perte, mais elle reste difficile "
            "à vendre. Sujet ouvert avec Beyond au point du 8 septembre.",
            "En attendant, un <strong>nudge quotidien</strong> signale les appartements <em>hors pilote</em> qui "
            "ont une nuit seule à vendre dans les 14 prochains jours ET une surcote flaggée inefficace — pour "
            "faire baisser le prix à la main, ou pour décider de basculer l'appartement dans le pilote.",
            "Les <strong>règles saisonnières posées par l'équipe dans Beyond sont préservées</strong> (un plancher "
            "équipe plus haut gagne toujours). Nuit vendue ou gap disparu → la fenêtre est retirée le lendemain. "
            "Chaque modification est journalisée (audit complet, réversible en un clic).",
        ],
        exemple=[
            "17/07/2026 : 1er run — fenêtres posées sur 10 nuits seules des 5 premiers appartements "
            "(ex. P11-RIC75-0F, nuit du 10/09 : fourchette 394 → 419 €, ADR voisin 469 €). "
            "Quand une de ces nuits se vend : mail 🎉 automatique avec le prix vendu, le canal et la fenêtre.",
            "19/08/2026, le nudge surcote : 14 appartements avec une nuit seule à vendre sous 14 jours et une "
            "surcote flaggée inefficace (P09-CAR7-0G, P03-MAR326-3D, P08-MON58-0G, P02-ABO52-2D…). "
            "<strong>Tous hors pilote</strong> — sur le pilote, la surcote a été retirée et la machine pose la "
            "fourchette elle-même.",
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
        exemple=[
            "Le cas à connaître, parce qu'il se produit régulièrement : un client déplacé d'un appartement à "
            "un autre (surbooking, travaux) voit sa réservation <strong>annulée puis recréée</strong> dans Mews. "
            "Le DWH publie fidèlement l'événement « réservation annulée », et customers.io peut lui envoyer une "
            "relance de récupération alors qu'il arrive le lendemain. Ce n'est pas une erreur de donnée : c'est "
            "la façon dont Mews enregistre un changement d'appartement, et c'est au ciblage de la campagne de "
            "l'exclure.",
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
        exemple=[
            "Mail du 19/08/2026 : 3 appartements disponibles dès le matin · 7 cet après-midi · 1 libre sur au "
            "moins 3 nuits à partir d'après-demain. Chaque ligne ouvre la liste filtrée des appartements "
            "concernés (6.0 Disponibilités).",
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
            "Deux destinations selon le mail : le <strong>brief de 11h</strong> ouvre la page 6.5 Annulations "
            "sur les dernières 24h ; les <strong>alertes ciblées du digest de 7h</strong> ouvrent le journal "
            "des annulations côté Ventes (1.6), avec le chiffre d'affaires qui part.",
        ],
        exemple=[
            "Alerte du 18/08/2026 : annulation sur un <strong>4 chambres</strong> (P08-PON48-1D, Champs-Élysées) "
            "pour un check-in au 28/08 — dix jours pour revendre des nuits chères en pleine saison. C'est le "
            "profil type de l'alerte « grand appartement » : peu de volume, mais chaque cas pèse.",
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
        dashboard_url="https://direction.archides.fr/ops-front?tab=arrivals",
        quoi=[
            "Trois surveillances qui ne se manifestent <strong>que lorsqu'il y a un cas</strong> (la plupart "
            "des jours : rien) :",
            "• <strong>Check-in last-minute</strong> : réservation prise très peu de temps avant l'arrivée → "
            "vérifier que ménage, code d'accès et accueil suivent.<br/>"
            "• <strong>Double booking</strong> : deux réservations actives qui se chevauchent sur le même "
            "appartement → à résoudre avant l'arrivée.<br/>"
            "• <strong>Check-in sans appartement</strong> : arrivée imminente sans espace assigné dans Mews.",
            "Les deux alertes liées à une arrivée ouvrent la page <strong>6.1 Préparation Arrivées</strong> ; "
            "le double booking ouvre le tableau des disponibilités (6.0), où le chevauchement se voit.",
        ],
        exemple=[
            "18/08/2026 — check-in last-minute : réservation en <strong>direct</strong> de 3 nuits (887 € TTC) "
            "sur P15-VIA29-1G, prise la veille de l'arrivée. Ménage, code et accueil à confirmer dans la journée.",
            "08/08/2026 — check-in sans appartement : arrivée Airbnb à <strong>J-1</strong>, 6 nuits, 8 276 € — "
            "aucun espace assigné dans Mews. L'alerte est repartie le lendemain, toujours non assignée : c'est "
            "exactement le cas qu'on ne veut pas découvrir le jour même.",
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
            "Deux destinations : l'alerte <strong>mauvais avis</strong> ouvre le post-mortem de l'avis "
            "(5.4 Post-mortem ≤ 3★, avec la conversation et l'historique de l'appartement) ; l'alerte "
            "<strong>Superhost</strong> ouvre la vue par appartement (5.2).",
        ],
        exemple=[
            "Alerte Superhost du 19/08/2026 : <strong>P01-CHE6-3F à 3,9/5 sur 8 avis</strong> des 3 derniers "
            "mois (🔴, sous 4,0) et P01-RAM80-1G à 3,7/5 sur 3 avis. Sur les OTAs, la note affichée de ces "
            "appartements est encore bonne — elle décrochera dans quelques semaines si rien ne bouge.",
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
        dashboard_url="https://direction.archides.fr/ops-back?tab=pin_pipeline",
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
            "Le code fixe historique reste en place en parallèle sur la plupart des appartements, mais sa "
            "<strong>suppression est engagée depuis la mi-août</strong> : il est retiré de Duve appartement "
            "par appartement, et les codes des appartements sensibles ont été <strong>changés</strong>. À "
            "terme, le code par séjour est le seul que le client voit.",
            "Sur une réservation jugée <strong>à risque</strong>, le code peut être retenu au lieu d'être "
            "envoyé — voir la règle « Réservation à risque à l'arrivée ».",
            "<strong>Si un client appelle sans code</strong> : la page Arrivées (6.1) affiche, sur sa "
            "réservation, le code généré et les codes de l'appartement (bouton « Afficher les codes »), les "
            "heures de validité, et le <strong>bouton d'ouverture à distance</strong> — la RC peut dépanner "
            "pendant l'appel, sans quitter la page.",
        ],
        exemple=[
            "Réservation arrivant vendredi sur un appartement basculé : mardi, la machine pose un code unique "
            "valable du vendredi 15h au lundi 19h. Le client le reçoit dans son message Duve habituel. "
            "S'il prolonge d'une nuit dans Mews, le code est étendu automatiquement au run suivant.",
        ],
        modifier=[
            "La liste des appartements basculés est un référentiel DWH : l'élargissement se fait par lots "
            "après période d'observation — demande à Hatim.",
            "⚠️ <strong>Basculer un appartement ne supprime pas son code fixe.</strong> Les deux coexistent "
            "tant que le code fixe n'a pas été retiré du champ Duve — ce retrait est un chantier "
            "<strong>séparé</strong>, engagé depuis la mi-août appartement par appartement. Tant qu'il n'est "
            "pas fait, Duve continue d'afficher le code fixe en repli et le code par séjour ne remplace pas "
            "vraiment l'ancien.",
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
            "iseo_clavier_muet",
            "iseo_quota_licence",
            "iseo_code_fixe_supprime",
        ],
        domaine="Serrures & accès",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        owner="Sylvain / Mickael (à confirmer)",
        depuis="13 juin 2026 (récap quotidien par cause : 5 août 2026 · HyperGates : 10 août 2026 · "
               "clavier muet & plafond de licence : 18 août 2026)",
        source="trigger_iseo_pin_missing · trigger_iseo_reconciliation · trigger_iseo_etl_stale · trigger_iseo_gateway_offline → dash_ops_pin_reconciliation · stg_iseo__gateways · gaps orchestrateur",
        dashboard_url="https://direction.archides.fr/ops-back?tab=serrures",
        quoi=[
            "Le filet de sécurité de la règle « Codes d'accès automatiques ». Trois surveillances tournent "
            "<strong>toutes les 2 heures</strong>, parce qu'un client peut être devant la porte :",
            "• <strong>Porte dormante</strong> : un client est censé séjourner sur un appartement basculé, "
            "aucun code n'est posé ET la porte n'a pas été ouverte depuis 48h → vrai risque d'accès.<br/>"
            "• <strong>Réconciliation</strong> : l'état interne est comparé à l'état réel des serrures Sofia. "
            "Code supprimé à la main dans l'interface, code orphelin, code encore actif après le départ → "
            "chaque écart est signalé (l'incident fondateur : un code effacé par erreur dans l'UI, client "
            "bloqué dehors). Le détail est dans le "
            "<a href=\"https://direction.archides.fr/ops-back?tab=pin_pipeline\">7.8 Pipeline PIN</a>.<br/>"
            "• <strong>Données en retard</strong> : si la collecte ISEO ne remonte plus, alerte — on ne "
            "surveille jamais à l'aveugle.",
            "Trois autres ne partent qu'<strong>une fois par jour</strong>, parce que ce sont des pannes à "
            "planifier et non des urgences de l'heure qui vient : passerelles hors ligne, serrures muettes au "
            "clavier, plafond de licence (détaillées ci-dessous).",
            "Un même problème n'est signalé qu'une fois tant qu'il n'est pas résolu (déduplication 4h).",
            "S'y ajoute depuis le <strong>10 août 2026</strong> la surveillance des <strong>HyperGates</strong> "
            "(les boîtiers qui relient les serrures au réseau) : une passerelle qui n'a plus donné signe de vie "
            "depuis <strong>plus de 3 jours</strong> est signalée dans le mail quotidien, avec la liste des "
            "serrures qu'elle dessert. Sans passerelle, la serrure fonctionne toujours au clavier mais on ne "
            "peut plus ni poser un code à distance, ni ouvrir la porte à distance, ni voir les ouvertures — "
            "c'est une panne à traiter sur place. Cette surveillance rend <strong>redondant</strong> le rapport "
            "horaire envoyé par ISEO : la même information est désormais dans le DWH.",
            "En complément, un <strong>récapitulatif quotidien</strong> (~8h45) liste les arrivées ≤ J+3 "
            "toujours sans code, <strong>classées par cause</strong> : formulaire pre-checkin non rempli, "
            "paiement en échec (le code est volontairement retenu), serrure non résolue, ou anomalie à "
            "investiguer. Le même état est visible arrivée par arrivée sur la page Arrivées (6.1, "
            "« Code d'accès ISEO »).",
            "Depuis le <strong>18 août 2026</strong>, deux surveillances de plus : "
            "• <strong>serrure muette au clavier</strong> — une serrure qui répond à distance mais dont les "
            "ouvertures au clavier ne remontent plus depuis deux semaines alors que des clients y séjournent : "
            "le journal des entrées est aveugle. Relance une fois par semaine tant que ce n'est pas réparé ; "
            "le geste, validé avec ISEO : <strong>redémarrer l'HyperGate depuis l'interface Luckey</strong> "
            "(2 minutes, à distance — l'historique gardé en mémoire par la serrure remonte tout seul).<br/>"
            "• <strong>plafond de licence Luckey</strong> — l'abonnement limite le nombre d'« éléments » "
            "(utilisateurs, serrures, invitations). Alerte à 90 % (critique à 96 %) : au plafond, "
            "<strong>plus aucun code ne peut être posé, sur tout le parc</strong> (vécu le 15/08 : 3 heures "
            "sans programmation).",
            "Depuis le <strong>1<sup>er</sup> septembre 2026</strong> : <strong>code permanent supprimé "
            "alors qu'il servait encore</strong>. Quand un code permanent disparaît, <strong>le code "
            "affiché au client dans Duve ne change pas tout seul</strong> : tant que personne ne le met à "
            "jour, le client reçoit un code qui n'existe plus. Déclencheur : la purge du 31 août, quatre "
            "codes très utilisés retirés d'un coup, aucune alerte, écart repéré le lendemain à la main.",
            "La surveillance regarde <strong>deux endroits, et il en faut deux</strong> : la "
            "<strong>serrure</strong> elle-même, qui confirme le retrait — c'est la preuve la plus solide, "
            "mais elle n'existe que si la serrure remonte encore son journal ; et l'<strong>inventaire "
            "Sofia</strong>, où l'on voit le code disparaître même quand la serrure est muette. Sans ce "
            "second regard, les trois logements les plus exposés du parc passaient au travers : un "
            "appartement <strong>sans passerelle</strong> (aucun journal, aucune ouverture à distance) "
            "dont le code supprimé était encore envoyé à un client attendu trois jours plus tard, et deux "
            "autres derrière une passerelle en panne alors que leur code servait plusieurs fois par jour. "
            "Une serrure dont le journal est mort ne peut pas signaler son propre problème.",
            "L'alerte devient <strong>critique</strong> quand la réception n'a plus de recours : soit il ne "
            "reste plus <strong>aucun</strong> code permanent à dicter au téléphone, soit la serrure ne "
            "peut être ni observée ni ouverte à distance. Elle sonne aussi sur les <strong>rotations "
            "volontaires</strong>, et c'est voulu : une rotation dont on oublie le champ Duve laisse le "
            "client devant la porte exactement comme une suppression par erreur. Le geste demandé est "
            "toujours le même — <strong>vérifier le code du logement dans Duve</strong>.",
        ],
        exemple=[
            "Trois alertes réelles du 18-19/08/2026, une par famille : "
            "<strong>passerelle</strong> — l'HyperGate de P15-LAO4-0G n'a plus donné signe de vie depuis "
            "43 jours (1 serrure desservie, plus de pose de code ni d'ouverture à distance possible) ; "
            "<strong>clavier muet</strong> — ce même appartement n'a remonté aucune ouverture au clavier "
            "depuis 44 jours alors que 11 séjours s'y sont succédé (le journal des entrées est aveugle) ; "
            "<strong>licence</strong> — 561 éléments sur 600, soit 93 %, environ 19 séjours de marge avant "
            "de ne plus pouvoir programmer un seul code.",
        ],
    ),
    dict(
        space="TRAN",
        titre="Réservation à risque à l'arrivée — alerte immédiate & code retenu",
        slug="resa-risque-acces",
        domaine="Serrures & accès",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        # La porte tourne en observation : elle journalise et alerte, mais laisse
        # partir le code. Afficher « Actif » ferait croire que des codes sont déjà
        # retenus. À repasser en ("Green", "Actif") le jour du ISEO_HOLD_MODE=on.
        statut=("Yellow", "Alerte active · porte en observation"),
        frequence="Réévaluation toutes les 2 h (à chaque passage de l'orchestrateur)",
        canal="Mail ⚠️ / 🔒 → hatim@archides.fr (bascule vers la RC prévue avant l'activation complète)",
        owner="Emilia / RC (à confirmer)",
        depuis="15 août 2026 (après trois tentatives de fraude en trois jours)",
        source="orchestrateur serrures (porte de validation) → iseo_raw.hold_decisions",
        dashboard_url="https://direction.archides.fr/ops-back?tab=pin_pipeline&view=hold",
        quoi=[
            "<strong>Le moment critique est le pre-checkin</strong> : c'est en le remplissant que le client "
            "fait apparaître son code d'accès — sur les fraudes d'août, il a été rempli 20 à 47 minutes "
            "après la réservation, en pleine soirée.",
            "<strong>Il n'y a plus de mail « au fil de l'eau »</strong> (coupé le 24 août 2026) : il "
            "signalait les arrivées du jour sans pièce d'identité scannée, soit ~36 par mois, alors qu'une "
            "fausse pièce passe l'OCR — un cas est avéré chez nous. L'information vit désormais dans la "
            "page <em>6.1 Préparation Arrivées</em>, où elle sert au triage sans réclamer un geste.",
            "<strong>Porte de validation</strong> : sur une réservation en direct de dernière minute, ou en "
            "direct avec un solde impayé significatif, le code est créé mais <strong>pas envoyé au "
            "client</strong> — la RC vérifie, puis le libère. ⚠ <strong>Mode observation aujourd'hui</strong> : "
            "la décision est journalisée et alertée, mais le code part quand même ; l'activation réelle se "
            "fera appartement par appartement, au fil du retrait des codes fixes.",
            "Chaque rétention envoie un mail qui dit explicitement lequel des deux cas s'applique : "
            "<strong>« ⚠️ résa à risque (code envoyé) »</strong> = surveiller · <strong>« 🔒 code retenu à "
            "valider »</strong> = le client n'a PAS de code, agir avant son arrivée.",
            "La <strong>pièce d'identité ne libère jamais le code automatiquement</strong> : scanner un "
            "document coûte 30 secondes à un fraudeur (un cas de fausse pièce est avéré). Elle sert au "
            "triage humain, pas à la décision machine.",
            "Où voir les décisions : dashboard 7.8, sous-onglet « Porte (décisions) » — chaque évaluation, "
            "son motif et son issue réelle (retenu / observation / non provisionné).",
        ],
        exemple=[
            "Les quatre fraudes d'août suivaient le même scénario : réservation en direct prise dans la "
            "journée, pre-checkin rempli 20 à 47 minutes plus tard en soirée, aucune pièce d'identité "
            "scannée — et le code d'accès apparaissait dans la foulée. L'alerte immédiate part désormais à "
            "ce moment-là, pendant qu'il reste quelques heures pour regarder le dossier.",
            "Les deux mails ne demandent pas le même geste : <strong>« ⚠️ résa à risque (code envoyé) »</strong> "
            "= le client peut entrer, on surveille ; <strong>« 🔒 code retenu à valider »</strong> = le client "
            "n'a rien reçu, il faut trancher <em>avant</em> son arrivée, sinon il est dehors le soir venu.",
        ],
        modifier=[
            "Les critères exacts (délais, seuils de solde) sont volontairement <strong>absents de cette "
            "page</strong> : une page qui décrit précisément comment la machine décide est aussi une recette "
            "de contournement. Ils sont gérés dans le DWH — demande à Hatim.",
        ],
    ),
    dict(
        space="TRAN",
        titre="Intrusion sur code fixe (logement vide)",
        slug="intrusion-code-fixe",
        triggers=["iseo_code_fixe_intrusion"],
        domaine="Serrures & accès",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        owner="Sylvain / Mickael (à confirmer)",
        depuis="18 août 2026",
        source="trigger_iseo_code_fixe_intrusion → dash_ops_lock_events × fct_reservations × Breezeway",
        dashboard_url="https://direction.archides.fr/ops-back?tab=lock_events",
        quoi=[
            "Les <strong>codes fixes</strong> sont les codes permanents d'appartement, partagés entre tous "
            "ceux qui les ont un jour reçus. Les fraudes de juillet en sont passées par là : 16 ouvertures "
            "sur un appartement, 62 sur un autre, <strong>sans qu'aucune alerte n'existe</strong>.",
            "Chaque jour, la machine repère toute ouverture par code fixe sur un logement <strong>vide</strong> "
            "— aucun séjour en cours, aucune tâche ménage/maintenance prévue à ±1 jour. Une ouverture "
            "la <strong>nuit</strong> (22h-7h) est classée critique.",
            "Volume attendu : ~2 cas par mois — quand ça sonne, ce n'est pas du bruit.",
            "<strong>Le geste</strong> : ouvrir le journal des serrures (7.9) pour voir qui, quand et avec "
            "quel code ; si l'ouverture n'est pas explicable (équipe, prestataire connu), faire "
            "<strong>changer le code fixe</strong> de l'appartement.",
            "Périmètre : <strong>tout le parc</strong> depuis le 19/08 (le journal couvrait avant les seuls "
            "appartements basculés — précisément pas ceux où vivent les codes fixes). Une ouverture sur une "
            "serrure dont l'appartement n'est pas reconnu au parc ne déclenche plus rien : ce garde-fou, posé "
            "le 19/08, a supprimé 28 jours d'alertes fantômes venues d'une serrure mal nommée.",
        ],
        exemple=[
            "Le cas fondateur, mesuré le 11/08/2026 sur P09-CAU28-2G : un même code fixe ouvre la porte "
            "<strong>quatre fois</strong> dans la journée — 14h58, 16h45, 17h40, puis <strong>23h58</strong>. "
            "Aucun client en séjour, aucune tâche ménage ni maintenance prévue ce jour-là ni la veille. "
            "L'ouverture de minuit fait passer l'alerte en critique. C'est le seul cas de ce type sur "
            "60 jours : quand elle sonne, ce n'est pas du bruit.",
        ],
    ),
    # ── CXSXJ — 2. Comptabilité (fraude & litiges bancaires) ─────────────────
    # Déplacée de TRAN le 19/08 : la règle naît d'un incident serrures, mais ses
    # owners (Emilia, Philippe) et sa page de suivi (9.9 Contestations) sont
    # comptables. Elle reste liée aux règles serrures par le texte.
    dict(
        space="CXSXJ",
        titre="Fraude — usurpation d'identité & chargebacks",
        slug="fraude-alertes",
        triggers=["fraude_identite", "new_chargeback", "payment_double_exit", "test_cartes"],
        domaine="Finance — Fraude & litiges",
        niveau="N2", niveau_desc="la machine surveille et alerte, l'humain décide",
        owner="Emilia (dossier & relation OTA) / Philippe (écriture comptable)",
        depuis="15 août 2026 (chargebacks) · 19 août 2026 (usurpation d'identité)",
        source="trigger_fraude_identite → int_reservations__risk · trigger_new_chargeback / trigger_payment_double_exit → paiements Mews",
        dashboard_url="https://direction.archides.fr/ops-front?tab=contestations",
        quoi=[
            "<strong>🚨 Test de cartes</strong> (depuis le 1ᵉʳ septembre 2026) : une réservation directe dont "
            "la banque a refusé <strong>deux cartes ou plus pour motif de FRAUDE</strong> déclenche un mail "
            "immédiat qui demande un geste : <strong>la RC vérifie puis ANNULE la réservation</strong> (accord "
            "direction du 31/08 — on a le droit, même réservée 2 jours avant). L'annulation n'est jamais "
            "automatisée. ⚠ Si un remboursement est fait : toujours sur la carte d'origine, jamais par virement.",
            "<strong>La machine blackliste elle-même</strong> (01/09) : chaque test de cartes crée aussi la fiche "
            "6.7 Blacklist automatiquement — en <em>vigilance</em>, jamais en interdiction (la machine propose, "
            "l'interdiction reste un verdict humain). Les profils multiples du même individu sont liés entre eux "
            "dans les notes. Et un client qui revient sous le <strong>même nom</strong> avec un nouvel email "
            "déclenche l'alerte blacklist si sa réservation porte elle-même un comportement de fraude — le mail "
            "dit alors « HOMONYME, vérifier l'identité », jamais « c'est lui ».",
            "<strong>Suspicion d'usurpation avant l'arrivée</strong> : la machine croise trois signaux — "
            "réservation directe au dernier moment · échec de carte avant un paiement accepté (sur une "
            "réservation directe) · nom du formulaire ou de la pièce différent du nom de la réservation. "
            "<strong>Deux signaux ou plus</strong> → mail « [Merveil Fraude] » sous 2 h (~2 cas/mois). Un "
            "signal isolé ne déclenche jamais de mail : il colore seulement le badge risque du dashboard "
            "(6.1 / 6.7).",
            "<strong>Nouveau chargeback</strong> : chaque litige bancaire ouvert par un client est signalé "
            "immédiatement. ⏱ <strong>Le dossier de contestation Adyen se dépose sous ~48 h</strong> — "
            "au-delà, le litige est perdu d'office (en 2026 : 1 chargeback récupéré sur 20).",
            "<strong>Double sortie d'argent</strong> : le même séjour remboursé au client ET perdu en "
            "chargeback = payé deux fois → mail immédiat.",
            "<strong>Le geste</strong> — avant la remise des clés : vérifier identité et paiement (appeler "
            "le client au besoin) ; après un chargeback : déposer le dossier sous 48 h et tout tracer dans "
            "la page <strong>9.9 Contestations</strong> (statut, dates, notes) — c'est elle qui répond à "
            "« qu'a-t-on contesté, qu'a-t-on récupéré ».",
        ],
        exemple=[
            "Cas réel (août 2026) : deux séjours directs réservés au dernier moment à trois jours "
            "d'intervalle, 3-4 échecs de carte avant chaque paiement accepté, pre-checkin du second séjour "
            "rempli sous un <strong>autre nom</strong> que la réservation (même téléphone, même e-mail), "
            "aucune pièce scannée. Le combo aurait déclenché le mail dans les 2 h — détecté rétroactivement, "
            "chargeback attendu.",
        ],
    ),

    # ── GDA — badge risque (visualisation, pas de mail) ──────────────────────
    dict(
        space="GDA",
        titre="Badge « résa à risque » sur les arrivées (6.1 / 6.7)",
        slug="resa-risque-badge",
        domaine="Opérations — Front office",
        niveau="N2", niveau_desc="la machine surveille et signale, l'humain décide",
        frequence="Recalculé toutes les 2 heures",
        canal="Badge dans le dashboard (6.1 Arrivées · 6.7 Résas à risque) — pas de mail",
        owner="Emilia / RC",
        depuis="21 juin 2026 (signaux affinés en août 2026)",
        source="int_reservations__risk → dash_ops_arrivals · dash_resa_risk",
        dashboard_url="https://direction.archides.fr/ops-front?tab=risque",
        quoi=[
            "Chaque arrivée des prochaines semaines porte un niveau de risque : <strong>rouge</strong> "
            "(vérifier avant l'arrivée) · <strong>ambre</strong> (garder un œil) · vert.",
            "<strong>Signaux forts</strong> — un seul suffit à passer au rouge : solde impayé significatif à "
            "quelques jours de l'arrivée · réservation directe de dernière minute · groupe de jeunes adultes.",
            "<strong>Signaux moyens</strong> — il en faut <strong>deux</strong> pour le rouge, un seul met en "
            "ambre : échec de carte sur une réservation directe · <strong>réservation OTA faite moins de "
            "48 h avant l'arrivée</strong> · réservation faite pour quelqu'un d'autre · plus de personnes que "
            "la capacité · séjour local court le week-end · motif « célébration » · solde impayé sur une "
            "arrivée encore lointaine.",
            "<strong>Signaux faibles</strong> — mettent en ambre, jamais en rouge à eux seuls : pre-checkin "
            "toujours manquant la veille · nom du formulaire ≠ nom de la réservation (rapprochement "
            "tolérant aux fautes de saisie et aux noms composés).",
            "La <strong>pièce d'identité</strong> n'entre PAS dans le score : elle est affichée en 6.1 comme "
            "information de triage (« sans pièce »), parce qu'un fraudeur peut en téléverser une fausse — "
            "un filtre qu'il contrôle ne doit pas décider qui on regarde.",
            "Quand <strong>deux signaux de fraude ou plus</strong> se combinent, un mail part en parallèle — "
            "voir la règle « Fraude — usurpation d'identité & chargebacks » (space Serrures & accès).",
            "<strong>Le geste sur un rouge</strong> : ouvrir la fiche, vérifier paiement et identité, appeler "
            "le client au besoin — <em>avant</em> la remise des clés, pas après.",
            "Anti-bruit : les paiements des réservations OTA (Booking, Airbnb, Expedia…) sont encaissés par "
            "la plateforme — l'absence de paiement dans Mews n'y est <strong>pas</strong> un signal, et ces "
            "réservations ne sonnent pas pour ça.",
        ],
        exemple=[
            "Photo du 19/08/2026 : sur <strong>648 arrivées à venir</strong>, 11 rouges, 93 ambres, 544 vertes. "
            "Le rouge reste rare par construction — c'est ce qui le rend actionnable : onze fiches à ouvrir, "
            "pas six cents.",
        ],
    ),
]

NIVEAUX = ("Grille de lecture des niveaux : <strong>N2</strong> = la machine surveille et alerte · "
           "<strong>N3</strong> = la machine <em>propose</em> une action, l'humain valide · "
           "<strong>N4</strong> = la machine agit, l'humain audite · <strong>N5</strong> = autonome.")

FOOTER = ('<em>Contact : Hatim (hatim@archides.fr) — pages générées automatiquement chaque matin, '
          'commentaires bienvenus, édition manuelle déconseillée.</em>')
