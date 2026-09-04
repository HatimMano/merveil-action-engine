# merveil-action-engine — Rules Engine

## Overview
Python 3.12 + **Cloud Run Job** (runs once and terminates — not an HTTP server).
**4 Cloud Run Jobs distincts** (1 par fréquence, FREQ hardcodé dans chaque job) :
- `merveil-action-engine` → FREQ=4h (dispatcher trigger/action) — **scheduler PAUSED depuis 2026-05-30**
- `merveil-action-engine-daily` → FREQ=daily (dispatcher trigger/action) — englobe maintenant tous les triggers
- `merveil-action-engine-cancellations-brief` → FREQ=cancellations_brief (standalone, court-circuite le dispatcher pour envoyer un rapport mail annulations 24h à 11h Paris ; cf. `src/handlers/cancellations_brief.py`)
- `merveil-action-engine-iseo` → FREQ=iseo_orchestrator (⭐ **aussi déclenché event-driven depuis le 03/09** : le webhook-gateway lance une exécution sur chaque `preCheckInDone` à CI ≤ J+3 — debounce 120s + skip-if-running côté gateway, latence form→code+push ~2-3 min ; le scheduler 2h reste le filet, cf. `webhook-gateway/CLAUDE.md` + plan du 03/09) (standalone, pipeline V3 ISEO — recreate PINs Sofia à J-3 du CI (J-7 → J-3 le 2026-07-13) avec MÊME deviceId capturé au pre-checkin done par webhook-gateway ; cf. `src/handlers/iseo_orchestrator.py`). Scheduler **`merveil-action-engine-iseo-2h` toutes les 2h à :45** Europe/Paris (`45 */2 * * *`) — passé de quotidien (7h30) à 2h le **2026-06-18** pour fermer le lockout near-CI + la latence d'annulation. Calé sur la cascade : ETL `:00` → dbt `:15` → dashboard `:30` → **orchestrateur `:45`** (fct_reservations frais, zéro chevauchement). Cf. ADR 2026-06-18.
- `merveil-action-engine-beyond` → FREQ=beyond_push (standalone, `src/handlers/beyond_push.py`) — **push déclaratif des fenêtres de prix sur les gaps vers Beyond** (1er N4 revenue, ADR 2026-07-17 ; élargi ADR 2026-08-06). **3 schedulers** : `merveil-action-engine-beyond-morning` **6h45** (après le dbt de 6h15 — pousse les nuits orphelines dès le matin) + `merveil-action-engine-beyond-daily` **10h45** + **`merveil-action-engine-beyond-evening` 20h45** (ajouté le 16/08). ⚠ **Le run du soir n'est pas un choix arbitraire** : mesuré sur 90 j, **10,9 % seulement** des réservations/annulations tombaient dans la fenêtre couverte 6h45→10h45, **89 % dans l'angle mort de 20 h** (pic 19h–minuit 25,5 %, minuit–6h45 22,3 %) — et le run de 10h45 fait **0 action 5 jours sur 6**. 20h45 minimise la latence d'exposition pondérée par le volume horaire réel : moyenne **9,3 h → 4,5 h**, pire cas **19,3 h → 9,3 h** (optimum plat de 19h45 à 22h45 ; 14h45 serait bien moins bon à 6,7 h car il laisse la nuit intacte). Calé sur la cascade (dbt 20h15, même décalage de 30 min que les deux autres). Coût : 4 Ko + 21 Ko scannés et ~2 min de Cloud Run par run, < 0,20 €/mois. ⚠ **Si l'API Beyond se plaint du débit** (limite inconnue ; le commentaire « l'API v1 est généreuse » date de l'époque des 6 listings, on en visite ~31 + les orphelines du parc entier) → **couper 10h45**, jamais 6h45 : sans 10h45 la latence moyenne reste à 5,68 h, alors que 6h45 est le seul run qui pose les nuits orphelines de la journée. Cf. ADR `Archides/docs/decisions.md` 16/08. État voulu = `dashboard_ventes.dash_beyond_push_targets`, **3 types de cibles depuis le 06/08** au grain fenêtre `(window_start, window_end)` (`_load_targets` filtre `window_end >= CURRENT_DATE` — la fenêtre tient jusqu'au soir de sa DERNIÈRE nuit, décision 04/08 cas MER21) : **gap_1n** (J+0..J+90, min = ménage+ops+coussin plein, start = end) · **gap_2n** (fenêtre start ≠ end — PATCH validé live sur le listing POC MAR359-1G — min = plancher par nuit ÷ 2 ; le min-stay 2 posé par Beyond sur ces trous empêche la vente 1N au plancher réduit ; 1re nuit passée → la cible sort du refresh dbt et la nuit restante redevient orpheline plancher plein) · **orphan** (nuit du jour J libre + arrivée demain + veille passée → plancher PLEIN posé pour la journée, max = ADR arrivant ÷ 1,12 ; détectée depuis fct_reservations car dash_ventes_gaps exclut les gaps au début passé ; **PARC ENTIER depuis le 06/08** — pure protection plancher. ⚠️⚠️ **CORRECTION 16/08 : l'ordre est l'INVERSE de ce qui était écrit ici.** La formule réelle, prouvée à l'euro sur 6 cas, est `prix posté = clamp(base, [min, max]) × (1 + surcote 1N)` — **Beyond applique la surcote 1N APRÈS le clamp**, pas avant. Conséquence : sur un appartement qui porte encore une surcote, la nuit orpheline est publiée **1,5 à 2,1× au-dessus de notre max** (ex. 16/08 : RIV85-1G 479 × 1,9 = 910 € posté ; GOD24-0F 474 × 2,1 = 995 €). **Le plancher tient** (`min × (1+surcote) ≥ min`, on ne vend jamais sous coussin) **mais le plafond ne tient pas** → la nuit devient invendable. Ligne de partage exacte : Fanny a retiré les surcotes le 07/08 **sur les 31 whitelistés seulement** (leur prix posté = notre clamp au centime), les **94 autres portent encore +88 % en moyenne, jusqu'à +160 %** — et ce sont eux que couvre le push orphelin parc entier. **Décision Hatim 16/08 : on ne touche à rien** — la règle « pas de résa sous coussin » est respectée, et une 1N vendue le jour même est rare et souvent à risque. À rediscuter au **call du 08/09** (élargir la whitelist n'est pas faisable à court terme, et compenser en divisant notre max par (1+surcote) piloterait à l'aveugle sur un seed périmé). ⚠ Corollaire pour le dashboard : un `au_dessus_fenetre` sur une nuit orpheline est **un vrai signal**, à ne pas neutraliser comme les 42 artefacts du 16/08. Cf. ADR `Archides/docs/decisions.md` 16/08 ; le job visite whitelist ∪ listings avec cible ∪ listings possédant une fenêtre, pour garantir le retrait J+1 hors whitelist). Whitelist = **`dwh_inputs.beyond_push_whitelist` + colonne `scope` (1N/2N/BOTH, NULL→1N)** via `stg_inputs__beyond_push_whitelist` — ⚠ le RuleEditor keyset insère sans scope (défaut 1N) ; passer un appart en 2N/BOTH = UPDATE SQL. **Whitelist live = 31 apparts** (28 × 1N le 07/08 après retrait surcotes Fanny, gaps 2N activés le 10/08 sur 5 apparts — 14 fenêtres) ; les nuits orphelines sont ACTIVES sur le PARC ENTIER (dégatées de la whitelist le 06/08). Par listing : GET seasonal-prices → sépare fenêtres DWH (registre = `beyond_raw.price_pushes_log`, ownership par (listing, start, end) dernière action ≠ remove — Beyond n'a pas de champ label) / règles équipe (préservées telles quelles, rollover inclus) → PATCH **seulement si diff** (support Beyond : 1 envoi suffit). ⚠ **Beyond TRONQUE le start des fenêtres au jour courant** (incident ROY15-6D 11/08 : la 2N 10→11/08 servie 11→11/08 le lendemain matin passait pour une règle équipe → doublon de plage exact avec l'orpheline du jour → PATCH 400 `Cannot have overlapping date ranges` → nuit protégée au plancher ÷2 au lieu du plancher plein). Fix : le matching accepte la **clé effective** (start clampé à aujourd'hui, Europe/Paris) ; une clé possédée entièrement passée est inerte (Beyond droppe la fenêtre du GET) ; garde anti-doublon : une règle équipe posée exactement sur une fenêtre voulue est **absorbée** (son min a déjà relevé le nôtre) au lieu de dupliquer la plage. Fenêtre = `[ménage+ops+COUSSIN (posé tel quel en prix Beyond, marge nette ≥ coussin), max(min, ADR voisin ÷ 1,12)]` — règle Hatim 17/07 « mieux vaut ne pas vendre que vendre sous coussin » : ADR voisin sous coussin → prix FIXE rentable, aucun gap exclu ; plancher équipe plus haut relève le min, plus bas bypassé ; gap comblé → fenêtre retirée au run suivant (déclaratif, latence 24h). `BEYOND_SHADOW_MODE=true` = dry-run. Secret `BEYOND_PAT` = `beyond-pat-dwh`. Mail erreurs → `BEYOND_ALERT_TO`. **Garde-fou bornes prix (2026-08-01)** : dernier filet Python avant PATCH, indépendant du SQL amont — fenêtre hors `[BEYOND_PRICE_FLOOR=50, BEYOND_PRICE_CEILING=5000]` €/nuit HT (bornes du test dbt ADR) → écartée de l'état voulu (donc retirée de Beyond si possédée), log `action='skip' status='error'` + erreur mail. Couvre target dbt corrompu (ex. coussin cassé par le référentiel Sheets), plancher équipe extrême, et prix NULL (coercé -1 → recalé, le run continue). `[0,0]` ne peut plus partir.
- `merveil-action-engine-2h` → FREQ=2h (dispatcher, bucket `2h`) — **alertes serrures ISEO** (`iseo_pin_missing`, `iseo_etl_stale`, **`iseo_reconciliation`**). Scheduler **toutes les 2h à :50** Europe/Paris (`50 */2 * * *`) — ⭐ **passé de `:00` à `:50` le 2026-09-04**. À `:00` le dispatcher tournait AVANT le dbt de la même cascade (ETL `:00` → dbt `:15` → dash `:30` → ISEO `:45`) : il ne lisait donc jamais que des triggers vieux d'**1h45**. Mesuré sur le cas Francis Rouet (résa 61213, 03/09) : créée 17h39 → trigger dbt 18h15 → mail 20h01, soit **2h22**, et la RC avait déjà annulé à 18h50 — l'alerte est arrivée après le geste qu'elle demandait. À `:50` le même cas partait à 18h50 (latence max 2h → 1h50, moyenne ~55 min). ⚠ Vaut pour TOUS les buckets flushés par ce job, pas seulement les serrures : `fraude`, `blacklist`, `chargeback` portent `flush_with: 2h`. Bucket `2h` TTL 4h (dédup). Destinataires : `digest_buckets.2h.default_recipients` dans `routing.yaml` (⚠ le flush n'utilise PAS `params.recipients` per-trigger). Cf. ADR 2026-06-13 + 2026-06-18. **`iseo_reconciliation` recréé 2026-07-06** sur le nouveau modèle `dash_ops_pin_reconciliation` (V3) : `MISSING_IN_SOFIA` (cache actif mais device absent Sofia = code supprimé UI / recreate planté = **incident fondateur Crystal Balcom**, CRITICAL), `ORPHAN_SOFIA` (device Sofia sans cache actif), `STALE_AFTER_CO` (cache actif CO passé = archive KO). L'ancien PHANTOM/DRIFT (schéma capture/recreate) était bien supprimé le 20/06 → la réconciliation a été **absente entre-temps**.
- `confluence-rules-sync` → **SANS FREQ** (command override `python -m utils.confluence_sync`, ne passe pas par main.py → le fail-fast FREQ ne s'applique pas) — **pages vivantes Confluence** : régénère les pages « Règle — … » des spaces métier VD/GDA/TRAN + index global EN (merveil.atlassian.net) depuis la prose `RULES` + blocs vivants BQ. Scheduler `confluence-rules-sync-daily` **daily 7h40 Paris** (après le digest 7h → « dernière activité » fraîche). Cf. section dédiée.

9 Cloud Schedulers : **daily 7h + 2h serrures (:50, ex-:00 — cf. ci-dessus) + iseo 2h (:45) + confluence-sync 7h40 + 11h cancellations + beyond 6h45, 10h45 & 20h45 ENABLED** (prod), 4H + weekly PAUSED.
Reads pending actions (Breezeway) + rule tables (digest) produced by dbt.

### Bascule 4h → daily (2026-05-30)
TTL du bucket 4h = 4h = fréquence du job → dédup neutralisée → ~60 alertes renvoyées à chaque cycle.
Fix : les 5 triggers anciennement bucket `4h` (cancellation_vip, satisfaction_low_review, last_minute_checkin, double_booking, checkin_no_apartment) ont été basculés en `bucket: daily` dans `config/routing.yaml`. Le scheduler `merveil-action-engine-4h` a été pausé. Pas de perte d'info : annulations VIP restent dans le mail RC 11h `cancellations-brief`.

### Fix explosion alertes daily — borne 24h sur `_load_triggers` (2026-06-17)
`action_engine.triggers` est append-only (dedup par jour). Le dispatcher faisait
`SELECT * FROM triggers` sans borne → rechargeait tout l'historique ; les triggers
anciens, dont le `dispatched_action` avait dépassé son TTL, étaient renvoyés à chaque
run (~1000 alertes). Fix : `_load_triggers` filtre `detected_at >= now - 24h`. Un trigger
persistant génère une ligne fraîche par jour donc reste capté ; borne ≤ TTL daily (24h)
→ pas de double envoi inter-jour. Uniforme pour tous les buckets (daily, 2h serrures, manuel).

### Mode `cancellations_brief` (2026-05-23)
Mail récap quotidien envoyé à 11h Paris à `alerte_ventes@archides.fr` + `emilia@archides.fr` (override via env `CANCELLATIONS_TO`). Ne passe **pas** par le dispatcher trigger/action — c'est un rapport, pas un événement déclencheur. Query directe sur `dashboard_ops.dash_ops_cancellations_recent` filtré sur `cancelled_at >= NOW() - 24h`. HTML minimaliste (KPIs + table compacte) avec bouton `Voir le détail dans le dashboard →` qui pointe vers `https://direction.archides.fr/ops-front?tab=cancellations&preset=24h`. Reuse infra Gmail API (secret `alerts-gmail-sa-key` + Domain-Wide Delegation existante).

### Mode `confluence-rules-sync` — pages vivantes Confluence (2026-07-21, multi-spaces 2026-07-22)
La doc des règles machine devient une **conséquence** de l'état réel, plus une action oubliable. Job standalone (`utils/confluence_sync.py`, exécuté par command override — pas de FREQ), daily 7h40 Paris. Pour chaque règle de la constante `RULES` : upsert (jamais de delete) de la page « Règle — <titre> » dans son space métier, avec pour les règles portant un champ `live` un panel « 📊 État actuel — généré automatiquement le … » requêté dans BQ au run. Collectors : `beyond_push` (whitelist `dwh_inputs.beyond_push_whitelist`, audit `rules_audit_log`, fenêtres actives + dernier run `price_pushes_log`, dernière nuit vendue via `beyond_gap_filled`) + `iseo` (whitelist seed `staging.iseo_whitelisted_apartments`, codes actifs/à venir `iseo_raw.merveil_pin_cache`, provisions 7j, écarts `dash_ops_pin_reconciliation`). Une édition whitelist depuis le dash (rules-edition) est reflétée au run suivant (+ déclenchement fire-and-forget depuis `POST /api/rules`, debounce 30s — V2 du 21/07).
- **Multi-spaces (2026-07-22)** : registre `SPACES` — `VD` Ventes (4 règles) · `GDA` Opérations (4 : digest dispo, annulations 11h+ciblées, alertes séjour, suivi avis/superhost) · `TRAN` Serrures & accès (2 : orchestrateur N4 avec bloc vivant `iseo`, surveillance 2h). 1 page racine « 🤖 Automatisations (DWH) — <domaine> » par space, posée en sibling des sections existantes (contenu préexistant jamais touché). **Index global cross-spaces** « 🤖 Tout ce que fait la machine » dans `EN` (00. Entreprise) : report CQL `label = "regle-dwh"` sans filtre space + liens vers les racines. ⚠ COR (0. Finance) est **archivé** → futur lot finance dans `CXSXJ`. Chaque règle porte `space` (défaut VD) ; nouveau domaine = 1 entrée `SPACES`.
- ⭐ **Split prose / moteur (2026-08-12)** : la prose vit dans **`utils/confluence_rules.py`** (`SPACES`, `RULES`, `NIVEAUX`, `FOOTER` — ~400 l de contenu pur), `confluence_sync.py` ne garde que le moteur (~490 l). ⚠ **La négation `.gitignore` a été ajoutée** (`!utils/confluence_rules.py`) — sans elle le module est absent de l'image.
- ⭐ **Faits dérivés de `routing.yaml` (2026-08-12)** — une règle qui déclare **`triggers=[...]`** ne porte plus à la main son statut, sa fréquence, son canal ni ses destinataires : `derive_facts()` les lit dans le routing au run (statut `Actif` / `Partiellement active (n/m)` / `Inactive` / `Inconnu du routing` ; canal = `digest_buckets[b].subject_prefix` + `default_recipients` ; fréquence = `BUCKET_CADENCE`, seul mapping resté déclaratif car portée par les schedulers). **Avant, le statut était `Actif` en dur** → une règle `enabled: false` s'affichait active sur Confluence. Règle hors dispatcher (job autonome, outil externe) = pas de `triggers` → champs `frequence`/`canal` manuels. Part hors routing greffée sur une même règle = `frequence_extra` / `canal_extra` (ex. brief annulations 11h, push Beyond 6h45/10h45, récap serrures 8h45).
- **Contrôle de couverture** : `coverage_gap()` logue en fin de run les triggers `enabled` qu'aucune règle ne documente. **10 au 19/08** : `abandoned_cart`, `low_occupation`, `low_review_cleanliness`, `revenue_anomaly_adr`, `revenue_anomaly_adr_per_room`, `revenue_apt_zero`, `data_contract_breach`, `dbt_test_failure`, `finance_flow_stale`, `genai_stale` (les 3 derniers = monitoring IT interne, pas de prose Confluence, assumé).
- ⭐ **Contrôle des liens (2026-08-19)** : `link_gap()` confronte le `dashboard_url` de chaque règle au seed **`staging.dashboard_routes`** (inventaire page/onglet/sous-onglet + drapeau `_dev`, généré depuis le code du front par `merveil-dashboards-v2/scripts/extract-routes.mjs`) et logue en warning `lien mort : <slug> : <cause>`. **Même référentiel que le test dbt `assert_dashboard_links_valid`**, qui fait le même contrôle sur les `action_url` des mails — doc et mails ne peuvent plus dériver chacun de leur côté. ⚠ Le seed est un snapshot : le **régénérer** après tout ajout/renommage/passage en `dev` d'un onglet, sinon le contrôle valide un front qui n'existe plus. Ce qu'il ne voit pas : qu'un lien valide ouvre la BONNE vue (`gaps_actions` était un alias parfaitement valide vers un tableau alimenté par un autre modèle que celui du mail).
- ⭐ **Cross-links dans les pages process de l'équipe (2026-08-12)** : registre `CROSSLINKS` (dans `confluence_rules.py`) → `sync_crosslinks()` pose un encart « 🤖 Assisté par la machine (DWH) » (panel info + liens vers les pages Règle) dans les pages process concernées, en fin de run. 2 pages au 12/08 : VD `1.0. Ventes - Vérifications Quotidiennes` (378077185) + TRAN `2. Gestion des clés` (98830694). `_splice()` remplace le bloc existant par son marqueur, sinon l'ajoute en fin de page — **le reste de la page n'est jamais touché**. `CROSSLINK_DRY_RUN=1` = visualiser sans écrire. ⚠ **2 pièges vérifiés en écrivant chez les autres** : (a) Confluence **échappe les accents** du storage (`é` → `&eacute;`) → le marqueur DOIT être ASCII pur (il est planqué dans l'URL des liens : `?src=dwh-crosslink`) — un marqueur unicode n'est plus retrouvable au run suivant et l'encart se ré-ajoute chaque jour ; (b) Confluence **ajoute `ac:macro-id`/`ac:schema-version`** au 1er enregistrement → comparer brut ferait un PUT (donc +1 version) par jour dans une page qui n'est pas la nôtre — `_norm()` normalise avant comparaison (no-op vérifié en prod : `cross-link inchangé`). Étendre = 1 entrée `CROSSLINKS` ; **ne cibler qu'une page dont le contenu décrit vraiment le geste automatisé** (les pages FRONT OFFICE GDA = supports de formation 2021, écartées).
- **Étendre à une autre règle** : ajouter l'entrée dans `RULES` (+ `space`, + `triggers` si elle passe par le dispatcher) ; bloc vivant = champ `live="<clé>"` + collecteur dans `LIVE_COLLECTORS` (retourne `{generated_at, last_run?, items[]}`).
- **Éditer la prose** : modifier `RULES` dans **`utils/confluence_rules.py`** (l'ancien script local `Archides/docs/confluence/build_rules_merveil.py` = stub pointeur), puis laisser le daily tourner ou `gcloud run jobs execute confluence-rules-sync`.
- **Règles documentées (14 au 19/08)** : VD → budget, **trous 1N/2N restant à traiter à la main** (ex-« Gaps de pricing »), push Beyond 1N/2N, **séquences CRM customers.io** (N4, seule règle où la machine écrit au client ; ⚠ aucune stat d'envoi/ouverture côté DWH — le retour CIO→BQ exige un abonnement premium) · GDA → digest dispo, annulations, alertes séjour, suivi avis, badge résa à risque · TRAN → codes d'accès auto, surveillance serrures, résa à risque (porte), intrusion code fixe · **CXSXJ → fraude & chargebacks** (déplacée de TRAN le 19/08 : owners Emilia/Philippe, page de suivi 9.9).
- ⭐ **Fusion surcote 1N → push Beyond (19/08)** : l'ancienne règle `surcote-1n-inefficace` décrivait le même levier que le bloc « Modifier » de la page push, et depuis la mesure du 16/08 la surcote n'est plus un sujet autonome — c'est le mécanisme qui rend notre plafond inopérant. Son trigger `beyond_surcote_gap` est porté par la page push. ⚠️ **Renommer ou fusionner une règle laisse une page orpheline** : l'identité d'une page est son titre et le sync ne supprime jamais → retirer le label `regle-dwh` puis archiver à la main (procédure dans la docstring d'`upsert`).
- ⭐ **Champ `statut` optionnel (19/08)** : une règle sans `triggers` (hors dispatcher) s'affichait toujours `Actif` — la porte de validation, en observation depuis le 15/08, se lisait donc comme si des codes étaient déjà retenus. Elle déclare `statut=("Yellow", "Alerte active · porte en observation")`. À repasser en `("Green", "Actif")` le jour du `ISEO_HOLD_MODE=on`.
- ⚠ **Cadence d'un bucket satellite** : `BUCKET_CADENCE` ne liste que les buckets qui ont leur PROPRE scheduler ; les autres sont résolus par leur `flush_with` (`_cadence`). Sans ça un nouveau bucket affiche son nom technique en fréquence — la page fraude allait sortir « Fréquence : fraude » (corrigé 19/08).
- ⚠ **CONTRAT ANTI-DÉRIVE : toute modif du COMPORTEMENT d'une règle (handler, seuil, fenêtre, périmètre) = mise à jour de sa prose `RULES` dans le MÊME commit.** Les blocs vivants (whitelists, activité) se corrigent seuls à chaque run ; la prose, non — c'est le seul maillon humain restant. Décision 2026-08-04 : la prose reste dans le code (même repo = même commit = cohérence atomique) tant qu'Hatim est le seul à l'éditer ; migration vers une table `dwh_inputs` éditable depuis le dash (pattern rules-edition + `rules_audit_log`) le jour où quelqu'un d'autre doit écrire, ou si la cadence d'édition dépasse ~1/semaine.
- **Secrets** : `confluence-api-token` + `alerts-gmail-sa-key` lus via l'API Secret Manager au runtime (rien dans `--set-secrets`). Crash → mail `CONFLUENCE_ALERT_TO` (défaut hatim) + exit 1.
- ⚠ **`.gitignore` : `utils/` est ignoré SAUF `confluence_sync.py`** (`utils/*` + négation). `gcloud builds submit` génère son ignore depuis `.gitignore` → un module prod ignoré = absent de l'image (`ModuleNotFoundError`, vécu au 1er deploy). Tout futur module prod dans `utils/` doit être ajouté en négation.

### Mode `iseo_orchestrator` — Pipeline V3 100% DWH (refonte 2026-06-20, natif coupé)
Cf. [[project_iseo_integration_2026]] + `Archides/to_do_20_06.md`.

⚠️ **Refonte majeure 2026-06-20** : l'intégration native Duve↔Sofia est **coupée dans les 2 sens** (création Duve→Sofia + livraison Sofia→Duve). Le DWH est **seul maître** du cycle PIN. **Le cacher `webhook-gateway/iseo_pin_cacher.py` est retiré** (plus de `DUVE_PIN` à capturer). L'ancienne logique capture/recreate + PHANTOM/DRIFT est **supprimée**.

⭐ **Pivot "stay" (2026-07-23) — remplace le pivot par résa Duve.** L'unité de provisioning n'est plus la résa Duve mais le **stay** = occupation CONTINUE d'un guest sur une serrure = regroupement (gaps-and-islands, CTE `_STAYS_CTE`) des résas Mews non annulées d'un même `(customer_id, resource_id)` aux intervalles `[CI,CO]` contigus/chevauchants. Fenêtre = `min(CI)→max(CO)`, **1 seul code**, identité = `canonical_duve` (duve de la résa la plus tôt), `member_duve_ids` = tous les duve du stay (le code est poussé à chacun via `_duve_push_all`). Un client avec ≥2 résas back-to-back sur le même appart = 1 code fusionné ; un TROU entre 2 périodes = 2 stays = 2 codes. **Résa unique (99%) = strictement identique à l'ancien comportement, zéro migration.** Attache Duve↔résa par date avec tolérance -14j (robuste au drift post-pré-checkin → pas d'archivage à tort). Fixe le bug d'oscillation de fenêtre toutes les 2h quand le join Duve↔Mews `(customer,resource)` était ambigu (cas Amy Kultgen P02-CAI31-3D, 2 résas 22→26 + 26→28 : fenêtre du PIN basculait entre les deux → guest lockout). Cache : colonne `stay_member_duve_ids` (CSV des duve du stay). Les 3 requêtes `_resa_to_{provision,resync,archive}` pivotent sur le stay (dédup sur canonical ET membres → jamais 2 devices/stay ; archive = duve absent de tout stay live).

⭐ **Borne BASSE sur la création — 2026-08-26 (ADR).** `_resa_to_provision` n'avait qu'un plafond sur le check-in : un séjour déjà commencé sans ligne de cache active était re-sélectionné **à chaque run**, et un code lui était fabriqué en pleine occupation. C'était le rattrapage qui sauvait les résas débloquées le jour J tant que le **gate paiement** empêchait la création (cas Goldwyn 22/08) ; depuis que la création est inconditionnelle (25/08), le code existe depuis J-3 et seul le push Duve reste à décider → un code créé au 3ᵉ jour de séjour n'ajoute qu'un **élément de quota Luckey**, à un client qui a déjà le sien. Borne posée : `stay_ci >= J-1`. ⚠ **Ne borne QUE la création** — `_resa_to_resync` part du cache et doit tourner tout le séjour (drift de dates, early CI / late CO achetés après coup). ⚠ **`_whitelisted_gaps` porte la MÊME borne** : son mail promet « un code aurait dû être généré », ce qui cesse d'être vrai hors de la fenêtre de création — sans quoi il accuserait chaque jour, indéfiniment, pour une action que la machine ne tentera plus. Le séjour en cours sans code reste visible en 6.1 (`pin_state`). Fenêtre de rattrapage humain = **J et J+1**. Mesuré le 26/08 : 3 séjours en cours concernés, tous déjà non provisionnables (2 sans pré-checkin, 1 sur la passerelle morte LAO4).

**Provision (J-3, défaut `ISEO_LOOKAHEAD_DAYS=3` — J-7 jusqu'au 2026-07-13)** — `_resa_to_provision()` : **stays** (cf. pivot ci-dessus) non annulés, `stay_ci ∈ [today-1, today+3j]`, CO futur, **pas déjà couverts par une row de cache active** (`archived_at IS NULL`, ni par le canonical ni par un membre). Pour chacun (`_provision`) :
- **A.** génère un code PIN **4 chiffres** unique account-wide (retry sur `already present`).
- **B.** crée (get-or-create par extId `MERVEIL_USER - <duve_id>`) un **user Sofia dédié à la résa, au vrai nom du guest** (firstname/lastname), **avec un password aléatoire** (jamais partagé) → le user est `enabled=True` et son tag `user` auto-créé sert de `guestTagId` → l'UI Luckey affiche le vrai nom. Puis `POST /api/v2/standardDevices` (extId `MERVEIL_RESA - <duve_id>`, credentialRule sur ce guest tag + le lock tag de l'appart). Le device ancre le user. ⭐ **Window — refondue le 2026-08-25 (`_stay_hours`, ADR)** : `ouverture = min(politique appart, early check-in ACHETÉ, ISEO_DEFAULT_CI_HOUR=13:00)` plancher **07:00** · `fermeture = max(politique, ISEO_LATE_CO_HOUR=18:00 si late check-out acheté, ISEO_DEFAULT_CO_HOUR=11:00)` plafond **23:59**, tz Paris. **La fenêtre ne peut que s'élargir, jamais se rétrécir.** Avant, seule la **politique** annoncée au pré-checkin Duve était lue — une heure d'intention, jamais l'heure **payée** : mesuré le 25/08, **12 des 15** early check-ins tombés sur un appart intégré avaient un code qui n'ouvrait pas à l'heure achetée (cas Hilary LaBrash, 140 € pour 11 h, code valable à 16 h). Source des 2 nouveaux champs : `fct_reservations.purchased_early_checkin_hour` / `has_purchased_late_checkout` (modèle `int_reservations__checkin_services`, cf. dbt CLAUDE.md). ⚠ **`ISEO_LATE_CO_HOUR=18:00` est un PARAMÈTRE, pas une mesure** : Duve ne transmet pas l'heure prolongée (`delivery_at` vaut l'heure standard sur 10 des 11 orders) → on ne lit qu'un booléen, 18 h = la promesse produit (arbitrage Hatim 21/08). ⚠ Agrégation au **stay** : `MIN` de l'heure et `LOGICAL_OR` du booléen sur les membres d'un back-to-back — élargir une fenêtre déjà payée ne coûte rien, la rater laisse un client à la porte.
- **C.** `POST /api/v2/invitations` (extId `MERVEIL_INV - <duve_id>`, `smartLockIds=[lock_id]`, `numberOfDevices:0`) → `code` → lien `https://archides.jago.cloud/remoteOpen?code=<code>` (⚠️ host `archides`, PAS le `api-archides` renvoyé par l'API). Lien gated sur la window (OK pendant le séjour).
- **D.** `POST` intégration entrante Duve (`DUVE_CONNECT_URL?pid=DUVE_CONNECT_PID`, secret `duve-connect-token`) : `primaryCode` (code clavier) + champ `merveil_paris_iseo_access_link_eIhhEnlspM` (lien). N'émet aucun message (messages auto Duve lisent le champ).
- **E.** INSERT état dans `iseo_raw.merveil_pin_cache` (colonnes `iseo_device_id`, `iseo_invitation_id`, `invitation_code/link`, `provisioned_at`, `duve_pushed_at`). Device + invitation sont **get-or-create par extId** → idempotent sur retry partiel.

**Résolution ids appart (JOIN BQ, pas de seed)** : `duve property_id (GUID) == Mews resource_id == nom du lock tag`. → `lock_id` + `lock_tag_id` via `stg_iseo__smart_locks`. Le `guest_tag_id` n'est **plus** dérivé de l'appart : c'est le tag `user` du user dédié de la résa (créé en B). `customer_name` vient de `fct_reservations`.

⭐⭐ **LE PAIEMENT N'EMPÊCHE PLUS LA CRÉATION — 2026-08-25 (ADR).** `_provision` portait un `if row.get("payment_unpaid"): return False, "skipped: paiement non validé"` qui s'exécutait **avant** la porte : aucun code créé, donc rien à lire au dashboard, et la RC en fabriquait un à la main le jour de l'arrivée (cas Liliana Goldwyn, 22/08). Deux organes retenaient, un seul alertait. Le gate était en plus **faux sur un angle mort mesuré** : un paiement porté par un compte `Company` avec `reservation_id` NULL lui reste invisible pour toujours — **59 résas sur 287** à échec restent « impayées » à ses yeux sur 120 j.
- **Désormais** : `_provision` ne refuse que sur des **prérequis techniques** (whitelist, serrure non résolue, dates absentes, checkout passé) — sans eux l'appel Sofia n'a pas de sens. Un commentaire y interdit explicitement d'en remettre un.
- **Le paiement est le 3ᵉ critère de `_evaluate_hold`**, et c'est le **seul applicable à TOUS LES CANAUX**. C'est sûr parce qu'il exige une tentative **refusée** (`n_failed > 0`) : une résa Booking/Airbnb payée à l'OTA n'a aucun paiement dans Mews. ⚠ Ne PAS le confondre avec `direct_unpaid`, qui se lit sur le solde seul et retiendrait **69 %** des arrivées s'il était généralisé.
- ⚠ **`ISEO_HOLD_MODE=off` désactive donc AUSSI le contrôle paiement.** Prix assumé de l'organe unique — le passer à `off` ne se fait plus « juste pour couper la porte fraude ».
- ⚠ **Garde-fou anti-boucle d'alerte** (`_hold_already_notified`) : en mode `observe` le cache ne porte PAS de `hold_reason`, donc la garde `cache_hold` du resync était **inopérante** et chaque resync renvoyait le mail. Invisible tant que la porte ne retenait que ~12 directs/mois ; en y versant le paiement (tous canaux), le resync devenait un émetteur régulier. La question « a-t-on déjà prévenu ? » se pose **AVANT** `_log_hold_decision`, sinon la query voit la ligne qu'on vient d'écrire et le mail ne part jamais.
- **Aval** : `bloque_paiement` retiré de `pin_state` (`dash_ops_arrivals`), absorbé par `retenu` + colonne `pin_hold_motif` ; `dash_360_resa_fleet` repointé ; miroir `dash_ops_hold_simulation` aligné (`critere_paiement`). Colonne `payment_unpaid` ajoutée à `iseo_raw.hold_decisions`.
- ⚠ **Ordre de déploiement : action-engine AVANT dbt.** L'inverse fait tomber les résas impayées dans `manquant`, qui affirme « aurait dû être généré ».
- ⚠ La cause `paiement` du mail quotidien « résas sans code » devient **inatteignable** — elle est gardée comme **détecteur de régression** : si elle se remet à compter, un blocage a été réintroduit avant la création.

⚠️⚠️ **GARDE-FOU HYPERGATE — 2026-08-19 (ADR), ÉTENDU LE 2026-08-25.**

⭐⭐ **3ᵉ condition ajoutée le 25/08 : `push_stuck` — « elle pingue » n'est PAS « elle écrit les codes ».** Les deux tests d'origine lisent la **connexion** de la passerelle, et l'incident du 08-24/08 a montré que ça ne suffit pas : les six passerelles fautives pinguaient toutes à la minute — donc vertes ici, vertes dans le staging, vertes dans le trigger `iseo_gateway_offline` — pendant que **sept séjours** recevaient un code que la serrure n'a jamais appris. **`P15-LAO4-0G` a tenu SEPT SEMAINES** dans cet état, et le garde-fou posé le 19/08 précisément pour ce cas l'a laissé passer. L'observable est le statut du dernier push `CREDENTIALS_UPDATED` (`stg_iseo__gateway_push_health`, alimenté par le flux ETL `gateway_push_health`). ⚠ `COALESCE(ph.push_stuck, FALSE)` et pas TRUE : tant que le flux n'a pas tourné, l'absence de mesure ne doit pas bloquer le parc. **Mesuré au 25/08 : 11 passerelles bloquées, dont 6 portant des apparts intégrés.** Alerte associée : trigger **`iseo_gateway_push_stuck`** (bucket **daily** — un état persistant ne se répète pas 12×/jour, leçon LAO4 du 20/08) ; il ne double PAS `iseo_gateway_offline`, qui lit la connexion et ne voit donc que les débranchées. Le mail quotidien « résas sans code » distingue les deux causes, qui n'appellent pas le même geste : débranchée → se déplacer · répond mais n'écrit plus → `utils/iseo_unstick_gateway.py --gateway <id>`, escalade Sofia après six échecs.

Version d'origine du garde-fou (connexion) : Une serrure ISEO stocke ses codes **en local** ; c'est la **passerelle** qui y pousse les nouveaux **et** qui remonte le journal des ouvertures. Passerelle hors ligne = le code part dans Duve mais **n'atteint jamais la serrure** → le client trouve porte close. Vécu sur `P15-LAO4-0G` : HyperGate morte depuis le 06/07, **9 codes poussés quand même**, **4 clients bloqués dehors** (verbatims chat Duve les 23/07, 27/07, 07/08, 12/08). La donnée était en base depuis le début (`stg_iseo__gateways`), rien ne la lisait. → `_LOCKS_CTE` expose **`gateway_dead`** (pas de gateway, ou **≥ 24 h** sans connexion), propagé jusqu'à `_resa_to_provision` ; la boucle **saute la résa avant toute création Sofia**. ⚠️ **CORRIGÉ le 20/08 : PAS de mail dédié.** La 1re version en envoyait un à **chaque run** au motif que « ça ne se résout pas tout seul » — résultat, **8 mails dans la nuit pour la même réservation** (Isaac Stocks / LAO4-0G). Une passerelle morte est un état **persistant** : le répéter n'ajoute aucune information et apprend au lecteur à ignorer l'expéditeur. La cause est désormais portée par le **mail quotidien « résas sans code »** (`_whitelisted_gaps`, cause **`gateway`**, classée AVANT `precheckin`/`paiement` — quand la passerelle est morte, c'est la seule cause qui compte) et compte dans le `n_urgent` du sujet. Un seul mail, au bon endroit, avec les autres causes. **Ne rien pousser est l'état SÛR** : Duve retombe sur le code fixe, lui bien programmé dans la serrure. ⚠ **Ce n'est PAS un `hold`** : la porte retient un code *valide* en attendant un humain, ici le code serait *inutilisable* quoi qu'on décide. ⚠ **Borné à la whitelist**, comme le 1er skip de `_provision` : sinon `ROY15-5D` (aucune passerelle, jamais provisionné) sonnerait à vide toutes les 2 h (mesuré : 3 séjours). ⚠️ **Seuil resserré 7 j → 24 h le 19/08 au soir** (il était trop laxiste dans la 1re version) : mesuré sur les **106 HyperGates, 101 pinguent dans l'heure et AUCUNE ne se situe entre 1 h et 24 h** — les 5 restantes sont à 51 h, 70 h, 72 h, 82 h et 1 072 h. **Une passerelle est vivante ou morte, il n'y a pas d'entre-deux**, donc le palier « stale » n'existe pas dans les faits. À 7 j, 4 passerelles réellement tombées restaient non protégées, dont **`CAI31-2D` qui est intégré**. L'asymétrie justifie de bloquer tôt : **bloquer à tort** coûte un code de séjour en moins (le client garde le code fixe, qui marche, et le provisioning repart au run suivant 2 h après) ; **pousser à tort** met le client dehors. ⚠ **Ne PAS aligner sur le seuil du trigger `iseo_gateway_offline` (72 h)** : alerter et bloquer n'ont pas le même coût d'erreur. Vérifié avant déploiement : 1 séjour bloqué (Isaac Stocks, LAO4, CI 23/08 — le 5ᵉ client qui aurait trouvé porte close).

⭐ **PORTE DE VALIDATION (hold) — 2026-08-15, démarrée en `observe`.** Une résa jugée à risque voit son code **créé côté Sofia** (donc lisible au dashboard, révocable, archivable normalement) mais **pas poussé à Duve** : le client ne le voit pas, la RC valide puis le lui envoie. `ISEO_HOLD_MODE` = `off` | `observe` (décision journalisée, on pousse quand même) | `on`. Colonnes `hold_reason` / `held_at` / `released_at` / `released_by` dans `merveil_pin_cache`. Notification mail à la création de la rétention (`ISEO_HOLD_ALERT_TO`) — sans elle, la porte transforme une fraude évitée en client dehors à 22 h.
- **Critères** (`_evaluate_hold`, recalibrés le 2026-08-15) — **les deux sur le canal DIRECT** : **réservé ≤ `ISEO_HOLD_LEAD_HOURS` (72 h par défaut) avant l'arrivée** (~12/mois, le critère qui porte la valeur : les 4 fraudes d'août sont toutes en direct, 3 réservées le jour même, Bossongo à J-2 — que le seuil 24 h ratait) · **solde restant dû** (> `ISEO_HOLD_MIN_BALANCE` 1 € ET ≥ `ISEO_HOLD_MIN_BALANCE_RATIO` 50 % du séjour, sur une résa posée ≤ `ISEO_HOLD_BALANCE_MAX_LEAD_HOURS` 720 h avant l'arrivée ; ~6/mois dont 2 déjà pris par le 1er critère).
- ⚠ **Le last-minute était TOUS CANAUX (≤24 h) jusqu'au 15/08** — restreint au direct depuis : mesuré, **87 % des résas du jour même sont des OTA** (2,8 direct sur 22,4/mois), payées à l'OTA avec moyen de paiement vérifié et recours possible, et **absentes des 4 fraudes**. Retenir leur code = client dehors le soir pour un gain de sécurité nul. Ne pas re-généraliser sans nouvelle mesure.
- ⭐⭐ **LE SOLDE SE CALCULE AU COMPTE PAYEUR, JAMAIS SUR LES PAIEMENTS DE LA RÉSERVATION** (refonte 15/08 après-midi, ADR du jour). Mews attache la tentative **refusée** à la résa mais le paiement **réussi** au bill / au compte payeur (`reservation_id` NULL) — et ce compte n'est même pas `customer_id` (profil « shadow » : mêmes 12 derniers caractères du GUID). L'ancien `n_charged` par réservation faisait donc voir « impayé » des séjours entièrement encaissés : **4 faux positifs sur 9** mesurés sur 1 035 arrivées, dont **2 apparts whitelistés laissés SANS code** (Jack Spence en séjour, Ray Javier à J-1). `_PAYMENTS_CTE` reconstitue désormais `amount_due` / `amount_charged` / `balance` via l'**union dédupliquée par `payment_id`** de deux chemins : (1) paiements portant la **réservation** — indispensable, ce sont les `ExternalPayment` OTA — et (2) paiements portant le **compte payeur** (items de la résa → `payer_account_id`), restreint aux comptes **`Customer`** (les comptes `Company` sont les comptes OTA : 5,2 M€ agrégés côté Airbnb → toute résa OTA passerait pour créditrice). Mesuré sur 1 976 séjours en cours/à venir : union 129 soldés · par résa seule 119 · par compte seule 116 — **aucun des deux chemins ne suffit**. `GhostPayment` exclu (re-routage comptable ; vérifié : aucun séjour soldé par un ghost seul). ⚠ Il n'existe **aucun** champ solde / « to be paid » côté API Mews (vérifié sur `raw_reservations`), il faut bien le reconstituer. `payment_unpaid` (gate VCC) = carte refusée **ET** rien d'encaissé sur le compte **ET** solde positif.
- ⚠ **Le paiement n'est pas un signal de fraude** : les 4 fraudes d'août avaient toutes payé (les pertes sont des **chargebacks**). C'est un signal de créance — d'où les bornes de matérialité et d'ancienneté sur le critère solde (sans elles il sonnait sur un reliquat de 75 € et sur des résas vieilles de 6 mois). Le critère qui protège reste le last-minute direct.
- ⭐ **Vérification rétro** : modèle dbt **`dash_ops_hold_simulation`** (bloc « Rétro » du sub-tab Porte, 7.8) rejoue les critères sur toutes les arrivées J-30 → J+3 **annulées comprises** (les fraudes finissent annulées) → les 5 cas connus en sortent. ⚠ Les seuils y sont **dupliqués** : toute modif se fait des DEUX côtés.
- ⚠ **Ne PAS appliquer le critère paiement à tous les canaux** : une résa Booking/Airbnb est payée à l'OTA et n'a **aucun** paiement dans Mews → mesuré, ça retiendrait **69 % des arrivées**.
- ⚠ **Volontairement PAS conditionné à « pièce d'identité scannée »** : scanner une pièce coûte 30 s à un fraudeur (n'importe quel document passe l'OCR) — et **un cas de fausse pièce est avéré côté Merveil** → en faire une condition de libération rendrait la porte contournable par une action que l'attaquant contrôle. La pièce sert au **triage humain** au moment de libérer.
- ⚠ **`hold ⇒ alerte` ne vaut que pour les résas qui atteignent le provisioning** : les skips de `_provision` (whitelist, lock non résolue, `payment_unpaid`) s'exécutent **AVANT** la porte, donc une résa jugée à risque mais skippée n'envoie **pas** de mail de rétention. Vérifié au run du 15/08 : 4 HOLD-OBSERVE journalisés, **0 mail** — les 4 étaient des skips paiement. Ce n'est pas un trou : le skip paiement est un blocage **plus fort** que la porte (aucun code n'existe), et ces résas sortent déjà dans le **mail quotidien « résas sans code »** (`_whitelisted_gaps`, cause `paiement`). Ne pas « corriger » en remontant `_notify_hold` avant les skips : on doublonnerait le mail gaps sur des résas sans code.
- ⭐ **Toute rétention envoie un mail, mode `observe` COMPRIS** (`_notify_hold`, corrigé le 15/08 — il n'était appelé qu'en mode `on`, donc en `observe` la décision était journalisée sans que personne ne l'apprenne). Le mail dit explicitement, dans chaque mode, **si le client a le code ou non** : ce sont deux gestes RC opposés (`🔒 Code retenu à valider` vs `⚠️ Résa à risque (code envoyé)`). Règle générale : **hold ⇒ alerte**, sans quoi la porte transforme une fraude évitée en client dehors à 22 h. ✅ `ISEO_HOLD_ALERT_TO` = **`hello@merveil.co,hatim@archides.fr` depuis le 02/09** (volume mesuré ~2-4 mails/mois sur `hold_decisions` ; le mail ne porte AUCUN code de porte — compatible boîte lue par des externes).
- ⚠ **La porte ne mord qu'après retrait du code fixe du champ Duve de l'appartement** — sinon Duve l'affiche en repli et le client entre quand même. Elle s'active donc progressivement, appartement par appartement, au rythme des suppressions côté ops. C'est ce qui rend l'activation sans risque, et pourquoi le périmètre n'a pas besoin d'être configuré.
- **3 pièges traités, à ne pas défaire** : (1) `_resa_duve_retry` **exclut** les rétentions — une ligne retenue a exactement la même signature qu'un push raté (`provisioned_at` rempli, `duve_pushed_at` NULL), sans le filtre le retry enverrait au run suivant le code que la porte vient de retenir, rendant la porte silencieusement inopérante ; (2) **`_resync` réévalue la porte sur les dates LIVE** — sinon réserver à J+5 (la porte passe), recevoir le code à J-3, puis avancer les dates à aujourd'hui contourne le contrôle ; une rétention déjà libérée (`released_at`) n'est pas re-fermée ; (3) `held_at` posé via `COALESCE` au resync pour ne pas rajeunir l'ancienneté d'une rétention en attente.
- ⭐ **Journal des décisions → `iseo_raw.hold_decisions`** (append-only, 15/08) + modèle `dash_ops_hold_decisions` + **sub-tab « Porte (décisions) » du 7.8**. Écrit par `_log_hold_decision` **après** `_provision`, donc porte l'**issue réelle** : `held` (code retenu) · `pushed_observe` (décision calculée, code parti quand même) · `skipped:<raison>` (bloqué en amont, aucun mail envoyé) · `error:<msg>`. Stocke aussi `hold_mode` et `hold_lead_hours_setting` → un recalibrage de seuil se relit a posteriori. Best-effort (une écriture ratée ne fait jamais échouer un provisioning). ⚠ **Table SÉPARÉE du cache à dessein** : on ne peut pas écrire `hold_reason` dans `merveil_pin_cache` en mode `observe` sans casser `_resa_duve_retry`, qui filtre dessus pour distinguer une rétention d'un push Duve raté — un push réellement échoué ne serait alors plus jamais retenté. Ne pas fusionner les deux.
- **Libération v1** : poser `released_at` / `released_by` à la main (le retry repousse alors au run suivant). Le bouton 1-clic viendra avec le framework `action_requests`.

**Resync — dates ET heures, par PUT (2026-06-20, refondu 2026-08-25)** — `_resa_to_resync()` : rows actives dont la window cache ≠ window live (séjour étendu/raccourci/décalé), invitation manquante, **ou** dont l'heure achetée n'est pas reflétée par le credential Sofia.

⭐ **Le point qui manquait** : un early check-in / late check-out acheté **après** le provisioning (J-3) — le cas fréquent, l'achat se faisant quand l'arrivée approche — **ne déplace AUCUNE date**, et **le cache ne stocke pas les heures**. Les 3 conditions historiques y étaient donc aveugles, et corriger `_stay_hours` seul n'aurait rattrapé personne. La détection compare désormais l'heure achetée à **ce que Sofia porte réellement** : le snapshot `staging.stg_iseo__standard_devices` (`ISEO_DEVICES_STG_TABLE`, fraîcheur ~4 h = ETL 2 h + dbt 2 h) repère les candidats, et `_resync` fait un **GET live** avant toute écriture → une ligne déjà corrigée mais encore périmée au snapshot ne produit qu'un GET sans effet.

⭐ **Le déplacement se fait par `PUT`** (`_put_device_window` / `_put_invitation_window`, validés en prod le 21/08 puis re-vérifiés en no-op le 25/08), plus par DELETE + re-POST : le code ne disparaît **jamais**, même une seconde — le delete+recreate ouvrait une fenêtre de lockout si le POST échouait derrière. **Bénéfice non anticipé : l'invitation garde son code**, donc le lien remote-open déjà parti dans un message Duve (figé) reste valide, alors que chaque resync le tuait. **Repli automatique** sur l'ancien chemin si le PUT échoue. Le re-POST ne sert plus qu'au cas « device absent de Sofia ».

⚠ **Asymétrie du DTO Sofia dans le PUT** : l'entrée attend `lockTagIds`/`guestTagIds` (**singulier** « Tag »), le GET renvoie les tags en objets `lockTags[].id`/`guestTags[].id` et laisse les champs pluriels à NULL → reconstruire depuis les objets. Sans ça : `"Cannot invoke java.util.List.size() because guestIds is null"`.

**Archive** — `_resa_to_archive()` : rows actives où CO passé OU résa annulée (cross-check Mews). `_archive` : `DELETE /standardDevices/{id}` (par extId) + `DELETE /invitations/{id}` + **`DELETE /users/{id}`** (le user dédié de la résa, par extId `MERVEIL_USER - <duve_id>` — sinon accumulation de users guest).

**Mapping Duve↔Mews** : payload checkin Duve embarque `guestProfiles[isPrimary].externalId = customer_id` ; JOIN `fct_reservations` sur `customer_id + resource_id` (résa au CI le plus proche). Skip annulées au provision + archive.

**Mode shadow** (`ISEO_SHADOW_MODE=true`) : log "would provision" sans appel Sofia/Duve ni écriture d'état.

**Whitelist** — ⭐ **source unique = seed dbt `iseo_whitelisted_apartments`** (`apartment_code, property_id`), depuis 2026-07-06. Chargée au run par `_load_whitelist()` (query BQ, fallback `ISEO_ALLOWED_PROPERTY_IDS` env si seed vide/inaccessible) ET par les 2 modèles dbt `trigger_iseo_pin_missing.sql` + `dash_ops_lock_events.sql` (via `ref()`). **Élargir le cutover = ajouter 1 ligne dans le seed CSV + `dbt seed`** (le job dbt le fait avant `dbt run`), aucun redeploy de l'orchestrateur. Fini le drift des 3 whitelists hardcodées. Cutover actuel = **40 apparts** (lot 3 du 2026-08-03, +20 validés CEO — sélection : serrure saine + lockTag vérifié + gateway, priorisés par arrivées 30j ; écartés : BRS1-5F mismatch lockTag, ROY15-5D no gateway, MAR135-2F batterie, 3 stale). Source = seed `staging.iseo_whitelisted_apartments` ; l'`ISEO_ALLOWED_PROPERTY_IDS` de `deploy.sh` n'est qu'un fallback à 7 GUID, non représentatif. ~90 apparts restants (lockTag ok) à élargir par lots progressifs, monitorés via 7.8 réconciliation + gaps.

⚠️⚠️ **PLAFOND LUCKEY — 600 ÉLÉMENTS (incident 15/08/2026)** : le plan Basic compte **1 élément par utilisateur, par serrure ET par invitation active**. Notre modèle consomme **2 éléments par séjour** (user dédié + invitation), soit **~2,44 par appartement** (un appartement porte ~1,6 séjour simultané). Le 15/08 à 16h14 le plafond a été touché (416 users + 130 serrures + 54 invitations = 600) → `POST /api/v2/users` répond `403 {"code":11201,"message":"User is not authorize to perform user creation"}` et **plus AUCUN code n'est programmé, sur tous les apparts**. ⚠ Le message parle d'autorisation, **pas de quota** — c'est le piège, y penser en premier. Déblocage : `merveil-etl-v2/utils/iseo_purge_orphan_users.py` (dry-run par défaut ; `--safe` = désactivés sans code + codes expirés, `--externes` = invités orphelins de l'ère Duve native ; ne touche jamais les users DWH, les porteurs de code vivant, ni les `@archides.fr`). 35 éléments libérés le 15/08 → création débloquée immédiatement. ⚠ Le compteur **n'est pas lisible par l'API** (`/api/v1.1/subscriptions` → 404 sur notre tenant) : le prochain plafond se manifestera par le même 403 muet, surveiller le mail d'erreurs. Conséquence stratégique : le retrait des codes fixes devient *économiquement* nécessaire (chaque appart intégré = 1 user manuel supprimable). Cf. ADR `decisions.md` 15/08 + `docs/iseo-vision.md` §9 pièges 12-13.

**Secrets requis** : `ISEO_MANAGER_USERNAME` + `ISEO_MANAGER_PASSWORD` + `DUVE_CONNECT_TOKEN` (=`duve-connect-token`). Env : `DUVE_CONNECT_PID=6a357cbd2e45c374a9a9fd18`. SA `action-engine-sa` a `secretAccessor` project-wide.

**Retry Duve** (`_resa_duve_retry`) : à chaque run, re-pousse Duve pour les rows `provisioned_at IS NOT NULL AND duve_pushed_at IS NULL` (Sofia OK mais Duve KO à un run précédent) — code + lien lus depuis la cache, sans rappel Sofia.

**Alerting** : `run()` est un wrapper qui envoie un **mail** (`ISEO_ALERT_TO`, infra Gmail `alerts-gmail-sa-key` via Secret Manager + DWD) — récap si ≥1 erreur (provision/retry/archive), CRASH + exit non-zero si exception. ⚠️ délimiteur env `^;^` dans `deploy.sh` (les emails contiennent `@`).

**Mail quotidien « résas sans code » (refondu 2026-08-05)** : `_whitelisted_gaps()` classe chaque résa whitelistée à provisionner (CI ≤ J+lookahead, sans row cache active) par **cause** : `lock` (serrure non résolue, anormal) · `precheckin` (pas de mapping Duve, n'alerte qu'à CI ≤ J+1 — bruit auto-résolu avant) · `paiement` (gate volontaire : tous les paiements `Failed`, aucun `Charged` — VCC Expedia/VRBO typiquement) · `autre` (**le vrai signal** : provisionnable en apparence mais toujours pas de code — invisible de l'ancien mail qui ne voyait que no_duve/no_lock). Mail **HTML** (`_build_gaps_html`, style aligné brief annulations : KPIs par cause + 1 table par section + hint), sujet 🔴 si ≥1 lock/autre, envoyé 1×/jour au run de 8h Paris, destinataire = `ISEO_ALERT_TO` (hatim seul). Même sémantique que `pin_state` de `dash_ops_arrivals` (6.1) — garder les deux alignés.

**État cutover (22/06)** : live sur **7 apparts**. Les 5 d'origine AVEC guest tag (OUR12-1D, TBG52-1D, TBG52-1G, SEB23-3F, SEB23-3G) + **CLE7-0D** (1er appart **via fallback guest tag 132094**, validé E2E en prod le 22/06 : device+invitation+lien+Duve OK) + **MRI16-0D** (guest tag propre). `ISEO_SHADOW_MODE=false`. Restent hors whitelist : ABO58, POC5, SEB44 (tagless → prochain paquet fallback).

**Trous connus à traiter** : (1) ~~drift de dates~~ → **FAIT 2026-06-20** (`_resa_to_resync`, cf. ci-dessus) ; (2) ~~**fallback guest tag**~~ → **OBSOLÈTE depuis 2026-06-30** : plus de tag dérivé de l'appart ni de fallback générique. Chaque résa a son user dédié au vrai nom (cf. « Label Luckey » ci-dessous) → tout appart est provisionnable sans dépendre d'un guest tag pré-existant. `ISEO_DEFAULT_GUEST_TAG_ID` supprimé de `deploy.sh`. ⚠ Pour réellement provisionner les 4 apparts ABO58/CLE7/POC5/SEB44, il reste à **ajouter leurs GUID à `ISEO_ALLOWED_PROPERTY_IDS`** (cutover live = décision séparée) ; (3) élargir whitelist aux 13 ; (4) ~~retirer le cacher~~ → **FAIT 2026-06-20** (cacher webhook + topic/sub/endpoint supprimés ; reste à droper le topic `iseo-pin-to-cache` + sub + DLQ côté infra) ; (5) ~~tab dashboard~~ → **FAIT 2026-06-20** (`dash_ops_pin_pipeline` refondu sur le nouveau vocabulaire provision/invitation/duve + drift ; ancienne réconciliation PHANTOM/DRIFT supprimée).

**Onboarding lockTags — 96 serrures taggées 2026-06-29** : le pipeline mappe une serrure→appart via un lockTag dont le `name` = `resource_id` Mews. Seules 18/130 serrures l'avaient (créés jadis par l'intégration native) → 96 sans tag = non provisionnables. `merveil-etl-v2/utils/iseo_sync_lock_tags.py --apply` les a toutes onboardées : `createLockTag {name: resource_id}` (idempotent par extId, dérivé de `raw_resources` via le nom de la serrure) + `updateSmartLock` en ré-incluant les tag ids existants (ADMIN préservé). **Prérequis levé pour élargir `ISEO_ALLOWED_PROPERTY_IDS`** au-delà des 7 actuels (le tag n'est qu'un prérequis ; le provision reste gaté par la whitelist). **+8 cas limites résolus via `--fix-edge-cases`** (map `EDGE_CASES`, resource_id vérifiés) : A = 4 serrures renommées pour matcher Mews (RIC75 O→0, VIA29 casse, BAC116 espace, FRE17→FRE17B) + 2 faux positifs regex taggés (VAL1B, DUL16) ; B = 2 préfixes décalés taggés sans rename (MTM13 lock-P02/Mews-P01, PAL7 lock-P03/Mews-P02). **Restent (décision ops/Mews)** : 3 C = unité serrure absente de Mews (ARC77-2F, MAR181-0F, POI8-1G) ; 3 D = vraiment absents (DES5-5F, DES-5G, SEB76-2F) ; 2 E = non-apparts (LOCAL HK, Test applique) ; 1 MISMATCH (BRS1-5F taggée avec le resource_id de SEB76-2D).

**⚠ Prérequis onboarding n°2 : `guestRemoteOpenEnabled` (découvert 2026-07-08)** : le lien remote-open d'une invitation ne marche que si la SERRURE a `configuration.guestRemoteOpenEnabled=true` (sinon page Luckey « No smart lock found / You don't have the necessary right »). Les 7 apparts d'origine l'avaient, les 5 du lot du 06/07 non (constaté sur Murray/LOU18 + AlRabiah/PON48 ; Levine/OUR12 OK) → activé le 08/07 sur les 5 via `PUT /api/v2/smartLocks/{id}` `{"configuration": {...existante, "guestRemoteOpenEnabled": true}}` (PUT partiel OK, tags intacts). **✅ RÉGLÉ 2026-07-12 : 130/130 serrures ont le flag** — mode `--enable-remote-open` ajouté à `iseo_sync_lock_tags.py` (GET live + PUT partiel config préservée, idempotent, compteur au dry-run) et exécuté sur les 100 manquantes. Le prérequis n°2 ne bloque plus aucun élargissement. Même jour : serrures SEB23 renommées `P03-…`→`P01-…` (préfixe ≠ Mews → 858 ouvertures/30j invisibles du journal 7.9) + fix staging `stg_iseo__smart_lock_events` (apartment_code dérivé du nom ACTUEL de la serrure via lock_id, plus du nom figé dans l'event → les renames réparent l'historique). **Timing cutover** : le message d'accès Duve part à ~J-1 04h — toute résa avec CI ≤ 1-3j au moment de l'ajout d'un appart à la whitelist a déjà reçu l'ANCIEN code fixe (cas AlRabiah : message 05/07 avec code fixe, provision 06/07 13h36 ; le champ Duve est à jour mais le message ne repart pas). Pas de lockout (code fixe actif), mais le code dynamique reste inutilisé sur ces résas de transition.

⭐ **Suppression du doublon natif au provisioning — ajouté 2026-08-18** (`_purge_native_duplicate`, appelé en fin de `_provision` pour chaque résa Duve du séjour) : supprime le `DUVE_PIN - <id>` **et** le `DUVE - <id>` (l'invitation) juste après que notre code a été écrit dans Duve. Complète `_purge_native_orphan` ci-dessous, qui ne voyait que les résas annulées ou parties : au 18/08 il restait **19 objets natifs actifs ou futurs** correspondant à des séjours réels. ⚠ **Pourquoi accroché à `_provision` et pas en purge groupée** : tant que nous n'avons rien poussé, le code natif est ce que la Guest App affiche, donc le **SEUL** code du client — et 3 des 12 séjours concernés sont sur des appartements NON intégrés où nous ne provisionnerons jamais (RIV85-1G, OUR12-1G, BRS1-5F). Une purge en masse les mettait dehors. Ici la substitution est atomique (nouveau code posé → ancien retiré) et ne touche que la whitelist par construction. Best-effort : un échec ne fait jamais échouer un provisioning réussi. Les derniers doublons partiront d'eux-mêmes d'ici janvier 2027.

⭐ **Retry push automatique des passerelles coincées — ajouté 2026-09-04** (phase 5 de `_run_inner`, `_stuck_gateways_to_retry` + `_retry_push`) : à chaque run, **1 `POST /gateways/{id}/notifications {CONFIGURATION, CREDENTIALS_UPDATED}` par passerelle `push_stuck` portant un appart de la whitelist** (même périmètre que `trigger_iseo_gateway_push_stuck`), tant que `n_pushes_since_applied < ISEO_RETRY_PUSH_MAX` (36 = 3 jours à 1 push/2 h ; au-delà = restart manuel + escalade). **Fondement** : réponse ISEO du 04/09 — ce POST est LEUR contournement officiel (« forces a global credential synchronization ») et leur plateforme ne ré-émet JAMAIS un push `FAILED` (seul `IN_TRANSIT` est renvoyé 10×). Campagne manuelle du 04/09 : 8/11 débloquées, toutes en ≤ 4 pushes → à 1 push/2 h la plupart repartent dans la journée. ⚠ **Sans `GATEWAY_RESTART`** (coupe l'ouverture à distance ~5 min, et le job a un `task-timeout` de 300 s) et **fire-and-forget** : le résultat se lit au snapshot ETL suivant, pas dans le run. Le garde-fou `gateway_dead` est inchangé — on ne pousse un code qu'après un APPLIED constaté par l'ETL. Shadow mode → skip. Compteur `retry-push=` dans la ligne DONE. Le restart + la salve de 6 restent manuels (`merveil-etl-v2/utils/iseo_unstick_gateway.py`). Cf. `docs/iseo-vision.md` §14.

**Purge DUVE_PIN natifs orphelins — ajouté 2026-06-29** : l'intégration native étant coupée, plus personne ne supprime les `DUVE_PIN` à l'annulation/au départ → un guest annulé gardait un code valide (cas Bianca Aranha : résa annulée, PIN 7552 toujours actif). Nouvelle phase `_purge_native_orphan` : DELETE les `DUVE_PIN - <duve_id>` dont la résa Mews est **annulée OU checked-out**, **scopé STRICTEMENT à `ISEO_ALLOWED_PROPERTY_IDS`** (hors whitelist le DUVE_PIN natif reste l'UNIQUE code du guest → ne jamais purger ailleurs). Les résas ACTIVE ne sont pas touchées (leur DUVE_PIN double notre code mais reste l'unique code des résas >J-3 pas encore provisionnées) → nettoyées au checkout. Compteur `purged=` dans le log DONE.

**Label Luckey — user dédié au vrai nom par résa (2026-06-30, remplace le tag générique)** : l'UI Luckey affiche le **firstname/lastname du user** porteur du guest tag (pas le nom du tag, qui vaut l'email). Donc `_provision`/`_resync` créent un **user Sofia par résa au vrai nom du guest** (`firstname/lastname` depuis `fct_reservations.customer_name`), supprimé à l'archive. Fin de l'approche « tag générique partagé » du 29/06 qui était cassée.

⚠️ **Finding clé `enabled` (2026-06-30)** : un user créé via l'API est **`enabled=False`** par défaut (le schéma POST `/api/v2/users` n'a pas de champ `enabled`, et `/disable` existe mais pas `/enable` ; PUT `enabled=True` ignoré). Un user `enabled=False` est **garbage-collecté en quelques minutes s'il n'a pas de device**, et un user désactivé **perd son tag `user`** (vérifié : 7/44 users natifs `enabled=False` = 0 guest tag → PIN cassé). **Seul moyen de créer un user `enabled=True` : fournir un `password` à la création** (testé : password → `enabled=True` + tag, sans password → `enabled=False`). D'où le password aléatoire systématique dans `_get_or_create_guest_user` (le guest ne se connecte jamais, il ouvre au PIN clavier + lien remote-open).

✅ **Clarification support Sofia (Pascal, 2026-07-06)** : comme on n'utilise PAS les règles d'accès smartphone (BLE app), **il n'est pas nécessaire de créer le user avec email + password** — un user créé **SANS email est automatiquement actif** (`enabled=True`). Réconcilie le finding ci-dessus : c'est l'email (pseudo `resa-…@guest.archides.fr`) qui déclenche l'état « pending set-password » → d'où le besoin du password pour forcer actif. **Simplification possible (non urgente)** : supprimer email+password de `_get_or_create_guest_user`, créer le user sans email → auto-actif. Autres confirmations Pascal : (1) **`dateInterval` = epoch Unix** (notre code envoie des ms et ça marche → OK) ; (2) **`Invitations` = add-on payant** (abonnement séparé) — si l'abonnement saute, plus de liens remote-open (les PINs clavier continuent) ; (3) **les APIs sont stables et ne changeront pas** (aucune modif à ce jour) → le risque « changement silencieux Sofia » (flaggé à l'audit) est écarté. Réponse des devs Sofia encore en attente.

**Historique des échecs (à ne pas refaire)** : 29/06 tag « Merveil Guest » 184106 créé en `type: system` → refusé par Sofia `HTTP 400 "guestTagId must be a user type"` (un guestTagId doit être un tag `type: user`, càd attaché à un user). 30/06 tag user 184181 d'un user guest standalone (sans device, `enabled=False`) → user GC en <6 min → `HTTP 404 "UserTag not found"`. Les deux pistes mortes ; tags/users de test purgés.

**Bug churn provision/archive — corrigé 2026-06-29** : quand une résa Duve mappe vers **2 résas Mews mêmes dates** (jumeau actif + jumeau annulé issu d'un rebooking Mews cancel+recreate), `_resa_to_archive` pouvait piocher le jumeau **annulé** (son `QUALIFY` ne filtrait pas `is_cancelled`, tie-break à égalité sur les dates) → archivait un PIN actif → `_resa_to_provision` le recréait au run suivant (cache row archivée = plus couverte) → **boucle create/delete toutes les 2h** (~40 devices/invitations Sofia créés+supprimés sur CLE7-0D, push Duve à chaque cycle). Fix : ajouter `COALESCE(m.is_cancelled, FALSE) ASC` en tête du `ORDER BY` du QUALIFY d'archive (le jumeau actif gagne) — même logique que provision. Idem dans `dash_ops_pin_pipeline.sql` (le dash affichait 1 ligne/résa via QUALIFY → churn invisible, et tie-break aléatoire). Observé sur RUIQING XU / CLE7-0D, CI 04/07.

**Test local** : `gcloud run jobs execute merveil-action-engine-iseo --region=europe-west1 --project=merveil-data-warehouse --wait`. Forcer une résa >7j : `--update-env-vars ISEO_LOOKAHEAD_DAYS=400`.

## Execution Flow
```
dbt run (merveil-dbt-schedule : 06:00, 09:00, 12:00, 15:00, 18:00, 21:00 UTC)
    ↓  [30 min plus tard]
action-engine (merveil-action-engine-4h : 06:30, 09:30, 12:30, 15:30, 18:30, 21:30 UTC)
    Phase 1 — Breezeway
      1. resolve_completed_triggers (Breezeway webhook task-completed)
      2. Lit pending_actions → crée les tâches Breezeway
    Phase 2 — Digest (si FREQ défini)
      1. resolve_digest_triggers(FREQ) — purge les triggers email_digest expirés (TTL)
      2. Lit rule_{FREQ} depuis BigQuery
      3. Filtre les nouvelles alertes (non déjà dans action_triggers)
      4. Envoie 1 digest email (EmailDigestHandler.execute_batch)
      5. Logue heartbeat + alertes dans action_triggers
```

## TTL — Purge automatique des triggers digest
`resolve_digest_triggers(freq)` s'exécute au début de chaque phase 2.
Résout tous les triggers `destination='email_digest'` + `status='open'` expirés :

| FREQ | TTL |
|---|---|
| `4h` | 4 heures |
| `daily` | 24 heures |
| `weekly` | 168 heures |
| `monthly` | 720 heures |

Sans cette purge, un trigger `open` permanent bloquerait les ré-alertes sur le même `property_id`.

## BigQuery Datasets
| Dataset | Tables | Role |
|---|---|---|
| `action_engine` | `pending_actions`, `rule_*`, `action_triggers` | Action-engine state and history |
| `dashboard_alerts` | `dash_alerts` | Alerts computed by dbt (source for the email digest) |

## Adding a Rule — 3 Steps
1. **Create the SQL** in `dbt/models/rules/rule_<name>.sql`
   - Required schema: `rule_name, property_id, context (JSON), detected_at`
   - `property_id` = deduplication key (e.g. reservation_id, CURRENT_DATE...)
2. **Add it** to `dbt/models/rules/pending_actions.sql` (UNION ALL)
3. **Declare destinations** in `config/rules.yaml`:
```yaml
rules:
  my_new_rule:
    enabled: true
    frequency: daily        # 4h | daily | weekly | monthly | (absent = toujours exécuté)
    destinations:
      - type: breezeway_task
        params:
          name_template: "Task — {apartment_name}"
          department: housekeeping
          priority: normal
      - type: email_digest
        params:
          to: "alertes@archides.fr"   # destinataire par règle (override GMAIL_TO)
```

## Frequency Filtering
Le job lit `FREQ` au démarrage. **FREQ absent/vide = CRITICAL + exit(1)** (fail-fast 2026-07-02) : les actions directes tourneraient mais les digests seraient bufferisés puis jetés sans erreur. Toujours définir FREQ, y compris en exécution manuelle.

| FREQ value | Triggered by |
|---|---|
⚠️ **Horaires relevés sur les schedulers le 2026-08-24, PAS recopiés de la doc** — deux d'entre eux
sont en `Etc/UTC` et la table les présentait comme des heures locales. Ce n'est pas cosmétique : le
décalage de 2 h est exactement ce qui a fait que `satisfaction_low_review` n'a **jamais** envoyé un
seul mail (fenêtre du dispatcher ancrée sur un `detected_at` antidaté à minuit UTC, cf. la docstring
de `_load_triggers`). Toujours lire `gcloud scheduler jobs describe <job> --location=europe-west1`.

| FREQ value | Triggered by | Cron | TZ | Heure Paris |
|---|---|---|---|---|
| `4h` | `merveil-action-engine-4h` | `30 6,10,14,18,22 * * *` | Europe/Paris | 06:30 · 10:30 · 14:30 · 18:30 · 22:30 |
| `daily` | `merveil-action-engine-daily` | `0 7 * * *` | **Etc/UTC** | **09:00** (⚠ pas 07:00) |
| `weekly` | `merveil-action-engine-weekly` | `0 8 * * 1` | **Etc/UTC** | **10:00 lundi** (⚠ pas 08:00) |
| `monthly` | — | — | — | ⚠ **aucun scheduler n'existe** : le FREQ est géré par le code mais n'est déclenché par rien |
| (not set) | ⛔ **CRITICAL + exit(1) depuis 2026-07-02** — sans FREQ, le dispatcher bufferisait les digests puis les jetait (`run(freq=None)` ne flush jamais) = alertes muettes silencieuses (typiquement après un deploy qui écrase les env vars). L'ancien "runs all rules" décrivait le legacy ActionRunner supprimé en Phase E. Exécution manuelle : toujours passer un FREQ explicite. |

### Cloud Scheduler setup
⚠️ Utiliser l'API v1 régionale (pas v2) — seule compatible avec l'auth OAuth du scheduler.
⚠️ Utiliser `scheduler-invoker@` comme SA OAuth (pas `action-engine-sa@`).
⚠️ `gcloud run jobs execute --update-env-vars` est cassé (bug gcloud) — FREQ est hardcodé dans chaque job.

```bash
# 4H — pointe sur merveil-action-engine (FREQ=4h dans le job)
gcloud scheduler jobs create http merveil-action-engine-4h \
  --schedule="30 6,9,12,15,18,21 * * *" \
  --uri="https://europe-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/merveil-data-warehouse/jobs/merveil-action-engine:run" \
  --message-body='{}' \
  --headers="Content-Type=application/json" \
  --oauth-service-account-email="scheduler-invoker@merveil-data-warehouse.iam.gserviceaccount.com" \
  --location=europe-west1

# Daily — pointe sur merveil-action-engine-daily (FREQ=daily dans le job)
gcloud scheduler jobs create http merveil-action-engine-daily \
  --schedule="0 7 * * *" \
  --uri="https://europe-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/merveil-data-warehouse/jobs/merveil-action-engine-daily:run" \
  --message-body='{}' \
  --headers="Content-Type=application/json" \
  --oauth-service-account-email="scheduler-invoker@merveil-data-warehouse.iam.gserviceaccount.com" \
  --location=europe-west1
```

## ⭐ Refonte Trigger / Action / Routing (2026-05-21)

**Statut** : terminé. Code déployé, prod sur le nouveau dispatcher depuis 2026-05-21. Legacy `ActionRunner` + `rules.yaml` + `action_logger.py` **supprimés** le 2026-05-21 (Phase E). Tables BQ legacy (`action_engine.rule_*`, `pending_actions`, `action_triggers`) conservées en archive — non actualisées par dbt — à droper manuellement plus tard si besoin.

**Concepts** :
- `trigger` (dbt → `action_engine.triggers`) : 1 ligne = 1 occurrence métier détectée. Schema unifié `(trigger_name, entity_type, entity_id, property_id, severity, category, message, action_url, context, detected_at, expires_at)`. 1 fichier `models/triggers/trigger_<name>.sql` par condition.
- `action` (Python handler) : réaction typée (asana_task, breezeway_task, email_digest). Un trigger peut déclencher N actions.
- `routing` (`config/routing.yaml`) : mapping `trigger_name → [actions]`. Déclaratif.
- `dispatched_actions` (BQ table append-only) : log par (trigger, property_id, action_type, status). Cooldown via `status='open'`.

**Flux exécution** (`src/core/dispatcher.py::TriggerDispatcher.run`) :
0. ⭐ **Garde-fou « chaîne d'alertes muette » (2026-09-02, ADR)** — `_check_triggers_freshness()` : âge du **dernier MERGE réussi** sur `action_engine.triggers` via `INFORMATION_SCHEMA.JOBS_BY_PROJECT` (bat toutes les 2h même à 0 ligne insérée ; ⚠ `last_modified_time` ne bouge PAS sur un MERGE vide — réfuté par mesure, ne pas y revenir). Âge > `TRIGGERS_STALE_MAX_HOURS` (4.5 = 2 cascades manquées) → mail CRITICAL **direct via le mailer** à `TRIGGERS_STALE_ALERT_TO` (défaut hatim), jamais via la table (c'est elle qui est morte). Rappel stateless ~12h. Motivé par l'incident 01/09 : un trigger_* en erreur SQL → `triggers` SKIP → zéro alerte de la journée, rien ne le signalait. IAM : exige `roles/bigquery.resourceViewer` sur `action-engine-sa` (accordé 02/09 — les MERGE sont lancés par la SA dbt). Testé en prod dans les 2 sens (silencieux à frais, mail au seuil forcé).
1. Résoudre les Breezeway tasks completed (webhook) → `dispatched_actions` status=resolved
2. Résoudre les digests expirés (TTL 4h/24h/168h selon bucket) → resolved
3. Charger triggers + dispatched_actions ouverts (1 query chacun)
4. Pour chaque trigger × routing.actions : skip si déjà open, sinon (a) buffer pour email_digest, (b) handler.execute pour asana/breezeway
5. Flush 1 mail par bucket avec les triggers bufferisés

**Feature flag** `USE_NEW_DISPATCHER` : supprimé en Phase E. Le `deploy.sh` ne le set plus dans les env vars Cloud Run (utilise `--set-env-vars` qui réinitialise). Rollback rapide impossible désormais — git revert + redeploy si régression.

**Inventaire triggers** (cf. `dbt/models/triggers/`) — 18 actifs :
| trigger_name | bucket / action | enabled | volume actuel |
|---|---|---|---|
| cancellation_vip | email_digest@4h | ✅ | ~37 |
| satisfaction_low_review | email_digest@4h | ✅ | ~3 |
| last_minute_checkin | email_digest@4h | ✅ | ~3 |
| double_booking | email_digest@4h | ✅ | 0 |
| checkin_no_apartment | email_digest@4h | ✅ | 0 |
| abandoned_cart | email_digest@daily | ✅ | 0 |
| revenue_anomaly_adr | email_digest@daily | ✅ | 0 |
| revenue_anomaly_adr_per_room | email_digest@daily | ✅ | 0 |
| revenue_apt_zero | email_digest@daily | ✅ | 0 |
| low_occupation | email_digest@daily | ✅ | 0 |
| high_cancellations_daily | email_digest@daily | ✅ | 0 |
| cancellation_large_apt | email_digest@daily | ✅ | ~10 |
| superhost_risk | email_digest@daily | ✅ | ~25 |
| dispo_daily_summary | email_digest@daily | ✅ | ~2 |
| gap_pricing_summary | email_digest@daily | ✅ | ~3 |
| beyond_surcote_gap | email_digest@daily | ✅ | ~22 |
| beyond_gap_filled | email_digest@daily | ✅ | rare (🎉 gap 1N vendu sous fenêtre DWH — fenêtre 24h sur created_at) |
| budget_landing_gap | email_digest@daily | ✅ | 0-2 (mois courant <90% dès le 10 · M+1 OTB <50% dès le 15) |
| champagne_direct | breezeway_task | ❌ disabled | ~5 |
| low_review_cleanliness | breezeway_task | ✅ (placeholder) | 0 |
| client_risk | email_digest@daily | ❌ disabled | ~18 |
| inspection_overdue | asana_task | ❌ disabled (POC) | ~41 |
| data_contract_breach | email_digest@data_quality | ✅ (POC) | ~1 |
| finance_flow_stale | email_digest@data_quality | ✅ | 0 (tout frais) — 6 flux finance surveillés (exports Mews, ledger/factures Pennylane, payments API, écritures OTAs), seuils 30h→216h, CRITICAL à 2× |
| iseo_gateway_push_stuck | email_digest@daily | ✅ | 10 au 25/08 dont 5 CRITICAL — passerelle qui RÉPOND au cloud mais dont le dernier push `CREDENTIALS_UPDATED` n'a pas abouti. ⭐ **Depuis le 04/09 : UNIQUEMENT les passerelles portant un appart intégré, toujours CRITICAL** (le provisioning y est bloqué, les clients entrent au code fixe). Les WARNING sur passerelles non intégrées sont supprimés : un PIN maintenance parc entier créé à la main le 01/09 (lockTag ADMIN) avait fait passer le mail de 10 à 25 lignes en révélant 13 passerelles jamais sollicitées, et rien ne pouvait jamais les faire sortir du mail (cliquet — cf. `dbt/CLAUDE.md` § gateway_push_health). ⚠ **Complémentaire de `iseo_gateway_offline`, pas redondant** : celui-ci lit la connexion, donc ne voit que les débranchées — les 6 passerelles de l'incident du 08-24/08 pinguaient toutes normalement. Bucket daily : état persistant, cf. la leçon des 8 mails nocturnes du 20/08 |
| genai_stale | email_digest@data_quality | ✅ | 0 (tout frais) — fraîcheur des 4 pipelines genai (conversations 36h, calls 60h, reviews/grid 72h, CRITICAL à 2×). Créé 13/08 : depuis le fix BatchStillRunning, un pipeline bloqué sort en SUCCÈS côté Cloud Run → seule la fraîcheur genai.* le détecte |

### ⚠ Buckets digest — `flush_with` obligatoire pour les buckets custom (fix 2026-07-17)
`run(freq)` ne flushe que le bucket **homonyme** de FREQ + les buckets déclarant `flush_with: <freq>` dans `routing.yaml`. **Bug silencieux corrigé le 17/07** : le bucket `data_quality` (gouvernance, `dbt_test_failure`, `finance_flow_stale`) n'avait AUCUN `flush_with` → bufferisé puis jeté à chaque run daily, **0 mail envoyé depuis sa création** (vérifié `dispatched_actions` : aucune ligne bucket=data_quality). Tout nouveau bucket à destinataires dédiés DOIT porter `flush_with: daily` (ou `2h`/`4h`). Buckets satellites actuels : `data_quality` (Hatim, gouvernance) + `beyond` (hatim+raphael+mickael, trigger `beyond_gap_filled`) + **`chargeback`** (`flush_with: 2h`, triggers `new_chargeback` + `payment_double_exit`) + **`fraude`** (`flush_with: 2h`, triggers `fraude_identite` + `test_cartes`) + **`blacklist`** (`flush_with: 2h`, trigger `blacklist_resa`). Le TTL des satellites = défaut 24h (`DIGEST_TTL_HOURS`).

### Buckets `chargeback` et `fraude` — split du 2026-09-01 (ADR)

Un seul bucket `fraude` portait les 4 triggers jusqu'au 01/09. Il a été **scindé en deux**, qui ne diffèrent **que par la liste de diffusion** — comme `blacklist` vs `fraude`, et pour la même raison mécanique : **les destinataires se règlent PAR BUCKET**, le dispatcher n'a aucun override par trigger (`_flush_digest` lit `digest_buckets[b].default_recipients`, jamais `params.recipients`). Un bucket est donc la seule unité de routage disponible.

| | `chargeback` | `fraude` |
|---|---|---|
| Sujet | `[Merveil Chargeback]` | `[Merveil Fraude]` |
| Triggers | `new_chargeback`, `payment_double_exit` | `fraude_identite`, `test_cartes` |
| Nature | fait **accompli**, back-office | suspicion **avant l'arrivée** |
| Geste | monter le dossier de preuve (48 h), écriture 411 | **appeler** : vérifier l'identité, puis annuler |
| Destinataires | emilia · philippe · hatim · hello@merveil.co | emilia · philippe · hatim · hello@merveil.co · **externe-onepilot@merveil.co** |
| `flush_with` | `2h` | `2h` |

⛔ **La ligne de partage est le geste, pas le thème.** OnePilot est destinataire de `fraude` parce que ses agents décrochent : sur un test de cartes, ce sont eux qui vérifient et annulent au téléphone. Ils n'ont **aucun geste à poser sur un chargeback** → ne pas les y remettre « pour information » : ce mail porte des noms de clients ET des montants de litige, vers un compte **partagé externe**, pour une action qu'ils ne feront pas. `payment_double_exit` est rangé avec `new_chargeback` : c'est le même litige vu par l'autre bout (on a remboursé ET subi le chargeback).

⚠ **Les deux `@merveil.co` sont des boîtes partagées lues par des externes.** Acceptable sur ces deux buckets **et seulement parce qu'ils ne portent aucun code de porte** — ⛔ ne jamais y router un trigger serrures. `hello@` (astreinte RC) et `externe-onepilot@` se chevauchent partiellement sur `fraude` : doublon assumé, les deux n'ont pas la même astreinte.

⚠ **Rien à faire côté prose Confluence** : `derive_facts()` dérive le canal de chaque règle depuis `routing.yaml`, y compris quand une règle s'étale sur **plusieurs** buckets — la page « fraude & chargebacks » (CXSXJ) affiche les deux listes de diffusion toute seule au run suivant. Vérifié après le split.

**Historique** — le bucket unique n'avait **que hatim** de sa création (15/08) au 18/08 — trou resté invisible parce qu'**aucune des deux alertes n'avait encore tiré** (≈1,7 chargeback/mois, aucun sur la période) : une alerte jamais déclenchée ne révèle pas qu'elle est mal routée. À vérifier pour tout nouveau bucket rare. Emilia monte le dossier de preuve (48 h chrono, un litige non contesté est perdu d'office) et porte la relation OTA, Philippe passe l'écriture et suit le 411. ⚠ Emilia reçoit déjà une notification côté Mews — **doublon assumé** : notre mail porte le contexte séjour (appart, dates, remboursement éventuel côté nous = double sortie) que Mews ne donne pas. Vue qui accompagne le mail : **tab 9.6 Encaissement**, passée non-dev le 18/08 — mais **ouverte à Philippe seul** pour l'instant (l'accès dashboard des RC se décide avec Mickael). Emilia reçoit donc l'alerte sans avoir la page : c'est assumé et temporaire, son accès viendra avec la page **9.9 Contestations** (cf. CLAUDE.md dashboards-v2).

### Gouvernance données fixes — `data_contract_breach` (POC 2026-06-30)
Greffe la gouvernance des données de référence sur le pipeline trigger/action sans nouveau service. Cf. ADR `decisions.md` 2026-06-30 + `docs/audit/gouvernance-donnees-regles-2026-06-29.md`.
- **Détection** (dbt) : `dashboard_quality.dash_governance_contracts` = 1 ligne/violation de contrat sur les snapshots DWH Feed (POC : onglet Appartements — clé dupliquée, format code, chambres/surface illisibles, snapshot périmé). Scalable : 1 domaine = 1 bloc UNION.
- **Registre** (seed) : `gouvernance.gouvernance_ownership` = `domaine→owner→table→freshness_max_hours→alert_email`. Backbone, owner-as-data.
- **Trigger** : `trigger_data_contract_breach` agrège par domaine (1 alerte/domaine/jour), rattache l'owner, porte `owner_email` dans le `context`.
- **Routage** : bucket `data_quality` (subject `[Merveil Gouvernance]`). ⚠ POC route vers `hatim@archides.fr` ; basculer `default_recipients` sur l'owner (`emilia@archides.fr`) au passage prod, ou implémenter le routage dynamique depuis `gouvernance_ownership.alert_email` (le flush résout le destinataire par bucket, pas par trigger — Phase 2 handler).
- **Ajouter un domaine** : +1 ligne seed, +1 bloc UNION dans `dash_governance_contracts`, (si owner distinct) +1 bucket. 0 modif dispatcher.

**Ajouter un trigger** :
1. Créer `dbt/models/triggers/trigger_<name>.sql` (schema unifié, 1 ligne par occurrence)
2. Ajouter `UNION ALL SELECT * FROM {{ ref('trigger_<name>') }}` dans `dbt/models/triggers/triggers.sql`
3. Ajouter mapping dans `config/routing.yaml` (1 entrée sous `triggers:`)
4. `bash redeploy.sh` (dbt) + `bash deploy.sh` (action-engine) si routing.yaml modifié

**Backlog** :
- Externalisation `routing.yaml` → DWH Feed Sheets (cf. Archides/CLAUDE.md backlog)
- Drop manuel des tables BQ `action_engine.rule_*` + `pending_actions` + `action_triggers` quand l'archive ne sert plus

## Adding a Destination (handler)
1. Create `src/handlers/<destination>.py` with `class XxxHandler` and method `execute(action, params) -> str`
2. Add one line in `HANDLER_REGISTRY` in `src/core/dispatcher.py` (et `src/core/runner.py` pour le legacy si nécessaire)

## Existing Handlers
| Handler | Type | Description |
|---|---|---|
| `breezeway_task` | `BreezewayTasksHandler` | Creates Breezeway tasks (cleaning, inspection, logistics) |
| `email_digest` | `EmailDigestHandler` | Sends the daily alert digest via Gmail API (DWD) |
| `asana_task` | `AsanaTaskHandler` | **POC** — Creates Asana tasks (idempotence via `source_key:` in notes — workaround Asana free tier qui n'a pas de custom fields) |

### Asana — POC (mai 2026)
**État** : handler en place, rule `inspection_overdue` configurée `enabled: false` dans `rules.yaml`. Workspace MERVEIL `1199370302253551`, projet Test `1214996499081074`.

**Idempotence sans custom fields (Asana free tier)** : le `source_key = "{rule_name}:{property_id}"` est encodé dans les notes au format `source_key: rule:property`. Avant POST, le handler liste les tâches ouvertes du projet (1 GET caché par run) et skip si match. Cache mémoire par projet pour ne pas refaire 50 GET dans un même run.

**Test local** (sans déployer) :
```bash
cd merveil-action-engine
ASANA_PAT=<token> python -m utils.test_asana
```
Vérifie : (1) création de tâche, (2) idempotence (run 2 = SKIP), (3) listing.

**Pour activer en prod** :
1. Créer secret : `gcloud secrets create asana-pat --replication-policy=automatic` puis `echo -n "<PAT>" | gcloud secrets versions add asana-pat --data-file=-`
2. Grant accès au SA action-engine sur le secret
3. Monter dans `deploy.sh` : `--update-secrets=ASANA_PAT=asana-pat:latest`
4. Créer la SQL dbt `rule_inspection_overdue.sql` (insertion dans `action_engine.pending_actions`)
5. Mettre `enabled: true` dans `rules.yaml` + `assignee_email` correct
6. Redéployer

**Limites free tier à connaître** :
- Pas de custom fields → idempotence par parsing notes (fragile si quelqu'un édite les notes à la main et casse la ligne `source_key:`)
- Pas de webhooks "rules" Asana → si on veut une boucle de fermeture (Asana → DWH), faut polling périodique
- Asana Rules engine non disponible → toute automatisation côté DWH

## Mailer commun — `src/core/mailer.py` (2026-08-11)
`send_mail()` (Gmail DWD via `alerts-gmail-sa-key`, best-effort par défaut) + `build_email()` (shell HTML aligné sur le digest daily : header coloré par sévérité info/warning/critical, cartes KPI, table, bouton dashboard). Utilisé par les alertes erreurs/crash de `beyond_push`, `iseo_orchestrator` et `confluence_sync` (avant : 3 copies identiques de l'envoi Gmail + mails texte brut). Le digest daily (`email_digest.py`), le brief annulations et le mail gaps ISEO gardent leur HTML propre — migrables sur le shell plus tard. Toute nouvelle alerte technique = `build_email()` + `send_mail()`, ne pas recopier l'infra Gmail.

## Email Digest — Architecture Gmail API + DWD
**Sender**: `noreply@archides.fr` (dedicated Workspace account, never connected as a human)
**Recipient**: `alerte_ventes@archides.fr, hatim@archides.fr` — défini dans `config/rules.yaml` sous `digest.4h.to` et `digest.daily.to`
**Auth**: Domain-Wide Delegation — the SA `alerts-gmail-sender` impersonates `noreply@archides.fr`

Required secret in Secret Manager: `alerts-gmail-sa-key` (SA JSON key)
Env vars (defined in `deploy.sh`):
```
GMAIL_SENDER=noreply@archides.fr
GMAIL_TO=alerte_ventes@archides.fr   ← fallback si rules.yaml ne définit pas `to`
```

Pour forcer l'envoi en test sur une adresse unique : `gcloud run jobs update merveil-action-engine-daily --update-env-vars=TEST_RECIPIENT=<email>` (et `--remove-env-vars=TEST_RECIPIENT` pour repasser en prod). NE PAS utiliser `--set-env-vars` qui remplace toutes les vars (FREQ saute).

The SA `alerts-gmail-sender@merveil-data-warehouse.iam.gserviceaccount.com` must have access to `alerts-gmail-sa-key` in Secret Manager, and the Cloud Run Job must mount this secret.

### Template HTML (`src/handlers/email_digest.py::_build_html_from_rows`)

**Structure** :
- Header indigo · summary bar (`X alertes — N critique(s) · M warning(s)`)
- Table 3 colonnes (Alerte / Date / Action) groupée par `alert_category` (sections gris clair)
- Pour chaque alerte : emoji severity + `alert_message` + date + bouton "Voir →" (si `action_recommended.startswith("http")`) ou texte gris sinon

**Détection URL** : `action_recommended.startswith("http")` → bouton cliquable indigo. Sinon texte. Tous les `action_recommended` dans `dash_alerts.sql` sont désormais des URLs dashboard direct (cf. dbt CLAUDE.md "Alerting — refonte dashboard liens + multi-apparts").

### Regroupement multi-apparts

Pour les types qui touchent N apparts/résas simultanément (`superhost_risk` 29/jour, `revenue_anomaly`, `last_minute`, `cancellation_vip`, `cancellation_large_apt`, `abandoned_cart`) — si **≥ 3 occurrences** du même type dans une même catégorie, on regroupe en **1 case agrégée** au lieu de N lignes :

- Titre + count + breakdown severity (`🔴 6 critical · 🟡 23 warning`)
- Sous-blocs Critical puis Warning avec chips compacts (1 chip = `alert_message` complet)
- Bouton "Voir →" unique cliquable (URL = `action_recommended` du 1er élément)

**Tri intelligent (constante `GROUPABLE` en haut de email_digest.py)** :
- `superhost_risk` : `secondary_value` desc (= `nb_reviews_3m`, signal solide) puis `metric_value` asc (pires notes en haut)
- `revenue_anomaly` : `secondary_value` desc (= CA P-1, plus grosse chute en premier)
- Autres : par `alert_date`

**Pour ajouter un nouveau type groupable** : ajouter une entrée dans `GROUPABLE` avec `title` + lambda `sort`. Le seuil `GROUP_MIN_COUNT=3` est global.

### ⚠️ NULL CONCAT côté dbt

BigQuery `CONCAT(a, b, NULL, c)` retourne **NULL** dès qu'un seul argument est NULL → mail affiche "None". Si tu vois "None" dans la section d'une catégorie, le bug est côté `dash_alerts.sql` — wrapper tous les args potentiellement NULL en `COALESCE(col, '?')`. Cas connu corrigé 2026-05-20 : `cancellation_vip` avec `customer_name = NULL`.

## Deduplication
`rule_daily_alert_digest` emits `property_id = CURRENT_DATE` → max 1 email per day.
All rules: if an `open` trigger already exists for `(rule_name, property_id)` in
`action_engine.action_triggers`, the action is skipped.
If no alerts in `dash_alerts`, the handler raises `SkipAction` (not logged as an error).

The email digest triggers are never auto-resolved (no associated Breezeway task) —
this is expected, as the property_id changes each day so the cooldown doesn't block subsequent runs.

## Streaming buffer — BigQuery limitation
Inserts into `action_triggers` are done via `insert_rows_json` (streaming).
Rows inserted via streaming cannot be UPDATE/DELETE for ~90 min.
In production this is not a problem. For testing, to force a re-trigger:
```bash
# Wait 90 min OR use an INSERT INTO ... SELECT query instead of UPDATE
bq query --use_legacy_sql=false "UPDATE \`...\` SET status='resolved' WHERE ..."
```

## Debug — Cloud Run Job Logs
```bash
gcloud run jobs executions list --job merveil-action-engine --region europe-west1 --project merveil-data-warehouse
gcloud beta run jobs executions logs read <execution-name> --region europe-west1 --project merveil-data-warehouse
```

## Deploy
```bash
bash deploy.sh
```

⚠️ **`--max-retries 2` depuis le 2026-08-21** (était 1, sur les 7 jobs). Un hang d'infra au démarrage — conteneur tué au `taskTimeout` de 300 s **sans avoir émis une seule ligne de log**, donc avant `main.py` — consommait l'unique retry. Observé le 21/08 sur le job 2h : tentative 1 muette pendant 300 s, retry en succès en 12 s (les runs normaux durent 10-12 s, la 2ᵉ chance est quasi gratuite). ⚠ Le réglage est **pinné dans `deploy.sh`** à 4 endroits (`COMMON_ARGS` + les 3 jobs autonomes) : le changer à chaud via `gcloud run jobs update` ne survit pas au prochain deploy. ⚠ Angle mort connu, inchangé par ce réglage : `_flush_digest` envoie le mail **avant** d'écrire dans `dispatched_actions` — un conteneur qui meurt dans cette fenêtre de quelques ms fera renvoyer le digest à la tentative suivante.

## IAM — Prérequis
`action-engine-sa` doit avoir les rôles suivants :
- `roles/bigquery.dataEditor` — lire/écrire action_triggers
- `roles/bigquery.jobUser` — exécuter les queries BQ
- `roles/secretmanager.secretAccessor` — lire alerts-gmail-sa-key
- `roles/run.invoker` — **requis pour que Cloud Scheduler puisse déclencher le job**

```bash
gcloud projects add-iam-policy-binding merveil-data-warehouse \
  --member="serviceAccount:action-engine-sa@merveil-data-warehouse.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```

## Triggering manuel
```bash
# 4h
curl -s -X POST \
  "https://europe-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/merveil-data-warehouse/jobs/merveil-action-engine:run" \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  -d '{}'

# daily
curl -s -X POST \
  "https://europe-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/merveil-data-warehouse/jobs/merveil-action-engine-daily:run" \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  -d '{}'
```

## Known Errors
| Error | Cause | Fix |
|---|---|---|
| `pending_actions` stale date | dbt image not rebuilt after modifying `dbt_project.yml` | Rebuild dbt image: `cd DWH/dbt && bash redeploy.sh` |
| `pending_actions` empty | dbt rules produced no actions | Verify dbt ran and rules are enabled in `rules.yaml` |
| Duplicate action skipped | Trigger already `open` for the same entity | Normal — cooldown active |
| `KeyError` in a handler | Missing field in `pending_actions` | Check the SQL of the corresponding dbt model |
| Gmail API 401 | DWD not enabled or wrong Client ID | Check admin.google.com → Domain delegation |
| UPDATE streaming buffer error | Row too recent in action_triggers | Wait ~90 min |
| Scheduler PERMISSION_DENIED (code 7) | SA OAuth du scheduler pas dans la policy du job Cloud Run, ou API v2 utilisée | Utiliser API v1 + `scheduler-invoker@` + `gcloud run jobs add-iam-policy-binding` |
