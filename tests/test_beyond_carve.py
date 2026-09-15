"""Tests du découpage des règles équipe dans beyond_push (chevauchement partiel).

Le repo n'a pas de pytest (image Cloud Run volontairement mince) : script
autonome, asserts stdlib.

    python tests/test_beyond_carve.py     # → "N/N ok" et exit 0

Couvre l'incident DAL40-1D des 14-15/09 : une règle équipe qui recouvre
PARTIELLEMENT une fenêtre voulue faisait tomber le PATCH en 400 global, donc
aucun plancher poussé sur le listing. Et la contrepartie du découpage : la
règle équipe doit se RECOLLER quand notre fenêtre s'en va, sinon la nuit perd
le plancher équipe qui la couvrait avant nous.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import src.handlers.beyond_push as bp  # noqa: E402


class _Resp:
    def __init__(self, payload=None, status=200):
        self.status_code = status
        self._payload = payload or {}
        self.text = ""

    def json(self):
        return self._payload


def _patch_beyond(current_rules):
    """Remplace l'appel HTTP : GET rend `current_rules`, PATCH capture la liste."""
    captured = {}

    def fake(method, path, payload=None):
        if method == "GET":
            return _Resp({"data": {"attributes": {"seasonal-prices": current_rules}}})
        captured["sent"] = payload["data"]["attributes"]["seasonal-prices"]
        return _Resp()

    bp._beyond = fake
    return captured


def _rule(start, end, mn, mx=None, rollover=False):
    return {"start-date": start, "end-date": end, "rollover": rollover,
            "min-price": mn, "max-price": mx}


def _run(current, desired, owned=frozenset()):
    captured = _patch_beyond(current)
    logs, errs = bp._reconcile_listing(
        2315668, "P02-DAL40-1D", desired, set(owned), "TEST")
    return captured.get("sent"), logs, errs


def _keys(rules):
    """Plages triées : Beyond se moque de l'ordre, le PATCH envoie équipe puis nous."""
    return sorted((r["start-date"], r["end-date"], r.get("min-price")) for r in rules)


CASES = []


def case(fn):
    CASES.append(fn)
    return fn


@case
def test_chevauchement_partiel_decoupe():
    """DAL40 : équipe 15/09→31/10 @290, voulu 23/09 @441 → 3 plages disjointes."""
    sent, _, errs = _run(
        [_rule("2026-09-15", "2026-10-31", 290.0)],
        {("2026-09-23", "2026-09-23"): {"min": 441.0, "max": None}})
    assert not errs, errs
    assert _keys(sent) == [
        ("2026-09-15", "2026-09-22", 290.0),
        ("2026-09-23", "2026-09-23", 441.0),
        ("2026-09-24", "2026-10-31", 290.0),
    ], _keys(sent)


@case
def test_bord_gauche_un_seul_fragment():
    sent, _, _ = _run(
        [_rule("2026-09-23", "2026-10-31", 290.0)],
        {("2026-09-23", "2026-09-23"): {"min": 441.0, "max": None}})
    assert _keys(sent) == [
        ("2026-09-23", "2026-09-23", 441.0),
        ("2026-09-24", "2026-10-31", 290.0),
    ], _keys(sent)


@case
def test_bord_droit_un_seul_fragment():
    sent, _, _ = _run(
        [_rule("2026-09-15", "2026-09-23", 290.0)],
        {("2026-09-23", "2026-09-23"): {"min": 441.0, "max": None}})
    assert _keys(sent) == [
        ("2026-09-15", "2026-09-22", 290.0),
        ("2026-09-23", "2026-09-23", 441.0),
    ], _keys(sent)


@case
def test_fenetre_2n_englobe_la_regle_equipe():
    """Règle équipe entièrement couverte → elle disparaît, son min est absorbé."""
    sent, _, _ = _run(
        [_rule("2026-09-23", "2026-09-24", 500.0)],
        {("2026-09-23", "2026-09-24"): {"min": 441.0, "max": None}})
    # min-bump : 441 relevé à 500, on ne casse jamais un plancher équipe
    assert _keys(sent) == [("2026-09-23", "2026-09-24", 500.0)], _keys(sent)


@case
def test_egalite_exacte_inchangee():
    sent, _, _ = _run(
        [_rule("2026-09-23", "2026-09-23", 290.0)],
        {("2026-09-23", "2026-09-23"): {"min": 441.0, "max": None}})
    assert _keys(sent) == [("2026-09-23", "2026-09-23", 441.0)], _keys(sent)


@case
def test_rollover_traverse_intacte_et_ne_bloque_aucune_fenetre():
    """⛔ Le cas qui a coûté 244 planchers le 15/09.

    Presque tout le parc porte une saisonnière équipe ANNUELLE (ex. 01/09→31/10
    @ 330). Une version du découpage renonçait à notre fenêtre dès qu'une
    rollover la couvrait : le déclaratif a vu 245 fenêtres absentes de l'état
    voulu et les a RETIRÉES de Beyond. Or Beyond accepte une datée dans la
    plage d'une rollover (mesuré le 14/09 : la yearly 190 € ne prime pas, nos
    nuits publient bien 441 €). Les rollover traversent donc intactes, et ne
    font jamais disparaître une fenêtre.
    """
    sent, logs, errs = _run(
        [_rule("2026-01-01", "2026-12-31", 190.0, rollover=True),
         _rule("2026-09-15", "2026-10-31", 290.0)],
        {("2026-09-23", "2026-09-23"): {"min": 441.0, "max": None},
         ("2026-09-25", "2026-09-25"): {"min": 400.0, "max": None}})
    assert not errs, errs
    assert not [l for l in logs if l["status"] == "error"], logs
    assert _keys(sent) == [
        ("2026-01-01", "2026-12-31", 190.0),      # rollover, intacte
        ("2026-09-15", "2026-09-22", 290.0),
        ("2026-09-23", "2026-09-23", 441.0),
        ("2026-09-24", "2026-09-24", 290.0),
        ("2026-09-25", "2026-09-25", 400.0),
        ("2026-09-26", "2026-10-31", 290.0),
    ], _keys(sent)


@case
def test_rollover_seule_aucune_regle_datee_a_decouper():
    """Le cas le plus fréquent du parc : une rollover, et rien d'autre."""
    sent, _, errs = _run(
        [_rule("2026-09-01", "2026-10-31", 330.0, rollover=True)],
        {("2026-09-23", "2026-09-23"): {"min": 441.0, "max": None}})
    assert not errs, errs
    assert _keys(sent) == [
        ("2026-09-01", "2026-10-31", 330.0),
        ("2026-09-23", "2026-09-23", 441.0),
    ], _keys(sent)


@case
def test_aucune_fenetre_n_est_jamais_ecartee_en_silence():
    """Garde-fou transverse : sur ce job, écarter une fenêtre la DÉPROTÈGE.

    Toute fenêtre voulue doit finir soit dans le PATCH, soit dans une erreur
    explicite — jamais évaporée.
    """
    desired = {("2026-09-23", "2026-09-23"): {"min": 441.0, "max": None},
               ("2026-09-28", "2026-09-28"): {"min": 400.0, "max": None}}
    sent, logs, errs = _run(
        [_rule("2026-09-01", "2026-10-31", 330.0, rollover=True),
         _rule("2026-09-15", "2026-10-31", 290.0)],
        dict(desired))
    poussees = {(s, e) for s, e, _ in _keys(sent)}
    signalees = {(l["start_date"], l["end_date"]) for l in logs if l["status"] == "error"}
    for k in desired:
        assert k in poussees or k in signalees, (k, _keys(sent), errs)


@case
def test_recollage_apres_retrait_de_la_fenetre():
    """Gap comblé → fenêtre retirée → la règle équipe doit redevenir entière.

    Sans recollage la nuit du 23/09 retomberait sur le min-price de listing.
    """
    sent, _, errs = _run(
        [_rule("2026-09-15", "2026-09-22", 290.0),
         _rule("2026-09-23", "2026-09-23", 441.0),
         _rule("2026-09-24", "2026-10-31", 290.0)],
        {},                                      # plus aucune cible
        owned={("2026-09-23", "2026-09-23")})
    assert not errs, errs
    assert _keys(sent) == [("2026-09-15", "2026-10-31", 290.0)], _keys(sent)


@case
def test_pas_de_recollage_par_dessus_une_fenetre_vivante():
    """Le trou porte encore notre fenêtre → les fragments restent séparés."""
    sent, _, _ = _run(
        [_rule("2026-09-15", "2026-09-22", 290.0),
         _rule("2026-09-23", "2026-09-23", 441.0),
         _rule("2026-09-24", "2026-10-31", 290.0)],
        {("2026-09-23", "2026-09-23"): {"min": 441.0, "max": None},
         ("2026-09-26", "2026-09-26"): {"min": 450.0, "max": None}},
        owned={("2026-09-23", "2026-09-23")})
    assert ("2026-09-15", "2026-10-31", 290.0) not in _keys(sent), _keys(sent)
    assert ("2026-09-23", "2026-09-23", 441.0) in _keys(sent), _keys(sent)


@case
def test_prix_differents_jamais_recolles():
    sent, _, _ = _run(
        [_rule("2026-09-15", "2026-09-22", 290.0),
         _rule("2026-09-24", "2026-10-31", 310.0)],
        {("2026-09-26", "2026-09-26"): {"min": 450.0, "max": None}})
    assert ("2026-09-15", "2026-10-31", 290.0) not in _keys(sent), _keys(sent)


def main():
    bp.SHADOW_MODE = False
    ok = 0
    for fn in CASES:
        try:
            fn()
            ok += 1
        except AssertionError as exc:
            print(f"✗ {fn.__name__}: {exc}")
    print(f"{ok}/{len(CASES)} ok")
    return 0 if ok == len(CASES) else 1


if __name__ == "__main__":
    sys.exit(main())
