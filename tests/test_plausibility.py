"""Plausibilitaetscheck (build_plausibility in app.py) als reine Funktion.

Der Check vergleicht unsere Kategoriesummen mit IBKRs unkorrigierten
CSV-Summen. Alle Stillhalter-Korrekturen muessen dafuer zurueckgerechnet
werden: bei Aktien ueber stk/etf_correction_cy, bei Future-Optionen ueber
audit['future_assignment_corrections'] (Realfall audit1: 6EZ4, 325,91 EUR).
Ohne den Future-Addback meldete der Check nach jeder FOP-Andienung eine
Abweichung in Praemienhoehe, obwohl die Rechnung stimmte.

app.py ist ein Streamlit-Skript und kann nicht importiert werden;
build_plausibility wird wie in test_merge_completeness per ast extrahiert.
"""
import ast
import copy
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import calculate_tax_report  # noqa: E402

APP_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app.py')


def load_build_plausibility():
    with open(APP_PATH, encoding='utf-8') as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == 'build_plausibility':
            module = ast.Module(body=[node], type_ignores=[])
            namespace = {'calculate_tax_report': calculate_tax_report, 'copy': copy}
            exec(compile(module, APP_PATH, 'exec'), namespace)
            return namespace['build_plausibility']
    raise AssertionError('build_plausibility nicht in app.py gefunden')


build_plausibility = load_build_plausibility()
TOGGLES = {'invstg': False}


def base_report(**overrides):
    """IBKR meldet Futures -100. Wir: Praemie 20 separat (options_gain),
    FUT-PnL um 20 bereinigt (options_loss -120). Netto identisch -100."""
    report = {
        'csv_category_totals': {
            'Aktien': {'gain': 100, 'loss': 0, 'net': 100},
            'Futures': {'gain': 0, 'loss': -100, 'net': -100},
            'Devisen': {'gain': 0, 'loss': 0, 'net': 0},
        },
        'stocks_gain_eur': 100, 'stocks_loss_eur': 0,
        'options_gain_eur': 20, 'options_loss_eur': -120,
        'fx_total_gain': 0, 'fx_total_loss': 0,
        'audit': {
            'stillhalter_premium_eur': 20,
            'future_assignment_corrections': [
                {'assignment_symbol': 'X P100', 'future_symbol': 'X',
                 'mode': 'deferred_close', 'quantity': 1.0,
                 'amount_raw': 20, 'amount_eur': 20},
            ],
        },
        'kap_inv': {},
        'anlage_so': {},
    }
    report.update(overrides)
    return report


def topf2_row(plaus):
    return next(r for r in plaus['rows'] if r['label'] == 'Sonstiges (Topf 2) Netto')


def aktien_row(plaus):
    return next(r for r in plaus['rows'] if r['label'] == 'Aktien (Topf 1) Netto')


def test_future_assignment_correction_is_added_back():
    report = base_report()
    snapshot = copy.deepcopy(report)
    plaus = build_plausibility(report, TOGGLES)
    row = topf2_row(plaus)
    assert row['match'], row
    assert abs(row['diff']) < 0.01, row
    assert plaus['all_match']
    assert report == snapshot, 'build_plausibility darf den Report nicht mutieren'
    print("  OK  Future-Korrektur wird beim IBKR-Vergleich zurueckgerechnet")


def test_same_numbers_without_correction_entry_would_mismatch():
    """Kontrolle: derselbe Report ohne Korrektur-Eintrag zeigt die 20 EUR."""
    report = base_report(audit={'stillhalter_premium_eur': 20})
    row = topf2_row(build_plausibility(report, TOGGLES))
    assert not row['match'], row
    assert abs(row['diff'] + 20) < 0.01, row
    print("  OK  ohne Addback bliebe die Praemie als Abweichung sichtbar")


def test_report_without_future_corrections_is_unchanged():
    report = base_report(options_loss_eur=-100,
                         audit={'stillhalter_premium_eur': 20})
    plaus = build_plausibility(report, TOGGLES)
    assert topf2_row(plaus)['match']
    assert plaus['all_match']
    print("  OK  Reports ohne Future-Optionen rechnen wie bisher")


def test_stock_correction_addback_still_applies():
    """Aktien-Addback (stk_correction_cy) bleibt unabhaengig vom FUT-Pfad."""
    report = base_report(
        stocks_gain_eur=80,
        audit={'stillhalter_premium_eur': 20, 'stk_correction_cy': 20,
               'future_assignment_corrections': [{'amount_eur': 20}]},
    )
    plaus = build_plausibility(report, TOGGLES)
    assert aktien_row(plaus)['match'], aktien_row(plaus)
    assert topf2_row(plaus)['match'], topf2_row(plaus)
    assert plaus['all_match']
    print("  OK  Aktien- und Future-Addback wirken unabhaengig")


if __name__ == '__main__':
    print("Plausibilitaetscheck: Stillhalter-Addbacks")
    test_future_assignment_correction_is_added_back()
    test_same_numbers_without_correction_entry_would_mismatch()
    test_report_without_future_corrections_is_unchanged()
    test_stock_correction_addback_still_applies()
    print("OK: Plausibilitaetscheck rechnet Aktien- und Future-Korrekturen zurueck")
