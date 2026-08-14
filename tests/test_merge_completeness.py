"""Completeness-Guard fuer den Multi-Account-Merge.

merge_report_data (app.py) ist eine Allowlist: jedes neue report_data-Feld
muss dort manuell eingepflegt werden. Historischer Realfall: topf2_by_category
fehlte, wodurch die "Aufschluesselung Topf 2" bei Multi-Account-Nutzern
komplett verschwand. Dieser Test vergleicht die Schluesselmengen eines echten
calculate_tax()-Outputs gegen den Merge-Output (Top-Level + audit-Subkeys) und
schlaegt fehl, sobald ein Feld beim Merge still herausfaellt.

app.py ist ein Streamlit-Skript und kann nicht importiert werden (der Import
wuerde die UI ausfuehren) — merge_report_data wird deshalb per ast extrahiert
und isoliert kompiliert.
"""
import ast
import copy
import csv
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import calculate_tax_report
from calculate_tax_report import calculate_tax

APP_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app.py')

# Felder, die bewusst NICHT gemergt werden (aktuell keine). Neue Eintraege nur
# mit Begruendung — ein Eintrag hier bedeutet: das Feld existiert im
# Einzel-Report, fehlt aber im Multi-Account-Ergebnis.
KNOWN_UNMERGED_TOP_LEVEL = set()
KNOWN_UNMERGED_AUDIT = set()


def load_merge_report_data():
    with open(APP_PATH, encoding='utf-8') as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == 'merge_report_data':
            module = ast.Module(body=[node], type_ignores=[])
            namespace = {'calculate_tax_report': calculate_tax_report, 'copy': copy}
            exec(compile(module, APP_PATH, 'exec'), namespace)
            return namespace['merge_report_data']
    raise AssertionError('merge_report_data nicht in app.py gefunden')


def build_sample_report():
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, 'statement_of_funds.csv'), 'w', newline='', encoding='utf-8') as f:
            rows = [
                {
                    'activityCode': 'DIV', 'activityDescription': 'AAPL Cash Dividend',
                    'amount': '500', 'assetCategory': 'STK', 'currency': 'EUR',
                    'date': '2025-02-14', 'fxRateToBase': '1',
                    'isin': 'US0378331005', 'reportDate': '2025-02-14',
                    'symbol': 'AAPL', 'transactionID': 'T1',
                },
                {
                    'activityCode': 'FRTAX', 'activityDescription': 'AAPL Cash Dividend - US Tax',
                    'amount': '-75', 'assetCategory': 'STK', 'currency': 'EUR',
                    'date': '2025-02-14', 'fxRateToBase': '1',
                    'isin': 'US0378331005', 'reportDate': '2025-02-14',
                    'symbol': 'AAPL', 'transactionID': 'T2',
                },
            ]
            writer = csv.DictWriter(f, fieldnames=sorted(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        with open(os.path.join(tmp, 'trades.csv'), 'w', newline='', encoding='utf-8') as f:
            rows = [{
                'tradeID': 'TR1', 'levelOfDetail': 'EXECUTION', 'assetCategory': 'STK',
                'transactionType': 'ExchTrade', 'buySell': 'SELL', 'quantity': '-10',
                'symbol': 'AAPL', 'isin': 'US0378331005',
                'dateTime': '2025-03-10 10:00:00', 'tradeDate': '2025-03-10',
                'reportDate': '2025-03-10', 'closePrice': '150', 'tradePrice': '150',
                'fifoPnlRealized': '120', 'fxRateToBase': '1', 'currency': 'EUR',
                'multiplier': '1', 'strike': '', 'expiry': '', 'putCall': '',
                'underlyingSymbol': '', 'ibCommission': '-1',
            }]
            writer = csv.DictWriter(f, fieldnames=sorted(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        return calculate_tax(tmp, tax_year=2025)


def test_merge_keeps_every_report_data_field():
    merge_report_data = load_merge_report_data()
    report = build_sample_report()
    merged = merge_report_data([copy.deepcopy(report), copy.deepcopy(report)])

    missing = set(report.keys()) - set(merged.keys()) - KNOWN_UNMERGED_TOP_LEVEL
    assert not missing, (
        f"merge_report_data verliert Top-Level-Felder: {sorted(missing)} — "
        f"Merge-Regel in app.py ergaenzen (oder bewusst in KNOWN_UNMERGED_TOP_LEVEL eintragen)"
    )

    audit_missing = (
        set(report.get('audit', {}).keys())
        - set(merged.get('audit', {}).keys())
        - KNOWN_UNMERGED_AUDIT
    )
    assert not audit_missing, (
        f"merge_report_data verliert audit-Felder: {sorted(audit_missing)} — "
        f"merged_audit in app.py ergaenzen (oder bewusst in KNOWN_UNMERGED_AUDIT eintragen)"
    )


def test_merge_sums_topf2_by_category():
    merge_report_data = load_merge_report_data()
    report = build_sample_report()
    merged = merge_report_data([copy.deepcopy(report), copy.deepcopy(report)])

    single = report.get('topf2_by_category', {})
    combined = merged.get('topf2_by_category', {})
    assert set(combined.keys()) == set(single.keys())
    for label, vals in single.items():
        assert abs(combined[label]['gain'] - 2 * vals['gain']) < 1e-9
        assert abs(combined[label]['loss'] - 2 * vals['loss']) < 1e-9


def test_merge_keeps_partnership_blocker_and_observed_amounts():
    merge_report_data = load_merge_report_data()
    report = build_sample_report()
    report['partnership_tax_items'] = {
        'US91232N2071': {
            'ticker': 'USO', 'name': 'United States Oil Fund LP',
            'classification': 'personengesellschaft',
            'status': 'blocked_missing_annual_allocation',
            'required_documents': ['K-1/K-3 fuer 2025'],
            'sources': ['https://www.sec.gov/example'],
            'observed_trade_pnl_eur': 100.0,
            'observed_distributions_eur': 20.0,
            'observed_withholding_tax_eur': -3.0,
            'observed_other_cash_eur': 5.0,
            'observed_tageskurs_delta_eur': 4.0,
            'observed_transactions': 3,
            'excluded_from_automatic_tax_calculation': True,
        },
    }
    merged = merge_report_data([copy.deepcopy(report), copy.deepcopy(report)])
    item = merged['partnership_tax_items']['US91232N2071']
    assert item['status'] == 'blocked_missing_annual_allocation'
    assert item['excluded_from_automatic_tax_calculation'] is True
    assert item['observed_trade_pnl_eur'] == 200.0
    assert item['observed_distributions_eur'] == 40.0
    assert item['observed_withholding_tax_eur'] == -6.0
    assert item['observed_other_cash_eur'] == 10.0
    assert item['observed_tageskurs_delta_eur'] == 8.0
    assert item['observed_transactions'] == 6
    assert item['required_documents'] == ['K-1/K-3 fuer 2025']
    assert item['sources'] == ['https://www.sec.gov/example']


def test_merge_sums_transaction_tax_audit():
    merge_report_data = load_merge_report_data()
    report = build_sample_report()
    report['audit']['transaction_tax'] = {
        'found_count': 3,
        'applied_count': 1,
        'applied_eur': 0.05,
        'deferred_count': 1,
        'deferred_eur': 0.02,
        'already_in_trade_count': 1,
        'historical_count': 0,
        'unmatched_count': 0,
        'details': [{'status': 'applied_to_close', 'symbol': 'ITALYOPT'}],
    }
    merged = merge_report_data([copy.deepcopy(report), copy.deepcopy(report)])
    audit = merged['audit']['transaction_tax']
    assert audit['found_count'] == 6
    assert audit['applied_count'] == 2
    assert abs(audit['applied_eur'] - 0.10) < 1e-9
    assert audit['deferred_count'] == 2
    assert abs(audit['deferred_eur'] - 0.04) < 1e-9
    assert audit['already_in_trade_count'] == 2
    assert len(audit['details']) == 2


if __name__ == '__main__':
    test_merge_keeps_every_report_data_field()
    print('OK: Merge verliert keine report_data-Felder')
    test_merge_sums_topf2_by_category()
    print('OK: topf2_by_category wird summiert')
    test_merge_keeps_partnership_blocker_and_observed_amounts()
    print('OK: Partnership-Blocker wird gemergt')
    test_merge_sums_transaction_tax_audit()
    print('OK: TTAX-Audit wird gemergt')
