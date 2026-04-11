#!/usr/bin/env python3
"""Regression test runner: vergleicht Steuerberechnung gegen erwartete Werte.

Nutzt test_expectations.json als Referenz. Läuft sowohl mit Demo-Daten (im Repo)
als auch mit lokalen Audit-Daten (test_data/, gitignored).

Usage:
    python run_tests.py              # alle verfügbaren Szenarien
    python run_tests.py demo         # nur Demo-Szenarien (im Repo)
    python run_tests.py audit        # nur Audit-Szenarien (lokal)
"""
import json, os, sys, tempfile

SCENARIOS = {
    "demo_eur_single": {
        "extract": "python extract_ibkr_data.py demo/demo_2025.xml {out}",
    },
    "demo_eur_multi": {
        "extract": "python extract_ibkr_data.py demo/demo_2025.xml {out} --fx-history demo/demo_2021.xml demo/demo_2022.xml demo/demo_2023.xml demo/demo_2024.xml",
    },
    "demo_ext": {
        "extract": "python extract_ibkr_data.py demo/demo2_ext_2025.xml {out} --fx-history demo/demo2_ext_2024.xml",
    },
    "demo_usd": {
        "extract": "python extract_ibkr_data.py demo/demo_usd_2025.xml {out}",
    },
    "audit1_haupt": {
        "extract": "python extract_ibkr_data.py test_data/audit1_2024.xml {out} --history test_data/audit1_2023_history.xml",
    },
    "audit1_zusatz": {
        "extract": "python extract_ibkr_data.py test_data/audit1_2024_zusatzkonto.xml {out}",
    },
    "audit2": {
        "extract": "python extract_ibkr_data.py test_data/audit2_2022.xml {out} --history test_data/audit2_2021.xml",
    },
}

FIELDS = ['zeile_19', 'zeile_20', 'zeile_22', 'zeile_23', 'zeile_41']
FIELD_KEYS = {
    'zeile_19': 'zeile_19_netto_eur',
    'zeile_20': 'zeile_20_stock_gains_eur',
    'zeile_22': 'zeile_22_other_losses_eur',
    'zeile_23': 'zeile_23_stock_losses_eur',
    'zeile_41': 'zeile_41_withholding_tax_eur',
}


def run_tests(filter_mode=None):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    with open('test_expectations.json') as f:
        expectations = json.load(f)

    # Merge local audit expectations (gitignored) if available
    audit_path = os.path.join(script_dir, 'test_data', 'audit_expectations.json')
    if os.path.exists(audit_path):
        with open(audit_path) as f:
            expectations.update(json.load(f))

    from calculate_tax_report import calculate_tax

    passed = 0
    failed = 0
    skipped = 0

    for name, scenario in SCENARIOS.items():
        exp = expectations.get(name)
        if not exp:
            continue

        is_audit = not exp.get('in_repo', True)
        if filter_mode == 'demo' and is_audit:
            continue
        if filter_mode == 'audit' and not is_audit:
            continue

        # Check if source files exist
        extract_cmd = scenario['extract'].format(out='/tmp/_test_check')
        src_file = extract_cmd.split()[2]  # first XML path
        if not os.path.exists(src_file):
            print(f"  SKIP  {name:20s} ({exp['description']}) — Datei nicht vorhanden")
            skipped += 1
            continue

        # Extract
        out_dir = tempfile.mkdtemp(prefix=f'test_{name}_')
        cmd = scenario['extract'].format(out=out_dir)
        os.system(f"{cmd} > /dev/null 2>&1")

        # Calculate (suppress stdout)
        import io, contextlib
        with contextlib.redirect_stdout(io.StringIO()):
            rd = calculate_tax(out_dir)

        # Compare
        mismatches = []
        for field in FIELDS:
            actual = round(rd.get(FIELD_KEYS[field], 0), 2)
            expected = exp['expected'][field]
            if abs(actual - expected) > 0.01:
                mismatches.append(f"{field}: erwartet {expected}, bekommen {actual}")

        if mismatches:
            print(f"  FAIL  {name:20s} ({exp['description']})")
            for m in mismatches:
                print(f"        {m}")
            failed += 1
        else:
            z19 = exp['expected']['zeile_19']
            print(f"  OK    {name:20s} Z19={z19:>12.2f}  ({exp['description']})")
            passed += 1

    print(f"\n{'='*60}")
    print(f"Ergebnis: {passed} OK, {failed} FAIL, {skipped} SKIP")
    if failed > 0:
        sys.exit(1)


if __name__ == '__main__':
    mode = sys.argv[1] if len(sys.argv) > 1 else None
    run_tests(mode)
