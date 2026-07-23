#!/usr/bin/env python3
"""Regression test runner: vergleicht Steuerberechnung gegen erwartete Werte.

Nutzt test_data/audit_expectations.json als Referenz (echte IBKR-Daten, gitignored).
Vergleicht die GUI-defaults Werte: Tageskurs-Methode, InvStG/KAP-INV und
Zuflussprinzip sind aktiv; die optionale DBA-Quellensteuer-Beta ist aus.
Damit decken die Tests genau das ab, was der User ohne Beta-Opt-in sieht.

Usage:
    python run_tests.py              # alle verfügbaren Szenarien
"""
import json, os, shlex, subprocess, sys, tempfile

SCENARIOS = {
    "audit1_haupt": {
        "source": "test_data/audit1_2024.xml",
        "extract": [
            "extract_ibkr_data.py", "test_data/audit1_2024.xml", "{out}",
            "--history", "test_data/audit1_2023_history.xml",
        ],
    },
    "audit1_zusatz": {
        "source": "test_data/audit1_2024_zusatzkonto.xml",
        "extract": [
            "extract_ibkr_data.py", "test_data/audit1_2024_zusatzkonto.xml", "{out}",
        ],
    },
    "audit2": {
        "source": "test_data/audit2_2022.xml",
        "extract": [
            "extract_ibkr_data.py", "test_data/audit2_2022.xml", "{out}",
            "--history", "test_data/audit2_2021.xml",
        ],
    },
}

FIELDS = ['zeile_19', 'zeile_20', 'zeile_22', 'zeile_23', 'zeile_41',
          'etf_net_taxable', 'etf_wht', 'kap_inv_tageskurs']

SYNTHETIC_TESTS = [
    ("Tageskurs-Bruttozuordnung", "tests/test_tageskurs_gross_bucket.py"),
    ("Cross-Year-Series-Tests", "tests/test_cross_year_series.py"),
    ("Quarterly-History-Extraction", "tests/test_quarterly_history_extraction.py"),
    ("KAP-INV-WHT", "tests/test_kap_inv_wht.py"),
    ("KAP-INV-Tageskurs-TFS", "tests/test_kap_inv_tageskurs.py"),
    ("QYLD-und-Sonderprodukte", "tests/test_qyld_and_special_products.py"),
    ("German-Dividend-Tax", "-m unittest tests/test_german_dividend_tax.py"),
    ("FX-Margin-Negative-Balance", "tests/test_fx_negative_balance.py"),
    ("Stillhalter-Row-Korrektur", "tests/test_stillhalter_row_correction.py"),
    ("Trade-Funds-Dedupe", "tests/test_dedupe.py"),
    ("FX-Rate-Parse-Ausfaelle", "tests/test_fx_rate_parse_failures.py"),
    ("Options-Event-Sammlung", "tests/test_option_event_collection.py"),
    ("Tageskurs-Korrektur-Maps", "tests/test_tageskurs_adjustment_maps.py"),
]


def compute_user_facing(rd):
    """Repliziert das GUI-final-Dict mit allen Default-Toggles aktiv (Tageskurs,
    InvStG, Zuflussprinzip). Die Werte entsprechen dem, was der User sieht.
    Logik gespiegelt aus app.py."""
    from calculate_tax_report import (
        get_kap_inv_tageskurs_delta_for_reporting,
        get_kap_inv_wht_for_reporting,
    )

    pre_z19 = rd.get('zeile_19_netto_eur', 0)
    pre_z20 = rd.get('zeile_20_stock_gains_eur', 0)
    pre_z22 = rd.get('zeile_22_other_losses_eur', 0)
    pre_z23 = rd.get('zeile_23_stock_losses_eur', 0)
    z41 = rd.get('zeile_41_withholding_tax_eur', 0)

    fx_corr_total = rd.get('fx_correction_total', 0)
    fx_corr = rd.get('fx_correction_by_topf', {}) or {}
    tk_gain = rd.get('fx_corr_gain_adj', {}) or {}
    tk_loss = rd.get('fx_corr_loss_adj', {}) or {}
    kap_inv = rd.get('kap_inv', {}) or {}
    audit = rd.get('audit', {}) or {}
    stillhalter_details = audit.get('stillhalter_details', []) or []

    # Zuflussprinzip default-on, aber nur sichtbar wenn cross_year_details
    # vorhanden. Die GUI zieht audit['cross_year_premium_eur']
    # ab; genau diesen Wert verwenden wir hier, damit ein falsch aggregiertes
    # Audit-Feld (z.B. prior_zufluss doppelt enthalten) im Test sichtbar wird.
    cross_year_details = [d for d in stillhalter_details if d.get('is_cross_year')]
    cross_year_premium = audit.get('cross_year_premium_eur', 0)
    has_cross_year_details = bool(cross_year_details)
    adj_cross = cross_year_premium if has_cross_year_details else 0

    # Tageskurs-Toggle wird in der GUI nur gezeigt wenn |fx_corr_total| > 0.01
    # Wenn nicht gezeigt → tageskurs_aktiv=False → keine
    # Korrekturen anwenden. Verhindert dass sich aufhebende Topf1/Topf2-Korrekturen
    # den Test-Vergleich verfaelschen.
    tageskurs_aktiv = abs(fx_corr_total) > 0.01

    # InvStG aktiv → KAP-INV bleibt separat, Z19-Korrektur nur fuer Topf1+Topf2.
    z19 = pre_z19 - adj_cross
    z20 = pre_z20
    z22 = pre_z22
    z23 = pre_z23
    if tageskurs_aktiv:
        z19 += fx_corr.get('Topf1', 0) + fx_corr.get('Topf2', 0)
        z20 += tk_gain.get('Topf1', 0)
        z22 -= tk_loss.get('Topf2', 0)
        z23 -= tk_loss.get('Topf1', 0)
    has_etf = bool(kap_inv.get('etf_by_isin'))
    kap_inv_tageskurs = get_kap_inv_tageskurs_delta_for_reporting(rd) if (has_etf and tageskurs_aktiv) else 0
    etf_net = (kap_inv.get('etf_net_taxable_eur', 0) + kap_inv_tageskurs) if has_etf else 0
    return {
        'zeile_19': z19,
        'zeile_20': z20,
        'zeile_22': z22,
        'zeile_23': z23,
        'zeile_41': z41,
        'etf_net_taxable': etf_net,
        'etf_wht': get_kap_inv_wht_for_reporting(kap_inv) if has_etf else 0,
        'kap_inv_tageskurs': kap_inv_tageskurs,
    }


def run_tests():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    audit_path = os.path.join(script_dir, 'test_data', 'audit_expectations.json')
    if not os.path.exists(audit_path):
        print("FEHLER: test_data/audit_expectations.json nicht gefunden.")
        print("Audit-Daten (echte IBKR-XMLs) werden lokal benötigt.")
        sys.exit(1)

    with open(audit_path) as f:
        expectations = json.load(f)

    from calculate_tax_report import calculate_tax

    passed = 0
    failed = 0
    skipped = 0

    for name, scenario in SCENARIOS.items():
        exp = expectations.get(name)
        if not exp:
            continue

        # Check if source files exist
        src_file = scenario['source']
        if not os.path.exists(src_file):
            print(f"  SKIP  {name:20s} ({exp['description']}) — Datei nicht vorhanden")
            skipped += 1
            continue

        # Extract
        with tempfile.TemporaryDirectory(prefix=f'test_{name}_') as out_dir:
            cmd = [
                sys.executable,
                *(arg.format(out=out_dir) for arg in scenario['extract']),
            ]
            completed = subprocess.run(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                check=False,
            )
            if completed.returncode != 0:
                print(f"  FAIL  {name:20s} ({exp['description']}) — Extraktion fehlgeschlagen")
                failed += 1
                continue

            # Calculate (suppress stdout)
            import io, contextlib
            with contextlib.redirect_stdout(io.StringIO()):
                rd = calculate_tax(out_dir)

        # GUI-defaults anwenden (Tageskurs+InvStG+Zufluss aktiv, DBA-Beta aus)
        user = compute_user_facing(rd)

        mismatches = []
        missing_fields = []
        audit = rd.get('audit', {}) or {}
        stillhalter_details = audit.get('stillhalter_details', []) or []
        cross_year_detail_sum = sum(d.get('premium_eur', 0) for d in stillhalter_details
                                    if d.get('is_cross_year'))
        cross_year_field = audit.get('cross_year_premium_eur', 0)
        if abs(cross_year_field - cross_year_detail_sum) > 0.01:
            mismatches.append(
                "cross_year_premium_eur inkonsistent: "
                f"Audit-Feld {round(cross_year_field, 2)}, "
                f"Cross-Year-Details {round(cross_year_detail_sum, 2)}"
            )
        for field in FIELDS:
            if field not in exp['expected']:
                missing_fields.append(field)
                continue
            actual = round(user.get(field, 0), 2)
            expected = exp['expected'][field]
            if abs(actual - expected) > 0.01:
                mismatches.append(f"{field}: erwartet {expected}, bekommen {actual}")
        if 'cross_year_put_corrections_count' in exp['expected']:
            actual = len(audit.get('cross_year_put_corrections', []) or [])
            expected = exp['expected']['cross_year_put_corrections_count']
            if actual != expected:
                mismatches.append(
                    f"cross_year_put_corrections_count: erwartet {expected}, bekommen {actual}"
                )
        if missing_fields:
            print(f"  WARN  {name:20s} ({exp['description']}) — fehlende Felder in audit_expectations.json: {', '.join(missing_fields)}")
            print(f"        Hinweis: 'test_data/' ist gitignored. Nach Schema-Updates lokales JSON manuell ergaenzen.")

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

    # Synthetische Regressionen: echte Audit-Daten decken nicht alle Edge-Cases ab.
    print(f"\n{'-'*60}")
    print("Synthetische Regressionstests")
    for label, test_cmd in SYNTHETIC_TESTS:
        print(f"  RUN   {label}")
        sys.stdout.flush()
        completed = subprocess.run(
            [sys.executable, *shlex.split(test_cmd)], check=False,
        )
        if completed.returncode != 0:
            print(f"  FAIL  {label}")
            sys.exit(1)


if __name__ == '__main__':
    run_tests()
