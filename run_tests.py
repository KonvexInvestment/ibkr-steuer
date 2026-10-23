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
    # Echte franzoesische Finanztransaktionssteuer als Tagesaggregat ueber
    # zwei Kauf-Fills derselben Sekunde, Verkauf am Folgetag in zwei Fills
    # derselben Sekunde mit vollstaendigen CLOSED_LOTs (Issue #89).
    "ftt_tagesaggregat_2024": {
        "source": "test_data/ftt_tagesaggregat_2024.xml",
        "extract": [
            "extract_ibkr_data.py", "test_data/ftt_tagesaggregat_2024.xml",
            "{out}",
        ],
    },
}

FIELDS = ['zeile_19', 'zeile_20', 'zeile_22', 'zeile_23', 'zeile_41',
          'etf_net_taxable', 'etf_wht', 'kap_inv_tageskurs']

SYNTHETIC_TESTS = [
    ("Tageskurs-alle-Fremdwaehrungen", "tests/test_nonusd_tageskurs.py"),
    ("Kontoinformation-Fallback", "tests/test_account_info_fallback.py"),
    ("Pflichtfelder-Trades", "tests/test_required_trade_fields.py"),
    ("Tageskurs-Bruttozuordnung", "tests/test_tageskurs_gross_bucket.py"),
    ("Cross-Year-Series-Tests", "tests/test_cross_year_series.py"),
    ("Quarterly-History-Extraction", "tests/test_quarterly_history_extraction.py"),
    ("KAP-INV-WHT", "tests/test_kap_inv_wht.py"),
    ("KAP-INV-Tageskurs-TFS", "tests/test_kap_inv_tageskurs.py"),
    ("KAP-INV-Put-ROC-Basis", "tests/test_kap_inv_put_roc_basis.py"),
    ("QYLD-und-Sonderprodukte", "tests/test_qyld_and_special_products.py"),
    ("Produktklassifikation-und-ISIN-Evidenz", "tests/test_product_classification_evidence.py"),
    ("German-Dividend-Tax", "-m unittest tests/test_german_dividend_tax.py"),
    ("FX-Margin-Negative-Balance", "tests/test_fx_negative_balance.py"),
    ("Stillhalter-Row-Korrektur", "tests/test_stillhalter_row_correction.py"),
    ("Trade-Funds-Dedupe", "tests/test_dedupe.py"),
    ("FX-Rate-Parse-Ausfaelle", "tests/test_fx_rate_parse_failures.py"),
    ("Options-Event-Sammlung", "tests/test_option_event_collection.py"),
    ("Tageskurs-Korrektur-Maps", "tests/test_tageskurs_adjustment_maps.py"),
    ("Underlying-Symbol-Aliasse", "tests/test_underlying_symbol_matching.py"),
    ("Instrumentenkategorie-Routing", "tests/test_asset_category_routing.py"),
    ("Transaktionssteuern-TTAX", "tests/test_transaction_tax.py"),
    ("Merge-Completeness", "tests/test_merge_completeness.py"),
    ("Plausibilitaetscheck", "tests/test_plausibility.py"),
    ("UI-Eintragungsuebersicht", "tests/test_ui_result_summary.py"),
    ("UI-Model-Schicht", "tests/test_ui_model.py"),
    ("App-Verhalten (AppTest)", "tests/test_app_ui.py"),
]


def compute_user_facing(rd):
    """Duenner Adapter auf den GEMEINSAMEN Builder (ui_model.build_final_values).

    GUI (app.py) und Tests nutzen exakt dieselbe Arithmetik; hier wird nur
    noch auf die Feldnamen der audit_expectations gemappt. Getestet werden
    die GUI-Default-Toggles (Tageskurs, InvStG, Zuflussprinzip aktiv je nach
    Verfuegbarkeit; DBA-Beta und Variante B aus)."""
    import ui_model

    availability = ui_model.toggle_availability(rd)
    toggles = ui_model.effective_toggles(
        ui_model.default_toggles(), availability,
    )
    final = ui_model.build_final_values(rd, toggles)
    return {
        'zeile_19': final['zeile_19'],
        'zeile_20': final['zeile_20'],
        'zeile_22': final['zeile_22'],
        'zeile_23': final['zeile_23'],
        'zeile_41': final['quellensteuer'],
        'etf_net_taxable': final['etf_net_taxable'],
        'etf_wht': final['etf_wht'],
        'kap_inv_tageskurs': final['tageskurs_kapinv_corr'],
    }


def tageskurs_rate_mismatches(rd, out_dir):
    """Unabhaengige Gegenprobe der Tageskurs-Korrektur gegen IBKRs Daten.

    Auf EUR-Basiskonten gilt in allen echten Exporten: Ein Boersen-Fill
    (ExchTrade) traegt als fxRateToBase IBKRs Tageskurs (ConversionRate)
    seines tradeDate. Fuer jede Lot-Seite, deren eroeffnender bzw.
    schliessender Fill im Export steht, prueft die Gegenprobe deshalb:
    Kurstag == tradeDate des Fills; Kurs == fxRateToBase des Fills
    (ExchTrade) bzw. == ConversionRate des tradeDate (BookTrade, dessen
    fxRateToBase der 16:20-Settlement-Kurs ist). So prueft der Test gegen
    IBKR statt gegen den eigenen Code. CASH, TradeCancel und Fills
    ausserhalb des Exports werden uebersprungen.

    Rueckgabe (problems, compared_sides). Gibt es Details, aber keine
    einzige verglichene Seite, ist das selbst ein Problem: dann ist die
    Zuordnung Detail -> Lot -> Fill gebrochen und der Test waere wertlos.
    """
    from calculate_tax_report import load_csv, safe_float

    details = rd.get('fx_correction_details', []) or []
    if rd.get('base_currency') != 'EUR' or not details:
        return [], 0
    trades_path = os.path.join(out_dir, 'trades.csv')
    lots_path = os.path.join(out_dir, 'closed_lots.csv')
    if not (os.path.exists(trades_path) and os.path.exists(lots_path)):
        return [], 0
    conversion = {}
    rates_path = os.path.join(out_dir, 'conversion_rates.csv')
    if os.path.exists(rates_path):
        for row in load_csv(rates_path):
            if row.get('toCurrency') == 'EUR':
                conversion[(row.get('fromCurrency', ''),
                            row.get('reportDate', ''))] = safe_float(
                                row.get('rate'), 0)
    fills = {}
    for trade in load_csv(trades_path):
        kind = trade.get('transactionType')
        if kind in ('ExchTrade', 'BookTrade') and \
                trade.get('assetCategory') != 'CASH':
            fills[(trade.get('conid', ''), trade.get('dateTime', ''))] = (
                kind, (trade.get('tradeDate') or '')[:10],
                safe_float(trade.get('fxRateToBase'), 0))
    expected = {}
    for lot in load_csv(lots_path):
        key = (lot.get('symbol', ''), lot.get('openDateTime', ''),
               (lot.get('reportDate') or lot.get('dateTime') or '')[:10],
               lot.get('quantity', ''))
        conid = lot.get('conid', '')
        expected.setdefault(key, []).append((
            fills.get((conid, lot.get('openDateTime', ''))),
            fills.get((conid, lot.get('dateTime', ''))),
        ))
    problems = []
    compared = 0
    for detail in details:
        key = (detail.get('symbol', ''), detail.get('openDateTime', ''),
               detail.get('reportDate', ''), detail.get('quantity', ''))
        currency = detail.get('currency', '')
        for open_fill, close_fill in expected.get(key, []):
            for side, used, used_day, fill in (
                    ('Kauf', detail.get('fx_open'),
                     detail.get('fx_open_date'), open_fill),
                    ('Verkauf', detail.get('fx_close'),
                     detail.get('fx_close_date'), close_fill)):
                if not fill:
                    continue
                kind, trade_day, fill_rate = fill
                ibkr = (fill_rate if kind == 'ExchTrade'
                        else conversion.get((currency, trade_day)))
                compared += 1
                if used_day and trade_day and used_day != trade_day:
                    problems.append(
                        f"Tageskurs {side} {key[0]}: Kurstag {used_day}, "
                        f"IBKR-tradeDate {trade_day} ({kind})")
                elif ibkr and abs(used - ibkr) > 1e-6 * max(1.0, abs(ibkr)):
                    problems.append(
                        f"Tageskurs {side} {key[0]} ({trade_day}, {kind}): "
                        f"verwendet {used}, IBKR {ibkr}")
    if compared == 0:
        problems.append(
            "Tageskurs-Gegenprobe ohne Abdeckung: kein Lot liess sich einem "
            "IBKR-Fill zuordnen (Zuordnung Detail -> Lot -> Fill pruefen)")
    return problems, compared


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
            rate_problems, rate_sides = tageskurs_rate_mismatches(rd, out_dir)

        # GUI-defaults anwenden (Tageskurs+InvStG+Zufluss aktiv, DBA-Beta aus)
        user = compute_user_facing(rd)

        mismatches = list(rate_problems[:10])
        if len(rate_problems) > 10:
            mismatches.append(
                f"... {len(rate_problems) - 10} weitere Tageskurs-Abweichungen")
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
            print(f"  OK    {name:20s} Z19={z19:>12.2f}  ({exp['description']})"
                  + (f"  [Tageskurs-Gegenprobe: {rate_sides} Vergleiche]"
                     if rate_sides else ''))
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
