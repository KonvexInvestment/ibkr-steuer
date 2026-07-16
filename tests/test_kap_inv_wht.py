"""Regression tests for KAP-INV withholding-tax reporting."""
import contextlib
import csv
import io
import math
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (
    calculate_creditable_foreign_tax,
    calculate_tax,
    get_kap_line_41_for_reporting,
    get_kap_inv_wht_for_reporting,
    get_withholding_tax_for_reporting,
    merge_kap_inv_wht_for_reporting,
)


def calculate_for_funds(funds):
    fieldnames = sorted({k for row in funds for k in row})
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "account_info.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["currency", "tax_year", "fx_transactions_count"])
            writer.writeheader()
            writer.writerow({"currency": "EUR", "tax_year": "2025", "fx_transactions_count": "0"})
        with open(os.path.join(tmp, "statement_of_funds.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(funds)
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp)


def test_creditable_tax_uses_caps_instead_of_proportional_tfs_reduction():
    equity_fund = calculate_creditable_foreign_tax(1000, 150, 0, 0.30, 0.15)
    assert round(equity_fund["taxable_distribution_eur"], 2) == 700.00
    assert round(equity_fund["german_cap_eur"], 2) == 175.00
    assert round(equity_fund["creditable_tax_eur"], 2) == 150.00
    assert equity_fund["status"] == "matched"

    excessive_us_tax = calculate_creditable_foreign_tax(1000, 300, 0, 0.30, 0.15)
    assert round(excessive_us_tax["creditable_tax_eur"], 2) == 150.00
    assert round(excessive_us_tax["excess_tax_eur"], 2) == 150.00
    assert excessive_us_tax["status"] == "capped_review_refund"

    foreign_property_fund = calculate_creditable_foreign_tax(
        10000, 1500, 0, 0.80, 0.15
    )
    assert round(foreign_property_fund["german_cap_eur"], 2) == 500.00
    assert round(foreign_property_fund["creditable_tax_eur"], 2) == 500.00

    other_fund = calculate_creditable_foreign_tax(1000, 150, 0, 0.0, 0.15)
    assert round(other_fund["creditable_tax_eur"], 2) == 150.00

    unmatched_withholding = calculate_creditable_foreign_tax(
        0, 25, 0, 0.30, 0.15
    )
    assert unmatched_withholding["creditable_tax_eur"] == 0.0
    assert unmatched_withholding["status"] == "unmatched_withholding"
    assert unmatched_withholding["review_required"]

    # Cent-/FX-Rundungstoleranz: 15%-Einbehalt in USD liegt nach
    # EUR-Umrechnung Zehntel-Cents ueber gross_EUR x 0.15 -> kein Kappen
    rounding_noise = calculate_creditable_foreign_tax(99.37, 14.908, 0, 0.0, 0.15)
    assert round(rounding_noise["creditable_tax_eur"], 4) == 14.908
    assert rounding_noise["status"] == "matched"
    # Echte Ueberschreitung (> 2 Cent) kappt weiterhin
    real_breach = calculate_creditable_foreign_tax(100, 15.10, 0, 0.0, 0.15)
    assert round(real_breach["creditable_tax_eur"], 2) == 15.00


def test_kap_inv_wht_anrechenbar_uses_event_cap():
    rd = calculate_for_funds([
        {
            "activityCode": "DIV",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "1000",
            "currency": "EUR",
            "subCategory": "ETF",
            "isin": "US78462F1030",
            "symbol": "SPY",
        },
        {
            "activityCode": "WHT",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "-150",
            "currency": "EUR",
            "subCategory": "ETF",
            "isin": "US78462F1030",
            "symbol": "SPY",
        },
    ])
    kap_inv = rd["kap_inv"]
    assert round(kap_inv["etf_wht_eur"], 2) == 150.00
    assert round(kap_inv["etf_wht_anrechenbar_eur"], 2) == 150.00
    assert round(get_kap_inv_wht_for_reporting(kap_inv), 2) == 150.00
    assert round(rd["zeile_41_withholding_tax_eur"], 2) == 150.00


def test_kap_inv_wht_reporting_falls_back_for_legacy_data():
    assert get_kap_inv_wht_for_reporting({"etf_wht_eur": 150.0}) == 150.0


def test_line_41_invstg_toggle_replaces_fund_tax_without_double_counting():
    report = {
        "zeile_41_withholding_tax_eur": 160.0,
        "withholding_tax_eur": 10.0,
        "kap_inv": {
            "etf_wht_eur": 300.0,
            "etf_wht_anrechenbar_eur": 150.0,
        },
    }
    assert get_kap_line_41_for_reporting(report, True) == 160.0
    assert get_kap_line_41_for_reporting(report, False) == 310.0

    legacy_report = {
        "withholding_tax_eur": 10.0,
        "kap_inv": {"etf_wht_eur": 150.0},
    }
    assert get_kap_line_41_for_reporting(legacy_report, True) == 160.0
    assert get_kap_line_41_for_reporting(legacy_report, False) == 160.0


def test_multi_account_sums_finished_credits_without_global_recap():
    account_one = calculate_creditable_foreign_tax(1000, 300, 0, 0.30, 0.15)
    account_two = calculate_creditable_foreign_tax(1000, 0, 0, 0.30, 0.15)
    merged = merge_kap_inv_wht_for_reporting([
        {"etf_wht_anrechenbar_eur": account_one["creditable_tax_eur"]},
        {"etf_wht_anrechenbar_eur": account_two["creditable_tax_eur"]},
    ])
    assert merged == 150.0
    globally_recalculated = calculate_creditable_foreign_tax(
        2000, 300, 0, 0.30, 0.15
    )["creditable_tax_eur"]
    assert globally_recalculated == 300.0


def test_verified_us_funds_have_treaty_rate():
    from etf_classification import get_foreign_tax_treaty_rate
    # US-domizilierte InvStG-Fonds der Tabelle: DBA-USA 15 %
    assert get_foreign_tax_treaty_rate("US46090E1038") == 0.15  # QQQ
    assert get_foreign_tax_treaty_rate("US37954Y4834") == 0.15  # QYLD
    # no_invstg-Produkte (ETN) laufen nicht ueber den Fonds-QSt-Pfad
    assert get_foreign_tax_treaty_rate("US06748M1962") is None  # VXX
    # Nicht-RIC-Strukturen (LP/Grantor Trust): keine Dividenden i.S.d.
    # Art. 10 DBA-USA -> kein 15%-Blanket, bleiben dba_unverified
    assert get_foreign_tax_treaty_rate("US91232N2071") is None  # USO (LP)
    assert get_foreign_tax_treaty_rate("US46138K1034") is None  # FXE (Trust)
    # unbekannte ISINs bleiben unbelegt
    assert get_foreign_tax_treaty_rate("IE00B4L5Y983") is None


def test_paid_short_distributions_are_excluded_from_form_lines():
    common = {
        "currency": "EUR",
        "subCategory": "ETF",
        "isin": "US78462F1030",
        "symbol": "SPY",
    }
    rd = calculate_for_funds([
        {
            **common,
            "activityCode": "DIV",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "100",
        },
        {
            **common,
            "activityCode": "PIL",
            "reportDate": "2025-06-15",
            "date": "2025-06-15",
            "amount": "-50",
        },
    ])
    spy = rd["kap_inv"]["etf_by_isin"]["US78462F1030"]
    assert round(spy["div"], 2) == 50.00
    assert round(spy["div_received"], 2) == 100.00
    assert round(spy["div_paid"], 2) == -50.00
    form = rd["kap_inv_form"]
    lines = {line["line"]: line for line in form["lines"]}
    # Zeile 4 zeigt nur die ZUGEFLOSSENE Ausschuettung, kein Netting
    assert round(lines[4]["amount_raw_eur"], 2) == 100.00
    paid = form["negative_distribution_details"]
    assert len(paid) == 1
    assert paid[0]["isin"] == "US78462F1030"
    assert round(paid[0]["paid_distribution_eur"], 2) == -50.00
    assert form["status"] == "paid_distribution_review_required"
    assert any("gezahlte" in w.lower() for w in form["warnings"])


def test_withholding_tax_reporting_normalizes_zero():
    reported = get_withholding_tax_for_reporting(0)
    assert reported == 0.0
    assert math.copysign(1, reported) == 1.0


def test_kap_inv_wht_refunds_keep_their_sign():
    common = {
        "currency": "EUR",
        "subCategory": "ETF",
        "isin": "US78462F1030",
        "symbol": "SPY",
    }
    mixed = calculate_for_funds([
        {
            **common,
            "activityCode": "DIV",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "1000",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "-150",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-04-01",
            "date": "2025-04-01",
            "amount": "50",
        },
    ])["kap_inv"]
    assert round(mixed["etf_wht_eur"], 2) == 100.00
    assert round(mixed["etf_wht_anrechenbar_eur"], 2) == 100.00

    # Erstattung des NICHT angerechneten Ueberhangs darf Zeile 41 nicht
    # kuerzen: 300 einbehalten, DBA-Limit 150 -> 150 angerechnet, 150
    # Ueberhang. Spaetere Erstattung 150 konsumiert nur den Ueberhang.
    excess_refund = calculate_for_funds([
        {
            **common,
            "activityCode": "DIV",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "1000",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "-300",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-05-01",
            "date": "2025-05-01",
            "amount": "150",
        },
    ])
    excess_kap_inv = excess_refund["kap_inv"]
    assert round(excess_kap_inv["etf_wht_anrechenbar_eur"], 2) == 150.00
    assert round(excess_refund["zeile_41_withholding_tax_eur"], 2) == 150.00
    offset_events = [
        e for e in excess_kap_inv["wht_events"]
        if e.get("status") == "refund_offsets_excess"
    ]
    assert len(offset_events) == 1
    assert round(offset_events[0]["excess_offset_eur"], 2) == 150.00

    # Erstattung UEBER den Ueberhang hinaus kuerzt die Anrechnung um den Rest.
    over_refund = calculate_for_funds([
        {
            **common,
            "activityCode": "DIV",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "1000",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "-300",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-05-01",
            "date": "2025-05-01",
            "amount": "200",
        },
    ])["kap_inv"]
    assert round(over_refund["etf_wht_anrechenbar_eur"], 2) == 100.00

    refund_only = calculate_for_funds([
        {
            **common,
            "activityCode": "DIV",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "1000",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-04-01",
            "date": "2025-04-01",
            "amount": "20",
        },
    ])["kap_inv"]
    assert round(refund_only["etf_wht_eur"], 2) == -20.00
    assert round(refund_only["etf_wht_anrechenbar_eur"], 2) == -20.00
    assert round(get_kap_inv_wht_for_reporting(refund_only), 2) == -20.00
    review = refund_only["wht_review_items"]
    assert len(review) == 1
    assert review[0]["status"] == "unmatched_refund"


if __name__ == "__main__":
    test_creditable_tax_uses_caps_instead_of_proportional_tfs_reduction()
    test_kap_inv_wht_anrechenbar_uses_event_cap()
    test_kap_inv_wht_reporting_falls_back_for_legacy_data()
    test_line_41_invstg_toggle_replaces_fund_tax_without_double_counting()
    test_multi_account_sums_finished_credits_without_global_recap()
    test_verified_us_funds_have_treaty_rate()
    test_paid_short_distributions_are_excluded_from_form_lines()
    test_withholding_tax_reporting_normalizes_zero()
    test_kap_inv_wht_refunds_keep_their_sign()
    print("OK: KAP-INV WHT reporting")
