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
    test_withholding_tax_reporting_normalizes_zero()
    test_kap_inv_wht_refunds_keep_their_sign()
    print("OK: KAP-INV WHT reporting")
