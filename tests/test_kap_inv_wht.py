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
    calculate_tax,
    get_kap_inv_wht_for_reporting,
    get_withholding_tax_for_reporting,
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


def test_kap_inv_wht_anrechenbar_after_teilfreistellung():
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
    assert round(kap_inv["etf_wht_anrechenbar_eur"], 2) == 105.00
    assert round(get_kap_inv_wht_for_reporting(kap_inv), 2) == 105.00


def test_kap_inv_wht_reporting_falls_back_for_legacy_data():
    assert get_kap_inv_wht_for_reporting({"etf_wht_eur": 150.0}) == 150.0


def test_withholding_tax_reporting_normalizes_zero():
    reported = get_withholding_tax_for_reporting(0)
    assert reported == 0.0
    assert math.copysign(1, reported) == 1.0


def test_kap_inv_wht_refunds_keep_their_sign_after_tfs():
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
    assert round(mixed["etf_wht_anrechenbar_eur"], 2) == 70.00

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
    assert round(refund_only["etf_wht_anrechenbar_eur"], 2) == -14.00
    assert round(get_kap_inv_wht_for_reporting(refund_only), 2) == -14.00


if __name__ == "__main__":
    test_kap_inv_wht_anrechenbar_after_teilfreistellung()
    test_kap_inv_wht_reporting_falls_back_for_legacy_data()
    test_withholding_tax_reporting_normalizes_zero()
    test_kap_inv_wht_refunds_keep_their_sign_after_tfs()
    print("OK: KAP-INV WHT reporting")
