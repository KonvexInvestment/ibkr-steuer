"""Regression tests for QYLD, GLD and no-InvStG per-ISIN reporting."""
import contextlib
import csv
import io
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import calculate_tax, get_no_invstg_summary
from etf_classification import (
    ETF_CLASSIFICATION,
    get_classification,
    get_etf_info,
    get_teilfreistellung,
    is_investment_fund,
    is_valid_isin,
    requires_classification_review,
)


QYLD_ISIN = "US37954Y4834"
GLD_ISIN = "US78463V1070"
VXX_ISIN = "US06748M1962"
IBIT_ISIN = "US46438F1012"
USO_ISIN = "US91232N2071"


def calculate_fixture(trades=None, funds=None, anlage_so_overrides=None):
    trades = trades or []
    funds = funds or []
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "account_info.csv"), "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["currency", "tax_year", "fx_transactions_count"]
            )
            writer.writeheader()
            writer.writerow({
                "currency": "EUR",
                "tax_year": "2025",
                "fx_transactions_count": "0",
            })
        if trades:
            with open(os.path.join(tmp, "trades.csv"), "w", newline="") as f:
                writer = csv.DictWriter(
                    f, fieldnames=sorted({key for row in trades for key in row})
                )
                writer.writeheader()
                writer.writerows(trades)
        if funds:
            with open(os.path.join(tmp, "statement_of_funds.csv"), "w", newline="") as f:
                writer = csv.DictWriter(
                    f, fieldnames=sorted({key for row in funds for key in row})
                )
                writer.writeheader()
                writer.writerows(funds)
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp, anlage_so_overrides=anlage_so_overrides)


def test_qyld_is_aktienfonds_with_30_percent_tfs():
    info = get_etf_info(QYLD_ISIN)
    assert info is not None
    assert info["ticker"] == "QYLD"
    assert info["classification"] == "aktienfonds"
    assert get_teilfreistellung(QYLD_ISIN) == 0.30
    assert is_investment_fund(QYLD_ISIN)


def test_qyld_sale_and_distribution_route_to_kap_inv():
    rd = calculate_fixture(
        trades=[{
            "tradeID": "QYLD_SELL",
            "assetCategory": "STK",
            "subCategory": "ETF",
            "transactionType": "ExchTrade",
            "buySell": "SELL",
            "symbol": "QYLD",
            "isin": QYLD_ISIN,
            "quantity": "-100",
            "fifoPnlRealized": "100",
            "fxRateToBase": "1",
            "currency": "EUR",
            "dateTime": "2025-12-15 10:00:00",
            "tradeDate": "2025-12-15",
            "reportDate": "2025-12-15",
        }],
        funds=[
            {
                "activityCode": "DIV",
                "reportDate": "2025-12-01",
                "date": "2025-12-01",
                "amount": "50",
                "currency": "EUR",
                "subCategory": "ETF",
                "isin": QYLD_ISIN,
                "symbol": "QYLD",
            },
            {
                "activityCode": "WHT",
                "reportDate": "2025-12-01",
                "date": "2025-12-01",
                "amount": "-7.5",
                "currency": "EUR",
                "subCategory": "ETF",
                "isin": QYLD_ISIN,
                "symbol": "QYLD",
            },
        ],
    )

    kap_inv = rd["kap_inv"]
    qyld = kap_inv["etf_by_isin"][QYLD_ISIN]
    assert rd["zeile_19_netto_eur"] == 0
    assert round(kap_inv["etf_gain_raw_eur"], 2) == 100.00
    assert round(kap_inv["etf_dividends_raw_eur"], 2) == 50.00
    assert round(qyld["gain_taxable"], 2) == 70.00
    assert round(qyld["div_taxable"], 2) == 35.00
    # Standardmodus vor der optionalen DBA-Beta: Rohsteuer × (1 - TFS).
    assert round(qyld["wht_anrechenbar"], 2) == -5.25
    assert round(kap_inv["etf_wht_anrechenbar_eur"], 2) == 5.25
    assert round(rd["zeile_41_withholding_tax_eur"], 2) == 5.25
    assert round(kap_inv["etf_net_taxable_eur"], 2) == 105.00
    form_lines = {line["line"]: line for line in rd["kap_inv_form"]["lines"]}
    assert round(form_lines[4]["amount_raw_eur"], 2) == 50.00
    assert round(form_lines[4]["taxable_control_eur"], 2) == 35.00
    assert round(form_lines[14]["amount_raw_eur"], 2) == 100.00
    assert rd["kap_inv_form"]["status"] == "advance_lump_sum_review_required"


def test_gld_requires_classification_review_instead_of_guessing_no_invstg():
    assert get_classification(GLD_ISIN) is None
    assert get_teilfreistellung(GLD_ISIN) == 0.0
    assert not is_investment_fund(GLD_ISIN)
    assert requires_classification_review(GLD_ISIN)
    info = get_etf_info(GLD_ISIN)
    assert info["ticker"] == "GLD"
    assert info["review_required"] is True


def test_active_classifications_have_valid_isins_and_audited_special_cases():
    assert all(is_valid_isin(isin) for isin in ETF_CLASSIFICATION)
    assert get_classification("US91232N2071") == "no_invstg"  # USO LP
    assert get_classification("US9123184098") == "no_invstg"  # UNG LP
    assert requires_classification_review("US46641Q3323")  # JEPI
    assert requires_classification_review("US46654Q2030")  # JEPQ
    assert requires_classification_review("US74347W6012")  # UGL
    assert requires_classification_review("US74347W3530")  # AGQ
    assert requires_classification_review("US9129087964")  # CPER


def test_review_product_keeps_stable_route_and_is_flagged():
    rd = calculate_fixture(funds=[{
        "activityCode": "DIV",
        "reportDate": "2025-12-01",
        "date": "2025-12-01",
        "amount": "20",
        "currency": "EUR",
        "subCategory": "ETF",
        "isin": IBIT_ISIN,
        "symbol": "IBIT",
    }])

    assert IBIT_ISIN not in rd["kap_inv"]["etf_by_isin"]
    summary = get_no_invstg_summary(rd)
    assert round(summary[IBIT_ISIN]["div"], 2) == 20.00
    review = rd["classification_review_items"]
    assert len(review) == 1
    assert review[0]["isin"] == IBIT_ISIN
    assert review[0]["routing_classification"] == "no_invstg"
    assert "Rechtstyp" in review[0]["review_reason"]


def test_limited_partnership_routes_outside_invstg():
    rd = calculate_fixture(funds=[{
        "activityCode": "DIV",
        "reportDate": "2025-12-01",
        "date": "2025-12-01",
        "amount": "20",
        "currency": "EUR",
        "subCategory": "ETF",
        "isin": USO_ISIN,
        "symbol": "USO",
    }])

    assert USO_ISIN not in rd["kap_inv"]["etf_by_isin"]
    summary = get_no_invstg_summary(rd)
    assert round(summary[USO_ISIN]["div"], 2) == 20.00
    assert round(rd["zeile_19_netto_eur"], 2) == 20.00


def test_no_invstg_summary_is_reconciled_by_isin():
    summary = get_no_invstg_summary({
        "all_traded_etf_isins": [VXX_ISIN],
        "trade_details": [
            {
                "assetCategory": "STK", "topf": "Topf2",
                "isin": VXX_ISIN, "pnl_eur": 100,
            },
            {
                "assetCategory": "STK", "topf": "Topf2",
                "isin": VXX_ISIN, "pnl_eur": -30,
            },
        ],
        "fx_correction_details": [
            {"topf": "Topf2", "isin": VXX_ISIN, "delta_eur": 5},
        ],
        "no_invstg_income_by_isin": {
            VXX_ISIN: {"div": 2, "wht": -0.3},
        },
    }, include_tageskurs=True)

    vxx = summary[VXX_ISIN]
    assert vxx["ticker"] == "VXX"
    assert vxx["gain"] == 100
    assert vxx["loss"] == -30
    assert vxx["tageskurs"] == 5
    assert vxx["div"] == 2
    assert vxx["wht_reported"] == 0.3
    assert vxx["trade_net"] == 75
    assert vxx["total"] == 77


def test_no_invstg_sale_and_distribution_route_to_topf2_summary():
    rd = calculate_fixture(
        trades=[{
            "tradeID": "VXX_SELL",
            "assetCategory": "STK",
            "subCategory": "ETF",
            "transactionType": "ExchTrade",
            "buySell": "SELL",
            "symbol": "VXX",
            "isin": VXX_ISIN,
            "quantity": "-10",
            "fifoPnlRealized": "100",
            "fxRateToBase": "1",
            "currency": "EUR",
            "dateTime": "2025-12-15 10:00:00",
            "tradeDate": "2025-12-15",
            "reportDate": "2025-12-15",
        }],
        funds=[
            {
                "activityCode": "DIV",
                "reportDate": "2025-12-01",
                "date": "2025-12-01",
                "amount": "20",
                "currency": "EUR",
                "subCategory": "ETF",
                "isin": VXX_ISIN,
                "symbol": "VXX",
            },
            {
                "activityCode": "WHT",
                "reportDate": "2025-12-01",
                "date": "2025-12-01",
                "amount": "-3",
                "currency": "EUR",
                "subCategory": "ETF",
                "isin": VXX_ISIN,
                "symbol": "VXX",
            },
        ],
    )

    summary = get_no_invstg_summary(rd)
    vxx = summary[VXX_ISIN]
    assert round(rd["zeile_19_netto_eur"], 2) == 120.00
    assert VXX_ISIN not in rd["kap_inv"]["etf_by_isin"]
    assert round(vxx["gain"], 2) == 100.00
    assert round(vxx["div"], 2) == 20.00
    assert round(vxx["wht_reported"], 2) == 3.00
    assert round(vxx["total"], 2) == 120.00


def test_no_invstg_withholding_tax_refunds_keep_their_sign():
    rd = calculate_fixture(funds=[
        {
            "activityCode": "WHT",
            "reportDate": "2025-12-01",
            "date": "2025-12-01",
            "amount": "-10",
            "currency": "EUR",
            "subCategory": "ETF",
            "isin": VXX_ISIN,
            "symbol": "VXX",
        },
        {
            "activityCode": "WHT",
            "reportDate": "2025-12-02",
            "date": "2025-12-02",
            "amount": "4",
            "currency": "EUR",
            "subCategory": "ETF",
            "isin": VXX_ISIN,
            "symbol": "VXX",
        },
    ])

    summary = get_no_invstg_summary(rd)
    assert round(rd["zeile_41_withholding_tax_eur"], 2) == 6.00
    assert round(summary[VXX_ISIN]["wht_reported"], 2) == 6.00

    refund_only = calculate_fixture(funds=[{
        "activityCode": "WHT",
        "reportDate": "2025-12-03",
        "date": "2025-12-03",
        "amount": "5",
        "currency": "EUR",
        "subCategory": "ETF",
        "isin": VXX_ISIN,
        "symbol": "VXX",
    }])
    refund_summary = get_no_invstg_summary(refund_only)
    assert round(refund_only["zeile_41_withholding_tax_eur"], 2) == -5.00
    assert round(refund_summary[VXX_ISIN]["wht_reported"], 2) == -5.00


def test_no_invstg_summary_excludes_anlage_so_overrides():
    summary = get_no_invstg_summary({
        "all_traded_etf_isins": [VXX_ISIN],
        "anlage_so_overrides_applied": [VXX_ISIN],
        "trade_details": [{
            "assetCategory": "STK", "topf": "Anlage SO",
            "isin": VXX_ISIN, "pnl_eur": 100,
        }],
    })

    assert summary == {}


if __name__ == "__main__":
    test_qyld_is_aktienfonds_with_30_percent_tfs()
    test_qyld_sale_and_distribution_route_to_kap_inv()
    test_gld_requires_classification_review_instead_of_guessing_no_invstg()
    test_active_classifications_have_valid_isins_and_audited_special_cases()
    test_review_product_keeps_stable_route_and_is_flagged()
    test_limited_partnership_routes_outside_invstg()
    test_no_invstg_summary_is_reconciled_by_isin()
    test_no_invstg_sale_and_distribution_route_to_topf2_summary()
    test_no_invstg_withholding_tax_refunds_keep_their_sign()
    test_no_invstg_summary_excludes_anlage_so_overrides()
    print("OK: QYLD classification and no-InvStG reporting")
