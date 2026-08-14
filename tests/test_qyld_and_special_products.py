"""Regression tests for verified fund, ETN and partnership routing."""
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
    ETF_CLASSIFICATION_REVIEW,
    ISIN_CORRECTIONS,
    PRODUCT_CLASSIFICATION_EVIDENCE,
    get_classification,
    get_etf_info,
    get_routing_classification,
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
UNKNOWN_ISIN = "DE0000000001"


def calculate_fixture(
        trades=None, funds=None, closed_lots=None, conversion_rates=None,
        anlage_so_overrides=None, dba_beta=False):
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
        if closed_lots:
            with open(os.path.join(tmp, "closed_lots.csv"), "w", newline="") as f:
                writer = csv.DictWriter(
                    f, fieldnames=sorted({key for row in closed_lots for key in row})
                )
                writer.writeheader()
                writer.writerows(closed_lots)
        if conversion_rates:
            with open(os.path.join(tmp, "conversion_rates.csv"), "w", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=sorted({key for row in conversion_rates for key in row}),
                )
                writer.writeheader()
                writer.writerows(conversion_rates)
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(
                tmp,
                anlage_so_overrides=anlage_so_overrides,
                dba_wht_beta_enabled=dba_beta,
            )


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


def test_gld_is_verified_other_fund_without_quarantine():
    assert get_classification(GLD_ISIN) == "sonstiger_fonds"
    assert get_teilfreistellung(GLD_ISIN) == 0.0
    assert is_investment_fund(GLD_ISIN)
    assert not requires_classification_review(GLD_ISIN)
    info = get_etf_info(GLD_ISIN)
    assert info["ticker"] == "GLD"
    assert info["evidence"]["status"] == "verified"
    assert "§ 1 Abs. 2" in info["evidence"]["invstg_basis"]


def test_active_classifications_have_valid_isins_and_audited_special_cases():
    assert all(is_valid_isin(isin) for isin in ETF_CLASSIFICATION)
    assert ETF_CLASSIFICATION_REVIEW == {}
    assert len(ISIN_CORRECTIONS) == 70
    assert all(
        is_valid_isin(current_isin)
        for _ticker, current_isin, _source in ISIN_CORRECTIONS.values()
    )
    assert all(
        evidence["classification"] == get_classification(isin)
        for isin, evidence in PRODUCT_CLASSIFICATION_EVIDENCE.items()
    )
    for lp_isin in ("US91232N2071", "US9123184098"):  # USO, UNG
        assert not requires_classification_review(lp_isin)
        assert get_routing_classification(lp_isin) == "personengesellschaft"
        assert not is_investment_fund(lp_isin)
    assert get_classification("US46641Q3323") == "aktienfonds"  # JEPI
    assert get_classification("US46654Q2030") == "aktienfonds"  # JEPQ
    for other_fund in ("US74347W6012", "US74347W3530", "US9129087964"):
        assert get_classification(other_fund) == "sonstiger_fonds"
        assert not requires_classification_review(other_fund)


def test_ibit_routes_to_kap_inv_without_old_no_invstg_fallback():
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

    assert IBIT_ISIN in rd["kap_inv"]["etf_by_isin"]
    summary = get_no_invstg_summary(rd)
    assert IBIT_ISIN not in summary
    assert round(rd["kap_inv"]["etf_dividends_raw_eur"], 2) == 20.00
    assert round(rd["kap_inv"]["etf_dividends_taxable_eur"], 2) == 20.00
    assert rd["zeile_19_netto_eur"] == 0
    assert rd["classification_review_items"] == []


def test_limited_partnership_routes_outside_invstg():
    rd = calculate_fixture(
        trades=[{
            "tradeID": "USO_SELL", "assetCategory": "STK",
            "subCategory": "ETF", "transactionType": "ExchTrade",
            "buySell": "SELL", "symbol": "USO", "isin": USO_ISIN,
            "quantity": "-10", "fifoPnlRealized": "100",
            "fxRateToBase": "1", "currency": "EUR",
            "dateTime": "2025-12-15 10:00:00", "tradeDate": "2025-12-15",
            "reportDate": "2025-12-15",
        }],
        funds=[
            {
                "activityCode": "DIV", "reportDate": "2025-12-01",
                "date": "2025-12-01", "amount": "20", "currency": "EUR",
                "subCategory": "ETF", "isin": USO_ISIN, "symbol": "USO",
            },
            {
                "activityCode": "WHT", "reportDate": "2025-12-01",
                "date": "2025-12-01", "amount": "-3", "currency": "EUR",
                "subCategory": "ETF", "isin": USO_ISIN, "symbol": "USO",
            },
            {
                "activityCode": "INTR", "reportDate": "2025-12-02",
                "date": "2025-12-02", "amount": "5", "currency": "EUR",
                "subCategory": "ETF", "isin": USO_ISIN, "symbol": "USO",
            },
        ],
    )

    assert USO_ISIN not in rd["kap_inv"]["etf_by_isin"]
    summary = get_no_invstg_summary(rd)
    assert USO_ISIN not in summary
    assert round(rd["zeile_19_netto_eur"], 2) == 0.00
    assert round(rd["zeile_41_withholding_tax_eur"], 2) == 0.00
    assert rd["classification_review_items"] == []
    item = rd["partnership_tax_items"][USO_ISIN]
    assert item["status"] == "blocked_missing_annual_allocation"
    assert item["excluded_from_automatic_tax_calculation"] is True
    assert round(item["observed_trade_pnl_eur"], 2) == 100.00
    assert round(item["observed_distributions_eur"], 2) == 20.00
    assert round(item["observed_withholding_tax_eur"], 2) == -3.00
    assert round(item["observed_other_cash_eur"], 2) == 5.00
    assert any("K-1/K-3" in document for document in item["required_documents"])


def test_limited_partnership_tageskurs_delta_never_enters_global_tax_total():
    rd = calculate_fixture(
        closed_lots=[{
            "assetCategory": "STK", "subCategory": "ETF",
            "symbol": "USO", "isin": USO_ISIN, "currency": "USD",
            "quantity": "-10", "cost": "1000", "fifoPnlRealized": "100",
            "openDateTime": "2025-01-15 10:00:00",
            "dateTime": "2025-12-15 10:00:00", "reportDate": "2025-12-15",
        }],
        conversion_rates=[
            {
                "fromCurrency": "USD", "toCurrency": "EUR",
                "reportDate": "2025-01-15", "rate": "0.9",
            },
            {
                "fromCurrency": "USD", "toCurrency": "EUR",
                "reportDate": "2025-12-15", "rate": "1.0",
            },
        ],
    )

    assert round(rd["fx_correction_by_topf"]["Personengesellschaft"], 2) == 100.00
    assert round(rd["fx_correction_total"], 2) == 0.00
    assert round(rd["zeile_19_netto_eur"], 2) == 0.00
    item = rd["partnership_tax_items"][USO_ISIN]
    assert round(item["observed_tageskurs_delta_eur"], 2) == 100.00
    assert item["excluded_from_automatic_tax_calculation"] is True


def test_unknown_product_has_no_automatic_tax_or_wht_fallback():
    rd = calculate_fixture(
        trades=[{
            "tradeID": "UNKNOWN_SELL", "assetCategory": "STK",
            "subCategory": "ETF", "transactionType": "ExchTrade",
            "buySell": "SELL", "symbol": "UNKNOWN", "isin": UNKNOWN_ISIN,
            "quantity": "-10", "fifoPnlRealized": "100",
            "fxRateToBase": "1", "currency": "EUR",
            "dateTime": "2025-12-15 10:00:00", "tradeDate": "2025-12-15",
            "reportDate": "2025-12-15",
        }],
        funds=[
            {
                "activityCode": "DIV", "reportDate": "2025-12-01",
                "date": "2025-12-01", "amount": "20", "currency": "EUR",
                "subCategory": "ETF", "isin": UNKNOWN_ISIN,
                "symbol": "UNKNOWN",
            },
            {
                "activityCode": "WHT", "reportDate": "2025-12-01",
                "date": "2025-12-01", "amount": "-3", "currency": "EUR",
                "subCategory": "ETF", "isin": UNKNOWN_ISIN,
                "symbol": "UNKNOWN",
            },
        ],
        closed_lots=[{
            "assetCategory": "STK", "subCategory": "ETF",
            "symbol": "UNKNOWN", "isin": UNKNOWN_ISIN, "currency": "USD",
            "quantity": "-10", "cost": "1000", "fifoPnlRealized": "100",
            "openDateTime": "2025-01-15 10:00:00",
            "dateTime": "2025-12-15 10:00:00", "reportDate": "2025-12-15",
        }],
        conversion_rates=[
            {
                "fromCurrency": "USD", "toCurrency": "EUR",
                "reportDate": "2025-01-15", "rate": "0.9",
            },
            {
                "fromCurrency": "USD", "toCurrency": "EUR",
                "reportDate": "2025-12-15", "rate": "1.0",
            },
        ],
    )

    item = rd["kap_inv"]["etf_by_isin"][UNKNOWN_ISIN]
    assert item["classification"] is None
    assert item["classification_confirmed"] is False
    assert round(item["gain"], 2) == 100.00
    assert round(item["div"], 2) == 20.00
    assert round(item["gain_taxable"], 2) == 0.00
    assert round(item["div_taxable"], 2) == 0.00
    assert round(item["wht_anrechenbar"], 2) == 0.00
    assert round(rd["kap_inv"]["etf_net_taxable_eur"], 2) == 0.00
    assert round(rd["zeile_41_withholding_tax_eur"], 2) == 0.00
    assert UNKNOWN_ISIN in rd["kap_inv_form"]["blocked_isins"]
    assert rd["kap_inv_form"]["lines"] == []
    assert round(rd["fx_correction_by_topf"]["KAP-INV"], 2) == 100.00
    assert round(rd["fx_correction_kap_inv_taxable"], 2) == 0.00
    assert round(
        rd["fx_correction_kap_inv_by_isin"][UNKNOWN_ISIN]["taxable_delta"], 2
    ) == 0.00

    beta = calculate_fixture(
        funds=[
            {
                "activityCode": "DIV", "reportDate": "2025-12-01",
                "date": "2025-12-01", "amount": "20", "currency": "EUR",
                "subCategory": "ETF", "isin": UNKNOWN_ISIN,
                "symbol": "UNKNOWN",
            },
            {
                "activityCode": "WHT", "reportDate": "2025-12-01",
                "date": "2025-12-01", "amount": "-3", "currency": "EUR",
                "subCategory": "ETF", "isin": UNKNOWN_ISIN,
                "symbol": "UNKNOWN",
            },
        ],
        dba_beta=True,
    )
    assert round(beta["zeile_41_withholding_tax_eur"], 2) == 0.00
    review = beta["kap_inv"]["wht_review_items"]
    assert len(review) == 1
    assert review[0]["status"] == "classification_unconfirmed"
    assert round(review[0]["net_foreign_tax_eur"], 2) == 3.00


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
    test_gld_is_verified_other_fund_without_quarantine()
    test_active_classifications_have_valid_isins_and_audited_special_cases()
    test_ibit_routes_to_kap_inv_without_old_no_invstg_fallback()
    test_limited_partnership_routes_outside_invstg()
    test_limited_partnership_tageskurs_delta_never_enters_global_tax_total()
    test_unknown_product_has_no_automatic_tax_or_wht_fallback()
    test_no_invstg_summary_is_reconciled_by_isin()
    test_no_invstg_sale_and_distribution_route_to_topf2_summary()
    test_no_invstg_withholding_tax_refunds_keep_their_sign()
    test_no_invstg_summary_excludes_anlage_so_overrides()
    print("OK: QYLD classification and no-InvStG reporting")
