#!/usr/bin/env python3
"""Regression for Issue #88: KAP-INV put lots with foreign ROC basis cuts."""

import contextlib
import csv
import io
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (
    calculate_tax,
    get_kap_inv_tageskurs_delta_for_reporting,
)


STRIKE = 40.0
PREMIUM = 192.93
ROC_REDUCTION = 246.58
IBKR_BASIS = STRIKE * 100 - PREMIUM - ROC_REDUCTION
SALE_PNL_IBKR = -464.99
FX_OPEN = 0.92137
FX_CLOSE = 0.85135
QDTE_ISIN = "US77926X3044"
# Klassifizierter sonstiger_fonds (0% TFS): identische Zahlenwerte wie der
# unbestaetigte Fall, aber ohne Klassifikations-Blocker.
CLASSIFIED_FUND_ISIN = "US78463V1070"
PARTNERSHIP_ISINS = {
    "USO": "US91232N2071",
    "UNG": "US9123184098",
}


def assert_close(actual, expected, tol=0.001, label=""):
    if abs(actual - expected) > tol:
        raise AssertionError(
            f"{label}: erwartet {expected}, aktuell {actual} "
            f"(delta {actual - expected})"
        )


def _option_sell(symbol, assignment_date):
    sell_date = assignment_date
    return {
        "tradeID": f"{symbol}_put_sell_{sell_date}",
        "assetCategory": "OPT",
        "transactionType": "ExchTrade",
        "buySell": "SELL",
        "openCloseIndicator": "O",
        "putCall": "P",
        "strike": str(STRIKE),
        "expiry": assignment_date,
        "underlyingSymbol": symbol,
        "symbol": f"{symbol} {assignment_date} 40 P",
        "description": f"{symbol} 40 PUT",
        "quantity": "-1",
        "tradePrice": str(PREMIUM / 100),
        "closePrice": str(PREMIUM / 100),
        "multiplier": "100",
        "ibCommission": "0",
        "fxRateToBase": str(FX_OPEN),
        "currency": "USD",
        "dateTime": f"{sell_date} 10:00:00",
        "tradeDate": sell_date,
        "reportDate": sell_date,
        "fifoPnlRealized": "0",
        "cost": "0",
        "proceeds": str(PREMIUM),
    }


def _option_assignment(symbol, assignment_date):
    return {
        "tradeID": f"{symbol}_put_assignment_{assignment_date}",
        "assetCategory": "OPT",
        "transactionType": "BookTrade",
        "buySell": "BUY",
        "openCloseIndicator": "C",
        "putCall": "P",
        "strike": str(STRIKE),
        "expiry": assignment_date,
        "underlyingSymbol": symbol,
        "symbol": f"{symbol} {assignment_date} 40 P",
        "description": f"{symbol} 40 PUT",
        "quantity": "1",
        "tradePrice": "0",
        "closePrice": "0",
        "multiplier": "100",
        "ibCommission": "0",
        "fxRateToBase": str(FX_OPEN),
        "currency": "USD",
        "dateTime": f"{assignment_date} 16:20:00",
        "tradeDate": assignment_date,
        "reportDate": assignment_date,
        "fifoPnlRealized": "0",
        "cost": "0",
        "proceeds": "0",
    }


def _stock_assignment(symbol, isin, assignment_date, is_fund):
    return {
        "tradeID": f"{symbol}_stock_assignment_{assignment_date}",
        "assetCategory": "STK",
        "subCategory": "ETF" if is_fund else "",
        "transactionType": "BookTrade",
        "buySell": "BUY",
        "openCloseIndicator": "O",
        "underlyingSymbol": symbol,
        "symbol": symbol,
        "description": f"{symbol} SYNTHETIC",
        "quantity": "100",
        "tradePrice": str(STRIKE),
        "closePrice": str(STRIKE),
        "ibCommission": "0",
        "fxRateToBase": str(FX_OPEN),
        "currency": "USD",
        "dateTime": f"{assignment_date} 16:20:00",
        "tradeDate": assignment_date,
        "reportDate": assignment_date,
        "fifoPnlRealized": "0",
        "cost": str(IBKR_BASIS),
        "proceeds": str(-STRIKE * 100),
        "isin": isin,
    }


def _stock_sale(symbol, isin, is_fund):
    return {
        "tradeID": f"{symbol}_stock_sale",
        "assetCategory": "STK",
        "subCategory": "ETF" if is_fund else "",
        "transactionType": "ExchTrade",
        "buySell": "SELL",
        "openCloseIndicator": "C",
        "underlyingSymbol": symbol,
        "symbol": symbol,
        "description": f"{symbol} SYNTHETIC",
        "quantity": "-100",
        "tradePrice": "30.975",
        "closePrice": "30.975",
        "ibCommission": "-2",
        "fxRateToBase": str(FX_CLOSE),
        "currency": "USD",
        "dateTime": "2025-12-31 10:00:00",
        "tradeDate": "2025-12-31",
        "reportDate": "2025-12-31",
        "fifoPnlRealized": str(SALE_PNL_IBKR),
        "cost": str(IBKR_BASIS),
        "proceeds": "3097.50",
        "isin": isin,
    }


def _closed_lot(symbol, isin, assignment_date, is_fund):
    return {
        "assetCategory": "STK",
        "subCategory": "ETF" if is_fund else "",
        "currency": "USD",
        "symbol": symbol,
        "underlyingSymbol": symbol,
        "description": f"{symbol} SYNTHETIC",
        "isin": isin,
        "openDateTime": f"{assignment_date} 16:20:00",
        "dateTime": "2025-12-31 10:00:00",
        "reportDate": "2025-12-31",
        "quantity": "100",
        "buySell": "SELL",
        "cost": str(IBKR_BASIS),
        "fifoPnlRealized": str(SALE_PNL_IBKR),
        "fxRateToBase": str(FX_CLOSE),
    }


def calculate_case(symbol="QDTE", isin=QDTE_ISIN, is_fund=True,
                   assignment_date="2025-03-20"):
    trades = [
        _option_sell(symbol, assignment_date),
        _option_assignment(symbol, assignment_date),
        _stock_assignment(symbol, isin, assignment_date, is_fund),
        _stock_sale(symbol, isin, is_fund),
    ]
    trade_fields = sorted({key for row in trades for key in row})
    closed_lot = _closed_lot(symbol, isin, assignment_date, is_fund)

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
        with open(os.path.join(tmp, "trades.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=trade_fields)
            writer.writeheader()
            writer.writerows(trades)
        with open(os.path.join(tmp, "closed_lots.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=sorted(closed_lot))
            writer.writeheader()
            writer.writerow(closed_lot)
        with open(os.path.join(tmp, "conversion_rates.csv"), "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["reportDate", "fromCurrency", "toCurrency", "rate"],
            )
            writer.writeheader()
            writer.writerows([
                {
                    "reportDate": assignment_date,
                    "fromCurrency": "USD",
                    "toCurrency": "EUR",
                    "rate": str(FX_OPEN),
                },
                {
                    "reportDate": "2025-12-31",
                    "fromCurrency": "USD",
                    "toCurrency": "EUR",
                    "rate": str(FX_CLOSE),
                },
            ])
        with open(os.path.join(tmp, "financial_instruments.csv"), "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "symbol", "isin", "assetCategory", "subCategory", "description"
                ],
            )
            writer.writeheader()
            writer.writerow({
                "symbol": symbol,
                "isin": isin,
                "assetCategory": "STK",
                "subCategory": "ETF" if is_fund else "",
                "description": f"{symbol} SYNTHETIC",
            })

        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp, tax_year=2025)


def _sale_row(report, symbol):
    rows = [
        row for row in report["trade_details"]
        if row.get("source") == "trades"
        and row.get("assetCategory") == "STK"
        and row.get("symbol") == symbol
    ]
    assert len(rows) == 1, rows
    return rows[0]


def test_same_year_qdte_restores_roc_and_premium_to_strike_basis():
    report = calculate_case()
    sale = _sale_row(report, "QDTE")
    fx_lot = report["fx_correction_details"][0]

    assert_close(sale["cost"], 4000.0, label="QDTE trade basis")
    assert_close(sale["fifoPnlRealized"], -904.50, label="QDTE raw loss")
    assert_close(sale["stillhalter_adjustment_raw"], 439.51,
                 label="QDTE complete basis restore")
    assert_close(sale["invstg_basis_adjustment_raw"], ROC_REDUCTION,
                 label="QDTE ROC-only adjustment")
    assert_close(sale["pnl_eur"], -770.046075, label="QDTE sale loss EUR")

    assert_close(fx_lot["cost"], 4000.0, label="QDTE Tageskurs basis")
    assert_close(fx_lot["cost_basis_adjustment_raw"], 439.51,
                 label="QDTE Tageskurs complete restore")
    assert_close(fx_lot["invstg_basis_adjustment_raw"], ROC_REDUCTION,
                 label="QDTE Tageskurs ROC restore")
    assert_close(fx_lot["delta_eur"], -280.08,
                 label="QDTE Tageskurs correction")

    assert_close(report["kap_inv"]["etf_loss_raw_eur"], -770.046075,
                 label="QDTE KAP-INV loss before Tageskurs")
    # Unbestaetigte Fondsart: der steuerpflichtige Tageskurs-Wert bleibt
    # blockiert (0), bis die Klassifikation bestaetigt ist; der rohe Delta
    # ist vollstaendig erfasst (Blocker-Semantik des Frontend-Redesigns,
    # bestaetigter Pfad siehe test_same_year_classified_fund_...).
    tageskurs = get_kap_inv_tageskurs_delta_for_reporting(report)
    assert_close(tageskurs, 0.0, label="QDTE KAP-INV Tageskurs (blockiert)")
    by_isin = report["fx_correction_kap_inv_by_isin"][QDTE_ISIN]
    assert by_isin["classification_confirmed"] is False
    assert_close(by_isin["raw_delta"], -280.08,
                 label="QDTE KAP-INV Tageskurs roh")
    assert_close(by_isin["taxable_delta"], 0.0,
                 label="QDTE KAP-INV Tageskurs steuerpflichtig blockiert")
    assert QDTE_ISIN in report["kap_inv_form"]["blocked_isins"]
    assert report["kap_inv_form"]["status"] == \
        "classification_review_required"
    audit = report["audit"]["invstg_put_basis_adjustments"]
    assert len(audit) == 1 and audit[0]["source"] == "same_year_put"
    assert_close(audit[0]["amount_raw"], ROC_REDUCTION,
                 label="QDTE audit ROC amount")


def test_same_year_classified_fund_reports_taxable_tageskurs():
    report = calculate_case(isin=CLASSIFIED_FUND_ISIN)
    sale = _sale_row(report, "QDTE")

    assert_close(sale["cost"], 4000.0, label="classified fund basis")
    assert_close(sale["invstg_basis_adjustment_raw"], ROC_REDUCTION,
                 label="classified fund ROC-only adjustment")
    tageskurs = get_kap_inv_tageskurs_delta_for_reporting(report)
    assert_close(tageskurs, -280.08,
                 label="classified fund KAP-INV Tageskurs")
    assert_close(
        report["kap_inv"]["etf_net_taxable_eur"] + tageskurs,
        -1050.126075,
        label="classified fund combined KAP-INV loss",
    )
    assert CLASSIFIED_FUND_ISIN not in \
        report["kap_inv_form"]["blocked_isins"]


def test_regular_stock_keeps_premium_only_correction():
    report = calculate_case(
        symbol="REG", isin="US0000000001", is_fund=False
    )
    sale = _sale_row(report, "REG")
    fx_lot = report["fx_correction_details"][0]

    assert_close(sale["cost"], 3753.42, label="regular stock basis")
    assert_close(sale["fifoPnlRealized"], -657.92,
                 label="regular stock raw loss")
    assert "invstg_basis_adjustment_raw" not in sale
    assert_close(fx_lot["cost"], 3753.42,
                 label="regular stock Tageskurs basis")
    assert_close(fx_lot["delta_eur"], -262.8144684,
                 label="regular stock Tageskurs correction")
    assert "invstg_put_basis_adjustments" not in report["audit"]


def test_cross_year_qdte_uses_the_same_full_basis():
    report = calculate_case(assignment_date="2024-03-20")
    sale = _sale_row(report, "QDTE")
    correction = report["audit"]["cross_year_put_corrections"]

    assert_close(sale["cost"], 4000.0, label="cross-year QDTE basis")
    assert_close(sale["invstg_basis_adjustment_raw"], ROC_REDUCTION,
                 label="cross-year QDTE ROC-only adjustment")
    assert len(correction) == 1
    assert_close(correction[0]["correction_per_share_raw"], 4.3951,
                 label="cross-year complete correction per share")
    assert_close(
        correction[0]["invstg_basis_extra_per_share_raw"], 2.4658,
        label="cross-year ROC correction per share",
    )
    assert_close(report["fx_correction_details"][0]["cost"], 4000.0,
                 label="cross-year QDTE Tageskurs basis")
    audit = report["audit"]["invstg_put_basis_adjustments"]
    assert len(audit) == 1 and audit[0]["source"] == "cross_year_put"


def test_cross_year_partnership_puts_keep_premium_only_basis():
    for symbol, isin in PARTNERSHIP_ISINS.items():
        report = calculate_case(
            symbol=symbol,
            isin=isin,
            is_fund=True,
            assignment_date="2024-03-20",
        )
        sale = _sale_row(report, symbol)
        correction = report["audit"]["cross_year_put_corrections"]
        fx_lot = report["fx_correction_details"][0]

        assert_close(sale["cost"], 3753.42,
                     label=f"cross-year {symbol} premium-only basis")
        assert_close(sale["stillhalter_adjustment_raw"], PREMIUM,
                     label=f"cross-year {symbol} premium correction")
        assert "invstg_basis_adjustment_raw" not in sale
        assert len(correction) == 1
        assert_close(
            correction[0]["correction_per_share_raw"],
            PREMIUM / 100,
            label=f"cross-year {symbol} correction per share",
        )
        assert_close(
            correction[0]["invstg_basis_extra_per_share_raw"],
            0.0,
            label=f"cross-year {symbol} no InvStG extra correction",
        )
        assert not report["audit"].get("invstg_put_basis_adjustments")
        assert_close(fx_lot["cost"], 3753.42,
                     label=f"cross-year {symbol} Tageskurs basis")
        partnership = report["partnership_tax_items"][isin]
        assert partnership["classification"] == "personengesellschaft"
        assert partnership["excluded_from_automatic_tax_calculation"] is True


if __name__ == "__main__":
    test_same_year_qdte_restores_roc_and_premium_to_strike_basis()
    print("  OK  Same-Year QDTE: 4,000 USD basis, roher Tageskurs -280.08, "
          "steuerpflichtig blockiert bis Bestaetigung")
    test_same_year_classified_fund_reports_taxable_tageskurs()
    print("  OK  Klassifizierter Fonds: -280.08 EUR Tageskurs steuerwirksam")
    test_regular_stock_keeps_premium_only_correction()
    print("  OK  Regular stock control: premium-only behavior unchanged")
    test_cross_year_qdte_uses_the_same_full_basis()
    print("  OK  Cross-Year QDTE: same full-basis correction")
    test_cross_year_partnership_puts_keep_premium_only_basis()
    print("  OK  Cross-Year USO/UNG: premium-only, no InvStG basis restore")
    print("OK: Issue #88 KAP-INV put/ROC basis")
