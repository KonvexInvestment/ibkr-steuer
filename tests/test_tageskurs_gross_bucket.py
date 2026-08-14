"""Regression tests for gross gain/loss allocation after lot corrections."""
import contextlib
import csv
import io
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (  # noqa: E402
    build_topf2_breakdown,
    calculate_tageskurs_gross_adjustment,
    calculate_tax,
)


VXX_ISIN = "US06748M1962"


def _write_csv(path, rows):
    if not rows:
        return
    fields = sorted({key for row in rows for key in row})
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _put_assignment_fixture(no_invstg=False):
    symbol = "VXX" if no_invstg else "TEST"
    isin = VXX_ISIN if no_invstg else "US0000000001"
    subcategory = "ETF" if no_invstg else "COMMON"
    trades = [
        {
            "tradeID": "put_sell",
            "assetCategory": "OPT",
            "transactionType": "ExchTrade",
            "buySell": "SELL",
            "openCloseIndicator": "O",
            "putCall": "P",
            "strike": "100",
            "expiry": "20250102",
            "underlyingSymbol": symbol,
            "symbol": f"{symbol} 250102P00100000",
            "quantity": "-1",
            "tradePrice": "10",
            "closePrice": "10",
            "multiplier": "1",
            "ibCommission": "0",
            "fxRateToBase": "1",
            "currency": "USD",
            "dateTime": "2025-01-01 10:00:00",
            "tradeDate": "2025-01-01",
            "reportDate": "2025-01-01",
            "fifoPnlRealized": "0",
        },
        {
            "tradeID": "put_assignment",
            "assetCategory": "OPT",
            "transactionType": "BookTrade",
            "buySell": "BUY",
            "openCloseIndicator": "C",
            "putCall": "P",
            "strike": "100",
            "expiry": "20250102",
            "underlyingSymbol": symbol,
            "symbol": f"{symbol} 250102P00100000",
            "quantity": "1",
            "tradePrice": "0",
            "closePrice": "0",
            "multiplier": "1",
            "ibCommission": "0",
            "fxRateToBase": "1",
            "currency": "USD",
            "dateTime": "2025-01-02 16:20:00",
            "tradeDate": "2025-01-02",
            "reportDate": "2025-01-02",
            "fifoPnlRealized": "0",
        },
        {
            "tradeID": "stock_open",
            "assetCategory": "STK",
            "subCategory": subcategory,
            "transactionType": "BookTrade",
            "buySell": "BUY",
            "openCloseIndicator": "O",
            "symbol": symbol,
            "isin": isin,
            "quantity": "1",
            "cost": "90",
            "proceeds": "-100",
            "fifoPnlRealized": "0",
            "fxRateToBase": "1",
            "currency": "USD",
            "dateTime": "2025-01-02 16:20:00",
            "tradeDate": "2025-01-02",
            "reportDate": "2025-01-02",
        },
        {
            "tradeID": "stock_close",
            "assetCategory": "STK",
            "subCategory": subcategory,
            "transactionType": "ExchTrade",
            "buySell": "SELL",
            "openCloseIndicator": "C",
            "symbol": symbol,
            "isin": isin,
            "quantity": "-1",
            "cost": "-90",
            "proceeds": "95",
            "fifoPnlRealized": "5",
            "fxRateToBase": "0.9",
            "currency": "USD",
            "dateTime": "2025-02-01 10:00:00",
            "tradeDate": "2025-02-01",
            "reportDate": "2025-02-01",
        },
    ]
    closed_lots = [{
        "assetCategory": "STK",
        "subCategory": subcategory,
        "currency": "USD",
        "symbol": symbol,
        "isin": isin,
        "underlyingSymbol": symbol,
        "openDateTime": "2025-01-02 16:20:00",
        "dateTime": "2025-02-01 10:00:00",
        "reportDate": "2025-02-01",
        "buySell": "SELL",
        "quantity": "1",
        "cost": "90",
        "fifoPnlRealized": "5",
        "fxRateToBase": "0.9",
    }]
    instruments = [{
        "assetCategory": "STK",
        "subCategory": subcategory,
        "symbol": symbol,
        "isin": isin,
    }]
    return trades, closed_lots, instruments


def _calculate_assignment(no_invstg=False):
    trades, lots, instruments = _put_assignment_fixture(no_invstg)
    with tempfile.TemporaryDirectory() as tmp:
        _write_csv(os.path.join(tmp, "trades.csv"), trades)
        _write_csv(os.path.join(tmp, "closed_lots.csv"), lots)
        _write_csv(os.path.join(tmp, "financial_instruments.csv"), instruments)
        _write_csv(os.path.join(tmp, "account_info.csv"), [{
            "currency": "EUR",
            "tax_year": "2025",
            "fx_transactions_count": "0",
        }])
        _write_csv(os.path.join(tmp, "conversion_rates.csv"), [
            {
                "reportDate": "2025-01-02",
                "fromCurrency": "USD",
                "toCurrency": "EUR",
                "rate": "1.0",
            },
            {
                "reportDate": "2025-02-01",
                "fromCurrency": "USD",
                "toCurrency": "EUR",
                "rate": "0.9",
            },
        ])
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp)


def _assert_close(actual, expected, label):
    if abs(actual - expected) > 0.0001:
        raise AssertionError(f"{label}: erwartet {expected}, aktuell {actual}")


def test_stock_gain_flips_to_loss_before_tageskurs_split():
    report = _calculate_assignment(no_invstg=False)
    _assert_close(report["stocks_gain_eur"], 0.0, "Aktiengewinne vor TK")
    _assert_close(report["stocks_loss_eur"], -4.5, "Aktienverluste vor TK")
    _assert_close(report["fx_correction_by_topf"]["Topf1"], -10.0, "Topf1 TK netto")
    _assert_close(report["fx_corr_gain_adj"]["Topf1"], 0.0, "Topf1 Gewinnkorrektur")
    _assert_close(report["fx_corr_loss_adj"]["Topf1"], -10.0, "Topf1 Verlustkorrektur")

    final_gain = report["stocks_gain_eur"] + report["fx_corr_gain_adj"]["Topf1"]
    final_loss = report["stocks_loss_eur"] + report["fx_corr_loss_adj"]["Topf1"]
    _assert_close(final_gain, 0.0, "Zeile 20")
    _assert_close(abs(final_loss), 14.5, "Zeile 23")
    _assert_close(
        final_gain + final_loss,
        report["topf_1_aktien_netto"] + report["fx_correction_by_topf"]["Topf1"],
        "Topf1 Brutto/Netto-Invariante",
    )


def test_no_invstg_gain_flips_to_loss_in_topf2():
    report = _calculate_assignment(no_invstg=True)
    _assert_close(report["fx_correction_by_topf"]["Topf2"], -10.0, "Topf2 TK netto")
    _assert_close(report["fx_corr_gain_adj"]["Topf2"], 0.0, "Topf2 Gewinnkorrektur")
    _assert_close(report["fx_corr_loss_adj"]["Topf2"], -10.0, "Topf2 Verlustkorrektur")
    final_gain = report["options_gain_eur"] + report["fx_corr_gain_adj"]["Topf2"]
    final_loss = report["options_loss_eur"] + report["fx_corr_loss_adj"]["Topf2"]
    _assert_close(final_gain, 10.0, "Topf2 Stillhalter genau einmal")
    _assert_close(final_loss, -14.5, "Zeile 22")
    _assert_close(
        final_gain + final_loss,
        report["options_gain_eur"] + report["options_loss_eur"] - 10.0,
        "Topf2 Brutto/Netto-Invariante",
    )


def test_all_sign_transitions_reconcile():
    cases = [
        (10.0, 5.0, 5.0, 0.0),
        (-10.0, -5.0, 0.0, -5.0),
        (5.0, -10.0, -5.0, -5.0),
        (-5.0, 10.0, 5.0, 5.0),
    ]
    for before, delta, expected_gain, expected_loss in cases:
        result = calculate_tageskurs_gross_adjustment(before, delta)
        _assert_close(result["gain_adjustment"], expected_gain, "Gewinnkorrektur")
        _assert_close(result["loss_adjustment"], expected_loss, "Verlustkorrektur")
        _assert_close(
            result["gain_adjustment"] + result["loss_adjustment"],
            delta,
            "Lot-Invariante",
        )


def test_topf2_breakdown_keeps_gross_columns_additive():
    breakdown = build_topf2_breakdown(
        {
            "Optionen": {"gain": 250.0, "loss": -80.0},
            "Futures": {"gain": 40.0, "loss": -120.0},
        },
        dividends_eur=25.0,
        interest_eur=-5.0,
        tageskurs_gain_adjustment=30.0,
        tageskurs_loss_adjustment=7.5,
        zufluss_adjustment=-12.5,
    )

    _assert_close(
        breakdown["total_gain"],
        sum(row["gain"] for row in breakdown["rows"]),
        "Topf2 Gewinnspalte",
    )
    _assert_close(
        breakdown["total_loss"],
        sum(row["loss"] for row in breakdown["rows"]),
        "Topf2 Verlustspalte",
    )
    _assert_close(
        breakdown["net"],
        breakdown["total_gain"] + breakdown["total_loss"],
        "Topf2 Netto",
    )
    adjustment = next(
        row for row in breakdown["rows"]
        if row["label"] == "Tageskurs-Anpassung"
    )
    _assert_close(adjustment["gain"], 30.0, "TK Gewinnanteil")
    _assert_close(adjustment["loss"], 7.5, "TK Verlustanteil")
    _assert_close(adjustment["net"], 37.5, "TK Netto")


if __name__ == "__main__":
    test_stock_gain_flips_to_loss_before_tageskurs_split()
    test_no_invstg_gain_flips_to_loss_in_topf2()
    test_all_sign_transitions_reconcile()
    test_topf2_breakdown_keeps_gross_columns_additive()
    print("OK: Tageskurs-Bruttozuordnung")
