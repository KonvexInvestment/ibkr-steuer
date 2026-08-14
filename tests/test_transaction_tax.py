#!/usr/bin/env python3
"""Regressionen fuer tradebezogene IBKR-Transaktionssteuern (Issue #89).

Der reale IBKR-Fall liefert TTAX separat in StmtFunds. Die Tax-Row laesst
sich nicht ueber ihre tradeID, aber eindeutig ueber conid + Tag dem Trade
zuordnen; das Trade-Feld ``taxes`` und der FIFO-PnL enthalten den Betrag dort
nicht. Kaufsteuern muessen ueber CLOSED_LOT bis zur Veraeusserung getragen
werden, Verkaufssteuern mindern den Schluss unmittelbar.
"""
import contextlib
import csv
import io
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import calculate_tax  # noqa: E402


def _write_csv(path, rows):
    if not rows:
        return
    fields = sorted({key for row in rows for key in row})
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _trade(tid, conid, symbol, date, side, open_close, pnl, quantity,
           category="OPT", isin="", subcategory="", taxes="0"):
    return {
        "tradeID": tid,
        "transactionID": f"TX-{tid}",
        "conid": conid,
        "assetCategory": category,
        "subCategory": subcategory,
        "transactionType": "ExchTrade",
        "buySell": side,
        "openCloseIndicator": open_close,
        "symbol": symbol,
        "description": f"{symbol} Testinstrument",
        "isin": isin,
        "quantity": str(quantity),
        "tradePrice": "10",
        "closePrice": "10",
        "cost": "-100" if open_close == "C" else "100",
        "proceeds": "200" if open_close == "C" else "-100",
        "multiplier": "1",
        "ibCommission": "0",
        "taxes": str(taxes),
        "fxRateToBase": "1",
        "currency": "EUR",
        "dateTime": f"{date};10:00:00",
        "tradeDate": date,
        "reportDate": date,
        "fifoPnlRealized": str(pnl),
    }


def _ttax(tid, conid, symbol, date, amount, category="OPT"):
    # Absichtlich eine Tax-tradeID ohne Entsprechung im Trade: Das entspricht
    # dem echten IBKR-Export; der sichere Match laeuft ueber conid + Tag.
    return {
        "transactionID": f"TTX-{tid}",
        "tradeID": f"DAILY-{tid}",
        "conid": conid,
        "activityCode": "TTAX",
        "activityDescription": f"Transaction Tax {symbol}",
        "amount": str(amount),
        "assetCategory": category,
        "currency": "EUR",
        "fxRateToBase": "1",
        "symbol": symbol,
        "date": date,
        "reportDate": date,
        "levelOfDetail": "BaseCurrency",
    }


def _closed_lot(conid, symbol, open_date, close_date, quantity, pnl,
                category="OPT", isin="", subcategory=""):
    return {
        "tradeID": f"LOT-{close_date}",
        "transactionID": f"LOT-TX-{open_date}",
        "conid": conid,
        "assetCategory": category,
        "subCategory": subcategory,
        "symbol": symbol,
        "isin": isin,
        "currency": "EUR",
        "buySell": "SELL",
        "quantity": str(quantity),
        "cost": "100",
        "fifoPnlRealized": str(pnl),
        "fxRateToBase": "1",
        "openDateTime": f"{open_date};10:00:00",
        "dateTime": f"{close_date};10:00:00",
        "tradeDate": close_date,
        "reportDate": close_date,
        "levelOfDetail": "CLOSED_LOT",
    }


def _run(trades, funds, lots=None, tax_year=2025):
    with tempfile.TemporaryDirectory() as tmp:
        _write_csv(os.path.join(tmp, "trades.csv"), trades)
        _write_csv(os.path.join(tmp, "statement_of_funds.csv"), funds)
        if lots:
            _write_csv(os.path.join(tmp, "closed_lots.csv"), lots)
        _write_csv(os.path.join(tmp, "account_info.csv"), [{
            "currency": "EUR",
            "tax_year": str(tax_year),
            "fx_transactions_count": "0",
        }])
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp)


def _assert_close(actual, expected, label):
    if abs(actual - expected) > 0.0001:
        raise AssertionError(f"{label}: erwartet {expected}, aktuell {actual}")


def test_real_option_pattern_applies_open_and_close_tax():
    symbol = "ITALYOPT DEC25 14 C"
    trades = [
        _trade("OPEN", "C1", symbol, "2025-04-10", "BUY", "O", 0, 1),
        _trade("CLOSE", "C1", symbol, "2025-08-20", "SELL", "C", 216, -1),
    ]
    funds = [
        _ttax("OPEN", "C1", symbol, "2025-04-10", -0.025),
        _ttax("CLOSE", "C1", symbol, "2025-08-20", -0.025),
    ]
    lots = [_closed_lot(
        "C1", symbol, "2025-04-10", "2025-08-20", 1, 216,
    )]
    report = _run(trades, funds, lots)

    _assert_close(report["options_gain_eur"], 215.95, "Optionsgewinn nach TTAX")
    _assert_close(report["zeile_19_netto_eur"], 215.95, "Zeile 19")
    _assert_close(
        report["topf2_by_category"]["Optionen"]["gain"], 215.95,
        "Topf-2-Aufschluesselung",
    )
    detail = [r for r in report["trade_details"] if r["symbol"] == symbol][0]
    _assert_close(detail["pnl_eur"], 215.95, "Trade-Detail-PnL")
    _assert_close(detail["transaction_tax_eur"], -0.05, "Trade-Detail-TTAX")
    _assert_close(detail["fifoPnlRealized"], 216.0, "IBKR-Roh-PnL bleibt sichtbar")
    assert not report["audit"]["unhandled_activity_codes"]
    audit = report["audit"]["transaction_tax"]
    assert audit["applied_count"] == 2
    _assert_close(audit["applied_eur"], 0.05, "angewandte TTAX")
    print("  OK  Reales Optionsmuster: Kauf- und Verkaufs-TTAX in Topf 2")


def test_open_tax_is_allocated_partially_across_years():
    symbol = "SAN"
    trades = [
        _trade("OPEN", "S1", symbol, "2024-06-01", "BUY", "O", 0, 100,
               category="STK"),
        _trade("CLOSE", "S1", symbol, "2025-06-01", "SELL", "C", 100, -40,
               category="STK"),
    ]
    funds = [_ttax("OPEN", "S1", symbol, "2024-06-01", -10, category="STK")]
    lots = [_closed_lot(
        "S1", symbol, "2024-06-01", "2025-06-01", 40, 100,
        category="STK",
    )]
    report = _run(trades, funds, lots)

    _assert_close(report["stocks_gain_eur"], 96.0, "40 % der Kaufsteuer")
    _assert_close(report["zeile_20_stock_gains_eur"], 96.0, "Zeile 20")
    audit = report["audit"]["transaction_tax"]
    _assert_close(audit["applied_eur"], 4.0, "realisierter Steueranteil")
    _assert_close(audit["deferred_eur"], 6.0, "offener Steueranteil")
    assert not report["audit"]["unhandled_activity_codes"]
    print("  OK  Kauf-TTAX wird bei Teilverkauf quantity-genau realisiert")


def test_trade_taxes_field_prevents_double_counting():
    symbol = "EMBEDDED"
    trades = [
        _trade("CLOSE", "E1", symbol, "2025-07-18", "SELL", "C", 99, -1,
               taxes=-1),
    ]
    funds = [_ttax("CLOSE", "E1", symbol, "2025-07-18", -1)]
    report = _run(trades, funds)

    _assert_close(report["options_gain_eur"], 99.0, "kein Doppelabzug")
    audit = report["audit"]["transaction_tax"]
    assert audit["already_in_trade_count"] == 1
    _assert_close(audit["applied_eur"], 0.0, "nichts zusaetzlich angewandt")
    assert not report["audit"]["unhandled_activity_codes"]
    print("  OK  Bereits im Trade enthaltene Steuer wird nicht doppelt abgezogen")


def test_break_even_close_becomes_topf2_loss():
    symbol = "ZERO"
    trades = [
        _trade("CLOSE", "Z1", symbol, "2025-09-01", "SELL", "C", 0, -1,
               category="WAR"),
    ]
    funds = [_ttax("CLOSE", "Z1", symbol, "2025-09-01", -0.5,
                   category="WAR")]
    report = _run(trades, funds)

    _assert_close(report["options_loss_eur"], -0.5, "TTAX erzeugt Verlust")
    _assert_close(report["zeile_22_other_losses_eur"], 0.5, "Zeile 22")
    assert len(report["trade_details"]) == 1
    _assert_close(report["trade_details"][0]["pnl_eur"], -0.5, "Trade-Detail")
    print("  OK  Break-even-Schluss mit TTAX wird nicht uebersprungen")


def test_kap_inv_tax_reduces_raw_gain_before_partial_exemption():
    symbol = "SPY"
    isin = "US78462F1030"
    trades = [
        _trade("OPEN", "F1", symbol, "2024-03-01", "BUY", "O", 0, 10,
               category="STK", isin=isin, subcategory="ETF"),
        _trade("CLOSE", "F1", symbol, "2025-03-01", "SELL", "C", 100, -10,
               category="STK", isin=isin, subcategory="ETF"),
    ]
    funds = [_ttax("OPEN", "F1", symbol, "2024-03-01", -10, category="STK")]
    lots = [_closed_lot(
        "F1", symbol, "2024-03-01", "2025-03-01", 10, 100,
        category="STK", isin=isin, subcategory="ETF",
    )]
    report = _run(trades, funds, lots)

    kap = report["kap_inv"]
    _assert_close(kap["etf_gain_raw_eur"], 90.0, "KAP-INV Rohgewinn")
    _assert_close(kap["etf_gain_taxable_eur"], 63.0, "30 % TFS nach TTAX")
    _assert_close(kap["etf_net_taxable_eur"], 63.0, "KAP-INV Netto")
    print("  OK  KAP-INV: TTAX vor Teilfreistellung beruecksichtigt")


def test_ambiguous_same_day_match_stays_manual_review():
    symbol = "AMB"
    trades = [
        _trade("C1", "A1", symbol, "2025-10-01", "SELL", "C", 50, -1),
        _trade("C2", "A1", symbol, "2025-10-01", "SELL", "C", 60, -1),
    ]
    funds = [_ttax("DAILY", "A1", symbol, "2025-10-01", -1)]
    report = _run(trades, funds)

    _assert_close(report["options_gain_eur"], 110.0, "kein unsicherer Abzug")
    review = report["audit"]["unhandled_activity_codes"]
    assert len(review) == 1 and review[0]["code"] == "TTAX"
    assert report["audit"]["transaction_tax"]["unmatched_count"] == 1
    print("  OK  Mehrdeutige Same-Day-TTAX bleibt sichtbarer Prueffall")


def test_short_option_open_tax_stays_manual_review():
    symbol = "SHORTOPT"
    trades = [
        _trade("OPEN", "SOPT1", symbol, "2025-10-02", "SELL", "O", 0, -1),
    ]
    funds = [_ttax("OPEN", "SOPT1", symbol, "2025-10-02", -0.5)]
    report = _run(trades, funds)

    review = report["audit"]["unhandled_activity_codes"]
    assert len(review) == 1 and review[0]["code"] == "TTAX"
    audit = report["audit"]["transaction_tax"]
    assert audit["unmatched_count"] == 1
    assert audit["details"][0]["reason"] == "short_option_eroeffnung"
    print("  OK  TTAX auf Short-Options-Eröffnung bleibt im richtigen Jahr prüfbar")


if __name__ == "__main__":
    test_real_option_pattern_applies_open_and_close_tax()
    test_open_tax_is_allocated_partially_across_years()
    test_trade_taxes_field_prevents_double_counting()
    test_break_even_close_becomes_topf2_loss()
    test_kap_inv_tax_reduces_raw_gain_before_partial_exemption()
    test_ambiguous_same_day_match_stays_manual_review()
    test_short_option_open_tax_stays_manual_review()
    print("Alle TTAX-Tests bestanden.")
