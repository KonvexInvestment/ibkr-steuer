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
from ui_model import build_final_values, collect_notices  # noqa: E402


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


def _with_compact_dates(rows):
    compact_rows = []
    for source in rows:
        row = dict(source)
        for field in ('date', 'reportDate', 'tradeDate'):
            if row.get(field):
                row[field] = row[field].replace('-', '')
        for field in ('dateTime', 'openDateTime'):
            if not row.get(field):
                continue
            date_part, time_part = row[field].split(';', 1)
            row[field] = (
                date_part.replace('-', '') + ';' + time_part.replace(':', '')
            )
        compact_rows.append(row)
    return compact_rows


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


def test_compact_dates_preserve_closed_lot_and_tax_matching():
    symbol = "ITALYOPT DEC25 14 C"
    trades = _with_compact_dates([
        _trade("OPEN", "C1", symbol, "2025-04-10", "BUY", "O", 0, 1),
        _trade("CLOSE", "C1", symbol, "2025-08-20", "SELL", "C", 216, -1),
    ])
    funds = _with_compact_dates([
        _ttax("OPEN", "C1", symbol, "2025-04-10", -0.025),
        _ttax("CLOSE", "C1", symbol, "2025-08-20", -0.025),
    ])
    lots = _with_compact_dates([_closed_lot(
        "C1", symbol, "2025-04-10", "2025-08-20", 1, 216,
    )])
    report = _run(trades, funds, lots)

    _assert_close(report["options_gain_eur"], 215.95, "kompakter TTAX-Match")
    detail = [r for r in report["trade_details"] if r["symbol"] == symbol][0]
    assert detail["reportDate"] == "2025-08-20"
    assert detail["dateTime"] == "2025-08-20 10:00:00"
    assert report["audit"]["transaction_tax"]["applied_count"] == 2
    print("  OK  Kompakte Daten: CLOSED_LOT- und TTAX-Matching unveraendert")


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
    """Gemischte Richtungen oder mehrere TTAX-Zeilen pro Tag ohne
    identifizierende Stueckzahl bleiben Prueffall."""
    symbol = "AMB"
    mixed = [
        _trade("O1", "A1", symbol, "2025-10-01", "BUY", "O", 0, 1),
        _trade("C2", "A1", symbol, "2025-10-01", "SELL", "C", 60, -1),
    ]
    report = _run(mixed, [_ttax("DAILY", "A1", symbol, "2025-10-01", -1)])
    _assert_close(report["options_gain_eur"], 60.0, "kein unsicherer Abzug")
    review = report["audit"]["unhandled_activity_codes"]
    assert len(review) == 1 and review[0]["code"] == "TTAX"
    audit = report["audit"]["transaction_tax"]
    assert audit["unmatched_count"] == 1
    assert audit["details"][0]["reason"] == "mehrere_trades_am_selben_tag"
    assert audit["details"][0]["fills"] == 2

    # Zwei Einzelsteuern ohne Stueckzahl fuer zwei gleichgerichtete Fills:
    # keine Zeile laesst sich einem Fill zuordnen.
    two_closes = [
        _trade("C1", "A1", symbol, "2025-10-01", "SELL", "C", 50, -1),
        _trade("C2", "A1", symbol, "2025-10-01", "SELL", "C", 60, -1),
    ]
    funds = [_ttax("T1", "A1", symbol, "2025-10-01", -1),
             _ttax("T2", "A1", symbol, "2025-10-01", -1)]
    report = _run(two_closes, funds)
    _assert_close(report["options_gain_eur"], 110.0, "kein unsicherer Abzug")
    assert report["audit"]["transaction_tax"]["unmatched_count"] == 2
    print("  OK  Mehrdeutige Same-Day-TTAX bleibt sichtbarer Prueffall")


def _french_ftt_fixture(lot_ids=True, second_buy_quantity=97,
                        described_quantity=170):
    """Realmuster U770 (Issue #89): HO, 0,3 % FTT als Tagesaggregat.

    Kauf 19.06. in zwei Fills derselben Sekunde (73 + 97 = 170), eine
    TTAX-Zeile "French Daily Trade Charge Tax HO 170" ueber 78,96 EUR
    (= 0,3 % von 11.304,05 + 15.015,60). Verkauf 20.06. in zwei Fills
    derselben Sekunde (100 + 70) mit drei CLOSED_LOTs (73 | 27 | 70), deren
    transactionID auf die eroeffnende Transaktion zeigt.
    """
    buy_a = _trade("B73", "917799", "HO", "2025-06-19", "BUY", "O", 0, 73,
                   category="STK")
    buy_a["proceeds"] = "-11304.05"
    buy_b = _trade("B97", "917799", "HO", "2025-06-19", "BUY", "O", 0,
                   second_buy_quantity, category="STK")
    buy_b["proceeds"] = "-15015.6"
    sell_a = _trade("S100", "917799", "HO", "2025-06-20", "SELL", "C",
                    235.740675, -100, category="STK")
    sell_b = _trade("S70", "917799", "HO", "2025-06-20", "SELL", "C",
                    167.57475, -70, category="STK")
    ttax = _ttax("DAILY", "917799", "HO", "2025-06-19", -78.96, category="STK")
    ttax["activityDescription"] = (
        f"French Daily Trade Charge Tax HO {described_quantity}")
    lots = [
        _closed_lot("917799", "HO", "2025-06-19", "2025-06-20", 73, 171.1047,
                    category="STK"),
        _closed_lot("917799", "HO", "2025-06-19", "2025-06-20", 27, 64.635975,
                    category="STK"),
        _closed_lot("917799", "HO", "2025-06-19", "2025-06-20", 70, 167.57475,
                    category="STK"),
    ]
    if lot_ids:
        lots[0]["transactionID"] = buy_a["transactionID"]
        lots[1]["transactionID"] = buy_b["transactionID"]
        lots[2]["transactionID"] = buy_b["transactionID"]
    return [buy_a, buy_b, sell_a, sell_b], [ttax], lots


def _ho_rows(report):
    return {
        row["quantity"]: row for row in report["trade_details"]
        if row.get("symbol") == "HO" and row.get("source") == "trades"
        and row.get("buySell") == "SELL"
    }


def test_daily_aggregate_tax_is_distributed_across_same_day_fills():
    """Issue #89: Tagesaggregat ueber zwei Kauf-Fills wird voll realisiert."""
    trades, funds, lots = _french_ftt_fixture()
    report = _run(trades, funds, lots)

    _assert_close(report["stocks_gain_eur"], 235.740675 + 167.57475 - 78.96,
                  "Aktiengewinn nach FTT")
    assert not report["audit"]["unhandled_activity_codes"]
    audit = report["audit"]["transaction_tax"]
    assert audit["unmatched_count"] == 0
    assert audit["applied_count"] == 1 and audit["distributed_count"] == 1
    assert audit["deferred_count"] == 0
    _assert_close(audit["applied_eur"], 78.96, "gesamte FTT angewandt")
    detail = audit["details"][0]
    assert detail["status"] == "applied_via_closed_lot"
    assert detail["fills"] == 2
    shares = {d["tradeID"]: d["share"] for d in detail["distribution"]}
    _assert_close(shares["B73"], 11304.05 / 26319.65, "Anteil nach Kaufwert 73")
    _assert_close(shares["B97"], 15015.6 / 26319.65, "Anteil nach Kaufwert 97")
    rows = _ho_rows(report)
    # Beide Verkaufs-Fills liegen in derselben Sekunde: mengenproportional.
    _assert_close(rows["-100"]["transaction_tax_eur"], -78.96 * 100 / 170,
                  "FTT-Anteil Verkauf 100")
    _assert_close(rows["-70"]["transaction_tax_eur"], -78.96 * 70 / 170,
                  "FTT-Anteil Verkauf 70")
    _assert_close(rows["-100"]["pnl_eur"], 235.740675 - 78.96 * 100 / 170,
                  "PnL Verkauf 100 nach FTT")
    print("  OK  Tagesaggregat (HO 170 = 73 + 97) wird auf beide Fills verteilt")


def test_daily_aggregate_without_lot_ids_consumes_each_lot_once():
    """Ohne Lot-IDs teilen sich die Fills derselben Sekunde die Lots; kein
    Lot wird doppelt beansprucht, das Ergebnis bleibt identisch."""
    trades, funds, lots = _french_ftt_fixture(lot_ids=False)
    report = _run(trades, funds, lots)
    _assert_close(report["stocks_gain_eur"], 235.740675 + 167.57475 - 78.96,
                  "Aktiengewinn nach FTT ohne Lot-IDs")
    audit = report["audit"]["transaction_tax"]
    assert audit["unmatched_count"] == 0 and audit["deferred_count"] == 0
    _assert_close(audit["applied_eur"], 78.96, "gesamte FTT angewandt")
    print("  OK  Lots derselben Sekunde werden ueber alle Fills nur einmal konsumiert")


def test_daily_aggregate_quantity_mismatch_stays_review():
    """Stueckzahl im Text (170) passt nicht zu den Fills (73 + 80)."""
    trades, funds, lots = _french_ftt_fixture(second_buy_quantity=80)
    report = _run(trades, funds, lots)
    _assert_close(report["stocks_gain_eur"], 235.740675 + 167.57475,
                  "kein geschaetzter Abzug")
    audit = report["audit"]["transaction_tax"]
    assert audit["unmatched_count"] == 1 and audit["applied_count"] == 0
    assert audit["details"][0]["reason"] == "tagesaggregat_menge_abweichend"
    review = report["audit"]["unhandled_activity_codes"]
    assert len(review) == 1 and review[0]["code"] == "TTAX"
    _assert_close(review[0]["amount_eur"], -78.96, "sichtbarer Pruefbetrag")
    print("  OK  Abweichende Stueckzahl im Buchungstext bleibt Prueffall")


def test_daily_aggregate_partially_open_defers_remainder():
    """Nur 100 von 170 Stueck verkauft: Rest der Kaufsteuer bleibt offen."""
    trades, funds, lots = _french_ftt_fixture()
    trades = trades[:3]  # SELL 70 entfaellt, Position teilweise offen
    lots = lots[:2]      # Lots 73 + 27 gehoeren zum SELL 100
    report = _run(trades, funds, lots)
    audit = report["audit"]["transaction_tax"]
    assert audit["unmatched_count"] == 0
    assert audit["applied_count"] == 1 and audit["deferred_count"] == 1
    applied = 78.96 * (11304.05 / 26319.65) + 78.96 * (15015.6 / 26319.65) * 27 / 97
    _assert_close(audit["applied_eur"], applied, "realisierter FTT-Anteil")
    _assert_close(audit["deferred_eur"], 78.96 - applied, "offener FTT-Anteil")
    assert audit["details"][0]["status"] == "partially_applied"
    _assert_close(report["stocks_gain_eur"], 235.740675 - applied,
                  "Aktiengewinn nach realisiertem Anteil")
    print("  OK  Teilverkauf: nur der realisierte FTT-Anteil mindert den Gewinn")


def test_same_second_closes_share_lot_reduction():
    """Ein Kauf-Fill, zwei Verkaufs-Fills derselben Sekunde: das Lot wird
    mengenproportional auf beide Schluss-Zeilen verteilt statt Prueffall."""
    buy = _trade("B170", "917799", "HO", "2025-06-19", "BUY", "O", 0, 170,
                 category="STK")
    sell_a = _trade("S100", "917799", "HO", "2025-06-20", "SELL", "C", 200,
                    -100, category="STK")
    sell_b = _trade("S70", "917799", "HO", "2025-06-20", "SELL", "C", 140,
                    -70, category="STK")
    ttax = _ttax("DAILY", "917799", "HO", "2025-06-19", -78.96, category="STK")
    lot = _closed_lot("917799", "HO", "2025-06-19", "2025-06-20", 170, 340,
                      category="STK")
    report = _run([buy, sell_a, sell_b], [ttax], [lot])
    audit = report["audit"]["transaction_tax"]
    assert audit["unmatched_count"] == 0 and audit["applied_count"] == 1
    _assert_close(audit["applied_eur"], 78.96, "volle Kaufsteuer realisiert")
    rows = _ho_rows(report)
    _assert_close(rows["-100"]["transaction_tax_eur"], -78.96 * 100 / 170,
                  "Anteil Schluss 100")
    _assert_close(rows["-70"]["transaction_tax_eur"], -78.96 * 70 / 170,
                  "Anteil Schluss 70")
    print("  OK  Schluss-Fills derselben Sekunde teilen sich das Lot")


def test_described_quantity_identifies_single_fill_among_same_day_trades():
    """Zwei Einzelsteuern mit Stueckzahl (EL 25, EL 27) fuer zwei Fills
    desselben Tages: jede Zeile findet ihren Fill, beide bleiben offen."""
    fill_a = _trade("E25", "E1", "EL", "2025-03-03", "BUY", "O", 0, 25,
                    category="STK")
    fill_b = _trade("E27", "E1", "EL", "2025-03-03", "BUY", "O", 0, 27,
                    category="STK")
    tax_a = _ttax("T25", "E1", "EL", "2025-03-03", -12.5, category="STK")
    tax_a["activityDescription"] = "French Daily Trade Charge Tax EL 25"
    tax_b = _ttax("T27", "E1", "EL", "2025-03-03", -13.5, category="STK")
    tax_b["activityDescription"] = "French Daily Trade Charge Tax EL 27"
    report = _run([fill_a, fill_b], [tax_a, tax_b])
    audit = report["audit"]["transaction_tax"]
    assert audit["unmatched_count"] == 0
    assert audit["deferred_count"] == 2 and audit["distributed_count"] == 0
    _assert_close(audit["deferred_eur"], 26.0, "beide Kaufsteuern offen")
    assert not report["audit"]["unhandled_activity_codes"]
    print("  OK  Stueckzahl im Text identifiziert den einzelnen Fill")


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


def test_missing_or_mismatched_lots_stay_review_items():
    trades = [
        _trade('OPEN', 'S1', 'EL', '2025-04-10', 'BUY', 'O', 0, 10,
               category='STK'),
        _trade('CLOSE', 'S1', 'EL', '2025-08-20', 'SELL', 'C', 100, -10,
               category='STK'),
    ]
    funds = [_ttax('OPEN', 'S1', 'EL', '2025-04-10', -10, category='STK')]
    lot = _closed_lot('S1', 'EL', '2025-04-10', '2025-08-20', 10, 100,
                      category='STK')
    for lots in ([], [dict(lot, openDateTime='2025-04-10;10:00:01')]):
        report = _run(trades, funds, lots)
        final = build_final_values(report, {})
        _assert_close(final['zeile_19'], 100, 'kein geschaetzter Abzug Z19')
        _assert_close(final['zeile_20'], 100, 'kein geschaetzter Abzug Z20')
        audit = report['audit']['transaction_tax']
        assert audit['unmatched_count'] == 1
        assert audit['deferred_count'] == audit['applied_count'] == 0
        assert (audit['details'][0]['reason']
                == 'closed_lot_abdeckung_unvollstaendig')
        review = report['audit']['unhandled_activity_codes']
        assert len(review) == 1 and review[0]['code'] == 'TTAX'
        _assert_close(review[0]['amount_eur'], -10, 'sichtbarer Pruefbetrag')
        notices = {n['id']: n for n in collect_notices(report)}
        assert 'unhandled_activity_codes' in notices
        assert 'transaction_tax_processed' not in notices
    print('  OK  Fehlende/falsch datierte Lots bleiben als Prueffall sichtbar')


def test_incomplete_lots_roll_back_only_the_affected_tax():
    trades = [
        _trade('OPEN', 'S1', 'EL', '2025-04-10', 'BUY', 'O', 0, 10,
               category='STK'),
        _trade('CLOSE', 'S1', 'EL', '2025-08-20', 'SELL', 'C', 100, -10,
               category='STK'),
    ]
    funds = [
        _ttax('OPEN', 'S1', 'EL', '2025-04-10', -10, category='STK'),
        _ttax('CLOSE', 'S1', 'EL', '2025-08-20', -2, category='STK'),
    ]
    lots = [_closed_lot('S1', 'EL', '2025-04-10', '2025-08-20', 4, 40,
                        category='STK')]
    for ordered_funds in (funds, list(reversed(funds))):
        report = _run(trades, ordered_funds, lots)
        _assert_close(report['stocks_gain_eur'], 98, 'sichere Verkaufssteuer')
        audit = report['audit']['transaction_tax']
        assert audit['unmatched_count'] == audit['applied_count'] == 1
        assert audit['deferred_count'] == 0
        _assert_close(audit['applied_eur'], 2, 'keine anteilige Kaufsteuer')
        _assert_close(report['trade_details'][0]['transaction_tax_eur'], -2,
                      'Trade-Detail ohne zurueckgerollte Kaufsteuer')
    print('  OK  Lueckenhafte Lots: nur unsichere Kaufsteuer zurueckgerollt')


def test_proven_other_opening_does_not_block_deferral():
    trades = [
        _trade('OLDER', 'S1', 'EL', '2025-03-10', 'BUY', 'O', 0, 10,
               category='STK'),
        _trade('OPEN', 'S1', 'EL', '2025-04-10', 'BUY', 'O', 0, 10,
               category='STK'),
        _trade('CLOSE', 'S1', 'EL', '2025-08-20', 'SELL', 'C', 100, -10,
               category='STK'),
    ]
    funds = [_ttax('OPEN', 'S1', 'EL', '2025-04-10', -10, category='STK')]
    lots = [_closed_lot('S1', 'EL', '2025-03-10', '2025-08-20', 10, 100,
                        category='STK')]
    report = _run(trades, funds, lots)
    assert not report['audit']['unhandled_activity_codes']
    audit = report['audit']['transaction_tax']
    assert audit['deferred_count'] == 1 and audit['applied_count'] == 0
    _assert_close(audit['deferred_eur'], 10, 'Kauf bleibt nachgewiesen offen')
    _assert_close(report['stocks_gain_eur'], 100, 'anderes Lot unveraendert')
    print('  OK  Belegter Verkauf einer anderen Anschaffung erlaubt')


def test_open_position_and_unrelated_closes_remain_deferred():
    opening = _trade('OPEN', 'S1', 'EL', '2025-04-10', 'BUY', 'O', 0, 10,
                     category='STK')
    funds = [_ttax('OPEN', 'S1', 'EL', '2025-04-10', -10, category='STK')]
    closes = [
        _trade('EARLIER', 'S1', 'EL', '2025-03-10', 'SELL', 'C', 100, -10,
               category='STK'),
        _trade('FUTURE', 'S1', 'EL', '2026-08-20', 'SELL', 'C', 100, -10,
               category='STK'),
        _trade('OTHER', 'S2', 'SAN', '2025-08-20', 'SELL', 'C', 100, -10,
               category='STK'),
    ]
    for extra_trades in ([], closes):
        report = _run([opening] + extra_trades, funds)
        audit = report['audit']['transaction_tax']
        assert audit['deferred_count'] == 1 and audit['unmatched_count'] == 0
        _assert_close(audit['deferred_eur'], 10, 'offene Kaufsteuer')
        assert not report['audit']['unhandled_activity_codes']
    print('  OK  Offene Position sowie fruehere/spaetere/fremde Schliessungen')


if __name__ == "__main__":
    test_real_option_pattern_applies_open_and_close_tax()
    test_compact_dates_preserve_closed_lot_and_tax_matching()
    test_open_tax_is_allocated_partially_across_years()
    test_trade_taxes_field_prevents_double_counting()
    test_break_even_close_becomes_topf2_loss()
    test_kap_inv_tax_reduces_raw_gain_before_partial_exemption()
    test_ambiguous_same_day_match_stays_manual_review()
    test_short_option_open_tax_stays_manual_review()
    test_missing_or_mismatched_lots_stay_review_items()
    test_incomplete_lots_roll_back_only_the_affected_tax()
    test_proven_other_opening_does_not_block_deferral()
    test_open_position_and_unrelated_closes_remain_deferred()
    test_daily_aggregate_tax_is_distributed_across_same_day_fills()
    test_daily_aggregate_without_lot_ids_consumes_each_lot_once()
    test_daily_aggregate_quantity_mismatch_stays_review()
    test_daily_aggregate_partially_open_defers_remainder()
    test_same_second_closes_share_lot_reduction()
    test_described_quantity_identifies_single_fill_among_same_day_trades()
    print("Alle TTAX-Tests bestanden.")
