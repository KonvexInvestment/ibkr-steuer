"""Synthetische Regression-Tests für Issue #98: Aktiensplit zwischen
Andienung und Verkauf.

Nach einem Split führt IBKR die Lots unter der neuen conid mit dem
ursprünglichen openDateTime weiter, aber in Stückzahlen nach dem Split
(belegt an echten Exporten: ANET 4:1, BTC 1:5). Die Andienung steht in
Stückzahlen vor dem Split. Ohne Umrechnung wurde nur ein Bruchteil der
eingebetteten Prämie (Reverse Split) bzw. bei Fonds die Lücke zwischen Strike
und Basis nach dem Split (Forward Split) korrigiert.

Echte Splits zwischen Andienung und Verkauf kommen in den lokalen Exporten
nicht vor; die Fixtures folgen dem Muster der CorporateActions-Sektion
(Typ FS/RS/FI, levelOfDetail DETAIL, actionID verbindet alte und neue conid).

Aufruf: python tests/test_stock_split_assignments.py
"""
import contextlib
import csv
import io
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (
    _build_stock_split_index,
    _put_lot_above_strike_review,
    _split_factor_between,
    calculate_tax,
)
from ui_model import build_final_values, default_toggles


FUND_ISIN = "US81369Y5069"  # XLE, aktienfonds (30 % Teilfreistellung)


def assert_close(actual, expected, tol=0.001, label=""):
    if abs(actual - expected) > tol:
        raise AssertionError(
            f"{label}: erwartet {expected}, aktuell {actual} "
            f"(delta {actual - expected})"
        )


def _option(symbol, side, date_time, contracts, price, strike, expiry, pc,
            transaction_type, pnl=0.0, currency="EUR", fx=1.0):
    sign = -1 if side == "SELL" else 1
    return {
        "tradeID": f"{symbol}_{pc}_{transaction_type}_{date_time[:10]}",
        "assetCategory": "OPT",
        "transactionType": transaction_type,
        "buySell": side,
        "openCloseIndicator": "O" if side == "SELL" else "C",
        "putCall": pc,
        "strike": str(strike),
        "expiry": expiry,
        "underlyingSymbol": symbol,
        "symbol": f"{symbol} {expiry} {strike} {pc}",
        "quantity": str(sign * contracts),
        "tradePrice": str(price),
        "closePrice": str(price),
        "multiplier": "100",
        "ibCommission": "0",
        "fxRateToBase": str(fx),
        "currency": currency,
        "dateTime": date_time,
        "tradeDate": date_time[:10],
        "reportDate": date_time[:10],
        "fifoPnlRealized": str(pnl),
    }


def _put_pair(symbol, sell_date, assign_ts, contracts, price, strike,
              currency="EUR", fx=1.0):
    expiry = assign_ts[:10]
    return [
        _option(symbol, "SELL", f"{sell_date} 10:00:00", contracts, price,
                strike, expiry, "P", "ExchTrade", currency=currency, fx=fx),
        _option(symbol, "BUY", assign_ts, contracts, 0, strike, expiry, "P",
                "BookTrade", currency=currency, fx=fx),
    ]


def _stock(symbol, conid, tid, side, oc, date_time, qty, price, cost, pnl,
           transaction_type="ExchTrade", isin="", is_fund=False,
           currency="EUR", fx=1.0):
    signed_qty = qty if side == "BUY" else -qty
    return {
        "tradeID": tid,
        "assetCategory": "STK",
        "subCategory": "ETF" if is_fund else "",
        "transactionType": transaction_type,
        "buySell": side,
        "openCloseIndicator": oc,
        "underlyingSymbol": symbol,
        "symbol": symbol,
        "description": f"{symbol} CORP",
        "conid": conid,
        "isin": isin,
        "quantity": str(signed_qty),
        "tradePrice": str(price),
        "closePrice": str(price),
        "ibCommission": "0",
        "fxRateToBase": str(fx),
        "currency": currency,
        "dateTime": date_time,
        "tradeDate": date_time[:10],
        "reportDate": date_time[:10],
        "fifoPnlRealized": str(pnl),
        "cost": str(cost),
        "proceeds": str(-signed_qty * price),
    }


def _lot(symbol, conid, open_ts, close_ts, qty, cost, pnl, isin="",
         is_fund=False, currency="EUR", fx=1.0):
    return {
        "assetCategory": "STK",
        "subCategory": "ETF" if is_fund else "",
        "currency": currency,
        "symbol": symbol,
        "underlyingSymbol": symbol,
        "description": f"{symbol} CORP",
        "conid": conid,
        "isin": isin,
        "openDateTime": open_ts,
        "dateTime": close_ts,
        "reportDate": close_ts[:10],
        "quantity": str(qty),
        "buySell": "SELL" if qty > 0 else "BUY",
        "cost": str(cost),
        "fifoPnlRealized": str(pnl),
        "fxRateToBase": str(fx),
    }


def _split(symbol, date_time, old_conid, old_qty, new_conid, new_qty, text,
           split_type, action_id):
    """CorporateActions-Zeilen eines Splits (DETAIL plus SUMMARY-Zeile)."""
    def row(conid, qty, level="DETAIL"):
        return {
            "assetCategory": "STK", "levelOfDetail": level,
            "type": split_type, "actionID": action_id,
            "actionDescription": text, "description": text,
            "conid": conid, "symbol": symbol, "quantity": str(qty),
            "dateTime": date_time, "reportDate": date_time[:10],
        }
    rows = [row(old_conid, old_qty)]
    if new_conid:
        rows.append(row(new_conid, new_qty))
    rows.append(row(new_conid or old_conid, 0, level="SUMMARY"))
    return rows


def _write(path, rows):
    fields = sorted({key for row in rows for key in row})
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def calculate(trades, lots=None, corporate_actions=None, instruments=None,
              tax_year=2025, anlage_so_overrides=None, conversion_rates=None):
    with tempfile.TemporaryDirectory() as tmp:
        _write(os.path.join(tmp, "account_info.csv"), [{
            "currency": "EUR", "tax_year": str(tax_year),
            "fx_transactions_count": "0",
        }])
        _write(os.path.join(tmp, "trades.csv"), trades)
        if lots:
            _write(os.path.join(tmp, "closed_lots.csv"), lots)
        if corporate_actions:
            _write(os.path.join(tmp, "corporate_actions.csv"),
                   corporate_actions)
        if instruments:
            _write(os.path.join(tmp, "financial_instruments.csv"), instruments)
        if conversion_rates:
            _write(os.path.join(tmp, "conversion_rates.csv"), [
                {"reportDate": day, "fromCurrency": "USD",
                 "toCurrency": "EUR", "rate": str(rate)}
                for day, rate in conversion_rates.items()])
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp, tax_year=tax_year,
                                 anlage_so_overrides=anlage_so_overrides)


def _stock_rows(report, symbol):
    return [row for row in report["trade_details"]
            if row.get("source") == "trades"
            and row.get("assetCategory") == "STK"
            and row.get("symbol") == symbol]


def _only_adjustment(report):
    adjustments = report["audit"].get("stock_split_adjustments", [])
    assert len(adjustments) == 1, adjustments
    return adjustments[0]


# ---------------------------------------------------------------------------
# Split-Index
# ---------------------------------------------------------------------------

def test_split_index_reads_ratio_from_text_and_tolerates_cash_in_lieu():
    """Reverse Split 1:10: 805 alte werden 80 neue Aktien (5 bar abgefunden)."""
    rows = _split("TLRY", "2025-12-01 20:25:00", "111", -805, "222", 80,
                  "TLRY(US88688T1007) SPLIT 1 FOR 10 (TLRY, TILRAY BRANDS, "
                  "US88688T2088)", "RS", "A1")
    index = _build_stock_split_index(rows)
    assert set(index) == {"111", "222"}
    event = index["222"][0]
    assert event is index["111"][0], "beide conids teilen ein Ereignis"
    assert_close(event["factor"], 0.1, label="Faktor aus Text")
    assert event["issue"] == ""
    factor, events = _split_factor_between(
        "222", "2025-01-10 16:20:00", "2025-12-10 10:00:00", index)
    assert_close(factor, 0.1, label="Faktor zwischen Andienung und Verkauf")
    assert events == [event]
    print("  OK  Split-Index: Verhältnis aus dem Text, Barabfindung toleriert")


def test_split_index_flags_contradicting_quantities_and_missing_ratio():
    mismatch = _split("XYZ", "2025-05-01 20:25:00", "1", -500, "2", 100,
                      "XYZ SPLIT 1 FOR 10", "RS", "B1")
    missing = _split("ABC", "2025-05-01 20:25:00", "3", -500, "4", 50,
                     "ABC REVERSE SPLIT", "RS", "B2")
    index = _build_stock_split_index(mismatch + missing)
    assert index["2"][0]["factor"] is None
    assert index["2"][0]["issue"] == "quantity_mismatch"
    assert index["4"][0]["factor"] is None
    assert index["4"][0]["issue"] == "ratio_missing"
    factor, events = _split_factor_between(
        "4", "2025-01-02 16:20:00", "2025-06-01 10:00:00", index)
    assert factor is None and len(events) == 1
    print("  OK  Split-Index: widersprüchliche Mengen und fehlendes "
          "Verhältnis ergeben keinen Faktor")


def test_split_factor_follows_chain_and_respects_time_window():
    """A→B (2:1) und danach B→C (3:1); Lot unter C."""
    rows = (_split("CH", "2025-03-01 20:25:00", "A", -100, "B", 200,
                   "CH SPLIT 2 FOR 1", "FI", "C1")
            + _split("CH", "2025-06-01 20:25:00", "B", -200, "C", 600,
                     "CH SPLIT 3 FOR 1", "FI", "C2"))
    index = _build_stock_split_index(rows)
    factor, events = _split_factor_between(
        "C", "2025-01-10 16:20:00", "2025-09-01 10:00:00", index)
    assert_close(factor, 6.0, label="Kette A→B→C")
    assert [e["action_id"] for e in events] == ["C2", "C1"]
    factor, _ = _split_factor_between(
        "C", "2025-04-01 16:20:00", "2025-09-01 10:00:00", index)
    assert_close(factor, 3.0, label="Eröffnung zwischen den Splits")
    factor, events = _split_factor_between(
        "A", "2025-01-10 16:20:00", "2025-02-01 10:00:00", index)
    assert factor == 1.0 and events == [], "Split nach dem Verkauf zählt nicht"
    print("  OK  Split-Faktor: Kette rückwärts verfolgt, Zeitfenster beachtet")


# ---------------------------------------------------------------------------
# Put-Andienungen
# ---------------------------------------------------------------------------

def test_reverse_split_same_year_put_restores_full_premium():
    """Reverse Split 1:10 zwischen Andienung (500 Stück) und Verkauf (50 Stück).

    Prämie 5 × 100 × 0,12 = 60 EUR, IBKR-Basis 750 − 60 = 690 EUR.
    Ohne Umrechnung korrigierte der Code nur 50 von 500 Stück (6 EUR) und
    versteuerte 54 EUR Prämie doppelt.
    """
    trades = _put_pair("TLRY", "2025-01-02", "2025-01-10 16:20:00", 5, 0.12,
                       1.5)
    trades += [
        _stock("TLRY", "111", "tlry_assign", "BUY", "O",
               "2025-01-10 16:20:00", 500, 1.5, 690, 0, "BookTrade"),
        _stock("TLRY", "222", "tlry_sale", "SELL", "C",
               "2025-12-10 10:00:00", 50, 16, 690, 110),
    ]
    lots = [_lot("TLRY", "222", "2025-01-10 16:20:00", "2025-12-10 10:00:00",
                 50, 690, 110)]
    actions = _split("TLRY", "2025-12-01 20:25:00", "111", -500, "222", 50,
                     "TLRY(US88688T1007) SPLIT 1 FOR 10 (TLRY, TILRAY BRANDS, "
                     "US88688T2088)", "RS", "R1")
    report = calculate(trades, lots, actions)

    sale, = _stock_rows(report, "TLRY")
    assert sale.get("stillhalter_adjusted")
    assert_close(sale["stillhalter_adjustment_raw"], 60, label="Korrektur")
    assert_close(sale["cost"], 750, label="Basis zum Strike")
    assert_close(sale["fifoPnlRealized"], 50, label="Aktien-PnL")
    assert_close(report["stocks_gain_eur"], 50, label="Topf 1")
    assert_close(report["options_gain_eur"], 60, label="Topf 2")
    assert_close(report["audit"]["put_nosell_premium_eur"], 0,
                 label="kein offener Rest")
    assert report["audit"].get("stillhalter_corrections_dropped", []) == []
    adj = _only_adjustment(report)
    assert adj["status"] == "umgerechnet" and adj["source"] == "same_year"
    assert_close(adj["split_factor"], 0.1, label="Faktor")
    assert_close(adj["assignment_shares"], 500, label="Andienungs-Stücke")
    assert_close(adj["lot_shares"], 50, label="Lot-Stücke")
    assert_close(adj["correction_raw"], 60, label="Korrektur im Audit")
    assert "1:10" in adj["splits"]
    print("  OK  Reverse Split 1:10: volle Prämie 60 aus dem Aktien-PnL "
          "(110 → 50)")


def test_forward_split_fund_restores_strike_not_post_split_gap():
    """KAP-INV-Fonds, Split 2:1, Basis zusätzlich um 50 EUR gemindert (ROC).

    Strike 90, Prämie 200, IBKR-Basis 9000 − 200 − 50 = 8750 für 100 Stück,
    nach dem Split 200 Stück zu 43,75. Die Fonds-Regel stellt die volle
    Strike-Basis wieder her: 9000, nicht 200 × (90 − 43,75).
    """
    trades = _put_pair("XLE", "2025-02-03", "2025-02-21 16:20:00", 1, 2.0, 90)
    trades += [
        _stock("XLE", "301", "xle_assign", "BUY", "O", "2025-02-21 16:20:00",
               100, 90, 8750, 0, "BookTrade", FUND_ISIN, True),
        _stock("XLE", "302", "xle_sale", "SELL", "C", "2025-11-03 10:00:00",
               200, 50, 8750, 1250, isin=FUND_ISIN, is_fund=True),
    ]
    lots = [_lot("XLE", "302", "2025-02-21 16:20:00", "2025-11-03 10:00:00",
                 200, 8750, 1250, FUND_ISIN, True)]
    actions = _split("XLE", "2025-06-02 20:25:00", "301", -100, "302", 200,
                     "XLE(US81369Y5069) SPLIT 2 FOR 1 (XLE, ENERGY SELECT "
                     "SECTOR SPDR, US81369Y5069)", "FI", "F1")
    instruments = [{"symbol": "XLE", "isin": FUND_ISIN, "assetCategory": "STK",
                    "subCategory": "ETF", "description": "XLE"}]
    report = calculate(trades, lots, actions, instruments)

    sale, = _stock_rows(report, "XLE")
    assert_close(sale["cost"], 9000, label="Fonds-Basis zum Strike")
    assert_close(sale["fifoPnlRealized"], 1000, label="Fonds-PnL")
    assert_close(sale["stillhalter_adjustment_raw"], 250,
                 label="Prämie plus Basisminderung")
    assert_close(sale["invstg_basis_adjustment_raw"], 50,
                 label="nur die Basisminderung")
    assert_close(report["kap_inv"]["etf_gain_raw_eur"], 1000,
                 label="KAP-INV-Rohertrag")
    assert_close(report["options_gain_eur"], 200, label="Prämie in Topf 2")
    assert report["audit"].get("stillhalter_corrections_dropped", []) == []
    adj = _only_adjustment(report)
    assert_close(adj["split_factor"], 2.0, label="Faktor")
    assert_close(adj["correction_raw"], 250, label="Korrektur im Audit")
    print("  OK  Fonds mit Split 2:1: Basis 8750 → 9000 (Strike), "
          "nicht 200 × 46,25")


def test_forward_split_without_conid_change_and_partial_sale():
    """Split 2:1 ohne conid-Wechsel (eine CA-Zeile mit der Zusatzmenge).

    100 Stück zu 100 angedient (Prämie 200, Basis 9800), nach dem Split
    200 Stück zu 49; verkauft werden 120. Korrigiert wird der Anteil von
    60 Andienungs-Stücken (120 EUR), der Rest der Prämie gehört zum offenen
    Bestand.
    """
    trades = _put_pair("PART", "2025-02-03", "2025-02-21 16:20:00", 1, 2.0,
                       100)
    trades += [
        _stock("PART", "401", "part_assign", "BUY", "O",
               "2025-02-21 16:20:00", 100, 100, 9800, 0, "BookTrade"),
        _stock("PART", "401", "part_sale", "SELL", "C", "2025-10-01 10:00:00",
               120, 55, 5880, 720),
    ]
    lots = [_lot("PART", "401", "2025-02-21 16:20:00", "2025-10-01 10:00:00",
                 120, 5880, 720)]
    actions = [row for row in _split(
        "PART", "2025-06-02 20:25:00", "401", 100, None, 0,
        "PART(US0000000401) SPLIT 2 FOR 1 (PART, PART CORP, US0000000401)",
        "FS", "P1") if row["levelOfDetail"] == "DETAIL"]
    report = calculate(trades, lots, actions)

    sale, = _stock_rows(report, "PART")
    assert_close(sale["stillhalter_adjustment_raw"], 120, label="Teilkorrektur")
    assert_close(sale["cost"], 6000, label="Basis 120 × 50")
    assert_close(sale["fifoPnlRealized"], 600, label="Aktien-PnL")
    assert_close(report["stocks_gain_eur"], 600, label="Topf 1")
    assert_close(report["options_gain_eur"], 200, label="volle Prämie Topf 2")
    assert report["audit"].get("stillhalter_corrections_dropped", []) == []
    adj = _only_adjustment(report)
    assert_close(adj["assignment_shares"], 60, label="Andienungs-Stücke")
    assert_close(adj["lot_shares"], 120, label="Lot-Stücke")
    print("  OK  Split 2:1 ohne conid-Wechsel, Teilverkauf: 120 von 200 EUR "
          "korrigiert")


def test_cross_year_put_assignment_with_split_before_sale():
    """Andienung 2024, Split 3:1 und Verkauf 2025 (Steuerjahr 2025).

    Prämie 150 wurde 2024 versteuert; 2025 wird nur die Aktienbasis auf den
    Strike zurückgesetzt: 5850 → 6000, PnL 1650 → 1500.
    """
    trades = _put_pair("XYR", "2024-05-01", "2024-06-21 16:20:00", 1, 1.5, 60)
    trades += [
        _stock("XYR", "501", "xyr_assign", "BUY", "O", "2024-06-21 16:20:00",
               100, 60, 5850, 0, "BookTrade"),
        _stock("XYR", "502", "xyr_sale", "SELL", "C", "2025-04-10 10:00:00",
               300, 25, 5850, 1650),
    ]
    lots = [_lot("XYR", "502", "2024-06-21 16:20:00", "2025-04-10 10:00:00",
                 300, 5850, 1650)]
    actions = _split("XYR", "2025-03-03 20:25:00", "501", -100, "502", 300,
                     "XYR(US0000000501) SPLIT 3 FOR 1 (XYR, XYR CORP, "
                     "US0000000502)", "FI", "X1")
    report = calculate(trades, lots, actions)

    sale, = _stock_rows(report, "XYR")
    assert_close(sale["cost"], 6000, label="Basis zum Strike")
    assert_close(sale["fifoPnlRealized"], 1500, label="Aktien-PnL")
    assert_close(report["stocks_gain_eur"], 1500, label="Topf 1")
    assert_close(report["options_gain_eur"], 0, label="Prämie im Vorjahr")
    correction, = report["audit"]["cross_year_put_corrections"]
    assert_close(correction["correction_per_share_raw"], 0.5,
                 label="Korrektur je Lot-Stück")
    assert report["audit"].get("stillhalter_corrections_dropped", []) == []
    adj = _only_adjustment(report)
    assert adj["source"] == "cross_year"
    assert_close(adj["split_factor"], 3.0, label="Faktor")
    assert_close(adj["correction_raw"], 150, label="Korrektur im Audit")
    print("  OK  Vorjahres-Andienung, Split 3:1 im Steuerjahr: Basis "
          "5850 → 6000")


def test_split_without_ratio_is_review_case_not_silent_correction():
    trades = _put_pair("TLRY", "2025-01-02", "2025-01-10 16:20:00", 5, 0.12,
                       1.5)
    trades += [
        _stock("TLRY", "111", "tlry_assign", "BUY", "O",
               "2025-01-10 16:20:00", 500, 1.5, 690, 0, "BookTrade"),
        _stock("TLRY", "222", "tlry_sale", "SELL", "C",
               "2025-12-10 10:00:00", 50, 16, 690, 110),
    ]
    lots = [_lot("TLRY", "222", "2025-01-10 16:20:00", "2025-12-10 10:00:00",
                 50, 690, 110)]
    actions = _split("TLRY", "2025-12-01 20:25:00", "111", -500, "222", 50,
                     "TLRY(US88688T1007) REVERSE SPLIT (TLRY, US88688T2088)",
                     "RS", "R2")
    report = calculate(trades, lots, actions)

    sale, = _stock_rows(report, "TLRY")
    assert not sale.get("stillhalter_adjusted"), "keine stille Korrektur"
    assert_close(sale["fifoPnlRealized"], 110, label="PnL unverändert")
    dropped = report["audit"]["stillhalter_corrections_dropped"]
    assert [d["reason"] for d in dropped] == ["stock_split_ratio_unknown"]
    assert_close(dropped[0]["leftover_raw"], 60, label="betroffene Prämie")
    adj = _only_adjustment(report)
    assert adj["status"] == "unbelegt" and adj["split_factor"] is None
    print("  OK  Split ohne belegtes Verhältnis: Prüffall statt Korrektur")


def test_reverse_split_without_corporate_actions_is_review_case():
    """Ohne CorporateActions-Sektion liegt die Lot-Basis je Stück über dem
    Strike (690 / 50 = 13,80 > 1,50). Das darf nicht still übergangen werden.
    """
    trades = _put_pair("TLRY", "2025-01-02", "2025-01-10 16:20:00", 5, 0.12,
                       1.5)
    trades += [
        _stock("TLRY", "111", "tlry_assign", "BUY", "O",
               "2025-01-10 16:20:00", 500, 1.5, 690, 0, "BookTrade"),
        _stock("TLRY", "222", "tlry_sale", "SELL", "C",
               "2025-12-10 10:00:00", 50, 16, 690, 110),
    ]
    lots = [_lot("TLRY", "222", "2025-01-10 16:20:00", "2025-12-10 10:00:00",
                 50, 690, 110)]
    report = calculate(trades, lots)

    sale, = _stock_rows(report, "TLRY")
    assert not sale.get("stillhalter_adjusted")
    dropped = report["audit"]["stillhalter_corrections_dropped"]
    assert [d["reason"] for d in dropped] == ["put_lot_basis_above_strike"]
    assert not report["audit"].get("stock_split_adjustments")
    print("  OK  Reverse Split ohne Kapitalmaßnahmen-Daten: Prüffall "
          "'Basis über dem Strike'")


def test_anlage_so_lot_premium_uses_assignment_shares_after_split():
    """Anlage-SO-Override: Prämie je Lot nach Andienungs-Stücken verteilen.

    100 Stück angedient (Prämie 200), Split 2:1, Verkauf von 100 der 200
    Lot-Stücke: auf das Lot entfällt die Hälfte der Prämie (100), nicht
    100 / 100 = alles.
    """
    isin = "US0000000701"
    trades = _put_pair("GSO", "2025-02-03", "2025-02-21 16:20:00", 1, 2.0, 100)
    trades += [
        _stock("GSO", "701", "gso_assign", "BUY", "O", "2025-02-21 16:20:00",
               100, 100, 9800, 0, "BookTrade", isin, True),
        _stock("GSO", "701", "gso_sale", "SELL", "C", "2025-10-01 10:00:00",
               100, 55, 4900, 600, isin=isin, is_fund=True),
    ]
    lots = [_lot("GSO", "701", "2025-02-21 16:20:00", "2025-10-01 10:00:00",
                 100, 4900, 600, isin, True)]
    actions = [row for row in _split(
        "GSO", "2025-06-02 20:25:00", "701", 100, None, 0,
        "GSO(US0000000701) SPLIT 2 FOR 1 (GSO, GSO TRUST, US0000000701)",
        "FS", "G1") if row["levelOfDetail"] == "DETAIL"]
    instruments = [{"symbol": "GSO", "isin": isin, "assetCategory": "STK",
                    "subCategory": "ETF", "description": "GSO"}]
    report = calculate(trades, lots, actions, instruments,
                       anlage_so_overrides=[isin])

    so = report["anlage_so"]
    detail, = so["details"]
    assert_close(detail["pnl_eur"], 500, label="SO-Lot: 600 - halbe Prämie")
    assert_close(so["taxable_gain"], 500, label="SO steuerpflichtig")
    print("  OK  Anlage SO nach Split 2:1: halbe Prämie je Lot (600 → 500)")


def _split_between_two_sales(currency="EUR"):
    """Vorjahres-Andienung (100 Stück, Prämie 200), 50 Stück vor und 100 Stück
    nach einem Split 2:1 verkauft. Beide Verkäufe tragen je 100 Prämie.

    USD-Variante: Kauf zu 0,9, Verkauf A zu 0,8, Verkauf B zu 0,7.
    """
    fx = {"2024-05-01": 0.9, "2024-06-21": 0.9, "2025-02-10": 0.8,
          "2025-05-10": 0.7} if currency == "USD" else {}
    rate = (lambda day: fx[day]) if fx else (lambda day: 1.0)
    trades = _put_pair("ORD", "2024-05-01", "2024-06-21 16:20:00", 1, 2.0, 100,
                       currency=currency, fx=rate("2024-06-21"))
    trades.append(_stock("ORD", "801", "ord_assign", "BUY", "O",
                         "2024-06-21 16:20:00", 100, 100, 9800, 0, "BookTrade",
                         currency=currency, fx=rate("2024-06-21")))
    sale_a = _stock("ORD", "801", "ord_sale_a", "SELL", "C",
                    "2025-02-10 10:00:00", 50, 90, 4900, -400,
                    currency=currency, fx=rate("2025-02-10"))
    sale_b = _stock("ORD", "802", "ord_sale_b", "SELL", "C",
                    "2025-05-10 10:00:00", 100, 52, 4900, 300,
                    currency=currency, fx=rate("2025-05-10"))
    lot_a = _lot("ORD", "801", "2024-06-21 16:20:00", "2025-02-10 10:00:00",
                 50, 4900, -400, currency=currency, fx=rate("2025-02-10"))
    lot_b = _lot("ORD", "802", "2024-06-21 16:20:00", "2025-05-10 10:00:00",
                 100, 4900, 300, currency=currency, fx=rate("2025-05-10"))
    actions = _split("ORD", "2025-03-03 20:25:00", "801", -50, "802", 100,
                     "ORD(US0000000801) SPLIT 2 FOR 1 (ORD, ORD CORP, "
                     "US0000000802)", "FI", "O1")
    return trades, (sale_a, sale_b), (lot_a, lot_b), actions, fx


def test_cross_year_corrections_follow_lots_not_input_order():
    """Nach dem Split tragen die beiden Lots verschiedene Stückwerte
    (2,00 vor, 1,00 nach dem Split). Die Reihenfolge in trades.csv darf
    nicht entscheiden, welcher Verkauf welchen Wert erhält."""
    trades, sales, lots, actions, _ = _split_between_two_sales()
    for order in (sales, sales[::-1]):
        report = calculate(trades + list(order), list(lots), actions)
        by_day = {row["reportDate"][:10]: row
                  for row in _stock_rows(report, "ORD")}
        assert_close(by_day["2025-02-10"]["stillhalter_adjustment_raw"], 100,
                     label="Verkauf vor dem Split")
        assert_close(by_day["2025-05-10"]["stillhalter_adjustment_raw"], 100,
                     label="Verkauf nach dem Split")
        assert_close(report["stocks_gain_eur"], 200, label="Topf-1-Gewinne")
        assert_close(report["stocks_loss_eur"], -500, label="Topf-1-Verluste")
        corrections = report["audit"]["cross_year_put_corrections"]
        assert all("row_oid" not in c for c in corrections)
    print("  OK  Vorjahres-Korrekturen folgen dem Lot, nicht der "
          "Reihenfolge der Verkäufe")


def test_tageskurs_restore_follows_lots_not_file_order():
    trades, sales, lots, actions, fx = _split_between_two_sales("USD")
    for lot_order in (lots, lots[::-1]):
        report = calculate(trades + list(sales[::-1]), list(lot_order),
                           actions, conversion_rates=fx)
        details = {d["reportDate"]: d
                   for d in report["fx_correction_details"]}
        for day in ("2025-02-10", "2025-05-10"):
            assert_close(details[day]["cost_basis_adjustment_raw"], 100,
                         label=f"Basis-Restore {day}")
        # 4900 + 100 = 5000 USD Basis je Lot: 5000 x (0,8 - 0,9) und
        # 5000 x (0,7 - 0,9).
        assert_close(report["fx_correction_total"], -1500,
                     label="Tageskurs gesamt")
    print("  OK  Tageskurs-Restore folgt dem Lot, nicht der Dateireihenfolge")


def test_history_corporate_actions_reach_the_split_index():
    """Andienung und Split im Vorjahr, Verkauf im Steuerjahr. Der Split steht
    nur im Vorjahres-Export und muss über die Multi-XML-Extraktion ankommen.
    """
    from extract_ibkr_data import extract_fx_multi_xml

    def xml(path, from_date, to_date, body):
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(f"""<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse><FlexStatements count="1">
<FlexStatement accountId="U1" fromDate="{from_date}" toDate="{to_date}">
<AccountInformation accountId="U1" currency="EUR" />
{body}
</FlexStatement></FlexStatements></FlexQueryResponse>""")
        return path

    def trade(**fields):
        base = {"levelOfDetail": "EXECUTION", "currency": "EUR",
                "fxRateToBase": "1", "ibCommission": "0"}
        base.update(fields)
        return "<Trade " + " ".join(
            f'{key}="{value}"' for key, value in base.items()) + " />"

    option = dict(assetCategory="OPT", putCall="P", strike="60",
                  expiry="2024-06-21", underlyingSymbol="HXY",
                  symbol="HXY 240621P60", conid="990", multiplier="100")
    history = f"""<Trades>
{trade(tradeID="h1", transactionType="ExchTrade", buySell="SELL",
       quantity="-1", tradePrice="1.5", dateTime="2024-05-01 10:00:00",
       tradeDate="2024-05-01", reportDate="2024-05-01",
       fifoPnlRealized="0", **option)}
{trade(tradeID="h2", transactionType="BookTrade", buySell="BUY",
       quantity="1", tradePrice="0", dateTime="2024-06-21 16:20:00",
       tradeDate="2024-06-21", reportDate="2024-06-21",
       fifoPnlRealized="0", **option)}
{trade(tradeID="h3", assetCategory="STK", symbol="HXY", conid="901",
       transactionType="BookTrade", buySell="BUY", openCloseIndicator="O",
       quantity="100", tradePrice="60", cost="5850", proceeds="-6000",
       dateTime="2024-06-21 16:20:00", tradeDate="2024-06-21",
       reportDate="2024-06-21", fifoPnlRealized="0")}
</Trades>
<CorporateActions>
<CorporateAction assetCategory="STK" levelOfDetail="DETAIL" type="FI"
  actionID="A9" transactionID="T1" conid="901" symbol="HXY.OLD"
  quantity="-100" dateTime="2024-09-03 20:25:00" reportDate="2024-09-04"
  description="HXY(US0000000901) SPLIT 3 FOR 1 (HXY.OLD, HXY CORP, US0000000901)" />
<CorporateAction assetCategory="STK" levelOfDetail="DETAIL" type="FI"
  actionID="A9" transactionID="T2" conid="902" symbol="HXY"
  quantity="300" dateTime="2024-09-03 20:25:00" reportDate="2024-09-04"
  description="HXY(US0000000901) SPLIT 3 FOR 1 (HXY, HXY CORP, US0000000902)" />
</CorporateActions>"""
    main = f"""<Trades>
{trade(tradeID="m1", assetCategory="STK", symbol="HXY", conid="902",
       transactionType="ExchTrade", buySell="SELL", openCloseIndicator="C",
       quantity="-300", tradePrice="25", cost="5850", proceeds="7500",
       dateTime="2025-04-10 10:00:00", tradeDate="2025-04-10",
       reportDate="2025-04-10", fifoPnlRealized="1650")}
<Lot levelOfDetail="CLOSED_LOT" assetCategory="STK" symbol="HXY" conid="902"
  currency="EUR" fxRateToBase="1" quantity="300" buySell="SELL" cost="5850"
  fifoPnlRealized="1650" openDateTime="2024-06-21 16:20:00"
  dateTime="2025-04-10 10:00:00" tradeDate="2025-04-10"
  reportDate="2025-04-10" />
</Trades>"""
    with tempfile.TemporaryDirectory() as tmp:
        paths = [xml(os.path.join(tmp, "h.xml"), "2024-01-01", "2024-12-31",
                     history),
                 xml(os.path.join(tmp, "m.xml"), "2025-01-01", "2025-12-31",
                     main)]
        out = os.path.join(tmp, "out")
        os.mkdir(out)
        with contextlib.redirect_stdout(io.StringIO()):
            extract_fx_multi_xml(paths, out)
            extract_fx_multi_xml(paths + [paths[0]], out)  # Wiederholung
            report = calculate_tax(out)
        with open(os.path.join(out, "corporate_actions.csv"),
                  encoding="utf-8") as handle:
            actions = list(csv.DictReader(handle))
    assert len(actions) == 2, "Vorjahres-Split genau einmal übernommen"
    sale, = _stock_rows(report, "HXY")
    assert_close(sale["cost"], 6000, label="Basis zum Strike")
    assert_close(sale["fifoPnlRealized"], 1500, label="Aktien-PnL")
    assert _only_adjustment(report)["source"] == "cross_year"
    print("  OK  Split im Vorjahres-Export erreicht die Berechnung "
          "(Multi-XML-Extraktion)")


# ---------------------------------------------------------------------------
# Call-Andienung mit Short-Eröffnung
# ---------------------------------------------------------------------------

def _call_short_case(with_lots, text="SPC(US0000000601) SPLIT 2 FOR 1 "
                                    "(SPC, SPC CORP, US0000000602)"):
    """Call-Andienung eröffnet 100 Stück short; Split 2:1; Cover 200 Stück.

    Prämie 100, IBKR-Basis des Shorts −(5000 + 100) = −5100, Cover zu 24:
    PnL 5100 − 4800 = 300, davon 100 Prämie.
    """
    expiry = "2025-03-21"
    trades = [
        _option("SPC", "SELL", "2025-03-03 10:00:00", 1, 1.0, 50, expiry, "C",
                "ExchTrade"),
        _option("SPC", "BUY", f"{expiry} 16:20:00", 1, 0, 50, expiry, "C",
                "BookTrade"),
        _stock("SPC", "601", "spc_short_open", "SELL", "O",
               f"{expiry} 16:20:00", 100, 50, -5100, 0, "BookTrade"),
        _stock("SPC", "602", "spc_cover", "BUY", "C", "2025-09-15 10:00:00",
               200, 24, -5100, 300),
    ]
    lots = [_lot("SPC", "602", f"{expiry} 16:20:00", "2025-09-15 10:00:00",
                 -200, -5100, 300)] if with_lots else None
    actions = _split("SPC", "2025-06-02 20:25:00", "601", 100, "602", -200,
                     text, "FI", "S1")
    return calculate(trades, lots, actions)


def test_call_cover_with_unknown_ratio_is_review_not_open_short():
    """Cover nach einem Split ohne belegtes Verhältnis: der Short ist
    geschlossen, darf also weder als offener Short gelten noch still
    doppelt versteuert werden."""
    for with_lots in (True, False):
        report = _call_short_case(with_lots, text="SPC REVERSE SPLIT")
        label = "mit Lots" if with_lots else "ohne Lots"
        reasons = [d["reason"] for d in
                   report["audit"]["stillhalter_corrections_dropped"]]
        assert reasons == ["stock_split_ratio_unknown"], (label, reasons)
        assert not report["audit"].get("stillhalter_open_short"), label
    print("  OK  Call-Cover mit unbelegtem Split-Verhältnis: Prüffall, "
          "kein offener Short")


def test_call_short_cover_after_split_with_and_without_lots():
    for with_lots in (True, False):
        report = _call_short_case(with_lots)
        label = "mit Lots" if with_lots else "ohne Lots"
        adjusted = [row for row in _stock_rows(report, "SPC")
                    if row.get("stillhalter_adjusted")]
        assert len(adjusted) == 1, (label, adjusted)
        assert adjusted[0]["buySell"] == "BUY"
        assert_close(adjusted[0]["fifoPnlRealized"], 200,
                     label=f"Cover-PnL {label}")
        assert_close(report["stocks_gain_eur"], 200, label=f"Topf 1 {label}")
        assert_close(report["options_gain_eur"], 100, label=f"Topf 2 {label}")
        assert report["audit"].get("stillhalter_corrections_dropped", []) == []
        adj = _only_adjustment(report)
        assert adj["putCall"] == "C"
        assert_close(adj["split_factor"], 2.0, label=f"Faktor {label}")
        assert_close(adj["lot_shares"], 200, label=f"Lot-Stücke {label}")
    print("  OK  Call-Short über Split 2:1: Cover 300 → 200, mit und ohne "
          "Lots")


def test_cross_year_reverse_split_without_corporate_actions_is_review_case():
    """Review-Befund: Andienung 2024, Reverse Split 1:10 ohne
    Kapitalmaßnahmen-Daten, Verkauf 2025. Der Vorjahres-Pfad meldet die
    Basis über dem Strike wie der Pfad des laufenden Jahres, statt die
    Prämie still im Aktienergebnis zu lassen."""
    trades = _put_pair("TLRY", "2024-01-02", "2024-01-10 16:20:00", 5, 0.12,
                       1.5)
    trades += [
        _stock("TLRY", "111", "tlry_assign", "BUY", "O",
               "2024-01-10 16:20:00", 500, 1.5, 690, 0, "BookTrade"),
        _stock("TLRY", "222", "tlry_sale", "SELL", "C",
               "2025-12-10 10:00:00", 50, 16, 690, 110),
    ]
    lots = [_lot("TLRY", "222", "2024-01-10 16:20:00", "2025-12-10 10:00:00",
                 50, 690, 110)]
    report = calculate(trades, lots)

    sale, = _stock_rows(report, "TLRY")
    assert not sale.get("stillhalter_adjusted")
    dropped = report["audit"]["stillhalter_corrections_dropped"]
    assert [d["reason"] for d in dropped] == ["put_lot_basis_above_strike"]
    assert dropped[0]["assignment_date"] == "2024-01-10"
    print("  OK  Vorjahres-Andienung, Reverse Split ohne Kapitalmaßnahmen: "
          "Prüffall")


def test_above_strike_review_skips_short_lots_and_basis_at_strike():
    """Short-Lots tragen in 'cost' den Verkaufserlös; Basis zum Strike ist
    ein bekanntes IBKR-Muster. Beides bleibt ohne Prüffall."""
    det = {"strike": "50", "assignment_date": "2025-03-21 16:20:00"}
    long_above = {"shares": 100, "cost": 5600, "open_date": "2025-03-21"}
    item = _put_lot_above_strike_review(det, "ABC", long_above, 1.0)
    assert item["reason"] == "put_lot_basis_above_strike"
    assert item["assignment_date"] == "2025-03-21"
    assert_close(item["leftover_raw"], 100, label="Prämie des Lots")
    short_above = dict(long_above, is_short_lot=True)
    assert _put_lot_above_strike_review(det, "ABC", short_above, 1.0) is None
    at_strike = dict(long_above, cost=5000)
    assert _put_lot_above_strike_review(det, "ABC", at_strike, 1.0) is None
    print("  OK  Prüffall 'Basis über dem Strike' nur für Long-Lots")


def test_split_chain_without_action_id_links_rows_by_security_identity():
    """Codex-Befund: Ohne actionID verbindet die Wertpapier-Kennung am
    Anfang der Beschreibung alte und neue conid eines Splits; ohne Kennung
    bleibt die Kette unbelegt (Prüffall statt Teilfaktor)."""
    def chain(text_ab, text_bc):
        return (_split("CH", "2025-03-01 20:25:00", "A", -100, "B", 200,
                       text_ab, "FI", "")
                + _split("CH", "2025-06-01 20:25:00", "B", -200, "C", 600,
                         text_bc, "FI", ""))

    index = _build_stock_split_index(chain(
        "CH(US0000000001) SPLIT 2 FOR 1 (CH, CH CORP, US0000000002)",
        "CH(US0000000002) SPLIT 3 FOR 1 (CH, CH CORP, US0000000003)"))
    factor, events = _split_factor_between(
        "C", "2025-01-10 16:20:00", "2025-09-01 10:00:00", index)
    assert_close(factor, 6.0, label="Kette ohne actionID")
    assert len(events) == 2
    index = _build_stock_split_index(chain("CH SPLIT 2 FOR 1",
                                           "CH SPLIT 3 FOR 1"))
    factor, events = _split_factor_between(
        "C", "2025-01-10 16:20:00", "2025-09-01 10:00:00", index)
    assert factor is None
    assert events[0]["issue"] == "event_unlinked"
    print("  OK  Split-Kette ohne actionID: über die Kennung verbunden, "
          "sonst Prüffall")


def test_split_chain_without_action_id_corrects_with_full_factor():
    """Gegenfall aus dem Codex-Review: 100 Stück angedient (Prämie 200),
    A→B 2:1 und B→C 3:1 ohne actionIDs, Verkauf von 300 der 600 Stück.
    300 Lot-Stücke = 50 angediente Stücke → Korrektur 100 (nicht 200)."""
    trades = _put_pair("CHN", "2025-01-02", "2025-01-17 16:20:00", 1, 2.0, 50)
    trades += [
        _stock("CHN", "A", "chn_assign", "BUY", "O", "2025-01-17 16:20:00",
               100, 50, 4800, 0, "BookTrade"),
        _stock("CHN", "C", "chn_sale", "SELL", "C", "2025-09-15 10:00:00",
               300, 10, 2400, 600),
    ]
    lots = [_lot("CHN", "C", "2025-01-17 16:20:00", "2025-09-15 10:00:00",
                 300, 2400, 600)]
    actions = (
        _split("CHN", "2025-03-03 20:25:00", "A", -100, "B", 200,
               "CHN(US0000000011) SPLIT 2 FOR 1 (CHN, CHN CORP, "
               "US0000000012)", "FI", "")
        + _split("CHN", "2025-06-02 20:25:00", "B", -200, "C", 600,
                 "CHN(US0000000012) SPLIT 3 FOR 1 (CHN, CHN CORP, "
                 "US0000000013)", "FI", ""))
    report = calculate(trades, lots, actions)

    sale, = _stock_rows(report, "CHN")
    assert_close(sale["fifoPnlRealized"], 500, label="Aktien-PnL")
    assert_close(sale["cost"], 2500, label="Basis")
    adj = _only_adjustment(report)
    assert_close(adj["split_factor"], 6.0, label="Faktor")
    assert not report["audit"].get("stillhalter_corrections_dropped")
    print("  OK  Split-Kette ohne actionID: Faktor 6, Korrektur 100")


def test_partial_year_exports_keep_split_chain_without_transaction_ids():
    """Codex-Befund: Der Quartals-Merge deduplizierte CorporateActions über
    (transactionID, dateTime). Ohne transactionID fielen alte und neue conid
    desselben Splits zusammen, die Kette A→B→C riss (Faktor 3 statt 6) und
    die volle Prämie wurde abgezogen. Jahres- und Teiljahres-Export müssen
    dieselben Zeilen und dieselben Steuerwerte liefern."""
    from xml.sax.saxutils import quoteattr

    from extract_ibkr_data import extract_quarterly_xmls, parse_ibkr_xml

    def element(tag, row):
        return f"<{tag} " + " ".join(
            f"{key}={quoteattr(str(value))}" for key, value in row.items()
            if key != "transactionID" or value) + " />"

    def xml(path, from_date, to_date, trades, lots, actions):
        body = "\n".join(
            [element("Trade", dict(row, levelOfDetail="EXECUTION"))
             for row in trades]
            + [element("Lot", dict(row, levelOfDetail="CLOSED_LOT"))
               for row in lots])
        ca = "\n".join(element("CorporateAction", row) for row in actions)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(f"""<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse><FlexStatements count="1">
<FlexStatement accountId="U1" fromDate="{from_date}" toDate="{to_date}">
<AccountInformation accountId="U1" currency="EUR" />
<Trades>{body}</Trades>
<CorporateActions>{ca}</CorporateActions>
</FlexStatement></FlexStatements></FlexQueryResponse>""")
        return path

    first_half = _put_pair("CHN", "2025-01-02", "2025-01-17 16:20:00", 1,
                           2.0, 50)
    first_half.append(_stock("CHN", "A", "chn_assign", "BUY", "O",
                             "2025-01-17 16:20:00", 100, 50, 4800, 0,
                             "BookTrade"))
    second_half = [_stock("CHN", "C", "chn_sale", "SELL", "C",
                          "2025-09-15 10:00:00", 300, 10, 2400, 600)]
    lots = [_lot("CHN", "C", "2025-01-17 16:20:00", "2025-09-15 10:00:00",
                 300, 2400, 600)]
    actions = (
        _split("CHN", "2025-03-03 20:25:00", "A", -100, "B", 200,
               "CHN(US0000000011) SPLIT 2 FOR 1 (CHN, CHN CORP, "
               "US0000000012)", "FI", "")
        + _split("CHN", "2025-06-02 20:25:00", "B", -200, "C", 600,
                 "CHN(US0000000012) SPLIT 3 FOR 1 (CHN, CHN CORP, "
                 "US0000000013)", "FI", ""))

    def run(extract):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out")
            os.mkdir(out)
            with contextlib.redirect_stdout(io.StringIO()):
                extract(tmp, out)
                report = calculate_tax(out)
            with open(os.path.join(out, "corporate_actions.csv"),
                      encoding="utf-8") as handle:
                rows = sorted(
                    (row["conid"], row["quantity"], row["levelOfDetail"],
                     row["dateTime"]) for row in csv.DictReader(handle))
        return report, rows

    def annual(tmp, out):
        parse_ibkr_xml(xml(os.path.join(tmp, "y.xml"), "2025-01-01",
                           "2025-12-31", first_half + second_half, lots,
                           actions), out)

    def halves(tmp, out):
        extract_quarterly_xmls([
            xml(os.path.join(tmp, "h1.xml"), "2025-01-01", "2025-06-30",
                first_half, [], actions),
            xml(os.path.join(tmp, "h2.xml"), "2025-07-01", "2025-12-31",
                second_half, lots, []),
        ], out)

    year_report, year_rows = run(annual)
    half_report, half_rows = run(halves)
    assert len(year_rows) == 6, year_rows
    assert half_rows == year_rows, (half_rows, year_rows)
    for label, report in (("Jahr", year_report), ("Teiljahre", half_report)):
        sale, = _stock_rows(report, "CHN")
        assert_close(sale["fifoPnlRealized"], 500, label=f"{label}: PnL")
        assert_close(_only_adjustment(report)["split_factor"], 6.0,
                     label=f"{label}: Faktor")
        assert not report["audit"].get("stillhalter_corrections_dropped")
    year_final = build_final_values(year_report, default_toggles())
    half_final = build_final_values(half_report, default_toggles())
    for field in ("zeile_19", "zeile_20", "zeile_22", "zeile_23"):
        assert_close(half_final[field], year_final[field], label=field)
    print("  OK  Teiljahres-Exporte behalten die Split-Kette ohne "
          "transactionID (gleiche Werte wie der Jahresexport)")


def test_anlage_so_cross_year_partial_sale_uses_full_assignment():
    """Codex-Befund: Im Vorjahres-Lookup standen die gesamte Prämie und nur
    die verkauften Stücke. Andienung Dezember 2024 (100 Stück, Prämie 200),
    Split 2:1, Verkauf von 100 der 200 Stück 2025: Anlage SO und Trade-Zeile
    ziehen beide 100 ab (600 → 500)."""
    isin = "US0000000801"
    trades = _put_pair("GSX", "2024-12-02", "2024-12-20 16:20:00", 1, 2.0,
                       100)
    trades += [
        _stock("GSX", "801", "gsx_assign", "BUY", "O", "2024-12-20 16:20:00",
               100, 100, 9800, 0, "BookTrade", isin, True),
        _stock("GSX", "801", "gsx_sale", "SELL", "C", "2025-10-01 10:00:00",
               100, 55, 4900, 600, isin=isin, is_fund=True),
    ]
    lots = [_lot("GSX", "801", "2024-12-20 16:20:00", "2025-10-01 10:00:00",
                 100, 4900, 600, isin, True)]
    actions = [row for row in _split(
        "GSX", "2025-06-02 20:25:00", "801", 100, None, 0,
        "GSX(US0000000801) SPLIT 2 FOR 1 (GSX, GSX TRUST, US0000000801)",
        "FS", "") if row["levelOfDetail"] == "DETAIL"]
    instruments = [{"symbol": "GSX", "isin": isin, "assetCategory": "STK",
                    "subCategory": "ETF", "description": "GSX"}]
    report = calculate(trades, lots, actions, instruments,
                       anlage_so_overrides=[isin])

    detail, = report["anlage_so"]["details"]
    assert_close(detail["pnl_eur"], 500, label="SO-Lot")
    sale, = _stock_rows(report, "GSX")
    assert_close(sale["fifoPnlRealized"], 500, label="Trade-Zeile")
    print("  OK  Anlage SO, Vorjahres-Andienung mit Teilverkauf: 600 → 500 "
          "wie die Trade-Zeile")


def test_split_with_new_symbol_links_assignment_and_sale():
    """Codex-Befund (Runde 4): Split 2:1 mit vollständigem Identitätswechsel
    (OLD/conid 401 → NEW/conid 402, neue ISIN, dieselbe actionID). Put-
    Andienung 100 OLD zum Strike 100 EUR, Prämie 200 EUR (IBKR-Basis 9.800),
    Verkauf aller 200 NEW zu 55 EUR. Ohne Verbindung über das Split-Ereignis
    fand die Andienung das Lot nicht: Prämie doppelt (Zeile 19 1.400 statt
    1.200, Zeile 20 1.200 statt 1.000), ohne Prüffall."""
    trades = _put_pair("OLD", "2025-01-02", "2025-01-10 16:20:00", 1, 2.0,
                       100)
    trades += [
        _stock("OLD", "401", "old_assign", "BUY", "O", "2025-01-10 16:20:00",
               100, 100, 9800, 0, "BookTrade", isin="US0000000401"),
        _stock("NEW", "402", "new_sale", "SELL", "C", "2025-12-10 10:00:00",
               200, 55, 9800, 1200, isin="US0000000402"),
    ]
    lots = [_lot("NEW", "402", "2025-01-10 16:20:00", "2025-12-10 10:00:00",
                 200, 9800, 1200, isin="US0000000402")]
    text = "OLD(US0000000401) SPLIT 2 FOR 1 (NEW, NEW CORP, US0000000402)"
    actions = []
    for symbol, conid, qty in (("OLD", "401", -100), ("NEW", "402", 200)):
        actions.append({
            "assetCategory": "STK", "levelOfDetail": "DETAIL", "type": "FI",
            "actionID": "N1", "actionDescription": text, "description": text,
            "conid": conid, "symbol": symbol, "quantity": str(qty),
            "dateTime": "2025-06-02 20:25:00", "reportDate": "2025-06-02",
        })
    report = calculate(trades, lots, actions)

    sale, = _stock_rows(report, "NEW")
    assert sale.get("stillhalter_adjusted")
    assert_close(sale["stillhalter_adjustment_raw"], 200, label="Korrektur")
    assert_close(sale["cost"], 10000, label="Basis zum Strike")
    assert_close(report["audit"]["put_nosell_premium_eur"], 0,
                 label="kein unverkaufter Rest")
    assert report["audit"].get("stillhalter_corrections_dropped", []) == []
    adj = _only_adjustment(report)
    assert adj["status"] == "umgerechnet"
    assert_close(adj["split_factor"], 2.0, label="Faktor")
    final = build_final_values(report, default_toggles())
    assert_close(final["zeile_19"], 1200, label="Zeile 19")
    assert_close(final["zeile_20"], 1000, label="Zeile 20")
    print("  OK  Split mit Symbol-, conid- und ISIN-Wechsel: Andienung "
          "findet das Lot unter dem neuen Symbol")


def test_anlage_so_cross_year_lookup_uses_assignment_day_not_booking_day():
    """Steuerlogik-Review: Vorjahres-Andienung Freitag 20.12.2024, Buchung
    Montag 23.12. Der Anlage-SO-Teil sucht über das openDateTime des Lots
    (Andienungstag); stand im Lookup der Buchungstag, blieb die Prämie im
    §23-Gewinn (700 statt 500), obwohl sie im Vorjahr versteuert ist."""
    isin = "US0000000801"
    trades = _put_pair("GSX", "2024-12-02", "2024-12-20 16:20:00", 1, 2.0,
                       100)
    trades[1]["reportDate"] = "2024-12-23"
    assign = _stock("GSX", "801", "gsx_assign", "BUY", "O",
                    "2024-12-20 16:20:00", 100, 100, 9800, 0, "BookTrade",
                    isin, True)
    assign["reportDate"] = "2024-12-23"
    trades += [assign,
               _stock("GSX", "801", "gsx_sale", "SELL", "C",
                      "2025-10-01 10:00:00", 100, 105, 9800, 700, isin=isin,
                      is_fund=True)]
    lots = [_lot("GSX", "801", "2024-12-20 16:20:00", "2025-10-01 10:00:00",
                 100, 9800, 700, isin, True)]
    instruments = [{"symbol": "GSX", "isin": isin, "assetCategory": "STK",
                    "subCategory": "ETF", "description": "GSX"}]
    report = calculate(trades, lots, None, instruments,
                       anlage_so_overrides=[isin])

    detail, = report["anlage_so"]["details"]
    assert_close(detail["pnl_eur"], 500, label="SO-Lot")
    sale, = _stock_rows(report, "GSX")
    assert_close(sale["fifoPnlRealized"], 500, label="Trade-Zeile")
    print("  OK  Anlage SO, Vorjahres-Andienung mit Buchung am Montag: "
          "Lookup über den Andienungstag (700 → 500)")


def test_split_identity_accepts_symbols_with_spaces():
    """Steuerlogik-Review: 'BRK B(US0846707026) SPLIT …' ohne actionID darf
    nicht als unverbundenes Ereignis zum Prüffall werden."""
    rows = [row for row in _split(
        "BRK B", "2025-06-02 20:25:00", "72063691", -100, "72063692", 200,
        "BRK B(US0846707026) SPLIT 2 FOR 1 (BRK B, BERKSHIRE HATHAWAY INC-CL B, "
        "US0846707026)", "FS", "") if row["levelOfDetail"] == "DETAIL"]
    index = _build_stock_split_index(rows)
    event = index["72063692"][0]
    assert event["issue"] == "", event
    assert_close(event["factor"], 2.0, label="Faktor")
    assert event is index["72063691"][0]
    print("  OK  Split-Kennung ohne actionID auch bei Symbolen mit Leerzeichen")


def test_split_link_ignores_reused_old_ticker():
    """Codex-Befund (Runde 5): Split im Januar OLD/conid 111 → NEW/conid
    222; conid 111 steht nur in CorporateActions. Im September heißt eine
    andere Aktie (conid 333) OLD. Ein Put darauf liefert 100 Stück (Prämie
    200 EUR, bleiben offen). Am selben Tag wird NEW unabhängig für 9.800
    gekauft und später für 10.500 verkauft. Die Split-Verbindung darf das
    wiederverwendete OLD nicht mit NEW verbinden: Zeile 19/20 = 900/700,
    nicht 700/500."""
    trades = _put_pair("OLD", "2025-09-01", "2025-09-12 16:20:00", 1, 2.0,
                       100)
    trades += [
        _stock("OLD", "333", "old_assign", "BUY", "O", "2025-09-12 16:20:00",
               100, 100, 9800, 0, "BookTrade", isin="US0000000333"),
        _stock("NEW", "222", "new_buy", "BUY", "O", "2025-09-12 10:00:00",
               100, 98, 9800, 0, isin="US0000000222"),
        _stock("NEW", "222", "new_sale", "SELL", "C", "2025-11-03 10:00:00",
               100, 105, 9800, 700, isin="US0000000222"),
    ]
    lots = [_lot("NEW", "222", "2025-09-12 10:00:00", "2025-11-03 10:00:00",
                 100, 9800, 700, isin="US0000000222")]
    text = "OLD(US0000000111) SPLIT 2 FOR 1 (NEW, NEW CORP, US0000000222)"
    actions = []
    for symbol, conid, qty in (("OLD", "111", -50), ("NEW", "222", 100)):
        actions.append({
            "assetCategory": "STK", "levelOfDetail": "DETAIL", "type": "FI",
            "actionID": "R1", "actionDescription": text, "description": text,
            "conid": conid, "symbol": symbol, "quantity": str(qty),
            "dateTime": "2025-01-15 20:25:00", "reportDate": "2025-01-15",
        })
    report = calculate(trades, lots, actions)

    sale, = _stock_rows(report, "NEW")
    assert not sale.get("stillhalter_adjusted"), sale
    assert_close(sale["fifoPnlRealized"], 700, label="NEW-Gewinn")
    final = build_final_values(report, default_toggles())
    assert_close(final["zeile_19"], 900, label="Zeile 19")
    assert_close(final["zeile_20"], 700, label="Zeile 20")
    print("  OK  Split-Verbindung übergeht ein später wiederverwendetes "
          "altes Symbol")


def test_split_chain_with_symbol_change_then_new_conid_stays_linked():
    """Codex-Befund (Runde 6): Put-Andienung 100 OLD/401 (Prämie 200 EUR,
    IBKR-Basis 9.800), dann zwei 2:1-Splits OLD/401 → NEW/402 (Symbol-
    wechsel) und NEW/402 → NEW/403 (nur neue conid), Verkauf der 400 NEW
    für 11.000. Die conid 402 steht nur in CorporateActions; sie darf NEW
    nicht als wiederverwendetes Symbol erscheinen lassen, denn beide
    Ereignisse belegen dieselbe Aktie. Sonst Prämie doppelt: Zeile 19/20
    1.400/1.200 statt 1.200/1.000."""
    trades = _put_pair("OLD", "2025-01-02", "2025-01-10 16:20:00", 1, 2.0,
                       100)
    trades += [
        _stock("OLD", "401", "old_assign", "BUY", "O", "2025-01-10 16:20:00",
               100, 100, 9800, 0, "BookTrade", isin="US0000000401"),
        _stock("NEW", "403", "new_sale", "SELL", "C", "2025-12-10 10:00:00",
               400, 27.5, 9800, 1200, isin="US0000000403"),
    ]
    lots = [_lot("NEW", "403", "2025-01-10 16:20:00", "2025-12-10 10:00:00",
                 400, 9800, 1200, isin="US0000000403")]
    actions = []
    for action_id, date, text, legs in (
            ("N1", "2025-04-01",
             "OLD(US0000000401) SPLIT 2 FOR 1 (NEW, NEW CORP, US0000000402)",
             (("OLD", "401", -100), ("NEW", "402", 200))),
            ("N2", "2025-08-01",
             "NEW(US0000000402) SPLIT 2 FOR 1 (NEW, NEW CORP, US0000000403)",
             (("NEW", "402", -200), ("NEW", "403", 400)))):
        for symbol, conid, qty in legs:
            actions.append({
                "assetCategory": "STK", "levelOfDetail": "DETAIL",
                "type": "FI", "actionID": action_id,
                "actionDescription": text, "description": text,
                "conid": conid, "symbol": symbol, "quantity": str(qty),
                "dateTime": f"{date} 20:25:00", "reportDate": date,
            })
    report = calculate(trades, lots, actions)

    sale, = _stock_rows(report, "NEW")
    assert sale.get("stillhalter_adjusted"), sale
    assert_close(sale["stillhalter_adjustment_raw"], 200, label="Korrektur")
    assert_close(sale["cost"], 10000, label="Basis zum Strike")
    assert report["audit"].get("stillhalter_corrections_dropped", []) == []
    adj = _only_adjustment(report)
    assert adj["status"] == "umgerechnet"
    assert_close(adj["split_factor"], 4.0, label="Faktor der Kette")
    final = build_final_values(report, default_toggles())
    assert_close(final["zeile_19"], 1200, label="Zeile 19")
    assert_close(final["zeile_20"], 1000, label="Zeile 20")
    print("  OK  Split-Kette mit Symbolwechsel und weiterer neuer conid "
          "bleibt verbunden")


def test_anlage_so_cross_year_lookup_follows_lot_open_day():
    """Codex-Befund (Runde 5): Andienung dateTime 20.12.2024 23:59:59,
    Handels- und Buchungstag 21.12.; das gelieferte Lot öffnet am 21.12.
    um 00:00. Der Lot-Matcher akzeptiert den Handelstag; der Anlage-SO-
    Schlüssel muss deshalb dem Eröffnungstag des gematchten Lots folgen
    (500 statt 700)."""
    isin = "US0000000801"
    trades = _put_pair("GSX", "2024-12-02", "2024-12-20 23:59:59", 1, 2.0,
                       100)
    trades[1]["tradeDate"] = trades[1]["reportDate"] = "2024-12-21"
    trades += [
        _stock("GSX", "801", "gsx_assign", "BUY", "O", "2024-12-21 00:00:00",
               100, 100, 9800, 0, "BookTrade", isin, True),
        _stock("GSX", "801", "gsx_sale", "SELL", "C", "2025-10-01 10:00:00",
               100, 105, 9800, 700, isin=isin, is_fund=True),
    ]
    lots = [_lot("GSX", "801", "2024-12-21 00:00:00", "2025-10-01 10:00:00",
                 100, 9800, 700, isin, True)]
    instruments = [{"symbol": "GSX", "isin": isin, "assetCategory": "STK",
                    "subCategory": "ETF", "description": "GSX"}]
    report = calculate(trades, lots, None, instruments,
                       anlage_so_overrides=[isin])

    detail, = report["anlage_so"]["details"]
    assert_close(detail["pnl_eur"], 500, label="SO-Lot")
    sale, = _stock_rows(report, "GSX")
    assert_close(sale["fifoPnlRealized"], 500, label="Trade-Zeile")
    print("  OK  Anlage SO, Vorjahres-Andienung: Lookup über den "
          "Eröffnungstag des gematchten Lots (700 → 500)")


if __name__ == "__main__":
    test_split_index_reads_ratio_from_text_and_tolerates_cash_in_lieu()
    test_split_index_flags_contradicting_quantities_and_missing_ratio()
    test_split_factor_follows_chain_and_respects_time_window()
    test_split_chain_without_action_id_links_rows_by_security_identity()
    test_split_chain_without_action_id_corrects_with_full_factor()
    test_partial_year_exports_keep_split_chain_without_transaction_ids()
    test_reverse_split_same_year_put_restores_full_premium()
    test_forward_split_fund_restores_strike_not_post_split_gap()
    test_forward_split_without_conid_change_and_partial_sale()
    test_cross_year_put_assignment_with_split_before_sale()
    test_split_without_ratio_is_review_case_not_silent_correction()
    test_reverse_split_without_corporate_actions_is_review_case()
    test_cross_year_reverse_split_without_corporate_actions_is_review_case()
    test_above_strike_review_skips_short_lots_and_basis_at_strike()
    test_anlage_so_lot_premium_uses_assignment_shares_after_split()
    test_anlage_so_cross_year_partial_sale_uses_full_assignment()
    test_split_with_new_symbol_links_assignment_and_sale()
    test_anlage_so_cross_year_lookup_uses_assignment_day_not_booking_day()
    test_split_identity_accepts_symbols_with_spaces()
    test_split_link_ignores_reused_old_ticker()
    test_split_chain_with_symbol_change_then_new_conid_stays_linked()
    test_anlage_so_cross_year_lookup_follows_lot_open_day()
    test_cross_year_corrections_follow_lots_not_input_order()
    test_tageskurs_restore_follows_lots_not_file_order()
    test_history_corporate_actions_reach_the_split_index()
    test_call_cover_with_unknown_ratio_is_review_not_open_short()
    test_call_short_cover_after_split_with_and_without_lots()
    print("OK: Issue #98 Aktiensplit zwischen Andienung und Verkauf")
