"""Synthetische Regression-Tests fuer GH Issue #83.

Symbol-Matching Option↔Aktie via conid/ISIN-Aequivalenzklassen: IBKR fuehrt
dieselbe Aktie je nach Row unter verschiedenen Symbolen — Handelsplatz-Suffix
auf der STK-Row ('CONd', Continental AG an deutscher Boerse) vs. Options-
Underlying ('CON'), oder Ticker-Umbenennung im Jahresverlauf (NYCB→FLG,
identische conid). Reines String-Matching verfehlte dann die Stillhalter-
Korrekturen: die Praemie blieb in der Aktien-Kostenbasis eingebettet UND wurde
separat als Stillhalterertrag versteuert (Doppelbesteuerung; Real-Fall
Issue #83: CON 18MAR22 75.0 P, Praemie 108 EUR, Verlust -269,13 statt
korrekt -377,13 EUR).

Aufruf: python tests/test_underlying_symbol_matching.py
"""
import os
import sys
import contextlib
import csv
import io
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (
    _build_underlying_alias_map,
    _canon_symbol,
    _propagate_alias_isins,
    _put_assignment_closed_lot_matches,
    _symbols_equivalent,
    calculate_tax,
)


# ---------------------------------------------------------------------------
# Fixture-Helper
# ---------------------------------------------------------------------------

def make_opt(date, qty, price, pnl="0", transaction_type="ExchTrade",
             buysell="SELL", strike="75", expiry="2022-03-18", pc="P",
             underlying="CON", u_conid="", u_secid="", commission="0",
             conid="", currency="EUR"):
    time = "16:20:00" if transaction_type == "BookTrade" else "10:00:00"
    return {
        "tradeID": f"opt_{transaction_type}_{buysell}_{date}_{qty}",
        "assetCategory": "OPT",
        "transactionType": transaction_type,
        "buySell": buysell,
        "putCall": pc,
        "strike": strike,
        "expiry": expiry,
        "underlyingSymbol": underlying,
        "underlyingConid": u_conid,
        "underlyingSecurityID": u_secid,
        "conid": conid,
        "symbol": f"{underlying} {expiry} {strike} {pc}",
        "quantity": str(qty),
        "tradePrice": str(price),
        "closePrice": str(price),
        "multiplier": "100",
        "ibCommission": commission,
        "fxRateToBase": "1.0",
        "currency": currency,
        "dateTime": f"{date} {time}",
        "tradeDate": date,
        "reportDate": date,
        "fifoPnlRealized": str(pnl),
    }


def make_stk(date, qty, price, pnl, symbol="CONd", conid="", isin="",
             transaction_type="ExchTrade", cost="", currency="EUR"):
    buysell = "SELL" if float(qty) < 0 else "BUY"
    return {
        "tradeID": f"stk_{symbol}_{buysell}_{date}",
        "assetCategory": "STK",
        "transactionType": transaction_type,
        "buySell": buysell,
        "putCall": "",
        "strike": "",
        "expiry": "",
        "underlyingSymbol": "",
        "underlyingConid": "",
        "underlyingSecurityID": "",
        "symbol": symbol,
        "conid": conid,
        "isin": isin,
        "quantity": str(qty),
        "tradePrice": str(price),
        "closePrice": str(price),
        "multiplier": "1",
        "ibCommission": "0",
        "fxRateToBase": "1.0",
        "currency": currency,
        "dateTime": f"{date} 12:00:00",
        "tradeDate": date,
        "reportDate": date,
        "fifoPnlRealized": str(pnl),
        "cost": cost,
    }


def make_closed_lot(open_date, close_date, qty, cost, symbol="CONd", conid="",
                    isin="", currency="EUR"):
    return {
        "assetCategory": "STK",
        "symbol": symbol,
        "conid": conid,
        "isin": isin,
        "currency": currency,
        "underlyingSymbol": "",
        "quantity": str(qty),
        "cost": str(cost),
        "buySell": "SELL",
        "openDateTime": f"{open_date} 16:20:00",
        "dateTime": f"{close_date} 12:00:00",
        "reportDate": close_date,
        "fifoPnlRealized": "0",
        "fxRateToBase": "1.0",
    }


def calculate_for_trades(trades, tax_year, closed_lots=None):
    fieldnames = sorted({k for row in trades for k in row})
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "account_info.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["currency", "tax_year", "fx_transactions_count"])
            writer.writeheader()
            writer.writerow({"currency": "EUR", "tax_year": str(tax_year), "fx_transactions_count": "0"})
        with open(os.path.join(tmp, "trades.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(trades)
        if closed_lots:
            lot_fields = sorted({k for row in closed_lots for k in row})
            with open(os.path.join(tmp, "closed_lots.csv"), "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=lot_fields)
                writer.writeheader()
                writer.writerows(closed_lots)
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp)


def assert_close(actual, expected, tol=0.001, label=""):
    assert abs(actual - expected) <= tol, f"{label}: {actual} != {expected}"


# ---------------------------------------------------------------------------
# Unit-Tests: Alias-Map-Aufbau
# ---------------------------------------------------------------------------

def test_alias_map_via_conid():
    trades = [
        make_opt("2022-03-01", -1, 1.08, u_conid="555"),
        make_stk("2023-06-14", -100, 71.30, -262.0, conid="555"),
    ]
    alias = _build_underlying_alias_map(trades)
    assert alias.get("CON") == "CONd", alias
    assert alias.get("CONd") == "CONd", alias
    assert _symbols_equivalent("CONd", "CON", alias)
    assert _canon_symbol("CON", alias) == "CONd"
    print("  OK  Alias via underlyingConid: CON = CONd")


def test_alias_map_via_isin_fallback():
    trades = [
        make_opt("2022-03-01", -1, 1.08, u_secid="DE0005439004"),
        make_stk("2023-06-14", -100, 71.30, -262.0, isin="DE0005439004"),
    ]
    alias = _build_underlying_alias_map(trades)
    assert alias.get("CON") == "CONd", alias
    print("  OK  Alias via underlyingSecurityID/ISIN-Fallback")


def test_alias_map_without_identifiers_stays_empty():
    trades = [
        make_opt("2022-03-01", -1, 1.08),
        make_stk("2023-06-14", -100, 71.30, -262.0),
    ]
    alias = _build_underlying_alias_map(trades)
    assert alias == {}, alias
    assert not _symbols_equivalent("CONd", "CON", alias)
    print("  OK  Ohne conid/ISIN keine Alias-Raterei (Identitaet)")


def test_alias_map_class_shares_stay_separate():
    # 'BRK A' und 'BRK B' haben verschiedene conids/ISINs — kein manufaktu-
    # riertes Wurzel-Symbol darf sie verschmelzen. Die OCC-Schreibweise
    # 'BRKB' gruppiert nur mit 'BRK B' (gleiche conid).
    trades = [
        make_stk("2024-01-10", 10, 300000, 0, symbol="BRK A", conid="1", isin="US0846701086"),
        make_stk("2024-01-10", 10, 400, 0, symbol="BRK B", conid="2", isin="US0846707026"),
        make_opt("2024-02-01", -1, 5.0, underlying="BRKB", u_conid="2",
                 strike="400", expiry="2024-03-15"),
    ]
    alias = _build_underlying_alias_map(trades)
    assert alias.get("BRKB") == "BRK B", alias
    assert "BRK A" not in alias, alias
    assert not _symbols_equivalent("BRK A", "BRK B", alias)
    print("  OK  Klassen-Aktien bleiben getrennt (BRK A vs. BRK B/BRKB)")


def test_alias_map_rename_prefers_most_frequent_stk_symbol():
    trades = [
        make_stk("2024-07-25", 100, 10.0, 0, symbol="NYCB", conid="9"),
        make_stk("2024-08-01", -100, 10.5, 50.0, symbol="NYCB", conid="9"),
        make_stk("2024-12-05", -100, 11.0, 30.0, symbol="FLG", conid="9"),
        make_opt("2024-12-03", -1, 0.5, underlying="FLG", u_conid="9",
                 strike="11", expiry="2025-01-17"),
    ]
    alias = _build_underlying_alias_map(trades)
    assert alias.get("FLG") == "NYCB", alias
    assert alias.get("NYCB") == "NYCB", alias
    print("  OK  Ticker-Rename gruppiert, kanonisch = haeufigstes STK-Symbol")


def test_reused_stock_symbol_does_not_bridge_distinct_conids():
    # Ein Broker-Symbol kann nach Delisting/Reuse spaeter eine andere conid
    # bezeichnen. Das gemeinsame Label darf die stabilen Identitaeten nicht
    # transitiv verschmelzen.
    trades = [
        make_stk("2022-01-10", 100, 10.0, 0, symbol="DUP", conid="1"),
        make_opt("2022-02-01", -1, 1.0, underlying="OLDOPT", u_conid="1"),
        make_stk("2024-01-10", 100, 20.0, 0, symbol="DUP", conid="2"),
        make_opt("2024-02-01", -1, 2.0, underlying="NEWOPT", u_conid="2"),
    ]
    alias = _build_underlying_alias_map(trades)
    assert not _symbols_equivalent("OLDOPT", "NEWOPT", alias), alias
    print("  OK  Wiederverwendetes Symbol verbindet keine unterschiedlichen conids")


def test_alias_map_ignores_futures_options():
    # FOP-Underlyings sind Futures — auch bei (hypothetisch) kollidierender
    # conid darf keine Aktien-Gruppe entstehen.
    fop = make_opt("2024-03-01", -1, 10.0, underlying="ESZ4", u_conid="7")
    fop["assetCategory"] = "FOP"
    trades = [fop, make_stk("2024-03-01", 10, 50.0, 0, symbol="ES", conid="7")]
    alias = _build_underlying_alias_map(trades)
    assert "ESZ4" not in alias, alias
    print("  OK  FOP-Underlyings werden nicht registriert")


def test_alias_map_option_only_group_skipped():
    trades = [make_opt("2024-03-01", -1, 1.0, underlying="SPX", u_conid="42")]
    alias = _build_underlying_alias_map(trades)
    assert alias == {}, alias
    print("  OK  Gruppe ohne STK-Row bleibt draussen (Index-Optionen)")


def test_propagate_alias_isins():
    symbol_to_isin = {"CONd": "DE0005439004"}
    alias = {"CON": "CONd", "CONd": "CONd"}
    _propagate_alias_isins(symbol_to_isin, alias)
    assert symbol_to_isin.get("CON") == "DE0005439004"
    assert symbol_to_isin["CONd"] == "DE0005439004"
    print("  OK  ISIN-Propagation auf Alias-Mitglieder")


def test_put_lot_matcher_uses_alias_map():
    det = {"putCall": "P", "assignment_date": "2022-03-18",
           "assignment_trade_date": "2022-03-18"}
    lots = [make_closed_lot("2022-03-18", "2023-06-14", 100, 7392.0)]
    without = _put_assignment_closed_lot_matches(lots, det, "CON", 100)
    with_alias = _put_assignment_closed_lot_matches(
        lots, det, "CON", 100, alias_map={"CON": "CONd", "CONd": "CONd"})
    assert without == [], without
    assert len(with_alias) == 1 and with_alias[0]["shares"] == 100, with_alias
    print("  OK  _put_assignment_closed_lot_matches respektiert alias_map")


# ---------------------------------------------------------------------------
# Integration: Issue-#83-Szenario (Georg, CON/CONd)
# ---------------------------------------------------------------------------

def _con_cross_year_trades(with_conid=True):
    u_conid = "555" if with_conid else ""
    conid = "555" if with_conid else ""
    return [
        # 2022: Put verkauft (Praemie 108 EUR) und angedient
        make_opt("2022-03-01", -1, 1.08, u_conid=u_conid),
        make_opt("2022-03-18", 1, 0, transaction_type="BookTrade",
                 buysell="BUY", u_conid=u_conid),
        # 2022: Aktien-Einbuchung aus der Andienung (IBKR-Basis: Strike - Praemie)
        make_stk("2022-03-18", 100, 75.0, 0, conid=conid,
                 transaction_type="BookTrade", cost="7392"),
        # 2023: Verkauf; IBKR-PnL enthaelt die eingebettete Praemie
        make_stk("2023-06-14", -100, 71.30, -262.0, conid=conid, cost="7392"),
    ]


def test_cross_year_con_cond_correction_applies():
    lots = [make_closed_lot("2022-03-18", "2023-06-14", 100, 7392.0, conid="555")]
    rd = calculate_for_trades(_con_cross_year_trades(), tax_year=2023, closed_lots=lots)

    corrections = rd["audit"]["cross_year_put_corrections"]
    assert len(corrections) == 1, corrections
    assert_close(corrections[0]["correction_eur"], 108.0, label="correction_eur")
    # Aktienverlust 2023: -262 (IBKR) -> -370 (Kostenbasis = Strike 7500);
    # die Praemie 108 war 2022 als Stillhalterertrag zu versteuern und wird
    # 2023 NICHT erneut angesetzt.
    assert_close(rd["stocks_loss_eur"], -370.0, label="stocks_loss_eur")
    assert_close(rd["options_gain_eur"], 0.0, label="options_gain_eur")
    assert rd["audit"]["underlying_symbol_aliases"] == {"CONd": ["CON"]}
    # Excel-Row: Kostenbasis restauriert, PnL korrigiert
    row = next(r for r in rd["trade_details"]
               if r.get("assetCategory") == "STK" and r.get("buySell") == "SELL")
    assert row.get("stillhalter_adjusted") is True, row
    assert_close(float(row["cost"]), 7500.0, label="row cost restore")
    assert_close(row["pnl_eur"], -370.0, label="row pnl_eur")
    print("  OK  Issue #83 Cross-Year: CON-Praemie korrigiert CONd-Verkauf (7500 / -370)")


def test_cross_year_without_identifiers_keeps_old_behaviour():
    lots = [make_closed_lot("2022-03-18", "2023-06-14", 100, 7392.0)]
    rd = calculate_for_trades(_con_cross_year_trades(with_conid=False),
                              tax_year=2023, closed_lots=lots)
    assert rd["audit"]["cross_year_put_corrections"] == []
    assert_close(rd["stocks_loss_eur"], -262.0, label="stocks_loss_eur")
    assert rd["audit"]["underlying_symbol_aliases"] == {}
    print("  OK  Ohne conid/ISIN keine Korrektur (dokumentierter Fallback)")


def test_same_year_con_cond_premium_not_double_taxed():
    trades = [
        # Alles im Steuerjahr 2023: Verkauf, Andienung, Aktienverkauf
        make_opt("2023-03-01", -1, 1.08, u_conid="555", expiry="2023-03-17"),
        make_opt("2023-03-17", 1, 0, transaction_type="BookTrade",
                 buysell="BUY", u_conid="555", expiry="2023-03-17"),
        make_stk("2023-03-17", 100, 75.0, 0, conid="555",
                 transaction_type="BookTrade", cost="7392"),
        make_stk("2023-06-14", -100, 71.30, -262.0, conid="555", cost="7392"),
    ]
    lots = [make_closed_lot("2023-03-17", "2023-06-14", 100, 7392.0, conid="555")]
    rd = calculate_for_trades(trades, tax_year=2023, closed_lots=lots)

    # Praemie als Stillhalterertrag in Topf 2, Aktienverlust auf Strike-Basis.
    # Vor dem Fix: put_nosell_premium 108 UND Aktien-PnL -262 -> die Praemie
    # war doppelt erfasst (einmal Topf 2, einmal im zu hohen Aktien-PnL).
    assert_close(rd["audit"]["put_nosell_premium_eur"], 0.0,
                 label="put_nosell_premium_eur")
    assert_close(rd["audit"]["stillhalter_premium_eur"], 108.0,
                 label="stillhalter_premium_eur")
    assert_close(rd["options_gain_eur"], 108.0, label="options_gain_eur")
    assert_close(rd["stocks_loss_eur"], -370.0, label="stocks_loss_eur")
    print("  OK  Issue #83 Same-Year: Praemie in Topf 2, Aktienbasis restauriert")


def test_same_year_multiword_stock_symbol_is_not_truncated():
    trades = [
        make_opt("2023-03-01", -1, 2.0, underlying="BRKB", u_conid="2",
                 strike="400", expiry="2023-03-17"),
        make_opt("2023-03-17", 1, 0, transaction_type="BookTrade",
                 buysell="BUY", underlying="BRKB", u_conid="2",
                 strike="400", expiry="2023-03-17"),
        make_stk("2023-03-17", 100, 400.0, 0, symbol="BRK B", conid="2",
                 transaction_type="BookTrade", cost="39800"),
        make_stk("2023-06-14", -100, 390.0, -800.0, symbol="BRK B",
                 conid="2", cost="39800"),
    ]
    lots = [
        make_closed_lot("2023-03-17", "2023-06-14", 100, 39800.0,
                        symbol="BRK B", conid="2")
    ]
    rd = calculate_for_trades(trades, tax_year=2023, closed_lots=lots)

    assert_close(rd["audit"]["put_nosell_premium_eur"], 0.0,
                 label="put_nosell_premium_eur")
    assert_close(rd["options_gain_eur"], 200.0, label="options_gain_eur")
    assert_close(rd["stocks_loss_eur"], -1000.0, label="stocks_loss_eur")
    row = next(r for r in rd["trade_details"]
               if r.get("assetCategory") == "STK" and r.get("buySell") == "SELL")
    assert row.get("stillhalter_adjusted") is True, row
    print("  OK  Mehrwort-Ticker bleibt beim Matching intakt (BRKB ↔ BRK B)")


def test_alias_cross_currency_premium_is_not_applied():
    # Review F1: Die Alias-Map ueberbrueckt Listings desselben Instruments.
    # Eine USD-Optionspraemie darf aber keine EUR-Listing-Row korrigieren
    # (Roh-Feld-Mathe waere ein Waehrungsmix). Erwartung: Verhalten wie vor
    # Issue #83 — Praemie eigenstaendig in Topf 2 (put_nosell), Aktien-PnL
    # unkorrigiert.
    trades = [
        make_opt("2023-03-01", -1, 1.08, u_secid="DE0005439004",
                 expiry="2023-03-17", currency="USD"),
        make_opt("2023-03-17", 1, 0, transaction_type="BookTrade",
                 buysell="BUY", u_secid="DE0005439004",
                 expiry="2023-03-17", currency="USD"),
        make_stk("2023-03-17", 100, 75.0, 0, isin="DE0005439004",
                 transaction_type="BookTrade", cost="7392", currency="EUR"),
        make_stk("2023-06-14", -100, 71.30, -262.0, isin="DE0005439004",
                 cost="7392", currency="EUR"),
    ]
    lots = [make_closed_lot("2023-03-17", "2023-06-14", 100, 7392.0,
                            isin="DE0005439004", currency="EUR")]
    rd = calculate_for_trades(trades, tax_year=2023, closed_lots=lots)

    assert_close(rd["audit"]["put_nosell_premium_eur"], 108.0,
                 label="put_nosell_premium_eur")
    assert_close(rd["stocks_loss_eur"], -262.0, label="stocks_loss_eur")
    row = next(r for r in rd["trade_details"]
               if r.get("assetCategory") == "STK" and r.get("buySell") == "SELL")
    assert not row.get("stillhalter_adjusted"), row
    print("  OK  Waehrungs-Guard: USD-Praemie korrigiert keine EUR-Listing-Row")


def test_propagate_alias_isins_prefers_canonical():
    # Review F3: ISIN-Quelle ist das kanonische Symbol, nicht das alphabetisch
    # erste Gruppenmitglied (relevant bei ISIN-Wechsel im Rename-Fall).
    symbol_to_isin = {"ABCD": "US0000000009", "CONd": "DE0005439004"}
    alias = {"CON": "CONd", "CONd": "CONd", "ABCD": "CONd"}
    _propagate_alias_isins(symbol_to_isin, alias)
    assert symbol_to_isin["CON"] == "DE0005439004", symbol_to_isin
    assert symbol_to_isin["ABCD"] == "US0000000009", symbol_to_isin
    print("  OK  ISIN-Propagation bevorzugt das kanonische Symbol")


def test_cross_year_option_rename_matches_original_sell():
    trades = [
        make_opt("2022-03-01", -1, 1.08, underlying="OLD", u_conid="9",
                 conid="opt-99"),
        make_opt("2022-03-18", 1, 0, transaction_type="BookTrade",
                 buysell="BUY", underlying="NEW", u_conid="9",
                 conid="opt-99"),
        make_stk("2022-03-18", 100, 75.0, 0, symbol="NEW", conid="9",
                 transaction_type="BookTrade", cost="7392"),
        make_stk("2023-06-14", -100, 71.30, -262.0, symbol="NEW",
                 conid="9", cost="7392"),
    ]
    lots = [
        make_closed_lot("2022-03-18", "2023-06-14", 100, 7392.0,
                        symbol="NEW", conid="9")
    ]
    rd = calculate_for_trades(trades, tax_year=2023, closed_lots=lots)

    corrections = rd["audit"]["cross_year_put_corrections"]
    assert len(corrections) == 1, corrections
    assert_close(corrections[0]["correction_eur"], 108.0,
                 label="correction_eur")
    assert_close(rd["stocks_loss_eur"], -370.0, label="stocks_loss_eur")
    print("  OK  Cross-Year-Rename findet Original-SELL via stabile Identitaet")


def test_same_year_option_rename_uses_option_conid_fifo():
    trades = [
        make_opt("2023-03-01", -1, 1.08, underlying="OLD", u_conid="9",
                 conid="opt-99", expiry="2023-03-17"),
        make_opt("2023-03-17", 1, 0, transaction_type="BookTrade",
                 buysell="BUY", underlying="NEW", u_conid="9",
                 conid="opt-99", expiry="2023-03-17"),
        make_stk("2023-03-17", 100, 75.0, 0, symbol="NEW", conid="9",
                 transaction_type="BookTrade", cost="7392"),
        make_stk("2023-06-14", -100, 71.30, -262.0, symbol="NEW",
                 conid="9", cost="7392"),
    ]
    lots = [
        make_closed_lot("2023-03-17", "2023-06-14", 100, 7392.0,
                        symbol="NEW", conid="9")
    ]
    rd = calculate_for_trades(trades, tax_year=2023, closed_lots=lots)

    assert_close(rd["audit"]["stillhalter_premium_eur"], 108.0,
                 label="stillhalter_premium_eur")
    assert_close(rd["audit"]["put_nosell_premium_eur"], 0.0,
                 label="put_nosell_premium_eur")
    assert_close(rd["options_gain_eur"], 108.0, label="options_gain_eur")
    assert_close(rd["stocks_loss_eur"], -370.0, label="stocks_loss_eur")
    print("  OK  Same-Year-Rename nutzt Options-conid fuer den FIFO-Match")


if __name__ == "__main__":
    print("Symbol-Matching Option↔Aktie (Issue #83, CON/CONd)")
    test_alias_map_via_conid()
    test_alias_map_via_isin_fallback()
    test_alias_map_without_identifiers_stays_empty()
    test_alias_map_class_shares_stay_separate()
    test_alias_map_rename_prefers_most_frequent_stk_symbol()
    test_reused_stock_symbol_does_not_bridge_distinct_conids()
    test_alias_map_ignores_futures_options()
    test_alias_map_option_only_group_skipped()
    test_propagate_alias_isins()
    test_put_lot_matcher_uses_alias_map()
    test_cross_year_con_cond_correction_applies()
    test_cross_year_without_identifiers_keeps_old_behaviour()
    test_same_year_con_cond_premium_not_double_taxed()
    test_same_year_multiword_stock_symbol_is_not_truncated()
    test_alias_cross_currency_premium_is_not_applied()
    test_propagate_alias_isins_prefers_canonical()
    test_cross_year_option_rename_matches_original_sell()
    test_same_year_option_rename_uses_option_conid_fifo()
    print("Alle Tests bestanden.")
