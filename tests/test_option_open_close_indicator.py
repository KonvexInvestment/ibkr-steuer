#!/usr/bin/env python3
"""Options-Eröffnung und -Schließung nach IBKRs openCloseIndicator.

Bis 2026-10 erkannte der Zufluss-FIFO Eröffnung und Schließung am
realisierten Ergebnis (Verkauf mit Ergebnis 0 = Eröffnung, Kauf mit
Ergebnis = Schließung). Drei Folgen:
  - Rückkauf zum Einstand (Ergebnis 0) wurde übersehen, die Prämie zählte
    zusätzlich als Zufluss (Codex-Befund P1, in echten Exporten nie belegt).
  - Long-Verkauf zum Einstand galt als eröffneter Short.
  - C;O-Ausführungen (schließt und eröffnet zugleich) trugen ein Ergebnis,
    der eröffnete Short fehlte im Zufluss. Realfall U72 2025: RUTW
    260130C02700000 (1 Kontrakt) und RUTW 260227P02470000 (2 Kontrakte),
    zusammen 9.372,84 EUR.
Die Fixtures der C;O-Fälle tragen die Zahlen dieser echten Zeilen.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ui_model
from calculate_tax_report import (
    _annotate_close_open_lot_quantities,
    _close_open_closing_quantity,
    _collect_option_series_events,
    _detect_zufluss_unmatched,
    _get_open_option_sells,
    _option_close_open_unresolved,
    _option_opening_closing_quantities,
    _option_sell_open_quantity,
    _run_zufluss_fifo,
)
from test_cross_year_series import (
    assert_close,
    calculate_for_trades,
    make_assignment,
    make_buy_close,
    make_sell,
)


def _with(row, **fields):
    row = dict(row)
    row.update(fields)
    return row


def _rutw_call_long_buy():
    """U72: BUY 1 RUTW 260130C02700000 @ 21,67 (eröffnet Long)."""
    return _with(
        make_buy_close("2025-12-12", 1, 21.67, 0, strike="2700",
                       expiry="2026-01-30", pc="C", underlying="RUTW"),
        tradeID="rutw_c_buy", openCloseIndicator="O", cost="2168.1641",
        ibCommission="-1.1641", proceeds="-2167", transactionID="5089064706")


def _rutw_call_close_open_sell():
    """U72: SELL 2 @ 11,95, schließt 1 Long und eröffnet 1 Short (C;O)."""
    return _with(
        make_sell("2025-12-16", 2, 11.95, strike="2700", expiry="2026-01-30",
                  pc="C", underlying="RUTW", commission=-1.6282),
        tradeID="rutw_c_co", openCloseIndicator="C;O", cost="-2168.1641",
        fifoPnlRealized="-973.9782", proceeds="2390", currency="USD",
        transactionID="5104603531")


def _rutw_put_close_open_sell():
    """U72: SELL 4 RUTW 260227P02470000 @ 49,19, schließt 2 Long, eröffnet 2 Short."""
    return _with(
        make_sell("2025-12-26", 4, 49.19, strike="2470", expiry="2026-02-27",
                  pc="P", underlying="RUTW", commission=-3.2564),
        openCloseIndicator="C;O", cost="-10940.3607",
        fifoPnlRealized="-1103.9889", proceeds="19676")


def _svol_close_open_buy():
    """audit1 2024: BUY 3 SVOL 241018P00021000 @ 0,05, schließt 2 Short, eröffnet 1 Long."""
    return _with(
        make_buy_close("2024-10-03", 3, 0.05, 15.788986, strike="21",
                       expiry="2024-10-18", pc="P", underlying="SVOL"),
        openCloseIndicator="C;O", cost="26.899886", ibCommission="-1.67355",
        proceeds="-15")


def _run_fifo_on(trades, tax_year):
    events, _ = _collect_option_series_events(trades, tax_year)
    prior, current = [], []
    _run_zufluss_fifo(
        events, tax_year,
        on_prior_close=lambda key, sell, qty: prior.append((sell, qty)),
        on_current_open=lambda key, sell, qty: current.append((sell, qty)),
    )
    return prior, current


def test_close_open_split_follows_ibkr_amounts():
    assert _close_open_closing_quantity(_rutw_call_close_open_sell()) == 1.0
    assert _close_open_closing_quantity(_rutw_put_close_open_sell()) == 2.0
    # SVOL: IBKR verteilt die Provision nicht genau anteilig (1,99914)
    assert _close_open_closing_quantity(_svol_close_open_buy()) == 2.0
    assert _option_opening_closing_quantities(_rutw_put_close_open_sell()) == (2.0, 2.0)
    print("  OK  C;O: geschlossene Menge aus cost/fifoPnlRealized (RUTW 1 und 2, SVOL 2)")


def _close_open_lot(execution, quantity):
    return {"assetCategory": "OPT", "currency": "EUR", "quantity": str(quantity),
            "closingTransactionID": execution, "levelOfDetail": "CLOSED_LOT"}


def test_lots_are_primary_source_of_the_split():
    sell = _with(_rutw_put_close_open_sell(), transactionID="CO1", cost="-1000")
    assert _close_open_closing_quantity(sell) is None  # Beträge passen nicht
    _annotate_close_open_lot_quantities([sell], [_close_open_lot("CO1", 1),
                                                 _close_open_lot("CO1", 1)])
    assert _close_open_closing_quantity(sell) == 2.0
    contradicting = _with(_rutw_put_close_open_sell(), transactionID="CO2")
    _annotate_close_open_lot_quantities([contradicting],
                                        [_close_open_lot("CO2", 4)])
    assert _close_open_closing_quantity(contradicting) is None
    rd = calculate_for_trades([_with(_rutw_put_close_open_sell(),
                                     transactionID="CO1", cost="-1000")],
                              tax_year=2025,
                              closed_lots=[_close_open_lot("CO1", 2)])
    # 2 × 49,19 × 100 + anteilige Kommission −3,2564 × 2 / 4
    assert_close(rd["audit"].get("zufluss_premium_eur", 0), 9836.3718,
                 label="Zufluss aus Lot-Aufteilung")
    assert rd["audit"]["option_close_open_unresolved"] == []
    print("  OK  C;O: Lots der Ausführung gehen vor; Widerspruch wird Prüffall")


def test_opened_quantity_is_exact_for_assignment_lookup():
    """Verkauf 5, davon 4 geschlossen: 1 Short offen, nicht 0,9999… → 0."""
    long_buy = _with(make_buy_close("2025-02-03", 4, 1.0, 0, expiry="2025-06-20"),
                     openCloseIndicator="O", cost="400", proceeds="-400")
    co_sell = _with(make_sell("2025-03-03", 5, 2.0, expiry="2025-06-20",
                              commission=0),
                    openCloseIndicator="C;O", cost="-400", fifoPnlRealized="400",
                    proceeds="1000")
    assignment = _with(make_assignment("2025-04-15", 1, expiry="2025-06-20"),
                       openCloseIndicator="C")
    assert _option_sell_open_quantity(co_sell) == 1.0
    state = _get_open_option_sells([long_buy, co_sell, assignment], "OPT", "100",
                                   "2025-06-20", "P", 1, underlying="TEST")
    assert [s["_open_qty"] for s in state] == [1]
    rd = calculate_for_trades([long_buy, co_sell, assignment], tax_year=2025)
    audit = rd["audit"]
    assert audit.get("stillhalter_unmatched") == []
    # Long-Gewinn 400 plus Prämie des angedienten Shorts 200
    assert_close(rd.get("zeile_19_netto_eur", 0), 600.0, label="Zeile 19")
    print("  OK  C;O 5/4: eröffneter Short 1 wird für die Andienung gefunden")


def test_indicator_wins_over_result():
    breakeven_buyback = _with(make_buy_close("2025-03-01", 1, 1.0, 0),
                              openCloseIndicator="C")
    breakeven_long_sale = _with(make_sell("2025-03-01", 1, 1.0),
                                openCloseIndicator="C")
    no_indicator_sell = make_sell("2025-03-01", 1, 1.0)
    assert _option_opening_closing_quantities(breakeven_buyback) == (0.0, 1.0)
    assert _option_opening_closing_quantities(breakeven_long_sale) == (0.0, 1.0)
    assert _option_opening_closing_quantities(no_indicator_sell) == (1.0, 0.0)
    print("  OK  Kennzeichen vor Ergebnis; ohne Kennzeichen bisherige Regel")


def test_breakeven_buyback_creates_no_premium():
    """Codex-Repro: Verkauf und Rückkauf zum selben Preis, Zeile 19 = 0."""
    trades = [
        _with(make_sell("2025-03-03", 1, 100.0, expiry="2025-06-20",
                        commission=0), openCloseIndicator="O"),
        _with(make_buy_close("2025-04-01", 1, 100.0, 0, expiry="2025-06-20"),
              openCloseIndicator="C"),
    ]
    rd = calculate_for_trades(trades, tax_year=2025)
    audit = rd.get("audit", {})
    assert_close(audit.get("zufluss_premium_eur", 0), 0.0, label="Zufluss")
    assert_close(rd.get("options_gain_eur", 0), 0.0, label="options_gain")
    assert_close(rd.get("zeile_19_netto_eur", 0), 0.0, label="Zeile 19")
    print("  OK  Rückkauf zum Einstand: kein Zufluss, Zeile 19 = 0 (vorher 10.000)")


def test_breakeven_long_sale_is_no_short_opening():
    trades = [
        _with(make_buy_close("2025-03-03", 1, 5.0, 0, expiry="2025-06-20"),
              openCloseIndicator="O"),
        _with(make_sell("2025-04-01", 1, 5.0, expiry="2025-06-20",
                        commission=0), openCloseIndicator="C"),
    ]
    prior, current = _run_fifo_on(trades, 2025)
    assert prior == [] and current == []
    rd = calculate_for_trades(trades, tax_year=2025)
    assert_close(rd.get("audit", {}).get("zufluss_premium_eur", 0), 0.0,
                 label="Zufluss")
    print("  OK  Long-Verkauf zum Einstand eröffnet keinen Short (vorher 500 Zufluss)")


def test_close_open_sell_adds_opened_short_to_zufluss():
    """U72-Muster: nur der eröffnete Short zählt als Zufluss, zum Verkaufskurs."""
    trades = [_rutw_call_long_buy(), _rutw_call_close_open_sell()]
    prior, current = _run_fifo_on(trades, 2025)
    assert prior == [] and len(current) == 1 and current[0][1] == 1.0
    rd = calculate_for_trades(trades, tax_year=2025)
    audit = rd.get("audit", {})
    # 1 × 11,95 × 100 + anteilige Kommission −1,6282 / 2 (fxRateToBase 1)
    assert_close(audit.get("zufluss_premium_eur", 0), 1194.1859,
                 label="Zufluss C;O")
    # Long-Verlust −973,98 (Topf 2) plus Zufluss des eröffneten Shorts
    assert_close(rd.get("zeile_19_netto_eur", 0), 1194.1859 - 973.9782,
                 label="Zeile 19")
    assert audit.get("option_close_open_unresolved") == []
    print("  OK  C;O-Verkauf: eröffneter Short 1194,19 im Zufluss (vorher 0)")


def test_close_open_short_is_corrected_in_closing_year():
    """Folgejahr zu U72: der 2025 eröffnete Short wird 2026 zurückgekauft."""
    buyback = _with(
        make_buy_close("2026-01-20", 1, 5.0, 693.1859, strike="2700",
                       expiry="2026-01-30", pc="C", underlying="RUTW"),
        openCloseIndicator="C", cost="1194.1859", ibCommission="-1.0")
    trades = [_rutw_call_long_buy(), _rutw_call_close_open_sell(), buyback]
    prior, current = _run_fifo_on(trades, 2026)
    assert len(prior) == 1 and prior[0][1] == 1.0 and current == []
    rd = calculate_for_trades(trades, tax_year=2026)
    audit = rd.get("audit", {})
    assert_close(audit.get("prior_zufluss_correction_eur", 0), 1194.1859,
                 label="Vorjahreskorrektur")
    assert audit.get("zufluss_unmatched") == []
    print("  OK  Folgejahr: Rückkauf korrigiert die 2025 versteuerte Prämie")


def test_breakeven_buyback_of_prior_year_short_is_corrected():
    """Vorjahres-Short, Rückkauf zum Einstand: Prämie (Vorjahr) aus dem Ergebnis."""
    trades = [
        _with(make_sell("2024-11-04", 1, 2.0, expiry="2025-06-20",
                        commission=0), openCloseIndicator="O"),
        _with(make_buy_close("2025-02-03", 1, 2.0, 0, expiry="2025-06-20"),
              openCloseIndicator="C"),
    ]
    prior, current = _run_fifo_on(trades, 2025)
    assert len(prior) == 1 and current == []
    rd = calculate_for_trades(trades, tax_year=2025)
    assert_close(rd.get("audit", {}).get("prior_zufluss_correction_eur", 0),
                 200.0, label="Vorjahreskorrektur")
    print("  OK  Vorjahres-Short zum Einstand zurückgekauft: Korrektur 200")


def test_close_open_buy_closes_only_closing_part():
    """SVOL-Muster: Kauf 3 schließt 2 Vorjahres-Shorts, die Korrektur gilt 2."""
    sell = _with(make_sell("2023-08-29", 2, 0.15, strike="21",
                           expiry="2024-10-18", underlying="SVOL",
                           commission=-3.100114),
                 openCloseIndicator="O", cost="-26.899886")
    prior, current = _run_fifo_on([sell, _svol_close_open_buy()], 2024)
    assert [qty for _sell, qty in prior] == [2.0] and current == []
    print("  OK  C;O-Kauf: schließender Teil 2 von 3")


def test_breakeven_buyback_without_opening_sell_warns():
    trades = [_with(make_buy_close("2025-02-03", 1, 2.0, 0, expiry="2025-06-20"),
                    openCloseIndicator="C")]
    events, open_keys = _collect_option_series_events(trades, 2025)
    assert len(_detect_zufluss_unmatched(trades, 2025, open_keys)) == 1
    print("  OK  Rückkauf zum Einstand ohne Eröffnung: Warnung fehlendes Vorjahr")


def test_open_sells_for_assignment_ignore_long_sales():
    """Long-Verkauf derselben Serie trägt keine Prämie für eine Andienung."""
    short = _with(make_sell("2025-01-10", 1, 3.0), openCloseIndicator="O")
    long_sale = _with(make_sell("2025-05-10", 1, 4.0), openCloseIndicator="C",
                      fifoPnlRealized="50")
    trades = [short, make_assignment("2025-03-21", 1),
              _with(make_buy_close("2025-04-01", 1, 3.5, 0),
                    openCloseIndicator="O"),
              long_sale]
    state = _get_open_option_sells(trades, "OPT", "100", "2024-12-20", "P", 1,
                                   underlying="TEST")
    assert [s["tradeID"] for s in state] == [short["tradeID"]]
    assert state[0]["_open_qty"] == 1
    no_indicator = [{k: v for k, v in row.items() if k != "openCloseIndicator"}
                    for row in trades]
    state = _get_open_option_sells(no_indicator, "OPT", "100", "2024-12-20", "P",
                                   1, underlying="TEST")
    assert [s["tradeID"] for s in state] == [short["tradeID"]]
    print("  OK  Andienung: offener Verkauf ist der Short, nicht der Long-Verkauf")


def test_breakeven_buyback_keeps_tageskurs_correction():
    """Rückkauf zum Einstand ist keine Andienung: Kursdifferenz bleibt."""
    sell = _with(make_sell("2024-11-04", 1, 2.0, expiry="2025-06-20",
                           commission=0),
                 openCloseIndicator="O", currency="USD", fxRateToBase="0.90",
                 conid="777", transactionID="S1", cost="-200")
    buyback = _with(make_buy_close("2025-02-03", 1, 2.0, 0, expiry="2025-06-20"),
                    openCloseIndicator="C", fxRateToBase="0.95", conid="777",
                    transactionID="B1", cost="200")
    lot = {"assetCategory": "OPT", "symbol": buyback["symbol"], "conid": "777",
           "currency": "USD", "buySell": "BUY", "quantity": "-1",
           "cost": "-200", "fifoPnlRealized": "0", "fxRateToBase": "0.95",
           "openDateTime": "2024-11-04 10:00:00",
           "dateTime": "2025-02-03 10:00:00", "tradeDate": "2025-02-03",
           "reportDate": "2025-02-03", "transactionID": "S1",
           "closingTransactionID": "B1", "levelOfDetail": "CLOSED_LOT",
           "multiplier": "100", "putCall": "P", "strike": "100",
           "expiry": "2025-06-20", "underlyingSymbol": "TEST"}
    rd = calculate_for_trades([sell, buyback], tax_year=2025, closed_lots=[lot])
    # Vorjahresprämie 200 × 0,90 korrigiert, Kursdifferenz −200 × (0,95 − 0,90)
    assert_close(rd["audit"].get("prior_zufluss_correction_eur", 0), 180.0,
                 label="Vorjahreskorrektur")
    assert_close(rd.get("fx_correction_total", 0), -10.0, label="Tageskurs")
    print("  OK  Einstands-Rückkauf: Tageskurs −10 bleibt (Rückkauf 190 EUR Verlust)")


def test_close_open_without_derivable_split_is_review_case():
    broken = _with(_rutw_put_close_open_sell(), cost="-1000")
    assert _close_open_closing_quantity(broken) is None
    # Ohne belegte Aufteilung: ganz als Schließung, auch mit Ergebnis 0
    assert _option_opening_closing_quantities(broken) == (0.0, 4.0)
    assert _option_opening_closing_quantities(
        _with(broken, fifoPnlRealized="0")) == (0.0, 4.0)
    items = _option_close_open_unresolved([broken], 2025)
    assert len(items) == 1 and items[0]["quantity"] == 4.0
    # C;O-Kauf ohne Aufteilung schließt alle Shorts und eröffnet einen Long
    assert _option_close_open_unresolved(
        [_with(_svol_close_open_buy(), cost="1")], 2025) == []
    rd = calculate_for_trades([broken], tax_year=2025)
    assert len(rd["audit"]["option_close_open_unresolved"]) == 1
    notices = ui_model.collect_notices(rd, {})
    assert any(n["id"] == "option_close_open_unresolved" for n in notices)
    print("  OK  C;O ohne belegbare Aufteilung: Prüffall statt stiller Annahme")


if __name__ == "__main__":
    print("Options-Eröffnung/-Schließung nach openCloseIndicator")
    test_close_open_split_follows_ibkr_amounts()
    test_lots_are_primary_source_of_the_split()
    test_opened_quantity_is_exact_for_assignment_lookup()
    test_indicator_wins_over_result()
    test_breakeven_buyback_creates_no_premium()
    test_breakeven_long_sale_is_no_short_opening()
    test_close_open_sell_adds_opened_short_to_zufluss()
    test_close_open_short_is_corrected_in_closing_year()
    test_breakeven_buyback_of_prior_year_short_is_corrected()
    test_close_open_buy_closes_only_closing_part()
    test_breakeven_buyback_without_opening_sell_warns()
    test_open_sells_for_assignment_ignore_long_sales()
    test_breakeven_buyback_keeps_tageskurs_correction()
    test_close_open_without_derivable_split_is_review_case()
    print("Alle Tests bestanden.")
