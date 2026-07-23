#!/usr/bin/env python3
"""Unit-Tests fuer die Tageskurs-Korrektur-Maps (FP-Audit R1).

_build_tageskurs_pnl_adjustment_maps + _consume_tageskurs_pnl_adjustment:
Zuordnung der Stillhalter-Korrekturen zu CLOSED_LOTs fuer die
Brutto-Gewinn/Verlust-Buckets (Commit 2e0208f, CCJ-Fall).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (
    _build_tageskurs_pnl_adjustment_maps,
    _build_tageskurs_put_adjustments,
    _consume_tageskurs_pnl_adjustment,
)


def _debug_row(**overrides):
    base = {
        'source': 'trades',
        'assetCategory': 'STK',
        'symbol': 'TLT',
        'underlyingSymbol': '',
        'buySell': 'SELL',
        'quantity': '-100',
        'dateTime': '2025-06-20 16:20:00',
        'stillhalter_adjustment_raw': 250.0,
    }
    base.update(overrides)
    return base


def _lot(**overrides):
    base = {
        'symbol': 'TLT',
        'underlyingSymbol': '',
        'buySell': 'SELL',
        'quantity': '-100',
        'dateTime': '2025-06-20 16:20:00',
    }
    base.update(overrides)
    return base


def test_build_filters_and_per_share():
    rows = [
        _debug_row(),
        _debug_row(source='pnl_summary'),                 # keine Trade-Row
        _debug_row(assetCategory='OPT'),                  # kein STK
        _debug_row(stillhalter_adjustment_raw=0.0),       # keine Korrektur
    ]
    exact, date_map = _build_tageskurs_pnl_adjustment_maps(rows)
    assert len(exact) == 1 and len(date_map) == 1
    entry = exact[('TLT', '2025-06-20 16:20:00', 'SELL')][0]
    assert entry['remaining_shares'] == 100.0
    assert abs(entry['adjustment_per_share_raw'] - 2.5) < 1e-12
    print("  OK  Build: Filter (trades/STK/adjustment>0) + Per-Share")


def test_consume_exact_match_full():
    exact, date_map = _build_tageskurs_pnl_adjustment_maps([_debug_row()])
    adj = _consume_tageskurs_pnl_adjustment(_lot(), exact, date_map)
    assert abs(adj - 250.0) < 1e-9
    print("  OK  Consume: exakter Timestamp, voller Betrag")


def test_shared_entries_prevent_double_consumption():
    """Exact- und Date-Map teilen Entry-Objekte: kein Doppelkonsum."""
    exact, date_map = _build_tageskurs_pnl_adjustment_maps([_debug_row()])
    first = _consume_tageskurs_pnl_adjustment(_lot(), exact, date_map)
    second = _consume_tageskurs_pnl_adjustment(_lot(), exact, date_map)
    assert abs(first - 250.0) < 1e-9
    assert second == 0.0, f"Zweiter Konsum muss leer ausgehen: {second}"
    print("  OK  Consume: geteilte Entries verhindern Doppelkonsum")


def test_same_day_fallback_for_consolidated_lot():
    """IBKR-konsolidierter Lot (200 Shares) zieht beide Same-Day-Rows."""
    rows = [
        _debug_row(quantity='-100', dateTime='2025-06-20 15:00:00',
                   stillhalter_adjustment_raw=100.0),
        _debug_row(quantity='-100', dateTime='2025-06-20 16:20:00',
                   stillhalter_adjustment_raw=300.0),
    ]
    exact, date_map = _build_tageskurs_pnl_adjustment_maps(rows)
    lot = _lot(quantity='-200', dateTime='2025-06-20 15:00:00')
    adj = _consume_tageskurs_pnl_adjustment(lot, exact, date_map)
    assert abs(adj - 400.0) < 1e-9, adj
    print("  OK  Consume: Same-Day-Fallback fuer konsolidierte Lots")


def test_sides_are_separated():
    """SELL-Lot konsumiert keine BUY-Korrektur (Short-Cover) desselben Tages."""
    exact, date_map = _build_tageskurs_pnl_adjustment_maps(
        [_debug_row(buySell='BUY', quantity='100')])
    adj = _consume_tageskurs_pnl_adjustment(_lot(buySell='SELL'), exact, date_map)
    assert adj == 0.0
    adj_buy = _consume_tageskurs_pnl_adjustment(
        _lot(buySell='BUY', quantity='100'), exact, date_map)
    assert abs(adj_buy - 250.0) < 1e-9
    print("  OK  Consume: Sides getrennt (SELL zieht keine BUY-Korrektur)")


def test_missing_side_derived_from_quantity_sign():
    """CLOSED_LOT ohne buySell: positives Vorzeichen -> SELL, negatives -> BUY."""
    exact, date_map = _build_tageskurs_pnl_adjustment_maps([_debug_row()])
    lot = _lot(buySell='', quantity='100')   # positiv -> SELL
    adj = _consume_tageskurs_pnl_adjustment(lot, exact, date_map)
    assert abs(adj - 250.0) < 1e-9
    print("  OK  Consume: fehlende Side aus Quantity-Vorzeichen abgeleitet")


def test_put_adjustments_only_puts_with_per_share_premium():
    """Nur Put-Andienungen (Call-Praemien liegen im Erloes, nicht im Cost)."""
    details = [
        {'putCall': 'P', 'symbol': 'TLT 240126P00095000', 'multiplier': 100,
         'quantity': 2, 'premium_raw': 300.0, 'assignment_date': '2025-06-20 16:20:00'},
        {'putCall': 'C', 'symbol': 'TLT 240126C00095000', 'multiplier': 100,
         'quantity': 1, 'premium_raw': 100.0, 'assignment_date': '2025-06-20'},
    ]
    put_adj = _build_tageskurs_put_adjustments(details, {})
    assert list(put_adj.keys()) == ['TLT']
    lot = put_adj['TLT'][0]
    assert lot['shares_remaining'] == 200
    assert abs(lot['premium_per_share_raw'] - 1.5) < 1e-12
    assert lot['date'] == '2025-06-20'
    print("  OK  Put-Adj: nur Puts, Per-Share-Praemie, Datum gekuerzt")


def test_put_adjustments_merge_cross_year_and_sort_fifo():
    """Cross-Year-Lots werden gemerged und pro Symbol nach Datum sortiert."""
    details = [
        {'putCall': 'P', 'symbol': 'TLT X', 'multiplier': 100, 'quantity': 1,
         'premium_raw': 100.0, 'assignment_date': '2025-06-20'},
    ]
    xy_lots = {'TLT': [
        {'date_str': '2024-11-15', 'shares': 100, 'premium_per_share_raw': 2.0},
        {'date_str': '2024-01-19', 'shares': 0, 'premium_per_share_raw': 9.9},  # leer
    ]}
    put_adj = _build_tageskurs_put_adjustments(details, xy_lots)
    lots = list(put_adj['TLT'])
    assert len(lots) == 2
    assert [l['date'] for l in lots] == ['2024-11-15', '2025-06-20'], "FIFO-Sortierung"
    assert lots[0]['premium_per_share_raw'] == 2.0
    print("  OK  Put-Adj: Cross-Year-Merge + FIFO-Sortierung, leere Lots raus")


if __name__ == '__main__':
    print("Tageskurs-Korrektur-Maps (reine Kerne)")
    test_build_filters_and_per_share()
    test_consume_exact_match_full()
    test_shared_entries_prevent_double_consumption()
    test_same_day_fallback_for_consolidated_lot()
    test_sides_are_separated()
    test_missing_side_derived_from_quantity_sign()
    test_put_adjustments_only_puts_with_per_share_premium()
    test_put_adjustments_merge_cross_year_and_sort_fifo()
    print("Alle Tests bestanden.")
