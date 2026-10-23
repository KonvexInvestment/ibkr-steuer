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
    _closing_quantity_by_execution,
    _build_tageskurs_pnl_adjustment_maps,
    _build_tageskurs_put_adjustments,
    _claim_exact_tageskurs_adjustments,
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


def test_put_adjustments_use_proven_per_share_correction():
    """Der Tageskurs-Pfad uebernimmt die zuvor lot-genau belegte Korrektur."""
    same_year_lots = {
        'TLT': [{
            'date_str': '2025-06-20 16:20:00',
            'shares': 200,
            'correction_per_share_raw': 1.5,
            'premium_per_share_raw': 1.5,
            'invstg_basis_extra_per_share_raw': 0.0,
        }],
        'EMPTY': [{
            'date_str': '2025-06-20',
            'shares': 0,
            'correction_per_share_raw': 9.9,
        }],
    }
    put_adj = _build_tageskurs_put_adjustments(same_year_lots, {})
    assert list(put_adj.keys()) == ['TLT']
    lot = put_adj['TLT'][0]
    assert lot['shares_remaining'] == 200
    assert abs(lot['correction_per_share_raw'] - 1.5) < 1e-12
    assert abs(lot['premium_per_share_raw'] - 1.5) < 1e-12
    assert lot['date'] == '2025-06-20'
    print("  OK  Put-Adj: belegte Per-Share-Korrektur, Datum gekuerzt")


def test_put_adjustments_merge_cross_year_and_sort_fifo():
    """Cross-Year-Lots werden gemerged und pro Symbol nach Datum sortiert."""
    same_year_lots = {'TLT': [{
        'date_str': '2025-06-20',
        'shares': 100,
        'correction_per_share_raw': 1.0,
        'premium_per_share_raw': 1.0,
        'invstg_basis_extra_per_share_raw': 0.0,
    }]}
    xy_lots = {'TLT': [
        {'date_str': '2024-11-15', 'shares': 100,
         'correction_per_share_raw': 4.5, 'premium_per_share_raw': 2.0,
         'invstg_basis_extra_per_share_raw': 2.5},
        {'date_str': '2024-01-19', 'shares': 0,
         'correction_per_share_raw': 9.9, 'premium_per_share_raw': 9.9},  # leer
    ]}
    put_adj = _build_tageskurs_put_adjustments(same_year_lots, xy_lots)
    lots = list(put_adj['TLT'])
    assert len(lots) == 2
    assert [l['date'] for l in lots] == ['2024-11-15', '2025-06-20'], "FIFO-Sortierung"
    assert lots[0]['premium_per_share_raw'] == 2.0
    assert lots[0]['correction_per_share_raw'] == 4.5
    assert lots[0]['invstg_basis_extra_per_share_raw'] == 2.5
    print("  OK  Put-Adj: Cross-Year-Merge + FIFO-Sortierung, leere Lots raus")


def test_same_day_fallback_requires_the_lot_rate():
    """#99: Die Korrektur wirkt jetzt auf das Nettoergebnis. Ein ExchTrade-Lot
    am Vormittag (Kurs des Handelstags) darf nicht die Stillhalter-Korrektur
    der BookTrade-Zeile (Buchung am Montag, anderer Kurs) ziehen, sonst
    hinge das Ergebnis von der Lot-Reihenfolge ab."""
    rows = [_debug_row(quantity='-100', dateTime='2025-06-20 16:20:00',
                       fxRateToBase='0.92', stillhalter_adjustment_raw=300.0)]
    for order in ('Vormittag zuerst', 'BookTrade zuerst'):
        exact, date_map = _build_tageskurs_pnl_adjustment_maps(rows)
        morning = _lot(quantity='-100', dateTime='2025-06-20 10:00:00',
                       fxRateToBase='0.90')
        booked = _lot(quantity='-100', dateTime='2025-06-20 16:20:00',
                      fxRateToBase='0.92')
        lots = [morning, booked] if order == 'Vormittag zuerst' \
            else [booked, morning]
        got = {id(lot): _consume_tageskurs_pnl_adjustment(
            lot, exact, date_map) for lot in lots}
        assert got[id(morning)] == 0.0, (order, got)
        assert abs(got[id(booked)] - 300.0) < 1e-9, (order, got)
    # Gleicher Kurs: konsolidierte Lots ziehen weiter über den Fallback.
    exact, date_map = _build_tageskurs_pnl_adjustment_maps(rows)
    same_rate = _lot(quantity='-100', dateTime='2025-06-20 10:00:00',
                     fxRateToBase='0.92')
    assert abs(_consume_tageskurs_pnl_adjustment(
        same_rate, exact, date_map) - 300.0) < 1e-9
    print("  OK  Consume: Same-Day-Fallback nur bei gleichem Kurs")


def test_entries_of_other_asset_categories_are_ignored():
    """TTAX-Map nimmt alle Kategorien; Options- und Aktienzeilen teilen das
    Underlying-Symbol und dürfen sich nicht gegenseitig bedienen."""
    rows = [_debug_row(assetCategory='OPT', symbol='TLT 250620C90',
                       underlyingSymbol='TLT', stillhalter_adjustment_raw=0,
                       transaction_tax_eur=-5.0)]
    exact, date_map = _build_tageskurs_pnl_adjustment_maps(
        rows, field='transaction_tax_eur')
    stock_lot = _lot(assetCategory='STK')
    assert _consume_tageskurs_pnl_adjustment(
        stock_lot, exact, date_map, by_instrument=True) == 0.0
    option_lot = _lot(assetCategory='OPT', symbol='TLT 250620C90',
                      underlyingSymbol='TLT')
    assert abs(_consume_tageskurs_pnl_adjustment(
        option_lot, exact, date_map, by_instrument=True) + 5.0) < 1e-9
    print("  OK  Consume: keine Korrektur über Kategoriegrenzen")


def test_ttax_map_separates_option_contracts_on_one_underlying():
    """Codex-Befund: Zwei Kontrakte auf denselben Basiswert teilten über
    das Underlying-Symbol den Tages-Schlüssel. Die TTAX-Map schlüsselt nach
    conid und Währung; das Lot von A zieht nie die Steuer von B."""
    rows = [_debug_row(assetCategory='OPT', symbol='ABC 250620C50',
                       underlyingSymbol='ABC', _conid='B', currency='USD',
                       stillhalter_adjustment_raw=0, quantity='-10',
                       dateTime='2025-03-03 11:00:00',
                       transaction_tax_eur=-100.0)]
    for order in ('A zuerst', 'B zuerst'):
        exact, date_map = _build_tageskurs_pnl_adjustment_maps(
            rows, field='transaction_tax_eur')
        lot_a = _lot(assetCategory='OPT', symbol='ABC 250620C40',
                     underlyingSymbol='ABC', conid='A', currency='USD',
                     quantity='-10', dateTime='2025-03-03 10:00:00')
        lot_b = _lot(assetCategory='OPT', symbol='ABC 250620C50',
                     underlyingSymbol='ABC', conid='B', currency='USD',
                     quantity='-10', dateTime='2025-03-03 11:00:00')
        lots = [lot_a, lot_b] if order == 'A zuerst' else [lot_b, lot_a]
        got = {id(lot): _consume_tageskurs_pnl_adjustment(
            lot, exact, date_map, by_instrument=True) for lot in lots}
        assert got[id(lot_a)] == 0.0, (order, got)
        assert abs(got[id(lot_b)] + 100.0) < 1e-9, (order, got)
    print("  OK  TTAX-Map: Kontrakte desselben Basiswerts getrennt")


def test_exact_matches_of_all_lots_precede_any_same_day_fallback():
    """Codex-Befund: Verkäufe derselben conid um 10 und 11 Uhr, Kaufsteuer
    nur auf dem 11-Uhr-Verkauf. Ohne Vorlauf zog das 10-Uhr-Lot (kein
    eigener Treffer) die Steuer über den Tagesfallback, bevor das 11-Uhr-Lot
    an der Reihe war. Mit exact_claims hängt das Ergebnis nicht von der
    Lot-Reihenfolge ab."""
    rows = [_debug_row(_conid='C1', currency='USD', quantity='-10',
                       dateTime='2025-03-03 11:00:00',
                       stillhalter_adjustment_raw=0,
                       transaction_tax_eur=-4.0)]
    for order in ('10 Uhr zuerst', '11 Uhr zuerst'):
        exact, date_map = _build_tageskurs_pnl_adjustment_maps(
            rows, field='transaction_tax_eur')
        early = _lot(conid='C1', currency='USD', quantity='-10',
                     dateTime='2025-03-03 10:00:00')
        late = _lot(conid='C1', currency='USD', quantity='-10',
                    dateTime='2025-03-03 11:00:00')
        lots = [early, late] if order == '10 Uhr zuerst' else [late, early]
        claims = _claim_exact_tageskurs_adjustments(
            lots, exact, by_instrument=True)
        got = {id(lot): _consume_tageskurs_pnl_adjustment(
            lot, exact, date_map, by_instrument=True, exact_claims=claims)
            for lot in lots}
        assert got[id(early)] == 0.0, (order, got)
        assert abs(got[id(late)] + 4.0) < 1e-9, (order, got)
    # Ohne eigenen Treffer greift der Tagesfallback weiterhin (konsolidiertes
    # Lot mit abweichendem Zeitstempel).
    exact, date_map = _build_tageskurs_pnl_adjustment_maps(
        rows, field='transaction_tax_eur')
    merged = _lot(conid='C1', currency='USD', quantity='-10',
                  dateTime='2025-03-03 12:00:00')
    claims = _claim_exact_tageskurs_adjustments(
        [merged], exact, by_instrument=True)
    got = _consume_tageskurs_pnl_adjustment(
        merged, exact, date_map, by_instrument=True, exact_claims=claims)
    assert abs(got + 4.0) < 1e-9, got
    print("  OK  Consume: exakte Treffer aller Lots vor jedem Tagesfallback")


def test_exact_timestamp_also_requires_the_lot_rate():
    """Codex-Befund: Börsenverkauf und Call-Andienung in derselben Sekunde
    (16:20). Das ExchTrade-Lot (Kurs des Handelstags) darf die Prämien-
    korrektur der BookTrade-Zeile (Kurs des Buchungstags) auch über den
    exakten Zeitstempel nicht ziehen."""
    rows = [_debug_row(quantity='-100', dateTime='2025-01-31 16:20:00',
                       fxRateToBase='0.97', stillhalter_adjustment_raw=200.0)]
    for order in ('Börse zuerst', 'Andienung zuerst'):
        exact, date_map = _build_tageskurs_pnl_adjustment_maps(rows)
        exchange = _lot(quantity='-100', dateTime='2025-01-31 16:20:00',
                        fxRateToBase='0.96')
        booked = _lot(quantity='-100', dateTime='2025-01-31 16:20:00',
                      fxRateToBase='0.97')
        lots = [exchange, booked] if order == 'Börse zuerst' \
            else [booked, exchange]
        got = {id(lot): _consume_tageskurs_pnl_adjustment(
            lot, exact, date_map) for lot in lots}
        assert got[id(exchange)] == 0.0, (order, got)
        assert abs(got[id(booked)] - 200.0) < 1e-9, (order, got)
    print("  OK  Consume: auch der exakte Zeitstempel prüft den Kurs")


def test_entries_of_other_currencies_are_ignored():
    """Codex-Befund: EUR- und USD-Listing derselben Aktie (Alias), gleicher
    Schluss-Zeitstempel, zufällig gleicher Kurs. Der exakte Vorlauf läuft
    über alle Lots; das EUR-Lot (in der Tageskurs-Schleife übersprungen)
    darf die Prämienkorrektur der USD-Zeile nicht reservieren."""
    rows = [_debug_row(currency='USD', fxRateToBase='1',
                       stillhalter_adjustment_raw=200.0)]
    for order in ('EUR zuerst', 'USD zuerst'):
        exact, date_map = _build_tageskurs_pnl_adjustment_maps(rows)
        eur = _lot(currency='EUR', fxRateToBase='1')
        usd = _lot(currency='USD', fxRateToBase='1')
        lots = [eur, usd] if order == 'EUR zuerst' else [usd, eur]
        claims = _claim_exact_tageskurs_adjustments(lots, exact)
        got = {id(lot): _consume_tageskurs_pnl_adjustment(
            lot, exact, date_map, exact_claims=claims) for lot in lots}
        assert got[id(eur)] == 0.0, (order, got)
        assert abs(got[id(usd)] - 200.0) < 1e-9, (order, got)
    print("  OK  Consume: Zeilen anderer Währung zählen nie")


def test_entries_of_other_executions_are_ignored():
    """Echtdaten-Befund (Runde 5): Zwei Verkäufe derselben Sekunde, nur
    der zweite (387 Stück) trägt die Stillhalter-Korrektur. Das Lot des
    ersten Verkaufs (13 Stück, closingTransactionID E1) darf davon nichts
    verbrauchen, sonst verschiebt die Bruttozuordnung je Ausführung
    Gewinn und Verlust zwischen den beiden Verkäufen."""
    rows = [_debug_row(quantity='-13', stillhalter_adjustment_raw=0.0,
                       _transaction_id='E1'),
            _debug_row(quantity='-387', stillhalter_adjustment_raw=200.0,
                       _transaction_id='E2')]
    exact, date_map = _build_tageskurs_pnl_adjustment_maps(rows)
    first = _lot(quantity='13', closingTransactionID='E1')
    second = [_lot(quantity='187', closingTransactionID='E2'),
              _lot(quantity='200', closingTransactionID='E2')]
    lots = [first] + second
    claims = _claim_exact_tageskurs_adjustments(lots, exact)
    got = {id(lot): _consume_tageskurs_pnl_adjustment(
        lot, exact, date_map, exact_claims=claims) for lot in lots}
    assert got[id(first)] == 0.0, got
    assert abs(sum(got[id(lot)] for lot in second) - 200.0) < 1e-9, got
    print("  OK  Consume: Zeilen einer anderen Ausführung derselben Sekunde "
          "zählen nie")


def test_closing_and_opening_execution_keeps_correction_in_its_lots():
    """Echtdaten-Befund (Runde 5): Eine Andienung kauft 200 Aktien, deckt
    damit einen Short über 100 und eröffnet 100 neu (IBKR-Code C;O). Die
    Stillhalter-Korrektur der Zeile (134) betrifft nur das realisierte
    Ergebnis und gehört ganz zum einen geschlossenen Lot; je Zeilenstück
    verteilt bekäme das Lot nur 67."""
    rows = [_debug_row(buySell='BUY', quantity='200',
                       stillhalter_adjustment_raw=134.0,
                       _transaction_id='E1')]
    lot = _lot(buySell='BUY', quantity='-100', closingTransactionID='E1')
    quantities = _closing_quantity_by_execution([lot])
    assert quantities == {'E1': 100.0}, quantities
    exact, date_map = _build_tageskurs_pnl_adjustment_maps(
        rows, closing_quantities=quantities)
    got = _consume_tageskurs_pnl_adjustment(lot, exact, date_map)
    assert abs(got - 134.0) < 1e-9, got
    print("  OK  Build: schließend-eröffnende Ausführung verteilt die "
          "Korrektur nur auf geschlossene Stücke")


if __name__ == '__main__':
    print("Tageskurs-Korrektur-Maps (reine Kerne)")
    test_build_filters_and_per_share()
    test_consume_exact_match_full()
    test_shared_entries_prevent_double_consumption()
    test_same_day_fallback_for_consolidated_lot()
    test_sides_are_separated()
    test_missing_side_derived_from_quantity_sign()
    test_same_day_fallback_requires_the_lot_rate()
    test_entries_of_other_asset_categories_are_ignored()
    test_ttax_map_separates_option_contracts_on_one_underlying()
    test_exact_timestamp_also_requires_the_lot_rate()
    test_exact_matches_of_all_lots_precede_any_same_day_fallback()
    test_entries_of_other_currencies_are_ignored()
    test_entries_of_other_executions_are_ignored()
    test_closing_and_opening_execution_keeps_correction_in_its_lots()
    test_put_adjustments_use_proven_per_share_correction()
    test_put_adjustments_merge_cross_year_and_sort_fifo()
    print("Alle Tests bestanden.")
