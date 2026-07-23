#!/usr/bin/env python3
"""Unit-Tests fuer die Options-Event-Sammlung (FP-Audit R1).

Deckt die aus calculate_tax extrahierten reinen Kerne ab:
  - _collect_option_assignments: Andienungs-Detection (BMF Rn. 26/33)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (
    _collect_option_assignments,
    _collect_option_series_events,
    _detect_zufluss_unmatched,
    _occ_family_key,
    _option_key,
    _run_zufluss_fifo,
)


def _run_fifo_on(trades, tax_year=2025):
    """Collector + FIFO mit Recording-Callbacks; liefert (prior, current, renames)."""
    events, _ = _collect_option_series_events(trades, tax_year)
    prior, current = [], []
    renames = _run_zufluss_fifo(
        events, tax_year,
        on_prior_close=lambda key, sell, qty: prior.append((key, sell, qty)),
        on_current_open=lambda key, sell, qty: current.append((key, sell, qty)),
    )
    return prior, current, renames


def _assignment(**overrides):
    base = {
        'assetCategory': 'OPT',
        'transactionType': 'BookTrade',
        'buySell': 'BUY',
        'putCall': 'P',
        'fifoPnlRealized': '0',
        'reportDate': '2025-06-20',
        'quantity': '-1',
    }
    base.update(overrides)
    return base


def test_valid_call_and_put_assignments_detected():
    rows = [_assignment(putCall='P'), _assignment(putCall='C'),
            _assignment(assetCategory='FOP'), _assignment(assetCategory='FSFOP')]
    result = _collect_option_assignments(rows, 2025)
    assert result == rows
    print("  OK  Detection: Put/Call, OPT/FOP/FSFOP erkannt")


def test_non_assignments_excluded():
    rows = [
        _assignment(transactionType='ExchTrade'),        # Rueckkauf, kein Assignment
        _assignment(buySell='SELL'),                     # Eroeffnung
        _assignment(putCall=''),                         # kein Optionsrecht
        _assignment(fifoPnlRealized='55.00'),            # Verfall (PnL = Praemie)
        _assignment(assetCategory='STK'),                # Aktien-BookTrade
        _assignment(reportDate='2024-12-20'),            # falsches Steuerjahr
        _assignment(reportDate='', dateTime='', tradeDate=''),  # kein Datum
    ]
    assert _collect_option_assignments(rows, 2025) == []
    print("  OK  Detection: Nicht-Andienungen ausgeschlossen")


def test_date_fallback_chain_and_original_order():
    """reportDate > dateTime > tradeDate; Original-Reihenfolge bleibt erhalten."""
    a = _assignment(reportDate='', dateTime='2025-03-03 16:20:00')
    b = _assignment(reportDate='', dateTime='', tradeDate='2025-01-02')
    c = _assignment(reportDate='2025-09-19')
    result = _collect_option_assignments([a, b, c], 2025)
    assert result == [a, b, c], "unsortiert, Aufrufer sortiert (Issue #53)"
    print("  OK  Detection: Datums-Fallback-Kette, Reihenfolge unveraendert")


def test_small_pnl_tolerance():
    """|PnL| < 0.01 gilt als Assignment (Rundungsrauschen), >= 0.01 nicht."""
    near_zero = _assignment(fifoPnlRealized='0.009')
    at_limit = _assignment(fifoPnlRealized='0.01')
    result = _collect_option_assignments([near_zero, at_limit], 2025)
    assert result == [near_zero]
    print("  OK  Detection: PnL-Toleranzgrenze 0.01")


def test_occ_family_key_groups_renamed_opt_series_only():
    """MMM1 -> MMM-Familie (OPT); FOP-Ziffern-Suffixe (ESZ4) bleiben eigenstaendig."""
    original = ('OPT', 'MMM', '95', '2025-06-20', 'P')
    renamed = ('OPT', 'MMM1', '95', '2025-06-20', 'P')
    assert _occ_family_key(original) == _occ_family_key(renamed)
    fop_dec = ('FOP', 'ESZ4', '5000', '2024-12-20', 'C')
    fop_dec5 = ('FOP', 'ESZ5', '5000', '2024-12-20', 'C')
    assert _occ_family_key(fop_dec) == fop_dec
    assert _occ_family_key(fop_dec) != _occ_family_key(fop_dec5)
    # Rein numerisches Underlying darf nicht zu leerem Root kollabieren
    numeric = ('OPT', '123', '10', '2025-01-17', 'C')
    assert _occ_family_key(numeric) == numeric
    print("  OK  _occ_family_key: OPT-Familien, FOP unangetastet")


def _series_row(**overrides):
    base = {
        'assetCategory': 'OPT',
        'underlyingSymbol': 'TLT',
        'strike': '94.5',
        'expiry': '2025-06-20',
        'putCall': 'P',
        'transactionType': 'ExchTrade',
        'buySell': 'SELL',
        'fifoPnlRealized': '0',
        'reportDate': '2025-03-03',
    }
    base.update(overrides)
    return base


def test_series_events_classification():
    sell_open = _series_row()
    buy_close = _series_row(buySell='BUY', fifoPnlRealized='120.5')
    buy_no_pnl = _series_row(buySell='BUY')                       # Long-Open: kein Event
    booktrade = _series_row(transactionType='BookTrade', buySell='BUY')
    stk = _series_row(assetCategory='STK')
    next_year = _series_row(reportDate='2026-01-05')
    prior_year = _series_row(reportDate='2024-11-11')             # History-Sell

    events, open_keys = _collect_option_series_events(
        [sell_open, buy_close, buy_no_pnl, booktrade, stk, next_year, prior_year], 2025)

    key = _option_key(sell_open)
    assert events[key] == [sell_open, buy_close, booktrade, prior_year]
    assert open_keys == {key}
    print("  OK  series_events: SELL-open/BUY-close/BookTrade, Jahr-Filter")


def test_series_events_separate_underlyings_same_strike():
    """KWEB P30 und FXI P30 duerfen nicht in einer Series landen."""
    kweb = _series_row(underlyingSymbol='KWEB', strike='30')
    fxi = _series_row(underlyingSymbol='FXI', strike='30')
    events, _ = _collect_option_series_events([kweb, fxi], 2025)
    assert len(events) == 2
    print("  OK  series_events: Underlying trennt gleiche Strike/Expiry")


def test_fifo_open_current_year_sell_is_zufluss():
    sell = _series_row(reportDate='2025-03-03', quantity='-2')
    prior, current, renames = _run_fifo_on([sell])
    assert prior == [] and renames == []
    assert current == [(_option_key(sell), sell, 2)]
    print("  OK  FIFO: offener Steuerjahr-SELL wird Zufluss")


def test_fifo_prior_year_sell_closed_in_tax_year():
    """TC13-Analog: 2025-Buyback konsumiert Vorjahres-SELL → prior_close."""
    sell = _series_row(reportDate='2024-11-11', quantity='-1')
    buyback = _series_row(buySell='BUY', fifoPnlRealized='120.5',
                          reportDate='2025-02-02', quantity='1')
    prior, current, renames = _run_fifo_on([sell, buyback])
    assert prior == [(_option_key(sell), sell, 1)]
    assert current == [] and renames == []
    print("  OK  FIFO: Vorjahres-SELL + Steuerjahr-Buyback → prior_close")


def test_fifo_assignment_consumes_without_callback():
    """Assignment (BookTrade, PnL≈0) konsumiert Lot OHNE prior_close-Detail."""
    sell = _series_row(reportDate='2024-11-11', quantity='-1')
    assignment = _series_row(transactionType='BookTrade', buySell='BUY',
                             reportDate='2025-06-20', quantity='1')
    prior, current, renames = _run_fifo_on([sell, assignment])
    assert prior == [] and current == [] and renames == []
    print("  OK  FIFO: Assignment konsumiert still (keine Doppelkorrektur)")


def test_fifo_prior_year_closed_short_stays_done():
    """TC14-Analog: im Vorjahr geschlossener Short erzeugt 2025 nichts mehr."""
    sell = _series_row(reportDate='2024-05-05', quantity='-1')
    buyback_2024 = _series_row(buySell='BUY', fifoPnlRealized='80.0',
                               reportDate='2024-08-08', quantity='1')
    prior, current, renames = _run_fifo_on([sell, buyback_2024])
    assert prior == [] and current == [] and renames == []
    print("  OK  FIFO: im Vorjahr geschlossener Short bleibt erledigt")


def test_fifo_same_year_partial_close():
    """3 verkauft, 1 zurueckgekauft (Same-Year): Rest 2 wird Zufluss."""
    sell = _series_row(reportDate='2025-01-10', quantity='-3')
    buyback = _series_row(buySell='BUY', fifoPnlRealized='-15.0',
                          reportDate='2025-04-04', quantity='1')
    prior, current, renames = _run_fifo_on([sell, buyback])
    assert prior == []
    assert current == [(_option_key(sell), sell, 2)]
    print("  OK  FIFO: Same-Year-Teilglattstellung, Rest wird Zufluss")


def _xle_split_rows(close_quantity='2', close_cost='119.23311',
                    sell_year=2025):
    sell = _series_row(
        accountId='TEST',
        conid='653278898',
        symbol='XLE   260116P00088000',
        underlyingSymbol='XLE',
        strike='88',
        expiry='2026-01-16',
        quantity='-1',
        cost='-119.23311',
        tradePrice='1.2',
        ibCommission='-0.76689',
        reportDate=f'{sell_year}-12-04',
    )
    close = _series_row(
        accountId='TEST',
        conid='653278898',
        symbol='XLE   260116P00044000',
        underlyingSymbol='XLE',
        strike='44',
        expiry='2026-01-16',
        buySell='BUY',
        fifoPnlRealized='51.90491',
        quantity=close_quantity,
        cost=close_cost,
        reportDate='2025-12-30',
    )
    return sell, close


def test_fifo_split_uses_conid_and_cost_basis():
    """Jans XLE-Fall: 1x P88 wird nach 2:1-Split durch 2x P44 geschlossen."""
    sell, close = _xle_split_rows()
    prior, current, matches = _run_fifo_on([sell, close])
    assert prior == []
    assert current == []
    assert len(matches) == 1
    assert matches[0]['match_type'] == 'split'
    assert matches[0]['conid'] == '653278898'
    assert matches[0]['quantity'] == 1
    assert matches[0]['close_quantity'] == 2
    assert matches[0]['ratio'] == 2

    _, open_keys = _collect_option_series_events([sell, close], 2025)
    assert _detect_zufluss_unmatched([sell, close], 2025, open_keys) == []
    print("  OK  FIFO: XLE 2:1-Split via conid und FIFO-Kostenbasis")


def test_fifo_split_partial_close_leaves_proportional_open_premium():
    """Nur 1 von 2 neuen P44 schliesst die Haelfte des alten P88-Lots."""
    sell, close = _xle_split_rows(
        close_quantity='1',
        close_cost='59.616555',
    )
    prior, current, matches = _run_fifo_on([sell, close])
    assert prior == []
    assert len(current) == 1
    assert abs(current[0][2] - 0.5) < 0.0000001
    assert len(matches) == 1
    assert abs(matches[0]['quantity'] - 0.5) < 0.0000001
    assert abs(matches[0]['close_quantity'] - 1.0) < 0.0000001
    assert abs(matches[0]['ratio'] - 2.0) < 0.0000001
    print("  OK  FIFO: Split-Teilglattstellung laesst 0,5 Alt-Kontrakte offen")


def test_fifo_cross_year_split_corrects_full_prior_premium():
    """Vorjahres-P88 wird nach Split voll geschlossen und komplett korrigiert."""
    sell, close = _xle_split_rows(sell_year=2024)
    prior, current, matches = _run_fifo_on([sell, close])
    assert prior == [(_option_key(sell), sell, 1)]
    assert current == []
    assert len(matches) == 1
    assert matches[0]['match_type'] == 'split'
    print("  OK  FIFO: Cross-Year-Split konsumiert die volle Vorjahrespraemie")


def test_fifo_same_terms_different_conid_stay_separate():
    """Gleiche Terms mit anderer conid duerfen nicht account-intern kollidieren."""
    sell = _series_row(
        accountId='TEST', conid='111', symbol='XLE P44 A',
        underlyingSymbol='XLE', strike='44', quantity='-1',
        cost='-100', reportDate='2025-01-10')
    close = _series_row(
        accountId='TEST', conid='222', symbol='XLE P44 B',
        underlyingSymbol='XLE', strike='44', buySell='BUY',
        fifoPnlRealized='25', quantity='1', cost='100',
        reportDate='2025-02-10')
    prior, current, matches = _run_fifo_on([sell, close])
    assert prior == [] and matches == []
    assert current == [(_option_key(sell), sell, 1)]
    _, open_keys = _collect_option_series_events([sell, close], 2025)
    assert len(_detect_zufluss_unmatched([sell, close], 2025, open_keys)) == 1
    print("  OK  FIFO: identische Terms mit verschiedener conid bleiben getrennt")


def test_fifo_changed_terms_without_cost_evidence_do_not_match():
    """conid allein reicht bei geaenderten Terms ohne FIFO-Kostenbasis nicht."""
    sell, close = _xle_split_rows()
    sell.pop('cost')
    close.pop('cost')
    prior, current, matches = _run_fifo_on([sell, close])
    assert prior == [] and matches == []
    assert current == [(_option_key(sell), sell, 1)]
    _, open_keys = _collect_option_series_events([sell, close], 2025)
    assert len(_detect_zufluss_unmatched([sell, close], 2025, open_keys)) == 1
    print("  OK  FIFO: kein Split-Match ohne belastbare Kostenbasis")


def test_fifo_exact_conid_uses_contract_quantity():
    """Bei unveraenderten Terms darf eine abweichende cost keine Restposition erzeugen."""
    sell = _series_row(
        accountId='TEST', conid='661282172',
        symbol='ABR   240419C00016000', underlyingSymbol='ABR',
        strike='16', putCall='C', quantity='-3', cost='-93',
        reportDate='2024-02-16',
    )
    close = _series_row(
        accountId='TEST', conid='661282172',
        symbol='ABR   240419C00016000', underlyingSymbol='ABR',
        strike='16', putCall='C', buySell='BUY', quantity='3',
        cost='92.400336', fifoPnlRealized='92.400336',
        reportDate='2024-04-19',
    )
    prior, current, matches = _run_fifo_on([sell, close], tax_year=2024)
    assert prior == []
    assert current == []
    assert matches == []
    print("  OK  FIFO: exakte conid-Serie schliesst nach Stückzahl, nicht cost")


def test_fifo_occ_rename_family_fallback():
    """TC33-Analog: MMM1-Buyback matcht Original-MMM-SELL inkl. Tracking."""
    sell = _series_row(underlyingSymbol='MMM', symbol='MMM 250620P95',
                       reportDate='2024-12-01', quantity='-1')
    close = _series_row(underlyingSymbol='MMM1', symbol='MMM1 250620P95',
                        buySell='BUY', fifoPnlRealized='128.24',
                        reportDate='2025-03-03', quantity='1')
    prior, current, renames = _run_fifo_on([sell, close])
    assert prior == [(_option_key(sell), sell, 1)]
    assert current == []
    assert len(renames) == 1
    assert renames[0]['sell_underlying'] == 'MMM'
    assert renames[0]['close_underlying'] == 'MMM1'
    assert renames[0]['quantity'] == 1
    print("  OK  FIFO: OCC-Umbenennung via Familien-Fallback + Tracking")


def test_fifo_exact_key_priority_over_family():
    """TC34-Analog: Close konsumiert die exakte Serie vor der Schwester-Serie."""
    sell_orig = _series_row(underlyingSymbol='MMM', reportDate='2024-12-01',
                            quantity='-1')
    sell_renamed = _series_row(underlyingSymbol='MMM1', reportDate='2024-12-02',
                               quantity='-1')
    close_renamed = _series_row(underlyingSymbol='MMM1', buySell='BUY',
                                fifoPnlRealized='50.0',
                                reportDate='2025-02-02', quantity='1')
    prior, current, renames = _run_fifo_on([sell_orig, sell_renamed, close_renamed])
    # Exakte Serie (MMM1) wird konsumiert, Original-MMM bleibt unberuehrt offen
    # (Vorjahres-SELL, kein Zufluss im Steuerjahr) — kein Familien-Fallback.
    assert prior == [(_option_key(sell_renamed), sell_renamed, 1)]
    assert renames == []
    assert current == []
    print("  OK  FIFO: Exact-Key-Prioritaet vor Familien-Fallback")


def test_unmatched_close_without_history_is_warned_once():
    """Buyback/Verfall ohne Eroeffnungs-SELL → genau eine Warnung pro Serie."""
    buyback = _series_row(buySell='BUY', fifoPnlRealized='75.0',
                          reportDate='2025-02-02', quantity='1')
    verfall = _series_row(transactionType='BookTrade', buySell='BUY',
                          fifoPnlRealized='55.0', reportDate='2025-06-20',
                          quantity='2')
    assignment = _series_row(transactionType='BookTrade', buySell='BUY',
                             reportDate='2025-06-20', quantity='1')  # PnL≈0
    result = _detect_zufluss_unmatched([buyback, verfall, assignment], 2025, set())
    assert len(result) == 1, result
    assert result[0]['underlyingSymbol'] == 'TLT'
    print("  OK  unmatched: Warnung dedupliziert, Assignment ausgenommen")


def test_unmatched_respects_exact_and_family_open_keys():
    """Offener SELL der exakten Serie ODER der OCC-Familie unterdrueckt die Warnung."""
    close = _series_row(underlyingSymbol='MMM1', buySell='BUY',
                        fifoPnlRealized='75.0', reportDate='2025-02-02',
                        quantity='1')
    exact_key = _option_key(close)
    family_original = ('OPT', 'MMM', close['strike'], close['expiry'], 'P')
    assert _detect_zufluss_unmatched([close], 2025, {exact_key}) == []
    assert _detect_zufluss_unmatched([close], 2025, {family_original}) == []
    assert len(_detect_zufluss_unmatched([close], 2025, set())) == 1
    print("  OK  unmatched: Exact- und Familien-Match unterdruecken Warnung")


if __name__ == '__main__':
    print("Options-Event-Sammlung (reine Kerne)")
    test_valid_call_and_put_assignments_detected()
    test_non_assignments_excluded()
    test_date_fallback_chain_and_original_order()
    test_small_pnl_tolerance()
    test_occ_family_key_groups_renamed_opt_series_only()
    test_series_events_classification()
    test_series_events_separate_underlyings_same_strike()
    test_fifo_open_current_year_sell_is_zufluss()
    test_fifo_prior_year_sell_closed_in_tax_year()
    test_fifo_assignment_consumes_without_callback()
    test_fifo_prior_year_closed_short_stays_done()
    test_fifo_same_year_partial_close()
    test_fifo_split_uses_conid_and_cost_basis()
    test_fifo_split_partial_close_leaves_proportional_open_premium()
    test_fifo_cross_year_split_corrects_full_prior_premium()
    test_fifo_same_terms_different_conid_stay_separate()
    test_fifo_changed_terms_without_cost_evidence_do_not_match()
    test_fifo_exact_conid_uses_contract_quantity()
    test_fifo_occ_rename_family_fallback()
    test_fifo_exact_key_priority_over_family()
    test_unmatched_close_without_history_is_warned_once()
    test_unmatched_respects_exact_and_family_open_keys()
    print("Alle Tests bestanden.")
