#!/usr/bin/env python3
"""Unit-Tests fuer _dedupe_trades / _dedupe_funds (FP-Audit R1).

Reine Funktionen ohne Dateisystem — Fixtures sind In-Memory-Row-Dicts wie
sie load_csv liefern wuerde.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import _dedupe_funds, _dedupe_trades


def test_trades_dedupe_by_trade_id():
    """Gleiche tradeID = Duplikat, auch wenn andere Felder abweichen."""
    rows = [
        {'tradeID': 'T1', 'dateTime': '2025-01-02 10:00:00', 'quantity': '10'},
        {'tradeID': 'T1', 'dateTime': '2025-01-02 10:00:00', 'quantity': '10'},
        {'tradeID': 'T2', 'dateTime': '2025-01-02 10:00:00', 'quantity': '10'},
    ]
    trades, dups = _dedupe_trades(rows)
    assert len(trades) == 2 and dups == 1
    assert trades[0] is rows[0] and trades[1] is rows[2], "erste Row gewinnt"
    print("  OK  Trades: tradeID-Dedupe, erste Row gewinnt")


def test_trades_partial_fills_with_same_attributes_survive():
    """Partial Fills: identische Attribute, verschiedene tradeIDs — KEIN Dedupe."""
    fill = {'dateTime': '2025-03-03 15:30:00', 'isin': 'US0000000001',
            'buySell': 'BUY', 'quantity': '5', 'closePrice': '100',
            'fifoPnlRealized': '0'}
    rows = [dict(fill, tradeID='A1'), dict(fill, tradeID='A2')]
    trades, dups = _dedupe_trades(rows)
    assert len(trades) == 2 and dups == 0
    print("  OK  Trades: Partial Fills mit eigenen tradeIDs bleiben erhalten")


def test_trades_composite_key_fallback_without_trade_id():
    """Ohne tradeID greift der Composite-Key (Basis-Flex-Query)."""
    fill = {'tradeID': '', 'dateTime': '2025-03-03 15:30:00',
            'isin': 'US0000000001', 'buySell': 'BUY', 'quantity': '5',
            'closePrice': '100', 'fifoPnlRealized': '0'}
    other = dict(fill, quantity='7')
    trades, dups = _dedupe_trades([fill, dict(fill), other])
    assert len(trades) == 2 and dups == 1
    print("  OK  Trades: Composite-Key-Fallback ohne tradeID")


def test_funds_dedupe_key_includes_activity_description():
    """Gleiche transactionID, verschiedene Aktivitaeten = beide legitim."""
    rows = [
        {'transactionID': 'X9', 'activityDescription': 'Borrow Fees',
         'amount': '-1.5'},
        {'transactionID': 'X9', 'activityDescription': 'SYEP Interest',
         'amount': '0.3'},
        {'transactionID': 'X9', 'activityDescription': 'Borrow Fees',
         'amount': '-1.5'},
    ]
    funds, dups = _dedupe_funds(rows)
    assert len(funds) == 2 and dups == 1
    print("  OK  Funds: (transactionID, activityDescription)-Key")


def test_funds_without_transaction_id_use_full_row():
    """Ohne transactionID zaehlt die komplette Row als Key."""
    a = {'transactionID': '', 'activityDescription': 'Dividende', 'amount': '10'}
    b = {'transactionID': '', 'activityDescription': 'Dividende', 'amount': '20'}
    funds, dups = _dedupe_funds([a, dict(a), b])
    assert len(funds) == 2 and dups == 1
    print("  OK  Funds: Full-Row-Key ohne transactionID")


def test_inputs_not_mutated():
    """Purity-Probe: Eingabelisten und Rows bleiben unveraendert."""
    rows = [{'tradeID': 'T1', 'quantity': '1'}, {'tradeID': 'T1', 'quantity': '1'}]
    snapshot = [dict(r) for r in rows]
    _dedupe_trades(rows)
    assert rows == snapshot and len(rows) == 2
    frows = [{'transactionID': 'F1', 'activityDescription': 'x'}]
    fsnapshot = [dict(r) for r in frows]
    _dedupe_funds(frows)
    assert frows == fsnapshot
    print("  OK  Purity: Eingaben unveraendert")


if __name__ == '__main__':
    print("Trade-/Funds-Dedupe (reine Kerne)")
    test_trades_dedupe_by_trade_id()
    test_trades_partial_fills_with_same_attributes_survive()
    test_trades_composite_key_fallback_without_trade_id()
    test_funds_dedupe_key_includes_activity_description()
    test_funds_without_transaction_id_use_full_row()
    test_inputs_not_mutated()
    print("Alle Tests bestanden.")
