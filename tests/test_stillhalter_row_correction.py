#!/usr/bin/env python3
"""Unit-Tests fuer _apply_stillhalter_row_correction (FP-Audit R5).

Der Helper ist der gemeinsame Kern der beiden Stillhalter-Apply-Loops
(Same-Year und Cross-Year) in calculate_tax. Er braucht keinerlei Dateien:
reine Row-Mutation mit Rueckgabewert — genau das sichern diese Tests ab.
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (
    _apply_stillhalter_row_correction,
    _split_stillhalter_correction,
)


def test_eur_base_long_position():
    """EUR-Base, Long-Lot (cost >= 0): Kostenbasis rauf, PnL runter."""
    row = {
        'cost': 9500.0,          # IBKR: (strike - praemie) * qty
        'fifoPnlRealized': 300.0,
        'fxRateToBase': 0.92,
        'pnl_eur': round(300.0 * 0.92, 5),
        'dateTime': '2024-06-03 12:00:00',
    }
    correction_eur = _apply_stillhalter_row_correction(row, 200.0, 'EUR', {})

    assert row['cost'] == 9700.0, row['cost']
    assert row['fifoPnlRealized'] == 100.0, row['fifoPnlRealized']
    assert row['pnl_eur'] == round(100.0 * 0.92, 5), row['pnl_eur']
    assert row['stillhalter_adjustment_raw'] == 200.0
    assert row['stillhalter_adjusted'] is True
    assert abs(correction_eur - (300.0 * 0.92 - 100.0 * 0.92)) < 1e-9
    print("  OK  EUR-Base Long: Cost-Restore, PnL-Reduktion, correction_eur")


def test_eur_base_short_position_negative_cost():
    """Short-Lot (cost < 0): Betrag der Kostenbasis waechst in die Gegenrichtung."""
    row = {
        'cost': -4800.0,
        'fifoPnlRealized': -50.0,
        'fxRateToBase': 1.0,
        'pnl_eur': -50.0,
        'dateTime': '2024-06-03 12:00:00',
    }
    _apply_stillhalter_row_correction(row, 120.0, 'EUR', {})
    assert row['cost'] == -4920.0, row['cost']
    assert row['fifoPnlRealized'] == -170.0
    assert row['pnl_eur'] == -170.0
    print("  OK  EUR-Base Short: negative Kostenbasis korrekt erweitert")


def test_usd_base_uses_trade_date_rate():
    """USD-Base: pnl_eur nutzt den USD→EUR-Kurs des Trade-Datums."""
    rates = {date(2024, 6, 3): 0.9, date(2024, 6, 4): 0.5}
    row = {
        'cost': 1000.0,
        'fifoPnlRealized': 100.0,
        'fxRateToBase': 1.0,     # USD-Trade in USD-Base-Konto
        'pnl_eur': 90.0,
        'dateTime': '2024-06-03 12:00:00',
    }
    correction_eur = _apply_stillhalter_row_correction(row, 40.0, 'USD', rates)
    assert row['pnl_eur'] == round(60.0 * 1.0 * 0.9, 5), row['pnl_eur']
    assert abs(correction_eur - (90.0 - 54.0)) < 1e-9
    print("  OK  USD-Base: Tageskurs des Trade-Datums (nicht 06-04)")


def test_adjustment_raw_accumulates_over_multiple_corrections():
    """Zwei Korrekturen derselben Row addieren stillhalter_adjustment_raw."""
    row = {
        'cost': 5000.0,
        'fifoPnlRealized': 500.0,
        'fxRateToBase': 1.0,
        'pnl_eur': 500.0,
        'dateTime': '2024-06-03 12:00:00',
    }
    _apply_stillhalter_row_correction(row, 100.0, 'EUR', {})
    _apply_stillhalter_row_correction(row, 60.0, 'EUR', {})
    assert row['stillhalter_adjustment_raw'] == 160.0
    assert row['fifoPnlRealized'] == 340.0
    assert row['cost'] == 5160.0
    print("  OK  Mehrfach-Korrektur: adjustment_raw akkumuliert")


def test_only_row_is_mutated():
    """Purity-Probe: nur die uebergebene Row wird veraendert."""
    rates = {date(2024, 6, 3): 0.9}
    rates_before = dict(rates)
    row = {
        'cost': 1000.0,
        'fifoPnlRealized': 100.0,
        'fxRateToBase': 1.0,
        'pnl_eur': 90.0,
        'dateTime': '2024-06-03 12:00:00',
    }
    _apply_stillhalter_row_correction(row, 40.0, 'USD', rates)
    assert rates == rates_before, "usd_to_eur_rates darf nicht mutiert werden"
    print("  OK  Purity: Rate-Map unveraendert")


def test_split_gain_first_then_loss():
    """Korrektur groesser als Gewinn: Rest kippt in den Verlust-Bucket."""
    bucket, from_gain, from_loss = _split_stillhalter_correction(
        150.0, 100.0, None, False)
    assert bucket == 'stk'
    assert from_gain == 100.0
    assert from_loss == 50.0
    print("  OK  Split: Gewinn zuerst, Rest in Verlust (Kippfall)")


def test_split_negative_original_all_loss():
    """Row war schon Verlust: komplette Korrektur in den Verlust-Bucket."""
    bucket, from_gain, from_loss = _split_stillhalter_correction(
        80.0, -20.0, None, False)
    assert bucket == 'stk'
    assert from_gain == 0.0
    assert from_loss == 80.0
    print("  OK  Split: negativer Ursprungs-PnL geht voll in Verlust")


def test_split_bucket_routing():
    """Pool-Zuordnung: anlage_so / etf / no_invstg / stk."""
    assert _split_stillhalter_correction(10.0, 5.0, 'anlage_so', True)[0] == 'anlage_so'
    assert _split_stillhalter_correction(10.0, 5.0, 'aktienfonds', True)[0] == 'etf'
    assert _split_stillhalter_correction(10.0, 5.0, None, True)[0] == 'etf'
    # no_invstg gilt unabhaengig davon, ob die ISIN im etf_isins-Set steht
    assert _split_stillhalter_correction(10.0, 5.0, 'no_invstg', True)[0] == 'no_invstg'
    assert _split_stillhalter_correction(10.0, 5.0, 'no_invstg', False)[0] == 'no_invstg'
    assert _split_stillhalter_correction(10.0, 5.0, None, False)[0] == 'stk'
    assert _split_stillhalter_correction(10.0, 5.0, 'anlage_so', False)[0] == 'stk'
    print("  OK  Split: Bucket-Routing (anlage_so/etf/no_invstg/stk)")


if __name__ == '__main__':
    print("Stillhalter-Row-Korrektur (gemeinsamer Apply-Kern)")
    test_eur_base_long_position()
    test_eur_base_short_position_negative_cost()
    test_usd_base_uses_trade_date_rate()
    test_adjustment_raw_accumulates_over_multiple_corrections()
    test_only_row_is_mutated()
    test_split_gain_first_then_loss()
    test_split_negative_original_all_loss()
    test_split_bucket_routing()
    print("Alle Tests bestanden.")
