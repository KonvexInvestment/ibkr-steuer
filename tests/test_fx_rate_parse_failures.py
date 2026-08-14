#!/usr/bin/env python3
"""Unit-Tests fuer sichtbare Parse-Ausfaelle (FP-Audit R4).

parse_date faengt nur noch erwartbare Fehler; get_exchange_rates liefert
unparsbare EUR-Zeilen als Zaehler zurueck statt sie still zu verschlucken.
"""
import csv
import os
import sys
import tempfile
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import get_exchange_rates, load_csv, parse_date


def test_parse_date_contract():
    """Gueltige Formate parsen, Muell und None liefern None."""
    assert parse_date('2025-03-07') == date(2025, 3, 7)
    assert parse_date('2025-03-07 20:20:00') == date(2025, 3, 7)
    assert parse_date('20250307') == date(2025, 3, 7)
    assert parse_date('20250307;202000') == date(2025, 3, 7)
    assert parse_date('kein-datum') is None
    assert parse_date('') is None
    assert parse_date(None) is None
    print("  OK  parse_date: Formate + None-Kontrakt")


def test_valid_rows_build_rates_without_failures():
    trades = [{'currency': 'EUR', 'fxRateToBase': '1.10',
               'dateTime': '2025-02-03 10:00:00'}]
    funds = [{'currency': 'EUR', 'fxRateToBase': '1.08',
              'date': '2025-02-04'}]
    rates, failures = get_exchange_rates(trades, funds)
    assert abs(rates[date(2025, 2, 3)] - 1 / 1.10) < 1e-9
    assert abs(rates[date(2025, 2, 4)] - 1 / 1.08) < 1e-9
    assert failures == {'funds': 0, 'trades': 0}
    print("  OK  get_exchange_rates: valide Zeilen, keine Ausfaelle")


def test_compact_rows_build_rates_without_failures():
    trades = [{'currency': 'EUR', 'fxRateToBase': '1.10',
               'dateTime': '20250203;100000'}]
    funds = [{'currency': 'EUR', 'fxRateToBase': '1.08',
              'date': '20250204'}]
    rates, failures = get_exchange_rates(trades, funds)
    assert abs(rates[date(2025, 2, 3)] - 1 / 1.10) < 1e-9
    assert abs(rates[date(2025, 2, 4)] - 1 / 1.08) < 1e-9
    assert failures == {'funds': 0, 'trades': 0}
    print("  OK  get_exchange_rates: kompakte IBKR-Standardformate")


def test_unparsable_fx_is_counted_not_swallowed():
    trades = [
        {'currency': 'EUR', 'fxRateToBase': 'kaputt',
         'dateTime': '2025-02-03 10:00:00'},
        {'currency': 'EUR', 'fxRateToBase': '1.10',
         'dateTime': '2025-02-05 10:00:00'},
    ]
    funds = [{'currency': 'EUR', 'fxRateToBase': 'auch-kaputt',
              'date': '2025-02-04'}]
    rates, failures = get_exchange_rates(trades, funds)
    assert failures == {'funds': 1, 'trades': 1}
    assert list(rates.keys()) == [date(2025, 2, 5)], rates
    print("  OK  get_exchange_rates: kaputter Kurs wird gezaehlt")


def test_unparsable_date_is_counted_and_never_stored_as_none_key():
    """Kaputtes Datum: zaehlen, aber KEIN None-Key in der Rate-Map.

    Ein None-Key wuerde get_rate_for_date beim naechsten Datums-Miss an
    sorted() crashen lassen (None < date ist in Python 3 ein TypeError).
    """
    trades = [{'currency': 'EUR', 'fxRateToBase': '1.10',
               'dateTime': 'not-a-date'}]
    funds = [{'currency': 'EUR', 'fxRateToBase': '1.08',
              'date': 'auch-kein-datum'}]
    rates, failures = get_exchange_rates(trades, funds)
    assert failures == {'funds': 1, 'trades': 1}
    assert rates == {}, f"None-Key darf nie gespeichert werden: {rates}"
    print("  OK  get_exchange_rates: kaputtes Datum gezaehlt, kein None-Key")


def test_deliberate_skips_are_not_failures():
    """Bogus-1.0-Buchungen und Out-of-Range-Kurse sind gewollte Filter."""
    funds = [
        {'currency': 'EUR', 'fxRateToBase': '1.0', 'date': '2025-02-04'},
        {'currency': 'EUR', 'fxRateToBase': '9.99', 'date': '2025-02-05'},
        {'currency': 'USD', 'fxRateToBase': 'irrelevant', 'date': '2025-02-06'},
    ]
    rates, failures = get_exchange_rates([], funds)
    assert rates == {}
    assert failures == {'funds': 0, 'trades': 0}
    print("  OK  get_exchange_rates: gewollte Filter zaehlen nicht als Ausfall")


def test_internal_csv_loader_rejects_unknown_date_format():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'trades.csv')
        with open(path, 'w', newline='', encoding='utf-8') as handle:
            writer = csv.DictWriter(handle, fieldnames=['reportDate', 'symbol'])
            writer.writeheader()
            writer.writerow({'reportDate': '07/03/2025', 'symbol': 'TEST'})
        try:
            load_csv(path)
        except ValueError as exc:
            assert 'Nicht unterstütztes IBKR-Datumsformat' in str(exc)
            assert 'reportDate: 1' in str(exc)
        else:
            raise AssertionError('Unbekanntes Datumsformat darf nicht still ausfallen')
    print("  OK  load_csv: unbekanntes Datumsformat bricht sichtbar ab")


if __name__ == '__main__':
    print("FX-Rate-Parse-Ausfaelle (Fehler als Werte)")
    test_parse_date_contract()
    test_valid_rows_build_rates_without_failures()
    test_compact_rows_build_rates_without_failures()
    test_unparsable_fx_is_counted_not_swallowed()
    test_unparsable_date_is_counted_and_never_stored_as_none_key()
    test_deliberate_skips_are_not_failures()
    test_internal_csv_loader_rejects_unknown_date_format()
    print("Alle Tests bestanden.")
