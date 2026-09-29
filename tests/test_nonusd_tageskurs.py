"""Non-USD lot FX: independent currencies, base conversion and gross buckets."""
import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from calculate_tax_report import calculate_tax
from tests.test_tageskurs_gross_bucket import _write_csv
from ui_model import build_final_values, default_toggles, effective_toggles, toggle_availability


def calculate(currency='CHF', category='STK', cost=1000, pnl=10,
              base='EUR', official=True, short=False, etf=False):
    symbol = 'SPY' if etf else 'TEST'
    common = dict(assetCategory=category, currency=currency, symbol=symbol,
                  subCategory='ETF' if etf else 'COMMON',
                  isin='US78462F1030' if etf else 'CH0000000001',
                  multiplier='1', conid='123', underlyingSymbol=symbol,
                  putCall='C' if category == 'OPT' else '', strike='100',
                  expiry='20250201', quantity='-10' if short else '10')
    # EUR value moves 1.10 -> 1.00. On a USD-base account, rates are
    # currency/USD (2.20 -> 2.00), with USD/EUR fixed at 0.50.
    open_fx = (1.1 if currency != 'EUR' else 1) * (2 if base == 'USD' else 1)
    close_fx = (1 if currency != 'EUR' else 1) * (2 if base == 'USD' else 1)
    opening = dict(common, tradeID='open', transactionID='open',
                   dateTime='2025-01-02 10:00:00', reportDate='2025-01-02',
                   tradeDate='2025-01-02', transactionType='ExchTrade',
                   buySell='SELL' if short else 'BUY', openCloseIndicator='O',
                   cost=str(cost), proceeds=str(-cost), tradePrice='100',
                   fifoPnlRealized='0', fxRateToBase=str(open_fx), ibCommission='0')
    closing = dict(common, tradeID='close', transactionID='close',
                   dateTime='2025-02-01 10:00:00', reportDate='2025-02-01',
                   tradeDate='2025-02-01', transactionType='ExchTrade',
                   buySell='BUY' if short else 'SELL', openCloseIndicator='C',
                   quantity='10' if short else '-10', cost=str(-cost),
                   proceeds=str(cost + pnl), tradePrice='101',
                   fifoPnlRealized=str(pnl), fxRateToBase=str(close_fx), ibCommission='0')
    lot = dict(common, openDateTime=opening['dateTime'], dateTime=closing['dateTime'],
               reportDate=closing['reportDate'], transactionID='open',
               buySell=closing['buySell'], cost=str(cost), fifoPnlRealized=str(pnl),
               fxRateToBase=str(close_fx))
    rates = [dict(reportDate=day, fromCurrency=curr, toCurrency='EUR', rate=str(rate))
             for curr, pair in [(currency, (1.1, 1)), ('USD', (.5, .5))]
             for day, rate in zip(['2025-01-02', '2025-02-01'], pair)]
    with tempfile.TemporaryDirectory() as tmp:
        for name, rows in [('trades', [opening, closing]), ('closed_lots', [lot]),
                           ('account_info', [dict(currency=base, tax_year='2025', fx_transactions_count='0')]),
                           ('conversion_rates', rates if official else [])]:
            _write_csv(str(Path(tmp) / (name + '.csv')), rows)
        with contextlib.redirect_stdout(io.StringIO()), patch(
                'calculate_tax_report.fetch_ecb_rates', return_value={
                    date(2025, 1, 2): .5, date(2025, 2, 1): .5}):
            report = calculate_tax(tmp)
    toggles = effective_toggles(default_toggles(), toggle_availability(report))
    return report, build_final_values(report, toggles)


class NonUsdTageskurs(unittest.TestCase):
    def test_chf_gain_becomes_loss_and_form_gross_fields_reconcile(self):
        report, final = calculate()
        self.assertAlmostEqual(report['fx_correction_by_topf']['Topf1'], -100)
        self.assertAlmostEqual(final['stocks_gain'], 0)
        self.assertAlmostEqual(final['stocks_loss'], -90)
        self.assertAlmostEqual(final['zeile_23'], 90)
        self.assertAlmostEqual(final['topf_1'], -90)
        detail = report['fx_correction_details'][0]
        self.assertEqual(detail['currency'], 'CHF')
        self.assertAlmostEqual(detail['fx_open'], 1.1)
        self.assertAlmostEqual(detail['fx_close'], 1)

    def test_other_currency_has_own_rates_and_fallback(self):
        for currency in ('CHF', 'GBP', 'JPY'):
            for official in (True, False):
                with self.subTest(currency=currency, official=official):
                    report, _ = calculate(currency=currency, official=official)
                    self.assertAlmostEqual(report['fx_correction_total'], -100)

    def test_usd_base_uses_cross_rate_including_gross_bucket(self):
        for official in (True, False):
            report, final = calculate(base='USD', official=official)
            self.assertAlmostEqual(report['fx_correction_total'], -100)
            self.assertAlmostEqual(final['stocks_gain'], 0)
            self.assertAlmostEqual(final['stocks_loss'], -90)

    def test_short_option_signed_cost(self):
        report, _ = calculate(category='OPT', cost=-1000, pnl=-10, short=True)
        self.assertAlmostEqual(report['fx_correction_by_topf']['Topf2'], 100)

    def test_futures_and_zero_pnl_option_assignments_are_excluded(self):
        for category, pnl in [('FUT', 10), ('OPT', 0)]:
            report, _ = calculate(category=category, pnl=pnl)
            self.assertAlmostEqual(report['fx_correction_total'], 0)

    def test_eur_needs_no_correction(self):
        report, _ = calculate(currency='EUR')
        self.assertEqual(report['fx_correction_details'], [])

    def test_fund_correction_keeps_partial_exemption(self):
        report, _ = calculate(etf=True)
        self.assertAlmostEqual(report['fx_correction_by_topf']['KAP-INV'], -100)
        self.assertAlmostEqual(report['fx_correction_kap_inv_taxable'], -70)


if __name__ == '__main__':
    unittest.main()
