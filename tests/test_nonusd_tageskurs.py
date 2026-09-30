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
              base='EUR', official=True, short=False, etf=False,
              open_time='2025-01-02 10:00:00', close_time='2025-02-01 10:00:00',
              extra_rates=(), lot_conid='123', drop_opening=False,
              lot_open_time=None, lot_tx='open', lot_trade_date='',
              extra_trades=()):
    # open_time/close_time are IBKR's ET timestamps; tradeDate stays the
    # local trading day (2025-01-02 / 2025-02-01), as on Asian listings.
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
                   dateTime=open_time, reportDate='2025-01-02',
                   tradeDate='2025-01-02', transactionType='ExchTrade',
                   buySell='SELL' if short else 'BUY', openCloseIndicator='O',
                   cost=str(cost), proceeds=str(-cost), tradePrice='100',
                   fifoPnlRealized='0', fxRateToBase=str(open_fx), ibCommission='0')
    closing = dict(common, tradeID='close', transactionID='close',
                   dateTime=close_time, reportDate='2025-02-01',
                   tradeDate='2025-02-01', transactionType='ExchTrade',
                   buySell='BUY' if short else 'SELL', openCloseIndicator='C',
                   quantity='10' if short else '-10', cost=str(-cost),
                   proceeds=str(cost + pnl), tradePrice='101',
                   fifoPnlRealized=str(pnl), fxRateToBase=str(close_fx), ibCommission='0')
    lot = dict(common, conid=lot_conid,
               openDateTime=lot_open_time or opening['dateTime'],
               dateTime=closing['dateTime'], tradeDate=lot_trade_date,
               reportDate=closing['reportDate'], transactionID=lot_tx,
               buySell=closing['buySell'], cost=str(cost), fifoPnlRealized=str(pnl),
               fxRateToBase=str(close_fx))
    rates = [dict(reportDate=day, fromCurrency=curr, toCurrency='EUR', rate=str(rate))
             for curr, pair in [(currency, (1.1, 1)), ('USD', (.5, .5))]
             for day, rate in zip(['2025-01-02', '2025-02-01'], pair)]
    rates += [dict(reportDate=day, fromCurrency=currency, toCurrency='EUR',
                   rate=str(rate)) for day, rate in extra_rates]
    trades = ([closing] if drop_opening else [opening, closing])
    trades += [dict(common, tradeID=f'x{i}', transactionID=f'x{i}',
                    conid=f'x{i}', dateTime=when, tradeDate=when[:10],
                    reportDate=when[:10], transactionType='ExchTrade',
                    buySell='BUY', openCloseIndicator='O', cost='1',
                    proceeds='-1', tradePrice='1', fifoPnlRealized='0',
                    fxRateToBase=str(rate), ibCommission='0')
               for i, (when, rate) in enumerate(extra_trades)]
    with tempfile.TemporaryDirectory() as tmp:
        for name, rows in [('trades', trades), ('closed_lots', [lot]),
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

    def test_futures_cfds_and_zero_pnl_options_are_excluded(self):
        # FUT/CFD: the lot cost is a notional that never changes hands.
        for category, pnl in [('FUT', 10), ('CFD', 10), ('OPT', 0)]:
            report, _ = calculate(category=category, pnl=pnl)
            self.assertAlmostEqual(report['fx_correction_total'], 0)

    def test_trade_date_not_et_calendar_day_selects_the_rate(self):
        # Asian listing: both fills happen on an ET evening, i.e. on the
        # next local trading day. The ET days carry misleading rates.
        report, final = calculate(
            open_time='2025-01-01 21:00:00', close_time='2025-01-31 21:00:00',
            extra_rates=[('2025-01-01', 1.3), ('2025-01-31', 0.8)])
        detail = report['fx_correction_details'][0]
        self.assertEqual(detail['fx_open_date'], '2025-01-02')
        self.assertEqual(detail['fx_close_date'], '2025-02-01')
        self.assertAlmostEqual(detail['fx_open'], 1.1)
        self.assertAlmostEqual(detail['fx_close'], 1.0)
        self.assertAlmostEqual(report['fx_correction_total'], -100)
        self.assertAlmostEqual(final['topf_1'], -90)

    def test_lot_transaction_id_resolves_trade_date_after_conid_change(self):
        report, _ = calculate(open_time='2025-01-01 21:00:00',
                              extra_rates=[('2025-01-01', 1.3)],
                              lot_conid='999')
        detail = report['fx_correction_details'][0]
        self.assertEqual(detail['fx_open_date'], '2025-01-02')
        self.assertAlmostEqual(detail['fx_open'], 1.1)

    def test_synthetic_lot_timestamp_resolves_via_transaction_id(self):
        # IBKR sometimes stamps the lot with a time no fill carries.
        report, _ = calculate(open_time='2025-01-01 21:00:00',
                              lot_open_time='2025-01-01 21:26:00',
                              extra_rates=[('2025-01-01', 1.3)])
        detail = report['fx_correction_details'][0]
        self.assertEqual(detail['fx_open_date'], '2025-01-02')
        self.assertAlmostEqual(detail['fx_open'], 1.1)

    def test_transaction_id_of_a_fill_on_another_day_is_ignored(self):
        report, _ = calculate(open_time='2025-01-01 21:00:00',
                              extra_rates=[('2025-01-01', 1.3)],
                              drop_opening=True, lot_tx='close')
        detail = report['fx_correction_details'][0]
        self.assertEqual(detail['fx_open_date'], '2025-01-01')

    def test_lot_trade_date_decides_close_day_without_closing_fill(self):
        report, _ = calculate(close_time='2025-01-31 21:00:00',
                              extra_rates=[('2025-01-31', 0.8)],
                              lot_conid='999', lot_trade_date='2025-02-01')
        detail = report['fx_correction_details'][0]
        self.assertEqual(detail['fx_close_date'], '2025-02-01')
        self.assertAlmostEqual(detail['fx_close'], 1.0)

    def test_fallback_series_is_keyed_by_trade_date(self):
        # Without ConversionRate the opening rate comes from the trades.
        # Another fill booked on the ET day must not blend into the rate.
        report, _ = calculate(official=False, open_time='2025-01-01 21:00:00',
                              extra_trades=[('2025-01-01 10:00:00', 1.3)])
        detail = report['fx_correction_details'][0]
        self.assertAlmostEqual(detail['fx_open'], 1.1)
        self.assertAlmostEqual(report['fx_correction_total'], -100)

    def test_without_opening_fill_the_timestamp_day_is_the_fallback(self):
        # Prior-year lot without history: no fill to read tradeDate from.
        report, _ = calculate(open_time='2025-01-01 21:00:00',
                              extra_rates=[('2025-01-01', 1.3)],
                              drop_opening=True)
        detail = report['fx_correction_details'][0]
        self.assertEqual(detail['fx_open_date'], '2025-01-01')
        self.assertAlmostEqual(detail['fx_open'], 1.3)

    def test_eur_needs_no_correction(self):
        report, _ = calculate(currency='EUR')
        self.assertEqual(report['fx_correction_details'], [])

    def test_fund_correction_keeps_partial_exemption(self):
        report, _ = calculate(etf=True)
        self.assertAlmostEqual(report['fx_correction_by_topf']['KAP-INV'], -100)
        self.assertAlmostEqual(report['fx_correction_kap_inv_taxable'], -70)


if __name__ == '__main__':
    unittest.main()
