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
              extra_trades=(), official_days=('2025-01-02', '2025-02-01')):
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
             for day, rate in zip(['2025-01-02', '2025-02-01'], pair)
             if curr == 'USD' or day in official_days]
    rates += [dict(reportDate=day, fromCurrency=currency, toCurrency='EUR',
                   rate=str(rate)) for day, rate in extra_rates]
    trades = ([closing] if drop_opening else [opening, closing])
    # extra_trades: (ET timestamp, fxRateToBase[, field overrides])
    trades += [{**dict(common, tradeID=f'x{i}', transactionID=f'x{i}',
                       conid=f'x{i}', dateTime=when, tradeDate=when[:10],
                       reportDate=when[:10], transactionType='ExchTrade',
                       buySell='BUY', openCloseIndicator='O', cost='1',
                       proceeds='-1', tradePrice='1', fifoPnlRealized='0',
                       fxRateToBase=str(rate), ibCommission='0'),
                **(rest[0] if rest else {})}
               for i, (when, rate, *rest) in enumerate(extra_trades)]
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

    def test_conversion_rate_gap_keeps_the_purchase_fill_rate(self):
        # ConversionRate covers the currency but not the purchase day. The
        # opening fill's own rate must survive instead of being replaced by
        # the nearest official rate (1.00): otherwise +10 instead of -90.
        for base in ('EUR', 'USD'):
            with self.subTest(base=base):
                report, final = calculate(
                    base=base, official_days=('2025-02-01',),
                    extra_rates=[('2024-12-31', 1.0)])
                detail = report['fx_correction_details'][0]
                self.assertAlmostEqual(detail['fx_open'], 1.1)
                self.assertAlmostEqual(detail['fx_close'], 1.0)
                self.assertAlmostEqual(final['topf_1'], -90)

    def test_cancelled_fills_and_conversions_stay_out_of_the_rate_series(self):
        # A cancelled fill and an automatic currency conversion on the
        # purchase day carry other rates; they must not blend into it.
        report, _ = calculate(official=False, extra_trades=[
            ('2025-01-02 11:00:00', 1.3, {'transactionType': 'TradeCancel'}),
            ('2025-01-02 12:00:00', 1.3, {'assetCategory': 'CASH'}),
        ])
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


# Issue #99: IBKR rechnet ein Ergebnis zum Kurs des Buchungstags um. Bei
# Verfall und Andienung am Freitag mit Buchung am Montag (reportDate) ist das
# der Montagskurs (Lot-fxRateToBase), maßgeblich ist aber der Handelstag.
BOOKING_RATES = {'2025-01-02': 0.95, '2025-01-10': 0.955,
                 '2025-01-31': 0.96, '2025-02-03': 0.97}


def _booked_case(trades, lot):
    rates = [dict(reportDate=day, fromCurrency='USD', toCurrency='EUR',
                  rate=str(rate)) for day, rate in BOOKING_RATES.items()]
    with tempfile.TemporaryDirectory() as tmp:
        for name, rows in [('trades', trades), ('closed_lots', [lot]),
                           ('account_info', [dict(currency='EUR', tax_year='2025',
                                                  fx_transactions_count='0')]),
                           ('conversion_rates', rates)]:
            _write_csv(str(Path(tmp) / (name + '.csv')), rows)
        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(tmp)
    toggles = effective_toggles(default_toggles(), toggle_availability(report))
    return report, build_final_values(report, toggles)


def _usd_row(**fields):
    row = dict(currency='USD', ibCommission='0', multiplier='1',
               subCategory='COMMON', isin='US0000000099', conid='99',
               symbol='BOOK', underlyingSymbol='BOOK', putCall='', strike='',
               expiry='')
    row.update(fields)
    return row


def _expiring_option_case(short):
    """Option eröffnet am 02.01., verfällt Freitag 31.01., gebucht Montag 03.02."""
    side_open, side_close = ('SELL', 'BUY') if short else ('BUY', 'SELL')
    option = dict(assetCategory='OPT', multiplier='100', putCall='C',
                  strike='60', expiry='2025-01-31', conid='991',
                  symbol='BOOK 250131C60')
    cost = -100 if short else 100
    pnl = -cost
    opening = _usd_row(**option, tradeID='open', transactionID='open',
                       transactionType='ExchTrade', buySell=side_open,
                       openCloseIndicator='O',
                       quantity='-1' if short else '1',
                       dateTime='2025-01-02 10:00:00', tradeDate='2025-01-02',
                       reportDate='2025-01-02', tradePrice='1', cost=str(cost),
                       proceeds=str(-cost), fifoPnlRealized='0',
                       fxRateToBase='0.95')
    expiry = _usd_row(**option, tradeID='expiry', transactionID='expiry',
                      transactionType='BookTrade', buySell=side_close,
                      openCloseIndicator='C', quantity='1' if short else '-1',
                      dateTime='2025-01-31 16:20:00', tradeDate='2025-01-31',
                      reportDate='2025-02-03', tradePrice='0', cost=str(-cost),
                      proceeds='0', fifoPnlRealized=str(pnl),
                      fxRateToBase='0.97', notes='Ep')
    lot = _usd_row(**option, openDateTime='2025-01-02 10:00:00',
                   dateTime='2025-01-31 16:20:00', tradeDate='2025-01-31',
                   reportDate='2025-02-03', transactionID='open',
                   buySell=side_close, quantity='-1' if short else '1',
                   cost=str(cost), fifoPnlRealized=str(pnl), fxRateToBase='0.97')
    return _booked_case([opening, expiry], lot)


class BookingRateOnLaterBooking(unittest.TestCase):
    def test_worthless_long_option_is_valued_at_purchase_rate(self):
        # Verlust = gezahlte Prämie zum Kurs des Kauftags: -100 x 0,95.
        # IBKR: -100 x 0,97 (Montag); Korrektur +1 (Basis) +1 (Buchungskurs).
        report, final = _expiring_option_case(short=False)
        detail = report['fx_correction_details'][0]
        self.assertAlmostEqual(detail['fx_close'], 0.96)
        self.assertAlmostEqual(detail['fx_booked'], 0.97)
        self.assertAlmostEqual(detail['booking_rate_delta_eur'], 1.0)
        self.assertAlmostEqual(report['fx_correction_total'], 2.0)
        self.assertAlmostEqual(final['options_loss'], -95.0)
        self.assertAlmostEqual(final['options_gain'], 0.0)

    def test_worthless_short_option_is_valued_at_sale_rate(self):
        # Zufluss der Prämie zum Kurs des Verkaufstags: 100 x 0,95.
        report, final = _expiring_option_case(short=True)
        self.assertAlmostEqual(report['fx_correction_total'], -2.0)
        self.assertAlmostEqual(final['options_gain'], 95.0)
        self.assertAlmostEqual(final['options_loss'], 0.0)

    def test_same_day_booking_needs_no_booking_rate_correction(self):
        report, _ = calculate()
        detail = report['fx_correction_details'][0]
        self.assertAlmostEqual(detail['booking_rate_delta_eur'], 0.0)

    def test_call_assigned_stock_uses_trade_day_after_premium_split(self):
        # 100 Aktien zu 50 (0,95) gekauft, Call 55 für 200 USD am 10.01.
        # (0,955) verkauft, Andienung Freitag, Buchung Montag (0,97).
        # IBKR legt die Prämie in den Aktienerlös (PnL 700). Steuerlich:
        # Aktie 5500 x 0,96 - 5000 x 0,95 = 530, Prämie 200 x 0,955 = 191.
        stock = dict(assetCategory='STK', conid='99', symbol='BOOK')
        call = dict(assetCategory='OPT', multiplier='100', putCall='C',
                    strike='55', expiry='2025-01-31', conid='992',
                    symbol='BOOK 250131C55')
        trades = [
            _usd_row(**stock, tradeID='buy', transactionID='buy',
                     transactionType='ExchTrade', buySell='BUY',
                     openCloseIndicator='O', quantity='100',
                     dateTime='2025-01-02 10:00:00', tradeDate='2025-01-02',
                     reportDate='2025-01-02', tradePrice='50', cost='5000',
                     proceeds='-5000', fifoPnlRealized='0', fxRateToBase='0.95'),
            _usd_row(**call, tradeID='call_sell', transactionID='call_sell',
                     transactionType='ExchTrade', buySell='SELL',
                     openCloseIndicator='O', quantity='-1',
                     dateTime='2025-01-10 10:00:00', tradeDate='2025-01-10',
                     reportDate='2025-01-10', tradePrice='2', cost='-200',
                     proceeds='200', fifoPnlRealized='0', fxRateToBase='0.955'),
            _usd_row(**call, tradeID='call_assign', transactionID='call_assign',
                     transactionType='BookTrade', buySell='BUY',
                     openCloseIndicator='C', quantity='1',
                     dateTime='2025-01-31 16:20:00', tradeDate='2025-01-31',
                     reportDate='2025-02-03', tradePrice='0', cost='200',
                     proceeds='0', fifoPnlRealized='0', fxRateToBase='0.97',
                     notes='A'),
            _usd_row(**stock, tradeID='stock_sell', transactionID='stock_sell',
                     transactionType='BookTrade', buySell='SELL',
                     openCloseIndicator='C', quantity='-100',
                     dateTime='2025-01-31 16:20:00', tradeDate='2025-01-31',
                     reportDate='2025-02-03', tradePrice='55', cost='-5000',
                     proceeds='5700', fifoPnlRealized='700', fxRateToBase='0.97',
                     notes='A'),
        ]
        lot = _usd_row(**stock, openDateTime='2025-01-02 10:00:00',
                       dateTime='2025-01-31 16:20:00', tradeDate='2025-01-31',
                       reportDate='2025-02-03', transactionID='buy',
                       buySell='SELL', quantity='100', cost='5000',
                       fifoPnlRealized='700', fxRateToBase='0.97')
        report, final = _booked_case(trades, lot)
        detail = report['fx_correction_details'][0]
        self.assertAlmostEqual(detail['stillhalter_adjustment_raw'], 200)
        self.assertAlmostEqual(detail['booking_rate_delta_eur'], -5.0)
        self.assertAlmostEqual(final['stocks_gain'], 530.0)
        self.assertAlmostEqual(final['options_gain'], 191.0)


if __name__ == '__main__':
    unittest.main()
