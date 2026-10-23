"""Flex Queries without the trade fields the tax logic needs fail loudly.

Real exports from a reduced Flex Query lacked symbol, underlyingSymbol and
conid on every trade. Assignments could then not be linked to the delivered
stock, and put premiums were taxed twice (as Stillhalterprämie and inside
the stock or fund result) without any error. Such exports are now rejected
in every extraction path, before any file is written.
"""
import contextlib
import io
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract_ibkr_data import (
    extract_fx_multi_xml,
    extract_quarterly_xmls,
    parse_ibkr_xml,
)

ACCOUNT = '<AccountInformation accountId="U123" currency="EUR" />'
STOCK = ('<Trade levelOfDetail="EXECUTION" assetCategory="STK" '
         'currency="USD" fxRateToBase="0.9" symbol="SYN" conid="1" '
         'dateTime="2024-03-01 10:00:00" tradeDate="2024-03-01" '
         'buySell="BUY" quantity="10" />')
OPTION = ('<Trade levelOfDetail="EXECUTION" assetCategory="OPT" '
          'currency="USD" fxRateToBase="0.9" symbol="SYN 240315P00010000" '
          'conid="2" underlyingSymbol="SYN" putCall="P" strike="10" '
          'dateTime="2024-03-01 10:00:00" tradeDate="2024-03-01" '
          'buySell="SELL" quantity="-1" />')


def reduced(row, *fields):
    for field in fields:
        row = row.replace(f' {field}="', f' dropped_{field}="')
    return row


def write_xml(tmp, name, trades, from_date='2024-01-01',
              to_date='2024-12-31'):
    path = os.path.join(tmp, name)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(f"""<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse><FlexStatements count="1">
  <FlexStatement accountId="U123" fromDate="{from_date}" toDate="{to_date}">
    {ACCOUNT}<Trades>{trades}</Trades>
  </FlexStatement>
</FlexStatements></FlexQueryResponse>
""")
    return path


class RequiredTradeFields(unittest.TestCase):
    def extract(self, path):
        out = os.path.join(os.path.dirname(path), 'out')
        os.makedirs(out, exist_ok=True)
        with contextlib.redirect_stdout(io.StringIO()):
            parse_ibkr_xml(path, out)
        return out

    def test_reduced_template_is_rejected_before_writing(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = (reduced(STOCK, 'symbol', 'conid')
                    + reduced(OPTION, 'symbol', 'conid', 'underlyingSymbol'))
            path = write_xml(tmp, 'x.xml', rows)
            out = os.path.join(tmp, 'out')
            os.makedirs(out)
            with contextlib.redirect_stdout(io.StringIO()), \
                    self.assertRaises(ValueError) as caught:
                parse_ibkr_xml(path, out)
            message = str(caught.exception)
            for label in ('Symbol', 'Conid', 'Underlying Symbol', 'x.xml'):
                self.assertIn(label, message)
            self.assertEqual(os.listdir(out), [])

    def test_option_rows_need_underlying_symbol(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = write_xml(tmp, 'x.xml', STOCK + reduced(
                OPTION, 'underlyingSymbol'))
            with self.assertRaisesRegex(ValueError, 'Underlying Symbol'):
                self.extract(path)

    def test_stock_only_export_needs_no_underlying_symbol(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = self.extract(write_xml(tmp, 'x.xml', STOCK))
            self.assertTrue(os.path.exists(os.path.join(out, 'trades.csv')))

    def test_a_single_row_without_the_field_is_not_a_reduced_query(self):
        # e.g. a TradeCancel row: the field was selected in the query.
        with tempfile.TemporaryDirectory() as tmp:
            out = self.extract(write_xml(
                tmp, 'x.xml', STOCK + reduced(STOCK, 'conid') + OPTION))
            self.assertTrue(os.path.exists(os.path.join(out, 'trades.csv')))

    def test_quarterly_merge_rejects_a_reduced_quarter(self):
        with tempfile.TemporaryDirectory() as tmp:
            q1 = write_xml(tmp, 'q1.xml', STOCK, to_date='2024-03-31')
            q2 = write_xml(tmp, 'q2.xml', reduced(STOCK, 'symbol', 'conid'),
                           from_date='2024-04-01', to_date='2024-06-30')
            out = os.path.join(tmp, 'out')
            os.makedirs(out)
            with contextlib.redirect_stdout(io.StringIO()), \
                    self.assertRaisesRegex(ValueError, 'q2.xml'):
                extract_quarterly_xmls([q1, q2], out)

    def test_reduced_history_file_is_rejected_not_logged(self):
        # The history loop only logged its errors; a reduced prior-year
        # export used to corrupt cross-year Stillhalter matching silently.
        with tempfile.TemporaryDirectory() as tmp:
            main = write_xml(tmp, 'main.xml', STOCK.replace('2024', '2025'),
                             '2025-01-01', '2025-12-31')
            history = write_xml(tmp, 'history.xml', reduced(
                OPTION, 'symbol', 'conid', 'underlyingSymbol'))
            out = os.path.join(tmp, 'out')
            os.makedirs(out)
            with contextlib.redirect_stdout(io.StringIO()), \
                    self.assertRaisesRegex(ValueError, 'history.xml'):
                extract_fx_multi_xml([main, history], out)


if __name__ == '__main__':
    unittest.main()
