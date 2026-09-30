"""Exports without AccountInformation: no silent tax year / base currency.

Two real exports lack the AccountInformation section. The extraction used to
write no account_info.csv for them, and the core then fell back to tax year
2025 in EUR without any warning: a 2024 export became an almost empty 2025
report. Tax year now always comes from the FlexStatement period and the base
currency from the booking data; if that is impossible, extraction fails.
"""
import contextlib
import csv
import io
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import calculate_tax
from extract_ibkr_data import extract_quarterly_xmls, parse_ibkr_xml


def write_xml(tmp, name, body, from_date='2024-01-01', to_date='2024-12-31',
              account_info=''):
    path = os.path.join(tmp, name)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(f"""<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse>
  <FlexStatements count="1">
    <FlexStatement accountId="U123" fromDate="{from_date}" toDate="{to_date}">
      {account_info}
      {body}
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
""")
    return path


def dividend(currency='EUR', day='2024-03-01', tx='D1', amount='100'):
    return (f'<StatementOfFundsLine levelOfDetail="BaseCurrency" '
            f'currency="{currency}" activityCode="DIV" '
            f'activityDescription="SYN Cash Dividend" date="{day}" '
            f'reportDate="{day}" transactionID="{tx}" amount="{amount}" '
            f'fxRateToBase="1" />')


def funds(*rows):
    return '<StmtFunds>' + ''.join(rows) + '</StmtFunds>'


def read_account_info(out_dir):
    with open(os.path.join(out_dir, 'account_info.csv'), newline='',
              encoding='utf-8') as f:
        return list(csv.DictReader(f))[0]


def extract(xml_path, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    with contextlib.redirect_stdout(io.StringIO()):
        parse_ibkr_xml(xml_path, out_dir)


class AccountInfoFallback(unittest.TestCase):
    def test_missing_section_keeps_tax_year_and_base_currency(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, 'out')
            extract(write_xml(tmp, 'x.xml', funds(dividend())), out)
            info = read_account_info(out)
            self.assertEqual(info['tax_year'], '2024')
            self.assertEqual(info['currency'], 'EUR')
            self.assertEqual(info['account_info_source'], 'derived')
            self.assertEqual(info['accountId'], 'U123')
            with contextlib.redirect_stdout(io.StringIO()):
                report = calculate_tax(out)
            # Before the fix: tax year 2025 and a dividend of 0.
            self.assertEqual(report['tax_year'], 2024)
            self.assertAlmostEqual(report['dividends_eur'], 100)

    def test_usd_base_currency_comes_from_base_currency_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, 'out')
            extract(write_xml(tmp, 'x.xml', funds(dividend('USD'))), out)
            self.assertEqual(read_account_info(out)['currency'], 'USD')

    def test_trades_at_rate_one_identify_the_base_currency(self):
        trade = ('<Trades><Trade levelOfDetail="EXECUTION" '
                 'assetCategory="STK" currency="USD" fxRateToBase="1" '
                 'symbol="SYN" dateTime="2024-03-01 10:00:00" '
                 'tradeDate="2024-03-01" /></Trades>')
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, 'out')
            extract(write_xml(tmp, 'x.xml', trade), out)
            self.assertEqual(read_account_info(out)['currency'], 'USD')

    def test_undeterminable_base_currency_fails_before_writing(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, 'out')
            os.makedirs(out)
            path = write_xml(tmp, 'x.xml', '<StmtFunds />')
            with contextlib.redirect_stdout(io.StringIO()), \
                    self.assertRaisesRegex(ValueError, 'Basiswährung'):
                parse_ibkr_xml(path, out)
            self.assertEqual(os.listdir(out), [])

    def test_existing_section_is_used_unchanged(self):
        acct = ('<AccountInformation accountId="U123" currency="EUR" '
                'name="Synthetic" />')
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, 'out')
            extract(write_xml(tmp, 'x.xml', funds(dividend('USD')),
                              account_info=acct), out)
            info = read_account_info(out)
            self.assertEqual(info['currency'], 'EUR')
            self.assertNotIn('account_info_source', info)

    def test_quarterly_merge_without_section_writes_account_info(self):
        with tempfile.TemporaryDirectory() as tmp:
            q1 = write_xml(tmp, 'q1.xml', funds(dividend(tx='Q1')),
                           to_date='2024-03-31')
            q2 = write_xml(tmp, 'q2.xml',
                           funds(dividend(day='2024-05-02', tx='Q2')),
                           from_date='2024-04-01', to_date='2024-06-30')
            out = os.path.join(tmp, 'out')
            os.makedirs(out)
            with contextlib.redirect_stdout(io.StringIO()):
                extract_quarterly_xmls([q1, q2], out)
            info = read_account_info(out)
            self.assertEqual((info['tax_year'], info['currency']),
                             ('2024', 'EUR'))

    def test_quarterly_merge_rejects_mixed_base_currencies(self):
        with tempfile.TemporaryDirectory() as tmp:
            q1 = write_xml(tmp, 'q1.xml', funds(dividend(tx='Q1')),
                           to_date='2024-03-31')
            q2 = write_xml(tmp, 'q2.xml',
                           funds(dividend('USD', '2024-05-02', 'Q2')),
                           from_date='2024-04-01', to_date='2024-06-30')
            out = os.path.join(tmp, 'out')
            os.makedirs(out)
            with contextlib.redirect_stdout(io.StringIO()), \
                    self.assertRaisesRegex(ValueError, 'Basiswährungen'):
                extract_quarterly_xmls([q1, q2], out)


if __name__ == '__main__':
    unittest.main()
