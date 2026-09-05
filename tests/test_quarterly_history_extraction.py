"""Regression tests for quarterly tax-year XMLs with prior-year history."""
import contextlib
from collections import Counter
import csv
import io
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract_ibkr_data import (
    extract_fx_multi_xml,
    extract_quarterly_xmls,
    parse_ibkr_xml,
)


def write_xml(tmp, name, from_date, to_date, body):
    path = os.path.join(tmp, name)
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse>
  <FlexStatements count="1">
    <FlexStatement accountId="U123" fromDate="{from_date}" toDate="{to_date}">
      <AccountInformation accountId="U123" name="Synthetic" currency="EUR" />
      {body}
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(xml)
    return path


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_mixed_period_formats_sort_and_normalize_quarterly_xmls():
    with tempfile.TemporaryDirectory() as tmp:
        q1 = write_xml(tmp, "q1.xml", "2025-01-01", "2025-03-31", """
      <StmtFunds>
        <StatementOfFunds transactionID="Q1" levelOfDetail="Detail"
               activityCode="DIV" date="2025-02-15" reportDate="2025-02-15"
               currency="EUR" amount="10" fxRateToBase="1" />
      </StmtFunds>
""")
        q2 = write_xml(tmp, "q2.xml", "20250401", "20250630", """
      <StmtFunds>
        <StatementOfFunds transactionID="Q2" levelOfDetail="Detail"
               activityCode="DIV" date="20250515" reportDate="20250515"
               currency="EUR" amount="20" fxRateToBase="1" />
      </StmtFunds>
""")
        out_dir = os.path.join(tmp, "out")
        os.mkdir(out_dir)
        with contextlib.redirect_stdout(io.StringIO()):
            extract_quarterly_xmls([q2, q1], out_dir)

        funds = read_csv(os.path.join(out_dir, "statement_of_funds.csv"))
        assert [row["transactionID"] for row in funds] == ["Q1", "Q2"]
        assert [row["reportDate"] for row in funds] == [
            "2025-02-15", "2025-05-15",
        ]
        account_info = read_csv(os.path.join(out_dir, "account_info.csv"))
        assert account_info[0]["tax_year"] == "2025"


def test_quarterly_tax_year_with_history_keeps_tax_year_sections():
    with tempfile.TemporaryDirectory() as tmp:
        history = write_xml(tmp, "history_2024.xml", "2024-01-01", "2024-12-31", """
      <Trades>
        <Trade tradeID="HIST_OPT_SELL" levelOfDetail="EXECUTION" assetCategory="OPT"
               transactionType="ExchTrade" buySell="SELL" quantity="-1"
               symbol="SYN 100 P" dateTime="2024-12-20 10:00:00"
               tradeDate="2024-12-20" reportDate="2024-12-20"
               closePrice="1.00" fifoPnlRealized="0" />
      </Trades>
      <StmtFunds>
        <StatementOfFunds transactionID="HIST_DIV" levelOfDetail="Detail"
               activityDescription="Dividends" date="2024-06-01"
               currency="USD" amount="50" fxRateToBase="0.92" />
        <StatementOfFunds transactionID="FX_HIST" levelOfDetail="Currency"
               activityDescription="Starting Balance" date="2024-12-31"
               currency="USD" amount="1000" balance="1000" fxRateToBase="0.92" />
      </StmtFunds>
      <ConversionRates>
        <ConversionRate reportDate="2024-12-20" fromCurrency="USD"
               toCurrency="EUR" rate="0.92" />
      </ConversionRates>
""")
        q1 = write_xml(tmp, "q1_2025.xml", "2025-01-01", "2025-03-31", """
      <StmtFunds>
        <StatementOfFunds transactionID="DIV_Q1" levelOfDetail="Detail"
               activityDescription="Dividends" date="2025-02-15"
               currency="USD" amount="100" fxRateToBase="0.90" />
        <StatementOfFunds transactionID="FX_Q1" levelOfDetail="Currency"
               activityDescription="Currency Conversion" date="2025-02-01"
               currency="USD" amount="200" balance="1200" fxRateToBase="0.91" />
      </StmtFunds>
      <SecuritiesInfo>
        <SecurityInfo conid="1" isin="US0000000001" symbol="DIV"
               assetCategory="STK" />
      </SecuritiesInfo>
      <ConversionRates>
        <ConversionRate reportDate="2025-02-15" fromCurrency="USD"
               toCurrency="EUR" rate="0.90" />
      </ConversionRates>
""")
        q2 = write_xml(tmp, "q2_2025.xml", "2025-04-01", "2025-06-30", """
      <Trades>
        <Trade tradeID="BUY_Q2" levelOfDetail="EXECUTION" assetCategory="STK"
               transactionType="ExchTrade" buySell="BUY" quantity="10"
               symbol="DIV" dateTime="2025-04-10 10:00:00"
               tradeDate="2025-04-10" reportDate="2025-04-10"
               closePrice="90" fifoPnlRealized="0" />
        <Trade levelOfDetail="CLOSED_LOT" assetCategory="STK" currency="USD"
               symbol="DIV" openDateTime="2025-04-10 10:00:00"
               dateTime="2025-06-15 10:00:00" reportDate="2025-06-15"
               quantity="10" cost="900" fifoPnlRealized="100"
               fxRateToBase="0.93" />
      </Trades>
""")
        q4 = write_xml(tmp, "q4_2025.xml", "2025-10-01", "2025-12-31", """
      <Trades>
        <Trade tradeID="SELL_Q4" levelOfDetail="EXECUTION" assetCategory="STK"
               transactionType="ExchTrade" buySell="SELL" quantity="-10"
               symbol="DIV" dateTime="2025-11-20 10:00:00"
               tradeDate="2025-11-20" reportDate="2025-11-20"
               closePrice="100" fifoPnlRealized="100" />
      </Trades>
      <StmtFunds>
        <StatementOfFunds transactionID="INT_Q4" levelOfDetail="Detail"
               activityDescription="Interest" date="2025-11-01"
               currency="USD" amount="10" fxRateToBase="0.94" />
      </StmtFunds>
""")

        out_dir = os.path.join(tmp, "out")
        os.mkdir(out_dir)
        with contextlib.redirect_stdout(io.StringIO()):
            extract_fx_multi_xml([q4, q1, history, q2], out_dir)

        funds = read_csv(os.path.join(out_dir, "statement_of_funds.csv"))
        fund_ids = {row.get("transactionID") for row in funds}
        assert "DIV_Q1" in fund_ids, "Q1 steuerjahr-Dividende fehlt"
        assert "INT_Q4" in fund_ids, "Q4 steuerjahr-Zins fehlt"
        assert "HIST_DIV" not in fund_ids, "Vorjahres-Dividende wurde als Steuerjahr-Fund gemergt"

        trades = read_csv(os.path.join(out_dir, "trades.csv"))
        trade_ids = {row.get("tradeID") for row in trades}
        assert {"HIST_OPT_SELL", "BUY_Q2", "SELL_Q4"} <= trade_ids

        closed_lots = read_csv(os.path.join(out_dir, "closed_lots.csv"))
        assert any(row.get("symbol") == "DIV" for row in closed_lots), "Q2 CLOSED_LOT fehlt"

        fx_rows = read_csv(os.path.join(out_dir, "fx_transactions.csv"))
        fx_ids = {row.get("transactionID") for row in fx_rows}
        assert {"FX_HIST", "FX_Q1"} <= fx_ids

        account_info = read_csv(os.path.join(out_dir, "account_info.csv"))
        assert account_info[0].get("tax_year") == "2025"


def test_repeated_transaction_id_keeps_every_ledger_row():
    """IBKR vergibt dieselbe transactionID fuer Folgebuchungen derselben Position.

    Realfall: alle taeglichen MTM-Abrechnungen eines Futures teilen sich eine ID
    (audit1_2024.xml, tid 654722380 = 40+ Zeilen "M6E 18MAR24 Position MTM"), und
    auch am selben Tag kollidieren fachlich verschiedene Buchungen. Ein Dedupe ueber
    die ID allein loeschte diese Zeilen: audit1 verlor netto -2.004,15 USD, audit2
    -39.693,75 USD, wodurch der kumulierte Saldo und die FIFO-Naeherung (Option C)
    verfaelscht wurden. Nur bitidentische Zeilen sind echte Duplikate.
    """
    with tempfile.TemporaryDirectory() as tmp:
        history = write_xml(tmp, "history_2024.xml", "2024-01-01", "2024-12-31", """
      <StmtFunds>
        <StatementOfFunds transactionID="SB" levelOfDetail="Currency"
               activityDescription="Starting Balance" date="2024-01-01"
               currency="USD" amount="0" balance="0" fxRateToBase="0.92" />
        <StatementOfFunds transactionID="MTM1" levelOfDetail="Currency"
               activityCode="ADJ" activityDescription="M6E 18MAR24 Position MTM"
               date="2024-12-30" currency="USD" amount="-100" balance="-100"
               fxRateToBase="0.92" />
      </StmtFunds>
""")
        # Gleiche ID, gleicher Tag, andere Buchung + gleiche ID an Folgetagen.
        current = write_xml(tmp, "tax_2025.xml", "2025-01-01", "2025-12-31", """
      <StmtFunds>
        <StatementOfFunds transactionID="MTM1" levelOfDetail="Currency"
               activityCode="ADJ" activityDescription="M6E 18MAR24 Position MTM"
               date="2025-01-02" currency="USD" amount="-250" balance="-350"
               fxRateToBase="0.91" />
        <StatementOfFunds transactionID="MTM1" levelOfDetail="Currency"
               activityCode="ADJ" activityDescription="M6E 18MAR24 Position MTM"
               date="2025-01-03" currency="USD" amount="400" balance="50"
               fxRateToBase="0.91" />
        <StatementOfFunds transactionID="SAMEDAY" levelOfDetail="Currency"
               activityCode="" activityDescription="USD Borrow Fees for Dec-2024"
               date="2025-01-06" currency="USD" amount="-19.63" balance="30.37"
               fxRateToBase="0.91" />
        <StatementOfFunds transactionID="SAMEDAY" levelOfDetail="Currency"
               activityCode="CINT" activityDescription="SYEP Interest"
               date="2025-01-06" currency="USD" amount="1.82" balance="32.19"
               fxRateToBase="0.91" />
        <StatementOfFunds transactionID="MTM1" levelOfDetail="Currency"
               activityCode="ADJ" activityDescription="M6E 18MAR24 Position MTM"
               date="2025-01-02" currency="USD" amount="-250" balance="-350"
               fxRateToBase="0.91" />
      </StmtFunds>
""")
        out_dir = os.path.join(tmp, "out")
        os.makedirs(out_dir, exist_ok=True)
        with contextlib.redirect_stdout(io.StringIO()):
            extract_fx_multi_xml([current, history], out_dir)

        rows = [r for r in read_csv(os.path.join(out_dir, "fx_transactions.csv"))
                if r.get("activityDescription") != "Starting Balance"]

        mtm = [r for r in rows if r.get("transactionID") == "MTM1"]
        assert len(mtm) == 3, (
            f"Folgebuchungen derselben ID muessen erhalten bleiben, sind {len(mtm)}: "
            f"{[(r.get('date'), r.get('amount')) for r in mtm]}"
        )

        sameday = [r for r in rows if r.get("transactionID") == "SAMEDAY"]
        assert len(sameday) == 2, (
            f"Verschiedene Buchungen mit gleicher ID am selben Tag muessen beide "
            f"bleiben, sind {len(sameday)}"
        )

        # Die bitidentische Wiederholung (2025-01-02, -250) ist ein echtes Duplikat.
        dupes = [r for r in mtm if r.get("date") == "2025-01-02"]
        assert len(dupes) == 1, "Bitidentische Zeile muss dedupliziert werden"

        # Der kumulierte Saldo trifft damit wieder IBKRs gemeldeten Endstand.
        total = sum(float(r["amount"]) for r in rows)
        assert abs(total - 32.19) < 0.01, f"Saldo-Kumulation: erwartet 32.19, ist {total}"


def test_pure_quarterly_merge_keeps_repeated_transaction_id_rows():
    """Der reine Quartals-Pfad (extract_quarterly_xmls, ohne Vorjahres-History)
    darf FX-Ledgerzeilen nicht ueber die transactionID allein deduplizieren —
    dieselbe Bug-Klasse wie im Multi-XML-Pfad (Fix 2026-07-27), dort war der
    Quartals-Pfad aber nicht abgedeckt.
    """
    with tempfile.TemporaryDirectory() as tmp:
        q1 = write_xml(tmp, "q1_2025.xml", "2025-01-01", "2025-03-31", """
      <StmtFunds>
        <StatementOfFunds transactionID="SB" levelOfDetail="Currency"
               activityDescription="Starting Balance" date="2025-01-01"
               currency="USD" amount="0" balance="0" fxRateToBase="0.92" />
        <StatementOfFunds transactionID="MTM1" levelOfDetail="Currency"
               activityCode="ADJ" activityDescription="M6E 18MAR25 Position MTM"
               date="2025-01-02" currency="USD" amount="-250" balance="-250"
               fxRateToBase="0.91" />
        <StatementOfFunds transactionID="MTM1" levelOfDetail="Currency"
               activityCode="ADJ" activityDescription="M6E 18MAR25 Position MTM"
               date="2025-01-03" currency="USD" amount="400" balance="150"
               fxRateToBase="0.91" />
        <StatementOfFunds transactionID="SAMEDAY" levelOfDetail="Currency"
               activityCode="" activityDescription="USD Borrow Fees for Dec-2024"
               date="2025-01-06" currency="USD" amount="-19.63" balance="130.37"
               fxRateToBase="0.91" />
        <StatementOfFunds transactionID="SAMEDAY" levelOfDetail="Currency"
               activityCode="CINT" activityDescription="SYEP Interest"
               date="2025-01-06" currency="USD" amount="1.82" balance="132.19"
               fxRateToBase="0.91" />
      </StmtFunds>
""")
        # Q2 wiederholt die letzte Q1-Zeile bitidentisch (ueberlappender Export).
        q2 = write_xml(tmp, "q2_2025.xml", "2025-04-01", "2025-06-30", """
      <StmtFunds>
        <StatementOfFunds transactionID="SAMEDAY" levelOfDetail="Currency"
               activityCode="CINT" activityDescription="SYEP Interest"
               date="2025-01-06" currency="USD" amount="1.82" balance="132.19"
               fxRateToBase="0.91" />
        <StatementOfFunds transactionID="MTM1" levelOfDetail="Currency"
               activityCode="ADJ" activityDescription="M6E 18MAR25 Position MTM"
               date="2025-04-02" currency="USD" amount="-80" balance="52.19"
               fxRateToBase="0.93" />
      </StmtFunds>
""")
        out_dir = os.path.join(tmp, "out")
        os.makedirs(out_dir, exist_ok=True)
        with contextlib.redirect_stdout(io.StringIO()):
            extract_quarterly_xmls([q1, q2], out_dir)

        rows = [r for r in read_csv(os.path.join(out_dir, "fx_transactions.csv"))
                if r.get("activityDescription") != "Starting Balance"]

        mtm = [r for r in rows if r.get("transactionID") == "MTM1"]
        assert len(mtm) == 3, (
            f"Folgebuchungen derselben ID muessen erhalten bleiben, sind {len(mtm)}: "
            f"{[(r.get('date'), r.get('amount')) for r in mtm]}"
        )

        sameday = [r for r in rows if r.get("transactionID") == "SAMEDAY"]
        assert len(sameday) == 2, (
            f"Verschiedene Buchungen mit gleicher ID am selben Tag muessen beide "
            f"bleiben, sind {len(sameday)}"
        )

        total = sum(float(r["amount"]) for r in rows)
        assert abs(total - 52.19) < 0.01, f"Saldo-Kumulation: erwartet 52.19, ist {total}"


def test_quarterly_merge_preserves_fill_and_lot_multiplicity():
    """F4: gleicher Zeitstempel/Menge ist keine eindeutige Buchungs-ID."""
    trade = '''<Trade levelOfDetail="EXECUTION" assetCategory="STK"
        symbol="TEST" isin="US0000000001" currency="USD" buySell="SELL"
        dateTime="2025-02-03 10:00:00" quantity="-2" closePrice="100"
        fifoPnlRealized="10" ibCommission="-1" />'''
    lot = '''<Lot levelOfDetail="CLOSED_LOT" assetCategory="STK"
        symbol="TEST" openDateTime="2025-01-02 10:00:00"
        dateTime="2025-02-03 10:00:00" quantity="2"
        transactionID="L1" cost="-190" fifoPnlRealized="10" />'''
    fx = '''<FxTransaction levelOfDetail="TRANSACTION"
        reportDate="2025-02-03" dateTime="2025-02-03 10:00:00"
        functionalCurrency="EUR" fxCurrency="USD" quantity="-200"
        realizedPL="13.22" cost="-190" proceeds="200" code="C" />'''
    body = (
        '<Trades>' + trade * 2
        + trade.replace('fifoPnlRealized="10"', 'fifoPnlRealized="11"')
        + trade.replace('ibCommission="-1"', 'ibCommission="-2"')
        + lot * 2 + lot.replace('transactionID="L1"', 'transactionID="L2"')
        + '</Trades><FxTransactions>' + fx * 2
        + fx.replace('realizedPL="13.22"', 'realizedPL="15"')
        + '</FxTransactions>'
    )
    with tempfile.TemporaryDirectory() as tmp:
        xml = write_xml(tmp, 'q1.xml', '2025-01-01', '2025-03-31', body)
        single, quarterly = (os.path.join(tmp, n) for n in ('single', 'quarterly'))
        os.mkdir(single)
        os.mkdir(quarterly)
        with contextlib.redirect_stdout(io.StringIO()):
            parse_ibkr_xml(xml, single)
            extract_quarterly_xmls([xml], quarterly)
        for name, expected in [('trades.csv', 4), ('closed_lots.csv', 3),
                               ('fx_realized_pnl.csv', 3)]:
            original = read_csv(os.path.join(single, name))
            merged = read_csv(os.path.join(quarterly, name))
            assert len(merged) == expected, (name, len(merged), expected)
            assert Counter(tuple(sorted(r.items())) for r in merged) == Counter(
                tuple(sorted(r.items())) for r in original), name


def test_quarterly_pnl_summary_separates_security_ids():
    def summary(isin, conid, pnl, currency='EUR'):
        return f'''<FIFOPerformanceSummaryUnderlying assetCategory="STK"
            symbol="SAME" isin="{isin}" conid="{conid}" currency="{currency}"
            realizedSTProfit="{pnl}" totalRealizedPnl="{pnl}" />'''

    with tempfile.TemporaryDirectory() as tmp:
        q1 = write_xml(tmp, 'q1.xml', '2025-01-01', '2025-03-31',
                       '<FIFOPerformanceSummaryInBase>'
                       + summary('OLD', '1', 0) + summary('NEW', '2', 20)
                       + summary('NEW', '3', 5)
                       + '</FIFOPerformanceSummaryInBase>')
        q2 = write_xml(tmp, 'q2.xml', '2025-04-01', '2025-06-30',
                       '<FIFOPerformanceSummaryInBase>'
                       + summary('NEW', '2', 10) + summary('NEW', '2', 7, 'USD')
                       + '</FIFOPerformanceSummaryInBase>')
        output = os.path.join(tmp, 'out')
        os.mkdir(output)
        with contextlib.redirect_stdout(io.StringIO()):
            extract_quarterly_xmls([q2, q1], output)
        rows = read_csv(os.path.join(output, 'pnl_summary.csv'))
        expected = {('OLD', '1', 'EUR'): 0, ('NEW', '2', 'EUR'): 30,
                    ('NEW', '3', 'EUR'): 5, ('NEW', '2', 'USD'): 7}
        assert len(rows) == len(expected), rows
        for row in rows:
            key = (row['isin'], row['conid'], row['currency'])
            assert float(row['realizedSTProfit']) == expected[key], row
            assert float(row['totalRealizedPnl']) == expected[key], row


def test_quarterly_fx_totals_match_single_and_history_paths():
    """F4: gleiche Fills bleiben zahlungswirksam; Dateikopien zaehlen nicht doppelt."""
    from calculate_tax_report import calculate_tax
    from run_tests import compute_user_facing

    def fx_body(day):
        row = f'''<FxTransaction levelOfDetail="TRANSACTION"
            reportDate="{day}" dateTime="{day} 10:00:00"
            functionalCurrency="EUR" fxCurrency="USD" quantity="-200"
            realizedPL="13.22" code="C" />'''
        return '<FxTransactions>' + row * 2 + '</FxTransactions>'

    with tempfile.TemporaryDirectory() as tmp:
        q1 = write_xml(tmp, 'q1.xml', '2025-01-01', '2025-03-31', fx_body('2025-02-03'))
        q2 = write_xml(tmp, 'q2.xml', '2025-04-01', '2025-06-30', fx_body('2025-05-03'))
        copy_q1 = write_xml(tmp, 'copy.xml', '2025-01-01', '2025-03-31', fx_body('2025-02-03'))
        annual_body = (fx_body('2025-02-03')[:-len('</FxTransactions>')]
                       + fx_body('2025-05-03')[len('<FxTransactions>'):])
        annual = write_xml(tmp, 'annual.xml', '2025-01-01', '2025-06-30', annual_body)
        history = write_xml(tmp, 'history.xml', '2024-01-01', '2024-12-31', '')
        reports = []
        for mode, paths in [('single', [annual]), ('quarters', [q2, q1]),
                            ('copies', [q2, q1, copy_q1]),
                            ('history', [history, q2, q1])]:
            out = os.path.join(tmp, mode)
            os.mkdir(out)
            with contextlib.redirect_stdout(io.StringIO()):
                if mode == 'single':
                    parse_ibkr_xml(paths[0], out)
                elif mode == 'history':
                    extract_fx_multi_xml(paths, out)
                else:
                    extract_quarterly_xmls(paths, out)
                report = calculate_tax(out)
            assert len(read_csv(os.path.join(out, 'fx_realized_pnl.csv'))) == 4, mode
            assert abs(report['fx_total_gain'] - 52.88) < 1e-9, mode
            reports.append(compute_user_facing(report))
        assert all(r == reports[0] for r in reports), reports


def test_parse_ibkr_xml_rejects_malformed_xml():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "malformed.xml")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("<FlexQueryResponse>")

        try:
            with contextlib.redirect_stdout(io.StringIO()):
                parse_ibkr_xml(path, os.path.join(tmp, "out"))
        except ValueError as exc:
            assert "XML-Datei konnte nicht geparst werden" in str(exc)
            assert "malformed.xml" in str(exc)
        else:
            raise AssertionError("Kaputtes XML muss sichtbar abgewiesen werden")


def test_parse_ibkr_xml_rejects_non_flex_xml():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "not_flex.xml")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("<root><AccountInformation currency=\"EUR\" /></root>")

        try:
            with contextlib.redirect_stdout(io.StringIO()):
                parse_ibkr_xml(path, os.path.join(tmp, "out"))
        except ValueError as exc:
            assert "Kein FlexStatement" in str(exc)
            assert "not_flex.xml" in str(exc)
        else:
            raise AssertionError("XML ohne FlexStatement muss sichtbar abgewiesen werden")


if __name__ == "__main__":
    test_mixed_period_formats_sort_and_normalize_quarterly_xmls()
    print("OK: gemischte Quartals-Datumsformate werden normalisiert")
    test_quarterly_tax_year_with_history_keeps_tax_year_sections()
    print("OK: quarterly-history extraction")
    test_repeated_transaction_id_keeps_every_ledger_row()
    print("OK: wiederholte transactionID verliert keine Ledgerzeile")
    test_pure_quarterly_merge_keeps_repeated_transaction_id_rows()
    print("OK: reiner Quartals-Merge verliert keine Ledgerzeile")
    test_quarterly_merge_preserves_fill_and_lot_multiplicity()
    print("OK: F4 identische Fills und verschiedene Lots bleiben erhalten")
    test_quarterly_pnl_summary_separates_security_ids()
    print("OK: F4 PnL-Summary trennt ISIN, conid und Waehrung")
    test_quarterly_fx_totals_match_single_and_history_paths()
    print("OK: F4 Steuerwerte identisch bei Einzel-, Quartals- und History-Import")
    test_parse_ibkr_xml_rejects_malformed_xml()
    print("OK: kaputtes XML wird sichtbar abgewiesen")
    test_parse_ibkr_xml_rejects_non_flex_xml()
    print("OK: Nicht-Flex-XML wird sichtbar abgewiesen")
