"""Regression tests for quarterly tax-year XMLs with prior-year history."""
import contextlib
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
    test_quarterly_tax_year_with_history_keeps_tax_year_sections()
    print("OK: quarterly-history extraction")
    test_repeated_transaction_id_keeps_every_ledger_row()
    print("OK: wiederholte transactionID verliert keine Ledgerzeile")
    test_pure_quarterly_merge_keeps_repeated_transaction_id_rows()
    print("OK: reiner Quartals-Merge verliert keine Ledgerzeile")
    test_parse_ibkr_xml_rejects_malformed_xml()
    print("OK: kaputtes XML wird sichtbar abgewiesen")
    test_parse_ibkr_xml_rejects_non_flex_xml()
    print("OK: Nicht-Flex-XML wird sichtbar abgewiesen")
