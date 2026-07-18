"""Regression tests for XML execution completeness controls."""
import contextlib
import io
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (  # noqa: E402
    build_completeness_control,
    calculate_tax,
    format_completeness_control_text,
)
from extract_ibkr_data import parse_ibkr_xml  # noqa: E402


def _trade(trade_id, category, pnl, quantity="1"):
    return (
        f'<Trade levelOfDetail="EXECUTION" tradeID="{trade_id}" '
        f'assetCategory="{category}" symbol="TEST" quantity="{quantity}" '
        f'buySell="BUY" transactionType="ExchTrade" currency="EUR" '
        f'fifoPnlRealized="{pnl}" fxRateToBase="1" '
        f'dateTime="2025-06-01 10:00:00" reportDate="2025-06-01"/>'
    )


def _fixture_xml():
    # P1/P2 intentionally have otherwise identical fields: distinct tradeIDs
    # represent genuine partial executions and must both remain present.
    rows = [
        _trade("OPEN1", "OPT", "0"),
        _trade("OPEN2", "OPT", "0"),
        _trade("P1", "OPT", "10", "-1"),
        _trade("P2", "OPT", "10", "-1"),
        _trade("STK1", "STK", "0"),
        _trade("CASH1", "CASH", "0"),
        '<Trade levelOfDetail="CLOSED_LOT" assetCategory="OPT" '
        'symbol="TEST" quantity="1" fifoPnlRealized="10"/>',
    ]
    return (
        '<FlexQueryResponse><FlexStatements count="1">'
        '<FlexStatement accountId="SYNTH" fromDate="2025-01-01" '
        'toDate="2025-12-31">'
        '<AccountInformation accountId="SYNTH" currency="EUR"/>'
        f'<Trades>{"".join(rows)}</Trades>'
        '</FlexStatement></FlexStatements></FlexQueryResponse>'
    )


def test_import_counts_physical_executions_and_report_rows_separately():
    with tempfile.TemporaryDirectory() as tmp:
        xml_path = os.path.join(tmp, "fixture.xml")
        with open(xml_path, "w", encoding="utf-8") as handle:
            handle.write(_fixture_xml())
        with contextlib.redirect_stdout(io.StringIO()):
            parse_ibkr_xml(xml_path, tmp)
            report = calculate_tax(tmp)

        with open(os.path.join(tmp, "import_control.json"), encoding="utf-8") as handle:
            extracted = json.load(handle)

    assert extracted["xml_execution_rows"] == 6
    assert extracted["extracted_execution_rows"] == 6
    assert extracted["xml_execution_by_asset_category"] == {
        "CASH": 1,
        "OPT": 4,
        "STK": 1,
    }
    assert extracted["xml_option_fifo_realized_rows"] == 2
    assert extracted["xml_option_fifo_zero_rows"] == 2

    control = build_completeness_control(report, include_tageskurs=False)
    total = control["totals"]
    assert total["tax_detail_original_rows"] == 2
    assert total["tax_detail_option_realized_rows"] == 2
    assert not control["has_import_warning"]
    text = format_completeness_control_text(control)
    assert "6 XML-Ausführungen" in text
    assert "2 mit / 2 ohne realisiertes FIFO-Ergebnis" in text
    assert "keine 1:1-Kopie aller Eröffnungsbuchungen" in text


def test_multi_account_controls_add_finished_account_counts():
    first = {
        "account_id": "A",
        "xml_execution_rows": 3,
        "extracted_execution_rows": 3,
        "xml_execution_by_asset_category": {"OPT": 2, "STK": 1},
        "xml_option_fifo_realized_rows": 1,
        "xml_option_fifo_zero_rows": 1,
        "tax_detail_original_rows": 1,
        "tax_detail_option_realized_rows": 1,
        "derived_rows_by_type": {"stillhalter_korrektur": 1},
    }
    second = {
        "account_id": "B",
        "xml_execution_rows": 2,
        "extracted_execution_rows": 2,
        "xml_execution_by_asset_category": {"OPT": 2},
        "xml_option_fifo_realized_rows": 1,
        "xml_option_fifo_zero_rows": 1,
        "tax_detail_original_rows": 1,
        "tax_detail_option_realized_rows": 1,
        "derived_rows_by_type": {"tageskurs_korrektur": 2},
    }
    control = build_completeness_control(
        {"completeness_accounts": [first, second]},
        include_tageskurs=True,
    )
    total = control["totals"]
    assert len(control["accounts"]) == 2
    assert total["xml_execution_rows"] == 5
    assert total["xml_execution_by_asset_category"] == {"OPT": 4, "STK": 1}
    assert total["derived_rows_by_type"] == {
        "stillhalter_korrektur": 1,
        "tageskurs_korrektur": 2,
    }
    text = format_completeness_control_text(control)
    assert "A: 3 XML-Ausführungen" in text
    assert "B: 2 XML-Ausführungen" in text
    assert "Gesamt: 5 XML-Ausführungen" in text


def test_extraction_or_realized_option_gap_is_a_warning():
    control = build_completeness_control({
        "completeness_accounts": [{
            "account_id": "GAP",
            "xml_execution_rows": 4,
            "extracted_execution_rows": 3,
            "xml_option_fifo_realized_rows": 2,
            "xml_option_fifo_zero_rows": 1,
            "tax_detail_original_rows": 1,
            "tax_detail_option_realized_rows": 1,
        }],
    })
    assert control["has_import_warning"]
    warnings = control["accounts"][0]["warnings"]
    assert len(warnings) == 2
    assert "extrahierte Originalzeilen" in warnings[0]
    assert "Nicht alle Optionsausführungen" in warnings[1]


if __name__ == "__main__":
    test_import_counts_physical_executions_and_report_rows_separately()
    test_multi_account_controls_add_finished_account_counts()
    test_extraction_or_realized_option_gap_is_a_warning()
    print("OK: XML-Vollständigkeitskontrolle")
