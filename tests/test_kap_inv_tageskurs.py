"""Regression tests for KAP-INV Tageskurs correction with Teilfreistellung."""
import contextlib
import csv
import io
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (
    build_kap_inv_form,
    calculate_tax,
    get_kap_inv_tageskurs_delta_for_reporting,
)


def calculate_for_trades(trades, closed_lots, conversion_rates):
    trade_fields = sorted({k for row in trades for k in row})
    lot_fields = sorted({k for row in closed_lots for k in row})
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "account_info.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["currency", "tax_year", "fx_transactions_count"])
            writer.writeheader()
            writer.writerow({"currency": "EUR", "tax_year": "2025", "fx_transactions_count": "0"})
        with open(os.path.join(tmp, "trades.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=trade_fields)
            writer.writeheader()
            writer.writerows(trades)
        with open(os.path.join(tmp, "closed_lots.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=lot_fields)
            writer.writeheader()
            writer.writerows(closed_lots)
        with open(os.path.join(tmp, "conversion_rates.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["reportDate", "fromCurrency", "toCurrency", "rate"])
            writer.writeheader()
            writer.writerows(conversion_rates)
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp)


def make_trade(symbol, isin, trade_id, pnl):
    return {
        "tradeID": trade_id,
        "assetCategory": "STK",
        "subCategory": "ETF",
        "transactionType": "ExchTrade",
        "buySell": "SELL",
        "symbol": symbol,
        "isin": isin,
        "quantity": "-10",
        "tradePrice": "110",
        "closePrice": "110",
        "fifoPnlRealized": str(pnl),
        "fxRateToBase": "0.9",
        "currency": "USD",
        "dateTime": "2025-02-01 10:00:00",
        "tradeDate": "2025-02-01",
        "reportDate": "2025-02-01",
    }


def make_closed_lot(symbol, isin):
    return {
        "assetCategory": "STK",
        "subCategory": "ETF",
        "currency": "USD",
        "symbol": symbol,
        "isin": isin,
        "openDateTime": "2025-01-01 10:00:00",
        "dateTime": "2025-02-01 10:00:00",
        "reportDate": "2025-02-01",
        "quantity": "10",
        "cost": "1000",
        "fifoPnlRealized": "100",
        "fxRateToBase": "0.9",
    }


def test_kap_inv_tageskurs_delta_applies_tfs_per_isin():
    rd = calculate_for_trades(
        trades=[
            make_trade("SPY", "US78462F1030", "SPY_SELL", 100),
            make_trade("SHY", "US4642874576", "SHY_SELL", 100),
        ],
        closed_lots=[
            make_closed_lot("SPY", "US78462F1030"),
            make_closed_lot("SHY", "US4642874576"),
        ],
        conversion_rates=[
            {"reportDate": "2025-01-01", "fromCurrency": "USD", "toCurrency": "EUR", "rate": "0.8"},
            {"reportDate": "2025-02-01", "fromCurrency": "USD", "toCurrency": "EUR", "rate": "0.9"},
        ],
    )

    raw_kap_inv_delta = rd["fx_correction_by_topf"]["KAP-INV"]
    taxable_kap_inv_delta = get_kap_inv_tageskurs_delta_for_reporting(rd)
    by_isin = rd["fx_correction_kap_inv_by_isin"]

    assert round(raw_kap_inv_delta, 2) == 200.00
    assert round(by_isin["US78462F1030"]["taxable_delta"], 2) == 70.00
    assert round(by_isin["US4642874576"]["taxable_delta"], 2) == 100.00
    assert round(taxable_kap_inv_delta, 2) == 170.00
    form_lines = {line["line"]: line for line in rd["kap_inv_form"]["lines"]}
    assert round(form_lines[14]["amount_raw_eur"], 2) == 190.00
    assert round(form_lines[14]["taxable_control_eur"], 2) == 133.00
    assert round(form_lines[26]["amount_raw_eur"], 2) == 190.00
    assert round(form_lines[26]["taxable_control_eur"], 2) == 190.00


def test_kap_inv_form_aggregates_by_fund_type_and_blocks_unknowns():
    form = build_kap_inv_form(
        {
            "EQ1": {
                "ticker": "EQ1", "classification": "aktienfonds",
                "tfs_rate": 0.30, "gain": 100, "loss": 0, "div": 20,
            },
            "EQ2": {
                "ticker": "EQ2", "classification": "aktienfonds",
                "tfs_rate": 0.30, "gain": 0, "loss": -40, "div": 30,
            },
            "PROP": {
                "ticker": "PROP", "classification": "auslands_immobilienfonds",
                "tfs_rate": 0.80, "gain": 50, "loss": 0, "div": 100,
            },
            "UNKNOWN": {
                "ticker": "UNK", "classification": "sonstiger_fonds",
                "tfs_rate": 0.0, "gain": 10, "loss": 0, "div": 5,
            },
        },
        fx_by_isin={"EQ1": {"raw_delta": 10}},
        unknown_isins=["UNKNOWN"],
    )
    lines = {line["line"]: line for line in form["lines"]}
    assert round(lines[4]["amount_raw_eur"], 2) == 50.00
    assert round(lines[14]["amount_raw_eur"], 2) == 70.00
    assert round(lines[14]["taxable_control_eur"], 2) == 49.00
    assert round(lines[7]["amount_raw_eur"], 2) == 100.00
    assert round(lines[23]["amount_raw_eur"], 2) == 50.00
    assert round(lines[23]["taxable_control_eur"], 2) == 10.00
    assert "UNKNOWN" in form["blocked_isins"]
    assert form["blocked_details"] == [{
        "isin": "UNKNOWN",
        "ticker": "UNK",
        "classification": "sonstiger_fonds",
        "review_reason": "",
        "distribution_raw_eur": 5.0,
        "sale_raw_eur": 10.0,
        "tageskurs_raw_eur": 0.0,
    }]
    assert form["status"] == "classification_review_required"


def test_kap_inv_form_excludes_paid_distributions_per_isin():
    # Komponenten-Tracking: gezahlte Betraege raus aus der Zeile, kein Netting
    form = build_kap_inv_form({
        "EQ1": {
            "ticker": "EQ1", "classification": "aktienfonds", "tfs_rate": 0.30,
            "gain": 0, "loss": 0,
            "div": 50, "div_received": 100, "div_paid": -50,
        },
    })
    lines = {line["line"]: line for line in form["lines"]}
    assert round(lines[4]["amount_raw_eur"], 2) == 100.00
    assert round(lines[4]["taxable_control_eur"], 2) == 70.00
    assert round(form["negative_distribution_details"][0]["paid_distribution_eur"], 2) == -50.00
    assert form["status"] == "paid_distribution_review_required"

    # Legacy-Fallback ohne Komponenten-Felder: netto-negative Ausschuettung
    # erzeugt keine Formularzeile, sondern nur den Prueffall
    legacy = build_kap_inv_form({
        "SHORT": {
            "ticker": "SHORT", "classification": "aktienfonds", "tfs_rate": 0.30,
            "gain": 0, "loss": 0, "div": -93.71,
        },
    })
    assert 4 not in {line["line"] for line in legacy["lines"]}
    paid = legacy["negative_distribution_details"]
    assert len(paid) == 1
    assert round(paid[0]["paid_distribution_eur"], 2) == -93.71
    assert legacy["status"] == "paid_distribution_review_required"


if __name__ == "__main__":
    test_kap_inv_tageskurs_delta_applies_tfs_per_isin()
    test_kap_inv_form_aggregates_by_fund_type_and_blocks_unknowns()
    test_kap_inv_form_excludes_paid_distributions_per_isin()
    print("OK: KAP-INV Tageskurs TFS")
