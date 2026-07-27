#!/usr/bin/env python3
"""Routing aller Instrumentenkategorien in die Steuertoepfe (Issues #72, #85).

Deckt zwei Bug-Klassen ab, die vorher still zu falschen Zahlen fuehrten:

1. Kategorien ohne Routing-Zweig (WAR, CFD) verschwanden komplett aus der
   Berechnung: kein Topf, keine Zeile, keine Warnung.
2. Der Debug-/Excel-Export markierte dieselben Rows trotzdem als "Topf2",
   sodass Excel und Anlage KAP sich widersprachen.

Zusaetzlich der Guard: eine unbekannte Kategorie darf nie mehr still
verschluckt werden, sondern muss als Prueffall auftauchen.
"""
import contextlib
import csv
import io
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (  # noqa: E402
    FEE_ACTIVITY_CODES,
    INCOME_ACTIVITY_CODES,
    KNOWN_IGNORED_ACTIVITY_CODES,
    KNOWN_UNROUTED_ASSET_CATEGORIES,
    MANUAL_REVIEW_ACTIVITY_CODES,
    TOPF2_ASSET_CATEGORIES,
    TOPF2_CAT_LABELS,
    calculate_tax,
    register_unhandled_activity_code,
    register_unrouted_category,
)


def _write_csv(path, rows):
    if not rows:
        return
    fields = sorted({key for row in rows for key in row})
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _trade(tid, category, pnl, symbol="TESTW", **extra):
    row = {
        "tradeID": tid,
        "assetCategory": category,
        "subCategory": "",
        "transactionType": "ExchTrade",
        "buySell": "SELL",
        "openCloseIndicator": "C",
        "symbol": symbol,
        "description": f"{symbol} Testinstrument",
        "isin": "",
        "quantity": "1",
        "tradePrice": "10",
        "closePrice": "10",
        "cost": "100",
        "proceeds": "110",
        "multiplier": "1",
        "ibCommission": "0",
        "fxRateToBase": "1",
        "currency": "EUR",
        "dateTime": "2025-03-03 10:00:00",
        "tradeDate": "2025-03-03",
        "reportDate": "2025-03-03",
        "fifoPnlRealized": str(pnl),
    }
    row.update(extra)
    return row


def _fund(tid, code, amount, description="CFD Interest"):
    return {
        "transactionID": tid,
        "activityCode": code,
        "activityDescription": description,
        "amount": str(amount),
        "currency": "EUR",
        "fxRateToBase": "1",
        "isin": "",
        "symbol": "",
        "subCategory": "",
        "date": "2025-04-04",
        "reportDate": "2025-04-04",
    }


def _run(trades, funds=None, summary_rows=None):
    with tempfile.TemporaryDirectory() as tmp:
        _write_csv(os.path.join(tmp, "trades.csv"), trades)
        if funds:
            _write_csv(os.path.join(tmp, "statement_of_funds.csv"), funds)
        if summary_rows:
            _write_csv(os.path.join(tmp, "pnl_summary.csv"), summary_rows)
        _write_csv(os.path.join(tmp, "account_info.csv"), [{
            "currency": "EUR",
            "tax_year": "2025",
            "fx_transactions_count": "0",
        }])
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp)


def _assert_close(actual, expected, label):
    if abs(actual - expected) > 0.0001:
        raise AssertionError(f"{label}: erwartet {expected}, aktuell {actual}")


def _topf_of(report, symbol):
    rows = [r for r in report["trade_details"] if r.get("symbol") == symbol]
    assert rows, f"keine Trade-Detail-Row fuer {symbol}"
    return rows[0]["topf"]


def test_warrants_route_to_topf2():
    """WAR ist eine verbriefte Kapitalforderung (§20 Abs. 1/2 Nr. 7) → Topf 2."""
    report = _run([
        _trade("w1", "WAR", 250.0, symbol="ABC WARRANT"),
        _trade("w2", "WAR", -100.0, symbol="XYZ WARRANT"),
    ])
    _assert_close(report["options_gain_eur"], 250.0, "WAR-Gewinn in Topf 2")
    _assert_close(report["options_loss_eur"], -100.0, "WAR-Verlust in Topf 2")
    _assert_close(report["stocks_gain_eur"], 0.0, "WAR gehoert nicht in Topf 1")
    _assert_close(report["topf_2_sonstiges_netto"], 150.0, "Topf-2-Saldo")
    _assert_close(report["zeile_19_netto_eur"], 150.0, "Zeile 19")
    _assert_close(report["zeile_22_other_losses_eur"], 100.0, "Zeile 22")

    cats = report["topf2_by_category"]
    assert "Optionsscheine" in cats, f"Kategorie fehlt: {sorted(cats)}"
    _assert_close(cats["Optionsscheine"]["gain"], 250.0, "Kategorie-Gewinn")
    _assert_close(cats["Optionsscheine"]["loss"], -100.0, "Kategorie-Verlust")
    assert not report["audit"]["unrouted_asset_categories"], "WAR darf keine Warnung mehr sein"
    print("  OK  WAR: Gewinn und Verlust in Topf 2, eigene Kategorie")


def test_cfd_trades_route_to_topf2():
    """CFD-Kursergebnisse sind Termingeschaefte → Topf 2."""
    report = _run([_trade("c1", "CFD", 80.0, symbol="DAX CFD")])
    _assert_close(report["options_gain_eur"], 80.0, "CFD-Gewinn in Topf 2")
    _assert_close(report["zeile_19_netto_eur"], 80.0, "Zeile 19")
    assert "CFDs" in report["topf2_by_category"]
    assert not report["audit"]["unrouted_asset_categories"]
    print("  OK  CFD: Kursergebnis in Topf 2")


def test_cfd_interest_is_income_and_cfd_fees_are_not_deductible():
    """Habenzinsen §20 Abs. 1 Nr. 7 EStG; Finanzierungskosten §20 Abs. 9 EStG."""
    report = _run(
        [_trade("c1", "CFD", 0.0)],
        funds=[
            _fund("f1", "CFD", 5.26, "CFD Interest on short position"),
            _fund("f2", "CFD", -1.12, "CFD Borrow Fee"),
        ],
    )
    _assert_close(report["interest_eur"], 5.26, "CFD-Habenzinsen sind Kapitalertrag")
    _assert_close(report["debit_interest_eur"], -1.12, "CFD-Kosten nicht abzugsfaehig")
    _assert_close(report["zeile_19_netto_eur"], 5.26, "nur die Zinsen erhoehen Zeile 19")
    _assert_close(report["audit"]["cfd_interest_income_eur"], 5.26, "Audit: CFD-Zinsen")
    _assert_close(report["audit"]["cfd_financing_cost_eur"], -1.12, "Audit: CFD-Kosten")
    print("  OK  CFD-Zinsen: Ertrag in Zeile 19, Kosten nur nachrichtlich")


def test_unknown_category_is_flagged_instead_of_swallowed():
    """Unbekannte Kategorie: kein Topf, aber ein sichtbarer Prueffall."""
    report = _run([
        _trade("u1", "CMDTY", 500.0, symbol="XAUUSD"),
        _trade("u2", "CMDTY", -20.0, symbol="XAGUSD"),
    ])
    _assert_close(report["stocks_gain_eur"], 0.0, "nicht in Topf 1")
    _assert_close(report["options_gain_eur"], 0.0, "nicht in Topf 2")
    _assert_close(report["zeile_19_netto_eur"], 0.0, "keine stille Zuordnung")

    unrouted = report["audit"]["unrouted_asset_categories"]
    assert len(unrouted) == 1, f"erwartet 1 Eintrag, aktuell {unrouted}"
    entry = unrouted[0]
    assert entry["category"] == "CMDTY"
    assert entry["count"] == 2
    _assert_close(entry["pnl_eur"], 480.0, "gemeldeter Saldo")
    assert set(entry["symbols"]) == {"XAUUSD", "XAGUSD"}, entry["symbols"]

    assert _topf_of(report, "XAUUSD") == "Nicht zugeordnet", (
        "Export darf nicht Topf 2 behaupten, wenn die Rechnung nicht routet"
    )
    print("  OK  Unbekannte Kategorie: gemeldet statt verschluckt")


def test_cash_rows_do_not_trigger_the_guard():
    """CASH ist Devisenumsatz; das Ergebnis kommt aus der FX-Engine."""
    report = _run([_trade("x1", "CASH", 42.0, symbol="EUR.USD")])
    assert not report["audit"]["unrouted_asset_categories"], (
        "CASH ist bewusst nicht hier geroutet und darf nicht warnen"
    )
    _assert_close(report["options_gain_eur"], 0.0, "keine Doppelzaehlung zur FX-Engine")
    assert _topf_of(report, "EUR.USD") == "Topf2", "CASH-Ausweis bleibt unveraendert"
    print("  OK  CASH: kein Fehlalarm, Ausweis unveraendert")


def test_pnl_summary_fallback_routes_warrants():
    """Der Summary-Fallback hatte dieselbe Luecke wie der Trade-Loop."""
    report = _run(
        [_trade("s1", "OPT", 10.0, symbol="FILLER")],
        summary_rows=[{
            "assetCategory": "WAR",
            "subCategory": "",
            "symbol": "ABC WARRANT",
            "description": "ABC Warrant",
            "isin": "DE000TESTWAR1",
            "realizedSTProfit": "300",
            "realizedLTProfit": "0",
            "realizedSTLoss": "-50",
            "realizedLTLoss": "0",
        }],
    )
    _assert_close(report["options_gain_eur"], 310.0, "Summary-WAR-Gewinn + Filler")
    _assert_close(report["options_loss_eur"], -50.0, "Summary-WAR-Verlust")
    assert "Optionsscheine" in report["topf2_by_category"]
    assert not report["audit"]["unrouted_asset_categories"]
    print("  OK  pnl_summary-Fallback: WAR wird geroutet")


def test_pnl_summary_fallback_flags_unknown_category():
    report = _run(
        [_trade("s1", "OPT", 10.0, symbol="FILLER")],
        summary_rows=[{
            "assetCategory": "CMDTY",
            "subCategory": "",
            "symbol": "XAUUSD",
            "description": "Spot Gold",
            "isin": "DE000TESTCMD1",
            "realizedSTProfit": "70",
            "realizedLTProfit": "0",
            "realizedSTLoss": "0",
            "realizedLTLoss": "0",
        }],
    )
    _assert_close(report["options_gain_eur"], 10.0, "nur der Filler-Trade zaehlt")
    unrouted = report["audit"]["unrouted_asset_categories"]
    assert len(unrouted) == 1 and unrouted[0]["category"] == "CMDTY", unrouted
    assert unrouted[0]["sources"] == ["pnl_summary"], unrouted[0]["sources"]
    print("  OK  pnl_summary-Fallback: unbekannte Kategorie gemeldet")


def test_registry_helper_is_additive_and_skips_known_categories():
    registry = {}
    register_unrouted_category(registry, "WAR", 10.0, symbol="A")
    register_unrouted_category(registry, "WAR", -4.0, symbol="B")
    register_unrouted_category(registry, "WAR", 1.0, symbol="A")
    _assert_close(registry["WAR"]["pnl_eur"], 7.0, "Saldo summiert")
    assert registry["WAR"]["count"] == 3
    assert registry["WAR"]["symbols"] == ["A", "B"], "Symbole dedupliziert"

    register_unrouted_category(registry, "CASH", 99.0)
    assert "CASH" not in registry, "bekannte Ausnahme darf nicht registriert werden"

    register_unrouted_category(registry, "", 5.0)
    assert "(leer)" in registry, "leere Kategorie bekommt sprechenden Namen"
    print("  OK  Registry-Helper: additiv, dedupliziert, CASH ausgenommen")


def test_fee_codes_are_reported_and_transaction_tax_requires_review():
    """OFEE/STAX sind nachrichtlich; TTAX darf nicht als laufende Gebuehr verschwinden."""
    report = _run(
        [_trade("t1", "OPT", 100.0, symbol="FILLER")],
        funds=[
            _fund("f1", "OFEE", -12.50, "Monthly Market Data Fee"),
            _fund("f2", "STAX", -3.20, "Sales Tax on Commission"),
            _fund("f3", "TTAX", -0.05, "Transaction Tax"),
            _fund("f4", "CINT", 7.00, "Credit Interest"),
        ],
    )
    _assert_close(report["other_fees_eur"], -15.70, "laufende Gebuehren nachrichtlich")
    _assert_close(report["interest_eur"], 7.00, "Gebuehren mindern keine Zinsen")
    _assert_close(report["debit_interest_eur"], 0.0, "Gebuehren sind kein Sollzins")
    _assert_close(
        report["zeile_19_netto_eur"], 107.0,
        "TTAX bleibt bis zur belastbaren Topf-Zuordnung ausserhalb der Automatik",
    )
    fees = report["audit"]["fee_by_activity_code"]
    assert set(fees) == {"OFEE", "STAX"}, fees
    _assert_close(fees["OFEE"], -12.50, "OFEE einzeln")
    review = report["audit"]["unhandled_activity_codes"]
    assert len(review) == 1 and review[0]["code"] == "TTAX", review
    _assert_close(review[0]["amount_eur"], -0.05, "TTAX-Pruefbetrag")
    print("  OK  Gebuehren nachrichtlich; TTAX als manueller Prueffall sichtbar")


def test_unknown_activity_code_is_flagged():
    """Ein unbekannter Buchungscode darf nicht wortlos verschwinden."""
    report = _run(
        [_trade("t1", "OPT", 100.0, symbol="FILLER")],
        funds=[
            _fund("f1", "XYZNEW", 250.0, "Neue IBKR-Buchungsart"),
            _fund("f2", "XYZNEW", -50.0, "Neue IBKR-Buchungsart"),
        ],
    )
    _assert_close(report["zeile_19_netto_eur"], 100.0, "keine stille Vereinnahmung")
    unhandled = report["audit"]["unhandled_activity_codes"]
    assert len(unhandled) == 1, unhandled
    assert unhandled[0]["code"] == "XYZNEW"
    assert unhandled[0]["count"] == 2
    _assert_close(unhandled[0]["amount_eur"], 200.0, "gemeldeter Saldo")
    assert unhandled[0]["descriptions"] == ["Neue IBKR-Buchungsart"], unhandled[0]
    print("  OK  Unbekannter Buchungscode: gemeldet statt verschluckt")


def test_ignored_codes_stay_silent():
    """Trade-Settlements und Cash-Bewegungen duerfen nicht warnen.

    CORP sind in echten Daten T-Bill-Nominalrueckzahlungen; der Ertrag kommt
    aus dem BILL-Pfad. Eine Warnung waere hier ein Dauerfehlalarm.
    """
    report = _run(
        [_trade("t1", "OPT", 100.0, symbol="FILLER")],
        funds=[
            _fund("f1", "BUY", -5000.0, "Kauf"),
            _fund("f2", "SELL", 5100.0, "Verkauf"),
            _fund("f3", "DEP", 10000.0, "Einzahlung"),
            _fund("f4", "FOREX", -800.0, "Devisenumsatz"),
            _fund("f5", "CORP", 41785.20, "Treasury Bill Maturity"),
            _fund("f6", "ADJ", -3.0, "Futures MTM"),
        ],
    )
    assert not report["audit"]["unhandled_activity_codes"], (
        f"Fehlalarm: {report['audit']['unhandled_activity_codes']}"
    )
    _assert_close(report["zeile_19_netto_eur"], 100.0, "keine dieser Buchungen ist Ertrag")
    _assert_close(report["other_fees_eur"], 0.0, "keine Gebuehren")
    print("  OK  Bekannte Nicht-Ertragscodes bleiben still")


def test_activity_code_registry_helper():
    registry = {}
    register_unhandled_activity_code(registry, "ABC", 10.0, "erste")
    register_unhandled_activity_code(registry, "ABC", -3.0, "zweite")
    register_unhandled_activity_code(registry, "ABC", 1.0, "erste")
    _assert_close(registry["ABC"]["amount_eur"], 8.0, "Saldo")
    assert registry["ABC"]["count"] == 3
    assert registry["ABC"]["descriptions"] == ["erste", "zweite"], registry["ABC"]
    register_unhandled_activity_code(registry, "", 1.0)
    assert "(leer)" in registry
    print("  OK  Activity-Code-Registry: additiv, Beschreibungen dedupliziert")


def test_activity_code_tables_are_disjoint():
    for a, b, label in (
        (INCOME_ACTIVITY_CODES, FEE_ACTIVITY_CODES, "Ertrag/Gebuehr"),
        (INCOME_ACTIVITY_CODES, KNOWN_IGNORED_ACTIVITY_CODES, "Ertrag/Ignoriert"),
        (INCOME_ACTIVITY_CODES, MANUAL_REVIEW_ACTIVITY_CODES, "Ertrag/Prueffall"),
        (FEE_ACTIVITY_CODES, KNOWN_IGNORED_ACTIVITY_CODES, "Gebuehr/Ignoriert"),
        (FEE_ACTIVITY_CODES, MANUAL_REVIEW_ACTIVITY_CODES, "Gebuehr/Prueffall"),
        (KNOWN_IGNORED_ACTIVITY_CODES, MANUAL_REVIEW_ACTIVITY_CODES,
         "Ignoriert/Prueffall"),
    ):
        overlap = a & b
        assert not overlap, f"{label} ueberschneiden sich: {overlap}"
    # Codes, die in echten Nutzerdaten vorkommen, muessen alle eingeordnet sein
    seen_in_real_exports = {
        "", "ADJ", "ASSIGN", "BUY", "CFD", "CINT", "CORP", "DEP", "DINT", "DIV",
        "EXE", "FOREX", "FRTAX", "INTP", "INTR", "OFEE", "PIL", "SELL", "STAX",
        "TTAX", "WITH",
    }
    known = (
        INCOME_ACTIVITY_CODES
        | FEE_ACTIVITY_CODES
        | KNOWN_IGNORED_ACTIVITY_CODES
        | MANUAL_REVIEW_ACTIVITY_CODES
    )
    missing = sorted(seen_in_real_exports - known)
    assert not missing, f"Code aus echten Exporten nicht eingeordnet: {missing}"
    print("  OK  Activity-Code-Tabellen disjunkt und vollstaendig")


def test_category_tables_are_consistent():
    missing = sorted(c for c in TOPF2_ASSET_CATEGORIES if c not in TOPF2_CAT_LABELS)
    assert not missing, f"Topf-2-Kategorien ohne Anzeigename: {missing}"
    overlap = TOPF2_ASSET_CATEGORIES & KNOWN_UNROUTED_ASSET_CATEGORIES
    assert not overlap, f"Kategorie gleichzeitig geroutet und ausgenommen: {overlap}"
    assert "STK" not in TOPF2_ASSET_CATEGORIES, "Aktien laufen ueber den eigenen Zweig"
    print("  OK  Kategorie-Tabellen konsistent")


if __name__ == "__main__":
    print("Instrumentenkategorien: Routing und Guard")
    test_warrants_route_to_topf2()
    test_cfd_trades_route_to_topf2()
    test_cfd_interest_is_income_and_cfd_fees_are_not_deductible()
    test_unknown_category_is_flagged_instead_of_swallowed()
    test_cash_rows_do_not_trigger_the_guard()
    test_pnl_summary_fallback_routes_warrants()
    test_pnl_summary_fallback_flags_unknown_category()
    test_registry_helper_is_additive_and_skips_known_categories()
    test_category_tables_are_consistent()
    test_fee_codes_are_reported_and_transaction_tax_requires_review()
    test_unknown_activity_code_is_flagged()
    test_ignored_codes_stay_silent()
    test_activity_code_registry_helper()
    test_activity_code_tables_are_disjoint()
    print("Alle Tests bestanden.")
