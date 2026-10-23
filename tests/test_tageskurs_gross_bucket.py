"""Regression tests for gross gain/loss allocation after lot corrections."""
import contextlib
import csv
import io
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (  # noqa: E402
    build_topf2_breakdown,
    calculate_tageskurs_gross_adjustment,
    calculate_tax,
)
from ui_model import build_final_values, default_toggles  # noqa: E402


VXX_ISIN = "US06748M1962"


def _write_csv(path, rows):
    if not rows:
        return
    fields = sorted({key for row in rows for key in row})
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _put_assignment_fixture(no_invstg=False):
    symbol = "VXX" if no_invstg else "TEST"
    isin = VXX_ISIN if no_invstg else "US0000000001"
    subcategory = "ETF" if no_invstg else "COMMON"
    trades = [
        {
            "tradeID": "put_sell",
            "assetCategory": "OPT",
            "transactionType": "ExchTrade",
            "buySell": "SELL",
            "openCloseIndicator": "O",
            "putCall": "P",
            "strike": "100",
            "expiry": "20250102",
            "underlyingSymbol": symbol,
            "symbol": f"{symbol} 250102P00100000",
            "quantity": "-1",
            "tradePrice": "10",
            "closePrice": "10",
            "multiplier": "1",
            "ibCommission": "0",
            "fxRateToBase": "1",
            "currency": "USD",
            "dateTime": "2025-01-01 10:00:00",
            "tradeDate": "2025-01-01",
            "reportDate": "2025-01-01",
            "fifoPnlRealized": "0",
        },
        {
            "tradeID": "put_assignment",
            "assetCategory": "OPT",
            "transactionType": "BookTrade",
            "buySell": "BUY",
            "openCloseIndicator": "C",
            "putCall": "P",
            "strike": "100",
            "expiry": "20250102",
            "underlyingSymbol": symbol,
            "symbol": f"{symbol} 250102P00100000",
            "quantity": "1",
            "tradePrice": "0",
            "closePrice": "0",
            "multiplier": "1",
            "ibCommission": "0",
            "fxRateToBase": "1",
            "currency": "USD",
            "dateTime": "2025-01-02 16:20:00",
            "tradeDate": "2025-01-02",
            "reportDate": "2025-01-02",
            "fifoPnlRealized": "0",
        },
        {
            "tradeID": "stock_open",
            "assetCategory": "STK",
            "subCategory": subcategory,
            "transactionType": "BookTrade",
            "buySell": "BUY",
            "openCloseIndicator": "O",
            "symbol": symbol,
            "isin": isin,
            "quantity": "1",
            "cost": "90",
            "proceeds": "-100",
            "fifoPnlRealized": "0",
            "fxRateToBase": "1",
            "currency": "USD",
            "dateTime": "2025-01-02 16:20:00",
            "tradeDate": "2025-01-02",
            "reportDate": "2025-01-02",
        },
        {
            "tradeID": "stock_close",
            "assetCategory": "STK",
            "subCategory": subcategory,
            "transactionType": "ExchTrade",
            "buySell": "SELL",
            "openCloseIndicator": "C",
            "symbol": symbol,
            "isin": isin,
            "quantity": "-1",
            "cost": "-90",
            "proceeds": "95",
            "fifoPnlRealized": "5",
            "fxRateToBase": "0.9",
            "currency": "USD",
            "dateTime": "2025-02-01 10:00:00",
            "tradeDate": "2025-02-01",
            "reportDate": "2025-02-01",
        },
    ]
    closed_lots = [{
        "assetCategory": "STK",
        "subCategory": subcategory,
        "currency": "USD",
        "symbol": symbol,
        "isin": isin,
        "underlyingSymbol": symbol,
        "openDateTime": "2025-01-02 16:20:00",
        "dateTime": "2025-02-01 10:00:00",
        "reportDate": "2025-02-01",
        "buySell": "SELL",
        "quantity": "1",
        "cost": "90",
        "fifoPnlRealized": "5",
        "fxRateToBase": "0.9",
    }]
    instruments = [{
        "assetCategory": "STK",
        "subCategory": subcategory,
        "symbol": symbol,
        "isin": isin,
    }]
    return trades, closed_lots, instruments


def _calculate_assignment(no_invstg=False):
    trades, lots, instruments = _put_assignment_fixture(no_invstg)
    with tempfile.TemporaryDirectory() as tmp:
        _write_csv(os.path.join(tmp, "trades.csv"), trades)
        _write_csv(os.path.join(tmp, "closed_lots.csv"), lots)
        _write_csv(os.path.join(tmp, "financial_instruments.csv"), instruments)
        _write_csv(os.path.join(tmp, "account_info.csv"), [{
            "currency": "EUR",
            "tax_year": "2025",
            "fx_transactions_count": "0",
        }])
        _write_csv(os.path.join(tmp, "conversion_rates.csv"), [
            {
                "reportDate": "2025-01-02",
                "fromCurrency": "USD",
                "toCurrency": "EUR",
                "rate": "1.0",
            },
            {
                "reportDate": "2025-02-01",
                "fromCurrency": "USD",
                "toCurrency": "EUR",
                "rate": "0.9",
            },
        ])
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp)


def _assert_close(actual, expected, label):
    if abs(actual - expected) > 0.0001:
        raise AssertionError(f"{label}: erwartet {expected}, aktuell {actual}")


def test_stock_gain_flips_to_loss_before_tageskurs_split():
    report = _calculate_assignment(no_invstg=False)
    _assert_close(report["stocks_gain_eur"], 0.0, "Aktiengewinne vor TK")
    _assert_close(report["stocks_loss_eur"], -4.5, "Aktienverluste vor TK")
    _assert_close(report["fx_correction_by_topf"]["Topf1"], -10.0, "Topf1 TK netto")
    _assert_close(report["fx_corr_gain_adj"]["Topf1"], 0.0, "Topf1 Gewinnkorrektur")
    _assert_close(report["fx_corr_loss_adj"]["Topf1"], -10.0, "Topf1 Verlustkorrektur")

    final_gain = report["stocks_gain_eur"] + report["fx_corr_gain_adj"]["Topf1"]
    final_loss = report["stocks_loss_eur"] + report["fx_corr_loss_adj"]["Topf1"]
    _assert_close(final_gain, 0.0, "Zeile 20")
    _assert_close(abs(final_loss), 14.5, "Zeile 23")
    _assert_close(
        final_gain + final_loss,
        report["topf_1_aktien_netto"] + report["fx_correction_by_topf"]["Topf1"],
        "Topf1 Brutto/Netto-Invariante",
    )


def test_no_invstg_gain_flips_to_loss_in_topf2():
    report = _calculate_assignment(no_invstg=True)
    _assert_close(report["fx_correction_by_topf"]["Topf2"], -10.0, "Topf2 TK netto")
    _assert_close(report["fx_corr_gain_adj"]["Topf2"], 0.0, "Topf2 Gewinnkorrektur")
    _assert_close(report["fx_corr_loss_adj"]["Topf2"], -10.0, "Topf2 Verlustkorrektur")
    final_gain = report["options_gain_eur"] + report["fx_corr_gain_adj"]["Topf2"]
    final_loss = report["options_loss_eur"] + report["fx_corr_loss_adj"]["Topf2"]
    _assert_close(final_gain, 10.0, "Topf2 Stillhalter genau einmal")
    _assert_close(final_loss, -14.5, "Zeile 22")
    _assert_close(
        final_gain + final_loss,
        report["options_gain_eur"] + report["options_loss_eur"] - 10.0,
        "Topf2 Brutto/Netto-Invariante",
    )


def test_all_sign_transitions_reconcile():
    cases = [
        (10.0, 5.0, 5.0, 0.0),
        (-10.0, -5.0, 0.0, -5.0),
        (5.0, -10.0, -5.0, -5.0),
        (-5.0, 10.0, 5.0, 5.0),
    ]
    for before, delta, expected_gain, expected_loss in cases:
        result = calculate_tageskurs_gross_adjustment(before, delta)
        _assert_close(result["gain_adjustment"], expected_gain, "Gewinnkorrektur")
        _assert_close(result["loss_adjustment"], expected_loss, "Verlustkorrektur")
        _assert_close(
            result["gain_adjustment"] + result["loss_adjustment"],
            delta,
            "Lot-Invariante",
        )


def test_topf2_breakdown_keeps_gross_columns_additive():
    breakdown = build_topf2_breakdown(
        {
            "Optionen": {"gain": 250.0, "loss": -80.0},
            "Futures": {"gain": 40.0, "loss": -120.0},
        },
        dividends_eur=25.0,
        interest_eur=-5.0,
        tageskurs_gain_adjustment=30.0,
        tageskurs_loss_adjustment=7.5,
        zufluss_adjustment=-12.5,
    )

    _assert_close(
        breakdown["total_gain"],
        sum(row["gain"] for row in breakdown["rows"]),
        "Topf2 Gewinnspalte",
    )
    _assert_close(
        breakdown["total_loss"],
        sum(row["loss"] for row in breakdown["rows"]),
        "Topf2 Verlustspalte",
    )
    _assert_close(
        breakdown["net"],
        breakdown["total_gain"] + breakdown["total_loss"],
        "Topf2 Netto",
    )
    adjustment = next(
        row for row in breakdown["rows"]
        if row["label"] == "Tageskurs-Anpassung"
    )
    _assert_close(adjustment["gain"], 30.0, "TK Gewinnanteil")
    _assert_close(adjustment["loss"], 7.5, "TK Verlustanteil")
    _assert_close(adjustment["net"], 37.5, "TK Netto")


def _mix_stock(tid, side, oc, day, qty, price, cost, pnl, fx, tx=""):
    return {
        "tradeID": tid, "transactionID": tx, "assetCategory": "STK",
        "subCategory": "COMMON", "transactionType": "ExchTrade",
        "buySell": side, "openCloseIndicator": oc, "symbol": "MIX",
        "conid": "77", "isin": "US0000000077", "quantity": str(qty),
        "tradePrice": str(price), "cost": str(cost),
        "proceeds": str(-qty * price), "fifoPnlRealized": str(pnl),
        "fxRateToBase": str(fx), "currency": "USD", "ibCommission": "0",
        "dateTime": f"{day} 10:00:00", "tradeDate": day, "reportDate": day,
    }


def _mix_lot(open_day, qty, cost, pnl, closing_tx=""):
    return {
        "assetCategory": "STK", "subCategory": "COMMON", "currency": "USD",
        "symbol": "MIX", "conid": "77", "isin": "US0000000077",
        "openDateTime": f"{open_day} 10:00:00",
        "dateTime": "2025-02-03 10:00:00", "reportDate": "2025-02-03",
        "buySell": "SELL", "quantity": str(qty), "cost": str(cost),
        "fifoPnlRealized": str(pnl), "fxRateToBase": "1.0",
        "closingTransactionID": closing_tx,
    }


def _mix_report(trades, lots):
    rates = [{"reportDate": day, "fromCurrency": "USD", "toCurrency": "EUR",
              "rate": rate}
             for day, rate in (("2025-01-02", "0.97"), ("2025-01-03", "1.0"),
                               ("2025-01-06", "1.0"), ("2025-02-03", "1.0"))]
    with tempfile.TemporaryDirectory() as tmp:
        _write_csv(os.path.join(tmp, "trades.csv"), trades)
        _write_csv(os.path.join(tmp, "closed_lots.csv"), lots)
        _write_csv(os.path.join(tmp, "conversion_rates.csv"), rates)
        _write_csv(os.path.join(tmp, "account_info.csv"), [{
            "currency": "EUR", "tax_year": "2025",
            "fx_transactions_count": "0",
        }])
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp)


_MIX_BUYS = [
    _mix_stock("B1", "BUY", "O", "2025-01-02", 10, 110, 1100, 0, 0.97),
    _mix_stock("B2", "BUY", "O", "2025-01-03", 5, 97, 485, 0, 1.0),
]


def _mixed_lot_sale_report(lot_order):
    """Ein Verkauf (15 Aktien zu 109 USD, Kurs 1,00) schließt zwei Lots:
    10 Aktien zu 110 USD (Kaufkurs 0,97, Ergebnis −10 USD) und 5 Aktien zu
    97 USD (Kaufkurs 1,00, Ergebnis +60 USD)."""
    trades = _MIX_BUYS + [
        _mix_stock("S1", "SELL", "C", "2025-02-03", -15, 109, -1585, 50, 1.0),
    ]
    lots = {"verlust": _mix_lot("2025-01-02", 10, 1100, -10),
            "gewinn": _mix_lot("2025-01-03", 5, 485, 60)}
    return _mix_report(trades, [lots[name] for name in lot_order])


def test_gross_split_follows_closing_trade_for_lots_of_mixed_sign():
    """Der Pool führt den Verkauf als einen Gewinn von 50 EUR; das
    Tageskurs-Delta (1.100 × 0,03 = 33 EUR) erhöht diesen Gewinn auf 83 EUR.
    Die Zuordnung je Lot sah beim Verlust-Lot −10 → +23 und buchte +10 in
    einen leeren Verlustpool (Zeile 23 = −10, Zeile 20 = 73)."""
    for order in (("verlust", "gewinn"), ("gewinn", "verlust")):
        report = _mixed_lot_sale_report(order)
        _assert_close(report["stocks_gain_eur"], 50.0, "Aktiengewinn vor TK")
        _assert_close(report["stocks_loss_eur"], 0.0, "Aktienverlust vor TK")
        _assert_close(report["fx_correction_by_topf"]["Topf1"], 33.0,
                      "Topf1 TK netto")
        _assert_close(report["fx_corr_gain_adj"]["Topf1"], 33.0,
                      "Topf1 Gewinnkorrektur")
        _assert_close(report["fx_corr_loss_adj"]["Topf1"], 0.0,
                      "Topf1 Verlustkorrektur")
        final = build_final_values(report, default_toggles())
        _assert_close(final["zeile_20"], 83.0, "Zeile 20")
        _assert_close(final["zeile_23"], 0.0, "Zeile 23")


def test_same_second_executions_keep_their_own_gross_split():
    """Codex-Befund (Runde 5): Zwei Verkäufe derselben Sekunde, 10 Aktien
    (Lot −10 USD, Kaufkurs 0,97) und 5 Aktien (Lot +60 USD). Die Pools
    führen −10 als Verlust und +60 als Gewinn; das Tageskurs-Delta von
    33 EUR kippt nur den Verlust (−10 → +23). Richtig: Zeile 20/23 = 83/0.
    Eine Gruppe je Zeitstempel saldierte beide Ausführungen (+50 → 83) und
    ließ den Verlust in Zeile 23 stehen (93/10). Gilt mit und ohne
    schließende transactionID am Lot."""
    for link in (False, True):
        for order in ((0, 1), (1, 0)):
            trades = _MIX_BUYS + [
                _mix_stock("S1", "SELL", "C", "2025-02-03", -10, 109, -1100,
                           -10, 1.0, tx="T1"),
                _mix_stock("S2", "SELL", "C", "2025-02-03", -5, 109, -485,
                           60, 1.0, tx="T2"),
            ]
            lots = [_mix_lot("2025-01-02", 10, 1100, -10,
                             "T1" if link else ""),
                    _mix_lot("2025-01-03", 5, 485, 60, "T2" if link else "")]
            report = _mix_report(trades, [lots[i] for i in order])
            _assert_close(report["fx_corr_gain_adj"]["Topf1"], 23.0,
                          "Topf1 Gewinnkorrektur")
            _assert_close(report["fx_corr_loss_adj"]["Topf1"], 10.0,
                          "Topf1 Verlustkorrektur")
            final = build_final_values(report, default_toggles())
            _assert_close(final["zeile_20"], 83.0, "Zeile 20")
            _assert_close(final["zeile_23"], 0.0, "Zeile 23")
            # Ohne Verknüpfung ist die Rechnung je Lot hier zufällig richtig,
            # belegt ist sie nicht: Prüffall (Codex-Runde 6).
            coverage = report["audit"]["closed_lot_coverage"]
            assert coverage["lots_without_closing_execution"] == (
                0 if link else 2), coverage


def test_pure_opening_in_same_second_keeps_execution_unique():
    """Codex-Befund (Runde 6): Der Verkauf aus dem Test oben (ein Gewinn-
    und ein Verlust-Lot, Pool +50, Delta +33) ohne transactionID; in
    derselben Sekunde eröffnet ein weiterer Verkauf nur einen Short
    (Indikator 'O', Ergebnis 0). Er schließt kein Lot und darf den Verkauf
    nicht mehrdeutig machen: Zeile 19/20/22/23 = 83/83/0/0 statt
    83/73/0/10 aus der Rechnung je Lot."""
    for order in ((0, 1), (1, 0)):
        trades = _MIX_BUYS + [
            _mix_stock("S1", "SELL", "C", "2025-02-03", -15, 109, -1585, 50,
                       1.0),
            _mix_stock("S0", "SELL", "O", "2025-02-03", -3, 109, -327, 0,
                       1.0),
        ]
        lots = [_mix_lot("2025-01-02", 10, 1100, -10),
                _mix_lot("2025-01-03", 5, 485, 60)]
        report = _mix_report(trades, [lots[i] for i in order])
        _assert_close(report["fx_corr_gain_adj"]["Topf1"], 33.0,
                      "Topf1 Gewinnkorrektur")
        _assert_close(report["fx_corr_loss_adj"]["Topf1"], 0.0,
                      "Topf1 Verlustkorrektur")
        final = build_final_values(report, default_toggles())
        _assert_close(final["zeile_19"], 83.0, "Zeile 19")
        _assert_close(final["zeile_20"], 83.0, "Zeile 20")
        _assert_close(final["zeile_22"], 0.0, "Zeile 22")
        _assert_close(final["zeile_23"], 0.0, "Zeile 23")
        assert report["audit"]["closed_lot_coverage"][
            "lots_without_closing_execution"] == 0


def test_unlinked_lots_on_one_side_are_not_a_review_case():
    """Zwei Verkäufe derselben Sekunde ohne transactionID, beide Lots
    bleiben nach der Tageskurs-Korrektur Gewinne (+90 → +120, +60). Jede
    Zuordnung der Lots zu den Ausführungen liefert dieselben Zeilen; kein
    Prüffall."""
    trades = [
        _mix_stock("B1", "BUY", "O", "2025-01-02", 10, 100, 1000, 0, 0.97),
        _mix_stock("B2", "BUY", "O", "2025-01-03", 5, 97, 485, 0, 1.0),
        _mix_stock("S1", "SELL", "C", "2025-02-03", -10, 109, -1000, 90, 1.0),
        _mix_stock("S2", "SELL", "C", "2025-02-03", -5, 109, -485, 60, 1.0),
    ]
    lots = [_mix_lot("2025-01-02", 10, 1000, 90),
            _mix_lot("2025-01-03", 5, 485, 60)]
    report = _mix_report(trades, lots)
    _assert_close(report["fx_corr_gain_adj"]["Topf1"], 30.0,
                  "Topf1 Gewinnkorrektur")
    final = build_final_values(report, default_toggles())
    _assert_close(final["zeile_20"], 180.0, "Zeile 20")
    _assert_close(final["zeile_23"], 0.0, "Zeile 23")
    assert report["audit"]["closed_lot_coverage"][
        "lots_without_closing_execution"] == 0


def _premium_and_second_sale_report(lot_order, linked):
    """Put-Andienung TEST (Prämie 10 USD zu 1,00, Lot-Basis 90 statt
    Strike 100) und ein Börsenkauf zu 100 USD am 03.01. (Kurs 1,00); beide
    Aktien gehen in derselben Sekunde zu 120 USD (Kurs 0,90) weg. Je
    Ausführung: 18 EUR Gewinn, Tageskurs je -10 EUR."""
    trades, lots, instruments = _put_assignment_fixture()
    stock_close = trades[3]
    stock_close.update(proceeds="120", fifoPnlRealized="30",
                       transactionID="T1" if linked else "")
    trades += [
        dict(stock_close, tradeID="stock_buy2", transactionType="ExchTrade",
             buySell="BUY", openCloseIndicator="O", quantity="1",
             cost="100", proceeds="-100", fifoPnlRealized="0",
             fxRateToBase="1.0", dateTime="2025-01-03 10:00:00",
             tradeDate="2025-01-03", reportDate="2025-01-03",
             transactionID=""),
        dict(stock_close, tradeID="stock_close2", cost="-100",
             fifoPnlRealized="20", transactionID="T2" if linked else ""),
    ]
    lot_a = dict(lots[0], fifoPnlRealized="30",
                 closingTransactionID="T1" if linked else "")
    lot_b = dict(lots[0], openDateTime="2025-01-03 10:00:00", cost="100",
                 fifoPnlRealized="20",
                 closingTransactionID="T2" if linked else "")
    chosen = {"andienung": lot_a, "kauf": lot_b}
    with tempfile.TemporaryDirectory() as tmp:
        _write_csv(os.path.join(tmp, "trades.csv"), trades)
        _write_csv(os.path.join(tmp, "closed_lots.csv"),
                   [chosen[name] for name in lot_order])
        _write_csv(os.path.join(tmp, "financial_instruments.csv"), instruments)
        _write_csv(os.path.join(tmp, "account_info.csv"), [{
            "currency": "EUR", "tax_year": "2025",
            "fx_transactions_count": "0",
        }])
        _write_csv(os.path.join(tmp, "conversion_rates.csv"), [
            {"reportDate": day, "fromCurrency": "USD", "toCurrency": "EUR",
             "rate": rate}
            for day, rate in (("2025-01-02", "1.0"), ("2025-01-03", "1.0"),
                              ("2025-02-01", "0.9"))])
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp)


def test_unlinked_lots_with_premium_correction_are_a_review_case():
    """Codex-Befund (Runde 7), Stillhalter-Variante: Ohne transactionID
    übernimmt das zuerst verarbeitete Lot die Prämienkorrektur. Zieht sie
    das Andienungs-Lot, bleiben beide Lots Gewinne (18 -> 8, 18 -> 8) und
    die Vorzeichen sähen eindeutig aus; zieht sie das Kauf-Lot, kippt es
    (9 -> -1) und Zeile 20/23 = 17/1 statt 16/0. Die Zuordnung ist ohne
    Ausführungs-ID nicht belegt: Prüffall in beiden Reihenfolgen. Mit ID
    gilt 26/16/0/0 ohne Prüffall."""
    for order in (("andienung", "kauf"), ("kauf", "andienung")):
        for linked in (True, False):
            report = _premium_and_second_sale_report(order, linked)
            _assert_close(report["audit"]["stk_correction_cy"], 9.0,
                          "Prämienkorrektur")
            _assert_close(report["fx_correction_total"], -20.0, "Tageskurs")
            final = build_final_values(report, default_toggles())
            _assert_close(final["zeile_19"], 26.0, "Zeile 19")
            unlinked = report["audit"]["closed_lot_coverage"][
                "lots_without_closing_execution"]
            if linked:
                _assert_close(final["zeile_20"], 16.0, "Zeile 20")
                _assert_close(final["zeile_23"], 0.0, "Zeile 23")
                assert unlinked == 0, unlinked
            else:
                assert unlinked == 2, (order, unlinked)


def test_closing_transaction_links_lots_to_their_execution():
    """Zwei Verkäufe derselben Sekunde: S1 (15 Aktien) schließt das
    Verlust- und das Gewinn-Lot aus dem Test oben (Pool +50, Delta +33),
    S2 (5 Aktien, Kauf zu 113 USD) schließt ein Lot mit −20 ohne Delta.
    Je Ausführung: Zeile 20 = 50 + 33 = 83, Zeile 23 = 20. Ohne die
    Zuordnung über closingTransactionID bliebe nur die Rechnung je Lot
    (73/10), weil zwei Ausführungen zum Zeitstempel passen."""
    trades = _MIX_BUYS + [
        _mix_stock("B3", "BUY", "O", "2025-01-06", 5, 113, 565, 0, 1.0),
        _mix_stock("S1", "SELL", "C", "2025-02-03", -15, 109, -1585, 50, 1.0,
                   tx="T1"),
        _mix_stock("S2", "SELL", "C", "2025-02-03", -5, 109, -565, -20, 1.0,
                   tx="T2"),
    ]
    lots = [_mix_lot("2025-01-02", 10, 1100, -10, "T1"),
            _mix_lot("2025-01-06", 5, 565, -20, "T2"),
            _mix_lot("2025-01-03", 5, 485, 60, "T1")]
    report = _mix_report(trades, lots)
    _assert_close(report["fx_corr_gain_adj"]["Topf1"], 33.0,
                  "Topf1 Gewinnkorrektur")
    _assert_close(report["fx_corr_loss_adj"]["Topf1"], 0.0,
                  "Topf1 Verlustkorrektur")
    final = build_final_values(report, default_toggles())
    _assert_close(final["zeile_20"], 83.0, "Zeile 20")
    _assert_close(final["zeile_23"], 20.0, "Zeile 23")


if __name__ == "__main__":
    test_stock_gain_flips_to_loss_before_tageskurs_split()
    test_no_invstg_gain_flips_to_loss_in_topf2()
    test_all_sign_transitions_reconcile()
    test_topf2_breakdown_keeps_gross_columns_additive()
    test_gross_split_follows_closing_trade_for_lots_of_mixed_sign()
    test_same_second_executions_keep_their_own_gross_split()
    test_closing_transaction_links_lots_to_their_execution()
    test_unlinked_lots_with_premium_correction_are_a_review_case()
    test_pure_opening_in_same_second_keeps_execution_unique()
    test_unlinked_lots_on_one_side_are_not_a_review_case()
    print("OK: Tageskurs-Bruttozuordnung")
