"""Synthetische Regression-Tests fuer GH Issues #56, #61 und #62.

Cross-Year-Same-Series-FIFO-Konflikt: Wenn dieselbe Option-Series sowohl im
Vorjahr als auch im Steuerjahr angedient wurde, hat der Same-Year-Block frueher
faelschlich die aeltesten Sells konsumiert (die im Vorjahres-Lauf bereits
versteuert waren). Pre-consume im _current_year_series_state-Build verschiebt
den FIFO-Startpunkt auf die juengeren Sells.

Mixed-Year-Konsum: Wenn eine Steuerjahr-Andienung Sells aus mehreren Jahren
konsumiert, muss nur der Vorjahresanteil als cross-year gelten.

Aufruf: python tests/test_cross_year_series.py
"""
import os
import sys
import contextlib
import csv
import io
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (
    _build_stillhalter_details_for_assignment,
    _claim_long_put_exercise_short_shares,
    _consume_open_sells_fifo,
    _correction_matches_row,
    _get_open_option_sells,
    _long_put_exercise_short_openings,
    calculate_tax,
    safe_float,
)


def make_sell(date, qty, price, strike="100", expiry="2024-12-20", pc="P",
              underlying="TEST", a_cat="OPT", multiplier="100", commission=-1.0):
    return {
        "tradeID": f"sell_{date}_{qty}",
        "assetCategory": a_cat,
        "transactionType": "ExchTrade",
        "buySell": "SELL",
        "putCall": pc,
        "strike": strike,
        "expiry": expiry,
        "underlyingSymbol": underlying,
        "symbol": f"{underlying} {strike} {expiry} {pc}",
        "quantity": str(-qty),
        "tradePrice": str(price),
        "closePrice": str(price),
        "multiplier": multiplier,
        "ibCommission": str(commission),
        "fxRateToBase": "1.0",
        "dateTime": f"{date} 10:00:00",
        "tradeDate": date,
        "reportDate": date,
        "fifoPnlRealized": "0",
    }


def make_assignment(date, qty, strike="100", expiry="2024-12-20", pc="P",
                    underlying="TEST", a_cat="OPT", multiplier="100"):
    return {
        "tradeID": f"assign_{date}_{qty}",
        "assetCategory": a_cat,
        "transactionType": "BookTrade",
        "buySell": "BUY",
        "putCall": pc,
        "strike": strike,
        "expiry": expiry,
        "underlyingSymbol": underlying,
        "symbol": f"{underlying} {strike} {expiry} {pc}",
        "quantity": str(qty),
        "tradePrice": "0",
        "closePrice": "0",
        "multiplier": multiplier,
        "ibCommission": "0",
        "fxRateToBase": "1.0",
        "dateTime": f"{date} 16:20:00",
        "tradeDate": date,
        "reportDate": date,
        "fifoPnlRealized": "0",
    }


def make_buy_close(date, qty, price, pnl, strike="100", expiry="2024-12-20",
                   pc="P", underlying="TEST", a_cat="OPT", multiplier="100"):
    return {
        "tradeID": f"close_{underlying}_{date}_{qty}_{price}",
        "assetCategory": a_cat,
        "transactionType": "ExchTrade",
        "buySell": "BUY",
        "putCall": pc,
        "strike": strike,
        "expiry": expiry,
        "underlyingSymbol": underlying,
        "symbol": f"{underlying} {strike} {expiry} {pc}",
        "quantity": str(qty),
        "tradePrice": str(price),
        "closePrice": str(price),
        "multiplier": multiplier,
        "ibCommission": "0",
        "fxRateToBase": "1.0",
        "currency": "USD",
        "dateTime": f"{date} 10:00:00",
        "tradeDate": date,
        "reportDate": date,
        "fifoPnlRealized": str(pnl),
    }


def make_expiry(date, qty, pnl, strike="100", expiry="2024-12-20", pc="P",
                underlying="TEST", a_cat="OPT", multiplier="100"):
    """Wertloser Verfall eines Shorts: BookTrade BUY mit fifoPnlRealized = Praemie."""
    return {
        "tradeID": f"expire_{underlying}_{date}_{qty}",
        "assetCategory": a_cat,
        "transactionType": "BookTrade",
        "buySell": "BUY",
        "putCall": pc,
        "strike": strike,
        "expiry": expiry,
        "underlyingSymbol": underlying,
        "symbol": f"{underlying} {strike} {expiry} {pc}",
        "quantity": str(qty),
        "tradePrice": "0",
        "closePrice": "0",
        "multiplier": multiplier,
        "ibCommission": "0",
        "fxRateToBase": "1.0",
        "dateTime": f"{date} 16:20:00",
        "tradeDate": date,
        "reportDate": date,
        "fifoPnlRealized": str(pnl),
        "notes": "Ep",
    }


def calculate_for_trades(trades, tax_year=2022, closed_lots=None, conversion_rates=None):
    fieldnames = sorted({k for row in trades for k in row})
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "account_info.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["currency", "tax_year", "fx_transactions_count"])
            writer.writeheader()
            writer.writerow({"currency": "EUR", "tax_year": str(tax_year), "fx_transactions_count": "0"})
        with open(os.path.join(tmp, "trades.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(trades)
        if closed_lots:
            lot_fields = sorted({k for row in closed_lots for k in row})
            with open(os.path.join(tmp, "closed_lots.csv"), "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=lot_fields)
                writer.writeheader()
                writer.writerows(closed_lots)
        if conversion_rates:
            with open(os.path.join(tmp, "conversion_rates.csv"), "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["reportDate", "fromCurrency", "toCurrency", "rate"])
                writer.writeheader()
                writer.writerows(conversion_rates)
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp)


def simulate_pre_consume(trades, series_key, tax_year, base_currency="EUR",
                         usd_to_eur_rates=None):
    """Repliziert den Pre-consume-Block aus calculate_tax_report.py.

    Liefert den State NACH Pre-consume zurueck. Same-Year-Iteration kann
    dann auf diesem State fortsetzen.
    """
    a_cat, a_underlying, strike, expiry, pc = series_key
    assign_qty_series = sum(
        abs(int(safe_float(t.get("quantity"))))
        for t in trades
        if t.get("assetCategory") == a_cat
        and t.get("transactionType") == "BookTrade"
        and t.get("buySell") == "BUY"
        and t.get("strike") == strike
        and t.get("expiry") == expiry
        and t.get("putCall") == pc
        and t.get("underlyingSymbol", "") == a_underlying
        and abs(safe_float(t.get("fifoPnlRealized"))) < 0.01
    )
    state = _get_open_option_sells(
        trades, a_cat, strike, expiry, pc, assign_qty_series, underlying=a_underlying
    )

    from calculate_tax_report import parse_date
    prior_assigns = sorted(
        [t for t in trades
         if t.get("assetCategory") == a_cat
         and t.get("transactionType") == "BookTrade"
         and t.get("buySell") == "BUY"
         and t.get("strike") == strike
         and t.get("expiry") == expiry
         and t.get("putCall") == pc
         and t.get("underlyingSymbol", "") == a_underlying
         and abs(safe_float(t.get("fifoPnlRealized"))) < 0.01
         and (pd_ := parse_date(t.get("reportDate") or t.get("dateTime") or t.get("tradeDate"))) is not None
         and pd_.year < tax_year],
        key=lambda t: (t.get("dateTime", "") or t.get("tradeDate", "") or t.get("reportDate", "") or "")
    )
    if prior_assigns and state:
        first_open_pre = next((o for o in state if o.get("_open_qty", 0) > 0), None)
        if first_open_pre and safe_float(first_open_pre.get("multiplier")) > 0:
            mult_pre = int(safe_float(first_open_pre.get("multiplier"), 100))
        else:
            mult_pre = int(safe_float(prior_assigns[0].get("multiplier"), 100))
        for pa in prior_assigns:
            pa_qty = abs(int(safe_float(pa.get("quantity"))))
            if pa_qty <= 0:
                continue
            _consume_open_sells_fifo(state, pa_qty, mult_pre, base_currency, usd_to_eur_rates)

    return state


def assert_close(actual, expected, tol=0.001, label=""):
    if abs(actual - expected) > tol:
        raise AssertionError(f"{label}: erwartet {expected}, aktuell {actual} (delta {actual - expected})")


def test_cross_year_put_series():
    """TC1: Put-Series mit Vorjahr- und Steuerjahr-Andienung.

    Sells und Andienungen so konstruiert, dass close_qty = 0 (alle Sells offen).
    Vor-Fix (ohne Pre-consume): Same-Year-Block startet bei aeltestem Sell ->
    falsche Praemie. Mit Pre-consume: Vorjahres-Andienung verbraucht aeltesten
    Sell, Same-Year-Block startet bei juengerem Sell -> korrekte Praemie.
    """
    trades = [
        make_sell("2023-01-15", 10, 1.00),
        make_sell("2023-06-15", 10, 3.00),
        make_sell("2024-03-15", 10, 5.00),
        make_assignment("2023-12-15", 10),
        make_assignment("2024-04-15", 20),
    ]
    series_key = ("OPT", "TEST", "100", "2024-12-20", "P")
    state = simulate_pre_consume(trades, series_key, tax_year=2024)

    open_after_pre = [(o.get("dateTime"), o.get("_open_qty")) for o in state]
    assert open_after_pre[0][1] == 0, f"2023-01-Sell muss nach Pre-consume 0 sein, ist {open_after_pre[0][1]}"
    assert open_after_pre[1][1] == 10, f"2023-06-Sell muss 10 sein, ist {open_after_pre[1][1]}"
    assert open_after_pre[2][1] == 10, f"2024-03-Sell muss 10 sein, ist {open_after_pre[2][1]}"

    premium_raw, _comm, _fx, premium_eur, sells_consumed, consumed = _consume_open_sells_fifo(
        state, a_qty=20, mult=100, base_currency="EUR"
    )

    assert consumed == 20, f"erwartet 20 ct konsumiert, aktuell {consumed}"
    assert_close(premium_raw, 10 * 3 * 100 + 10 * 5 * 100, label="TC1 premium_raw")
    consumed_dates = [o[0].get("dateTime") for o in sells_consumed]
    assert "2023-06-15 10:00:00" in consumed_dates and "2024-03-15 10:00:00" in consumed_dates, \
        f"erwartete Sells: 2023-06 + 2024-03, aktuell {consumed_dates}"
    assert "2023-01-15 10:00:00" not in consumed_dates, "2023-01-Sell darf NICHT im Same-Year-Konsum sein"

    print("  TC1 Cross-Year-Put-Series: OK")
    print(f"    Same-Year-Praemie raw = {premium_raw:.2f} USD (erwartet 8000.00)")


def test_cross_year_call_series():
    """TC2: Call-Series mit Vorjahr- und Steuerjahr-Andienung.

    Pre-consume gilt fuer Calls UND Puts (series_key enthaelt pc).
    Vorjahres-Call-Praemie wird verworfen (im Vorjahres-Lauf bereits versteuert),
    Same-Year-Block sieht nur die juengeren Sells.
    """
    trades = [
        make_sell("2023-02-10", 5, 2.00, pc="C", underlying="AAPL"),
        make_sell("2024-01-10", 5, 4.00, pc="C", underlying="AAPL"),
        make_assignment("2023-12-15", 5, pc="C", underlying="AAPL"),
        make_assignment("2024-05-15", 5, pc="C", underlying="AAPL"),
    ]
    series_key = ("OPT", "AAPL", "100", "2024-12-20", "C")
    state = simulate_pre_consume(trades, series_key, tax_year=2024)

    open_after_pre = [(o.get("dateTime"), o.get("_open_qty")) for o in state]
    assert open_after_pre[0][1] == 0, f"2023-02-Sell muss 0 sein nach Pre-consume, ist {open_after_pre[0][1]}"
    assert open_after_pre[1][1] == 5, f"2024-01-Sell muss 5 sein, ist {open_after_pre[1][1]}"

    premium_raw, _comm, _fx, premium_eur, sells_consumed, consumed = _consume_open_sells_fifo(
        state, a_qty=5, mult=100, base_currency="EUR"
    )
    assert consumed == 5, f"erwartet 5 ct konsumiert, aktuell {consumed}"
    assert_close(premium_raw, 5 * 4 * 100, label="TC2 premium_raw")
    consumed_dates = [o[0].get("dateTime") for o in sells_consumed]
    assert "2024-01-10 10:00:00" in consumed_dates, \
        f"erwartet 2024-01-Sell, aktuell {consumed_dates}"
    assert "2023-02-10 10:00:00" not in consumed_dates, \
        "2023-02-Sell darf nicht doppelt versteuert werden"

    print("  TC2 Cross-Year-Call-Series: OK")
    print(f"    Same-Year-Praemie raw = {premium_raw:.2f} USD (erwartet 2000.00)")


def test_steueryahr_only_no_op():
    """TC3: Series ohne Vorjahres-Andienung. Pre-consume ist no-op."""
    trades = [
        make_sell("2024-02-10", 10, 2.50),
        make_sell("2024-08-10", 10, 4.00),
        make_assignment("2024-09-15", 10),
    ]
    series_key = ("OPT", "TEST", "100", "2024-12-20", "P")
    state = simulate_pre_consume(trades, series_key, tax_year=2024)

    open_qtys = sum(o.get("_open_qty", 0) for o in state)
    assert open_qtys == 10, f"State muss 10 OPEN qty haben (close_qty=10), ist {open_qtys}"

    premium_raw, _comm, _fx, premium_eur, sells_consumed, consumed = _consume_open_sells_fifo(
        state, a_qty=10, mult=100, base_currency="EUR"
    )
    assert consumed == 10
    consumed_dates = [o[0].get("dateTime") for o in sells_consumed]
    assert len(consumed_dates) == 1, f"erwartet 1 Sell konsumiert, aktuell {len(consumed_dates)}"
    print(f"  TC3 Steuerjahr-only no-op: OK (consumed Sell {consumed_dates[0]})")


def test_mixed_year_assignment_splits_cross_year_premium():
    """TC4: Eine Steuerjahr-Andienung konsumiert Sells aus zwei Jahren.

    Nur der 2023-Anteil darf cross-year sein. Vor Issue #62 wurde wegen des
    fruehesten orig_sell_date die komplette Assignment-Praemie markiert.
    """
    trades = [
        make_sell("2023-06-15", 2, 3.00),
        make_sell("2024-03-15", 5, 5.00),
        make_assignment("2024-04-15", 7),
    ]
    assignment = trades[-1]
    state = _get_open_option_sells(
        trades, "OPT", "100", "2024-12-20", "P", 7, underlying="TEST"
    )
    premium_raw, commission_raw, _fx, premium_eur, sells_consumed, consumed = _consume_open_sells_fifo(
        state, a_qty=7, mult=100, base_currency="EUR"
    )
    assert consumed == 7

    details = _build_stillhalter_details_for_assignment(
        assignment, "100", "2024-12-20", "P", 7, 100, 2024,
        sells_consumed, premium_raw, commission_raw, premium_eur, base_currency="EUR"
    )

    assert len(details) == 2, f"erwartet 2 Detail-Splits, aktuell {len(details)}"
    by_year = {d["orig_sell_year"]: d for d in details}
    assert by_year[2023]["quantity"] == 2
    assert by_year[2024]["quantity"] == 5
    assert by_year[2023]["is_cross_year"] is True
    assert by_year[2024]["is_cross_year"] is False

    cross_year_premium = sum(d["premium_eur"] for d in details if d["is_cross_year"])
    detail_total = sum(d["premium_eur"] for d in details)
    assert_close(cross_year_premium, 2 * 3 * 100 - 1, label="TC4 cross_year_premium")
    assert_close(detail_total, premium_eur, label="TC4 detail_total")

    print("  TC4 Mixed-Year-Assignment-Split: OK")
    print(f"    Cross-Year-Praemie = {cross_year_premium:.2f} EUR, Gesamt = {detail_total:.2f} EUR")


def test_issue_56_prior_year_correction_uses_underlying():
    """TC5: Vorjahres-Zufluss darf gleichartige Serien anderer Underlyings nicht konsumieren."""
    trades = [
        make_sell("2021-12-01", 2, 19.90, strike="155", expiry="2022-01-21", underlying="GPN"),
        make_sell("2021-12-03", 1, 3.20, strike="155", expiry="2022-01-21", underlying="SQ"),
        make_buy_close("2022-01-05", 1, 11.65, -847, strike="155", expiry="2022-01-21", underlying="SQ"),
    ]
    rd = calculate_for_trades(trades, tax_year=2022)
    audit = rd.get("audit", {})

    assert_close(audit.get("prior_zufluss_correction_eur", 0), 319.0,
                 label="TC5 prior_zufluss_correction_eur")
    details = audit.get("prior_zufluss_details", [])
    assert len(details) == 1, f"erwartet 1 Vorjahres-Korrektur, aktuell {len(details)}"
    assert details[0].get("underlyingSymbol") == "SQ", \
        f"erwartet SQ-Korrektur, aktuell {details[0].get('underlyingSymbol')}"

    print("  TC5 Issue #56 Vorjahres-Korrektur nach Underlying: OK")
    print(f"    Korrektur = {audit.get('prior_zufluss_correction_eur', 0):.2f} EUR (SQ, nicht GPN)")


def test_issue_56_current_year_zufluss_uses_underlying():
    """TC6: Current-year Zufluss muss offene Fills pro Underlying bestimmen."""
    trades = [
        make_sell("2022-01-01", 1, 10.00, strike="155", expiry="2022-01-21", underlying="GPN"),
        make_sell("2022-01-02", 1, 2.00, strike="155", expiry="2022-01-21", underlying="SQ"),
        make_buy_close("2022-01-03", 1, 5.00, -300, strike="155", expiry="2022-01-21", underlying="SQ"),
    ]
    rd = calculate_for_trades(trades, tax_year=2022)
    audit = rd.get("audit", {})

    assert_close(audit.get("zufluss_premium_eur", 0), 999.0,
                 label="TC6 zufluss_premium_eur")
    details = audit.get("zufluss_details", [])
    assert len(details) == 1, f"erwartet 1 offene Zufluss-Position, aktuell {len(details)}"
    assert details[0].get("underlyingSymbol") == "GPN", \
        f"erwartet GPN-Zufluss, aktuell {details[0].get('underlyingSymbol')}"

    print("  TC6 Issue #56 Current-Year-Zufluss nach Underlying: OK")
    print(f"    Zufluss = {audit.get('zufluss_premium_eur', 0):.2f} EUR (GPN offen, SQ geschlossen)")


def _mu_put_assignment_trade_set(stock_cost, stock_pnl, assignment_datetime="2025-04-28 16:20:00",
                                 stock_book_cost=None):
    """Synthetic MU weekly put assignment based on the user-reported screenshots."""
    premium = 184.37773
    return [
        {
            "tradeID": "mu_put_sell",
            "assetCategory": "OPT",
            "transactionType": "ExchTrade",
            "buySell": "SELL",
            "openCloseIndicator": "O",
            "putCall": "P",
            "strike": "84",
            "expiry": "2025-04-25",
            "underlyingSymbol": "MU",
            "symbol": "MU 25APR25 84 P",
            "description": "MU 25APR25 84 P",
            "quantity": "-1",
            "tradePrice": str(premium / 100),
            "closePrice": str(premium / 100),
            "multiplier": "100",
            "ibCommission": "0",
            "fxRateToBase": "0.87998",
            "currency": "USD",
            "dateTime": "2025-04-25 10:00:00",
            "tradeDate": "2025-04-25",
            "reportDate": "2025-04-25",
            "fifoPnlRealized": "0",
            "cost": "0",
            "proceeds": str(premium),
        },
        {
            "tradeID": "mu_put_assignment",
            "assetCategory": "OPT",
            "transactionType": "BookTrade",
            "buySell": "BUY",
            "openCloseIndicator": "C",
            "putCall": "P",
            "strike": "84",
            "expiry": "2025-04-25",
            "underlyingSymbol": "MU",
            "symbol": "MU 25APR25 84 P",
            "description": "MU 25APR25 84 P",
            "quantity": "1",
            "tradePrice": "0",
            "closePrice": "4.22",
            "multiplier": "100",
            "ibCommission": "0",
            "fxRateToBase": "0.87551",
            "currency": "USD",
            "dateTime": assignment_datetime,
            "tradeDate": "2025-04-25",
            "reportDate": assignment_datetime[:10],
            "fifoPnlRealized": "0",
            "cost": "0",
            "proceeds": "0",
        },
        {
            "tradeID": "mu_stock_assignment",
            "assetCategory": "STK",
            "transactionType": "BookTrade",
            "buySell": "BUY",
            "openCloseIndicator": "O",
            "underlyingSymbol": "MU",
            "symbol": "MU",
            "description": "MICRON TECHNOLOGY INC",
            "quantity": "100",
            "tradePrice": "84",
            "closePrice": "78.56",
            "ibCommission": "0",
            "fxRateToBase": "0.87551",
            "currency": "USD",
            "dateTime": "2025-04-25 16:20:00",
            "tradeDate": "2025-04-25",
            "reportDate": assignment_datetime[:10],
            "fifoPnlRealized": "0",
            "cost": str(stock_book_cost if stock_book_cost is not None else stock_cost),
            "proceeds": "-8400",
            "isin": "US5951121038",
        },
        {
            "tradeID": "mu_stock_sale",
            "assetCategory": "STK",
            "transactionType": "ExchTrade",
            "buySell": "SELL",
            "openCloseIndicator": "C",
            "underlyingSymbol": "MU",
            "symbol": "MU",
            "description": "MICRON TECHNOLOGY INC",
            "quantity": "-100",
            "tradePrice": "116.38",
            "closePrice": "116.38",
            "ibCommission": "-1.02",
            "fxRateToBase": "0.8705",
            "currency": "USD",
            "dateTime": "2025-06-11 10:00:00",
            "tradeDate": "2025-06-11",
            "reportDate": "2025-06-11",
            "fifoPnlRealized": str(stock_pnl),
            "cost": str(stock_cost),
            "proceeds": "11636.98",
            "isin": "US5951121038",
        },
    ]


def _mu_closed_lot(cost):
    return [{
        "assetCategory": "STK",
        "currency": "USD",
        "reportDate": "2025-06-11",
        "dateTime": "2025-06-11 10:00:00",
        "openDateTime": "2025-04-25 16:20:00",
        "quantity": "100",
        "cost": str(cost),
        "fifoPnlRealized": "3236.98",
        "fxRateToBase": "0.8705",
        "symbol": "MU",
        "description": "MICRON TECHNOLOGY INC",
        "isin": "US5951121038",
        "underlyingSymbol": "MU",
    }]


def test_put_assignment_does_not_double_correct_strike_basis():
    """TC7: Weekly/early put assignment where IBKR already uses strike as stock basis."""
    trades = _mu_put_assignment_trade_set(8400.0, 3236.98, stock_book_cost=8400.0)
    rd = calculate_for_trades(
        trades,
        tax_year=2025,
        closed_lots=_mu_closed_lot(8400.0),
        conversion_rates=[
            {"reportDate": "2025-04-25", "fromCurrency": "USD", "toCurrency": "EUR", "rate": "0.87998"},
            {"reportDate": "2025-06-11", "fromCurrency": "USD", "toCurrency": "EUR", "rate": "0.87050"},
        ],
    )
    stock_rows = [r for r in rd["trade_details"] if r.get("symbol") == "MU" and r.get("source") == "trades"]
    assert len(stock_rows) == 1, f"erwartet 1 MU-Aktienzeile, aktuell {len(stock_rows)}"
    assert_close(stock_rows[0]["cost"], 8400.0, label="TC7 stock cost")
    assert_close(stock_rows[0]["fifoPnlRealized"], 3236.98, label="TC7 stock pnl raw")

    fx_details = rd.get("fx_correction_details", [])
    assert len(fx_details) == 1, f"erwartet 1 Tageskurs-Lot, aktuell {len(fx_details)}"
    assert_close(fx_details[0]["cost"], 8400.0, label="TC7 Tageskurs cost")

    print("  TC7 Put-Assignment mit Strike-Basis nicht doppelt korrigiert: OK")
    print(f"    Kostenbasis bleibt {stock_rows[0]['cost']:.2f} USD")


def test_put_assignment_corrects_reduced_cost_basis():
    """TC8: Put assignment where IBKR stock basis is reduced by the premium."""
    premium = 184.37773
    reduced_cost = 8400.0 - premium
    reduced_pnl = 11636.98 - reduced_cost
    trades = _mu_put_assignment_trade_set(
        reduced_cost, reduced_pnl, assignment_datetime="2025-04-25 16:20:00",
        stock_book_cost=reduced_cost
    )
    rd = calculate_for_trades(
        trades,
        tax_year=2025,
        closed_lots=_mu_closed_lot(reduced_cost),
        conversion_rates=[
            {"reportDate": "2025-04-25", "fromCurrency": "USD", "toCurrency": "EUR", "rate": "0.87998"},
            {"reportDate": "2025-06-11", "fromCurrency": "USD", "toCurrency": "EUR", "rate": "0.87050"},
        ],
    )
    stock_rows = [r for r in rd["trade_details"] if r.get("symbol") == "MU" and r.get("source") == "trades"]
    assert len(stock_rows) == 1, f"erwartet 1 MU-Aktienzeile, aktuell {len(stock_rows)}"
    assert_close(stock_rows[0]["cost"], 8400.0, label="TC8 stock cost")
    assert_close(stock_rows[0]["fifoPnlRealized"], 3236.98, label="TC8 stock pnl raw")
    assert_close(rd["fx_correction_details"][0]["cost"], 8400.0, label="TC8 Tageskurs cost")

    print("  TC8 Put-Assignment mit reduzierter IBKR-Basis korrigiert: OK")
    print(f"    {reduced_cost:.2f} USD -> {stock_rows[0]['cost']:.2f} USD")


def test_same_day_put_assignment_does_not_double_correct_strike_basis():
    """TC9: Early/same-day put assignment where IBKR already uses strike as stock basis."""
    trades = _mu_put_assignment_trade_set(
        8400.0, 3236.98, assignment_datetime="2025-04-25 16:20:00",
        stock_book_cost=8400.0
    )
    rd = calculate_for_trades(
        trades,
        tax_year=2025,
        closed_lots=_mu_closed_lot(8400.0),
        conversion_rates=[
            {"reportDate": "2025-04-25", "fromCurrency": "USD", "toCurrency": "EUR", "rate": "0.87998"},
            {"reportDate": "2025-06-11", "fromCurrency": "USD", "toCurrency": "EUR", "rate": "0.87050"},
        ],
    )
    stock_rows = [r for r in rd["trade_details"] if r.get("symbol") == "MU" and r.get("source") == "trades"]
    assert len(stock_rows) == 1, f"erwartet 1 MU-Aktienzeile, aktuell {len(stock_rows)}"
    assert_close(stock_rows[0]["cost"], 8400.0, label="TC9 stock cost")
    assert_close(stock_rows[0]["fifoPnlRealized"], 3236.98, label="TC9 stock pnl raw")

    print("  TC9 Same-Day-Put-Assignment mit Strike-Basis nicht doppelt korrigiert: OK")
    print(f"    Kostenbasis bleibt {stock_rows[0]['cost']:.2f} USD")


def test_prior_year_put_lot_sold_before_tax_year_does_not_touch_current_sale():
    """TC10: History-lot from prior year must not leak into an unrelated 2025 sale."""
    prior_year_trades = [
        make_sell("2024-03-01", 1, 2.00, strike="70", expiry="2024-03-15",
                  pc="P", underlying="MU", commission=-1.0),
        make_assignment("2024-03-15", 1, strike="70", expiry="2024-03-15",
                        pc="P", underlying="MU"),
        {
            "tradeID": "mu_2024_stock_sale",
            "assetCategory": "STK",
            "transactionType": "ExchTrade",
            "buySell": "SELL",
            "openCloseIndicator": "C",
            "underlyingSymbol": "MU",
            "symbol": "MU",
            "description": "MICRON TECHNOLOGY INC",
            "quantity": "-100",
            "tradePrice": "75",
            "closePrice": "75",
            "ibCommission": "0",
            "fxRateToBase": "1.0",
            "currency": "USD",
            "dateTime": "2024-04-01 10:00:00",
            "tradeDate": "2024-04-01",
            "reportDate": "2024-04-01",
            "fifoPnlRealized": "500",
            "cost": "7000",
            "proceeds": "7500",
            "isin": "US5951121038",
        },
    ]
    trades = prior_year_trades + _mu_put_assignment_trade_set(
        8400.0, 3236.98, assignment_datetime="2025-04-25 16:20:00",
        stock_book_cost=8400.0
    )
    rd = calculate_for_trades(
        trades,
        tax_year=2025,
        closed_lots=_mu_closed_lot(8400.0),
        conversion_rates=[
            {"reportDate": "2025-04-25", "fromCurrency": "USD", "toCurrency": "EUR", "rate": "0.87998"},
            {"reportDate": "2025-06-11", "fromCurrency": "USD", "toCurrency": "EUR", "rate": "0.87050"},
        ],
    )
    stock_rows = [r for r in rd["trade_details"] if r.get("symbol") == "MU" and r.get("source") == "trades"]
    assert len(stock_rows) == 1, f"erwartet 1 MU-Aktienzeile, aktuell {len(stock_rows)}"
    assert_close(stock_rows[0]["cost"], 8400.0, label="TC10 stock cost")
    assert_close(stock_rows[0]["fifoPnlRealized"], 3236.98, label="TC10 stock pnl raw")
    assert not rd["audit"].get("cross_year_put_corrections"), \
        f"unerwartete Cross-Year-Korrektur: {rd['audit'].get('cross_year_put_corrections')}"

    print("  TC10 Verkaufte Vorjahres-Andienung leakt nicht in 2025-MU-Verkauf: OK")
    print(f"    Kostenbasis bleibt {stock_rows[0]['cost']:.2f} USD")


def test_prior_year_put_lot_sold_in_tax_year_is_still_corrected():
    """TC11: Matching CLOSED_LOT open date still allows real cross-year correction."""
    trades = [
        make_sell("2024-03-01", 1, 2.00, strike="70", expiry="2024-03-15",
                  pc="P", underlying="MU", commission=-1.0),
        make_assignment("2024-03-15", 1, strike="70", expiry="2024-03-15",
                        pc="P", underlying="MU"),
        {
            "tradeID": "mu_2025_sale_prior_lot",
            "assetCategory": "STK",
            "transactionType": "ExchTrade",
            "buySell": "SELL",
            "openCloseIndicator": "C",
            "underlyingSymbol": "MU",
            "symbol": "MU",
            "description": "MICRON TECHNOLOGY INC",
            "quantity": "-100",
            "tradePrice": "80",
            "closePrice": "80",
            "ibCommission": "0",
            "fxRateToBase": "1.0",
            "currency": "USD",
            "dateTime": "2025-02-01 10:00:00",
            "tradeDate": "2025-02-01",
            "reportDate": "2025-02-01",
            "fifoPnlRealized": "1199",
            "cost": "6801",
            "proceeds": "8000",
            "isin": "US5951121038",
        },
    ]
    closed_lots = [{
        "assetCategory": "STK",
        "currency": "USD",
        "reportDate": "2025-02-01",
        "dateTime": "2025-02-01 10:00:00",
        "openDateTime": "2024-03-15 16:20:00",
        "quantity": "100",
        "cost": "6801",
        "fifoPnlRealized": "1199",
        "fxRateToBase": "1.0",
        "symbol": "MU",
        "description": "MICRON TECHNOLOGY INC",
        "isin": "US5951121038",
        "underlyingSymbol": "MU",
    }]
    rd = calculate_for_trades(trades, tax_year=2025, closed_lots=closed_lots)
    stock_rows = [r for r in rd["trade_details"] if r.get("symbol") == "MU" and r.get("source") == "trades"]
    assert len(stock_rows) == 1, f"erwartet 1 MU-Aktienzeile, aktuell {len(stock_rows)}"
    assert_close(stock_rows[0]["cost"], 7000.0, label="TC11 stock cost")
    assert_close(stock_rows[0]["fifoPnlRealized"], 1000.0, label="TC11 stock pnl raw")
    assert len(rd["audit"].get("cross_year_put_corrections", [])) == 1, \
        f"erwartet 1 Cross-Year-Korrektur, aktuell {rd['audit'].get('cross_year_put_corrections')}"

    print("  TC11 Echte Cross-Year-Andienung mit CLOSED_LOT-Match bleibt korrigiert: OK")
    print(f"    Kostenbasis {stock_rows[0]['cost']:.2f} USD")


def test_same_year_put_requires_matching_closed_lot():
    """TC12: Same-Year-Put darf keinen alten Aktienverkauf desselben Symbols korrigieren."""
    trades = [
        {
            "tradeID": "mu_old_stock_sale",
            "assetCategory": "STK",
            "transactionType": "ExchTrade",
            "buySell": "SELL",
            "openCloseIndicator": "C",
            "underlyingSymbol": "MU",
            "symbol": "MU",
            "description": "MICRON TECHNOLOGY INC",
            "quantity": "-100",
            "tradePrice": "90",
            "closePrice": "90",
            "ibCommission": "0",
            "fxRateToBase": "1.0",
            "currency": "USD",
            "dateTime": "2025-01-10 10:00:00",
            "tradeDate": "2025-01-10",
            "reportDate": "2025-01-10",
            "fifoPnlRealized": "1000",
            "cost": "8000",
            "proceeds": "9000",
            "isin": "US5951121038",
        },
    ] + _mu_put_assignment_trade_set(
        8400.0, 3236.98, assignment_datetime="2025-04-25 16:20:00",
        stock_book_cost=8400.0
    )[:3]
    closed_lots = [{
        "assetCategory": "STK",
        "currency": "USD",
        "reportDate": "2025-01-10",
        "dateTime": "2025-01-10 10:00:00",
        "openDateTime": "2024-12-01 10:00:00",
        "quantity": "100",
        "cost": "8000",
        "fifoPnlRealized": "1000",
        "fxRateToBase": "1.0",
        "symbol": "MU",
        "description": "MICRON TECHNOLOGY INC",
        "isin": "US5951121038",
        "underlyingSymbol": "MU",
    }]
    rd = calculate_for_trades(trades, tax_year=2025, closed_lots=closed_lots)
    stock_rows = [r for r in rd["trade_details"] if r.get("symbol") == "MU" and r.get("source") == "trades"]
    assert len(stock_rows) == 1, f"erwartet 1 MU-Aktienzeile, aktuell {len(stock_rows)}"
    assert_close(stock_rows[0]["cost"], 8000.0, label="TC12 stock cost")
    assert_close(stock_rows[0]["fifoPnlRealized"], 1000.0, label="TC12 stock pnl raw")
    assert not stock_rows[0].get("stillhalter_adjusted"), "alter MU-Verkauf darf nicht korrigiert werden"

    print("  TC12 Same-Year-Put ohne CLOSED_LOT-Match korrigiert keinen Altbestand: OK")
    print(f"    alter MU-Verkauf bleibt bei {stock_rows[0]['cost']:.2f} USD Kostenbasis")


def test_zufluss_fifo_current_close_consumes_prior_sell_first():
    """TC13: Ein Steuerjahr-Rueckkauf schliesst FIFO erst den Vorjahres-Short."""
    trades = [
        make_sell("2024-12-15", 1, 5.00, strike="100", expiry="2025-03-21", underlying="FIFO"),
        make_sell("2025-01-10", 1, 7.00, strike="100", expiry="2025-03-21", underlying="FIFO"),
        make_buy_close("2025-01-20", 1, 2.00, -300, strike="100", expiry="2025-03-21", underlying="FIFO"),
    ]
    rd = calculate_for_trades(trades, tax_year=2025)
    audit = rd.get("audit", {})

    assert_close(audit.get("prior_zufluss_correction_eur", 0), 499.0,
                 label="TC13 prior_zufluss_correction_eur")
    assert_close(audit.get("zufluss_premium_eur", 0), 699.0,
                 label="TC13 zufluss_premium_eur")

    print("  TC13 Zufluss-FIFO: Steuerjahr-Close konsumiert Vorjahres-Sell zuerst: OK")
    print("    Vorjahreskorrektur 499.00 EUR, offener Steuerjahr-Zufluss 699.00 EUR")


def test_zufluss_fifo_prior_close_consumes_prior_sell_before_tax_year():
    """TC14: Bereits im Vorjahr geschlossene Shorts duerfen 2025 nicht erneut korrigiert werden."""
    trades = [
        make_sell("2024-12-01", 1, 5.00, strike="100", expiry="2025-03-21", underlying="FIFO"),
        make_buy_close("2024-12-15", 1, 1.00, 399, strike="100", expiry="2025-03-21", underlying="FIFO"),
        make_sell("2025-01-10", 1, 7.00, strike="100", expiry="2025-03-21", underlying="FIFO"),
        make_buy_close("2025-01-20", 1, 2.00, 499, strike="100", expiry="2025-03-21", underlying="FIFO"),
    ]
    rd = calculate_for_trades(trades, tax_year=2025)
    audit = rd.get("audit", {})

    assert_close(audit.get("prior_zufluss_correction_eur", 0), 0.0,
                 label="TC14 prior_zufluss_correction_eur")
    assert_close(audit.get("zufluss_premium_eur", 0), 0.0,
                 label="TC14 zufluss_premium_eur")

    print("  TC14 Zufluss-FIFO: im Vorjahr geschlossener Short bleibt erledigt: OK")
    print("    keine falsche Vorjahreskorrektur in 2025")


def test_cross_year_put_topf1_consistent_across_fx_rates():
    """TC15: Topf-1-Saldo (Website) muss der Summe der Topf-1-Trade-Details (Excel) entsprechen.

    Cross-Year-Put: Option im Vorjahr verkauft (FX 0.90), Aktie im Steuerjahr
    verkauft (FX 0.80). Der Backend reduziert stocks_gain mit premium_per_share_eur
    (= Praemie zum Options-Verkaufskurs), die debug_row aber mit
    premium_per_share_raw x fx_aktienverkauf. Bei abweichenden FX-Kursen klaffen
    topf_1_aktien_netto und die Summe der Topf-1-Zeilen auseinander.
    """
    sell = make_sell("2024-03-01", 1, 2.00, strike="70", expiry="2024-03-15",
                     pc="P", underlying="MU", commission=-1.0)
    sell["fxRateToBase"] = "0.90"
    sell["currency"] = "USD"
    trades = [
        sell,
        make_assignment("2024-03-15", 1, strike="70", expiry="2024-03-15",
                        pc="P", underlying="MU"),
        {
            "tradeID": "mu_2025_sale_prior_lot",
            "assetCategory": "STK",
            "transactionType": "ExchTrade",
            "buySell": "SELL",
            "openCloseIndicator": "C",
            "underlyingSymbol": "MU",
            "symbol": "MU",
            "description": "MICRON TECHNOLOGY INC",
            "quantity": "-100",
            "tradePrice": "80",
            "closePrice": "80",
            "ibCommission": "0",
            "fxRateToBase": "0.80",
            "currency": "USD",
            "dateTime": "2025-02-01 10:00:00",
            "tradeDate": "2025-02-01",
            "reportDate": "2025-02-01",
            "fifoPnlRealized": "1199",
            "cost": "6801",
            "proceeds": "8000",
            "isin": "US5951121038",
        },
    ]
    closed_lots = [{
        "assetCategory": "STK",
        "currency": "USD",
        "reportDate": "2025-02-01",
        "dateTime": "2025-02-01 10:00:00",
        "openDateTime": "2024-03-15 16:20:00",
        "quantity": "100",
        "cost": "6801",
        "fifoPnlRealized": "1199",
        "fxRateToBase": "0.80",
        "symbol": "MU",
        "description": "MICRON TECHNOLOGY INC",
        "isin": "US5951121038",
        "underlyingSymbol": "MU",
    }]
    rd = calculate_for_trades(trades, tax_year=2025, closed_lots=closed_lots)

    assert len(rd["audit"].get("cross_year_put_corrections", [])) == 1, \
        f"erwartet 1 Cross-Year-Korrektur, aktuell {rd['audit'].get('cross_year_put_corrections')}"

    topf_1 = rd["topf_1_aktien_netto"]
    topf1_rows_sum = sum(
        r.get("pnl_eur", 0) for r in rd["trade_details"] if r.get("topf") == "Topf1"
    )

    # IBKR-Rohwert 1199 USD, Praemie 199 USD raus -> 1000 USD x 0.80 = 800.00 EUR
    assert_close(topf1_rows_sum, 800.0, label="TC15 Topf-1 Trade-Details Summe")
    assert_close(topf_1, 800.0, label="TC15 topf_1_aktien_netto")
    assert_close(topf_1, topf1_rows_sum, label="TC15 Topf-1 Website vs Excel")

    # Audit-Werte (Box + Plausibilitaetscheck) muessen den tatsaechlich
    # subtrahierten Betrag tragen (stock_fx), nicht die Praemie zum Options-Kurs.
    # Reduktion = 1199 x 0.80 - 1000 x 0.80 = 199 x 0.80 = 159.20 EUR.
    audit = rd["audit"]
    assert_close(audit["cross_year_put_total"], 159.2, label="TC15 cross_year_put_total")
    cyp = audit["cross_year_put_corrections"]
    assert_close(sum(c["correction_eur"] for c in cyp), 159.2,
                 label="TC15 Summe correction_eur")

    print("  TC15 Cross-Year-Put Topf-1 konsistent ueber FX-Kurse: OK")
    print(f"    topf_1_aktien_netto = {topf_1:.2f} EUR, Trade-Details-Summe = {topf1_rows_sum:.2f} EUR")
    print(f"    cross_year_put_total = {audit['cross_year_put_total']:.2f} EUR (tatsaechlich subtrahiert)")


def test_cross_year_put_correction_only_hits_sell_rows():
    """TC16: Cross-Year-Put-Korrektur darf nur den Aktien-VERKAUF treffen.

    Die Korrekturen werden ausschliesslich aus STK-SELL-Trades gebaut. Liegt im
    Steuerjahr zusaetzlich ein STK-BUY desselben Symbols (z.B. Short-Cover) und
    steht der vor dem SELL in debug_rows, darf er die Korrektur nicht abgreifen.
    """
    sell_opt = make_sell("2024-03-01", 1, 2.00, strike="70", expiry="2024-03-15",
                         pc="P", underlying="MU", commission=-1.0)
    sell_opt["currency"] = "USD"
    stk_buy = {
        "tradeID": "mu_2025_short_cover",
        "assetCategory": "STK",
        "transactionType": "ExchTrade",
        "buySell": "BUY",
        "openCloseIndicator": "C",
        "underlyingSymbol": "MU",
        "symbol": "MU",
        "description": "MICRON TECHNOLOGY INC",
        "quantity": "100",
        "tradePrice": "60",
        "closePrice": "60",
        "ibCommission": "0",
        "fxRateToBase": "1.0",
        "currency": "USD",
        "dateTime": "2025-01-05 10:00:00",
        "tradeDate": "2025-01-05",
        "reportDate": "2025-01-05",
        "fifoPnlRealized": "300",
        "cost": "-6300",
        "proceeds": "6000",
        "isin": "US5951121038",
    }
    stk_sell = {
        "tradeID": "mu_2025_sale_prior_lot",
        "assetCategory": "STK",
        "transactionType": "ExchTrade",
        "buySell": "SELL",
        "openCloseIndicator": "C",
        "underlyingSymbol": "MU",
        "symbol": "MU",
        "description": "MICRON TECHNOLOGY INC",
        "quantity": "-100",
        "tradePrice": "80",
        "closePrice": "80",
        "ibCommission": "0",
        "fxRateToBase": "1.0",
        "currency": "USD",
        "dateTime": "2025-02-01 10:00:00",
        "tradeDate": "2025-02-01",
        "reportDate": "2025-02-01",
        "fifoPnlRealized": "1199",
        "cost": "6801",
        "proceeds": "8000",
        "isin": "US5951121038",
    }
    # STK-BUY bewusst VOR dem STK-SELL in der trades-Liste -> auch in debug_rows zuerst.
    trades = [
        sell_opt,
        make_assignment("2024-03-15", 1, strike="70", expiry="2024-03-15",
                        pc="P", underlying="MU"),
        stk_buy,
        stk_sell,
    ]
    closed_lots = [{
        "assetCategory": "STK",
        "currency": "USD",
        "reportDate": "2025-02-01",
        "dateTime": "2025-02-01 10:00:00",
        "openDateTime": "2024-03-15 16:20:00",
        "quantity": "100",
        "cost": "6801",
        "fifoPnlRealized": "1199",
        "fxRateToBase": "1.0",
        "symbol": "MU",
        "description": "MICRON TECHNOLOGY INC",
        "isin": "US5951121038",
        "underlyingSymbol": "MU",
    }]
    rd = calculate_for_trades(trades, tax_year=2025, closed_lots=closed_lots)

    stk_rows = [r for r in rd["trade_details"]
                if r.get("symbol") == "MU" and r.get("source") == "trades"
                and r.get("assetCategory") == "STK"]
    buy_rows = [r for r in stk_rows if r.get("buySell") == "BUY"]
    sell_rows = [r for r in stk_rows if r.get("buySell") == "SELL"]
    assert len(buy_rows) == 1 and len(sell_rows) == 1, "erwarte je 1 BUY- und SELL-Row"
    assert not buy_rows[0].get("stillhalter_adjusted"), \
        "STK-BUY (Short-Cover) darf NICHT von der Cross-Year-Put-Korrektur getroffen werden"
    assert sell_rows[0].get("stillhalter_adjusted"), \
        "STK-SELL muss die Cross-Year-Put-Korrektur erhalten"
    assert_close(buy_rows[0]["cost"], -6300.0, label="TC16 BUY cost unveraendert")
    assert_close(sell_rows[0]["cost"], 7000.0, label="TC16 SELL cost = strike x qty")

    print("  TC16 Cross-Year-Put-Korrektur trifft nur SELL-Rows: OK")
    print(f"    BUY-cost {buy_rows[0]['cost']:.2f} (unveraendert), SELL-cost {sell_rows[0]['cost']:.2f}")


def test_cross_year_put_correction_handles_spaced_underlying_symbol():
    """TC17: Cross-Year-Put-Korrektur fuer Klassen-Aktien mit Leerzeichen im Symbol.

    IBKR fuehrt Klassen-Aktien als 'BRK B'. put_assignment_lots, der trades-Loop
    und der closed_lots-Index keyen mit dem vollen underlyingSymbol. Die
    debug_rows-Schleife und der closed_lots-Index duerfen das underlyingSymbol
    NICHT auf 'BRK' splitten, sonst bleibt die Korrektur stumm bei 0 und die
    Pools / cross_year_put_total uncorrected.
    """
    sell_opt = make_sell("2024-03-01", 1, 2.00, strike="70", expiry="2024-03-15",
                         pc="P", underlying="BRK B", commission=-1.0)
    sell_opt["currency"] = "USD"
    stk_sell = {
        "tradeID": "brkb_2025_sale_prior_lot",
        "assetCategory": "STK",
        "transactionType": "ExchTrade",
        "buySell": "SELL",
        "openCloseIndicator": "C",
        "underlyingSymbol": "BRK B",
        "symbol": "BRK B",
        "description": "BERKSHIRE HATHAWAY INC-CL B",
        "quantity": "-100",
        "tradePrice": "80",
        "closePrice": "80",
        "ibCommission": "0",
        "fxRateToBase": "1.0",
        "currency": "USD",
        "dateTime": "2025-02-01 10:00:00",
        "tradeDate": "2025-02-01",
        "reportDate": "2025-02-01",
        "fifoPnlRealized": "1199",
        "cost": "6801",
        "proceeds": "8000",
        "isin": "US0846707026",
    }
    trades = [
        sell_opt,
        make_assignment("2024-03-15", 1, strike="70", expiry="2024-03-15",
                        pc="P", underlying="BRK B"),
        stk_sell,
    ]
    closed_lots = [{
        "assetCategory": "STK",
        "currency": "USD",
        "reportDate": "2025-02-01",
        "dateTime": "2025-02-01 10:00:00",
        "openDateTime": "2024-03-15 16:20:00",
        "quantity": "100",
        "cost": "6801",
        "fifoPnlRealized": "1199",
        "fxRateToBase": "1.0",
        "symbol": "BRK B",
        "description": "BERKSHIRE HATHAWAY INC-CL B",
        "isin": "US0846707026",
        "underlyingSymbol": "BRK B",
    }]
    rd = calculate_for_trades(trades, tax_year=2025, closed_lots=closed_lots)

    stock_rows = [r for r in rd["trade_details"]
                  if r.get("symbol") == "BRK B" and r.get("source") == "trades"]
    assert len(stock_rows) == 1, f"erwartet 1 BRK B-Aktienzeile, aktuell {len(stock_rows)}"
    assert stock_rows[0].get("stillhalter_adjusted"), \
        "BRK B-Verkauf muss die Cross-Year-Put-Korrektur erhalten"
    assert_close(stock_rows[0]["cost"], 7000.0, label="TC17 stock cost = strike x qty")
    assert_close(stock_rows[0]["fifoPnlRealized"], 1000.0, label="TC17 stock pnl raw")

    audit = rd["audit"]
    assert len(audit.get("cross_year_put_corrections", [])) == 1, \
        f"erwartet 1 Cross-Year-Korrektur, aktuell {audit.get('cross_year_put_corrections')}"
    assert_close(audit["cross_year_put_total"], 199.0, label="TC17 cross_year_put_total")

    print("  TC17 Cross-Year-Put fuer Leerzeichen-Symbol (BRK B): OK")
    print(f"    cost {stock_rows[0]['cost']:.2f}, cross_year_put_total {audit['cross_year_put_total']:.2f}")


def test_cross_year_worthless_expiry_gets_prior_zufluss_correction():
    """TC18: Wertloser Verfall eines Vorjahres-Shorts darf nicht doppelt versteuert werden.

    SELL 2024 (Zufluss 2024 versteuert), wertloser Verfall 2025 (BookTrade BUY,
    fifoPnlRealized = Praemie). Ohne Korrektur wuerde die Praemie 2025 erneut
    voll in options_gain laufen (Audit-Finding F2/H1, Beleg TLT 94.5P).
    """
    trades = [
        make_sell("2024-12-15", 1, 5.00, strike="100", expiry="2025-01-17",
                  underlying="EXPF"),
        make_expiry("2025-01-17", 1, 499.0, strike="100", expiry="2025-01-17",
                    underlying="EXPF"),
    ]
    rd = calculate_for_trades(trades, tax_year=2025)
    audit = rd.get("audit", {})

    assert_close(audit.get("prior_zufluss_correction_eur", 0), 499.0,
                 label="TC18 prior_zufluss_correction_eur")
    assert_close(audit.get("zufluss_premium_eur", 0), 0.0,
                 label="TC18 zufluss_premium_eur")
    # Netto-Steuerwirkung 2025: Verfalls-PnL (+499) minus Korrektur (-499) = 0
    assert_close(rd.get("options_gain_eur", 0), 0.0,
                 label="TC18 options_gain_eur netto")

    print("  TC18 Cross-Year-Verfall erzeugt Vorjahreskorrektur: OK")
    print("    Verfalls-PnL 499.00 EUR durch prior_zufluss -499.00 EUR neutralisiert")


def test_same_year_worthless_expiry_no_correction():
    """TC19: Same-Year-Verfall (SELL und Verfall im Steuerjahr) braucht KEINE Korrektur.

    Die Praemie wird genau einmal als Verfalls-PnL versteuert; weder Zufluss-
    Detail (Lot ist konsumiert) noch prior_zufluss (Sell-Jahr == Steuerjahr).
    """
    trades = [
        make_sell("2025-06-16", 1, 5.00, strike="100", expiry="2025-07-18",
                  underlying="EXPS"),
        make_expiry("2025-07-18", 1, 499.0, strike="100", expiry="2025-07-18",
                    underlying="EXPS"),
    ]
    rd = calculate_for_trades(trades, tax_year=2025)
    audit = rd.get("audit", {})

    assert_close(audit.get("prior_zufluss_correction_eur", 0), 0.0,
                 label="TC19 prior_zufluss_correction_eur")
    assert_close(audit.get("zufluss_premium_eur", 0), 0.0,
                 label="TC19 zufluss_premium_eur")
    assert_close(rd.get("options_gain_eur", 0), 499.0,
                 label="TC19 options_gain_eur")

    print("  TC19 Same-Year-Verfall ohne Doppel-Korrektur: OK")
    print("    Praemie genau einmal als Verfalls-PnL 499.00 EUR versteuert")


def _stock_sell_row(trade_id, symbol, date, qty, pnl, cost, proceeds,
                    transaction_type="ExchTrade"):
    return {
        "tradeID": trade_id,
        "assetCategory": "STK",
        "transactionType": transaction_type,
        "buySell": "SELL",
        "openCloseIndicator": "C",
        "underlyingSymbol": symbol,
        "symbol": symbol,
        "description": f"{symbol} CORP",
        "quantity": str(-qty),
        "tradePrice": str(proceeds / qty),
        "closePrice": str(proceeds / qty),
        "ibCommission": "0",
        "fxRateToBase": "1.0",
        "currency": "EUR",
        "dateTime": f"{date} 16:20:00",
        "tradeDate": date,
        "reportDate": date,
        "fifoPnlRealized": str(pnl),
        "cost": str(cost),
        "proceeds": str(proceeds),
    }


def test_call_assignment_correction_only_hits_assignment_day_sale():
    """TC20: Call-Korrektur darf fruehere Verkaeufe desselben Underlyings nicht treffen.

    Audit-Finding F1a: Call-Korrekturen ohne Datums-Gate (close_date='') matchten
    jede STK-Row des Underlyings in Dateireihenfolge (SVOL-Fall: Mai-Andienung
    korrigierte Februar/Mai-Verkaeufe und raeumte die Rows fuer spaetere Puts leer).
    """
    trades = [
        # Unabhaengiger Verkauf im Maerz, steht in Dateireihenfolge VOR der Andienung
        _stock_sell_row("whl_march_sale", "WHL", "2025-03-10", 100, 200.0, 5000.0, 5200.0),
        make_sell("2025-06-01", 1, 3.00, strike="105", expiry="2025-07-18",
                  pc="C", underlying="WHL"),
        make_assignment("2025-07-18", 1, strike="105", expiry="2025-07-18",
                        pc="C", underlying="WHL"),
        # Andienungs-Verkauf: IBKR-PnL enthaelt die Call-Praemie (echt 500 + 299)
        _stock_sell_row("whl_assignment_sale", "WHL", "2025-07-18", 100, 799.0,
                        9701.0, 10500.0, transaction_type="BookTrade"),
    ]
    rd = calculate_for_trades(trades, tax_year=2025)

    rows = {r["reportDate"]: r for r in rd["trade_details"]
            if r.get("symbol") == "WHL" and r.get("source") == "trades"}
    march = rows["2025-03-10"]
    july = rows["2025-07-18"]

    assert not march.get("stillhalter_adjusted"), \
        "TC20: Maerz-Verkauf darf die Call-Korrektur NICHT erhalten"
    assert_close(march["fifoPnlRealized"], 200.0, label="TC20 maerz pnl unveraendert")
    assert july.get("stillhalter_adjusted"), \
        "TC20: Andienungs-Verkauf muss die Call-Korrektur erhalten"
    assert_close(july["fifoPnlRealized"], 500.0, label="TC20 juli pnl korrigiert")
    assert_close(rd.get("stocks_gain_eur", 0), 700.0, label="TC20 stocks_gain")
    assert rd["audit"].get("stillhalter_corrections_dropped", []) == [], \
        "TC20: keine verworfenen Korrekturen erwartet"

    print("  TC20 Call-Korrektur nur auf Andienungs-Tag-Verkauf: OK")
    print("    Maerz-Row 200.00 unveraendert, Juli-Row 799.00 -> 500.00")


def test_put_and_call_premium_stack_on_same_stock_row():
    """TC21: Dieselben Shares tragen legitim Put- UND Call-Praemie (IWM-Fall, F1b).

    Put-Andienung kauft die Aktie (Praemie in Kostenbasis eingebettet), Call-
    Andienung verkauft sie (Praemie im Erloes). Beide Korrekturen muessen auf
    dieselbe Verkaufszeile; das alte gemeinsame Quantity-Cap liess nur eine zu.
    """
    trades = [
        make_sell("2025-06-02", 1, 2.00, strike="100", expiry="2025-06-20",
                  pc="P", underlying="STKD"),
        make_assignment("2025-06-20", 1, strike="100", expiry="2025-06-20",
                        pc="P", underlying="STKD"),
        {
            "tradeID": "stkd_stock_assignment_buy",
            "assetCategory": "STK", "transactionType": "BookTrade", "buySell": "BUY",
            "openCloseIndicator": "O", "underlyingSymbol": "STKD", "symbol": "STKD",
            "description": "STKD CORP", "quantity": "100",
            "tradePrice": "100", "closePrice": "98", "ibCommission": "0",
            "fxRateToBase": "1.0", "currency": "EUR",
            "dateTime": "2025-06-20 16:20:00", "tradeDate": "2025-06-20",
            "reportDate": "2025-06-20", "fifoPnlRealized": "0",
            "cost": "9801", "proceeds": "-9801",
        },
        make_sell("2025-07-01", 1, 3.00, strike="105", expiry="2025-07-18",
                  pc="C", underlying="STKD"),
        make_assignment("2025-07-18", 1, strike="105", expiry="2025-07-18",
                        pc="C", underlying="STKD"),
        # IBKR-PnL = 10500 - 9801 (reduzierte Basis) + 299 (Call-Praemie) = 998
        _stock_sell_row("stkd_assignment_sale", "STKD", "2025-07-18", 100, 998.0,
                        9801.0, 10500.0, transaction_type="BookTrade"),
    ]
    closed_lots = [{
        "assetCategory": "STK", "currency": "EUR",
        "reportDate": "2025-07-18", "dateTime": "2025-07-18 16:20:00",
        "openDateTime": "2025-06-20 16:20:00",
        "quantity": "100", "cost": "9801", "fifoPnlRealized": "998",
        "fxRateToBase": "1.0", "symbol": "STKD", "description": "STKD CORP",
        "underlyingSymbol": "STKD",
    }]
    rd = calculate_for_trades(trades, tax_year=2025, closed_lots=closed_lots)

    rows = [r for r in rd["trade_details"]
            if r.get("symbol") == "STKD" and r.get("source") == "trades"
            and r.get("buySell") == "SELL"]
    assert len(rows) == 1
    row = rows[0]
    assert row.get("stillhalter_adjusted"), "TC21: Verkaufszeile muss korrigiert sein"
    # 998 - 199 (Put-Praemie netto) - 299 (Call-Praemie netto) = 500 echter Aktien-PnL
    assert_close(row["fifoPnlRealized"], 500.0, label="TC21 pnl beide Praemien raus")
    assert_close(rd.get("stocks_gain_eur", 0), 500.0, label="TC21 stocks_gain")
    assert_close(rd.get("options_gain_eur", 0), 498.0, label="TC21 options_gain")
    assert rd["audit"].get("stillhalter_corrections_dropped", []) == [], \
        "TC21: keine verworfenen Korrekturen erwartet"

    print("  TC21 Put+Call-Praemien-Stack auf derselben Verkaufszeile: OK")
    print("    998.00 -> 500.00 (Put -199.00, Call -299.00)")


def test_unapplied_correction_is_tracked_and_warned():
    """TC22: Nicht zuordenbare Korrekturen duerfen nicht still verfallen (F1c).

    Gibt es am Andienungstag keine passende Verkaufszeile, bleibt die Praemie im
    Aktien-PnL eingebettet (Doppelversteuerung). Das muss im Audit-Feld
    stillhalter_corrections_dropped sichtbar werden, statt still zu verschwinden.
    """
    trades = [
        _stock_sell_row("orph_march_sale", "ORPH", "2025-03-10", 100, 200.0,
                        5000.0, 5200.0),
        make_sell("2025-06-01", 1, 3.00, strike="105", expiry="2025-07-18",
                  pc="C", underlying="ORPH"),
        make_assignment("2025-07-18", 1, strike="105", expiry="2025-07-18",
                        pc="C", underlying="ORPH"),
        # KEINE Stock-Verkaufszeile am Andienungstag (Datenanomalie)
    ]
    rd = calculate_for_trades(trades, tax_year=2025)

    rows = [r for r in rd["trade_details"]
            if r.get("symbol") == "ORPH" and r.get("source") == "trades"]
    assert len(rows) == 1
    assert not rows[0].get("stillhalter_adjusted"), \
        "TC22: Maerz-Verkauf darf nicht korrigiert werden"
    assert_close(rows[0]["fifoPnlRealized"], 200.0, label="TC22 maerz pnl")

    dropped = rd["audit"].get("stillhalter_corrections_dropped", [])
    assert len(dropped) == 1, f"TC22: erwartet 1 dropped-Eintrag, aktuell {dropped}"
    assert dropped[0]["underlying"] == "ORPH"
    assert_close(dropped[0]["leftover_raw"], 299.0, label="TC22 leftover_raw")
    assert dropped[0]["leftover_shares"] == 100

    print("  TC22 Verworfene Korrektur wird getrackt und gewarnt: OK")
    print(f"    ORPH: 299.00 auf 100 Stueck ohne passende Verkaufszeile")


def test_call_assignment_short_cover_correction_on_buy_row():
    """TC23: Call-Andienung ohne Bestand: Praemie sitzt im spaeteren Short-Cover.

    Audit-Realfall SPY/BITO/MPW: Die Andienung eroeffnet einen Aktien-Short
    (SELL, PnL=0, oc=O); IBKR realisiert den PnL inkl. Praemie erst beim
    Rueckkauf. Die Korrektur muss auf die BUY-Row des Cover-Tags (per
    Short-Lot-Match openDateTime == Andienungstag), nicht auf den Andienungstag.
    """
    trades = [
        make_sell("2025-06-01", 1, 3.00, strike="105", expiry="2025-07-18",
                  pc="C", underlying="SHRT"),
        make_assignment("2025-07-18", 1, strike="105", expiry="2025-07-18",
                        pc="C", underlying="SHRT"),
        # Andienung eroeffnet Short: SELL mit PnL=0
        {
            "tradeID": "shrt_assignment_short_open",
            "assetCategory": "STK", "transactionType": "BookTrade", "buySell": "SELL",
            "openCloseIndicator": "O", "underlyingSymbol": "SHRT", "symbol": "SHRT",
            "description": "SHRT CORP", "quantity": "-100",
            "tradePrice": "105", "closePrice": "105", "ibCommission": "0",
            "fxRateToBase": "1.0", "currency": "EUR",
            "dateTime": "2025-07-18 16:20:00", "tradeDate": "2025-07-18",
            "reportDate": "2025-07-18", "fifoPnlRealized": "0",
            "cost": "-10799", "proceeds": "10500",
        },
        # Cover: BUY realisiert IBKR-PnL = 200 echt + 299 Praemie = 499
        {
            "tradeID": "shrt_cover_buy",
            "assetCategory": "STK", "transactionType": "ExchTrade", "buySell": "BUY",
            "openCloseIndicator": "C", "underlyingSymbol": "SHRT", "symbol": "SHRT",
            "description": "SHRT CORP", "quantity": "100",
            "tradePrice": "103", "closePrice": "103", "ibCommission": "0",
            "fxRateToBase": "1.0", "currency": "EUR",
            "dateTime": "2025-08-05 10:00:00", "tradeDate": "2025-08-05",
            "reportDate": "2025-08-05", "fifoPnlRealized": "499",
            "cost": "-10799", "proceeds": "-10300",
        },
    ]
    closed_lots = [{
        "assetCategory": "STK", "currency": "EUR",
        "reportDate": "2025-08-05", "dateTime": "2025-08-05 10:00:00",
        "openDateTime": "2025-07-18 16:20:00",
        "quantity": "-100", "buySell": "BUY",
        "cost": "-10799", "fifoPnlRealized": "499",
        "fxRateToBase": "1.0", "symbol": "SHRT", "description": "SHRT CORP",
        "underlyingSymbol": "SHRT",
    }]
    rd = calculate_for_trades(trades, tax_year=2025, closed_lots=closed_lots)

    stk_rows = [r for r in rd["trade_details"]
                if r.get("symbol") == "SHRT" and r.get("source") == "trades"]
    # Die Short-Eroeffnung (PnL=0) erzeugt keine trade_details-Row; es darf
    # ausschliesslich der Cover-BUY korrigiert worden sein.
    adjusted = [r for r in stk_rows if r.get("stillhalter_adjusted")]
    assert len(adjusted) == 1, f"TC23: genau 1 korrigierte Row erwartet, aktuell {len(adjusted)}"
    cover = adjusted[0]
    assert (cover["reportDate"], cover["buySell"]) == ("2025-08-05", "BUY"), \
        f"TC23: Korrektur muss auf dem Cover-BUY sitzen, aktuell {cover['reportDate']}/{cover['buySell']}"
    assert_close(cover["fifoPnlRealized"], 200.0, label="TC23 cover pnl korrigiert")
    assert_close(rd.get("stocks_gain_eur", 0), 200.0, label="TC23 stocks_gain")
    assert_close(rd.get("options_gain_eur", 0), 299.0, label="TC23 options_gain")
    assert rd["audit"].get("stillhalter_corrections_dropped", []) == [], \
        "TC23: keine verworfenen Korrekturen erwartet"

    print("  TC23 Call-Short-Cover-Korrektur auf BUY-Row des Cover-Tags: OK")
    print("    Cover 499.00 -> 200.00, Short-Eroeffnung unveraendert")


def test_two_same_day_call_assignments_use_separate_cover_lots():
    """TC24: Zwei Same-Day-Call-Andienungen duerfen nicht denselben Cover-Lot claimen.

    Codex-Review-Finding (P2): _call_assignment_short_lot_matches scannte pro
    Detail von vorne; bei zwei Andienungen desselben Underlyings am selben Tag
    matchen beide den ersten Cover-Lot, die zweite Korrektur verfaellt als
    dropped und die zweite Cover-Row behaelt die eingebettete Praemie.
    """
    a1 = make_assignment("2025-07-18", 1, strike="105", expiry="2025-07-18",
                         pc="C", underlying="DUP")
    a2 = make_assignment("2025-07-18", 1, strike="110", expiry="2025-07-18",
                         pc="C", underlying="DUP")
    a1["tradeID"] = "assign_dup_c105"
    a2["tradeID"] = "assign_dup_c110"

    trades = [
        make_sell("2025-06-01", 1, 3.00, strike="105", expiry="2025-07-18",
                  pc="C", underlying="DUP"),
        make_sell("2025-06-02", 1, 2.00, strike="110", expiry="2025-07-18",
                  pc="C", underlying="DUP"),
        a1, a2,
        _call_stk_row("DUP", "dup_short_open_1", "SELL", "O", -100, "2025-07-18", 0,
                      tt="BookTrade"),
        _call_stk_row("DUP", "dup_short_open_2", "SELL", "O", -100, "2025-07-18", 0,
                      tt="BookTrade"),
        _call_stk_row("DUP", "dup_cover_1", "BUY", "C", 100, "2025-08-05", 499),  # 200 echt + 299
        _call_stk_row("DUP", "dup_cover_2", "BUY", "C", 100, "2025-09-10", 299),  # 100 echt + 199
    ]
    closed_lots = [
        {"assetCategory": "STK", "currency": "EUR", "reportDate": "2025-08-05",
         "dateTime": "2025-08-05 10:00:00", "openDateTime": "2025-07-18 16:20:00",
         "quantity": "-100", "buySell": "BUY", "cost": "-10500",
         "fifoPnlRealized": "499", "fxRateToBase": "1.0",
         "symbol": "DUP", "underlyingSymbol": "DUP"},
        {"assetCategory": "STK", "currency": "EUR", "reportDate": "2025-09-10",
         "dateTime": "2025-09-10 10:00:00", "openDateTime": "2025-07-18 16:20:00",
         "quantity": "-100", "buySell": "BUY", "cost": "-11000",
         "fifoPnlRealized": "299", "fxRateToBase": "1.0",
         "symbol": "DUP", "underlyingSymbol": "DUP"},
    ]
    rd = calculate_for_trades(trades, tax_year=2025, closed_lots=closed_lots)

    rows = {r["reportDate"]: r for r in rd["trade_details"]
            if r.get("symbol") == "DUP" and r.get("source") == "trades"
            and r.get("buySell") == "BUY"}
    assert rows["2025-08-05"].get("stillhalter_adjusted"), "TC24: Cover 1 muss korrigiert sein"
    assert rows["2025-09-10"].get("stillhalter_adjusted"), "TC24: Cover 2 muss korrigiert sein"
    assert_close(rows["2025-08-05"]["fifoPnlRealized"], 200.0, label="TC24 cover1 pnl")
    assert_close(rows["2025-09-10"]["fifoPnlRealized"], 100.0, label="TC24 cover2 pnl")
    assert_close(rd.get("stocks_gain_eur", 0), 300.0, label="TC24 stocks_gain")
    assert rd["audit"].get("stillhalter_corrections_dropped", []) == [], \
        "TC24: keine verworfenen Korrekturen erwartet"

    print("  TC24 Zwei Same-Day-Call-Andienungen nutzen separate Cover-Lots: OK")
    print("    Cover1 499 -> 200, Cover2 299 -> 100, dropped leer")


def test_two_same_day_put_assignments_use_separate_lots():
    """TC25: Zwei Same-Day-Put-Teilandienungen duerfen nicht denselben Lot claimen.

    Audit-Finding F3 (Put-Variante des Codex-Findings): _put_assignment_
    closed_lot_matches konsumierte pro Detail ab Listenanfang neu; beide
    Details claimten denselben Lot-Slice, die zweite Korrektur verfiel und der
    spaetere Verkauf behielt die eingebettete Praemie.
    """
    s1 = make_sell("2025-06-02", 1, 2.00, strike="100", expiry="2025-06-20",
                   pc="P", underlying="PRT")
    s2 = make_sell("2025-06-03", 1, 2.00, strike="100", expiry="2025-06-20",
                   pc="P", underlying="PRT")
    a1 = make_assignment("2025-06-20", 1, strike="100", expiry="2025-06-20",
                         pc="P", underlying="PRT")
    a2 = make_assignment("2025-06-20", 1, strike="100", expiry="2025-06-20",
                         pc="P", underlying="PRT")
    a1["tradeID"] = "assign_prt_1"
    a2["tradeID"] = "assign_prt_2"

    trades = [
        s1, s2, a1, a2,
        _call_stk_row("PRT", "prt_buy_1", "BUY", "O", 100, "2025-06-20", 0,
                      tt="BookTrade", cost=9801),
        _call_stk_row("PRT", "prt_buy_2", "BUY", "O", 100, "2025-06-20", 0,
                      tt="BookTrade", cost=9801),
        # IBKR-PnL enthaelt je die eingebettete Praemie (199):
        _call_stk_row("PRT", "prt_sale_1", "SELL", "C", -100, "2025-07-10", 199.0,
                      cost=9801),   # echt 0
        _call_stk_row("PRT", "prt_sale_2", "SELL", "C", -100, "2025-08-15", 399.0,
                      cost=9801),   # echt 200
    ]
    closed_lots = [
        {"assetCategory": "STK", "currency": "EUR", "reportDate": "2025-07-10",
         "dateTime": "2025-07-10 10:00:00", "openDateTime": "2025-06-20 16:20:00",
         "quantity": "100", "buySell": "SELL", "cost": "9801",
         "fifoPnlRealized": "199", "fxRateToBase": "1.0",
         "symbol": "PRT", "underlyingSymbol": "PRT"},
        {"assetCategory": "STK", "currency": "EUR", "reportDate": "2025-08-15",
         "dateTime": "2025-08-15 10:00:00", "openDateTime": "2025-06-20 16:20:00",
         "quantity": "100", "buySell": "SELL", "cost": "9801",
         "fifoPnlRealized": "399", "fxRateToBase": "1.0",
         "symbol": "PRT", "underlyingSymbol": "PRT"},
    ]
    rd = calculate_for_trades(trades, tax_year=2025, closed_lots=closed_lots)

    rows = {r["reportDate"]: r for r in rd["trade_details"]
            if r.get("symbol") == "PRT" and r.get("source") == "trades"
            and r.get("buySell") == "SELL"}
    assert rows["2025-07-10"].get("stillhalter_adjusted"), "TC25: Verkauf 1 muss korrigiert sein"
    assert rows["2025-08-15"].get("stillhalter_adjusted"), "TC25: Verkauf 2 muss korrigiert sein"
    assert_close(rows["2025-07-10"]["fifoPnlRealized"], 0.0, label="TC25 sale1 pnl")
    assert_close(rows["2025-08-15"]["fifoPnlRealized"], 200.0, label="TC25 sale2 pnl")
    assert_close(rd.get("stocks_gain_eur", 0), 200.0, label="TC25 stocks_gain")
    assert rd["audit"].get("stillhalter_corrections_dropped", []) == [], \
        "TC25: keine verworfenen Korrekturen erwartet"

    print("  TC25 Zwei Same-Day-Put-Andienungen nutzen separate Lots: OK")
    print("    Sale1 199 -> 0, Sale2 399 -> 200, dropped leer")


def test_call_short_cover_without_closed_lots_falls_back_to_trades():
    """TC26: Short-Cover-Call MUSS auch ohne closed_lots.csv korrigiert werden.

    Codex-Review-Finding 2 (P2, Regression): Ohne CLOSED_LOT-Daten ist der
    Lot-Match leer und der SELL-Fallback greift ins Leere (Short-Eroeffnung hat
    PnL=0 und erzeugt keine debug_row) — die Praemie blieb doppelt versteuert.
    Fallback-Stufe 3: Cover-Kandidaten direkt aus trades.csv (BUY mit PnL!=0
    nach dem Andienungstag, chronologisch).
    """
    trades = [
        make_sell("2025-06-01", 1, 3.00, strike="105", expiry="2025-07-18",
                  pc="C", underlying="NLOT"),
        make_assignment("2025-07-18", 1, strike="105", expiry="2025-07-18",
                        pc="C", underlying="NLOT"),
        {
            "tradeID": "nlot_short_open",
            "assetCategory": "STK", "transactionType": "BookTrade", "buySell": "SELL",
            "openCloseIndicator": "O", "underlyingSymbol": "NLOT", "symbol": "NLOT",
            "description": "NLOT CORP", "quantity": "-100",
            "tradePrice": "105", "closePrice": "105", "ibCommission": "0",
            "fxRateToBase": "1.0", "currency": "EUR",
            "dateTime": "2025-07-18 16:20:00", "tradeDate": "2025-07-18",
            "reportDate": "2025-07-18", "fifoPnlRealized": "0",
            "cost": "-10799", "proceeds": "10500",
        },
        {
            "tradeID": "nlot_cover_buy",
            "assetCategory": "STK", "transactionType": "ExchTrade", "buySell": "BUY",
            "openCloseIndicator": "C", "underlyingSymbol": "NLOT", "symbol": "NLOT",
            "description": "NLOT CORP", "quantity": "100",
            "tradePrice": "103", "closePrice": "103", "ibCommission": "0",
            "fxRateToBase": "1.0", "currency": "EUR",
            "dateTime": "2025-08-05 10:00:00", "tradeDate": "2025-08-05",
            "reportDate": "2025-08-05", "fifoPnlRealized": "499",
            "cost": "-10799", "proceeds": "-10300",
        },
    ]
    # BEWUSST keine closed_lots!
    rd = calculate_for_trades(trades, tax_year=2025)

    adjusted = [r for r in rd["trade_details"]
                if r.get("symbol") == "NLOT" and r.get("source") == "trades"
                and r.get("stillhalter_adjusted")]
    assert len(adjusted) == 1, f"TC26: genau 1 korrigierte Row erwartet, aktuell {len(adjusted)}"
    assert (adjusted[0]["reportDate"], adjusted[0]["buySell"]) == ("2025-08-05", "BUY"), \
        f"TC26: Korrektur muss auf dem Cover-BUY sitzen, aktuell {adjusted[0]['reportDate']}/{adjusted[0]['buySell']}"
    assert_close(adjusted[0]["fifoPnlRealized"], 200.0, label="TC26 cover pnl korrigiert")
    assert_close(rd.get("stocks_gain_eur", 0), 200.0, label="TC26 stocks_gain")
    assert_close(rd.get("options_gain_eur", 0), 299.0, label="TC26 options_gain")
    assert rd["audit"].get("stillhalter_corrections_dropped", []) == [], \
        "TC26: keine verworfenen Korrekturen erwartet"

    print("  TC26 Short-Cover ohne CLOSED_LOT-Daten via trades-Fallback: OK")
    print("    Cover 499 -> 200 ohne closed_lots.csv, dropped leer")


def _call_stk_row(symbol, tid, bs, oc, qty, date, pnl, tt="ExchTrade", cost="0"):
    return {"tradeID": tid, "assetCategory": "STK", "transactionType": tt,
            "buySell": bs, "openCloseIndicator": oc, "underlyingSymbol": symbol,
            "symbol": symbol, "description": f"{symbol} CORP", "quantity": str(qty),
            "tradePrice": "100", "closePrice": "100", "ibCommission": "0",
            "fxRateToBase": "1.0", "currency": "EUR",
            "dateTime": f"{date} 16:20:00", "tradeDate": date, "reportDate": date,
            "fifoPnlRealized": str(pnl), "cost": str(cost), "proceeds": "0"}


def _call_cover_lot(symbol, open_date, close_date, qty, pnl):
    return {"assetCategory": "STK", "currency": "EUR", "reportDate": close_date,
            "dateTime": f"{close_date} 10:00:00",
            "openDateTime": f"{open_date} 16:20:00",
            "quantity": str(-qty), "buySell": "BUY", "cost": "-10500",
            "fifoPnlRealized": str(pnl), "fxRateToBase": "1.0",
            "symbol": symbol, "underlyingSymbol": symbol}


def test_call_cover_with_partial_closed_lots():
    """TC27: Unvollstaendige closed_lots duerfen den trades-Fallback nicht abschalten.

    Codex-Review-Finding 3 (P2): Sobald EIN Lot matchte, war der trades-Fallback
    fuer den uncovered-Rest deaktiviert — der zweite Cover blieb unkorrigiert.
    Andienung 2 Kontrakte (200 Shares short), zwei Covers, nur Cover 1 in
    closed_lots.csv.
    """
    trades = [
        make_sell("2025-06-01", 2, 3.00, strike="105", expiry="2025-07-18",
                  pc="C", underlying="PART"),
        make_assignment("2025-07-18", 2, strike="105", expiry="2025-07-18",
                        pc="C", underlying="PART"),
        _call_stk_row("PART", "part_short_open", "SELL", "O", -200, "2025-07-18", 0,
                      tt="BookTrade"),
        # Prämie netto 599 -> 2.995/Share; Cover 1: 200 echt + 299.5
        _call_stk_row("PART", "part_cover_1", "BUY", "C", 100, "2025-08-05", 499.5),
        # Cover 2: 100 echt + 299.5 — Lot fehlt in closed_lots!
        _call_stk_row("PART", "part_cover_2", "BUY", "C", 100, "2025-09-10", 399.5),
    ]
    closed_lots = [_call_cover_lot("PART", "2025-07-18", "2025-08-05", 100, 499.5)]
    rd = calculate_for_trades(trades, tax_year=2025, closed_lots=closed_lots)

    rows = {r["reportDate"]: r for r in rd["trade_details"]
            if r.get("symbol") == "PART" and r.get("source") == "trades"
            and r.get("buySell") == "BUY"}
    assert rows["2025-08-05"].get("stillhalter_adjusted"), "TC27: Cover 1 (Lot) muss korrigiert sein"
    assert rows["2025-09-10"].get("stillhalter_adjusted"), \
        "TC27: Cover 2 (ohne Lot) muss via trades-Fallback korrigiert sein"
    assert_close(rows["2025-08-05"]["fifoPnlRealized"], 200.0, label="TC27 cover1 pnl")
    assert_close(rows["2025-09-10"]["fifoPnlRealized"], 100.0, label="TC27 cover2 pnl")
    assert_close(rd.get("stocks_gain_eur", 0), 300.0, label="TC27 stocks_gain")
    assert rd["audit"].get("stillhalter_corrections_dropped", []) == [], \
        "TC27: keine verworfenen Korrekturen erwartet"

    print("  TC27 Partielle closed_lots: Rest via trades-Fallback korrigiert: OK")
    print("    Cover1 (Lot) 499.5 -> 200, Cover2 (trades) 399.5 -> 100")


def test_call_assignment_mixed_long_and_short_without_lots():
    """TC28: Gemischte Call-Andienung (Long-Close + Short-Open) ohne closed_lots.

    Codex-Review-Finding 3, zweiter Trigger: has_assignment_day_sale (binaer)
    schaltete den trades-Fallback ab, sobald IRGENDEIN Long-Verkauf am
    Andienungstag existierte — der Short-Anteil blieb unkorrigiert.
    """
    trades = [
        make_sell("2025-06-01", 2, 3.00, strike="105", expiry="2025-07-18",
                  pc="C", underlying="MIXD"),
        make_assignment("2025-07-18", 2, strike="105", expiry="2025-07-18",
                        pc="C", underlying="MIXD"),
        # 100 Shares aus Long-Bestand verkauft: PnL = 50 echt + 299.5 Praemie
        _call_stk_row("MIXD", "mixd_long_sale", "SELL", "C", -100, "2025-07-18", 349.5,
                      tt="BookTrade"),
        # 100 Shares als Short eroeffnet (PnL=0)
        _call_stk_row("MIXD", "mixd_short_open", "SELL", "O", -100, "2025-07-18", 0,
                      tt="BookTrade"),
        # Cover: PnL = 100 echt + 299.5 Praemie
        _call_stk_row("MIXD", "mixd_cover", "BUY", "C", 100, "2025-09-10", 399.5),
    ]
    rd = calculate_for_trades(trades, tax_year=2025)  # KEINE closed_lots

    rows = {(r["reportDate"], r["buySell"]): r for r in rd["trade_details"]
            if r.get("symbol") == "MIXD" and r.get("source") == "trades"}
    long_sale = rows[("2025-07-18", "SELL")]
    cover = rows[("2025-09-10", "BUY")]
    assert long_sale.get("stillhalter_adjusted"), "TC28: Long-Verkauf muss korrigiert sein"
    assert cover.get("stillhalter_adjusted"), "TC28: Short-Cover muss korrigiert sein"
    assert_close(long_sale["fifoPnlRealized"], 50.0, label="TC28 long sale pnl")
    assert_close(cover["fifoPnlRealized"], 100.0, label="TC28 cover pnl")
    assert_close(rd.get("stocks_gain_eur", 0), 150.0, label="TC28 stocks_gain")
    assert rd["audit"].get("stillhalter_corrections_dropped", []) == [], \
        "TC28: keine verworfenen Korrekturen erwartet"

    print("  TC28 Mixed Long/Short-Call-Andienung ohne closed_lots: OK")
    print("    Long-Sale 349.5 -> 50, Cover 399.5 -> 100")


def test_call_assignment_open_short_is_not_an_error():
    """TC29: Short aus Call-Andienung am Jahresende noch offen = KEIN Fehler.

    Der Aktien-PnL ist unrealisiert; die Praemie gehoert nur in Topf 2. Bisher
    landete der Fall faelschlich in stillhalter_corrections_dropped (Warnung
    Doppelversteuerung) — korrekt ist: keine Korrektur, Info-Tracking in
    stillhalter_open_short fuer den Folgejahr-Lauf.
    """
    trades = [
        make_sell("2025-11-03", 1, 3.00, strike="105", expiry="2025-12-19",
                  pc="C", underlying="OPSH"),
        make_assignment("2025-12-19", 1, strike="105", expiry="2025-12-19",
                        pc="C", underlying="OPSH"),
        _call_stk_row("OPSH", "opsh_short_open", "SELL", "O", -100, "2025-12-19", 0,
                      tt="BookTrade"),
        # KEIN Cover bis Jahresende
    ]
    rd = calculate_for_trades(trades, tax_year=2025)

    assert rd["audit"].get("stillhalter_corrections_dropped", []) == [], \
        "TC29: offener Short darf NICHT als dropped/Doppelversteuerung gemeldet werden"
    open_short = rd["audit"].get("stillhalter_open_short", [])
    assert len(open_short) == 1, f"TC29: erwartet 1 open_short-Eintrag, aktuell {open_short}"
    assert open_short[0]["underlying"] == "OPSH"
    assert_close(open_short[0]["shares"], 100.0, label="TC29 open shares")
    # Praemie korrekt in Topf 2, kein Aktien-PnL korrigiert
    assert_close(rd.get("options_gain_eur", 0), 299.0, label="TC29 options_gain")
    assert_close(rd.get("stocks_gain_eur", 0), 0.0, label="TC29 stocks_gain")

    print("  TC29 Offener Short aus Call-Andienung ist kein Fehler: OK")
    print("    Praemie 299 in Topf 2, open_short getrackt, dropped leer")


def test_call_correction_targets_assignment_row_not_unrelated_same_day_trade():
    """TC30: Fremde Same-Day-Row darf die Call-Korrektur nicht konsumieren.

    Codex-Review Finding (4. Runde, P2): Das Datums-/Richtungs-Gate liess die
    erste Row des Tages in debug_rows-Reihenfolge gewinnen. Die Korrektur muss
    die Row-Identitaet tragen (Resolver kennt die konsumierte Row) und der
    Resolver muss die BookTrade-Andienungsrow vor fremden ExchTrades waehlen.
    Materieller Schaden ohne Fix: gain/loss-Split kippt (Fremd-Row ist Verlust).
    """
    trades = [
        make_sell("2025-06-01", 1, 3.00, strike="105", expiry="2025-07-18",
                  pc="C", underlying="UNRL"),
        make_assignment("2025-07-18", 1, strike="105", expiry="2025-07-18",
                        pc="C", underlying="UNRL"),
        # Unabhaengiger Verkauf am SELBEN Tag, VOR der Andienungs-Row, Verlust-Row
        _call_stk_row("UNRL", "unrl_unrelated_sale", "SELL", "C", -100,
                      "2025-07-18", -80.0, tt="ExchTrade"),
        # Andienungs-Verkauf (BookTrade): PnL = 50 echt + 299 Praemie
        _call_stk_row("UNRL", "unrl_assignment_sale", "SELL", "C", -100,
                      "2025-07-18", 349.0, tt="BookTrade"),
    ]
    rd = calculate_for_trades(trades, tax_year=2025)

    rows = {r["transactionType"]: r for r in rd["trade_details"]
            if r.get("symbol") == "UNRL" and r.get("source") == "trades"}
    unrelated = rows["ExchTrade"]
    assignment = rows["BookTrade"]
    assert not unrelated.get("stillhalter_adjusted"), \
        "TC30: fremde Same-Day-Row darf NICHT korrigiert werden"
    assert_close(unrelated["fifoPnlRealized"], -80.0, label="TC30 fremde Row pnl")
    assert assignment.get("stillhalter_adjusted"), \
        "TC30: Andienungs-Row (BookTrade) muss die Korrektur erhalten"
    assert_close(assignment["fifoPnlRealized"], 50.0, label="TC30 assignment pnl")
    assert_close(rd.get("stocks_gain_eur", 0), 50.0, label="TC30 stocks_gain")
    assert_close(rd.get("stocks_loss_eur", 0), -80.0, label="TC30 stocks_loss")

    print("  TC30 Call-Korrektur trifft Andienungs-Row, nicht fremde Same-Day-Row: OK")
    print("    BookTrade 349 -> 50, fremde ExchTrade-Row -80 unveraendert")


def test_put_correction_prefers_matching_lot_cost_row():
    """TC32: Put-Korrektur waehlt unter Same-Day-Verkaeufen die Lot-passende Row.

    Gleiche Klasse wie TC30 fuer Puts: zwei Verkaeufe am Lot-close_date; die
    Row mit der Lot-Kostenbasis (9801 = Strike - Praemie) ist das echte Ziel,
    nicht die fremde Verlust-Row, die zufaellig zuerst in debug_rows steht.
    """
    trades = [
        make_sell("2025-06-02", 1, 2.00, strike="100", expiry="2025-06-20",
                  pc="P", underlying="PREF"),
        make_assignment("2025-06-20", 1, strike="100", expiry="2025-06-20",
                        pc="P", underlying="PREF"),
        {
            "tradeID": "pref_assignment_buy",
            "assetCategory": "STK", "transactionType": "BookTrade", "buySell": "BUY",
            "openCloseIndicator": "O", "underlyingSymbol": "PREF", "symbol": "PREF",
            "description": "PREF CORP", "quantity": "100",
            "tradePrice": "100", "closePrice": "98", "ibCommission": "0",
            "fxRateToBase": "1.0", "currency": "EUR",
            "dateTime": "2025-06-20 16:20:00", "tradeDate": "2025-06-20",
            "reportDate": "2025-06-20", "fifoPnlRealized": "0",
            "cost": "9801", "proceeds": "-9801",
        },
        # Fremder Alt-Bestands-Verkauf am selben Tag (Verlust, cost 5000), zuerst
        {
            "tradeID": "pref_unrelated_sale",
            "assetCategory": "STK", "transactionType": "ExchTrade", "buySell": "SELL",
            "openCloseIndicator": "C", "underlyingSymbol": "PREF", "symbol": "PREF",
            "description": "PREF CORP", "quantity": "-100",
            "tradePrice": "49.2", "closePrice": "49.2", "ibCommission": "0",
            "fxRateToBase": "1.0", "currency": "EUR",
            "dateTime": "2025-07-10 10:00:00", "tradeDate": "2025-07-10",
            "reportDate": "2025-07-10", "fifoPnlRealized": "-80",
            "cost": "5000", "proceeds": "4920",
        },
        # Echter Verkauf des angedienten Bestands (cost 9801 = reduzierte Basis)
        {
            "tradeID": "pref_real_sale",
            "assetCategory": "STK", "transactionType": "ExchTrade", "buySell": "SELL",
            "openCloseIndicator": "C", "underlyingSymbol": "PREF", "symbol": "PREF",
            "description": "PREF CORP", "quantity": "-100",
            "tradePrice": "100", "closePrice": "100", "ibCommission": "0",
            "fxRateToBase": "1.0", "currency": "EUR",
            "dateTime": "2025-07-10 11:00:00", "tradeDate": "2025-07-10",
            "reportDate": "2025-07-10", "fifoPnlRealized": "199",
            "cost": "9801", "proceeds": "10000",
        },
    ]
    closed_lots = [{
        "assetCategory": "STK", "currency": "EUR",
        "reportDate": "2025-07-10", "dateTime": "2025-07-10 11:00:00",
        "openDateTime": "2025-06-20 16:20:00",
        "quantity": "100", "buySell": "SELL", "cost": "9801",
        "fifoPnlRealized": "199", "fxRateToBase": "1.0",
        "symbol": "PREF", "underlyingSymbol": "PREF",
    }]
    rd = calculate_for_trades(trades, tax_year=2025, closed_lots=closed_lots)

    rows = {r["tradePrice"]: r for r in rd["trade_details"]
            if r.get("symbol") == "PREF" and r.get("source") == "trades"
            and r.get("buySell") == "SELL"}
    unrelated = rows[49.2]
    real = rows[100.0]
    assert not unrelated.get("stillhalter_adjusted"), \
        "TC32: fremde Row (cost 5000) darf NICHT korrigiert werden"
    assert_close(unrelated["fifoPnlRealized"], -80.0, label="TC32 fremde Row pnl")
    assert real.get("stillhalter_adjusted"), \
        "TC32: Lot-passende Row (cost 9801) muss korrigiert werden"
    assert_close(real["fifoPnlRealized"], 0.0, label="TC32 echte Row pnl")
    assert_close(rd.get("stocks_gain_eur", 0), 0.0, label="TC32 stocks_gain")
    assert_close(rd.get("stocks_loss_eur", 0), -80.0, label="TC32 stocks_loss")

    print("  TC32 Put-Korrektur waehlt Lot-passende Row (cost-Match): OK")
    print("    Echte Row 199 -> 0, fremde Row -80 unveraendert")


def test_worthless_expiry_without_history_warns_unmatched():
    """TC31: Verfall eines Vorjahres-Shorts OHNE geladene History muss warnen.

    Codex-Review Finding (4. Runde, P2): Der Missing-History-Detektor scannte
    nur ExchTrade-BUYs. Ein BookTrade-Verfall (PnL = Praemie) ohne Eroeffnungs-
    SELL im Datensatz blieb unbemerkt, obwohl die Praemie doppelt versteuert
    wird (Zufluss im Vorjahr + Verfalls-PnL im Steuerjahr).
    """
    trades = [
        # NUR der Verfall — der 2024-SELL fehlt (keine --history geladen)
        make_expiry("2025-01-17", 1, 499.0, strike="100", expiry="2025-01-17",
                    underlying="NOHIST"),
    ]
    rd = calculate_for_trades(trades, tax_year=2025)

    unmatched = rd["audit"].get("zufluss_unmatched", [])
    assert len(unmatched) == 1, \
        f"TC31: erwartet 1 zufluss_unmatched-Eintrag, aktuell {unmatched}"
    assert unmatched[0]["underlyingSymbol"] == "NOHIST"
    # PnL bleibt (ohne History unvermeidbar) voll in options_gain
    assert_close(rd.get("options_gain_eur", 0), 499.0, label="TC31 options_gain")

    print("  TC31 Verfall ohne Vorjahres-XML erzeugt zufluss_unmatched-Warnung: OK")
    print("    NOHIST 499.00 als doppelt-versteuert-Risiko gemeldet")


def test_prior_put_assignment_without_original_sell_warns_unmatched():
    """Cross-Year-Andienung ohne Original-SELL muss als Prueffall erscheinen."""
    trades = [
        make_assignment(
            "2024-12-20", 1, strike="100", expiry="2024-12-20",
            underlying="NOSELL",
        ),
    ]
    rd = calculate_for_trades(trades, tax_year=2025)

    unmatched = rd["audit"].get("stillhalter_unmatched", [])
    assert len(unmatched) == 1, \
        f"Cross-Year-Andienung ohne SELL nicht eindeutig gemeldet: {unmatched}"
    item = unmatched[0]
    assert item["type"] == "cross_year"
    assert item["symbol"].startswith("NOSELL")
    assert item["putCall"] == "P"
    assert item["quantity"] == 1

    print("  TC44 Cross-Year-Andienung ohne Original-SELL warnt: OK")


def test_unrelated_prior_put_without_sell_does_not_warn_current_report():
    """Alte Andienung ohne im Steuerjahr geschlossenes Lot ist kein Prueffall."""
    trades = [
        make_assignment(
            "2022-12-30", 1, strike="100", expiry="2022-12-30",
            underlying="OLDPUT",
        ),
    ]
    closed_lots = [{
        "assetCategory": "STK",
        "reportDate": "2025-06-30",
        "dateTime": "2025-06-30 10:00:00",
        "openDateTime": "2024-01-15 10:00:00",
        "quantity": "100",
        "symbol": "OTHER",
        "underlyingSymbol": "OTHER",
    }]
    rd = calculate_for_trades(
        trades, tax_year=2025, closed_lots=closed_lots,
    )

    unmatched = rd["audit"].get("stillhalter_unmatched", [])
    assert unmatched == [], (
        "Eine historische Andienung ohne aktuellen Closed-Lot-Bezug darf den "
        f"Steuerjahresbericht nicht warnen: {unmatched}"
    )

    print("  TC45 Irrelevante Alt-Andienung erzeugt keine aktuelle Warnung: OK")


def test_cross_year_assignment_matches_prior_sell_across_date_formats():
    """TC46: Vorjahres-SELL (ISO) matcht Steuerjahr-Andienung im Kompaktformat.

    Flex Queries koennen pro Query unterschiedlich konfigurierte Datumsformate
    liefern (Issue #90: yyyyMMdd + Semikolon-Separator als IBKR-Default). Nach
    der Normalisierung in load_csv muessen expiry-Keys und Datums-Slices
    identisch sein — sonst verfehlt der Cross-Year-Pfad den Original-SELL und
    die Praemie faellt als Prueffall aus, obwohl die Daten vollstaendig sind.
    """
    def compact(row):
        row = dict(row)
        for field in ("tradeDate", "reportDate", "expiry"):
            row[field] = row[field].replace("-", "")
        date_part, time_part = row["dateTime"].split(" ", 1)
        row["dateTime"] = (
            date_part.replace("-", "") + ";" + time_part.replace(":", "")
        )
        return row

    trades = [
        make_sell("2024-11-15", 1, 2.00, strike="100", expiry="2025-06-20",
                  underlying="MIXFMT"),
        compact(make_assignment("2025-06-20", 1, strike="100",
                                expiry="2025-06-20", underlying="MIXFMT")),
    ]
    rd = calculate_for_trades(trades, tax_year=2025)

    unmatched = rd["audit"].get("stillhalter_unmatched", [])
    assert unmatched == [], (
        f"Gemischte Datumsformate duerfen keinen Prueffall erzeugen: {unmatched}"
    )
    assert_close(rd["audit"]["cross_year_premium_eur"], 1 * 2.00 * 100 - 1,
                 label="TC46 cross_year_premium")
    details = rd["audit"].get("stillhalter_details", [])
    assert any(d.get("is_cross_year") for d in details), \
        "Cross-Year-Detail fehlt trotz Vorjahres-SELL"

    print("  TC46 Gemischte IBKR-Datumsformate im Cross-Year-Match: OK")


def test_occ_renamed_series_close_matches_original_sell():
    """TC33: OCC-Umbenennung (Spinoff): Close unter MMM1 schliesst den SELL unter MMM.

    Real-Fall Konvex 2024 (Solventum-Spinoff der 3M Company, 01.04.2024): Put
    verkauft unter MMM, nach der Kapitalmassnahme unter MMM1 zurueckgekauft.
    Ohne Familien-Matching galt der SELL als offen -> Praemie doppelt erfasst
    (Zufluss-Praemie UND Rueckkauf-PnL) plus falsche unmatched-Warnung.
    """
    trades = [
        make_sell("2024-01-23", 1, 1.40, strike="80", expiry="2024-07-19",
                  underlying="MMM", commission=-0.80076),
        make_buy_close("2024-04-11", 1, 0.25, 113.40239, strike="80",
                       expiry="2024-07-19", underlying="MMM1"),
    ]
    rd = calculate_for_trades(trades, tax_year=2024)
    audit = rd.get("audit", {})

    assert_close(audit.get("zufluss_premium_eur", 0), 0.0,
                 label="TC33 zufluss_premium_eur")
    unmatched = audit.get("zufluss_unmatched", [])
    assert unmatched == [], f"TC33: unerwartete unmatched-Warnung: {unmatched}"
    assert_close(rd.get("options_gain_eur", 0), 113.40239,
                 label="TC33 options_gain (nur Rueckkauf-PnL, keine Doppelzaehlung)")
    # Transparenz: Familien-Match wird als occ_rename_match getrackt (GUI-Hinweis)
    renames = audit.get("occ_rename_matches", [])
    assert len(renames) == 1, f"TC33: erwartet 1 occ_rename_match, aktuell {renames}"
    assert renames[0]["sell_underlying"] == "MMM"
    assert renames[0]["close_underlying"] == "MMM1"
    assert renames[0]["quantity"] == 1

    print("  TC33 OCC-Umbenennung: MMM1-Close matcht MMM-SELL, keine Doppelzaehlung: OK")
    print("    options_gain 113.40 EUR statt 252.64 EUR, occ_rename_match getrackt")


def test_occ_family_prefers_exact_series():
    """TC34: Koexistieren Original- und adjusted Serie, gewinnt der exakte Key.

    Der MMM1-Close konsumiert den (juengeren) MMM1-SELL, NICHT den aelteren
    MMM-SELL — der Familien-Fallback greift nur fuer sonst unmatchte Closes.
    """
    trades = [
        make_sell("2024-02-01", 1, 2.00, strike="80", expiry="2024-07-19",
                  underlying="MMM", commission=0.0),
        make_sell("2024-03-01", 1, 3.00, strike="80", expiry="2024-07-19",
                  underlying="MMM1", commission=0.0),
        make_buy_close("2024-04-11", 1, 0.50, 250.0, strike="80",
                       expiry="2024-07-19", underlying="MMM1"),
    ]
    rd = calculate_for_trades(trades, tax_year=2024)
    audit = rd.get("audit", {})

    # MMM1-SELL (300) konsumiert, MMM-SELL (200) bleibt offener Zufluss
    assert_close(audit.get("zufluss_premium_eur", 0), 200.0,
                 label="TC34 zufluss_premium_eur (nur MMM-SELL offen)")
    zd = [d for d in audit.get("zufluss_details", []) or []
          if d.get("underlyingSymbol") == "MMM"]
    assert len(zd) == 1, \
        f"TC34: erwartet MMM als offenen Zufluss, aktuell {audit.get('zufluss_details')}"
    # Exakter Match ist KEIN Rename-Fall: kein occ_rename_match, kein GUI-Hinweis
    assert audit.get("occ_rename_matches", []) == [], \
        f"TC34: exakter Match darf kein occ_rename_match erzeugen: {audit.get('occ_rename_matches')}"

    print("  TC34 Exact-Key-Prioritaet: MMM1-Close konsumiert MMM1-SELL zuerst: OK")
    print("    MMM-SELL bleibt offener Zufluss (200.00 EUR)")


def test_fop_digit_suffix_not_grouped():
    """TC35: FOP-Underlyings mit legitimen Ziffern-Suffixen (ESZ4/ESZ5) bleiben getrennt."""
    trades = [
        make_sell("2024-02-01", 1, 2.00, strike="5000", expiry="2024-06-21",
                  underlying="ESZ4", a_cat="FOP", multiplier="50", commission=0.0),
        make_buy_close("2024-04-11", 1, 0.50, 150.0, strike="5000",
                       expiry="2024-06-21", underlying="ESZ5", a_cat="FOP",
                       multiplier="50"),
    ]
    rd = calculate_for_trades(trades, tax_year=2024)
    audit = rd.get("audit", {})

    # Keine Familien-Zuordnung: ESZ4-SELL bleibt offener Zufluss (2.00 x 50),
    # der ESZ5-Close bleibt unmatched und wird gewarnt.
    assert_close(audit.get("zufluss_premium_eur", 0), 100.0,
                 label="TC35 zufluss_premium_eur (ESZ4 bleibt offen)")
    unmatched = audit.get("zufluss_unmatched", [])
    assert len(unmatched) == 1 and unmatched[0]["underlyingSymbol"] == "ESZ5", \
        f"TC35: erwartet ESZ5-unmatched-Warnung, aktuell {unmatched}"

    print("  TC35 FOP-Ziffern-Suffix wird NICHT als OCC-Familie gruppiert: OK")
    print("    ESZ4-Zufluss 100.00 EUR bleibt, ESZ5-Close warnt unmatched")


def test_option_split_matches_by_conid_and_cost_basis():
    """TC36: Ein Optionssplit wird trotz geaenderter Stückzahl und Strike korrekt geschlossen."""
    sell = make_sell(
        "2025-12-04", 1, 1.20, strike="88", expiry="2026-01-16",
        underlying="XLE", commission=-0.76689,
    )
    sell.update({
        "accountId": "TEST",
        "conid": "653278898",
        "symbol": "XLE   260116P00088000",
        "cost": "-119.23311",
        "fxRateToBase": "0.85878",
    })
    close = make_buy_close(
        "2025-12-30", 2, 0.33, 51.90491, strike="44",
        expiry="2026-01-16", underlying="XLE",
    )
    close.update({
        "accountId": "TEST",
        "conid": "653278898",
        "symbol": "XLE   260116P00044000",
        "cost": "119.23311",
        "ibCommission": "-1.3282",
        "fxRateToBase": "0.85122",
    })

    rd = calculate_for_trades([sell, close], tax_year=2025)
    audit = rd.get("audit", {})

    assert_close(audit.get("zufluss_premium_eur", 0), 0.0,
                 label="TC36 zufluss_premium_eur")
    assert audit.get("zufluss_unmatched", []) == [], \
        f"TC36: unerwartete unmatched-Warnung: {audit.get('zufluss_unmatched')}"
    assert_close(rd.get("options_gain_eur", 0), 44.182497,
                 label="TC36 options_gain (nur realisiertes IBKR-Ergebnis)")

    matches = audit.get("occ_rename_matches", [])
    assert len(matches) == 1, \
        f"TC36: erwartet einen Split-Match, aktuell {matches}"
    match = matches[0]
    assert match.get("match_type") == "split"
    assert match.get("conid") == "653278898"
    assert_close(match.get("quantity"), 1.0, label="TC36 alte Kontraktzahl")
    assert_close(match.get("close_quantity"), 2.0,
                 label="TC36 neue Kontraktzahl")
    assert_close(match.get("ratio"), 2.0, label="TC36 Split-Verhaeltnis")

    print("  TC36 Optionssplit: XLE P88 1x matcht XLE P44 2x per conid/cost: OK")
    print("    Kein falscher Zufluss und keine unmatched-Warnung")


def test_split_call_assignment_reclassifies_premium_and_stock_pnl():
    """TC37: Split-Andienung nutzt alte Praemie und neue Aktienmenge."""
    sell = make_sell(
        "2025-12-04", 1, 1.20, strike="88", expiry="2026-01-16",
        pc="C", underlying="XLE", commission=-0.76689,
    )
    sell.update({
        "accountId": "TEST",
        "conid": "653278898",
        "symbol": "XLE   260116C00088000",
        "cost": "-119.23311",
    })
    assignment = make_assignment(
        "2025-12-30", 2, strike="44", expiry="2026-01-16",
        pc="C", underlying="XLE",
    )
    assignment.update({
        "accountId": "TEST",
        "conid": "653278898",
        "symbol": "XLE   260116C00044000",
        "cost": "119.23311",
    })
    stock_sale = _stock_sell_row(
        "xle_split_call_sale", "XLE", "2025-12-30", 200,
        319.23311, 8480.76689, 8800.0, transaction_type="BookTrade",
    )

    rd = calculate_for_trades(
        [sell, assignment, stock_sale], tax_year=2025,
    )
    audit = rd.get("audit", {})

    assert audit.get("stillhalter_unmatched", []) == []
    assert audit.get("stillhalter_corrections_dropped", []) == []
    details = audit.get("stillhalter_details", [])
    assert len(details) == 1
    assert_close(details[0]["quantity"], 2.0,
                 label="TC37 neue Kontraktzahl")
    assert_close(details[0]["premium_raw"], 119.23311,
                 label="TC37 alte Gesamtpraemie")
    assert_close(rd.get("options_gain_eur", 0), 119.23311,
                 label="TC37 options_gain")
    assert_close(rd.get("stocks_gain_eur", 0), 200.0,
                 label="TC37 stocks_gain")

    stock_rows = [
        row for row in rd["trade_details"]
        if row.get("symbol") == "XLE"
        and row.get("source") == "trades"
        and row.get("buySell") == "SELL"
    ]
    assert len(stock_rows) == 1
    assert stock_rows[0].get("stillhalter_adjusted")
    assert_close(stock_rows[0]["fifoPnlRealized"], 200.0,
                 label="TC37 Aktien-PnL ohne Praemie")

    matches = audit.get("occ_rename_matches", [])
    assert len(matches) == 1 and matches[0].get("match_type") == "split"
    assert_close(matches[0].get("ratio"), 2.0,
                 label="TC37 Split-Verhaeltnis")

    print("  TC37 Split-Call-Andienung: alte Praemie auf 2 neue Kontrakte verteilt: OK")
    print("    Optionspraemie 119.23 EUR, Aktien-PnL 319.23 -> 200.00 EUR")


def test_cross_year_split_put_assignment_uses_new_contract_quantity():
    """TC38: Vorjahres-Praemie bleibt bei Split-Put-Andienung korrekt zugeordnet."""
    sell = make_sell(
        "2024-12-04", 1, 1.20, strike="88", expiry="2026-01-16",
        pc="P", underlying="XLE", commission=-0.76689,
    )
    sell.update({
        "accountId": "TEST",
        "conid": "653278898",
        "symbol": "XLE   260116P00088000",
        "cost": "-119.23311",
    })
    assignment = make_assignment(
        "2025-12-30", 2, strike="44", expiry="2026-01-16",
        pc="P", underlying="XLE",
    )
    assignment.update({
        "accountId": "TEST",
        "conid": "653278898",
        "symbol": "XLE   260116P00044000",
        "cost": "119.23311",
    })

    rd = calculate_for_trades([sell, assignment], tax_year=2025)
    audit = rd.get("audit", {})

    assert audit.get("stillhalter_unmatched", []) == []
    details = audit.get("stillhalter_details", [])
    assert len(details) == 1
    assert_close(details[0]["quantity"], 2.0,
                 label="TC38 neue Put-Kontraktzahl")
    assert details[0]["is_cross_year"] is True
    assert_close(details[0]["premium_raw"], 119.23311,
                 label="TC38 Vorjahrespraemie")
    assert_close(audit.get("cross_year_premium_eur", 0), 119.23311,
                 label="TC38 cross_year_premium")
    assert_close(audit.get("put_nosell_premium_eur", 0), 119.23311,
                 label="TC38 put_nosell")

    print("  TC38 Split-Put-Andienung: Vorjahrespraemie korrekt auf 2 Kontrakte: OK")
    print("    Voller Zufluss 119.23 EUR bleibt dem Verkaufsjahr 2024 zugeordnet")


def test_put_ratio_assignment_corrects_closed_short_slice():
    """TC39: 1x2 Put-Ratio-Spread mit gemischtem Aktien-BUY (C;O).

    Der Long Put eröffnet 100 Aktien short. Zwei Short-Put-Andienungen kaufen
    200 Aktien: 100 decken den Short, 100 bleiben long. IBKR mischt Long-Put-
    Kosten und eine Short-Put-Prämie in den sofort realisierten Short-Cover-PnL.
    Die absolute Lot-Basis liegt dadurch ÜBER dem Short-Put-Strike; die normale
    strike-cost-Heuristik darf die notwendige Prämienkorrektur nicht verwerfen.
    """
    short_put_sell = make_sell(
        "2025-03-03", 2, 1.5, strike="100", expiry="2025-03-21",
        pc="P", underlying="RATIO", commission=0,
    )
    short_put_sell.update({
        "tradeID": "ratio_short_put_sell",
        "currency": "EUR",
        "fxRateToBase": "1",
    })
    short_put_assignment = make_assignment(
        "2025-03-21", 2, strike="100", expiry="2025-03-21",
        pc="P", underlying="RATIO",
    )
    short_put_assignment.update({
        "tradeID": "ratio_short_put_assignment",
        "currency": "EUR",
        "fxRateToBase": "1",
    })
    long_put_open = {
        "tradeID": "ratio_long_put_open",
        "assetCategory": "OPT", "transactionType": "ExchTrade",
        "buySell": "BUY", "openCloseIndicator": "O",
        "putCall": "P", "strike": "105", "expiry": "2025-03-21",
        "underlyingSymbol": "RATIO", "symbol": "RATIO 105 2025-03-21 P",
        "quantity": "1", "tradePrice": "2", "multiplier": "100",
        "ibCommission": "0", "fxRateToBase": "1",
        "currency": "EUR", "dateTime": "2025-03-03 11:00:00",
        "tradeDate": "2025-03-03", "reportDate": "2025-03-03",
        "fifoPnlRealized": "0",
    }
    long_put_exercise = {
        "tradeID": "ratio_long_put_exercise",
        "assetCategory": "OPT", "transactionType": "BookTrade",
        "buySell": "SELL", "openCloseIndicator": "C",
        "putCall": "P", "strike": "105", "expiry": "2025-03-21",
        "underlyingSymbol": "RATIO", "symbol": "RATIO 105 2025-03-21 P",
        "quantity": "-1", "tradePrice": "0", "multiplier": "100",
        "ibCommission": "0", "fxRateToBase": "1",
        "currency": "EUR", "dateTime": "2025-03-21 16:20:00",
        "tradeDate": "2025-03-21", "reportDate": "2025-03-21",
        "fifoPnlRealized": "0", "cost": "-200", "proceeds": "0",
    }
    stock_short_open = {
        "tradeID": "ratio_stock_short_open",
        "assetCategory": "STK", "transactionType": "BookTrade",
        "buySell": "SELL", "openCloseIndicator": "O",
        "underlyingSymbol": "RATIO", "symbol": "RATIO",
        "quantity": "-100", "tradePrice": "105", "multiplier": "1",
        "ibCommission": "0", "fxRateToBase": "1",
        "currency": "EUR", "dateTime": "2025-03-21 16:20:00",
        "tradeDate": "2025-03-21", "reportDate": "2025-03-21",
        "fifoPnlRealized": "0", "cost": "-10500",
        "proceeds": "10500",
    }
    mixed_stock_buy = {
        "tradeID": "ratio_mixed_stock_buy",
        "assetCategory": "STK", "transactionType": "BookTrade",
        "buySell": "BUY", "openCloseIndicator": "C;O",
        "underlyingSymbol": "RATIO", "symbol": "RATIO",
        "quantity": "200", "tradePrice": "100", "multiplier": "1",
        "ibCommission": "0", "fxRateToBase": "1",
        "currency": "EUR", "dateTime": "2025-03-21 16:20:00",
        "tradeDate": "2025-03-21", "reportDate": "2025-03-21",
        "fifoPnlRealized": "450", "cost": "10300",
        "proceeds": "-20000",
    }
    closed_lots = [{
        "assetCategory": "STK", "currency": "EUR",
        "reportDate": "2025-03-21", "dateTime": "2025-03-21 16:20:00",
        "openDateTime": "2025-03-21 16:20:00",
        "quantity": "-100", "buySell": "BUY", "cost": "-10300",
        "fifoPnlRealized": "450", "fxRateToBase": "1",
        "symbol": "RATIO", "underlyingSymbol": "RATIO",
    }]

    rd = calculate_for_trades(
        [
            short_put_sell, short_put_assignment,
            long_put_open, long_put_exercise,
            stock_short_open, mixed_stock_buy,
        ],
        tax_year=2025,
        closed_lots=closed_lots,
    )
    stock_rows = [
        row for row in rd["trade_details"]
        if row.get("symbol") == "RATIO" and row.get("source") == "trades"
    ]
    assert len(stock_rows) == 1
    row = stock_rows[0]

    half_short_premium_raw = 150
    assert row.get("stillhalter_adjusted")
    assert_close(
        row.get("stillhalter_adjustment_raw", 0),
        half_short_premium_raw,
        label="TC39 herausgerechnete Short-Put-Praemie",
    )
    assert_close(row["fifoPnlRealized"], 300,
                 label="TC39 korrigierter Aktien-PnL raw")
    assert_close(row["pnl_eur"], 300,
                 label="TC39 korrigierter Aktien-PnL EUR")
    assert_close(row["cost"], 10450,
                 label="TC39 korrigierte Cover-Kosten")
    assert_close(rd["audit"]["stillhalter_premium_eur"], 300,
                 label="TC39 volle Stillhalterpraemie Topf 2")
    assert_close(rd["audit"]["put_nosell_premium_eur"], 150,
                 label="TC39 offene Long-Haelfte")
    assert_close(rd["stocks_gain_eur"], 300,
                 label="TC39 Topf 1")
    assert_close(rd["options_gain_eur"], 300,
                 label="TC39 Topf 2")
    assert rd["audit"].get("stillhalter_corrections_dropped", []) == []

    print("  TC39 Put-Ratio-Assignment: geschlossener Short-Anteil korrigiert: OK")
    print("    Aktien-PnL 450 -> 300 EUR, volle Prämie separat in Topf 2")


def test_same_day_independent_stock_short_keeps_strike_basis():
    """TC40: Fremder Same-Day-Short bleibt trotz Long-Put-Ausübung unverändert.

    Der Short Put deckt FIFO einen um 10:00 Uhr manuell eröffneten Aktien-Short.
    Um 16:20 Uhr wird zwar zusätzlich ein Long Put ausgeübt, dessen Aktien-SELL
    eröffnet aber einen anderen, noch offenen Short-Lot. Ein bloßer Same-Day-
    Marker darf die beiden Lots nicht vermischen.
    """
    short_put_sell = make_sell(
        "2025-06-01", 1, 2.0, strike="100", expiry="2025-06-20",
        pc="P", underlying="SAFE", commission=0,
    )
    short_put_sell.update({"currency": "EUR"})
    short_put_assignment = make_assignment(
        "2025-06-20", 1, strike="100", expiry="2025-06-20",
        pc="P", underlying="SAFE",
    )
    short_put_assignment.update({"currency": "EUR"})
    long_put_open = {
        "tradeID": "safe_long_put_open",
        "assetCategory": "OPT", "transactionType": "ExchTrade",
        "buySell": "BUY", "openCloseIndicator": "O",
        "putCall": "P", "strike": "105", "expiry": "2025-06-20",
        "underlyingSymbol": "SAFE", "symbol": "SAFE 105 2025-06-20 P",
        "quantity": "1", "tradePrice": "1", "multiplier": "100",
        "ibCommission": "0", "fxRateToBase": "1",
        "currency": "EUR", "dateTime": "2025-06-01 11:00:00",
        "tradeDate": "2025-06-01", "reportDate": "2025-06-01",
        "fifoPnlRealized": "0",
    }
    long_put_exercise = {
        "tradeID": "safe_long_put_exercise",
        "assetCategory": "OPT", "transactionType": "BookTrade",
        "buySell": "SELL", "openCloseIndicator": "C",
        "putCall": "P", "strike": "105", "expiry": "2025-06-20",
        "underlyingSymbol": "SAFE", "symbol": "SAFE 105 2025-06-20 P",
        "quantity": "-1", "tradePrice": "0", "multiplier": "100",
        "ibCommission": "0", "fxRateToBase": "1",
        "currency": "EUR", "dateTime": "2025-06-20 16:20:00",
        "tradeDate": "2025-06-20", "reportDate": "2025-06-20",
        "fifoPnlRealized": "0", "cost": "-100", "proceeds": "0",
    }
    stock_short_open = {
        "tradeID": "safe_stock_short_open",
        "assetCategory": "STK", "transactionType": "ExchTrade",
        "buySell": "SELL", "openCloseIndicator": "O",
        "underlyingSymbol": "SAFE", "symbol": "SAFE",
        "quantity": "-100", "tradePrice": "110", "multiplier": "1",
        "ibCommission": "0", "fxRateToBase": "1",
        "currency": "EUR", "dateTime": "2025-06-20 10:00:00",
        "tradeDate": "2025-06-20", "reportDate": "2025-06-20",
        "fifoPnlRealized": "0", "cost": "-11000", "proceeds": "11000",
    }
    exercise_stock_short_open = {
        "tradeID": "safe_exercise_stock_short_open",
        "assetCategory": "STK", "transactionType": "BookTrade",
        "buySell": "SELL", "openCloseIndicator": "O",
        "underlyingSymbol": "SAFE", "symbol": "SAFE",
        "quantity": "-100", "tradePrice": "105", "multiplier": "1",
        "ibCommission": "0", "fxRateToBase": "1",
        "currency": "EUR", "dateTime": "2025-06-20 16:20:00",
        "tradeDate": "2025-06-20", "reportDate": "2025-06-20",
        "fifoPnlRealized": "0", "cost": "-10500", "proceeds": "10500",
    }
    stock_cover = {
        "tradeID": "safe_stock_cover",
        "assetCategory": "STK", "transactionType": "BookTrade",
        "buySell": "BUY", "openCloseIndicator": "C",
        "underlyingSymbol": "SAFE", "symbol": "SAFE",
        "quantity": "100", "tradePrice": "100", "multiplier": "1",
        "ibCommission": "0", "fxRateToBase": "1",
        "currency": "EUR", "dateTime": "2025-06-20 16:20:00",
        "tradeDate": "2025-06-20", "reportDate": "2025-06-20",
        "fifoPnlRealized": "1000", "cost": "10000", "proceeds": "-10000",
    }
    closed_lots = [{
        "assetCategory": "STK", "currency": "EUR",
        "reportDate": "2025-06-20", "dateTime": "2025-06-20 16:20:00",
        "openDateTime": "2025-06-20 10:00:00",
        "quantity": "-100", "buySell": "BUY", "cost": "-10000",
        "fifoPnlRealized": "1000", "fxRateToBase": "1",
        "symbol": "SAFE", "underlyingSymbol": "SAFE",
    }]

    rd = calculate_for_trades(
        [
            short_put_sell, short_put_assignment,
            long_put_open, long_put_exercise,
            stock_short_open, exercise_stock_short_open, stock_cover,
        ],
        tax_year=2025,
        closed_lots=closed_lots,
    )
    stock_rows = [
        row for row in rd["trade_details"]
        if row.get("symbol") == "SAFE" and row.get("source") == "trades"
    ]
    assert len(stock_rows) == 1
    row = stock_rows[0]

    assert not row.get("stillhalter_adjusted")
    assert_close(row["fifoPnlRealized"], 1000,
                 label="TC40 unveraenderter Aktien-PnL")
    assert_close(row["cost"], 10000,
                 label="TC40 unveraenderte Cover-Kosten")
    assert_close(rd["stocks_gain_eur"], 1000,
                 label="TC40 Topf 1")
    assert_close(rd["options_gain_eur"], 200,
                 label="TC40 Topf 2")

    print("  TC40 Fremder Same-Day-Short trotz Long-Put-Exercise unangetastet: OK")


def test_long_put_exercise_evidence_is_quantity_capped():
    """TC41: Ein Long-Put-Kontrakt belegt höchstens 100 Stock-Short-Aktien."""
    det = {
        "putCall": "P", "strike": "100", "currency": "EUR",
        "underlyingSymbol": "CAP", "symbol": "CAP 100 2025-06-20 P",
        "assignment_date": "2025-06-20",
        "assignment_trade_date": "2025-06-20",
    }
    long_put_exercise = {
        "tradeID": "cap_long_put_exercise",
        "assetCategory": "OPT", "transactionType": "BookTrade",
        "buySell": "SELL", "openCloseIndicator": "C",
        "putCall": "P", "strike": "105", "expiry": "2025-06-20",
        "underlyingSymbol": "CAP", "symbol": "CAP 105 2025-06-20 P",
        "quantity": "-1", "tradePrice": "0", "multiplier": "100",
        "currency": "EUR", "dateTime": "2025-06-20 16:20:00",
        "tradeDate": "2025-06-20", "reportDate": "2025-06-20",
        "fifoPnlRealized": "0",
    }
    consolidated_stock_short = {
        "tradeID": "cap_stock_short_open",
        "assetCategory": "STK", "transactionType": "BookTrade",
        "buySell": "SELL", "openCloseIndicator": "O",
        "underlyingSymbol": "CAP", "symbol": "CAP",
        "quantity": "-200", "tradePrice": "105", "multiplier": "1",
        "currency": "EUR", "dateTime": "2025-06-20 16:20:00",
        "tradeDate": "2025-06-20", "reportDate": "2025-06-20",
        "fifoPnlRealized": "0",
    }

    evidence = _long_put_exercise_short_openings(
        [long_put_exercise, consolidated_stock_short],
        det, "CAP",
    )
    assert_close(sum(item["shares"] for item in evidence), 100,
                 label="TC41 Evidenz-Cap aus Optionsmultiplikator")

    match = {
        "is_short_lot": True,
        "open_datetime": "2025-06-20 16:20:00",
        "shares": 200,
    }
    consumed = {}
    assert_close(
        _claim_long_put_exercise_short_shares(match, evidence, consumed),
        100,
        label="TC41 erster Claim",
    )
    assert_close(
        _claim_long_put_exercise_short_shares(match, evidence, consumed),
        0,
        label="TC41 Evidenz nicht doppelt konsumierbar",
    )

    print("  TC41 Long-Put-Evidenz: auf 100 Aktien gecappt und einmalig: OK")


def test_long_put_exercise_override_requires_embedded_premium():
    """TC42: Override verweigert, wenn der Cover-PnL die Prämie nicht trägt.

    Gleiche synthetische Konstellation wie TC39, aber IBKR hat den
    Andienungs-BUY zum reinen Strike gebucht: der realisierte Short-Cover-PnL
    (300 = Basis − Strike×100) enthält KEINE eingebettete Prämie. Der Beleg allein
    darf die Korrektur dann nicht erzwingen (das wäre ein Doppelabzug in
    Topf 1); stattdessen erscheint ein sichtbarer Prüffall.
    """
    short_put_sell = make_sell(
        "2025-03-03", 2, 1.5, strike="100", expiry="2025-03-21",
        pc="P", underlying="RATIO", commission=0,
    )
    short_put_sell.update({
        "tradeID": "ratio_short_put_sell",
        "currency": "EUR",
        "fxRateToBase": "1",
    })
    short_put_assignment = make_assignment(
        "2025-03-21", 2, strike="100", expiry="2025-03-21",
        pc="P", underlying="RATIO",
    )
    short_put_assignment.update({
        "tradeID": "ratio_short_put_assignment",
        "currency": "EUR",
        "fxRateToBase": "1",
    })
    long_put_exercise = {
        "tradeID": "ratio_long_put_exercise",
        "assetCategory": "OPT", "transactionType": "BookTrade",
        "buySell": "SELL", "openCloseIndicator": "C",
        "putCall": "P", "strike": "105", "expiry": "2025-03-21",
        "underlyingSymbol": "RATIO", "symbol": "RATIO 105 2025-03-21 P",
        "quantity": "-1", "tradePrice": "0", "multiplier": "100",
        "ibCommission": "0", "fxRateToBase": "1",
        "currency": "EUR", "dateTime": "2025-03-21 16:20:00",
        "tradeDate": "2025-03-21", "reportDate": "2025-03-21",
        "fifoPnlRealized": "0", "cost": "-200", "proceeds": "0",
    }
    stock_short_open = {
        "tradeID": "ratio_stock_short_open",
        "assetCategory": "STK", "transactionType": "BookTrade",
        "buySell": "SELL", "openCloseIndicator": "O",
        "underlyingSymbol": "RATIO", "symbol": "RATIO",
        "quantity": "-100", "tradePrice": "105", "multiplier": "1",
        "ibCommission": "0", "fxRateToBase": "1",
        "currency": "EUR", "dateTime": "2025-03-21 16:20:00",
        "tradeDate": "2025-03-21", "reportDate": "2025-03-21",
        "fifoPnlRealized": "0", "cost": "-10500",
        "proceeds": "10500",
    }
    mixed_stock_buy_plain_strike = {
        "tradeID": "ratio_mixed_stock_buy",
        "assetCategory": "STK", "transactionType": "BookTrade",
        "buySell": "BUY", "openCloseIndicator": "C;O",
        "underlyingSymbol": "RATIO", "symbol": "RATIO",
        "quantity": "200", "tradePrice": "100", "multiplier": "1",
        "ibCommission": "0", "fxRateToBase": "1",
        "currency": "EUR", "dateTime": "2025-03-21 16:20:00",
        "tradeDate": "2025-03-21", "reportDate": "2025-03-21",
        "fifoPnlRealized": "300", "cost": "10000",
        "proceeds": "-20000",
    }
    closed_lots = [{
        "assetCategory": "STK", "currency": "EUR",
        "reportDate": "2025-03-21", "dateTime": "2025-03-21 16:20:00",
        "openDateTime": "2025-03-21 16:20:00",
        "quantity": "-100", "buySell": "BUY", "cost": "-10300",
        "fifoPnlRealized": "300", "fxRateToBase": "1",
        "symbol": "RATIO", "underlyingSymbol": "RATIO",
    }]

    rd = calculate_for_trades(
        [
            short_put_sell, short_put_assignment,
            long_put_exercise,
            stock_short_open, mixed_stock_buy_plain_strike,
        ],
        tax_year=2025,
        closed_lots=closed_lots,
    )
    stock_rows = [
        row for row in rd["trade_details"]
        if row.get("symbol") == "RATIO" and row.get("source") == "trades"
    ]
    assert len(stock_rows) == 1
    row = stock_rows[0]

    assert not row.get("stillhalter_adjusted")
    assert_close(row["fifoPnlRealized"], 300,
                 label="TC42 unveraenderter Aktien-PnL")
    dropped = rd["audit"].get("stillhalter_corrections_dropped", [])
    mismatch = [d for d in dropped
                if d.get("reason") == "long_put_exercise_pnl_mismatch"]
    assert len(mismatch) == 1, f"TC42 erwartet 1 Pruef-Fall, got {dropped}"
    assert mismatch[0]["underlying"] == "RATIO"
    assert_close(mismatch[0]["leftover_raw"], 150,
                 label="TC42 Pruef-Fall-Betrag")
    assert_close(rd["options_gain_eur"], 300,
                 label="TC42 Praemie bleibt voll in Topf 2")

    print("  TC42 Override ohne eingebettete Praemie verweigert + Pruef-Fall: OK")


def test_correction_stage3_respects_target_direction():
    """TC43: Stufe-3-Fallback matcht BUY-Korrekturen nicht auf SELL-Rows."""
    corr_buy = {"close_date": "2025-06-20", "target_buysell": "BUY"}
    sell_row = {"buySell": "SELL", "reportDate": "2025-06-20"}
    buy_row = {"buySell": "BUY", "reportDate": "2025-06-20"}

    assert not _correction_matches_row(corr_buy, sell_row), \
        "TC43: BUY-Korrektur darf SELL-Row nicht matchen"
    assert _correction_matches_row(corr_buy, buy_row), \
        "TC43: BUY-Korrektur muss BUY-Row matchen"

    corr_legacy = {"close_date": "2025-06-20"}
    assert _correction_matches_row(corr_legacy, sell_row), \
        "TC43: Korrektur ohne target_buysell behaelt Altverhalten"

    print("  TC43 Stufe-3-Richtungs-Gate: OK")


def _future_option_sell(date, symbol, future_symbol, future_conid, pc,
                        price, multiplier, strike="100", expiry="2025-06-20"):
    row = make_sell(
        date, 1, price, strike=strike, expiry=expiry, pc=pc,
        underlying=future_symbol, a_cat="FOP", multiplier=str(multiplier),
        commission=0,
    )
    row.update({
        "accountId": "TEST_ACCOUNT",
        "tradeID": f"sell_{symbol}",
        "conid": f"opt_{symbol}",
        "underlyingConid": str(future_conid),
        "symbol": symbol,
        "currency": "EUR",
        "cost": str(-price * multiplier),
    })
    return row


def _future_option_assignment(date, symbol, future_symbol, future_conid, pc,
                              premium, multiplier, strike="100",
                              expiry="2025-06-20"):
    row = make_assignment(
        date, 1, strike=strike, expiry=expiry, pc=pc,
        underlying=future_symbol, a_cat="FOP", multiplier=str(multiplier),
    )
    row.update({
        "accountId": "TEST_ACCOUNT",
        "tradeID": f"assignment_{symbol}",
        "conid": f"opt_{symbol}",
        "underlyingConid": str(future_conid),
        "symbol": symbol,
        "currency": "EUR",
        "cost": str(premium),
        "openCloseIndicator": "C",
    })
    return row


def _future_trade(date_time, trade_id, symbol, conid, side, quantity,
                  multiplier, cost, proceeds, pnl, transaction_type,
                  open_close):
    date = date_time[:10]
    return {
        "accountId": "TEST_ACCOUNT",
        "tradeID": trade_id,
        "transactionID": trade_id,
        "assetCategory": "FUT",
        "transactionType": transaction_type,
        "buySell": side,
        "openCloseIndicator": open_close,
        "conid": str(conid),
        "symbol": symbol,
        "underlyingSymbol": symbol,
        "quantity": str(quantity),
        "tradePrice": "100",
        "multiplier": str(multiplier),
        "ibCommission": "0",
        "fxRateToBase": "1",
        "currency": "EUR",
        "dateTime": date_time,
        "tradeDate": date,
        "reportDate": date,
        "fifoPnlRealized": str(pnl),
        "cost": str(cost),
        "proceeds": str(proceeds),
    }


def _future_closed_lot(open_date_time, close_date_time, symbol, conid, side,
                       quantity, multiplier, cost, pnl,
                       opening_transaction_id=""):
    row = {
        "accountId": "TEST_ACCOUNT",
        "assetCategory": "FUT",
        "buySell": side,
        "conid": str(conid),
        "symbol": symbol,
        "underlyingSymbol": symbol,
        "quantity": str(quantity),
        "multiplier": str(multiplier),
        "currency": "EUR",
        "openDateTime": open_date_time,
        "dateTime": close_date_time,
        "reportDate": close_date_time[:10],
        "cost": str(cost),
        "fifoPnlRealized": str(pnl),
    }
    if opening_transaction_id:
        row["transactionID"] = opening_transaction_id
    return row


def test_future_option_assignments_correct_only_embedded_future_pnl():
    """TC47: FOP-Put deferred + FOP-Call direct close landen exakt in Topf 2."""
    put_sell = _future_option_sell(
        "2025-06-01", "FUTP P100", "FUTP", 7001, "P", 2, 100)
    put_assignment = _future_option_assignment(
        "2025-06-20", "FUTP P100", "FUTP", 7001, "P", 200, 100)
    put_delivery = _future_trade(
        "2025-06-20 16:20:00", "put_delivery", "FUTP", 7001,
        "BUY", 1, 100, 10000, -10000, 0, "BookTrade", "O")
    put_close = _future_trade(
        "2025-08-01 10:00:00", "put_close", "FUTP", 7001,
        "SELL", -1, 100, -9800, 9850, 50, "ExchTrade", "C")
    put_lot = _future_closed_lot(
        "2025-06-20 16:20:00", "2025-08-01 10:00:00",
        "FUTP", 7001, "SELL", 1, 100, 9800, 50)

    call_sell = _future_option_sell(
        "2025-06-01", "FUTC C100", "FUTC", 7002, "C", 3, 50)
    call_assignment = _future_option_assignment(
        "2025-06-20", "FUTC C100", "FUTC", 7002, "C", 150, 50)
    call_sell["assetCategory"] = "FSFOP"
    call_assignment["assetCategory"] = "FSFOP"
    call_direct_close = _future_trade(
        "2025-06-20 16:20:00", "call_direct", "FUTC", 7002,
        "SELL", -1, 50, -5050, 5000, 100, "BookTrade", "C")

    rd = calculate_for_trades(
        [
            put_sell, put_assignment, put_delivery, put_close,
            call_sell, call_assignment, call_direct_close,
        ],
        tax_year=2025,
        closed_lots=[put_lot],
    )
    future_rows = {
        row["symbol"]: row for row in rd["trade_details"]
        if row.get("assetCategory") == "FUT"
    }
    put_row = future_rows["FUTP"]
    call_row = future_rows["FUTC"]
    assert_close(put_row["fifoPnlRealized"], -150,
                 label="TC47 Put-FUT-PnL")
    assert_close(put_row["cost"], -10000,
                 label="TC47 Put-FUT-Kostenbasis")
    assert_close(call_row["fifoPnlRealized"], -50,
                 label="TC47 Call-FUT-PnL")
    assert_close(call_row["cost"], -5050,
                 label="TC47 Direct-Close-Kosten unveraendert")
    assert_close(rd["options_gain_eur"], 350,
                 label="TC47 Stillhalter-Gewinn")
    assert_close(rd["options_loss_eur"], -200,
                 label="TC47 Futures-Verlust")
    assert_close(rd["zeile_19_netto_eur"], 150,
                 label="TC47 Zeile 19")
    assert_close(rd["zeile_22_other_losses_eur"], 200,
                 label="TC47 Zeile 22")
    assert_close(rd["topf2_by_category"]["Futures"]["gain"], 0,
                 label="TC47 Futures-Gewinne")
    assert_close(rd["topf2_by_category"]["Futures"]["loss"], -200,
                 label="TC47 Futures-Verluste")
    assert_close(rd["audit"]["put_nosell_premium_eur"], 0,
                 label="TC47 FOP ist kein put_nosell")
    corrections = rd["audit"]["future_assignment_corrections"]
    assert len(corrections) == 2
    assert {item["mode"] for item in corrections} == {
        "deferred_close", "direct_close",
    }
    assert {item["target_trade_id"] for item in corrections} == {
        "put_close", "call_direct",
    }
    assert sorted(item["amount_raw"] for item in corrections) == [150, 200]
    assert_close(put_row["stillhalter_adjustment_raw"], 200,
                 label="TC47 Put-Korrekturbetrag")
    assert_close(call_row["stillhalter_adjustment_raw"], 150,
                 label="TC47 Call-Korrekturbetrag")
    assert put_row["stillhalter_adjusted"]
    assert call_row["stillhalter_adjusted"]
    assert not rd["audit"]["stillhalter_corrections_dropped"]

    print("  TC47 FOP-Put/Call korrigieren nur belegten FUT-PnL: OK")


def test_future_assignment_requires_exact_open_timestamp():
    """TC48: Ein fremdes Same-Day-FUT-Lot darf nicht konsumiert werden."""
    sell = _future_option_sell(
        "2025-06-01", "SAFE P100", "SAFE", 7101, "P", 2, 100)
    assignment = _future_option_assignment(
        "2025-06-20", "SAFE P100", "SAFE", 7101, "P", 200, 100)
    delivery = _future_trade(
        "2025-06-20 16:20:00", "safe_delivery", "SAFE", 7101,
        "BUY", 1, 100, 10000, -10000, 0, "BookTrade", "O")
    unrelated_close = _future_trade(
        "2025-08-01 10:00:00", "safe_close", "SAFE", 7101,
        "SELL", -1, 100, -9800, 9850, 50, "ExchTrade", "C")
    unrelated_lot = _future_closed_lot(
        "2025-06-20 10:00:00", "2025-08-01 10:00:00",
        "SAFE", 7101, "SELL", 1, 100, 9800, 50)

    rd = calculate_for_trades(
        [sell, assignment, delivery, unrelated_close],
        tax_year=2025,
        closed_lots=[unrelated_lot],
    )
    row = next(
        item for item in rd["trade_details"]
        if item.get("symbol") == "SAFE" and item.get("source") == "trades"
    )
    assert_close(row["fifoPnlRealized"], 50,
                 label="TC48 fremder FUT-PnL")
    assert_close(row["cost"], -9800,
                 label="TC48 fremde FUT-Kosten")
    assert not row.get("stillhalter_adjusted")
    assert_close(rd["options_gain_eur"], 250,
                 label="TC48 Praemie plus fremder FUT-Gewinn")
    assert_close(rd["options_loss_eur"], 0,
                 label="TC48 kein FUT-Verlust")
    assert not rd["audit"]["future_assignment_corrections"]
    assert not rd["audit"]["stillhalter_corrections_dropped"]

    print("  TC48 FUT-Matching verlangt exakten Assignment-Timestamp: OK")


def test_cross_year_future_assignment_corrects_only_current_close():
    """TC49: Vorjahres-FOP-Praemie wird nicht erneut erfasst, FUT-Close schon korrigiert."""
    sell = _future_option_sell(
        "2024-06-01", "XYF P100", "XYF", 7201, "P", 2, 100,
        expiry="2024-06-20")
    assignment = _future_option_assignment(
        "2024-06-20", "XYF P100", "XYF", 7201, "P", 200, 100,
        expiry="2024-06-20")
    delivery = _future_trade(
        "2024-06-20 16:20:00", "xy_delivery", "XYF", 7201,
        "BUY", 1, 100, 10000, -10000, 0, "BookTrade", "O")
    close = _future_trade(
        "2025-08-01 10:00:00", "xy_close", "XYF", 7201,
        "SELL", -1, 100, -9800, 9850, 50, "ExchTrade", "C")
    lot = _future_closed_lot(
        "2024-06-20 16:20:00", "2025-08-01 10:00:00",
        "XYF", 7201, "SELL", 1, 100, 9800, 50)

    rd = calculate_for_trades(
        [sell, assignment, delivery, close],
        tax_year=2025,
        closed_lots=[lot],
    )
    row = next(
        item for item in rd["trade_details"]
        if item.get("symbol") == "XYF" and item.get("source") == "trades"
    )
    assert_close(row["fifoPnlRealized"], -150,
                 label="TC49 Cross-Year-FUT-PnL")
    assert_close(row["cost"], -10000,
                 label="TC49 Cross-Year-FUT-Kostenbasis")
    assert_close(rd["options_gain_eur"], 0,
                 label="TC49 keine erneute Vorjahrespraemie")
    assert_close(rd["options_loss_eur"], -150,
                 label="TC49 aktueller FUT-Verlust")
    assert_close(rd["zeile_19_netto_eur"], -150,
                 label="TC49 Zeile 19")
    assert_close(rd["zeile_22_other_losses_eur"], 150,
                 label="TC49 Zeile 22")
    assert_close(rd["audit"]["stillhalter_premium_eur"], 0,
                 label="TC49 Stillhalterpraemie bleibt im Vorjahr")
    corrections = rd["audit"]["future_assignment_corrections"]
    assert len(corrections) == 1
    assert corrections[0]["mode"] == "deferred_close"
    assert corrections[0]["target_trade_id"] == "xy_close"
    assert_close(corrections[0]["amount_raw"], 200,
                 label="TC49 Korrekturbetrag")
    assert not rd["audit"]["stillhalter_corrections_dropped"]

    print("  TC49 Cross-Year-FOP korrigiert nur aktuellen FUT-Close: OK")


def test_future_assignment_without_closed_lot_warns_and_stays_unchanged():
    """TC50: Ein spaeterer FUT-Close ohne Lot-Beleg darf nicht still bleiben."""
    sell = _future_option_sell(
        "2025-06-01", "NOLOT P100", "NOLOT", 7301, "P", 2, 100)
    assignment = _future_option_assignment(
        "2025-06-20", "NOLOT P100", "NOLOT", 7301, "P", 200, 100)
    delivery = _future_trade(
        "2025-06-20 16:20:00", "nolot_delivery", "NOLOT", 7301,
        "BUY", 1, 100, 10000, -10000, 0, "BookTrade", "O")
    close = _future_trade(
        "2025-08-01 10:00:00", "nolot_close", "NOLOT", 7301,
        "SELL", -1, 100, -9800, 9850, 50, "ExchTrade", "C")

    rd = calculate_for_trades(
        [sell, assignment, delivery, close],
        tax_year=2025,
        closed_lots=[],
    )
    row = next(
        item for item in rd["trade_details"]
        if item.get("symbol") == "NOLOT" and item.get("source") == "trades"
    )
    assert_close(row["fifoPnlRealized"], 50,
                 label="TC50 unbelegter FUT-PnL bleibt unveraendert")
    assert_close(row["cost"], -9800,
                 label="TC50 unbelegte Kostenbasis bleibt unveraendert")
    assert not row.get("stillhalter_adjusted")
    assert not rd["audit"]["future_assignment_corrections"]
    dropped = rd["audit"]["stillhalter_corrections_dropped"]
    assert len(dropped) == 1
    assert dropped[0]["reason"] == "future_assignment_close_unproven"
    assert_close(dropped[0]["leftover_raw"], 200,
                 label="TC50 gemeldeter Risikobetrag")

    print("  TC50 FUT-Close ohne CLOSED_LOT wird als Prueffall gemeldet: OK")


def test_cross_year_future_assignment_without_sell_history_warns():
    """TC51: Fehlende Vorjahres-FOP-Historie wird nicht still uebergangen."""
    assignment = _future_option_assignment(
        "2024-06-20", "NOHISTF P100", "NOHISTF", 7401, "P", 200, 100,
        expiry="2024-06-20")
    delivery = _future_trade(
        "2024-06-20 16:20:00", "nohistf_delivery", "NOHISTF", 7401,
        "BUY", 1, 100, 10000, -10000, 0, "BookTrade", "O")
    close = _future_trade(
        "2025-08-01 10:00:00", "nohistf_close", "NOHISTF", 7401,
        "SELL", -1, 100, -9800, 9850, 50, "ExchTrade", "C")
    lot = _future_closed_lot(
        "2024-06-20 16:20:00", "2025-08-01 10:00:00",
        "NOHISTF", 7401, "SELL", 1, 100, 9800, 50)

    rd = calculate_for_trades(
        [assignment, delivery, close],
        tax_year=2025,
        closed_lots=[lot],
    )
    row = next(
        item for item in rd["trade_details"]
        if item.get("symbol") == "NOHISTF"
        and item.get("source") == "trades"
    )
    assert_close(row["fifoPnlRealized"], 50,
                 label="TC51 FUT-PnL ohne Praemienhistorie")
    assert_close(row["cost"], -9800,
                 label="TC51 Kosten ohne Praemienhistorie")
    assert not rd["audit"]["future_assignment_corrections"]
    dropped = rd["audit"]["stillhalter_corrections_dropped"]
    assert len(dropped) == 1
    assert dropped[0]["reason"] == "future_assignment_history_missing"
    assert not rd["audit"]["stillhalter_unmatched"]

    print("  TC51 Fehlende Cross-Year-FOP-Historie wird gemeldet: OK")


def test_historical_direct_future_close_not_reported_in_current_audit():
    """TC52: Ein vollstaendiger 2024-Direct-Close gehoert nicht ins Audit 2025."""
    sell = _future_option_sell(
        "2024-06-01", "OLD C100", "OLD", 7501, "C", 3, 50,
        expiry="2024-06-20")
    assignment = _future_option_assignment(
        "2024-06-20", "OLD C100", "OLD", 7501, "C", 150, 50,
        expiry="2024-06-20")
    direct_close = _future_trade(
        "2024-06-20 16:20:00", "old_direct", "OLD", 7501,
        "SELL", -1, 50, -5050, 5000, 100, "BookTrade", "C")

    rd = calculate_for_trades(
        [sell, assignment, direct_close],
        tax_year=2025,
        closed_lots=[],
    )
    assert_close(rd["options_gain_eur"], 0,
                 label="TC52 kein historischer Gewinn")
    assert_close(rd["options_loss_eur"], 0,
                 label="TC52 kein historischer Verlust")
    assert not rd["audit"]["future_assignment_corrections"]
    assert not rd["audit"]["stillhalter_corrections_dropped"]

    # Auch fehlerhafte historische Evidenz darf keinen kritischen Prueffall
    # im aktuellen Steuerjahr erzeugen.
    assignment["cost"] = "999"
    mismatch_rd = calculate_for_trades(
        [sell, assignment, direct_close],
        tax_year=2025,
        closed_lots=[],
    )
    assert not mismatch_rd["audit"]["future_assignment_corrections"]
    assert not mismatch_rd["audit"]["stillhalter_corrections_dropped"]

    print("  TC52 Historischer Direct-Close bleibt aus aktuellem Audit: OK")


def test_future_assignment_rejects_conflicting_lot_transaction_id():
    """TC53: Gleicher Timestamp reicht bei widerspruechlicher Opening-ID nicht."""
    sell = _future_option_sell(
        "2025-06-01", "TXID P100", "TXID", 7601, "P", 2, 100)
    assignment = _future_option_assignment(
        "2025-06-20", "TXID P100", "TXID", 7601, "P", 200, 100)
    delivery = _future_trade(
        "2025-06-20 16:20:00", "txid_delivery", "TXID", 7601,
        "BUY", 1, 100, 10000, -10000, 0, "BookTrade", "O")
    close = _future_trade(
        "2025-08-01 10:00:00", "txid_close", "TXID", 7601,
        "SELL", -1, 100, -9800, 9850, 50, "ExchTrade", "C")
    conflicting_lot = _future_closed_lot(
        "2025-06-20 16:20:00", "2025-08-01 10:00:00",
        "TXID", 7601, "SELL", 1, 100, 9800, 50,
        opening_transaction_id="different_opening")

    rd = calculate_for_trades(
        [sell, assignment, delivery, close],
        tax_year=2025,
        closed_lots=[conflicting_lot],
    )
    row = next(
        item for item in rd["trade_details"]
        if item.get("symbol") == "TXID" and item.get("source") == "trades"
    )
    assert_close(row["fifoPnlRealized"], 50,
                 label="TC53 PnL bei widerspruechlicher Opening-ID")
    assert_close(row["cost"], -9800,
                 label="TC53 Kosten bei widerspruechlicher Opening-ID")
    assert not rd["audit"]["future_assignment_corrections"]
    dropped = rd["audit"]["stillhalter_corrections_dropped"]
    assert len(dropped) == 1
    assert dropped[0]["reason"] == "future_assignment_close_unproven"

    print("  TC53 Widerspruechliche FUT-Opening-ID wird abgelehnt: OK")


def test_deferred_fsfop_call_corrects_future_cover():
    """TC54: Reale FSFOP-Richtung SELL/O -> spaeterer BUY-Cover."""
    sell = _future_option_sell(
        "2025-06-01", "DCALL C100", "DCALL", 7701, "C", 3, 50)
    assignment = _future_option_assignment(
        "2025-06-20", "DCALL C100", "DCALL", 7701, "C", 150, 50)
    sell["assetCategory"] = "FSFOP"
    assignment["assetCategory"] = "FSFOP"
    delivery = _future_trade(
        "2025-06-20 16:20:00", "dcall_delivery", "DCALL", 7701,
        "SELL", -1, 50, -5000, 5000, 0, "BookTrade", "O")
    cover = _future_trade(
        "2025-08-01 10:00:00", "dcall_cover", "DCALL", 7701,
        "BUY", 1, 50, 5150, -5100, 50, "ExchTrade", "C")
    lot = _future_closed_lot(
        "2025-06-20 16:20:00", "2025-08-01 10:00:00",
        "DCALL", 7701, "BUY", -1, 50, 5150, 50,
        opening_transaction_id="dcall_delivery")

    rd = calculate_for_trades(
        [sell, assignment, delivery, cover],
        tax_year=2025,
        closed_lots=[lot],
    )
    row = next(
        item for item in rd["trade_details"]
        if item.get("symbol") == "DCALL" and item.get("source") == "trades"
    )
    assert_close(row["fifoPnlRealized"], -100,
                 label="TC54 Call-FUT-Cover-PnL")
    assert_close(row["cost"], 5000,
                 label="TC54 Call-FUT-Cover-Kosten")
    assert_close(rd["options_gain_eur"], 150,
                 label="TC54 FSFOP-Praemie")
    assert_close(rd["options_loss_eur"], -100,
                 label="TC54 FUT-Cover-Verlust")
    corrections = rd["audit"]["future_assignment_corrections"]
    assert len(corrections) == 1
    assert corrections[0]["mode"] == "deferred_close"
    assert corrections[0]["target_trade_id"] == "dcall_cover"
    assert_close(corrections[0]["amount_raw"], 150,
                 label="TC54 Call-Korrekturbetrag")
    assert not rd["audit"]["stillhalter_corrections_dropped"]

    print("  TC54 Deferred FSFOP-Call korrigiert belegten FUT-Cover: OK")


def test_partial_future_assignment_warns_only_for_realized_remainder():
    """TC55: Quantity-2 warnt fuer unbelegten Close, nicht fuer offenen Rest."""
    sell = _future_option_sell(
        "2025-06-01", "PART P100", "PART", 7801, "P", 2, 100)
    sell["quantity"] = "-2"
    assignment = _future_option_assignment(
        "2025-06-20", "PART P100", "PART", 7801, "P", 400, 100)
    assignment["quantity"] = "2"
    delivery = _future_trade(
        "2025-06-20 16:20:00", "part_delivery", "PART", 7801,
        "BUY", 2, 100, 20000, -20000, 0, "BookTrade", "O")
    close_one = _future_trade(
        "2025-08-01 10:00:00", "part_close_one", "PART", 7801,
        "SELL", -1, 100, -9800, 9850, 50, "ExchTrade", "C")
    close_two = _future_trade(
        "2025-08-02 10:00:00", "part_close_two", "PART", 7801,
        "SELL", -1, 100, -9800, 9850, 50, "ExchTrade", "C")
    lot_one = _future_closed_lot(
        "2025-06-20 16:20:00", "2025-08-01 10:00:00",
        "PART", 7801, "SELL", 1, 100, 9800, 50,
        opening_transaction_id="part_delivery")

    rd = calculate_for_trades(
        [sell, assignment, delivery, close_one, close_two],
        tax_year=2025,
        closed_lots=[lot_one],
    )
    rows = {
        row["dateTime"]: row for row in rd["trade_details"]
        if row.get("symbol") == "PART" and row.get("source") == "trades"
    }
    assert_close(rows["2025-08-01 10:00:00"]["fifoPnlRealized"], -150,
                 label="TC55 belegter erster Slice")
    assert_close(rows["2025-08-02 10:00:00"]["fifoPnlRealized"], 50,
                 label="TC55 unbelegter zweiter Slice")
    assert len(rd["audit"]["future_assignment_corrections"]) == 1
    dropped = rd["audit"]["stillhalter_corrections_dropped"]
    assert len(dropped) == 1
    assert dropped[0]["reason"] == "future_assignment_close_unproven"
    assert_close(dropped[0]["leftover_shares"], 1,
                 label="TC55 ungeklärte Menge")
    assert_close(dropped[0]["leftover_raw"], 200,
                 label="TC55 ungeklärter Praemienanteil")

    # Ist der zweite Kontrakt noch offen, ist der belegte erste Slice korrekt
    # und es gibt keinen Grund fuer einen Prueffall.
    open_remainder_rd = calculate_for_trades(
        [sell, assignment, delivery, close_one],
        tax_year=2025,
        closed_lots=[lot_one],
    )
    assert len(open_remainder_rd["audit"]["future_assignment_corrections"]) == 1
    assert not open_remainder_rd["audit"]["stillhalter_corrections_dropped"]

    print("  TC55 Partielle FUT-Andienung warnt nur fuer realisierten Rest: OK")


def test_cross_year_future_assignment_missing_delivery_warns_from_lot():
    """TC56: Aktuelles Lot macht Vorjahresfall trotz fehlender Delivery relevant."""
    sell = _future_option_sell(
        "2024-06-01", "NODEL P100", "NODEL", 7901, "P", 2, 100,
        expiry="2024-06-20")
    assignment = _future_option_assignment(
        "2024-06-20", "NODEL P100", "NODEL", 7901, "P", 200, 100,
        expiry="2024-06-20")
    close = _future_trade(
        "2025-08-01 10:00:00", "nodel_close", "NODEL", 7901,
        "SELL", -1, 100, -9800, 9850, 50, "ExchTrade", "C")
    lot = _future_closed_lot(
        "2024-06-20 16:20:00", "2025-08-01 10:00:00",
        "NODEL", 7901, "SELL", 1, 100, 9800, 50)

    rd = calculate_for_trades(
        [sell, assignment, close],
        tax_year=2025,
        closed_lots=[lot],
    )
    row = next(
        item for item in rd["trade_details"]
        if item.get("symbol") == "NODEL" and item.get("source") == "trades"
    )
    assert_close(row["fifoPnlRealized"], 50,
                 label="TC56 PnL ohne Delivery-Beleg")
    assert_close(row["cost"], -9800,
                 label="TC56 Kosten ohne Delivery-Beleg")
    assert not rd["audit"]["future_assignment_corrections"]
    dropped = rd["audit"]["stillhalter_corrections_dropped"]
    assert len(dropped) == 1
    assert dropped[0]["reason"] == \
        "future_assignment_delivery_missing_or_ambiguous"
    assert_close(dropped[0]["leftover_raw"], 200,
                 label="TC56 gemeldeter Praemienanteil")

    print("  TC56 Fehlende FUT-Delivery wird aus aktuellem Lot erkannt: OK")


if __name__ == "__main__":
    test_cross_year_put_series()
    test_cross_year_call_series()
    test_steueryahr_only_no_op()
    test_mixed_year_assignment_splits_cross_year_premium()
    test_issue_56_prior_year_correction_uses_underlying()
    test_issue_56_current_year_zufluss_uses_underlying()
    test_put_assignment_does_not_double_correct_strike_basis()
    test_put_assignment_corrects_reduced_cost_basis()
    test_same_day_put_assignment_does_not_double_correct_strike_basis()
    test_prior_year_put_lot_sold_before_tax_year_does_not_touch_current_sale()
    test_prior_year_put_lot_sold_in_tax_year_is_still_corrected()
    test_same_year_put_requires_matching_closed_lot()
    test_zufluss_fifo_current_close_consumes_prior_sell_first()
    test_zufluss_fifo_prior_close_consumes_prior_sell_before_tax_year()
    test_cross_year_put_topf1_consistent_across_fx_rates()
    test_cross_year_put_correction_only_hits_sell_rows()
    test_cross_year_put_correction_handles_spaced_underlying_symbol()
    test_cross_year_worthless_expiry_gets_prior_zufluss_correction()
    test_same_year_worthless_expiry_no_correction()
    test_call_assignment_correction_only_hits_assignment_day_sale()
    test_put_and_call_premium_stack_on_same_stock_row()
    test_unapplied_correction_is_tracked_and_warned()
    test_call_assignment_short_cover_correction_on_buy_row()
    test_two_same_day_call_assignments_use_separate_cover_lots()
    test_two_same_day_put_assignments_use_separate_lots()
    test_call_short_cover_without_closed_lots_falls_back_to_trades()
    test_call_cover_with_partial_closed_lots()
    test_call_assignment_mixed_long_and_short_without_lots()
    test_call_assignment_open_short_is_not_an_error()
    test_call_correction_targets_assignment_row_not_unrelated_same_day_trade()
    test_worthless_expiry_without_history_warns_unmatched()
    test_prior_put_assignment_without_original_sell_warns_unmatched()
    test_unrelated_prior_put_without_sell_does_not_warn_current_report()
    test_cross_year_assignment_matches_prior_sell_across_date_formats()
    test_put_correction_prefers_matching_lot_cost_row()
    test_occ_renamed_series_close_matches_original_sell()
    test_occ_family_prefers_exact_series()
    test_fop_digit_suffix_not_grouped()
    test_option_split_matches_by_conid_and_cost_basis()
    test_split_call_assignment_reclassifies_premium_and_stock_pnl()
    test_cross_year_split_put_assignment_uses_new_contract_quantity()
    test_put_ratio_assignment_corrects_closed_short_slice()
    test_same_day_independent_stock_short_keeps_strike_basis()
    test_long_put_exercise_evidence_is_quantity_capped()
    test_long_put_exercise_override_requires_embedded_premium()
    test_correction_stage3_respects_target_direction()
    test_future_option_assignments_correct_only_embedded_future_pnl()
    test_future_assignment_requires_exact_open_timestamp()
    test_cross_year_future_assignment_corrects_only_current_close()
    test_future_assignment_without_closed_lot_warns_and_stays_unchanged()
    test_cross_year_future_assignment_without_sell_history_warns()
    test_historical_direct_future_close_not_reported_in_current_audit()
    test_future_assignment_rejects_conflicting_lot_transaction_id()
    test_deferred_fsfop_call_corrects_future_cover()
    test_partial_future_assignment_warns_only_for_realized_remainder()
    test_cross_year_future_assignment_missing_delivery_warns_from_lot()
    print("\nOK: alle 56 TCs gruen")
