"""Synthetischer Regression-Test fuer GH Issue #61.

Cross-Year-Same-Series-FIFO-Konflikt: Wenn dieselbe Option-Series sowohl im
Vorjahr als auch im Steuerjahr angedient wurde, hat der Same-Year-Block frueher
faelschlich die aeltesten Sells konsumiert (die im Vorjahres-Lauf bereits
versteuert waren). Pre-consume im _current_year_series_state-Build verschiebt
den FIFO-Startpunkt auf die juengeren Sells.

Aufruf: python tests/test_cross_year_series.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (
    _consume_open_sells_fifo,
    _get_open_option_sells,
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


if __name__ == "__main__":
    test_cross_year_put_series()
    test_cross_year_call_series()
    test_steueryahr_only_no_op()
    print("\nOK: alle 3 TCs gruen")
