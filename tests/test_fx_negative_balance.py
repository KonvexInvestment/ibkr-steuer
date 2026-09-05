"""Synthetische Tests fuer GH Issues #59 und #84 (Margin-Korrektur bei FX-Gewinnen).

TC1-TC22 pruefen die Saldo-getragene FIFO-Engine (`_init_fx_state`,
`_process_fx_event`), die den Option-C-Pfad traegt:

- Abfluesse aus negativem Saldo loesen keinen steuerbaren FX-PnL aus
- Zufluesse auf negativen Saldo tilgen Schuld (keine Lot-Erzeugung bis Saldo positiv)
- BUY/SELL/ADJ/DINT konsumieren Lots ohne PnL (Stale-Lot-Schutz)
- Negative Starting Balance startet als Schuld (kein Lot)
- DINT veraendert Saldo, loest aber keinen PnL aus

TC23-TC29 pruefen das Schuldtilgungs-Gate von Option A (Issue #84), das statt
eines nachgebauten Saldos das Vorzeichen der IBKR-Buchung auswertet:

- Zufluss mit realisiertem Ergebnis = Short-Close = Tilgung, nicht steuerbar
- Abfluss = Long-Close, ungekuerzt steuerbar (IBKR trennt den gedeckten Teil selbst)
- Margin-Tage stammen aus IBKRs balance-Spalte, mit Kumulations-Fallback

Aufruf: python tests/test_fx_negative_balance.py
"""
import os
import sys
import csv
import io
import contextlib
import tempfile
from datetime import date
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (
    _init_fx_state,
    _process_fx_event,
    calculate_tax,
    calculate_fx_gains,
)


TAX_YEAR = 2025


def make_tx(date, amount, fx, code, *, desc=None, txid=None, balance=None, currency="USD"):
    """Hilfsfunktion fuer fx_transactions-Zeilen (Dict-Form aus CSV)."""
    return {
        "currency": currency,
        "activityCode": code,
        "activityDescription": desc or "",
        "amount": str(amount),
        "fxRateToBase": str(fx),
        "date": date,
        "transactionID": txid or f"{date}_{code}_{amount}",
        "balance": "" if balance is None else str(balance),
        "symbol": "",
        "tradePrice": "",
    }


def starting_balance_tx(date, balance, fx, currency="USD"):
    return {
        "currency": currency,
        "activityCode": "",
        "activityDescription": "Starting Balance",
        "amount": "0",
        "fxRateToBase": str(fx),
        "date": date,
        "transactionID": "",
        "balance": str(balance),
        "symbol": "",
        "tradePrice": "",
    }


def approx(a, b, tol=0.01):
    return abs(a - b) <= tol


def write_csv(path, rows):
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def tc1_margin_tilgung():
    """Issue #59 Beispiel: Saldo wird zwischendurch negativ, dann wieder positiv.

    Events (USD):
    +1000 @ 1.10 (DIV) → balance 1000, Lot[1000@1.10]
    -1500 @ 1.05 (FRTAX): 1000 aus Lot konsumiert (PnL=1000*(1.05-1.10)=-50), 500 baut Schuld auf
    +500 @ 1.00 (DIV): tilgt Schuld komplett (Saldo=0), kein neuer Lot
    -500 @ 1.15 (FRTAX): Saldo war 0 → komplett aus Schuld, kein PnL

    Erwartung corrected: gain=0, loss=-50, net=-50.
    """
    fx_tx = [
        make_tx("2025-01-10", 1000.0, 1.10, "DIV"),
        make_tx("2025-02-10", -1500.0, 1.05, "FRTAX"),
        make_tx("2025-03-10", 500.0, 1.00, "DIV"),
        make_tx("2025-04-10", -500.0, 1.15, "FRTAX"),
    ]
    results, total_gain, total_loss, _ = calculate_fx_gains([], fx_tx, TAX_YEAR)
    usd = results.get("USD", {})
    assert approx(usd.get("gain", 0), 0.0), f"TC1 gain: erwartet 0, ist {usd.get('gain')}"
    assert approx(usd.get("loss", 0), -50.0), f"TC1 loss: erwartet -50, ist {usd.get('loss')}"
    assert usd.get("days_negative", 0) > 0, "TC1 days_negative sollte > 0 sein"
    print(f"TC1 OK — corrected: gain={usd['gain']:.2f}, loss={usd['loss']:.2f}, "
          f"raw_net={usd['raw_net']:.2f}, neg_days={usd['days_negative']}")


def tc2_dauerhaft_margin_via_aktienkauf():
    """User mit USD-Margin: EUR eingezahlt, USD-Aktien gekauft (USD-Saldo geht ins Minus).
    Spaeter Aktien verkauft, USD-Saldo wieder auf 0.

    Events (USD):
    BUY -50000 @ 1.10 (Aktienkauf USD-Outflow) → balance -50000, Schuld
    DIV +500 @ 1.08 (Dividenden-Zufluss) → tilgt 500 von Schuld, kein Lot, balance -49500
    FRTAX -75 @ 1.08 (Quellensteuer) → balance -49575, alles aus Schuld, kein PnL
    SELL +50000 @ 1.05 (Aktienverkauf USD-Inflow) → tilgt restliche Schuld, lot_amount=425, balance 425
    FRTAX -100 @ 1.20 (spaeter Quellensteuer) → balance 325, from_credit=100, PnL=100*(1.20-1.05)=15

    Erwartung corrected: gain=15, loss=0.
    Erwartung raw: viel mehr (sieht die Aktien-Trades nicht, baut komplett falsche Lots).
    """
    fx_tx = [
        make_tx("2025-01-15", -50000.0, 1.10, "BUY", desc="STK BUY"),
        make_tx("2025-03-15", 500.0, 1.08, "DIV"),
        make_tx("2025-03-15", -75.0, 1.08, "FRTAX"),
        make_tx("2025-06-15", 50000.0, 1.05, "SELL", desc="STK SELL"),
        make_tx("2025-09-15", -100.0, 1.20, "FRTAX"),
    ]
    results, _, _, _ = calculate_fx_gains([], fx_tx, TAX_YEAR)
    usd = results.get("USD", {})
    assert approx(usd.get("gain", 0), 15.0), f"TC2 gain: erwartet 15, ist {usd.get('gain')}"
    assert approx(usd.get("loss", 0), 0.0), f"TC2 loss: erwartet 0, ist {usd.get('loss')}"
    assert usd["days_negative"] > 0, "TC2 sollte negative Tage haben"
    print(f"TC2 OK — corrected: gain={usd['gain']:.2f}, loss={usd['loss']:.2f}, "
          f"raw_net={usd['raw_net']:.2f}, neg_days={usd['days_negative']}")


def tc3_voll_im_plus():
    """Strukturell positiver Saldo: corrected und raw muessen IDENTISCH sein
    (sonst hat die Engine die alte Logik zerstoert)."""
    fx_tx = [
        starting_balance_tx("2025-01-01", 5000.0, 1.10),
        make_tx("2025-02-01", 1000.0, 1.12, "DIV"),
        make_tx("2025-05-01", -800.0, 1.08, "FRTAX"),
        make_tx("2025-07-01", 200.0, 1.06, "CINT"),
        make_tx("2025-10-01", -1500.0, 1.04, "FRTAX"),
    ]
    results, _, _, _ = calculate_fx_gains([], fx_tx, TAX_YEAR)
    usd = results["USD"]
    assert approx(usd["gain"], usd["raw_gain"]), \
        f"TC3 gain Mismatch: corrected={usd['gain']:.4f}, raw={usd['raw_gain']:.4f}"
    assert approx(usd["loss"], usd["raw_loss"]), \
        f"TC3 loss Mismatch: corrected={usd['loss']:.4f}, raw={usd['raw_loss']:.4f}"
    assert usd["days_negative"] == 0, f"TC3 days_negative={usd['days_negative']}, erwartet 0"
    print(f"TC3 OK — corrected==raw: gain={usd['gain']:.2f}, loss={usd['loss']:.2f}")


def tc4_negative_starting_balance():
    """Vorjahr endete bei -5000 USD. Steuerjahr beginnt mit Schuld.

    +6000 USD @ 1.05 → tilgt 5000, Rest 1000 wird Lot@1.05, balance 1000
    -1000 USD @ 1.10 → from_credit=1000, PnL=1000*(1.10-1.05)=50.

    Erwartung corrected: gain=50, loss=0.
    """
    fx_tx = [
        starting_balance_tx("2025-01-01", -5000.0, 1.08),
        make_tx("2025-03-01", 6000.0, 1.05, "DIV"),
        make_tx("2025-06-01", -1000.0, 1.10, "FRTAX"),
    ]
    results, _, _, _ = calculate_fx_gains([], fx_tx, TAX_YEAR)
    usd = results["USD"]
    assert approx(usd["gain"], 50.0), f"TC4 gain: erwartet 50, ist {usd['gain']}"
    assert approx(usd["loss"], 0.0), f"TC4 loss: erwartet 0, ist {usd['loss']}"
    assert usd["days_negative"] > 0, "TC4 negativer Startsaldo muss als Margin-Phase sichtbar sein"
    print(f"TC4 OK — neg Start: gain={usd['gain']:.2f}, loss={usd['loss']:.2f}")


def tc5_dint_auf_schuld():
    """Saldo bei -10000 USD, DINT bucht -50 USD Margin-Zinsen → Saldo -10050,
    aber kein FX-PnL, weil Schuldzinsen keine Veraeusserung sind.

    Erwartung corrected: gain=loss=0, days_negative > 0.
    """
    fx_tx = [
        starting_balance_tx("2025-01-01", -10000.0, 1.10),
        make_tx("2025-03-01", -50.0, 1.05, "DINT"),
        make_tx("2025-06-01", -75.0, 1.08, "DINT"),
    ]
    results, _, _, _ = calculate_fx_gains([], fx_tx, TAX_YEAR)
    # Bei rein negativen Saldo OHNE PnL-Events ist USD evtl. gar nicht in results.
    usd = results.get("USD", {"gain": 0.0, "loss": 0.0, "days_negative": 0})
    assert approx(usd["gain"], 0.0), f"TC5 gain: erwartet 0, ist {usd['gain']}"
    assert approx(usd["loss"], 0.0), f"TC5 loss: erwartet 0, ist {usd['loss']}"
    print(f"TC5 OK — DINT auf Schuld: gain={usd['gain']:.2f}, loss={usd['loss']:.2f}")


def tc6_stale_lot_bug():
    """Kern-Bug: Start +1000, BUY -1000 (skipped in alter Logik), DIV +100, FEE -100.

    Alte Logik (raw): BUY wird ignoriert, der +1000-Lot bleibt liegen.
    FIFO konsumiert beim FEE den 1000-Lot zum Starting-FX → falscher PnL.

    Neue Logik (corrected): BUY konsumiert den 1000-Lot ohne PnL. FEE konsumiert
    den 100-Lot vom DIV → PnL = 100 * (fx_fee - fx_div).

    Events:
    Start +1000 @ 1.10 → Lot[1000@1.10]
    BUY -1000 @ 1.05 → Lot[1000@1.10] wird konsumiert ohne PnL (allow_pnl=False),
        lots leer, balance=0
    DIV +100 @ 1.20 → Lot[100@1.20], balance=100
    FEE -100 @ 1.15 → PnL = 100*(1.15-1.20)=-5

    Erwartung corrected: gain=0, loss=-5.
    Erwartung raw: gain = 100*(1.15-1.10)=5 (falsch, weil BUY ignoriert).
    """
    fx_tx = [
        starting_balance_tx("2025-01-01", 1000.0, 1.10),
        make_tx("2025-02-01", -1000.0, 1.05, "BUY", desc="STK BUY"),
        make_tx("2025-05-01", 100.0, 1.20, "DIV"),
        make_tx("2025-08-01", -100.0, 1.15, "OFEE"),
    ]
    results, _, _, _ = calculate_fx_gains([], fx_tx, TAX_YEAR)
    usd = results["USD"]
    assert approx(usd["gain"], 0.0), f"TC6 gain corrected: erwartet 0, ist {usd['gain']}"
    assert approx(usd["loss"], -5.0), f"TC6 loss corrected: erwartet -5, ist {usd['loss']}"
    # Raw zeigt den Bug-Wert
    assert approx(usd["raw_gain"], 5.0), \
        f"TC6 raw_gain: erwartet 5 (alte Logik), ist {usd['raw_gain']}"
    print(f"TC6 OK — Stale-Lot-Schutz: corrected loss={usd['loss']:.2f}, "
          f"raw gain={usd['raw_gain']:.2f} (bug)")


def tc7_engine_unit_init_negative():
    """Engine-Unit-Test: Negative Starting Balance erzeugt KEINEN Lot."""
    state = _init_fx_state(-5000.0, "2025-01-01", 1.10)
    assert state["balance"] == -5000.0
    assert len(state["lots_corrected"]) == 0
    assert len(state["lots_raw"]) == 0
    print("TC7 OK — Engine init: negative SB ohne Lot")


def tc8_engine_unit_zufluss_tilgt_teilweise():
    """Engine-Unit: Zufluss tilgt Schuld teilweise, Rest wird Lot."""
    state = _init_fx_state(-200.0, "2025-01-01", 1.10)
    _process_fx_event(state, "2025-02-01", 500.0, 1.05, "DIV", TAX_YEAR)
    assert state["balance"] == 300.0
    assert len(state["lots_corrected"]) == 1
    lot = state["lots_corrected"][0]
    assert approx(lot[1], 300.0), f"Lot qty: {lot[1]}"
    assert approx(lot[2], 1.05), f"Lot rate: {lot[2]}"
    print(f"TC8 OK — Zufluss tilgt teilweise: Lot[{lot[1]:.2f}@{lot[2]:.2f}]")


def tc9_positive_sb_ohne_rate():
    """P2-1: Positive Starting Balance ohne brauchbare Rate (fxRateToBase=1.0,
    keine trades.csv-Daten) darf KEINEN Lot zu fx=1.0 seeden — sonst entsteht
    Phantom-PnL bei späteren Abflüssen.

    Korrekt: Der Anfangsbestand bleibt als unbewerteter FIFO-Lot erhalten. Er
    blockiert jüngere Lots in der FIFO-Reihenfolge, erzeugt aber keinen PnL.
    """
    state = _init_fx_state(5000.0, "2025-01-01", 0.0)  # fx=0 -> unbewerteter Lot
    assert state["balance"] == 5000.0
    assert len(state["lots_corrected"]) == 1, "Unbewerteter FIFO-Lot muss erhalten bleiben"
    assert state["lots_corrected"][0][2] is None, "Fehlende Rate muss als None markiert sein"
    # Späterer Abfluss aus diesem unbewerteten Guthaben darf keinen PnL erzeugen
    _process_fx_event(state, "2025-06-01", -1000.0, 1.05, "FRTAX", TAX_YEAR)
    assert state["gain_corrected"] == 0.0, "Phantom-Gain darf nicht entstehen"
    assert state["loss_corrected"] == 0.0, "Phantom-Loss darf nicht entstehen"
    assert approx(state["lots_corrected"][0][1], 4000.0), "Unbewerteter Lot muss FIFO-konsumiert werden"
    print("TC9 OK — Positive SB ohne Rate: unbewerteter Lot, kein Phantom-PnL")


def tc10_same_sign_match():
    """P2-3: Same-Date-Inflow gleicher Größe darf NICHT auf einen Outflow matchen.

    Szenario: An einem Tag werden DIV +500 und FRTAX -500 gebucht.
    Wenn der Match auf |amount|-basierend liefe, würde DIV als prev-balance-Quelle
    für FRTAX gewählt — falsche prev-balance.

    Engine-Test: Saldo +1000, DIV +500 → 1500, FRTAX -500 → 1000.
    Der FX-PnL der FRTAX bezieht sich auf den Saldo NACH DIV (= 1500), nicht
    auf den Saldo vor DIV (= 1000).
    """
    state = _init_fx_state(1000.0, "2025-01-01", 1.10)
    _process_fx_event(state, "2025-03-15", 500.0, 1.12, "DIV", TAX_YEAR)
    assert state["balance"] == 1500.0
    _process_fx_event(state, "2025-03-15", -500.0, 1.15, "FRTAX", TAX_YEAR)
    assert state["balance"] == 1000.0
    # FIFO: FRTAX konsumiert den SB-Lot (1000@1.10) zuerst → 500 @ 1.10
    # PnL = 500 × (1.15 - 1.10) = 25
    assert approx(state["gain_corrected"], 25.0), \
        f"TC10 gain corrected: erwartet 25, ist {state['gain_corrected']}"
    print(f"TC10 OK — Same-sign-Match: gain={state['gain_corrected']:.2f} "
          f"(FRTAX konsumiert SB-Lot, nicht DIV-Lot)")


def tc11_multi_currency_consumed_scoping():
    """P2-2: Bei Multi-Currency dürfen sich consumed-Sets verschiedener Currencies
    nicht überlagern. Test prüft, dass _lookup_balance_before_event den
    Per-Currency-Scope korrekt nutzt.
    """
    # Wir testen die High-Level-Verarbeitung über calculate_fx_gains für zwei Currencies
    # mit jeweils einem Saldo-Korrektur-relevanten Event.
    fx_tx = [
        # USD: kommt ins Minus, dann Tilgung
        make_tx("2025-01-10", -2000.0, 1.10, "BUY", currency="USD"),
        make_tx("2025-03-10", 1500.0, 1.05, "DIV", currency="USD"),
        # JPY: parallel mit eigener Margin-Phase
        make_tx("2025-01-15", -1000.0, 0.0065, "BUY", currency="JPY"),
        make_tx("2025-03-15", 800.0, 0.0062, "DIV", currency="JPY"),
    ]
    results, _, _, _ = calculate_fx_gains([], fx_tx, TAX_YEAR)
    # Beide Currencies müssen days_negative > 0 zeigen (Beweis: Korrektur greift unabhängig)
    usd_neg = results.get("USD", {}).get("days_negative", 0)
    jpy_neg = results.get("JPY", {}).get("days_negative", 0)
    assert usd_neg > 0, f"TC11 USD days_negative={usd_neg}, erwartet > 0"
    assert jpy_neg > 0, f"TC11 JPY days_negative={jpy_neg}, erwartet > 0"
    print(f"TC11 OK — Multi-Currency: USD neg_days={usd_neg}, JPY neg_days={jpy_neg}")


def tc12_sort_key_mixed_txid():
    """P2-4: Sort-key darf bei mixed-type transactionID nicht crashen."""
    # Events mit gemischten txid-Typen: numerisch, leer, non-numeric
    fx_tx = [
        make_tx("2025-03-15", 100.0, 1.10, "DIV", txid="12345"),
        make_tx("2025-03-15", -50.0, 1.12, "FRTAX", txid=""),
        make_tx("2025-03-15", 200.0, 1.09, "DIV", txid="ADJ-001"),  # non-numeric
    ]
    # Darf nicht crashen
    results, _, _, _ = calculate_fx_gains([], fx_tx, TAX_YEAR)
    print(f"TC12 OK — Mixed txid sort: kein Crash, USD result: {results.get('USD', {}).get('net', 0):.2f}")


def tc13_missing_rate_event_tracks_balance():
    """Events ohne brauchbare Rate müssen den Saldo trotzdem bewegen.

    Vor Fix wurde BUY -1000 mit fx=0 komplett übersprungen. Danach hätte DIV +1000
    einen steuerbaren Lot erzeugt und FRTAX -100 fälschlich PnL gebucht. Korrekt:
    BUY baut Schuld auf, DIV tilgt sie, FRTAX kommt aus Saldo 0/Schuld.
    """
    fx_tx = [
        make_tx("2025-01-10", -1000.0, 0.0, "BUY"),
        make_tx("2025-02-10", 1000.0, 1.20, "DIV"),
        make_tx("2025-03-10", -100.0, 1.30, "FRTAX"),
    ]
    results, _, _, _ = calculate_fx_gains([], fx_tx, TAX_YEAR)
    usd = results.get("USD", {})
    assert usd.get("days_negative", 0) > 0, "Missing-rate BUY muss negative Tage erzeugen"
    assert approx(usd.get("gain", 0.0), 0.0), f"TC13 gain: erwartet 0, ist {usd.get('gain')}"
    assert approx(usd.get("loss", 0.0), 0.0), f"TC13 loss: erwartet 0, ist {usd.get('loss')}"
    print(f"TC13 OK — Missing-rate Event trackt Saldo: neg_days={usd['days_negative']}")


def tc14_unknown_starting_lot_preserves_fifo_order():
    """Unbewerteter Starting Balance darf jüngere bewertete Lots nicht überspringen.

    Start +1000 ohne Rate, danach DIV +100 @1.20 und FRTAX -100 @1.30. FIFO
    konsumiert zuerst den unbewerteten Start-Lot, deshalb kein PnL.
    """
    state = _init_fx_state(1000.0, "2025-01-01", 0.0)
    _process_fx_event(state, "2025-02-01", 100.0, 1.20, "DIV", TAX_YEAR)
    _process_fx_event(state, "2025-03-01", -100.0, 1.30, "FRTAX", TAX_YEAR)
    assert approx(state["gain_corrected"], 0.0), f"TC14 gain: erwartet 0, ist {state['gain_corrected']}"
    assert approx(state["loss_corrected"], 0.0), f"TC14 loss: erwartet 0, ist {state['loss_corrected']}"
    assert len(state["lots_corrected"]) == 2, "Unbewerteter Restlot und DIV-Lot müssen erhalten sein"
    assert approx(state["lots_corrected"][0][1], 900.0), "FIFO muss zuerst den unbewerteten Start-Lot konsumieren"
    assert state["lots_corrected"][0][2] is None, "Erster Restlot bleibt unbewertet"
    assert approx(state["lots_corrected"][1][1], 100.0), "Jüngerer DIV-Lot darf nicht vorgezogen werden"
    print("TC14 OK — Unbewerteter Start-Lot bewahrt FIFO-Reihenfolge")


def tc15_blank_activity_code_consumes_without_pnl():
    """Leerer activityCode gehoert wie bisher zu den Skip-Codes.

    Der Saldo/Lot-Bestand muss trotzdem laufen, aber der Abfluss darf keinen
    steuerlichen FX-PnL buchen.
    """
    state = _init_fx_state(1000.0, "2025-01-01", 1.10)
    _process_fx_event(state, "2025-04-01", -100.0, 1.20, "", TAX_YEAR)
    assert approx(state["gain_corrected"], 0.0), f"TC15 gain: erwartet 0, ist {state['gain_corrected']}"
    assert approx(state["loss_corrected"], 0.0), f"TC15 loss: erwartet 0, ist {state['loss_corrected']}"
    assert approx(state["lots_corrected"][0][1], 900.0), "Blank-Code Event muss den Lot konsumieren"
    print("TC15 OK — Leerer activityCode konsumiert Lot ohne PnL")


def tc16_option_b_skips_csv_when_starting_balance_negative():
    """CSV-Fallback darf bei negativem Starting Balance nicht gewinnen.

    Wenn der erste Steuerjahr-Event eine Schuld direkt ins Plus tilgt, muss die
    Margin-Phase trotzdem erkannt werden. Sonst wuerde Option B den aggregierten
    IBKR-CSV-Rohwert uebernehmen, obwohl Option C saldokorrigiert rechnen muss.
    """
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "EUR", "tax_year": str(TAX_YEAR), "fx_transactions_count": "0"}
        ])
        write_csv(os.path.join(tmp, "fx_transactions.csv"), [
            starting_balance_tx("2025-01-01", -5000.0, 1.08),
            make_tx("2025-03-01", 6000.0, 1.05, "DIV"),
            make_tx("2025-06-01", -1000.0, 1.10, "FRTAX"),
        ])
        csv_path = os.path.join(tmp, "ibkr_report.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("Übersicht  zur realisierten und unrealisierten Performance,Data,Devisen,USD,,999,0,0,0,999\n")

        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(tmp, tax_year=TAX_YEAR, fx_csv_path=csv_path)

    assert report["fx_source"] == "fifo", f"TC16 fx_source: erwartet fifo, ist {report['fx_source']}"
    assert report["fx_has_negative_balance"] is True, "TC16 muss negative Balance erkennen"
    assert approx(report["fx_total_gain"], 50.0), f"TC16 gain: erwartet 50, ist {report['fx_total_gain']}"
    print("TC16 OK — Option B ueberspringt CSV bei negativem Startsaldo")


def tc17_negative_days_are_calendar_days():
    """Negative Tage muessen Kalendertage sein, nicht nur Buchungstage."""
    fx_tx = [
        make_tx("2025-01-01", -1000.0, 1.10, "BUY"),
        make_tx("2025-03-01", 1000.0, 1.05, "DIV"),
    ]
    results, _, _, _ = calculate_fx_gains([], fx_tx, TAX_YEAR)
    usd = results["USD"]
    assert usd["days_negative"] == 60, \
        f"TC17 days_negative: erwartet 60 Kalendertage, ist {usd['days_negative']}"
    print("TC17 OK — Negative-Tage sind Kalendertage, nicht Buchungstage")


def tc18_option_a_without_cash_timeline():
    """XML-FX-PnL darf auch ohne fx_transactions.csv nicht crashen."""
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "EUR", "tax_year": str(TAX_YEAR), "fx_transactions_count": "0"}
        ])
        write_csv(os.path.join(tmp, "fx_realized_pnl.csv"), [
            {
                "reportDate": "2025-06-01",
                "dateTime": "2025-06-01 10:00:00",
                "functionalCurrency": "EUR",
                "fxCurrency": "USD",
                "activityDescription": "TEST",
                "quantity": "-100",
                "proceeds": "110",
                "cost": "-100",
                "realizedPL": "10",
                "code": "C",
                "levelOfDetail": "TRANSACTION",
            }
        ])

        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(tmp, tax_year=TAX_YEAR)

    assert report["fx_source"] == "xml", f"TC18 fx_source: erwartet xml, ist {report['fx_source']}"
    assert approx(report["fx_total_gain"], 10.0), f"TC18 gain: erwartet 10, ist {report['fx_total_gain']}"
    print("TC18 OK — Option A laeuft ohne Cash-Timeline")


def tc19_option_a_keeps_negative_currency_without_pnl():
    """Negative Saldo-Waehrungen ohne PnL-Zeile muessen fuer die UI erhalten bleiben."""
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "EUR", "tax_year": str(TAX_YEAR), "fx_transactions_count": "0"}
        ])
        write_csv(os.path.join(tmp, "fx_realized_pnl.csv"), [
            {
                "reportDate": "2025-06-01",
                "dateTime": "2025-06-01 10:00:00",
                "functionalCurrency": "EUR",
                "fxCurrency": "USD",
                "activityDescription": "TEST",
                "quantity": "-100",
                "proceeds": "110",
                "cost": "-100",
                "realizedPL": "10",
                "code": "C",
                "levelOfDetail": "TRANSACTION",
            }
        ])
        write_csv(os.path.join(tmp, "fx_transactions.csv"), [
            make_tx("2025-01-01", -1000.0, 0.006, "BUY", currency="JPY"),
            make_tx("2025-02-01", 1000.0, 0.006, "SELL", currency="JPY"),
        ])

        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(tmp, tax_year=TAX_YEAR)

    jpy = report["fx_results"].get("JPY")
    assert jpy is not None, "TC19 JPY mit negativer Margin-Phase muss in fx_results stehen"
    assert jpy["days_negative"] == 32, f"TC19 JPY days_negative: erwartet 32, ist {jpy['days_negative']}"
    assert approx(jpy["net"], 0.0), f"TC19 JPY net: erwartet 0, ist {jpy['net']}"
    print("TC19 OK — Option A zeigt negative Waehrung ohne PnL")


def tc20_option_a_opt_out_uses_xml_raw_value():
    """Opt-out muss bei XML FxTransactions den IBKR-Rohwert in Topf 2 uebernehmen.

    Die Buchung ist ein Zufluss mit Ergebnis, also eine Schuldtilgung: korrigiert
    waere sie 0. Mit deaktivierter Korrektur muss der IBKR-Rohwert 10 aktiv sein.
    """
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "EUR", "tax_year": str(TAX_YEAR), "fx_transactions_count": "1"}
        ])
        write_csv(os.path.join(tmp, "fx_transactions.csv"), [
            make_tx("2025-06-01", 100.0, 1.10, "SELL", txid="1001", balance=-500.0),
        ])
        write_csv(os.path.join(tmp, "fx_realized_pnl.csv"), [
            {
                "reportDate": "2025-06-01",
                "dateTime": "2025-06-01 10:00:00",
                "functionalCurrency": "EUR",
                "fxCurrency": "USD",
                "activityDescription": "TEST",
                "quantity": "100",
                "proceeds": "110",
                "cost": "-100",
                "realizedPL": "10",
                "code": "C",
                "levelOfDetail": "TRANSACTION",
            }
        ])

        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(
                tmp,
                tax_year=TAX_YEAR,
                fx_margin_correction_enabled=False,
            )

    usd = report["fx_results"]["USD"]
    assert report["fx_source"] == "xml", f"TC20 fx_source: erwartet xml, ist {report['fx_source']}"
    assert report["fx_margin_correction_enabled"] is False, "TC20 Opt-out Flag fehlt"
    assert approx(report["fx_total_gain"], 10.0), f"TC20 active gain: erwartet 10, ist {report['fx_total_gain']}"
    assert approx(usd["net"], 10.0), f"TC20 active net: erwartet 10, ist {usd['net']}"
    assert approx(usd["raw_net"], 10.0), f"TC20 raw_net: erwartet 10, ist {usd['raw_net']}"
    assert approx(usd["corrected_net"], 0.0), f"TC20 corrected_net: erwartet 0, ist {usd['corrected_net']}"
    print("TC20 OK — Opt-out uebernimmt XML-Rohwert, behält korrigierten Vergleich")


def tc21_option_b_opt_out_uses_csv_raw_despite_negative_balance():
    """Opt-out darf den aggregierten IBKR-CSV-Rohwert trotz negativem Saldo nutzen."""
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "EUR", "tax_year": str(TAX_YEAR), "fx_transactions_count": "0"}
        ])
        write_csv(os.path.join(tmp, "fx_transactions.csv"), [
            starting_balance_tx("2025-01-01", -5000.0, 1.08),
            make_tx("2025-03-01", 6000.0, 1.05, "DIV"),
            make_tx("2025-06-01", -1000.0, 1.10, "FRTAX"),
        ])
        csv_path = os.path.join(tmp, "ibkr_report.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("Übersicht  zur realisierten und unrealisierten Performance,Data,Devisen,USD,,999,0,0,0,999\n")

        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(
                tmp,
                tax_year=TAX_YEAR,
                fx_csv_path=csv_path,
                fx_margin_correction_enabled=False,
            )

    assert report["fx_source"] == "csv", f"TC21 fx_source: erwartet csv, ist {report['fx_source']}"
    assert report["fx_has_negative_balance"] is True, "TC21 muss negative Balance weiter anzeigen"
    assert approx(report["fx_total_gain"], 999.0), f"TC21 gain: erwartet 999, ist {report['fx_total_gain']}"
    assert report["fx_option_a_meta"].get("csv_raw_only") is True, "TC21 CSV-Raw-Marker fehlt"
    print("TC21 OK — Opt-out laesst CSV-Rohwert trotz negativer Balance zu")


def tc22_option_c_opt_out_uses_fifo_raw_path():
    """Opt-out muss bei FIFO den alten Rohpfad statt corrected Topf-2 nutzen."""
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "EUR", "tax_year": str(TAX_YEAR), "fx_transactions_count": "3"}
        ])
        write_csv(os.path.join(tmp, "fx_transactions.csv"), [
            starting_balance_tx("2025-01-01", 1000.0, 1.10),
            make_tx("2025-02-01", -1000.0, 1.10, "BUY"),
            make_tx("2025-03-01", 100.0, 1.20, "DIV"),
            make_tx("2025-04-01", -100.0, 1.15, "FRTAX"),
        ])

        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(
                tmp,
                tax_year=TAX_YEAR,
                fx_margin_correction_enabled=False,
            )

    usd = report["fx_results"]["USD"]
    assert report["fx_source"] == "fifo", f"TC22 fx_source: erwartet fifo, ist {report['fx_source']}"
    assert approx(usd["corrected_net"], -5.0), f"TC22 corrected_net: erwartet -5, ist {usd['corrected_net']}"
    assert approx(usd["raw_net"], 5.0), f"TC22 raw_net: erwartet 5, ist {usd['raw_net']}"
    assert approx(usd["net"], 5.0), f"TC22 active net: erwartet 5, ist {usd['net']}"
    assert approx(report["fx_total_gain"], 5.0), f"TC22 active gain: erwartet 5, ist {report['fx_total_gain']}"
    assert approx(report["fx_total_loss"], 0.0), f"TC22 active loss: erwartet 0, ist {report['fx_total_loss']}"
    print("TC22 OK — Opt-out nutzt FIFO-Rohpfad statt Saldo-Korrektur")


# --- Issue #84: Schuldtilgungs-Gate (Vorzeichen statt Saldo-Rekonstruktion) ---


def fx_pnl_row(date, quantity, realized_pl, *, code="C", desc="TEST", currency="USD"):
    """Hilfsfunktion fuer fx_realized_pnl-Zeilen (FxTransactions levelOfDetail=TRANSACTION)."""
    return {
        "reportDate": date,
        "dateTime": f"{date} 10:00:00",
        "functionalCurrency": "EUR",
        "fxCurrency": currency,
        "activityDescription": desc,
        "quantity": str(quantity),
        "proceeds": "0",
        "cost": "0",
        "realizedPL": str(realized_pl),
        "code": code,
        "levelOfDetail": "TRANSACTION",
    }


def tc23_debt_repayment_unit():
    """is_fx_debt_repayment prueft Vorzeichen UND IBKRs Closing-Code."""
    from calculate_tax_report import is_fx_debt_repayment, is_fx_closing_row

    assert is_fx_debt_repayment(500.0, -12.0, "C") is True, "TC23 Zufluss + Close = Short-Close"
    assert is_fx_debt_repayment(500.0, 12.0, "C") is True, "TC23 Zufluss mit Gewinn ebenfalls"
    assert is_fx_debt_repayment(500.0, -12.0, "C;O") is True, "TC23 C;O schliesst ebenfalls"
    assert is_fx_debt_repayment(-500.0, -12.0, "C") is False, "TC23 Abfluss = Long-Close, steuerbar"
    assert is_fx_debt_repayment(500.0, 0.0, "O") is False, "TC23 Zufluss ohne PnL ist ein Open"
    assert is_fx_debt_repayment(500.0, 0.0005, "C") is False, "TC23 Rauschen unter 0,001 zaehlt nicht"
    # Opening mit Ergebnis widerspricht IBKRs Konvention -> nicht still als Tilgung werten
    assert is_fx_debt_repayment(500.0, -12.0, "O") is False, \
        "TC23 code='O' darf nicht als Schuldtilgung durchgehen"
    # Fehlender Code (aeltere Extraktionen): Vorzeichen entscheidet wie bisher
    assert is_fx_debt_repayment(500.0, -12.0, "") is True, "TC23 ohne Code faellt auf Vorzeichen zurueck"

    assert is_fx_closing_row("C") is True
    assert is_fx_closing_row("C;O") is True
    assert is_fx_closing_row("o;c") is True, "TC23 Code-Parsing ist case- und reihenfolgeunabhaengig"
    assert is_fx_closing_row("O") is False
    assert is_fx_closing_row("") is True, "TC23 leerer Code gilt als Closing (Rueckfall)"
    print("TC23 OK — is_fx_debt_repayment prueft Vorzeichen und Closing-Code")


def tc29_opening_row_with_pnl_is_reported():
    """code='O' mit Ergebnis wird als Anomalie gemeldet, nicht still gefiltert."""
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "EUR", "tax_year": str(TAX_YEAR), "fx_transactions_count": "0"}
        ])
        write_csv(os.path.join(tmp, "fx_realized_pnl.csv"), [
            fx_pnl_row("2025-06-01", 2000.0, -50.0, code="O", desc="STK: -100 ACME"),
            fx_pnl_row("2025-06-03", -800.0, -20.0, code="C", desc="STK: 40 ACME"),
        ])

        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(tmp, tax_year=TAX_YEAR)

    meta = report["fx_option_a_meta"]
    usd = report["fx_results"]["USD"]
    assert len(meta["open_rows_with_pnl"]) == 1, \
        f"TC29 Anomalie muss gemeldet werden, sind {len(meta['open_rows_with_pnl'])}"
    assert meta["open_rows_with_pnl"][0]["code"] == "O"
    assert meta["debt_repayments"] == 0, "TC29 Opening-Zeile zaehlt nicht als Tilgung"
    assert approx(usd["net"], -70.0), \
        f"TC29 Anomalie bleibt steuerbar (konservativ), net ist {usd['net']}"
    print("TC29 OK — Opening-Zeile mit Ergebnis wird zum Prueffall statt still gefiltert")


def tc24_inflow_realization_is_not_taxable():
    """Zufluss-Realisierung (Schuldtilgung) darf nicht in Topf 2 landen.

    Kern von Issue #84: IBKR bucht beim Tilgen einer USD-Schuld ein FX-Ergebnis.
    Es fehlt aber das Fremdwaehrungsguthaben als Bezugsobjekt (BMF Rn. 131).
    """
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "EUR", "tax_year": str(TAX_YEAR), "fx_transactions_count": "0"}
        ])
        write_csv(os.path.join(tmp, "fx_realized_pnl.csv"), [
            fx_pnl_row("2025-06-01", 2000.0, -50.0, desc="STK: -100 ACME"),   # Schuldtilgung
            fx_pnl_row("2025-06-02", 1500.0, 30.0, desc="STK: -50 ACME"),     # Schuldtilgung, Gewinn
            fx_pnl_row("2025-06-03", -800.0, -20.0, desc="STK: 40 ACME"),     # Guthaben-Veraeusserung
        ])

        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(tmp, tax_year=TAX_YEAR)

    usd = report["fx_results"]["USD"]
    meta = report["fx_option_a_meta"]
    assert approx(usd["net"], -20.0), f"TC24 net: erwartet -20 (nur Abfluss), ist {usd['net']}"
    assert approx(usd["gain"], 0.0), f"TC24 gain: Zufluss-Gewinn darf nicht zaehlen, ist {usd['gain']}"
    assert approx(usd["raw_net"], -40.0), f"TC24 raw_net: erwartet -40, ist {usd['raw_net']}"
    assert meta["debt_repayments"] == 2, f"TC24 debt_repayments: erwartet 2, ist {meta['debt_repayments']}"
    assert approx(meta["debt_repayment_pnl"], -20.0), \
        f"TC24 debt_repayment_pnl: erwartet -20, ist {meta['debt_repayment_pnl']}"
    print("TC24 OK — Schuldtilgung bleibt aus Topf 2, Guthaben-Veraeusserung nicht")


def tc25_partially_covered_outflow_is_not_scaled():
    """Ein nur teilweise gedeckter Abfluss wird NICHT anteilig gekuerzt.

    IBKR weist auf so einer Buchung bereits allein das Ergebnis des gedeckten
    Teils aus; der neu eroeffnete Short geht zum Tageskurs ein und traegt null bei.
    Die frueher hier angesetzte Skalierung war eine zweite Kuerzung desselben Betrags.
    """
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "EUR", "tax_year": str(TAX_YEAR), "fx_transactions_count": "2"}
        ])
        # Saldo: +300 vor dem Abfluss von -1000 -> nur 30 % gedeckt.
        write_csv(os.path.join(tmp, "fx_transactions.csv"), [
            starting_balance_tx("2025-01-01", 0.0, 1.10),
            make_tx("2025-05-01", 300.0, 1.10, "DIV", txid="1001", balance=300.0),
            make_tx("2025-06-01", -1000.0, 1.05, "BUY", txid="1002", balance=-700.0),
        ])
        write_csv(os.path.join(tmp, "fx_realized_pnl.csv"), [
            fx_pnl_row("2025-06-01", -1000.0, -15.0, code="C;O", desc="STK: 10 ACME"),
        ])

        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(tmp, tax_year=TAX_YEAR)

    usd = report["fx_results"]["USD"]
    assert approx(usd["net"], -15.0), \
        f"TC25 net: erwartet -15 (ungekuerzt), ist {usd['net']}"
    assert report["fx_option_a_meta"]["debt_repayments"] == 0, "TC25 Abfluss ist keine Schuldtilgung"
    print("TC25 OK — teilweise gedeckter Abfluss wird nicht ein zweites Mal gekuerzt")


def tc26_opt_out_keeps_debt_repayment():
    """Opt-out uebernimmt auch die Schuldtilgungs-Zeilen mit IBKR-Rohwert."""
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "EUR", "tax_year": str(TAX_YEAR), "fx_transactions_count": "0"}
        ])
        write_csv(os.path.join(tmp, "fx_realized_pnl.csv"), [
            fx_pnl_row("2025-06-01", 2000.0, -50.0, desc="STK: -100 ACME"),
            fx_pnl_row("2025-06-03", -800.0, -20.0, desc="STK: 40 ACME"),
        ])

        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(tmp, tax_year=TAX_YEAR,
                                   fx_margin_correction_enabled=False)

    usd = report["fx_results"]["USD"]
    assert approx(usd["net"], -70.0), f"TC26 net: erwartet -70 (Rohwert), ist {usd['net']}"
    assert approx(usd["corrected_net"], -20.0), \
        f"TC26 corrected_net muss den Vergleichswert behalten, ist {usd['corrected_net']}"
    assert report["fx_option_a_meta"]["debt_repayments"] == 1, \
        "TC26 Zaehler muss auch im Opt-out gefuellt sein"
    print("TC26 OK — Opt-out uebernimmt Schuldtilgung, behaelt korrigierten Vergleich")


def tc27_margin_days_use_ibkr_balance_column():
    """Margin-Tage kommen aus IBKRs balance-Spalte, nicht aus einer Eigenkumulation.

    Realfall audit2: Bei gemergten Mehrjahres-Exporten driftet eine Kumulation ueber
    `amount` weg, sodass echte Schuldphasen unsichtbar blieben. Hier weicht die
    Kumulation bewusst vom gemeldeten Saldo ab.
    """
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "EUR", "tax_year": str(TAX_YEAR), "fx_transactions_count": "2"}
        ])
        # Kumulation ergaebe 0 -> +100 -> +50 (nie negativ).
        # IBKRs balance sagt: -2000 nach der zweiten Buchung.
        write_csv(os.path.join(tmp, "fx_transactions.csv"), [
            starting_balance_tx("2025-01-01", 0.0, 1.10),
            make_tx("2025-03-01", 100.0, 1.10, "DIV", txid="1001", balance=100.0),
            make_tx("2025-03-11", -50.0, 1.10, "FRTAX", txid="1002", balance=-2000.0),
        ])
        write_csv(os.path.join(tmp, "fx_realized_pnl.csv"), [
            fx_pnl_row("2025-06-01", -800.0, -20.0, desc="STK: 40 ACME"),
        ])

        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(tmp, tax_year=TAX_YEAR)

    assert report["fx_has_negative_balance"] is True, \
        "TC27 gemeldeter Negativsaldo muss erkannt werden"
    assert report["fx_results"]["USD"]["days_negative"] > 0, \
        "TC27 Margin-Tage muessen aus der balance-Spalte kommen"
    print("TC27 OK — Margin-Tage folgen IBKRs balance-Spalte")


def tc28_missing_balance_column_falls_back_to_cumulation():
    """Zeilen ohne balance-Spalte fallen auf die Kumulation zurueck (Alt-Fixtures)."""
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "EUR", "tax_year": str(TAX_YEAR), "fx_transactions_count": "2"}
        ])
        write_csv(os.path.join(tmp, "fx_transactions.csv"), [
            starting_balance_tx("2025-01-01", 0.0, 1.10),
            make_tx("2025-03-01", -500.0, 1.10, "BUY", txid="1001"),   # keine balance
            make_tx("2025-03-21", 500.0, 1.10, "SELL", txid="1002"),   # keine balance
        ])
        write_csv(os.path.join(tmp, "fx_realized_pnl.csv"), [
            fx_pnl_row("2025-06-01", -800.0, -20.0, desc="STK: 40 ACME"),
        ])

        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(tmp, tax_year=TAX_YEAR)

    assert report["fx_has_negative_balance"] is True, \
        "TC28 Fallback-Kumulation muss die Schuldphase weiterhin erkennen"
    assert report["fx_results"]["USD"]["days_negative"] == 21, \
        f"TC28 days_negative: erwartet 21, ist {report['fx_results']['USD']['days_negative']}"
    print("TC28 OK — Fallback auf Kumulation ohne balance-Spalte")


def tc30_incompatible_fx_currency_stops_computation():
    """F2: Weder Teilwerte noch ungepruefter FIFO-Fallback bei falscher Einheit."""
    invalid_rows = [
        dict(fx_pnl_row("2025-06-01", -13000, 1051.17),
             functionalCurrency="USD", fxCurrency="EUR"),
        dict(fx_pnl_row("2025-06-01", -100, 10),
             functionalCurrency="USD", fxCurrency="GBP"),
        dict(fx_pnl_row("2025-06-01", -100, 0),
             functionalCurrency="USD"),
        dict(fx_pnl_row("2025-06-01", -100, 10),
             functionalCurrency=""),
        dict(fx_pnl_row("2025-06-01", -100, 10), fxCurrency="EUR"),
    ]
    for invalid in invalid_rows:
        for correction_enabled in (True, False):
            with tempfile.TemporaryDirectory() as tmp:
                write_csv(os.path.join(tmp, "account_info.csv"), [
                    {"currency": "EUR", "tax_year": str(TAX_YEAR)}
                ])
                write_csv(os.path.join(tmp, "fx_realized_pnl.csv"), [
                    fx_pnl_row("2025-05-01", -100, 10), invalid,
                ])
                # Ein verfuegbarer Option-C-Pfad darf die Sperre nicht umgehen.
                write_csv(os.path.join(tmp, "fx_transactions.csv"), [
                    make_tx("2025-06-01", -100, 1.1, "BUY"),
                ])
                try:
                    with contextlib.redirect_stdout(io.StringIO()):
                        calculate_tax(tmp, tax_year=TAX_YEAR,
                                      fx_margin_correction_enabled=correction_enabled)
                except ValueError as exc:
                    assert "FX-Ergebniswährung" in str(exc), str(exc)
                    assert "functionalCurrency" in str(exc), str(exc)
                else:
                    raise AssertionError(f"F2 muss Berechnung sperren: {invalid}")
    print("TC30 OK — falsche/fehlende FX-Ergebniswaehrung sperrt die Berechnung")


def tc31_fx_currency_gate_only_checks_tax_year():
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "EUR", "tax_year": str(TAX_YEAR)}
        ])
        write_csv(os.path.join(tmp, "fx_realized_pnl.csv"), [
            dict(fx_pnl_row("2024-06-01", -100, 999), functionalCurrency="USD"),
            fx_pnl_row("2025-06-01", -100, 10),
            dict(fx_pnl_row("2025-06-02", -100, -3), fxCurrency="GBP"),
        ])
        with contextlib.redirect_stdout(io.StringIO()):
            report = calculate_tax(tmp, tax_year=TAX_YEAR)
    assert report["fx_source"] == "xml"
    assert approx(report["fx_total_gain"], 10)
    assert approx(report["fx_total_loss"], -3)
    print("TC31 OK — passende Ergebniswaehrung unveraendert, Vorjahr ignoriert")


def tc32_usd_functional_currency_keeps_existing_conversion():
    with tempfile.TemporaryDirectory() as tmp:
        write_csv(os.path.join(tmp, "account_info.csv"), [
            {"currency": "USD", "tax_year": str(TAX_YEAR)}
        ])
        write_csv(os.path.join(tmp, "fx_realized_pnl.csv"), [
            dict(fx_pnl_row("2025-06-02", -100, 10),
                 functionalCurrency="USD", fxCurrency="EUR"),
        ])
        with contextlib.redirect_stdout(io.StringIO()), patch(
                'calculate_tax_report.fetch_ecb_rates',
                return_value={date(2025, 6, 2): 0.8}):
            report = calculate_tax(tmp, tax_year=TAX_YEAR)
    assert report["fx_source"] == "xml"
    assert approx(report["fx_total_gain"], 8)
    print("TC32 OK — passendes USD-Konto behaelt seine USD/EUR-Umrechnung")


def run_all():
    tests = [tc1_margin_tilgung, tc2_dauerhaft_margin_via_aktienkauf,
             tc3_voll_im_plus, tc4_negative_starting_balance,
             tc5_dint_auf_schuld, tc6_stale_lot_bug,
             tc7_engine_unit_init_negative, tc8_engine_unit_zufluss_tilgt_teilweise,
             tc9_positive_sb_ohne_rate, tc10_same_sign_match,
             tc11_multi_currency_consumed_scoping, tc12_sort_key_mixed_txid,
             tc13_missing_rate_event_tracks_balance,
             tc14_unknown_starting_lot_preserves_fifo_order,
             tc15_blank_activity_code_consumes_without_pnl,
             tc16_option_b_skips_csv_when_starting_balance_negative,
             tc17_negative_days_are_calendar_days,
             tc18_option_a_without_cash_timeline,
             tc19_option_a_keeps_negative_currency_without_pnl,
             tc20_option_a_opt_out_uses_xml_raw_value,
             tc21_option_b_opt_out_uses_csv_raw_despite_negative_balance,
             tc22_option_c_opt_out_uses_fifo_raw_path,
             tc23_debt_repayment_unit,
             tc24_inflow_realization_is_not_taxable,
             tc25_partially_covered_outflow_is_not_scaled,
             tc26_opt_out_keeps_debt_repayment,
             tc27_margin_days_use_ibkr_balance_column,
             tc28_missing_balance_column_falls_back_to_cumulation,
             tc29_opening_row_with_pnl_is_reported,
             tc30_incompatible_fx_currency_stops_computation,
             tc31_fx_currency_gate_only_checks_tax_year,
             tc32_usd_functional_currency_keeps_existing_conversion]
    failed = 0
    for tc in tests:
        try:
            tc()
        except AssertionError as e:
            failed += 1
            print(f"FAIL {tc.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"ERROR {tc.__name__}: {type(e).__name__}: {e}")
    if failed:
        print(f"\n{failed}/{len(tests)} Tests fehlgeschlagen.")
        sys.exit(1)
    print(f"\nAlle {len(tests)} FX-Margin-Tests grün.")


if __name__ == "__main__":
    run_all()
