"""Regression tests for KAP-INV withholding-tax reporting."""
import contextlib
import copy
import csv
import io
import math
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_tax_report import (
    build_wht_review_rows,
    calculate_creditable_foreign_tax,
    calculate_tax,
    compare_kap_inv_wht_modes,
    format_german_date,
    get_kap_line_41_for_reporting,
    get_kap_inv_wht_for_reporting,
    get_withholding_tax_for_reporting,
    get_wht_event_status_label,
    merge_kap_inv_wht_for_reporting,
)


def calculate_for_funds(funds, dba_beta=False):
    fieldnames = sorted({k for row in funds for k in row})
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "account_info.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["currency", "tax_year", "fx_transactions_count"])
            writer.writeheader()
            writer.writerow({"currency": "EUR", "tax_year": "2025", "fx_transactions_count": "0"})
        with open(os.path.join(tmp, "statement_of_funds.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(funds)
        with contextlib.redirect_stdout(io.StringIO()):
            return calculate_tax(tmp, dba_wht_beta_enabled=dba_beta)


def test_creditable_tax_uses_caps_instead_of_proportional_tfs_reduction():
    equity_fund = calculate_creditable_foreign_tax(1000, 150, 0, 0.30, 0.15)
    assert round(equity_fund["taxable_distribution_eur"], 2) == 700.00
    assert round(equity_fund["german_cap_eur"], 2) == 175.00
    assert round(equity_fund["creditable_tax_eur"], 2) == 150.00
    assert equity_fund["status"] == "matched"

    excessive_us_tax = calculate_creditable_foreign_tax(1000, 300, 0, 0.30, 0.15)
    assert round(excessive_us_tax["creditable_tax_eur"], 2) == 150.00
    assert round(excessive_us_tax["excess_tax_eur"], 2) == 150.00
    assert excessive_us_tax["status"] == "capped_review_refund"

    foreign_property_fund = calculate_creditable_foreign_tax(
        10000, 1500, 0, 0.80, 0.15
    )
    assert round(foreign_property_fund["german_cap_eur"], 2) == 500.00
    assert round(foreign_property_fund["creditable_tax_eur"], 2) == 500.00

    other_fund = calculate_creditable_foreign_tax(1000, 150, 0, 0.0, 0.15)
    assert round(other_fund["creditable_tax_eur"], 2) == 150.00

    unmatched_withholding = calculate_creditable_foreign_tax(
        0, 25, 0, 0.30, 0.15
    )
    assert unmatched_withholding["creditable_tax_eur"] == 0.0
    assert unmatched_withholding["status"] == "unmatched_withholding"
    assert unmatched_withholding["review_required"]

    # Cent-/FX-Rundungstoleranz: 15%-Einbehalt in USD liegt nach
    # EUR-Umrechnung Zehntel-Cents ueber gross_EUR x 0.15 -> kein Kappen
    rounding_noise = calculate_creditable_foreign_tax(99.37, 14.908, 0, 0.0, 0.15)
    assert round(rounding_noise["creditable_tax_eur"], 4) == 14.908
    assert rounding_noise["status"] == "matched"
    # Echte Ueberschreitung (> 2 Cent) kappt weiterhin
    real_breach = calculate_creditable_foreign_tax(100, 15.10, 0, 0.0, 0.15)
    assert round(real_breach["creditable_tax_eur"], 2) == 15.00


def test_kap_inv_wht_anrechenbar_uses_event_cap():
    rd = calculate_for_funds([
        {
            "activityCode": "DIV",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "1000",
            "currency": "EUR",
            "subCategory": "ETF",
            "isin": "US78462F1030",
            "symbol": "SPY",
        },
        {
            "activityCode": "WHT",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "-150",
            "currency": "EUR",
            "subCategory": "ETF",
            "isin": "US78462F1030",
            "symbol": "SPY",
        },
    ], dba_beta=True)
    kap_inv = rd["kap_inv"]
    assert round(kap_inv["etf_wht_eur"], 2) == 150.00
    assert round(kap_inv["etf_wht_anrechenbar_eur"], 2) == 150.00
    assert round(get_kap_inv_wht_for_reporting(kap_inv), 2) == 150.00
    assert round(rd["zeile_41_withholding_tax_eur"], 2) == 150.00


def test_kap_inv_wht_reporting_falls_back_for_legacy_data():
    assert get_kap_inv_wht_for_reporting({"etf_wht_eur": 150.0}) == 150.0


def test_dba_beta_is_opt_in_and_legacy_mode_is_default():
    rows = [
        {
            "activityCode": "DIV",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "1000",
            "currency": "EUR",
            "subCategory": "ETF",
            "isin": "US78462F1030",
            "symbol": "SPY",
        },
        {
            "activityCode": "WHT",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "-300",
            "currency": "EUR",
            "subCategory": "ETF",
            "isin": "US78462F1030",
            "symbol": "SPY",
        },
    ]

    stable = calculate_for_funds(rows)
    assert stable["dba_wht_beta_enabled"] is False
    # Vor-DBA-Verhalten: 300 Rohsteuer × (1 - 30% TFS) = 210.
    assert round(stable["kap_inv"]["etf_wht_anrechenbar_eur"], 2) == 210.00
    assert stable["kap_inv"]["wht_events"] == []
    assert stable["kap_inv"]["wht_review_items"] == []

    beta = calculate_for_funds(rows, dba_beta=True)
    assert beta["dba_wht_beta_enabled"] is True
    assert round(beta["kap_inv"]["etf_wht_anrechenbar_eur"], 2) == 150.00
    assert len(beta["kap_inv"]["wht_events"]) == 1


def test_line_41_invstg_toggle_replaces_fund_tax_without_double_counting():
    report = {
        "zeile_41_withholding_tax_eur": 160.0,
        "withholding_tax_eur": 10.0,
        "kap_inv": {
            "etf_wht_eur": 300.0,
            "etf_wht_anrechenbar_eur": 150.0,
        },
    }
    assert get_kap_line_41_for_reporting(report, True) == 160.0
    assert get_kap_line_41_for_reporting(report, False) == 310.0

    legacy_report = {
        "withholding_tax_eur": 10.0,
        "kap_inv": {"etf_wht_eur": 150.0},
    }
    assert get_kap_line_41_for_reporting(legacy_report, True) == 160.0
    assert get_kap_line_41_for_reporting(legacy_report, False) == 160.0


def test_multi_account_sums_finished_credits_without_global_recap():
    account_one = calculate_creditable_foreign_tax(1000, 300, 0, 0.30, 0.15)
    account_two = calculate_creditable_foreign_tax(1000, 0, 0, 0.30, 0.15)
    merged = merge_kap_inv_wht_for_reporting([
        {"etf_wht_anrechenbar_eur": account_one["creditable_tax_eur"]},
        {"etf_wht_anrechenbar_eur": account_two["creditable_tax_eur"]},
    ])
    assert merged == 150.0
    globally_recalculated = calculate_creditable_foreign_tax(
        2000, 300, 0, 0.30, 0.15
    )["creditable_tax_eur"]
    assert globally_recalculated == 300.0


def test_verified_us_funds_have_treaty_rate():
    from etf_classification import get_foreign_tax_treaty_rate, is_valid_isin
    # US-domizilierte InvStG-Fonds der Tabelle: DBA-USA 15 %
    assert get_foreign_tax_treaty_rate("US46090E1038") == 0.15  # QQQ
    assert get_foreign_tax_treaty_rate("US37954Y4834") == 0.15  # QYLD
    # no_invstg-Produkte (ETN) laufen nicht ueber den Fonds-QSt-Pfad
    assert get_foreign_tax_treaty_rate("US06748M1962") is None  # VXX
    # Nicht-RIC-Strukturen (LP/Commodity Pool/Grantor Trust): keine
    # Dividenden i.S.d. Art. 10 DBA-USA -> kein 15%-Blanket, dba_unverified
    for non_ric_isin in (
        "US91232N2071",  # USO (LP)
        "US46138K1034",  # FXE (Grantor Trust)
        "US74347Y7489",  # BOIL (ProShares Trust II, PTP)
        "US74347Y6804",  # UVXY (ProShares Trust II, PTP)
        "US74347W6012",  # UGL (ProShares Trust II, PTP)
        "US74347W3530",  # AGQ (ProShares Trust II, PTP)
        "US92891H1014",  # SVIX (VS Trust, Commodity Pool)
    ):
        assert get_foreign_tax_treaty_rate(non_ric_isin) is None, non_ric_isin
    # ProShares Trust I (1940-Act-RICs) behalten den 15%-Satz
    assert get_foreign_tax_treaty_rate("US74347X8314") == 0.15  # TQQQ
    assert get_foreign_tax_treaty_rate("US74347G4405") == 0.15  # BITO
    assert not is_valid_isin("US74347X8492")
    # unbekannte ISINs bleiben unbelegt
    assert get_foreign_tax_treaty_rate("IE00B4L5Y983") is None


def test_paid_short_distributions_are_excluded_from_form_lines():
    common = {
        "currency": "EUR",
        "subCategory": "ETF",
        "isin": "US78462F1030",
        "symbol": "SPY",
    }
    rd = calculate_for_funds([
        {
            **common,
            "activityCode": "DIV",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "100",
        },
        {
            **common,
            "activityCode": "PIL",
            "reportDate": "2025-06-15",
            "date": "2025-06-15",
            "amount": "-50",
        },
    ])
    spy = rd["kap_inv"]["etf_by_isin"]["US78462F1030"]
    assert round(spy["div"], 2) == 50.00
    assert round(spy["div_received"], 2) == 100.00
    assert round(spy["div_paid"], 2) == -50.00
    form = rd["kap_inv_form"]
    lines = {line["line"]: line for line in form["lines"]}
    # Zeile 4 zeigt nur die ZUGEFLOSSENE Ausschuettung, kein Netting
    assert round(lines[4]["amount_raw_eur"], 2) == 100.00
    paid = form["negative_distribution_details"]
    assert len(paid) == 1
    assert paid[0]["isin"] == "US78462F1030"
    assert round(paid[0]["paid_distribution_eur"], 2) == -50.00
    assert form["status"] == "paid_distribution_review_required"
    assert any("gezahlte" in w.lower() for w in form["warnings"])


def test_report_date_sets_tax_year_and_entitlement_date_is_preserved():
    # Erstattung mit Bezugsdatum im Vorjahr (date=2024), aber Buchung im
    # Steuerjahr (reportDate=2025): reportDate bestimmt die Zuordnung,
    # das historische Bezugsdatum bleibt am Event erhalten.
    rd = calculate_for_funds([
        {
            "activityCode": "WHT",
            "reportDate": "2025-02-05",
            "date": "2024-12-15",
            "amount": "27.44",
            "currency": "EUR",
            "subCategory": "ETF",
            "isin": "US4642874329",
            "symbol": "TLT",
        },
    ], dba_beta=True)
    events = rd["kap_inv"]["wht_events"]
    assert len(events) == 1
    event = events[0]
    assert event["date"] == "2024-12-15"
    assert event["report_dates"] == ["2025-02-05"]
    assert event["status"] == "unmatched_refund"

    rows = build_wht_review_rows(
        rd["kap_inv"]["wht_review_items"], rd["kap_inv"]["etf_by_isin"]
    )
    assert len(rows) == 1
    assert rows[0]["booking_date"] == "05.02.2025"
    assert rows[0]["entitlement_date"] == "15.12.2024"


def test_review_rows_carry_product_identity():
    # TLT wird ueber die Klassifizierungstabelle erkannt; Ticker, ISIN und
    # Produktname stehen in den vorbereiteten Tabellenzeilen bereit.
    rows = build_wht_review_rows([
        {
            "isin": "US4642874329",
            "date": "2023-12-01",
            "report_dates": ["2024-02-05"],
            "net_foreign_tax_eur": -27.44,
            "german_cap_eur": 0.0,
            "treaty_cap_eur": None,
            "creditable_tax_eur": -27.44,
            "status": "unmatched_refund",
        },
    ])
    assert rows[0]["ticker"] == "TLT"
    assert rows[0]["product"] == "TLT · US4642874329"
    assert "Treasury" in rows[0]["name"]
    assert format_german_date("2024-02-05") == "05.02.2024"


def test_status_labels_are_user_facing_with_safe_fallback():
    assert "Zeitversetzte Erstattung" in get_wht_event_status_label("unmatched_refund")
    assert get_wht_event_status_label("matched") == "Zugeordnet"
    assert get_wht_event_status_label("fully_refunded") == "Vollständig erstattet"
    assert "DBA" in get_wht_event_status_label("dba_unverified")
    assert "Überhang" in get_wht_event_status_label("refund_offsets_excess")
    # Unbekannte kuenftige Status verschwinden nicht: lesbarer Fallback
    fallback = get_wht_event_status_label("some_future_status")
    assert "some_future_status" in fallback
    assert "Prüfen" in fallback
    assert "Prüfen" in get_wht_event_status_label("")


def test_mode_comparison_sums_per_account_and_never_recalculates_merged():
    # Konto A: Einbehalt 15 auf 20 Ausschuettung (0% TFS, kein DBA-Eintrag)
    # -> Beta: Cap 5, Ueberhang 10; Standard: 15.
    account_a = {
        "XX0000000001": {
            "tfs_rate": 0.0,
            "wht": -15.0,
            "wht_events": [{
                "gross_distribution_eur": 20.0,
                "tax_withheld_eur": 15.0,
                "tax_refunded_eur": 0.0,
            }],
        },
    }
    # Konto B: Einbehalt 10 (voll anrechenbar) + separate Erstattung 10
    # -> Beta: 10 - 10 = 0 (kein eigener Ueberhang); Standard: 0.
    account_b = {
        "XX0000000001": {
            "tfs_rate": 0.0,
            "wht": 0.0,
            "wht_events": [
                {
                    "gross_distribution_eur": 40.0,
                    "tax_withheld_eur": 10.0,
                    "tax_refunded_eur": 0.0,
                },
                {
                    "gross_distribution_eur": 0.0,
                    "tax_withheld_eur": 0.0,
                    "tax_refunded_eur": 10.0,
                },
            ],
        },
    }
    snapshot_a = copy.deepcopy(account_a)
    snapshot_b = copy.deepcopy(account_b)

    result = compare_kap_inv_wht_modes([account_a, account_b])
    # Kontoweise Summe ist der korrekte Vergleichswert.
    assert round(result["standard_eur"], 2) == 15.00
    assert round(result["beta_eur"], 2) == 5.00
    assert round(result["difference_eur"], 2) == -10.00
    # Der Vergleich mutiert die Eingabedaten nicht.
    assert account_a == snapshot_a
    assert account_b == snapshot_b

    # Gegenprobe: auf dem GEMERGTEN Event-Pool verrechnet der Refund-Offset
    # die Erstattung aus Konto B gegen den Ueberhang aus Konto A -> 15 statt 5.
    # Genau deshalb darf der Vergleich nie auf gemergten Events rechnen.
    merged_pool = {
        "XX0000000001": {
            "tfs_rate": 0.0,
            "wht": -15.0,
            "wht_events": (
                copy.deepcopy(account_a["XX0000000001"]["wht_events"])
                + copy.deepcopy(account_b["XX0000000001"]["wht_events"])
            ),
        },
    }
    merged_result = compare_kap_inv_wht_modes([merged_pool])
    assert round(merged_result["beta_eur"], 2) == 15.00


def test_withholding_tax_reporting_normalizes_zero():
    reported = get_withholding_tax_for_reporting(0)
    assert reported == 0.0
    assert math.copysign(1, reported) == 1.0


def test_kap_inv_wht_refunds_keep_their_sign():
    common = {
        "currency": "EUR",
        "subCategory": "ETF",
        "isin": "US78462F1030",
        "symbol": "SPY",
    }
    mixed = calculate_for_funds([
        {
            **common,
            "activityCode": "DIV",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "1000",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "-150",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-04-01",
            "date": "2025-04-01",
            "amount": "50",
        },
    ], dba_beta=True)["kap_inv"]
    assert round(mixed["etf_wht_eur"], 2) == 100.00
    assert round(mixed["etf_wht_anrechenbar_eur"], 2) == 100.00

    # Erstattung des NICHT angerechneten Ueberhangs darf Zeile 41 nicht
    # kuerzen: 300 einbehalten, DBA-Limit 150 -> 150 angerechnet, 150
    # Ueberhang. Spaetere Erstattung 150 konsumiert nur den Ueberhang.
    excess_refund = calculate_for_funds([
        {
            **common,
            "activityCode": "DIV",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "1000",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "-300",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-05-01",
            "date": "2025-05-01",
            "amount": "150",
        },
    ], dba_beta=True)
    excess_kap_inv = excess_refund["kap_inv"]
    assert round(excess_kap_inv["etf_wht_anrechenbar_eur"], 2) == 150.00
    assert round(excess_refund["zeile_41_withholding_tax_eur"], 2) == 150.00
    offset_events = [
        e for e in excess_kap_inv["wht_events"]
        if e.get("status") == "refund_offsets_excess"
    ]
    assert len(offset_events) == 1
    assert round(offset_events[0]["excess_offset_eur"], 2) == 150.00

    # Erstattung UEBER den Ueberhang hinaus kuerzt die Anrechnung um den Rest.
    over_refund = calculate_for_funds([
        {
            **common,
            "activityCode": "DIV",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "1000",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "-300",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-05-01",
            "date": "2025-05-01",
            "amount": "200",
        },
    ], dba_beta=True)["kap_inv"]
    assert round(over_refund["etf_wht_anrechenbar_eur"], 2) == 100.00

    refund_only = calculate_for_funds([
        {
            **common,
            "activityCode": "DIV",
            "reportDate": "2025-03-15",
            "date": "2025-03-15",
            "amount": "1000",
        },
        {
            **common,
            "activityCode": "WHT",
            "reportDate": "2025-04-01",
            "date": "2025-04-01",
            "amount": "20",
        },
    ], dba_beta=True)["kap_inv"]
    assert round(refund_only["etf_wht_eur"], 2) == -20.00
    assert round(refund_only["etf_wht_anrechenbar_eur"], 2) == -20.00
    assert round(get_kap_inv_wht_for_reporting(refund_only), 2) == -20.00
    review = refund_only["wht_review_items"]
    assert len(review) == 1
    assert review[0]["status"] == "unmatched_refund"


if __name__ == "__main__":
    test_creditable_tax_uses_caps_instead_of_proportional_tfs_reduction()
    test_kap_inv_wht_anrechenbar_uses_event_cap()
    test_kap_inv_wht_reporting_falls_back_for_legacy_data()
    test_dba_beta_is_opt_in_and_legacy_mode_is_default()
    test_line_41_invstg_toggle_replaces_fund_tax_without_double_counting()
    test_multi_account_sums_finished_credits_without_global_recap()
    test_verified_us_funds_have_treaty_rate()
    test_paid_short_distributions_are_excluded_from_form_lines()
    test_report_date_sets_tax_year_and_entitlement_date_is_preserved()
    test_review_rows_carry_product_identity()
    test_status_labels_are_user_facing_with_safe_fallback()
    test_mode_comparison_sums_per_account_and_never_recalculates_merged()
    test_withholding_tax_reporting_normalizes_zero()
    test_kap_inv_wht_refunds_keep_their_sign()
    print("OK: KAP-INV WHT reporting")
