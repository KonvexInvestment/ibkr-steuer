"""Regression guard for corrected identifiers and resolved product law cases."""

import csv
import io
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etf_classification import (
    CLASSIFICATION_CATALOG_AS_OF,
    ETF_CLASSIFICATION,
    ETF_CLASSIFICATION_REVIEW,
    IDENTIFIER_PRIMARY_SOURCES,
    ISIN_CORRECTIONS,
    PRODUCT_CLASSIFICATION_EVIDENCE,
    classification_catalog_to_csv,
    get_classification,
    get_classification_catalog,
    get_foreign_tax_treaty_rate,
    get_routing_classification,
    is_investment_fund,
    is_valid_isin,
)


EXPECTED_CEF_EQUITY = {
    "US6706ER1015",  # BXMX
    "US27828X1000",  # ETB
    "US27829F1084",  # EXG
}
EXPECTED_CEF_POLICY_FILINGS = {
    "US6706ER1015": "d614442d497.htm",  # BXMX prospectus
    "US27828X1000": "etbn2final.htm",  # ETB Form N-2
    "US27829F1084": "exgn2final.htm",  # EXG Form N-2
}
EXPECTED_CEF_OTHER = {
    "US00302L1089", "US1846911030", "US94987B1052", "US27828Q1058",
    "US27827X1019", "US27828H1059", "US31647Q1067", "US87911K1007",
    "US67073B1061", "US55607W1009", "US95766M1053", "US0188251096",
    "US6706821039", "US89148B1017", "US76970B1017", "US19247X1000",
    "US2316312014", "US19248A1097", "US46131M1062",
}
EXPECTED_TRUST_OTHER = {
    "US78463V1070", "US4642852044", "US46428Q1094", "US98149E3036",
    "US0032621023", "US46438F1012", "US3896371099", "US3896381072",
    "US0919481095", "US46138B1035", "US46140H7008", "US46428R1077",
    "US88166A8707", "US03210A1079", "US46138K1034", "US9129087964",
    "US92891H1014", "US74347Y7489", "US74347Y6804", "US74347W6012",
    "US74347W3530",
}
EXPECTED_PARTNERSHIPS = {"US91232N2071", "US9123184098"}
EXPECTED_COVERED_CALL_EQUITY = {"US46641Q3323", "US46654Q2030"}
EXPECTED_HISTORY_ONLY_PRODUCTS = {"US25459Y2072", "LU0290358497"}


def test_active_table_contains_only_valid_current_isins():
    assert ETF_CLASSIFICATION_REVIEW == {}
    assert all(is_valid_isin(isin) for isin in ETF_CLASSIFICATION)
    assert len(ISIN_CORRECTIONS) == 70
    for old_isin, (ticker, current_isin, source_key) in ISIN_CORRECTIONS.items():
        assert old_isin not in ETF_CLASSIFICATION
        assert is_valid_isin(current_isin)
        assert current_isin in ETF_CLASSIFICATION
        assert ETF_CLASSIFICATION[current_isin][0] == ticker
        assert source_key in IDENTIFIER_PRIMARY_SOURCES
        assert IDENTIFIER_PRIMARY_SOURCES[source_key].startswith("https://")


def test_every_resolved_legal_case_has_consistent_primary_evidence():
    expected = (
        EXPECTED_CEF_EQUITY | EXPECTED_CEF_OTHER | EXPECTED_TRUST_OTHER
        | EXPECTED_PARTNERSHIPS | EXPECTED_COVERED_CALL_EQUITY
        | EXPECTED_HISTORY_ONLY_PRODUCTS
    )
    assert set(PRODUCT_CLASSIFICATION_EVIDENCE) == expected
    for isin, evidence in PRODUCT_CLASSIFICATION_EVIDENCE.items():
        assert evidence["status"] == "verified"
        assert evidence["classification"] == get_classification(isin)
        assert get_routing_classification(isin) == get_classification(isin)
        assert evidence["sources"]
        assert all(source.startswith("https://") for source in evidence["sources"])
        assert "InvStG" in evidence["invstg_basis"]


def test_closed_end_fund_quota_decisions_are_product_specific():
    for isin in EXPECTED_CEF_EQUITY:
        assert get_classification(isin) == "aktienfonds"
        assert "80 %" in PRODUCT_CLASSIFICATION_EVIDENCE[isin]["quota_basis"]
        assert any(
            source.endswith(EXPECTED_CEF_POLICY_FILINGS[isin])
            for source in PRODUCT_CLASSIFICATION_EVIDENCE[isin]["sources"]
        )
    for isin in EXPECTED_CEF_OTHER:
        assert get_classification(isin) == "sonstiger_fonds"
        assert ">50" in PRODUCT_CLASSIFICATION_EVIDENCE[isin]["quota_basis"]
    assert all(is_investment_fund(isin) for isin in EXPECTED_CEF_EQUITY | EXPECTED_CEF_OTHER)


def test_trusts_are_funds_but_do_not_get_unproved_dba_dividend_cap():
    for isin in EXPECTED_TRUST_OTHER:
        assert get_classification(isin) == "sonstiger_fonds"
        assert is_investment_fund(isin)
        assert get_foreign_tax_treaty_rate(isin) is None


def test_partnership_and_equity_quota_outcomes_are_final_routes():
    for isin in EXPECTED_PARTNERSHIPS:
        assert get_classification(isin) == "personengesellschaft"
        assert not is_investment_fund(isin)
    for isin in EXPECTED_COVERED_CALL_EQUITY:
        assert get_classification(isin) == "aktienfonds"
        assert "ELNs" in PRODUCT_CLASSIFICATION_EVIDENCE[isin]["quota_basis"]


def test_history_only_upload_products_are_final_and_evidenced():
    assert get_classification("US25459Y2072") == "aktienfonds"
    assert "99,9 %" in PRODUCT_CLASSIFICATION_EVIDENCE["US25459Y2072"]["quota_basis"]
    assert get_classification("LU0290358497") == "sonstiger_fonds"
    assert "Swap" in PRODUCT_CLASSIFICATION_EVIDENCE["LU0290358497"]["quota_basis"]
    assert all(is_investment_fund(isin) for isin in EXPECTED_HISTORY_ONLY_PRODUCTS)
    catalog = {
        row["isin"]: row
        for row in get_classification_catalog(EXPECTED_HISTORY_ONLY_PRODUCTS)
    }
    qqqe_labels = [label for label, _url in catalog["US25459Y2072"]["product_sources"]]
    assert qqqe_labels == [
        "SEC-Produktdokument", "SEC-Produktdokument", "Direxion-Produktseite",
    ]
    assert catalog["LU0290358497"]["product_sources"][0][0] == "DWS-Produktdokument"


def test_transparency_catalog_covers_every_active_classification_once():
    catalog = get_classification_catalog()
    assert len(catalog) == len(ETF_CLASSIFICATION) == 256
    assert {row["isin"] for row in catalog} == set(ETF_CLASSIFICATION)

    required_fields = {
        "isin", "ticker", "name", "classification",
        "classification_label", "tfs_rate", "tfs_label", "tax_route",
        "legal_form", "decision_reason", "legal_basis", "evidence_status",
        "evidence_label", "as_of", "legal_sources", "product_sources",
    }
    for row in catalog:
        assert required_fields <= set(row)
        assert row["decision_reason"]
        assert row["legal_basis"]
        assert row["tax_route"]
        assert row["as_of"] == CLASSIFICATION_CATALOG_AS_OF
        assert all(url.startswith("https://") for _label, url in row["legal_sources"])
        assert all(url.startswith("https://") for _label, url in row["product_sources"])


def test_transparency_catalog_distinguishes_product_evidence_from_rules():
    catalog_by_isin = {
        row["isin"]: row for row in get_classification_catalog()
    }
    product_verified = {
        isin for isin, row in catalog_by_isin.items()
        if row["evidence_status"] == "product_verified"
    }
    standard = {
        isin for isin, row in catalog_by_isin.items()
        if row["evidence_status"] == "standard_classification"
    }
    assert product_verified == set(PRODUCT_CLASSIFICATION_EVIDENCE)
    assert len(product_verified) == 49
    assert standard == set(ETF_CLASSIFICATION) - product_verified
    assert all(catalog_by_isin[isin]["product_sources"] for isin in product_verified)
    assert all(not catalog_by_isin[isin]["product_sources"] for isin in standard)
    assert all(
        catalog_by_isin[isin]["evidence_label"] == "Katalogzuordnung · aktiv"
        for isin in standard
    )
    assert all(
        "weder unklassifiziert noch ein Quarantäne- oder Prüffall"
        in catalog_by_isin[isin]["decision_reason"]
        for isin in standard
    )

    gld = catalog_by_isin["US78463V1070"]
    assert gld["classification_label"] == "Sonstiger Fonds"
    assert "passiver Trust/Commodity Pool" in gld["decision_reason"]
    assert "keine verbindliche Kapitalbeteiligungsquote" in gld["decision_reason"]

    uso = catalog_by_isin["US91232N2071"]
    assert uso["classification_label"] == "Personengesellschaft"
    assert uso["tfs_rate"] is None
    assert "Blockiert" in uso["tax_route"]

    xetra_gold = catalog_by_isin["DE000A0S9GB0"]
    assert xetra_gold["classification_label"] == "Anlage SO"
    assert xetra_gold["evidence_status"] == "standard_classification"
    assert "Sachlieferungsanspruch" in xetra_gold["decision_reason"]


def test_transparency_catalog_keeps_requested_unknowns_unclassified():
    unknown_isin = "DE0000000000"
    rows = get_classification_catalog(["US78463V1070", unknown_isin])
    by_isin = {row["isin"]: row for row in rows}
    assert set(by_isin) == {"US78463V1070", unknown_isin}
    unknown = by_isin[unknown_isin]
    assert unknown["classification"] is None
    assert unknown["evidence_status"] == "user_confirmation_required"
    assert unknown["tfs_rate"] is None
    assert "keine automatische" in unknown["decision_reason"]
    assert not unknown["legal_sources"]
    assert not unknown["product_sources"]


def test_transparency_catalog_csv_contains_all_rows_and_sources():
    csv_rows = list(csv.DictReader(
        io.StringIO(classification_catalog_to_csv()), delimiter=";"
    ))
    assert len(csv_rows) == len(ETF_CLASSIFICATION)
    assert set(csv_rows[0]) == {
        "ISIN", "Ticker", "Name", "Zuordnung", "Teilfreistellung",
        "Steuerpfad", "Nachweisstatus", "Rechtsform", "Begründung",
        "Rechtsgrundlage", "Stand", "Produktquellen", "Rechtsquellen",
    }
    gld = next(row for row in csv_rows if row["ISIN"] == "US78463V1070")
    assert gld["Nachweisstatus"] == "Produktindividuell geprüft"
    assert "sec.gov" in gld["Produktquellen"]
    assert "gesetze-im-internet.de" in gld["Rechtsquellen"]


if __name__ == "__main__":
    test_active_table_contains_only_valid_current_isins()
    test_every_resolved_legal_case_has_consistent_primary_evidence()
    test_closed_end_fund_quota_decisions_are_product_specific()
    test_trusts_are_funds_but_do_not_get_unproved_dba_dividend_cap()
    test_partnership_and_equity_quota_outcomes_are_final_routes()
    test_history_only_upload_products_are_final_and_evidenced()
    test_transparency_catalog_covers_every_active_classification_once()
    test_transparency_catalog_distinguishes_product_evidence_from_rules()
    test_transparency_catalog_keeps_requested_unknowns_unclassified()
    test_transparency_catalog_csv_contains_all_rows_and_sources()
    print("OK: ISIN corrections, evidence, and transparency catalog")
