"""AppTest-Verhaltenstests fuer app.py (PLAN.md Revision 6, Phase 3).

st.file_uploader ist im AppTest-Elementbaum nicht steuerbar; deshalb nutzt
jeder Test den dokumentierten Test-Seam: ein synthetischer Upload-Snapshot
wird direkt in ``AppTest.session_state['dataset']`` injiziert, der Rest der
App (Compute, View-Model, Renderer, Navigation) laeuft echt.

Abgedeckt: Start-Screen, alle Bereiche ohne Exception, Nav-Normalisierung,
Widget-Persistenz ueber Bereichswechsel (Fondsbestaetigung), Compute-Cache
(kein Neulauf bei Navigation, Neulauf bei Compute-Toggle), Datensatzwechsel
setzt Domain-State zurueck, Fehlerpfad committet keinen Snapshot,
Upload-Validierung (Duplikate, Ueberlappung, Fremd-XML,
Multi-Statement-Dateien), Exportvollstaendigkeit.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ui_model  # noqa: E402

from streamlit.testing.v1 import AppTest  # noqa: E402

APP_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py",
)
INDEX_PATH = os.path.join(os.path.dirname(APP_PATH), "index.html")

SYNTHETIC_BODY = """
      <Trades>
        <Trade accountId="U123" assetCategory="STK" subCategory="COMMON" symbol="AAPL" description="APPLE" conid="1" isin="US0378331005" tradeID="t1" reportDate="2025-03-10" dateTime="2025-03-10 10:00:00" buySell="SELL" openClose="C" quantity="-10" tradePrice="200" closePrice="200" proceeds="2000" cost="-1800" fifoPnlRealized="200" fxRateToBase="0.9" ibCommission="-1" currency="USD" levelOfDetail="EXECUTION" transactionType="ExchTrade" multiplier="1" />
        <Trade accountId="U123" assetCategory="STK" subCategory="ETF" symbol="FAKE" description="FAKE ETF" conid="2" isin="XX0000000001" tradeID="t2" reportDate="2025-04-10" dateTime="2025-04-10 10:00:00" buySell="SELL" openClose="C" quantity="-5" tradePrice="50" closePrice="50" proceeds="250" cost="-200" fifoPnlRealized="50" fxRateToBase="0.9" ibCommission="-1" currency="USD" levelOfDetail="EXECUTION" transactionType="ExchTrade" multiplier="1" />
      </Trades>
      <StmtFunds>
        <StatementOfFundsLine accountId="U123" currency="EUR" fxRateToBase="1" assetCategory="STK" symbol="AAPL" isin="US0378331005" reportDate="2025-05-15" date="2025-05-15" activityCode="DIV" activityDescription="AAPL Cash Dividend" amount="100" transactionID="f1" levelOfDetail="BaseCurrency" />
        <StatementOfFundsLine accountId="U123" currency="EUR" fxRateToBase="1" assetCategory="STK" symbol="AAPL" isin="US0378331005" reportDate="2025-05-15" date="2025-05-15" activityCode="FRTAX" activityDescription="AAPL Withholding Tax" amount="-15" transactionID="f2" levelOfDetail="BaseCurrency" />
      </StmtFunds>
"""


def make_xml(body=SYNTHETIC_BODY, from_date="2025-01-01",
             to_date="2025-12-31", account="U123"):
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse>
  <FlexStatements count="1">
    <FlexStatement accountId="{account}" fromDate="{from_date}" toDate="{to_date}">
      <AccountInformation accountId="{account}" name="Synthetic" currency="EUR" />
      {body}
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""


def make_dataset(named_xmls, csv=None):
    entries = []
    for name, xml in named_xmls:
        data = xml.encode("utf-8")
        entries.append({
            'name': name, 'digest': ui_model.file_digest(data),
            'kind': 'xml', 'data': data,
        })
    meta = [{'name': e['name'], 'digest': e['digest'], 'kind': e['kind']}
            for e in entries]
    return {
        'files': entries,
        'dropped_duplicates': [],
        'dataset_id': ui_model.build_dataset_id(meta),
        'raw_upload_id': ui_model.build_raw_upload_id(meta),
        'csv': csv,
    }


def make_compact_xml():
    """IBKR defaults: yyyyMMdd, HHmmss and semicolon separator."""
    body = SYNTHETIC_BODY
    replacements = {
        "2025-03-10 10:00:00": "20250310;100000",
        "2025-04-10 10:00:00": "20250410;100000",
        "2025-03-10": "20250310",
        "2025-04-10": "20250410",
        "2025-05-15": "20250515",
    }
    for old, new in replacements.items():
        body = body.replace(old, new)
    return make_xml(body=body, from_date="20250101", to_date="20251231")


def make_csv_entry():
    data = b"Dividenden,Data,Gesamt Dividenden in EUR,,,100.00\n"
    return {
        'name': 'ibkr_report.csv',
        'digest': ui_model.file_digest(data),
        'kind': 'csv',
        'data': data,
    }


def run_app(dataset=None, nav=None, session=None):
    at = AppTest.from_file(APP_PATH, default_timeout=300)
    if dataset is not None:
        at.session_state['dataset'] = dataset
    if nav is not None:
        at.session_state['nav'] = nav
    for key, value in (session or {}).items():
        at.session_state[key] = value
    at.run()
    return at


def assert_no_exception(at, label):
    excs = [str(e.value)[:300] for e in at.exception]
    assert not excs, f"{label}: {excs}"


def all_markdown(at):
    return "\n".join(m.value for m in at.markdown)


def test_start_screen_without_dataset():
    at = run_app()
    assert_no_exception(at, "Start-Screen")
    rendered = all_markdown(at)
    assert 'start-title' in rendered
    assert "IBKR-Steuerbericht" in rendered
    assert "Dein IBKR-Steuerbericht" not in rendered
    assert "100 % lokal" in rendered
    assert "vollständig lokal auf dem eigenen Rechner" in rendered
    assert "Keine Steuerberatung · Haftungsbeschränkung" in rendered
    assert "Nutzung und Prüfung der Ergebnisse erfolgen eigenverantwortlich" in rendered
    assert "Soweit gesetzlich zulässig" in rendered
    assert all(button.label != "Beispielbericht ansehen" for button in at.button)


def test_stlite_bundle_includes_date_normalizer():
    with open(INDEX_PATH, encoding='utf-8') as handle:
        index_html = handle.read()
    assert '"ibkr_dates.py": { url: "./ibkr_dates.py" }' in index_html


def test_all_pages_render_and_nav_normalizes():
    dataset = make_dataset([("synthetic_2025.xml", make_xml())])
    at = run_app(dataset)
    assert_no_exception(at, "overview")
    vm_pages = ['overview', 'kap', 'kap_inv', 'prueffaelle', 'rechenwege',
                'export']
    for page in vm_pages[1:]:
        at.session_state['nav'] = page
        at.run()
        assert_no_exception(at, page)
    # Unsichtbare/unbekannte Seite normalisiert atomar auf overview
    at.session_state['nav'] = 'anlage_so'  # keine SO-Daten im Fixture
    at.run()
    assert_no_exception(at, "anlage_so normalisiert")
    assert at.session_state['nav'] == 'overview'
    at.session_state['nav'] = 'gibt_es_nicht'
    at.run()
    assert at.session_state['nav'] == 'overview'


def test_compact_dates_and_optional_csv_preserve_expected_values():
    dataset = make_dataset(
        [("compact_2025.xml", make_compact_xml())],
        csv=make_csv_entry(),
    )
    at = run_app(dataset)
    assert_no_exception(at, "kompakte IBKR-Datumswerte mit CSV")

    payload = at.session_state['snapshot']['payload']
    report = payload['merged']
    assert payload['csv_present'] is True
    assert payload['csv_enabled'] is True
    assert report['zeile_19_netto_eur'] == 280.0
    assert report['zeile_20_stock_gains_eur'] == 180.0
    assert report['dividends_eur'] == 100.0
    assert report['zeile_41_withholding_tax_eur'] == 15.0
    assert report['csv_income_totals']['dividends_eur'] == 100.0
    assert len(report['trade_details']) == 2
    assert report['trade_details'][0]['reportDate'] == '2025-03-10'
    assert report['trade_details'][0]['dateTime'] == '2025-03-10 10:00:00'


def test_widget_persistence_fund_confirmation_survives_navigation():
    dataset = make_dataset([("synthetic_2025.xml", make_xml())])
    dataset_id = dataset['dataset_id']
    isin = 'XX0000000001'
    conf_key = f"_ui_etf_conf_{dataset_id[:12]}_{isin}"

    at = run_app(dataset, nav='kap_inv')
    assert_no_exception(at, "kap_inv initial")
    checkbox = at.checkbox(key=conf_key)
    checkbox.check()
    at.run()
    assert_no_exception(at, "kap_inv nach Bestaetigung")
    domain = at.session_state['domain'][dataset_id]
    assert isin in domain['etf_overrides'], \
        "Fondsbestaetigung muss im Domain-State liegen, nicht im Widget-State"

    # Zwei Bereichswechsel: das Widget wird nicht gerendert, die
    # Bestaetigung muss trotzdem erhalten bleiben.
    for page in ('kap', 'overview', 'kap_inv'):
        at.session_state['nav'] = page
        at.run()
        assert_no_exception(at, f"wechsel {page}")
        domain = at.session_state['domain'][dataset_id]
        assert isin in domain['etf_overrides'], \
            f"Bestaetigung nach Wechsel zu {page} verloren"
    # Zurueck im Bereich: Widget spiegelt den Domain-State
    assert at.checkbox(key=conf_key).value is True


def test_compute_cache_hits_on_navigation_and_recomputes_on_compute_toggle():
    dataset = make_dataset([("synthetic_2025.xml", make_xml())])
    at = run_app(dataset)
    gen_after_first = at.session_state['compute_generation']
    snapshot_key = at.session_state['snapshot']['input_key']

    for page in ('kap', 'prueffaelle', 'export', 'overview'):
        at.session_state['nav'] = page
        at.run()
    assert at.session_state['compute_generation'] == gen_after_first, \
        "Navigation darf keine Neuberechnung ausloesen"
    assert at.session_state['snapshot']['input_key'] == snapshot_key

    # Compute-Toggle (DBA-Beta; FX-Saldo ist ohne FX-Daten unsichtbar)
    # aendert den input_key -> genau ein Neulauf
    dataset_id = dataset['dataset_id']
    dba_key = f"_ui_tg_dba_beta_{dataset_id[:12]}"
    at.checkbox(key=dba_key).check()
    at.run()
    assert_no_exception(at, "nach fx_margin-Toggle")
    assert at.session_state['compute_generation'] == gen_after_first + 1
    assert at.session_state['snapshot']['input_key'] != snapshot_key


def test_dataset_switch_resets_domain_state_and_nav():
    dataset_a = make_dataset([("a_2025.xml", make_xml())])
    at = run_app(dataset_a, nav='kap_inv')
    isin = 'XX0000000001'
    conf_key = f"_ui_etf_conf_{dataset_a['dataset_id'][:12]}_{isin}"
    at.checkbox(key=conf_key).check()
    at.run()
    assert isin in at.session_state['domain'][dataset_a['dataset_id']][
        'etf_overrides']

    # Neuer Datensatz (ohne ETF): eigene Domain, Nav normalisiert.
    body_no_etf = SYNTHETIC_BODY.replace('subCategory="ETF"',
                                         'subCategory="COMMON"')
    dataset_b = make_dataset([("b_2025.xml", make_xml(body=body_no_etf))])
    at.session_state['dataset'] = dataset_b
    at.session_state['nav'] = 'kap_inv'
    at.run()
    assert_no_exception(at, "dataset switch")
    assert at.session_state['nav'] == 'overview', \
        "kap_inv ist ohne ETF-Daten unsichtbar -> Normalisierung"
    domain_b = at.session_state['domain'][dataset_b['dataset_id']]
    assert domain_b['etf_overrides'] == {}, \
        "Domain-State darf nicht auf den neuen Datensatz durchsickern"


def test_failed_compute_commits_no_snapshot():
    dataset = make_dataset([("kaputt.xml", "<FlexQueryResponse><Flex")])
    at = run_app(dataset)
    assert_no_exception(at, "kaputtes XML wird als Uploadfehler behandelt")
    assert 'snapshot' not in at.session_state, \
        "Fehlgeschlagene Berechnung darf keinen Snapshot committen"
    rendered = all_markdown(at)
    assert "Berechnung nicht möglich" in rendered


def test_incompatible_fx_currency_keeps_marked_partial_snapshot():
    body = SYNTHETIC_BODY + """
      <FxTransactions>
        <FxTransaction reportDate="2025-06-01" dateTime="2025-06-01 10:00:00"
          functionalCurrency="USD" fxCurrency="EUR" quantity="-13000"
          realizedPL="1051.17" code="C" levelOfDetail="TRANSACTION" />
      </FxTransactions>
    """
    dataset = make_dataset([
        ("valid.xml", make_xml(account="U111", body=SYNTHETIC_BODY + '''
            <FxTransactions><FxTransaction reportDate="2025-06-01"
              functionalCurrency="EUR" fxCurrency="USD" quantity="-100"
              realizedPL="22" code="C" levelOfDetail="TRANSACTION" />
            </FxTransactions>''')),
        ("wrong-fx.xml", make_xml(body=body, account="U222")),
    ])
    at = run_app(dataset)
    assert_no_exception(at, "F2 erlaubt gekennzeichnete Teilberechnung")
    report = at.session_state['snapshot']['payload']['merged']
    final = ui_model.build_final_values(report, ui_model.default_toggles())
    assert final['fx_incomplete']
    assert report['fx_total_gain'] == 22, 'FX des kompatiblen Kontos bleibt enthalten'
    assert report['dividends_eur'] == 200
    assert report['stocks_gain_eur'] == 360
    assert final['zeile_19'] == 582
    assert len(report['fx_unresolved']) == 1
    assert report['fx_unresolved'][0]['account_id'] == 'U222'
    rendered = all_markdown(at)
    assert "Berechnung nicht möglich" not in rendered
    assert "Vorläufige Teilberechnung" in rendered
    assert 'vorläufig · FX fehlt' in rendered
    assert 'Die Formularwerte zum Übertragen' not in rendered

    at.session_state['nav'] = 'kap'
    at.run()
    assert_no_exception(at, 'FX-Sektion mit Warnhinweis')
    rendered = all_markdown(at)
    assert "functionalCurrency=USD" in rendered
    assert "Kontobasiswährung EUR" in rendered
    assert "nicht zwingend fehlerhaft" in rendered
    assert "ursprünglichen Anschaffungskosten in EUR" in rendered
    assert 'keine automatische Ersatzberechnung' in rendered
    assert 'Berechneter FX-Anteil der übrigen Konten' in rendered
    assert 'Konto 2 (Synthetic) · FX ungeklärt' in rendered
    assert "<br><br>" in rendered, "Erklaerung in lesbare Absaetze gliedern"

    at.session_state['nav'] = 'export'
    at.run()
    assert_no_exception(at, 'Teilbericht exportierbar')
    exports = at.session_state['export_cache']
    assert at.get('download_button'), 'Exporte bleiben verfuegbar'
    assert 'Vorläufige Teilberechnung' in exports['txt']
    assert 'Konto U222' in exports['txt']
    assert 'Nicht ermittelt bedeutet nicht null' in exports['txt']
    assert 'Zeile 19 (Netto):' in exports['txt']
    assert '582,00 EUR (vorläufig · FX fehlt)' in exports['txt']
    import io
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(exports['xlsx']))
    assert wb.active.title == 'FX-Prüfhinweis'
    assert 'Vorläufige Teilberechnung' in wb.active['A1'].value
    assert 'Konto U222' in wb.active['A2'].value
    z19 = next(row for row in wb['Zusammenfassung'].values
               if row[1] == 'Zeile 19 - ausländische Kapitalerträge netto')
    assert z19[2] == 582
    assert 'Vorläufig' in z19[3]
    assert 'Vorläufig' in wb['Trade-Details 2025']['A2'].value

    # Ein neuer, kompatibler Datensatz darf weder Warnung noch alten Export behalten.
    at.session_state['dataset'] = make_dataset([('fixed.xml', make_xml())])
    at.session_state['nav'] = 'overview'
    at.run()
    assert_no_exception(at, 'Datensatzwechsel entfernt FX-Prueffall')
    assert 'FX ungeklärt' not in all_markdown(at)
    assert not at.session_state['snapshot']['payload']['merged']['fx_unresolved']


def test_fx_partial_single_account_has_no_zero_fx_result():
    body = SYNTHETIC_BODY + '''<FxTransactions>
      <FxTransaction reportDate="2025-06-01" functionalCurrency="USD"
        fxCurrency="EUR" quantity="-100" realizedPL="0" code="O"
        levelOfDetail="TRANSACTION" /></FxTransactions>'''
    at = run_app(make_dataset([('partial.xml', make_xml(body=body))]), nav='kap')
    assert_no_exception(at, 'Einzelkonto mit Null-PnL in abweichender Waehrung')
    rendered = all_markdown(at)
    assert 'Fremdwährungs-Gewinne/Verluste · ungeklärt' in rendered
    assert 'FX Netto' not in rendered
    assert 'FX Gewinne' not in rendered
    assert 'functionalCurrency=USD' in rendered
    assert at.session_state['snapshot']['payload']['merged']['fx_results'] == {}


def test_fx_currency_guidance_escapes_xml_content():
    body = '''<FxTransactions>
        <FxTransaction reportDate="2025-06-01" dateTime="2025-06-01 10:00:00"
          functionalCurrency="&lt;img src=x&gt;" fxCurrency="EUR" quantity="-100"
          realizedPL="10" code="C" levelOfDetail="TRANSACTION" />
      </FxTransactions>'''
    at = run_app(make_dataset([('untrusted-currency.xml', make_xml(body=body))]))
    assert_no_exception(at, 'Waehrungstext aus XML wird escaped')
    rendered = all_markdown(at)
    assert '&lt;IMG SRC=X&gt;' in rendered
    assert '<IMG SRC=X>' not in rendered
    assert 'snapshot' not in at.session_state


def test_quarterly_fx_fills_reach_final_values():
    def body(day):
        row = f'''<FxTransaction levelOfDetail="TRANSACTION"
            reportDate="{day}" dateTime="{day} 10:00:00"
            functionalCurrency="EUR" fxCurrency="USD" quantity="-200"
            realizedPL="13.22" code="C" />'''
        return '<FxTransactions>' + row * 2 + '</FxTransactions>'

    dataset = make_dataset([
        ('q1.xml', make_xml(body=body('2025-02-03'), to_date='2025-03-31')),
        ('q2.xml', make_xml(body=body('2025-05-03'),
                            from_date='2025-04-01', to_date='2025-06-30')),
    ])
    at = run_app(dataset)
    assert_no_exception(at, 'F4 Quartals-Upload')
    report = at.session_state['snapshot']['payload']['merged']
    assert abs(report['fx_total_gain'] - 52.88) < 1e-9
    final = ui_model.build_final_values(report, ui_model.default_toggles())
    assert abs(final['zeile_19'] - 52.88) < 1e-9

    # Ein Snapshot von vor den Rechenfixes darf auch bei gleichen Uploads
    # nicht weiterverwendet werden.
    old_generation = at.session_state['compute_generation']
    snapshot = dict(at.session_state['snapshot'])
    snapshot['schema_version'] = ui_model.SCHEMA_VERSION - 1
    at.session_state['snapshot'] = snapshot
    at.run()
    assert_no_exception(at, 'F4 veralteter Snapshot wird neu berechnet')
    assert at.session_state['compute_generation'] == old_generation + 1
    assert at.session_state['snapshot']['schema_version'] == ui_model.SCHEMA_VERSION


def test_mixed_valid_and_non_flex_xml_is_a_hard_error():
    dataset = make_dataset([
        ("valid.xml", make_xml()),
        ("not-flex.xml", "<?xml version='1.0'?><root><value>42</value></root>"),
    ])
    at = run_app(dataset)
    assert_no_exception(at, "gemischter Upload")
    rendered = all_markdown(at)
    assert "Ungültige Upload-Datei(en)" in rendered
    assert "not-flex.xml" in rendered
    assert 'snapshot' not in at.session_state, \
        "Keine ausgewählte XML darf still aus dem Steuerreport fallen"


def test_multi_statement_xml_is_a_hard_error():
    xml = make_xml()
    start = xml.index('    <FlexStatement')
    end = xml.index('  </FlexStatements>')
    second_statement = xml[start:end].replace('U123', 'U999')
    multi_xml = xml.replace('count="1"', 'count="2"', 1).replace(
        '  </FlexStatements>', second_statement + '  </FlexStatements>', 1)

    at = run_app(make_dataset([("two-accounts.xml", multi_xml)]))
    assert_no_exception(at, "Multi-Statement-Upload")
    rendered = all_markdown(at)
    assert "Mehrere Konten innerhalb derselben XML" in rendered
    assert "U123, U999" in rendered
    assert 'snapshot' not in at.session_state, \
        "Ein Teilreport aus nur dem ersten FlexStatement ist unzulässig"


def test_overlapping_periods_are_a_hard_error():
    xml_h1 = make_xml(from_date="2025-01-01", to_date="2025-06-30")
    xml_h2 = make_xml(from_date="2025-04-01", to_date="2025-12-31")
    dataset = make_dataset([("h1.xml", xml_h1), ("h2.xml", xml_h2)])
    at = run_app(dataset)
    assert_no_exception(at, "Ueberlappung")
    rendered = all_markdown(at)
    assert "Überlappende Berichtszeiträume" in rendered
    assert 'snapshot' not in at.session_state, \
        "Ueberlappende Zeitraeume duerfen keinen Snapshot committen"


def test_partnership_trade_is_visible_in_export_summary():
    uso_xml = make_xml().replace(
        'symbol="FAKE" description="FAKE ETF" conid="2" '
        'isin="XX0000000001"',
        'symbol="USO" description="United States Oil Fund LP" conid="2" '
        'isin="US91232N2071"',
    )
    at = run_app(make_dataset([("uso.xml", uso_xml)]), nav='export')
    assert_no_exception(at, "Personengesellschaft im Export")
    rendered = all_markdown(at)
    assert "Detailabstimmung: 2 Trades, 2 Wertpapiere" in rendered
    assert "Details Personengesellschaft: 45,00 EUR" in rendered


def test_guidance_copy_and_rechenwege_grouping():
    dataset = make_dataset([("synthetic_2025.xml", make_xml())])

    at = run_app(dataset, nav='rechenwege')
    assert_no_exception(at, "gegliederte Rechenwege")
    tab_labels = [tab.label for tab in at.tabs]
    assert tab_labels == [
        "Methoden im Report",
        "Produktzuordnungen",
        "Berechnung & Diagnose",
        "Steuerlogik",
        "XML & Verarbeitung",
        "Diagnose",
        "Rechtliches",
    ]
    captions = "\n".join(c.value for c in at.caption)
    assert "Die konkrete Wirkung steht jeweils direkt darunter" in captions
    assert "meldet Fondswerte auf Anlage KAP-INV" in captions
    rendered = all_markdown(at)
    assert "Methoden im aktuellen Report" not in rendered
    assert "Aktive Methoden im Report" in rendered
    assert "Produkte aus dem Upload · inklusive Vorjahreshistorie" in rendered
    assert "Eigenverantwortliche Nutzung" in rendered
    assert "Zwei-Töpfe-Struktur" in rendered
    assert "Schritt 1: XML-Extraktion" in rendered
    expander_labels = [expander.label for expander in at.expander]
    assert "InvStG-Klassifizierung · Fondsarten und betroffene Produkte" not in expander_labels
    assert "Produkte aus dem Upload · inklusive Vorjahreshistorie" not in expander_labels
    assert any(label.startswith("Gesamtkatalog ·") for label in expander_labels)
    assert "Haftung, Datenschutz und Rechtsstand" not in expander_labels
    assert "Regeln anzeigen - So kommen die Ergebnisse zustande" not in expander_labels
    assert "Berechnungsdetails - So werden die XML-Daten verarbeitet" not in expander_labels

    kap_body = SYNTHETIC_BODY.replace(
        'assetCategory="STK" subCategory="COMMON" symbol="AAPL"',
        'assetCategory="OPT" subCategory="" '
        'symbol="AAPL  250620C00200000"',
        1,
    )
    kap_at = run_app(
        make_dataset([("synthetic_kap_2025.xml", make_xml(body=kap_body))]),
        nav='kap',
    )
    assert_no_exception(kap_at, "direkte Topf-2-Aufschlüsselung")
    rendered = all_markdown(kap_at)
    expander_labels = [expander.label for expander in kap_at.expander]
    assert "Aufschlüsselung Topf 2" in rendered
    assert "Aufschlüsselung Topf 2" not in expander_labels

    at.session_state['nav'] = 'kap_inv'
    at.run()
    assert_no_exception(at, "KAP-INV-Hierarchie")
    rendered = all_markdown(at)
    assert "Formularwerte" in rendered
    assert "Noch zu prüfen" in rendered
    assert "Quellensteuer" in rendered

    at.session_state['nav'] = 'export'
    at.run()
    assert_no_exception(at, "Export-Erklärung")
    rendered = all_markdown(at)
    captions = "\n".join(c.value for c in at.caption)
    assert "Formularwerte und Detailwerte sind zwei verschiedene Ebenen" in rendered
    assert "Kontrollnachweis und kein zweiter Satz Formularwerte" in captions


if __name__ == '__main__':
    tests = [
        test_start_screen_without_dataset,
        test_stlite_bundle_includes_date_normalizer,
        test_all_pages_render_and_nav_normalizes,
        test_compact_dates_and_optional_csv_preserve_expected_values,
        test_widget_persistence_fund_confirmation_survives_navigation,
        test_compute_cache_hits_on_navigation_and_recomputes_on_compute_toggle,
        test_dataset_switch_resets_domain_state_and_nav,
        test_failed_compute_commits_no_snapshot,
        test_incompatible_fx_currency_keeps_marked_partial_snapshot,
        test_fx_partial_single_account_has_no_zero_fx_result,
        test_fx_currency_guidance_escapes_xml_content,
        test_quarterly_fx_fills_reach_final_values,
        test_mixed_valid_and_non_flex_xml_is_a_hard_error,
        test_multi_statement_xml_is_a_hard_error,
        test_overlapping_periods_are_a_hard_error,
        test_partnership_trade_is_visible_in_export_summary,
        test_guidance_copy_and_rechenwege_grouping,
    ]
    failures = 0
    for fn in tests:
        try:
            fn()
            print(f"  {fn.__name__}: OK")
        except AssertionError as exc:
            failures += 1
            print(f"  {fn.__name__}: FAIL - {exc}")
    if failures:
        raise SystemExit(1)
    print("OK: AppTest-Verhalten (Seam, Nav, Persistenz, Cache, Reset)")
