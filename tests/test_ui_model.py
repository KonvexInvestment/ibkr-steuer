"""Unit-Tests fuer ui_model.py — die pure View-Model-Schicht.

Deckt ab (PLAN.md Revision 6):
- Upload-Identitaet: Digests, dataset_id (dedupliziert), raw_upload_id,
  Duplikat-Verwerfung, Ueberlappungs-Erkennung
- Cache-Protokoll: input_key (jeder rechenwirksame Parameter), view_key
  (jede Domain-Eingabe), Commit-Guard, snapshot_is_current
- Toggle-Inventar: Verfuegbarkeit (inkl. Tageskurs-Netto-Null-Fall und
  Variante-B aus der Pre-Override-Zeile-7), effektive Toggles
- build_final_values: Zufluss, InvStG-Reintegration, Tageskurs,
  Variante-B-Invariante, WHT-Override
- apply_etf_overrides / recalc_wht_per_account (kontoweise Summierung)
- Immutabilitaets-Guard: kein Builder/Collector mutiert seine Eingaben
- collect_notices: Registry-Vollstaendigkeit (kein Warnpfad verschwindet)
- Navigation: visible_pages / normalize_nav
"""

import copy
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ui_model  # noqa: E402
import calculate_tax_report  # noqa: E402


# ── Fixtures ─────────────────────────────────────────────────────────────────

def make_report(**overrides):
    report = {
        'tax_year': 2025,
        'base_currency': 'EUR',
        'stocks_gain_eur': 1000.0,
        'stocks_loss_eur': -200.0,
        'dividends_eur': 100.0,
        'interest_eur': 10.0,
        'options_gain_eur': 500.0,
        'options_loss_eur': -100.0,
        'topf_1_aktien_netto': 800.0,
        'topf_2_sonstiges_netto': 510.0,
        'zeile_19_netto_eur': 1310.0,
        'zeile_20_stock_gains_eur': 1000.0,
        'zeile_22_other_losses_eur': 100.0,
        'zeile_23_stock_losses_eur': 200.0,
        'zeile_7_kapitalertraege_mit_inlaendischem_steuerabzug_eur': 0.0,
        'zeile_37_kapitalertragsteuer_eur': 0.0,
        'zeile_38_solidaritaetszuschlag_eur': 0.0,
        'zeile_41_withholding_tax_eur': 15.0,
        'withholding_tax_eur': 15.0,
        'fx_correction_total': 0.0,
        'fx_correction_by_topf': {},
        'fx_corr_gain_adj': {},
        'fx_corr_loss_adj': {},
        'fx_results': {},
        'kap_inv': {},
        'anlage_so': {},
        'audit': {'stillhalter_details': [], 'cross_year_premium_eur': 0.0},
    }
    report.update(overrides)
    return report


def all_toggles_off():
    return {key: False for key in ui_model.default_toggles()}


# ── Upload-Identitaet ────────────────────────────────────────────────────────

def test_dataset_id_order_independent_and_deduplicated():
    a = {'name': 'a.xml', 'digest': 'd1', 'kind': 'xml'}
    b = {'name': 'b.xml', 'digest': 'd2', 'kind': 'xml'}
    assert (ui_model.build_dataset_id([a, b])
            == ui_model.build_dataset_id([b, a]))
    assert (ui_model.build_dataset_id([a])
            != ui_model.build_dataset_id([a, b]))
    # Dateityp ist Teil der Identitaet
    a_csv = dict(a, kind='csv')
    assert (ui_model.build_dataset_id([a])
            != ui_model.build_dataset_id([a_csv]))


def test_validate_uploads_drops_identical_digests():
    result = ui_model.validate_uploads([
        {'name': 'q1.xml', 'digest': 'x', 'kind': 'xml'},
        {'name': 'q1_copy.xml', 'digest': 'x', 'kind': 'xml'},
        {'name': 'q2.xml', 'digest': 'y', 'kind': 'xml'},
    ])
    assert [f['name'] for f in result['files']] == ['q1.xml', 'q2.xml']
    assert result['dropped_duplicates'] == ['q1_copy.xml']
    # dataset_id aus der deduplizierten Liste == Identitaet ohne Duplikat
    dedup_id = ui_model.build_dataset_id(result['files'])
    clean_id = ui_model.build_dataset_id([
        {'name': 'q1.xml', 'digest': 'x', 'kind': 'xml'},
        {'name': 'q2.xml', 'digest': 'y', 'kind': 'xml'},
    ])
    assert dedup_id == clean_id
    # Multiplizitaet lebt nur in der raw_upload_id — identisch dedupliziert,
    # verschieden roh waere hier gleich, weil raw ueber sortierte Tokens geht;
    # entscheidend: raw_id unterscheidet sich von der dataset_id-Definition
    # nicht strukturell, sondern durch die Eingabemenge.
    raw_with_dup = ui_model.build_raw_upload_id([
        {'name': 'q1.xml', 'digest': 'x', 'kind': 'xml'},
        {'name': 'q1_copy.xml', 'digest': 'x', 'kind': 'xml'},
    ])
    raw_without = ui_model.build_raw_upload_id([
        {'name': 'q1.xml', 'digest': 'x', 'kind': 'xml'},
    ])
    assert raw_with_dup != raw_without


def test_upload_dataset_updates_xml_and_csv_independently():
    xml_data = b'<FlexQueryResponse />'
    xml_entry = {
        'name': 'steuerjahr.xml',
        'digest': ui_model.file_digest(xml_data),
        'kind': 'xml',
        'data': xml_data,
    }
    csv_data = b'Statement,Header\n'
    csv_entry = {
        'name': 'kontrollbericht.csv',
        'digest': ui_model.file_digest(csv_data),
        'kind': 'csv',
        'data': csv_data,
    }

    # XML zuerst, CSV spaeter: der CSV-Callback darf das XML nicht verlieren.
    xml_only = ui_model.update_upload_dataset({}, xml_entries=[xml_entry])
    with_csv = ui_model.update_upload_dataset(xml_only, csv_entry=csv_entry)
    assert with_csv['files'] == [xml_entry]
    assert with_csv['dataset_id'] == xml_only['dataset_id']
    assert with_csv['csv'] == csv_entry

    # CSV zuerst, XML spaeter: der XML-Callback muss die CSV erhalten.
    csv_only = ui_model.update_upload_dataset({}, csv_entry=csv_entry)
    complete = ui_model.update_upload_dataset(
        csv_only, xml_entries=[xml_entry])
    assert complete['files'] == [xml_entry]
    assert complete['csv'] == csv_entry

    # Nur eine ausdruecklich leere Auswahl entfernt den jeweiligen Datentyp.
    no_xml = ui_model.update_upload_dataset(complete, xml_entries=[])
    assert no_xml['files'] == []
    assert no_xml['csv'] == csv_entry
    no_csv = ui_model.update_upload_dataset(complete, csv_entry=None)
    assert no_csv['files'] == [xml_entry]
    assert no_csv['csv'] is None


def test_upload_dataset_deduplicates_xml_without_mutating_input():
    payload = b'<FlexQueryResponse />'
    entries = [
        {'name': 'a.xml', 'digest': ui_model.file_digest(payload),
         'kind': 'xml', 'data': payload},
        {'name': 'copy.xml', 'digest': ui_model.file_digest(payload),
         'kind': 'xml', 'data': payload},
    ]
    before = copy.deepcopy(entries)
    result = ui_model.update_upload_dataset({}, xml_entries=entries)
    assert [entry['name'] for entry in result['files']] == ['a.xml']
    assert result['dropped_duplicates'] == ['copy.xml']
    assert entries == before


def test_append_xml_uploads_preserves_existing_dataset_and_csv():
    current_data = b'<FlexQueryResponse id="current" />'
    history_data = b'<FlexQueryResponse id="history" />'
    current_entry = {
        'name': 'steuerjahr.xml',
        'digest': ui_model.file_digest(current_data),
        'kind': 'xml',
        'data': current_data,
    }
    history_entry = {
        'name': 'vorjahr.xml',
        'digest': ui_model.file_digest(history_data),
        'kind': 'xml',
        'data': history_data,
    }
    csv_entry = {
        'name': 'kontrollbericht.csv',
        'digest': ui_model.file_digest(b'csv'),
        'kind': 'csv',
        'data': b'csv',
    }
    existing = ui_model.update_upload_dataset(
        {}, xml_entries=[current_entry], csv_entry=csv_entry)
    before = copy.deepcopy(existing)

    appended = ui_model.append_xml_uploads(existing, [history_entry])
    assert [entry['name'] for entry in appended['files']] == [
        'steuerjahr.xml', 'vorjahr.xml']
    assert appended['csv'] == csv_entry
    assert appended['dataset_id'] != existing['dataset_id']
    assert existing == before, "Der vorhandene Upload-Snapshot wurde mutiert"

    # Ein noch gemounteter Sidebar-Eintrag darf weder dupliziert noch als
    # erneuter Duplikat-Prueffall gemeldet werden.
    repeated = ui_model.append_xml_uploads(appended, [history_entry])
    assert repeated['files'] == appended['files']
    assert repeated['dataset_id'] == appended['dataset_id']
    assert repeated['dropped_duplicates'] == []


def test_find_period_overlaps():
    def acct(*ranges):
        return {'U1': [
            {'from_date': f, 'to_date': t, 'name': f"x{i}.xml"}
            for i, (f, t) in enumerate(ranges)
        ]}

    # Disjunkte Quartale: ok
    assert ui_model.find_period_overlaps(acct(
        ('2025-01-01', '2025-03-31'), ('2025-04-01', '2025-06-30'),
    )) == []
    # Ueberlappung im selben Jahr: Fehler
    overlaps = ui_model.find_period_overlaps(acct(
        ('2025-01-01', '2025-06-30'), ('2025-04-01', '2025-12-31'),
    ))
    assert len(overlaps) == 1 and overlaps[0]['account_id'] == 'U1'
    # Verschiedene Jahre (History-Modus): nie ein Fehler
    assert ui_model.find_period_overlaps(acct(
        ('2024-01-01', '2024-12-31'), ('2025-01-01', '2025-12-31'),
    )) == []


# ── Cache-Protokoll ──────────────────────────────────────────────────────────

def test_input_key_changes_with_every_compute_parameter():
    base = ui_model.build_input_key('d1', 'c1', True, False, ['A'])
    assert base == ui_model.build_input_key('d1', 'c1', True, False, ['A'])
    assert base != ui_model.build_input_key('d2', 'c1', True, False, ['A'])
    assert base != ui_model.build_input_key('d1', 'c2', True, False, ['A'])
    assert base != ui_model.build_input_key('d1', 'c1', False, False, ['A'])
    assert base != ui_model.build_input_key('d1', 'c1', True, True, ['A'])
    assert base != ui_model.build_input_key('d1', 'c1', True, False, [])
    # Reihenfolge der SO-Overrides ist kanonisch
    assert (ui_model.build_input_key('d1', 'c1', True, False, ['B', 'A'])
            == ui_model.build_input_key('d1', 'c1', True, False, ['A', 'B']))


def test_view_key_changes_with_every_domain_input():
    toggles = ui_model.default_toggles()
    base = ui_model.build_view_key('ik', toggles, {}, [])
    assert base == ui_model.build_view_key('ik', dict(toggles), {}, [])
    assert base != ui_model.build_view_key('ik2', toggles, {}, [])
    for view_toggle in ('zufluss', 'invstg', 'tageskurs', 'variante_b'):
        flipped = dict(toggles)
        flipped[view_toggle] = not flipped[view_toggle]
        assert base != ui_model.build_view_key('ik', flipped, {}, []), \
            f"View-Toggle {view_toggle} muss den view_key aendern"
    # Fondsbestaetigung aendert den Key (Export-Cache-Schutz)
    assert base != ui_model.build_view_key(
        'ik', toggles, {'XX0000000001': 0.30}, ['XX0000000001'])
    # Kanonische Reihenfolge der Overrides
    two = {'A': 0.30, 'B': 0.15}
    assert (ui_model.build_view_key('ik', toggles, two, ['A', 'B'])
            == ui_model.build_view_key('ik', toggles,
                                       dict(reversed(list(two.items()))),
                                       ['B', 'A']))


def test_commit_guard_and_snapshot_currency():
    ok = ui_model.should_commit_snapshot('k1', 3, 'k1', 3)
    assert ok is True
    assert ui_model.should_commit_snapshot('k1', 4, 'k1', 3) is False
    assert ui_model.should_commit_snapshot('k2', 3, 'k1', 3) is False
    assert ui_model.should_commit_snapshot(
        'k1', 3, 'k1', 3, schema_version=ui_model.SCHEMA_VERSION + 1) is False

    snap = {'status': 'ok', 'schema_version': ui_model.SCHEMA_VERSION,
            'input_key': 'k1'}
    assert ui_model.snapshot_is_current(snap, 'k1') is True
    assert ui_model.snapshot_is_current(snap, 'k2') is False
    assert ui_model.snapshot_is_current(dict(snap, status='error'), 'k1') is False
    assert ui_model.snapshot_is_current(
        dict(snap, schema_version=0), 'k1') is False
    assert ui_model.snapshot_is_current(None, 'k1') is False


# ── Toggle-Verfuegbarkeit ────────────────────────────────────────────────────

def test_tageskurs_availability_keeps_netto_null_case_inactive():
    # Netto null, aber gegenlaeufige Topf-Korrekturen: Sichtbarkeitsbedingung
    # bleibt |fx_correction_total| > 0.01 — sonst wuerden Z20/22/23 erstmals
    # veraendert (separater fachlicher Beschluss, siehe PLAN.md).
    report = make_report(
        fx_correction_total=0.005,
        fx_correction_by_topf={'Topf1': 100.0, 'Topf2': -100.0},
        fx_corr_gain_adj={'Topf1': 100.0},
        fx_corr_loss_adj={'Topf2': -100.0},
    )
    availability = ui_model.toggle_availability(report)
    assert availability['tageskurs'] is False
    toggles = ui_model.effective_toggles(ui_model.default_toggles(),
                                         availability)
    assert toggles['tageskurs'] is False
    final = ui_model.build_final_values(report, toggles)
    assert abs(final['zeile_20'] - 1000.0) < 1e-9
    assert abs(final['zeile_22'] - 100.0) < 1e-9
    assert abs(final['zeile_23'] - 200.0) < 1e-9


def test_variante_b_availability_from_pre_override_zeile_7():
    report = make_report(
        zeile_7_kapitalertraege_mit_inlaendischem_steuerabzug_eur=100.0,
        zeile_37_kapitalertragsteuer_eur=25.0,
        zeile_38_solidaritaetszuschlag_eur=1.375,
    )
    availability = ui_model.toggle_availability(report)
    assert availability['variante_b'] is True
    # Aktivierte Variante B nullt zeile_7 im final-Dict — die Verfuegbarkeit
    # muss trotzdem True bleiben (sie kommt aus dem Compute-Snapshot).
    toggles = ui_model.effective_toggles(
        dict(ui_model.default_toggles(), variante_b=True), availability)
    final = ui_model.build_final_values(report, toggles)
    assert final['zeile_7'] == 0
    assert ui_model.toggle_availability(report)['variante_b'] is True


# ── build_final_values ───────────────────────────────────────────────────────

def test_final_values_raw_when_all_toggles_off():
    report = make_report()
    final = ui_model.build_final_values(report, all_toggles_off())
    assert abs(final['zeile_19'] - 1310.0) < 1e-9
    assert abs(final['zeile_20'] - 1000.0) < 1e-9
    assert abs(final['topf_1'] - 800.0) < 1e-9
    assert abs(final['topf_2'] - 510.0) < 1e-9


def test_final_values_zufluss_subtracts_cross_year_premium():
    report = make_report(audit={
        'stillhalter_details': [{'is_cross_year': True, 'premium_eur': 40.0}],
        'cross_year_premium_eur': 40.0,
    })
    toggles = ui_model.effective_toggles(
        ui_model.default_toggles(), ui_model.toggle_availability(report))
    assert toggles['zufluss'] is True
    final = ui_model.build_final_values(report, toggles)
    assert abs(final['zeile_19'] - (1310.0 - 40.0)) < 1e-9
    assert abs(final['topf_2'] - (510.0 - 40.0)) < 1e-9
    assert abs(final['options_gain'] - (500.0 - 40.0)) < 1e-9


def test_final_values_invstg_off_reintegrates_etf():
    report = make_report(kap_inv={
        'etf_by_isin': {'XX1': {'gain': 300.0}},
        'etf_gain_raw_eur': 300.0,
        'etf_loss_raw_eur': -50.0,
        'etf_dividends_raw_eur': 30.0,
        'etf_net_taxable_eur': 175.0,
        'etf_wht_eur': 4.5,
    })
    toggles = dict(all_toggles_off())
    final = ui_model.build_final_values(report, toggles)
    # Reintegration: Topf 1 + ETF-Netto, Z20/Z23 brutto, Dividenden in Topf 2
    assert abs(final['topf_1'] - (800.0 + 250.0)) < 1e-9
    assert abs(final['zeile_20'] - (1000.0 + 300.0)) < 1e-9
    assert abs(final['zeile_23'] - (200.0 + 50.0)) < 1e-9
    assert abs(final['dividends'] - (100.0 + 30.0)) < 1e-9
    assert abs(final['zeile_19'] - (1310.0 + 250.0 + 30.0)) < 1e-9
    assert final['etf_net_taxable'] == 0
    assert final['etf_wht'] == 0


def test_final_values_variante_b_keeps_invariant():
    report = make_report(
        zeile_7_kapitalertraege_mit_inlaendischem_steuerabzug_eur=100.0,
        zeile_37_kapitalertragsteuer_eur=25.0,
        zeile_38_solidaritaetszuschlag_eur=1.375,
    )
    toggles = dict(all_toggles_off(), variante_b=True)
    final = ui_model.build_final_values(report, toggles)
    assert final['zeile_7'] == 0
    assert final['zeile_37'] == 0
    assert final['zeile_38'] == 0
    assert abs(final['zeile_19'] - (1310.0 + 100.0)) < 1e-9
    assert abs(final['quellensteuer'] - (15.0 + 26.375)) < 1e-9
    # Hero-Invariante bleibt: zeile_19 = topf_1 + topf_2
    assert abs(final['zeile_19'] - (final['topf_1'] + final['topf_2'])) < 1e-6


def test_final_values_wht_override_adjusts_zeile_41():
    report = make_report(kap_inv={
        'etf_by_isin': {'XX1': {'gain': 0.0}},
        'etf_net_taxable_eur': 0.0,
        'etf_wht_eur': 10.0,
        'etf_wht_anrechenbar_eur': 7.0,
    })
    toggles = ui_model.effective_toggles(
        ui_model.default_toggles(), ui_model.toggle_availability(report))
    base = ui_model.build_final_values(report, toggles)
    overridden = ui_model.build_final_values(
        report, toggles, etf_wht_override=9.0)
    assert abs(overridden['etf_wht'] - 9.0) < 1e-9
    assert abs(
        (overridden['quellensteuer'] - base['quellensteuer']) - (9.0 - 7.0)
    ) < 1e-9


# ── ETF-Overrides und kontoweise WHT ─────────────────────────────────────────

def make_kap_inv_with_unknown():
    return {
        'etf_by_isin': {
            'XX0000000001': {
                'ticker': 'FAKE', 'gain': 100.0, 'loss': -50.0, 'div': 20.0,
                'gain_taxable': 100.0, 'loss_taxable': -50.0,
                'div_taxable': 20.0, 'tfs_rate': 0.0, 'wht': 3.0,
            },
        },
        'etf_unknown_isins': ['XX0000000001'],
        'etf_net_taxable_eur': 70.0,
        'etf_wht_eur': 3.0,
    }


def test_apply_etf_overrides_math_and_unconfirmed():
    kap_inv = make_kap_inv_with_unknown()
    fx_by_isin = {'XX0000000001': {'raw_delta': 10.0, 'taxable_delta': 10.0}}
    result = ui_model.apply_etf_overrides(
        kap_inv, fx_by_isin, {'XX0000000001': 0.30}, tageskurs_active=True)
    assert result['unconfirmed_unknown_isins'] == []
    # Deltas: G/V/Div je *0.7 minus alt + Tageskurs 7-10
    expected = (70.0 - 100.0) + (-35.0 + 50.0) + (14.0 - 20.0) + (7.0 - 10.0)
    assert abs(result['net_taxable_delta'] - expected) < 1e-9
    assert abs(result['tageskurs_taxable_delta'] - (-3.0)) < 1e-9
    entry = result['kap_inv']['etf_by_isin']['XX0000000001']
    assert entry['classification'] == 'aktienfonds'
    assert abs(entry['gain_taxable'] - 70.0) < 1e-9
    # Ohne Bestaetigung bleibt die ISIN offen
    result2 = ui_model.apply_etf_overrides(kap_inv, fx_by_isin, {}, True)
    assert result2['unconfirmed_unknown_isins'] == ['XX0000000001']


def test_recalc_wht_per_account_sums_per_pool():
    pool_a = {'XA': {'wht': 10.0, 'tfs_rate': 0.30, 'wht_events': []}}
    pool_b = {'XB': {'wht': 4.0, 'tfs_rate': 0.0, 'wht_events': []}}
    merged = {}
    merged.update(copy.deepcopy(pool_a))
    merged.update(copy.deepcopy(pool_b))
    result = ui_model.recalc_wht_per_account([pool_a, pool_b], merged, False)
    individual = sum(
        calculate_tax_report.calculate_kap_inv_wht_for_mode(
            copy.deepcopy(p), dba_wht_beta_enabled=False,
        )['creditable_tax_eur']
        for p in (pool_a, pool_b)
    )
    assert abs(result['creditable_tax_eur'] - individual) < 1e-9
    # Overrides aus dem gemergten Pool werden auf die Konto-Kopien gespiegelt
    # (Vorzeichenkonvention: Legacy liefert signed-cash, wht +10/TFS 30% -> -7;
    # ohne TFS -> -10).
    merged_override = copy.deepcopy(merged)
    merged_override['XA']['tfs_rate'] = 0.0
    result_override = ui_model.recalc_wht_per_account(
        [pool_a, pool_b], merged_override, False)
    assert abs(result_override['creditable_tax_eur'] - (-14.0)) < 1e-9
    assert abs(result['creditable_tax_eur'] - (-11.0)) < 1e-9


# ── Immutabilitaets-Guard ────────────────────────────────────────────────────

def full_trigger_report():
    return make_report(
        has_trade_price=False,
        xml_has_fx_data=False,
        fx_source='none',
        partnership_tax_items={'XP': {'ticker': 'USO'}},
        classification_review_items=[{'isin': 'XR', 'ticker': 'GLD'}],
        anlage_so={'by_isin': {'XS': {'total': 1.0}}, 'unknown_gain': 5.0,
                   'unknown_loss': 0.0},
        anlage_so_overrides_applied=[],
        fx_option_a_meta={'open_rows_with_pnl': [{'date': '2025-01-02'}]},
        kap_inv=make_kap_inv_with_unknown(),
        fx_correction_kap_inv_by_isin={},
        audit={
            'stillhalter_details': [
                {'is_cross_year': True, 'premium_eur': 40.0}],
            'cross_year_premium_eur': 40.0,
            'stillhalter_unmatched': [
                {'symbol': 'AAA', 'expiry': '2025-01-17'},
                {'symbol': 'BBB', 'type': 'cross_year'},
            ],
            'zufluss_unmatched': [{'symbol': 'CCC'}],
            'unrouted_asset_categories': [
                {'category': 'IOPT', 'count': 1, 'pnl_eur': 5.0,
                 'symbols': ['X']}],
            'unhandled_activity_codes': [
                {'code': 'TTAX', 'count': 1, 'amount_eur': -3.0,
                 'descriptions': []}],
            'transaction_tax': {
                'applied_count': 2, 'applied_eur': 0.05,
                'deferred_count': 1, 'deferred_eur': 0.02,
                'already_in_trade_count': 1, 'details': [],
            },
            'fx_rate_parse_failures': {'funds': 1, 'trades': 0},
            'occ_rename_matches': [{'sell_symbol': 'MMM', 'sell_date': 'x',
                                    'close_symbol': 'MMM1', 'close_date': 'y',
                                    'quantity': 1}],
            'underlying_symbol_aliases': {'CON': ['CONd']},
            'stillhalter_corrections_dropped': [{'symbol': 'DDD'}],
            'stillhalter_open_short': [{'symbol': 'EEE'}],
        },
    )


def full_trigger_context():
    return {
        'multi_stmt_files': [{'name': 'multi.xml', 'account_ids': ['U1', 'U2']}],
        'accounts_skipped': ['U9 (nur bis 2023)'],
        'dropped_duplicates': ['dup.xml'],
        'csv_disabled_multi_account': True,
        'plausibility_mismatch': True,
        'kap_inv_form': {
            'warnings': ['Testwarnung'],
            'negative_distribution_details': [
                {'isin': 'XX1', 'paid_distribution_eur': -5.0}],
            'lines': [{'kind': 'sale', 'line': 14}],
        },
        'wht_review_items': [{'status': 'dba_unverified',
                              'creditable_tax_eur': 1.0}],
        'dba_beta_enabled': True,
        'invstg_on': True,
    }


NOTICE_REGISTRY = {
    'stillhalter_unmatched', 'stillhalter_prior_lot_unmatched',
    'zufluss_unmatched', 'unrouted_asset_categories',
    'unhandled_activity_codes', 'partnership_blocked',
    'transaction_tax_processed',
    'classification_review', 'etf_unknown_classification',
    'kap_inv_form_warning', 'kap_inv_paid_distributions',
    'kap_inv_sale_preliminary', 'dba_wht_review', 'fx_open_rows_with_pnl',
    'missing_trade_price', 'missing_fx_transactions', 'fx_from_csv',
    'fx_rate_parse_failures', 'so_unknown_holding_period',
    'multi_statement_file', 'accounts_skipped', 'duplicate_uploads_dropped',
    'csv_disabled_multi_account', 'plausibility_mismatch',
    'occ_rename_matches', 'underlying_symbol_aliases',
    'stillhalter_corrections_dropped', 'stillhalter_open_short',
}


def test_collect_notices_registry_is_complete():
    report = full_trigger_report()
    ctx = full_trigger_context()
    ids = {n['id'] for n in ui_model.collect_notices(report, ctx)}
    # fx_from_csv und missing_fx_transactions schliessen sich aus
    expected = NOTICE_REGISTRY - {'fx_from_csv'}
    missing = expected - ids
    unexpected = ids - expected
    assert not missing, f"Notices fehlen (still verschwundene Warnpfade): {missing}"
    assert not unexpected, f"Unbekannte Notice-IDs (Registry ergaenzen): {unexpected}"
    # CSV-Variante
    report_csv = dict(report, fx_source='csv')
    ids_csv = {n['id'] for n in ui_model.collect_notices(report_csv, ctx)}
    assert 'fx_from_csv' in ids_csv
    assert 'missing_fx_transactions' not in ids_csv
    # Jede Notice traegt Klasse/Schweregrad/Ziel
    for n in ui_model.collect_notices(report, ctx):
        assert n['class'] in ui_model.NOTICE_CLASSES
        assert n['severity'] in ('kritisch', 'normal')


def test_builders_do_not_mutate_inputs():
    # Genau hier entstand die fruehere In-place-Mutation (Einzelkonto-Report
    # aus merge_report_data). Single- UND Multi-Account-Formen pruefen.
    report = full_trigger_report()
    ctx = full_trigger_context()
    pools = [
        {'XA': {'wht': 10.0, 'tfs_rate': 0.30, 'wht_events': []}},
        {'XB': {'wht': 4.0, 'tfs_rate': 0.0, 'wht_events': []}},
    ]
    domain = {
        'toggles': ui_model.default_toggles(),
        'etf_overrides': {'XX0000000001': 0.30},
        'dba_beta_enabled': False,
    }
    report_before = copy.deepcopy(report)
    ctx_before = copy.deepcopy(ctx)
    pools_before = copy.deepcopy(pools)
    domain_before = copy.deepcopy(domain)

    availability = ui_model.toggle_availability(report)
    toggles = ui_model.effective_toggles(domain['toggles'], availability)
    ui_model.build_final_values(report, toggles)
    ui_model.apply_etf_overrides(
        report['kap_inv'], report.get('fx_correction_kap_inv_by_isin', {}),
        domain['etf_overrides'], True)
    ui_model.recalc_wht_per_account(pools, pools[0], False)
    ui_model.collect_notices(report, ctx)
    ui_model.build_view_model(report, pools, domain, ctx)

    assert report == report_before, "Compute-Snapshot wurde mutiert"
    assert ctx == ctx_before, "Kontext wurde mutiert"
    assert pools == pools_before, "Per-Account-Pools wurden mutiert"
    assert domain == domain_before, "Domain-State wurde mutiert"


# ── View-Model und Navigation ────────────────────────────────────────────────

def test_view_model_pages_and_notice_counts():
    report = full_trigger_report()
    vm = ui_model.build_view_model(report, [], {
        'toggles': ui_model.default_toggles(),
        'etf_overrides': {},
        'dba_beta_enabled': False,
    }, full_trigger_context())
    assert 'kap_inv' in vm['visible_pages']       # ETF-Daten vorhanden
    assert 'anlage_so' in vm['visible_pages']     # SO-Daten vorhanden
    assert vm['notice_counts']['prueffaelle'] > 0
    assert vm['notice_counts']['kritisch'] > 0
    assert vm['view_key']

    plain = make_report()
    vm2 = ui_model.build_view_model(plain, [], {
        'toggles': ui_model.default_toggles(),
        'etf_overrides': {},
        'dba_beta_enabled': False,
    }, {})
    assert 'kap_inv' not in vm2['visible_pages']
    assert 'anlage_so' not in vm2['visible_pages']
    assert 'prueffaelle' in vm2['visible_pages']  # bleibt immer sichtbar


def test_per_account_finals_include_active_display_corrections():
    corrected = make_report(
        fx_correction_total=15.0,
        fx_correction_by_topf={'Topf1': 10.0, 'Topf2': 5.0},
        fx_corr_gain_adj={'Topf1': 10.0},
        audit={
            'stillhalter_details': [
                {'is_cross_year': True, 'premium_eur': 40.0}],
            'cross_year_premium_eur': 40.0,
        },
    )
    plain = make_report()
    domain = {
        'toggles': ui_model.default_toggles(),
        'etf_overrides': {},
        'dba_beta_enabled': False,
    }
    finals = ui_model.build_per_account_finals(
        [corrected, plain], [{}, {}], domain)

    assert len(finals) == 2
    assert abs(finals[0]['topf_1'] - 810.0) < 1e-9
    assert abs(finals[0]['topf_2'] - 475.0) < 1e-9
    assert abs(finals[0]['zeile_19'] - 1285.0) < 1e-9
    assert abs(finals[1]['zeile_19'] - 1310.0) < 1e-9
    assert corrected['zeile_19_netto_eur'] == 1310.0


def test_per_account_finals_reject_misaligned_wht_pools():
    try:
        ui_model.build_per_account_finals(
            [make_report(), make_report()], [{}],
            {'toggles': ui_model.default_toggles()},
        )
    except ValueError as exc:
        assert 'nicht ausgerichtet' in str(exc)
    else:
        raise AssertionError("Fehlender Pool muss sichtbar fehlschlagen")


def test_normalize_nav_falls_back_to_overview():
    visible = ['overview', 'kap', 'prueffaelle', 'rechenwege', 'export']
    assert ui_model.normalize_nav('kap', visible) == 'kap'
    assert ui_model.normalize_nav('kap_inv', visible) == 'overview'
    assert ui_model.normalize_nav('unbekannt', visible) == 'overview'
    assert ui_model.normalize_nav(None, visible) == 'overview'


def test_toggle_inventory_has_all_six_options():
    keys = {t['key'] for t in ui_model.TOGGLE_INVENTORY}
    assert keys == {'zufluss', 'invstg', 'tageskurs', 'fx_margin',
                    'dba_beta', 'variante_b'}
    scopes = {t['key']: t['scope'] for t in ui_model.TOGGLE_INVENTORY}
    assert scopes['fx_margin'] == 'compute'
    assert scopes['dba_beta'] == 'compute'
    assert scopes['variante_b'] == 'view'


if __name__ == '__main__':
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn):
            try:
                fn()
                print(f"  {name}: OK")
            except AssertionError as exc:
                failures += 1
                print(f"  {name}: FAIL - {exc}")
    if failures:
        raise SystemExit(1)
    print("OK: ui_model Schicht (Keys, Guard, Builder, Notices, Nav)")
