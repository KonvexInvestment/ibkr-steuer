"""Pure view-model layer for the Streamlit UI (app.py) and run_tests.py.

This module is the single source of truth for everything the presentation
layer derives from a computed ``report_data`` snapshot:

- canonical identity of uploaded datasets (``file_digest``/``build_dataset_id``)
- upload validation (duplicate files, overlapping periods)
- the compute-cache protocol (``build_input_key``, ``should_commit_snapshot``)
- the toggle inventory (all six tax-relevant options)
- the toggle-adjusted ``final`` values (``build_final_values``)
- manual ETF classification overrides (``apply_etf_overrides``)
- the complete notice inventory (``collect_notices``)
- the assembled view model (``build_view_model``) and navigation helpers

It must stay importable without Streamlit (stdlib + tax core only) so that
``run_tests.py`` exercises exactly the arithmetic the GUI shows. Renderers
read from the view model; they never derive tax values themselves.

Immutability contract: no function in this module mutates its inputs.
``tests/test_ui_model.py`` guards this with deep-copy comparisons.
"""

import copy
import hashlib
import threading

import calculate_tax_report

# Bump when the snapshot payload layout changes; stale session_state snapshots
# from an older code version are then recomputed instead of rendered.
SCHEMA_VERSION = 1
# Bump when the view-model/export layout changes (part of the view key).
VIEW_SCHEMA_VERSION = 1

# Owned here (imported module, exactly one instance in sys.modules) because
# Streamlit creates a fresh __main__ per script run; a lock global in app.py
# would not be shared between parallel runs. Serialises the stdout-redirect
# plus compute block (redirect_stdout mutates process-global sys.stdout).
REDIRECT_LOCK = threading.Lock()


# ── Upload identity and validation ───────────────────────────────────────────

def file_digest(data):
    """Canonical content digest of one uploaded file (SHA-256 hex)."""
    return hashlib.sha256(data).hexdigest()


def validate_uploads(entries):
    """Validate the raw upload list before any parsing.

    ``entries``: list of {'name': str, 'digest': str, 'kind': 'xml'|'csv'}.
    Identical digests are never intentional (same file twice) and are dropped
    with a visible notice. Returns {'files': [...], 'dropped_duplicates': [...]}
    where ``files`` keeps the first occurrence of each digest in input order.
    """
    seen = set()
    files = []
    dropped = []
    for entry in entries:
        key = (entry['kind'], entry['digest'])
        if key in seen:
            dropped.append(entry['name'])
            continue
        seen.add(key)
        files.append(dict(entry))
    return {'files': files, 'dropped_duplicates': dropped}


def build_dataset_id(validated_files):
    """Dataset identity over the deduplicated, validated file list.

    Length-prefixed, sorted sequence of per-file ``kind:digest`` tokens.
    Identical effective datasets get identical ids regardless of upload
    order; raw-upload multiplicity lives only in ``build_raw_upload_id``.
    """
    tokens = sorted(
        f"{entry['kind']}:{entry['digest']}" for entry in validated_files
    )
    h = hashlib.sha256()
    for token in tokens:
        h.update(f"{len(token)}:".encode('ascii'))
        h.update(token.encode('ascii'))
    return h.hexdigest()


def build_raw_upload_id(entries):
    """Diagnostic identity over the raw upload list including duplicates."""
    tokens = sorted(
        f"{entry['kind']}:{entry['digest']}" for entry in entries
    )
    h = hashlib.sha256()
    for token in tokens:
        h.update(f"{len(token)}:".encode('ascii'))
        h.update(token.encode('ascii'))
    return h.hexdigest()


_UPLOAD_UNCHANGED = object()


def update_upload_dataset(existing=None, *, xml_entries=_UPLOAD_UNCHANGED,
                          csv_entry=_UPLOAD_UNCHANGED):
    """Return an updated immutable upload snapshot.

    XML and CSV uploaders are separate controls which can be mounted in
    different parts of the Streamlit page.  A callback from one control must
    therefore never interpret the absent sibling control as an explicit
    deletion.  ``_UPLOAD_UNCHANGED`` distinguishes that case from an empty
    XML list or ``None`` CSV, both of which intentionally remove data.

    ``xml_entries`` contains complete in-memory entries including ``data``;
    duplicate XML contents are collapsed while the first file is retained.
    """
    current = existing or {}

    if xml_entries is _UPLOAD_UNCHANGED:
        files = [dict(entry) for entry in current.get('files', [])]
        dropped = list(current.get('dropped_duplicates', []))
        dataset_id = current.get('dataset_id')
        raw_upload_id = current.get('raw_upload_id')
        if dataset_id is None:
            dataset_id = build_dataset_id(files)
        if raw_upload_id is None:
            raw_upload_id = build_raw_upload_id(files)
    else:
        raw_entries = [dict(entry) for entry in (xml_entries or [])]
        metadata = [
            {
                'name': entry['name'],
                'digest': entry['digest'],
                'kind': entry['kind'],
            }
            for entry in raw_entries
        ]
        validated = validate_uploads(metadata)
        kept_keys = {
            (entry['kind'], entry['digest'])
            for entry in validated['files']
        }
        files = []
        seen = set()
        for entry in raw_entries:
            key = (entry['kind'], entry['digest'])
            if key in kept_keys and key not in seen:
                seen.add(key)
                files.append(entry)
        dropped = list(validated['dropped_duplicates'])
        dataset_id = build_dataset_id(files)
        raw_upload_id = build_raw_upload_id(raw_entries)

    if csv_entry is _UPLOAD_UNCHANGED:
        csv_value = current.get('csv')
        csv_value = dict(csv_value) if csv_value is not None else None
    else:
        csv_value = dict(csv_entry) if csv_entry is not None else None

    return {
        'files': files,
        'dropped_duplicates': dropped,
        'dataset_id': dataset_id,
        'raw_upload_id': raw_upload_id,
        'csv': csv_value,
    }


def append_xml_uploads(existing, xml_entries):
    """Add XML entries without removing files already in the snapshot.

    The persistent sidebar uploader mounts empty after the start uploader has
    created a dataset.  Its selection therefore represents additions, not a
    complete replacement.  Entries already present by kind/digest are ignored
    here so a still-mounted sidebar selection is not reported as a new
    duplicate on every subsequent addition.
    """
    current = existing or {}
    current_files = [dict(entry) for entry in current.get('files', [])]
    current_keys = {
        (entry.get('kind'), entry.get('digest')) for entry in current_files
    }
    additions = [
        dict(entry) for entry in (xml_entries or [])
        if (entry.get('kind'), entry.get('digest')) not in current_keys
    ]
    return update_upload_dataset(
        current, xml_entries=current_files + additions,
    )


def find_period_overlaps(accounts):
    """Detect overlapping same-year reporting periods per account.

    ``accounts``: classify_xmls() result (accountId -> list of xml entries with
    ``from_date``/``to_date``/``name``). Multiple XMLs of the same account in
    the same year are only valid as disjoint quarterly intervals — an overlap
    would double PnL in the summing quarterly merge. Different years (history
    mode) are always fine. Returns a list of error dicts.
    """
    errors = []
    for account_id, xmls in accounts.items():
        by_year = {}
        for xml in xmls:
            frm = xml.get('from_date') or ''
            to = xml.get('to_date') or ''
            if not frm or not to:
                continue
            if frm[:4] != to[:4]:
                continue
            by_year.setdefault(frm[:4], []).append(xml)
        for year, group in by_year.items():
            if len(group) < 2:
                continue
            ordered = sorted(group, key=lambda x: x['from_date'])
            for prev, cur in zip(ordered, ordered[1:]):
                if cur['from_date'] <= prev['to_date']:
                    errors.append({
                        'account_id': account_id,
                        'year': year,
                        'files': [prev.get('name', '?'), cur.get('name', '?')],
                        'periods': [
                            f"{prev['from_date']}..{prev['to_date']}",
                            f"{cur['from_date']}..{cur['to_date']}",
                        ],
                    })
    return errors


# ── Compute cache protocol ───────────────────────────────────────────────────

def build_input_key(dataset_id, csv_digest, fx_margin_correction_enabled,
                    dba_wht_beta_enabled, anlage_so_overrides):
    """Key over every parameter that reaches extraction/calculate_tax()."""
    parts = [
        f"schema={SCHEMA_VERSION}",
        f"dataset={dataset_id}",
        f"csv={csv_digest or ''}",
        f"fx_margin={bool(fx_margin_correction_enabled)}",
        f"dba_beta={bool(dba_wht_beta_enabled)}",
        "so=" + ",".join(sorted(anlage_so_overrides or [])),
    ]
    return hashlib.sha256("|".join(parts).encode('utf-8')).hexdigest()


def should_commit_snapshot(requested_key, requested_generation,
                           computed_key, computed_generation,
                           schema_version=SCHEMA_VERSION):
    """Atomic-commit guard against fast reruns.

    A compute run may only commit its result if the request it served is
    still the current one (same key AND same generation) and the code schema
    has not changed underneath it. A stale run must never overwrite a newer
    request's snapshot.
    """
    if schema_version != SCHEMA_VERSION:
        return False
    if computed_key != requested_key:
        return False
    if computed_generation != requested_generation:
        return False
    return True


def snapshot_is_current(snapshot, requested_key):
    """Whether a stored snapshot may be rendered for the requested key."""
    if not snapshot:
        return False
    if snapshot.get('status') != 'ok':
        return False
    if snapshot.get('schema_version') != SCHEMA_VERSION:
        return False
    return snapshot.get('input_key') == requested_key


# ── Toggle inventory ─────────────────────────────────────────────────────────
# All six tax-relevant options. 'compute' toggles are part of the input_key
# (they change calculate_tax()); 'view' toggles only change the view key.
TOGGLE_INVENTORY = (
    {'key': 'zufluss', 'label': 'Zuflussprinzip', 'scope': 'view',
     'default': True, 'visible_when': 'cross_year_details'},
    {'key': 'invstg', 'label': 'InvStG/KAP-INV', 'scope': 'view',
     'default': True, 'visible_when': 'etf_data'},
    {'key': 'tageskurs', 'label': 'Tageskurs-Methode', 'scope': 'view',
     'default': True, 'visible_when': 'abs(fx_correction_total) > 0.01'},
    {'key': 'fx_margin', 'label': 'FX-Saldo-Korrektur', 'scope': 'compute',
     'default': True, 'visible_when': 'fx_results'},
    {'key': 'dba_beta', 'label': 'DBA-QSt-Beta', 'scope': 'compute',
     'default': False, 'visible_when': 'always (Expander)'},
    {'key': 'variante_b', 'label': 'DE-KESt Variante B', 'scope': 'view',
     'default': False, 'visible_when': 'abs(zeile_7_pre_override) > 0.01'},
)


def default_toggles():
    return {t['key']: t['default'] for t in TOGGLE_INVENTORY}


def build_view_key(input_key, toggles, etf_overrides, confirmed_isins):
    """Key over the full view-relevant domain state.

    Every domain input that changes displayed or exported values must change
    this key: display toggles, Variante B, confirmed fund classifications.
    (Anlage-SO overrides and compute toggles are already in the input_key.)
    Exports are cached against this key so a fund confirmation can never
    serve a stale XLSX/TXT.
    """
    toggle_part = ",".join(
        f"{t['key']}={bool((toggles or {}).get(t['key'], t['default']))}"
        for t in TOGGLE_INVENTORY if t['scope'] == 'view'
    )
    override_part = ",".join(
        f"{isin}={tfs:.4f}" for isin, tfs in sorted((etf_overrides or {}).items())
    )
    confirmed_part = ",".join(sorted(confirmed_isins or []))
    parts = [
        f"view_schema={VIEW_SCHEMA_VERSION}",
        f"input={input_key}",
        toggle_part,
        f"overrides={override_part}",
        f"confirmed={confirmed_part}",
    ]
    return hashlib.sha256("|".join(parts).encode('utf-8')).hexdigest()


# ── Toggle availability (from the raw report, pre-override) ──────────────────

def toggle_availability(report):
    """Which toggles the sidebar shows, derived from the raw compute snapshot.

    Variante-B availability MUST come from the pre-override Zeile 7 — the
    active toggle zeroes Zeile 7 in the final dict, so deriving visibility
    from the view model would hide the enabled toggle.
    """
    audit = report.get('audit', {}) or {}
    details = audit.get('stillhalter_details', []) or []
    kap_inv = report.get('kap_inv', {}) or {}
    return {
        'zufluss': any(d.get('is_cross_year') for d in details),
        'invstg': bool(kap_inv.get('etf_by_isin')),
        'tageskurs': abs(report.get('fx_correction_total', 0)) > 0.01,
        'fx_margin': bool(report.get('fx_results')),
        'dba_beta': True,
        'variante_b': abs(report.get(
            'zeile_7_kapitalertraege_mit_inlaendischem_steuerabzug_eur', 0
        )) > 0.01,
    }


def effective_toggles(toggles, availability):
    """Toggles as they actually apply: an unavailable toggle acts as its
    neutral state (off), mirroring the old GUI where hidden checkboxes never
    contributed adjustments."""
    toggles = dict(toggles or {})
    result = {}
    for t in TOGGLE_INVENTORY:
        wanted = bool(toggles.get(t['key'], t['default']))
        result[t['key']] = wanted and availability.get(t['key'], True)
    # compute toggles pass through unchanged (they were already applied in
    # calculate_tax); availability only gates their sidebar visibility.
    result['fx_margin'] = bool(toggles.get('fx_margin', True))
    result['dba_beta'] = bool(toggles.get('dba_beta', False))
    return result


# ── ETF classification overrides (pure) ──────────────────────────────────────

TFS_TO_CLASSIFICATION = {
    0.30: 'aktienfonds',
    0.15: 'mischfonds',
    0.60: 'immobilienfonds',
    0.80: 'auslands_immobilienfonds',
    0.0: 'sonstiger_fonds',
}


def apply_etf_overrides(kap_inv, kapinv_fx_by_isin, etf_overrides,
                        tageskurs_active):
    """Apply confirmed manual fund classifications on copies.

    ``etf_overrides``: {isin: tfs_rate} for CONFIRMED classifications only.
    Returns a dict with the overridden ``kap_inv`` copy, the list of ISINs
    still unconfirmed, and the aggregate deltas the final dict needs.
    Never mutates the inputs (the compute snapshot stays frozen).
    """
    kap_inv = copy.deepcopy(kap_inv or {})
    etf_by_isin = kap_inv.get('etf_by_isin', {}) or {}
    unknown = list(kap_inv.get('etf_unknown_isins', []) or [])
    unconfirmed = list(unknown)

    net_taxable_delta = 0.0
    tageskurs_taxable_delta = 0.0

    for isin, new_tfs in (etf_overrides or {}).items():
        if isin not in unknown or isin not in etf_by_isin:
            continue
        info = etf_by_isin[isin]
        if isin in unconfirmed:
            unconfirmed.remove(isin)
        old_tfs = info.get('tfs_rate', 0.0)
        raw_gain = info.get('gain', 0)
        raw_loss = info.get('loss', 0)
        raw_div = info.get('div', 0)
        old_gain_tax = info.get('gain_taxable', raw_gain)
        old_loss_tax = info.get('loss_taxable', raw_loss)
        old_div_tax = info.get('div_taxable', raw_div)
        new_gain_tax = raw_gain * (1 - new_tfs)
        new_loss_tax = raw_loss * (1 - new_tfs)
        new_div_tax = raw_div * (1 - new_tfs)
        fx_entry = (kapinv_fx_by_isin or {}).get(isin, {})
        raw_tk_delta = fx_entry.get('raw_delta', 0) if tageskurs_active else 0
        old_tk_tax = calculate_tax_report.safe_float(
            fx_entry.get('taxable_delta', 0)
        ) if tageskurs_active else 0
        new_tk_tax = raw_tk_delta * (1 - new_tfs)
        if new_tfs != old_tfs:
            net_taxable_delta += (
                (new_gain_tax - old_gain_tax)
                + (new_loss_tax - old_loss_tax)
                + (new_div_tax - old_div_tax)
                + (new_tk_tax - old_tk_tax)
            )
            tageskurs_taxable_delta += (new_tk_tax - old_tk_tax)
        info['tfs_rate'] = new_tfs
        info['gain_taxable'] = new_gain_tax
        info['loss_taxable'] = new_loss_tax
        info['div_taxable'] = new_div_tax
        info['classification'] = TFS_TO_CLASSIFICATION.get(
            new_tfs, 'sonstiger_fonds'
        )

    return {
        'kap_inv': kap_inv,
        'unconfirmed_unknown_isins': unconfirmed,
        'net_taxable_delta': net_taxable_delta,
        'tageskurs_taxable_delta': tageskurs_taxable_delta,
    }


def recalc_wht_per_account(per_account_pools, merged_etf_by_isin,
                           dba_wht_beta_enabled):
    """Fund withholding tax after overrides, computed PER ACCOUNT.

    The DBA beta is non-linear (refund offset per ISIN), so overrides must be
    applied on each account's own pool and only the finished per-account
    credits summed — never recomputed on the merged pool. Overrides are
    mirrored onto copies via tfs_rate/classification of the merged entries.
    Returns {'creditable_tax_eur', 'events', 'review_items'}.
    """
    pools = per_account_pools or [merged_etf_by_isin or {}]
    total = 0.0
    events = []
    review_items = []
    for pool in pools:
        pool_copy = copy.deepcopy(pool or {})
        for isin, entry in pool_copy.items():
            merged_entry = (merged_etf_by_isin or {}).get(isin)
            if merged_entry:
                # Nur VORHANDENE Keys spiegeln: die Legacy-Rechnung
                # unterscheidet zwischen fehlendem classification-Key und
                # explizitem None (= unbestaetigt, 0 Credit).
                if 'tfs_rate' in merged_entry:
                    entry['tfs_rate'] = merged_entry['tfs_rate']
                if 'classification' in merged_entry:
                    entry['classification'] = merged_entry['classification']
        result = calculate_tax_report.calculate_kap_inv_wht_for_mode(
            pool_copy, dba_wht_beta_enabled=dba_wht_beta_enabled,
        )
        total += result['creditable_tax_eur']
        events.extend(result['events'])
        review_items.extend(result['review_items'])
    return {
        'creditable_tax_eur': total,
        'events': events,
        'review_items': review_items,
    }


# ── Final values (toggle-adjusted, Single Source of Truth) ───────────────────

def build_final_values(report, toggles, kap_inv_override=None,
                       etf_wht_override=None, override_deltas=None):
    """The toggle-adjusted final dict — mirrors what the GUI displays.

    ``report`` is the (merged) compute snapshot; it is not mutated.
    ``toggles`` are the EFFECTIVE view toggles (see effective_toggles()).
    ``kap_inv_override``/``etf_wht_override``/``override_deltas`` carry the
    result of apply_etf_overrides()/recalc_wht_per_account() when manual
    fund confirmations exist; without overrides the raw kap_inv is used.
    """
    d = report
    toggles = toggles or {}
    zufluss_on = bool(toggles.get('zufluss'))
    invstg_on = bool(toggles.get('invstg'))
    tageskurs_on = bool(toggles.get('tageskurs'))
    variante_b_on = bool(toggles.get('variante_b'))
    deltas = override_deltas or {}

    kap_inv = kap_inv_override if kap_inv_override is not None else (
        d.get('kap_inv', {}) or {}
    )
    has_etf_data = bool((d.get('kap_inv', {}) or {}).get('etf_by_isin'))
    audit = d.get('audit', {}) or {}

    topf_1 = d.get('topf_1_aktien_netto', d.get('stocks_net_eur', 0))
    topf_2 = d.get('topf_2_sonstiges_netto',
                   d.get('dividends_eur', 0) + d.get('interest_eur', 0)
                   + d.get('options_gain_eur', 0) + d.get('options_loss_eur', 0))
    zeile_19 = d.get('zeile_19_netto_eur', topf_1 + topf_2)
    zeile_20 = d.get('zeile_20_stock_gains_eur', d.get('stocks_gain_eur', 0))
    zeile_22 = d.get('zeile_22_other_losses_eur',
                     abs(d.get('options_loss_eur', 0)))
    zeile_23 = d.get('zeile_23_stock_losses_eur',
                     abs(d.get('stocks_loss_eur', 0)))
    zeile_7 = d.get(
        'zeile_7_kapitalertraege_mit_inlaendischem_steuerabzug_eur', 0)
    zeile_37 = d.get('zeile_37_kapitalertragsteuer_eur', 0)
    zeile_38 = d.get('zeile_38_solidaritaetszuschlag_eur', 0)
    quellensteuer = calculate_tax_report.get_kap_line_41_for_reporting(d)

    cross_year_premium = audit.get('cross_year_premium_eur', 0)
    adj_cross = cross_year_premium if zufluss_on else 0
    adj_topf_2 = topf_2 - adj_cross
    adj_zeile_19 = zeile_19 - adj_cross

    # InvStG off: reintegrate raw ETF values into the KAP pools.
    if has_etf_data and not invstg_on:
        raw_kap_inv = d.get('kap_inv', {}) or {}
        etf_raw_net = (raw_kap_inv.get('etf_gain_raw_eur', 0)
                       + raw_kap_inv.get('etf_loss_raw_eur', 0))
        topf_1 += etf_raw_net
        zeile_19 += etf_raw_net
        adj_zeile_19 += etf_raw_net
        zeile_20 += max(raw_kap_inv.get('etf_gain_raw_eur', 0), 0)
        zeile_23 += abs(min(raw_kap_inv.get('etf_loss_raw_eur', 0), 0))
        adj_topf_2 += raw_kap_inv.get('etf_dividends_raw_eur', 0)
        adj_zeile_19 += raw_kap_inv.get('etf_dividends_raw_eur', 0)
        zeile_19 += raw_kap_inv.get('etf_dividends_raw_eur', 0)
        quellensteuer = calculate_tax_report.get_kap_line_41_for_reporting(
            d, invstg_enabled=False
        )

    # Tageskurs correction (§20 Abs. 4 S. 1 EStG).
    fx_corr_total = d.get('fx_correction_total', 0)
    fx_corr_by_topf = d.get('fx_correction_by_topf', {}) or {}
    tk_gain_adj = d.get('fx_corr_gain_adj', {}) or {}
    tk_loss_adj = d.get('fx_corr_loss_adj', {}) or {}
    tageskurs_kapinv_corr_raw = 0
    tageskurs_kapinv_corr = 0
    if tageskurs_on:
        corr_topf1 = fx_corr_by_topf.get('Topf1', 0)
        corr_topf2 = fx_corr_by_topf.get('Topf2', 0)
        tageskurs_kapinv_corr_raw = fx_corr_by_topf.get('KAP-INV', 0)
        tageskurs_kapinv_corr = (
            calculate_tax_report.get_kap_inv_tageskurs_delta_for_reporting(d)
            + deltas.get('tageskurs_taxable_delta', 0)
        )
        topf_1 += corr_topf1
        adj_topf_2 += corr_topf2
        # Mirrors app.py exactly: invstg_on is only True with ETF data, so the
        # else branch also covers ETF-free accounts (KAP-INV buckets are 0
        # there, fx_corr_total == Topf1 + Topf2).
        if invstg_on:
            adj_zeile_19 += corr_topf1 + corr_topf2
            zeile_19 += corr_topf1 + corr_topf2
        else:
            topf_1 += tageskurs_kapinv_corr_raw
            adj_zeile_19 += fx_corr_total
            zeile_19 += fx_corr_total
        zeile_20 += tk_gain_adj.get('Topf1', 0)
        zeile_23 -= tk_loss_adj.get('Topf1', 0)
        zeile_22 -= tk_loss_adj.get('Topf2', 0)
        if not invstg_on:
            zeile_20 += tk_gain_adj.get('KAP-INV', 0)
            zeile_23 -= tk_loss_adj.get('KAP-INV', 0)

    # ETF-WHT after overrides feeds Zeile 41 exactly like the old GUI path
    # (final quellensteuer += new_wht − previous_wht).
    etf_wht = 0
    if has_etf_data and invstg_on:
        previous_wht = calculate_tax_report.get_kap_inv_wht_for_reporting(
            d.get('kap_inv', {}) or {}
        )
        if etf_wht_override is not None:
            etf_wht = etf_wht_override
            quellensteuer += etf_wht - previous_wht
        else:
            etf_wht = previous_wht

    _etf_reintegrate = has_etf_data and not invstg_on
    raw_kap_inv = d.get('kap_inv', {}) or {}
    final = {
        'stocks_gain': (
            d.get('stocks_gain_eur', 0)
            + (tk_gain_adj.get('Topf1', 0) if tageskurs_on else 0)
            + (tk_gain_adj.get('KAP-INV', 0)
               if (tageskurs_on and _etf_reintegrate) else 0)
            + (max(raw_kap_inv.get('etf_gain_raw_eur', 0), 0)
               if _etf_reintegrate else 0)
        ),
        'stocks_loss': (
            d.get('stocks_loss_eur', 0)
            + (tk_loss_adj.get('Topf1', 0) if tageskurs_on else 0)
            + (tk_loss_adj.get('KAP-INV', 0)
               if (tageskurs_on and _etf_reintegrate) else 0)
            + (min(raw_kap_inv.get('etf_loss_raw_eur', 0), 0)
               if _etf_reintegrate else 0)
        ),
        'dividends': (
            d.get('dividends_eur', 0)
            + (raw_kap_inv.get('etf_dividends_raw_eur', 0)
               if _etf_reintegrate else 0)
        ),
        'interest': d.get('interest_eur', 0),
        'options_gain': (
            d.get('options_gain_eur', 0)
            - (cross_year_premium if zufluss_on else 0)
            + (tk_gain_adj.get('Topf2', 0) if tageskurs_on else 0)
        ),
        'options_loss': (
            d.get('options_loss_eur', 0)
            + (tk_loss_adj.get('Topf2', 0) if tageskurs_on else 0)
        ),
        'topf_1': topf_1,
        'topf_2': adj_topf_2,
        'zeile_7': zeile_7,
        'zeile_19': adj_zeile_19,
        'zeile_20': zeile_20,
        'zeile_22': zeile_22,
        'zeile_23': zeile_23,
        'zeile_37': zeile_37,
        'zeile_38': zeile_38,
        'quellensteuer': quellensteuer,
        'etf_net_taxable': (
            (kap_inv.get('etf_net_taxable_eur', 0)
             + deltas.get('net_taxable_delta', 0)
             + tageskurs_kapinv_corr
             - deltas.get('tageskurs_taxable_delta', 0))
            if (has_etf_data and invstg_on) else 0
        ),
        'etf_wht': etf_wht,
        'tageskurs_kapinv_corr_raw': tageskurs_kapinv_corr_raw,
        'tageskurs_kapinv_corr': tageskurs_kapinv_corr,
        'adj_cross': adj_cross,
    }

    if variante_b_on:
        final = apply_variante_b(final)

    return final


def apply_variante_b(values):
    """Variante-B-Umbuchung (DE-KESt nach Z19/Z41) auf einem Werte-Dict.

    Einzige Stelle dieser Arithmetik — build_final_values UND die
    Multi-Account-Anzeige nutzen sie, damit nichts auseinanderdriftet.
    Erwartet Keys zeile_7/zeile_37/zeile_38/zeile_19/quellensteuer, optional
    dividends/topf_2 (haelt die Invariante zeile_19 = topf_1 + topf_2).
    Gibt ein NEUES Dict zurueck; unter 0,01 EUR in Zeile 7 unveraendert.
    """
    v = dict(values)
    if abs(v.get('zeile_7', 0)) <= 0.01:
        return v
    z7 = v['zeile_7']
    for key in ('dividends', 'topf_2'):
        if key in v:
            v[key] += z7
    v['zeile_19'] = v.get('zeile_19', 0) + z7
    v['quellensteuer'] = (v.get('quellensteuer', 0)
                          + v.get('zeile_37', 0) + v.get('zeile_38', 0))
    v['zeile_7'] = 0
    v['zeile_37'] = 0
    v['zeile_38'] = 0
    return v


# ── Notices (single inventory for badge, overview, Prüffälle, export) ────────

NOTICE_CLASSES = ('prueffall', 'transparenz', 'fehler')


def _notice(notice_id, cls, severity, title, body, target, count=1,
            data=None):
    return {
        'id': notice_id,
        'class': cls,
        'severity': severity,
        'title': title,
        'body': body,
        'target': target,
        'count': count,
        'data': data if data is not None else [],
    }


def collect_notices(report, context=None):
    """The complete notice inventory derived from a compute snapshot.

    ``context`` carries input diagnostics the report does not know about:
    {'multi_stmt_files': [...], 'accounts_skipped': [...],
     'dropped_duplicates': [...], 'csv_disabled_multi_account': bool,
     'kap_inv_form': {...} or None, 'wht_review_items': [...],
     'dba_beta_enabled': bool, 'invstg_on': bool,
     'plausibility_mismatch': bool}

    Classes: 'prueffall' (action needed, severity 'kritisch'|'normal'),
    'transparenz' (information only), 'fehler' (hard errors are raised at
    their call sites; this inventory only carries recoverable ones).
    Every legacy warning path of the old UI must appear here —
    tests/test_ui_model.py checks the registry for completeness.
    """
    ctx = context or {}
    audit = report.get('audit', {}) or {}
    notices = []

    unmatched = audit.get('stillhalter_unmatched', []) or []
    cy_unmatched = [u for u in unmatched if u.get('type') != 'cross_year']
    prior_unmatched = [u for u in unmatched if u.get('type') == 'cross_year']
    if cy_unmatched:
        syms = ", ".join(
            f"{u.get('symbol', '?')} ({u.get('expiry', '?')})"
            for u in cy_unmatched
        )
        notices.append(_notice(
            'stillhalter_unmatched', 'prueffall', 'kritisch',
            'Stillhalterprämie nicht zuordenbar',
            f"Für {len(cy_unmatched)} Andienung(en) fehlt der ursprüngliche "
            f"Optionsverkauf: {syms}. Die Prämie bleibt in Topf 1 eingebettet "
            "und würde doppelt versteuert. Lösung: das Vorjahres-XML mit dem "
            "Optionsverkauf zusätzlich hochladen.",
            'prueffaelle', len(cy_unmatched), cy_unmatched,
        ))
    if prior_unmatched:
        syms = ", ".join(
            f"{u.get('symbol', '?')}" for u in prior_unmatched
        )
        notices.append(_notice(
            'stillhalter_prior_lot_unmatched', 'prueffall', 'kritisch',
            'Vorjahres-Lot ohne Optionsverkauf',
            f"Für {len(prior_unmatched)} frühere Put-Andienung(en), deren "
            f"Aktien-Lot im Steuerjahr veräußert wurde, fehlt der "
            f"ursprüngliche Optionsverkauf: {syms}. Ohne den Original-Trade "
            "kann die damalige Prämie nicht aus dem Aktienergebnis "
            "herausgerechnet werden. Lösung: ältere XML-Historie hochladen.",
            'prueffaelle', len(prior_unmatched), prior_unmatched,
        ))

    zufluss_unmatched = audit.get('zufluss_unmatched', []) or []
    if zufluss_unmatched:
        syms = ", ".join(str(u.get('symbol', '?')) for u in zufluss_unmatched)
        notices.append(_notice(
            'zufluss_unmatched', 'prueffall', 'kritisch',
            'Glattstellung ohne Eröffnungs-Verkauf',
            f"{len(zufluss_unmatched)} Glattstellung(en) ohne Eröffnungs-SELL: "
            f"{syms}. Ohne das Vorjahres-XML wird die Prämie doppelt "
            "versteuert (Verkaufsjahr und Schließungsjahr). Lösung: "
            "Vorjahres-XML hochladen.",
            'prueffaelle', len(zufluss_unmatched), zufluss_unmatched,
        ))

    unrouted = audit.get('unrouted_asset_categories', []) or []
    if unrouted:
        total = sum(e.get('pnl_eur', 0) for e in unrouted)
        cats = ", ".join(str(e.get('category', '?')) for e in unrouted)
        notices.append(_notice(
            'unrouted_asset_categories', 'prueffall', 'kritisch',
            'Instrumente ohne Steuertopf',
            f"{len(unrouted)} Instrumentenkategorie(n) ({cats}) mit Saldo "
            f"{total:,.2f} EUR sind in keiner Zeile der Anlage KAP enthalten "
            "und müssen manuell nachgetragen werden. Die Trades stehen im "
            "Excel-Export im Block 'Nicht zugeordnet'.",
            'prueffaelle', len(unrouted), unrouted,
        ))

    unhandled = audit.get('unhandled_activity_codes', []) or []
    if unhandled:
        total = sum(e.get('amount_eur', 0) for e in unhandled)
        codes = ", ".join(str(e.get('code', '?')) for e in unhandled)
        notices.append(_notice(
            'unhandled_activity_codes', 'prueffall', 'normal',
            'Cash-Buchungen ohne automatische Zuordnung',
            f"Buchungsarten ohne sichere automatische Behandlung ({codes}), "
            f"Saldo {total:,.2f} EUR. Diese Beträge sind in keiner Zeile "
            "enthalten; anhand der IBKR-Abrechnung manuell zuordnen.",
            'prueffaelle', len(unhandled), unhandled,
        ))

    transaction_tax = audit.get('transaction_tax', {}) or {}
    ttax_applied = transaction_tax.get('applied_count', 0)
    ttax_deferred = transaction_tax.get('deferred_count', 0)
    ttax_embedded = transaction_tax.get('already_in_trade_count', 0)
    if ttax_applied or ttax_deferred or ttax_embedded:
        parts = []
        if ttax_applied:
            parts.append(
                f"{ttax_applied} Buchung(en) über "
                f"{transaction_tax.get('applied_eur', 0):,.2f} EUR wurden "
                "im realisierten Ergebnis berücksichtigt"
            )
        if ttax_deferred:
            parts.append(
                f"{transaction_tax.get('deferred_eur', 0):,.2f} EUR entfallen "
                "noch auf offene Positionen"
            )
        if ttax_embedded:
            parts.append(
                f"{ttax_embedded} Buchung(en) waren bereits im Trade enthalten"
            )
        notices.append(_notice(
            'transaction_tax_processed', 'transparenz', 'normal',
            'Transaktionssteuer berücksichtigt',
            "; ".join(parts) + ".",
            'methodik',
            ttax_applied + ttax_deferred + ttax_embedded,
            transaction_tax.get('details', []),
        ))

    partnership = report.get('partnership_tax_items', {}) or {}
    if partnership:
        names = ", ".join(
            f"{item.get('ticker', isin)}"
            for isin, item in sorted(partnership.items())
        )
        notices.append(_notice(
            'partnership_blocked', 'prueffall', 'kritisch',
            'Personengesellschaften blockiert',
            f"Steuerberechnung blockiert für: {names}. Beträge sind bewusst "
            "in keiner KAP-/KAP-INV-Zeile enthalten; benötigt werden K-1/K-3 "
            "bzw. die äquivalente Jahresallokation.",
            'prueffaelle', len(partnership), sorted(partnership.items()),
        ))

    review_items = report.get('classification_review_items', []) or []
    if review_items:
        names = ", ".join(
            str(item.get('ticker', item.get('isin', '?')))
            for item in review_items
        )
        notices.append(_notice(
            'classification_review', 'prueffall', 'normal',
            'Produktklassifikation offen',
            f"Für {len(review_items)} Produkt(e) ist die steuerliche "
            f"Klassifikation offen: {names}. Kein Altpfad-Fallback; die "
            "Übernahme bleibt bis zu einem Nachweis blockiert.",
            'prueffaelle', len(review_items), review_items,
        ))

    unknown_isins = ctx.get('unconfirmed_unknown_isins')
    if unknown_isins is None:
        unknown_isins = (report.get('kap_inv', {}) or {}).get(
            'etf_unknown_isins', []) or []
    if unknown_isins and ctx.get('invstg_on', True):
        notices.append(_notice(
            'etf_unknown_classification', 'prueffall', 'kritisch',
            'Fondsart bestätigen',
            f"{len(unknown_isins)} Fondsprodukt(e) ohne bestätigte Fondsart. "
            "Deren Gewinne, Ausschüttungen und Quellensteuer sind bis zur "
            "Bestätigung in keiner Formularzeile enthalten; ohne Bestätigung "
            "wird zu wenig erklärt. Zuordnung im Bereich Anlage KAP-INV "
            "bestätigen.",
            'kap_inv', len(unknown_isins), list(unknown_isins),
        ))

    kap_inv_form = ctx.get('kap_inv_form') or {}
    for warning in kap_inv_form.get('warnings', []) or []:
        text = str(warning)
        # Vorabpauschale, gezahlte Ausschuettungen und unbestaetigte Fondsart
        # haben eigene, praezisere Notices (kap_inv_sale_preliminary /
        # kap_inv_paid_distributions / etf_unknown_classification) —
        # die Form-Warnung dazu nicht doppelt listen.
        if ('Vorabpauschale' in text or 'Gezahlte Dividenden' in text
                or 'Fondsart muss vor der' in text):
            continue
        notices.append(_notice(
            'kap_inv_form_warning', 'prueffall', 'normal',
            'KAP-INV-Hinweis', text, 'kap_inv',
        ))
    paid_details = kap_inv_form.get('negative_distribution_details', []) or []
    if paid_details:
        paid_total = sum(
            item.get('paid_distribution_eur', 0) for item in paid_details
        )
        notices.append(_notice(
            'kap_inv_paid_distributions', 'prueffall', 'normal',
            'Gezahlte Ausschüttungen (Short)',
            f"Gezahlte Dividenden/Ersatzzahlungen auf Short-Positionen "
            f"({paid_total:,.2f} EUR) sind nicht in den Ausschüttungszeilen "
            "enthalten; steuerliche Behandlung manuell prüfen.",
            'kap_inv', len(paid_details), paid_details,
        ))
    sale_lines = [
        line for line in kap_inv_form.get('lines', []) or []
        if line.get('kind') == 'sale'
    ]
    if sale_lines:
        notices.append(_notice(
            'kap_inv_sale_preliminary', 'prueffall', 'normal',
            'KAP-INV-Veräußerungszeilen vorläufig',
            "Veräußerungszeilen gelten bis zur Berücksichtigung bereits "
            "versteuerter Vorabpauschalen als vorläufig.",
            'kap_inv', len(sale_lines), sale_lines,
        ))

    wht_review = ctx.get('wht_review_items') or []
    if ctx.get('dba_beta_enabled') and wht_review:
        notices.append(_notice(
            'dba_wht_review', 'prueffall', 'normal',
            'Quellensteuer-Prüffälle (DBA-Beta)',
            f"{len(wht_review)} Quellensteuer-Ereignis(se) benötigen manuelle "
            "Prüfung (unbelegter DBA-Satz, Erstattungen oder nicht "
            "zuordenbare Buchungen). Details im Bereich Anlage KAP-INV.",
            'kap_inv', len(wht_review), wht_review,
        ))

    fx_meta = report.get('fx_option_a_meta', {}) or {}
    open_rows = fx_meta.get('open_rows_with_pnl', []) or []
    if open_rows:
        notices.append(_notice(
            'fx_open_rows_with_pnl', 'prueffall', 'normal',
            'FX-Buchungen mit widersprüchlichem Code',
            f"{len(open_rows)} Devisen-Buchungen tragen ein realisiertes "
            "Ergebnis, obwohl IBKR sie als Eröffnung ausweist. Sie wurden "
            "konservativ als steuerbar behandelt; bitte prüfen.",
            'prueffaelle', len(open_rows), open_rows,
        ))

    if not report.get('has_trade_price', True):
        notices.append(_notice(
            'missing_trade_price', 'prueffall', 'normal',
            'Flex Query ohne tradePrice',
            "Die Flex Query enthält kein tradePrice-Feld; Stillhalterprämien "
            "werden mit dem Tagesschlusskurs statt dem Ausführungspreis "
            "berechnet. In IBKR eine erweiterte Query mit Execution-Details "
            "exportieren.",
            'prueffaelle',
        ))

    xml_has_fx = report.get('xml_has_fx_data', True)
    fx_source = report.get('fx_source', 'none')
    if not xml_has_fx and fx_source != 'csv':
        notices.append(_notice(
            'missing_fx_transactions', 'prueffall', 'normal',
            'Keine FX-Transaktionsdaten',
            "Die Flex Query enthält keine FxTransactions-Sektion; "
            "FX-Gewinne/-Verluste werden nur approximiert. Lösung: in IBKR "
            "die Sektion FX Transactions aktivieren, oder bei EUR-Basiskonten "
            "den IBKR-Standardbericht (CSV) zusätzlich hochladen.",
            'prueffaelle',
        ))
    elif not xml_has_fx and fx_source == 'csv':
        notices.append(_notice(
            'fx_from_csv', 'transparenz', 'normal',
            'FX-Daten aus CSV übernommen',
            "Die Flex Query enthält keine FxTransactions; der "
            "IBKR-Standardbericht liefert aggregierte FIFO-Rohwerte. Eine "
            "buchungsweise Schuldtilgungsprüfung ist damit nicht möglich.",
            'rechenwege',
        ))

    parse_failures = audit.get('fx_rate_parse_failures', {}) or {}
    n_parse = (parse_failures.get('funds', 0) or 0) + (
        parse_failures.get('trades', 0) or 0)
    if n_parse:
        notices.append(_notice(
            'fx_rate_parse_failures', 'prueffall', 'normal',
            'Nicht lesbare Wechselkurse',
            f"{n_parse} Wechselkurs-Feld(er) konnten nicht gelesen werden "
            "(funds/trades). Betroffene Buchungen könnten mit fehlendem Kurs "
            "verarbeitet worden sein; Plausibilität prüfen.",
            'prueffaelle', n_parse,
        ))

    so = report.get('anlage_so', {}) or {}
    so_unknown = abs(so.get('unknown_gain', 0)) + abs(so.get('unknown_loss', 0))
    if so_unknown > 0.01:
        notices.append(_notice(
            'so_unknown_holding_period', 'prueffall', 'normal',
            'Anlage SO: Haltedauer nicht ermittelbar',
            f"Für {so_unknown:,.2f} EUR kann die Haltedauer nicht bestimmt "
            "werden; die Positionen werden konservativ als steuerpflichtig "
            "behandelt. Lösung: XML des Kaufjahres als Historie hochladen.",
            'anlage_so',
        ))

    for f in ctx.get('multi_stmt_files', []) or []:
        notices.append(_notice(
            'multi_statement_file', 'prueffall', 'normal',
            'Mehrere Konten in einer Datei',
            f"{f.get('name', '?')} enthält {len(f.get('account_ids', []))} "
            f"Konten; nur das erste ({(f.get('account_ids') or ['?'])[0]}) "
            "wird verarbeitet. Pro Konto eine eigene Flex Query exportieren.",
            'prueffaelle',
        ))

    skipped = ctx.get('accounts_skipped', []) or []
    if skipped:
        notices.append(_notice(
            'accounts_skipped', 'prueffall', 'normal',
            'Konten übersprungen',
            f"{len(skipped)} Konto/Konten ohne XML für das Steuerjahr wurden "
            f"übersprungen: {', '.join(skipped)}.",
            'prueffaelle', len(skipped),
        ))

    dropped = ctx.get('dropped_duplicates', []) or []
    if dropped:
        notices.append(_notice(
            'duplicate_uploads_dropped', 'transparenz', 'normal',
            'Doppelte Dateien verworfen',
            f"{len(dropped)} identische Datei(en) wurden verworfen: "
            f"{', '.join(dropped)}. Jede Datei zählt nur einmal.",
            'rechenwege', len(dropped),
        ))

    if ctx.get('csv_disabled_multi_account'):
        notices.append(_notice(
            'csv_disabled_multi_account', 'transparenz', 'normal',
            'CSV-Check bei mehreren Konten deaktiviert',
            "Der CSV-Plausibilitätscheck und der FX-CSV-Fallback sind nur "
            "bei genau einem Konto aktiv.",
            'rechenwege',
        ))

    if ctx.get('plausibility_mismatch'):
        notices.append(_notice(
            'plausibility_mismatch', 'prueffall', 'normal',
            'Plausibilitätscheck weicht ab',
            "Mindestens eine Kategorie weicht unerklärt vom IBKR-Bericht ab. "
            "Details im Bereich Rechenwege.",
            'rechenwege',
        ))

    occ_renames = audit.get('occ_rename_matches', []) or []
    if occ_renames:
        notices.append(_notice(
            'occ_rename_matches', 'transparenz', 'normal',
            'Kapitalmaßnahmen zugeordnet',
            f"{len(occ_renames)} Options-Serienumbenennung(en)/Split(s) "
            "wurden über die IBKR-Kontraktidentität zugeordnet, damit die "
            "Prämie nur einmal versteuert wird.",
            'rechenwege', len(occ_renames), occ_renames,
        ))

    aliases = audit.get('underlying_symbol_aliases', {}) or {}
    if aliases:
        notices.append(_notice(
            'underlying_symbol_aliases', 'transparenz', 'normal',
            'Symbol-Aliasse erkannt',
            f"{len(aliases)} Basiswert(e) mit mehreren IBKR-Schreibweisen "
            "wurden über conid/ISIN zusammengeführt.",
            'rechenwege', len(aliases), sorted(aliases.items()),
        ))

    dropped_corr = audit.get('stillhalter_corrections_dropped', []) or []
    if dropped_corr:
        notices.append(_notice(
            'stillhalter_corrections_dropped', 'prueffall', 'kritisch',
            'Stillhalter-Korrektur nicht zuordenbar',
            f"{len(dropped_corr)} Prämien-Korrektur(en) konnten keinem "
            "Aktien-Trade zugeordnet werden; die Prämie bliebe sonst doppelt "
            "versteuert. Trade-Details prüfen.",
            'prueffaelle', len(dropped_corr), dropped_corr,
        ))

    open_short = audit.get('stillhalter_open_short', []) or []
    if open_short:
        notices.append(_notice(
            'stillhalter_open_short', 'transparenz', 'normal',
            'Offene Short-Positionen aus Andienungen',
            f"{len(open_short)} Andienung(en) führten zu offenen "
            "Short-Positionen (PnL unrealisiert, keine Korrektur nötig). "
            "Beim Folgejahr-Lauf dieses XML als Historie mitladen.",
            'rechenwege', len(open_short), open_short,
        ))

    return notices


def notice_counts(notices):
    prueffaelle = [n for n in notices if n['class'] == 'prueffall']
    critical = [n for n in prueffaelle if n['severity'] == 'kritisch']
    return {
        'prueffaelle': len(prueffaelle),
        'kritisch': len(critical),
        'transparenz': sum(1 for n in notices if n['class'] == 'transparenz'),
    }


# ── View model assembly ──────────────────────────────────────────────────────

def build_view_model(merged, per_account_wht_pools, domain_state,
                     context=None):
    """Assemble the complete, render-ready view model (three stages).

    Stage 1 builds every tax derivation (final values, overridden kap_inv,
    kap_inv_form, export context) WITHOUT notices; stage 2 collects the
    notices from that base model; stage 3 adds notices, badge counts and
    navigation visibility. Renderers only read from the result. ``merged``
    and ``per_account_wht_pools`` are treated as frozen (never mutated).

    ``domain_state``: {'toggles': {...}, 'etf_overrides': {isin: tfs},
                       'dba_beta_enabled': bool}
    ``context``: input diagnostics for collect_notices (multi_stmt_files,
                 accounts_skipped, dropped_duplicates,
                 csv_disabled_multi_account, plausibility_mismatch).
    """
    d = merged
    domain = domain_state or {}
    ctx = dict(context or {})
    availability = toggle_availability(d)
    toggles = effective_toggles(domain.get('toggles'), availability)
    dba_beta = bool(domain.get('dba_beta_enabled',
                               toggles.get('dba_beta', False)))

    raw_kap_inv = d.get('kap_inv', {}) or {}
    has_etf_data = bool(raw_kap_inv.get('etf_by_isin'))
    kapinv_fx_by_isin = d.get('fx_correction_kap_inv_by_isin', {}) or {}

    # Stage 1a: manual fund-classification overrides on copies.
    etf_overrides = dict(domain.get('etf_overrides') or {})
    ov = apply_etf_overrides(
        raw_kap_inv, kapinv_fx_by_isin, etf_overrides, toggles['tageskurs'],
    )
    kap_inv = ov['kap_inv']
    etf_by_isin = kap_inv.get('etf_by_isin', {}) or {}

    etf_wht_override = None
    wht_events = kap_inv.get('wht_events', []) or []
    wht_review_items = kap_inv.get('wht_review_items', []) or []
    if has_etf_data and etf_overrides:
        wht = recalc_wht_per_account(
            per_account_wht_pools, etf_by_isin, dba_beta,
        )
        etf_wht_override = wht['creditable_tax_eur']
        wht_events = wht['events']
        wht_review_items = wht['review_items']
        kap_inv['etf_wht_anrechenbar_eur'] = etf_wht_override
        kap_inv['wht_events'] = wht_events
        kap_inv['wht_review_items'] = wht_review_items

    # Stage 1b: toggle-adjusted final values.
    final = build_final_values(
        d, toggles,
        kap_inv_override=kap_inv,
        etf_wht_override=etf_wht_override,
        override_deltas={
            'net_taxable_delta': ov['net_taxable_delta'],
            'tageskurs_taxable_delta': ov['tageskurs_taxable_delta'],
        },
    )

    # Stage 1c: form structures and export context.
    kap_inv_form = {}
    if has_etf_data and toggles['invstg']:
        kap_inv_form = calculate_tax_report.build_kap_inv_form(
            etf_by_isin,
            kapinv_fx_by_isin,
            ov['unconfirmed_unknown_isins'],
            include_tageskurs=toggles['tageskurs'],
        )
        kap_inv_form['kap_line_41_creditable_tax_eur'] = final['etf_wht']
    no_invstg_summary = calculate_tax_report.get_no_invstg_summary(
        d, include_tageskurs=toggles['tageskurs'],
    )

    anlage_so = d.get('anlage_so', {}) or {}
    has_so_data = bool(anlage_so.get('by_isin'))

    # Stage 2: notices from the notice-free base model.
    ctx.update({
        'kap_inv_form': kap_inv_form,
        'wht_review_items': wht_review_items,
        'dba_beta_enabled': dba_beta,
        'invstg_on': toggles['invstg'],
        'unconfirmed_unknown_isins': ov['unconfirmed_unknown_isins'],
    })
    notices = collect_notices(d, ctx)
    counts = notice_counts(notices)

    # Stage 3: navigation and identity.
    has_kap_inv_page = has_etf_data and toggles['invstg']
    has_so_page = has_so_data or bool(d.get('anlage_so_overrides_applied'))
    pages = visible_pages(has_kap_inv_page, has_so_page)

    input_key = ctx.get('input_key', '')
    view_key = build_view_key(
        input_key, toggles, etf_overrides,
        sorted(etf_overrides.keys()),
    )

    return {
        'final': final,
        'toggles': toggles,
        'toggle_availability': availability,
        'dba_beta_enabled': dba_beta,
        'kap_inv': kap_inv,
        'etf_by_isin': etf_by_isin,
        'has_etf_data': has_etf_data,
        'unconfirmed_unknown_isins': ov['unconfirmed_unknown_isins'],
        'kap_inv_form': kap_inv_form,
        'no_invstg_summary': no_invstg_summary,
        'wht_events': wht_events,
        'wht_review_items': wht_review_items,
        'anlage_so': anlage_so,
        'has_so_data': has_so_data,
        'notices': notices,
        'notice_counts': counts,
        'visible_pages': pages,
        'view_key': view_key,
    }


def build_per_account_finals(reports, per_account_wht_pools, domain_state):
    """Build the same final values for every account as for the total view.

    The account table must not mix raw compute values with a total that
    already includes Zufluss-, Tageskurs-, InvStG- or Variante-B adjustments.
    Reusing the canonical view-model path keeps every row comparable with the
    merged total and preserves per-account DBA withholding-tax calculation.
    """
    reports = list(reports or [])
    pools = list(per_account_wht_pools or [])
    if pools and len(pools) != len(reports):
        raise ValueError("WHT-Pools und Kontenberichte sind nicht ausgerichtet")

    finals = []
    for index, report in enumerate(reports):
        pool = pools[index] if pools else {}
        account_vm = build_view_model(
            report,
            [pool],
            domain_state,
            {'input_key': f'account-{index}'},
        )
        finals.append(account_vm['final'])
    return finals


# ── Navigation ───────────────────────────────────────────────────────────────

PAGES = (
    ('overview', 'Übersicht'),
    ('kap', 'Anlage KAP'),
    ('kap_inv', 'Anlage KAP-INV'),
    ('anlage_so', 'Anlage SO'),
    ('prueffaelle', 'Prüffälle'),
    ('rechenwege', 'Rechenwege'),
    ('export', 'Export'),
)


def visible_pages(has_kap_inv, has_anlage_so):
    """Ordered page ids for the sidebar. KAP-INV and Anlage SO are
    conditional; Prüffälle stays visible even with zero cases."""
    pages = []
    for page_id, _label in PAGES:
        if page_id == 'kap_inv' and not has_kap_inv:
            continue
        if page_id == 'anlage_so' and not has_anlage_so:
            continue
        pages.append(page_id)
    return pages


def page_label(page_id):
    for pid, label in PAGES:
        if pid == page_id:
            return label
    return page_id


def normalize_nav(nav, visible):
    """Validated nav state: an id pointing at a hidden/unknown page falls
    back atomically to the overview."""
    if nav in visible:
        return nav
    return 'overview'
