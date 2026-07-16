
import csv
import io
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict, deque

def load_csv(filepath):
    if not os.path.exists(filepath):
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def parse_date(date_str):
    # Formats: 2025-01-01 or 2025-01-01 20:20:00
    try:
        return datetime.strptime(date_str[:10], '%Y-%m-%d').date()
    except:
        return None

def safe_float(val, default=0.0):
    """Convert to float, returning default for empty strings or None."""
    if val is None or val == '':
        return default
    return float(val)

def get_withholding_tax_for_reporting(signed_cash_amount):
    """Convert IBKR's signed tax cash flow to the report sign convention.

    IBKR records tax deductions as negative cash flows and refunds as positive
    cash flows. The tax report uses the inverse sign: creditable tax is positive,
    while a net refund remains negative and must not become a new tax credit.
    """
    signed_cash_amount = safe_float(signed_cash_amount)
    return 0.0 if signed_cash_amount == 0 else -signed_cash_amount

def get_kap_inv_wht_for_reporting(kap_inv):
    """Return creditable fund withholding tax, with legacy fallback."""
    if not kap_inv:
        return 0.0
    if 'etf_wht_anrechenbar_eur' in kap_inv:
        return safe_float(kap_inv.get('etf_wht_anrechenbar_eur'))
    return safe_float(kap_inv.get('etf_wht_eur'))


def get_kap_line_41_for_reporting(report_data, invstg_enabled=True):
    """Return Anlage KAP line 41 without double-counting fund tax."""
    report_data = report_data or {}
    kap_inv = report_data.get('kap_inv') or {}
    fund_credit = get_kap_inv_wht_for_reporting(kap_inv)
    if 'zeile_41_withholding_tax_eur' in report_data:
        total = safe_float(report_data.get('zeile_41_withholding_tax_eur'))
    else:
        # Legacy reports stored only non-fund tax in withholding_tax_eur.
        total = safe_float(report_data.get('withholding_tax_eur')) + fund_credit
    if invstg_enabled:
        return total
    return (
        total
        - fund_credit
        + safe_float(kap_inv.get('etf_wht_eur'))
    )


def merge_kap_inv_wht_for_reporting(kap_inv_reports):
    """Sum finished per-account fund credits without a new global cap."""
    return sum(
        get_kap_inv_wht_for_reporting(kap_inv)
        for kap_inv in (kap_inv_reports or ())
    )


def calculate_creditable_foreign_tax(gross_distribution, tax_withheld,
                                     tax_refunded=0.0, tfs_rate=0.0,
                                     treaty_rate=None):
    """Calculate creditable foreign tax for one fund distribution event.

    Inputs use positive amounts for the gross distribution, tax withheld and
    tax refunded. The returned credit uses the tax-report sign convention:
    positive for creditable tax and negative for a refund surplus.
    """
    gross_distribution = max(safe_float(gross_distribution), 0.0)
    tax_withheld = max(safe_float(tax_withheld), 0.0)
    tax_refunded = max(safe_float(tax_refunded), 0.0)
    tfs_rate = min(max(safe_float(tfs_rate), 0.0), 1.0)
    treaty_rate = None if treaty_rate is None else max(safe_float(treaty_rate), 0.0)

    taxable_distribution = gross_distribution * (1.0 - tfs_rate)
    german_cap = taxable_distribution * 0.25
    treaty_cap = gross_distribution * treaty_rate if treaty_rate is not None else None
    net_foreign_tax = tax_withheld - tax_refunded

    if net_foreign_tax < 0:
        # A refund surplus corrects prior credited tax in the refund year. It
        # must stay negative and must never become a new positive tax credit.
        creditable_tax = net_foreign_tax
    elif gross_distribution <= 0:
        # With no matched positive income there is no defensible current-event
        # cap for a tax deduction. Keep the raw row visible for manual review.
        creditable_tax = 0.0
    else:
        caps = [net_foreign_tax, german_cap]
        # Cent-/FX-Rundungstoleranz: IBKR behaelt den DBA-Satz in der
        # Handelswaehrung ein; nach EUR-Umrechnung und Cent-Rundung kann der
        # Einbehalt um Zehntel-Cents ueber gross_EUR x Satz liegen. Nur echte
        # Ueberschreitungen (> 2 Cent) deuten auf einen Erstattungsanspruch
        # und kappen die Anrechnung.
        if treaty_cap is not None and net_foreign_tax > treaty_cap + 0.02:
            caps.append(treaty_cap)
        creditable_tax = min(caps)

    excess_tax = max(net_foreign_tax - max(creditable_tax, 0.0), 0.0)
    if gross_distribution <= 0 and net_foreign_tax < 0:
        status = 'unmatched_refund'
    elif gross_distribution <= 0 and net_foreign_tax > 0:
        status = 'unmatched_withholding'
    elif net_foreign_tax < 0:
        status = 'refund_exceeds_withholding'
    elif net_foreign_tax == 0:
        status = 'fully_refunded'
    elif treaty_rate is None:
        status = 'dba_unverified'
    elif excess_tax > 0.005:
        status = 'capped_review_refund'
    else:
        status = 'matched'

    return {
        'gross_distribution_eur': gross_distribution,
        'taxable_distribution_eur': taxable_distribution,
        'tax_withheld_eur': tax_withheld,
        'tax_refunded_eur': tax_refunded,
        'net_foreign_tax_eur': net_foreign_tax,
        'german_cap_eur': german_cap,
        'treaty_rate': treaty_rate,
        'treaty_cap_eur': treaty_cap,
        'creditable_tax_eur': creditable_tax,
        'excess_tax_eur': excess_tax,
        'status': status,
        'review_required': status not in ('matched', 'fully_refunded'),
    }


def recalculate_kap_inv_wht(etf_by_isin, treaty_rate_getter=None):
    """Recalculate event-level fund tax after a TFS/classification change.

    The function mutates the per-ISIN dictionaries so the UI manual override
    and the core calculation use exactly the same cap logic.
    """
    if treaty_rate_getter is None:
        from etf_classification import get_foreign_tax_treaty_rate
        treaty_rate_getter = get_foreign_tax_treaty_rate

    total_creditable = 0.0
    all_events = []
    review_items = []
    for isin, data in (etf_by_isin or {}).items():
        tfs_rate = safe_float(data.get('tfs_rate'))
        events = data.get('wht_events') or []
        isin_events = []
        for event in events:
            result = calculate_creditable_foreign_tax(
                event.get('gross_distribution_eur'),
                event.get('tax_withheld_eur'),
                event.get('tax_refunded_eur'),
                tfs_rate,
                treaty_rate_getter(isin),
            )
            event.update(result)
            event['isin'] = isin
            event['excess_offset_eur'] = 0.0
            isin_events.append(event)

        # Erstattungsueberschuesse zuerst gegen die NICHT angerechneten
        # Ueberhaenge (excess_tax) derselben ISIN verrechnen: eine Erstattung
        # des ohnehin nicht angerechneten Teils darf die Anrechnung nicht
        # kuerzen. Erst ein darueber hinausgehender Rest reduziert Zeile 41.
        excess_pool = sum(e['excess_tax_eur'] for e in isin_events)
        for event in isin_events:
            if event['creditable_tax_eur'] < 0 and excess_pool > 0.005:
                offset = min(-event['creditable_tax_eur'], excess_pool)
                event['excess_offset_eur'] = offset
                event['creditable_tax_eur'] += offset
                excess_pool -= offset
                if event['creditable_tax_eur'] >= -0.005:
                    event['status'] = 'refund_offsets_excess'

        isin_creditable = 0.0
        for event in isin_events:
            isin_creditable += event['creditable_tax_eur']
            all_events.append(event)
            if event['review_required']:
                review_items.append(event)
        # Per-ISIN legacy field retains IBKR's signed-cash convention.
        data['wht_anrechenbar'] = -isin_creditable
        total_creditable += isin_creditable

    return {
        'creditable_tax_eur': total_creditable,
        'events': all_events,
        'review_items': review_items,
    }


def calculate_legacy_kap_inv_wht(etf_by_isin):
    """Return the pre-DBA fund-tax calculation.

    Before the event-level DBA beta, fund withholding tax was reduced only by
    the fund's Teilfreistellung rate. Keep that stable behavior as the default
    and expose the more granular treaty/refund logic only through an explicit
    opt-in.
    """
    signed_creditable_tax = 0.0
    for data in (etf_by_isin or {}).values():
        tfs_rate = min(max(safe_float(data.get('tfs_rate')), 0.0), 1.0)
        signed_amount = safe_float(data.get('wht')) * (1.0 - tfs_rate)
        data['wht_anrechenbar'] = signed_amount
        signed_creditable_tax += signed_amount

    return {
        'creditable_tax_eur': get_withholding_tax_for_reporting(
            signed_creditable_tax
        ),
        'events': [],
        'review_items': [],
    }


def calculate_kap_inv_wht_for_mode(etf_by_isin, dba_wht_beta_enabled=False,
                                   treaty_rate_getter=None):
    """Select stable legacy or optional event-level DBA fund-tax logic."""
    if not dba_wht_beta_enabled:
        return calculate_legacy_kap_inv_wht(etf_by_isin)
    return recalculate_kap_inv_wht(
        etf_by_isin,
        treaty_rate_getter=treaty_rate_getter,
    )


def compare_kap_inv_wht_modes(etf_by_isin_pools, treaty_rate_getter=None):
    """Compare standard vs. DBA beta fund-tax PER ACCOUNT, then sum.

    The report calculation runs each account separately and adds the finished
    results. The comparison must do the same: recalculating the beta on a
    merged event pool would offset refunds of one account against uncredited
    excess of another (non-linear) and overstate the beta value. All work
    happens on deep copies; neither events nor wht_anrechenbar of the caller
    are mutated.
    """
    import copy

    standard_total = 0.0
    beta_total = 0.0
    for pool in (etf_by_isin_pools or []):
        if not pool:
            continue
        standard_total += calculate_legacy_kap_inv_wht(
            copy.deepcopy(pool)
        )['creditable_tax_eur']
        beta_total += recalculate_kap_inv_wht(
            copy.deepcopy(pool),
            treaty_rate_getter=treaty_rate_getter,
        )['creditable_tax_eur']

    return {
        'standard_eur': standard_total,
        'beta_eur': beta_total,
        'difference_eur': beta_total - standard_total,
    }


# Interne Event-Status → verstaendliche Anzeige-Texte. Unbekannte kuenftige
# Status duerfen nicht verschwinden (lesbarer Fallback in der Getter-Funktion).
WHT_EVENT_STATUS_LABELS = {
    'matched': 'Zugeordnet',
    'fully_refunded': 'Vollständig erstattet',
    'dba_unverified': 'DBA-Höchstsatz nicht belegt',
    'capped_review_refund': 'Höchstbetrag überschritten; Erstattungsanspruch prüfen',
    'refund_offsets_excess': 'Erstattung mit nicht angerechnetem Überhang verrechnet',
    'unmatched_refund': 'Zeitversetzte Erstattung; Bezugszufluss nicht im aktuellen Datensatz',
    'unmatched_withholding': 'Steuereinbehalt ohne zugeordneten Zufluss',
    'refund_exceeds_withholding': 'Erstattung übersteigt den Einbehalt des Ereignisses',
}


def get_wht_event_status_label(status):
    """Translate an internal event status into user-facing German text."""
    label = WHT_EVENT_STATUS_LABELS.get(status)
    if label:
        return label
    return f'Prüfen (interner Status: {status})' if status else 'Prüfen (ohne Status)'


def format_german_date(iso_date):
    """Format 'YYYY-MM-DD' (optionally with time suffix) as 'DD.MM.YYYY'."""
    if not iso_date:
        return ''
    date_part = str(iso_date)[:10]
    parts = date_part.split('-')
    if len(parts) == 3 and all(parts):
        return f'{parts[2]}.{parts[1]}.{parts[0]}'
    return str(iso_date)


def build_wht_review_rows(review_items, etf_by_isin=None):
    """Prepare display rows for the withholding-tax review table.

    Separates the booking date (report_dates; determines the tax year) from
    the historical entitlement date (date; e.g. the 2023 distribution a 2024
    refund refers to). Product identity comes from etf_by_isin with a lookup
    fallback, never hard-coded.
    """
    etf_by_isin = etf_by_isin or {}
    rows = []
    for event in (review_items or []):
        isin = event.get('isin', '') or ''
        info = etf_by_isin.get(isin) or {}
        ticker = info.get('ticker')
        name = info.get('name')
        if not ticker or name is None:
            from etf_classification import get_etf_info
            lookup = get_etf_info(isin) or {}
            ticker = ticker or lookup.get('ticker') or (isin[:12] if isin else '?')
            name = name if name else lookup.get('name', '')
        report_dates = sorted(set(event.get('report_dates') or []))
        booking = ', '.join(format_german_date(rd) for rd in report_dates)
        rows.append({
            'isin': isin,
            'ticker': ticker,
            'name': name or '',
            'product': f'{ticker} · {isin}' if isin else ticker,
            'booking_date': booking or '-',
            'entitlement_date': format_german_date(event.get('date')) or '-',
            'net_foreign_tax_eur': safe_float(event.get('net_foreign_tax_eur')),
            'german_cap_eur': safe_float(event.get('german_cap_eur')),
            'treaty_cap_eur': event.get('treaty_cap_eur'),
            'creditable_tax_eur': safe_float(event.get('creditable_tax_eur')),
            'status': event.get('status', ''),
            'status_label': get_wht_event_status_label(event.get('status', '')),
        })
    return rows


KAP_INV_FORM_MAPPING = {
    'aktienfonds': {
        'label': 'Aktienfonds', 'tfs_rate': 0.30,
        'distribution_line': 4, 'advance_lump_sum_line': 9, 'sale_line': 14,
    },
    'mischfonds': {
        'label': 'Mischfonds', 'tfs_rate': 0.15,
        'distribution_line': 5, 'advance_lump_sum_line': 10, 'sale_line': 17,
    },
    'immobilienfonds': {
        'label': 'Immobilienfonds', 'tfs_rate': 0.60,
        'distribution_line': 6, 'advance_lump_sum_line': 11, 'sale_line': 20,
    },
    'auslands_immobilienfonds': {
        'label': 'Auslands-Immobilienfonds', 'tfs_rate': 0.80,
        'distribution_line': 7, 'advance_lump_sum_line': 12, 'sale_line': 23,
    },
    'sonstiger_fonds': {
        'label': 'Sonstige Investmentfonds', 'tfs_rate': 0.00,
        'distribution_line': 8, 'advance_lump_sum_line': 13, 'sale_line': 26,
    },
}


def build_kap_inv_form(etf_by_isin, fx_by_isin=None, unknown_isins=None,
                       include_tageskurs=True):
    """Build the single source of truth for KAP-INV form lines.

    Form amounts are gross amounts before partial exemption. Taxable amounts
    are included only as a control calculation and are never direct form
    inputs. Sale amounts are explicitly preliminary until accumulated advance
    lump sums (Vorabpauschalen) are supplied in the later implementation phase.
    """
    fx_by_isin = fx_by_isin or {}
    unknown_isins = set(unknown_isins or ())
    line_totals = {}
    details = []
    blocked_isins = []
    blocked_details = []
    has_sale_activity = False

    def add_line(line, kind, classification, mapping, raw_amount, taxable_control):
        if abs(raw_amount) <= 0.005:
            return
        entry = line_totals.setdefault(line, {
            'line': line,
            'kind': kind,
            'fund_classification': classification,
            'fund_type': mapping['label'],
            'amount_raw_eur': 0.0,
            'taxable_control_eur': 0.0,
            'is_form_input': kind == 'distribution',
            'requires_advance_lump_sum_review': kind == 'sale',
        })
        entry['amount_raw_eur'] += raw_amount
        entry['taxable_control_eur'] += taxable_control

    negative_distribution_details = []

    for isin, data in sorted((etf_by_isin or {}).items()):
        classification = data.get('classification') or 'sonstiger_fonds'
        mapping = KAP_INV_FORM_MAPPING.get(classification)
        raw_distribution_net = safe_float(data.get('div'))
        # Zugeflossene und gezahlte Betraege trennen: nur zugeflossene
        # Ausschuettungen sind Formularwerte (amtliche KAP-INV-Hilfe);
        # gezahlte Dividenden/Ersatzzahlungen auf Short-Positionen werden
        # NICHT gegengerechnet, sondern als Prueffall ausgewiesen.
        # Legacy-Fallback ohne Komponenten-Tracking: Netto-Vorzeichen.
        if 'div_received' in data or 'div_paid' in data:
            raw_distribution = safe_float(data.get('div_received'))
            paid_distribution = safe_float(data.get('div_paid'))
        else:
            raw_distribution = max(raw_distribution_net, 0.0)
            paid_distribution = min(raw_distribution_net, 0.0)
        raw_fx_delta = (
            safe_float((fx_by_isin.get(isin) or {}).get('raw_delta'))
            if include_tageskurs else 0.0
        )
        raw_sale = (
            safe_float(data.get('gain'))
            + safe_float(data.get('loss'))
            + raw_fx_delta
        )
        if paid_distribution < -0.005:
            negative_distribution_details.append({
                'isin': isin,
                'ticker': data.get('ticker', isin[:12]),
                'classification': classification,
                'fund_type': mapping['label'] if mapping else classification,
                'paid_distribution_eur': paid_distribution,
                'received_distribution_eur': raw_distribution,
            })
        if not mapping or isin in unknown_isins:
            blocked_isins.append(isin)
            blocked_details.append({
                'isin': isin,
                'ticker': data.get('ticker', isin[:12]),
                'classification': classification,
                'review_reason': data.get('review_reason', ''),
                'distribution_raw_eur': raw_distribution,
                'sale_raw_eur': raw_sale,
                'tageskurs_raw_eur': raw_fx_delta,
            })
            continue

        tfs_rate = safe_float(data.get('tfs_rate'), mapping['tfs_rate'])
        taxable_distribution = raw_distribution * (1.0 - tfs_rate)
        taxable_sale = raw_sale * (1.0 - tfs_rate)
        sale_activity = (
            abs(safe_float(data.get('gain'))) > 0.005
            or abs(safe_float(data.get('loss'))) > 0.005
        )
        has_sale_activity = has_sale_activity or sale_activity

        detail = {
            'isin': isin,
            'ticker': data.get('ticker', isin[:12]),
            'name': data.get('name', ''),
            'classification': classification,
            'fund_type': mapping['label'],
            'tfs_rate': tfs_rate,
            'distribution_line': mapping['distribution_line'],
            'advance_lump_sum_line': mapping['advance_lump_sum_line'],
            'sale_line': mapping['sale_line'],
            'distribution_raw_eur': raw_distribution,
            'distribution_paid_eur': paid_distribution,
            'distribution_taxable_control_eur': taxable_distribution,
            'sale_raw_eur': raw_sale,
            'sale_taxable_control_eur': taxable_sale,
            'tageskurs_raw_eur': raw_fx_delta,
            'sale_before_advance_lump_sum_deduction': sale_activity,
        }
        details.append(detail)
        add_line(
            mapping['distribution_line'], 'distribution', classification, mapping,
            raw_distribution, taxable_distribution,
        )
        add_line(
            mapping['sale_line'], 'sale', classification, mapping, raw_sale, taxable_sale,
        )

    warnings = []
    if blocked_isins:
        warnings.append(
            'Fondsart muss vor der Übernahme in die Steuererklärung '
            'bestätigt werden: ' + ', '.join(blocked_isins)
        )
    if has_sale_activity:
        warnings.append(
            'Veräußerungswerte berücksichtigen noch keine bereits '
            'versteuerten Vorabpauschalen und sind daher vorläufig.'
        )
    if negative_distribution_details:
        paid_total = sum(
            d['paid_distribution_eur'] for d in negative_distribution_details
        )
        paid_isins = ', '.join(
            f"{d['ticker']} ({d['paid_distribution_eur']:.2f} EUR)"
            for d in negative_distribution_details
        )
        warnings.append(
            'Gezahlte Dividenden/Ersatzzahlungen auf Short-Positionen '
            f'({paid_total:.2f} EUR gesamt: {paid_isins}) sind keine '
            'negativen Ausschüttungen und wurden NICHT in die '
            'Ausschüttungszeilen eingerechnet. Die steuerliche Behandlung '
            'gezahlter Ersatzzahlungen ist manuell zu prüfen.'
        )

    if blocked_isins:
        status = 'classification_review_required'
    elif negative_distribution_details:
        status = 'paid_distribution_review_required'
    elif has_sale_activity:
        status = 'advance_lump_sum_review_required'
    else:
        status = 'complete_for_distributions'

    return {
        'lines': [line_totals[key] for key in sorted(line_totals)],
        'details': details,
        'blocked_isins': blocked_isins,
        'blocked_details': blocked_details,
        'negative_distribution_details': negative_distribution_details,
        'status': status,
        'warnings': warnings,
        'includes_tageskurs': bool(include_tageskurs),
        'withholding_tax_form': 'Anlage KAP Zeile 41',
        'sale_values_final': not has_sale_activity,
    }

def get_kap_inv_tageskurs_delta_for_reporting(report_data):
    """Return KAP-INV Tageskurs delta after Teilfreistellung, with legacy fallback."""
    if not report_data:
        return 0.0
    if 'fx_correction_kap_inv_taxable' in report_data:
        return safe_float(report_data.get('fx_correction_kap_inv_taxable'))
    return safe_float((report_data.get('fx_correction_by_topf') or {}).get('KAP-INV'))

def get_no_invstg_summary(report_data, include_tageskurs=False):
    """Aggregate no-InvStG instruments by ISIN from final trade details.

    Trade rows already contain Stillhalter/cross-year corrections. Optional
    Tageskurs deltas are added separately so the summary stays reconcilable.
    """
    from etf_classification import get_etf_info, get_routing_classification

    report_data = report_data or {}
    summary = {}
    anlage_so_overrides = set(report_data.get('anlage_so_overrides_applied') or [])

    def ensure_entry(isin):
        if isin not in summary:
            info = get_etf_info(isin) or {}
            summary[isin] = {
                'ticker': info.get('ticker', isin[:12]),
                'name': info.get('name', ''),
                'gain': 0.0,
                'loss': 0.0,
                'tageskurs': 0.0,
                'div': 0.0,
                'wht': 0.0,
            }
        return summary[isin]

    # Auch reine Käufe ohne realisierten Gewinn sichtbar machen.
    for isin in report_data.get('all_traded_etf_isins', []) or []:
        if (isin and isin not in anlage_so_overrides
                and get_routing_classification(isin) == 'no_invstg'):
            ensure_entry(isin)

    for isin, income in (report_data.get('no_invstg_income_by_isin') or {}).items():
        if (not isin or isin in anlage_so_overrides
                or get_routing_classification(isin) != 'no_invstg'):
            continue
        entry = ensure_entry(isin)
        entry['div'] += safe_float(income.get('div'))
        entry['wht'] += safe_float(income.get('wht'))

    for row in report_data.get('trade_details', []) or []:
        isin = (row.get('isin') or '').strip()
        if (row.get('assetCategory') != 'STK' or row.get('topf') != 'Topf2'
                or not isin or isin in anlage_so_overrides
                or get_routing_classification(isin) != 'no_invstg'):
            continue
        pnl = safe_float(row.get('pnl_eur'))
        entry = ensure_entry(isin)
        if pnl >= 0:
            entry['gain'] += pnl
        else:
            entry['loss'] += pnl

    if include_tageskurs:
        for lot in report_data.get('fx_correction_details', []) or []:
            isin = (lot.get('isin') or '').strip()
            if (lot.get('topf') != 'Topf2' or not isin
                    or isin in anlage_so_overrides
                    or get_routing_classification(isin) != 'no_invstg'):
                continue
            ensure_entry(isin)['tageskurs'] += safe_float(lot.get('delta_eur'))

    for entry in summary.values():
        entry['trade_net'] = entry['gain'] + entry['loss'] + entry['tageskurs']
        entry['total'] = entry['trade_net'] + entry['div']
        entry['wht_reported'] = get_withholding_tax_for_reporting(entry['wht'])

    return summary

GERMAN_DIVIDEND_TAX_TOTAL_RATE = 0.26375
GERMAN_KEST_RATE = 0.25
GERMAN_SOLI_RATE = 0.01375

def is_de_isin(row):
    return row.get('isin', '').strip().upper().startswith('DE')

def funds_match_key(row):
    return (
        row.get('reportDate') or row.get('date') or '',
        row.get('isin', '').strip().upper(),
        row.get('symbol', '').strip().upper(),
    )

def is_german_dividend_tax_row(row):
    desc = (row.get('activityDescription') or '').lower()
    code = (row.get('activityCode') or '').strip().upper()
    has_de_tax_marker = (
        'de steuer' in desc
        or 'de tax' in desc
        or '- de steuer' in desc
        or '- de tax' in desc
    )
    return is_de_isin(row) and has_de_tax_marker and (code in ('', 'FRTAX', 'WHT'))

def get_exchange_rates(trades, funds):
    # Map Date -> USD_to_EUR rate
    # fxRateToBase for EUR records = EUR -> USD (e.g. 1.05 means 1 EUR = 1.05 USD)
    # We need USD -> EUR = 1 / fxRateToBase.
    #
    # IMPORTANT: statement_of_funds.csv contains EUR-traded instruments (e.g. ETPs on
    # European exchanges) with fxRateToBase=1, because IBKR books EUR->EUR cash flows
    # without a real FX conversion. These bogus 1.0 values must be excluded.
    #
    # Strategy:
    #   1. Process funds first (lower priority)
    #   2. Process trades second — trades always overwrite funds for the same date
    #   3. Reject any rate outside the plausible EUR/USD range [0.70, 1.30]

    RATE_MIN, RATE_MAX = 0.70, 1.30  # plausible USD-per-EUR bounds

    rates = {}

    # funds first (lower priority — may contain bogus fxRateToBase=1 entries)
    for r in funds:
        curr = r.get('currency')
        fx = r.get('fxRateToBase')
        date_str = r.get('date') or r.get('reportDate')
        if curr == 'EUR' and fx and date_str:
            d = parse_date(date_str)
            try:
                rate = float(fx)
                if abs(rate - 1.0) < 0.001:
                    continue  # Skip bogus EUR-native bookings (fxRateToBase=1.0)
                eur_per_usd = 1.0 / rate
                if RATE_MIN < eur_per_usd < RATE_MAX:
                    rates[d] = eur_per_usd
            except:
                pass

    # trades second — overwrite any fund rate for the same date (trades are more reliable)
    for r in trades:
        curr = r.get('currency')
        fx = r.get('fxRateToBase')
        date_str = r.get('date') or r.get('dateTime') or r.get('reportDate')
        if curr == 'EUR' and fx and date_str:
            d = parse_date(date_str)
            try:
                rate = float(fx)
                eur_per_usd = 1.0 / rate
                if RATE_MIN < eur_per_usd < RATE_MAX:
                    rates[d] = eur_per_usd
            except:
                pass

    return rates

def fetch_ecb_rates(tax_year):
    """Statische EZB-Referenzkurse USD→EUR für das Steuerjahr laden.

    Verwendet eingebettete Kursdaten aus ecb_rates.py (offline, kein Internet nötig).
    Verfügbar: 2024, 2025. Für andere Jahre: leeres dict.
    Returns dict {date -> eur_per_usd}.
    """
    try:
        from ecb_rates import get_ecb_rates
        return get_ecb_rates(tax_year)
    except ImportError:
        print(f"  EZB-Kursmodul (ecb_rates.py) nicht gefunden.")
        return {}

def get_rate_for_date(target_date, rates_map):
    if not rates_map:
        raise RuntimeError(
            f"get_rate_for_date({target_date}) ohne Wechselkurs-Map aufgerufen — "
            f"calculate_tax muss USD-Base-Validierung am Eingang sicherstellen."
        )

    if target_date in rates_map:
        return rates_map[target_date]

    sorted_dates = sorted(rates_map.keys())

    # Use most recent prior date (financial convention)
    prior_dates = [d for d in sorted_dates if d <= target_date]
    if prior_dates:
        return rates_map[prior_dates[-1]]
    # If target is before all data, use earliest available
    return rates_map[sorted_dates[0]]

def parse_ibkr_csv_report(csv_path):
    """
    Parst den IBKR Standard-Bericht ("Übersicht: realisierter G&V") als CSV.

    Extrahiert:
    - FX-Gewinne/Verluste per Währung aus der "Devisen"-Kategorie
    - Kategorie-Summen für Plausibilitätscheck (Aktien, Optionen, Futures, etc.)

    Returns:
        dict with 'fx_results', 'fx_total_gain', 'fx_total_loss', 'category_totals'
    """
    import csv as csv_module
    import io

    fx_results = {}
    fx_total_gain = 0.0
    fx_total_loss = 0.0
    category_totals = {}  # {category: {gain, loss, net}}
    income_totals = {}  # {dividends_eur, interest_eur, withholding_tax_eur}

    last_category = None

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        for line in f:
            parts = list(csv_module.reader(io.StringIO(line)))[0]

            # Dividenden/Zinsen/Quellensteuer EUR totals
            # Multi-Currency-CSVs haben eine explizite "Gesamt X in EUR"-Zeile (echte Summe
            # über alle Währungen). Single-Currency-CSVs haben nur "Gesamtwert in EUR"
            # (USD-Teil umgerechnet = Gesamt). Präzise Zeile gewinnt, Gesamtwert ist Fallback.
            if len(parts) >= 6:
                field = parts[2].strip() if len(parts) > 2 else ''
                if line.startswith('Dividenden,Data,Gesamt Dividenden in EUR'):
                    income_totals['dividends_eur'] = safe_float(parts[5], 0)
                    continue
                elif line.startswith('Dividenden,Data,Gesamtwert in EUR'):
                    if 'dividends_eur' not in income_totals:
                        income_totals['dividends_eur'] = safe_float(parts[5], 0)
                    continue
                elif line.startswith('Zinsen,Data,Gesamt Zinsen in EUR'):
                    income_totals['interest_eur'] = safe_float(parts[5], 0)
                    continue
                elif line.startswith('Zinsen,Data,Gesamtwert in EUR'):
                    if 'interest_eur' not in income_totals:
                        income_totals['interest_eur'] = safe_float(parts[5], 0)
                    continue
                elif line.startswith('Quellensteuer,Data,Gesamt Quellensteuer in EUR'):
                    income_totals['withholding_tax_eur'] = safe_float(parts[5], 0)
                    continue
                elif line.startswith('Quellensteuer,Data,Gesamtwert in EUR'):
                    if 'withholding_tax_eur' not in income_totals:
                        income_totals['withholding_tax_eur'] = safe_float(parts[5], 0)
                    continue

            if not line.startswith('Übersicht  zur realisierten und unrealisierten Performance,Data,'):
                continue
            if len(parts) < 10:
                continue

            category = parts[2].strip()

            if category == 'Gesamt (Alle Vermögenswerte)':
                continue

            if category == 'Gesamt':
                # Summary row for previous category
                if last_category:
                    g = safe_float(parts[5], 0) + safe_float(parts[7], 0)  # ST + LT gain
                    l = safe_float(parts[6], 0) + safe_float(parts[8], 0)  # ST + LT loss
                    n = safe_float(parts[9], 0)
                    category_totals[last_category] = {'gain': g, 'loss': l, 'net': n}
                continue

            last_category = category

            # Individual currency rows in "Devisen" category
            if category == 'Devisen':
                curr = parts[3].strip()
                if not curr:
                    continue
                g = safe_float(parts[5], 0) + safe_float(parts[7], 0)
                l = safe_float(parts[6], 0) + safe_float(parts[8], 0)
                n = safe_float(parts[9], 0)
                if abs(g) > 0.01 or abs(l) > 0.01:
                    fx_results[curr] = {
                        'gain': g,
                        'loss': abs(l) if l > 0 else -l if l < 0 else 0,  # ensure loss is stored negative
                        'net': n,
                        'lots_remaining': 0,
                        'disposals_count': 0,
                    }
                    # loss from CSV is already negative
                    fx_results[curr]['loss'] = l
                    fx_total_gain += g
                    fx_total_loss += l

    return {
        'fx_results': fx_results,
        'fx_total_gain': fx_total_gain,
        'fx_total_loss': fx_total_loss,
        'category_totals': category_totals,
        'income_totals': income_totals,
    }


# --- FX Lot-Inventar mit Saldo-Tracking (Margin-Korrektur, Issue #59) ---
#
# Steuerrechtliche Grundlage: BMF 14.05.2025 Rn. 131 ordnet Währungsgewinne/
# -verluste aus verzinslichem Fremdwährungsguthaben §20 Abs. 2 S. 1 Nr. 7
# i.V.m. Abs. 4 S. 1 EStG zu. Eine Margin-Verbindlichkeit ist kein Guthaben:
# Abflüsse aus negativem Saldo erzeugen keinen steuerbaren Vorgang, Zuflüsse auf
# negatives Konto tilgen Schuld (keine Lot-Erzeugung bis Saldo positiv).
#
# Die Engine läuft zwei FIFO-Inventare parallel pro Währung:
#   - `lots_corrected`: alle Cash-Events (inkl. BUY/SELL/ADJ) sichtbar, Saldo-Gate
#     filtert PnL auf positiv-gedeckte Anteile. Gewählte Form für Topf 2.
#   - `lots_raw`: alte Logik (BUY/SELL/ADJ unsichtbar, kein Saldo-Gate). Nur
#     als Vergleichswert für UI/Plausibilität.

_FX_PNL_OFF_CODES = frozenset({'BUY', 'SELL', 'ADJ', 'DINT', ''})
_FX_LEGACY_SKIP_CODES = frozenset({'BUY', 'SELL', 'ADJ', ''})


def _parse_fx_opt_desc(desc):
    """Parse 'OPT: -1 TLT 240126P00095000' aus fx_realized_pnl.activityDescription.

    Liefert dict mit symbol/expiry/putCall/strike oder None, falls Format nicht passt.
    """
    import re
    m = re.match(r'^OPT:\s+(-?\d+)\s+(\S+)\s+(\d{6})([CP])(\d{8})$', desc or '')
    if not m:
        return None
    return {
        'symbol': m.group(2),
        'expiry': m.group(3),         # YYMMDD aus OPT-String
        'putCall': m.group(4),
        'strike': int(m.group(5)) / 1000.0,
    }


def _parse_fx_inst_symbol(desc):
    """Parse erstes Symbol nach Asset-Type-Präfix aus fx_realized_pnl.activityDescription.

    'STK: 100 TLT' → 'TLT'
    'OPT: -1 TLT 240126P00095000' → 'TLT'
    'BILL: 60000 912797HT7' → '912797HT7'
    """
    import re
    m = re.match(r'^(?:STK|OPT|FOP|FSFOP|BILL|BOND):\s+-?\d+\s+(\S+)', desc or '')
    return m.group(1) if m else None


def _resolve_fx_outflows(fx_pnl_rows, fx_balance_timeline, tax_year):
    """Pre-resolve aller fx_realized_pnl-Outflows zu prev_balance in der Cash-Timeline.

    Vier Match-Passes (greedy, mit consumed-Set pro Currency):
      1. Exakter Match auf (date, currency, amount) zwischen fx_realized_pnl.quantity
         und fx_transactions.amount. Trifft die Mehrheit der Single-Trade-Outflows.
      2. Description-Aggregat: alle Outflows mit gleicher (date, currency, description)
         werden zu einer Gruppe; ihre summierte quantity wird gegen amount gematched.
         Trifft IBKRs FIFO-Splits einer Buchung (z.B. zwei Lot-Auflösungen einer
         FRTAX-Buchung).
      3. OPT-Description-Parser: fx_realized_pnl-Beschreibungen vom Format
         'OPT: 1 TLT 240126P00095000' werden gegen StmtFunds-Rows mit passendem
         symbol/strike/expiry/putCall gematched (entkoppelt von amount).
      4. Symbol-Aggregat: ungematchte Outflows pro (date, currency, symbol) werden
         summiert und gegen einen größeren Cash-Event gematched. Trifft Multi-Split
         eines einzigen STK-Trades (z.B. STK: 2 QQQ + STK: 6 QQQ + ... vs.
         'Buy 30 INVESCO QQQ TRUST').

    Returns (resolved_dict, consumed_dict):
        resolved_dict[row_idx] = {'prev_balance': float, 'aggregat_qty': float,
                                  'match_type': 'exact'/'aggregat'/'opt_parse'/'symbol_aggregat'}
        consumed_dict[currency] = set(timeline_idx) — verbrauchte Cash-Events.
            Muss in einen nachfolgenden Fallback-Matcher weitergereicht werden,
            damit derselbe Cash-Event nicht zweimal als prev-Balance-Quelle dient
            (siehe Codex-Hinweis 2026-05-27 zum Duplicate-Split-Bug).
    """
    import re
    from collections import defaultdict

    # Sammle alle relevanten Outflows mit ihrem Listen-Index.
    # WICHTIG (Codex-Fix 2026-05-27): KEIN abs(pnl)-Filter hier. IBKR emittiert
    # FIFO-Splits, bei denen eine Leg zufällig realizedPL=0 haben kann (z.B. wenn
    # Lot-Rate exakt der Disposal-Rate entspricht oder bei BookTrade-Settlements).
    # Würden diese Null-PnL-Legs verworfen, würde die Aggregat-Summe in Pass 2/4
    # nicht mehr zum Cash-Event passen und die Geschwister-Rows mit echtem PnL
    # blieben fälschlich im IBKR-Rohwert. Der Aufrufer (Option-A-Loop) filtert
    # Null-PnL-Rows danach sowieso aus der PnL-Buchung aus.
    outflows = []
    for idx, row in enumerate(fx_pnl_rows):
        rd = parse_date(row.get('reportDate'))
        if not rd or rd.year != tax_year:
            continue
        qty = safe_float(row.get('quantity'), 0)
        curr = row.get('fxCurrency', '')
        if qty >= 0 or not curr:
            continue
        if not fx_balance_timeline.get(curr):
            continue
        outflows.append({
            'idx': idx,
            'date_short': row.get('reportDate', '')[:10],
            'curr': curr,
            'qty': qty,
            'desc': row.get('activityDescription', ''),
        })

    resolved = {}
    # Per-Currency consumed-Set der Timeline-Indices
    consumed = defaultdict(set)

    def _find_exact_match(curr, date_short, target_amt, require_unique=False):
        """Liefert (timeline_idx, prev_balance) oder (None, None).

        require_unique=False (Default, Pass 1): greedy, gibt den ersten passenden
        Cash-Event zurück. Bei mehreren passenden Events am Tag verteilt sich die
        Korrelation natürlich, wenn mehrere fx_realized_pnl-Rows die gleiche
        quantity haben (jeder Single-Row matched einen anderen Cash-Event).

        require_unique=True (Pass 2/4): matched nur, wenn am Tag GENAU EIN
        passender Cash-Event existiert. Bei Aggregat-Matches sind mehrere
        fx_realized_pnl-Rows zu einer Gruppe zusammengefasst — wenn am Tag zwei
        unabhängige Cash-Events mit dem aggregierten amount existieren, ist nicht
        klar, welcher zur Gruppe gehört. Konservativ: kein Match, Fallback nutzen.
        (Codex-Hinweis 2026-05-27.)
        """
        timeline = fx_balance_timeline.get(curr, [])
        matches = []
        for i, (d, _txid, amt, prev, _after) in enumerate(timeline):
            if i in consumed[curr]:
                continue
            if d != date_short:
                continue
            if amt * target_amt <= 0:  # opposite sign
                continue
            if abs(amt - target_amt) < 0.01:
                if not require_unique:
                    return i, prev
                matches.append((i, prev))
        if require_unique and len(matches) == 1:
            return matches[0]
        return None, None

    # Pass 1: Exakter (date, currency, amount)-Match (greedy)
    for ev in outflows:
        ti, prev = _find_exact_match(ev['curr'], ev['date_short'], ev['qty'])
        if ti is not None:
            consumed[ev['curr']].add(ti)
            resolved[ev['idx']] = {'prev_balance': prev, 'aggregat_qty': ev['qty'],
                                   'match_type': 'exact'}

    # Pass 2: Aggregat pro (date, currency, description) — IBKR FIFO-Splits einer Buchung
    # require_unique: bei zwei Cash-Events mit gleichem amount am Tag ist die
    # Zuordnung nicht eindeutig.
    buckets = defaultdict(list)
    for ev in outflows:
        if ev['idx'] in resolved:
            continue
        buckets[(ev['date_short'], ev['curr'], ev['desc'])].append(ev)
    for (date_short, curr, _desc), group in buckets.items():
        if not group:
            continue
        sum_qty = sum(g['qty'] for g in group)
        ti, prev = _find_exact_match(curr, date_short, sum_qty, require_unique=True)
        if ti is not None:
            consumed[curr].add(ti)
            for g in group:
                resolved[g['idx']] = {'prev_balance': prev, 'aggregat_qty': sum_qty,
                                      'match_type': 'aggregat'}

    # Pass 3: OPT-Description-Parser — Match auf symbol/strike/expiry/putCall
    for ev in outflows:
        if ev['idx'] in resolved:
            continue
        opt = _parse_fx_opt_desc(ev['desc'])
        if not opt:
            continue
        timeline = fx_balance_timeline.get(ev['curr'], [])
        # Hier brauchen wir Zugriff auf die StmtFunds-Attribute (symbol/strike/expiry/putCall).
        # Die fx_balance_timeline hat aber nur (date, txid, amount, prev, after). Daher
        # Pass 3 nur sinnvoll, wenn wir Timeline mit Description erweitern. Tun wir hier
        # nicht — überspringe, weil Pass 1+2 für die meisten Fälle reicht und Pass 4
        # ähnliche Fälle abdeckt.
        # (Lass den Match-Type 'opt_parse' im API für künftige Erweiterung)
        pass

    # Pass 4: Symbol-Aggregat — mehrere Split-Outflows pro (date, currency, symbol)
    # Auch hier require_unique, weil mehrere Cash-Events mit gleichem aggregierten
    # amount nicht sicher dem Symbol-Bucket zugeordnet werden können.
    sym_buckets = defaultdict(list)
    for ev in outflows:
        if ev['idx'] in resolved:
            continue
        sym = _parse_fx_inst_symbol(ev['desc'])
        if not sym:
            continue
        sym_buckets[(ev['date_short'], ev['curr'], sym)].append(ev)
    for (date_short, curr, _sym), group in sym_buckets.items():
        if len(group) < 2:
            continue  # Single-row hätte Pass 1 oder 2 erwischt
        sum_qty = sum(g['qty'] for g in group)
        ti, prev = _find_exact_match(curr, date_short, sum_qty, require_unique=True)
        if ti is not None:
            consumed[curr].add(ti)
            for g in group:
                resolved[g['idx']] = {'prev_balance': prev, 'aggregat_qty': sum_qty,
                                      'match_type': 'symbol_aggregat'}

    return resolved, dict(consumed)


def _fx_event_sort_key(date_str, txid):
    """Consistent ordering for same-day FX cash events."""
    if not txid:
        return (date_str, 0, 0, '')
    try:
        return (date_str, 0, int(txid), '')
    except (TypeError, ValueError):
        return (date_str, 1, 0, str(txid))


def _add_fx_negative_days(day_set, start_date, end_date, tax_year):
    """Add calendar days in tax_year for which a currency balance was negative."""
    if not start_date or not end_date:
        return
    start = max(start_date, datetime(tax_year, 1, 1).date())
    end = min(end_date, datetime(tax_year, 12, 31).date())
    while start <= end:
        day_set.add(start.isoformat())
        start += timedelta(days=1)


def _negative_days_from_balance_timeline(timeline_rows, starting_date_str, starting_balance, tax_year):
    """Calendar days in tax_year where a balance was negative at any point."""
    days = set()
    last_date = parse_date(starting_date_str)
    prev_balance = float(starting_balance or 0)

    for d, _txid, _amt, _prev, after in timeline_rows:
        current_date = parse_date(d)
        if current_date:
            if prev_balance < -0.01 and last_date:
                _add_fx_negative_days(days, last_date, current_date, tax_year)
            if after < -0.01:
                _add_fx_negative_days(days, current_date, current_date, tax_year)
            last_date = current_date
        prev_balance = after

    if prev_balance < -0.01 and last_date:
        _add_fx_negative_days(days, last_date, datetime(tax_year, 12, 31).date(), tax_year)

    return days


def _init_fx_state(starting_balance, sb_date_str, sb_rate):
    """State-Dict für eine Währung initialisieren."""
    state = {
        'balance': float(starting_balance or 0),
        'lots_corrected': deque(),
        'lots_raw': deque(),
        'gain_corrected': 0.0,
        'loss_corrected': 0.0,
        'gain_raw': 0.0,
        'loss_raw': 0.0,
        'disposals_corrected': 0,
        'disposals_raw': 0,
        'days_negative': set(),
        'last_balance_date': parse_date(sb_date_str),
    }
    # Positive Anfangsbestände ohne brauchbare Rate bleiben als unbewertete Lots
    # im FIFO. So blockieren sie spätere, jüngere Lots korrekt, erzeugen aber keinen
    # Phantom-PnL aus einem erfundenen Kurs.
    if state['balance'] > 0.01:
        lot_rate = sb_rate if sb_rate and sb_rate > 0 else None
        state['lots_corrected'].append([sb_date_str, state['balance'], lot_rate])
        state['lots_raw'].append([sb_date_str, state['balance'], lot_rate])
    return state


def _process_fx_event(state, date_str, amount, fx, activity_code, tax_year):
    """Verarbeitet ein FX-Event; mutiert state.

    Wichtig: Der corrected-Pfad sieht ALLE Events (inkl. BUY/SELL/ADJ), damit das
    Lot-Inventar dem realen Cash-Saldo folgt. PnL-Buchung wird per activity_code
    gated:
      - BUY/SELL/ADJ/DINT/leer: Lots werden konsumiert/erzeugt, aber kein FX-PnL gebucht
        (BUY/SELL: FX-Effekt liegt in fifoPnlRealized + Tageskurs-Korrektur;
         ADJ: in Future-fifoPnlRealized;
         DINT: Schuldzinsen sind keine Veräußerung von Fremdwährungsguthaben).
      - Alle anderen Codes (DIV, FRTAX, FOREX, CINT, PIL, DEP, WITH, INTR, INTP,
        OFEE, CORP): Lot-Konsum/Erzeugung UND PnL-Buchung.
    """
    allow_pnl_corrected = activity_code not in _FX_PNL_OFF_CODES
    skip_legacy = activity_code in _FX_LEGACY_SKIP_CODES
    event_rate = fx if fx and fx > 0 else None

    date = parse_date(date_str)
    in_tax_year = bool(date) and date.year == tax_year
    prev = state['balance']

    if date and prev < -0.01 and state.get('last_balance_date'):
        _add_fx_negative_days(state['days_negative'], state['last_balance_date'], date, tax_year)

    state['balance'] = prev + amount

    if date and state['balance'] < -0.01:
        _add_fx_negative_days(state['days_negative'], date, date, tax_year)
    if date:
        state['last_balance_date'] = date

    # --- Raw-Pfad (alte Logik, nur PnL-Events, kein Saldo-Gate) ---
    if not skip_legacy:
        if amount > 0:
            state['lots_raw'].append([date_str, amount, event_rate])
        else:
            remaining = abs(amount)
            while remaining > 0.001 and state['lots_raw']:
                lot_date, lot_qty, lot_rate = state['lots_raw'][0]
                take = min(remaining, lot_qty)
                if in_tax_year and event_rate is not None and lot_rate is not None:
                    pnl = take * (event_rate - lot_rate)
                    if pnl > 0:
                        state['gain_raw'] += pnl
                    else:
                        state['loss_raw'] += pnl
                    state['disposals_raw'] += 1
                remaining -= take
                lot_qty -= take
                if lot_qty < 0.001:
                    state['lots_raw'].popleft()
                else:
                    state['lots_raw'][0][1] = lot_qty

    # --- Corrected-Pfad (alle Events, Saldo-Gate) ---
    if amount > 0:
        # Zufluss: Erst Schuld tilgen, Rest wird FIFO-Lot
        tilgung = max(0.0, min(amount, -prev))
        lot_amount = amount - tilgung
        if lot_amount > 0.001:
            state['lots_corrected'].append([date_str, lot_amount, event_rate])
    else:
        # Abfluss
        if prev <= 0:
            # Alles aus Schuld → kein steuerbarer Vorgang, kein Lot-Konsum
            return
        from_credit = min(abs(amount), prev)
        remaining = from_credit
        while remaining > 0.001 and state['lots_corrected']:
            lot_date, lot_qty, lot_rate = state['lots_corrected'][0]
            take = min(remaining, lot_qty)
            if allow_pnl_corrected and in_tax_year and event_rate is not None and lot_rate is not None:
                pnl = take * (event_rate - lot_rate)
                if pnl > 0:
                    state['gain_corrected'] += pnl
                else:
                    state['loss_corrected'] += pnl
                state['disposals_corrected'] += 1
            remaining -= take
            lot_qty -= take
            if lot_qty < 0.001:
                state['lots_corrected'].popleft()
            else:
                state['lots_corrected'][0][1] = lot_qty


def _finalize_fx_state(state, tax_year):
    """Extend negative-balance day count through tax-year end if still negative."""
    last_date = state.get('last_balance_date')
    if state['balance'] < -0.01 and last_date:
        _add_fx_negative_days(state['days_negative'], last_date, datetime(tax_year, 12, 31).date(), tax_year)


def calculate_fx_gains(trades, fx_transactions, tax_year, base_currency='EUR'):
    """
    Berechnet FIFO-basierte Fremdwährungs-Gewinne/Verluste pro Währung.

    Verwendet fx_transactions.csv (StmtFunds Currency-Level) mit Raten-Substitution:
    - Einträge mit fxRateToBase ≈ 1.0 (unbrauchbar auf Aggregat-Ebene) erhalten
      den Tageskurs aus trades.csv (fxRateToBase der Trades an diesem Tag)
    - BUY/SELL/ADJ werden im corrected-Pfad mit eingerechnet (Saldo + Lot-Inventar),
      lösen aber keinen steuerlichen FX-PnL aus
    - FOREX, DIV, FRTAX, Zinsen, Gebühren etc. werden als FX-Ereignisse mit
      PnL-Buchung getrackt

    Lots werden über alle Jahre aufgebaut (Multi-Year-Support), aber Gewinne/Verluste
    werden nur für Abflüsse im tax_year gezählt.

    Returns:
        dict per currency (mit gain/loss/net + raw_gain/raw_loss + days_negative),
        float total_gain (corrected), float total_loss (corrected),
        bool has_prior_data
    """
    import bisect

    # --- Build daily rate maps per currency from trades.csv ---
    daily_rates_raw = defaultdict(lambda: defaultdict(list))
    for t in trades:
        curr = t.get('currency', '')
        fx = safe_float(t.get('fxRateToBase'), 0)
        dt = (t.get('dateTime') or '')[:10]
        if curr and fx > 0 and dt:
            daily_rates_raw[curr][dt].append(fx)

    rate_maps = {}
    sorted_dates_map = {}
    for curr, dates in daily_rates_raw.items():
        rate_maps[curr] = {d: sum(r) / len(r) for d, r in dates.items()}
        sorted_dates_map[curr] = sorted(rate_maps[curr].keys())

    def get_daily_rate(curr, day):
        """Get rate for currency on date, interpolating to nearest available date."""
        cmap = rate_maps.get(curr, {})
        if day in cmap:
            return cmap[day]
        sorted_d = sorted_dates_map.get(curr, [])
        if not sorted_d:
            return 0
        idx = bisect.bisect_left(sorted_d, day)
        if idx == 0:
            return cmap[sorted_d[0]]
        if idx >= len(sorted_d):
            return cmap[sorted_d[-1]]
        return cmap[sorted_d[idx - 1]]  # use previous available day

    # --- Process fx_transactions: corrected-Pfad sieht ALLE Events, raw-Pfad
    # nur PnL-relevante (alte Logik). Engine intern unterscheidet via activityCode. ---
    by_currency = defaultdict(list)
    starting_balances = {}  # curr -> (balance, date_str, fx)

    # Detect multi-year data
    starting_balance_total = 0.0
    for tx in fx_transactions:
        if tx.get('activityDescription') == 'Starting Balance':
            starting_balance_total += abs(safe_float(tx.get('balance'), 0))

    has_prior_data = starting_balance_total < 100

    for tx in fx_transactions:
        curr = tx.get('currency', '')
        if not curr:
            continue

        activity_desc = tx.get('activityDescription', '')
        code = tx.get('activityCode', '')

        # Starting Balance → wird in starting_balances gesammelt (auch negative Werte,
        # damit der Saldo-Tracker im corrected-Pfad mit der Schuld startet)
        if activity_desc == 'Starting Balance':
            balance = safe_float(tx.get('balance'), 0)
            date_str = tx.get('date', '')
            fx = safe_float(tx.get('fxRateToBase'), 0)
            if fx <= 0 or abs(fx - 1.0) < 0.001:
                fx = get_daily_rate(curr, date_str[:10])
            # Kein Rate-Fallback auf 1.0 (würde bei positivem SB Phantom-PnL erzeugen).
            # Bei fx<=0 wird ein unbewerteter Lot angelegt: FIFO-Reihenfolge und Saldo
            # bleiben korrekt, PnL wird erst bei Events mit bekannter Basis gerechnet.
            starting_balances[curr] = (balance, date_str, fx if fx > 0 else 0.0)
            continue

        # Ending Balance: nicht verarbeiten
        if activity_desc == 'Ending Balance':
            continue

        date_str = tx.get('date', '')
        amount = safe_float(tx.get('amount'), 0)
        if abs(amount) < 0.001:
            continue

        fx = safe_float(tx.get('fxRateToBase'), 0)

        # Rate substitution for entries with fxRateToBase ≈ 1.0
        if fx <= 0 or abs(fx - 1.0) < 0.001:
            # Prefer daily rate from trades.csv (date-specific)
            fx = get_daily_rate(curr, date_str[:10])
            # Fallback for currencies with no trade data: FOREX tradePrice
            if fx <= 0 and code == 'FOREX':
                symbol = tx.get('symbol', '')
                tp = safe_float(tx.get('tradePrice'), 0)
                if symbol.startswith('EUR.') and tp > 0:
                    fx = 1.0 / tp

        txid = tx.get('transactionID', '')
        # Auch ohne Rate muss der Saldo-Tracker das Event sehen. Die Engine führt
        # solche Beträge als unbewertete Lots und unterdrückt nur die PnL-Buchung.
        by_currency[curr].append((date_str, txid, amount, fx if fx > 0 else 0.0, code))

    # --- Engine-Run per currency ---
    if has_prior_data:
        print(f"FX: Multi-Year-Daten erkannt. FIFO-Lots werden vollständig aufgebaut.")
    elif starting_balance_total > 0.01:
        print(f"FX: Nur Steuerjahr {tax_year} geladen. Anfangsbestände ({starting_balance_total:,.0f} Fremdwährung) "
              f"werden zum 01.01.-Kurs angesetzt (Vereinfachung).")

    results = {}
    total_gain = 0.0
    total_loss = 0.0

    for curr in sorted(by_currency.keys()):
        events = sorted(by_currency[curr], key=lambda ev: _fx_event_sort_key(ev[0], ev[1]))
        sb_balance, sb_date, sb_fx = starting_balances.get(curr, (0.0, '', 1.0))
        state = _init_fx_state(sb_balance, sb_date, sb_fx)

        for date_str, _txid, amount, fx, code in events:
            _process_fx_event(state, date_str, amount, fx, code, tax_year)
        _finalize_fx_state(state, tax_year)

        gain = state['gain_corrected']
        loss = state['loss_corrected']
        raw_gain = state['gain_raw']
        raw_loss = state['loss_raw']
        days_neg = len(state['days_negative'])

        # Currency in results aufnehmen, wenn PnL existiert ODER Margin-Phasen vorlagen
        # (auch ohne PnL relevant für UI-Anzeige der negativen Tage).
        has_any = (abs(gain) > 0.01 or abs(loss) > 0.01
                   or abs(raw_gain) > 0.01 or abs(raw_loss) > 0.01
                   or days_neg > 0)
        if has_any:
            results[curr] = {
                'gain': gain,
                'loss': loss,
                'net': gain + loss,
                'lots_remaining': len(state['lots_corrected']),
                'disposals_count': state['disposals_corrected'],
                'raw_gain': raw_gain,
                'raw_loss': raw_loss,
                'raw_net': raw_gain + raw_loss,
                'raw_disposals_count': state['disposals_raw'],
                'days_negative': days_neg,
                'final_balance': state['balance'],
                'starting_balance': sb_balance,
            }
            total_gain += gain
            total_loss += loss

    return results, total_gain, total_loss, has_prior_data


def _get_open_option_sells(trades, a_cat, strike, expiry, pc, assignment_qty_for_series,
                           underlying=None):
    """Return only SELL trades still open after FIFO-consuming closed positions.

    IBKR may have multiple SELL ExchTrades for the same option series (strike/expiry/putCall).
    Some may have been bought back (BUY ExchTrade) or expired worthless before an assignment.
    This function uses FIFO to determine which sells are still open:
      close_qty = total_sell_qty - assignment_qty_for_series
    The oldest close_qty sells are consumed; the remaining are returned with '_open_qty' set.

    Wenn `underlying` angegeben ist, werden nur Sells fuer dieses Underlying
    beruecksichtigt — wichtig, weil verschiedene Aktien dieselbe strike/expiry-
    Kombination haben koennen (z.B. KWEB P 30 exp 2024-12-20 vs FXI P 30 exp 2024-12-20).
    """
    all_sells = sorted(
        [t for t in trades
         if t.get('assetCategory') == a_cat
         and t.get('transactionType') == 'ExchTrade'
         and t.get('strike') == strike
         and t.get('expiry') == expiry
         and t.get('putCall') == pc
         and t.get('buySell') == 'SELL'
         and (underlying is None or t.get('underlyingSymbol', '') == underlying)],
        key=lambda t: t.get('dateTime', '') or t.get('tradeDate', '')
    )
    total_sell_qty = sum(abs(int(safe_float(t.get('quantity')))) for t in all_sells)
    close_qty = max(0, total_sell_qty - assignment_qty_for_series)

    remaining_close = close_qty
    open_sells = []
    for s in all_sells:
        s_qty = abs(int(safe_float(s.get('quantity'))))
        if remaining_close >= s_qty:
            remaining_close -= s_qty
            continue  # Fully consumed by close (buyback or expiry)
        elif remaining_close > 0:
            open_qty = s_qty - remaining_close
            remaining_close = 0
            s_copy = dict(s)
            s_copy['_open_qty'] = open_qty
            open_sells.append(s_copy)
        else:
            s_copy = dict(s)
            s_copy['_open_qty'] = s_qty
            open_sells.append(s_copy)
    return open_sells


def _consume_open_sells_fifo(originals_state, a_qty, mult, base_currency='EUR', usd_to_eur_rates=None):
    """FIFO-Konsum aus open Sells fuer eine einzelne Andienung.

    originals_state: Liste von _get_open_option_sells()-Dicts (sortiert nach
    dateTime aufsteigend). Wird IN-PLACE mutiert: '_open_qty' wird pro Eintrag
    reduziert um die durch diese Andienung verbrauchte Menge.

    Wichtig: premium_eur wird per-Fill akkumuliert, nicht ueber einen kontrakt-
    gewichteten FX-Mittelwert. Sonst entstehen bei Fills mit unterschiedlichen
    Preisen und FX-Raten Konversionsfehler (Issue Codex P2).

    Returns: (premium_raw, commission_raw, fx_weighted, premium_eur, sells_consumed, consumed_qty)
    - premium_raw, commission_raw: Brutto-Werte in Trade-Waehrung
    - fx_weighted: kontrakt-gewichtete Summe der fxRateToBase (nur fuer Display
      des effektiven Mittelkurses; NICHT fuer EUR-Konversion verwenden)
    - premium_eur: NETTO-EUR (Praemie + Kommission), per-Fill exakt umgerechnet
    - sells_consumed: Liste von (orig_dict, consume_qty) fuer Detail-Tracking
    """
    remaining = a_qty
    premium_raw = 0.0
    commission_raw = 0.0
    fx_weighted = 0.0
    premium_eur = 0.0
    consumed = 0
    sells_consumed = []
    for orig in originals_state:
        if remaining <= 0:
            break
        q_avail = orig.get('_open_qty', 0)
        if q_avail <= 0:
            continue
        consume = min(remaining, q_avail)
        components = _premium_components_for_consumed_sell(
            orig, consume, mult, base_currency, usd_to_eur_rates
        )
        if components is None:
            orig['_open_qty'] = q_avail - consume
            remaining -= consume
            continue
        premium_raw += components['premium_raw']
        commission_raw += components['commission_raw']
        fx_weighted += components['fx_weighted']
        premium_eur += components['premium_eur']
        consumed += consume
        sells_consumed.append((orig, consume))
        orig['_open_qty'] = q_avail - consume
        remaining -= consume
    return premium_raw, commission_raw, fx_weighted, premium_eur, sells_consumed, consumed


def _premium_components_for_consumed_sell(orig, consume, mult, base_currency='EUR', usd_to_eur_rates=None):
    """Return premium components for a consumed SELL slice."""
    price = safe_float(orig.get('tradePrice')) or safe_float(orig.get('closePrice'))
    if price <= 0 or consume <= 0:
        return None
    orig_full_qty = abs(int(safe_float(orig.get('quantity'))))
    comm_full = safe_float(orig.get('ibCommission'), 0)
    comm_share = comm_full * consume / orig_full_qty if orig_full_qty else 0
    premium_raw = price * mult * consume
    net_raw = premium_raw + comm_share
    fx = safe_float(orig.get('fxRateToBase'), 1.0)
    if base_currency == 'EUR':
        premium_eur = net_raw * fx
    else:
        sd = parse_date(orig.get('dateTime') or orig.get('tradeDate'))
        r_eur = get_rate_for_date(sd, usd_to_eur_rates) if usd_to_eur_rates else 1.0
        premium_eur = net_raw * fx * r_eur
    return {
        'quantity': consume,
        'premium_raw': premium_raw,
        'commission_raw': comm_share,
        'net_premium_raw': net_raw,
        'fx_weighted': fx * consume,
        'premium_eur': premium_eur,
    }


def _build_stillhalter_details_for_assignment(a, strike, expiry, pc, a_qty, mult, tax_year,
                                              sells_consumed, premium_raw, commission_raw,
                                              premium_eur, base_currency='EUR',
                                              usd_to_eur_rates=None):
    """Build assignment details split by original SELL year."""
    assignment_date = parse_date(a.get('dateTime') or a.get('tradeDate'))
    detail_parts = {}
    for orig, consume_qty in sells_consumed:
        od = parse_date(orig.get('dateTime') or orig.get('tradeDate'))
        components = _premium_components_for_consumed_sell(
            orig, consume_qty, mult, base_currency, usd_to_eur_rates
        )
        if components is None:
            continue
        if od is None:
            od = assignment_date
        if od is None:
            continue
        yr = od.year
        if yr not in detail_parts:
            detail_parts[yr] = {
                'orig_sell_date': od,
                'quantity': 0,
                'premium_eur': 0.0,
                'premium_raw': 0.0,
                'commission_raw': 0.0,
            }
        part = detail_parts[yr]
        if od < part['orig_sell_date']:
            part['orig_sell_date'] = od
        part['quantity'] += components['quantity']
        part['premium_eur'] += components['premium_eur']
        part['premium_raw'] += components['net_premium_raw']
        part['commission_raw'] += components['commission_raw']

    if not detail_parts:
        detail_parts[tax_year] = {
            'orig_sell_date': assignment_date,
            'quantity': a_qty,
            'premium_eur': premium_eur,
            'premium_raw': premium_raw + commission_raw,
            'commission_raw': commission_raw,
        }

    def _detail_sort_key(item):
        d = item[1]['orig_sell_date'] or assignment_date
        return str(d) if d else ''

    details = []
    for yr, part in sorted(detail_parts.items(), key=_detail_sort_key):
        details.append({
            'symbol': a.get('symbol') or a.get('description') or f"{strike} {expiry} {pc}",
            'strike': strike,
            'expiry': expiry,
            'putCall': pc,
            'quantity': part['quantity'],
            'multiplier': mult,
            'premium_eur': part['premium_eur'],
            'premium_raw': part['premium_raw'],
            'commission_raw': part['commission_raw'],
            'assignment_date': str(assignment_date) if assignment_date else '',
            'assignment_trade_date': (a.get('tradeDate') or (a.get('dateTime') or '')[:10]),
            'orig_sell_date': str(part['orig_sell_date']) if part['orig_sell_date'] else '',
            'orig_sell_year': yr,
            'is_cross_year': yr < tax_year,
        })
    return details


def _put_assignment_relevant_dates(det):
    dates = {
        (det.get('assignment_date') or '')[:10],
        (det.get('assignment_trade_date') or '')[:10],
    }
    dates.discard('')
    return dates


def _symbol_root(value):
    """Erstes Token eines IBKR-Symbols ('BRK B' -> 'BRK'); leer-sicher."""
    parts = (value or '').split()
    return parts[0] if parts else ''


def _put_assignment_closed_lot_matches(closed_lots, det, underlying, shares, consumed=None):
    """Return STK closed-lot slices that originate from this put assignment.

    `consumed` (optional dict, key: id(lot)) trackt bereits geclaimte Lot-Shares
    über mehrere Details hinweg. Ohne geteilten State claimen zwei Same-Day-
    Andienungen desselben Underlyings denselben Lot-Slice doppelt, die zweite
    Korrektur verfällt und der spätere Verkauf behält die eingebettete Prämie
    (Audit-Finding F3 / Codex-Review P2).
    """
    if det.get('putCall') != 'P':
        return []

    remaining = abs(safe_float(shares, 0))
    if remaining <= 0:
        return []

    relevant_dates = _put_assignment_relevant_dates(det)
    matches = []
    for lot in sorted(closed_lots, key=lambda x: x.get('dateTime') or x.get('reportDate') or ''):
        if remaining <= 0:
            break
        if lot.get('assetCategory') != 'STK':
            continue
        sym = _symbol_root(lot.get('underlyingSymbol') or lot.get('symbol'))
        if sym != underlying:
            continue
        open_date = (lot.get('openDateTime') or '')[:10]
        if relevant_dates and open_date not in relevant_dates:
            continue
        qty = abs(safe_float(lot.get('quantity'), 0))
        if qty <= 0:
            continue
        avail = qty if consumed is None else qty - consumed.get(id(lot), 0.0)
        if avail <= 0:
            continue
        take = min(avail, remaining)
        close_date = (lot.get('reportDate') or lot.get('dateTime') or '')[:10]
        matches.append({
            'shares': take,
            'cost': abs(safe_float(lot.get('cost'), 0)) * take / qty,
            'open_date': open_date,
            'close_date': close_date,
        })
        if consumed is not None:
            consumed[id(lot)] = consumed.get(id(lot), 0.0) + take
        remaining -= take
    return matches


def _call_assignment_short_lot_matches(closed_lots, det, underlying, shares, consumed=None):
    """Return STK short closed-lot slices opened by this call assignment.

    Call-Andienung ohne Long-Bestand eröffnet einen Aktien-Short (SELL mit PnL=0,
    openClose=O); erst der spätere Rückkauf realisiert den PnL inkl. eingebetteter
    Prämie. Solche Lots haben openDateTime == Andienungstag und Short-Richtung
    (negative Quantity / schließender buySell=BUY). Die Korrektur muss dann auf
    die BUY-Row des Cover-Tags, nicht auf den Andienungstag (Audit-Finding F1,
    SPY/BITO/MPW-Fälle). `consumed` wie bei _put_assignment_closed_lot_matches:
    geteilter Lot-Konsum-State gegen Doppel-Claims durch Same-Day-Details
    (Codex-Review P2).
    """
    if det.get('putCall') != 'C':
        return []

    remaining = abs(safe_float(shares, 0))
    if remaining <= 0:
        return []

    relevant_dates = _put_assignment_relevant_dates(det)
    matches = []
    for lot in sorted(closed_lots, key=lambda x: x.get('dateTime') or x.get('reportDate') or ''):
        if remaining <= 0:
            break
        if lot.get('assetCategory') != 'STK':
            continue
        sym = _symbol_root(lot.get('underlyingSymbol') or lot.get('symbol'))
        if sym != underlying:
            continue
        is_short_lot = (safe_float(lot.get('quantity'), 0) < 0
                        or (lot.get('buySell') or '').upper() == 'BUY')
        if not is_short_lot:
            continue
        open_date = (lot.get('openDateTime') or '')[:10]
        if relevant_dates and open_date not in relevant_dates:
            continue
        qty = abs(safe_float(lot.get('quantity'), 0))
        if qty <= 0:
            continue
        avail = qty if consumed is None else qty - consumed.get(id(lot), 0.0)
        if avail <= 0:
            continue
        take = min(avail, remaining)
        close_date = (lot.get('reportDate') or lot.get('dateTime') or '')[:10]
        matches.append({
            'shares': take,
            'open_date': open_date,
            'close_date': close_date,
        })
        if consumed is not None:
            consumed[id(lot)] = consumed.get(id(lot), 0.0) + take
        remaining -= take
    return matches


def _call_short_cover_candidates_from_trades(trades, underlying, after_dates, shares, consumed=None):
    """Fallback ohne CLOSED_LOT-Daten: Short-Cover-Kandidaten direkt aus trades.

    Wenn closed_lots.csv fehlt oder das Short-Lot nicht enthält, ist der
    Lot-Match leer — die Cover-BUYs (PnL≠0) nach dem Andienungstag sind aber in
    trades.csv selbst sichtbar. Chronologisch früheste zuerst (trades.csv ist
    NICHT chronologisch sortiert). Ohne diesen Fallback bliebe die Prämie im
    Cover-PnL eingebettet und würde doppelt versteuert (Codex-Review Finding 2).
    `consumed` wie bei den Lot-Matchern (Doppel-Claim-Schutz, key id(trade)).
    """
    remaining = abs(safe_float(shares, 0))
    after = min([d for d in after_dates if d]) if after_dates else ''
    if remaining <= 0 or not after:
        return []

    candidates = []
    for t in trades:
        if t.get('assetCategory') != 'STK':
            continue
        sym = _symbol_root(t.get('symbol'))
        if sym != underlying:
            continue
        if (t.get('buySell') or '').upper() != 'BUY':
            continue
        if abs(safe_float(t.get('fifoPnlRealized'))) < 0.01:
            continue  # Opening-Kauf ohne realisierten PnL — kein Cover
        t_date = (t.get('reportDate') or t.get('dateTime') or '')[:10]
        if not t_date or t_date <= after:
            continue
        candidates.append((t_date, t))

    matches = []
    for t_date, t in sorted(candidates, key=lambda x: x[0]):
        if remaining <= 0:
            break
        qty = abs(safe_float(t.get('quantity'), 0))
        if qty <= 0:
            continue
        ckey = (id(t), 'C')
        avail = qty if consumed is None else qty - consumed.get(ckey, 0.0)
        if avail <= 0:
            continue
        take = min(avail, remaining)
        matches.append({'shares': take, 'close_date': t_date, 'oid': id(t)})
        if consumed is not None:
            consumed[ckey] = consumed.get(ckey, 0.0) + take
        remaining -= take
    return matches


def _consume_assignment_day_stock_sells(trades, underlying, day_dates, max_shares, consumed, realized):
    """Konsumiere STK-SELL-Rows am Andienungstag quantity-genau (FIFO).

    realized=True: Rows mit |fifoPnlRealized| ≥ 0.01 — der Long-Close-Anteil
    der Andienung, dessen PnL (inkl. eingebetteter Prämie) sofort realisiert ist.
    realized=False: Rows mit PnL≈0 — die Short-Eröffnung; Evidenz dafür, dass
    dieser Anteil der Andienung als offener Short weiterlebt.
    `consumed` (key id(trade)) verhindert Doppel-Zählung über mehrere Details.

    BookTrade-Rows zuerst: Der Aktien-Trade einer Andienung ist ein BookTrade
    (nicht-börslicher Buchungsvorgang) — unabhängige Verkäufe am selben Tag
    sind ExchTrades und dürfen die Korrektur nicht an sich ziehen.
    Returns (taken_shares, row_oids).
    """
    remaining = abs(safe_float(max_shares, 0))
    if remaining <= 0 or not day_dates:
        return 0.0, set()

    candidates = []
    for idx, t in enumerate(trades):
        if t.get('assetCategory') != 'STK':
            continue
        sym = _symbol_root(t.get('symbol'))
        if sym != underlying:
            continue
        if (t.get('buySell') or '').upper() != 'SELL':
            continue
        has_pnl = abs(safe_float(t.get('fifoPnlRealized'))) >= 0.01
        if realized != has_pnl:
            continue
        t_date = (t.get('reportDate') or '')[:10]
        t_date2 = (t.get('dateTime') or '')[:10]
        if t_date not in day_dates and t_date2 not in day_dates:
            continue
        is_booktrade = t.get('transactionType') == 'BookTrade'
        candidates.append((0 if is_booktrade else 1, t_date or t_date2, idx, t))

    taken = 0.0
    oids = set()
    for _bt, _d, _idx, t in sorted(candidates, key=lambda x: (x[0], x[1], x[2])):
        if remaining <= 0:
            break
        qty = abs(safe_float(t.get('quantity'), 0))
        if qty <= 0:
            continue
        ckey = (id(t), 'C')
        avail = qty - consumed.get(ckey, 0.0)
        if avail <= 0:
            continue
        take = min(avail, remaining)
        consumed[ckey] = consumed.get(ckey, 0.0) + take
        oids.add(id(t))
        taken += take
        remaining -= take
    return taken, oids


def _claim_stock_rows_for_date(trades, underlying, close_date, shares, consumed,
                               buysell, prefer_cost=None, claim_side='C'):
    """Beansprucht STK-Rows (PnL≠0) der Richtung `buysell` am close_date und
    liefert deren Row-Identitäten (id(trade)) zurück.

    Zwei Zwecke: (1) Lots und trades-Rows beschreiben dieselbe Realität — ohne
    Claiming würde der trades-Fallback eines anderen Details bereits per Lot
    zugeordnete Shares erneut vergeben. (2) Die zurückgegebenen OIDs erlauben
    dem Apply, exakt diese Rows zu treffen statt der ersten Same-Day-Row in
    Dateireihenfolge (fremde Trades am selben Tag dürfen die Korrektur nicht
    konsumieren — Codex-Review 4. Runde).

    `prefer_cost` (Put-Pfad): bevorzugt die Row, deren Kostenbasis der
    Lot-Basis entspricht — die Andienungs-Lots tragen die charakteristische
    (Strike − Prämie)-Basis; fremde Same-Day-Verkäufe nicht. Tie-Break: FIFO.
    """
    remaining = abs(safe_float(shares, 0))
    if remaining <= 0 or not close_date:
        return set()

    candidates = []
    for idx, t in enumerate(trades):
        if t.get('assetCategory') != 'STK':
            continue
        sym = _symbol_root(t.get('symbol'))
        if sym != underlying:
            continue
        if (t.get('buySell') or '').upper() != buysell:
            continue
        if abs(safe_float(t.get('fifoPnlRealized'))) < 0.01:
            continue
        t_date = (t.get('reportDate') or t.get('dateTime') or '')[:10]
        if t_date != close_date:
            continue
        if prefer_cost is not None:
            cost_mismatch = abs(abs(safe_float(t.get('cost'), 0)) - abs(prefer_cost))
        else:
            cost_mismatch = 0.0
        candidates.append((cost_mismatch, idx, t))

    claimed_oids = set()
    for _mismatch, _idx, t in sorted(candidates, key=lambda x: (x[0], x[1])):
        if remaining <= 0:
            break
        qty = abs(safe_float(t.get('quantity'), 0))
        # Seiten-getrennte Claims (analog remaining_by_side im Apply): dieselbe
        # Row darf Put- UND Call-Praemie tragen (Wheel: Put-Andienung kauft,
        # Call-Andienung verkauft dieselben Shares) — ein seitenloser Claim
        # liesse die Call-Stufe-2 auf fremde Covers ausweichen (IWM/BITO).
        ckey = (id(t), claim_side)
        avail = qty - consumed.get(ckey, 0.0)
        if avail <= 0:
            continue
        take = min(avail, remaining)
        consumed[ckey] = consumed.get(ckey, 0.0) + take
        claimed_oids.add(id(t))
        remaining -= take
    return claimed_oids


def _resolve_call_assignment_targets(trades, closed_lots, det, underlying, total_shares, consumed):
    """Löst eine Call-Andienung ANTEILIG in Korrektur-Ziele auf (kein binäres Gate).

    IBKR realisiert den Aktien-PnL einer Call-Andienung (inkl. eingebetteter
    Prämie) je nach Bestand an unterschiedlichen Stellen — auch gemischt
    innerhalb EINER Andienung. Quellen-Kaskade, jede Stufe konsumiert nur den
    noch offenen Rest (Audit F1 + Codex-Findings 1–3):

      1. Short-Cover-Lots (openDateTime == Andienungstag) → BUY @ lot.close_date.
         Beansprucht zusätzlich die korrespondierenden Cover-Rows in `consumed`.
      2. Long-Close-Anteil: SELL-Rows am Andienungstag mit PnL≠0 → SELL @ Tag.
      3. Restliche Cover-BUYs (PnL≠0, nach Andienungstag) direkt aus trades —
         deckt fehlende UND unvollständige closed_lots ab.
      4. Short-Open-Evidenz (SELL @ Tag, PnL≈0) ohne Cover im Steuerjahr →
         Position offen, PnL unrealisiert → bewusst KEINE Korrektur, kein Fehler.

    Returns (targets, open_short_shares, unresolved_shares):
      targets: [{'shares', 'close_dates', 'target_buysell'}]
      open_short_shares: Anteil mit offenem Short (→ stillhalter_open_short)
      unresolved_shares: Rest ohne jede Evidenz (→ Anomalie, dropped+Warnung)
    """
    call_dates = sorted({
        (det.get('assignment_date') or '')[:10],
        (det.get('assignment_trade_date') or '')[:10],
    } - {''})
    rest = abs(safe_float(total_shares, 0))
    targets = []
    if rest <= 0 or not call_dates:
        return targets, 0.0, rest

    # Stufe 1: Short-Cover-Lots (autoritativste Quelle). Die OIDs der
    # korrespondierenden Cover-Rows wandern mit ins Target, damit das Apply
    # exakt diese Rows trifft (und Stufe 3 anderer Details sie nicht doppelt
    # vergibt).
    for match in _call_assignment_short_lot_matches(closed_lots, det, underlying,
                                                    rest, consumed=consumed):
        row_oids = _claim_stock_rows_for_date(trades, underlying,
                                              match['close_date'],
                                              match['shares'], consumed,
                                              buysell='BUY')
        targets.append({'shares': match['shares'],
                        'close_dates': [match['close_date']],
                        'target_buysell': 'BUY',
                        'row_oids': row_oids or None})
        rest -= match['shares']

    # Stufe 2: Long-Close-Anteil am Andienungstag (BookTrade-Rows bevorzugt —
    # fremde ExchTrades am selben Tag dürfen die Korrektur nicht erhalten).
    if rest > 0:
        long_qty, long_oids = _consume_assignment_day_stock_sells(
            trades, underlying, call_dates, rest, consumed, realized=True)
        if long_qty > 0:
            targets.append({'shares': long_qty,
                            'close_dates': call_dates,
                            'target_buysell': 'SELL',
                            'row_oids': long_oids or None})
            rest -= long_qty

    # Stufe 3: restliche Covers aus trades (closed_lots fehlt oder ist lückenhaft).
    if rest > 0:
        for match in _call_short_cover_candidates_from_trades(
                trades, underlying, call_dates, rest, consumed=consumed):
            targets.append({'shares': match['shares'],
                            'close_dates': [match['close_date']],
                            'target_buysell': 'BUY',
                            'row_oids': {match['oid']}})
            rest -= match['shares']

    # Stufe 4: offener Short (Short-Open-Evidenz, kein Cover im Steuerjahr).
    open_short = 0.0
    if rest > 0:
        open_short, _open_oids = _consume_assignment_day_stock_sells(
            trades, underlying, call_dates, rest, consumed, realized=False)
        rest -= open_short

    return targets, open_short, rest


def _correction_matches_row(corr, row):
    """Gate: Darf diese pending-Korrektur auf diese debug_row angewendet werden?

    Drei Stufen, stärkste zuerst:
    1. `row_oids`: Der Resolver hat die Ziel-Row bereits identifiziert — nur
       exakt diese Row darf die Korrektur erhalten (fremde Same-Day-Trades
       bleiben unberührt).
    2. `close_dates` (+ optional `target_buysell`): Fallback ohne Row-Identität
       (z.B. unresolved-Anomalie) — Datum + Richtung.
    3. `close_date` (Put-Pfad ohne OIDs): einzelnes Datum gegen das erste
       nicht-leere Row-Datum. ACHTUNG: bewusst first-nonempty (reportDate vor
       dateTime), NICHT Set-Schnitt wie Stufe 2 — delayed bookings (1a13795)
       würden sonst zusätzlich über dateTime matchen.
    """
    corr_oids = corr.get('row_oids')
    if corr_oids is not None:
        return row.get('_trade_oid') in corr_oids
    if corr.get('close_dates') is not None:
        target_bs = corr.get('target_buysell', '')
        if target_bs and (row.get('buySell') or '').upper() != target_bs:
            return False
        row_dates = {(row.get('reportDate') or '')[:10],
                     (row.get('dateTime') or '')[:10]} - {''}
        return bool(row_dates & set(corr.get('close_dates')))
    corr_close_date = corr.get('close_date') or ''
    row_close_date = (row.get('reportDate') or row.get('dateTime') or '')[:10]
    return not corr_close_date or corr_close_date == row_close_date


def _put_assignment_lot_cost_correction_per_share(closed_lots, det, underlying, shares, default_per_share,
                                                  require_match=False, matches=None):
    """Use realized STK lot cost to decide whether IBKR embedded the put premium.

    `matches`: vorberechnete Lot-Matches durchreichen, wenn der Aufrufer bereits
    mit geteiltem Konsum-State gematcht hat — ein interner Neu-Scan würde sonst
    Lots doppelt konsumieren bzw. eine abweichende Zuordnung sehen.
    """
    if det.get('putCall') != 'P':
        return default_per_share

    shares = abs(safe_float(shares, 0))
    strike = safe_float(det.get('strike'), 0)
    if shares <= 0 or strike <= 0:
        return default_per_share

    if matches is None:
        matches = _put_assignment_closed_lot_matches(closed_lots, det, underlying, shares)
    lot_qty = sum(m['shares'] for m in matches)
    lot_cost = sum(m['cost'] for m in matches)

    if lot_qty <= 0 or lot_cost <= 0:
        if require_match:
            return None
        return default_per_share

    actual_cost_per_share = lot_cost / lot_qty
    reduction_per_share = strike - actual_cost_per_share
    tolerance_per_share = max(0.01, abs(default_per_share) * 0.05)
    if reduction_per_share <= tolerance_per_share:
        return 0.0
    return default_per_share


def calculate_tax(ib_tax_dir, tax_year=None, fx_csv_path=None, anlage_so_overrides=None,
                  fx_margin_correction_enabled=True,
                  dba_wht_beta_enabled=False):
    # 0. Detect base currency, tax year, and XML metadata from account_info.csv
    base_currency = 'EUR'  # default — most IBKR accounts for German tax filers are EUR-based
    xml_has_fx_data = False
    acct_path = os.path.join(ib_tax_dir, 'account_info.csv')
    if os.path.exists(acct_path):
        acct_rows = load_csv(acct_path)
        if acct_rows:
            base_currency = acct_rows[0].get('currency', 'EUR')
            fx_count = int(acct_rows[0].get('fx_transactions_count', '-1'))
            xml_has_fx_data = fx_count > 0
            if tax_year is None:
                detected = acct_rows[0].get('tax_year', '')
                if detected:
                    tax_year = int(detected)
    if tax_year is None:
        tax_year = 2025  # fallback
    print(f"Base currency: {base_currency}, Steuerjahr: {tax_year}")

    # 1. Load and Deduplicate Trades
    all_trades = load_csv(os.path.join(ib_tax_dir, 'trades.csv'))
    if not all_trades:
        if not os.path.exists(os.path.join(ib_tax_dir, 'trades.csv')):
            print("Hinweis: Keine trades.csv gefunden — die Flex Query XML enthält keine Trades im gewählten Zeitraum. "
                  "Es werden nur Dividenden, Zinsen und Quellensteuern ausgewertet.")
    
    unique_trades_set = set()
    trades = []
    duplicates_count = 0
    
    for t in all_trades:
        # Create a unique key based on relevant fields
        # Include tradeID when available (extended Flex Query) to avoid
        # falsely deduplicating partial fills with identical attributes
        trade_id = t.get('tradeID', '').strip()
        if trade_id:
            key = (trade_id,)
        else:
            key = (
                t.get('dateTime'),
                t.get('isin'),
                t.get('buySell'),
                t.get('quantity'),
                t.get('closePrice'),
                t.get('fifoPnlRealized')
            )
        if key in unique_trades_set:
            duplicates_count += 1
            continue
        unique_trades_set.add(key)
        trades.append(t)
        
    print(f"Loaded {len(all_trades)} trade rows. Removed {duplicates_count} duplicates. Unique trades: {len(trades)}")

    # Detect extended Flex Query (has tradePrice for accurate Stillhalter premium calc)
    has_trade_price = any(t.get('tradePrice', '') not in ('', '0', None) for t in trades)
    if has_trade_price:
        print("Erweiterte Flex Query erkannt (tradePrice verfügbar).")
    else:
        print("Basis-Flex-Query erkannt (kein tradePrice — Stillhalterprämien nutzen closePrice als Näherung).")

    all_funds = load_csv(os.path.join(ib_tax_dir, 'statement_of_funds.csv'))
    unique_funds_set = set()
    funds = []
    funds_duplicates = 0
    
    for f in all_funds:
        # Use (transactionID, activityDescription) — IBKR bundles multiple items
        # (e.g. Borrow Fees + SYEP Interest) under the same transactionID.
        # Using only transactionID would drop legitimate entries.
        tid = f.get('transactionID')
        if tid:
            key = (tid, f.get('activityDescription', ''))
        else:
            key = tuple(f.items())
            
        if key in unique_funds_set:
            funds_duplicates += 1
            continue
        unique_funds_set.add(key)
        funds.append(f)
        
    print(f"Loaded {len(all_funds)} fund rows. Removed {funds_duplicates} duplicates. Unique funds: {len(funds)}")
    
    # 2. Build Exchange Rates (USD -> EUR) — only needed for USD-based accounts
    usd_to_eur_rates = {}
    ecb_rates_used = False
    if base_currency == 'USD':
        usd_to_eur_rates = get_exchange_rates(trades, funds)
        ibkr_rate_count = len(usd_to_eur_rates)
        print(f"IBKR-Wechselkurse: {ibkr_rate_count} Tageskurse aus Transaktionsdaten.")

        # EZB-Referenzkurse als Ergänzung/Fallback laden (statisch eingebettet, kein Internet nötig)
        ecb_rates = fetch_ecb_rates(tax_year)
        if ecb_rates:
            # EZB-Kurse nur für Tage einfügen, an denen kein IBKR-Kurs vorliegt
            ecb_filled = 0
            for d, rate in ecb_rates.items():
                if d not in usd_to_eur_rates:
                    usd_to_eur_rates[d] = rate
                    ecb_filled += 1
            ecb_rates_used = ecb_filled > 0
            print(f"EZB-Referenzkurse:  {len(ecb_rates)} Tageskurse (statisch/offline), {ecb_filled} Lücken gefüllt.")
        else:
            print(f"EZB-Referenzkurse:  nicht verfügbar für Steuerjahr {tax_year} (nur 2024/2025 eingebettet).")

        print(f"Wechselkurse gesamt: {len(usd_to_eur_rates)} Tageskurse.")

        if not usd_to_eur_rates:
            raise RuntimeError(
                f"Keine USD/EUR-Wechselkurse verfügbar für Steuerjahr {tax_year}. "
                f"Weder IBKR-Trade-Daten noch EZB-Referenzkurse (ecb_rates.py) liefern Werte. "
                f"Bitte EZB-Kursdaten für {tax_year} in ecb_rates.py ergänzen oder Steuerjahr 2024/2025 verwenden."
            )
    else:
        print(f"Base currency is {base_currency} — no USD→EUR rate map needed.")

    # 2b. Build ETF lookup from financial_instruments.csv
    from etf_classification import (
        ETF_CLASSIFICATION,
        get_classification,
        get_etf_info,
        get_foreign_tax_treaty_rate,
        get_routing_classification,
        get_teilfreistellung,
        is_known_etf,
        requires_classification_review,
    )

    anlage_so_overrides_set = set(anlage_so_overrides or ())

    def _effective_classification(isin):
        # Respects Session-Overrides aus der GUI (Issue #51): Nutzer kann ETFs
        # manuell als Anlage SO markieren, auch wenn sie nicht im Lookup stehen.
        if isin and isin in anlage_so_overrides_set:
            return 'anlage_so'
        return get_routing_classification(isin)

    etf_isins = set()  # all ISINs that IBKR marks as ETF (subCategory)
    symbol_to_isin = {}  # for Stillhalter underlying lookup
    fi_path = os.path.join(ib_tax_dir, 'financial_instruments.csv')
    if os.path.exists(fi_path):
        for fi in load_csv(fi_path):
            sym = fi.get('symbol', '').strip()
            isin = fi.get('isin', '').strip()
            if sym and isin:
                symbol_to_isin[sym] = isin
            if fi.get('assetCategory') == 'STK' and isin and (fi.get('subCategory') == 'ETF' or is_known_etf(isin)):
                etf_isins.add(isin)
    # Also pick up ETFs from trades themselves
    for t in trades:
        if t.get('assetCategory') == 'STK':
            isin = t.get('isin', '').strip()
            if isin and (t.get('subCategory') == 'ETF' or is_known_etf(isin)):
                etf_isins.add(isin)
    if etf_isins:
        print(f"ETF-Erkennung: {len(etf_isins)} ETF-ISINs gefunden (subCategory=ETF).")

    # 3. Capital Gains (Stocks & Options)
    stocks_gain = 0.0
    stocks_loss = 0.0

    options_gain = 0.0
    options_loss = 0.0

    # Topf 2 breakdown by instrument category (for detailed reporting)
    TOPF2_CAT_LABELS = {
        'OPT': 'Optionen', 'FOP': 'Optionen', 'FSFOP': 'Optionen',
        'FUT': 'Futures', 'BILL': 'T-Bills', 'BOND': 'Anleihen',
    }
    topf2_by_category = {}  # {label: {'gain': float, 'loss': float}}

    def add_topf2_detail(cat_label, amount):
        if cat_label not in topf2_by_category:
            topf2_by_category[cat_label] = {'gain': 0.0, 'loss': 0.0}
        if amount > 0:
            topf2_by_category[cat_label]['gain'] += amount
        else:
            topf2_by_category[cat_label]['loss'] += amount

    # no_invstg ETP tracking (for plausibility check — IBKR counts these as STK/Aktien)
    no_invstg_gain = 0.0
    no_invstg_loss = 0.0
    no_invstg_income_by_isin = {}

    # Anlage SO tracking (§23 EStG — physische Gold-ETCs mit Lieferanspruch)
    # Trades are collected for holding period analysis; gains/losses excluded from KAP entirely
    anlage_so_trades = []  # list of dicts with trade details for holding period check

    # InvStG ETF tracking (KAP-INV)
    etf_invstg_gain = 0.0       # InvStG fund gains (before Teilfreistellung)
    etf_invstg_loss = 0.0       # InvStG fund losses (before Teilfreistellung)
    etf_dividends_eur = 0.0     # InvStG fund dividends
    etf_wht_eur = 0.0           # InvStG fund withholding tax (sum, negative)
    etf_by_isin = {}            # per-ISIN tracking for Teilfreistellung
    etf_wht_event_buckets = {}  # event-level DIV/PIL and WHT/FRTAX matching
    debug_rows = []             # per-trade debug export

    def ensure_etf_fund_entry(isin, classification=None):
        if isin not in etf_by_isin:
            info = get_etf_info(isin)
            etf_by_isin[isin] = {
                'ticker': info['ticker'] if info else isin[:12],
                'name': info['name'] if info else '',
                'classification': classification or 'sonstiger_fonds',
                'gain': 0.0,
                'loss': 0.0,
                'div': 0.0,
                'div_received': 0.0,
                'div_paid': 0.0,
                'wht': 0.0,
                'wht_events': [],
            }
            if info and info.get('review_required'):
                etf_by_isin[isin]['review_reason'] = info.get(
                    'review_reason', 'Steuerliche Klassifikation nicht belegt.'
                )
        return etf_by_isin[isin]

    def add_etf_distribution(entry, amount_eur):
        # Zugeflossene und gezahlte Betraege getrennt fuehren: gezahlte
        # Dividenden/Ersatzzahlungen auf Short-Positionen sind keine
        # (negativen) Ausschuettungen i.S.d. Anlage KAP-INV.
        entry['div'] += amount_eur
        if amount_eur >= 0:
            entry['div_received'] = entry.get('div_received', 0.0) + amount_eur
        else:
            entry['div_paid'] = entry.get('div_paid', 0.0) + amount_eur

    def get_etf_wht_event(isin, event_date, currency):
        date_key = event_date.isoformat() if event_date else ''
        currency_key = currency or base_currency
        key = (isin, date_key, currency_key)
        if key not in etf_wht_event_buckets:
            etf_wht_event_buckets[key] = {
                'event_key': '|'.join(key),
                'date': date_key,
                'currency': currency_key,
                'gross_distribution_eur': 0.0,
                'tax_withheld_eur': 0.0,
                'tax_refunded_eur': 0.0,
                'transaction_ids': [],
                'descriptions': [],
                'report_dates': [],
            }
        return etf_wht_event_buckets[key]

    def add_etf_wht_source(event, row, report_date):
        transaction_id = (row.get('transactionID') or '').strip()
        description = (row.get('activityDescription') or '').strip()
        report_date_value = report_date.isoformat() if report_date else ''
        if transaction_id and transaction_id not in event['transaction_ids']:
            event['transaction_ids'].append(transaction_id)
        if description and description not in event['descriptions']:
            event['descriptions'].append(description)
        if report_date_value and report_date_value not in event['report_dates']:
            event['report_dates'].append(report_date_value)

    for t in trades:
        # Use reportDate for tax year assignment (Settlement/Buchungsdatum)
        # Trades at year boundary (e.g., dateTime=2023-12-29, settlement=2024-01-02)
        # belong to the tax year of settlement
        report_date = parse_date(t.get('reportDate') or t.get('dateTime') or t.get('tradeDate'))
        date = parse_date(t.get('dateTime') or t.get('tradeDate'))
        if not report_date or report_date.year != tax_year:
            continue

        # Check if Realized PnL event
        pnl_str = t.get('fifoPnlRealized')
        if not pnl_str or float(pnl_str) == 0:
            continue

        pnl_raw = float(pnl_str)
        fx_to_base = safe_float(t.get('fxRateToBase'), 1.0)

        if base_currency == 'EUR':
            # EUR base: pnl_raw × fxRateToBase already gives EUR
            pnl_eur = pnl_raw * fx_to_base
        else:
            # USD base: two-step conversion (trade currency → USD → EUR)
            pnl_usd = pnl_raw * fx_to_base
            rate_eur = get_rate_for_date(date, usd_to_eur_rates)
            pnl_eur = pnl_usd * rate_eur

        category = t.get('assetCategory')

        if category == 'STK':
            isin = t.get('isin', '').strip()
            sub = t.get('subCategory', '')
            # Treat as ETF/ETP also when our classification table knows the ISIN even if
            # IBKR does not flag subCategory="ETF" (Spot-Krypto-Trusts wie BSOL etc.).
            if isin and (sub == 'ETF' or is_known_etf(isin)):
                cls = _effective_classification(isin)
                if cls == 'anlage_so':
                    # Physical Gold-ETC with delivery claim → §23 EStG (not §20)
                    # Excluded from KAP entirely; holding period determines taxability
                    info = get_etf_info(isin)
                    anlage_so_trades.append({
                        'isin': isin,
                        'ticker': info['ticker'] if info else isin[:12],
                        'name': info['name'] if info else '',
                        'pnl_eur': pnl_eur,
                        'quantity': safe_float(t.get('quantity'), 0),
                        'dateTime': t.get('dateTime', ''),
                        'reportDate': t.get('reportDate', ''),
                        'buySell': t.get('buySell', ''),
                    })
                elif cls == 'no_invstg':
                    # Crypto/Commodity ETPs: NOT a stock → Topf 2 (§20 Abs. 2 S. 1 Nr. 7 EStG)
                    if pnl_eur > 0:
                        options_gain += pnl_eur
                        no_invstg_gain += pnl_eur
                    else:
                        options_loss += pnl_eur
                        no_invstg_loss += pnl_eur
                    add_topf2_detail('Crypto/Commodity ETPs', pnl_eur)
                else:
                    # InvStG fund → KAP-INV (not Topf 1)
                    if pnl_eur > 0:
                        etf_invstg_gain += pnl_eur
                    else:
                        etf_invstg_loss += pnl_eur
                    # Per-ISIN tracking
                    ensure_etf_fund_entry(isin, cls)
                    if pnl_eur > 0:
                        etf_by_isin[isin]['gain'] += pnl_eur
                    else:
                        etf_by_isin[isin]['loss'] += pnl_eur
            else:
                # Regular stock
                if pnl_eur > 0:
                    stocks_gain += pnl_eur
                else:
                    stocks_loss += pnl_eur
        elif category in ['OPT', 'FUT', 'FOP', 'FSFOP', 'BILL', 'BOND']:
            # FSFOP = Flex Single-Stock Futures Options, BILL = Treasury Bills, BOND = Bonds
            if pnl_eur > 0:
                options_gain += pnl_eur
            else:
                options_loss += pnl_eur
            add_topf2_detail(TOPF2_CAT_LABELS.get(category, category), pnl_eur)

        # Collect debug row
        sub = t.get('subCategory', '')
        isin = t.get('isin', '').strip()
        if category == 'STK' and isin and (sub == 'ETF' or is_known_etf(isin)):
            cls = _effective_classification(isin)
            if cls == 'anlage_so':
                topf = 'Anlage SO'
            elif cls == 'no_invstg':
                topf = 'Topf2'
            else:
                topf = 'KAP-INV'
        elif category == 'STK':
            topf = 'Topf1'
        else:
            topf = 'Topf2'
        debug_rows.append({
            'dateTime': t.get('dateTime', ''),
            'reportDate': t.get('reportDate', ''),
            'symbol': t.get('symbol', ''),
            'description': t.get('description', ''),
            'isin': isin,
            'assetCategory': category,
            'subCategory': sub,
            'buySell': t.get('buySell', ''),
            'openClose': t.get('openCloseIndicator', ''),
            'quantity': t.get('quantity', ''),
            'transactionType': t.get('transactionType', ''),
            'currency': t.get('currency', ''),
            'tradePrice': safe_float(t.get('tradePrice'), 0),
            'cost': safe_float(t.get('cost'), 0),
            'proceeds': safe_float(t.get('proceeds'), 0),
            'fifoPnlRealized': pnl_raw,
            'fxRateToBase': fx_to_base,
            'ibCommission': safe_float(t.get('ibCommission'), 0),
            'pnl_eur': round(pnl_eur, 5),
            'topf': topf,
            'strike': t.get('strike', ''),
            'expiry': t.get('expiry', ''),
            'putCall': t.get('putCall', ''),
            'multiplier': t.get('multiplier', ''),
            'underlyingSymbol': t.get('underlyingSymbol', ''),
            'source': 'trades',
            # Interne Row-Identität (id des Quell-Trades): erlaubt dem
            # Stillhalter-Apply, exakt die vom Resolver konsumierte Row zu
            # treffen statt der ersten Same-Day-Row in Dateireihenfolge.
            # Unterstrich-Felder werden nicht exportiert und nach dem Apply entfernt.
            '_trade_oid': id(t),
        })

    # Write debug CSV
    if debug_rows:
        import csv as csv_mod
        debug_path = os.path.join(ib_tax_dir, 'trades_debug_eur.csv')
        with open(debug_path, 'w', newline='', encoding='utf-8') as f:
            export_fields = [k for k in debug_rows[0].keys() if not k.startswith('_')]
            w = csv_mod.DictWriter(f, fieldnames=export_fields, extrasaction='ignore')
            w.writeheader()
            w.writerows(debug_rows)
        print(f"Debug: {len(debug_rows)} Trades mit EUR-Umrechnung → {debug_path}")

    # --- Stillhalterprämien: separate assigned option premiums from stock PnL ---
    # When a short option is assigned, IBKR bundles the premium into the stock's
    # fifoPnlRealized and shows pnl=0 on the option BookTrade. Per BMF Rn. 26 (Call)
    # and Rn. 33 (Put), the premium is §20 Abs. 1 Nr. 11 income (Topf 2), and is
    # NOT to be considered in the stock gain/loss calculation (Topf 1).
    #
    # Detection: OPT BookTrade BUY with fifoPnlRealized≈0 → assignment
    # Both CALL and PUT assignments need fixing:
    #   - Short call assigned (Rn. 26): premium bundled into stock SALE PnL
    #   - Short put assigned (Rn. 33): premium reduces stock acquisition cost
    #   - Long option exercised: premium is acquisition cost — correct as-is

    stillhalter_premium_eur = 0.0
    stillhalter_count = 0
    stillhalter_unmatched = []
    stillhalter_corrections_dropped = []
    stillhalter_open_short = []
    stillhalter_details = []

    opt_assignments = [t for t in trades
                       if t.get('assetCategory') in ('OPT', 'FOP', 'FSFOP')
                       and t.get('transactionType') == 'BookTrade'
                       and t.get('buySell') == 'BUY'      # closing a short position
                       and t.get('putCall') in ('C', 'P')  # both call and put assignments
                       and abs(safe_float(t.get('fifoPnlRealized'))) < 0.01
                       and (d := parse_date(t.get('reportDate') or t.get('dateTime') or t.get('tradeDate'))) is not None
                       and d.year == tax_year]             # only assignments in tax year

    # Issue #53: Bei mehreren Andienungen derselben Series werden die Original-Sells
    # FIFO konsumiert (aelteste zuerst), nicht als Durchschnitt verteilt. State pro
    # Series wird zwischen den Iterationen weitergetragen — analog zum Cross-Year-
    # Block (Issue #54). Andienungen werden zeitlich sortiert, damit (a) der Series-
    # State chronologisch konsumiert wird und (b) pending_stk_corrections[underlying]
    # in chronologischer Reihenfolge entsteht — Voraussetzung fuer FIFO-konforme
    # Praemien-Korrektur ueber Stock-Verkaeufe desselben Underlyings (z.B. mehrere
    # SVOL-Series mit unterschiedlichen Strikes). trades.csv ist in IBKR-Flex-Queries
    # NICHT garantiert chronologisch, daher muss explizit sortiert werden.
    opt_assignments_sorted = sorted(
        opt_assignments,
        key=lambda t: (t.get('dateTime', '') or t.get('tradeDate', '') or t.get('reportDate', '') or '')
    )
    _current_year_series_state = {}  # {(a_cat, underlying, strike, expiry, putCall): originals_state}

    for a in opt_assignments_sorted:
        strike = a.get('strike')
        expiry = a.get('expiry')
        pc = a.get('putCall')
        a_cat = a.get('assetCategory')
        a_qty = abs(int(safe_float(a.get('quantity'))))
        if not strike or not expiry or not pc or a_qty == 0:
            continue

        # Total assignment qty for this series (all years) to determine open sells.
        # underlyingSymbol einbeziehen — verschiedene Aktien koennen dieselbe
        # strike/expiry-Kombination haben (z.B. KWEB P 30 vs FXI P 30).
        a_underlying = a.get('underlyingSymbol', '')
        series_key = (a_cat, a_underlying, strike, expiry, pc)

        if series_key not in _current_year_series_state:
            assign_qty_series = sum(
                abs(int(safe_float(t.get('quantity'))))
                for t in trades
                if t.get('assetCategory') == a_cat
                and t.get('transactionType') == 'BookTrade'
                and t.get('buySell') == 'BUY'
                and t.get('strike') == strike
                and t.get('expiry') == expiry
                and t.get('putCall') == pc
                and t.get('underlyingSymbol', '') == a_underlying
                and abs(safe_float(t.get('fifoPnlRealized'))) < 0.01
            )
            state = _get_open_option_sells(
                trades, a_cat, strike, expiry, pc, assign_qty_series, underlying=a_underlying
            )

            # Issue #61: Pre-consume Vorjahres-Andienungen derselben Series, damit
            # der Same-Year-Block FIFO bei den juengeren OPEN Sells startet. Ohne
            # diesen Schritt konsumiert der Same-Year-Block die aeltesten Sells,
            # die konzeptionell zur Vorjahres-Andienung gehoeren (im Vorjahres-Lauf
            # bereits versteuert). Gilt fuer Calls UND Puts (series_key enthaelt pc).
            prior_assigns = sorted(
                [t for t in trades
                 if t.get('assetCategory') == a_cat
                 and t.get('transactionType') == 'BookTrade'
                 and t.get('buySell') == 'BUY'
                 and t.get('strike') == strike
                 and t.get('expiry') == expiry
                 and t.get('putCall') == pc
                 and t.get('underlyingSymbol', '') == a_underlying
                 and abs(safe_float(t.get('fifoPnlRealized'))) < 0.01
                 and (pd_ := parse_date(t.get('reportDate') or t.get('dateTime') or t.get('tradeDate'))) is not None
                 and pd_.year < tax_year],
                key=lambda t: (t.get('dateTime', '') or t.get('tradeDate', '') or t.get('reportDate', '') or '')
            )
            if prior_assigns and state:
                first_open_pre = next((o for o in state if o.get('_open_qty', 0) > 0), None)
                if first_open_pre and safe_float(first_open_pre.get('multiplier')) > 0:
                    mult_pre = int(safe_float(first_open_pre.get('multiplier'), 100))
                else:
                    mult_pre = int(safe_float(prior_assigns[0].get('multiplier'), 100))
                for pa in prior_assigns:
                    pa_qty = abs(int(safe_float(pa.get('quantity'))))
                    if pa_qty <= 0:
                        continue
                    _consume_open_sells_fifo(state, pa_qty, mult_pre, base_currency, usd_to_eur_rates)

            _current_year_series_state[series_key] = state

        originals_state = _current_year_series_state[series_key]

        if not originals_state:
            symbol = a.get('symbol', f"{strike} {expiry} {pc}")
            print(f"  Stillhalter: Kein Original-SELL gefunden für {symbol} {expiry} {pc}")
            stillhalter_unmatched.append({
                'symbol': symbol,
                'strike': strike,
                'expiry': expiry,
                'putCall': pc,
                'quantity': a_qty,
                'dateTime': a.get('dateTime', a.get('tradeDate', ''))
            })
            continue

        # Multiplier aus dem ersten offenen Original-SELL bevorzugen (entspricht dem
        # Kontrakt, dessen Praemie konsumiert wird). Fallback auf BookTrade-Andienung,
        # wenn der Original-SELL keinen mult-Wert hat (FOP/FSFOP koennten abweichen).
        first_open = next((o for o in originals_state if o.get('_open_qty', 0) > 0), None)
        if first_open and safe_float(first_open.get('multiplier')) > 0:
            mult = int(safe_float(first_open.get('multiplier'), 100))
        else:
            mult = int(safe_float(a.get('multiplier'), 100))

        premium_raw, commission_raw, fx_weighted, premium_eur, sells_consumed, consumed_qty = \
            _consume_open_sells_fifo(originals_state, a_qty, mult, base_currency, usd_to_eur_rates)

        if consumed_qty == 0 or premium_raw == 0:
            continue

        stillhalter_premium_eur += premium_eur
        stillhalter_count += 1

        stillhalter_details.extend(_build_stillhalter_details_for_assignment(
            a, strike, expiry, pc, a_qty, mult, tax_year, sells_consumed,
            premium_raw, commission_raw, premium_eur, base_currency, usd_to_eur_rates
        ))

    # Move premiums from Topf 1 (stocks) / KAP-INV to Topf 2 (sonstiges)
    # For CALL assignments: IBKR embeds premium in stock SELL PnL → subtract from stocks_gain
    # For PUT assignments: premium is in stock cost basis → only subtract if stock was sold
    #   in the same tax year (otherwise premium is NOT in stocks_gain yet)
    etf_stillhalter_premium_eur = 0.0
    put_nosell_premium_eur = 0.0
    stk_gain_corr_cy = 0.0
    stk_loss_corr_cy = 0.0
    etf_gain_corr_cy = 0.0
    etf_loss_corr_cy = 0.0
    # Anlage-SO-Overrides (Issue #51): Prämien-Lookup für Lot-Level-Matching im
    # Anlage-SO-Build. Key: (underlying_symbol, assignment_date_YYYY-MM-DD).
    # Wird aus Stillhalter-current-year und prior-put-assignments befüllt — getrennt
    # pro Assignment, damit Mixed-Holding-Period-Fälle pro Lot korrekt zugeordnet werden.
    _so_premium_lookup = {}  # {(symbol, 'YYYY-MM-DD'): {'shares': int, 'premium_eur': float}}
    # Populate aus current-year Puts, wenn Underlying anlage_so ist
    for det in stillhalter_details:
        if det.get('putCall') != 'P':
            continue
        u_sym = det['symbol'].split()[0] if det.get('symbol') else ''
        u_isin = symbol_to_isin.get(u_sym, '')
        if not u_isin or _effective_classification(u_isin) != 'anlage_so':
            continue
        a_date_str = (det.get('assignment_date') or '')[:10]
        if not a_date_str:
            continue
        mult = det.get('multiplier', 100)
        shares = det['quantity'] * mult
        if shares <= 0:
            continue
        key = (u_sym, a_date_str)
        _so_premium_lookup.setdefault(key, {'shares': 0, 'premium_eur': 0.0})
        _so_premium_lookup[key]['shares'] += shares
        _so_premium_lookup[key]['premium_eur'] += det.get('premium_eur', 0)

    if stillhalter_premium_eur > 0:
        closed_lots_for_put_basis = []
        _cl_basis_path = os.path.join(ib_tax_dir, 'closed_lots.csv')
        if os.path.exists(_cl_basis_path):
            closed_lots_for_put_basis = load_csv(_cl_basis_path)

        # Split: check if underlying is an InvStG ETF
        stk_premium = 0.0
        etf_premium = 0.0
        put_nosell_premium = 0.0  # put assignment premiums where stock was NOT sold
        # Geteilter Lot-Konsum-State pro Schleife: ohne ihn claimen zwei Same-Day-
        # Andienungen desselben Underlyings denselben Lot-Slice (F3 / Codex P2).
        # Key-Invariante (gilt auch fuer _correction_lot_claims): closed_lots
        # claimen seitenlos per id(lot) — Put- und Call-Andienungen treffen
        # richtungsdisjunkte Lots; trades-Rows claimen seitengetrennt per
        # (id(trade), 'P'/'C') — dieselbe Row traegt im Wheel-Fall beide Praemien.
        _routing_lot_claims = {}
        for det in stillhalter_details:
            underlying = det['symbol'].split()[0] if det['symbol'] else ''
            underlying_isin = symbol_to_isin.get(underlying, '')
            source_premium_eur = det['premium_eur']

            # Put assignment: only subtract from stocks/ETF if stock was sold in tax_year
            # (if not sold, premium is in cost basis only — not yet in stocks_gain)
            if det['putCall'] == 'P':
                total_shares = det['quantity'] * det.get('multiplier', 100)
                matched_shares = sum(
                    m['shares'] for m in _put_assignment_closed_lot_matches(
                        closed_lots_for_put_basis, det, underlying, total_shares,
                        consumed=_routing_lot_claims
                    )
                )
                if matched_shares <= 0:
                    put_nosell_premium += det['premium_eur']
                    continue
                matched_ratio = min(1.0, matched_shares / total_shares) if total_shares else 0.0
                source_premium_eur = det['premium_eur'] * matched_ratio
                put_nosell_premium += det['premium_eur'] - source_premium_eur

            if underlying_isin and underlying_isin in etf_isins:
                cls = _effective_classification(underlying_isin)
                # anlage_so-Underlyings nicht als KAP-INV-Prämie zählen (Issue #51):
                # Optionsprämie bleibt §20 Abs. 1 Nr. 11 EStG (Topf 2), aber nicht KAP-INV.
                if cls not in ('no_invstg', 'anlage_so'):
                    etf_premium += source_premium_eur
                    continue
            stk_premium += source_premium_eur

        # NOTE: stocks_gain/etf_invstg_gain are NOT subtracted here.
        # The per-trade gain/loss split happens below in pending_stk_corrections,
        # same pattern as cross-year (Issue #23).
        etf_stillhalter_premium_eur = etf_premium
        put_nosell_premium_eur = put_nosell_premium
        options_gain += stillhalter_premium_eur  # total premium always to Topf 2
        add_topf2_detail('Stillhalterprämien', stillhalter_premium_eur)

        # Stillhalter: add premium rows to Topf 2 and correct stock trade debug_rows
        # Instead of separate Korrektur rows, we directly fix the stock trade's
        # cost/fifoPnlRealized/pnl_eur so the Excel shows the correct per-trade values.
        pending_stk_corrections = {}  # underlying_symbol → list of corrections
        # Eigener Konsum-State für diese Schleife (gleiche det-Reihenfolge und
        # Lot-Sortierung wie oben → identische Zuordnung wie das Routing).
        _correction_lot_claims = {}
        for det in stillhalter_details:
            underlying = det['symbol'].split()[0] if det['symbol'] else ''
            u_isin = symbol_to_isin.get(underlying, '')
            pc_label = 'Call' if det['putCall'] == 'C' else 'Put'
            # Determine source topf
            total_shares_for_put = det['quantity'] * det.get('multiplier', 100)
            put_lot_matches = []
            if det['putCall'] == 'P':
                put_lot_matches = _put_assignment_closed_lot_matches(
                    closed_lots_for_put_basis, det, underlying, total_shares_for_put,
                    consumed=_correction_lot_claims
                )
            if det['putCall'] == 'P' and not put_lot_matches:
                source_topf = 'Topf2'  # put_nosell: premium only in Topf 2, no subtraction
            elif u_isin and u_isin in etf_isins and _effective_classification(u_isin) == 'anlage_so':
                source_topf = 'Anlage SO'
            elif u_isin and u_isin in etf_isins and _effective_classification(u_isin) not in ('no_invstg', 'anlage_so'):
                source_topf = 'KAP-INV'
            else:
                source_topf = 'Topf1'
            # Stillhalterprämie row → always Topf 2
            debug_rows.append({
                'dateTime': det['assignment_date'], 'reportDate': det['assignment_date'],
                'symbol': det['symbol'], 'description': f'Stillhalterprämie ({pc_label}, BMF Rn. {"26" if det["putCall"] == "C" else "33"})',
                'isin': u_isin, 'assetCategory': 'OPT', 'subCategory': '',
                'buySell': '', 'quantity': str(det['quantity']),
                'transactionType': 'Stillhalter', 'currency': '',
                'tradePrice': 0, 'cost': 0, 'proceeds': 0,
                'fifoPnlRealized': 0, 'fxRateToBase': 0,
                'pnl_eur': round(det['premium_eur'], 5),
                'topf': 'Topf2',
                'strike': det['strike'], 'expiry': det['expiry'],
                'putCall': det['putCall'], 'multiplier': '',
                'underlyingSymbol': underlying,
                'source': 'stillhalter_korrektur',
            })
            # Queue stock trade correction (skip put_nosell — no stock trade to fix)
            if source_topf == 'Topf2':
                continue
            mult = det.get('multiplier', 100)
            total_shares = det['quantity'] * mult
            if total_shares > 0:
                premium_per_share_raw = det['premium_raw'] / total_shares
                if det['putCall'] == 'P':
                    premium_per_share_raw = _put_assignment_lot_cost_correction_per_share(
                        closed_lots_for_put_basis, det, underlying, total_shares,
                        premium_per_share_raw, require_match=True,
                        matches=put_lot_matches
                    )
                    if premium_per_share_raw is None:
                        continue
                    for match in put_lot_matches:
                        # Row-Identität der Ziel-Verkaufszeile: cost-präferiert
                        # (die Andienungs-Lots tragen die charakteristische
                        # Strike−Prämie-Basis), damit fremde Same-Day-Verkäufe
                        # die Korrektur nicht konsumieren.
                        put_row_oids = _claim_stock_rows_for_date(
                            trades, underlying, match['close_date'],
                            match['shares'], _correction_lot_claims,
                            buysell='SELL', prefer_cost=match.get('cost'),
                            claim_side='P'
                        )
                        pending_stk_corrections.setdefault(underlying, []).append({
                            'premium_per_share_raw': premium_per_share_raw,
                            'remaining_shares': match['shares'],
                            'close_date': match['close_date'],
                            'side': 'P',
                            'row_oids': put_row_oids or None,
                        })
                else:
                    # Anteilige Quellen-Kaskade statt binärer Gates (Audit F1 +
                    # Codex-Findings 1–3): Jede Call-Andienung wird quantity-genau
                    # in Ziele aufgelöst; offene Shorts sind kein Fehler.
                    call_targets, open_short_shares, unresolved_shares = \
                        _resolve_call_assignment_targets(
                            trades, closed_lots_for_put_basis, det, underlying,
                            total_shares, consumed=_correction_lot_claims
                        )
                    for tgt in call_targets:
                        pending_stk_corrections.setdefault(underlying, []).append({
                            'premium_per_share_raw': premium_per_share_raw,
                            'remaining_shares': tgt['shares'],
                            'close_dates': tgt['close_dates'],
                            'side': 'C',
                            'target_buysell': tgt['target_buysell'],
                            'row_oids': tgt.get('row_oids'),
                        })
                    if open_short_shares > 0:
                        stillhalter_open_short.append({
                            'underlying': underlying,
                            'shares': open_short_shares,
                            'premium_raw': premium_per_share_raw * open_short_shares,
                            'assignment_date': (det.get('assignment_date') or '')[:10],
                        })
                        print(f"  (i) Stillhalter {underlying}: Short aus Call-Andienung "
                              f"({open_short_shares:g} Stück) am Jahresende noch offen — "
                              f"Aktien-PnL unrealisiert, keine Korrektur nötig. Beim "
                              f"Folgejahr-Lauf dieses XML per --history laden, damit die "
                              f"Prämie dort nicht doppelt versteuert wird.")
                    if unresolved_shares > 0:
                        # Keine Evidenz in Lots ODER trades (Anomalie) → bisheriger
                        # SELL-Fallback; läuft kontrolliert in dropped + Warnung.
                        call_dates = sorted({
                            (det.get('assignment_date') or '')[:10],
                            (det.get('assignment_trade_date') or '')[:10],
                        } - {''})
                        pending_stk_corrections.setdefault(underlying, []).append({
                            'premium_per_share_raw': premium_per_share_raw,
                            'remaining_shares': unresolved_shares,
                            'close_dates': call_dates,
                            'side': 'C',
                            'target_buysell': 'SELL',
                        })

        # Apply pending corrections to stock trade debug_rows
        # IBKR embeds the premium in the stock's cost basis → cost too low, G/V too high.
        # We add the premium back to cost and subtract from fifoPnlRealized.
        # Also track gain/loss split per trade (same pattern as cross-year, Issue #23).
        stk_gain_corr_cy = 0.0
        stk_loss_corr_cy = 0.0
        etf_gain_corr_cy = 0.0
        etf_loss_corr_cy = 0.0
        nv_gain_corr_cy = 0.0  # no_invstg ETP correction → Topf 2
        nv_loss_corr_cy = 0.0
        _etf_by_isin_corr_cy = {}

        for row in debug_rows:
            if row.get('source') != 'trades' or row.get('assetCategory') != 'STK':
                continue
            sym_parts = (row.get('symbol', '') or '').split()
            row_symbol = sym_parts[0] if sym_parts else ''
            if not row_symbol or row_symbol not in pending_stk_corrections:
                continue
            qty = abs(safe_float(row.get('quantity', '0'), 0))
            if qty <= 0:
                continue
            original_pnl_eur = row['pnl_eur']
            total_correction_raw = 0.0
            # Put- (Kostenbasis-) und Call-Korrekturen (Erlösseite) zählen getrennte
            # Quantity-Budgets: dieselben Shares tragen legitim BEIDE Prämien, wenn
            # die Aktie per Put-Andienung gekauft und per Call-Andienung verkauft
            # wurde (Audit-Finding F1b, IWM-Fall).
            remaining_by_side = {'P': qty, 'C': qty}
            for corr in pending_stk_corrections[row_symbol]:
                side = corr.get('side', 'P')
                if corr['remaining_shares'] <= 0 or remaining_by_side[side] <= 0:
                    continue
                if not _correction_matches_row(corr, row):
                    continue
                shares = min(remaining_by_side[side], corr['remaining_shares'])
                total_correction_raw += corr['premium_per_share_raw'] * shares
                corr['remaining_shares'] -= shares
                remaining_by_side[side] -= shares
            if total_correction_raw > 0:
                # IBKR reduced absolute cost by premium → restore it
                if row['cost'] >= 0:
                    row['cost'] += total_correction_raw
                else:
                    row['cost'] -= total_correction_raw
                row['fifoPnlRealized'] -= total_correction_raw
                fx = row.get('fxRateToBase', 1.0)
                if base_currency == 'EUR':
                    row['pnl_eur'] = round(row['fifoPnlRealized'] * fx, 5)
                else:
                    d = parse_date(row.get('dateTime', ''))
                    r_eur = get_rate_for_date(d, usd_to_eur_rates)
                    row['pnl_eur'] = round(row['fifoPnlRealized'] * fx * r_eur, 5)
                row['stillhalter_adjusted'] = True

                # Per-trade gain/loss split (Issue #23 pattern)
                correction_eur = original_pnl_eur - row['pnl_eur']
                row_isin = row.get('isin', '')
                _row_cls = _effective_classification(row_isin) if row_isin else None
                is_so = bool(row_isin and row_isin in etf_isins and _row_cls == 'anlage_so')
                is_etf = bool(row_isin and row_isin in etf_isins
                              and _row_cls not in ('no_invstg', 'anlage_so'))
                if original_pnl_eur > 0:
                    from_gain = min(correction_eur, original_pnl_eur)
                    from_loss = correction_eur - from_gain
                else:
                    from_gain = 0.0
                    from_loss = correction_eur
                if is_so:
                    # Anlage-SO-Override (Issue #51): Keine Aggregation auf
                    # stocks/ETF-Pools. Die debug_row ist bereits korrigiert
                    # (Zeilen oben); Anlage-SO-PnL-Korrektur läuft per Lot im
                    # Anlage-SO-Build via _so_premium_lookup.
                    pass
                elif is_etf:
                    etf_gain_corr_cy += from_gain
                    etf_loss_corr_cy += from_loss
                    if row_isin not in _etf_by_isin_corr_cy:
                        _etf_by_isin_corr_cy[row_isin] = {'gain': 0.0, 'loss': 0.0}
                    _etf_by_isin_corr_cy[row_isin]['gain'] += from_gain
                    _etf_by_isin_corr_cy[row_isin]['loss'] += from_loss
                elif _row_cls == 'no_invstg':
                    # no_invstg-ETPs (GLD, IBIT, BSOL, …) wurden im Trade-Loop in
                    # options_gain/loss gebucht (Topf 2). Der Prämie-Zusatz oben
                    # addiert die Prämie erneut zu options_gain → hier raus-
                    # korrigieren, sonst wäre Topf 2 doppelt erfasst und ohne
                    # Zweig würde fälschlich Topf 1 (stocks) belastet.
                    nv_gain_corr_cy += from_gain
                    nv_loss_corr_cy += from_loss
                else:
                    stk_gain_corr_cy += from_gain
                    stk_loss_corr_cy += from_loss

        # Nicht zugeordnete Korrekturen sichtbar machen (Audit-Finding F1c): die
        # Prämie ist bereits in Topf 2 gebucht, aber im Aktien-/ETF-PnL noch
        # eingebettet → ohne Gegen-Korrektur Doppelversteuerung. Niemals still
        # verwerfen.
        for _underlying, _corrs in pending_stk_corrections.items():
            _leftover_raw = sum(c['premium_per_share_raw'] * c['remaining_shares']
                                for c in _corrs if c['remaining_shares'] > 0)
            if _leftover_raw > 0.01:
                _leftover_shares = sum(c['remaining_shares'] for c in _corrs
                                       if c['remaining_shares'] > 0)
                stillhalter_corrections_dropped.append({
                    'underlying': _underlying,
                    'leftover_shares': _leftover_shares,
                    'leftover_raw': _leftover_raw,
                })
                print(f"  (!) WARNUNG: Stillhalter-Korrektur für {_underlying} nicht "
                      f"vollständig zugeordnet: {_leftover_raw:,.2f} (Handelswährung) auf "
                      f"{_leftover_shares:g} Stück ohne passende Verkaufszeile. Die Prämie "
                      f"bleibt im Aktien-PnL eingebettet (potenzielle Doppelversteuerung) — "
                      f"bitte Trades des Underlyings prüfen.")

        # Interne Row-Identität nach dem Matching entfernen (kein Export-Leak).
        for row in debug_rows:
            row.pop('_trade_oid', None)

        # Apply per-trade gain/loss split (replaces old pauschal: stocks_gain -= stk_premium)
        stocks_gain -= stk_gain_corr_cy
        stocks_loss -= stk_loss_corr_cy
        etf_invstg_gain -= etf_gain_corr_cy
        etf_invstg_loss -= etf_loss_corr_cy
        options_gain -= nv_gain_corr_cy
        options_loss -= nv_loss_corr_cy
        # Shadow-Tracking (Plausibilitätscheck + Topf-2-Aufschlüsselung) synchron halten:
        # no_invstg_gain/loss speist den GUI-Plausibilitätscheck gegen pnl_summary.csv,
        # topf2_by_category die „Aufschlüsselung Topf 2" im Report. Ohne diese Sync
        # zeigt die Aufschlüsselung (Crypto/Commodity ETPs + Stillhalterprämien) eine
        # Summe, die den Topf-2-Saldo übersteigt.
        no_invstg_gain -= nv_gain_corr_cy
        no_invstg_loss -= nv_loss_corr_cy
        if 'Crypto/Commodity ETPs' in topf2_by_category:
            topf2_by_category['Crypto/Commodity ETPs']['gain'] -= nv_gain_corr_cy
            topf2_by_category['Crypto/Commodity ETPs']['loss'] -= nv_loss_corr_cy
        for _isin, _adj in _etf_by_isin_corr_cy.items():
            if _isin in etf_by_isin:
                etf_by_isin[_isin]['gain'] -= _adj['gain']
                etf_by_isin[_isin]['loss'] -= _adj['loss']

        price_source = "tradePrice" if has_trade_price else "closePrice (Näherung)"
        parts = []
        if stk_premium > 0:
            parts.append(f"{stk_premium:,.2f} von Aktien")
        if etf_premium > 0:
            parts.append(f"{etf_premium:,.2f} von ETF/KAP-INV")
        if put_nosell_premium > 0:
            parts.append(f"{put_nosell_premium:,.2f} Put-Andienung (Aktie nicht verkauft)")
        print(f"Stillhalterprämien: {stillhalter_count} Assignments, {stillhalter_premium_eur:,.2f} EUR → Topf 2 ({', '.join(parts)}) (Quelle: {price_source}).")
    if stillhalter_unmatched:
        print(f"  (!) WARNUNG: {len(stillhalter_unmatched)} Assignment(s) — der ursprüngliche Optionsverkauf "
              f"(ExchTrade SELL) wurde nicht gefunden. Vermutlich in einem Vorjahr eröffnet. "
              f"Ohne diesen kann die Stillhalterprämie nicht berechnet und von Topf 1 (Aktien) "
              f"nach Topf 2 (Sonstiges) verschoben werden. Vorjahres-XMLs per --history laden.")

    # --- Stillhalter-Zufluss: SELL-to-open Prämien (§11 EStG, BMF Rn. 25) ---
    # When a short option is SOLD to open, the premium is taxable income (Zufluss)
    # in the year of sale — regardless of when the position is closed.
    # IBKR shows fifoPnlRealized=0 for opening trades; the PnL only appears at close.
    # We detect unclosed SELL-to-open positions and add their premiums as Zufluss income.
    # Positions closed in the same year are already captured via fifoPnlRealized on the close.

    zufluss_premium_eur = 0.0
    zufluss_count = 0
    zufluss_details = []
    prior_zufluss_details = []

    from collections import defaultdict

    def _option_key(t):
        return (t.get('assetCategory'), t.get('underlyingSymbol', ''),
                t.get('strike'), t.get('expiry'), t.get('putCall'))

    def _occ_family_key(key):
        # OCC-adjusted Serien nach Kapitalmassnahmen (Spinoff/Merger) haengen eine
        # Ziffer an das Underlying an (MMM -> MMM1), strike/expiry/putCall bleiben
        # identisch. IBKRs eigenes FIFO verknuepft Close und Open ueber die
        # Umbenennung hinweg (fifoPnlRealized enthaelt die Praemie) — ohne
        # Familien-Matching gilt der Original-SELL faelschlich als offen und die
        # Praemie wird doppelt erfasst (Zufluss + Rueckkauf-PnL). Nur fuer OPT:
        # FOP-Underlyings (z.B. ESZ4) tragen legitime Ziffern-Suffixe.
        if key[0] != 'OPT':
            return key
        und = key[1] or ''
        root = und.rstrip('0123456789')
        return (key[0], root or und, key[2], key[3], key[4])

    def _option_sort_key(t):
        return t.get('dateTime') or t.get('tradeDate') or t.get('reportDate') or ''

    series_events = defaultdict(list)
    all_sell_open_keys = set()
    for t in trades:
        if t.get('assetCategory') not in ('OPT', 'FOP', 'FSFOP'):
            continue
        rd = parse_date(t.get('reportDate') or t.get('dateTime') or t.get('tradeDate'))
        if not rd or rd.year > tax_year:
            continue
        key = _option_key(t)
        if (t.get('transactionType') == 'ExchTrade' and t.get('buySell') == 'SELL'
                and abs(safe_float(t.get('fifoPnlRealized'))) < 0.01):
            series_events[key].append(t)
            all_sell_open_keys.add(key)
        elif ((t.get('transactionType') == 'ExchTrade' and t.get('buySell') == 'BUY'
               and abs(safe_float(t.get('fifoPnlRealized'))) >= 0.01)
              or (t.get('transactionType') == 'BookTrade' and t.get('buySell') == 'BUY')):
            series_events[key].append(t)

    current_zufluss_by_key = {}

    def _add_current_zufluss(key, sell, open_qty):
        components = _premium_components_for_consumed_sell(
            sell, open_qty, int(safe_float(sell.get('multiplier'), 100)),
            base_currency, usd_to_eur_rates
        )
        if components is None:
            return
        acc = current_zufluss_by_key.setdefault(key, {
            'first_sell': sell,
            'quantity': 0,
            'premium_raw': 0.0,
            'commission_raw': 0.0,
            'premium_eur': 0.0,
            'fx_weighted': 0.0,
        })
        acc['quantity'] += open_qty
        acc['premium_raw'] += components['premium_raw']
        acc['commission_raw'] += components['commission_raw']
        acc['premium_eur'] += components['premium_eur']
        acc['fx_weighted'] += components['fx_weighted']
        sd = parse_date(sell.get('dateTime') or sell.get('tradeDate'))
        first_sd = parse_date(acc['first_sell'].get('dateTime') or acc['first_sell'].get('tradeDate'))
        if sd and (first_sd is None or sd < first_sd):
            acc['first_sell'] = sell

    def _add_prior_zufluss_detail(key, sell, close_qty):
        components = _premium_components_for_consumed_sell(
            sell, close_qty, int(safe_float(sell.get('multiplier'), 100)),
            base_currency, usd_to_eur_rates
        )
        if components is None:
            return
        prior_zufluss_details.append({
            'symbol': sell.get('symbol') or sell.get('description') or f"{key[1]} {key[2]} {key[3]} {key[4]}",
            'underlyingSymbol': key[1],
            'strike': key[2],
            'expiry': key[3],
            'putCall': key[4],
            'quantity': close_qty,
            'premium_eur': components['premium_eur'],
            'premium_raw': components['net_premium_raw'],
            'commission_raw': components['commission_raw'],
            'fx_to_base': components['fx_weighted'] / close_qty if close_qty else 1.0,
            'currency': sell.get('currency', ''),
            'multiplier': int(safe_float(sell.get('multiplier'), 100)),
            'avg_price': components['premium_raw'] / (
                close_qty * int(safe_float(sell.get('multiplier'), 100))
            ) if close_qty else 0,
            'sell_date': str(parse_date(sell.get('dateTime') or sell.get('tradeDate'))) if parse_date(sell.get('dateTime') or sell.get('tradeDate')) else '',
            'sell_year': parse_date(sell.get('dateTime') or sell.get('tradeDate')).year if parse_date(sell.get('dateTime') or sell.get('tradeDate')) else tax_year - 1,
            'type': 'prior_year_correction',
        })

    # FIFO über die vollständige Series-Historie bis zum Steuerjahresende:
    # aktuelle Rückkäufe verbrauchen zuerst noch offene Vorjahres-Sells. Dadurch
    # werden aktuelle Sells nicht fälschlich als geschlossen behandelt und
    # Vorjahresprämien nur für tatsächlich im Steuerjahr geschlossene Lots korrigiert.
    # Serien laufen pro OCC-Familie (siehe _occ_family_key): ein Close konsumiert
    # zuerst Lots seiner exakten Serie, danach Lots umbenannter Schwester-Serien.
    series_families = defaultdict(list)
    for key in series_events:
        series_families[_occ_family_key(key)].append(key)

    # Transparenz: jede Familien-Fallback-Zuordnung (Close einer umbenannten
    # Serie auf den Original-SELL) wird getrackt und in der GUI angezeigt.
    occ_rename_matches = []

    for fam_keys in series_families.values():
        events = [(k, ev) for k in fam_keys for ev in series_events[k]]
        open_lots = []
        for k, ev in sorted(events, key=lambda pair: _option_sort_key(pair[1])):
            ev_date = parse_date(ev.get('reportDate') or ev.get('dateTime') or ev.get('tradeDate'))
            if not ev_date:
                continue
            if (ev.get('transactionType') == 'ExchTrade' and ev.get('buySell') == 'SELL'
                    and abs(safe_float(ev.get('fifoPnlRealized'))) < 0.01):
                qty = abs(int(safe_float(ev.get('quantity'))))
                if qty > 0:
                    open_lots.append({'trade': ev, 'remaining': qty, 'key': k})
                continue

            close_qty = abs(int(safe_float(ev.get('quantity'))))
            if close_qty <= 0:
                continue
            # Steuerwirksame Schließung = jeder BUY mit realisierter PnL: ExchTrade-Buyback
            # ODER BookTrade-Verfall (fifoPnlRealized = Prämie). BookTrade mit PnL≈0
            # (Assignment) bleibt ausgenommen — dessen Cross-Year-Prämie erfasst
            # _build_stillhalter_details_for_assignment separat (sonst Doppelkorrektur).
            is_buy_close = (ev.get('buySell') == 'BUY'
                            and abs(safe_float(ev.get('fifoPnlRealized'))) >= 0.01)
            remaining_close = close_qty
            # Exact-Key-Priorität: koexistieren Original- und adjusted Serie
            # (verschiedene Deliverables!), bleibt die eigene Serie zuerst dran;
            # der Familien-Fallback greift nur für sonst unmatchte Closes.
            for exact_pass in (True, False):
                for lot in open_lots:
                    if remaining_close <= 0:
                        break
                    if lot['remaining'] <= 0:
                        continue
                    if (lot['key'] == k) != exact_pass:
                        continue
                    take = min(lot['remaining'], remaining_close)
                    sell_date = parse_date(lot['trade'].get('reportDate') or lot['trade'].get('dateTime') or lot['trade'].get('tradeDate'))
                    if is_buy_close and ev_date.year == tax_year and sell_date and sell_date.year < tax_year:
                        _add_prior_zufluss_detail(lot['key'], lot['trade'], take)
                    if not exact_pass:
                        occ_rename_matches.append({
                            'sell_symbol': lot['trade'].get('symbol', ''),
                            'sell_underlying': lot['key'][1],
                            'sell_date': str(sell_date) if sell_date else '',
                            'close_symbol': ev.get('symbol', ''),
                            'close_underlying': k[1],
                            'close_date': str(ev_date),
                            'strike': k[2],
                            'expiry': k[3],
                            'putCall': k[4],
                            'quantity': take,
                        })
                    lot['remaining'] -= take
                    remaining_close -= take
                if remaining_close <= 0:
                    break

        for lot in open_lots:
            if lot['remaining'] <= 0:
                continue
            sell_date = parse_date(lot['trade'].get('reportDate') or lot['trade'].get('dateTime') or lot['trade'].get('tradeDate'))
            if sell_date and sell_date.year == tax_year:
                _add_current_zufluss(lot['key'], lot['trade'], lot['remaining'])

    if occ_rename_matches:
        renames = ", ".join(sorted({f"{m['sell_underlying']} -> {m['close_underlying']}"
                                    for m in occ_rename_matches}))
        print(f"  (i) Kapitalmassnahme erkannt: {len(occ_rename_matches)} Glattstellung(en) "
              f"umbenannter Optionsserien dem Original-SELL zugeordnet ({renames}). "
              f"Verhindert Doppelerfassung der Stillhalterpraemie.")

    for key, acc in current_zufluss_by_key.items():
        if acc['quantity'] <= 0 or acc['premium_raw'] == 0:
            continue
        sell = acc['first_sell']
        mult = int(safe_float(sell.get('multiplier'), 100))
        net_premium_raw = acc['premium_raw'] + acc['commission_raw']
        premium_eur = acc['premium_eur']
        fx_to_base = acc['fx_weighted'] / acc['quantity'] if acc['quantity'] else 1.0
        sell_date = parse_date(sell.get('dateTime') or sell.get('tradeDate'))

        zufluss_premium_eur += premium_eur
        zufluss_count += 1

        zufluss_details.append({
            'symbol': sell.get('symbol') or sell.get('description') or f"{key[1]} {key[2]} {key[3]} {key[4]}",
            'underlyingSymbol': key[1],
            'strike': key[2],
            'expiry': key[3],
            'putCall': key[4],
            'quantity': acc['quantity'],
            'premium_eur': premium_eur,
            'premium_raw': net_premium_raw,
            'commission_raw': acc['commission_raw'],
            'fx_to_base': fx_to_base,
            'currency': sell.get('currency', ''),
            'multiplier': mult,
            'avg_price': acc['premium_raw'] / (acc['quantity'] * mult) if (acc['quantity'] and mult) else 0,
            'sell_date': str(sell_date) if sell_date else '',
            'sell_year': sell_date.year if sell_date else tax_year,
            'type': 'sell_to_open',
        })

    if zufluss_premium_eur > 0:
        options_gain += zufluss_premium_eur
        add_topf2_detail('Stillhalterprämien', zufluss_premium_eur)
        print(f"Stillhalter-Zufluss: {zufluss_count} offene Position(en), "
              f"{zufluss_premium_eur:,.2f} EUR Prämien → Topf 2 (§11 EStG).")

        # Add zufluss premiums to trade details
        for det in zufluss_details:
            pc_label = 'Call' if det['putCall'] == 'C' else 'Put'
            underlying = det.get('underlyingSymbol') or (det['symbol'].split()[0] if det['symbol'] else '')
            debug_rows.append({
                'dateTime': det.get('sell_date', ''), 'reportDate': det.get('sell_date', ''),
                'symbol': det['symbol'],
                'description': f'Zufluss-Prämie ({pc_label}, §11 EStG, offene Position)',
                'isin': '', 'assetCategory': 'OPT', 'subCategory': '',
                'buySell': 'STO', 'openClose': 'O',
                'quantity': str(det['quantity']),
                'transactionType': 'Zufluss',
                'currency': det.get('currency', ''),
                'tradePrice': det.get('avg_price', 0),
                'cost': 0,
                'proceeds': det.get('premium_raw', 0),
                'ibCommission': det.get('commission_raw', 0),
                'fifoPnlRealized': det.get('premium_raw', 0),
                'fxRateToBase': det.get('fx_to_base', 0),
                'pnl_eur': round(det['premium_eur'], 5),
                'topf': 'Topf2',
                'strike': det['strike'], 'expiry': det['expiry'],
                'putCall': det['putCall'],
                'multiplier': str(det.get('multiplier', '')),
                'underlyingSymbol': underlying,
                'source': 'zufluss',
            })

    # --- Vorjahres-Stillhalter-Korrektur (Zuflussprinzip) ---
    # When --history XMLs are loaded, we find SELL-to-open from prior years that were
    # closed in the current tax year. IBKR's fifoPnlRealized on the close includes the
    # prior-year premium — but that premium was already taxable in the selling year.
    # We subtract the premium to avoid double-counting.

    prior_zufluss_correction_eur = sum(d['premium_eur'] for d in prior_zufluss_details)

    if prior_zufluss_correction_eur > 0:
        # Subtract prior-year premium from current PnL (already taxed in prior year)
        options_gain -= prior_zufluss_correction_eur
        add_topf2_detail('Stillhalterprämien', -prior_zufluss_correction_eur)
        print(f"Vorjahres-Stillhalter-Korrektur: {len(prior_zufluss_details)} Position(en), "
              f"-{prior_zufluss_correction_eur:,.2f} EUR (Prämie bereits im Verkaufsjahr versteuert).")

        for det in prior_zufluss_details:
            pc_label = 'Call' if det['putCall'] == 'C' else 'Put'
            underlying = det.get('underlyingSymbol') or (det['symbol'].split()[0] if det['symbol'] else '')
            debug_rows.append({
                'dateTime': det.get('sell_date', ''), 'reportDate': det.get('sell_date', ''),
                'symbol': det['symbol'],
                'description': f'Vorjahres-Zufluss-Korrektur ({pc_label}, Prämie {det["sell_year"]} bereits versteuert)',
                'isin': '', 'assetCategory': 'OPT', 'subCategory': '',
                'buySell': '', 'openClose': '',
                'quantity': str(det['quantity']),
                'transactionType': 'Zufluss-Korrektur',
                'currency': det.get('currency', ''),
                'tradePrice': det.get('avg_price', 0),
                'cost': 0,
                'proceeds': -det.get('premium_raw', 0),
                'ibCommission': -det.get('commission_raw', 0),
                'fifoPnlRealized': -det.get('premium_raw', 0),
                'fxRateToBase': det.get('fx_to_base', 0),
                'pnl_eur': round(-det['premium_eur'], 5),
                'topf': 'Topf2',
                'strike': det['strike'], 'expiry': det['expiry'],
                'putCall': det['putCall'],
                'multiplier': str(det.get('multiplier', '')),
                'underlyingSymbol': underlying,
                'source': 'zufluss_korrektur',
            })

    # --- Fehlende Vorjahres-XMLs erkennen ---
    # BUY-close (Glattstellung/Verfall) ohne matching SELL-to-open = Vorjahr fehlt
    # all_sell_open_keys contains current-year and prior-year openings from history.

    zufluss_unmatched = []
    # Familien-Check analog zum Zufluss-FIFO: ein Close unter einer OCC-
    # umbenannten Serie (MMM1) ist gematcht, wenn die Original-Serie (MMM)
    # einen Eröffnungs-SELL hat — keine False-Positive-Warnung.
    all_sell_open_family_keys = {_occ_family_key(k) for k in all_sell_open_keys}
    for t in trades:
        if t.get('assetCategory') not in ('OPT', 'FOP', 'FSFOP'):
            continue
        # Gleiche Close-Definition wie is_buy_close im Zufluss-FIFO: jeder BUY
        # mit realisierter PnL — ExchTrade-Buyback ODER BookTrade-Verfall
        # (fifoPnlRealized = Prämie). Assignments (PnL≈0) fallen durch den
        # PnL-Filter. Vorher wurden BookTrade-Verfälle ohne Vorjahres-XML
        # nicht gewarnt, obwohl die Prämie doppelt versteuert bleibt.
        if t.get('buySell') != 'BUY':
            continue
        if abs(safe_float(t.get('fifoPnlRealized'))) < 0.01:
            continue  # Opening BUY oder Assignment, not a taxable close
        rd = parse_date(t.get('reportDate') or t.get('dateTime') or t.get('tradeDate'))
        if not rd or rd.year != tax_year:
            continue
        key = (t.get('assetCategory'), t.get('underlyingSymbol', ''),
               t.get('strike'), t.get('expiry'), t.get('putCall'))
        if key not in all_sell_open_keys and _occ_family_key(key) not in all_sell_open_family_keys:
            symbol = t.get('symbol') or t.get('description') or f"{key[1]} {key[2]} {key[3]} {key[4]}"
            # Avoid duplicate warnings for same instrument
            if not any(u.get('underlyingSymbol', '') == key[1]
                       and u['strike'] == key[2]
                       and u['expiry'] == key[3]
                       and u['putCall'] == key[4]
                       for u in zufluss_unmatched):
                zufluss_unmatched.append({
                    'symbol': symbol,
                    'underlyingSymbol': key[1],
                    'strike': key[2],
                    'expiry': key[3],
                    'putCall': key[4],
                    'quantity': abs(int(safe_float(t.get('quantity')))),
                })

    if zufluss_unmatched:
        print(f"  (!) WARNUNG: {len(zufluss_unmatched)} Glattstellung(en) ohne Eröffnungs-SELL. "
              f"Die Option wurde in einem Vorjahr verkauft (Prämie kassiert). Ohne das Vorjahres-XML "
              f"kann die Zufluss-Korrektur nicht angewendet werden (Prämie wird doppelt versteuert).")

    # --- Cross-Year Put-Assignment Korrektur (BMF Rn. 33) ---
    # When a put was assigned in a PRIOR year, the stock was acquired at Strike.
    # IBKR reduced the cost basis by the premium (Strike - Premium).
    # The premium was already taxed in the assignment year as §20 Abs.1 Nr.11.
    # When the stock is sold in the CURRENT year, we must correct IBKR's PnL
    # by removing the premium effect (making the stock loss bigger / gain smaller).
    # Unlike same-year assignments, we do NOT add to options_gain (already taxed).

    prior_put_assignments = [t for t in trades
                             if t.get('assetCategory') in ('OPT', 'FOP', 'FSFOP')
                             and t.get('transactionType') == 'BookTrade'
                             and t.get('buySell') == 'BUY'
                             and t.get('putCall') == 'P'
                             and abs(safe_float(t.get('fifoPnlRealized'))) < 0.01
                             and (d := parse_date(t.get('reportDate') or t.get('dateTime') or t.get('tradeDate'))) is not None
                             and d.year < tax_year]

    # Build FIFO lots per underlying symbol from prior-year put assignments
    from collections import deque
    put_assignment_lots = {}  # {symbol: deque of (date, shares_remaining, premium_per_share_eur)}
    # Issue #55: paralleles immutable Dict fuer _tageskurs_put_adj. Da
    # put_assignment_lots durch die Apply-Schleife (popleft bei shares_remaining<=0)
    # mutiert wird, koennen die Original-Werte spaeter nicht mehr abgerufen werden.
    _xy_tageskurs_lots = {}  # {symbol: list of {date_str, shares, premium_per_share_raw}}
    cross_year_put_corrections = []
    cross_year_put_total = 0.0
    _xy_closed_share_remaining = None
    _xy_closed_path = os.path.join(ib_tax_dir, 'closed_lots.csv')
    if os.path.exists(_xy_closed_path):
        _xy_closed_share_remaining = {}
        for lot in load_csv(_xy_closed_path):
            if lot.get('assetCategory') != 'STK':
                continue
            report_date = parse_date(lot.get('reportDate') or lot.get('dateTime'))
            if not report_date or report_date.year != tax_year:
                continue
            # Key-Ableitung identisch zum trades-Loop und put_assignment_lots:
            # underlyingSymbol NICHT splitten (Klassen-Aktien wie 'BRK B'),
            # nur der symbol-Fallback wird gesplittet.
            sym = lot.get('underlyingSymbol') or lot.get('symbol', '').split()[0]
            open_date = (lot.get('openDateTime') or '')[:10]
            qty = abs(safe_float(lot.get('quantity'), 0))
            if not sym or not open_date or qty <= 0:
                continue
            key = (sym, open_date)
            _xy_closed_share_remaining[key] = _xy_closed_share_remaining.get(key, 0) + qty

    # Issue #54: Bei mehreren Andienungen derselben Series werden die Original-Sells
    # FIFO konsumiert (aelteste zuerst), nicht als Durchschnitt verteilt. State pro
    # Series wird zwischen den Iterationen weitergetragen. Andienungen werden zeitlich
    # sortiert, damit die fruehere Andienung die aelteren Sells bekommt.
    # Sort-Key: dateTime → tradeDate → reportDate (analog zum Filter oben).
    prior_put_assignments_sorted = sorted(
        prior_put_assignments,
        key=lambda t: (t.get('dateTime', '') or t.get('tradeDate', '') or t.get('reportDate', '') or '')
    )
    # series_key umfasst underlyingSymbol — verschiedene Aktien koennen dieselbe
    # strike/expiry-Kombination haben (z.B. KWEB P 30 vs FXI P 30).
    _prior_put_series_state = {}  # {(a_cat, underlying, strike, expiry): originals_state_list}

    for a in prior_put_assignments_sorted:
        strike = a.get('strike')
        expiry = a.get('expiry')
        a_cat = a.get('assetCategory')
        a_qty = abs(int(safe_float(a.get('quantity'))))
        mult = int(safe_float(a.get('multiplier'), 100))
        underlying = a.get('underlyingSymbol', '')
        if not strike or not underlying or a_qty == 0:
            continue

        series_key = (a_cat, underlying, strike, expiry)
        if series_key not in _prior_put_series_state:
            assign_qty_series = sum(
                abs(int(safe_float(t.get('quantity'))))
                for t in trades
                if t.get('assetCategory') == a_cat
                and t.get('transactionType') == 'BookTrade'
                and t.get('buySell') == 'BUY'
                and t.get('strike') == strike
                and t.get('expiry') == expiry
                and t.get('putCall') == 'P'
                and t.get('underlyingSymbol', '') == underlying
                and abs(safe_float(t.get('fifoPnlRealized'))) < 0.01
            )
            _prior_put_series_state[series_key] = _get_open_option_sells(
                trades, a_cat, strike, expiry, 'P', assign_qty_series, underlying=underlying)

        originals_state = _prior_put_series_state[series_key]
        if not originals_state:
            continue

        premium_raw, commission_raw, fx_weighted, premium_eur, sells_consumed, consumed_qty = \
            _consume_open_sells_fifo(originals_state, a_qty, mult, base_currency, usd_to_eur_rates)

        if consumed_qty == 0 or premium_raw == 0:
            continue

        net_premium_raw = premium_raw + commission_raw
        assignment_shares = consumed_qty * mult
        fx_to_base = fx_weighted / consumed_qty if consumed_qty else 1.0  # nur Display

        premium_per_share_eur = premium_eur / assignment_shares if assignment_shares else 0
        a_date = parse_date(a.get('reportDate') or a.get('dateTime') or a.get('tradeDate'))

        premium_per_share_raw = net_premium_raw / assignment_shares if assignment_shares else 0
        lot_open_dates = [
            ((a.get('dateTime') or a.get('tradeDate') or '')[:10]),
            ((a.get('reportDate') or '')[:10]),
        ]
        lot_open_dates = [d for i, d in enumerate(lot_open_dates) if d and d not in lot_open_dates[:i]]
        matched_open_date = lot_open_dates[0] if lot_open_dates else ''
        shares = assignment_shares
        if _xy_closed_share_remaining is not None:
            closed_key = None
            closed_shares = 0
            for candidate_date in lot_open_dates:
                candidate_key = (underlying, candidate_date)
                candidate_shares = _xy_closed_share_remaining.get(candidate_key, 0)
                if candidate_shares > 0:
                    closed_key = candidate_key
                    closed_shares = candidate_shares
                    matched_open_date = candidate_date
                    break
            if closed_shares <= 0:
                continue
            shares = min(assignment_shares, closed_shares)
            _xy_closed_share_remaining[closed_key] = closed_shares - shares
        if underlying not in put_assignment_lots:
            put_assignment_lots[underlying] = deque()
        put_assignment_lots[underlying].append({
            'date': a_date,
            'shares_remaining': shares,
            'premium_per_share_eur': premium_per_share_eur,
            'premium_per_share_raw': premium_per_share_raw,
            'strike': strike,
            'year': a_date.year if a_date else 0,
        })
        # Issue #55: Snapshot fuer _tageskurs_put_adj — bleibt erhalten auch wenn
        # put_assignment_lots durch Apply-Schleife geleert wird.
        # date_str nutzt das tatsaechlich in closed_lots gematchte Open-Datum.
        # IBKR kann bei Andienungen je nach Buchung tradeDate oder reportDate
        # als openDateTime fuehren.
        _xy_tageskurs_lots.setdefault(underlying, []).append({
            'date_str': matched_open_date,
            'shares': shares,
            'premium_per_share_raw': premium_per_share_raw,
        })
        # Anlage-SO-Lookup für cross-year (Issue #51)
        u_isin_xy = symbol_to_isin.get(underlying, '')
        if u_isin_xy and _effective_classification(u_isin_xy) == 'anlage_so' and a_date:
            so_key = (underlying, a_date.strftime('%Y-%m-%d'))
            _so_premium_lookup.setdefault(so_key, {'shares': 0, 'premium_eur': 0.0})
            _so_premium_lookup[so_key]['shares'] += shares
            _so_premium_lookup[so_key]['premium_eur'] += premium_eur

    # Apply corrections to STK sells in tax_year
    if put_assignment_lots:
        # Sort lots FIFO per symbol
        for sym in put_assignment_lots:
            put_assignment_lots[sym] = deque(sorted(put_assignment_lots[sym], key=lambda x: x['date'] or ''))

        cross_year_put_total = 0.0

        for t in trades:
            report_date = parse_date(t.get('reportDate') or t.get('dateTime') or t.get('tradeDate'))
            if not report_date or report_date.year != tax_year:
                continue
            if t.get('assetCategory') != 'STK':
                continue
            if t.get('buySell') not in ('SELL',):
                continue
            pnl_str = t.get('fifoPnlRealized')
            if not pnl_str or float(pnl_str) == 0:
                continue

            sym = t.get('underlyingSymbol') or t.get('symbol', '').split()[0]
            if sym not in put_assignment_lots:
                continue

            sell_qty = abs(int(safe_float(t.get('quantity'))))
            remaining = sell_qty

            while remaining > 0 and put_assignment_lots[sym]:
                lot = put_assignment_lots[sym][0]
                consumed = min(remaining, lot['shares_remaining'])
                # correction_eur wird erst in der debug_rows-Schleife unten gesetzt:
                # der tatsaechliche EUR-Betrag haengt vom FX-Kurs des Aktienverkaufs
                # ab, nicht vom Options-Verkaufskurs (premium_per_share_eur).
                cross_year_put_corrections.append({
                    'symbol': sym,
                    'shares': consumed,
                    'premium_per_share': lot['premium_per_share_eur'],
                    'premium_per_share_raw': lot['premium_per_share_raw'],
                    'correction_eur': 0.0,
                    'assignment_year': lot['year'],
                    'strike': lot['strike'],
                })
                lot['shares_remaining'] -= consumed
                remaining -= consumed
                if lot['shares_remaining'] <= 0:
                    put_assignment_lots[sym].popleft()

        if cross_year_put_corrections:
            # Correct stock debug_rows in-place AND derive the pool adjustment from the
            # actual per-row EUR delta — exact same pattern as the same-year
            # pending_stk_corrections block (Issue #23). Earlier this block subtracted
            # premium_per_share_eur (premium at the option-sell FX rate) from stocks_gain
            # while the debug_row used premium_per_share_raw × fx_stock_sale. With a
            # cross-year put those FX rates differ, so the Topf-1 saldo (GUI) drifted away
            # from the Trade-Details sum (Excel). Deriving the pool adjustment from
            # correction_eur = original_pnl_eur − row['pnl_eur'] keeps them identical.
            # IBKR cost = strike×qty − premium (embedded). Restore to strike×qty.
            stk_gain_corr = 0.0
            stk_loss_corr = 0.0
            etf_gain_corr = 0.0
            etf_loss_corr = 0.0
            nv_gain_corr = 0.0  # no_invstg ETP correction → Topf 2
            nv_loss_corr = 0.0
            _etf_by_isin_corr_xy = {}

            _xy_pending = {}  # {symbol: [{premium_per_share_raw, remaining_shares, corr_ref}]}
            for c in cross_year_put_corrections:
                _xy_pending.setdefault(c['symbol'], []).append({
                    'premium_per_share_raw': c['premium_per_share_raw'],
                    'remaining_shares': c['shares'],
                    'corr_ref': c,
                })
            for row in debug_rows:
                if row.get('source') != 'trades' or row.get('assetCategory') != 'STK':
                    continue
                # Die Korrekturen stammen ausschliesslich aus STK-SELL-Trades
                # (s. trades-Loop oben) — nur SELL-Rows duerfen sie konsumieren,
                # sonst greift z.B. ein Short-Cover-BUY desselben Symbols sie ab.
                if row.get('buySell') != 'SELL':
                    continue
                # Key-Ableitung identisch zum trades-Loop (sym) und put_assignment_lots:
                # underlyingSymbol NICHT splitten, sonst verfehlt 'BRK B' den
                # _xy_pending-Eintrag und die Korrektur bleibt stumm bei 0.
                row_symbol = row.get('underlyingSymbol') or row.get('symbol', '').split()[0]
                if not row_symbol or row_symbol not in _xy_pending:
                    continue
                qty = abs(safe_float(row.get('quantity', '0'), 0))
                if qty == 0:
                    continue
                original_pnl_eur = row['pnl_eur']
                total_correction_raw = 0.0
                remaining = qty
                _row_corr_refs = []  # [(cross_year_put_corrections-Eintrag, chunk_raw)]
                for corr in _xy_pending[row_symbol]:
                    if corr['remaining_shares'] <= 0:
                        continue
                    consumed = min(remaining, corr['remaining_shares'])
                    chunk_raw = consumed * corr['premium_per_share_raw']
                    total_correction_raw += chunk_raw
                    _row_corr_refs.append((corr['corr_ref'], chunk_raw))
                    corr['remaining_shares'] -= consumed
                    remaining -= consumed
                    if remaining <= 0:
                        break
                if total_correction_raw > 0:
                    if row['cost'] >= 0:
                        row['cost'] += total_correction_raw
                    else:
                        row['cost'] -= total_correction_raw
                    row['fifoPnlRealized'] -= total_correction_raw
                    fx = row.get('fxRateToBase', 1.0)
                    if base_currency == 'EUR':
                        row['pnl_eur'] = round(row['fifoPnlRealized'] * fx, 5)
                    else:
                        d = parse_date(row.get('dateTime', ''))
                        r_eur = get_rate_for_date(d, usd_to_eur_rates)
                        row['pnl_eur'] = round(row['fifoPnlRealized'] * fx * r_eur, 5)
                    row['stillhalter_adjusted'] = True

                    # Pool-Anpassung aus dem tatsächlichen Row-Delta ableiten
                    # (gain/loss-Split-Logik identisch zum Same-Year-Block).
                    correction_eur = original_pnl_eur - row['pnl_eur']
                    # Tatsaechlichen EUR-Korrekturbetrag (stock_fx) anteilig auf die
                    # konsumierten cross_year_put_corrections-Eintraege verteilen, damit
                    # Box-Gesamt, Einzelzeilen, Pool-Reduktion und Plausibilitaetscheck-
                    # Add-Back exakt dieselbe Basis haben (Codex P2).
                    for _ref, _chunk_raw in _row_corr_refs:
                        _ref['correction_eur'] += correction_eur * (
                            _chunk_raw / total_correction_raw)
                    row_isin = row.get('isin', '')
                    _row_cls = _effective_classification(row_isin) if row_isin else None
                    is_so = bool(row_isin and row_isin in etf_isins and _row_cls == 'anlage_so')
                    is_etf = bool(row_isin and row_isin in etf_isins
                                  and _row_cls not in ('no_invstg', 'anlage_so'))
                    if original_pnl_eur > 0:
                        from_gain = min(correction_eur, original_pnl_eur)
                        from_loss = correction_eur - from_gain
                    else:
                        from_gain = 0.0
                        from_loss = correction_eur
                    if is_so:
                        # Anlage-SO-Override (Issue #51): Keine Aggregation auf
                        # stocks/ETF-Pools. Korrektur läuft per Lot im Anlage-SO-Build.
                        pass
                    elif is_etf:
                        etf_gain_corr += from_gain
                        etf_loss_corr += from_loss
                        if row_isin not in _etf_by_isin_corr_xy:
                            _etf_by_isin_corr_xy[row_isin] = {'gain': 0.0, 'loss': 0.0}
                        _etf_by_isin_corr_xy[row_isin]['gain'] += from_gain
                        _etf_by_isin_corr_xy[row_isin]['loss'] += from_loss
                    elif _row_cls == 'no_invstg':
                        nv_gain_corr += from_gain
                        nv_loss_corr += from_loss
                    else:
                        stk_gain_corr += from_gain
                        stk_loss_corr += from_loss

            # NOT options_gain += ... (premium was already taxed in the assignment year)
            stocks_gain -= stk_gain_corr
            stocks_loss -= stk_loss_corr
            etf_invstg_gain -= etf_gain_corr
            etf_invstg_loss -= etf_loss_corr
            options_gain -= nv_gain_corr
            options_loss -= nv_loss_corr
            # Shadow-Tracking synchron halten (analog current-year, s. dortigen Kommentar).
            no_invstg_gain -= nv_gain_corr
            no_invstg_loss -= nv_loss_corr
            if 'Crypto/Commodity ETPs' in topf2_by_category:
                topf2_by_category['Crypto/Commodity ETPs']['gain'] -= nv_gain_corr
                topf2_by_category['Crypto/Commodity ETPs']['loss'] -= nv_loss_corr
            for _isin, _adj in _etf_by_isin_corr_xy.items():
                if _isin in etf_by_isin:
                    etf_by_isin[_isin]['gain'] -= _adj['gain']
                    etf_by_isin[_isin]['loss'] -= _adj['loss']

            # cross_year_put_total = tatsaechlich von den Pools subtrahierter Betrag
            # (stock_fx) — NICHT die Praemie zum Options-Verkaufskurs. app.py nutzt
            # diesen Wert fuer die Cross-Year-Put-Box und den Plausibilitaetscheck-
            # Add-Back; beide muessen exakt der Pool-/Trade-Details-Korrektur entsprechen.
            cross_year_put_total = sum(c['correction_eur'] for c in cross_year_put_corrections)
            print(f"Cross-Year Put-Korrektur: {len(cross_year_put_corrections)} Positionen, "
                  f"{cross_year_put_total:,.2f} EUR von PnL abgezogen (Prämie bereits in Vorjahr versteuert).")

    # Zuflussprinzip: cross-year premium aggregation
    # Combines three sources:
    # 1. Assignment in current year, SELL in prior year → subtract from current (existing)
    # 2. SELL-to-open unclosed in current year → add to current (zufluss_premium_eur, already applied above)
    # 3. Prior-year SELL closed in current year → subtract from current (prior_zufluss_correction_eur, already applied above)
    cross_year_premium_eur = sum(d['premium_eur'] for d in stillhalter_details if d['is_cross_year'])
    cross_year_by_year = {}
    for det in stillhalter_details:
        if det['is_cross_year']:
            yr = det['orig_sell_year']
            cross_year_by_year[yr] = cross_year_by_year.get(yr, 0) + det['premium_eur']
    # Add prior-year SELL-to-open corrections to cross_year_by_year (display only).
    # Do NOT add to cross_year_premium_eur — prior_zufluss_correction_eur is already
    # subtracted from options_gain in the block above (line ~1484). Aggregating it
    # here would cause the GUI's Zuflussprinzip toggle to subtract the same amount
    # a second time (Double-Dip in Z19).
    for det in prior_zufluss_details:
        yr = det['sell_year']
        cross_year_by_year[yr] = cross_year_by_year.get(yr, 0) + det['premium_eur']

    # --- PLAUSIBILITY: Raw Sums for Reconciliation ---
    # Use reportDate (booking date) for year assignment — Zuflussprinzip (§11 EStG)
    raw_div_base = sum(safe_float(f.get('amount')) for f in funds if f.get('activityCode') == 'DIV' and (d := parse_date(f.get('reportDate') or f.get('date'))) is not None and d.year == tax_year)
    raw_tax_base = sum(safe_float(f.get('amount')) for f in funds if (f.get('activityCode') in ['FRTAX', 'WHT'] or is_german_dividend_tax_row(f)) and (d := parse_date(f.get('reportDate') or f.get('date'))) is not None and d.year == tax_year)

    # 4. Dividends, Interest, and Withholding Tax
    dividends_eur = 0.0
    domestic_taxed_dividends_eur = 0.0
    interest_eur = 0.0  # Bond coupons, credit interest, Stückzinsen (abzugsfähig)
    debit_interest_eur = 0.0  # Margin-Sollzinsen, Leihgebühren (NICHT abzugsfähig, §20 Abs. 9 EStG)
    withholding_tax_eur = 0.0
    domestic_withholding_tax_eur = 0.0

    german_dividend_tax_keys = {
        funds_match_key(f) for f in funds
        if is_german_dividend_tax_row(f)
    }

    funds_processed = 0
    funds_skipped_year = 0

    def ensure_no_invstg_income(isin):
        if isin not in no_invstg_income_by_isin:
            info = get_etf_info(isin) or {}
            no_invstg_income_by_isin[isin] = {
                'ticker': info.get('ticker', isin[:12]),
                'name': info.get('name', ''),
                'div': 0.0,
                'wht': 0.0,
            }
        return no_invstg_income_by_isin[isin]

    for f in funds:
        code = f.get('activityCode')
        if not code and is_german_dividend_tax_row(f):
            code = 'FRTAX'
        # DIV = Dividends, PIL = Payment in Lieu (short dividends)
        # INTR = Bond Coupon/Interest, CINT = Credit Interest
        # INTP = Accrued Interest Paid (Stückzinsen)
        # DINT = Debit Interest (Margin-Sollzinsen, Leihgebühren, SYEP)
        # FRTAX/WHT = Withholding Tax
        if code not in ['DIV', 'PIL', 'INTR', 'CINT', 'INTP', 'DINT', 'FRTAX', 'WHT']:
            continue

        # Use reportDate (booking/settlement date) for tax year assignment
        # Zuflussprinzip (§11 EStG): taxed when received, not when the underlying event occurred
        # Example: Tax reclaim processed in 2025 for a 2024 dividend → belongs to 2025
        report_date = parse_date(f.get('reportDate') or f.get('date'))
        date = parse_date(f.get('date') or f.get('reportDate'))
        if not report_date or report_date.year != tax_year:
            funds_skipped_year += 1
            continue
            
        funds_processed += 1
            
        amount_raw = safe_float(f.get('amount'))
        curr = f.get('currency')

        if base_currency == 'EUR':
            # EUR base: StmtFunds shows BaseCurrency view — amounts already in EUR
            amount_eur = amount_raw
        else:
            # USD base: convert from original currency to EUR
            rate_eur = get_rate_for_date(date, usd_to_eur_rates)
            amount_eur = 0.0
            if curr == 'EUR':
                amount_eur = amount_raw
            elif curr == 'USD':
                amount_eur = amount_raw * rate_eur
            else:
                fx = safe_float(f.get('fxRateToBase'), 1.0)
                amount_usd = amount_raw * fx
                amount_eur = amount_usd * rate_eur
        
        # Check if this is an InvStG ETF dividend/WHT
        # Anlage-SO-ETFs (auch via Override, Issue #51) landen hier NICHT, denn
        # Ausschüttungen auf physische Edelmetall-ETCs sind nicht als InvStG-
        # Fondsausschüttungen zu behandeln — sie fließen in reguläre Dividenden.
        is_etf_fund = False
        is_no_invstg = False
        fund_isin = ''
        fund_cls = None
        _fund_isin_raw = f.get('isin', '').strip()
        if f.get('subCategory') == 'ETF' or (_fund_isin_raw and is_known_etf(_fund_isin_raw)):
            fund_isin = _fund_isin_raw
            if fund_isin:
                etf_isins.add(fund_isin)
                fund_cls = _effective_classification(fund_isin)
                is_no_invstg = fund_cls == 'no_invstg'
                if fund_cls not in ('no_invstg', 'anlage_so'):
                    is_etf_fund = True

        if code == 'DIV':
            if is_etf_fund:
                etf_dividends_eur += amount_eur
                entry = ensure_etf_fund_entry(fund_isin, fund_cls)
                add_etf_distribution(entry, amount_eur)
                event = get_etf_wht_event(fund_isin, date, curr)
                event['gross_distribution_eur'] += max(amount_eur, 0.0)
                add_etf_wht_source(event, f, report_date)
            elif is_de_isin(f) and funds_match_key(f) in german_dividend_tax_keys:
                domestic_taxed_dividends_eur += amount_eur
            else:
                dividends_eur += amount_eur
                if is_no_invstg:
                    ensure_no_invstg_income(fund_isin)['div'] += amount_eur
        elif code == 'PIL':
            # Payment in Lieu: positive = received (long position lent out)
            # negative = paid (short position owes dividend)
            # Net with dividends as per German tax law
            if is_etf_fund:
                etf_dividends_eur += amount_eur
                entry = ensure_etf_fund_entry(fund_isin, fund_cls)
                add_etf_distribution(entry, amount_eur)
                event = get_etf_wht_event(fund_isin, date, curr)
                event['gross_distribution_eur'] += max(amount_eur, 0.0)
                add_etf_wht_source(event, f, report_date)
            elif is_de_isin(f) and funds_match_key(f) in german_dividend_tax_keys:
                domestic_taxed_dividends_eur += amount_eur
            else:
                dividends_eur += amount_eur
                if is_no_invstg:
                    ensure_no_invstg_income(fund_isin)['div'] += amount_eur
        elif code == 'DINT':
            # Margin-Sollzinsen, Leihgebühren, SYEP — NICHT abzugsfähig (§20 Abs. 9 EStG)
            # Werbungskosten bei Kapitalerträgen → nur Sparer-Pauschbetrag erlaubt
            debit_interest_eur += amount_eur
        elif code in ['INTR', 'CINT', 'INTP']:
            # INTR = Bond Coupon/Interest, CINT = Credit Interest
            # INTP = Accrued interest paid (Stückzinsen — negative Einnahme, abzugsfähig)
            interest_eur += amount_eur
        elif code in ['FRTAX', 'WHT']:
            # IBKR: Einbehalt negativ, Erstattung positiv. Zuerst vorzeichenbehaftet
            # saldieren; die Berichtskonvention wird erst nach dem Loop angewendet.
            if is_german_dividend_tax_row(f) and not is_etf_fund:
                domestic_withholding_tax_eur += amount_eur
            elif is_etf_fund:
                etf_wht_eur += amount_eur
                entry = ensure_etf_fund_entry(fund_isin, fund_cls)
                entry['wht'] += amount_eur
                event = get_etf_wht_event(fund_isin, date, curr)
                if amount_eur < 0:
                    event['tax_withheld_eur'] += -amount_eur
                else:
                    event['tax_refunded_eur'] += amount_eur
                add_etf_wht_source(event, f, report_date)
            else:
                withholding_tax_eur += amount_eur
                if is_no_invstg:
                    ensure_no_invstg_income(fund_isin)['wht'] += amount_eur
            
    # Ausländische QSt: Vorzeichen invertieren, nicht absolut setzen. So bleibt
    # ein Erstattungsüberschuss im Bericht negativ statt zur Scheingutschrift zu werden.
    withholding_tax_eur = get_withholding_tax_for_reporting(withholding_tax_eur)
    # Inländische KESt/Soli ist ein separater bestehender Berechnungspfad.
    domestic_withholding_tax_eur = abs(domestic_withholding_tax_eur)
    zeile_37_kapitalertragsteuer_eur = (
        domestic_withholding_tax_eur
        * GERMAN_KEST_RATE
        / GERMAN_DIVIDEND_TAX_TOTAL_RATE
        if domestic_withholding_tax_eur else 0.0
    )
    zeile_38_solidaritaetszuschlag_eur = (
        domestic_withholding_tax_eur
        * GERMAN_SOLI_RATE
        / GERMAN_DIVIDEND_TAX_TOTAL_RATE
        if domestic_withholding_tax_eur else 0.0
    )
            
    # --- Fallback: Realized PnL from Summary ---
    # Use ISIN to identify already-processed instruments (trades.csv lacks 'symbol')
    # Only add summary PnL if trades.csv had ZERO PnL for that ISIN
    summary_path = os.path.join(ib_tax_dir, 'pnl_summary.csv')
    summary_rows = []  # initialise so top-5 block can reference it safely
    added_from_summary = 0
    if os.path.exists(summary_path):
        summary_rows = load_csv(summary_path)
        
        # Track PnL by ISIN from trades.csv (in base currency for correct comparison)
        pnl_by_isin = {}
        for t in trades:
            isin = t.get('isin', '').strip()
            if not isin:
                continue
            pnl_raw = safe_float(t.get('fifoPnlRealized'), 0)
            fx = safe_float(t.get('fxRateToBase'), 1.0)
            pnl_base = pnl_raw * fx
            pnl_by_isin[isin] = pnl_by_isin.get(isin, 0) + pnl_base

        # Build set of stock symbols/ISINs received via put assignment
        # Needed to skip phantom PnL entries in pnl_summary when the stock
        # BookTrade is absent from trades.csv (varies by Flex Query config)
        put_assign_syms = set()   # underlying ticker symbols
        put_assign_isins = set()  # underlying ISINs
        all_put_assigns = [a for a in opt_assignments if a.get('putCall') == 'P']
        all_put_assigns.extend(prior_put_assignments)
        for a in all_put_assigns:
            underlying = a.get('underlyingSymbol', '').strip()
            if not underlying:
                sym = a.get('symbol', '')
                if sym:
                    underlying = sym.split()[0]
            if underlying:
                put_assign_syms.add(underlying)
                if underlying in symbol_to_isin:
                    put_assign_isins.add(symbol_to_isin[underlying])
            uid = a.get('underlyingSecurityID', '').strip()
            if uid:
                put_assign_isins.add(uid)
            
        # FX rate for summary fallback (pnl_summary is "InBase" = base currency)
        if base_currency == 'EUR':
            default_fallback_rate = 1.0  # Already in EUR
        elif usd_to_eur_rates:
            last_date = sorted(usd_to_eur_rates.keys())[-1]
            default_fallback_rate = usd_to_eur_rates[last_date]
        else:
            raise RuntimeError(
                "PnL-Summary-Fallback: USD-Base ohne Wechselkurse — "
                "diese Bedingung sollte durch die Eingangs-Validierung in calculate_tax abgefangen sein."
            )

        added_from_summary = 0
        for s_row in summary_rows:
            isin = s_row.get('isin', '').strip()
            asset = s_row.get('assetCategory')
            
            # Skip if ISIN is empty (can't match)
            if not isin:
                continue
            
            # Get PnL from summary — include both ST and LT (German tax makes no distinction)
            summary_gain_usd = (float(s_row.get('realizedSTProfit', 0) or 0) +
                                float(s_row.get('realizedLTProfit', 0) or 0))
            summary_loss_usd = (float(s_row.get('realizedSTLoss', 0) or 0) +
                                float(s_row.get('realizedLTLoss', 0) or 0))
            
            if summary_gain_usd == 0 and summary_loss_usd == 0:
                continue
            
            # Get what trades.csv already captured
            trade_pnl = pnl_by_isin.get(isin, 0)
            
            # For BILL and BOND: add the DIFFERENCE since maturity events 
            # don't appear in trades.csv but are in the summary
            if asset in ['BILL', 'BOND']:
                # Summary reports total; trades may have partial
                # Calculate net gain/loss from summary
                summary_net = summary_gain_usd + summary_loss_usd
                # Difference = what we haven't captured yet
                diff_usd = summary_net - trade_pnl
                if abs(diff_usd) > 0.01:
                    diff_eur = diff_usd * default_fallback_rate
                    if diff_eur > 0:
                        options_gain += diff_eur
                    else:
                        options_loss += diff_eur
                    add_topf2_detail(TOPF2_CAT_LABELS.get(asset, asset), diff_eur)
                    added_from_summary += 1
                    debug_rows.append({
                        'dateTime': '', 'reportDate': '',
                        'symbol': s_row.get('symbol', ''),
                        'description': s_row.get('description', ''),
                        'isin': isin,
                        'assetCategory': asset,
                        'subCategory': s_row.get('subCategory', ''),
                        'buySell': '', 'quantity': '',
                        'transactionType': '',
                        'currency': base_currency,
                        'tradePrice': 0, 'cost': 0, 'proceeds': 0,
                        'fifoPnlRealized': diff_usd,
                        'fxRateToBase': default_fallback_rate if base_currency != 'EUR' else 1.0,
                        'pnl_eur': round(diff_eur, 5),
                        'topf': 'Topf2',
                        'strike': '', 'expiry': '', 'putCall': '', 'multiplier': '',
                        'underlyingSymbol': s_row.get('symbol', '').split()[0] if s_row.get('symbol') else '',
                        'source': 'pnl_summary',
                    })
            else:
                # For STK and OPT: skip if ISIN appears in trades.csv at all
                # (even with PnL=0, e.g. assignment BookTrades — those are correctly
                # handled by the main trades loop; using pnl_summary here would
                # double-count or add phantom gains/losses)
                if isin in pnl_by_isin:
                    continue

                # Also skip phantom PnL for stocks received only via put assignment.
                # Some Flex Query configs omit the stock BookTrade from trades.csv,
                # but pnl_summary still shows a phantom realized loss (IBKR data quirk).
                if asset == 'STK':
                    summary_sym = s_row.get('symbol', '').strip()
                    if summary_sym in put_assign_syms or isin in put_assign_isins:
                        continue
                    
                gain_eur = summary_gain_usd * default_fallback_rate
                loss_eur = summary_loss_usd * default_fallback_rate
                summary_topf = 'Topf2'  # default

                if asset == 'STK':
                    sub_cat = s_row.get('subCategory', '')
                    if sub_cat == 'ETF' or (isin and is_known_etf(isin)):
                        cls = _effective_classification(isin)
                        if cls == 'anlage_so':
                            # Physical Gold-ETC → §23 EStG, not KAP
                            summary_topf = 'Anlage SO'
                            info = get_etf_info(isin)
                            total_pnl = gain_eur + loss_eur
                            anlage_so_trades.append({
                                'isin': isin,
                                'ticker': info['ticker'] if info else isin[:12],
                                'name': info['name'] if info else '',
                                'pnl_eur': total_pnl,
                                'quantity': 0,
                                'dateTime': '',
                                'reportDate': '',
                                'buySell': '',
                            })
                        elif cls not in ('no_invstg', None):
                            summary_topf = 'KAP-INV'
                            etf_invstg_gain += gain_eur
                            etf_invstg_loss += loss_eur
                            ensure_etf_fund_entry(isin, cls)
                            etf_by_isin[isin]['gain'] += gain_eur
                            etf_by_isin[isin]['loss'] += loss_eur
                        else:
                            # no_invstg ETPs (Crypto, Commodities) → Topf 2
                            options_gain += gain_eur
                            options_loss += loss_eur
                            no_invstg_gain += gain_eur
                            no_invstg_loss += loss_eur
                            add_topf2_detail('Crypto/Commodity ETPs', gain_eur)
                            add_topf2_detail('Crypto/Commodity ETPs', loss_eur)
                    else:
                        summary_topf = 'Topf1'
                        stocks_gain += gain_eur
                        stocks_loss += loss_eur
                elif asset in ['OPT', 'FUT', 'FOP', 'FSFOP']:
                    options_gain += gain_eur
                    options_loss += loss_eur
                    add_topf2_detail(TOPF2_CAT_LABELS.get(asset, asset), gain_eur)
                    add_topf2_detail(TOPF2_CAT_LABELS.get(asset, asset), loss_eur)
                added_from_summary += 1
                net_eur = gain_eur + loss_eur
                debug_rows.append({
                    'dateTime': '', 'reportDate': '',
                    'symbol': s_row.get('symbol', ''),
                    'description': s_row.get('description', ''),
                    'isin': isin,
                    'assetCategory': asset,
                    'subCategory': s_row.get('subCategory', ''),
                    'buySell': '', 'quantity': '',
                    'transactionType': '',
                    'currency': base_currency,
                    'tradePrice': 0, 'cost': 0, 'proceeds': 0,
                    'fifoPnlRealized': summary_gain_usd + summary_loss_usd,
                    'fxRateToBase': default_fallback_rate if base_currency != 'EUR' else 1.0,
                    'pnl_eur': round(net_eur, 5),
                    'topf': summary_topf,
                    'strike': '', 'expiry': '', 'putCall': '', 'multiplier': '',
                    'underlyingSymbol': s_row.get('symbol', '').split()[0] if s_row.get('symbol') else '',
                    'source': 'pnl_summary',
                })
        
        if added_from_summary > 0:
            print(f"Added {added_from_summary} instruments from PnL Summary fallback (ISIN-based).")

    # --- Fremdwährungs-Gewinne/Verluste ---
    fx_results = {}
    fx_total_gain = 0.0
    fx_total_loss = 0.0
    fx_has_prior_data = True
    fx_source = 'none'  # 'csv', 'fifo', or 'none'
    csv_category_totals = {}  # plausibility data from CSV report
    csv_income_totals = {}  # dividends/interest/withholding tax from CSV report

    # Parse IBKR standard CSV report (always for plausibility check)
    if fx_csv_path and os.path.exists(fx_csv_path):
        csv_data = parse_ibkr_csv_report(fx_csv_path)
        csv_category_totals = csv_data['category_totals']
        csv_income_totals = csv_data.get('income_totals', {})

    # --- Saldo-Timeline aus fx_transactions.csv (Margin-Korrektur, Issue #59) ---
    # Wird sowohl für Option A (Filter) als auch für Option B (Entwertung) gebraucht.
    fx_tx_path = os.path.join(ib_tax_dir, 'fx_transactions.csv')
    fx_balance_timeline = defaultdict(list)  # curr -> [(date, txid, amount, prev_balance, after_balance)]
    fx_has_negative_balance = False
    _curr_sbs = {}
    _curr_sb_dates = {}
    if os.path.exists(fx_tx_path):
        _fx_tx_for_timeline = load_csv(fx_tx_path)
        _curr_events = defaultdict(list)
        for _tx in _fx_tx_for_timeline:
            _curr = _tx.get('currency', '')
            if not _curr:
                continue
            _desc = _tx.get('activityDescription', '')
            if _desc == 'Starting Balance':
                _curr_sbs[_curr] = safe_float(_tx.get('balance'), 0)
                _curr_sb_dates[_curr] = _tx.get('date', '')
                continue
            if _desc == 'Ending Balance':
                continue
            _amt = safe_float(_tx.get('amount'), 0)
            if abs(_amt) < 0.001:
                continue
            _d = _tx.get('date', '')
            _txid = _tx.get('transactionID', '')
            _curr_events[_curr].append((_d, _txid, _amt))
        for _curr, _evs in _curr_events.items():
            _evs.sort(key=lambda x: _fx_event_sort_key(x[0], x[1]))
            _bal = float(_curr_sbs.get(_curr, 0.0))
            for _d, _txid, _amt in _evs:
                _prev = _bal
                _bal += _amt
                fx_balance_timeline[_curr].append((_d, _txid, _amt, _prev, _bal))
            if _negative_days_from_balance_timeline(
                    fx_balance_timeline[_curr],
                    _curr_sb_dates.get(_curr, ''),
                    _curr_sbs.get(_curr, 0.0),
                    tax_year):
                fx_has_negative_balance = True

    def _lookup_balance_before_event(curr, target_date_short, target_qty, consumed_per_curr):
        """Findet balance VOR einem Event in fx_realized_pnl.csv.

        Match-Strategie (greedy mit per-Currency-consumed-Set):
          1. Same date + same sign + |amount| ≈ |target_qty| → exakter prev-balance.
          2. Kein Event am Target-Tag → Saldo nach letztem Event davor (EXAKT, weil
             keine Events dazwischen).
          3. Same-Day-Fallback: Events am Tag vorhanden, kein exakter |amount|-Match.
             Hier ist der Tagesanfangs-Saldo (`first_prev`) nur asymmetrisch
             belastbar:
             - `first_prev ≤ 0` und alle Same-Sign-Outflows → unser tatsächlicher
               prev bleibt für jeden Same-Sign-Outflow zwischenzeitlich ≤ 0
               (jeder Outflow drückt nur weiter ins Minus). scale=0 ist sicher.
             - `first_prev > 0` → der Tagesanfangs-Saldo ist eine OBERE Schranke
               für den echten prev. Bei mehreren same-day-Outflows hat der echte
               prev bereits Cash verbraucht, sodass `first_prev/|qty|` (partial)
               systematisch zu großzügig wäre (besteuert mehr als nötig).
               Codex-Hinweis 2026-05-27: prev_is_exact=False, IBKR-Rohwert behalten.
             - Mixed-Sign-Tag → zwischenzeitliche Inflows könnten den Saldo
               hochgezogen haben → in jedem Fall unsicher.

        P2-2: consumed_per_curr ist dict[currency] → set[idx], damit Indices verschiedener
        Currencies nicht kollidieren.
        P2-3: target_qty trägt das Vorzeichen; gematched wird nur, wenn amt dasselbe
        Vorzeichen hat — verhindert falsches Matching eines gleichgroßen Inflows auf
        einen gesuchten Outflow.
        Markiert gefundene Events als consumed.

        Returns (prev_balance, matched_event_amount, prev_is_exact):
            prev_is_exact=True → prev_balance darf ohne Vorbehalt für die scale-Logik
                                 genutzt werden (exakter Match, No-Event-Tag oder
                                 Same-Sign-Tag mit first_prev ≤ 0).
            prev_is_exact=False → prev_balance ist eine Approximation; der Aufrufer
                                  sollte nur den Counter bumpen, nicht skalieren.
        """
        timeline = fx_balance_timeline.get(curr, [])
        if not timeline:
            return None, None, False
        consumed = consumed_per_curr.setdefault(curr, set())
        target_abs = abs(target_qty)
        target_sign = 1 if target_qty > 0 else -1

        # 1) Exakter Match auf (date, same-sign, |amount|)
        for idx, (d, txid, amt, prev, after) in enumerate(timeline):
            if idx in consumed:
                continue
            if d != target_date_short:
                continue
            if amt * target_sign <= 0:  # opposite sign oder Null → kein Match
                continue
            if abs(abs(amt) - target_abs) < 0.01:
                consumed.add(idx)
                return prev, amt, True

        # Sammle alle Events am Target-Tag (für Fall 3 oder Fall 2)
        same_day_events = [ev for ev in timeline if ev[0] == target_date_short]

        if same_day_events:
            # 3) Same-Day-Fallback: erster prev des Tages
            first_prev = same_day_events[0][3]
            all_same_sign = all(ev[2] * target_sign > 0 for ev in same_day_events)
            # Nur sicher, wenn der Tagesanfangs-Saldo bereits ≤ 0 ist (sichere untere
            # Schranke für skipped_full). Bei first_prev > 0 würde same-day-Verbrauch
            # durch andere unmatched Outflows die scale-Logik verfälschen → approx.
            prev_is_exact = all_same_sign and first_prev <= 0
            return first_prev, None, prev_is_exact

        # 2) Kein Event am Target-Tag: Saldo NACH dem letzten Event davor ist exakt.
        prev_after = float(timeline[0][3])
        for d, txid, amt, prev, after in timeline:
            if d < target_date_short:
                prev_after = after
            else:
                break
        return prev_after, None, True

    # Option A: Exact FX from XML FxTransactions (IBKR's own FIFO, per-transaction realizedPL)
    # Mit Saldo-Korrektur (Issue #59): Abflüsse aus negativem Saldo erzeugen keinen
    # steuerbaren FX-PnL; teilweise gedeckte Abflüsse werden proportional gekürzt.
    fx_pnl_path = os.path.join(ib_tax_dir, 'fx_realized_pnl.csv')
    fx_option_a_meta = {}
    if not fx_results and os.path.exists(fx_pnl_path):
        fx_pnl_rows = load_csv(fx_pnl_path)
        fx_by_curr = {}
        approx_matches = 0  # Events ohne sicheren prev_balance (IBKR-Rohwert behalten)
        skipped_full = 0    # Events aus negativem Saldo (kein PnL)
        partial_count = 0   # Events mit proportionaler Kürzung
        # Pre-resolve: 4 strukturelle Match-Passes über fx_realized_pnl ↔ fx_transactions.
        # Liefert dict[row_idx] = {prev_balance, aggregat_qty, match_type}.
        # Match-Typen: exact (Pass 1, 1:1 amount-Match) | aggregat (Pass 2, FIFO-Splits
        # einer Cash-Buchung) | symbol_aggregat (Pass 4, mehrere Splits eines STK-Trades).
        # Das consumed-Dict trägt die im Pre-Resolve bereits verbrauchten Cash-Events
        # in den Fallback-Matcher hinein — Codex-Fix 2026-05-27: ohne diesen Transfer
        # könnte der Fallback denselben Cash-Event ein zweites Mal exact-matchen
        # (z.B. wenn fx_realized_pnl unabhängig vom Pre-Resolve eine duplizierte
        # quantity hat, die zufällig auf eine bereits konsumierte Cash-Buchung passt).
        fx_resolved, consumed_timeline_idx = _resolve_fx_outflows(
            fx_pnl_rows, fx_balance_timeline, tax_year)
        # Sets aus Pre-Resolve mutierbar machen, damit der Fallback weiter konsumieren kann
        consumed_timeline_idx = {curr: set(idxs) for curr, idxs in consumed_timeline_idx.items()}
        resolved_counts = {'exact': 0, 'aggregat': 0, 'symbol_aggregat': 0}
        for row_idx, row in enumerate(fx_pnl_rows):
            rd = parse_date(row.get('reportDate'))
            if not rd or rd.year != tax_year:
                continue
            curr = row.get('fxCurrency', '')
            pnl_raw = safe_float(row.get('realizedPL'), 0)
            qty = safe_float(row.get('quantity'), 0)
            if not curr or abs(pnl_raw) < 0.001:
                continue

            # Saldo-Korrektur nur bei Abflüssen (quantity < 0) anwenden.
            # Zuflüsse erzeugen kein realizedPL > 0 in IBKRs FIFO (Lots werden gebildet, nicht aufgelöst).
            #
            # Match-Strategie:
            #  1. Pre-Resolve-Dict (strukturelle Matches Pass 1-4) liefert präzisen prev_balance.
            #     Bei Aggregat-Match teilen mehrere Rows einen prev_balance und nutzen die
            #     aggregierte qty-Summe als Referenz für die scale-Logik (nicht die Einzel-qty).
            #  2. Fallback _lookup_balance_before_event deckt No-Event-am-Tag und Same-Sign-
            #     Tag mit first_prev ≤ 0 ab (sichere konservative Approximation).
            #  3. Sonst: approx_matches, IBKR-Rohwert behalten.
            scale = 1.0
            if qty < 0 and fx_balance_timeline.get(curr):
                resolution = fx_resolved.get(row_idx)
                if resolution is not None:
                    prev_bal = resolution['prev_balance']
                    aggregat_qty = resolution['aggregat_qty']
                    resolved_counts[resolution['match_type']] = resolved_counts.get(
                        resolution['match_type'], 0) + 1
                    if prev_bal <= 0:
                        scale = 0.0
                        skipped_full += 1
                    elif prev_bal < abs(aggregat_qty):
                        # Proportionale Kürzung auf Basis der Aggregat-Summe: alle Rows
                        # der Gruppe teilen denselben gedeckten Anteil.
                        scale = prev_bal / abs(aggregat_qty)
                        partial_count += 1
                else:
                    # Fallback für ungelöste Rows: alte heuristische Logik (No-Event-Tag,
                    # Same-Sign-Day mit first_prev ≤ 0). P2-3: qty mit Vorzeichen.
                    prev_bal, _matched_amt, prev_is_exact = _lookup_balance_before_event(
                        curr, row.get('reportDate', '')[:10], qty, consumed_timeline_idx)
                    if not prev_is_exact or prev_bal is None:
                        approx_matches += 1
                    elif prev_bal <= 0:
                        scale = 0.0
                        skipped_full += 1
                    elif prev_bal < abs(qty):
                        scale = prev_bal / abs(qty)
                        partial_count += 1

            pnl_corrected_raw = pnl_raw * scale

            # EUR base: realizedPL already in EUR; USD base: realizedPL in USD → convert
            if base_currency == 'EUR':
                pnl = pnl_corrected_raw
                pnl_raw_eur = pnl_raw
            else:
                rate_eur = get_rate_for_date(rd, usd_to_eur_rates)
                pnl = pnl_corrected_raw * rate_eur
                pnl_raw_eur = pnl_raw * rate_eur
            if curr not in fx_by_curr:
                fx_by_curr[curr] = {'gain': 0, 'loss': 0, 'net': 0, 'lots_remaining': 0, 'disposals_count': 0,
                                    'raw_gain': 0.0, 'raw_loss': 0.0, 'raw_net': 0.0,
                                    'raw_disposals_count': 0, 'days_negative': 0,
                                    'final_balance': 0.0, 'starting_balance': 0.0}
            if pnl > 0:
                fx_by_curr[curr]['gain'] += pnl
            elif pnl < 0:
                fx_by_curr[curr]['loss'] += pnl
            fx_by_curr[curr]['net'] += pnl
            if abs(pnl) > 0.001:
                fx_by_curr[curr]['disposals_count'] += 1
            # Raw-Werte (ungefiltert) für Vergleich
            if pnl_raw_eur > 0:
                fx_by_curr[curr]['raw_gain'] += pnl_raw_eur
            else:
                fx_by_curr[curr]['raw_loss'] += pnl_raw_eur
            fx_by_curr[curr]['raw_net'] += pnl_raw_eur
            fx_by_curr[curr]['raw_disposals_count'] += 1

        # Negative-Tage-Counter pro Währung aus Timeline ableiten. Currencies mit
        # Margin-Phasen, aber ohne eigene PnL-Zeile, bleiben so in der UI sichtbar.
        for curr in set(fx_by_curr.keys()) | set(fx_balance_timeline.keys()):
            final_bal = 0.0
            for d, txid, amt, prev, after in fx_balance_timeline.get(curr, []):
                final_bal = after
            neg_days = _negative_days_from_balance_timeline(
                fx_balance_timeline.get(curr, []),
                _curr_sb_dates.get(curr, ''),
                _curr_sbs.get(curr, 0.0),
                tax_year
            )
            if curr not in fx_by_curr and neg_days:
                fx_by_curr[curr] = {'gain': 0, 'loss': 0, 'net': 0,
                                    'lots_remaining': 0, 'disposals_count': 0,
                                    'raw_gain': 0.0, 'raw_loss': 0.0, 'raw_net': 0.0,
                                    'raw_disposals_count': 0, 'days_negative': 0,
                                    'final_balance': 0.0, 'starting_balance': _curr_sbs.get(curr, 0.0)}
            if curr not in fx_by_curr:
                continue
            fx_by_curr[curr]['days_negative'] = len(neg_days)
            fx_by_curr[curr]['final_balance'] = final_bal
            fx_by_curr[curr]['starting_balance'] = _curr_sbs.get(curr, 0.0)

        for data in fx_by_curr.values():
            data['corrected_gain'] = data.get('gain', 0.0)
            data['corrected_loss'] = data.get('loss', 0.0)
            data['corrected_net'] = data.get('net', 0.0)
            data['corrected_disposals_count'] = data.get('disposals_count', 0)
            if not fx_margin_correction_enabled:
                data['gain'] = data.get('raw_gain', data.get('gain', 0.0))
                data['loss'] = data.get('raw_loss', data.get('loss', 0.0))
                data['net'] = data.get('raw_net', data.get('net', 0.0))
                data['disposals_count'] = data.get('raw_disposals_count', data.get('disposals_count', 0))

        if fx_by_curr:
            fx_results = fx_by_curr
            fx_total_gain = sum(d['gain'] for d in fx_by_curr.values())
            fx_total_loss = sum(d['loss'] for d in fx_by_curr.values())
            fx_source = 'xml'
            fx_option_a_meta = {
                'approx_matches': approx_matches,
                'skipped_full': skipped_full,
                'partial_count': partial_count,
                'has_negative_balance': fx_has_negative_balance,
                'correction_enabled': fx_margin_correction_enabled,
                'corrected_total': sum(d.get('corrected_net', d.get('net', 0.0)) for d in fx_by_curr.values()),
                'raw_total': sum(d.get('raw_net', d.get('net', 0.0)) for d in fx_by_curr.values()),
                # Pass-Counter aus der strukturellen Pre-Resolve-Engine (Pass 1-4):
                'resolve_exact': resolved_counts.get('exact', 0),
                'resolve_aggregat': resolved_counts.get('aggregat', 0),
                'resolve_symbol_aggregat': resolved_counts.get('symbol_aggregat', 0),
            }
            fx_label = 'USD' if base_currency == 'USD' else '/'.join(fx_by_curr.keys())
            print(f"FX: Exakte Werte aus XML FxTransactions übernommen ({len(fx_pnl_rows)} Einträge).")
            if (skipped_full or partial_count) and fx_margin_correction_enabled:
                print(f"  Saldo-Korrektur aktiv: {skipped_full} Events aus Schuld (kein PnL), "
                      f"{partial_count} proportional gekürzt, {approx_matches} approximative Matches.")
            elif (skipped_full or partial_count) and not fx_margin_correction_enabled:
                print(f"  Saldo-Korrektur deaktiviert: IBKR-Rohwerte übernommen "
                      f"({skipped_full} Events aus Schuld, {partial_count} proportional wären betroffen).")
            if base_currency == 'USD':
                print(f"  USD-Konto: FX-Gewinne/-Verluste aus EUR-Transaktionen (IBKR trackt EUR als Fremdwährung).")

    # Option B: Exact FX from IBKR CSV report (same data as XML FxTransactions)
    # Achtung: Aggregierter Wert ohne Saldo-Differenzierung. Bei negativer Balance
    # kann er nicht saldogetreu korrigiert werden. Standard: Option B ueberspringen
    # und Option C nutzen. Opt-out: CSV-Rohwert bewusst uebernehmen, aber die
    # Margin-Metadaten fuer die UI sichtbar halten.
    if not fx_results and fx_csv_path and os.path.exists(fx_csv_path) and base_currency == 'EUR':
        if fx_has_negative_balance and fx_margin_correction_enabled:
            print(f"FX: IBKR-CSV-Bericht übersprungen — negativer Währungssaldo im Steuerjahr erkannt, "
                  f"Fallback auf FIFO mit Saldo-Korrektur (Issue #59).")
        else:
            fx_results = csv_data['fx_results']
            for curr, data in fx_results.items():
                data.setdefault('raw_gain', data.get('gain', 0.0))
                data.setdefault('raw_loss', data.get('loss', 0.0))
                data.setdefault('raw_net', data.get('net', 0.0))
                data.setdefault('raw_disposals_count', data.get('disposals_count', 0))
                data.setdefault('corrected_gain', data.get('gain', 0.0))
                data.setdefault('corrected_loss', data.get('loss', 0.0))
                data.setdefault('corrected_net', data.get('net', 0.0))
                data.setdefault('corrected_disposals_count', data.get('disposals_count', 0))
                if curr in fx_balance_timeline:
                    neg_days = _negative_days_from_balance_timeline(
                        fx_balance_timeline.get(curr, []),
                        _curr_sb_dates.get(curr, ''),
                        _curr_sbs.get(curr, 0.0),
                        tax_year
                    )
                    data['days_negative'] = len(neg_days)
            for curr, timeline in fx_balance_timeline.items():
                neg_days = _negative_days_from_balance_timeline(
                    timeline,
                    _curr_sb_dates.get(curr, ''),
                    _curr_sbs.get(curr, 0.0),
                    tax_year
                )
                if neg_days and curr not in fx_results:
                    fx_results[curr] = {
                        'gain': 0.0, 'loss': 0.0, 'net': 0.0,
                        'raw_gain': 0.0, 'raw_loss': 0.0, 'raw_net': 0.0,
                        'corrected_gain': 0.0, 'corrected_loss': 0.0, 'corrected_net': 0.0,
                        'lots_remaining': 0, 'disposals_count': 0,
                        'raw_disposals_count': 0, 'corrected_disposals_count': 0,
                        'days_negative': len(neg_days),
                        'final_balance': timeline[-1][4] if timeline else _curr_sbs.get(curr, 0.0),
                        'starting_balance': _curr_sbs.get(curr, 0.0),
                    }
            fx_total_gain = csv_data['fx_total_gain']
            fx_total_loss = csv_data['fx_total_loss']
            fx_source = 'csv'
            fx_option_a_meta = {
                'approx_matches': 0,
                'skipped_full': 0,
                'partial_count': 0,
                'has_negative_balance': fx_has_negative_balance,
                'correction_enabled': fx_margin_correction_enabled,
                'csv_raw_only': fx_has_negative_balance and not fx_margin_correction_enabled,
                'corrected_total': sum(d.get('corrected_net', d.get('net', 0.0)) for d in fx_results.values()),
                'raw_total': sum(d.get('raw_net', d.get('net', 0.0)) for d in fx_results.values()),
            }
            if fx_has_negative_balance and not fx_margin_correction_enabled:
                print(f"FX: IBKR-CSV-Rohwerte übernommen — Saldo-Korrektur ist deaktiviert.")
            else:
                print(f"FX: Exakte Werte aus IBKR Standard-Bericht übernommen.")

    # Option C: FIFO approximation from fx_transactions.csv (mit Saldo-Korrektur)
    fx_path = os.path.join(ib_tax_dir, 'fx_transactions.csv')
    if not fx_results and os.path.exists(fx_path) and base_currency == 'EUR':
        fx_transactions = load_csv(fx_path)
        fx_results, fx_total_gain, fx_total_loss, fx_has_prior_data = calculate_fx_gains(
            trades, fx_transactions, tax_year, base_currency
        )
        for data in fx_results.values():
            data['corrected_gain'] = data.get('gain', 0.0)
            data['corrected_loss'] = data.get('loss', 0.0)
            data['corrected_net'] = data.get('net', 0.0)
            data['corrected_disposals_count'] = data.get('disposals_count', 0)
            if not fx_margin_correction_enabled:
                data['gain'] = data.get('raw_gain', data.get('gain', 0.0))
                data['loss'] = data.get('raw_loss', data.get('loss', 0.0))
                data['net'] = data.get('raw_net', data.get('net', 0.0))
                data['disposals_count'] = data.get('raw_disposals_count', data.get('disposals_count', 0))
        if not fx_margin_correction_enabled:
            fx_total_gain = sum(d.get('gain', 0.0) for d in fx_results.values())
            fx_total_loss = sum(d.get('loss', 0.0) for d in fx_results.values())
        fx_option_a_meta = {
            'approx_matches': 0,
            'skipped_full': 0,
            'partial_count': 0,
            'has_negative_balance': fx_has_negative_balance,
            'correction_enabled': fx_margin_correction_enabled,
            'corrected_total': sum(d.get('corrected_net', d.get('net', 0.0)) for d in fx_results.values()),
            'raw_total': sum(d.get('raw_net', d.get('net', 0.0)) for d in fx_results.values()),
        }
        fx_source = 'fifo'

    if fx_results:
        # FX gains/losses go into Topf 2 (verzinsliches Fremdwährungsguthaben → §20 Abs. 2 S. 1 Nr. 7)
        options_gain += fx_total_gain
        options_loss += fx_total_loss
        if fx_total_gain > 0:
            add_topf2_detail('Devisen', fx_total_gain)
        if fx_total_loss < 0:
            add_topf2_detail('Devisen', fx_total_loss)
        print(f"FX Währungsgewinne: {fx_total_gain:,.2f} EUR, Währungsverluste: {fx_total_loss:,.2f} EUR")
        for curr, data in sorted(fx_results.items()):
            print(f"  {curr}: Gewinn {data['gain']:,.2f}, Verlust {data['loss']:,.2f}, Netto {data['net']:,.2f} EUR ({data['disposals_count']} Veräußerungen)")

    # Load MTM summary for plausibility comparison
    fx_mtm = {}
    fx_mtm_path = os.path.join(ib_tax_dir, 'fx_mtm_summary.csv')
    if os.path.exists(fx_mtm_path):
        for row in load_csv(fx_mtm_path):
            sym = row.get('symbol', '')
            total = float(row.get('total', 0) or 0)
            if sym:
                fx_mtm[sym] = total

    # Load IBKR's own fxTranslationGainLoss as reference
    fx_translation = 0.0
    fx_tgl_path = os.path.join(ib_tax_dir, 'fx_translation.csv')
    if os.path.exists(fx_tgl_path):
        tgl_rows = load_csv(fx_tgl_path)
        if tgl_rows:
            fx_translation = float(tgl_rows[0].get('fxTranslationGainLoss', 0) or 0)

    # --- Teilfreistellung (InvStG §20) ---
    # Apply partial exemption per ETF based on classification
    etf_gain_taxable = 0.0
    etf_loss_taxable = 0.0
    etf_div_taxable = 0.0
    etf_unknown_isins = []  # ISINs with subCategory=ETF but not in lookup table
    for isin in etf_isins:
        if get_classification(isin) is None and isin in etf_by_isin:
            etf_unknown_isins.append(isin)

    for (isin, _date_key, _currency), event in sorted(etf_wht_event_buckets.items()):
        ensure_etf_fund_entry(isin, _effective_classification(isin))['wht_events'].append(event)

    for isin, data in etf_by_isin.items():
        tfs_rate = get_teilfreistellung(isin)
        if tfs_rate is None:
            tfs_rate = 0.0
        data['tfs_rate'] = tfs_rate
        data['gain_taxable'] = data['gain'] * (1 - tfs_rate)
        data['loss_taxable'] = data['loss'] * (1 - tfs_rate)
        data['div_taxable'] = data['div'] * (1 - tfs_rate)
        etf_gain_taxable += data['gain_taxable']
        etf_loss_taxable += data['loss_taxable']
        etf_div_taxable += data['div_taxable']

    etf_wht_reported = get_withholding_tax_for_reporting(etf_wht_eur)
    etf_wht_calculation = calculate_kap_inv_wht_for_mode(
        etf_by_isin,
        dba_wht_beta_enabled=dba_wht_beta_enabled,
        treaty_rate_getter=get_foreign_tax_treaty_rate,
    )
    etf_wht_anrechenbar = etf_wht_calculation['creditable_tax_eur']
    etf_net_taxable = etf_gain_taxable + etf_loss_taxable + etf_div_taxable

    if etf_by_isin:
        tfs_reduction = (etf_invstg_gain + etf_invstg_loss + etf_dividends_eur) - etf_net_taxable
        print(f"InvStG ETFs: {len(etf_by_isin)} Fonds erkannt. "
              f"Gewinne {etf_invstg_gain:,.2f}, Verluste {etf_invstg_loss:,.2f}, "
              f"Dividenden {etf_dividends_eur:,.2f}, WHT {etf_wht_reported:,.2f} EUR. "
              f"Teilfreistellung: {tfs_reduction:,.2f} EUR Reduktion.")
    if etf_unknown_isins:
        print(f"  (!) {len(etf_unknown_isins)} ETF(s) nicht in Klassifizierungstabelle — als sonstiger Fonds (0% TFS) behandelt.")

    # --- Per-Lot FX Correction (CLOSED_LOT Tageskurs-Methode) ---
    # Compares IBKR method (net PnL × close rate) vs. correct method
    # (proceeds × close rate - cost × open rate) per FIFO lot.
    # Delta per lot = cost_trade_ccy × (fxRate_close - fxRate_open)
    # IBKR CLOSED_LOT: cost > 0 bei Longs (Kaufpreis), cost < 0 bei Shorts (Verkaufserlös)

    # Build lookup for Stillhalter put assignment cost corrections:
    # IBKR embeds the premium in the stock's cost basis (cost = strike - premium).
    # The Tageskurs formula needs the corrected cost (= strike), so we add the
    # premium back per share for stock CLOSED_LOTs acquired through put assignments.
    _tageskurs_put_adj = {}  # {underlying_symbol: deque of {date, shares_remaining, premium_per_share_raw}}
    for det in stillhalter_details:
        if det.get('putCall') != 'P':
            continue  # Only put assignments embed premium in stock COST basis
        underlying = det['symbol'].split()[0] if det['symbol'] else ''
        if not underlying:
            continue
        mult = det.get('multiplier', 100)
        shares = det['quantity'] * mult
        if shares <= 0 or det['premium_raw'] <= 0:
            continue
        a_date = (det.get('assignment_date') or '')[:10]
        _tageskurs_put_adj.setdefault(underlying, deque()).append({
            'date': a_date,
            'shares_remaining': shares,
            'premium_per_share_raw': det['premium_raw'] / shares,
        })
    # Include cross-year put assignments (assigned in prior years).
    # Issue #55: Premium-Werte werden aus _xy_tageskurs_lots gelesen (dort waehrend
    # der prior_put_assignments-Schleife unter FIFO-Logik gespeichert, siehe Issue
    # #54 Fix). Das eliminiert die fruehere parallele Berechnung mit dem identischen
    # Durchschnitts-Bug.
    for sym, snap_lots in _xy_tageskurs_lots.items():
        for snap in snap_lots:
            if snap['shares'] <= 0:
                continue
            _tageskurs_put_adj.setdefault(sym, deque()).append({
                'date': snap['date_str'],
                'shares_remaining': snap['shares'],
                'premium_per_share_raw': snap['premium_per_share_raw'],
            })
    # Sort each symbol's lots by date (FIFO)
    for sym in _tageskurs_put_adj:
        _tageskurs_put_adj[sym] = deque(sorted(_tageskurs_put_adj[sym], key=lambda x: x['date']))

    fx_correction_total = 0.0
    fx_correction_details = []
    fx_corr_by_topf = {'Topf1': 0.0, 'Topf2': 0.0, 'KAP-INV': 0.0}
    fx_correction_kap_inv_taxable = 0.0
    fx_correction_kap_inv_by_isin = {}
    # Per-Topf gain/loss adjustments for consistent Zeilen 20/22/23
    fx_corr_gain_adj = {'Topf1': 0.0, 'Topf2': 0.0, 'KAP-INV': 0.0}
    fx_corr_loss_adj = {'Topf1': 0.0, 'Topf2': 0.0, 'KAP-INV': 0.0}
    closed_lots_path = os.path.join(ib_tax_dir, 'closed_lots.csv')
    if os.path.exists(closed_lots_path):
        import bisect

        closed_lots = load_csv(closed_lots_path)

        # Load ConversionRate data (primary FX source for Tageskurs, Issue #33)
        conv_rate_map = {}
        cr_path = os.path.join(ib_tax_dir, 'conversion_rates.csv')
        if os.path.exists(cr_path):
            for cr in load_csv(cr_path):
                if cr.get('fromCurrency') == 'USD' and cr.get('toCurrency') == 'EUR':
                    rate = safe_float(cr.get('rate'), 0)
                    if rate > 0:
                        conv_rate_map[cr['reportDate']] = rate

        if base_currency == 'EUR':
            if conv_rate_map:
                # Primary: ConversionRate — IBKR's official daily rate (Issue #33)
                # Full daily coverage, no ExchTrade/BookTrade distinction needed.
                fx_map = dict(conv_rate_map)
            else:
                # Fallback: ExchTrade/BookTrade from trades (original logic)
                daily_exch = defaultdict(list)
                daily_book = defaultdict(list)
                for t in trades:
                    curr = t.get('currency', '')
                    fx = safe_float(t.get('fxRateToBase'), 0)
                    dt = (t.get('dateTime') or '')[:10]
                    if curr == 'USD' and fx > 0 and dt:
                        if t.get('transactionType') == 'BookTrade':
                            daily_book[dt].append(fx)
                        else:
                            daily_exch[dt].append(fx)
                fx_map = {}
                for d in set(daily_exch) | set(daily_book):
                    if d in daily_exch:
                        fx_map[d] = sum(daily_exch[d]) / len(daily_exch[d])
                    else:
                        fx_map[d] = sum(daily_book[d]) / len(daily_book[d])
        else:
            # USD base: usd_to_eur_rates as baseline, ConversionRate overwrites
            fx_map = {d.strftime('%Y-%m-%d'): r for d, r in usd_to_eur_rates.items()}
            if conv_rate_map:
                fx_map.update(conv_rate_map)

        fx_dates = sorted(fx_map.keys())
        if conv_rate_map:
            print(f"  Tageskurs FX-Quelle: ConversionRate ({len(conv_rate_map)} Tageskurse)")
        else:
            print(f"  Tageskurs FX-Quelle: ExchTrade/BookTrade Fallback ({len(fx_map)} Tageskurse)")

        def lookup_fx(date_str):
            day = date_str[:10] if date_str else ''
            if day in fx_map:
                return fx_map[day]
            if not fx_dates:
                return 0
            idx = bisect.bisect_left(fx_dates, day)
            if idx == 0:
                return fx_map[fx_dates[0]]
            if idx >= len(fx_dates):
                return fx_map[fx_dates[-1]]
            return fx_map[fx_dates[idx - 1]]

        lots_processed = 0

        for lot in closed_lots:
            if lot.get('currency') != 'USD':
                continue
            report_date = parse_date(lot.get('reportDate') or lot.get('dateTime'))
            if not report_date or report_date.year != tax_year:
                continue

            # Skip FUT — notional-based cost creates phantom FX gains
            # (futures settle via margin, not full notional exchange)
            category = lot.get('assetCategory', '')
            if category == 'FUT':
                continue

            # Skip assigned/exercised options (fifoPnlRealized ≈ 0):
            # - Short assignments (BUY): Premium already handled as Stillhalterprämie
            #   at option sell-date FX rate. Tageskurs correction would double-count.
            # - Long exercises (SELL): Cost bundled into stock's cost basis by IBKR.
            #   Tageskurs on the option lot is phantom.
            if category in ('OPT', 'FOP', 'FSFOP'):
                lot_pnl = abs(safe_float(lot.get('fifoPnlRealized'), 0))
                if lot_pnl < 0.01:
                    continue

            cost_raw = safe_float(lot.get('cost'), 0)

            # dateTime = actual trade date; reportDate = settlement/booking date.
            # Use trade date for FX lookup (§20 Abs. 4 S. 1 EStG: "Veräußerungszeitpunkt").
            # IBKR settles expiries/assignments on the next business day (e.g. Friday→Monday),
            # but the steuerlich relevant rate is the trade date rate.
            close_dt = (lot.get('dateTime') or lot.get('reportDate') or '')[:10]
            if base_currency == 'EUR' and not conv_rate_map:
                # Fallback: fxRateToBase on lot = USD→EUR rate at close
                fx_close = safe_float(lot.get('fxRateToBase'), 0)
            else:
                # ConversionRate (EUR-base) or usd_to_eur_rates+ConversionRate (USD-base)
                fx_close = lookup_fx(close_dt)

            open_dt = lot.get('openDateTime', '')
            fx_open = lookup_fx(open_dt)

            if fx_close <= 0 or fx_open <= 0:
                continue

            # For STK lots from put assignments: IBKR embeds premium in cost basis
            # (cost = strike×qty - premium). Restore correct cost (= strike×qty)
            # so the Tageskurs formula uses the right basis.
            if category == 'STK' and _tageskurs_put_adj:
                lot_sym = lot.get('symbol', '').split()[0]
                if lot_sym in _tageskurs_put_adj:
                    lot_open_date = open_dt[:10]
                    lot_qty = abs(safe_float(lot.get('quantity'), 0))
                    remaining = lot_qty
                    for adj_lot in _tageskurs_put_adj[lot_sym]:
                        if adj_lot['shares_remaining'] <= 0:
                            continue
                        if adj_lot['date'] and lot_open_date and adj_lot['date'] != lot_open_date:
                            continue
                        consumed = min(remaining, adj_lot['shares_remaining'])
                        cost_raw += consumed * adj_lot['premium_per_share_raw']
                        adj_lot['shares_remaining'] -= consumed
                        remaining -= consumed
                        if remaining <= 0:
                            break

            delta = cost_raw * (fx_close - fx_open)
            fx_correction_total += delta
            lots_processed += 1

            # Determine topf
            sub = lot.get('subCategory', '')
            isin = lot.get('isin', '').strip()
            kap_inv_tfs_rate = None
            kap_inv_classification = ''
            if category == 'STK' and isin and (sub == 'ETF' or is_known_etf(isin)):
                cls = _effective_classification(isin)
                if cls == 'anlage_so':
                    continue  # Gold-ETCs excluded from KAP entirely
                if cls not in ('no_invstg', None):
                    topf = 'KAP-INV'
                    kap_inv_tfs_rate = get_teilfreistellung(isin)
                    kap_inv_classification = cls
                else:
                    topf = 'Topf2'
            elif category == 'STK':
                topf = 'Topf1'
            else:
                topf = 'Topf2'
            fx_corr_by_topf[topf] += delta
            if topf == 'KAP-INV' and isin:
                tfs_rate = kap_inv_tfs_rate if kap_inv_tfs_rate is not None else get_teilfreistellung(isin)
                taxable_delta = delta * (1 - tfs_rate)
                fx_correction_kap_inv_taxable += taxable_delta
                info = get_etf_info(isin)
                if isin not in fx_correction_kap_inv_by_isin:
                    fx_correction_kap_inv_by_isin[isin] = {
                        'ticker': info['ticker'] if info else isin[:12],
                        'name': info['name'] if info else '',
                        'classification': kap_inv_classification or (info['classification'] if info else ''),
                        'tfs_rate': tfs_rate,
                        'raw_delta': 0.0,
                        'taxable_delta': 0.0,
                    }
                fx_correction_kap_inv_by_isin[isin]['raw_delta'] += delta
                fx_correction_kap_inv_by_isin[isin]['taxable_delta'] += taxable_delta

            detail = {
                'symbol': lot.get('symbol', ''),
                'description': lot.get('description', ''),
                'isin': isin,
                'assetCategory': category,
                'subCategory': sub,
                'openDateTime': open_dt,
                'reportDate': (lot.get('reportDate') or lot.get('dateTime') or '')[:10],
                'quantity': lot.get('quantity', ''),
                'cost': cost_raw,
                'currency': lot.get('currency', ''),
                'fx_open': fx_open,
                'fx_close': fx_close,
                'delta_eur': round(delta, 5),
                'topf': topf,
                'underlyingSymbol': lot.get('underlyingSymbol', ''),
            }
            if topf == 'KAP-INV':
                detail['tfs_rate'] = kap_inv_tfs_rate if kap_inv_tfs_rate is not None else get_teilfreistellung(isin)
                detail['taxable_delta_eur'] = round(delta * (1 - detail['tfs_rate']), 5)
            fx_correction_details.append(detail)

            # Track gain/loss shift per lot for consistent Zeilen 20/22/23
            pnl_raw = safe_float(lot.get('fifoPnlRealized'), 0)
            if base_currency == 'EUR':
                original_pnl = pnl_raw * fx_close
            else:
                original_pnl = pnl_raw * get_rate_for_date(report_date, usd_to_eur_rates)
            corrected_pnl = original_pnl + delta

            # How did gains/losses shift?
            orig_gain = max(original_pnl, 0)
            orig_loss = min(original_pnl, 0)
            corr_gain = max(corrected_pnl, 0)
            corr_loss = min(corrected_pnl, 0)
            fx_corr_gain_adj[topf] += corr_gain - orig_gain
            fx_corr_loss_adj[topf] += corr_loss - orig_loss

        if lots_processed > 0:
            print(f"\nTageskurs-Korrektur (CLOSED_LOT): {lots_processed} Lots analysiert.")
            print(f"  FX-Korrektur gesamt: {fx_correction_total:>+12,.2f} EUR")
            for topf, val in sorted(fx_corr_by_topf.items()):
                if abs(val) > 0.01:
                    print(f"    {topf}: {val:>+12,.2f} EUR")

    # --- Anlage SO: Holding period analysis for Gold-ETCs (§23 EStG) ---
    anlage_so_result = {
        'total_gain': 0.0,
        'total_loss': 0.0,
        'taxable_gain': 0.0,     # holding period <= 1 year
        'taxable_loss': 0.0,     # holding period <= 1 year
        'tax_free_gain': 0.0,    # holding period > 1 year
        'tax_free_loss': 0.0,    # holding period > 1 year
        'unknown_gain': 0.0,     # no lot data → conservatively taxable
        'unknown_loss': 0.0,
        'details': [],           # per-lot details
        'by_isin': {},           # per-ISIN summary
    }

    if anlage_so_trades:
        # Try CLOSED_LOT data first (has openDateTime for exact holding period)
        closed_lots_for_so = []
        if os.path.exists(os.path.join(ib_tax_dir, 'closed_lots.csv')):
            all_closed = load_csv(os.path.join(ib_tax_dir, 'closed_lots.csv'))
            so_isins = {t['isin'] for t in anlage_so_trades}
            closed_lots_for_so = [
                lot for lot in all_closed
                if lot.get('isin', '').strip() in so_isins
                and lot.get('assetCategory') == 'STK'
            ]

        if closed_lots_for_so:
            # Use CLOSED_LOT data for exact per-lot holding period
            _so_lot_corr_total = 0.0
            for lot in closed_lots_for_so:
                report_date = parse_date(lot.get('reportDate') or lot.get('dateTime'))
                if not report_date or report_date.year != tax_year:
                    continue

                isin = lot.get('isin', '').strip()
                open_dt = parse_date(lot.get('openDateTime', ''))
                close_dt = report_date

                pnl_raw = safe_float(lot.get('fifoPnlRealized'), 0)
                fx = safe_float(lot.get('fxRateToBase'), 1.0)
                if base_currency == 'EUR':
                    pnl_eur = pnl_raw * fx
                else:
                    rate = get_rate_for_date(close_dt, usd_to_eur_rates)
                    pnl_eur = pnl_raw * fx * rate

                qty = safe_float(lot.get('quantity'), 0)
                info = get_etf_info(isin)
                ticker = info['ticker'] if info else isin[:12]

                # Lot-Level Stillhalter-Korrektur für Anlage-SO-Override (Issue #51):
                # Wenn dieser Lot über ein Put-Assignment entstanden ist, die eingebettete
                # Prämie aus der PnL rausrechnen (sonst Double-Count — Prämie ist bereits
                # separat in Topf 2 gebucht).
                if open_dt and _so_premium_lookup:
                    lot_sym = ((lot.get('symbol') or '').strip().split() or [''])[0]
                    open_date_str = str(open_dt)[:10]
                    so_entry = _so_premium_lookup.get((lot_sym, open_date_str))
                    if so_entry and so_entry['shares'] > 0:
                        premium_for_lot = so_entry['premium_eur'] * abs(qty) / so_entry['shares']
                        pnl_eur -= premium_for_lot
                        _so_lot_corr_total += premium_for_lot

                if open_dt:
                    # §23 EStG: > 1 year holding = tax free
                    try:
                        one_year_later = open_dt.replace(year=open_dt.year + 1)
                    except ValueError:
                        # Feb 29 → Mar 1 fallback
                        one_year_later = open_dt.replace(year=open_dt.year + 1, day=28) + timedelta(days=1)
                    is_tax_free = close_dt > one_year_later
                else:
                    is_tax_free = False  # conservative: taxable if unknown

                detail = {
                    'isin': isin, 'ticker': ticker,
                    'open_date': str(open_dt) if open_dt else '?',
                    'close_date': str(close_dt),
                    'quantity': qty,
                    'pnl_eur': pnl_eur,
                    'is_tax_free': is_tax_free,
                }
                anlage_so_result['details'].append(detail)
                anlage_so_result['total_gain'] += max(pnl_eur, 0)
                anlage_so_result['total_loss'] += min(pnl_eur, 0)

                if is_tax_free:
                    anlage_so_result['tax_free_gain'] += max(pnl_eur, 0)
                    anlage_so_result['tax_free_loss'] += min(pnl_eur, 0)
                else:
                    anlage_so_result['taxable_gain'] += max(pnl_eur, 0)
                    anlage_so_result['taxable_loss'] += min(pnl_eur, 0)

                if isin not in anlage_so_result['by_isin']:
                    anlage_so_result['by_isin'][isin] = {
                        'ticker': ticker, 'name': info['name'] if info else '',
                        'taxable': 0.0, 'tax_free': 0.0, 'total': 0.0,
                    }
                anlage_so_result['by_isin'][isin]['total'] += pnl_eur
                if is_tax_free:
                    anlage_so_result['by_isin'][isin]['tax_free'] += pnl_eur
                else:
                    anlage_so_result['by_isin'][isin]['taxable'] += pnl_eur

            print(f"\nAnlage SO (§23 EStG): {len(anlage_so_result['details'])} Gold-ETC-Lots analysiert.")
            if _so_lot_corr_total > 0.01:
                print(f"  Stillhalter-Korrektur (Lot-Level): -{_so_lot_corr_total:,.2f} EUR (Prämie bereits in Topf 2).")
        else:
            # Fallback: own FIFO from trades for holding period
            # Build buy lots per ISIN from all trades (including history)
            so_isins = {t['isin'] for t in anlage_so_trades}
            buy_lots = defaultdict(list)  # isin -> list of (date, qty_remaining, qty_original)

            for t in trades:
                isin = t.get('isin', '').strip()
                if isin not in so_isins:
                    continue
                sub = t.get('subCategory', '')
                if sub != 'ETF':
                    continue
                qty = safe_float(t.get('quantity'), 0)
                buy_sell = t.get('buySell', '')
                dt = parse_date(t.get('dateTime') or t.get('tradeDate'))
                if not dt:
                    continue
                if buy_sell == 'BUY' and qty > 0:
                    buy_lots[isin].append({'date': dt, 'remaining': qty, 'original': qty})

            # Sort buy lots FIFO (oldest first)
            for isin in buy_lots:
                buy_lots[isin].sort(key=lambda x: x['date'])

            # Process sales (only tax-year) with FIFO matching
            for t in anlage_so_trades:
                isin = t['isin']
                pnl_eur = t['pnl_eur']
                sell_qty = abs(t['quantity'])
                sell_date = parse_date(t['reportDate'] or t['dateTime'])

                info = get_etf_info(isin)
                ticker = info['ticker'] if info else isin[:12]

                if isin not in anlage_so_result['by_isin']:
                    anlage_so_result['by_isin'][isin] = {
                        'ticker': ticker, 'name': info['name'] if info else '',
                        'taxable': 0.0, 'tax_free': 0.0, 'total': 0.0,
                    }

                anlage_so_result['total_gain'] += max(pnl_eur, 0)
                anlage_so_result['total_loss'] += min(pnl_eur, 0)
                anlage_so_result['by_isin'][isin]['total'] += pnl_eur

                lots = buy_lots.get(isin, [])
                if sell_qty > 0 and lots and sell_date:
                    # FIFO matching
                    remaining_sell = sell_qty
                    matched_tax_free = 0.0
                    matched_taxable = 0.0
                    for lot in lots:
                        if lot['remaining'] <= 0:
                            continue
                        match = min(lot['remaining'], remaining_sell)
                        try:
                            one_year_later = lot['date'].replace(year=lot['date'].year + 1)
                        except ValueError:
                            one_year_later = lot['date'].replace(year=lot['date'].year + 1, day=28)
                        if sell_date > one_year_later:
                            matched_tax_free += match
                        else:
                            matched_taxable += match
                        lot['remaining'] -= match
                        remaining_sell -= match
                        if remaining_sell <= 0:
                            break

                    total_matched = matched_tax_free + matched_taxable + remaining_sell
                    if total_matched > 0:
                        free_ratio = matched_tax_free / total_matched
                        taxable_ratio = 1.0 - free_ratio
                    else:
                        free_ratio = 0.0
                        taxable_ratio = 1.0

                    pnl_free = pnl_eur * free_ratio
                    pnl_taxable = pnl_eur * taxable_ratio

                    anlage_so_result['tax_free_gain'] += max(pnl_free, 0)
                    anlage_so_result['tax_free_loss'] += min(pnl_free, 0)
                    anlage_so_result['taxable_gain'] += max(pnl_taxable, 0)
                    anlage_so_result['taxable_loss'] += min(pnl_taxable, 0)
                    anlage_so_result['by_isin'][isin]['tax_free'] += pnl_free
                    anlage_so_result['by_isin'][isin]['taxable'] += pnl_taxable

                    detail = {
                        'isin': isin, 'ticker': ticker,
                        'open_date': 'FIFO',
                        'close_date': str(sell_date) if sell_date else '?',
                        'quantity': sell_qty,
                        'pnl_eur': pnl_eur,
                        'is_tax_free': free_ratio > 0.99,
                        'free_ratio': free_ratio,
                    }
                    anlage_so_result['details'].append(detail)
                else:
                    # No buy lots found → conservatively taxable
                    anlage_so_result['unknown_gain'] += max(pnl_eur, 0)
                    anlage_so_result['unknown_loss'] += min(pnl_eur, 0)
                    anlage_so_result['taxable_gain'] += max(pnl_eur, 0)
                    anlage_so_result['taxable_loss'] += min(pnl_eur, 0)
                    anlage_so_result['by_isin'][isin]['taxable'] += pnl_eur

            print(f"\nAnlage SO (§23 EStG): {len(anlage_so_trades)} Gold-ETC-Verkäufe, FIFO-Haltedauer berechnet.")

        so_taxable_net = anlage_so_result['taxable_gain'] + anlage_so_result['taxable_loss']
        so_free_net = anlage_so_result['tax_free_gain'] + anlage_so_result['tax_free_loss']
        print(f"  Steuerpflichtig (≤ 1 Jahr): {so_taxable_net:>+12,.2f} EUR")
        print(f"  Steuerfrei (> 1 Jahr):      {so_free_net:>+12,.2f} EUR")

    # Correct Anlage KAP Structure (2025):
    # Two separate "pots" (Töpfe) for loss offsetting:
    #
    # TOPF 1: Aktien (Stocks only)
    #   - Stock Gains - Stock Losses = Net Stocks
    #   - Stock losses can ONLY offset stock gains
    #
    # TOPF 2: Sonstiges (Everything else incl. Termingeschäfte from 2025)
    #   - Dividends + Interest + Option Gains - Option Losses = Net Sonstiges
    #
    # Zeile 19 = NET TOTAL (Topf 1 + Topf 2) - This is what gets taxed!
    # Zeile 20, 22, 23 are "Davon" (breakdown) lines
    
    # Calculate pools
    topf_1_aktien = stocks_gain + stocks_loss  # Net stocks (stocks_loss is negative)
    topf_2_sonstiges = dividends_eur + interest_eur + options_gain + options_loss  # Net sonstiges (options_loss is negative)
    
    # Zeile 19 = NET value (after loss offsetting)
    zeile_19_netto = topf_1_aktien + topf_2_sonstiges
    
    # Zeile 20 - "Davon: Aktiengewinne" (gross, for information)
    zeile_20_stock_gains = stocks_gain
    
    # Zeile 22 - "Verluste ohne Aktien" (absolute value, positive number for form)
    zeile_22_other_losses = abs(options_loss)

    # Zeile 23 - "Aktienverluste" (absolute value, positive number for form)
    zeile_23_stock_losses = abs(stocks_loss)

    # Sort trade details chronologically for reporting
    debug_rows.sort(key=lambda r: r.get('dateTime', '') or r.get('reportDate', '') or 'zzzz')

    # Alle je in diesem Report vorkommenden ETF-ISINs (unabhängig von Bucket) —
    # wird von der GUI für die Anlage-SO-Override-Auswahl gebraucht (Issue #51).
    all_traded_etf_isins = sorted(
        set(isin for isin in etf_isins if isin)
        | set(isin for isin in etf_by_isin.keys() if isin)
        | set(t.get('isin', '') for t in anlage_so_trades if t.get('isin'))
    )
    classification_review_items = []
    for isin in all_traded_etf_isins:
        if not requires_classification_review(isin):
            continue
        info = get_etf_info(isin) or {}
        classification_review_items.append({
            'isin': isin,
            'ticker': info.get('ticker', isin[:12]),
            'name': info.get('name', ''),
            'routing_classification': get_routing_classification(isin),
            'review_reason': info.get(
                'review_reason', 'Steuerliche Klassifikation nicht belegt.'
            ),
        })

    # Foreign tax on fund distributions is entered on Anlage KAP line 41,
    # not on KAP-INV. Keep the legacy non-fund field separate for reconciliation.
    zeile_41_withholding_tax_eur = withholding_tax_eur + etf_wht_anrechenbar
    kap_inv_form = build_kap_inv_form(
        etf_by_isin,
        fx_correction_kap_inv_by_isin,
        etf_unknown_isins,
        include_tageskurs=True,
    )
    kap_inv_form['kap_line_41_creditable_tax_eur'] = etf_wht_anrechenbar

    report_data = {
        "zeile_7_kapitalertraege_mit_inlaendischem_steuerabzug_eur": domestic_taxed_dividends_eur,
        "zeile_19_netto_eur": zeile_19_netto,
        "zeile_20_stock_gains_eur": zeile_20_stock_gains,
        "zeile_22_other_losses_eur": zeile_22_other_losses,
        "zeile_23_stock_losses_eur": zeile_23_stock_losses,
        "zeile_37_kapitalertragsteuer_eur": zeile_37_kapitalertragsteuer_eur,
        "zeile_38_solidaritaetszuschlag_eur": zeile_38_solidaritaetszuschlag_eur,
        "zeile_41_withholding_tax_eur": zeile_41_withholding_tax_eur,
        # Pool details
        "topf_1_aktien_netto": topf_1_aktien,
        "topf_2_sonstiges_netto": topf_2_sonstiges,
        "no_invstg_income_by_isin": no_invstg_income_by_isin,
        # Keep old keys for backward compatibility
        "dividends_eur": dividends_eur,
        "domestic_taxed_dividends_eur": domestic_taxed_dividends_eur,
        "interest_eur": interest_eur,
        "debit_interest_eur": debit_interest_eur,
        "stocks_gain_eur": stocks_gain,
        "stocks_loss_eur": stocks_loss,
        "stocks_net_eur": stocks_gain + stocks_loss,
        "options_gain_eur": options_gain,
        "options_loss_eur": options_loss,
        "options_net_eur": options_gain + options_loss,
        "topf2_by_category": topf2_by_category,
        "withholding_tax_eur": withholding_tax_eur,
        "domestic_withholding_tax_eur": domestic_withholding_tax_eur,
        "base_currency": base_currency,
        "tax_year": tax_year,
        # FX currency gains/losses
        "fx_results": fx_results,
        "fx_total_gain": fx_total_gain,
        "fx_total_loss": fx_total_loss,
        "fx_mtm": fx_mtm,
        "fx_translation": fx_translation,
        "fx_has_prior_data": fx_has_prior_data,
        "fx_source": fx_source,
        # Issue #59: Saldo-Korrektur-Metadaten (Margin-Schulden)
        "fx_option_a_meta": fx_option_a_meta,
        "fx_has_negative_balance": fx_has_negative_balance,
        "fx_margin_correction_enabled": fx_margin_correction_enabled,
        "dba_wht_beta_enabled": bool(dba_wht_beta_enabled),
        "xml_has_fx_data": xml_has_fx_data,
        "csv_category_totals": csv_category_totals,
        "csv_income_totals": csv_income_totals,
        # Per-lot FX correction (Tageskurs-Methode)
        "fx_correction_total": fx_correction_total,
        "fx_correction_by_topf": fx_corr_by_topf,
        "fx_correction_kap_inv_taxable": fx_correction_kap_inv_taxable,
        "fx_correction_kap_inv_by_isin": fx_correction_kap_inv_by_isin,
        "fx_correction_details": fx_correction_details,
        "fx_corr_gain_adj": fx_corr_gain_adj,
        "fx_corr_loss_adj": fx_corr_loss_adj,
        # InvStG / Anlage KAP-INV
        "kap_inv": {
            "etf_gain_raw_eur": etf_invstg_gain,
            "etf_loss_raw_eur": etf_invstg_loss,
            "etf_gain_taxable_eur": etf_gain_taxable,
            "etf_loss_taxable_eur": etf_loss_taxable,
            "etf_dividends_raw_eur": etf_dividends_eur,
            "etf_dividends_taxable_eur": etf_div_taxable,
            "etf_wht_eur": etf_wht_reported,
            "etf_wht_anrechenbar_eur": etf_wht_anrechenbar,
            "wht_events": etf_wht_calculation['events'],
            "wht_review_items": etf_wht_calculation['review_items'],
            "etf_net_taxable_eur": etf_net_taxable,
            "etf_by_isin": etf_by_isin,
            "etf_unknown_isins": etf_unknown_isins,
            "etf_stillhalter_premium_eur": etf_stillhalter_premium_eur,
        },
        "kap_inv_form": kap_inv_form,
        # Anlage SO (§23 EStG — physische Gold-ETCs)
        "anlage_so": anlage_so_result,
        # Alle ETF-ISINs, die im Report auftauchen (für GUI-Override-Auswahl)
        "all_traded_etf_isins": all_traded_etf_isins,
        "anlage_so_overrides_applied": sorted(anlage_so_overrides_set),
        "classification_review_items": classification_review_items,
        # Trade-level details for FA reporting (Issue #17)
        "trade_details": debug_rows,
        # Plausibility Metadata
        "has_trade_price": has_trade_price,
        "audit": {
            "funds_processed": funds_processed,
            "funds_skipped_year": funds_skipped_year,
            "raw_div_base": raw_div_base,
            "raw_tax_base": raw_tax_base,
            "added_from_summary": added_from_summary,
            "usd_to_eur_rates_count": len(usd_to_eur_rates),
            "ecb_rates_used": ecb_rates_used,
            "stillhalter_count": stillhalter_count,
            "stillhalter_premium_eur": stillhalter_premium_eur,
            "put_nosell_premium_eur": put_nosell_premium_eur,
            "stk_correction_cy": stk_gain_corr_cy + stk_loss_corr_cy,
            "etf_correction_cy": etf_gain_corr_cy + etf_loss_corr_cy,
            "stillhalter_unmatched": stillhalter_unmatched,
            "stillhalter_corrections_dropped": stillhalter_corrections_dropped,
            "stillhalter_open_short": stillhalter_open_short,
            "stillhalter_details": stillhalter_details,
            "cross_year_premium_eur": cross_year_premium_eur,
            "cross_year_by_year": cross_year_by_year,
            "cross_year_put_corrections": cross_year_put_corrections,
            "cross_year_put_total": cross_year_put_total,
            "no_invstg_gain": no_invstg_gain,
            "no_invstg_loss": no_invstg_loss,
            "zufluss_premium_eur": zufluss_premium_eur,
            "zufluss_count": zufluss_count,
            "zufluss_details": zufluss_details,
            "prior_zufluss_correction_eur": prior_zufluss_correction_eur,
            "prior_zufluss_details": prior_zufluss_details,
            "zufluss_unmatched": zufluss_unmatched,
            "occ_rename_matches": occ_rename_matches,
        }
    }

    print("\n" + "="*60)
    print(f"GERMAN TAX REPORT - ANLAGE KAP {tax_year}")
    print("="*60)
    print(f"Base Currency: {base_currency}")
    print("-" * 60)
    
    print("TOPF 1: AKTIEN (Separate Verrechnung)")
    print(f"    Aktiengewinne:         {stocks_gain:>12,.2f} EUR")
    print(f"    Aktienverluste:        {stocks_loss:>12,.2f} EUR")
    print(f"    ─────────────────────────────────────")
    print(f"    Saldo Aktien:          {topf_1_aktien:>12,.2f} EUR")
    
    print("-" * 60)
    print("TOPF 2: SONSTIGES (inkl. Termingeschäfte)")
    print(f"    Dividenden (netto):    {dividends_eur:>12,.2f} EUR")
    if domestic_taxed_dividends_eur > 0.01:
        print(f"    DE-Dividenden m. StAbz:{domestic_taxed_dividends_eur:>12,.2f} EUR  (separat Zeile 7)")
    print(f"    Zinsen:                {interest_eur:>12,.2f} EUR")
    if abs(debit_interest_eur) > 0.01:
        print(f"    Sollzinsen (n. abzf.): {debit_interest_eur:>12,.2f} EUR  (§20 Abs. 9 EStG, nicht in Berechnung)")
    if stillhalter_premium_eur > 0:
        print(f"    Stillhalterprämien:    {stillhalter_premium_eur:>12,.2f} EUR  ({stillhalter_count} Assignments)")
    print(f"    Sonstige Gewinne:      {options_gain:>12,.2f} EUR")
    print(f"    Sonstige Verluste:     {options_loss:>12,.2f} EUR")
    if topf2_by_category:
        print(f"      Aufschlüsselung:")
        for cat, vals in sorted(topf2_by_category.items()):
            net = vals['gain'] + vals['loss']
            print(f"        {cat:24s} G {vals['gain']:>10,.2f}  V {vals['loss']:>10,.2f}  N {net:>10,.2f}")
    print(f"    ─────────────────────────────────────")
    print(f"    Saldo Sonstiges:       {topf_2_sonstiges:>12,.2f} EUR")
    
    if fx_results:
        print("-" * 60)
        print("FREMDWÄHRUNGS-GEWINNE/VERLUSTE (FIFO, §20 Abs. 2 S. 1 Nr. 7)")
        for curr, data in sorted(fx_results.items()):
            mtm_val = fx_mtm.get(curr)
            mtm_info = f"  (MTM: {mtm_val:,.2f})" if mtm_val is not None else ""
            print(f"    {curr}: Gewinn {data['gain']:>10,.2f}  Verlust {data['loss']:>10,.2f}  Netto {data['net']:>10,.2f} EUR{mtm_info}")
        print(f"    ─────────────────────────────────────")
        print(f"    FX Gesamt Gewinn:      {fx_total_gain:>12,.2f} EUR")
        print(f"    FX Gesamt Verlust:     {fx_total_loss:>12,.2f} EUR")
        print(f"    FX Netto:              {fx_total_gain + fx_total_loss:>12,.2f} EUR")
        if fx_translation != 0:
            print(f"    IBKR Referenz (fxTranslationGainLoss): {fx_translation:>10,.2f} EUR")
        if not fx_has_prior_data:
            print(f"    (!) HINWEIS: Anfangsbestände zum 01.01.-Kurs angesetzt (Vereinfachung).")
            print(f"        Für exakte FIFO-Lots: Flex Query ab Kontoeröffnung laden.")
        else:
            print(f"    Multi-Year-Daten: FIFO-Lots vollständig ab Kontoeröffnung.")
        print(f"    (in Topf 2 enthalten)")

    if etf_by_isin:
        print("-" * 60)
        print("ANLAGE KAP-INV (InvStG Investmentfonds)")
        for isin, data in sorted(etf_by_isin.items(), key=lambda x: abs(x[1]['gain'] + x[1]['loss']), reverse=True):
            tfs_pct = int(data.get('tfs_rate', 0) * 100)
            net_raw = data['gain'] + data['loss']
            print(f"    {data['ticker']:6s} ({data['classification'][:12]:12s} {tfs_pct:2d}% TFS)  G/V: {net_raw:>10,.2f}  Div: {data['div']:>8,.2f}  WHT: {data['wht']:>8,.2f}")
        print(f"    ─────────────────────────────────────")
        print(f"    ETF-Gewinne (roh):     {etf_invstg_gain:>12,.2f} EUR")
        print(f"    ETF-Verluste (roh):    {etf_invstg_loss:>12,.2f} EUR")
        print(f"    ETF-Dividenden (roh):  {etf_dividends_eur:>12,.2f} EUR")
        tfs_reduction = (etf_invstg_gain + etf_invstg_loss + etf_dividends_eur) - etf_net_taxable
        if abs(tfs_reduction) > 0.01:
            print(f"    Teilfreistellung:      {-tfs_reduction:>12,.2f} EUR")
        print(f"    ETF-Netto (stpfl.):    {etf_net_taxable:>12,.2f} EUR")
        print(f"    ETF-QSt (roh):         {etf_wht_reported:>12,.2f} EUR")
        print(f"    ETF-QSt anrechenbar (Anlage KAP Z. 41): {etf_wht_anrechenbar:>12,.2f} EUR")

    if anlage_so_result['details'] or anlage_so_result['total_gain'] != 0 or anlage_so_result['total_loss'] != 0:
        print("-" * 60)
        print("ANLAGE SO (§23 EStG — Private Veräußerungsgeschäfte)")
        print("    Physische Gold-ETCs mit Lieferanspruch (BFH VIII R 4/15)")
        for isin, data in sorted(anlage_so_result['by_isin'].items(), key=lambda x: abs(x[1]['total']), reverse=True):
            print(f"    {data['ticker']:6s}  Gesamt: {data['total']:>10,.2f}  Stpfl.: {data['taxable']:>10,.2f}  Frei: {data['tax_free']:>10,.2f}")
        so_taxable = anlage_so_result['taxable_gain'] + anlage_so_result['taxable_loss']
        so_free = anlage_so_result['tax_free_gain'] + anlage_so_result['tax_free_loss']
        print(f"    ─────────────────────────────────────")
        print(f"    Steuerpflichtig (≤1J): {so_taxable:>12,.2f} EUR  → Anlage SO")
        print(f"    Steuerfrei (>1J):      {so_free:>12,.2f} EUR")
        print(f"    (NICHT auf Anlage KAP)")

    print("-" * 60)
    print("ZEILE 19 (Ausländische Kapitalerträge - NETTO):")
    print(f"    = Saldo Aktien + Saldo Sonstiges")
    print(f"    = {topf_1_aktien:,.2f} + {topf_2_sonstiges:,.2f}")
    print(f"    ═════════════════════════════════════")
    print(f"    ZEILE 19:              {zeile_19_netto:>12,.2f} EUR")
    if etf_by_isin:
        print(f"    KAP-INV (ETF netto):   {etf_net_taxable:>12,.2f} EUR")
    
    print("-" * 60)
    if domestic_taxed_dividends_eur > 0.01:
        print(f"ZEILE 7 (Kapitalerträge mit inländischem Steuerabzug): {domestic_taxed_dividends_eur:>12,.2f} EUR")
        print(f"ZEILE 37 (Kapitalertragsteuer):                       {zeile_37_kapitalertragsteuer_eur:>12,.2f} EUR")
        print(f"ZEILE 38 (Solidaritätszuschlag):                      {zeile_38_solidaritaetszuschlag_eur:>12,.2f} EUR")
    print(f"ZEILE 20 (Davon: Aktiengewinne):   {zeile_20_stock_gains:>12,.2f} EUR")
    print(f"ZEILE 22 (Verluste ohne Aktien):   {zeile_22_other_losses:>12,.2f} EUR")
    print(f"ZEILE 23 (Aktienverluste):         {zeile_23_stock_losses:>12,.2f} EUR")
    print(f"ZEILE 41 (ausländische Quellensteuer): {zeile_41_withholding_tax_eur:>12,.2f} EUR")

    if abs(fx_correction_total) > 0.01:
        corrected_z19 = zeile_19_netto + fx_correction_total
        print("-" * 60)
        print("TAGESKURS-VERGLEICH (Erlös/AK je zum eigenen Tageskurs)")
        print(f"    IBKR-Methode (Netto × Schlusskurs):  {zeile_19_netto:>12,.2f} EUR")
        print(f"    FX-Korrektur (CLOSED_LOT Analyse):   {fx_correction_total:>+12,.2f} EUR")
        print(f"    Tageskurs-Methode Zeile 19:          {corrected_z19:>12,.2f} EUR")
        print(f"    Differenz:                           {fx_correction_total:>+12,.2f} EUR ({fx_correction_total/max(abs(zeile_19_netto),1)*100:+.2f}%)")

    print("\n" + "="*60)
    print("PLAUSIBILITÄTSPRÜFUNG (AUDIT)")
    print("="*60)
    print(f"Verarbeitete Cash-Transaktionen:   {funds_processed}")
    print(f"Übersprungene Jahre (nicht {tax_year}):  {funds_skipped_year}")
    print(f"Instrumente aus PnL Summary:       {added_from_summary}")
    print(f"Gefundene Wechselkurse:            {len(usd_to_eur_rates)}")
    if ecb_rates_used:
        print(f"Kursquelle:                        IBKR + EZB-Referenzkurse")
    elif usd_to_eur_rates:
        print(f"Kursquelle:                        IBKR-Transaktionsdaten")

    # Check if exchange rates are in plausible range (roughly 0.9 - 1.0 for 2025)
    if usd_to_eur_rates:
        avg_rate = sum(usd_to_eur_rates.values()) / len(usd_to_eur_rates)
        print(f"Kursschnitt (USD/EUR):             {avg_rate:>12.4f}")
        if not (0.85 < avg_rate < 1.15):
            print("(!) WARNUNG: Wechselkurs-Schnitt ist ungewöhnlich.")

    # Recon check
    print(f"Roh-Summe Dividenden ({base_currency}):        {raw_div_base:>12.2f} {base_currency}")
    print(f"Roh-Summe Quellensteuer ({base_currency}):     {raw_tax_base:>12.2f} {base_currency}")
    
    print("="*60)
    
    return report_data

if __name__ == "__main__":
    if len(sys.argv) > 1:
        ib_tax_dir = sys.argv[1]
    else:
        ib_tax_dir = './'
        
    calculate_tax(ib_tax_dir)
