#!/usr/bin/env python3
"""Vergleich Tageskurs-Korrektur: ExchTrade/BookTrade (alt) vs ConversionRate (neu).

Nutzung:
  python compare_tageskurs.py "2024 XML.xml"
  python compare_tageskurs.py "2024 XML.xml" --history "2023 XML.xml"
"""

import argparse
import bisect
import os
import shutil
import sys
import tempfile
from collections import defaultdict

# Import project modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from extract_ibkr_data import parse_ibkr_xml, extract_fx_multi_xml
from calculate_tax_report import calculate_tax, load_csv, safe_float


def build_old_fx_map(trades):
    """Build fx_map using the OLD ExchTrade/BookTrade logic."""
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
    return fx_map


def make_lookup(fx_map):
    """Create a lookup function with bisect fallback (same as calculate_tax_report)."""
    fx_dates = sorted(fx_map.keys())
    def lookup(date_str):
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
    return lookup


def run_comparison(xml_path, history_path=None):
    print("=" * 70)
    label = "Nur Hauptjahr" if not history_path else "Mit History"
    print(f"  {label}: {os.path.basename(xml_path)}")
    if history_path:
        print(f"  History: {os.path.basename(history_path)}")
    print("=" * 70)

    # Extract to temp dir
    tmpdir = tempfile.mkdtemp(prefix='tageskurs_cmp_')
    try:
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
        if history_path:
            extract_fx_multi_xml([history_path, xml_path], tmpdir)
        else:
            parse_ibkr_xml(xml_path, tmpdir)

        # Run calculate_tax (NEW ConversionRate logic)
        report = calculate_tax(tmpdir)
        new_details = report.get('fx_correction_details', [])
        new_total = report.get('fx_correction_total', 0)

        if not new_details:
            print("\nKeine Tageskurs-Korrekturen vorhanden.")
            return

        # Build OLD fx_map from trades
        trades = load_csv(os.path.join(tmpdir, 'trades.csv'))
        old_fx_map = build_old_fx_map(trades)
        old_lookup = make_lookup(old_fx_map)

        # Build lot fxRateToBase index for old fx_close
        closed_lots = load_csv(os.path.join(tmpdir, 'closed_lots.csv'))
        lot_fx_index = {}
        for lot in closed_lots:
            key = (
                lot.get('symbol', ''),
                lot.get('openDateTime', ''),
                (lot.get('reportDate') or lot.get('dateTime') or '')[:10],
                safe_float(lot.get('cost'), 0),
            )
            lot_fx_index[key] = safe_float(lot.get('fxRateToBase'), 0)

        # Compare per lot
        old_total = 0.0
        old_by_topf = defaultdict(float)
        new_by_topf = defaultdict(float)
        diffs = []

        for d in new_details:
            new_delta = d['delta_eur']
            new_fx_open = d['fx_open']
            new_fx_close = d['fx_close']
            cost = d['cost']
            topf = d['topf']

            # Old fx_open: from old ExchTrade/BookTrade map
            old_fx_open = old_lookup(d['openDateTime'])

            # Old fx_close: from lot's fxRateToBase
            lot_key = (
                d['symbol'],
                d['openDateTime'],
                d['reportDate'],
                cost,
            )
            old_fx_close = lot_fx_index.get(lot_key, 0)
            if old_fx_close <= 0:
                # Fallback: try without cost matching
                for k, v in lot_fx_index.items():
                    if k[0] == d['symbol'] and k[1] == d['openDateTime'] and k[2] == d['reportDate']:
                        old_fx_close = v
                        break
            if old_fx_close <= 0:
                old_fx_close = new_fx_close  # Can't determine, assume same

            old_delta = cost * (old_fx_close - old_fx_open)
            old_total += old_delta
            old_by_topf[topf] += old_delta
            new_by_topf[topf] += new_delta

            diff = new_delta - old_delta
            if abs(diff) > 0.005:
                diffs.append({
                    'symbol': d['symbol'],
                    'open': d['openDateTime'][:10],
                    'close': d['reportDate'],
                    'cost': cost,
                    'old_open': old_fx_open,
                    'new_open': new_fx_open,
                    'old_close': old_fx_close,
                    'new_close': new_fx_close,
                    'old_delta': old_delta,
                    'new_delta': new_delta,
                    'diff': diff,
                    'topf': topf,
                })

        # Summary
        print(f"\n{'':=<70}")
        print(f"  ZUSAMMENFASSUNG")
        print(f"{'':=<70}")
        print(f"  Lots analysiert:  {len(new_details)}")
        print(f"  Lots mit Abweichung (>0.5ct): {len(diffs)}")
        print()
        print(f"  {'':30s} {'ALT (ExchTrade)':>16s} {'NEU (ConvRate)':>16s} {'Differenz':>12s}")
        print(f"  {'-'*74}")
        print(f"  {'GESAMT':30s} {old_total:>+16.2f} {new_total:>+16.2f} {new_total - old_total:>+12.2f}")
        for topf in sorted(set(list(old_by_topf.keys()) + list(new_by_topf.keys()))):
            ov = old_by_topf.get(topf, 0)
            nv = new_by_topf.get(topf, 0)
            print(f"  {topf:30s} {ov:>+16.2f} {nv:>+16.2f} {nv - ov:>+12.2f}")

        # Rate source coverage
        conv_rates = load_csv(os.path.join(tmpdir, 'conversion_rates.csv'))
        usd_eur_count = sum(1 for r in conv_rates
                           if r.get('fromCurrency') == 'USD' and r.get('toCurrency') == 'EUR')
        print(f"\n  ConversionRate USD→EUR Eintraege: {usd_eur_count}")
        print(f"  ExchTrade/BookTrade Tage:         {len(old_fx_map)}")

        # Detail table for significant diffs
        if diffs:
            diffs.sort(key=lambda x: abs(x['diff']), reverse=True)
            print(f"\n{'':=<70}")
            print(f"  EINZELABWEICHUNGEN (Top {min(30, len(diffs))} nach Betrag)")
            print(f"{'':=<70}")
            print(f"  {'Symbol':<12s} {'Open':>10s} {'Close':>10s} {'Topf':<8s} "
                  f"{'alt_open':>8s} {'neu_open':>8s} {'alt_close':>8s} {'neu_close':>8s} "
                  f"{'alt_delta':>10s} {'neu_delta':>10s} {'diff':>8s}")
            print(f"  {'-'*108}")
            for d in diffs[:30]:
                print(f"  {d['symbol']:<12s} {d['open']:>10s} {d['close']:>10s} {d['topf']:<8s} "
                      f"{d['old_open']:>8.5f} {d['new_open']:>8.5f} "
                      f"{d['old_close']:>8.5f} {d['new_close']:>8.5f} "
                      f"{d['old_delta']:>+10.2f} {d['new_delta']:>+10.2f} {d['diff']:>+8.2f}")

            # Statistics
            abs_diffs = [abs(d['diff']) for d in diffs]
            print(f"\n  Max Abweichung:    {max(abs_diffs):>8.2f} EUR")
            print(f"  Median Abweichung: {sorted(abs_diffs)[len(abs_diffs)//2]:>8.2f} EUR")
            print(f"  Summe |Abweichung|: {sum(abs_diffs):>8.2f} EUR")
        else:
            print(f"\n  Keine signifikanten Einzelabweichungen (alle <0.5ct).")

        print()

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description='Tageskurs-Vergleich: ExchTrade vs ConversionRate')
    parser.add_argument('xml', help='Haupt-XML (Steuerjahr)')
    parser.add_argument('--history', help='History-XML (Vorjahr)')
    args = parser.parse_args()

    if not os.path.exists(args.xml):
        print(f"Fehler: {args.xml} nicht gefunden")
        sys.exit(1)
    if args.history and not os.path.exists(args.history):
        print(f"Fehler: {args.history} nicht gefunden")
        sys.exit(1)

    # Run without history
    run_comparison(args.xml)

    # Run with history if provided
    if args.history:
        print("\n\n")
        run_comparison(args.xml, args.history)


if __name__ == '__main__':
    main()
