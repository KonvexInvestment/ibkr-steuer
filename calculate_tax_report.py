
import csv
import os
import sys
from datetime import datetime, timedelta

def load_csv(filepath):
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

def get_rate_for_date(target_date, rates_map):
    if not rates_map:
        return 0.95 # Fallback average 2025 prediction

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
            if len(parts) >= 6:
                field = parts[2].strip() if len(parts) > 2 else ''
                if line.startswith('Dividenden,Data,Gesamt Dividenden in EUR'):
                    income_totals['dividends_eur'] = safe_float(parts[5], 0)
                    continue
                elif line.startswith('Zinsen,Data,Gesamt Zinsen in EUR'):
                    income_totals['interest_eur'] = safe_float(parts[5], 0)
                    continue
                elif line.startswith('Quellensteuer,Data,Gesamtwert in EUR'):
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


def calculate_fx_gains(trades, fx_transactions, tax_year, base_currency='EUR'):
    """
    Berechnet FIFO-basierte Fremdwährungs-Gewinne/Verluste pro Währung.

    Verwendet fx_transactions.csv (StmtFunds Currency-Level) mit Raten-Substitution:
    - Einträge mit fxRateToBase ≈ 1.0 (unbrauchbar auf Aggregat-Ebene) erhalten
      den Tageskurs aus trades.csv (fxRateToBase der Trades an diesem Tag)
    - BUY/SELL/ADJ werden übersprungen (deren FX-Effekt steckt in fifoPnlRealized)
    - FOREX, DIV, FRTAX, Zinsen, Gebühren etc. werden als FX-Ereignisse getrackt

    Lots werden über alle Jahre aufgebaut (Multi-Year-Support), aber Gewinne/Verluste
    werden nur für Abflüsse im tax_year gezählt.

    Returns:
        dict per currency, float total_gain, float total_loss, bool has_prior_data
    """
    from collections import defaultdict, deque
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

    # --- Process fx_transactions with rate substitution ---
    skip_codes = {'BUY', 'SELL', 'ADJ', ''}

    by_currency = defaultdict(list)

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

        # Starting Balance → seed lot (single-year mode or with rate substitution)
        if activity_desc == 'Starting Balance':
            balance = safe_float(tx.get('balance'), 0)
            if balance > 0.01:
                date_str = tx.get('date', '')
                fx = safe_float(tx.get('fxRateToBase'), 0)
                if fx <= 0 or abs(fx - 1.0) < 0.001:
                    fx = get_daily_rate(curr, date_str[:10])
                if fx > 0:
                    by_currency[curr].append((date_str, balance, fx))
            continue

        if code in skip_codes:
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

        if fx <= 0:
            continue

        by_currency[curr].append((date_str, amount, fx))

    # --- FIFO processing per currency ---
    if has_prior_data:
        print(f"FX: Multi-Year-Daten erkannt. FIFO-Lots werden vollständig aufgebaut.")
    elif starting_balance_total > 0.01:
        print(f"FX: Nur Steuerjahr {tax_year} geladen. Anfangsbestände ({starting_balance_total:,.0f} Fremdwährung) "
              f"werden zum 01.01.-Kurs angesetzt (Vereinfachung).")

    results = {}
    total_gain = 0.0
    total_loss = 0.0

    for curr, events in sorted(by_currency.items()):
        events.sort(key=lambda x: x[0])  # sort by date

        lots = deque()
        gain = 0.0
        loss = 0.0
        disposals = 0

        for date_str, amount, fx in events:
            if amount > 0:
                lots.append([date_str, amount, fx])
            else:
                dispose_amount = abs(amount)
                date = parse_date(date_str)

                while dispose_amount > 0.001 and lots:
                    lot_date, lot_remaining, lot_rate = lots[0]
                    take = min(dispose_amount, lot_remaining)

                    if date and date.year == tax_year:
                        pnl = take * (fx - lot_rate)
                        if pnl > 0:
                            gain += pnl
                        else:
                            loss += pnl
                        disposals += 1

                    lot_remaining -= take
                    dispose_amount -= take

                    if lot_remaining < 0.001:
                        lots.popleft()
                    else:
                        lots[0][1] = lot_remaining

        net = gain + loss
        if abs(gain) > 0.01 or abs(loss) > 0.01:
            results[curr] = {
                'gain': gain,
                'loss': loss,
                'net': net,
                'lots_remaining': len(lots),
                'disposals_count': disposals,
            }
            total_gain += gain
            total_loss += loss

    return results, total_gain, total_loss, has_prior_data


def calculate_tax(ib_tax_dir, tax_year=None, fx_csv_path=None):
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
        # Use transactionID if available, otherwise full tuple
        tid = f.get('transactionID')
        if tid:
            key = tid
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
    if base_currency == 'USD':
        usd_to_eur_rates = get_exchange_rates(trades, funds)
        print(f"Loaded {len(usd_to_eur_rates)} daily exchange rates.")
    else:
        print(f"Base currency is {base_currency} — no USD→EUR rate map needed.")

    # 2b. Build ETF lookup from financial_instruments.csv
    from etf_classification import get_classification, get_etf_info, get_teilfreistellung, is_known_etf, ETF_CLASSIFICATION
    etf_isins = set()  # all ISINs that IBKR marks as ETF (subCategory)
    symbol_to_isin = {}  # for Stillhalter underlying lookup
    fi_path = os.path.join(ib_tax_dir, 'financial_instruments.csv')
    if os.path.exists(fi_path):
        for fi in load_csv(fi_path):
            sym = fi.get('symbol', '').strip()
            isin = fi.get('isin', '').strip()
            if sym and isin:
                symbol_to_isin[sym] = isin
            if fi.get('subCategory') == 'ETF' and fi.get('assetCategory') == 'STK' and isin:
                etf_isins.add(isin)
    # Also pick up ETFs from trades themselves
    for t in trades:
        if t.get('subCategory') == 'ETF' and t.get('assetCategory') == 'STK':
            isin = t.get('isin', '').strip()
            if isin:
                etf_isins.add(isin)
    if etf_isins:
        print(f"ETF-Erkennung: {len(etf_isins)} ETF-ISINs gefunden (subCategory=ETF).")

    # 3. Capital Gains (Stocks & Options)
    stocks_gain = 0.0
    stocks_loss = 0.0

    options_gain = 0.0
    options_loss = 0.0

    # InvStG ETF tracking (KAP-INV)
    etf_invstg_gain = 0.0       # InvStG fund gains (before Teilfreistellung)
    etf_invstg_loss = 0.0       # InvStG fund losses (before Teilfreistellung)
    etf_dividends_eur = 0.0     # InvStG fund dividends
    etf_wht_eur = 0.0           # InvStG fund withholding tax (sum, negative)
    etf_by_isin = {}            # per-ISIN tracking for Teilfreistellung
    debug_rows = []             # per-trade debug export

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
            if sub == 'ETF' and isin:
                cls = get_classification(isin)
                if cls == 'no_invstg':
                    # Crypto/Commodity ETPs: stay in Topf 1 like regular stocks
                    if pnl_eur > 0:
                        stocks_gain += pnl_eur
                    else:
                        stocks_loss += pnl_eur
                else:
                    # InvStG fund → KAP-INV (not Topf 1)
                    if pnl_eur > 0:
                        etf_invstg_gain += pnl_eur
                    else:
                        etf_invstg_loss += pnl_eur
                    # Per-ISIN tracking
                    if isin not in etf_by_isin:
                        info = get_etf_info(isin)
                        etf_by_isin[isin] = {'ticker': info['ticker'] if info else isin[:12], 'name': info['name'] if info else '', 'classification': cls or 'sonstiger_fonds', 'gain': 0.0, 'loss': 0.0, 'div': 0.0, 'wht': 0.0}
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

        # Collect debug row
        sub = t.get('subCategory', '')
        isin = t.get('isin', '').strip()
        if category == 'STK' and sub == 'ETF' and isin:
            cls = get_classification(isin)
            topf = 'KAP-INV' if cls and cls != 'no_invstg' else 'Topf1'
        elif category == 'STK':
            topf = 'Topf1'
        else:
            topf = 'Topf2'
        debug_rows.append({
            'dateTime': t.get('dateTime', ''),
            'reportDate': t.get('reportDate', ''),
            'symbol': t.get('symbol', ''),
            'isin': isin,
            'assetCategory': category,
            'subCategory': sub,
            'buySell': t.get('buySell', ''),
            'quantity': t.get('quantity', ''),
            'transactionType': t.get('transactionType', ''),
            'currency': t.get('currency', ''),
            'fifoPnlRealized': pnl_raw,
            'fxRateToBase': fx_to_base,
            'pnl_eur': round(pnl_eur, 2),
            'topf': topf,
        })

    # Write debug CSV
    if debug_rows:
        import csv as csv_mod
        debug_path = os.path.join(ib_tax_dir, 'trades_debug_eur.csv')
        with open(debug_path, 'w', newline='', encoding='utf-8') as f:
            w = csv_mod.DictWriter(f, fieldnames=debug_rows[0].keys())
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
    stillhalter_details = []

    opt_assignments = [t for t in trades
                       if t.get('assetCategory') in ('OPT', 'FOP', 'FSFOP')
                       and t.get('transactionType') == 'BookTrade'
                       and t.get('buySell') == 'BUY'      # closing a short position
                       and t.get('putCall') in ('C', 'P')  # both call and put assignments
                       and abs(safe_float(t.get('fifoPnlRealized'))) < 0.01
                       and (d := parse_date(t.get('reportDate') or t.get('dateTime') or t.get('tradeDate'))) is not None
                       and d.year == tax_year]             # only assignments in tax year

    for a in opt_assignments:
        strike = a.get('strike')
        expiry = a.get('expiry')
        pc = a.get('putCall')
        a_cat = a.get('assetCategory')
        a_qty = abs(int(safe_float(a.get('quantity'))))
        if not strike or not expiry or not pc or a_qty == 0:
            continue

        # Opposite side: if assignment is BUY (closing short), original was SELL (opening)
        orig_side = 'SELL' if a.get('buySell') == 'BUY' else 'BUY'

        # Find ALL original opening ExchTrades (may be multiple partial fills)
        originals = [t for t in trades
                     if t.get('assetCategory') == a_cat
                     and t.get('transactionType') == 'ExchTrade'
                     and t.get('strike') == strike
                     and t.get('expiry') == expiry
                     and t.get('putCall') == pc
                     and t.get('buySell') == orig_side]

        if not originals:
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

        # Weighted average premium across all fills
        # Use tradePrice (actual fill price) if available, fall back to closePrice
        total_premium_raw = 0.0
        total_orig_qty = 0
        mult = int(safe_float(originals[0].get('multiplier'), 100))

        for orig in originals:
            price = safe_float(orig.get('tradePrice')) or safe_float(orig.get('closePrice'))
            qty = abs(int(safe_float(orig.get('quantity'))))
            if qty > 0 and price > 0:
                total_premium_raw += price * mult * qty
                total_orig_qty += qty

        if total_orig_qty == 0 or total_premium_raw == 0:
            continue

        # Scale to assignment quantity (may be less than total originally sold)
        premium_raw = total_premium_raw * a_qty / total_orig_qty

        # Convert to EUR using the original trade's FX rate
        # Use weighted average rate from originals
        fx_weighted = sum(safe_float(o.get('fxRateToBase'), 1.0) * abs(int(safe_float(o.get('quantity'))))
                         for o in originals if safe_float(o.get('quantity')) != 0)
        fx_to_base = fx_weighted / total_orig_qty if total_orig_qty else 1.0

        if base_currency == 'EUR':
            premium_eur = premium_raw * fx_to_base
        else:
            date = parse_date(a.get('dateTime') or a.get('tradeDate'))
            rate_eur = get_rate_for_date(date, usd_to_eur_rates)
            premium_eur = premium_raw * fx_to_base * rate_eur

        stillhalter_premium_eur += premium_eur
        stillhalter_count += 1

        # Collect per-assignment details for Zuflussprinzip
        orig_sell_date = None
        for orig in originals:
            od = parse_date(orig.get('dateTime') or orig.get('tradeDate'))
            if od is not None and (orig_sell_date is None or od < orig_sell_date):
                orig_sell_date = od
        assignment_date = parse_date(a.get('dateTime') or a.get('tradeDate'))
        stillhalter_details.append({
            'symbol': a.get('symbol') or a.get('description') or f"{strike} {expiry} {pc}",
            'strike': strike,
            'expiry': expiry,
            'quantity': a_qty,
            'premium_eur': premium_eur,
            'assignment_date': str(assignment_date) if assignment_date else '',
            'orig_sell_date': str(orig_sell_date) if orig_sell_date else '',
            'orig_sell_year': orig_sell_date.year if orig_sell_date else tax_year,
            'is_cross_year': (orig_sell_date.year < tax_year) if orig_sell_date else False,
        })

    # Move premiums from Topf 1 (stocks) / KAP-INV to Topf 2 (sonstiges)
    etf_stillhalter_premium_eur = 0.0
    if stillhalter_premium_eur > 0:
        # Split: check if underlying is an InvStG ETF
        stk_premium = 0.0
        etf_premium = 0.0
        for det in stillhalter_details:
            underlying = det['symbol'].split()[0] if det['symbol'] else ''
            underlying_isin = symbol_to_isin.get(underlying, '')
            if underlying_isin and underlying_isin in etf_isins:
                cls = get_classification(underlying_isin)
                if cls and cls != 'no_invstg':
                    etf_premium += det['premium_eur']
                    # Adjust per-ISIN tracking (keeps etf_by_isin in sync with etf_invstg_gain)
                    # Both must be adjusted: etf_invstg_gain for total, etf_by_isin for per-ISIN TFS
                    if underlying_isin in etf_by_isin:
                        etf_by_isin[underlying_isin]['gain'] -= det['premium_eur']
                    continue
            stk_premium += det['premium_eur']

        stocks_gain -= stk_premium
        etf_invstg_gain -= etf_premium
        etf_stillhalter_premium_eur = etf_premium
        options_gain += stillhalter_premium_eur  # total premium always to Topf 2
        price_source = "tradePrice" if has_trade_price else "closePrice (Näherung)"
        parts = []
        if stk_premium > 0:
            parts.append(f"{stk_premium:,.2f} von Aktien")
        if etf_premium > 0:
            parts.append(f"{etf_premium:,.2f} von ETF/KAP-INV")
        print(f"Stillhalterprämien: {stillhalter_count} Assignments, {stillhalter_premium_eur:,.2f} EUR → Topf 2 ({', '.join(parts)}) (Quelle: {price_source}).")
    if stillhalter_unmatched:
        print(f"  (!) WARNUNG: {len(stillhalter_unmatched)} Assignment(s) — der ursprüngliche Optionsverkauf "
              f"(ExchTrade SELL) wurde nicht gefunden. Vermutlich in einem Vorjahr eröffnet. "
              f"Ohne diesen kann die Stillhalterprämie nicht berechnet und von Topf 1 (Aktien) "
              f"nach Topf 2 (Sonstiges) verschoben werden. Vorjahres-XMLs per --history laden.")

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
    cross_year_put_corrections = []
    cross_year_put_total = 0.0

    for a in prior_put_assignments:
        strike = a.get('strike')
        expiry = a.get('expiry')
        a_cat = a.get('assetCategory')
        a_qty = abs(int(safe_float(a.get('quantity'))))
        mult = int(safe_float(a.get('multiplier'), 100))
        underlying = a.get('underlyingSymbol', '')
        if not strike or not underlying or a_qty == 0:
            continue

        # Find original put SELL
        originals = [t for t in trades
                     if t.get('assetCategory') == a_cat
                     and t.get('transactionType') == 'ExchTrade'
                     and t.get('strike') == strike
                     and t.get('expiry') == expiry
                     and t.get('putCall') == 'P'
                     and t.get('buySell') == 'SELL']

        if not originals:
            continue

        total_premium_raw = 0.0
        total_orig_qty = 0
        for orig in originals:
            price = safe_float(orig.get('tradePrice')) or safe_float(orig.get('closePrice'))
            qty = abs(int(safe_float(orig.get('quantity'))))
            if qty > 0 and price > 0:
                total_premium_raw += price * mult * qty
                total_orig_qty += qty

        if total_orig_qty == 0 or total_premium_raw == 0:
            continue

        premium_raw = total_premium_raw * a_qty / total_orig_qty
        shares = a_qty * mult

        fx_weighted = sum(safe_float(o.get('fxRateToBase'), 1.0) * abs(int(safe_float(o.get('quantity'))))
                         for o in originals if safe_float(o.get('quantity')) != 0)
        fx_to_base = fx_weighted / total_orig_qty if total_orig_qty else 1.0

        if base_currency == 'EUR':
            premium_eur = premium_raw * fx_to_base
        else:
            date = parse_date(a.get('dateTime') or a.get('tradeDate'))
            rate_eur = get_rate_for_date(date, usd_to_eur_rates)
            premium_eur = premium_raw * fx_to_base * rate_eur

        premium_per_share_eur = premium_eur / shares if shares else 0
        a_date = parse_date(a.get('reportDate') or a.get('dateTime') or a.get('tradeDate'))

        if underlying not in put_assignment_lots:
            put_assignment_lots[underlying] = deque()
        put_assignment_lots[underlying].append({
            'date': a_date,
            'shares_remaining': shares,
            'premium_per_share_eur': premium_per_share_eur,
            'strike': strike,
            'year': a_date.year if a_date else 0,
        })

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
                correction = consumed * lot['premium_per_share_eur']
                cross_year_put_total += correction
                cross_year_put_corrections.append({
                    'symbol': sym,
                    'shares': consumed,
                    'premium_per_share': lot['premium_per_share_eur'],
                    'correction_eur': correction,
                    'assignment_year': lot['year'],
                    'strike': lot['strike'],
                })
                lot['shares_remaining'] -= consumed
                remaining -= consumed
                if lot['shares_remaining'] <= 0:
                    put_assignment_lots[sym].popleft()

        if cross_year_put_total > 0:
            # Split correction between regular stocks and InvStG ETFs
            stk_corr = 0.0
            etf_corr = 0.0
            for c in cross_year_put_corrections:
                sym = c['symbol']
                underlying_isin = symbol_to_isin.get(sym, '')
                if underlying_isin and underlying_isin in etf_isins:
                    cls = get_classification(underlying_isin)
                    if cls and cls != 'no_invstg':
                        etf_corr += c['correction_eur']
                        # Adjust per-ISIN (keeps in sync with etf_invstg_gain)
                        if underlying_isin in etf_by_isin:
                            etf_by_isin[underlying_isin]['gain'] -= c['correction_eur']
                        continue
                stk_corr += c['correction_eur']
            stocks_gain -= stk_corr
            etf_invstg_gain -= etf_corr
            # NOT options_gain += ... (premium was already taxed in the assignment year)
            print(f"Cross-Year Put-Korrektur: {len(cross_year_put_corrections)} Positionen, "
                  f"{cross_year_put_total:,.2f} EUR von PnL abgezogen (Prämie bereits in Vorjahr versteuert).")

    # Zuflussprinzip: cross-year premium aggregation
    cross_year_premium_eur = sum(d['premium_eur'] for d in stillhalter_details if d['is_cross_year'])
    cross_year_by_year = {}
    for det in stillhalter_details:
        if det['is_cross_year']:
            yr = det['orig_sell_year']
            cross_year_by_year[yr] = cross_year_by_year.get(yr, 0) + det['premium_eur']

    # --- PLAUSIBILITY: Raw Sums for Reconciliation ---
    # Use reportDate (booking date) for year assignment — Zuflussprinzip (§11 EStG)
    raw_div_base = sum(safe_float(f.get('amount')) for f in funds if f.get('activityCode') == 'DIV' and (d := parse_date(f.get('reportDate') or f.get('date'))) is not None and d.year == tax_year)
    raw_tax_base = sum(safe_float(f.get('amount')) for f in funds if f.get('activityCode') in ['FRTAX', 'WHT'] and (d := parse_date(f.get('reportDate') or f.get('date'))) is not None and d.year == tax_year)

    # 4. Dividends, Interest, and Withholding Tax
    dividends_eur = 0.0
    interest_eur = 0.0  # Bond coupons, credit interest
    withholding_tax_eur = 0.0

    funds_processed = 0
    funds_skipped_year = 0

    for f in funds:
        code = f.get('activityCode')
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
        is_etf_fund = False
        fund_isin = ''
        if f.get('subCategory') == 'ETF':
            fund_isin = f.get('isin', '').strip()
            if fund_isin:
                cls = get_classification(fund_isin)
                if cls and cls != 'no_invstg':
                    is_etf_fund = True

        if code == 'DIV':
            if is_etf_fund:
                etf_dividends_eur += amount_eur
                if fund_isin not in etf_by_isin:
                    info = get_etf_info(fund_isin)
                    etf_by_isin[fund_isin] = {'ticker': info['ticker'] if info else fund_isin[:12], 'name': info['name'] if info else '', 'classification': cls or 'sonstiger_fonds', 'gain': 0.0, 'loss': 0.0, 'div': 0.0, 'wht': 0.0}
                etf_by_isin[fund_isin]['div'] += amount_eur
            else:
                dividends_eur += amount_eur
        elif code == 'PIL':
            # Payment in Lieu: positive = received (long position lent out)
            # negative = paid (short position owes dividend)
            # Net with dividends as per German tax law
            if is_etf_fund:
                etf_dividends_eur += amount_eur
                if fund_isin not in etf_by_isin:
                    info = get_etf_info(fund_isin)
                    etf_by_isin[fund_isin] = {'ticker': info['ticker'] if info else fund_isin[:12], 'name': info['name'] if info else '', 'classification': cls or 'sonstiger_fonds', 'gain': 0.0, 'loss': 0.0, 'div': 0.0, 'wht': 0.0}
                etf_by_isin[fund_isin]['div'] += amount_eur
            else:
                dividends_eur += amount_eur
        elif code in ['INTR', 'CINT', 'INTP', 'DINT']:
            # Interest income (bond coupons, credit interest)
            # INTP = Accrued interest paid (deductible Stückzinsen)
            # DINT = Debit interest (margin interest, borrow fees, SYEP — negative)
            interest_eur += amount_eur
        elif code in ['FRTAX', 'WHT']:
            # Tax is usually negative. We want the absolute value of the NET tax paid.
            # If there are adjustments/refunds (positive), they reduce the total tax.
            # We track the sum directly and take the absolute value later.
            if is_etf_fund:
                etf_wht_eur += amount_eur
                if fund_isin in etf_by_isin:
                    etf_by_isin[fund_isin]['wht'] += amount_eur
            else:
                withholding_tax_eur += amount_eur
            
    # Finalize tax: convert net sum to absolute value for "Tax Paid" field
    withholding_tax_eur = abs(withholding_tax_eur)
            
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
            
        # FX rate for summary fallback (pnl_summary is "InBase" = base currency)
        if base_currency == 'EUR':
            default_fallback_rate = 1.0  # Already in EUR
        elif usd_to_eur_rates:
            last_date = sorted(usd_to_eur_rates.keys())[-1]
            default_fallback_rate = usd_to_eur_rates[last_date]
        else:
            default_fallback_rate = 0.95

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
                    added_from_summary += 1
            else:
                # For STK and OPT: skip if trades.csv already has non-zero PnL
                if abs(trade_pnl) > 0.01:
                    continue
                    
                gain_eur = summary_gain_usd * default_fallback_rate
                loss_eur = summary_loss_usd * default_fallback_rate
                
                if asset == 'STK':
                    sub_cat = s_row.get('subCategory', '')
                    if sub_cat == 'ETF':
                        cls = get_classification(isin)
                        if cls and cls != 'no_invstg':
                            etf_invstg_gain += gain_eur
                            etf_invstg_loss += loss_eur
                            if isin not in etf_by_isin:
                                info = get_etf_info(isin)
                                etf_by_isin[isin] = {'ticker': info['ticker'] if info else isin[:12], 'name': info['name'] if info else '', 'classification': cls or 'sonstiger_fonds', 'gain': 0.0, 'loss': 0.0, 'div': 0.0, 'wht': 0.0}
                            etf_by_isin[isin]['gain'] += gain_eur
                            etf_by_isin[isin]['loss'] += loss_eur
                        else:
                            stocks_gain += gain_eur
                            stocks_loss += loss_eur
                    else:
                        stocks_gain += gain_eur
                        stocks_loss += loss_eur
                elif asset in ['OPT', 'FUT', 'FOP', 'FSFOP']:
                    options_gain += gain_eur
                    options_loss += loss_eur
                added_from_summary += 1
        
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

    # Option A: Exact FX from XML FxTransactions (IBKR's own FIFO, per-transaction realizedPL)
    fx_pnl_path = os.path.join(ib_tax_dir, 'fx_realized_pnl.csv')
    if not fx_results and os.path.exists(fx_pnl_path) and base_currency == 'EUR':
        fx_pnl_rows = load_csv(fx_pnl_path)
        fx_by_curr = {}
        for row in fx_pnl_rows:
            rd = parse_date(row.get('reportDate'))
            if not rd or rd.year != tax_year:
                continue
            curr = row.get('fxCurrency', '')
            pnl = safe_float(row.get('realizedPL'), 0)
            if not curr or abs(pnl) < 0.001:
                continue
            if curr not in fx_by_curr:
                fx_by_curr[curr] = {'gain': 0, 'loss': 0, 'net': 0, 'lots_remaining': 0, 'disposals_count': 0}
            if pnl > 0:
                fx_by_curr[curr]['gain'] += pnl
            else:
                fx_by_curr[curr]['loss'] += pnl
            fx_by_curr[curr]['net'] += pnl
            fx_by_curr[curr]['disposals_count'] += 1
        if fx_by_curr:
            fx_results = fx_by_curr
            fx_total_gain = sum(d['gain'] for d in fx_by_curr.values())
            fx_total_loss = sum(d['loss'] for d in fx_by_curr.values())
            fx_source = 'xml'
            print(f"FX: Exakte Werte aus XML FxTransactions übernommen ({len(fx_pnl_rows)} Einträge).")

    # Option B: Exact FX from IBKR CSV report (same data as XML FxTransactions)
    if not fx_results and fx_csv_path and os.path.exists(fx_csv_path) and base_currency == 'EUR':
        fx_results = csv_data['fx_results']
        fx_total_gain = csv_data['fx_total_gain']
        fx_total_loss = csv_data['fx_total_loss']
        fx_source = 'csv'
        print(f"FX: Exakte Werte aus IBKR Standard-Bericht übernommen.")

    # Option C: FIFO approximation from fx_transactions.csv (least accurate)
    fx_path = os.path.join(ib_tax_dir, 'fx_transactions.csv')
    if not fx_results and os.path.exists(fx_path) and base_currency == 'EUR':
        fx_transactions = load_csv(fx_path)
        fx_results, fx_total_gain, fx_total_loss, fx_has_prior_data = calculate_fx_gains(
            trades, fx_transactions, tax_year, base_currency
        )
        fx_source = 'fifo'

    if fx_results:
        # FX gains/losses go into Topf 2 (verzinsliches Fremdwährungsguthaben → §20 Abs. 2 S. 1 Nr. 7)
        options_gain += fx_total_gain
        options_loss += fx_total_loss
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
        if not is_known_etf(isin) and isin in etf_by_isin:
            etf_unknown_isins.append(isin)

    for isin, data in etf_by_isin.items():
        tfs_rate = get_teilfreistellung(isin)
        data['tfs_rate'] = tfs_rate
        data['gain_taxable'] = data['gain'] * (1 - tfs_rate)
        data['loss_taxable'] = data['loss'] * (1 - tfs_rate)
        data['div_taxable'] = data['div'] * (1 - tfs_rate)
        etf_gain_taxable += data['gain_taxable']
        etf_loss_taxable += data['loss_taxable']
        etf_div_taxable += data['div_taxable']

    etf_wht_abs = abs(etf_wht_eur)  # positive for reporting
    etf_net_taxable = etf_gain_taxable + etf_loss_taxable + etf_div_taxable

    if etf_by_isin:
        tfs_reduction = (etf_invstg_gain + etf_invstg_loss + etf_dividends_eur) - etf_net_taxable
        print(f"InvStG ETFs: {len(etf_by_isin)} Fonds erkannt. "
              f"Gewinne {etf_invstg_gain:,.2f}, Verluste {etf_invstg_loss:,.2f}, "
              f"Dividenden {etf_dividends_eur:,.2f}, WHT {etf_wht_abs:,.2f} EUR. "
              f"Teilfreistellung: {tfs_reduction:,.2f} EUR Reduktion.")
    if etf_unknown_isins:
        print(f"  (!) {len(etf_unknown_isins)} ETF(s) nicht in Klassifizierungstabelle — als sonstiger Fonds (0% TFS) behandelt.")

    # --- Per-Lot FX Correction (CLOSED_LOT Tageskurs-Methode) ---
    # Compares IBKR method (net PnL × close rate) vs. correct method
    # (proceeds × close rate - cost × open rate) per FIFO lot.
    # Delta per lot = |cost_trade_ccy| × (fxRate_close - fxRate_open)
    fx_correction_total = 0.0
    fx_correction_details = []
    fx_corr_by_topf = {'Topf1': 0.0, 'Topf2': 0.0, 'KAP-INV': 0.0}
    closed_lots_path = os.path.join(ib_tax_dir, 'closed_lots.csv')
    if os.path.exists(closed_lots_path) and base_currency == 'EUR':
        from collections import defaultdict
        import bisect

        closed_lots = load_csv(closed_lots_path)

        # Build daily FX rate map from all trades (same as calculate_fx_gains)
        daily_fx = defaultdict(list)
        for t in trades:
            curr = t.get('currency', '')
            fx = safe_float(t.get('fxRateToBase'), 0)
            dt = (t.get('dateTime') or '')[:10]
            if curr == 'USD' and fx > 0 and dt:
                daily_fx[dt].append(fx)
        fx_map = {d: sum(r) / len(r) for d, r in daily_fx.items()}
        fx_dates = sorted(fx_map.keys())

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

            cost_raw = safe_float(lot.get('cost'), 0)

            fx_close = safe_float(lot.get('fxRateToBase'), 0)
            open_dt = lot.get('openDateTime', '')
            fx_open = lookup_fx(open_dt)

            if fx_close <= 0 or fx_open <= 0:
                continue

            delta = cost_raw * (fx_close - fx_open)
            fx_correction_total += delta
            lots_processed += 1

            # Determine topf
            sub = lot.get('subCategory', '')
            isin = lot.get('isin', '').strip()
            if category == 'STK' and sub == 'ETF' and isin:
                cls = get_classification(isin)
                topf = 'KAP-INV' if cls and cls != 'no_invstg' else 'Topf1'
            elif category == 'STK':
                topf = 'Topf1'
            else:
                topf = 'Topf2'
            fx_corr_by_topf[topf] += delta

        if lots_processed > 0:
            print(f"\nTageskurs-Korrektur (CLOSED_LOT): {lots_processed} Lots analysiert.")
            print(f"  FX-Korrektur gesamt: {fx_correction_total:>+12,.2f} EUR")
            for topf, val in sorted(fx_corr_by_topf.items()):
                if abs(val) > 0.01:
                    print(f"    {topf}: {val:>+12,.2f} EUR")

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
    
    report_data = {
        "zeile_19_netto_eur": zeile_19_netto,
        "zeile_20_stock_gains_eur": zeile_20_stock_gains,
        "zeile_22_other_losses_eur": zeile_22_other_losses,
        "zeile_23_stock_losses_eur": zeile_23_stock_losses,
        "zeile_41_withholding_tax_eur": withholding_tax_eur,
        # Pool details
        "topf_1_aktien_netto": topf_1_aktien,
        "topf_2_sonstiges_netto": topf_2_sonstiges,
        # Keep old keys for backward compatibility
        "dividends_eur": dividends_eur,
        "interest_eur": interest_eur,
        "stocks_gain_eur": stocks_gain,
        "stocks_loss_eur": stocks_loss,
        "stocks_net_eur": stocks_gain + stocks_loss,
        "options_gain_eur": options_gain,
        "options_loss_eur": options_loss,
        "options_net_eur": options_gain + options_loss,
        "withholding_tax_eur": withholding_tax_eur,
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
        "xml_has_fx_data": xml_has_fx_data,
        "csv_category_totals": csv_category_totals,
        "csv_income_totals": csv_income_totals,
        # Per-lot FX correction (Tageskurs-Methode)
        "fx_correction_total": fx_correction_total,
        "fx_correction_by_topf": fx_corr_by_topf,
        # InvStG / Anlage KAP-INV
        "kap_inv": {
            "etf_gain_raw_eur": etf_invstg_gain,
            "etf_loss_raw_eur": etf_invstg_loss,
            "etf_gain_taxable_eur": etf_gain_taxable,
            "etf_loss_taxable_eur": etf_loss_taxable,
            "etf_dividends_raw_eur": etf_dividends_eur,
            "etf_dividends_taxable_eur": etf_div_taxable,
            "etf_wht_eur": etf_wht_abs,
            "etf_net_taxable_eur": etf_net_taxable,
            "etf_by_isin": etf_by_isin,
            "etf_unknown_isins": etf_unknown_isins,
            "etf_stillhalter_premium_eur": etf_stillhalter_premium_eur,
        },
        # Plausibility Metadata
        "has_trade_price": has_trade_price,
        "audit": {
            "funds_processed": funds_processed,
            "funds_skipped_year": funds_skipped_year,
            "raw_div_base": raw_div_base,
            "raw_tax_base": raw_tax_base,
            "added_from_summary": added_from_summary,
            "usd_to_eur_rates_count": len(usd_to_eur_rates),
            "stillhalter_count": stillhalter_count,
            "stillhalter_premium_eur": stillhalter_premium_eur,
            "stillhalter_unmatched": stillhalter_unmatched,
            "stillhalter_details": stillhalter_details,
            "cross_year_premium_eur": cross_year_premium_eur,
            "cross_year_by_year": cross_year_by_year,
            "cross_year_put_corrections": cross_year_put_corrections,
            "cross_year_put_total": cross_year_put_total
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
    print(f"    Zinsen:                {interest_eur:>12,.2f} EUR")
    if stillhalter_premium_eur > 0:
        print(f"    Stillhalterprämien:    {stillhalter_premium_eur:>12,.2f} EUR  ({stillhalter_count} Assignments)")
    print(f"    Optionsgewinne:        {options_gain:>12,.2f} EUR")
    print(f"    Optionsverluste:       {options_loss:>12,.2f} EUR")
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
        print(f"    ETF-Quellensteuer:     {etf_wht_abs:>12,.2f} EUR")

    print("-" * 60)
    print("ZEILE 19 (Ausländische Kapitalerträge - NETTO):")
    print(f"    = Saldo Aktien + Saldo Sonstiges")
    print(f"    = {topf_1_aktien:,.2f} + {topf_2_sonstiges:,.2f}")
    print(f"    ═════════════════════════════════════")
    print(f"    ZEILE 19:              {zeile_19_netto:>12,.2f} EUR")
    if etf_by_isin:
        print(f"    KAP-INV (ETF netto):   {etf_net_taxable:>12,.2f} EUR")
    
    print("-" * 60)
    print(f"ZEILE 20 (Davon: Aktiengewinne):   {zeile_20_stock_gains:>12,.2f} EUR")
    print(f"ZEILE 22 (Verluste ohne Aktien):   {zeile_22_other_losses:>12,.2f} EUR")
    print(f"ZEILE 23 (Aktienverluste):         {zeile_23_stock_losses:>12,.2f} EUR")
    print(f"ZEILE 41 (Quellensteuer):          {withholding_tax_eur:>12,.2f} EUR")

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

