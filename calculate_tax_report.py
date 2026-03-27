
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

def calculate_tax(ib_tax_dir, tax_year=2025):
    # 0. Detect base currency from account_info.csv
    base_currency = 'USD'  # default for backward compatibility
    acct_path = os.path.join(ib_tax_dir, 'account_info.csv')
    if os.path.exists(acct_path):
        acct_rows = load_csv(acct_path)
        if acct_rows:
            base_currency = acct_rows[0].get('currency', 'USD')
    print(f"Base currency: {base_currency}")

    # 1. Load and Deduplicate Trades
    all_trades = load_csv(os.path.join(ib_tax_dir, 'trades.csv'))
    
    unique_trades_set = set()
    trades = []
    duplicates_count = 0
    
    for t in all_trades:
        # Create a unique key based on relevant fields
        # Note: floating point fields might vary slightly? using string repr
        key = (
            t.get('dateTime'), 
            t.get('isin'), 
            t.get('buySell'), 
            t.get('quantity'), 
            t.get('closePrice'), # closePrice is used for trade value
            t.get('fifoPnlRealized')
        )
        if key in unique_trades_set:
            duplicates_count += 1
            continue
        unique_trades_set.add(key)
        trades.append(t)
        
    print(f"Loaded {len(all_trades)} trade rows. Removed {duplicates_count} duplicates. Unique trades: {len(trades)}")
    
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

    # 3. Capital Gains (Stocks & Options)
    stocks_gain = 0.0
    stocks_loss = 0.0

    options_gain = 0.0
    options_loss = 0.0

    for t in trades:
        # Check if Realized PnL event
        pnl_str = t.get('fifoPnlRealized')
        if not pnl_str or float(pnl_str) == 0:
            continue

        pnl_raw = float(pnl_str)
        fx_to_base = float(t.get('fxRateToBase', 1.0))

        if base_currency == 'EUR':
            # EUR base: pnl_raw × fxRateToBase already gives EUR
            pnl_eur = pnl_raw * fx_to_base
        else:
            # USD base: two-step conversion (trade currency → USD → EUR)
            pnl_usd = pnl_raw * fx_to_base
            date = parse_date(t.get('dateTime') or t.get('tradeDate'))
            rate_eur = get_rate_for_date(date, usd_to_eur_rates)
            pnl_eur = pnl_usd * rate_eur
        
        category = t.get('assetCategory')
        
        if category == 'STK':
            if pnl_eur > 0:
                stocks_gain += pnl_eur
            else:
                stocks_loss += pnl_eur
        elif category in ['OPT', 'FUT', 'FOP', 'BILL', 'BOND']:
            # BILL = Treasury Bills, BOND = Bonds - treated as "sonstige" (other income)
            if pnl_eur > 0:
                options_gain += pnl_eur
            else:
                options_loss += pnl_eur

    # --- Stillhalterprämien: separate assigned option premiums from stock PnL ---
    # When a short option is assigned, IBKR bundles the premium into the stock's
    # fifoPnlRealized and shows pnl=0 on the option BookTrade. Per BMF Rn. 25-35,
    # the premium is §20 Abs. 1 Nr. 11 income (Topf 2), not stock gain (Topf 1).
    #
    # Detection: OPT BookTrade with fifoPnlRealized=0 → assignment
    # Only SHORT CALL assignments need fixing:
    #   - Short call assigned (BUY closing, putCall=C): premium bundled into stock SALE PnL
    #   - Short put assigned: premium reduces stock cost basis (BMF Rn. 31) — correct as-is
    #   - Long option exercised: premium is acquisition cost — correct as-is

    stillhalter_premium_eur = 0.0
    stillhalter_count = 0

    opt_assignments = [t for t in trades
                       if t.get('assetCategory') == 'OPT'
                       and t.get('transactionType') == 'BookTrade'
                       and t.get('buySell') == 'BUY'      # closing a short position
                       and t.get('putCall') == 'C'         # call options only
                       and abs(float(t.get('fifoPnlRealized', 0) or 0)) < 0.01]

    for a in opt_assignments:
        strike = a.get('strike')
        expiry = a.get('expiry')
        pc = a.get('putCall')
        a_qty = abs(int(float(a.get('quantity', 0) or 0)))
        if not strike or not expiry or not pc or a_qty == 0:
            continue

        # Opposite side: if assignment is BUY (closing short), original was SELL (opening)
        orig_side = 'SELL' if a.get('buySell') == 'BUY' else 'BUY'

        # Find the original opening ExchTrade
        originals = [t for t in trades
                     if t.get('assetCategory') == 'OPT'
                     and t.get('transactionType') == 'ExchTrade'
                     and t.get('strike') == strike
                     and t.get('expiry') == expiry
                     and t.get('putCall') == pc
                     and t.get('buySell') == orig_side]

        if not originals:
            continue

        orig = originals[0]
        price = float(orig.get('closePrice', 0) or 0)
        mult = int(float(orig.get('multiplier', 100) or 100))
        orig_qty = abs(int(float(orig.get('quantity', 0) or 0)))
        if orig_qty == 0 or price == 0:
            continue

        # Premium per contract, scaled to assignment quantity
        premium_raw = price * mult * a_qty / orig_qty

        # Convert to EUR using the assignment date's rate
        fx_to_base = float(orig.get('fxRateToBase') or 1.0)
        if base_currency == 'EUR':
            premium_eur = premium_raw * fx_to_base
        else:
            date = parse_date(a.get('dateTime') or a.get('tradeDate'))
            rate_eur = get_rate_for_date(date, usd_to_eur_rates)
            premium_eur = premium_raw * fx_to_base * rate_eur

        stillhalter_premium_eur += premium_eur
        stillhalter_count += 1

    # Move premiums from Topf 1 (stocks) to Topf 2 (sonstiges)
    if stillhalter_premium_eur > 0:
        stocks_gain -= stillhalter_premium_eur
        options_gain += stillhalter_premium_eur
        print(f"Stillhalterprämien: {stillhalter_count} Assignments, {stillhalter_premium_eur:,.2f} EUR von Topf 1 → Topf 2 verschoben.")

    # --- PLAUSIBILITY: Raw Sums for Reconciliation ---
    raw_div_base = sum(float(f.get('amount', 0)) for f in funds if f.get('activityCode') == 'DIV' and (d := parse_date(f.get('date'))) is not None and d.year == tax_year)
    raw_tax_base = sum(float(f.get('amount', 0)) for f in funds if f.get('activityCode') in ['FRTAX', 'WHT'] and (d := parse_date(f.get('date'))) is not None and d.year == tax_year)
    
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
        # FRTAX/WHT = Withholding Tax
        if code not in ['DIV', 'PIL', 'INTR', 'CINT', 'INTP', 'FRTAX', 'WHT']:
            continue
            
        date = parse_date(f.get('date') or f.get('reportDate'))
        # Only process entries for the report year (2025)
        if not date or date.year != tax_year:
            funds_skipped_year += 1
            continue
            
        funds_processed += 1
            
        amount_raw = float(f.get('amount', 0) or 0)
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
                fx = float(f.get('fxRateToBase', 1.0))
                amount_usd = amount_raw * fx
                amount_eur = amount_usd * rate_eur
        
        if code == 'DIV':
            dividends_eur += amount_eur
        elif code == 'PIL':
            # Payment in Lieu: positive = received (long position lent out)
            # negative = paid (short position owes dividend)
            # Net with dividends as per German tax law
            dividends_eur += amount_eur
        elif code in ['INTR', 'CINT', 'INTP']:
            # Interest income (bond coupons, credit interest)
            # INTP = Accrued interest paid (deductible Stückzinsen)
            interest_eur += amount_eur
        elif code in ['FRTAX', 'WHT']:
            # Tax is usually negative. We want the absolute value of the NET tax paid.
            # If there are adjustments/refunds (positive), they reduce the total tax.
            # We track the sum directly and take the absolute value later.
            withholding_tax_eur += amount_eur
            
    # Finalize tax: convert net sum to absolute value for "Tax Paid" field
    withholding_tax_eur = abs(withholding_tax_eur)
            
    # --- Fallback: Realized PnL from Summary ---
    # Use ISIN to identify already-processed instruments (trades.csv lacks 'symbol')
    # Only add summary PnL if trades.csv had ZERO PnL for that ISIN
    summary_path = os.path.join(ib_tax_dir, 'pnl_summary.csv')
    summary_rows = []  # initialise so top-5 block can reference it safely
    added_from_summary = 0
    default_fallback_rate = 1.0 if base_currency == 'EUR' else 0.95
    if os.path.exists(summary_path):
        summary_rows = load_csv(summary_path)
        
        # Track PnL by ISIN from trades.csv
        pnl_by_isin = {}
        for t in trades:
            isin = t.get('isin', '').strip()
            if not isin:
                continue
            pnl_val = float(t.get('fifoPnlRealized', 0) or 0)
            pnl_by_isin[isin] = pnl_by_isin.get(isin, 0) + pnl_val
            
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
                    stocks_gain += gain_eur
                    stocks_loss += loss_eur
                elif asset in ['OPT', 'FUT', 'FOP']:
                    options_gain += gain_eur
                    options_loss += loss_eur
                added_from_summary += 1
        
        if added_from_summary > 0:
            print(f"Added {added_from_summary} instruments from PnL Summary fallback (ISIN-based).")

    # --- Top 5 Gains / Losses (plausibility check) ---
    # Source: pnl_summary.csv (has readable descriptions for all instrument types)
    # EUR conversion uses average daily rate (approximation — for display only)
    top_gains = []
    top_losses = []

    if os.path.exists(summary_path):
        # Build ISIN -> ticker from financial_instruments.csv
        isin_to_ticker = {}
        fi_path = os.path.join(ib_tax_dir, 'financial_instruments.csv')
        if os.path.exists(fi_path):
            for row in load_csv(fi_path):
                isin = row.get('isin', '').strip()
                sym  = row.get('symbol', '').strip()
                desc = row.get('description', '').strip()
                if isin:
                    isin_to_ticker[isin] = sym if sym else desc[:25]

        avg_rate = (sum(usd_to_eur_rates.values()) / len(usd_to_eur_rates)
                    if usd_to_eur_rates else default_fallback_rate)

        instruments_pnl = []
        for row in (summary_rows if summary_rows else []):
            # Skip summary/total rows (no asset category = aggregate rows)
            if not row.get('assetCategory', '').strip():
                continue
            total_usd = float(row.get('totalRealizedPnl', 0) or 0)
            if abs(total_usd) < 0.01:
                continue

            isin = row.get('isin', '').strip()
            sym  = row.get('symbol', '').strip()
            desc = row.get('description', '').strip()
            cat  = row.get('assetCategory', '')

            # Best human-readable label:
            # STK with ISIN: use clean ticker from financial_instruments lookup
            # OPT/BOND/BILL: use description (e.g. "AAPL 21FEB25 200 P", "DBR 0 08/15/52")
            if cat == 'STK' and isin and isin in isin_to_ticker:
                label = isin_to_ticker[isin]
            elif desc:
                label = desc[:30]
            elif sym:
                label = sym[:30]
            else:
                label = isin or '—'

            instruments_pnl.append({
                'ticker':   label,
                'category': cat,
                'pnl_eur':  total_usd * avg_rate,
                'pnl_usd':  total_usd,
            })

        instruments_pnl.sort(key=lambda x: x['pnl_eur'], reverse=True)
        top_gains  = [x for x in instruments_pnl if x['pnl_eur'] > 0][:5]
        top_losses = [x for x in reversed(instruments_pnl) if x['pnl_eur'] < 0][:5]

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
        # Top 5 for plausibility check
        "top_gains":  top_gains,
        "top_losses": top_losses,
        # Plausibility Metadata
        "audit": {
            "funds_processed": funds_processed,
            "funds_skipped_year": funds_skipped_year,
            "raw_div_base": raw_div_base,
            "raw_tax_base": raw_tax_base,
            "added_from_summary": added_from_summary,
            "usd_to_eur_rates_count": len(usd_to_eur_rates),
            "stillhalter_count": stillhalter_count,
            "stillhalter_premium_eur": stillhalter_premium_eur
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
    
    print("-" * 60)
    print("ZEILE 19 (Ausländische Kapitalerträge - NETTO):")
    print(f"    = Saldo Aktien + Saldo Sonstiges")
    print(f"    = {topf_1_aktien:,.2f} + {topf_2_sonstiges:,.2f}")
    print(f"    ═════════════════════════════════════")
    print(f"    ZEILE 19:              {zeile_19_netto:>12,.2f} EUR")
    
    print("-" * 60)
    print(f"ZEILE 20 (Davon: Aktiengewinne):   {zeile_20_stock_gains:>12,.2f} EUR")
    print(f"ZEILE 22 (Verluste ohne Aktien):   {zeile_22_other_losses:>12,.2f} EUR")
    print(f"ZEILE 23 (Aktienverluste):         {zeile_23_stock_losses:>12,.2f} EUR")
    print(f"ZEILE 41 (Quellensteuer):          {withholding_tax_eur:>12,.2f} EUR")
    
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

