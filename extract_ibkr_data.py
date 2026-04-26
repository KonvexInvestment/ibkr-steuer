import xml.etree.ElementTree as ET
import csv
import os
import sys

FX_FIELDS = ['date', 'settleDate', 'currency', 'fxRateToBase', 'activityCode',
              'activityDescription', 'amount', 'debit', 'credit', 'balance',
              'transactionID', 'levelOfDetail', 'assetCategory', 'symbol',
              'buySell', 'tradeQuantity', 'tradePrice', 'tradeGross',
              'tradeCommission']


def extract_conversion_rates(root):
    """Extract ConversionRate elements (official IBKR daily rates) from XML."""
    rows = []
    for cr in root.findall('.//ConversionRate'):
        rows.append({
            'reportDate': cr.get('reportDate', ''),
            'fromCurrency': cr.get('fromCurrency', ''),
            'toCurrency': cr.get('toCurrency', ''),
            'rate': cr.get('rate', ''),
        })
    return rows


def extract_fx_from_root(root, base_curr, fx_fields=None):
    """Extract FX transactions from a parsed XML root element."""
    if fx_fields is None:
        fx_fields = FX_FIELDS
    stmtfunds_node = root.find('.//StmtFunds')
    if stmtfunds_node is None:
        return []
    fx_rows = []
    for row in stmtfunds_node:
        attrib = row.attrib
        if attrib.get('levelOfDetail') != 'Currency':
            continue
        if attrib.get('currency') == base_curr:
            continue
        record = {k: attrib.get(k, '') for k in fx_fields}
        fx_rows.append(record)
    return fx_rows


def extract_trades_from_root(root):
    """Extract EXECUTION-level trades from a parsed XML root element."""
    trades_node = root.find('.//Trades')
    if trades_node is None:
        return [], set()
    headers = set()
    rows = []
    for row in trades_node:
        attrib = row.attrib
        lod = attrib.get('levelOfDetail', '')
        if lod and lod != 'EXECUTION':
            continue
        headers.update(attrib.keys())
        rows.append(attrib.copy())
    return rows, headers


def extract_fx_multi_xml(xml_files, output_dir):
    """Extract and merge FX transactions and trades from multiple XML files (multi-year).

    The main XML (last file) is used for all standard sections (trades, funds, etc.).
    FX transactions and trades are merged from ALL files for complete FIFO lot history
    and Stillhalter matching across years.
    """
    if not xml_files:
        return

    # Detect tax year from XML content (FlexStatement toDate) to find the main XML
    def get_xml_end_date(path):
        try:
            t = ET.parse(path)
            stmt = t.getroot().find('.//FlexStatement')
            if stmt is not None:
                return stmt.attrib.get('toDate', '')
        except Exception:
            pass
        return ''

    xml_files = sorted(xml_files, key=lambda p: get_xml_end_date(p))
    main_xml = xml_files[-1]  # Latest end date = tax year
    history_xmls = xml_files[:-1]

    print(f"Multi-XML: {len(xml_files)} Dateien, Haupt-XML: {os.path.basename(main_xml)}")

    # 1. Parse main XML normally (all standard sections)
    parse_ibkr_xml(main_xml, output_dir)

    # 2. Detect base currency from main XML
    tree = ET.parse(main_xml)
    root = tree.getroot()
    acct = root.find('.//AccountInformation')
    base_curr = acct.attrib.get('currency', 'EUR') if acct is not None else 'EUR'

    # 3. Merge trades from history XMLs into trades.csv (for Stillhalter matching)
    if history_xmls:
        # Load existing trades from main XML
        trades_path = os.path.join(output_dir, 'trades.csv')
        existing_trades = []
        existing_headers = set()
        if os.path.exists(trades_path):
            with open(trades_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                existing_headers = set(reader.fieldnames or [])
                existing_trades = list(reader)

        # Build dedup keys from existing trades
        def trade_dedup_key(t):
            return (t.get('dateTime', ''), t.get('isin', ''), t.get('buySell', ''),
                    t.get('quantity', ''), t.get('closePrice', ''), t.get('fifoPnlRealized', ''))

        existing_keys = {trade_dedup_key(t) for t in existing_trades}
        added_history_trades = 0

        for xml_path in history_xmls:
            try:
                t = ET.parse(xml_path)
                r = t.getroot()
                rows, hdrs = extract_trades_from_root(r)
                existing_headers.update(hdrs)
                for row in rows:
                    key = trade_dedup_key(row)
                    if key not in existing_keys:
                        existing_keys.add(key)
                        row['__source_section__'] = 'Trades'
                        existing_trades.append(row)
                        added_history_trades += 1
            except Exception as e:
                print(f"  FEHLER bei Trades aus {xml_path}: {e}")

        if added_history_trades > 0:
            existing_headers.add('__source_section__')
            sorted_h = sorted(existing_headers)
            with open(trades_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted_h)
                writer.writeheader()
                writer.writerows(existing_trades)
            print(f"  Trades: {added_history_trades} historische Trades aus Vorjahren hinzugefügt (für Stillhalter-Matching)")

    # 4. Merge FX transactions from ALL XMLs
    all_fx = []
    for xml_path in xml_files:
        try:
            t = ET.parse(xml_path)
            r = t.getroot()
            rows = extract_fx_from_root(r, base_curr)
            # Tag the source file for debugging
            from_date = ''
            stmt = r.find('.//FlexStatement')
            if stmt is not None:
                from_date = stmt.attrib.get('fromDate', '')
            print(f"  {os.path.basename(xml_path)}: {len(rows)} FX-Einträge (ab {from_date})")
            all_fx.extend(rows)
        except Exception as e:
            print(f"  FEHLER bei {xml_path}: {e}")

    # Sort chronologically and deduplicate by transactionID
    all_fx.sort(key=lambda x: x.get('date', ''))
    seen_ids = set()
    deduped = []
    for row in all_fx:
        tid = row.get('transactionID', '')
        desc = row.get('activityDescription', '')
        if desc == 'Starting Balance':
            # Only keep the earliest Starting Balance per currency
            key = ('SB', row.get('currency', ''), row.get('date', ''))
        elif tid:
            key = tid
        else:
            key = (row.get('date'), row.get('currency'), row.get('amount'))
        if key in seen_ids:
            continue
        seen_ids.add(key)
        deduped.append(row)

    # Only keep the EARLIEST Starting Balance per currency (from earliest XML)
    sb_seen = set()
    final_rows = []
    for row in deduped:
        if row.get('activityDescription') == 'Starting Balance':
            curr = row.get('currency', '')
            if curr in sb_seen:
                continue
            sb_seen.add(curr)
        final_rows.append(row)

    # Write merged FX transactions
    fx_path = os.path.join(output_dir, 'fx_transactions.csv')
    sorted_h = sorted(FX_FIELDS)
    with open(fx_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=sorted_h)
        writer.writeheader()
        writer.writerows(final_rows)
    print(f"Saved {len(final_rows)} merged FX transactions to {fx_path} (aus {len(xml_files)} XMLs)")

    # 5. Merge ConversionRates from ALL XMLs (for Tageskurs-Korrektur across years)
    all_conv = []
    for xml_path in xml_files:
        try:
            t = ET.parse(xml_path)
            r = t.getroot()
            rows = extract_conversion_rates(r)
            all_conv.extend(rows)
        except Exception:
            pass
    # Deduplicate: keep first occurrence per (reportDate, fromCurrency, toCurrency)
    seen_conv = set()
    deduped_conv = []
    for row in all_conv:
        key = (row['reportDate'], row['fromCurrency'], row['toCurrency'])
        if key not in seen_conv:
            seen_conv.add(key)
            deduped_conv.append(row)
    if deduped_conv:
        cr_path = os.path.join(output_dir, 'conversion_rates.csv')
        with open(cr_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['reportDate', 'fromCurrency', 'toCurrency', 'rate'])
            writer.writeheader()
            writer.writerows(deduped_conv)
        print(f"Saved {len(deduped_conv)} merged ConversionRates to {cr_path} (aus {len(xml_files)} XMLs)")


def extract_quarterly_xmls(xml_files, output_dir):
    """Merge multiple quarterly XMLs (same account, same year) into combined CSVs.

    Unlike extract_fx_multi_xml (which only merges trades + FX from history),
    this merges ALL sections from all XMLs for same-year quarterly exports.
    """
    if not xml_files:
        return

    # Sort by fromDate (Q1 first)
    def get_from_date(path):
        try:
            stmt = ET.parse(path).getroot().find('.//FlexStatement')
            return stmt.get('fromDate', '') if stmt is not None else ''
        except Exception:
            return ''

    xml_files = sorted(xml_files, key=get_from_date)
    print(f"Quartals-Merge: {len(xml_files)} XMLs")

    # Collectors
    trades_seen, trades_rows, trades_headers = set(), [], set()
    lots_seen, lots_rows, lots_headers = set(), [], set()
    funds_seen, funds_rows, funds_headers = set(), [], set()
    instruments_seen, instruments_rows, instruments_headers = set(), [], set()
    cash_seen, cash_rows, cash_headers = set(), [], set()
    corp_seen, corp_rows, corp_headers = set(), [], set()
    pnl_agg, pnl_headers = {}, set()
    fx_pnl_seen, fx_pnl_rows = set(), []
    all_fx_currency = []
    all_conv_rates = []
    earliest_sb_date = None
    base_curr = 'EUR'
    tax_year = None
    acct_data = None
    fx_trans_count = 0

    for xml_path in xml_files:
        try:
            root = ET.parse(xml_path).getroot()
        except Exception as e:
            print(f"  FEHLER beim Parsen von {xml_path}: {e}")
            continue

        stmt = root.find('.//FlexStatement')
        from_date = stmt.get('fromDate', '') if stmt is not None else ''
        to_date = stmt.get('toDate', '') if stmt is not None else ''
        print(f"  {os.path.basename(xml_path)}: {from_date} – {to_date}")

        # AccountInfo (from first XML)
        acct = root.find('.//AccountInformation')
        if acct is not None and acct_data is None:
            acct_data = acct.attrib.copy()
            base_curr = acct_data.get('currency', 'EUR')
        if to_date and (tax_year is None or to_date > tax_year):
            tax_year = to_date[:4]

        # ── Trades (EXECUTION + Lot/CLOSED_LOT) ──
        trades_node = root.find('.//Trades')
        if trades_node is not None:
            for row in trades_node:
                attrib = row.attrib
                lod = attrib.get('levelOfDetail', '')

                # Lot / CLOSED_LOT
                if lod == 'CLOSED_LOT' or row.tag == 'Lot':
                    key = (attrib.get('symbol', ''), attrib.get('openDateTime', ''),
                           attrib.get('dateTime', ''), attrib.get('quantity', ''))
                    lots_headers.update(attrib.keys())
                    if key not in lots_seen:
                        lots_seen.add(key)
                        lots_rows.append(attrib.copy())
                    continue

                # Only Trade elements with EXECUTION
                if row.tag != 'Trade':
                    continue
                if lod and lod != 'EXECUTION':
                    continue

                tid = attrib.get('tradeID', '')
                key = tid if tid else (attrib.get('dateTime', ''), attrib.get('isin', ''),
                                       attrib.get('buySell', ''), attrib.get('quantity', ''),
                                       attrib.get('closePrice', ''))
                if key not in trades_seen:
                    trades_seen.add(key)
                    trades_headers.update(attrib.keys())
                    rec = attrib.copy()
                    rec['__source_section__'] = 'Trades'
                    trades_rows.append(rec)

        # ── StmtFunds ──
        stmtfunds = root.find('.//StmtFunds')
        if stmtfunds is not None:
            for row in stmtfunds:
                attrib = row.attrib
                act = attrib.get('activityDescription', '')

                # Starting Balance: only keep from earliest XML (Q1)
                if act == 'Starting Balance':
                    date = attrib.get('date', '')
                    if earliest_sb_date is None:
                        earliest_sb_date = date
                    if date != earliest_sb_date:
                        continue

                tid = attrib.get('transactionID', '')
                if tid:
                    key = (tid, act)
                else:
                    key = (act, attrib.get('date', ''), attrib.get('currency', ''),
                           attrib.get('balance', ''), attrib.get('amount', ''))
                if key not in funds_seen:
                    funds_seen.add(key)
                    funds_headers.update(attrib.keys())
                    rec = attrib.copy()
                    rec['__source_section__'] = 'StmtFunds'
                    funds_rows.append(rec)

        # ── SecuritiesInfo ──
        secinfo = root.find('.//SecuritiesInfo')
        if secinfo is not None:
            for row in secinfo:
                attrib = row.attrib
                key = attrib.get('isin', '') or attrib.get('conid', '') or attrib.get('symbol', '')
                if key not in instruments_seen:
                    instruments_seen.add(key)
                    instruments_headers.update(attrib.keys())
                    rec = attrib.copy()
                    rec['__source_section__'] = 'SecuritiesInfo'
                    instruments_rows.append(rec)

        # ── CashTransactions ──
        ct_node = root.find('.//CashTransactions')
        if ct_node is not None:
            for row in ct_node:
                attrib = row.attrib
                key = (attrib.get('transactionID', ''), attrib.get('dateTime', ''),
                       attrib.get('type', ''))
                if key not in cash_seen:
                    cash_seen.add(key)
                    cash_headers.update(attrib.keys())
                    rec = attrib.copy()
                    rec['__source_section__'] = 'CashTransactions'
                    cash_rows.append(rec)

        # ── CorporateActions ──
        ca_node = root.find('.//CorporateActions')
        if ca_node is not None:
            for row in ca_node:
                attrib = row.attrib
                key = (attrib.get('transactionID', ''), attrib.get('dateTime', ''))
                if key not in corp_seen:
                    corp_seen.add(key)
                    corp_headers.update(attrib.keys())
                    rec = attrib.copy()
                    rec['__source_section__'] = 'CorporateActions'
                    corp_rows.append(rec)

        # ── FIFOPerformanceSummaryInBase (aggregate per instrument) ──
        pnl_node = root.find('.//FIFOPerformanceSummaryInBase')
        if pnl_node is not None:
            for row in pnl_node:
                attrib = row.attrib
                pnl_headers.update(attrib.keys())
                ac = attrib.get('assetCategory', '')
                sym = attrib.get('symbol', '')
                key = (ac, sym)
                if key not in pnl_agg:
                    pnl_agg[key] = attrib.copy()
                else:
                    # Sum numeric PnL fields across quarters
                    # Both "total" prefixed (used by top_gains/top_losses) and
                    # unprefixed (used by BILL/BOND fallback in calculate_tax_report)
                    for field in ('totalRealizedPnl', 'totalRealizedSTPnl', 'totalRealizedLTPnl',
                                  'totalRealizedSTProfit', 'totalRealizedSTLoss',
                                  'totalRealizedLTProfit', 'totalRealizedLTLoss',
                                  'realizedSTProfit', 'realizedLTProfit',
                                  'realizedSTLoss', 'realizedLTLoss'):
                        if field in attrib or field in pnl_agg[key]:
                            old = float(pnl_agg[key].get(field, '0') or '0')
                            new = float(attrib.get(field, '0') or '0')
                            pnl_agg[key][field] = str(old + new)

        # ── FxTransactions (realized PnL) ──
        fxt = root.find('.//FxTransactions')
        if fxt is not None:
            fx_trans_count += len(list(fxt))
            for elem in fxt:
                if elem.get('levelOfDetail') == 'TRANSACTION' and elem.get('realizedPL'):
                    key = (elem.get('dateTime', ''), elem.get('fxCurrency', ''),
                           elem.get('quantity', ''))
                    if key not in fx_pnl_seen:
                        fx_pnl_seen.add(key)
                        fx_pnl_rows.append({f: elem.get(f, '') for f in
                            ['reportDate', 'dateTime', 'functionalCurrency', 'fxCurrency',
                             'activityDescription', 'quantity', 'proceeds', 'cost',
                             'realizedPL', 'code', 'levelOfDetail']})

        # ── FX currency transactions + ConversionRates ──
        all_fx_currency.extend(extract_fx_from_root(root, base_curr))
        all_conv_rates.extend(extract_conversion_rates(root))

    # ═══════════════════════════════════════════════════════════════════════
    # Write all merged CSVs
    # ═══════════════════════════════════════════════════════════════════════

    def _write_csv(filename, rows, headers):
        if not rows:
            return
        h = set(headers)
        for r in rows:
            h.update(r.keys())
        path = os.path.join(output_dir, filename)
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, sorted(h))
            writer.writeheader()
            writer.writerows(rows)
        print(f"  Saved {len(rows)} rows to {filename}")

    # Trades
    trades_headers.add('__source_section__')
    _write_csv('trades.csv', trades_rows, trades_headers)

    # CLOSED_LOT
    if lots_rows:
        _write_csv('closed_lots.csv', lots_rows, lots_headers)

    # StmtFunds
    funds_headers.add('__source_section__')
    _write_csv('statement_of_funds.csv', funds_rows, funds_headers)

    # SecuritiesInfo / financial_instruments
    instruments_headers.add('__source_section__')
    _write_csv('financial_instruments.csv', instruments_rows, instruments_headers)

    # CashTransactions
    cash_headers.add('__source_section__')
    _write_csv('cash_transactions.csv', cash_rows, cash_headers)

    # CorporateActions
    corp_headers.add('__source_section__')
    _write_csv('corporate_actions.csv', corp_rows, corp_headers)

    # PnL Summary (aggregated)
    pnl_rows = list(pnl_agg.values())
    for r in pnl_rows:
        r['__source_section__'] = 'FIFOPerformanceSummaryInBase'
    pnl_headers.add('__source_section__')
    _write_csv('pnl_summary.csv', pnl_rows, pnl_headers)

    # FX realized PnL
    if fx_pnl_rows:
        fx_pnl_fields = ['reportDate', 'dateTime', 'functionalCurrency', 'fxCurrency',
                         'activityDescription', 'quantity', 'proceeds', 'cost',
                         'realizedPL', 'code', 'levelOfDetail']
        fx_pnl_path = os.path.join(output_dir, 'fx_realized_pnl.csv')
        with open(fx_pnl_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fx_pnl_fields)
            writer.writeheader()
            writer.writerows(fx_pnl_rows)
        total_pnl = sum(float(r['realizedPL']) for r in fx_pnl_rows)
        print(f"  Saved {len(fx_pnl_rows)} FX realized PnL entries (Total: {total_pnl:,.2f})")

    # FX currency transactions (dedup, earliest Starting Balance only)
    all_fx_currency.sort(key=lambda x: x.get('date', ''))
    sb_seen_curr = set()
    seen_fx_keys = set()
    final_fx = []
    for row in all_fx_currency:
        desc = row.get('activityDescription', '')
        tid = row.get('transactionID', '')
        if desc == 'Starting Balance':
            curr = row.get('currency', '')
            if curr in sb_seen_curr:
                continue
            sb_seen_curr.add(curr)
            key = ('SB', curr)
        elif tid:
            key = tid
        else:
            key = (row.get('date'), row.get('currency'), row.get('amount'))
        if key not in seen_fx_keys:
            seen_fx_keys.add(key)
            final_fx.append(row)

    if final_fx:
        fx_path = os.path.join(output_dir, 'fx_transactions.csv')
        sorted_h = sorted(FX_FIELDS)
        with open(fx_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted_h)
            writer.writeheader()
            writer.writerows(final_fx)
        print(f"  Saved {len(final_fx)} merged FX transactions")

    # ConversionRates (dedup)
    seen_conv = set()
    deduped_conv = []
    for row in all_conv_rates:
        key = (row['reportDate'], row['fromCurrency'], row['toCurrency'])
        if key not in seen_conv:
            seen_conv.add(key)
            deduped_conv.append(row)
    if deduped_conv:
        cr_path = os.path.join(output_dir, 'conversion_rates.csv')
        with open(cr_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['reportDate', 'fromCurrency', 'toCurrency', 'rate'])
            writer.writeheader()
            writer.writerows(deduped_conv)
        print(f"  Saved {len(deduped_conv)} merged ConversionRates")

    # AccountInfo
    if acct_data:
        acct_data['tax_year'] = tax_year or ''
        acct_data['fx_transactions_count'] = str(fx_trans_count)
        acct_path = os.path.join(output_dir, 'account_info.csv')
        with open(acct_path, 'w', newline='', encoding='utf-8') as f:
            headers = sorted(acct_data.keys())
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerow(acct_data)

    print(f"Quartals-Merge abgeschlossen: {len(trades_rows)} Trades, {len(funds_rows)} StmtFunds, "
          f"{len(fx_pnl_rows)} FX-PnL, {len(deduped_conv)} ConversionRates")


def parse_ibkr_xml(xml_file_path, output_dir):
    print(f"Parsing {xml_file_path}...")
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return

    # Define sections to extract
    # Mapping: XML Tag -> Output Filename
    sections = {
        'Trades': 'trades.csv',
        'CashTransactions': 'cash_transactions.csv',
        'CorporateActions': 'corporate_actions.csv',
        'SecuritiesInfo': 'financial_instruments.csv',
        'StmtFunds': 'statement_of_funds.csv',
        'FIFOPerformanceSummaryInBase': 'pnl_summary.csv'
    }
    
    closed_lot_rows = []  # CLOSED_LOT data for per-lot FX correction

    for section_tag, filename in sections.items():
        section_node = root.find(f'.//{section_tag}')
        
        if section_node is None:
            print(f"Section <{section_tag}> not found. Skipping.")
            continue
            
        print(f"Processing section: {section_tag}")
        
        # Get all children (rows)
        rows = list(section_node)
        if not rows:
            print(f"No rows found in {section_tag}")
            continue
            
        # Collect headers from all keys in all rows to handle optional attributes
        headers = set()
        data_rows = []
        skipped = 0

        for row in rows:
            attrib = row.attrib

            # For Trades: only keep EXECUTION-level rows (real trades).
            # CLOSED_LOT rows are saved separately for per-lot FX correction.
            if section_tag == 'Trades':
                lod = attrib.get('levelOfDetail', '')
                if lod == 'CLOSED_LOT':
                    closed_lot_rows.append(attrib.copy())
                    skipped += 1
                    continue
                elif lod and lod != 'EXECUTION':
                    skipped += 1
                    continue

            headers.update(attrib.keys())
            record = attrib.copy()
            record['__source_section__'] = section_tag

            data_rows.append(record)

        if skipped:
            print(f"  Filtered {skipped} non-EXECUTION rows from {section_tag}")


        # Write to CSV
        output_path = os.path.join(output_dir, filename)
        # Sort headers for consistency
        headers.add('__source_section__')
        sorted_headers = sorted(list(headers))
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted_headers)
            writer.writeheader()
            writer.writerows(data_rows)
            
        print(f"Saved {len(data_rows)} rows to {output_path}")

    # Extract FX transactions from StmtFunds (levelOfDetail="Currency", non-base currency)
    # These are needed for FIFO-based foreign currency gain/loss calculation
    acct_info_node = root.find('.//AccountInformation')
    base_curr = acct_info_node.attrib.get('currency', 'EUR') if acct_info_node is not None else 'EUR'

    fx_fields = ['date', 'settleDate', 'currency', 'fxRateToBase', 'activityCode',
                  'activityDescription', 'amount', 'debit', 'credit', 'balance',
                  'transactionID', 'levelOfDetail', 'assetCategory', 'symbol',
                  'buySell', 'tradeQuantity', 'tradePrice', 'tradeGross',
                  'tradeCommission']

    fx_rows = extract_fx_from_root(root, base_curr, fx_fields)

    if fx_rows:
        fx_path = os.path.join(output_dir, 'fx_transactions.csv')
        sorted_h = sorted(fx_fields)
        with open(fx_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted_h)
            writer.writeheader()
            writer.writerows(fx_rows)
        print(f"Saved {len(fx_rows)} FX transactions to {fx_path}")

    # Save CLOSED_LOT data for per-lot FX correction
    if closed_lot_rows:
        cl_path = os.path.join(output_dir, 'closed_lots.csv')
        cl_fields = set()
        for r in closed_lot_rows:
            cl_fields.update(r.keys())
        sorted_cl = sorted(cl_fields)
        with open(cl_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted_cl)
            writer.writeheader()
            writer.writerows(closed_lot_rows)
        print(f"Saved {len(closed_lot_rows)} CLOSED_LOT rows to {cl_path}")

    # Extract ConversionRates (official IBKR daily rates) — for accurate Tageskurs-Korrektur
    # ConversionRate is IBKR's official daily exchange rate, distinct from the BookTrade
    # settlement rate (16:20). BookTrade rates can differ by up to 1.7 cents.
    conv_rates_rows = extract_conversion_rates(root)
    if conv_rates_rows:
        cr_path = os.path.join(output_dir, 'conversion_rates.csv')
        with open(cr_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['reportDate', 'fromCurrency', 'toCurrency', 'rate'])
            writer.writeheader()
            writer.writerows(conv_rates_rows)
        print(f"Saved {len(conv_rates_rows)} ConversionRates to {cr_path}")

    # Extract MTM Performance Summary for CASH (FX positions) — plausibility reference
    mtm_section = root.find('.//MTMPerformanceSummaryInBase')
    if mtm_section is not None:
        mtm_rows = []
        mtm_headers = set()
        for row in mtm_section:
            attrib = row.attrib
            if attrib.get('assetCategory') != 'CASH':
                continue
            if attrib.get('symbol') == base_curr:
                continue
            record = attrib.copy()
            mtm_headers.update(record.keys())
            mtm_rows.append(record)

        if mtm_rows:
            mtm_path = os.path.join(output_dir, 'fx_mtm_summary.csv')
            sorted_h = sorted(mtm_headers)
            with open(mtm_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted_h)
                writer.writeheader()
                writer.writerows(mtm_rows)
            print(f"Saved {len(mtm_rows)} FX MTM summary rows to {mtm_path}")

    # Extract fxTranslationGainLoss from CashReportCurrency (IBKR's own FX PnL calc)
    cash_report = root.find('.//CashReportCurrency[@levelOfDetail="BaseCurrency"][@currency="BASE_SUMMARY"]')
    if cash_report is None:
        # Try iterating
        for cr in root.iter('CashReportCurrency'):
            if cr.attrib.get('currency') == 'BASE_SUMMARY' and cr.attrib.get('levelOfDetail') == 'BaseCurrency':
                cash_report = cr
                break
    if cash_report is not None:
        fx_tgl = cash_report.attrib.get('fxTranslationGainLoss', '0')
        fx_tgl_path = os.path.join(output_dir, 'fx_translation.csv')
        with open(fx_tgl_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['fxTranslationGainLoss'])
            writer.writeheader()
            writer.writerow({'fxTranslationGainLoss': fx_tgl})
        print(f"Saved fxTranslationGainLoss: {fx_tgl} to {fx_tgl_path}")

    # Detect tax year from FlexStatement period
    flex_stmt = root.find('.//FlexStatement')
    tax_year_detected = None
    if flex_stmt is not None:
        to_date = flex_stmt.get('toDate', '')
        if to_date and len(to_date) >= 4:
            tax_year_detected = to_date[:4]
            print(f"Steuerjahr erkannt: {tax_year_detected} (Zeitraum: {flex_stmt.get('fromDate', '?')} – {to_date})")

    # Detect and extract FxTransactions (IBKR's own FIFO PnL per FX event)
    fx_trans_node = root.find('.//FxTransactions')
    fx_trans_count = len(list(fx_trans_node)) if fx_trans_node is not None else -1
    if fx_trans_count > 0:
        print(f"FxTransactions: {fx_trans_count} Einträge gefunden (IBKR-internes FIFO)")
        fx_pnl_fields = ['reportDate', 'dateTime', 'functionalCurrency', 'fxCurrency',
                         'activityDescription', 'quantity', 'proceeds', 'cost', 'realizedPL',
                         'code', 'levelOfDetail']
        fx_pnl_rows = []
        for elem in fx_trans_node:
            row = {field: elem.get(field, '') for field in fx_pnl_fields}
            if row.get('levelOfDetail') == 'TRANSACTION' and row.get('realizedPL'):
                fx_pnl_rows.append(row)
        if fx_pnl_rows:
            fx_pnl_path = os.path.join(output_dir, 'fx_realized_pnl.csv')
            with open(fx_pnl_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fx_pnl_fields)
                writer.writeheader()
                writer.writerows(fx_pnl_rows)
            total_pnl = sum(float(r['realizedPL']) for r in fx_pnl_rows)
            print(f"Saved {len(fx_pnl_rows)} FX realized PnL entries to {fx_pnl_path} (Total: {total_pnl:,.2f} EUR)")
    elif fx_trans_count == 0:
        print("FxTransactions: Sektion vorhanden aber leer (Flex Query Konfiguration prüfen)")
    else:
        print("FxTransactions: Sektion nicht vorhanden")

    # Extract AccountInformation (single element with base currency)
    acct_info = acct_info_node
    if acct_info is not None:
        acct_data = acct_info.attrib.copy()
        acct_data['fx_transactions_count'] = str(fx_trans_count)
        if tax_year_detected:
            acct_data['tax_year'] = tax_year_detected
        acct_path = os.path.join(output_dir, 'account_info.csv')
        with open(acct_path, 'w', newline='', encoding='utf-8') as f:
            headers = sorted(acct_data.keys())
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerow(acct_data)
        print(f"Saved account info (base currency: {acct_data.get('currency', '?')}) to {acct_path}")

if __name__ == "__main__":
    if len(sys.argv) > 2:
        xml_file = sys.argv[1]
        output_dir = sys.argv[2]
    else:
        xml_file = "input.xml"
        output_dir = "./"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Check for --history / --fx-history flag: additional XMLs for FX lots + Stillhalter matching
    history_files = []
    remaining_args = sys.argv[3:]
    for flag in ('--history', '--fx-history'):
        if flag in remaining_args:
            idx = remaining_args.index(flag)
            history_files = remaining_args[idx + 1:]
            break

    if history_files:
        # Multi-XML mode: main XML + history files
        all_xmls = history_files + [xml_file]
        extract_fx_multi_xml(all_xmls, output_dir)
    else:
        parse_ibkr_xml(xml_file, output_dir)
