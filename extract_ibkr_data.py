import xml.etree.ElementTree as ET
import csv
import os
import sys

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
        
        for row in rows:
            attrib = row.attrib
            headers.update(attrib.keys())
            # Extract ALL records
            record = attrib.copy()
            record['__source_section__'] = section_tag
            
            data_rows.append(record)


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

    # Extract AccountInformation (single element with base currency)
    acct_info = root.find('.//AccountInformation')
    if acct_info is not None:
        acct_data = acct_info.attrib.copy()
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
        
    parse_ibkr_xml(xml_file, output_dir)
