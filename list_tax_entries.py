import csv
import os
from datetime import datetime

def list_taxes():
    file_path = 'statement_of_funds.csv'
    if not os.path.exists(file_path):
        print("File not found")
        return
    
    codes = ['FRTAX', 'WHT', 'GlTx']
    unique_entries = {}
    
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('activityCode') in codes:
                tid = row.get('transactionID')
                if tid not in unique_entries:
                    unique_entries[tid] = row
                    
    sorted_entries = sorted(unique_entries.values(), key=lambda x: x.get('date', ''))
    
    print(f"{'#':>3} | {'Date':<10} | {'Amount':>10} | {'Curr':<4} | {'Description'}")
    print("-" * 80)
    
    total_local = 0.0
    for i, entry in enumerate(sorted_entries, 1):
        amount = float(entry.get('amount', 0))
        total_local += amount
        print(f"{i:>3} | {entry.get('date'):<10} | {amount:>10.2f} | {entry.get('currency'):<4} | {entry.get('activityDescription')[:50]}")
    
    print("-" * 80)
    print(f"Total entries: {len(sorted_entries)}")
    print(f"Total Amount (Mixed Currencies): {total_local:.2f}")

if __name__ == "__main__":
    list_taxes()
