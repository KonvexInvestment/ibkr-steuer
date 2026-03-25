import csv
import os

def audit_tax():
    file_path = 'statement_of_funds.csv'
    if not os.path.exists(file_path):
        print("File not found")
        return
    
    codes = ['FRTAX', 'WHT', 'GlTx']
    results = []
    unique_keys = set()
    duplicates_info = []
    
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            tid = row.get('transactionID')
            key = tid if tid else tuple(row.items())
            
            if key in unique_keys:
                duplicates_info.append(row)
                continue
            
            unique_keys.add(key)
            code = row.get('activityCode', '')
            if code in codes:
                results.append(row)
                
    print(f"Total records in CSV: {len(unique_keys) + len(duplicates_info)}")
    print(f"Unique records: {len(unique_keys)}")
    print(f"Duplicates skipped: {len(duplicates_info)}")
    print(f"Total target entries after deduplication: {len(results)}")
    
    total_tax = 0
    for r in results:
        total_tax += abs(float(r.get('amount', 0)))
        
    print(f"\nSum of deduplicated tax amounts (in local currency): {total_tax:.2f}")
    
    # Check for specific suspicious entries
    if results:
        print("\nFirst 5 unique tax entries:")
        for r in results[:5]:
            print(f"TID: {r.get('transactionID')}, Date: {r.get('date')}, Amount: {r.get('amount')}, Desc: {r.get('activityDescription')}")

if __name__ == "__main__":
    audit_tax()
