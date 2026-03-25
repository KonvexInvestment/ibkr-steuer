# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

German tax report generator for Interactive Brokers (IBKR) accounts. Processes an IBKR Flex Query XML export and computes Anlage KAP figures (Zeilen 19, 20, 22, 23, 41) for German income tax returns (Steuerjahr 2025). Supports both USD and EUR base-currency accounts.

## Running the Scripts

All scripts use `.venv` (Python 3.9) in the project root.

```bash
source .venv/bin/activate

# Step 1: Extract CSVs from IBKR XML
python extract_ibkr_data.py [xml_file] [output_dir]

# Step 2: Calculate tax report
python calculate_tax_report.py [ib_tax_dir]

# Audit / inspection helpers
python audit_tax.py          # Withholding tax entries
python list_tax_entries.py   # All FRTAX/WHT/GlTx entries

# Streamlit GUI — must be run from PROJECT ROOT, not from gui_app/
streamlit run gui_app/app.py
```

## Architecture

**Data flow:**

1. `extract_ibkr_data.py` — Parses IBKR Flex Query XML → writes CSVs to output dir:
   - `trades.csv` (Trades section)
   - `statement_of_funds.csv` (StmtFunds — dividends, interest, withholding tax)
   - `pnl_summary.csv` (FIFOPerformanceSummaryInBase — per-instrument totals with readable descriptions)
   - `financial_instruments.csv` (instrument metadata: ISIN → symbol/description lookup)
   - `account_info.csv` (AccountInformation — base currency, account type)
   - `cash_transactions.csv`, `corporate_actions.csv`

2. `calculate_tax_report.py` — Core tax engine. Returns a `report_data` dict:
   - Reads `account_info.csv` to detect base currency (USD or EUR); defaults to USD for backward compatibility
   - Deduplicates trades (key: dateTime+ISIN+buySell+quantity+closePrice+fifoPnlRealized) and funds (key: transactionID)
   - **USD base:** Builds USD→EUR daily rate map from `fxRateToBase` on EUR-currency records; two-step conversion (trade currency → USD → EUR)
   - **EUR base:** `fifoPnlRealized × fxRateToBase` already gives EUR; no rate map needed; StmtFunds amounts are in EUR (BaseCurrency view)
   - Separates Stillhalterprämien from stock gains for short call assignments (BMF Rn. 25–35)
   - Falls back to `pnl_summary.csv` for instruments absent from `trades.csv` (e.g. T-Bill/Bond maturities); reads both ST and LT profit/loss fields
   - Builds `top_gains` / `top_losses` (top 5 each) using `pnl_summary.totalRealizedPnl` + ISIN→ticker lookup from `financial_instruments.csv`
   - `tax_year` parameter (default 2025) controls year filtering

3. `gui_app/app.py` — Streamlit UI. Uploads XML, runs both scripts in a `tempfile.TemporaryDirectory`, displays results. Uses `sys.path.append(parent)` to import the root-level modules.

## Key Data Quirks

**trades.csv:** `symbol` and `description` columns are **empty** for all rows. Use ISIN + `financial_instruments.csv` lookup for stock tickers. For options, use `pnl_summary.csv` `description` field (e.g. `"AAPL 21FEB25 200 P"`).

**statement_of_funds.csv:** Contains duplicate records (same `transactionID`) where the duplicate has `fxRateToBase=1` (IBKR's EUR-native booking of EUR-traded instruments). The first occurrence always has the correct rate; deduplication by transactionID handles this automatically.

**pnl_summary.csv:** Has one row per instrument plus a `"Total (All Assets)"` aggregate row with empty `assetCategory` — filter on `assetCategory != ''` to exclude it.

**EUR base-currency accounts:** All StmtFunds entries have `currency="EUR"` and `fxRateToBase=1` (BaseCurrency view — amounts pre-converted). USD trades have `fxRateToBase ≈ 0.86` (USD→EUR). Do NOT build a USD→EUR rate map for EUR-base accounts.

## Stillhalterprämien (Covered Call Assignments)

IBKR bundles the option premium into the stock's `fifoPnlRealized` and shows `fifoPnlRealized=0` on the option BookTrade for assigned short calls. The code detects and separates these:

**Detection:** `OPT` + `transactionType=BookTrade` + `buySell=BUY` + `putCall=C` + `fifoPnlRealized=0`

**Matching:** Finds the original `ExchTrade SELL` with same strike/expiry/putCall → calculates premium from `closePrice × multiplier × quantity`

**Action:** Subtracts premium from `stocks_gain` (Topf 1), adds to `options_gain` (Topf 2)

**Short put assignments are NOT separated** — per BMF Rn. 31, the premium reduces the stock's Anschaffungskosten (cost basis), which IBKR handles correctly via FIFO.

## Key Tax Logic Notes

- `fxRateToBase` converts trade currency → base currency. For USD base: inverted (`1/rate`) to get EUR-per-USD. For EUR base: used directly
- `fifoPnlRealized` is in the trade's local currency → `× fxRateToBase` → base currency → (if USD base) `× daily rate` → EUR
- `stocks_loss` and `options_loss` are negative; Zeile 22/23 use `abs()` (form expects positive)
- `options_gain`/`options_loss` covers OPT, FUT, FOP, BILL, BOND + separated Stillhalterprämien — misleading name, correct pool assignment
- INTP (Stückzinsen paid) is negative → reduces `interest_eur`; can go negative overall
- PIL (Payment in Lieu) netted with dividends

## German Tax Law Status (Steuerjahr 2025)

**§20 Abs. 6 Satz 5 EStG (Termingeschäfte €20k cap):** Abolished by Jahressteuergesetz 2024 (BGBl. 5.12.2024), retroactive for all open cases. No cap is applied — correct for 2025.

**Anlage KAP 2025 form:** Zeilen 9, 14 (Stillhalterprämien/Termingeschäfte) exist only in the inländischer-Steuerabzug section (Z7–15). For IBKR (ausländischer Broker, kein Steuerabzug) the correct section is Zeilen 18–23. Code uses Zeilen 19, 20, 22, 23, 41 — verified against official form.

**Two-Töpfe structure (§20 Abs. 6 EStG, BMF Rn. 118):**
- Topf 1 (Aktien): STK only; stock losses offset only stock gains (FA applies restriction via Z20/Z23)
- Topf 2 (Sonstiges): OPT, FUT, FOP, BILL, BOND, dividends, interest, Stillhalterprämien; freely offsettable

**BMF-Schreiben reference:** 14.05.2025, "Einzelfragen zur Abgeltungsteuer" (IV C 1 - S 2252/00075/016/070). Key Randnummern: Rn. 9–47 (Termingeschäfte), Rn. 25–35 (Stillhalter), Rn. 48–58 (Kapitalforderungen), Rn. 118–123 (Verlustverrechnung).

## Data Files (local only, not in repo)

- IBKR Flex Query XML (source of truth, covers 2025-01-01 to 2025-12-31)
- `trades.csv`, `statement_of_funds.csv`, `pnl_summary.csv`, `financial_instruments.csv`, `account_info.csv` — extracted CSVs
- `Grundlage/` — BMF-Schreiben PDF, Anlage KAP 2025 Formular (reference documents)
