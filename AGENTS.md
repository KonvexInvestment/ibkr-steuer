# Repository Guidelines

## Project Structure & Module Organization

This repository is a Python/Streamlit tool for calculating German Anlage KAP/KAP-INV values from Interactive Brokers Flex Query exports.

- `app.py` is the Streamlit UI and user-facing orchestration layer.
- `calculate_tax_report.py` contains the main tax calculation logic.
- `extract_ibkr_data.py` converts IBKR XML exports into CSV inputs.
- `etf_classification.py` maintains the InvStG fund classification table (Teilfreistellung rates) and documented treaty withholding-tax rates; `ecb_rates.py` and helper scripts provide FX, audit, and comparison utilities.
- `tests/` contains focused regression tests; `test_data/` is local and gitignored because it may contain real IBKR data.
- `Grundlage/` stores reference tax PDFs; root CSV/TXT/XML files are sample, generated, or local working data unless explicitly tracked.

## Build, Test, and Development Commands

Create a local environment and install the UI dependency:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install streamlit openpyxl
```

`openpyxl` ist optional, aber notwendig für den Excel-Export der Trade-Details. Ohne `openpyxl` läuft der Rest der App weiter, nur der Download-Button warnt.

Run the local app:

```bash
streamlit run app.py
```

Run the main regression runner:

```bash
python run_tests.py
```

Run individual synthetic tests while iterating (`run_tests.py` runs all files in `tests/` automatically at the end of a full run):

```bash
python tests/test_cross_year_series.py
python tests/test_kap_inv_wht.py
python -m unittest tests/test_german_dividend_tax.py
```

Extract IBKR XML data for manual checks:

```bash
python extract_ibkr_data.py <flex_query_export>.xml /tmp/ibkr_extract
```

## Coding Style & Naming Conventions

Use Python 3 with 4-space indentation and standard library modules where practical. Follow existing names: functions and variables use `snake_case`, constants use `UPPER_SNAKE_CASE`, and tax-form fields keep their `zeile_19` style identifiers. Keep comments short and useful, especially around tax-law edge cases, FIFO behavior, and cross-year option handling. No formatter or linter is currently configured; keep changes PEP 8 compatible and avoid broad rewrites.

## Testing Guidelines

Add regression coverage for tax logic changes, especially around realized gains, withholding tax crediting, FX conversion, ETF classification (KAP-INV form mapping), and cross-year Stillhalter handling. Prefer small synthetic fixtures in `tests/` for reproducible bugs; new synthetic test files must be registered in `SYNTHETIC_TESTS` in `run_tests.py` so the full run picks them up. `run_tests.py` also uses local `test_data/audit_expectations.json`; if unavailable, some audit scenarios cannot run and may be skipped or fail early.

## Commit & Pull Request Guidelines

Recent commits use concise imperative messages, sometimes with issue references, for example `Fix option matching across underlyings` or `Issue #44: Magic Fallback-Rate 0.95 durch RuntimeError ersetzen`. Keep commits focused on one behavior change. Pull requests should describe the tax scenario changed, list commands run, link related issues, and include UI screenshots when `app.py` output or layout changes.

## Security & Configuration Tips

Do not commit real IBKR XML, extracted CSVs, personal TXT reports, virtualenvs, or `test_data/`; these are covered by `.gitignore`. Treat financial exports as sensitive and keep processing local unless the user explicitly requests otherwise.
