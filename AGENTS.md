# Repository Guidelines

## Project Structure & Module Organization

This repository is a Python/Streamlit tool for calculating German Anlage KAP/KAP-INV values from Interactive Brokers Flex Query exports.

- `app.py` is the Streamlit UI: upload snapshot → per-account compute snapshot → view model → exactly one active page renderer (sidebar navigation with areas for overview, Anlage KAP, KAP-INV, SO, review items, methodology, and export). Duplicate file uploads are dropped, and overlapping report periods for the same account are rejected before any computation runs. Every selected XML must contain exactly one `FlexStatement`; foreign XMLs and bundled multi-account statements fail closed. Sidebar uploads add files to the existing dataset instead of replacing it.
- `ui_model.py` is the pure view-model layer (no Streamlit imports): toggle inventory and availability, cache keys and the snapshot commit protocol, the shared final-value arithmetic (`build_final_values`), ETF classification overrides, per-account withholding-tax recalculation, and the notice system. Both `app.py` and `run_tests.py` compute user-facing values through this module, so the GUI and the regression tests share a single arithmetic.
- For EUR-base exports whose FX section reports `functionalCurrency=USD`, computation continues with that account's entire FX section excluded. `fx_unresolved` carries the account-scoped review details; `final['fx_incomplete']` marks affected totals as provisional. Zero FX accumulator contributions are not a computed zero result. Preserve this status through account merges, UI, and TXT/XLSX exports; never silently fall back to CSV/FIFO or show the partial totals as ready for filing. Missing or other incompatible FX currency fields still fail closed.
- `calculate_tax_report.py` contains the main tax calculation logic. Option-to-stock assignment matching resolves IBKR symbol variants (exchange-suffix symbols, ticker renames) via conid/ISIN-based symbol equivalence classes (Issue #83); regression coverage lives in `tests/test_underlying_symbol_matching.py`.
  - Instrument categories and StmtFunds activity codes are routed via module-level tables, not inline literals: `TOPF2_ASSET_CATEGORIES` / `KNOWN_UNROUTED_ASSET_CATEGORIES` for `assetCategory`, and `INCOME_ACTIVITY_CODES` / `KNOWN_IGNORED_ACTIVITY_CODES` / `FEE_ACTIVITY_CODES` / `MANUAL_REVIEW_ACTIVITY_CODES` for `activityCode`. Anything not covered is collected by `register_unrouted_category` / `register_unhandled_activity_code` and surfaced as a review item (`audit['unrouted_asset_categories']`, `audit['unhandled_activity_codes']`) instead of being dropped silently. When adding support for a new category or code, extend the table rather than the branch, and add a case to `tests/test_asset_category_routing.py`. TTAX transaction taxes are the one review-class code with an automated path: unambiguously matched bookings are applied to the matched trade's realized result (`_collect_transaction_tax_adjustments`, covered by `tests/test_transaction_tax.py`); ambiguous or deliberately manual cases stay review items.
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

Run individual synthetic tests while iterating. At the end of a full run, `run_tests.py` executes every entry of its `SYNTHETIC_TESTS` list (currently 22 test files) as subprocesses — a file in `tests/` that is not registered there will NOT run automatically:

```bash
python tests/test_cross_year_series.py
python tests/test_kap_inv_wht.py
python tests/test_asset_category_routing.py
python tests/test_quarterly_history_extraction.py
python tests/test_merge_completeness.py
python tests/test_ui_model.py
python tests/test_app_ui.py       # Streamlit AppTest; needs streamlit installed
python -m unittest tests/test_german_dividend_tax.py
```

Extract IBKR XML data for manual checks:

```bash
python extract_ibkr_data.py <flex_query_export>.xml /tmp/ibkr_extract
```

## Coding Style & Naming Conventions

Use Python 3 with 4-space indentation and standard library modules where practical. Follow existing names: functions and variables use `snake_case`, constants use `UPPER_SNAKE_CASE`, and tax-form fields keep their `zeile_19` style identifiers. Keep comments short and useful, especially around tax-law edge cases, FIFO behavior, and cross-year option handling. No formatter or linter is currently configured; keep changes PEP 8 compatible and avoid broad rewrites.

## Testing Guidelines

Add regression coverage for tax logic changes, especially around realized gains, withholding tax crediting, FX conversion, ETF classification (KAP-INV form mapping), and cross-year Stillhalter handling. Prefer small synthetic fixtures in `tests/` for reproducible bugs; new synthetic test files must be registered in `SYNTHETIC_TESTS` in `run_tests.py` so the full run picks them up. When adding a new top-level field to the report data, give it an explicit merge rule in `merge_report_data` (`app.py`) — `tests/test_merge_completeness.py` fails the suite if a field is lost during the multi-account merge.

Changes to toggle behavior or displayed final values belong in `ui_model.build_final_values` (never as separate arithmetic in `app.py`), covered by `tests/test_ui_model.py`; end-to-end UI flows (navigation, caching, widget persistence, upload validation) are covered by `tests/test_app_ui.py` via Streamlit's AppTest.

When a change adds or reroutes an instrument category or booking code, a green suite is not enough on its own: synthetic fixtures only prove that the new branch works, not that it matches what IBKR actually exports. Verify the assumption against a real Flex Query export, and state plainly in the PR which parts remain synthetic-only. `run_tests.py` also uses local `test_data/audit_expectations.json`; if unavailable, some audit scenarios cannot run and may be skipped or fail early.

## Commit & Pull Request Guidelines

Recent commits use concise imperative messages, sometimes with issue references, for example `Fix option matching across underlyings` or `Issue #44: Magic Fallback-Rate 0.95 durch RuntimeError ersetzen`. Keep commits focused on one behavior change. Pull requests should describe the tax scenario changed, list commands run, link related issues, and include UI screenshots when `app.py` output or layout changes.

## Security & Configuration Tips

Do not commit real IBKR XML, extracted CSVs, personal TXT reports, virtualenvs, or `test_data/`; these are covered by `.gitignore`. Treat financial exports as sensitive and keep processing local unless the user explicitly requests otherwise.

`app.py` renders several blocks with `unsafe_allow_html=True`. Any value taken from the IBKR export (symbols, descriptions, activity codes, ISINs) must pass through `html.escape` before it is interpolated into such a block.
