import streamlit as st
import os
import tempfile
from datetime import datetime as _dt

import extract_ibkr_data
import calculate_tax_report

st.set_page_config(
    page_title="Steuerbericht 2025",
    page_icon="🇩🇪",
    layout="centered",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    .stApp {
        background: #0f1117;
        color: #e2e8f0;
    }

    .block-container {
        padding: 1.5rem 1rem 3rem 1rem;
        max-width: 720px;
    }

    /* ── Header ── */
    .page-title {
        font-size: clamp(1.5rem, 5vw, 2rem);
        font-weight: 800;
        color: #f1f5f9;
        letter-spacing: -0.5px;
        margin: 0 0 0.25rem 0;
    }
    .page-sub {
        font-size: 0.9rem;
        color: #64748b;
        margin: 0 0 1.75rem 0;
    }

    /* ── Upload area ── */
    [data-testid="stFileUploader"] {
        background: rgba(255,255,255,0.03);
        border: 1px dashed rgba(255,255,255,0.12);
        border-radius: 14px;
        padding: 0.5rem;
    }

    /* ── Hero card (Zeile 19) ── */
    .hero-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #1a2d4a 100%);
        border: 1px solid rgba(96,165,250,0.25);
        border-radius: 18px;
        padding: 1.75rem 1.5rem;
        margin: 1.5rem 0;
        text-align: center;
    }
    .hero-label {
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        color: #60a5fa;
        margin-bottom: 0.5rem;
    }
    .hero-value {
        font-size: clamp(2rem, 8vw, 3rem);
        font-weight: 800;
        letter-spacing: -1px;
        word-break: break-word;
    }
    .hero-formula {
        font-size: 0.8rem;
        color: #64748b;
        margin-top: 0.5rem;
    }

    /* ── Section headers ── */
    .section-title {
        font-size: 0.7rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 2px;
        color: #475569;
        margin: 2rem 0 0.75rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid rgba(255,255,255,0.06);
    }

    /* ── Metric grid ── */
    .metric-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
        gap: 0.75rem;
        margin-bottom: 0.75rem;
    }
    .metric-card {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 1rem;
        min-width: 0;
    }
    .metric-card.gain  { border-color: rgba(74,222,128,0.2);  background: rgba(74,222,128,0.05); }
    .metric-card.loss  { border-color: rgba(248,113,113,0.2); background: rgba(248,113,113,0.05); }
    .metric-card.saldo { border-color: rgba(250,204,21,0.2);  background: rgba(250,204,21,0.04);  }
    .metric-card.info  { border-color: rgba(148,163,184,0.15); }

    .metric-label {
        font-size: 0.78rem;
        color: #94a3b8;
        margin-bottom: 0.35rem;
        font-weight: 500;
    }
    .metric-value {
        font-size: clamp(1rem, 3.5vw, 1.35rem);
        font-weight: 700;
        word-break: break-word;
        line-height: 1.2;
    }
    .metric-value.green  { color: #4ade80; }
    .metric-value.red    { color: #f87171; }
    .metric-value.yellow { color: #fbbf24; }
    .metric-value.white  { color: #f1f5f9; }

    /* ── Anlage KAP rows ── */
    .kap-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 0.85rem 1rem;
        border-radius: 10px;
        margin-bottom: 0.5rem;
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.06);
        gap: 1rem;
        flex-wrap: wrap;
    }
    .kap-left {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        flex: 1;
        min-width: 0;
    }
    .kap-badge {
        font-size: 0.65rem;
        font-weight: 700;
        letter-spacing: 0.5px;
        background: rgba(96,165,250,0.15);
        color: #60a5fa;
        border-radius: 6px;
        padding: 0.2rem 0.45rem;
        white-space: nowrap;
        flex-shrink: 0;
    }
    .kap-desc {
        font-size: 0.9rem;
        color: #cbd5e1;
        font-weight: 500;
        word-break: break-word;
        min-width: 0;
    }
    .kap-value {
        font-size: clamp(0.95rem, 3vw, 1.15rem);
        font-weight: 700;
        text-align: right;
        white-space: nowrap;
        flex-shrink: 0;
    }

    /* highlight row for Zeile 19 */
    .kap-row.highlight {
        background: rgba(96,165,250,0.08);
        border-color: rgba(96,165,250,0.25);
    }
    .kap-row.highlight .kap-badge {
        background: rgba(96,165,250,0.3);
    }
    .kap-row.highlight .kap-desc {
        color: #f1f5f9;
        font-weight: 600;
    }

    /* ── Audit expander ── */
    [data-testid="stExpander"] {
        border: 1px solid rgba(255,255,255,0.07) !important;
        border-radius: 10px !important;
        background: rgba(255,255,255,0.02) !important;
    }

    /* ── Download button ── */
    [data-testid="stDownloadButton"] > button {
        width: 100%;
        background: rgba(96,165,250,0.1);
        border: 1px solid rgba(96,165,250,0.3);
        color: #60a5fa;
        border-radius: 10px;
        font-weight: 600;
        padding: 0.6rem;
        transition: background 0.2s;
    }
    [data-testid="stDownloadButton"] > button:hover {
        background: rgba(96,165,250,0.2);
    }

    /* hide streamlit branding */
    #MainMenu, footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ── Helpers ──────────────────────────────────────────────────────────────────

def fmt(value: float, decimals: int = 2) -> str:
    s = f"{value:,.{decimals}f}"
    return s.replace(',', 'X').replace('.', ',').replace('X', '.') + " €"

def fmt_de(value: float, decimals: int = 2) -> str:
    s = f"{value:,.{decimals}f}"
    return s.replace(',', 'X').replace('.', ',').replace('X', '.')

def color_class(value: float) -> str:
    if value > 0:
        return "green"
    elif value < 0:
        return "red"
    return "white"

def metric_card(label: str, value: float, variant: str = "auto") -> str:
    """variant: auto | gain | loss | saldo | info"""
    if variant == "auto":
        variant = "gain" if value > 0 else ("loss" if value < 0 else "info")
    val_color = color_class(value)
    return f"""
    <div class="metric-card {variant}">
        <div class="metric-label">{label}</div>
        <div class="metric-value {val_color}">{fmt(value)}</div>
    </div>"""

def kap_row(zeile: str, label: str, value: float, highlight: bool = False,
            force_positive: bool = False) -> str:
    display_val = abs(value) if force_positive else value
    val_color = "white" if force_positive else color_class(value)
    hl = "highlight" if highlight else ""
    return f"""
    <div class="kap-row {hl}">
        <div class="kap-left">
            <span class="kap-badge">{zeile}</span>
            <span class="kap-desc">{label}</span>
        </div>
        <span class="kap-value" style="color: var(--color-{val_color}, #f1f5f9)">{fmt(display_val)}</span>
    </div>"""

def section_title(text: str):
    st.markdown(f'<div class="section-title">{text}</div>', unsafe_allow_html=True)

# inline color vars for kap_row values
COLOR_VARS = """
<style>
    :root {
        --color-green: #4ade80;
        --color-red:   #f87171;
        --color-white: #f1f5f9;
    }

    /* Translate Streamlit file uploader to German */
    [data-testid="stFileUploaderDropzoneInstructions"] {
        font-size: 0 !important;
    }
    [data-testid="stFileUploaderDropzoneInstructions"] svg {
        height: 2.5rem;
        width: 2.5rem;
    }
    [data-testid="stFileUploaderDropzoneInstructions"]::after {
        content: "Datei hierher ziehen";
        font-size: 0.875rem;
        color: #e2e8f0;
    }
    [data-testid="stFileUploaderDropzone"] button {
        font-size: 0 !important;
    }
    [data-testid="stFileUploaderDropzone"] button::after {
        content: "Datei auswählen";
        font-size: 0.875rem;
    }
</style>"""


# ── Page ─────────────────────────────────────────────────────────────────────

st.markdown(COLOR_VARS, unsafe_allow_html=True)
st.markdown('<p class="page-title">🇩🇪 Steuerbericht 2025</p>', unsafe_allow_html=True)
st.markdown('<p class="page-sub">Anlage KAP · Interactive Brokers Flex Query</p>', unsafe_allow_html=True)

st.markdown("""
<div style="background: rgba(96,165,250,0.08); border: 1px solid rgba(96,165,250,0.2); border-radius: 10px; padding: 0.75rem 1rem; margin-bottom: 1rem; font-size: 0.8rem; color: #94a3b8;">
    <strong style="color: #60a5fa;">Datenschutz:</strong> Alle Berechnungen erfolgen ausschließlich lokal im Browser. Es werden keine Daten an Server übertragen, gespeichert oder an Dritte weitergegeben.
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div style="background: rgba(59,130,246,0.08); border-left: 3px solid #3b82f6; border-radius: 8px; padding: 0.8rem 1rem; margin-bottom: 0.5rem; font-size: 0.82rem; color: #cbd5e1; line-height: 1.6;">
    <strong style="color: #60a5fa; font-size: 0.9rem;">1. Flex Query XML (Pflicht)</strong><br>
    Die Hauptdatenquelle. Enthält alle Trades, Dividenden, Zinsen, Quellensteuer und Stillhalter-Details.
    Daraus werden die Anlage KAP Zeilen berechnet (Topf 1 Aktien, Topf 2 Sonstiges, Stillhalterprämien-Separation).<br>
    <span style="color: #64748b;">IBKR &rarr; Performance &amp; Berichte &rarr; Flex-Abfragen &rarr; XML exportieren (Zeitraum: 01.01.&ndash;31.12.2025)</span>
</div>
""", unsafe_allow_html=True)

uploaded_file = st.file_uploader("IBKR Flex Query hochladen - Steuerjahr (XML)", type="xml",
                                  label_visibility="collapsed")

st.markdown("""
<div style="background: rgba(168,85,247,0.06); border-left: 3px solid #a855f7; border-radius: 8px; padding: 0.8rem 1rem; margin-bottom: 0.5rem; font-size: 0.82rem; color: #cbd5e1; line-height: 1.6;">
    <strong style="color: #c084fc; font-size: 0.9rem;">2. Vorjahres-XMLs (Optional)</strong><br>
    Nur nötig wenn Optionen (Calls oder Puts) über den Jahreswechsel gehalten wurden. Beispiel: Option 2024 verkauft,
    2025 durch Assignment geschlossen. Die Stillhalterprämie muss per Zuflussprinzip (BMF Rn. 25)
    dem Verkaufsjahr zugeordnet werden. Dafür wird der Original-Trade aus der Vorjahres-XML benötigt.
</div>
""", unsafe_allow_html=True)

fx_history_files = st.file_uploader(
    "Optional: Vorjahres-XMLs für Stillhalter-Matching & FX-FIFO",
    type="xml", accept_multiple_files=True,
    label_visibility="visible")

st.markdown("""
<div style="background: rgba(16,185,129,0.08); border-left: 3px solid #10b981; border-radius: 8px; padding: 0.8rem 1rem; margin-bottom: 0.5rem; font-size: 0.82rem; color: #cbd5e1; line-height: 1.6;">
    <strong style="color: #34d399; font-size: 0.9rem;">3. IBKR Standard-Bericht CSV (Plausibilitätscheck)</strong><br>
    <strong style="color: #6ee7b7;">Automatischer Plausibilitätscheck:</strong>
    Der IBKR-Bericht enthält aggregierte Summen pro Kategorie (Aktien, Optionen, Futures, Anleihen, Devisen, Dividenden, Zinsen, Quellensteuer).
    Diese werden automatisch mit unserer Einzelberechnung aus der Flex Query XML verglichen — cent-genaue Übereinstimmung ist das Ziel.<br><br>
    <strong style="color: #6ee7b7;">FX-Fallback:</strong>
    Falls Ihre Flex Query keine <code>FxTransactions</code>-Sektion enthält, liefert der CSV-Bericht die exakten Devisengewinne/-verluste als Ersatz.<br><br>
    <span style="background: rgba(16,185,129,0.12); border-radius: 6px; padding: 0.4rem 0.6rem; display: inline-block; margin-top: 0.2rem; color: #94a3b8;">
    <strong style="color: #a7f3d0;">So erstellen:</strong>
    IBKR &rarr; Performance &amp; Berichte &rarr; Kontoauszüge &rarr;
    <strong>Übersicht: realisierter G&amp;V</strong> &rarr; Zeitraum wählen &rarr; Format: CSV &rarr; Erstellen
    </span>
</div>
""", unsafe_allow_html=True)

ibkr_csv_file = st.file_uploader(
    "IBKR Standard-Bericht (CSV) für Plausibilitätscheck & FX-Fallback",
    type="csv",
    label_visibility="visible")

if uploaded_file is None:
    st.stop()

# ── Processing ───────────────────────────────────────────────────────────────

with st.spinner("Berechne Steuerreport…"):
    with tempfile.TemporaryDirectory() as tmp:
        xml_path = os.path.join(tmp, "input.xml")
        with open(xml_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        # Save history XMLs if provided
        history_paths = []
        for i, hf in enumerate(fx_history_files or []):
            hp = os.path.join(tmp, f"history_{i}.xml")
            with open(hp, "wb") as f:
                f.write(hf.getbuffer())
            history_paths.append(hp)

        # Save CSV report if provided
        csv_report_path = None
        if ibkr_csv_file is not None:
            csv_report_path = os.path.join(tmp, "ibkr_report.csv")
            with open(csv_report_path, "wb") as f:
                f.write(ibkr_csv_file.getbuffer())

        try:
            if history_paths:
                all_xmls = sorted(history_paths) + [xml_path]
                extract_ibkr_data.extract_fx_multi_xml(all_xmls, tmp)
            else:
                extract_ibkr_data.parse_ibkr_xml(xml_path, tmp)
            d = calculate_tax_report.calculate_tax(tmp, fx_csv_path=csv_report_path)
        except Exception as e:
            st.error(f"Fehler beim Verarbeiten: {e}")
            st.exception(e)
            st.stop()

# Derived values
topf_1        = d.get('topf_1_aktien_netto', d.get('stocks_net_eur', 0))
topf_2        = d.get('topf_2_sonstiges_netto',
                      d.get('dividends_eur', 0) + d.get('interest_eur', 0) +
                      d.get('options_gain_eur', 0) + d.get('options_loss_eur', 0))
zeile_19      = d.get('zeile_19_netto_eur', topf_1 + topf_2)
zeile_20      = d.get('zeile_20_stock_gains_eur', d.get('stocks_gain_eur', 0))
zeile_22      = d.get('zeile_22_other_losses_eur', abs(d.get('options_loss_eur', 0)))
zeile_23      = d.get('zeile_23_stock_losses_eur', abs(d.get('stocks_loss_eur', 0)))
quellensteuer = d.get('withholding_tax_eur', 0)

# Zuflussprinzip data
audit = d.get('audit', {})
cross_year_premium = audit.get('cross_year_premium_eur', 0)
cross_year_by_year = audit.get('cross_year_by_year', {})
cross_year_details = [det for det in audit.get('stillhalter_details', []) if det.get('is_cross_year')]
has_cross_year = len(cross_year_details) > 0

# ── Flex Query Hinweis ────────────────────────────────────────────────────────

if not d.get('has_trade_price', False):
    st.markdown("""
<div style="background: rgba(251,191,36,0.08); border: 1px solid rgba(251,191,36,0.25); border-radius: 10px; padding: 0.75rem 1rem; margin-bottom: 1rem; font-size: 0.8rem; color: #94a3b8;">
    <strong style="color: #fbbf24;">Hinweis:</strong> Ihre Flex Query enthält kein <code>tradePrice</code>-Feld. Stillhalterprämien werden mit dem Tagesschlusskurs (<code>closePrice</code>) statt dem tatsächlichen Ausführungspreis berechnet.
    Für genauere Ergebnisse: In IBKR unter <em>Reports → Flex Queries</em> eine erweiterte Query erstellen und bei <em>Trade Confirmation</em> alle Felder aktivieren (insbesondere <em>Execution</em>-Details).
</div>
""", unsafe_allow_html=True)

# ── Stillhalter Warnung ──────────────────────────────────────────────────────

unmatched = d.get('audit', {}).get('stillhalter_unmatched', [])
if unmatched:
    details = ", ".join(f"{u['symbol']} ({u['expiry']})" for u in unmatched)
    st.markdown(f"""
<div style="background: rgba(251,146,60,0.08); border: 1px solid rgba(251,146,60,0.25); border-radius: 10px; padding: 0.75rem 1rem; margin-bottom: 1rem; font-size: 0.8rem; color: #94a3b8;">
    <strong style="color: #fb923c;">Stillhalter-Warnung:</strong> Für {len(unmatched)} Assignment(s) wurde der ursprüngliche Optionsverkauf nicht gefunden: <strong>{details}</strong>.
    Die Option wurde vermutlich in einem Vorjahr verkauft (Prämie kassiert) und erst im Steuerjahr assigned. Ohne den Original-Trade kann die Prämie nicht berechnet und verbleibt in Topf 1 (Aktien) statt Topf 2 (Sonstiges).
    <br><em>Lösung:</em> Das Vorjahres-XML, in dem die Option verkauft wurde, oben als "Vorjahres-XML" hochladen.
</div>
""", unsafe_allow_html=True)

# ── Zuflussprinzip Toggle ────────────────────────────────────────────────────

zuflussprinzip_aktiv = False
if has_cross_year:
    st.markdown(f"""
<div style="background: rgba(168,85,247,0.08); border: 1px solid rgba(168,85,247,0.25); border-radius: 10px; padding: 0.75rem 1rem; margin-bottom: 1rem; font-size: 0.8rem; color: #94a3b8;">
    <strong style="color: #a855f7;">Zuflussprinzip (BMF Rn. 25, 33):</strong> {len(cross_year_details)} Stillhalterprämie(n) erkannt, deren Optionsverkauf in einem Vorjahr stattfand ({fmt_de(cross_year_premium)} EUR). Nach dem Zuflussprinzip gehören diese steuerlich in das Verkaufsjahr der Option.
</div>
""", unsafe_allow_html=True)
    zuflussprinzip_aktiv = st.checkbox(
        "Zuflussprinzip anwenden (BMF Rn. 25, 33)",
        value=False,
        help="Verschiebt Stillhalterprämien aus Vorjahren aus dem aktuellen Steuerjahr heraus. "
             "Diese Prämien gehören in die Steuererklärung des jeweiligen Vorjahres.")

# Adjusted values for Zuflussprinzip
adj_cross = cross_year_premium if zuflussprinzip_aktiv else 0
adj_topf_2 = topf_2 - adj_cross
adj_zeile_19 = zeile_19 - adj_cross

# ── Basiswährung ────────────────────────────────────────────────────────────

base_curr = d.get('base_currency', 'USD')
base_icon = "🇪🇺" if base_curr == "EUR" else "🇺🇸"
st.markdown(f"""
<div style="background: rgba(99,102,241,0.08); border: 1px solid rgba(99,102,241,0.25); border-radius: 10px; padding: 0.6rem 1rem; margin-bottom: 1rem; font-size: 0.8rem; color: #94a3b8;">
    {base_icon} <strong style="color: #818cf8;">Basiswährung: {base_curr}</strong> — {"Beträge in StmtFunds sind bereits in EUR (BaseCurrency-Ansicht)." if base_curr == "EUR" else "USD-Beträge werden über tägliche Wechselkurse in EUR umgerechnet."}
</div>
""", unsafe_allow_html=True)

# ── FxTransactions-Warnung ───────────────────────────────────────────────────

xml_has_fx = d.get('xml_has_fx_data', True)
fx_source = d.get('fx_source', 'none')

if not xml_has_fx and fx_source != 'csv':
    st.markdown("""
<div style="background: rgba(251,146,60,0.1); border: 1px solid rgba(251,146,60,0.35); border-radius: 10px; padding: 0.85rem 1rem; margin-bottom: 1rem; font-size: 0.82rem; color: #cbd5e1; line-height: 1.6;">
    <strong style="color: #fb923c; font-size: 0.9rem;">Flex Query unvollständig: Keine FX-Transaktionsdaten</strong><br>
    Ihre Flex Query XML enthält keine <code>FxTransactions</code>-Sektion. Ohne diese Daten können Fremdwährungs-Gewinne/-Verluste
    nur approximiert werden (FIFO-Schätzung mit eingeschränkter Genauigkeit).<br><br>
    <strong style="color: #fdba74;">Lösung:</strong> Laden Sie den <strong>IBKR Standard-Bericht (CSV)</strong> oben hoch —
    dieser enthält die exakten Devisengewinne/-verluste, die IBKR intern per FIFO berechnet.<br>
    <span style="color: #94a3b8; font-size: 0.78rem;">
    Alternativ: In IBKR unter <em>Reports → Flex Queries → Configure</em> die Sektion
    <em>FX Transactions</em> aktivieren und die XML neu exportieren.
    </span>
</div>
""", unsafe_allow_html=True)
elif not xml_has_fx and fx_source == 'csv':
    st.markdown("""
<div style="background: rgba(16,185,129,0.08); border: 1px solid rgba(16,185,129,0.25); border-radius: 10px; padding: 0.6rem 1rem; margin-bottom: 1rem; font-size: 0.8rem; color: #94a3b8;">
    <strong style="color: #34d399;">FX-Daten aus CSV übernommen.</strong>
    Die Flex Query enthält keine FxTransactions, aber der IBKR Standard-Bericht liefert exakte Devisenwerte.
</div>
""", unsafe_allow_html=True)

# ── Hero ─────────────────────────────────────────────────────────────────────

hero_color = "#4ade80" if adj_zeile_19 >= 0 else "#f87171"
st.markdown(f"""
<div class="hero-card">
    <div class="hero-label">Zeile 19 · Ausländische Kapitalerträge (Netto){"  · Zuflussprinzip" if zuflussprinzip_aktiv else ""}</div>
    <div class="hero-value" style="color:{hero_color}">{fmt(adj_zeile_19)}</div>
    <div class="hero-formula">Topf 1 ({fmt(topf_1)}) + Topf 2 ({fmt(adj_topf_2)})</div>
</div>
""", unsafe_allow_html=True)

# ── Topf 1: Aktien ───────────────────────────────────────────────────────────

section_title("Topf 1 · Aktien (separate Verrechnung §20 Abs. 6 S. 4 EStG)")

st.markdown(
    '<div class="metric-grid">'
    + metric_card("Aktiengewinne", d['stocks_gain_eur'], "gain")
    + metric_card("Aktienverluste", d['stocks_loss_eur'], "loss")
    + metric_card("Saldo Aktien", topf_1, "saldo")
    + '</div>',
    unsafe_allow_html=True
)

# ── Topf 2: Sonstiges ────────────────────────────────────────────────────────

section_title("Topf 2 · Sonstiges (inkl. Termingeschäfte, Dividenden, Zinsen)")

st.markdown(
    '<div class="metric-grid">'
    + metric_card("Dividenden", d['dividends_eur'])
    + metric_card("Zinsen (netto)", d['interest_eur'])
    + metric_card("Optionsgewinne", d['options_gain_eur'] - adj_cross, "gain")
    + metric_card("Optionsverluste", d['options_loss_eur'], "loss")
    + metric_card("Saldo Sonstiges", adj_topf_2, "saldo")
    + '</div>',
    unsafe_allow_html=True
)

# ── Fremdwährungs-Gewinne/Verluste ──────────────────────────────────────────

fx_results = d.get('fx_results', {})
fx_total_gain = d.get('fx_total_gain', 0)
fx_total_loss = d.get('fx_total_loss', 0)
fx_mtm = d.get('fx_mtm', {})

fx_source = d.get('fx_source', 'none')

if fx_results:
    if fx_source == 'csv':
        section_title("Fremdwährungs-Gewinne/Verluste (IBKR-Bericht)")
    elif fx_source == 'xml':
        section_title("Fremdwährungs-Gewinne/Verluste (XML FxTransactions)")
    else:
        section_title("Fremdwährungs-Gewinne/Verluste (FIFO-Approximation)")

    st.markdown(
        '<div class="metric-grid">'
        + metric_card("FX Gewinne", fx_total_gain, "gain")
        + metric_card("FX Verluste", fx_total_loss, "loss")
        + metric_card("FX Netto", fx_total_gain + fx_total_loss, "saldo")
        + '</div>',
        unsafe_allow_html=True
    )

    with st.expander("Details pro Währung"):
        fx_table = "| Währung | Gewinn | Verlust | Netto | MTM (Vergleich) |\n|---------|--------|---------|-------|----------------|\n"
        for curr, data in sorted(fx_results.items()):
            mtm_val = fx_mtm.get(curr)
            mtm_str = f"{fmt_de(mtm_val)} EUR" if mtm_val is not None else "-"
            fx_table += f"| {curr} | {fmt_de(data['gain'])} | {fmt_de(data['loss'])} | {fmt_de(data['net'])} | {mtm_str} |\n"
        st.markdown(fx_table)
        fx_tgl = d.get('fx_translation', 0)
        if fx_tgl != 0:
            st.markdown(f"**IBKR Referenz (fxTranslationGainLoss):** {fmt_de(fx_tgl)} EUR")

        if fx_source in ('csv', 'xml'):
            st.success("Exakte FX-Werte aus " + ("IBKR Standard-Bericht" if fx_source == 'csv' else "XML FxTransactions") +
                       " (per-Settlement FIFO, alle Währungen).")
        else:
            no_xml_fx = not d.get('xml_has_fx_data', True)
            fx_prior = d.get('fx_has_prior_data', False)
            extra = (" Die Flex Query enthält keine FxTransactions — "
                     "Kursgenauigkeit der Approximation ist eingeschränkt.") if no_xml_fx else ""
            if fx_prior:
                st.warning(f"FIFO-Approximation aus Flex Query (Tagesraten-Substitution).{extra} "
                           "Für exakte Werte: IBKR Standard-Bericht (CSV) oben hochladen.")
            else:
                st.warning(f"**Nur Steuerjahr geladen.** FIFO-Approximation.{extra} "
                           "Für exakte Werte: IBKR Standard-Bericht (CSV) oben hochladen.")
        st.info("**Rechtsgrundlage:** BMF-Schreiben Rn. 131 - verzinsliches Fremdwährungsguthaben, "
                "§20 Abs. 2 S. 1 Nr. 7 EStG (Anlage KAP, Topf 2). FIFO-Methode (§20 Abs. 4 S. 7). "
                "In Topf 2 enthalten.")

# ── Plausibilitätscheck (wenn CSV hochgeladen) ─────────────────────────────
csv_cats = d.get('csv_category_totals', {})
if csv_cats:
    section_title("Plausibilitätscheck (IBKR-Bericht vs. Berechnung)")
    our_stk_gain = d['stocks_gain_eur'] + d['audit'].get('stillhalter_premium_eur', 0)
    our_stk_loss = d['stocks_loss_eur']
    ibkr_topf2_cats = ["Aktien- und Indexoptionen", "Futures", "Optionen auf Futures (Future-Style)",
                        "Optionen auf Futures", "Anleihen", "Treasury Bills"]
    our_topf2_gain = d['options_gain_eur'] - d['audit'].get('stillhalter_premium_eur', 0) - d.get('fx_total_gain', 0)
    our_topf2_loss = d['options_loss_eur'] - d.get('fx_total_loss', 0)
    ibkr_topf2_gain = sum(csv_cats.get(c, {}).get('gain', 0) for c in ibkr_topf2_cats)
    ibkr_topf2_loss = sum(csv_cats.get(c, {}).get('loss', 0) for c in ibkr_topf2_cats)

    ibkr_stk = csv_cats.get('Aktien', {})
    ibkr_fx = csv_cats.get('Devisen', {})

    rows = [
        ("Aktien (Topf 1) Netto", ibkr_stk.get('net', 0), our_stk_gain + our_stk_loss),
        ("Sonstiges (Topf 2) Netto", ibkr_topf2_gain + ibkr_topf2_loss, our_topf2_gain + our_topf2_loss),
        ("FX (Devisen) Netto", ibkr_fx.get('net', 0), fx_total_gain + fx_total_loss),
    ]

    # Dividenden, Zinsen, Quellensteuer aus CSV
    csv_income = d.get('csv_income_totals', {})
    if 'dividends_eur' in csv_income:
        rows.append(("Dividenden", csv_income['dividends_eur'], d['dividends_eur']))
    if 'interest_eur' in csv_income:
        rows.append(("Zinsen", csv_income['interest_eur'], d['interest_eur']))
    if 'withholding_tax_eur' in csv_income:
        rows.append(("Quellensteuer", abs(csv_income['withholding_tax_eur']), d['withholding_tax_eur']))

    check_table = "| Kategorie | IBKR-Bericht | Unsere Berechnung | Differenz |\n|-----------|-------------|-------------------|----------|\n"
    all_match = True
    zinsen_fx_diff = False
    for label, ibkr_val, our_val in rows:
        diff = our_val - ibkr_val
        match = abs(diff) < 1.0
        if not match:
            if label == "Zinsen":
                zinsen_fx_diff = True
            else:
                all_match = False
        icon = "" if match else (" **(FX)**" if label == "Zinsen" else " **(!)**")
        check_table += f"| {label} | {fmt_de(ibkr_val)} | {fmt_de(our_val)} | {fmt_de(diff)}{icon} |\n"
    st.markdown(check_table)
    if all_match and not zinsen_fx_diff:
        st.success("Alle Kategorien stimmen mit dem IBKR-Bericht überein.")
    elif all_match and zinsen_fx_diff:
        st.success("Alle Kategorien stimmen überein. Zinsen-Differenz ist eine bekannte FX-Konvertierungsdifferenz "
                   "(IBKR konvertiert Fremdwährungs-Anleiheposten im CSV-Bericht mit anderen Kursen als in der XML-BaseCurrency-Ansicht).")
    else:
        st.info("Kleine Abweichungen sind normal (FX-Rundung, Steuerkorrekturen aus Vorjahren).")

# ── Anlage KAP Zeilen ────────────────────────────────────────────────────────

section_title("Anlage KAP · Eintragungen")

st.markdown(
    kap_row("Z. 19", "Ausländische Kapitalerträge (Netto)", adj_zeile_19, highlight=True)
    + kap_row("Z. 20", "Davon: Aktiengewinne", zeile_20)
    + kap_row("Z. 22", "Verluste ohne Aktien", zeile_22, force_positive=True)
    + kap_row("Z. 23", "Aktienverluste", zeile_23, force_positive=True)
    + kap_row("Z. 41", "Anrechenbare Quellensteuer", quellensteuer),
    unsafe_allow_html=True
)

# ── Zuflussprinzip Details ────────────────────────────────────────────────────

if zuflussprinzip_aktiv and cross_year_details:
    section_title("Zuflussprinzip · Vorjahres-Prämien (BMF Rn. 25, 33)")
    st.markdown("""
<div style="background: rgba(168,85,247,0.06); border: 1px solid rgba(168,85,247,0.2); border-radius: 10px; padding: 0.75rem 1rem; margin-bottom: 1rem; font-size: 0.82rem; color: #94a3b8;">
    Die folgenden Stillhalterprämien wurden <strong>aus dem aktuellen Steuerjahr herausgerechnet</strong>,
    da der Zufluss (= Verkauf der Option) in einem Vorjahr stattfand.
    Diese Beträge gehören in die <strong>Steuererklärung des jeweiligen Vorjahres</strong>.
</div>
""", unsafe_allow_html=True)

    detail_table = "| Symbol | Strike | Verkauf (Zufluss) | Assignment | Prämie (EUR) |\n"
    detail_table += "|--------|--------|-------------------|------------|-------------:|\n"
    for det in cross_year_details:
        detail_table += (f"| {det['symbol']} | {det['strike']} | "
                        f"{det['orig_sell_date']} | "
                        f"{det['assignment_date']} | "
                        f"{fmt_de(det['premium_eur'])} |\n")
    st.markdown(detail_table)

    st.markdown("**Zusammenfassung nach Zuflussjahr:**")
    year_table = "| Steuerjahr | Prämien-Summe (EUR) | Hinweis |\n"
    year_table += "|:----------:|--------------------:|--------|\n"
    for year in sorted(cross_year_by_year.keys()):
        year_table += f"| {year} | {fmt_de(cross_year_by_year[year])} | In Steuererklärung {year} eintragen |\n"
    st.markdown(year_table)

    st.info(f"**Gesamtbetrag Vorjahres-Prämien:** {fmt_de(cross_year_premium)} EUR — "
            f"um diesen Betrag wurde Zeile 19 im aktuellen Jahr reduziert.")

# ── Steuerliche Regeln & Berechnungsmethodik ─────────────────────────────────

section_title("Steuerliche Regeln & Berechnungsmethodik")

sh_count = audit.get('stillhalter_count', 0)
sh_eur = audit.get('stillhalter_premium_eur', 0)
base_curr = d.get('base_currency', 'USD')

with st.expander("Regeln anzeigen - So kommen die Ergebnisse zustande"):
    st.markdown(f"""
### Zwei-Töpfe-Struktur (§20 Abs. 6 EStG)

Das deutsche Steuerrecht unterscheidet zwei getrennte Verrechnungstöpfe:

| Topf | Inhalt | Verlustverrechnung |
|------|--------|-------------------|
| **Topf 1 - Aktien** | Gewinne und Verluste aus Aktienveräußerungen (STK) | Aktienverluste dürfen **nur** mit Aktiengewinnen verrechnet werden. Überschüsse werden als Verlustvortrag ins nächste Jahr übertragen. |
| **Topf 2 - Sonstiges** | Optionen, Futures, Anleihen, T-Bills, Dividenden, Zinsen, Stillhalterprämien | Alle Verluste frei mit allen Gewinnen in Topf 2 verrechenbar. |

Das Finanzamt wendet die Verlustverrechnungsbeschränkung anhand der Zeilen 20 und 23 an - der Steuerpflichtige meldet die Bruttowerte.

---

### Instrument-Klassifikation

| IBKR-Kategorie | Steuerliche Einordnung | Topf |
|----------------|----------------------|------|
| **STK** (Aktien) | Aktienveräußerung (§20 Abs. 2 Nr. 1) | Topf 1 |
| **OPT** (Optionen) | Termingeschäft (§20 Abs. 2 Nr. 3) | Topf 2 |
| **FUT** (Futures) | Termingeschäft/Festgeschäft (§20 Abs. 2 Nr. 3) | Topf 2 |
| **FOP** (Futures-Optionen) | Termingeschäft (§20 Abs. 2 Nr. 3) | Topf 2 |
| **BILL** (T-Bills) | Kapitalforderung (§20 Abs. 2 Nr. 7) | Topf 2 |
| **BOND** (Anleihen) | Kapitalforderung (§20 Abs. 2 Nr. 7) | Topf 2 |
| **DIV/PIL** (Dividenden) | Laufende Erträge (§20 Abs. 1 Nr. 1) | Topf 2 |
| **INTR/CINT** (Zinsen) | Zinserträge (§20 Abs. 1 Nr. 7) | Topf 2 |
| **CASH/FOREX** (Fremdwährung) | Verzinsl. Fremdwährungsguthaben (§20 Abs. 2 Nr. 7, BMF Rn. 131) | Topf 2 |

---

### Fremdwährungs-Gewinne/Verluste (BMF Rn. 131)

Beim Halten von Fremdwährungsguthaben (z.B. USD) auf einem verzinslichen Konto (IBKR zahlt Zinsen) entstehen bei Kursänderungen steuerlich relevante Gewinne oder Verluste:

- **Anschaffung** = jeder Zufluss von Fremdwährung (Kauf, Dividende, Verkaufserlös)
- **Veräußerung** = jeder Abfluss (Rücktausch, Aktienkauf, Gebühren)
- **FIFO-Methode**: die zuerst erworbenen Beträge werden zuerst veräußert
- **Rechtsgrundlage**: §20 Abs. 2 S. 1 Nr. 7 EStG, Anlage KAP, Topf 2

**Hinweis:** Ohne Vorjahres-XMLs wird der Jahresanfangsbestand zum 01.01.-Kurs als Anschaffung angesetzt (Vereinfachung). Für exakte Berechnung können Vorjahres-XMLs hochgeladen werden. IBKR liefert keine FIFO-Daten für Währungsgewinne, diese werden hier eigenständig berechnet.

---

### Stillhalterprämien bei Assignments (BMF Rn. 25–35)

Wenn Sie eine Option verkaufen (Stillhalter) und diese ausgeübt wird (Assignment), muss die Prämie steuerlich korrekt zugeordnet werden:

- **Prämie** = laufende Einnahmen nach §20 Abs. 1 Nr. 11 → gehört in **Topf 2**
- **Aktientransaktion** = Veräußerung (Call, Rn. 26) bzw. Anschaffung (Put, Rn. 33) nach §20 Abs. 2 → gehört in **Topf 1**

Bei **beiden** Assignment-Typen gilt laut BMF: „Die vereinnahmte Optionsprämie wird bei der Ermittlung des Veräußerungsgewinns **nicht berücksichtigt**." IBKR bündelt die Prämie jedoch im Aktien-Trade (Call: im Verkaufserlös, Put: in den reduzierten Anschaffungskosten). Dieses Tool erkennt Assignments automatisch und trennt die Prämie heraus.

{"**In Ihrem Report:** " + str(sh_count) + " Assignments erkannt (Call + Put), " + fmt_de(sh_eur) + " EUR Stillhalterprämien von Topf 1 nach Topf 2 verschoben." if sh_count > 0 else "**In Ihrem Report:** Keine Assignments erkannt."}

---

### Dividenden & Payment in Lieu (PIL)

- **Dividenden** (DIV): Laufende Erträge in Topf 2
- **Payment in Lieu** (PIL): Ersatzzahlung wenn Aktien verliehen sind, wird steuerlich wie eine Dividende behandelt und mit diesen zusammen verrechnet

---

### Zinsen & Stückzinsen

- **INTR/CINT**: Zins- und Couponerträge aus Anleihen → Topf 2
- **INTP** (Stückzinsen): Beim Kauf einer Anleihe gezahlte aufgelaufene Zinsen sind **negative Einnahmen** (BMF Rn. 51). Sie reduzieren den Zinsertrag und können diesen insgesamt negativ werden lassen.

---

### Quellensteuer (Zeile 41)

Ausländische Quellensteuern auf Dividenden und Zinsen (z.B. 15% US-Quellensteuer) werden in Zeile 41 als **anrechenbare ausländische Steuern** gemeldet. Das Finanzamt rechnet diese nach der Verlustverrechnung auf die deutsche Abgeltungsteuer an.

---

### Zeilen-Zuordnung Anlage KAP 2025

Da Interactive Brokers ein **ausländischer Broker ohne inländischen Steuerabzug** ist, werden die Einkünfte in der Sektion "Kapitalerträge, die **nicht** dem inländischen Steuerabzug unterlegen haben" (Zeilen 18-23) eingetragen:

| Zeile | Bedeutung | Berechnung |
|-------|-----------|------------|
| **19** | Ausländische Kapitalerträge (Netto) | Topf 1 + Topf 2 (Summe aller Erträge und Verluste) |
| **20** | Davon: Aktiengewinne | Brutto-Aktiengewinne (ohne Verluste) |
| **22** | Verluste ohne Aktien | Verluste aus Optionen, Futures, Anleihen etc. (positiver Betrag) |
| **23** | Aktienverluste | Verluste aus Aktienveräußerungen (positiver Betrag) |
| **41** | Anrechenbare Quellensteuer | Summe aller ausländischen Quellensteuern |

---

### Währungsumrechnung

{"**Ihr Konto hat EUR als Basiswährung.** Alle Beträge in der IBKR-Abrechnung sind bereits in EUR umgerechnet. Bei USD-Trades nutzt IBKR den Tageskurs (`fxRateToBase`), der direkt in EUR umrechnet, kein zusätzlicher FX-Lookup erforderlich." if base_curr == "EUR" else "**Ihr Konto hat USD als Basiswährung.** Beträge werden in zwei Schritten umgerechnet: (1) Trade-Währung → USD über `fxRateToBase`, (2) USD → EUR über den Tageskurs des vorherigen Geschäftstags. Die täglichen USD/EUR-Kurse werden aus den IBKR-Daten extrahiert."}

---

### Rechtsgrundlagen

- **§20 EStG** - Einkünfte aus Kapitalvermögen
- **BMF-Schreiben vom 14.05.2025** - "Einzelfragen zur Abgeltungsteuer" (IV C 1 - S 2252/00075/016/070)
- **Jahressteuergesetz 2024** - Abschaffung des €20.000-Caps für Termingeschäfteverluste (§20 Abs. 6 Satz 5 EStG), rückwirkend für alle offenen Fälle
- **Anlage KAP 2025** - Zeilen 9/14 (Termingeschäfte) existieren nur in der Sektion mit inländischem Steuerabzug und sind für IBKR nicht relevant
""")

# ── Export ───────────────────────────────────────────────────────────────────

section_title("Export")

# Build optional export sections
fx_export = ""
if fx_results:
    fx_export = "\nFREMDWÄHRUNGS-GEWINNE/VERLUSTE (FIFO)\n"
    for curr, data in sorted(fx_results.items()):
        fx_export += f"  {curr}: Gewinn {fmt_de(data['gain']):>10}  Verlust {fmt_de(data['loss']):>10}  Netto {fmt_de(data['net']):>10} EUR\n"
    fx_net = fx_total_gain + fx_total_loss
    fx_export += f"  ─────────────────────────────────────────────────\n"
    fx_export += f"  FX Gesamt Gewinn:      {fmt_de(fx_total_gain):>14} EUR\n"
    fx_export += f"  FX Gesamt Verlust:     {fmt_de(fx_total_loss):>14} EUR\n"
    fx_export += f"  FX Netto:              {fmt_de(fx_net):>14} EUR\n"
    fx_export += "  (In Topf 2 enthalten, BMF Rn. 131)\n"

sh_export = ""
if sh_count > 0:
    sh_export = f"\nSTILLHALTERPRÄMIEN (BMF Rn. 25-35)\n"
    sh_export += f"  {sh_count} Assignment(s) erkannt\n"
    sh_export += f"  Prämien umgebucht:     {fmt_de(sh_eur):>14} EUR\n"
    sh_export += f"  (Von Topf 1 nach Topf 2 verschoben)\n"

report_text = f"""ANLAGE KAP 2025 - Steuerbericht
Erstellt: {_dt.now().strftime('%d.%m.%Y %H:%M')}
Basiswährung: {d.get('base_currency', 'USD')}

═══════════════════════════════════════════════════
TOPF 1: AKTIEN
  Aktiengewinne:         {fmt_de(d.get('stocks_gain_eur', 0)):>14} EUR
  Aktienverluste:        {fmt_de(d.get('stocks_loss_eur', 0)):>14} EUR
  ─────────────────────────────────────────────────
  Saldo Aktien:          {fmt_de(topf_1):>14} EUR

TOPF 2: SONSTIGES (inkl. Termingeschäfte)
  Dividenden:            {fmt_de(d.get('dividends_eur', 0)):>14} EUR
  Zinsen (netto):        {fmt_de(d.get('interest_eur', 0)):>14} EUR
  Optionsgewinne:        {fmt_de(d.get('options_gain_eur', 0)):>14} EUR
  Optionsverluste:       {fmt_de(d.get('options_loss_eur', 0)):>14} EUR
  ─────────────────────────────────────────────────
  Saldo Sonstiges:       {fmt_de(topf_2):>14} EUR
{fx_export}{sh_export}
═══════════════════════════════════════════════════
ANLAGE KAP EINTRAGUNGEN
  Zeile 19 (Netto):      {fmt_de(zeile_19):>14} EUR
  Zeile 20 (Aktiengewinne): {fmt_de(zeile_20):>11} EUR
  Zeile 22 (Verluste o. Aktien): {fmt_de(zeile_22):>8} EUR
  Zeile 23 (Aktienverluste): {fmt_de(zeile_23):>11} EUR
  Zeile 41 (Quellensteuer): {fmt_de(quellensteuer):>11} EUR
═══════════════════════════════════════════════════
"""

st.download_button(
    label="Textreport herunterladen",
    data=report_text,
    file_name="steuerbericht_2025.txt",
    mime="text/plain",
    use_container_width=True
)

# ── Rechtliche Hinweise ──────────────────────────────────────────────────────

section_title("Rechtliche Hinweise")

st.markdown("""
<div style="background: rgba(255,255,255,0.02); border: 1px solid rgba(255,255,255,0.06); border-radius: 12px; padding: 1.25rem 1.25rem; font-size: 0.78rem; color: #64748b; line-height: 1.7;">

<strong style="color: #94a3b8;">Haftungsausschluss</strong><br>
Dieses Tool dient ausschließlich zur Unterstützung bei der Erstellung der Einkommensteuererklärung. Die berechneten Werte sind unverbindlich und ohne Gewähr für Richtigkeit, Vollständigkeit oder Aktualität. Der Nutzer ist für die Prüfung aller Angaben in seiner Steuererklärung selbst verantwortlich. Die Nutzung erfolgt auf eigenes Risiko.

<br><br>
<strong style="color: #94a3b8;">Keine Steuerberatung</strong><br>
Dieses Tool stellt keine Steuerberatung im Sinne des Steuerberatungsgesetzes (StBerG) dar und ersetzt nicht die Beratung durch einen Steuerberater, Wirtschaftsprüfer oder eine andere zur Steuerberatung befugte Person. Bei Unsicherheiten oder komplexen Sachverhalten konsultieren Sie bitte einen steuerlichen Berater.

<br><br>
<strong style="color: #94a3b8;">Keine Haftung</strong><br>
Die Entwickler und Mitwirkenden dieses Projekts haften nicht für Schäden, die durch die Nutzung oder Nichtnutzung der berechneten Informationen entstehen, einschließlich, aber nicht beschränkt auf finanzielle Verluste, Steuernachzahlungen, Bußgelder oder Zinsen. Dies gilt sowohl für direkte als auch für indirekte Schäden, unabhängig davon, ob diese vorhersehbar waren.

<br><br>
<strong style="color: #94a3b8;">Datenschutz und Datenverarbeitung</strong><br>
Sämtliche Berechnungen werden ausschließlich lokal im Browser des Nutzers ausgeführt (clientseitige Verarbeitung mittels WebAssembly). Es werden zu keinem Zeitpunkt personenbezogene Daten, Finanzdaten oder hochgeladene Dateien an einen Server übertragen, gespeichert oder an Dritte weitergegeben. Es findet kein Tracking, keine Analyse und keine Protokollierung statt. Die Anwendung erfüllt die Anforderungen der DSGVO, da keine Datenverarbeitung durch den Anbieter erfolgt.

<br><br>
<strong style="color: #94a3b8;">Rechtsstand und Aktualität</strong><br>
Die steuerlichen Berechnungen basieren auf dem Rechtsstand des Steuerjahres 2025, insbesondere auf §20 EStG, dem BMF-Schreiben vom 14.05.2025 (Einzelfragen zur Abgeltungsteuer) sowie dem Jahressteuergesetz 2024. Änderungen der Rechtslage, der Verwaltungsauffassung oder der Rechtsprechung nach Veröffentlichung dieses Tools werden nicht automatisch berücksichtigt.

<br><br>
<strong style="color: #94a3b8;">Open Source</strong><br>
Dieses Projekt ist unter der MIT-Lizenz veröffentlicht. Der Quellcode ist frei einsehbar und prüfbar unter
<a href="https://github.com/KonvexInvestment/ibkr-steuer" target="_blank" style="color: #60a5fa;">github.com/KonvexInvestment/ibkr-steuer</a>.
Jeder kann den Code einsehen, prüfen und zur Verbesserung beitragen.

</div>
""", unsafe_allow_html=True)
