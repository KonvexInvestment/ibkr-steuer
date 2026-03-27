import streamlit as st
import sys
import os
import tempfile
from datetime import datetime as _dt
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
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

    /* ── Top-5 table ── */
    .top5-row {
        display: flex;
        align-items: center;
        gap: 0.65rem;
        padding: 0.65rem 0.85rem;
        border-radius: 9px;
        margin-bottom: 0.4rem;
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.06);
        min-width: 0;
    }
    .top5-rank {
        font-size: 0.7rem;
        font-weight: 700;
        color: #475569;
        width: 1.1rem;
        flex-shrink: 0;
        text-align: right;
    }
    .top5-cat {
        font-size: 0.6rem;
        font-weight: 700;
        padding: 0.15rem 0.35rem;
        border-radius: 4px;
        background: rgba(148,163,184,0.12);
        color: #94a3b8;
        white-space: nowrap;
        flex-shrink: 0;
    }
    .top5-ticker {
        font-size: 0.85rem;
        font-weight: 600;
        color: #e2e8f0;
        flex: 1;
        min-width: 0;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    .top5-value {
        font-size: 0.9rem;
        font-weight: 700;
        white-space: nowrap;
        flex-shrink: 0;
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
    return f"{value:,.{decimals}f} €"

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

# inline color vars for kap_row values
COLOR_VARS = """
<style>
    :root {
        --color-green: #4ade80;
        --color-red:   #f87171;
        --color-white: #f1f5f9;
    }
</style>"""


# ── Page ─────────────────────────────────────────────────────────────────────

st.markdown(COLOR_VARS, unsafe_allow_html=True)
st.markdown('<p class="page-title">🇩🇪 Steuerbericht 2025</p>', unsafe_allow_html=True)
st.markdown('<p class="page-sub">Anlage KAP · Interactive Brokers Flex Query</p>', unsafe_allow_html=True)

st.markdown("""
<div style="background: rgba(96,165,250,0.08); border: 1px solid rgba(96,165,250,0.2); border-radius: 10px; padding: 0.75rem 1rem; margin-bottom: 1rem; font-size: 0.8rem; color: #94a3b8;">
    <strong style="color: #60a5fa;">Datenschutz:</strong> Alle Berechnungen erfolgen ausschliesslich lokal in Ihrem Browser. Es werden keine Daten an Server uebertragen, gespeichert oder an Dritte weitergegeben.
</div>
""", unsafe_allow_html=True)

uploaded_file = st.file_uploader("IBKR Flex Query hochladen (XML)", type="xml",
                                  label_visibility="collapsed")

if uploaded_file is None:
    st.markdown("""
    <div style="text-align:center; padding: 3rem 1rem; color: #334155;">
        <div style="font-size:3rem; margin-bottom:1rem;">📂</div>
        <div style="font-size:1rem; font-weight:600; color:#475569;">XML-Datei hochladen</div>
        <div style="font-size:0.85rem; margin-top:0.4rem;">IBKR → Reports → Flex Query → XML exportieren</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ── Processing ───────────────────────────────────────────────────────────────

with st.spinner("Berechne Steuerreport…"):
    with tempfile.TemporaryDirectory() as tmp:
        xml_path = os.path.join(tmp, "input.xml")
        with open(xml_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        try:
            extract_ibkr_data.parse_ibkr_xml(xml_path, tmp)
            d = calculate_tax_report.calculate_tax(tmp)
        except Exception as e:
            st.error(f"Fehler beim Verarbeiten: {e}")
            st.exception(e)
            st.stop()

# Derived values
topf_1        = d.get('topf_1_aktien_netto',    d['stocks_net_eur'])
topf_2        = d.get('topf_2_sonstiges_netto',
                      d['dividends_eur'] + d['interest_eur'] +
                      d['options_gain_eur'] + d['options_loss_eur'])
zeile_19      = d.get('zeile_19_netto_eur',      topf_1 + topf_2)
zeile_20      = d.get('zeile_20_stock_gains_eur', d['stocks_gain_eur'])
zeile_22      = d.get('zeile_22_other_losses_eur', abs(d['options_loss_eur']))
zeile_23      = d.get('zeile_23_stock_losses_eur', abs(d['stocks_loss_eur']))
quellensteuer = d['withholding_tax_eur']

# ── Hero ─────────────────────────────────────────────────────────────────────

hero_color = "#4ade80" if zeile_19 >= 0 else "#f87171"
st.markdown(f"""
<div class="hero-card">
    <div class="hero-label">Zeile 19 · Ausländische Kapitalerträge (Netto)</div>
    <div class="hero-value" style="color:{hero_color}">{fmt(zeile_19)}</div>
    <div class="hero-formula">Topf 1 ({fmt(topf_1)}) + Topf 2 ({fmt(topf_2)})</div>
</div>
""", unsafe_allow_html=True)

# ── Topf 1: Aktien ───────────────────────────────────────────────────────────

st.markdown('<div class="section-title">Topf 1 · Aktien (separate Verrechnung §20 Abs. 6 S. 4 EStG)</div>',
            unsafe_allow_html=True)

st.markdown(
    '<div class="metric-grid">'
    + metric_card("Aktiengewinne", d['stocks_gain_eur'], "gain")
    + metric_card("Aktienverluste", d['stocks_loss_eur'], "loss")
    + metric_card("Saldo Aktien", topf_1, "saldo")
    + '</div>',
    unsafe_allow_html=True
)

# ── Topf 2: Sonstiges ────────────────────────────────────────────────────────

st.markdown('<div class="section-title">Topf 2 · Sonstiges (inkl. Termingeschäfte, Dividenden, Zinsen)</div>',
            unsafe_allow_html=True)

st.markdown(
    '<div class="metric-grid">'
    + metric_card("Dividenden", d['dividends_eur'])
    + metric_card("Zinsen (netto)", d['interest_eur'])
    + metric_card("Optionsgewinne", d['options_gain_eur'], "gain")
    + metric_card("Optionsverluste", d['options_loss_eur'], "loss")
    + metric_card("Saldo Sonstiges", topf_2, "saldo")
    + '</div>',
    unsafe_allow_html=True
)

# ── Anlage KAP Zeilen ────────────────────────────────────────────────────────

st.markdown('<div class="section-title">Anlage KAP · Eintragungen</div>', unsafe_allow_html=True)

st.markdown(
    kap_row("Z. 19", "Ausländische Kapitalerträge (Netto)", zeile_19, highlight=True)
    + kap_row("Z. 20", "Davon: Aktiengewinne", zeile_20)
    + kap_row("Z. 22", "Verluste ohne Aktien", zeile_22, force_positive=True)
    + kap_row("Z. 23", "Aktienverluste", zeile_23, force_positive=True)
    + kap_row("Z. 41", "Anrechenbare Quellensteuer", quellensteuer),
    unsafe_allow_html=True
)

# ── Steuerliche Regeln & Berechnungsmethodik ─────────────────────────────────

st.markdown('<div class="section-title">Steuerliche Regeln & Berechnungsmethodik</div>', unsafe_allow_html=True)

audit = d.get('audit', {})
sh_count = audit.get('stillhalter_count', 0)
sh_eur = audit.get('stillhalter_premium_eur', 0)
base_curr = d.get('base_currency', 'USD')

with st.expander("Regeln anzeigen  - So kommen die Ergebnisse zustande"):
    st.markdown(f"""
### Zwei-Töpfe-Struktur (§20 Abs. 6 EStG)

Das deutsche Steuerrecht unterscheidet zwei getrennte Verrechnungstöpfe:

| Topf | Inhalt | Verlustverrechnung |
|------|--------|-------------------|
| **Topf 1  - Aktien** | Gewinne und Verluste aus Aktienveräußerungen (STK) | Aktienverluste dürfen **nur** mit Aktiengewinnen verrechnet werden. Überschüsse werden als Verlustvortrag ins nächste Jahr übertragen. |
| **Topf 2  - Sonstiges** | Optionen, Futures, Anleihen, T-Bills, Dividenden, Zinsen, Stillhalterprämien | Alle Verluste frei mit allen Gewinnen in Topf 2 verrechenbar. |

Das Finanzamt wendet die Verlustverrechnungsbeschränkung anhand der Zeilen 20 und 23 an  - der Steuerpflichtige meldet die Bruttowerte.

---

### Instrument-Klassifikation

| IBKR-Kategorie | Steuerliche Einordnung | Topf |
|----------------|----------------------|------|
| **STK** (Aktien) | Aktienveräußerung (§20 Abs. 2 Nr. 1) | Topf 1 |
| **OPT** (Optionen) | Termingeschäft (§20 Abs. 2 Nr. 3) | Topf 2 |
| **FUT** (Futures) | Termingeschäft  - Festgeschäft (§20 Abs. 2 Nr. 3) | Topf 2 |
| **FOP** (Futures-Optionen) | Termingeschäft (§20 Abs. 2 Nr. 3) | Topf 2 |
| **BILL** (T-Bills) | Kapitalforderung (§20 Abs. 2 Nr. 7) | Topf 2 |
| **BOND** (Anleihen) | Kapitalforderung (§20 Abs. 2 Nr. 7) | Topf 2 |
| **DIV/PIL** (Dividenden) | Laufende Erträge (§20 Abs. 1 Nr. 1) | Topf 2 |
| **INTR/CINT** (Zinsen) | Zinserträge (§20 Abs. 1 Nr. 7) | Topf 2 |

---

### Covered Calls  - Stillhalterprämien (BMF Rn. 25–35)

Wenn Sie eine Call-Option verkaufen (Stillhalter) und diese ausgeübt wird (Assignment), muss die Prämie steuerlich korrekt zugeordnet werden:

- **Prämie** = laufende Einnahmen nach §20 Abs. 1 Nr. 11 → gehört in **Topf 2** (nicht Aktiengewinn)
- **Aktienverkauf** = Veräußerungsgeschäft nach §20 Abs. 2 Nr. 1 → gehört in **Topf 1**

IBKR bündelt beides im Aktien-Trade. Dieses Tool erkennt Assignments automatisch und trennt die Prämie heraus.

{"**In Ihrem Report:** " + str(sh_count) + " Call-Assignments erkannt, " + f"{sh_eur:,.2f}" + " EUR Stillhalterprämien von Topf 1 nach Topf 2 verschoben." if sh_count > 0 else "**In Ihrem Report:** Keine Call-Assignments erkannt."}

---

### Put-Assignments (BMF Rn. 31)

Bei Ausübung einer verkauften Put-Option kaufen Sie die Aktie zum Strike-Preis. Die erhaltene Prämie **mindert die Anschaffungskosten** der Aktie  - sie wird also nicht sofort als Ertrag erfasst, sondern verringert den Einstandspreis. Der steuerliche Effekt zeigt sich erst beim späteren Verkauf der Aktie.

IBKR berücksichtigt dies korrekt über die FIFO-Kostenbasis  - keine manuelle Korrektur nötig.

---

### Dividenden & Payment in Lieu (PIL)

- **Dividenden** (DIV): Laufende Erträge in Topf 2
- **Payment in Lieu** (PIL): Ersatzzahlung wenn Aktien verliehen sind  - wird steuerlich wie eine Dividende behandelt und mit diesen zusammen verrechnet

---

### Zinsen & Stückzinsen

- **INTR/CINT**: Zins- und Couponerträge aus Anleihen → Topf 2
- **INTP** (Stückzinsen): Beim Kauf einer Anleihe gezahlte aufgelaufene Zinsen sind **negative Einnahmen** (BMF Rn. 51). Sie reduzieren den Zinsertrag und können diesen insgesamt negativ werden lassen.

---

### Quellensteuer (Zeile 41)

Ausländische Quellensteuern auf Dividenden und Zinsen (z.B. 15% US-Quellensteuer) werden in Zeile 41 als **anrechenbare ausländische Steuern** gemeldet. Das Finanzamt rechnet diese nach der Verlustverrechnung auf die deutsche Abgeltungsteuer an.

---

### Zeilen-Zuordnung Anlage KAP 2025

Da Interactive Brokers ein **ausländischer Broker ohne inländischen Steuerabzug** ist, werden die Einkünfte in der Sektion „Kapitalerträge, die **nicht** dem inländischen Steuerabzug unterlegen haben" (Zeilen 18–23) eingetragen:

| Zeile | Bedeutung | Berechnung |
|-------|-----------|------------|
| **19** | Ausländische Kapitalerträge (Netto) | Topf 1 + Topf 2 (Summe aller Erträge und Verluste) |
| **20** | Davon: Aktiengewinne | Brutto-Aktiengewinne (ohne Verluste) |
| **22** | Verluste ohne Aktien | Verluste aus Optionen, Futures, Anleihen etc. (positiver Betrag) |
| **23** | Aktienverluste | Verluste aus Aktienveräußerungen (positiver Betrag) |
| **41** | Anrechenbare Quellensteuer | Summe aller ausländischen Quellensteuern |

---

### Währungsumrechnung

{"**Ihr Konto hat EUR als Basiswährung.** Alle Beträge in der IBKR-Abrechnung sind bereits in EUR umgerechnet. Bei USD-Trades nutzt IBKR den Tageskurs (`fxRateToBase`), der direkt in EUR umrechnet  - kein zusätzlicher FX-Lookup erforderlich." if base_curr == "EUR" else "**Ihr Konto hat USD als Basiswährung.** Beträge werden in zwei Schritten umgerechnet: (1) Trade-Währung → USD über `fxRateToBase`, (2) USD → EUR über den Tageskurs des vorherigen Geschäftstags. Die täglichen USD/EUR-Kurse werden aus den IBKR-Daten extrahiert."}

---

### Rechtsgrundlagen

- **§20 EStG**  - Einkünfte aus Kapitalvermögen
- **BMF-Schreiben vom 14.05.2025**  - „Einzelfragen zur Abgeltungsteuer" (IV C 1 - S 2252/00075/016/070)
- **Jahressteuergesetz 2024**  - Abschaffung des €20.000-Caps für Termingeschäfteverluste (§20 Abs. 6 Satz 5 EStG), rückwirkend für alle offenen Fälle
- **Anlage KAP 2025**  - Zeilen 9/14 (Termingeschäfte) existieren nur in der Sektion mit inländischem Steuerabzug und sind für IBKR nicht relevant
""")

# ── Export ───────────────────────────────────────────────────────────────────

st.markdown('<div class="section-title">Export</div>', unsafe_allow_html=True)

report_text = f"""ANLAGE KAP 2025  - Steuerbericht
Erstellt: {_dt.now().strftime('%d.%m.%Y %H:%M')}
Basiswährung: {d['base_currency']}

═══════════════════════════════════════════════════
TOPF 1: AKTIEN
  Aktiengewinne:         {d['stocks_gain_eur']:>14,.2f} EUR
  Aktienverluste:        {d['stocks_loss_eur']:>14,.2f} EUR
  ─────────────────────────────────────────────────
  Saldo Aktien:          {topf_1:>14,.2f} EUR

TOPF 2: SONSTIGES (inkl. Termingeschäfte)
  Dividenden:            {d['dividends_eur']:>14,.2f} EUR
  Zinsen (netto):        {d['interest_eur']:>14,.2f} EUR
  Optionsgewinne:        {d['options_gain_eur']:>14,.2f} EUR
  Optionsverluste:       {d['options_loss_eur']:>14,.2f} EUR
  ─────────────────────────────────────────────────
  Saldo Sonstiges:       {topf_2:>14,.2f} EUR

═══════════════════════════════════════════════════
ANLAGE KAP EINTRAGUNGEN
  Zeile 19 (Netto):      {zeile_19:>14,.2f} EUR
  Zeile 20 (Aktiengewinne): {zeile_20:>11,.2f} EUR
  Zeile 22 (Verluste o. Aktien): {zeile_22:>8,.2f} EUR
  Zeile 23 (Aktienverluste): {zeile_23:>11,.2f} EUR
  Zeile 41 (Quellensteuer): {quellensteuer:>11,.2f} EUR
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

st.markdown('<div class="section-title">Rechtliche Hinweise</div>', unsafe_allow_html=True)

st.markdown("""
<div style="background: rgba(255,255,255,0.02); border: 1px solid rgba(255,255,255,0.06); border-radius: 12px; padding: 1.25rem 1.25rem; font-size: 0.78rem; color: #64748b; line-height: 1.7;">

<strong style="color: #94a3b8;">Haftungsausschluss</strong><br>
Dieses Tool dient ausschliesslich zur Unterstuetzung bei der Erstellung der Einkommensteuererklaerung. Die berechneten Werte sind unverbindlich und ohne Gewaehr fuer Richtigkeit, Vollstaendigkeit oder Aktualitaet. Der Nutzer ist fuer die Pruefung aller Angaben in seiner Steuererklaerung selbst verantwortlich. Die Nutzung erfolgt auf eigenes Risiko.

<br><br>
<strong style="color: #94a3b8;">Keine Steuerberatung</strong><br>
Dieses Tool stellt keine Steuerberatung im Sinne des Steuerberatungsgesetzes (StBerG) dar und ersetzt nicht die Beratung durch einen Steuerberater, Wirtschaftspruefer oder eine andere zur Steuerberatung befugte Person. Bei Unsicherheiten oder komplexen Sachverhalten konsultieren Sie bitte einen steuerlichen Berater.

<br><br>
<strong style="color: #94a3b8;">Keine Haftung</strong><br>
Die Entwickler und Mitwirkenden dieses Projekts haften nicht fuer Schaeden, die durch die Nutzung oder Nichtnutzung der berechneten Informationen entstehen, einschliesslich, aber nicht beschraenkt auf finanzielle Verluste, Steuernachzahlungen, Bussgelder oder Zinsen. Dies gilt sowohl fuer direkte als auch fuer indirekte Schaeden, unabhaengig davon, ob diese vorhersehbar waren.

<br><br>
<strong style="color: #94a3b8;">Datenschutz und Datenverarbeitung</strong><br>
Saemtliche Berechnungen werden ausschliesslich lokal im Browser des Nutzers ausgefuehrt (clientseitige Verarbeitung mittels WebAssembly). Es werden zu keinem Zeitpunkt personenbezogene Daten, Finanzdaten oder hochgeladene Dateien an einen Server uebertragen, gespeichert oder an Dritte weitergegeben. Es findet kein Tracking, keine Analyse und keine Protokollierung statt. Die Anwendung erfuellt die Anforderungen der DSGVO, da keine Datenverarbeitung durch den Anbieter erfolgt.

<br><br>
<strong style="color: #94a3b8;">Rechtsstand und Aktualitaet</strong><br>
Die steuerlichen Berechnungen basieren auf dem Rechtsstand des Steuerjahres 2025, insbesondere auf §20 EStG, dem BMF-Schreiben vom 14.05.2025 (Einzelfragen zur Abgeltungsteuer) sowie dem Jahressteuergesetz 2024. Aenderungen der Rechtslage, der Verwaltungsauffassung oder der Rechtsprechung nach Veroeffentlichung dieses Tools werden nicht automatisch beruecksichtigt.

<br><br>
<strong style="color: #94a3b8;">Open Source</strong><br>
Dieses Projekt ist unter der MIT-Lizenz veroeffentlicht. Der Quellcode ist frei einsehbar und pruefbar unter
<a href="https://github.com/KonvexInvestment/ibkr-steuer" target="_blank" style="color: #60a5fa;">github.com/KonvexInvestment/ibkr-steuer</a>.
Jeder kann den Code einsehen, pruefen und zur Verbesserung beitragen.

</div>
""", unsafe_allow_html=True)
