# IBKR Steuerbericht - Anlage KAP 2025

Automatische Berechnung der Anlage KAP und KAP-INV aus Interactive Brokers Flex Query XML-Exporten. Läuft komplett im Browser - keine Installation, keine Cloud, keine Daten verlassen Ihren Rechner.

**[Jetzt starten](https://konvexinvestment.github.io/ibkr-steuer/)**

## Features

- **Zwei-Töpfe-Berechnung** (Aktien vs. Sonstiges) gemäß §20 Abs. 6 EStG mit Aufschlüsselung nach Gattungen
- **EUR- und USD-Basiswährung** - USD-Konten werden über EZB-Referenzkurse (offline) in EUR umgerechnet
- **InvStG ETF-Klassifizierung** - Automatische Teilfreistellung (30%/15%/60%/0%) und separate Anlage KAP-INV
- **Stillhalterprämien** - Erkennung und Trennung bei Call- und Put-Assignments (BMF Rn. 25-35)
- **Tageskurs-Methode** - Per-Lot Fremdwährungskorrektur nach §20 Abs. 4 S. 1 EStG (optional)
- **FX-Währungsgewinne/-verluste** - FIFO-basierte Berechnung nach §20 Abs. 2 S. 1 Nr. 7 EStG
- **Multi-Account** - Mehrere IBKR-Konten in einem Upload verarbeiten
- **Vorjahres-XMLs** - Cross-Year Stillhalter-Matching (Option verkauft 2024, Assignment 2025)
- **Plausibilitätscheck** - Vergleich der berechneten Werte gegen IBKRs eigene Zusammenfassung
- Optionen, Futures, T-Bills, Anleihen, Dividenden, Zinsen, Stückzinsen, PIL
- Ausländische Quellensteuer-Anrechnung (Zeile 41) und separate Erkennung deutscher Dividendensteuer (Zeile 7/37/38)
- Detaillierte Berechnungsschritte und steuerliche Regeln direkt in der App
- Textreport-Download für die Steuererklärung

## So funktioniert es

1. In IBKR einloggen → Reports → Flex Queries
2. Neue Flex Query erstellen (alle Sektionen, XML-Format, Zeitraum 01.01.2025 - 31.12.2025)
3. XML-Datei herunterladen
4. Auf der [Webseite](https://konvexinvestment.github.io/ibkr-steuer/) hochladen - fertig

Für genauere Ergebnisse können zusätzlich Vorjahres-XMLs hochgeladen werden (Multi-Year FX-FIFO, Cross-Year Stillhalter).

Die technischen XML-Felder sind grundsätzlich unabhängig von der Portalsprache. Am besten getestet ist derzeit eine englische Flex Query; einzelne von IBKR gelieferte Beschreibungstexte können sprachabhängig sein.

## Datenschutz

Die App läuft **vollständig im Browser** via WebAssembly (stlite/Pyodide). Es gibt keinen Server, keine Datenbank, keine Analyse. Ihre IBKR-Daten verlassen zu keinem Zeitpunkt Ihren Rechner.

## Lokale Entwicklung

### macOS / Linux

```bash
git clone https://github.com/KonvexInvestment/ibkr-steuer.git
cd ibkr-steuer
python3 -m venv .venv
source .venv/bin/activate
python -m pip install streamlit openpyxl
python -m streamlit run app.py
```

### Windows 10/11 (PowerShell)

```powershell
git clone https://github.com/KonvexInvestment/ibkr-steuer.git
cd ibkr-steuer
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install streamlit openpyxl
python -m streamlit run app.py
```

Falls PowerShell die Aktivierung blockiert, kann die App auch ohne Aktivierung gestartet werden:

```powershell
.\.venv\Scripts\python.exe -m pip install streamlit openpyxl
.\.venv\Scripts\python.exe -m streamlit run app.py
```

Regressionstests auf allen Plattformen:

```bash
python run_tests.py
```

## Steuerliche Grundlagen

- §20 EStG - Einkünfte aus Kapitalvermögen
- §20 Abs. 1 InvStG - Teilfreistellung für Investmentfonds
- BMF-Schreiben vom 14.05.2025 - Einzelfragen zur Abgeltungsteuer (Rn. 25-35: Stillhalter, Rn. 118-123: Verlustverrechnung)
- Jahressteuergesetz 2024 - Abschaffung des 20.000 Euro Caps für Termingeschäfteverluste
- Anlage KAP / KAP-INV 2025

## Haftungsausschluss

Dieses Tool dient ausschließlich zur Unterstützung bei der Steuererklärung. Es ersetzt keine steuerliche Beratung. Bitte prüfen Sie die Ergebnisse und konsultieren Sie bei Unsicherheiten einen Steuerberater. Keine Gewähr für Richtigkeit oder Vollständigkeit.

## Lizenz

MIT License
