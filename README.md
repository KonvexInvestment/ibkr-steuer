# IBKR Steuerbericht: Anlage KAP

Automatische Berechnung von Anlage KAP, KAP-INV und SO aus Interactive Brokers Flex Query XML-Exporten. Die Berechnung läuft komplett im Browser: keine Installation, kein Anwendungsserver, die hochgeladenen Kontodaten bleiben auf dem eigenen Rechner.

**[Jetzt starten](https://konvexinvestment.github.io/ibkr-steuer/)**

## Features

- **Zwei-Töpfe-Berechnung** (Aktien vs. Sonstiges) gemäß §20 Abs. 6 EStG mit Aufschlüsselung nach Gattungen
- **EUR- und USD-Basiswährung**: USD-Konten werden über EZB-Referenzkurse (offline) in EUR umgerechnet
- **InvStG ETF-Klassifizierung**: Teilfreistellung (30 %, 15 %, 60 %, 80 %, 0 %) und Anlage KAP-INV mit den ELSTER-Rohwerten vor Teilfreistellung; ein durchsuchbarer Transparenzkatalog zeigt Zuordnung, Begründung, Nachweisstatus und Quellen und lässt sich als CSV exportieren
- **Anlage SO**: physische Gold-ETCs mit Lieferanspruch als privates Veräußerungsgeschäft nach §23 EStG, inklusive Prüfung der einjährigen Spekulationsfrist (BFH VIII R 35/14 zur Veräußerung, VIII R 4/15 zur physischen Auslieferung)
- **Stillhalterprämien**: Erkennung und Trennung bei Call- und Put-Assignments (BMF Rn. 25-35), auch über den Jahreswechsel, nach Optionssplits und bei abweichenden IBKR-Symbolschreibweisen
- **Tageskurs-Methode**: Fremdwährungskorrektur je Lot nach §20 Abs. 4 S. 1 EStG (optional)
- **FX-Währungsgewinne und -verluste** nach §20 Abs. 2 S. 1 Nr. 7 EStG: primär aus IBKRs Ergebnis je Buchung, ersatzweise über eine eigene FIFO-Näherung. Tilgungen von Fremdwährungsschulden bleiben außen vor. Das ist eine bewusst konservative Auslegung: BMF Rn. 131 knüpft die Erfassung an ein Fremdwährungsguthaben, äußert sich zur Schuldseite aber nicht ausdrücklich
- **Quellensteuer**: ausländische Quellensteuer in Zeile 41, deutsche Kapitalertragsteuer getrennt davon in Zeile 7/37/38. Die anrechenbare Fonds-Quellensteuer setzt der Standardmodus als Rohsteuer abzüglich Teilfreistellung an; eine ereignisbezogene Prüfung je Ausschüttung mit DBA-Höchstsätzen (BMF Rn. 148) lässt sich als Beta zuschalten
- **Multi-Account und Multi-XML**: mehrere Konten in einem Upload, Quartalsexporte eines Jahres werden zusammengeführt, Vorjahres-XMLs vervollständigen Stillhalter-Matching und FX-Historie. Alle Konten müssen dieselbe Basiswährung haben
- **Prüffälle statt stiller Annahmen**: unbekannte Instrumentenkategorien, unbekannte Buchungscodes und wirklich unbekannte Produkte werden ausgewiesen statt geraten. USO/UNG werden als Personengesellschaften erst mit K-1/K-3 bzw. äquivalenter Jahresallokation berechnet, nicht ersatzweise über Topf 2
- **Plausibilitätscheck** gegen IBKRs eigene Summen, optional zusätzlich gegen den IBKR-Standardbericht als CSV. Dieser Abgleich ist nur bei einem einzelnen Konto verfügbar
- Optionen, Futures, T-Bills, Anleihen, Optionsscheine, CFDs, Dividenden, Zinsen, Stückzinsen, PIL
- Export als Textreport und als Excel-Steuerreport mit allen Detailpositionen
- Berechnungsschritte und steuerliche Regeln direkt in der App dokumentiert

## So funktioniert es

1. In IBKR einloggen: Performance & Berichte → Flex-Abfragen
2. Neue Flex Query anlegen: alle Sektionen aktivieren, Format XML, Zeitraum auf das gewünschte Steuerjahr
3. XML-Datei herunterladen
4. Auf der [Webseite](https://konvexinvestment.github.io/ibkr-steuer/) hochladen, fertig

Das Steuerjahr wird aus dem Berichtszeitraum der XML automatisch erkannt. Für genauere Ergebnisse können zusätzlich Vorjahres-XMLs hochgeladen werden: sie liefern das Stillhalter-Matching über den Jahreswechsel und eine vollständigere Lot-Historie für die FX-Berechnung.

Die technischen XML-Felder sind grundsätzlich unabhängig von der Portalsprache. Am besten getestet ist derzeit eine englische Flex Query; einzelne von IBKR gelieferte Beschreibungstexte können sprachabhängig sein.

## Datenschutz

Die Berechnung läuft **vollständig im Browser** via WebAssembly (stlite/Pyodide). Es gibt keinen Anwendungsserver und keine Datenbank: Die hochgeladenen IBKR-Dateien werden weder übertragen noch gespeichert und verlassen den Rechner nicht.

Beim Laden der Seite holt der Browser allerdings die Laufzeitumgebung (stlite/Pyodide) von jsDelivr und die Schriftart von Google Fonts. Diese CDNs sehen dabei die IP-Adresse, bekommen aber keine Kontodaten zu sehen. Wer auch das vermeiden möchte, klont das Repository und startet die App lokal (siehe unten).

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

### Tests

Die synthetischen Regressionstests in `tests/` laufen ohne weitere Daten und decken die bekannten Fehlerklassen ab (Cross-Year-Stillhalter, FX-FIFO, KAP-INV, Kategorie-Routing und weitere):

```bash
python tests/test_cross_year_series.py
python tests/test_asset_category_routing.py
python tests/test_fx_negative_balance.py
```

`python run_tests.py` startet zusätzlich Audit-Szenarien gegen echte IBKR-Exporte und ruft danach alle in seiner `SYNTHETIC_TESTS`-Liste registrierten Testdateien aus `tests/` auf. Der Runner benötigt dafür ein lokales `test_data/`-Verzeichnis, das echte Kontodaten enthält und deshalb nicht im Repository liegt. Ohne dieses Verzeichnis bricht er ab.

## Steuerliche Grundlagen

- [Abschlussvermerk zur InvStG-Produktklassifikation](docs/product_classification_research.md): Rechtskette, Produktentscheidungen, Primärquellen und Grenze der Bindungswirkung
- §1 Abs. 2, 3 InvStG i.V.m. §1 Abs. 1 KAGB: Fondsbegriff und Ausschluss von Personengesellschaften
- §20 EStG: Einkünfte aus Kapitalvermögen
- §20 InvStG: Teilfreistellung für Investmentfonds (Abs. 1 Aktienfonds, Abs. 2 Mischfonds, Abs. 3 Immobilien- und Auslands-Immobilienfonds)
- §23 Abs. 1 Nr. 2 EStG: private Veräußerungsgeschäfte (Anlage SO)
- BMF-Schreiben vom 14.05.2025: Einzelfragen zur Abgeltungsteuer (Rn. 25-35 Stillhalter, Rn. 118-123 Verlustverrechnung, Rn. 131 Fremdwährungsguthaben, Rn. 148 anrechenbare ausländische Steuer)
- Jahressteuergesetz 2024: Abschaffung des 20.000 Euro Caps für Verluste aus Termingeschäften
- Zeilennummern beziehen sich auf Anlage KAP und KAP-INV 2025

## Haftungsausschluss

Dieses Tool dient ausschließlich zur Unterstützung bei der Steuererklärung. Es ersetzt keine steuerliche Beratung. Die Ergebnisse sind vor der Abgabe zu prüfen, bei Unsicherheiten ist eine Steuerberatung hinzuzuziehen. Keine Gewähr für Richtigkeit oder Vollständigkeit.

## Lizenz

MIT License
