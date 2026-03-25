# IBKR Steuerbericht - Anlage KAP 2025

Automatische Berechnung der Anlage KAP (Zeilen 19, 20, 22, 23, 41) aus Interactive Brokers Flex Query XML-Exporten. Laeuft komplett im Browser - keine Installation, keine Cloud, keine Daten verlassen Ihren Rechner.

**[Jetzt starten](https://konvexinvestment.github.io/ibkr-steuer/)**

## Features

- Unterstuetzt EUR- und USD-Basiswaehrung
- Zwei-Toepfe-Berechnung (Aktien vs. Sonstiges) gemaess §20 Abs. 6 EStG
- Automatische Erkennung und Trennung von Stillhalterpraemien bei Covered-Call-Assignments (BMF Rn. 25-35)
- Korrekte Behandlung von Optionen, Futures, T-Bills, Anleihen, Dividenden, Zinsen
- Quellensteuer-Anrechnung (Zeile 41)
- Detaillierte Erklaerung aller steuerlichen Regeln direkt in der App
- Textreport-Download fuer die Steuererklaerung

## So funktioniert es

1. In IBKR einloggen → Reports → Flex Queries
2. Neue Flex Query erstellen (alle Sektionen, XML-Format, Zeitraum 01.01.2025 - 31.12.2025)
3. XML-Datei herunterladen
4. Auf der Webseite hochladen - fertig

## Datenschutz

Die App laeuft **vollstaendig im Browser** via WebAssembly (stlite/Pyodide). Es gibt keinen Server, keine Datenbank, keine Analyse. Ihre IBKR-Daten verlassen zu keinem Zeitpunkt Ihren Rechner.

## Lokale Entwicklung

```bash
git clone https://github.com/KonvexInvestment/ibkr-steuer.git
cd ibkr-steuer
python3 -m venv .venv
source .venv/bin/activate
pip install streamlit
streamlit run gui_app/app.py
```

## Steuerliche Grundlagen

- §20 EStG - Einkuenfte aus Kapitalvermoegen
- BMF-Schreiben vom 14.05.2025 - "Einzelfragen zur Abgeltungsteuer"
- Jahressteuergesetz 2024 - Abschaffung des €20.000-Caps fuer Termingeschaefteverluste
- Anlage KAP 2025 Formular

## Haftungsausschluss

Dieses Tool dient ausschliesslich zur Unterstuetzung bei der Steuererklarung. Es ersetzt keine steuerliche Beratung. Bitte pruefen Sie die Ergebnisse und konsultieren Sie bei Unsicherheiten einen Steuerberater. Keine Gewaehr fuer Richtigkeit oder Vollstaendigkeit.

## Lizenz

MIT License
