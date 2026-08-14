# Produktklassifikation InvStG – Abschlussvermerk 2026

Stand: 8. August 2026

## Ergebnis

Die bisherige Quarantäne für Single-Asset-/Grantor-Trusts, Commodity Pools,
US-Spot-Krypto-Trusts, Closed-End Funds, JEPI/JEPQ sowie USO/UNG ist fachlich
aufgelöst. Für keines dieser bekannten Produkte wird ein alter Rechenpfad mehr
als provisorischer Fallback benutzt.

Die Entscheidungen sind:

- Passive Trust-/Commodity-Strukturen wie GLD, IAU, SLV, GLDM, PALL, IBIT,
  GBTC, ETHE, BSOL, DBC, DBB, GSG, WEAT, BDRY, FXE, CPER, SVIX sowie die
  ProShares-Trust-II-Produkte BOIL, UVXY, UGL und AGQ sind Investmentfonds nach
  § 1 Abs. 2 InvStG i.V.m. § 1 Abs. 1 KAGB. Mangels verbindlicher
  Kapitalbeteiligungsquote sind sie `sonstiger_fonds` mit 0 %
  Teilfreistellung.
- Registered Closed-End Funds sind ebenfalls Investmentfonds. BXMX, ETB und
  EXG sind wegen ihrer verbindlichen 80-%-Stammaktienpolitik `aktienfonds`.
  Die übrigen geprüften CEFs sind `sonstiger_fonds`, weil ihre maßgeblichen
  Dokumente keine verbindliche Quote von mehr als 50 % qualifizierenden
  Kapitalbeteiligungen festlegen.
- JEPI und JEPQ sind `aktienfonds`: Mindestens 80 % sind in Equity Securities
  anzulegen, während ELNs auf 20 % begrenzt sind. Damit verbleibt selbst am
  zulässigen ELN-Maximum eine Quote von mindestens 60 % direkter Equity
  Securities.
- USO und UNG sind Delaware Limited Partnerships. Sie sind nach § 1 Abs. 3
  Nr. 2 InvStG ausdrücklich vom InvStG ausgeschlossen. Die richtige Folge ist
  aber nicht der alte pauschale Topf-2-Pfad. Maßgeblich ist die anteilige
  Jahresallokation der Personengesellschaft; ohne K-1/K-3 oder einen
  gleichwertigen Nachweis ist die Berechnung tatsächlich nicht möglich und
  wird deshalb sichtbar blockiert.
- ETNs und andere Schuldverschreibungen bleiben `no_invstg`. Deutsche
  physisch hinterlegte Gold-ETCs mit individuellem Lieferanspruch bleiben
  `anlage_so`.

Die exakten aktiven Kennnummern, 70 ersetzten Altkennnummern und 49
produktspezifischen Evidenzobjekte stehen in `etf_classification.py`. Alle
aktiven ISIN-Schlüssel bestehen die ISO-6166-Prüfziffer.

Eine künftig erstmals auftauchende ISIN erhält keine automatische
`sonstiger_fonds`-Klassifikation. Sie bleibt technisch unklassifiziert und
erzeugt erst dann eine KAP-INV-Formularzeile, einen steuerpflichtigen
Kontrollwert oder anrechenbare Fonds-Quellensteuer, wenn der Nutzer eine
Fondsart auswählt und ausdrücklich bestätigt. Rohwerte bleiben zur Prüfung
sichtbar.

## 1. Gesetzliche Entscheidungskette

### 1.1 Fondsbegriff

[§ 1 Abs. 2 InvStG](https://www.gesetze-im-internet.de/invstg_2018/BJNR173010016.html)
knüpft für den Investmentfondsbegriff an Investmentvermögen nach § 1 Abs. 1
KAGB an. Die aufsichtsrechtliche Entscheidung der BaFin bindet die steuerliche
Einordnung nicht.

[§ 1 Abs. 1 KAGB](https://www.gesetze-im-internet.de/kagb/__1.html) verlangt
einen Organismus für gemeinsame Anlagen, der von mehreren Anlegern Kapital
einsammelt, es nach einer festgelegten Anlagestrategie zum Nutzen dieser
Anleger investiert und kein operativ tätiges Unternehmen außerhalb des
Finanzsektors ist.

Weder die aktuelle Fassung des InvStG noch § 1 Abs. 1 KAGB verlangen eine
Risikomischung oder mehrere Basiswerte. Ein passiver Single-Asset-Trust kann
daher Investmentvermögen sein. Entscheidend sind Kapitalpooling, festgelegte
Anlagestrategie, Anlegernutzen und fehlende operative Tätigkeit.

### 1.2 Gesetzliche Ausschlüsse

[§ 1 Abs. 3 InvStG](https://www.gesetze-im-internet.de/invstg_2018/BJNR173010016.html)
schließt bestimmte Rechtsträger aus. Relevant ist insbesondere Nr. 2 für
Personengesellschaften oder vergleichbare ausländische Rechtsformen. Eine
ausdrücklich als Delaware Limited Partnership errichtete Struktur wie USO oder
UNG fällt darunter.

Der US-Bundessteuerstatus als „partnership“ oder „grantor trust“ ersetzt den
deutschen Rechtstypenvergleich nicht. Bei USO/UNG folgt der Ausschluss bereits
aus der zivilrechtlichen LP-Form. Bei Statutory-/Grantor-Trusts folgt aus einer
US-steuerlichen transparenten Behandlung dagegen nicht automatisch eine
deutsche Personengesellschaft.

### 1.3 Teilfreistellung ist eine zweite, getrennte Frage

[§ 2 Abs. 6, 7, 8 und 12 InvStG](https://www.gesetze-im-internet.de/invstg_2018/BJNR173010016.html)
unterscheidet den Fondsbegriff von der Fondsquote:

- Aktienfonds: fortlaufend mehr als 50 % Kapitalbeteiligungen;
- Mischfonds: fortlaufend mindestens 25 % Kapitalbeteiligungen;
- maßgeblich sind grundsätzlich die Anlagebedingungen bzw. Satzung oder das
  vergleichbare konstituierende Dokument;
- Derivate, Forderungen und Beteiligungen an transparenten Partnerships sind
  keine qualifizierenden Kapitalbeteiligungen allein wegen ihrer wirtschaftlichen
  Aktienähnlichkeit.

Fehlt eine verbindliche Quote, ist 0 % Teilfreistellung die gesetzliche
Standardfolge. Das ist kein offener Klassifikationsfall. Ein Anleger kann nach
den gesetzlichen Nachweisregeln eine tatsächlich durchgehend erfüllte Quote
belegen; dafür wären aber vollständige periodengerechte Bestandsdaten des
gesamten Geschäftsjahrs erforderlich. Eine aktuelle Portfolioaufnahme genügt
nicht.

[§ 20 InvStG](https://www.gesetze-im-internet.de/invstg_2018/BJNR173010016.html)
bestimmt die Teilfreistellung. Ergänzend wurde das aktuelle
[BMF-Schreiben vom 24. November 2025](https://www.bundesfinanzministerium.de/Content/DE/Downloads/BMF_Schreiben/Steuerarten/Investmentsteuer/2025-11-24-anwendungsfragen-InvStG.pdf?__blob=publicationFile&v=4)
herangezogen.

## 2. Single-Asset-, Rohstoff- und Krypto-Trusts

Die SEC-Berichte zeigen für die geprüften Produkte jeweils einen rechtlich
separaten Trust/Pool, ausgegebene handelbare Anteile, Kapital mehrerer Anleger,
eine festgelegte passive Anlage- oder Indexstrategie, Vermögen für Rechnung der
Anteilinhaber und keine operative Geschäftstätigkeit. Das erfüllt den weiten
KAGB-Fondsbegriff.

Wesentliche Produktbelege:

- [GLD 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1222333/000143774925036305/gld20250930_10k.htm)
- [IAU 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1278680/000143774926006055/iau20251231_10k.htm)
- [SLV 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1330568/000143774926006059/slv20251231_10k.htm)
- [GLDM 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1618181/000143774925036313/gldm20250930_10k.htm)
- [PALL 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1459862/000199937126004881/pall-10k_123125.htm)
- [IBIT 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1980994/000143774926006058/bit20251231_10k.htm)
- [GBTC 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1588489/000119312526071956/gbtc-20251231.htm)
- [ETHE 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1725210/000119312526071965/ethe-20251231.htm)
- [BSOL 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/2045872/000119312526117404/bsol-20251231.htm)
- [DBC 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1328237/000119312526083563/dbc-20251231.htm)
- [DBB 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1383084/000119312526083547/dbb-20251231.htm)
- [GSG 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1332174/000143774926006060/gsg20251231_10k.htm)
- [WEAT 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1471824/000143774926006385/weat20251231_10k.htm)
- [BDRY 2024/25 Form 10-K](https://www.sec.gov/Archives/edgar/data/1610940/000121390025092470/ea0256815-10k_amplify.htm)
- [FXE 2024 Form 10-K](https://www.sec.gov/Archives/edgar/data/1328598/000095017025027270/fxe-20241231.htm)
- [CPER/US Commodity Funds 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1479247/000110465926021525/usci-20251231x10k.htm)
- [SVIX/VS Trust 2024 Form 10-K](https://www.sec.gov/Archives/edgar/data/1793497/000101376225004207/ea0230452-10k_vstrust.htm)
- [ProShares Trust II 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1415311/000119312526077441/d785470d10k.htm)

### Kein pauschaler Anlage-SO-Lieferanspruch

Bei GLD, IAU, SLV, GLDM und PALL können gewöhnliche Retail-Anteilinhaber nicht
einzelne Anteile nach eigenem Ermessen in Metall einlösen. Rücknahmen erfolgen
über autorisierte Teilnehmer und Mindestkörbe. Damit fehlt der für die
deutschen Gold-ETC-Fälle prägende individuelle Sachlieferungsanspruch.

Die ältere
[BFH-Entscheidung VIII R 7/17](https://www.bundesfinanzhof.de/en/entscheidungen/entscheidungen-online/decision-detail/STRE202110158/)
betraf zudem die frühere Rechtslage und einen konkret ausgestalteten Schweizer
Gold-ETF. Sie trägt keine pauschale Umqualifizierung sämtlicher
Edelmetall-Trusts in § 23 EStG. Xetra-Gold/EUWAX-Produkte mit dokumentiertem
individuellem Lieferanspruch bleiben separat `anlage_so`.

## 3. Closed-End Funds

Die SEC-Unterlagen belegen bei allen 22 Produkten eine registrierte
Closed-End-Investmentgesellschaft bzw. einen Investment-Trust. „Closed-end“
beschreibt den Rücknahme-/Kapitalmechanismus, beseitigt aber weder das
Kapitalpooling noch die festgelegte Anlagestrategie.

### Aktienfonds

- BXMX – [Prospekt/SEC](https://www.sec.gov/Archives/edgar/data/1298699/000119312518296103/d614442d497.htm)
- ETB – [Annual Report/SEC](https://www.sec.gov/Archives/edgar/data/1308927/000119312526081329/d17837dncsr.htm)
- EXG – [Annual Report/SEC](https://www.sec.gov/Archives/edgar/data/1379438/000119312525336951/d92028dncsr.htm)

Die Dokumente schreiben jeweils mindestens 80 % Stammaktien fest. Das reicht
für die Aktienfondsquote.

### Sonstige Fonds mit 0 % Teilfreistellung

AWP, CBA, EAD, EFR, EIM, EVV, FMO, HQL, JPC, MFD, MMU, NCZ, NMZ, NTG, RIF,
RNP, SRV, UTF und VGM bleiben Investmentfonds, haben aber keine verbindliche
Mindestquote von mehr als 50 % qualifizierenden Kapitalbeteiligungen. Gründe
sind je nach Produkt Anleihen, Kommunalanleihen, Loans, Preferreds,
Convertible-Strukturen, MLP-/Midstream-Beteiligungen oder ein gemischtes
Aktien-/Debt-Mandat.

Beispiele aktueller Primärbelege:

- [EIM 2025 Annual Report](https://www.sec.gov/Archives/edgar/data/1176984/000119312525299957/d67021dncsr.htm)
- [EVV 2025 Annual Report](https://www.sec.gov/Archives/edgar/data/1222922/000119312525129438/d16534dncsr.htm)
- [JPC 2025 Annual Report](https://www.sec.gov/Archives/edgar/data/1216583/000119312525230231/d938948dncsr.htm)
- [MMU 2025 Annual Report](https://www.sec.gov/Archives/edgar/data/886043/000113322825007762/wammf-efp16583_ncsr.htm)
- [NMZ 2025 Annual Report](https://www.sec.gov/Archives/edgar/data/1266585/000119312526006359/d36352dncsr.htm)
- [SRV 2025 Annual Report](https://www.sec.gov/Archives/edgar/data/1400897/000139834426002464/fp0096655-3_ncsrixbrl.htm)
- [UTF SEC filing](https://www.sec.gov/Archives/edgar/data/1275617/000119312525211588/d23969d424b2.htm)
- [VGM SEC filing](https://www.sec.gov/Archives/edgar/data/880892/000119312526210681/d116361dncsr.htm)

Die vollständige produktgenaue Quellenliste ist maschinenlesbar in
`PRODUCT_CLASSIFICATION_EVIDENCE` hinterlegt.

## 4. JEPI und JEPQ

Die aktuellen Summary Prospectuses belegen die verbindliche Equity-Policy und
die Begrenzung der ELNs:

- [JEPI Summary Prospectus 2025](https://www.sec.gov/Archives/edgar/data/1485894/000119312525247725/d66087d497k.htm)
- [JEPQ Summary Prospectus 2025](https://www.sec.gov/Archives/edgar/data/1485894/000119312525247756/d812991d497k.htm)

Auch wenn ELNs für Zwecke der US-Anlagepolitik als Equity Securities erfasst
werden, begrenzt das Dokument sie auf 20 %. Bei einer 80-%-Equity-Policy
verbleiben deshalb mindestens 60 % nicht-ELN-Equity. Die >50-%-Schwelle des
InvStG ist damit nicht nur aus aktuellen Holdings abgeleitet, sondern durch die
zulässigen Portfoliogrenzen abgesichert.

## 5. USO und UNG: Warum nur hier Eingabedaten fehlen

Die aktuellen SEC-Jahresberichte bezeichnen beide Produkte ausdrücklich als
Delaware Limited Partnerships und beschreiben die jährliche Zurechnung von
steuerlichen Einkünften unabhängig von Ausschüttungen:

- [USO 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1327068/000110465926021501/uso-20251231x10k.htm)
- [UNG 2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1376227/000110465926021507/ung-20251231x10k.htm)

Ein Flex-Query-Export enthält realisierte Broker-PnL, Ausschüttungen und
Quellensteuer, aber nicht die anteilige steuerliche Jahresallokation. Diese
Brokerwerte sind deshalb nur ein Plausibilitätsnachweis. Das Tool schließt
Trades, Tageskurs-Deltas, Ausschüttungen, Quellensteuer und sonstige
ergebniswirksame Cash-Buchungen aus KAP/KAP-INV aus und nennt konkret die
fehlenden Unterlagen. Das ist ein echter Datenblocker und keine vorsorgliche
Rechtsquarantäne.

## 6. Grad der Rechtssicherheit

Die implementierten Entscheidungen sind anhand geltender Gesetze,
BMF-Verwaltungsauffassung und produktspezifischer Primärdokumente begründbar.
Eine allgemeine Softwareklassifikation erzeugt jedoch keine Bindungswirkung
gegenüber dem zuständigen Finanzamt.

Für eine rechtlich bindende Vorabentscheidung zu einem konkreten, noch nicht
verwirklichten Sachverhalt ist eine verbindliche Auskunft nach
[§ 89 Abs. 2 AO](https://www.gesetze-im-internet.de/ao_1977/__89.html) in
Verbindung mit der
[Bindungsregel des § 2 StAuskV](https://www.gesetze-im-internet.de/stauskv/__2.html)
das einschlägige Instrument. Das ist die Grenze dessen, was „endgültig“ ohne
individuellen Verwaltungsakt bedeuten kann.

Diese Grenze führt nicht zurück zur Quarantäne: Die Software verwendet die
begründete Klassifikation. Sie weist lediglich dort einen Blocker aus, wo die
steuerliche Bemessungsgrundlage tatsächlich nicht aus den vorhandenen Daten
ermittelt werden kann.

## Nicht bearbeiteter Umfang

Auf ausdrücklichen Wunsch wurden CORP-, TTAX- und DE-Fondssteuer-Routing
(Punkt 6 des vorherigen Reviews) nicht geändert.
