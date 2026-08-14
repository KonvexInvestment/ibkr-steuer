"""
ETF-Klassifikation nach deutschem InvStG (Investmentsteuergesetz).

Lookup-Tabelle der ~250 meistgehandelten ETFs/ETPs mit ISIN und Klassifikation
fuer die Berechnung der Teilfreistellung:
  - aktienfonds:      30% Teilfreistellung (>= 51% Aktienquote)
  - mischfonds:       15% Teilfreistellung (25-50% Aktienquote)
  - sonstiger_fonds:   0% Teilfreistellung (Anleihen-ETFs, Derivate-Fonds)
  - no_invstg:        Kein Investmentfonds i.S.d. InvStG (insb. Schuldverschreibungen)
  - personengesellschaft: Auslaendische Personengesellschaft; InvStG ausgeschlossen,
                      Besteuerung nur mit Jahresallokation (z.B. K-1/K-3) berechenbar
  - anlage_so:        Privates Veraeusserungsgeschaeft (ss 23 Abs. 1 Nr. 2 EStG)
                      Physische Gold-ETCs mit Lieferanspruch (BFH VIII R 35/14
                      zur Veraeusserung, VIII R 4/15 zur physischen Auslieferung).
                      Nach 1 Jahr Haltedauer steuerfrei (Spekulationsfrist).

Rechtsgrundlage: ss 20 InvStG (Teilfreistellung), ss 2 InvStG (Investmentfonds-Definition)
                 ss 23 Abs. 1 Nr. 2 EStG (private Veraeusserungsgeschaefte)

Aktive ISIN-Schluessel werden technisch nach ISO 6166 validiert. Fuer die 2026
geprueften Sonderprodukte ist die Entscheidung mit Primärquellen in
PRODUCT_CLASSIFICATION_EVIDENCE dokumentiert; eine alte Rechenroute wird nicht
mehr als stiller Review-Fallback weiterverwendet.
"""

import csv
import io

# ── Teilfreistellungssaetze ──────────────────────────────────────────────────
TEILFREISTELLUNG = {
    'aktienfonds':     0.30,   # 30 % — ss 20 Abs. 1 S. 1 InvStG
    'mischfonds':      0.15,   # 15 % — ss 20 Abs. 2 InvStG (halber Aktienfonds-Satz)
    'immobilienfonds': 0.60,   # 60 % — ss 20 Abs. 3 S. 1 InvStG
    'auslands_immobilienfonds': 0.80,  # 80 % — ss 20 Abs. 3 S. 2 InvStG
    'sonstiger_fonds': 0.00,   # 0 %  — keine Teilfreistellung
    'no_invstg':       None,   # kein Investmentfonds → normale Besteuerung nach ss 20 EStG
    'personengesellschaft': None,  # transparente Jahresallokation erforderlich
    'anlage_so':       None,   # privates Veräußerungsgeschäft → ss 23 EStG (nicht ss 20)
}


# Belastbar hinterlegte DBA-Hoechstsaetze fuer Ausschüttungen einzelner Fonds.
# Absichtlich keine Ableitung aus dem blossen ISIN-Laenderpraefix beliebiger
# Produkte: Sitz, Ertragsart und Erstattungsanspruch muessen je Produkt belegt
# sein. Fehlt ein Eintrag, kann die Steuerberechnung nur den deutschen
# 25-%-Hoechstbetrag anwenden und weist den Vorgang sichtbar zur DBA-Pruefung
# aus.
#
# Beta-Annahme fuer aktive US-Fonds: 15 % nach Art. 10 Abs. 2 Buchst. b
# DBA-USA. Diese Ableitung ist bewusst nur im optionalen DBA-Beta-Modus aktiv;
# Produktrechtsform, RIC-Status und konkrete Ertragsart sind noch nicht fuer
# jeden Eintrag abschliessend belegt. Review-/no_invstg-/anlage_so-Produkte
# erhalten keinen automatischen Satz.
FOREIGN_TAX_TREATY_RATES = {
    'US37954Y4834': 0.15,  # QYLD: US-Fonds, DBA-USA Dividenden-Hoechstsatz
    'US78462F1030': 0.15,  # SPY: US-Fonds, DBA-USA Dividenden-Hoechstsatz
}


# ── ETF-Lookup: ISIN → (ticker, name, classification) ────────────────────────
# Sortiert nach Kategorie, dann nach Subkategorie.
# Aktive Produkttabelle; Kennnummern- und Klassifikationsevidenz folgen direkt
# nach dem Literal.

ETF_CLASSIFICATION = {

    # ═══════════════════════════════════════════════════════════════════════════
    # AKTIENFONDS (>= 51% Aktienquote) → 30% Teilfreistellung
    # ═══════════════════════════════════════════════════════════════════════════

    # --- Breite US-Markt-Indizes (cbonds-verifiziert) ---
    'US78462F1030': ('SPY',  'SPDR S&P 500 ETF Trust',                         'aktienfonds'),   # cbonds /etf/5/
    'US4642872000': ('IVV',  'iShares Core S&P 500 ETF',                       'aktienfonds'),   # cbonds /etf/45/
    'US9229083632': ('VOO',  'Vanguard S&P 500 ETF',                           'aktienfonds'),   # cbonds /etf/33/
    'US9229087690': ('VTI',  'Vanguard Total Stock Market ETF',                 'aktienfonds'),   # cbonds /etf/39/
    'US46090E1038': ('QQQ',  'Invesco QQQ Trust (Nasdaq-100)',                  'aktienfonds'),   # cbonds, DivvyDiary
    'US25459Y2072': ('QQQE', 'Direxion NASDAQ-100 Equal Weighted Index ETF',    'aktienfonds'),   # SEC: 99.9% Common Stocks
    'US78467X1090': ('DIA',  'SPDR Dow Jones Industrial Average ETF Trust',     'aktienfonds'),   # cbonds /etf/11/
    'US46137V3574': ('RSP',  'Invesco S&P 500 Equal Weight ETF',               'aktienfonds'),   # cbonds /etf/4529/
    'US4642876555': ('IWM',  'iShares Russell 2000 ETF',                        'aktienfonds'),   # cbonds /etf/63/
    'US4642877397': ('IYR',  'iShares U.S. Real Estate ETF',                    'aktienfonds'),   # cbonds /etf/675/
    'US9229085538': ('VNQ',  'Vanguard Real Estate ETF',                        'aktienfonds'),   # cbonds /etf/31/
    'US4642876142': ('IWF',  'iShares Russell 1000 Growth ETF',                 'aktienfonds'),   # cbonds /etf/59/
    'US4642875987': ('IWD',  'iShares Russell 1000 Value ETF',                  'aktienfonds'),   # cbonds /etf/61/
    'US4642876225': ('IWB',  'iShares Russell 1000 ETF',                        'aktienfonds'),   # cbonds /etf/155/
    'US9229087443': ('VTV',  'Vanguard Value ETF',                              'aktienfonds'),   # cbonds /etf/41/
    'US9229087369': ('VUG',  'Vanguard Growth ETF',                             'aktienfonds'),   # cbonds /etf/29/
    'US78464A8541': ('SPLG', 'SPDR Portfolio S&P 500 ETF',                      'aktienfonds'),   # cbonds /etf/2099/ (ticker war SPLG, seit 10/2025 SPYM)
    'US4642873099': ('IVW',  'iShares S&P 500 Growth ETF',                      'aktienfonds'),   # cbonds /etf/165/
    'US4642874089': ('IVE',  'iShares S&P 500 Value ETF',                       'aktienfonds'),   # cbonds /etf/167/
    'US4642878049': ('IJR',  'iShares Core S&P Small-Cap ETF',                  'aktienfonds'),   # cbonds /etf/49/
    'US4642875078': ('IJH',  'iShares Core S&P Mid-Cap ETF',                    'aktienfonds'),   # cbonds /etf/47/
    'US9229086296': ('VO',   'Vanguard Mid-Cap ETF',                            'aktienfonds'),   # cbonds /etf/119/
    'US9229087518': ('VB',   'Vanguard Small-Cap ETF',                          'aktienfonds'),   # cbonds /etf/123/
    'US8085247976': ('SCHD', 'Schwab U.S. Dividend Equity ETF',                 'aktienfonds'),   # cbonds /etf/355/ — ISIN korrigiert (US46138G7060 = TAN)
    'US9219088443': ('VIG',  'Vanguard Dividend Appreciation ETF',              'aktienfonds'),   # cbonds /etf/23/
    'US4642871689': ('DVY',  'iShares Select Dividend ETF',                     'aktienfonds'),   # cbonds /etf/69/
    'US78464A7634': ('SDY',  'SPDR S&P Dividend ETF',                           'aktienfonds'),   # cbonds /etf/17/
    'US9219464065': ('VYM',  'Vanguard High Dividend Yield ETF',                'aktienfonds'),   # cbonds /etf/117/
    'US46434V6213': ('DGRO', 'iShares Core Dividend Growth ETF',                'aktienfonds'),

    # --- US-Sektor-ETFs (cbonds-verifiziert) ---
    'US81369Y5069': ('XLE',  'Energy Select Sector SPDR Fund',                  'aktienfonds'),   # cbonds /etf/87/
    'US81369Y6059': ('XLF',  'Financial Select Sector SPDR Fund',               'aktienfonds'),   # cbonds /etf/1/
    'US81369Y8030': ('XLK',  'Technology Select Sector SPDR Fund',              'aktienfonds'),   # cbonds /etf/21/
    'US81369Y2090': ('XLV',  'Health Care Select Sector SPDR Fund',             'aktienfonds'),   # cbonds /etf/89/
    'US81369Y1001': ('XLB',  'Materials Select Sector SPDR Fund',               'aktienfonds'),   # cbonds /etf/273/
    'US81369Y7040': ('XLI',  'Industrial Select Sector SPDR Fund',              'aktienfonds'),   # cbonds /etf/91/
    'US81369Y3080': ('XLP',  'Consumer Staples Select Sector SPDR Fund',        'aktienfonds'),   # cbonds /etf/81/
    'US81369Y8865': ('XLU',  'Utilities Select Sector SPDR Fund',               'aktienfonds'),   # cbonds /etf/111/
    'US81369Y4070': ('XLY',  'Consumer Discretionary Select Sector SPDR Fund',  'aktienfonds'),   # cbonds /etf/79/
    'US81369Y8527': ('XLC',  'Communication Services Select Sector SPDR Fund',  'aktienfonds'),   # cbonds /etf/2271/
    'US81369Y8600': ('XLRE', 'Real Estate Select Sector SPDR Fund',             'aktienfonds'),   # cbonds /etf/2273/
    'US4642875235': ('SOXX', 'iShares Semiconductor ETF',                       'aktienfonds'),   # cbonds /etf/893/
    'US92189F6768': ('SMH',  'VanEck Semiconductor ETF',                        'aktienfonds'),   # cbonds /etf/2857/
    'US37954Y8306': ('COPX', 'Global X Copper Miners ETF',                      'aktienfonds'),   # cbonds /etf/7777/
    'US4642875565': ('IBB',  'iShares Biotechnology ETF',                       'aktienfonds'),   # cbonds /etf/619/
    'US33734X8469': ('CIBR', 'First Trust NASDAQ Cybersecurity ETF',            'aktienfonds'),   # cbonds /etf/2649/

    # --- Internationale Aktien-ETFs (cbonds-verifiziert) ---
    'US4642874659': ('EFA',  'iShares MSCI EAFE ETF',                           'aktienfonds'),   # cbonds /etf/53/
    'US4642872349': ('EEM',  'iShares MSCI Emerging Markets ETF',               'aktienfonds'),   # cbonds /etf/55/
    'US9220428588': ('VWO',  'Vanguard FTSE Emerging Markets ETF',              'aktienfonds'),   # cbonds /etf/27/
    'US9219097683': ('VXUS', 'Vanguard Total International Stock ETF',          'aktienfonds'),   # cbonds /etf/419/
    'US46434G1031': ('IEMG', 'iShares Core MSCI Emerging Markets ETF',          'aktienfonds'),   # cbonds /etf/503/
    'US46432F8427': ('IEFA', 'iShares Core MSCI EAFE ETF',                      'aktienfonds'),   # cbonds /etf/833/
    'US46434G8226': ('EWJ',  'iShares MSCI Japan ETF',                          'aktienfonds'),   # cbonds /etf/57/
    'US4642868222': ('EWW',  'iShares MSCI Mexico ETF',                         'aktienfonds'),   # cbonds /etf/601/
    'US4642864007': ('EWZ',  'iShares MSCI Brazil ETF',                         'aktienfonds'),   # cbonds /etf/147/
    'US4642871846': ('FXI',  'iShares China Large-Cap ETF',                     'aktienfonds'),   # cbonds /etf/133/
    'US5007673065': ('KWEB', 'KraneShares CSI China Internet ETF',              'aktienfonds'),   # cbonds /etf/3125/
    'US4642868065': ('EWG',  'iShares MSCI Germany ETF',                        'aktienfonds'),   # cbonds /etf/593/
    'US46435G3341': ('EWU',  'iShares MSCI United Kingdom ETF',                 'aktienfonds'),   # cbonds /etf/615/
    'US4642867729': ('EWY',  'iShares MSCI South Korea ETF',                    'aktienfonds'),   # cbonds /etf/609/
    'US46434G7723': ('EWT',  'iShares MSCI Taiwan ETF',                         'aktienfonds'),   # cbonds /etf/3263/
    'US46429B5984': ('INDA', 'iShares MSCI India ETF',                          'aktienfonds'),   # cbonds /etf/817/
    'US97717W4226': ('EPI',  'WisdomTree India Earnings Fund',                  'aktienfonds'),   # cbonds /etf/2813/

    # --- Europaeische UCITS-ETFs (physische Aktienholdings, IE/LU-domiziliert) ---
    'IE00BYVQ9F29': ('NQSE', 'iShares Nasdaq 100 UCITS ETF EUR Hedged Acc',    'aktienfonds'),
    'IE00BYYW2V44': ('SPPE', 'SPDR S&P 500 UCITS ETF EUR Hedged Acc',          'aktienfonds'),

    # --- Gold/Silber/Rohstoff-Miner (physische Aktienholdings) ---
    'US92189F1066': ('GDX',  'VanEck Gold Miners ETF',                          'aktienfonds'),   # cbonds /etf/785/
    'US92189F7915': ('GDXJ', 'VanEck Junior Gold Miners ETF',                   'aktienfonds'),   # cbonds /etf/787/
    'US37954Y8488': ('SIL',  'Global X Silver Miners ETF',                      'aktienfonds'),   # cbonds /etf/749/
    'US0321086490': ('SILJ', 'Amplify Junior Silver Miners ETF',                'aktienfonds'),   # cbonds /etf/10435/
    'US92189H8051': ('REMX', 'VanEck Rare Earth/Strategic Metals ETF',          'aktienfonds'),   # cbonds, physische Miner-Aktien
    'US46434G8556': ('RING', 'iShares MSCI Global Gold Miners ETF',             'aktienfonds'),   # cbonds /etf/1109/
    'US46434G8481': ('PICK', 'iShares MSCI Global Metals & Mining Producers ETF', 'aktienfonds'),  # cbonds /etf/1107/
    'US78464A7550': ('XME',  'SPDR S&P Metals & Mining ETF',                    'aktienfonds'),   # cbonds /etf/793/
    'US37954Y8553': ('LIT',  'Global X Lithium & Battery Tech ETF',             'aktienfonds'),   # cbonds /etf/7739/
    'US37954Y8710': ('URA',  'Global X Uranium ETF',                            'aktienfonds'),   # cbonds /etf/7767/
    'US85208P3038': ('URNM', 'Sprott Uranium Miners ETF',                       'aktienfonds'),   # cbonds /etf/14121/

    # --- Energie (physische Aktienholdings) ---
    'US78468R5569': ('XOP',  'SPDR S&P Oil & Gas Exploration & Production ETF',  'aktienfonds'),   # cbonds /etf/735/
    'US92189H6071': ('OIH',  'VanEck Oil Services ETF',                          'aktienfonds'),   # cbonds /etf/269/

    # --- Thematische / aktiv gemanagte Aktien-ETFs ---
    'US00214Q1040': ('ARKK', 'ARK Innovation ETF',                              'aktienfonds'),   # cbonds /etf/11013/
    'US00214Q2055': ('ARKG', 'ARK Genomic Revolution ETF',                      'aktienfonds'),   # cbonds
    'US26922A8421': ('JETS', 'U.S. Global Jets ETF',                             'aktienfonds'),   # cbonds /etf/11727/
    'US92189H8390': ('BUZZ', 'VanEck Social Sentiment ETF',                      'aktienfonds'),   # cbonds /etf/11057/
    'US00768Y4531': ('MSOS', 'AdvisorShares Pure US Cannabis ETF',               'aktienfonds'),   # cbonds /etf/11481/
    'US46138G7060': ('TAN',  'Invesco Solar ETF',                                'aktienfonds'),   # cbonds /etf/3033/
    'US4642882249': ('ICLN', 'iShares Global Clean Energy ETF',                  'aktienfonds'),   # cbonds /etf/1023/

    # --- Weitere Aktien-ETFs (Björn-Audit, April 2026) ---
    'US37950E4089': ('CHIQ', 'Global X MSCI China Consumer Discretionary ETF',   'aktienfonds'),
    'US37954Y4420': ('CLOU', 'Global X Cloud Computing ETF',                     'aktienfonds'),
    'US0321084099': ('DIVO', 'Amplify CWP Enhanced Dividend Income ETF',         'aktienfonds'),
    'US37954Y4677': ('EBIZ', 'Global X E-commerce ETF',                          'aktienfonds'),
    'US4642891232': ('ENZL', 'iShares MSCI New Zealand ETF',                     'aktienfonds'),
    'US4642865095': ('EWC',  'iShares MSCI Canada ETF',                          'aktienfonds'),
    'US33735T1097': ('FDD',  'First Trust STOXX European Select Dividend Index Fund', 'aktienfonds'),
    'US33733E3027': ('FDN',  'First Trust Dow Jones Internet Index Fund',        'aktienfonds'),
    'US3369201039': ('FPX',  'First Trust US Equity Opportunities ETF',          'aktienfonds'),
    'US4642887941': ('IAI',  'iShares U.S. Broker-Dealers & Securities Exchanges ETF', 'aktienfonds'),
    'US4642874733': ('IWS',  'iShares Russell Mid-Cap Value ETF',                'aktienfonds'),
    'US4642877884': ('IYF',  'iShares U.S. Financials ETF',                      'aktienfonds'),
    'US78464A7717': ('KCE',  'SPDR S&P Capital Markets ETF',                     'aktienfonds'),
    'US5007678502': ('KGRN', 'KraneShares MSCI China Clean Technology ETF',      'aktienfonds'),
    'US78464A7899': ('KIE',  'SPDR S&P Insurance ETF',                           'aktienfonds'),
    'US78464A7394': ('KRE',  'SPDR S&P Regional Banking ETF',                    'aktienfonds'),
    'US46137V1008': ('PPA',  'Invesco Aerospace & Defense ETF',                  'aktienfonds'),
    'US46137V1180': ('PSP',  'Invesco Global Listed Private Equity ETF',         'aktienfonds'),
    'US8085242019': ('SCHX', 'Schwab U.S. Large-Cap ETF',                        'aktienfonds'),
    'US8123501061': ('SHLD', 'Global X Defense Tech ETF',                        'aktienfonds'),
    'US92189F2056': ('SLX',  'VanEck Steel ETF',                                 'aktienfonds'),
    'US4642867158': ('TUR',  'iShares MSCI Turkey ETF',                          'aktienfonds'),
    'US9220428745': ('VGK',  'Vanguard FTSE Europe ETF',                         'aktienfonds'),
    'US78464A6313': ('XAR',  'SPDR S&P Aerospace & Defense ETF',                 'aktienfonds'),
    'US78468R5494': ('XES',  'SPDR S&P Oil & Gas Equipment & Services ETF',      'aktienfonds'),
    'US78464A8889': ('XHB',  'SPDR S&P Homebuilders ETF',                        'aktienfonds'),
    'US78464A7147': ('XRT',  'SPDR S&P Retail ETF',                              'aktienfonds'),
    'US78464A8624': ('XSD',  'SPDR S&P Semiconductor ETF',                       'aktienfonds'),
    'US78467Y1073': ('MDY',  'SPDR S&P MidCap 400 ETF Trust',                    'aktienfonds'),
    'US4642882405': ('ACWX', 'iShares MSCI ACWI ex U.S. ETF',                    'aktienfonds'),
    'US37954Y4834': ('QYLD', 'Global X NASDAQ 100 Covered Call ETF',             'aktienfonds'),  # 2025-Prospekt: Nasdaq-100-Aktienkorb mit gedeckten Calls

    # --- Commodity-ETFs (Futures/Derivate-basiert, keine Aktien) ---
    'US46138B1035': ('DBC',  'Invesco DB Commodity Index Tracking Fund',         'sonstiger_fonds'),  # Commodity Pool, Futures
    'US46428R1077': ('GSG',  'iShares S&P GSCI Commodity-Indexed Trust',         'sonstiger_fonds'),  # Commodity Pool, Futures
    'US46090F1003': ('PDBC', 'Invesco Optimum Yield Diversified Commodity Strategy ETF', 'sonstiger_fonds'),  # Commodity-Futures via Subsidiary
    'US91232N2071': ('USO',  'United States Oil Fund LP',                        'personengesellschaft'),
    'US9123184098': ('UNG',  'United States Natural Gas Fund LP',                'personengesellschaft'),

    # --- Leveraged/Inverse ETFs (Derivate-basiert, keine physischen Aktien → 0% TFS) ---
    # §2 Abs. 8 InvStG: Swaps/Futures zaehlen nicht zur Aktienquote
    'US74347X8314': ('TQQQ', 'ProShares UltraPro QQQ (3x Nasdaq-100)',          'sonstiger_fonds'),   # cbonds /etf/757/
    'US74350P6759': ('SQQQ', 'ProShares UltraPro Short QQQ (-3x Nasdaq-100)',   'sonstiger_fonds'),   # cbonds /etf/3063/
    'US74347X8645': ('UPRO', 'ProShares UltraPro S&P500 (3x S&P 500)',         'sonstiger_fonds'),   # cbonds /etf/4207/
    'US74350P6593': ('SPXU', 'ProShares UltraPro Short S&P500 (-3x S&P 500)',  'sonstiger_fonds'),   # cbonds /etf/10929/
    'US74347R1077': ('SSO',  'ProShares Ultra S&P500 (2x S&P 500)',            'sonstiger_fonds'),   # cbonds /etf/4189/
    'US25459W4583': ('SOXL', 'Direxion Daily Semiconductor Bull 3X Shares',     'sonstiger_fonds'),   # cbonds /etf/5683/
    'US25460G1123': ('SOXS', 'Direxion Daily Semiconductor Bear 3X Shares',     'sonstiger_fonds'),   # cbonds /etf/5681/
    'US74347Y7489': ('BOIL', 'ProShares Ultra Bloomberg Natural Gas (2x)',       'sonstiger_fonds'),  # 2x leveraged Nat Gas Futures
    'US25460G7815': ('NUGT', 'Direxion Daily Gold Miners Index Bull 2X Shares',  'sonstiger_fonds'),  # 2x leveraged, Swaps/Futures
    'US25461A4783': ('DUST', 'Direxion Daily Gold Miners Index Bear 2X Shares',  'sonstiger_fonds'),  # 2x inverse, Swaps/Futures
    'US25460G8318': ('JNUG', 'Direxion Daily Junior Gold Miners Index Bull 2X',  'sonstiger_fonds'),  # 2x leveraged, Swaps/Futures
    'LU0411078552': ('XS2L', 'Xtrackers S&P 500 2x Leveraged Daily Swap UCITS ETF', 'sonstiger_fonds'),  # 2x leveraged, Swap-basiert
    'LU0290358497': ('XEON', 'Xtrackers II EUR Overnight Rate Swap UCITS ETF 1C', 'sonstiger_fonds'),  # OGAW-Swapfonds auf €STR, keine Aktienquote

    # ═══════════════════════════════════════════════════════════════════════════
    # SONSTIGER FONDS (0% Teilfreistellung) — Anleihen, Volatilitaet, Derivate
    # ═══════════════════════════════════════════════════════════════════════════

    # --- US-Staatsanleihen-ETFs (cbonds-verifiziert) ---
    'US4642874329': ('TLT',  'iShares 20+ Year Treasury Bond ETF',              'sonstiger_fonds'),  # cbonds /etf/493/
    'US4642874576': ('SHY',  'iShares 1-3 Year Treasury Bond ETF',              'sonstiger_fonds'),  # cbonds /etf/129/
    'US4642874402': ('IEF',  'iShares 7-10 Year Treasury Bond ETF',             'sonstiger_fonds'),  # cbonds /etf/497/
    'US4642886794': ('SHV',  'iShares Short Treasury Bond ETF',                 'sonstiger_fonds'),  # cbonds /etf/657/
    'US4642871762': ('TIP',  'iShares TIPS Bond ETF',                           'sonstiger_fonds'),  # cbonds /etf/71/
    'US46429B7477': ('STIP', 'iShares 0-5 Year TIPS Bond ETF',                 'sonstiger_fonds'),  # cbonds /etf/1077/
    'US46436E5776': ('GOVZ', 'iShares 25+ Year Treasury STRIPS Bond ETF',      'sonstiger_fonds'),  # cbonds /etf/9375/
    'US92206C1027': ('VGSH', 'Vanguard Short-Term Treasury ETF',                'sonstiger_fonds'),  # cbonds /etf/1695/
    'US92206C7065': ('VGIT', 'Vanguard Intermediate-Term Treasury ETF',         'sonstiger_fonds'),  # cbonds /etf/1689/
    'US92206C8477': ('VGLT', 'Vanguard Long-Term Treasury ETF',                 'sonstiger_fonds'),  # cbonds /etf/767/
    'US78464A6644': ('SPTL', 'SPDR Portfolio Long Term Treasury ETF',           'sonstiger_fonds'),  # cbonds /etf/2103/
    'US78468R6633': ('BIL',  'SPDR Bloomberg 1-3 Month T-Bill ETF',             'sonstiger_fonds'),  # cbonds /etf/2181/
    'US46436E7186': ('SGOV', 'iShares 0-3 Month Treasury Bond ETF',             'sonstiger_fonds'),  # cbonds /etf/7457/

    # --- Breite US-Anleihenmarkt-ETFs (cbonds-verifiziert) ---
    'US4642872265': ('AGG',  'iShares Core U.S. Aggregate Bond ETF',            'sonstiger_fonds'),  # cbonds /etf/51/
    'US9219378356': ('BND',  'Vanguard Total Bond Market ETF',                  'sonstiger_fonds'),  # cbonds /etf/37/
    'US78464A6495': ('SPAB', 'SPDR Portfolio Aggregate Bond ETF',               'sonstiger_fonds'),  # cbonds /etf/2083/

    # --- Unternehmensanleihen-ETFs (cbonds-verifiziert) ---
    'US4642872422': ('LQD',  'iShares iBoxx $ Investment Grade Corporate Bond ETF', 'sonstiger_fonds'),  # cbonds /etf/75/
    'US4642885135': ('HYG',  'iShares iBoxx $ High Yield Corporate Bond ETF',   'sonstiger_fonds'),  # cbonds /etf/73/
    'US46435U8532': ('USHY', 'iShares Broad USD High Yield Corporate Bond ETF', 'sonstiger_fonds'),  # cbonds /etf/1353/
    'US92206C8709': ('VCIT', 'Vanguard Intermediate-Term Corporate Bond ETF',   'sonstiger_fonds'),  # cbonds /etf/403/
    'US92206C4096': ('VCSH', 'Vanguard Short-Term Corporate Bond ETF',          'sonstiger_fonds'),  # cbonds /etf/121/
    'US92206C8139': ('VCLT', 'Vanguard Long-Term Corporate Bond ETF',           'sonstiger_fonds'),  # cbonds /etf/1677/
    'US4642886380': ('IGIB', 'iShares 5-10 Year Investment Grade Corporate Bond ETF', 'sonstiger_fonds'),  # cbonds /etf/145/
    'US4642886463': ('IGSB', 'iShares 1-5 Year Investment Grade Corporate Bond ETF',  'sonstiger_fonds'),  # cbonds /etf/973/

    # --- MBS / ABS / CMBS (cbonds-verifiziert) ---
    'US4642885887': ('MBB',  'iShares MBS ETF',                                 'sonstiger_fonds'),  # cbonds /etf/687/
    'US82889N5251': ('MTBA', 'Simplify MBS ETF',                                'sonstiger_fonds'),  # cbonds /etf/200421/
    'US46429B3666': ('CMBS', 'iShares CMBS ETF',                                'sonstiger_fonds'),  # cbonds /etf/1123/

    # --- Internationale Anleihen-ETFs (cbonds-verifiziert) ---
    'US9219468850': ('VWOB', 'Vanguard Emerging Markets Government Bond ETF',   'sonstiger_fonds'),  # cbonds /etf/1687/
    'US4642882819': ('EMB',  'iShares J.P. Morgan USD Emerging Markets Bond ETF', 'sonstiger_fonds'),  # cbonds /etf/557/
    'US92203J4076': ('BNDX', 'Vanguard Total International Bond ETF',           'sonstiger_fonds'),  # cbonds /etf/1685/

    # --- Volatilitaets-ETFs (strukturiert als Fonds, halten Derivate) ---
    'US82889N8636': ('SVOL', 'Simplify Volatility Premium ETF',                 'sonstiger_fonds'),  # cbonds /etf/11381/
    'US92891H1014': ('SVIX', '-1x Short VIX Futures ETF',                       'sonstiger_fonds'),  # cbonds /etf/14315/
    'US74347Y6804': ('UVXY', 'ProShares Ultra VIX Short-Term Futures ETF',      'sonstiger_fonds'),  # cbonds /etf/835/

    # --- Weitere Leveraged/Inverse ETFs (Björn-Audit, April 2026) ---
    'US25459Y8764': ('CURE', 'Direxion Daily Healthcare Bull 3X Shares',         'sonstiger_fonds'),
    'US25460E6611': ('DFEN', 'Direxion Daily Aerospace & Defense Bull 3X Shares','sonstiger_fonds'),
    'US25460G1537': ('DPST', 'Direxion Daily Regional Banks Bull 3X Shares',     'sonstiger_fonds'),
    'US25490K2814': ('EDC',  'Direxion Daily MSCI Emerging Markets Bull 3X Shares', 'sonstiger_fonds'),
    'US74347X5005': ('EFO',  'ProShares Ultra MSCI EAFE',                        'sonstiger_fonds'),
    'US25459Y2809': ('EURL', 'Direxion Daily FTSE Europe Bull 3X Shares',        'sonstiger_fonds'),
    'US25459Y6941': ('FAS',  'Direxion Daily Financial Bull 3X Shares',          'sonstiger_fonds'),
    'US25490K3317': ('INDL', 'Direxion Daily MSCI India Bull 2x Shares',         'sonstiger_fonds'),
    'US25460E2818': ('MEXX', 'Direxion Daily MSCI Mexico Bull 3X Shares',        'sonstiger_fonds'),
    'US25490K5965': ('NAIL', 'Direxion Daily Homebuilders & Supplies Bull 3X Shares', 'sonstiger_fonds'),
    'US25460E6462': ('PILL', 'Direxion Daily Pharmaceutical & Medical Bull 3X Shares', 'sonstiger_fonds'),
    'US74347R2067': ('QLD',  'ProShares Ultra QQQ',                              'sonstiger_fonds'),
    'US74350P4853': ('RXD',  'ProShares UltraShort Health Care',                 'sonstiger_fonds'),
    'US25459W8626': ('SPXL', 'Direxion Daily S&P 500 Bull 3X Shares',            'sonstiger_fonds'),
    'US74347B2016': ('TBT',  'ProShares UltraShort 20+ Year Treasury',          'sonstiger_fonds'),
    'US25459W8477': ('TNA',  'Direxion Daily Small Cap Bull 3X Shares',          'sonstiger_fonds'),
    'US74347W6012': ('UGL',  'ProShares Ultra Gold',                             'sonstiger_fonds'),
    'US74347X7993': ('URTY', 'ProShares UltraPro Russell 2000',                  'sonstiger_fonds'),
    'US25460E7114': ('UTSL', 'Direxion Daily Utilities Bull 3X Shares',          'sonstiger_fonds'),
    'US25459Y8012': ('WANT', 'Direxion Daily Consumer Discretionary Bull 3X Shares', 'sonstiger_fonds'),
    'US25460G1958': ('YINN', 'Direxion Daily FTSE China Bull 3X Shares',         'sonstiger_fonds'),
    'US74347W3530': ('AGQ',  'ProShares Ultra Silver',                           'sonstiger_fonds'),

    # --- Weitere Anleihen-ETFs (Björn-Audit) ---
    'US46138G8050': ('BAB',  'Invesco Taxable Municipal Bond ETF',               'sonstiger_fonds'),
    'US78464A3591': ('CWB',  'SPDR Bloomberg Convertible Securities ETF',        'sonstiger_fonds'),
    'US92189H4092': ('HYD',  'VanEck High-Yield Muni ETF',                       'sonstiger_fonds'),
    'US92189F3872': ('SHYD', 'VanEck Short High Yield Muni ETF',                 'sonstiger_fonds'),
    'US78468R6229': ('JNK',  'SPDR Bloomberg High Yield Bond ETF',               'sonstiger_fonds'),
    'US46138E7849': ('PCY',  'Invesco Emerging Markets Sovereign Debt ETF',      'sonstiger_fonds'),
    'US46138E6361': ('PICB', 'Invesco International Corporate Bond ETF',         'sonstiger_fonds'),
    'US97717Y5270': ('USFR', 'WisdomTree Floating Rate Treasury Fund',           'sonstiger_fonds'),
    'US02072L5654': ('BOXX', 'Alpha Architect 1-3 Month Box ETF',               'sonstiger_fonds'),
    'US82889N8552': ('PFIX', 'Simplify Interest Rate Hedge ETF',                 'sonstiger_fonds'),

    # --- Covered-Call / Income-Strategie ETFs (Derivate-basiert) ---
    'US46641Q3323': ('JEPI', 'JPMorgan Equity Premium Income ETF',               'aktienfonds'),
    'US46654Q2030': ('JEPQ', 'JPMorgan Nasdaq Equity Premium Income ETF',        'aktienfonds'),
    'US88634T7827': ('NFLY', 'YieldMax NFLX Option Income Strategy ETF',         'sonstiger_fonds'),
    'US88634T7744': ('NVDY', 'YieldMax NVDA Option Income Strategy ETF',         'sonstiger_fonds'),

    # --- Commodity-Fonds (Futures-basiert, Fund-Struktur) ---
    'US46431W8534': ('COMT', 'iShares GSCI Commodity Dynamic Roll Strategy ETF', 'sonstiger_fonds'),
    'US88166A8707': ('WEAT', 'Teucrium Wheat Fund',                              'sonstiger_fonds'),  # Post-Reverse-Split ISIN
    'US46140H7008': ('DBB',  'Invesco DB Base Metals Fund',                      'sonstiger_fonds'),  # Rohstoff-Futures (Aluminium, Kupfer, Zink)
    'US03210A1079': ('BDRY', 'Breakwave Dry Bulk Shipping ETF',                  'sonstiger_fonds'),
    'US97717Y5684': ('GDE',  'WisdomTree Efficient Gold Plus Equity Strategy Fund', 'sonstiger_fonds'),

    # --- Waehrungs-ETFs ---
    'US46138K1034': ('FXE',  'Invesco CurrencyShares Euro Currency Trust',       'sonstiger_fonds'),

    # --- Sonstige Strategie-ETFs ---
    'US00162Q1067': ('BTAL', 'AGF U.S. Market Neutral Anti-Beta Fund',           'sonstiger_fonds'),
    'US37950E4733': ('MLPA', 'Global X MLP ETF',                                 'sonstiger_fonds'),
    # AMLP hält ausschließlich MLP-Units (Publicly Traded Partnerships =
    # Personengesellschaften). Kapitalbeteiligungen i.S.d. §2 Abs. 8 InvStG sind
    # nur Anteile an KAPITALgesellschaften — MLP-Units zählen NICHT zur
    # Aktienquote → 0% Teilfreistellung statt 30% (wie MLPA, gleiche Assetklasse).
    # Audit-Fix H2 2026-06-10. Quelle: SSGA/ALPS-Prospekt, 100% MLP/PTP-Holdings.
    'US00162Q4525': ('AMLP', 'Alerian MLP ETF',                                  'sonstiger_fonds'),

    # ═══════════════════════════════════════════════════════════════════════════
    # SONSTIGE FONDS — passive Trust-/Commodity-Strukturen ohne Aktienquote
    # § 1 Abs. 2 InvStG verweist auf den weiten Investmentvermoegensbegriff des
    # § 1 Abs. 1 KAGB. Weder ein einzelner Basiswert noch Grantor-/Statutory-
    # Trust-Status schliessen den Fondsbegriff aus. Ohne verbindliche Quote gilt
    # 0 % Teilfreistellung. Produktbelege: PRODUCT_CLASSIFICATION_EVIDENCE.
    # ═══════════════════════════════════════════════════════════════════════════

    # --- Physische Edelmetall-Trusts ---
    'US78463V1070': ('GLD',  'SPDR Gold Shares',                                'sonstiger_fonds'),
    'US4642852044': ('IAU',  'iShares Gold Trust',                              'sonstiger_fonds'),
    'US46428Q1094': ('SLV',  'iShares Silver Trust',                            'sonstiger_fonds'),
    'US98149E3036': ('GLDM', 'SPDR Gold MiniShares Trust',                      'sonstiger_fonds'),

    # --- US-Spot-Krypto-Trusts ---
    'US46438F1012': ('IBIT', 'iShares Bitcoin Trust ETF',                       'sonstiger_fonds'),
    'US3896381072': ('ETHE', 'Grayscale Ethereum Staking ETF',                  'sonstiger_fonds'),
    'US3896371099': ('GBTC', 'Grayscale Bitcoin Trust ETF',                     'sonstiger_fonds'),
    'US0919481095': ('BSOL', 'Bitwise Solana Staking ETF',                      'sonstiger_fonds'),

    # --- Kein Investmentfonds: Schuldverschreibungen / ETPs ---
    'DE000A4A59D2': ('BSOL', 'Bitwise Solana Staking ETP (DE)',                 'no_invstg'),  # deutsche ETP-Variante (Schuldverschreibung, kein Fonds)
    'CH1471826029': ('HYPE', '21Shares Hyperliquid Staking ETP',                'no_invstg'),  # Krypto-ETN (CH), einzelner Basiswert; gilt für HYPEEUR und HYPEUSD
    'US74347G4405': ('BITO', 'ProShares Bitcoin Strategy ETF',                  'sonstiger_fonds'),  # Investment Company Act 1940, BTC-Futures + Treasuries → InvStG

    # --- ETNs (Schuldverschreibungen, kein Fonds) ---
    'US06748M1962': ('VXX',  'iPath Series B S&P 500 VIX Short-Term Futures ETN', 'no_invstg'),  # ETN = Inhaberschuldverschreibung
    'US06748M1889': ('VXZ',  'iPath Series B S&P 500 VIX Mid-Term Futures ETN',  'no_invstg'),  # ETN
    'US0636793855': ('FNGU', 'MicroSectors FANG+ Index 3X Leveraged ETN',        'no_invstg'),
    'US06746P5228': ('JJG',  'iPath Series B Bloomberg Grains Subindex Total Return ETN', 'no_invstg'),
    'US17325K5294': ('DLBR', 'VelocityShares Short LIBOR ETN',                   'no_invstg'),

    # --- Deutsche Gold-ETCs (physisch besichert, Lieferanspruch → Anlage SO) ---
    # BFH VIII R 35/14 (Veräußerung) und VIII R 4/15 (physische Auslieferung):
    # Xetra-Gold = privates Veräußerungsgeschäft (§23 EStG)
    # Nach 1 Jahr Haltedauer steuerfrei (Spekulationsfrist)
    'DE000EWG2LD7': ('EWG2',  'EUWAX Gold II',                                   'anlage_so'),  # physisches Gold-ETC, Lieferanspruch
    'DE000EWG0LD1': ('GOLD1', 'EUWAX Gold I',                                    'anlage_so'),  # physisches Gold-ETC, Lieferanspruch
    'DE000A0S9GB0': ('4GLD',  'Xetra-Gold',                                      'anlage_so'),  # physisches Gold-ETC, Lieferanspruch

    # --- Weitere physische Edelmetall-ETCs mit Lieferanspruch (Anlage SO) ---
    # Analog zu deutschen Gold-ETCs: physisch besichert, Lieferanspruch → § 23 Abs. 1 Nr. 2 EStG
    # Nach 1 Jahr Haltedauer steuerfrei (Spekulationsfrist)
    'JE00BQRFDY49': ('WSLV',  'WisdomTree Core Physical Silver',                 'anlage_so'),  # physisches Silber-ETC, Lieferanspruch (Issue #51)

    # --- Gehebelte/Inverse Rohstoff-ETPs (kein Investmentfonds) ---
    'IE00B6X4BP29': ('3GOS',  'WisdomTree Gold 3x Daily Short',                  'no_invstg'),  # gehebeltes ETP, Schuldverschreibung

    # --- Weitere physische / futuresbasierte Trusts ---
    'US0032621023': ('PALL',  'abrdn Physical Palladium Shares ETF',             'sonstiger_fonds'),
    'US9129087964': ('CPER',  'United States Copper Index Fund',                 'sonstiger_fonds'),

    # --- Registered Closed-End Funds ---
    # Nur BXMX, ETB und EXG belegen eine verbindliche >50%-Kapitalquote. Bei den
    # uebrigen CEFs ist 0 % die gesetzliche Rechtsfolge ohne nachgewiesene Quote,
    # nicht ein Klassifikations-Pruefstatus (§ 2 Abs. 6-8 InvStG:
    # Fondsdefinitionen Abs. 6/7, Kapitalbeteiligungen Abs. 8).
    'US00302L1089': ('AWP',  'abrdn Global Premier Properties Fund',             'sonstiger_fonds'),
    'US6706ER1015': ('BXMX', 'Nuveen S&P 500 Buy-Write Income Fund',             'aktienfonds'),
    'US1846911030': ('CBA',  'ClearBridge Energy Midstream Opportunity Fund',    'sonstiger_fonds'),
    'US94987B1052': ('EAD',  'Allspring Income Opportunities Fund',              'sonstiger_fonds'),
    'US27828Q1058': ('EFR',  'Eaton Vance Senior Floating-Rate Trust',           'sonstiger_fonds'),
    'US27827X1019': ('EIM',  'Eaton Vance Municipal Bond Fund',                  'sonstiger_fonds'),
    'US27828X1000': ('ETB',  'Eaton Vance Tax-Managed Buy-Write Income Fund',    'aktienfonds'),
    'US27828H1059': ('EVV',  'Eaton Vance Limited Duration Income Fund',         'sonstiger_fonds'),
    'US27829F1084': ('EXG',  'Eaton Vance Tax-Managed Global Diversified Equity Income Fund', 'aktienfonds'),
    'US31647Q1067': ('FMO',  'Fiduciary/Claymore Energy Infrastructure Fund',    'sonstiger_fonds'),
    'US87911K1007': ('HQL',  'Tekla Life Sciences Investors',                    'sonstiger_fonds'),
    'US67073B1061': ('JPC',  'Nuveen Preferred & Income Opportunities Fund',     'sonstiger_fonds'),
    'US55607W1009': ('MFD',  'Macquarie/First Trust Global Infrastructure/Utilities Dividend Fund', 'sonstiger_fonds'),
    'US95766M1053': ('MMU',  'Western Asset Managed Municipals Fund',            'sonstiger_fonds'),
    'US0188251096': ('NCZ',  'Virtus Convertible & Income Fund II',              'sonstiger_fonds'),
    'US6706821039': ('NMZ',  'Nuveen Municipal High Income Opportunity Fund',    'sonstiger_fonds'),
    'US89148B1017': ('NTG',  'Tortoise Midstream Energy Fund',                   'sonstiger_fonds'),
    'US76970B1017': ('RIF',  'RMR Real Estate Income Fund',                      'sonstiger_fonds'),
    'US19247X1000': ('RNP',  'Cohen & Steers REIT and Preferred Income Fund',    'sonstiger_fonds'),
    'US2316312014': ('SRV',  'NXG Cushing Midstream Energy Fund',                'sonstiger_fonds'),
    'US19248A1097': ('UTF',  'Cohen & Steers Infrastructure Fund',               'sonstiger_fonds'),
    'US46131M1062': ('VGM',  'Invesco Trust for Investment Grade Municipals',    'sonstiger_fonds'),
}


# ── Technische Validierung und steuerlicher Pruefstatus ─────────────────────

def is_valid_isin(isin: str) -> bool:
    """Validate an ISIN using its ISO-6166/Luhn check digit."""
    if not isinstance(isin, str) or len(isin) != 12 or not isin.isalnum():
        return False
    expanded = ''.join(
        char if char.isdigit() else str(ord(char.upper()) - 55)
        for char in isin
    )
    total = 0
    for index, char in enumerate(reversed(expanded)):
        value = int(char)
        if index % 2:
            value *= 2
            if value > 9:
                value -= 9
        total += value
    return total % 10 == 0


# Alte fehlerhafte Kennnummern bleiben ausschliesslich als nachvollziehbarer
# Korrektur-Audit erhalten. Sie sind keine aktiven Lookup-Schluessel. Der dritte
# Wert verweist auf die primaere Emittenten-/SEC-Quellengruppe unten.
ISIN_CORRECTIONS = {
    'US26922A2303': ('DGRO', 'US46434V6213', 'ishares'),
    'US37950E4090': ('CHIQ', 'US37950E4089', 'global_x'),
    'US78467X2090': ('DIA', 'US78467X1090', 'state_street'),
    'US0321088883': ('DIVO', 'US0321084099', 'amplify'),
    'US4642872341': ('EEM', 'US4642872349', 'ishares'),
    'US4642877730': ('EWJ', 'US46434G8226', 'ishares'),
    'US4642883523': ('EWT', 'US46434G7723', 'ishares'),
    'US33734X1054': ('FDN', 'US33733E3027', 'first_trust'),
    'US33733E1008': ('FPX', 'US3369201039', 'first_trust'),
    'US4642875017': ('IAI', 'US4642887941', 'ishares'),
    'US4642876557': ('IWM', 'US4642876555', 'ishares'),
    'US4642877696': ('IWS', 'US4642874733', 'ishares'),
    'US4642871771': ('IYF', 'US4642877884', 'ishares'),
    'US78464A7939': ('KCE', 'US78464A7717', 'state_street'),
    'US78464A7468': ('KIE', 'US78464A7899', 'state_street'),
    'US37954Y8559': ('LIT', 'US37954Y8553', 'global_x'),
    'US46090E2017': ('PPA', 'US46137V1008', 'invesco'),
    'US8085248694': ('SCHX', 'US8085242019', 'schwab'),
    'US92189F6093': ('SLX', 'US92189F2056', 'vaneck'),
    'US46138G1031': ('TAN', 'US46138G7060', 'invesco'),
    'US9220427752': ('VGK', 'US9220428745', 'vanguard'),
    'US9229088637': ('VOO', 'US9229083632', 'vanguard'),
    'US9229087286': ('VTV', 'US9229087443', 'vanguard'),
    'US78464A7307': ('XAR', 'US78464A6313', 'state_street'),
    'US78464A8690': ('XES', 'US78468R5494', 'state_street'),
    'US78464A8504': ('XHB', 'US78464A8889', 'state_street'),
    'US78464A7144': ('XRT', 'US78464A7147', 'state_street'),
    'US78464A8488': ('XSD', 'US78464A8624', 'state_street'),
    'US78467Y1070': ('MDY', 'US78467Y1073', 'state_street'),
    'US4642883984': ('ACWX', 'US4642882405', 'ishares'),
    'US74347A8351': ('SSO', 'US74347R1077', 'proshares'),
    'US74347X8492': ('UVXY', 'US74347Y6804', 'proshares'),
    'US25459L8820': ('CURE', 'US25459Y8764', 'direxion'),
    'US25459L7642': ('DFEN', 'US25460E6611', 'direxion'),
    'US25459L7691': ('DPST', 'US25460G1537', 'direxion'),
    'US74347F7061': ('EDC', 'US25490K2814', 'direxion'),
    'US74347F7022': ('EFO', 'US74347X5005', 'proshares'),
    'US25459L7984': ('EURL', 'US25459Y2809', 'direxion'),
    'US25459L7078': ('FAS', 'US25459Y6941', 'direxion'),
    'US25459L6659': ('INDL', 'US25490K3317', 'direxion'),
    'US25459L7896': ('MEXX', 'US25460E2818', 'direxion'),
    'US25459L7918': ('NAIL', 'US25490K5965', 'direxion'),
    'US69347Q1076': ('PILL', 'US25460E6462', 'direxion'),
    'US74347A8440': ('QLD', 'US74347R2067', 'proshares'),
    'US25459L7609': ('RXD', 'US74350P4853', 'proshares'),
    'US25459L7136': ('SPXL', 'US25459W8626', 'direxion'),
    'US25459L7285': ('TNA', 'US25459W8477', 'direxion'),
    'US74347B7421': ('URTY', 'US74347X7993', 'proshares'),
    'US25459L7376': ('UTSL', 'US25460E7114', 'direxion'),
    'US25459L7437': ('YINN', 'US25460G1958', 'direxion'),
    'US25459L7094': ('NUGT', 'US25460G7815', 'direxion'),
    'US78468R7068': ('CWB', 'US78464A3591', 'state_street'),
    'US4642885133': ('HYG', 'US4642885135', 'ishares'),
    'US4642872429': ('LQD', 'US4642872422', 'ishares'),
    'US78468R8785': ('JNK', 'US78468R6229', 'state_street'),
    'US69347A5369': ('PICB', 'US46138E6361', 'invesco'),
    'US4642871763': ('TIP', 'US4642871762', 'ishares'),
    'US4642878501': ('COMT', 'US46431W8534', 'ishares'),
    'US46138G1013': ('DBC', 'US46138B1035', 'invesco'),
    'US88107A1051': ('WEAT', 'US88166A8707', 'teucrium'),
    'US11410J2026': ('BDRY', 'US03210A1079', 'amplify'),
    'US97717W8281': ('GDE', 'US97717Y5684', 'wisdomtree'),
    'US18500Q1040': ('GLDM', 'US98149E3036', 'state_street'),
    'US3837861092': ('GBTC', 'US3896371099', 'grayscale'),
    'US62386A6997': ('FNGU', 'US0636793855', 'bmo'),
    'US06742L4785': ('JJG', 'US06746P5228', 'barclays_jjg'),
    'US06742W5R66': ('DLBR', 'US17325K5294', 'citi'),
    'US01924U1097': ('PALL', 'US0032621023', 'abrdn'),
    'US46138G7896': ('PCY', 'US46138E7849', 'invesco'),
    'US06747R4772': ('VXX', 'US06748M1962', 'barclays_vxx'),
}

IDENTIFIER_PRIMARY_SOURCES = {
    'abrdn': 'https://www.sec.gov/Archives/edgar/data/1459862/000199937126004881/pall-10k_123125.htm',
    'amplify': 'https://amplifyetfs.com/',
    'barclays_jjg': 'https://www.sec.gov/Archives/edgar/data/312070/000119312522208297/d340152dfwp.htm',
    'barclays_vxx': 'https://www.sec.gov/Archives/edgar/data/312070/000191870425013577/form424b2.htm',
    'bmo': 'https://microsectors.com/insights/bmo-announces-upcoming-ticker-symbol-change-for-microsectors-fang-3x-leveraged-etns/',
    'citi': 'https://www.sec.gov/Archives/edgar/data/831001/000095010317007894/dp79533_424b2-liboretns.htm',
    'direxion': 'https://www.direxion.com/etfs',
    'first_trust': 'https://www.ftportfolios.com/Retail/Etf/EtfSummary.aspx',
    'global_x': 'https://www.globalxetfs.com/funds',
    'grayscale': 'https://www.sec.gov/Archives/edgar/data/1588489/000119312526071956/gbtc-20251231.htm',
    'invesco': 'https://www.invesco.com/us/financial-products/etfs/product-detail',
    'ishares': 'https://www.ishares.com/us/products/etf-investments',
    'proshares': 'https://www.proshares.com/our-etfs/find-leveraged-and-inverse-etfs',
    'schwab': 'https://www.schwabassetmanagement.com/products/etfs',
    'state_street': 'https://www.ssga.com/us/en/intermediary/capabilities/etfs/fund-finder',
    'teucrium': 'https://teucrium.com/weat',
    'vaneck': 'https://www.vaneck.com/us/en/investments/',
    'vanguard': 'https://investor.vanguard.com/investment-products/list/etfs',
    'wisdomtree': 'https://www.wisdomtree.com/investments/etfs',
}

LEGAL_PRIMARY_SOURCES = {
    'invstg': 'https://www.gesetze-im-internet.de/invstg_2018/BJNR173010016.html',
    'kagb_1': 'https://www.gesetze-im-internet.de/kagb/__1.html',
    'estg_20': 'https://www.gesetze-im-internet.de/estg/__20.html',
    'estg_23': 'https://www.gesetze-im-internet.de/estg/__23.html',
    'ao_89': 'https://www.gesetze-im-internet.de/ao_1977/__89.html',
    'stauskv_2': 'https://www.gesetze-im-internet.de/stauskv/__2.html',
    'invstg_bmf_2025': ('https://www.bundesfinanzministerium.de/Content/DE/Downloads/'
                        'BMF_Schreiben/Steuerarten/Investmentsteuer/2025-11-24-'
                        'anwendungsfragen-InvStG.pdf?__blob=publicationFile&v=4'),
}

LEGAL_PRIMARY_SOURCE_LABELS = {
    'invstg': 'Investmentsteuergesetz',
    'kagb_1': '§ 1 KAGB',
    'estg_20': '§ 20 EStG',
    'estg_23': '§ 23 EStG',
    'ao_89': '§ 89 AO',
    'stauskv_2': '§ 2 StAuskV',
    'invstg_bmf_2025': 'BMF-Anwendungsschreiben zum InvStG (24.11.2025)',
}

CLASSIFICATION_CATALOG_AS_OF = '2026-08-08'

_CLASSIFICATION_PRESENTATION = {
    'aktienfonds': {
        'label': 'Aktienfonds',
        'tax_route': 'Anlage KAP-INV',
        'legal_form': 'Investmentfonds (Tabellenzuordnung)',
        'legal_basis': '§ 2 Abs. 6 und § 20 Abs. 1 InvStG',
        'legal_source_keys': ('invstg', 'kagb_1'),
        'reason': (
            'Tabellenzuordnung als Aktienfonds: maßgebliche Fondsquote von mehr '
            'als 50 %; daraus folgen 30 % Teilfreistellung.'
        ),
    },
    'mischfonds': {
        'label': 'Mischfonds',
        'tax_route': 'Anlage KAP-INV',
        'legal_form': 'Investmentfonds (Tabellenzuordnung)',
        'legal_basis': '§ 2 Abs. 7 und § 20 Abs. 2 InvStG',
        'legal_source_keys': ('invstg', 'kagb_1'),
        'reason': (
            'Tabellenzuordnung als Mischfonds: maßgebliche Fondsquote von '
            'mindestens 25 %; daraus folgen 15 % Teilfreistellung.'
        ),
    },
    'immobilienfonds': {
        'label': 'Immobilienfonds',
        'tax_route': 'Anlage KAP-INV',
        'legal_form': 'Investmentfonds (Tabellenzuordnung)',
        'legal_basis': '§ 2 Abs. 9 und § 20 Abs. 3 InvStG',
        'legal_source_keys': ('invstg', 'kagb_1'),
        'reason': (
            'Tabellenzuordnung als Immobilienfonds; daraus folgen 60 % '
            'Teilfreistellung.'
        ),
    },
    'auslands_immobilienfonds': {
        'label': 'Auslands-Immobilienfonds',
        'tax_route': 'Anlage KAP-INV',
        'legal_form': 'Investmentfonds (Tabellenzuordnung)',
        'legal_basis': '§ 2 Abs. 9 und § 20 Abs. 3 InvStG',
        'legal_source_keys': ('invstg', 'kagb_1'),
        'reason': (
            'Tabellenzuordnung als Auslands-Immobilienfonds; daraus folgen '
            '80 % Teilfreistellung.'
        ),
    },
    'sonstiger_fonds': {
        'label': 'Sonstiger Fonds',
        'tax_route': 'Anlage KAP-INV',
        'legal_form': 'Investmentfonds (Tabellenzuordnung)',
        'legal_basis': '§§ 1, 2 und 20 InvStG',
        'legal_source_keys': ('invstg', 'kagb_1'),
        'reason': (
            'Tabellenzuordnung als Investmentfonds ohne belegte '
            'qualifizierende Fondsquote; deshalb 0 % Teilfreistellung.'
        ),
    },
    'no_invstg': {
        'label': 'Kein Investmentfonds',
        'tax_route': 'Anlage KAP · Topf 2',
        'legal_form': 'Schuldverschreibung/ETN/ETP (Tabellenzuordnung)',
        'legal_basis': '§ 1 InvStG; § 20 EStG',
        'legal_source_keys': ('invstg', 'estg_20'),
        'reason': (
            'Tabellenzuordnung als Schuldverschreibung, ETN oder vergleichbares '
            'ETP außerhalb des InvStG; Erfassung im regulären Kapitalertragspfad.'
        ),
    },
    'personengesellschaft': {
        'label': 'Personengesellschaft',
        'tax_route': 'Blockiert bis Jahresallokation vorliegt',
        'legal_form': 'Ausländische Personengesellschaft',
        'legal_basis': '§ 1 Abs. 3 Nr. 2 InvStG',
        'legal_source_keys': ('invstg', 'kagb_1'),
        'reason': (
            'Ausländische Personengesellschaft; das InvStG ist ausgeschlossen. '
            'Die Berechnung benötigt eine anteilige Jahresallokation und deutsche '
            'Überleitungsrechnung.'
        ),
    },
    'anlage_so': {
        'label': 'Anlage SO',
        'tax_route': 'Anlage SO · § 23 EStG',
        'legal_form': 'Edelmetall-ETC mit Sachlieferungsanspruch (Tabellenzuordnung)',
        'legal_basis': '§ 23 Abs. 1 Nr. 2 EStG; BFH VIII R 35/14 und VIII R 4/15',
        'legal_source_keys': ('estg_23',),
        'reason': (
            'Tabellenzuordnung als physisch hinterlegtes Edelmetall-ETC mit '
            'individuellem Sachlieferungsanspruch; privates '
            'Veräußerungsgeschäft mit einjähriger Haltefrist.'
        ),
    },
}


def _evidence(classification, legal_form, quota_basis, source):
    """Create one immutable-shape, primary-source classification record."""
    return {
        'status': 'verified',
        'as_of': '2026-08-08',
        'classification': classification,
        'legal_form': legal_form,
        'invstg_basis': '§ 1 Abs. 2, 3 InvStG i.V.m. § 1 Abs. 1 KAGB',
        'quota_basis': quota_basis,
        'sources': (LEGAL_PRIMARY_SOURCES['invstg'], LEGAL_PRIMARY_SOURCES['kagb_1'], source),
    }


PRODUCT_CLASSIFICATION_EVIDENCE = {}

_TRUST_FILINGS = {
    'US78463V1070': 'https://www.sec.gov/Archives/edgar/data/1222333/000143774925036305/gld20250930_10k.htm',
    'US4642852044': 'https://www.sec.gov/Archives/edgar/data/1278680/000143774926006055/iau20251231_10k.htm',
    'US46428Q1094': 'https://www.sec.gov/Archives/edgar/data/1330568/000143774926006059/slv20251231_10k.htm',
    'US98149E3036': 'https://www.sec.gov/Archives/edgar/data/1618181/000143774925036313/gldm20250930_10k.htm',
    'US0032621023': 'https://www.sec.gov/Archives/edgar/data/1459862/000199937126004881/pall-10k_123125.htm',
    'US46438F1012': 'https://www.sec.gov/Archives/edgar/data/1980994/000143774926006058/bit20251231_10k.htm',
    'US3896371099': 'https://www.sec.gov/Archives/edgar/data/1588489/000119312526071956/gbtc-20251231.htm',
    'US3896381072': 'https://www.sec.gov/Archives/edgar/data/1725210/000119312526071965/ethe-20251231.htm',
    'US0919481095': 'https://www.sec.gov/Archives/edgar/data/2045872/000119312526117404/bsol-20251231.htm',
    'US46138B1035': 'https://www.sec.gov/Archives/edgar/data/1328237/000119312526083563/dbc-20251231.htm',
    'US46140H7008': 'https://www.sec.gov/Archives/edgar/data/1383084/000119312526083547/dbb-20251231.htm',
    'US46428R1077': 'https://www.sec.gov/Archives/edgar/data/1332174/000143774926006060/gsg20251231_10k.htm',
    'US88166A8707': 'https://www.sec.gov/Archives/edgar/data/1471824/000143774926006385/weat20251231_10k.htm',
    'US03210A1079': 'https://www.sec.gov/Archives/edgar/data/1610940/000121390025092470/ea0256815-10k_amplify.htm',
    'US46138K1034': 'https://www.sec.gov/Archives/edgar/data/1328598/000095017025027270/fxe-20241231.htm',
    'US9129087964': 'https://www.sec.gov/Archives/edgar/data/1479247/000110465926021525/usci-20251231x10k.htm',
    'US92891H1014': 'https://www.sec.gov/Archives/edgar/data/1793497/000101376225004207/ea0230452-10k_vstrust.htm',
    'US74347Y7489': 'https://www.sec.gov/Archives/edgar/data/1415311/000119312526077441/d785470d10k.htm',
    'US74347Y6804': 'https://www.sec.gov/Archives/edgar/data/1415311/000119312526077441/d785470d10k.htm',
    'US74347W6012': 'https://www.sec.gov/Archives/edgar/data/1415311/000119312526077441/d785470d10k.htm',
    'US74347W3530': 'https://www.sec.gov/Archives/edgar/data/1415311/000119312526077441/d785470d10k.htm',
}
for _isin, _source in _TRUST_FILINGS.items():
    PRODUCT_CLASSIFICATION_EVIDENCE[_isin] = _evidence(
        'sonstiger_fonds', 'passiver Trust/Commodity Pool',
        'keine verbindliche Kapitalbeteiligungsquote', _source,
    )

_CEF_EQUITY_POLICY_FILINGS = {
    # SEC-Prospekte/Registrierungserklärungen: Die 80-%-Aktienpolitik ist dort
    # als Anlagepolitik beschrieben. Aktuelle Jahresberichte unten dienen nur
    # als zusätzlicher Aktualitätsbeleg, nicht als alleinige Quotenquelle.
    'US6706ER1015': 'https://www.sec.gov/Archives/edgar/data/1298699/000119312518296103/d614442d497.htm',
    'US27828X1000': 'https://www.sec.gov/Archives/edgar/data/1308927/000094039423000457/etbn2final.htm',
    'US27829F1084': 'https://www.sec.gov/Archives/edgar/data/1379438/000094039419000179/exgn2final.htm',
}

_CEF_FILINGS = {
    'US00302L1089': 'https://www.sec.gov/Archives/edgar/data/1390195/000110465925005700/tm2424447d7_424b2.htm',
    'US6706ER1015': 'https://www.sec.gov/Archives/edgar/data/1298699/000119312518296103/d614442d497.htm',
    'US1846911030': 'https://www.sec.gov/Archives/edgar/data/1517518/000119312518216973/d671572dn148c.htm',
    'US94987B1052': 'https://www.sec.gov/Archives/edgar/data/927971/000121465917005276/d824171424b2.htm',
    'US27828Q1058': 'https://www.sec.gov/Archives/edgar/data/1258623/000119312525337066/d85290dncsr.htm',
    'US27827X1019': 'https://www.sec.gov/Archives/edgar/data/1176984/000119312525299957/d67021dncsr.htm',
    'US27828X1000': 'https://www.sec.gov/Archives/edgar/data/1308927/000119312526081329/d17837dncsr.htm',
    'US27828H1059': 'https://www.sec.gov/Archives/edgar/data/1222922/000119312525129438/d16534dncsr.htm',
    'US27829F1084': 'https://www.sec.gov/Archives/edgar/data/1379438/000119312525336951/d92028dncsr.htm',
    'US31647Q1067': 'https://www.sec.gov/Archives/edgar/data/1305197/000089180420000051/gug78879-ncsr.htm',
    'US87911K1007': 'https://www.sec.gov/Archives/edgar/data/884121/000110465922125155/tm2225213d1_ncsr.htm',
    'US67073B1061': 'https://www.sec.gov/Archives/edgar/data/1216583/000119312525230231/d938948dncsr.htm',
    'US55607W1009': 'https://www.sec.gov/Archives/edgar/data/1276469/000144554622000753/mfd_ncsr.htm',
    'US95766M1053': 'https://www.sec.gov/Archives/edgar/data/886043/000113322825007762/wammf-efp16583_ncsr.htm',
    'US0188251096': 'https://www.sec.gov/Archives/edgar/data/1227857/000119312513290953/d560759dpos8c.htm',
    'US6706821039': 'https://www.sec.gov/Archives/edgar/data/1266585/000119312526006359/d36352dncsr.htm',
    'US89148B1017': 'https://www.sec.gov/Archives/edgar/data/1268533/000121390026013154/ea0272590-01_ncsr.htm',
    'US76970B1017': 'https://www.sec.gov/Archives/edgar/data/1443387/000104746911007513/a2205317zn-csr.htm',
    'US19247X1000': 'https://www.sec.gov/Archives/edgar/data/1224450/000119312521073436/d867347dncsr.htm',
    'US2316312014': 'https://www.sec.gov/Archives/edgar/data/1400897/000139834426002464/fp0096655-3_ncsrixbrl.htm',
    'US19248A1097': 'https://www.sec.gov/Archives/edgar/data/1275617/000119312525211588/d23969d424b2.htm',
    'US46131M1062': 'https://www.sec.gov/Archives/edgar/data/880892/000119312526210681/d116361dncsr.htm',
}
for _isin, _source in _CEF_FILINGS.items():
    _classification = ETF_CLASSIFICATION[_isin][2]
    _quota = ('verbindliche Mindestanlage von 80 % in Stammaktien'
              if _classification == 'aktienfonds'
              else 'keine verbindliche >50-%-Kapitalbeteiligungsquote belegt')
    _policy_source = _CEF_EQUITY_POLICY_FILINGS.get(_isin, _source)
    _record = _evidence(
        _classification, 'registrierter Closed-End Investmentfonds',
        _quota, _policy_source,
    )
    if _policy_source != _source:
        _record['sources'] += (_source,)
    PRODUCT_CLASSIFICATION_EVIDENCE[_isin] = _record

for _isin, _source in {
    'US46641Q3323': 'https://www.sec.gov/Archives/edgar/data/1485894/000119312525247725/d66087d497k.htm',
    'US46654Q2030': 'https://www.sec.gov/Archives/edgar/data/1485894/000119312525247756/d812991d497k.htm',
}.items():
    PRODUCT_CLASSIFICATION_EVIDENCE[_isin] = _evidence(
        'aktienfonds', 'offener US-Registered-Investment-Company-Fonds',
        'mindestens 80 % Equity Securities; ELNs auf 20 % begrenzt, damit >50 % Direktaktien',
        _source,
    )

# Zwei im Audit-Upload nur in der Vorjahreshistorie vorkommende ETFs. Sie
# gehoeren trotzdem in den vollstaendigen Transparenzkatalog: QQQE ist ein
# offener US-Indexfonds mit belegter direkter Aktienquote; XEON ist ein
# Luxemburger OGAW-Swapfonds auf den Euro-Tagesgeldsatz ohne Aktienquote.
PRODUCT_CLASSIFICATION_EVIDENCE['US25459Y2072'] = {
    'status': 'verified',
    'as_of': '2026-08-08',
    'classification': 'aktienfonds',
    'legal_form': 'offener US-Registered-Investment-Company-Fonds',
    'invstg_basis': '§ 1 Abs. 2, 3 InvStG i.V.m. § 1 Abs. 1 KAGB',
    'quota_basis': (
        'NASDAQ-100-Aktienindex; offizieller Anlegerbericht weist 99,9 % '
        'Common Stocks aus, damit >50 % Kapitalbeteiligungen'
    ),
    'sources': (
        LEGAL_PRIMARY_SOURCES['invstg'],
        LEGAL_PRIMARY_SOURCES['kagb_1'],
        'https://www.sec.gov/Archives/edgar/data/1424958/000119312526075941/d798177d485bpos.htm',
        'https://www.sec.gov/Archives/edgar/data/1424958/000113322825006997/R2.htm',
        'https://www.direxion.com/product/nasdaq-100-equal-weighted-index-etf',
    ),
}
PRODUCT_CLASSIFICATION_EVIDENCE['LU0290358497'] = _evidence(
    'sonstiger_fonds',
    'Luxemburger OGAW-SICAV-Teilfonds',
    'Swap-basierter €STR-Overnight-Index; keine >50-%-Kapitalbeteiligungsquote',
    'https://etf.dws.com/download/asset/21148431-c688-4992-b674-a8c122ec6de2',
)

for _isin, _source in {
    'US91232N2071': 'https://www.sec.gov/Archives/edgar/data/1327068/000110465926021501/uso-20251231x10k.htm',
    'US9123184098': 'https://www.sec.gov/Archives/edgar/data/1376227/000110465926021507/ung-20251231x10k.htm',
}.items():
    PRODUCT_CLASSIFICATION_EVIDENCE[_isin] = _evidence(
        'personengesellschaft', 'Delaware Limited Partnership',
        'nicht anwendbar; § 1 Abs. 3 Nr. 2 InvStG schliesst Personengesellschaft aus',
        _source,
    )

# Keine bekannte Produktentscheidung bleibt in einer Altpfad-Quarantaene. Diese
# API bleibt fuer kuenftige, wirklich ungeklärte Produkte kompatibel.
ETF_CLASSIFICATION_REVIEW = {}


# ── Reverse-Lookup: Ticker → ISIN ────────────────────────────────────────────
TICKER_TO_ISIN = {}
for isin, (ticker, name, classification) in ETF_CLASSIFICATION.items():
    if ticker not in TICKER_TO_ISIN:
        TICKER_TO_ISIN[ticker] = isin
for isin, (ticker, name, classification, reason) in ETF_CLASSIFICATION_REVIEW.items():
    if is_valid_isin(isin) and ticker not in TICKER_TO_ISIN:
        TICKER_TO_ISIN[ticker] = isin


# DBA-Beta: 15 % fuer aktive US-domizilierte InvStG-Fonds der Tabelle
# (Risikohinweis siehe Kommentar an FOREIGN_TAX_TREATY_RATES).
# Explizite Eintraege oben behalten Vorrang (setdefault).
#
# AUSNAHME: Nicht-RIC-Strukturen (Limited Partnerships/Commodity Pools nach
# ss 1446 IRC, Grantor-/Statutory-Trusts) — ihre Ausschuettungen sind keine
# Dividenden i.S.d. Art. 10 Abs. 2 DBA-USA (bei Zins-Trusts wie FXE gilt
# Art. 11: 0 %). Diese ISINs bekommen bewusst KEINEN 15%-Eintrag und bleiben
# als dba_unverified im Prueffall-Bereich sichtbar.
_NON_RIC_US_FUNDS = {
    'US46138B1035',  # DBC  (Delaware Statutory Trust, Commodity Pool)
    'US46140H7008',  # DBB  (Delaware Statutory Trust, Commodity Pool)
    'US46428R1077',  # GSG  (Trust, Commodity Pool)
    'US91232N2071',  # USO  (LP, Commodity Pool)
    'US9123184098',  # UNG  (LP, Commodity Pool)
    'US88166A8707',  # WEAT (Statutory Trust / US-tax Partnership)
    'US03210A1079',  # BDRY (Statutory Trust / US-tax Partnership)
    'US46138K1034',  # FXE  (Grantor Trust, Zinsertraege -> Art. 11 DBA-USA)
    'US9129087964',  # CPER (Statutory Trust / Commodity Pool)
    'US78463V1070',  # GLD  (Grantor Trust)
    'US4642852044',  # IAU  (Grantor Trust)
    'US46428Q1094',  # SLV  (Grantor Trust)
    'US98149E3036',  # GLDM (Grantor Trust)
    'US0032621023',  # PALL (Grantor Trust)
    'US46438F1012',  # IBIT (Grantor Trust)
    'US3896371099',  # GBTC (Grantor Trust)
    'US3896381072',  # ETHE (Grantor Trust)
    'US0919481095',  # BSOL (Grantor Trust)
    # ProShares Trust II: Commodity Pools / Publicly Traded Partnerships
    # (steuerlich KEINE RICs, anders als ProShares Trust I wie TQQQ/SSO)
    'US74347Y7489',  # BOIL (ProShares Trust II, PTP)
    'US74347Y6804',  # UVXY (ProShares Trust II, PTP)
    'US74347W6012',  # UGL  (ProShares Trust II, PTP)
    'US74347W3530',  # AGQ  (ProShares Trust II, PTP)
    'US92891H1014',  # SVIX (VS Trust, Commodity Pool / PTP)
}
_INVSTG_FUND_CLASSES = (
    'aktienfonds', 'mischfonds', 'immobilienfonds',
    'auslands_immobilienfonds', 'sonstiger_fonds',
)
for isin, (ticker, name, classification) in ETF_CLASSIFICATION.items():
    if (isin.startswith('US') and classification in _INVSTG_FUND_CLASSES
            and isin not in _NON_RIC_US_FUNDS):
        FOREIGN_TAX_TREATY_RATES.setdefault(isin, 0.15)


# ── Helper-Funktionen ────────────────────────────────────────────────────────

def get_etf_info(isin: str):
    """Lookup ETF by ISIN. Returns dict with ticker, name, classification, teilfreistellung or None."""
    entry = ETF_CLASSIFICATION.get(isin)
    if entry is None:
        review_entry = ETF_CLASSIFICATION_REVIEW.get(isin)
        if review_entry is None:
            return None
        ticker, name, previous_classification, reason = review_entry
        return {
            'ticker': ticker,
            'name': name,
            'classification': None,
            'teilfreistellung': None,
            'review_required': True,
            'review_reason': reason,
            'previous_classification': previous_classification,
        }
    ticker, name, classification = entry
    result = {
        'ticker': ticker,
        'name': name,
        'classification': classification,
        'teilfreistellung': TEILFREISTELLUNG.get(classification),
    }
    if isin in PRODUCT_CLASSIFICATION_EVIDENCE:
        result['evidence'] = PRODUCT_CLASSIFICATION_EVIDENCE[isin]
    return result


def get_teilfreistellung(isin: str) -> float:
    """Return TFS rate; 0.0 for unknown and non-InvStG classifications."""
    entry = ETF_CLASSIFICATION.get(isin)
    if entry is None:
        return 0.0
    classification = entry[2]
    rate = TEILFREISTELLUNG.get(classification)
    return rate if rate is not None else 0.0


def is_known_etf(isin: str) -> bool:
    """Check if an ETF/ETP is known, including classification-review cases."""
    return isin in ETF_CLASSIFICATION or isin in ETF_CLASSIFICATION_REVIEW


def is_investment_fund(isin: str) -> bool:
    """Check if ISIN is an Investmentfonds i.S.d. InvStG."""
    entry = ETF_CLASSIFICATION.get(isin)
    if entry is None:
        return False
    return entry[2] not in ('no_invstg', 'personengesellschaft', 'anlage_so')


def is_anlage_so(isin: str) -> bool:
    """Check if ISIN is a physical Gold-ETC with delivery claim (§23 EStG)."""
    entry = ETF_CLASSIFICATION.get(isin)
    if entry is None:
        return False
    return entry[2] == 'anlage_so'


def get_classification(isin: str):
    """Returns classification string or None if unknown."""
    entry = ETF_CLASSIFICATION.get(isin)
    if entry is None:
        return None
    return entry[2]


def get_routing_classification(isin: str):
    """Return the verified route; never fall back to an old review decision."""
    return get_classification(isin)


def requires_classification_review(isin: str) -> bool:
    """Return True for known products without an automatic tax classification."""
    return isin in ETF_CLASSIFICATION_REVIEW


def get_foreign_tax_treaty_rate(isin: str):
    """Return an explicitly verified treaty cap for fund distributions."""
    return FOREIGN_TAX_TREATY_RATES.get(isin)


def lookup_by_ticker(ticker: str):
    """Lookup ETF by ticker symbol. Returns same dict as get_etf_info or None."""
    if not ticker:
        return None
    isin = TICKER_TO_ISIN.get(ticker.upper())
    if isin is None:
        return None
    return get_etf_info(isin)


def get_unknown_etf_isins(traded_isins):
    """Return unknown or deliberately unclassified ETF/ETP identifiers."""
    return [isin for isin in traded_isins if get_classification(isin) is None]


def _source_links(source_keys):
    """Return display labels and URLs for legal source keys."""
    return tuple(
        (LEGAL_PRIMARY_SOURCE_LABELS[key], LEGAL_PRIMARY_SOURCES[key])
        for key in source_keys
    )


def _product_source_label(url):
    """Return a concise, distinguishable label for product evidence links."""
    if 'sec.gov/' in url:
        return 'SEC-Produktdokument'
    if 'etf.dws.com/' in url:
        return 'DWS-Produktdokument'
    if 'direxion.com/' in url:
        return 'Direxion-Produktseite'
    return 'Produktdokument'


def _tfs_label(rate):
    if rate is None:
        return 'nicht anwendbar'
    return f'{rate * 100:.0f} %'


def _unknown_catalog_row(isin):
    return {
        'isin': isin,
        'ticker': isin,
        'name': '',
        'classification': None,
        'classification_label': 'Nicht klassifiziert',
        'tfs_rate': None,
        'tfs_label': 'nicht festgelegt',
        'tax_route': 'Blockiert bis zur ausdrücklichen Bestätigung',
        'legal_form': 'Nicht festgestellt',
        'decision_reason': (
            'Die ISIN ist nicht im Klassifikationskatalog enthalten. Es erfolgt '
            'keine automatische Fonds- oder Steuerpfadzuordnung; Rohwerte bleiben '
            'bis zur ausdrücklichen Nutzerbestätigung unberücksichtigt.'
        ),
        'legal_basis': 'Noch nicht produktspezifisch geprüft',
        'evidence_status': 'user_confirmation_required',
        'evidence_label': 'Nutzerbestätigung erforderlich',
        'as_of': CLASSIFICATION_CATALOG_AS_OF,
        'legal_sources': (),
        'product_sources': (),
    }


def get_classification_catalog(isins=None):
    """Build reusable transparency rows for all or selected product ISINs.

    Product-specific evidence is reported separately from a normal table
    classification so callers never present a generic catalogue entry as an
    individually researched product decision. Requested unknown ISINs are
    returned as visibly unclassified rows.
    """
    if isins is None:
        selected_isins = set(ETF_CLASSIFICATION)
    else:
        selected_isins = {
            str(isin).strip().upper()
            for isin in isins
            if str(isin).strip()
        }

    legal_urls = set(LEGAL_PRIMARY_SOURCES.values())
    rows = []
    for isin in selected_isins:
        entry = ETF_CLASSIFICATION.get(isin)
        if entry is None:
            rows.append(_unknown_catalog_row(isin))
            continue

        ticker, name, classification = entry
        presentation = _CLASSIFICATION_PRESENTATION[classification]
        rate = TEILFREISTELLUNG.get(classification)
        evidence = PRODUCT_CLASSIFICATION_EVIDENCE.get(isin)

        if evidence:
            if rate is None:
                outcome = (
                    f"Daraus folgt die Zuordnung als {presentation['label']}; "
                    "eine Teilfreistellung ist nicht anwendbar."
                )
            else:
                outcome = (
                    f"Daraus folgt die Zuordnung als {presentation['label']} "
                    f"mit {_tfs_label(rate)} Teilfreistellung."
                )
            decision_reason = (
                f"Produktspezifisch geprüft: {evidence['legal_form']}; "
                f"{evidence['quota_basis']}. {outcome}"
            )
            legal_form = evidence['legal_form']
            legal_basis = (
                f"{evidence['invstg_basis']}; {presentation['legal_basis']}"
            )
            evidence_status = 'product_verified'
            evidence_label = 'Produktindividuell geprüft'
            as_of = evidence['as_of']
            product_sources = tuple(
                (_product_source_label(source), source)
                for source in evidence['sources']
                if source not in legal_urls
            )
        else:
            decision_reason = (
                f"{presentation['reason']} Diese feste Katalogzuordnung wird "
                "aktiv berechnet; sie ist weder unklassifiziert noch ein "
                "Quarantäne- oder Prüffall. Für diesen Eintrag ist lediglich "
                "kein eigener produktindividueller Belegdatensatz hinterlegt."
            )
            legal_form = presentation['legal_form']
            legal_basis = presentation['legal_basis']
            evidence_status = 'standard_classification'
            evidence_label = 'Katalogzuordnung · aktiv'
            as_of = CLASSIFICATION_CATALOG_AS_OF
            product_sources = ()

        rows.append({
            'isin': isin,
            'ticker': ticker,
            'name': name,
            'classification': classification,
            'classification_label': presentation['label'],
            'tfs_rate': rate,
            'tfs_label': _tfs_label(rate),
            'tax_route': presentation['tax_route'],
            'legal_form': legal_form,
            'decision_reason': decision_reason,
            'legal_basis': legal_basis,
            'evidence_status': evidence_status,
            'evidence_label': evidence_label,
            'as_of': as_of,
            'legal_sources': _source_links(
                presentation['legal_source_keys']
            ),
            'product_sources': product_sources,
        })

    return sorted(
        rows,
        key=lambda row: (
            row['ticker'].casefold(), row['name'].casefold(), row['isin']
        ),
    )


def classification_catalog_to_csv(rows=None):
    """Serialize transparency rows as a semicolon-delimited UTF-8 CSV."""
    rows = get_classification_catalog() if rows is None else list(rows)
    fieldnames = (
        'ISIN', 'Ticker', 'Name', 'Zuordnung', 'Teilfreistellung',
        'Steuerpfad', 'Nachweisstatus', 'Rechtsform', 'Begründung',
        'Rechtsgrundlage', 'Stand', 'Produktquellen', 'Rechtsquellen',
    )
    output = io.StringIO()
    writer = csv.DictWriter(
        output, fieldnames=fieldnames, delimiter=';', lineterminator='\n'
    )
    writer.writeheader()
    for row in rows:
        writer.writerow({
            'ISIN': row['isin'],
            'Ticker': row['ticker'],
            'Name': row['name'],
            'Zuordnung': row['classification_label'],
            'Teilfreistellung': row['tfs_label'],
            'Steuerpfad': row['tax_route'],
            'Nachweisstatus': row['evidence_label'],
            'Rechtsform': row['legal_form'],
            'Begründung': row['decision_reason'],
            'Rechtsgrundlage': row['legal_basis'],
            'Stand': row['as_of'],
            'Produktquellen': ' | '.join(
                url for _label, url in row['product_sources']
            ),
            'Rechtsquellen': ' | '.join(
                url for _label, url in row['legal_sources']
            ),
        })
    return output.getvalue()


# ── Selbsttest ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print(f"ETF-Klassifikation: {len(ETF_CLASSIFICATION)} Eintraege")
    print()

    # Zaehle nach Kategorie
    counts = {}
    for isin, (ticker, name, cls) in ETF_CLASSIFICATION.items():
        counts[cls] = counts.get(cls, 0) + 1
    for cls, count in sorted(counts.items()):
        rate = TEILFREISTELLUNG.get(cls)
        rate_str = f"{rate*100:.0f}%" if rate is not None else "n/a"
        print(f"  {cls:20s}: {count:3d} ETFs  (Teilfreistellung: {rate_str})")

    # Stichproben pro Kategorie
    print()
    for cls_name in [
            'aktienfonds', 'sonstiger_fonds', 'no_invstg',
            'personengesellschaft', 'anlage_so']:
        examples = [(t, n) for _, (t, n, c) in ETF_CLASSIFICATION.items() if c == cls_name][:3]
        print(f"  {cls_name}: z.B. {', '.join(t for t, n in examples)}")

    # Duplikat-Check
    print()
    seen_tickers = {}
    duplicate_tickers = []
    for isin, (ticker, name, cls) in ETF_CLASSIFICATION.items():
        if ticker in seen_tickers and seen_tickers[ticker] != isin:
            print(f"  WARNUNG: Ticker {ticker} hat mehrere ISINs: {seen_tickers[ticker]}, {isin}")
            duplicate_tickers.append(ticker)
        seen_tickers[ticker] = isin
    print(
        f"Duplikat-Check: {len(seen_tickers)} unique Ticker, "
        f"{len(duplicate_tickers)} Mehrfach-Ticker."
    )
