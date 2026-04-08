"""
ETF-Klassifikation nach deutschem InvStG (Investmentsteuergesetz).

Lookup-Tabelle der ~250 meistgehandelten ETFs/ETPs mit ISIN und Klassifikation
fuer die Berechnung der Teilfreistellung:
  - aktienfonds:      30% Teilfreistellung (>= 51% Aktienquote)
  - mischfonds:       15% Teilfreistellung (25-50% Aktienquote)
  - sonstiger_fonds:   0% Teilfreistellung (Anleihen-ETFs, Derivate-Fonds)
  - no_invstg:        Kein Investmentfonds i.S.d. InvStG (physische Rohstoffe, Krypto-ETPs)

Rechtsgrundlage: ss 20 InvStG (Teilfreistellung), ss 2 InvStG (Investmentfonds-Definition)

ISINs verifiziert via cbonds.com (April 2026). Jede ISIN wurde einzeln geprueft.
"""

# ── Teilfreistellungssaetze ──────────────────────────────────────────────────
TEILFREISTELLUNG = {
    'aktienfonds':     0.30,   # 30 % — ss 20 Abs. 1 S. 1 InvStG
    'mischfonds':      0.15,   # 15 % — ss 20 Abs. 1 S. 2 InvStG
    'sonstiger_fonds': 0.00,   # 0 %  — keine Teilfreistellung
    'no_invstg':       None,   # kein Investmentfonds → normale Besteuerung nach ss 20 EStG
}


# ── ETF-Lookup: ISIN → (ticker, name, classification) ────────────────────────
# Sortiert nach Kategorie, dann nach Subkategorie.
# Alle ISINs via cbonds.com verifiziert (April 2026).

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
    'US26922A2303': ('DGRO', 'iShares Core Dividend Growth ETF',                'aktienfonds'),   # cbonds

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
    'US46434G8556': ('RING', 'iShares MSCI Global Gold Miners ETF',             'aktienfonds'),   # cbonds /etf/1109/
    'US46434G8481': ('PICK', 'iShares MSCI Global Metals & Mining Producers ETF', 'aktienfonds'),  # cbonds /etf/1107/
    'US78464A7550': ('XME',  'SPDR S&P Metals & Mining ETF',                    'aktienfonds'),   # cbonds /etf/793/
    'US37954Y8553': ('LIT',  'Global X Lithium & Battery Tech ETF',             'aktienfonds'),   # cbonds /etf/7739/
    'US37954Y8710': ('URA',  'Global X Uranium ETF',                            'aktienfonds'),   # cbonds /etf/7767/
    'US85208P3038': ('URNM', 'Sprott Uranium Miners ETF',                       'aktienfonds'),   # cbonds /etf/14121/

    # --- Energie (physische Aktienholdings) ---
    'US78468R5569': ('XOP',  'SPDR S&P Oil & Gas Exploration & Production ETF',  'aktienfonds'),   # cbonds /etf/735/
    'US92189H6071': ('OIH',  'VanEck Oil Services ETF',                          'aktienfonds'),   # cbonds /etf/269/
    'US00162Q4525': ('AMLP', 'Alerian MLP ETF',                                  'aktienfonds'),   # cbonds /etf/77/

    # --- Thematische / aktiv gemanagte Aktien-ETFs ---
    'US00214Q1040': ('ARKK', 'ARK Innovation ETF',                              'aktienfonds'),   # cbonds /etf/11013/
    'US00214Q2055': ('ARKG', 'ARK Genomic Revolution ETF',                      'aktienfonds'),   # cbonds
    'US26922A8421': ('JETS', 'U.S. Global Jets ETF',                             'aktienfonds'),   # cbonds /etf/11727/
    'US92189H8390': ('BUZZ', 'VanEck Social Sentiment ETF',                      'aktienfonds'),   # cbonds /etf/11057/
    'US00768Y4531': ('MSOS', 'AdvisorShares Pure US Cannabis ETF',               'aktienfonds'),   # cbonds /etf/11481/
    'US46138G7060': ('TAN',  'Invesco Solar ETF',                                'aktienfonds'),   # cbonds /etf/3033/
    'US4642882249': ('ICLN', 'iShares Global Clean Energy ETF',                  'aktienfonds'),   # cbonds /etf/1023/

    # --- Weitere Aktien-ETFs (Björn-Audit, April 2026) ---
    'US37950E4090': ('CHIQ', 'Global X MSCI China Consumer Discretionary ETF',   'aktienfonds'),
    'US37954Y4420': ('CLOU', 'Global X Cloud Computing ETF',                     'aktienfonds'),
    'US78467X2090': ('DIA',  'SPDR Dow Jones Industrial Average ETF Trust',      'aktienfonds'),   # alt. ISIN
    'US0321088883': ('DIVO', 'Amplify CWP Enhanced Dividend Income ETF',         'aktienfonds'),
    'US37954Y4677': ('EBIZ', 'Global X E-commerce ETF',                          'aktienfonds'),
    'US4642872341': ('EEM',  'iShares MSCI Emerging Markets ETF',                'aktienfonds'),   # alt. ISIN
    'US4642891232': ('ENZL', 'iShares MSCI New Zealand ETF',                     'aktienfonds'),
    'US4642865095': ('EWC',  'iShares MSCI Canada ETF',                          'aktienfonds'),
    'US4642877730': ('EWJ',  'iShares MSCI Japan ETF',                           'aktienfonds'),   # alt. ISIN
    'US4642883523': ('EWT',  'iShares MSCI Taiwan ETF',                          'aktienfonds'),   # alt. ISIN
    'US33735T1097': ('FDD',  'First Trust STOXX European Select Dividend Index Fund', 'aktienfonds'),
    'US33734X1054': ('FDN',  'First Trust Dow Jones Internet Index Fund',        'aktienfonds'),
    'US33733E1008': ('FPX',  'First Trust US Equity Opportunities ETF',          'aktienfonds'),
    'US4642875017': ('IAI',  'iShares U.S. Broker-Dealers & Securities Exchanges ETF', 'aktienfonds'),
    'US4642876557': ('IWM',  'iShares Russell 2000 ETF',                         'aktienfonds'),   # alt. ISIN
    'US4642877696': ('IWS',  'iShares Russell Mid-Cap Value ETF',                'aktienfonds'),
    'US4642871771': ('IYF',  'iShares U.S. Financials ETF',                      'aktienfonds'),
    'US78464A7939': ('KCE',  'SPDR S&P Capital Markets ETF',                     'aktienfonds'),
    'US5007678502': ('KGRN', 'KraneShares MSCI China Clean Technology ETF',      'aktienfonds'),
    'US78464A7468': ('KIE',  'SPDR S&P Insurance ETF',                           'aktienfonds'),
    'US78464A7394': ('KRE',  'SPDR S&P Regional Banking ETF',                    'aktienfonds'),
    'US37954Y8559': ('LIT',  'Global X Lithium & Battery Tech ETF',              'aktienfonds'),   # alt. ISIN
    'US46090E2017': ('PPA',  'Invesco Aerospace & Defense ETF',                  'aktienfonds'),   # alt. ISIN
    'US46137V1008': ('PPA',  'Invesco Aerospace & Defense ETF',                  'aktienfonds'),
    'US46137V1180': ('PSP',  'Invesco Global Listed Private Equity ETF',         'aktienfonds'),
    'US8085248694': ('SCHX', 'Schwab U.S. Large-Cap ETF',                        'aktienfonds'),
    'US8123501061': ('SHLD', 'Global X Defense Tech ETF',                        'aktienfonds'),
    'US92189F6093': ('SLX',  'VanEck Steel ETF',                                 'aktienfonds'),
    'US46138G1031': ('TAN',  'Invesco Solar ETF',                                'aktienfonds'),   # alt. ISIN
    'US4642867158': ('TUR',  'iShares MSCI Turkey ETF',                          'aktienfonds'),
    'US9220427752': ('VGK',  'Vanguard FTSE Europe ETF',                         'aktienfonds'),
    'US9229088637': ('VOO',  'Vanguard S&P 500 ETF',                             'aktienfonds'),   # alt. ISIN
    'US9229087286': ('VTV',  'Vanguard Value Index Fund ETF Shares',             'aktienfonds'),   # alt. ISIN
    'US78464A7307': ('XAR',  'SPDR S&P Aerospace & Defense ETF',                 'aktienfonds'),
    'US78464A8690': ('XES',  'SPDR S&P Oil & Gas Equipment & Services ETF',      'aktienfonds'),
    'US78464A8504': ('XHB',  'SPDR S&P Homebuilders ETF',                        'aktienfonds'),
    'US78464A7144': ('XRT',  'SPDR S&P Retail ETF',                              'aktienfonds'),
    'US78464A8488': ('XSD',  'SPDR S&P Semiconductor ETF',                       'aktienfonds'),
    'US78467Y1070': ('XSM',  'SPDR S&P MidCap 400 ETF',                          'aktienfonds'),
    'US4642883984': ('XUS',  'iShares MSCI ACWI ex U.S. ETF',                    'aktienfonds'),

    # --- Commodity-ETFs (Futures/Derivate-basiert, keine Aktien) ---
    'US46138B1035': ('DBC',  'Invesco DB Commodity Index Tracking Fund',         'sonstiger_fonds'),  # Commodity Pool, Futures
    'US46428R1077': ('GSG',  'iShares S&P GSCI Commodity-Indexed Trust',         'sonstiger_fonds'),  # Commodity Pool, Futures
    'US46090F1003': ('PDBC', 'Invesco Optimum Yield Diversified Commodity Strategy ETF', 'sonstiger_fonds'),  # Commodity-Futures via Subsidiary
    'US91232N2071': ('USO',  'United States Oil Fund LP',                        'sonstiger_fonds'),  # LP Commodity Pool, Crude Oil Futures
    'US9123184098': ('UNG',  'United States Natural Gas Fund LP',                'sonstiger_fonds'),  # LP Commodity Pool, Nat Gas Futures

    # --- Leveraged/Inverse ETFs (Derivate-basiert, keine physischen Aktien → 0% TFS) ---
    # §2 Abs. 8 InvStG: Swaps/Futures zaehlen nicht zur Aktienquote
    'US74347X8314': ('TQQQ', 'ProShares UltraPro QQQ (3x Nasdaq-100)',          'sonstiger_fonds'),   # cbonds /etf/757/
    'US74350P6759': ('SQQQ', 'ProShares UltraPro Short QQQ (-3x Nasdaq-100)',   'sonstiger_fonds'),   # cbonds /etf/3063/
    'US74347X8645': ('UPRO', 'ProShares UltraPro S&P500 (3x S&P 500)',         'sonstiger_fonds'),   # cbonds /etf/4207/
    'US74350P6593': ('SPXU', 'ProShares UltraPro Short S&P500 (-3x S&P 500)',  'sonstiger_fonds'),   # cbonds /etf/10929/
    'US74347R1077': ('SSO',  'ProShares Ultra S&P500 (2x S&P 500)',            'sonstiger_fonds'),   # cbonds /etf/4189/
    'US74347A8351': ('SSO',  'ProShares Ultra S&P500 (2x S&P 500)',            'sonstiger_fonds'),   # alt. ISIN
    'US25459W4583': ('SOXL', 'Direxion Daily Semiconductor Bull 3X Shares',     'sonstiger_fonds'),   # cbonds /etf/5683/
    'US25460G1123': ('SOXS', 'Direxion Daily Semiconductor Bear 3X Shares',     'sonstiger_fonds'),   # cbonds /etf/5681/
    'US74347Y7489': ('BOIL', 'ProShares Ultra Bloomberg Natural Gas (2x)',       'sonstiger_fonds'),  # 2x leveraged Nat Gas Futures
    'US25460G7815': ('NUGT', 'Direxion Daily Gold Miners Index Bull 2X Shares',  'sonstiger_fonds'),  # 2x leveraged, Swaps/Futures
    'US25461A4783': ('DUST', 'Direxion Daily Gold Miners Index Bear 2X Shares',  'sonstiger_fonds'),  # 2x inverse, Swaps/Futures
    'US25460G8318': ('JNUG', 'Direxion Daily Junior Gold Miners Index Bull 2X',  'sonstiger_fonds'),  # 2x leveraged, Swaps/Futures
    'LU0411078552': ('XS2L', 'Xtrackers S&P 500 2x Leveraged Daily Swap UCITS ETF', 'sonstiger_fonds'),  # 2x leveraged, Swap-basiert

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
    'US74347X8492': ('UVXY', 'ProShares Ultra VIX Short-Term Futures ETF',      'sonstiger_fonds'),  # alt. ISIN

    # --- Weitere Leveraged/Inverse ETFs (Björn-Audit, April 2026) ---
    'US25459L8820': ('CURE', 'Direxion Daily Healthcare Bull 3X Shares',         'sonstiger_fonds'),
    'US25459L7642': ('DFEN', 'Direxion Daily Aerospace & Defense Bull 3X Shares','sonstiger_fonds'),
    'US25459L7691': ('DPST', 'Direxion Daily Regional Banks Bull 3X Shares',     'sonstiger_fonds'),
    'US74347F7061': ('EDC',  'Direxion Daily MSCI Emerging Markets Bull 3X Shares', 'sonstiger_fonds'),
    'US74347F7022': ('EFO',  'ProShares Ultra MSCI EAFE',                        'sonstiger_fonds'),
    'US25459L7984': ('EURL', 'Direxion Daily FTSE Europe Bull 3X Shares',        'sonstiger_fonds'),
    'US25459L7078': ('FAS',  'Direxion Daily Financial Bull 3X Shares',          'sonstiger_fonds'),
    'US25459L6659': ('INDL', 'Direxion Daily MSCI India Bull 2x Shares',         'sonstiger_fonds'),
    'US25459L7896': ('MEXX', 'Direxion Daily MSCI Mexico Bull 3X Shares',        'sonstiger_fonds'),
    'US25459L7918': ('NAIL', 'Direxion Daily Homebuilders & Supplies Bull 3X Shares', 'sonstiger_fonds'),
    'US69347Q1076': ('PILL', 'Direxion Daily Pharmaceutical & Medical Bull 3X Shares', 'sonstiger_fonds'),
    'US74347A8440': ('QLD',  'ProShares Ultra QQQ',                              'sonstiger_fonds'),
    'US25459L7609': ('RXD',  'ProShares UltraShort Health Care',                 'sonstiger_fonds'),
    'US25459L7136': ('SPXL', 'Direxion Daily S&P 500 Bull 3X Shares',           'sonstiger_fonds'),
    'US74347B2016': ('TBT',  'ProShares UltraShort 20+ Year Treasury',          'sonstiger_fonds'),
    'US25459L7285': ('TNA',  'Direxion Daily Small Cap Bull 3X Shares',          'sonstiger_fonds'),
    'US74347F8164': ('UGL',  'ProShares Ultra Gold',                             'sonstiger_fonds'),
    'US74347B7421': ('URTY', 'ProShares UltraPro Russell 2000',                  'sonstiger_fonds'),
    'US25459L7376': ('UTSL', 'Direxion Daily Utilities Bull 3X Shares',          'sonstiger_fonds'),
    'US25459Y8012': ('WANT', 'Direxion Daily Consumer Discretionary Bull 3X Shares', 'sonstiger_fonds'),
    'US25459L7437': ('YINN', 'Direxion Daily FTSE China Bull 3X Shares',         'sonstiger_fonds'),
    'US74347F8157': ('AGQ',  'ProShares Ultra Silver',                           'sonstiger_fonds'),
    'US25459L7094': ('NUGT', 'Direxion Daily Gold Miners Index Bull 2X Shares',  'sonstiger_fonds'),  # alt. ISIN

    # --- Weitere Anleihen-ETFs (Björn-Audit) ---
    'US46138G8050': ('BAB',  'Invesco Taxable Municipal Bond ETF',               'sonstiger_fonds'),
    'US78468R7068': ('CWB',  'SPDR Bloomberg Convertible Securities ETF',        'sonstiger_fonds'),  # alt. ISIN
    'US78464A3591': ('CWB',  'SPDR Bloomberg Convertible Securities ETF',        'sonstiger_fonds'),
    'US92189H4092': ('HYD',  'VanEck High-Yield Muni ETF',                       'sonstiger_fonds'),
    'US92189F3872': ('SHYD', 'VanEck Short High Yield Muni ETF',                 'sonstiger_fonds'),
    'US4642885133': ('HYG',  'iShares iBoxx $ High Yield Corporate Bond ETF',    'sonstiger_fonds'),  # alt. ISIN
    'US4642872429': ('LQD',  'iShares iBoxx $ Investment Grade Corporate Bond ETF', 'sonstiger_fonds'),  # alt. ISIN
    'US78468R8785': ('JNK',  'SPDR Bloomberg High Yield Bond ETF',               'sonstiger_fonds'),
    'US46138G7896': ('PCY',  'Invesco Emerging Markets Sovereign Debt ETF',      'sonstiger_fonds'),  # alt. ISIN
    'US46138E7849': ('PCY',  'Invesco Emerging Markets Sovereign Debt ETF',      'sonstiger_fonds'),
    'US69347A5369': ('PICB', 'Invesco International Corporate Bond ETF',         'sonstiger_fonds'),
    'US4642871763': ('TIP',  'iShares TIPS Bond ETF',                            'sonstiger_fonds'),  # alt. ISIN
    'US97717Y5270': ('USFR', 'WisdomTree Floating Rate Treasury Fund',           'sonstiger_fonds'),
    'US02072L5654': ('BOXX', 'Alpha Architect 1-3 Month Box ETF',               'sonstiger_fonds'),
    'US82889N8552': ('PFIX', 'Simplify Interest Rate Hedge ETF',                 'sonstiger_fonds'),

    # --- Covered-Call / Income-Strategie ETFs (Derivate-basiert) ---
    'US46641Q3323': ('JEPI', 'JPMorgan Equity Premium Income ETF',               'sonstiger_fonds'),
    'US46654Q2038': ('JEPQ', 'JPMorgan Nasdaq Equity Premium Income ETF',        'sonstiger_fonds'),
    'US88634T7827': ('NFLY', 'YieldMax NFLX Option Income Strategy ETF',         'sonstiger_fonds'),
    'US88634T7744': ('NVDY', 'YieldMax NVDA Option Income Strategy ETF',         'sonstiger_fonds'),

    # --- Commodity-Fonds (Futures-basiert, Fund-Struktur) ---
    'US4642878501': ('COMT', 'iShares GSCI Commodity Dynamic Roll Strategy ETF', 'sonstiger_fonds'),
    'US46138G1013': ('DBC',  'Invesco DB Commodity Index Tracking Fund',         'sonstiger_fonds'),  # alt. ISIN
    'US88107A1051': ('WEAT', 'Teucrium Wheat Fund',                              'sonstiger_fonds'),
    'US88166A8707': ('WEAT', 'Teucrium Wheat Fund',                              'sonstiger_fonds'),  # Post-Reverse-Split ISIN
    'US46140H7008': ('DBB',  'Invesco DB Base Metals Fund',                      'sonstiger_fonds'),  # Rohstoff-Futures (Aluminium, Kupfer, Zink)
    'US11410J2026': ('BDRY', 'Breakwave Dry Bulk Shipping ETF',                  'sonstiger_fonds'),
    'US97717W8281': ('GDE',  'WisdomTree Efficient Gold Plus Equity Strategy Fund', 'sonstiger_fonds'),

    # --- Waehrungs-ETFs ---
    'US46138K1034': ('FXE',  'Invesco CurrencyShares Euro Currency Trust',       'sonstiger_fonds'),

    # --- Sonstige Strategie-ETFs ---
    'US00162Q1067': ('BTAL', 'AGF U.S. Market Neutral Anti-Beta Fund',           'sonstiger_fonds'),
    'US37950E4733': ('MLPA', 'Global X MLP ETF',                                 'sonstiger_fonds'),

    # ═══════════════════════════════════════════════════════════════════════════
    # NO_INVSTG — Kein Investmentfonds i.S.d. InvStG
    # Physische Rohstoff-Trusts, Krypto-ETPs → normale Besteuerung nach ss 20 EStG
    # ═══════════════════════════════════════════════════════════════════════════

    # --- Physische Edelmetall-Trusts ---
    'US78463V1070': ('GLD',  'SPDR Gold Shares',                                'no_invstg'),  # cbonds /etf/13/
    'US4642852044': ('IAU',  'iShares Gold Trust',                              'no_invstg'),  # cbonds /etf/143/
    'US46428Q1094': ('SLV',  'iShares Silver Trust',                            'no_invstg'),  # cbonds /etf/169/
    'US18500Q1040': ('GLDM', 'SPDR Gold MiniShares Trust',                      'no_invstg'),  # cbonds

    # --- Krypto-ETPs (Spot-Trusts = kein Fonds, Futures-Fonds = sonstiger_fonds) ---
    'US46438F1012': ('IBIT', 'iShares Bitcoin Trust ETF',                       'no_invstg'),  # Spot-Trust, einzelner Basiswert
    'US3896381072': ('ETHE', 'Grayscale Ethereum Trust ETF',                    'no_invstg'),  # Spot-Trust, einzelner Basiswert
    'US3837861092': ('GBTC', 'Grayscale Bitcoin Trust ETF',                     'no_invstg'),  # Spot-Trust, einzelner Basiswert
    'US74347G4405': ('BITO', 'ProShares Bitcoin Strategy ETF',                  'sonstiger_fonds'),  # Investment Company Act 1940, BTC-Futures + Treasuries → InvStG

    # --- ETNs (Schuldverschreibungen, kein Fonds) ---
    'US06748M1962': ('VXX',  'iPath Series B S&P 500 VIX Short-Term Futures ETN', 'no_invstg'),  # ETN = Inhaberschuldverschreibung
    'US06747R4772': ('VXX',  'iPath Series B S&P 500 VIX Short-Term Futures ETN', 'no_invstg'),  # alt. ISIN
    'US06748M1889': ('VXZ',  'iPath Series B S&P 500 VIX Mid-Term Futures ETN',  'no_invstg'),  # ETN
    'US62386A6997': ('FNGU', 'MicroSectors FANG+ Index 3X Leveraged ETN',        'no_invstg'),  # ETN
    'US06742L4785': ('JJG',  'iPath Series B Bloomberg Grains Subindex Total Return ETN', 'no_invstg'),  # ETN
    'US06742W5R66': ('DLBR', 'Barclays ETN+ FI Enhanced Global High Yield ETN',  'no_invstg'),  # ETN

    # --- Deutsche Gold-ETCs (physisch besichert, kein Investmentfonds) ---
    'DE000EWG2LD7': ('EWG2',  'EUWAX Gold II',                                   'no_invstg'),  # physisches Gold-ETC
    'DE000EWG0LD1': ('GOLD1', 'EUWAX Gold I',                                    'no_invstg'),  # physisches Gold-ETC
    'DE000A0S9GB0': ('4GLD',  'Xetra-Gold',                                      'no_invstg'),  # physisches Gold-ETC

    # --- Gehebelte/Inverse Rohstoff-ETPs (kein Investmentfonds) ---
    'IE00B6X4BP29': ('3GOS',  'WisdomTree Gold 3x Daily Short',                  'no_invstg'),  # gehebeltes ETP, Schuldverschreibung

    # --- Physische Rohstoff-Trusts (kein Fonds) ---
    'US01924U1097': ('PALL',  'Aberdeen Standard Physical Palladium Shares ETF', 'no_invstg'),  # physischer Palladium-Trust
    'US9129087967': ('CPER',  'United States Copper Index Fund LP',              'no_invstg'),  # Commodity Pool LP, einzelner Basiswert

    # --- Closed-End Funds (keine offenen Investmentfonds i.S.d. InvStG) ---
    'US00302L1089': ('AWP',  'Aberdeen Global Premier Properties Fund',          'no_invstg'),  # CEF
    'US6706ER1015': ('BXMX', 'Nuveen S&P 500 Buy-Write Income Fund',            'no_invstg'),  # CEF
    'US1846911030': ('CBA',  'ClearBridge American Energy MLP Fund',             'no_invstg'),  # CEF
    'US94987B1052': ('EAD',  'Wells Fargo Income Opportunities Fund',            'no_invstg'),  # CEF
    'US27828Q1058': ('EFR',  'Eaton Vance Senior Floating-Rate Trust',           'no_invstg'),  # CEF
    'US27827X1019': ('EIM',  'Eaton Vance Municipal Bond Fund',                  'no_invstg'),  # CEF
    'US27828X1000': ('ETB',  'Eaton Vance Tax-Managed Buy-Write Income Fund',    'no_invstg'),  # CEF
    'US27828H1059': ('EVV',  'Eaton Vance Limited Duration Income Fund',         'no_invstg'),  # CEF
    'US27829F1084': ('EXG',  'Eaton Vance Tax-Managed Global Diversified Equity Income Fund', 'no_invstg'),  # CEF
    'US31647Q1067': ('FMO',  'Fiduciary/Claymore Energy Infrastructure Fund',    'no_invstg'),  # CEF
    'US87911K1007': ('HQL',  'Tekla Life Sciences Investors',                    'no_invstg'),  # CEF
    'US67073B1061': ('JPC',  'Nuveen Preferred & Income Opportunities Fund',     'no_invstg'),  # CEF
    'US55607W1009': ('MFD',  'Macquarie/First Trust Global Infrastructure/Utilities Dividend Fund', 'no_invstg'),  # CEF
    'US95766M1053': ('MMU',  'Western Asset Managed Municipals Fund',            'no_invstg'),  # CEF
    'US0188251096': ('NCZ',  'AllianzGI Convertible & Income Fund II',           'no_invstg'),  # CEF
    'US6706821039': ('NMZ',  'Nuveen Municipal High Income Opportunity Fund',    'no_invstg'),  # CEF
    'US89148B1017': ('NTG',  'Tortoise Midstream Energy Fund',                   'no_invstg'),  # CEF
    'US76970B1017': ('RIF',  'RMR Real Estate Income Fund',                      'no_invstg'),  # CEF
    'US19247X1000': ('RNP',  'Cohen & Steers REIT and Preferred Income Fund',    'no_invstg'),  # CEF
    'US2316312014': ('SRV',  'Cushing MLP & Infrastructure Total Return Fund',   'no_invstg'),  # CEF
    'US19248A1097': ('UTF',  'Cohen & Steers Infrastructure Fund',               'no_invstg'),  # CEF
    'US46131M1062': ('VGM',  'Invesco Trust for Investment Grade Municipals',    'no_invstg'),  # CEF
}


# ── Reverse-Lookup: Ticker → ISIN ────────────────────────────────────────────
TICKER_TO_ISIN = {}
for isin, (ticker, name, classification) in ETF_CLASSIFICATION.items():
    if ticker not in TICKER_TO_ISIN:
        TICKER_TO_ISIN[ticker] = isin


# ── Helper-Funktionen ────────────────────────────────────────────────────────

def get_etf_info(isin: str):
    """Lookup ETF by ISIN. Returns dict with ticker, name, classification, teilfreistellung or None."""
    entry = ETF_CLASSIFICATION.get(isin)
    if entry is None:
        return None
    ticker, name, classification = entry
    return {
        'ticker': ticker,
        'name': name,
        'classification': classification,
        'teilfreistellung': TEILFREISTELLUNG.get(classification),
    }


def get_teilfreistellung(isin: str) -> float:
    """Returns Teilfreistellungssatz for an ISIN (0.0 if unknown or no_invstg)."""
    entry = ETF_CLASSIFICATION.get(isin)
    if entry is None:
        return 0.0
    classification = entry[2]
    rate = TEILFREISTELLUNG.get(classification)
    return rate if rate is not None else 0.0


def is_known_etf(isin: str) -> bool:
    """Check if ISIN is in the ETF lookup table."""
    return isin in ETF_CLASSIFICATION


def is_investment_fund(isin: str) -> bool:
    """Check if ISIN is an Investmentfonds i.S.d. InvStG (not no_invstg, not unknown)."""
    entry = ETF_CLASSIFICATION.get(isin)
    if entry is None:
        return False
    return entry[2] != 'no_invstg'


def get_classification(isin: str):
    """Returns classification string or None if unknown."""
    entry = ETF_CLASSIFICATION.get(isin)
    if entry is None:
        return None
    return entry[2]


def lookup_by_ticker(ticker: str):
    """Lookup ETF by ticker symbol. Returns same dict as get_etf_info or None."""
    if not ticker:
        return None
    isin = TICKER_TO_ISIN.get(ticker.upper())
    if isin is None:
        return None
    return get_etf_info(isin)


def get_unknown_etf_isins(traded_isins):
    """Given a list of traded ISINs, return those NOT in our lookup table.
    Useful for flagging instruments that might be ETFs requiring manual classification."""
    return [isin for isin in traded_isins if isin not in ETF_CLASSIFICATION]


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
    for cls_name in ['aktienfonds', 'sonstiger_fonds', 'no_invstg']:
        examples = [(t, n) for _, (t, n, c) in ETF_CLASSIFICATION.items() if c == cls_name][:3]
        print(f"  {cls_name}: z.B. {', '.join(t for t, n in examples)}")

    # Duplikat-Check
    print()
    seen_tickers = {}
    for isin, (ticker, name, cls) in ETF_CLASSIFICATION.items():
        if ticker in seen_tickers and seen_tickers[ticker] != isin:
            print(f"  WARNUNG: Ticker {ticker} hat mehrere ISINs: {seen_tickers[ticker]}, {isin}")
        seen_tickers[ticker] = isin
    print(f"Duplikat-Check: {len(seen_tickers)} unique Ticker, keine Konflikte.")
