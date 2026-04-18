#!/usr/bin/env python3
"""
RAPTOR WEALTH MERIDIAN — fetch_rwm.py v1.0
═══════════════════════════════════════════
Scansione notturna di tutti gli ETF RWM con logiche RAPTOR adattate.

Logiche per tipo:
  MONETARIO    → sempre HOLD, aggiorna prezzo e yield stimato
  OBBLIGAZ.    → prezzo vs MA200, momentum 3M, segnale ACCUMULA/HOLD/RIDUCI
  AZIONARIO    → KAMA + SAR + ER + score (come Scanner)
  LEVA 2x/3x   → come azionario ma soglia score ≥ 80
  COMMODITIES  → come azionario, soglia ≥ 65

Output: data/rwm_signals.json
        data/rwm_checkpoint.json
"""

import json, os, time, math
from datetime import datetime, timezone
from pathlib import Path

def install(pkg):
    os.system(f"pip install {pkg} --break-system-packages -q")

try:
    import yfinance as yf
except ImportError:
    install("yfinance"); import yfinance as yf

try:
    import numpy as np
except ImportError:
    install("numpy"); import numpy as np

BASE_DIR  = Path(__file__).parent
DATA_DIR  = BASE_DIR / "data"
OUT_FILE  = DATA_DIR / "rwm_signals.json"
CKPT_FILE = DATA_DIR / "rwm_checkpoint.json"

# ── UNIVERSO ETF RWM ─────────────────────────────────────────────
# [TICKER, NOME, CATEGORIA, TIPO_LOGICA, TIPO_DIST, YIELD_STIMATO, LIVELLI]
# TIPO_LOGICA: monetario | obbligaz | azionario | leva | commodity | reit
# TIPO_DIST: Acc | Dist
ETF_UNIVERSE = [
    # MONETARIO
    ("XEON.MI","Xtrackers EUR Overnight Rate Swap","Monetario","monetario","Acc",3.2,"A1,A2,C1,C2"),
    ("SMART.MI","iShares € Ultrashort Bond Dist","Monetario","monetario","Dist",3.0,"C1,C2"),
    ("IU0E.MI","iShares € Ultrashort Bond Acc","Monetario","monetario","Acc",3.0,"A1,A2"),
    ("XEOD.MI","Xtrackers EUR Overnight 1D Dist","Monetario","monetario","Dist",3.1,"C1,C2"),
    ("ERNX.MI","Amundi Euro Overnight Return","Monetario","monetario","Acc",3.1,"A1,A2"),

    # OBBLIGAZIONARIO BREVE
    ("IBTE.MI","iShares € Govt Bond 0-1yr","Obbligaz. Breve","obbligaz","Acc",3.0,"A2,A3,C2,C3"),
    ("SXRM.MI","iShares Core € Govt Bond Dist","Obbligaz. Governo","obbligaz","Dist",3.2,"C2,C3,C4"),
    ("EUNH.MI","iShares Core € Corp Bond Dist","Obbligaz. Corp","obbligaz","Dist",3.5,"C3,C4"),
    ("IEAC.MI","iShares Core € Corp Bond Acc","Obbligaz. Corp","obbligaz","Acc",3.5,"A3,A4"),
    ("IEAG.MI","iShares Core € Aggregate Bond","Obbligaz. Mix","obbligaz","Acc",3.3,"A3,A4,A5"),
    ("GOM.MI","iShares € Govt Bond 3-5yr","Obbligaz. Governo","obbligaz","Acc",3.1,"A3,A4,C3,C4"),
    ("IBTM.MI","iShares € Govt Bond 3-7yr","Obbligaz. Governo","obbligaz","Acc",3.2,"A3,A4,C3,C4"),
    ("SXRV.MI","iShares € Govt Bond 7-10yr","Obbligaz. Governo","obbligaz","Acc",3.4,"A4,A5,C4,C5"),

    # OBBLIGAZIONARIO GLOBALE
    ("IGLO.MI","iShares Global Govt Bond EUR Hdg","Obbligaz. Globale","obbligaz","Acc",3.5,"A4,A5"),
    ("VAGE.MI","Vanguard Global Aggregate EUR Hdg","Obbligaz. Globale","obbligaz","Acc",3.4,"A4,A5,A6"),
    ("AGGH.MI","iShares Core Global Aggregate Dist","Obbligaz. Globale","obbligaz","Dist",3.4,"C4,C5"),
    ("SEMB.MI","iShares JP Morgan EM Bond Dist","Obbligaz. EM","obbligaz","Dist",5.5,"C5,C6"),
    ("EMBE.MI","iShares JP Morgan EM Bond Acc","Obbligaz. EM","obbligaz","Acc",5.5,"A5,A6"),
    ("XUTC.MI","Xtrackers USD Corp Bond","Obbligaz. Corp USD","obbligaz","Acc",4.8,"A5,A6"),
    ("XUCS.MI","Xtrackers USD Corp Bond Short","Obbligaz. Corp USD","obbligaz","Acc",4.5,"A4,A5"),
    ("2B70.MI","iShares Global Corp Bond EUR Hdg Dist","Obbligaz. Corp Globale","obbligaz","Dist",4.2,"C4,C5,C6"),
    ("XDJE.MI","Xtrackers Eurozone Govt Bond 25+","Obbligaz. Lungo","obbligaz","Acc",3.8,"A5,A6"),

    # HIGH YIELD
    ("IHYU.MI","iShares € High Yield Corp Bond Dist","High Yield","obbligaz","Dist",6.2,"C5,C6,C7"),
    ("HYLD.MI","iShares Global HY Corp Bond Acc","High Yield Globale","obbligaz","Acc",6.0,"A6,A7"),
    ("DHYA.MI","Xtrackers Global HY Corp Bond","High Yield Globale","obbligaz","Acc",6.1,"A6,A7"),
    ("EUHA.MI","PIMCO Euro ST HY Acc","High Yield EUR","obbligaz","Acc",5.8,"A6,A7,C6,C7"),
    ("EUHI.MI","PIMCO Euro ST HY Dist","High Yield EUR","obbligaz","Dist",5.8,"C6,C7"),
    ("STHE.MI","PIMCO US ST HY EUR Hdg Dist","High Yield USA","obbligaz","Dist",6.0,"C6,C7"),
    ("JNHD.MI","JPMorgan Global HY Multi-Factor","High Yield Globale","obbligaz","Acc",6.3,"A6,A7"),

    # AZIONARIO CORE
    ("SWDA.MI","iShares Core MSCI World Acc","Azionario Globale","azionario","Acc",0.8,"A5,A6,A7,A8"),
    ("VWCE.DE","Vanguard FTSE All-World Acc","Azionario Globale","azionario","Acc",1.5,"A5,A6,A7,A8,A9"),
    ("CSPX.MI","iShares Core S&P 500 Acc","Azionario USA","azionario","Acc",1.2,"A6,A7,A8,A9"),
    ("IUSA.MI","iShares Core S&P 500 Dist","Azionario USA","azionario","Dist",1.5,"C7,C8,C9"),
    ("VUSA.MI","Vanguard S&P 500 Dist","Azionario USA","azionario","Dist",1.4,"C7,C8,C9"),
    ("MEUD.MI","Lyxor MSCI EMU Dist","Azionario Europa","azionario","Dist",2.8,"C6,C7,C8"),
    ("EQQQ.MI","Invesco NASDAQ-100","Azionario USA Tech","azionario","Acc",0.5,"A8,A9"),
    ("IUIT.MI","iShares S&P 500 IT Sector","Azionario USA Tech","azionario","Acc",0.6,"A8,A9"),
    ("XDWT.MI","Xtrackers MSCI World Swap","Azionario Globale","azionario","Acc",0.9,"A6,A7,A8"),
    ("EXXW.DE","iShares Core MSCI World DE","Azionario Globale","azionario","Acc",0.8,"A6,A7,A8"),
    ("EXX5.DE","iShares EURO STOXX 50 DE","Azionario Europa","azionario","Acc",2.5,"A7,A8,A9"),
    ("EXV1.DE","iShares Core DAX","Azionario Germania","azionario","Acc",2.2,"A7,A8,A9"),
    ("ISPA.DE","iShares MSCI Spain","Azionario Spagna","azionario","Acc",3.8,"A7,A8,A9"),
    ("VAPX.MI","Vanguard FTSE Asia Pac ex JP","Azionario Asia Pac","azionario","Acc",3.2,"A7,A8,A9"),
    ("IS3N.MI","iShares Core MSCI EM IMI","Azionario EM","azionario","Acc",2.8,"A7,A8,A9"),
    ("IQQQ.DE","iShares NASDAQ-100 DE","Azionario USA Tech","azionario","Acc",0.5,"A8,A9"),
    ("ESGE.MI","iShares MSCI World ESG Enhanced","Azionario ESG","azionario","Acc",1.0,"A5,A6,A7,C5,C6,C7"),
    ("SUSW.MI","iShares MSCI World SRI","Azionario ESG","azionario","Acc",1.1,"A5,A6,A7"),
    ("JPNH.MI","iShares Core MSCI Japan EUR Hdg","Azionario Giappone","azionario","Acc",2.0,"A7,A8,C7,C8"),

    # AZIONARIO DIVIDENDI
    ("VHYL.MI","Vanguard FTSE All-World High Div","Dividendi Globale","azionario","Dist",3.5,"C5,C6,C7,C8"),
    ("IDVY.MI","iShares € Dividend","Dividendi Europa","azionario","Dist",4.2,"C5,C6,C7"),
    ("FGEQ.MI","Fidelity Global Quality Income","Dividendi Globale","azionario","Dist",3.0,"C5,C6,C7,C8"),
    ("WENT.MI","WisdomTree Europe Equity Income","Dividendi Europa","azionario","Dist",3.8,"C6,C7,C8"),
    ("TDIV.MI","VanEck Morningstar Div Leaders","Dividendi Globale","azionario","Dist",4.0,"C5,C6,C7,C8"),
    ("EUDV.MI","SPDR Euro Dividend Aristocrats","Dividendi Europa","azionario","Dist",3.5,"C5,C6,C7"),
    ("EMDV.MI","SPDR EM Dividend","Dividendi EM","azionario","Dist",5.0,"C6,C7"),
    ("DHS.MI","WisdomTree US High Dividend","Dividendi USA","azionario","Dist",3.8,"C6,C7,C8"),

    # TEMATICI
    ("DFNS.MI","VanEck Defense","Difesa","azionario","Acc",1.2,"A8,A9,C8,C9"),
    ("XAIX.MI","Xtrackers AI & Big Data","AI & Tech","azionario","Acc",0.3,"A8,A9"),
    ("SMH.MI","VanEck Semiconductor","Semiconduttori","azionario","Acc",0.8,"A8,A9"),
    ("IFFF.MI","iShares MSCI World Financials","Settore Finanziario","azionario","Acc",2.5,"A8,A9"),
    ("RARE.MI","RARE Infrastructure Value","Infrastrutture","azionario","Acc",3.2,"A6,A7,C6,C7"),
    ("WHCS.MI","WisdomTree Healthcare Small Cap","Healthcare","azionario","Acc",1.0,"A8,A9"),
    ("QNTM.MI","Defiance Quantum ETF","Quantum Computing","azionario","Acc",0.2,"A9,C9"),
    ("IPRP.MI","iShares European Property Yield","Real Estate Europa","reit","Dist",4.5,"C5,C6,C7"),
    ("IWDP.MI","iShares Developed Markets Property","Real Estate Globale","reit","Dist",4.0,"C5,C6,C7"),
    ("XREA.MI","Xtrackers FTSE EPRA Europe","Real Estate Europa","reit","Acc",3.8,"A6,A7"),
    ("REET.MI","iShares Global REIT","Real Estate Globale","reit","Acc",4.0,"A6,A7"),

    # COMMODITIES
    ("PHAU.MI","WisdomTree Physical Gold","Oro Fisico","commodity","Acc",0.0,"A3,A4,A5,A6,C3,C4,C5"),
    ("SGLN.MI","iShares Physical Gold","Oro Fisico","commodity","Acc",0.0,"A3,A4,A5,C3,C4,C5"),
    ("SILVER.MI","WisdomTree Physical Silver","Argento","commodity","Acc",0.0,"A7,A8,A9,C7,C8"),
    ("AIGA.MI","iShares Diversified Commodity","Commodities Mix","commodity","Acc",0.0,"A6,A7,C6,C7"),
    ("CMOD.MI","Amundi Bloomberg Commodity","Commodities Mix","commodity","Acc",0.0,"A6,A7,C6,C7"),
    ("OILW.MI","WisdomTree WTI Crude Oil","Petrolio","commodity","Acc",0.0,"A8,A9,C8,C9"),
    ("COPA.MI","WisdomTree Copper","Rame","commodity","Acc",0.0,"A7,A8,A9,C7,C8"),
    ("ICOM.MI","iShares Commodity Producers","Produttori Comm.","commodity","Acc",2.5,"A7,A8,C7,C8"),

    # WISDOMTREE ETP
    ("WNAS.MI","WisdomTree Nasdaq-100 3x ETP","ETP Leva 3x","leva","Acc",0.0,"A9,C9"),
    ("WWRD.MI","WisdomTree World Equity ETP","ETP Globale","azionario","Acc",0.0,"A8,A9,C8,C9"),
    ("WSPE.MI","WisdomTree S&P 500 EUR Hdg ETP","ETP USA EUR Hdg","azionario","Acc",0.0,"A7,A8,A9,C7,C8,C9"),
    ("WSPX.MI","WisdomTree S&P 500 ETP","ETP USA","azionario","Acc",0.0,"A7,A8,A9,C7,C8,C9"),
    ("WS5X.MI","WisdomTree Euro Stoxx 50 ETP","ETP Europa","azionario","Acc",0.0,"A7,A8,C7,C8"),

    # EFFICIENT CORE
    ("NTSX.MI","WisdomTree US Efficient Core","Efficient Core USA","azionario","Acc",1.5,"A6,A7,A8,A9"),
    ("NTSG.MI","WisdomTree Global Efficient Core","Efficient Core Globale","azionario","Acc",1.3,"A6,A7,A8"),
    ("WRTY.MI","WisdomTree Russell 2000 EC","Efficient Core Small","azionario","Acc",1.2,"A8,A9"),
    ("NTSZ.MI","WisdomTree EM Efficient Core","Efficient Core EM","azionario","Acc",2.0,"A7,A8"),

    # LEVA 2x
    ("L2SP.MI","WisdomTree S&P 500 2x Daily","Leva 2x USA","leva","Acc",0.0,"A9,C9"),
    ("2LVE.MI","WisdomTree Euro Stoxx 50 2x","Leva 2x Europa","leva","Acc",0.0,"A9,C9"),
    ("UC44.MI","Xtrackers S&P 500 2x Leveraged","Leva 2x USA","leva","Acc",0.0,"A9,C9"),
    ("2NVD.MI","WisdomTree NVIDIA 2x Daily","Leva 2x NVIDIA","leva","Acc",0.0,"A9,C9"),
    ("2TSL.MI","WisdomTree Tesla 2x Daily","Leva 2x Tesla","leva","Acc",0.0,"A9,C9"),
    ("2AAP.MI","WisdomTree Apple 2x Daily","Leva 2x Apple","leva","Acc",0.0,"A9,C9"),
    ("2AMZ.MI","WisdomTree Amazon 2x Daily","Leva 2x Amazon","leva","Acc",0.0,"A9,C9"),

    # LEVA 3x
    ("3USL.MI","WisdomTree S&P 500 3x Daily","Leva 3x USA","leva","Acc",0.0,"A9,C9"),
    ("3EUL.MI","WisdomTree Euro Stoxx 50 3x","Leva 3x Europa","leva","Acc",0.0,"A9,C9"),
    ("QQQ3.MI","WisdomTree NASDAQ-100 3x","Leva 3x NASDAQ","leva","Acc",0.0,"A9,C9"),
    ("3NVD.MI","WisdomTree NVIDIA 3x Daily","Leva 3x NVIDIA","leva","Acc",0.0,"A9,C9"),
    ("3TSL.MI","WisdomTree Tesla 3x Daily","Leva 3x Tesla","leva","Acc",0.0,"A9,C9"),
    ("3MSF.MI","WisdomTree Microsoft 3x Daily","Leva 3x Microsoft","leva","Acc",0.0,"A9,C9"),
    ("GLD3.MI","WisdomTree Gold 3x Daily","Leva 3x Oro","leva","Acc",0.0,"A9,C9"),
    ("5MVW.MI","WisdomTree World 5x Daily","Leva 5x World","leva","Acc",0.0,"A9,C9"),
    ("3SUE.MI","WisdomTree Euro Stoxx 50 3x ETP","Leva 3x Europa","leva","Acc",0.0,"A9,C9"),
    ("EXSH.DE","iShares ShortDAX Daily Swap","ETP Inverso DAX","leva","Acc",0.0,"A9,C9"),
]

# ── INDICATORI ────────────────────────────────────────────────────
def calc_ma(arr, period):
    if len(arr) < period: return None
    return float(np.mean(arr[-period:]))

def calc_kama(c, n=10, fast=5, slow=20):
    c = np.array(c, dtype=float)
    fs, ss = 2/(fast+1), 2/(slow+1)
    kama = np.full(len(c), np.nan)
    if len(c) <= n: return kama
    kama[n] = c[n]
    for i in range(n+1, len(c)):
        direction = abs(c[i]-c[i-n])
        volatility = np.sum(np.abs(np.diff(c[i-n:i+1])))
        er = direction/volatility if volatility != 0 else 0
        sc = (er*(fs-ss)+ss)**2
        kama[i] = kama[i-1] + sc*(c[i]-kama[i-1])
    return kama

def calc_sar(h, l, af_step=0.02, af_max=0.2):
    h, l = np.array(h), np.array(l)
    sar = np.full(len(h), np.nan)
    bull = True; af = af_step; ep = h[0]; sar[0] = l[0]
    for i in range(1, len(h)):
        prev = sar[i-1]
        if bull:
            sar[i] = prev + af*(ep-prev)
            sar[i] = min(sar[i], l[i-1], l[i-2] if i>1 else l[i-1])
            if l[i] < sar[i]: bull=False; af=af_step; ep=l[i]; sar[i]=ep
            else:
                if h[i]>ep: ep=h[i]; af=min(af+af_step, af_max)
        else:
            sar[i] = prev + af*(ep-prev)
            sar[i] = max(sar[i], h[i-1], h[i-2] if i>1 else h[i-1])
            if h[i] > sar[i]: bull=True; af=af_step; ep=h[i]; sar[i]=ep
            else:
                if l[i]<ep: ep=l[i]; af=min(af+af_step, af_max)
    return sar, bull

def calc_er(c, period=10):
    if len(c) < period+1: return 0.0
    direction = abs(float(c[-1]) - float(c[-period]))
    volatility = float(np.sum(np.abs(np.diff(c[-period:]))))
    return direction/volatility if volatility > 0 else 0.0

def calc_atr(h, l, c, period=14):
    h, l, c = np.array(h), np.array(l), np.array(c)
    tr = np.maximum(h[1:]-l[1:], np.maximum(np.abs(h[1:]-c[:-1]), np.abs(l[1:]-c[:-1])))
    if len(tr) < period: return None
    return float(np.mean(tr[-period:]))

def clean_chart(arr, n=60):
    arr = np.array(arr, dtype=float)[-n:]
    return [round(float(v),4) if not math.isnan(v) else None for v in arr]

# ── SEGNALI PER TIPO ─────────────────────────────────────────────
def signal_monetario(ticker, closes, yield_est):
    price = float(closes[-1])
    ret_1w = float(closes[-1]/closes[-6]-1)*100 if len(closes)>6 else None
    return {
        "segnale": "HOLD",
        "score": 100,
        "colore": "green",
        "motivo": "Strumento monetario — sempre in portafoglio",
        "yield_annuo": yield_est,
        "cedola_mensile_per_mille": round(yield_est/12/100*1000, 2),
        "ret_1w": round(ret_1w,2) if ret_1w else None,
    }

def signal_obbligaz(ticker, closes, highs, lows, yield_est):
    c = np.array(closes)
    price = float(c[-1])
    n = len(c)
    ma50  = calc_ma(c, 50)
    ma200 = calc_ma(c, 200)
    ret_1w  = float(price/c[-6]-1)*100  if n>6  else None
    ret_3m  = float(price/c[-63]-1)*100 if n>63 else None
    ret_1m  = float(price/c[-21]-1)*100 if n>21 else None

    score = 50
    motivi = []

    # Prezzo vs MA200
    if ma200:
        if price > ma200 * 1.005:
            score += 20; motivi.append("Sopra MA200")
        elif price < ma200 * 0.995:
            score -= 20; motivi.append("Sotto MA200")

    # Momentum 3M
    if ret_3m is not None:
        if ret_3m > 1.0:   score += 15; motivi.append("Momentum 3M positivo")
        elif ret_3m < -2.0: score -= 15; motivi.append("Momentum 3M negativo")

    # Momentum 1M
    if ret_1m is not None:
        if ret_1m > 0.3:   score += 10
        elif ret_1m < -1.0: score -= 10

    score = max(0, min(100, score))

    if score >= 65:   segnale, colore = "ACCUMULA", "green"
    elif score >= 40: segnale, colore = "HOLD",     "orange"
    else:             segnale, colore = "RIDUCI",   "red"

    return {
        "segnale": segnale,
        "score": score,
        "colore": colore,
        "motivo": " · ".join(motivi) if motivi else "Neutro",
        "yield_annuo": yield_est,
        "cedola_mensile_per_mille": round(yield_est/12/100*1000, 2),
        "ma200": round(ma200,4) if ma200 else None,
        "ret_1w": round(ret_1w,2) if ret_1w else None,
        "ret_3m": round(ret_3m,2) if ret_3m else None,
    }

def signal_azionario(ticker, closes, highs, lows, yield_est, soglia=65):
    c = np.array(closes)
    h = np.array(highs)
    l = np.array(lows)
    price = float(c[-1])
    n = len(c)

    ma50  = calc_ma(c, 50)
    ma200 = calc_ma(c, 200)
    kama_arr = calc_kama(c, 10, 5, 20)
    kama = float(kama_arr[-1]) if not np.isnan(kama_arr[-1]) else None
    sar_arr, sar_bull = calc_sar(h, l)
    er = calc_er(c, 10)
    atr = calc_atr(h, l, c, 14)

    ret_1w = float(price/c[-6]-1)*100  if n>6  else None
    ret_4w = float(price/c[-21]-1)*100 if n>21 else None
    ret_3m = float(price/c[-63]-1)*100 if n>63 else None

    # Score
    score = 0
    motivi = []

    if kama and price > kama:     score += 25; motivi.append("Prezzo>KAMA")
    if sar_bull:                  score += 25; motivi.append("SAR Bull")
    if ma200 and price > ma200:   score += 20; motivi.append("Prezzo>MA200")
    er_score = int(er * 20)
    score += min(er_score, 20)
    if er >= 0.3: motivi.append(f"ER={er:.2f}")
    if ret_3m and ret_3m > 0:     score += 10; motivi.append("Mom3M+")

    score = max(0, min(100, score))

    if score >= soglia:     segnale, colore = "BUY",   "green"
    elif score >= 40:       segnale, colore = "WATCH", "orange"
    else:                   segnale, colore = "SELL",  "red"

    return {
        "segnale": segnale,
        "score": score,
        "colore": colore,
        "motivo": " · ".join(motivi) if motivi else "Segnale debole",
        "yield_annuo": yield_est,
        "cedola_mensile_per_mille": round(yield_est/12/100*1000, 2),
        "kama": round(kama,4) if kama else None,
        "sar_bull": bool(sar_bull),
        "ma200": round(ma200,4) if ma200 else None,
        "er": round(er,3),
        "atr": round(atr,4) if atr else None,
        "ret_1w": round(ret_1w,2) if ret_1w else None,
        "ret_4w": round(ret_4w,2) if ret_4w else None,
        "ret_3m": round(ret_3m,2) if ret_3m else None,
    }

# ── FETCH ─────────────────────────────────────────────────────────
def fetch_etf(ticker):
    try:
        tk = yf.Ticker(ticker)
        hist = tk.history(period="1y", auto_adjust=True)
        if hist.empty or len(hist) < 30: return None
        closes = hist['Close'].dropna().values.tolist()
        highs  = hist['High'].dropna().reindex(hist['Close'].dropna().index).values.tolist()
        lows   = hist['Low'].dropna().reindex(hist['Close'].dropna().index).values.tolist()
        dates  = [str(d.date()) for d in hist['Close'].dropna().index]
        return closes, highs, lows, dates
    except Exception as e:
        print(f"  ⚠ {ticker}: {e}")
        return None

def load_checkpoint():
    if CKPT_FILE.exists():
        try:
            with open(CKPT_FILE) as f: return json.load(f)
        except: pass
    return {}

def save_checkpoint(ck):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(CKPT_FILE, "w") as f:
        json.dump(ck, f, separators=(',',':'))

def make_serializable(obj):
    if isinstance(obj, dict):  return {k: make_serializable(v) for k,v in obj.items()}
    if isinstance(obj, list):  return [make_serializable(v) for v in obj]
    if isinstance(obj, (bool, )):  return bool(obj)
    if hasattr(obj, 'item'):   return obj.item()
    return obj

# ── MAIN ─────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print("="*60)
    print(f"RAPTOR WEALTH MERIDIAN v1.0 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*60)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    ck = load_checkpoint()
    results = {}
    ok = 0; fail = 0

    for etf in ETF_UNIVERSE:
        ticker, nome, cat, logica, tipo_dist, yield_est, livelli = etf
        print(f"  {ticker:12} [{logica:10}] ", end="", flush=True)

        # Usa cache se recente (< 24h)
        if ticker in ck:
            cached = ck[ticker]
            closes = cached['closes']
            highs  = cached.get('highs', closes)
            lows   = cached.get('lows', closes)
            dates  = cached.get('dates', [])
            print(f"cache ", end="")
        else:
            data = fetch_etf(ticker)
            if data is None:
                print(f"❌ fail")
                fail += 1
                continue
            closes, highs, lows, dates = data
            ck[ticker] = {'closes': closes, 'highs': highs, 'lows': lows, 'dates': dates}
            save_checkpoint(ck)
            print(f"✓ ", end="")
            time.sleep(0.5)

        price = float(closes[-1])
        prev  = float(closes[-2]) if len(closes)>1 else price
        ret_1d = round((price/prev-1)*100, 2)

        # Calcola segnale in base al tipo
        if logica == "monetario":
            sig = signal_monetario(ticker, closes, yield_est)
        elif logica == "obbligaz":
            sig = signal_obbligaz(ticker, closes, highs, lows, yield_est)
        elif logica == "leva":
            sig = signal_azionario(ticker, closes, highs, lows, yield_est, soglia=80)
        elif logica == "commodity":
            sig = signal_azionario(ticker, closes, highs, lows, yield_est, soglia=65)
        elif logica == "reit":
            sig = signal_azionario(ticker, closes, highs, lows, yield_est, soglia=60)
        else:  # azionario
            sig = signal_azionario(ticker, closes, highs, lows, yield_est, soglia=65)

        # Chart 60 barre
        n60 = min(60, len(closes))
        kama_arr = calc_kama(np.array(closes), 10, 5, 20)
        ma50_arr = [calc_ma(closes[:i+1], 50) for i in range(len(closes))]
        ma200_arr= [calc_ma(closes[:i+1], 200) for i in range(len(closes))]
        sar_arr, _ = calc_sar(np.array(highs), np.array(lows))

        chart = {
            "dates":  dates[-n60:],
            "closes": clean_chart(closes, n60),
            "ma50":   clean_chart(ma50_arr, n60),
            "ma200":  clean_chart(ma200_arr, n60),
            "kama":   clean_chart(kama_arr, n60),
            "sar":    clean_chart(sar_arr, n60),
        }

        results[ticker] = {
            "ticker":    ticker,
            "nome":      nome,
            "categoria": cat,
            "logica":    logica,
            "tipo":      tipo_dist,
            "livelli":   livelli,
            "price":     round(price, 4),
            "ret_1d":    ret_1d,
            **sig,
            "chart":     chart,
        }

        print(f"→ {sig['segnale']:7} score={sig['score']}")
        ok += 1

    elapsed = (time.time()-t0)/60

    # Riepilogo per segnale
    buy   = [t for t,v in results.items() if v['segnale'] in ('BUY','ACCUMULA','HOLD') and v['score']>=65]
    watch = [t for t,v in results.items() if v['segnale']=='WATCH']
    sell  = [t for t,v in results.items() if v['segnale'] in ('SELL','RIDUCI')]

    output = make_serializable({
        "generated":   datetime.now(timezone.utc).isoformat(),
        "version":     "1.0",
        "universe":    len(ETF_UNIVERSE),
        "analyzed":    ok,
        "failed":      fail,
        "stats": {
            "buy_hold":  len(buy),
            "watch":     len(watch),
            "sell":      len(sell),
        },
        "elapsed_min": round(elapsed, 1),
        "etfs":        results,
    })

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(',',':'))

    size = OUT_FILE.stat().st_size / 1024
    print(f"\n✅ {OUT_FILE.name} ({size:.0f} KB)")
    print(f"   BUY/HOLD: {len(buy)} · WATCH: {len(watch)} · SELL: {len(sell)}")
    print(f"   Tempo: {elapsed:.1f} min")

if __name__ == "__main__":
    main()
