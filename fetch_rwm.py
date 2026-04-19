#!/usr/bin/env python3
"""
RAPTOR WEALTH MERIDIAN — fetch_rwm.py v2.0
═══════════════════════════════════════════
Base 100 ETF fissi + integrazione dinamica RAPTOR:
  - raptor-settoriali → ticker con segnale LONG/WATCH
  - raptor-tematici   → ticker con segnale LONG/WATCH
  - raptor-geografia  → ticker con segnale LONG/WATCH

Logiche segnale per tipo:
  MONETARIO   → sempre HOLD, score 100
  OBBLIGAZ.   → MA200 + momentum → ACCUMULA/HOLD/RIDUCI
  AZIONARIO   → KAMA + SAR + ER + MA200 + mom3M → BUY/WATCH/SELL
  LEVA 2x/3x  → come azionario, soglia score ≥ 80
  COMMODITIES → come azionario, soglia ≥ 65
  REIT        → come azionario, soglia ≥ 60

Output: data/rwm_signals.json
        data/rwm_checkpoint.json
"""

import json, os, time, math, urllib.request
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

MAX_ETF_TOTAL = 300  # tetto massimo ETF analizzati

# URL JSON dei tool RAPTOR live
RAPTOR_SOURCES = {
    "settoriali": "https://raw.githubusercontent.com/Giorgiogoldoni/raptor-settoriali/main/settoriali.json",
    "tematici":   "https://raw.githubusercontent.com/Giorgiogoldoni/raptor-tematici/main/tematici.json",
    "geografia":  "https://raw.githubusercontent.com/Giorgiogaldoni/raptor-geografia/main/geografia.json",
}

# Segnali accettati come "attivi" dai tool RAPTOR
RAPTOR_OK = {"LONG_FORTE", "LONG", "EARLY_FORTE", "EARLY", "WATCH", "LONG_CONF"}

# ── UNIVERSO BASE (100 ETF FISSI) ──────────────────────────────────
# [TICKER, NOME, CATEGORIA, TIPO_LOGICA, TIPO_DIST, YIELD_STIMATO, LIVELLI]
ETF_UNIVERSE = [
    # MONETARIO (5)
    ("XEON.MI","Xtrackers EUR Overnight Rate Swap","Monetario","monetario","Acc",3.2,"A1,A2,C1,C2"),
    ("SMART.MI","iShares € Ultrashort Bond Dist","Monetario","monetario","Dist",3.0,"C1,C2"),
    ("IU0E.MI","iShares € Ultrashort Bond Acc","Monetario","monetario","Acc",3.0,"A1,A2"),
    ("XEOD.MI","Xtrackers EUR Overnight 1D Dist","Monetario","monetario","Dist",3.1,"C1,C2"),
    ("ERNX.MI","Amundi Euro Overnight Return","Monetario","monetario","Acc",3.1,"A1,A2"),

    # OBBLIGAZIONARIO GOVERNO (8)
    ("IBTE.MI","iShares € Govt Bond 0-1yr","Obbligaz. Breve","obbligaz","Acc",3.0,"A2,A3,C2,C3"),
    ("SXRM.MI","iShares Core € Govt Bond Dist","Obbligaz. Governo","obbligaz","Dist",3.2,"C2,C3,C4"),
    ("EUNH.MI","iShares Core € Corp Bond Dist","Obbligaz. Corp","obbligaz","Dist",3.5,"C3,C4"),
    ("IEAC.MI","iShares Core € Corp Bond Acc","Obbligaz. Corp","obbligaz","Acc",3.5,"A3,A4"),
    ("IEAG.MI","iShares Core € Aggregate Bond","Obbligaz. Mix","obbligaz","Acc",3.3,"A3,A4,A5"),
    ("GOM.MI","iShares € Govt Bond 3-5yr","Obbligaz. Governo","obbligaz","Acc",3.1,"A3,A4,C3,C4"),
    ("IBTM.MI","iShares € Govt Bond 3-7yr","Obbligaz. Governo","obbligaz","Acc",3.2,"A3,A4,C3,C4"),
    ("SXRV.MI","iShares € Govt Bond 7-10yr","Obbligaz. Governo","obbligaz","Acc",3.4,"A4,A5,C4,C5"),

    # OBBLIGAZIONARIO GLOBALE (5)
    ("IGLO.MI","iShares Global Govt Bond EUR Hdg","Obbligaz. Globale","obbligaz","Acc",3.5,"A4,A5"),
    ("VAGE.MI","Vanguard Global Aggregate EUR Hdg","Obbligaz. Globale","obbligaz","Acc",3.4,"A4,A5,A6"),
    ("AGGH.MI","iShares Core Global Aggregate Dist","Obbligaz. Globale","obbligaz","Dist",3.4,"C4,C5"),
    ("SEMB.MI","iShares JP Morgan EM Bond Dist","Obbligaz. EM","obbligaz","Dist",5.5,"C5,C6"),
    ("EMBE.MI","iShares JP Morgan EM Bond Acc","Obbligaz. EM","obbligaz","Acc",5.5,"A5,A6"),

    # HIGH YIELD (7)
    ("IHYU.MI","iShares € High Yield Corp Bond Dist","High Yield","obbligaz","Dist",6.2,"C5,C6,C7"),
    ("HYLD.MI","iShares Global HY Corp Bond Acc","High Yield Globale","obbligaz","Acc",6.0,"A6,A7"),
    ("DHYA.MI","Xtrackers Global HY Corp Bond","High Yield Globale","obbligaz","Acc",6.1,"A6,A7"),
    ("EUHA.MI","PIMCO Euro ST HY Acc","High Yield EUR","obbligaz","Acc",5.8,"A6,A7,C6,C7"),
    ("EUHI.MI","PIMCO Euro ST HY Dist","High Yield EUR","obbligaz","Dist",5.8,"C6,C7"),
    ("STHE.MI","PIMCO US ST HY EUR Hdg Dist","High Yield USA","obbligaz","Dist",6.0,"C6,C7"),
    ("JNHD.MI","JPMorgan Global HY Multi-Factor","High Yield Globale","obbligaz","Acc",6.3,"A6,A7"),

    # AZIONARIO CORE (11)
    ("SWDA.MI","iShares Core MSCI World Acc","Azionario Globale","azionario","Acc",0.8,"A5,A6,A7,A8"),
    ("VWCE.DE","Vanguard FTSE All-World Acc","Azionario Globale","azionario","Acc",1.5,"A5,A6,A7,A8,A9"),
    ("CSPX.MI","iShares Core S&P 500 Acc","Azionario USA","azionario","Acc",1.2,"A6,A7,A8,A9"),
    ("IUSA.MI","iShares Core S&P 500 Dist","Azionario USA","azionario","Dist",1.5,"C7,C8,C9"),
    ("VUSA.MI","Vanguard S&P 500 Dist","Azionario USA","azionario","Dist",1.4,"C7,C8,C9"),
    ("MEUD.MI","Lyxor MSCI EMU Dist","Azionario Europa","azionario","Dist",2.8,"C6,C7,C8"),
    ("EQQQ.MI","Invesco NASDAQ-100","Azionario USA Tech","azionario","Acc",0.5,"A8,A9"),
    ("IUIT.MI","iShares S&P 500 IT Sector","Azionario USA Tech","azionario","Acc",0.6,"A8,A9"),
    ("XDWT.MI","Xtrackers MSCI World Swap","Azionario Globale","azionario","Acc",0.9,"A6,A7,A8"),
    ("IS3N.MI","iShares Core MSCI EM IMI","Azionario EM","azionario","Acc",2.8,"A7,A8,A9"),
    ("ESGE.MI","iShares MSCI World ESG Enhanced","Azionario ESG","azionario","Acc",1.0,"A5,A6,A7,C5,C6,C7"),

    # AZIONARIO DIVIDENDI (8)
    ("VHYL.MI","Vanguard FTSE All-World High Div","Dividendi Globale","azionario","Dist",3.5,"C5,C6,C7,C8"),
    ("IDVY.MI","iShares € Dividend","Dividendi Europa","azionario","Dist",4.2,"C5,C6,C7"),
    ("FGEQ.MI","Fidelity Global Quality Income","Dividendi Globale","azionario","Dist",3.0,"C5,C6,C7,C8"),
    ("WENT.MI","WisdomTree Europe Equity Income","Dividendi Europa","azionario","Dist",3.8,"C6,C7,C8"),
    ("TDIV.MI","VanEck Morningstar Div Leaders","Dividendi Globale","azionario","Dist",4.0,"C5,C6,C7,C8"),
    ("EUDV.MI","SPDR Euro Dividend Aristocrats","Dividendi Europa","azionario","Dist",3.5,"C5,C6,C7"),
    ("EMDV.MI","SPDR EM Dividend","Dividendi EM","azionario","Dist",5.0,"C6,C7"),
    ("DHS.MI","WisdomTree US High Dividend","Dividendi USA","azionario","Dist",3.8,"C6,C7,C8"),

    # TEMATICI BASE (7)
    ("DFNS.MI","VanEck Defense","Difesa","azionario","Acc",1.2,"A8,A9,C8,C9"),
    ("SMH.MI","VanEck Semiconductor","Semiconduttori","azionario","Acc",0.8,"A8,A9"),
    ("IFFF.MI","iShares MSCI World Financials","Settore Finanziario","azionario","Acc",2.5,"A8,A9"),
    ("RARE.MI","VanEck Rare Earth","Materie Prime Rare","azionario","Acc",1.0,"A6,A7,C6,C7"),
    ("WHCS.MI","WisdomTree Healthcare","Healthcare","azionario","Acc",1.0,"A8,A9"),
    ("QNTM.MI","VanEck Quantum Computing","Quantum Computing","azionario","Acc",0.2,"A9,C9"),
    ("XAIX.MI","Xtrackers AI & Big Data","AI & Tech","azionario","Acc",0.3,"A8,A9"),

    # REIT (4)
    ("IPRP.MI","iShares European Property Yield","REIT Europa","reit","Dist",4.5,"C5,C6,C7"),
    ("IWDP.MI","iShares Developed Markets Property","REIT Globale","reit","Dist",4.0,"C5,C6,C7"),
    ("XREA.MI","Xtrackers FTSE EPRA Europe","REIT Europa","reit","Acc",3.8,"A6,A7"),
    ("REET.MI","iShares Global REIT","REIT Globale","reit","Acc",4.0,"A6,A7"),

    # COMMODITIES (8)
    ("PHAU.MI","WisdomTree Physical Gold","Oro Fisico","commodity","Acc",0.0,"A3,A4,A5,A6,C3,C4,C5"),
    ("SGLN.MI","iShares Physical Gold","Oro Fisico","commodity","Acc",0.0,"A3,A4,A5,C3,C4,C5"),
    ("SILVER.MI","WisdomTree Physical Silver","Argento","commodity","Acc",0.0,"A7,A8,A9,C7,C8"),
    ("AIGA.MI","iShares Diversified Commodity","Commodities Mix","commodity","Acc",0.0,"A6,A7,C6,C7"),
    ("CMOD.MI","Amundi Bloomberg Commodity","Commodities Mix","commodity","Acc",0.0,"A6,A7,C6,C7"),
    ("OILW.MI","WisdomTree WTI Crude Oil","Petrolio","commodity","Acc",0.0,"A8,A9,C8,C9"),
    ("COPA.MI","WisdomTree Copper","Rame","commodity","Acc",0.0,"A7,A8,A9,C7,C8"),
    ("ICOM.MI","iShares Commodity Producers","Produttori Comm.","commodity","Acc",2.5,"A7,A8,C7,C8"),

    # WISDOMTREE ETP (5)
    ("WWRD.MI","WisdomTree World Equity ETP","ETP Globale","azionario","Acc",0.0,"A8,A9,C8,C9"),
    ("WSPE.MI","WisdomTree S&P 500 EUR Hdg ETP","ETP USA EUR Hdg","azionario","Acc",0.0,"A7,A8,A9,C7,C8,C9"),
    ("WSPX.MI","WisdomTree S&P 500 ETP","ETP USA","azionario","Acc",0.0,"A7,A8,A9,C7,C8,C9"),
    ("WS5X.MI","WisdomTree Euro Stoxx 50 ETP","ETP Europa","azionario","Acc",0.0,"A7,A8,C7,C8"),
    ("NTSX.MI","WisdomTree US Efficient Core","Efficient Core USA","azionario","Acc",1.5,"A6,A7,A8,A9"),

    # EFFICIENT CORE (3)
    ("NTSG.MI","WisdomTree Global Efficient Core","Efficient Core Globale","azionario","Acc",1.3,"A6,A7,A8"),
    ("WRTY.MI","WisdomTree Russell 2000 EC","Efficient Core Small","azionario","Acc",1.2,"A8,A9"),
    ("NTSZ.MI","WisdomTree EM Efficient Core","Efficient Core EM","azionario","Acc",2.0,"A7,A8"),

    # LEVA 2x (7)
    ("L2SP.MI","WisdomTree S&P 500 2x Daily","Leva 2x USA","leva","Acc",0.0,"A9,C9"),
    ("2LVE.MI","WisdomTree Euro Stoxx 50 2x","Leva 2x Europa","leva","Acc",0.0,"A9,C9"),
    ("UC44.MI","Xtrackers S&P 500 2x Leveraged","Leva 2x USA","leva","Acc",0.0,"A9,C9"),
    ("2NVD.MI","WisdomTree NVIDIA 2x Daily","Leva 2x NVIDIA","leva","Acc",0.0,"A9,C9"),
    ("2TSL.MI","WisdomTree Tesla 2x Daily","Leva 2x Tesla","leva","Acc",0.0,"A9,C9"),
    ("2AAP.MI","WisdomTree Apple 2x Daily","Leva 2x Apple","leva","Acc",0.0,"A9,C9"),
    ("2AMZ.MI","WisdomTree Amazon 2x Daily","Leva 2x Amazon","leva","Acc",0.0,"A9,C9"),

    # LEVA 3x/5x (10)
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

# ── INTEGRAZIONE RAPTOR ────────────────────────────────────────────
def fetch_json_url(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "RaptorWM/2.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"  ⚠ {url.split('/')[-1]}: {e}")
        return None

def extract_settoriali(data):
    out = []
    if not data or "portfolios" not in data: return out
    for port_name, port in data["portfolios"].items():
        for etf in port.get("all", []):
            sig    = etf.get("signal","")
            ticker = etf.get("ticker","")
            nome   = etf.get("name","")
            score  = etf.get("score",0)
            if ticker and sig in RAPTOR_OK:
                out.append({"ticker":ticker,"nome":nome,
                    "categoria":f"Settoriale {port_name.title()}",
                    "logica":"azionario","tipo":"Acc","yield_est":1.5,
                    "livelli":"A7,A8,A9,C7,C8,C9",
                    "raptor_signal":sig,"raptor_score":score,"source":"settoriali"})
    return out

def extract_tematici(data):
    out = []
    if not data or "groups" not in data: return out
    for group_name, group in data["groups"].items():
        for etf in group.get("all", []):
            sig    = etf.get("signal","")
            ticker = etf.get("ticker","")
            nome   = etf.get("name","")
            score  = etf.get("score",0)
            er     = etf.get("er",0)
            if ticker and sig in RAPTOR_OK and er >= 0.35:
                out.append({"ticker":ticker,"nome":nome,
                    "categoria":f"Tematico {group.get('name',group_name)}",
                    "logica":"azionario","tipo":"Acc","yield_est":0.5,
                    "livelli":"A8,A9,C8,C9",
                    "raptor_signal":sig,"raptor_score":score,"source":"tematici"})
    return out

def extract_geografia(data):
    out = []
    if not data: return out
    for port_key in ["paesi","new_area"]:
        port = data.get(port_key,{})
        for etf in port.get("all",[]):
            sig    = etf.get("signal","")
            ticker = etf.get("ticker","")
            nome   = etf.get("name","")
            score  = etf.get("score",0)
            if ticker and sig in RAPTOR_OK:
                out.append({"ticker":ticker,"nome":nome,
                    "categoria":f"Geografico {port_key.replace('_',' ').title()}",
                    "logica":"azionario","tipo":"Acc","yield_est":2.0,
                    "livelli":"A6,A7,A8,A9,C6,C7,C8,C9",
                    "raptor_signal":sig,"raptor_score":score,"source":"geografia"})
    return out

def load_raptor_extensions(base_tickers_set):
    print("\n── Integrazione RAPTOR ──────────────────────────────────")
    all_ext = []
    seen    = set(base_tickers_set)
    fns     = {"settoriali":extract_settoriali,
               "tematici":extract_tematici,
               "geografia":extract_geografia}

    for name, url in RAPTOR_SOURCES.items():
        print(f"  📡 {name}... ", end="", flush=True)
        data = fetch_json_url(url)
        if data is None:
            print("❌ skip"); continue
        candidates = fns[name](data)
        added = 0
        for c in candidates:
            t = c["ticker"]
            # salta duplicati, ticker JPY-hedged, ticker lunghissimi
            if t in seen: continue
            if len(t) > 12 or t.endswith("J.MI"): continue
            seen.add(t)
            all_ext.append(c)
            added += 1
        print(f"✓ +{added} ETF")
        time.sleep(1)

    # ordina per score RAPTOR decrescente
    all_ext.sort(key=lambda x: x.get("raptor_score",0), reverse=True)

    # rispetta tetto
    slots = MAX_ETF_TOTAL - len(base_tickers_set)
    if len(all_ext) > slots:
        print(f"  ✂ Ridotto a {slots} (limite {MAX_ETF_TOTAL})")
        all_ext = all_ext[:slots]

    print(f"  Totale ETF aggiuntivi: {len(all_ext)}")
    print("─────────────────────────────────────────────────────────\n")
    return all_ext

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
        d = abs(c[i]-c[i-n])
        v = np.sum(np.abs(np.diff(c[i-n:i+1])))
        er = d/v if v != 0 else 0
        sc = (er*(fs-ss)+ss)**2
        kama[i] = kama[i-1] + sc*(c[i]-kama[i-1])
    return kama

def calc_sar(h, l, af_step=0.02, af_max=0.2):
    h, l = np.array(h), np.array(l)
    if len(h) < 5: return np.array(l), True
    sar = np.zeros(len(h))
    bull = True; sar[0]=l[0]; ep=h[0]; af=af_step
    for i in range(1, len(h)):
        prev = sar[i-1]
        if bull:
            sar[i] = prev+af*(ep-prev)
            sar[i] = min(sar[i], l[i-1], l[i-2] if i>1 else l[i-1])
            if l[i]<sar[i]: bull=False; af=af_step; ep=l[i]; sar[i]=ep
            else:
                if h[i]>ep: ep=h[i]; af=min(af+af_step,af_max)
        else:
            sar[i] = prev+af*(ep-prev)
            sar[i] = max(sar[i], h[i-1], h[i-2] if i>1 else h[i-1])
            if h[i]>sar[i]: bull=True; af=af_step; ep=h[i]; sar[i]=ep
            else:
                if l[i]<ep: ep=l[i]; af=min(af+af_step,af_max)
    return sar, bull

def calc_er(c, period=10):
    if len(c)<period+1: return 0.0
    d = abs(float(c[-1])-float(c[-period]))
    v = float(np.sum(np.abs(np.diff(c[-period:]))))
    return d/v if v>0 else 0.0

def calc_atr(h, l, c, period=14):
    h,l,c = np.array(h),np.array(l),np.array(c)
    tr = np.maximum(h[1:]-l[1:], np.maximum(np.abs(h[1:]-c[:-1]),np.abs(l[1:]-c[:-1])))
    if len(tr)<period: return None
    return float(np.mean(tr[-period:]))

def clean_chart(arr, n=60):
    arr = np.array(arr, dtype=float)[-n:]
    return [round(float(v),4) if not math.isnan(v) else None for v in arr]

# ── SEGNALI ───────────────────────────────────────────────────────
def ret1d(closes):
    p = float(closes[-1]); pr = float(closes[-2]) if len(closes)>1 else p
    return round((p/pr-1)*100,2)

def signal_monetario(closes, yield_est):
    return {"segnale":"HOLD","score":100,"colore":"green",
            "motivo":"Strumento monetario",
            "yield_annuo":yield_est,"ret_1d":ret1d(closes)}

def signal_obbligaz(closes, highs, lows, yield_est):
    c=np.array(closes); p=float(c[-1]); n=len(c)
    ma200=calc_ma(c,200)
    r3m=float(p/c[-63]-1)*100 if n>63 else None
    r1m=float(p/c[-21]-1)*100 if n>21 else None
    score=50; motivi=[]
    if ma200:
        if p>ma200*1.005:  score+=20; motivi.append("Sopra MA200")
        elif p<ma200*0.995: score-=20
    if r3m is not None:
        if r3m>1.0:   score+=15; motivi.append("Mom3M+")
        elif r3m<-2.0: score-=15
    if r1m is not None:
        if r1m>0.3:   score+=10
        elif r1m<-1.0: score-=10
    score=max(0,min(100,score))
    if score>=65:   seg,col="ACCUMULA","green"
    elif score>=40: seg,col="HOLD","orange"
    else:           seg,col="RIDUCI","red"
    return {"segnale":seg,"score":score,"colore":col,
            "motivo":" · ".join(motivi) if motivi else "Neutro",
            "yield_annuo":yield_est,"ma200":round(ma200,4) if ma200 else None,
            "ret_1d":ret1d(closes),
            "ret_3m":round(r3m,2) if r3m else None}

def signal_azionario(closes, highs, lows, yield_est, soglia=65):
    c=np.array(closes); h=np.array(highs); l=np.array(lows)
    p=float(c[-1]); n=len(c)
    ma200=calc_ma(c,200)
    ka=calc_kama(c,10,5,20); kama=float(ka[-1]) if not np.isnan(ka[-1]) else None
    _,sar_bull=calc_sar(h,l)
    er=calc_er(c,10); atr=calc_atr(h,l,c,14)
    r3m=float(p/c[-63]-1)*100 if n>63 else None
    r1w=float(p/c[-6]-1)*100  if n>6  else None
    score=0; motivi=[]
    if kama and p>kama:    score+=25; motivi.append("Prezzo>KAMA")
    if sar_bull:            score+=25; motivi.append("SAR Bull")
    if ma200 and p>ma200:  score+=20; motivi.append("Prezzo>MA200")
    score+=min(int(er*20),20)
    if er>=0.3: motivi.append(f"ER={er:.2f}")
    if r3m and r3m>0: score+=10; motivi.append("Mom3M+")
    score=max(0,min(100,score))
    if score>=soglia: seg,col="BUY","green"
    elif score>=40:   seg,col="WATCH","orange"
    else:             seg,col="SELL","red"
    return {"segnale":seg,"score":score,"colore":col,
            "motivo":" · ".join(motivi) if motivi else "Segnale debole",
            "yield_annuo":yield_est,
            "kama":round(kama,4) if kama else None,
            "sar_bull":bool(sar_bull),"ma200":round(ma200,4) if ma200 else None,
            "er":round(er,3),"atr":round(atr,4) if atr else None,
            "ret_1d":ret1d(closes),
            "ret_1w":round(r1w,2) if r1w else None,
            "ret_3m":round(r3m,2) if r3m else None}

# ── FETCH DATI ────────────────────────────────────────────────────
def fetch_etf(ticker):
    try:
        tk=yf.Ticker(ticker)
        hist=tk.history(period="1y",auto_adjust=True)
        if hist.empty or len(hist)<30: return None
        idx=hist['Close'].dropna().index
        closes=hist['Close'].dropna().values.tolist()
        highs =hist['High'].dropna().reindex(idx).values.tolist()
        lows  =hist['Low'].dropna().reindex(idx).values.tolist()
        dates =[str(d.date()) for d in idx]
        return closes,highs,lows,dates
    except Exception as e:
        print(f"  ⚠ {ticker}: {e}"); return None

def load_checkpoint():
    if CKPT_FILE.exists():
        try:
            with open(CKPT_FILE) as f: return json.load(f)
        except: pass
    return {}

def save_checkpoint(ck):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(CKPT_FILE,"w") as f: json.dump(ck,f,separators=(',',':'))

def make_serializable(obj):
    if isinstance(obj,dict): return {k:make_serializable(v) for k,v in obj.items()}
    if isinstance(obj,list): return [make_serializable(v) for v in obj]
    if isinstance(obj,bool): return bool(obj)
    if hasattr(obj,'item'):  return obj.item()
    return obj

def analyze_etf(ticker, nome, cat, logica, tipo_dist, yield_est, livelli,
                ck, raptor_meta=None):
    print(f"  {ticker:16} [{logica:10}] ", end="", flush=True)

    if ticker in ck:
        d=ck[ticker]; closes=d['closes']; highs=d.get('highs',closes)
        lows=d.get('lows',closes); dates=d.get('dates',[])
        print("cache ", end="")
    else:
        data=fetch_etf(ticker)
        if data is None: print("❌ fail"); return None
        closes,highs,lows,dates=data
        ck[ticker]={'closes':closes,'highs':highs,'lows':lows,'dates':dates}
        save_checkpoint(ck)
        print("✓ ", end="")
        time.sleep(0.4)

    price=float(closes[-1])

    if logica=="monetario": sig=signal_monetario(closes,yield_est)
    elif logica=="obbligaz": sig=signal_obbligaz(closes,highs,lows,yield_est)
    elif logica=="leva":     sig=signal_azionario(closes,highs,lows,yield_est,soglia=80)
    elif logica=="commodity":sig=signal_azionario(closes,highs,lows,yield_est,soglia=65)
    elif logica=="reit":     sig=signal_azionario(closes,highs,lows,yield_est,soglia=60)
    else:                    sig=signal_azionario(closes,highs,lows,yield_est,soglia=65)

    n60=min(60,len(closes))
    ka=calc_kama(np.array(closes),10,5,20)
    ma50a =[calc_ma(closes[:i+1],50)  for i in range(len(closes))]
    ma200a=[calc_ma(closes[:i+1],200) for i in range(len(closes))]
    sara,_=calc_sar(np.array(highs),np.array(lows))
    chart={"dates":dates[-n60:],"closes":clean_chart(closes,n60),
           "ma50":clean_chart(ma50a,n60),"ma200":clean_chart(ma200a,n60),
           "kama":clean_chart(ka,n60),"sar":clean_chart(sara,n60)}

    record={"ticker":ticker,"nome":nome,"categoria":cat,"logica":logica,
            "tipo":tipo_dist,"livelli":livelli,"price":round(price,4),
            **sig,"chart":chart}

    if raptor_meta:
        record["raptor_source"]=raptor_meta.get("source")
        record["raptor_signal"]=raptor_meta.get("raptor_signal")
        record["raptor_score"] =raptor_meta.get("raptor_score")

    print(f"→ {sig['segnale']:8} score={sig['score']}")
    return record

# ── MAIN ──────────────────────────────────────────────────────────
def main():
    t0=time.time()
    print("="*65)
    print(f"RAPTOR WEALTH MERIDIAN v2.0 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Base: {len(ETF_UNIVERSE)} ETF · Tetto max: {MAX_ETF_TOTAL}")
    print("="*65)
    DATA_DIR.mkdir(parents=True,exist_ok=True)

    ck=load_checkpoint(); results={}; ok=0; fail=0

    # 1. BASE
    print(f"\n── Universo Base ({len(ETF_UNIVERSE)} ETF) ──────────────────────────────")
    base_set=set()
    for etf in ETF_UNIVERSE:
        ticker,nome,cat,logica,tipo_dist,yield_est,livelli=etf
        base_set.add(ticker)
        rec=analyze_etf(ticker,nome,cat,logica,tipo_dist,yield_est,livelli,ck)
        if rec: results[ticker]=rec; ok+=1
        else: fail+=1

    # 2. RAPTOR EXTENSIONS
    ext_list=load_raptor_extensions(base_set)
    print(f"── ETF Estesi RAPTOR ({len(ext_list)}) ─────────────────────────────")
    for ext in ext_list:
        t=ext["ticker"]
        if t in results: continue
        rec=analyze_etf(t,ext["nome"],ext["categoria"],ext["logica"],
                        ext["tipo"],ext["yield_est"],ext["livelli"],ck,
                        raptor_meta=ext)
        if rec: results[t]=rec; ok+=1
        else: fail+=1

    elapsed=(time.time()-t0)/60

    # 3. STATS
    buy  =[t for t,v in results.items() if v['segnale'] in ('BUY','ACCUMULA','HOLD') and v['score']>=65]
    watch=[t for t,v in results.items() if v['segnale']=='WATCH']
    sell =[t for t,v in results.items() if v['segnale'] in ('SELL','RIDUCI')]
    by_src={}
    for t,v in results.items():
        s=v.get("raptor_source","base"); by_src[s]=by_src.get(s,0)+1

    output=make_serializable({
        "generated": datetime.now(timezone.utc).isoformat(),
        "version":   "2.0",
        "universe":  len(ETF_UNIVERSE),
        "extended":  len(ext_list),
        "analyzed":  ok, "failed": fail, "total": len(results),
        "stats":{"buy_hold":len(buy),"watch":len(watch),"sell":len(sell),
                 "by_source":by_src},
        "elapsed_min": round(elapsed,1),
        "etfs": results,
    })

    with open(OUT_FILE,"w",encoding="utf-8") as f:
        json.dump(output,f,ensure_ascii=False,separators=(',',':'))

    size=OUT_FILE.stat().st_size/1024
    print(f"\n{'='*65}")
    print(f"✅ {OUT_FILE.name} ({size:.0f} KB)")
    print(f"   Totale ETF: {len(results)} | base:{by_src.get('base',len(ETF_UNIVERSE))} "
          f"settoriali:{by_src.get('settoriali',0)} "
          f"tematici:{by_src.get('tematici',0)} "
          f"geografia:{by_src.get('geografia',0)}")
    print(f"   BUY/HOLD:{len(buy)} · WATCH:{len(watch)} · SELL:{len(sell)}")
    print(f"   Tempo: {elapsed:.1f} min")
    print("="*65)

if __name__=="__main__":
    main()
