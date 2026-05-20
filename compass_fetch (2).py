#!/usr/bin/env python3
"""
COMPASS — compass_fetch.py v2.0
════════════════════════════════
Script autonomo — nessuna dipendenza esterna tranne Yahoo Finance.

Funzioni:
  1. Classifica regime macro da 15 ETF proxy USA (Livello 0)
  2. Calcola segnale geografico dinamico (STOXX/SPY, EEM/SPY)
  3. Scarica prezzi per 86+ ETF Compass
  4. Calcola score differenziato per livello
  5. Gestisce storia portafogli modello (100.000€ per livello)
     - Backtest dal 01/01/2025
     - Trigger ribilanciamento: scenario, drawdown, segnale
     - Total return (price + yield pro-rata)
     - Reinvestimento ETF usciti

Output: data/compass_data.json
"""

import json, math, datetime, time, urllib.request
from pathlib import Path

# ── CONFIGURAZIONE ─────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
LEVELS_FILE = BASE_DIR / "data" / "levels.json"
OUT_FILE    = BASE_DIR / "data" / "compass_data.json"
BACKTEST_START = "2025-01-01"
CAPITALE_MODELLO = 100_000

# ── ETF PROXY per classificazione regime (15 ETF USA) ──────────────────────
ETF_PROXY = {
    "SPY" : {"goldilocks":0.9,"reflazione":0.7,"stagflazione":0.1,"risk_off":0.0,"neutro":0.5},
    "QQQ" : {"goldilocks":0.9,"reflazione":0.5,"stagflazione":0.0,"risk_off":0.0,"neutro":0.4},
    "IWM" : {"goldilocks":0.85,"reflazione":0.8,"stagflazione":0.1,"risk_off":0.0,"neutro":0.4},
    "VGK" : {"goldilocks":0.8,"reflazione":0.75,"stagflazione":0.15,"risk_off":0.0,"neutro":0.4},
    "EEM" : {"goldilocks":0.7,"reflazione":0.9,"stagflazione":0.2,"risk_off":0.0,"neutro":0.4},
    "EWJ" : {"goldilocks":0.7,"reflazione":0.65,"stagflazione":0.2,"risk_off":0.2,"neutro":0.5},
    "TLT" : {"goldilocks":0.4,"reflazione":0.1,"stagflazione":0.1,"risk_off":0.9,"neutro":0.5},
    "IEF" : {"goldilocks":0.5,"reflazione":0.2,"stagflazione":0.2,"risk_off":0.8,"neutro":0.5},
    "HYG" : {"goldilocks":0.85,"reflazione":0.7,"stagflazione":0.1,"risk_off":0.0,"neutro":0.5},
    "LQD" : {"goldilocks":0.6,"reflazione":0.4,"stagflazione":0.2,"risk_off":0.3,"neutro":0.5},
    "TIP" : {"goldilocks":0.5,"reflazione":0.85,"stagflazione":0.9,"risk_off":0.4,"neutro":0.5},
    "GLD" : {"goldilocks":0.3,"reflazione":0.8,"stagflazione":0.9,"risk_off":0.9,"neutro":0.5},
    "USO" : {"goldilocks":0.5,"reflazione":0.9,"stagflazione":0.85,"risk_off":0.2,"neutro":0.4},
    "VXX" : {"goldilocks":0.0,"reflazione":0.0,"stagflazione":0.4,"risk_off":1.0,"neutro":0.2},
    "UUP" : {"goldilocks":0.3,"reflazione":0.2,"stagflazione":0.6,"risk_off":0.7,"neutro":0.5},
}

SCENARI = ["goldilocks","reflazione","stagflazione","risk_off","neutro"]

# Pesi_override per scenario (moltiplicatori allocazione target)
SCENARIO_PESI = {
    "goldilocks" : {"monetario":0.5,"obbligaz_ig":0.7,"hy":1.1,"em_bond":0.9,"az_globale":1.3,"az_europa":1.1,"az_usa":1.3,"az_em":1.2,"tematico":0.8,"leva":1.2,"multi_asset":1.1},
    "reflazione" : {"monetario":0.4,"obbligaz_ig":0.6,"hy":1.2,"em_bond":1.2,"az_globale":1.1,"az_europa":1.0,"az_usa":1.0,"az_em":1.3,"tematico":1.5,"leva":0.8,"multi_asset":1.0},
    "stagflazione":{"monetario":1.5,"obbligaz_ig":0.7,"hy":0.5,"em_bond":0.5,"az_globale":0.3,"az_europa":0.3,"az_usa":0.3,"az_em":0.2,"tematico":1.8,"leva":0.1,"multi_asset":0.5},
    "risk_off"   : {"monetario":2.0,"obbligaz_ig":1.5,"hy":0.3,"em_bond":0.3,"az_globale":0.3,"az_europa":0.3,"az_usa":0.4,"az_em":0.2,"tematico":0.6,"leva":0.0,"multi_asset":0.4},
    "neutro"     : {"monetario":1.0,"obbligaz_ig":1.0,"hy":1.0,"em_bond":1.0,"az_globale":1.0,"az_europa":1.0,"az_usa":1.0,"az_em":1.0,"tematico":1.0,"leva":1.0,"multi_asset":1.0},
}

# ── PERSISTENZA per gruppo livello ─────────────────────────────────────────
LIVELLO_GRUPPO = {
    "C1":"conservativo","C2":"conservativo","C3":"conservativo",
    "C4":"bilanciato","C5":"bilanciato","C6":"bilanciato",
    "C7":"aggressivo","C8":"aggressivo","C9":"aggressivo",
    "A1":"conservativo","A2":"conservativo","A3":"conservativo",
    "A4":"bilanciato","A5":"bilanciato","A6":"bilanciato",
    "A7":"aggressivo","A8":"aggressivo","A9":"aggressivo",
}
PERSISTENZA = {
    "conservativo": {"settimane": 8, "soglia": 65},
    "bilanciato"  : {"settimane": 4, "soglia": 60},
    "aggressivo"  : {"settimane": 2, "soglia": 55},
}

# ── ETF UNIVERSO ────────────────────────────────────────────────────────────
ETF_UNIVERSE = sorted(set([
    "2B70.MI","2LVE.MI","2NVD.MI","3EUL.MI","3NVD.MI","3USL.MI",
    "AGGH.MI","AIGA.MI","CMOD.MI","COPA.MI","CSPX.MI","DFNS.MI",
    "DHS.MI","DHYA.MI","EMBE.MI","EMDV.MI","EQQQ.MI","ERNX.MI",
    "ESGE.MI","EUDV.MI","EUHA.MI","EUHI.MI","EUNH.MI","EXV1.DE",
    "EXX5.DE","EXXW.DE","FGEQ.MI","GOM.MI","HYLD.MI","IBTE.MI",
    "IBTM.MI","IDVY.MI","IEAC.MI","IEAG.MI","IFFF.MI","IGLO.MI",
    "IHYU.MI","IPRP.MI","IS3N.MI","ISPA.DE","IU0E.MI","IUIT.MI",
    "IUSA.MI","IWDP.MI","JNHD.MI","JPNH.MI","L2SP.MI","MEUD.MI",
    "MLAY.MI","NTSG.MI","NTSX.MI","NTSZ.MI","PHAU.MI","QNTM.MI",
    "QQQ3.MI","RARE.MI","REET.MI","SEMB.MI","SILVER.MI","SMART.MI",
    "SMH.MI","STHE.MI","SWDA.MI","SXRM.MI","SXRV.MI","TDIV.MI",
    "UC44.MI","VAGE.MI","VAPX.MI","VHYL.MI","VUSA.MI","VWCE.DE",
    "WENT.MI","WHCS.MI","WRTY.MI","WS5X.MI","WSPE.MI","WSPX.MI",
    "WWRD.MI","XAIX.MI","XDWT.MI","XEOD.MI","XEON.MI","XREA.MI",
    "XUCS.MI","XUTC.MI",
]))

ETF_CATEGORIA = {
    "XEON.MI":"monetario","SMART.MI":"monetario","IU0E.MI":"monetario",
    "XEOD.MI":"monetario","ERNX.MI":"monetario",
    "IEAC.MI":"obbligaz_ig","IEAG.MI":"obbligaz_ig","IBTM.MI":"obbligaz_ig",
    "GOM.MI":"obbligaz_ig","AGGH.MI":"obbligaz_ig","IGLO.MI":"obbligaz_ig",
    "SXRM.MI":"obbligaz_ig","SXRV.MI":"obbligaz_ig","VAGE.MI":"obbligaz_ig",
    "EUNH.MI":"obbligaz_ig","IBTE.MI":"obbligaz_ig","XUTC.MI":"obbligaz_ig",
    "XUCS.MI":"obbligaz_ig","JNHD.MI":"obbligaz_ig","FGEQ.MI":"obbligaz_ig",
    "2B70.MI":"obbligaz_ig",
    "IHYU.MI":"hy","EUHI.MI":"hy","HYLD.MI":"hy","STHE.MI":"hy",
    "DHYA.MI":"hy","EUHA.MI":"hy",
    "EMBE.MI":"em_bond","EMDV.MI":"em_bond","SEMB.MI":"em_bond",
    "SWDA.MI":"az_globale","VWCE.DE":"az_globale","CSPX.MI":"az_globale",
    "ESGE.MI":"az_globale","WWRD.MI":"az_globale","NTSX.MI":"az_globale",
    "NTSG.MI":"az_globale","XREA.MI":"az_globale","REET.MI":"az_globale",
    "VHYL.MI":"az_globale","TDIV.MI":"az_globale",
    "MEUD.MI":"az_europa","EXX5.DE":"az_europa","EXV1.DE":"az_europa",
    "EXXW.DE":"az_europa","ISPA.DE":"az_europa","WS5X.MI":"az_europa",
    "IDVY.MI":"az_europa","EUDV.MI":"az_europa",
    "IUSA.MI":"az_usa","VUSA.MI":"az_usa","WSPX.MI":"az_usa",
    "WSPE.MI":"az_usa","EQQQ.MI":"az_usa","WRTY.MI":"az_usa",
    "VAPX.MI":"az_em","JPNH.MI":"az_em","IS3N.MI":"az_em",
    "WENT.MI":"az_em","NTSZ.MI":"az_em",
    "SMH.MI":"tematico","XAIX.MI":"tematico","XDWT.MI":"tematico",
    "DFNS.MI":"tematico","QNTM.MI":"tematico","WHCS.MI":"tematico",
    "RARE.MI":"tematico","IPRP.MI":"tematico","IWDP.MI":"tematico",
    "IFFF.MI":"tematico","DHS.MI":"tematico","IUIT.MI":"tematico",
    "PHAU.MI":"tematico","SILVER.MI":"tematico","COPA.MI":"tematico",
    "CMOD.MI":"tematico","AIGA.MI":"tematico",
    "MLAY.MI":"multi_asset",
    "L2SP.MI":"leva","UC44.MI":"leva","2LVE.MI":"leva","2NVD.MI":"leva",
    "3USL.MI":"leva","QQQ3.MI":"leva","3EUL.MI":"leva","3NVD.MI":"leva",
}

ETF_TIPO = {
    "XEON.MI":"Acc","SMART.MI":"Acc","IU0E.MI":"Dist","XEOD.MI":"Dist",
    "ERNX.MI":"Dist","EUHA.MI":"Dist","IEAC.MI":"Dist","IEAG.MI":"Dist",
    "IBTM.MI":"Dist","GOM.MI":"Dist","AGGH.MI":"Acc","IGLO.MI":"Dist",
    "SXRM.MI":"Dist","SXRV.MI":"Dist","VAGE.MI":"Dist","EUNH.MI":"Dist",
    "IBTE.MI":"Dist","XUTC.MI":"Dist","XUCS.MI":"Acc","JNHD.MI":"Dist",
    "FGEQ.MI":"Dist","2B70.MI":"Dist","IHYU.MI":"Dist","EUHI.MI":"Dist",
    "HYLD.MI":"Dist","STHE.MI":"Dist","DHYA.MI":"Dist","EMBE.MI":"Dist",
    "EMDV.MI":"Dist","SEMB.MI":"Dist","SWDA.MI":"Acc","VWCE.DE":"Acc",
    "CSPX.MI":"Acc","ESGE.MI":"Acc","WWRD.MI":"Acc","NTSX.MI":"Acc",
    "NTSG.MI":"Acc","AIGA.MI":"Acc","XREA.MI":"Acc","REET.MI":"Acc",
    "MEUD.MI":"Acc","EXX5.DE":"Dist","EXV1.DE":"Dist","EXXW.DE":"Dist",
    "ISPA.DE":"Dist","WS5X.MI":"Acc","NTSZ.MI":"Acc","IUSA.MI":"Dist",
    "VUSA.MI":"Dist","WSPX.MI":"Acc","WSPE.MI":"Acc","EQQQ.MI":"Dist",
    "WRTY.MI":"Acc","VAPX.MI":"Dist","JPNH.MI":"Dist","IS3N.MI":"Acc",
    "WENT.MI":"Acc","SMH.MI":"Acc","XAIX.MI":"Acc","XDWT.MI":"Acc",
    "DFNS.MI":"Acc","QNTM.MI":"Acc","WHCS.MI":"Dist","RARE.MI":"Acc",
    "IPRP.MI":"Dist","IWDP.MI":"Dist","IFFF.MI":"Dist","DHS.MI":"Dist",
    "IUIT.MI":"Acc","L2SP.MI":"Acc","UC44.MI":"Acc","2LVE.MI":"Acc",
    "2NVD.MI":"Acc","3USL.MI":"Acc","QQQ3.MI":"Acc","3EUL.MI":"Acc",
    "3NVD.MI":"Acc","PHAU.MI":"Acc","SILVER.MI":"Dist","COPA.MI":"Acc",
    "CMOD.MI":"Acc","VHYL.MI":"Dist","IDVY.MI":"Dist","EUDV.MI":"Dist",
    "TDIV.MI":"Dist","MLAY.MI":"Dist",
}

ETF_NOMI = {
    "MLAY.MI":"IncomeShares 60/30/10 Multi-Asset Balanced ETP",
    "SWDA.MI":"iShares Core MSCI World UCITS ETF Acc",
    "VWCE.DE":"Vanguard FTSE All-World UCITS ETF Acc",
    "CSPX.MI":"iShares Core S&P 500 UCITS ETF USD Acc",
    "VHYL.MI":"Vanguard FTSE All-World High Dividend Yield",
    "TDIV.MI":"VanEck Developed Markets Dividend Leaders",
    "IDVY.MI":"iShares Euro Dividend UCITS ETF",
    "EUDV.MI":"SPDR S&P Euro Dividend Aristocrats",
    "FGEQ.MI":"Fidelity Global Quality Income UCITS ETF",
    "PHAU.MI":"WisdomTree Physical Gold",
    "SILVER.MI":"WisdomTree Physical Silver",
    "COPA.MI":"WisdomTree Copper",
    "CMOD.MI":"iShares Diversified Commodity Swap",
    "XEON.MI":"Xtrackers II EUR Overnight Rate Swap Acc",
    "SMART.MI":"iShares EUR Ultrashort Bond UCITS ETF",
    "XEOD.MI":"Xtrackers II EUR Overnight Rate Swap Dist",
    "IU0E.MI":"iShares EUR Ultrashort Bond UCITS ETF Dist",
    "ERNX.MI":"Amundi EUR Overnight Return UCITS ETF Dist",
    "IEAC.MI":"iShares Core EUR Corp Bond UCITS ETF",
    "IEAG.MI":"iShares Core EUR Aggregate Bond UCITS ETF",
    "EUNH.MI":"iShares Core EUR Govt Bond UCITS ETF",
    "IBTE.MI":"iShares EUR Govt Bond 1-3yr UCITS ETF",
    "IBTM.MI":"iShares EUR Govt Bond 3-7yr UCITS ETF",
    "AGGH.MI":"iShares Core Global Aggregate Bond UCITS ETF",
    "IGLO.MI":"iShares Global Govt Bond UCITS ETF",
    "VAGE.MI":"Vanguard EUR Aggregate Bond UCITS ETF",
    "SXRM.MI":"iShares EUR Corp Bond 1-5yr UCITS ETF",
    "SXRV.MI":"iShares EUR Govt Bond 1-5yr UCITS ETF",
    "EUNH.MI":"iShares Core EUR Govt Bond UCITS ETF",
    "XUCS.MI":"Xtrackers USD Corporate Bond UCITS ETF",
    "XUTC.MI":"Xtrackers II EUR Corporate Bond UCITS ETF",
    "GOM.MI":"Xtrackers II Global Govt Bond EUR Hedged",
    "JNHD.MI":"JPMorgan EUR Corporate Bond Research Enhanced",
    "2B70.MI":"iShares EUR Govt Bond 7-10yr UCITS ETF",
    "IHYU.MI":"iShares USD High Yield Corp Bond EUR Hedged",
    "EUHI.MI":"PIMCO Euro Short-Term High Yield Corporate Bond",
    "HYLD.MI":"iShares EUR High Yield Corp Bond UCITS ETF",
    "STHE.MI":"SPDR Bloomberg 0-3Y EUR HY Corp Bond",
    "DHYA.MI":"iShares EUR High Yield Corp Bond Climate",
    "EUHA.MI":"iShares EUR High Yield Corp Bond UCITS ETF",
    "EMBE.MI":"iShares JPM EM Bond EUR Hedged UCITS ETF",
    "EMDV.MI":"iShares JPM EM Local Govt Bond UCITS ETF",
    "SEMB.MI":"iShares JPM EM Bond UCITS ETF",
    "MEUD.MI":"SPDR MSCI Europe UCITS ETF",
    "EXX5.DE":"iShares Core EURO STOXX 50 UCITS ETF",
    "EXV1.DE":"iShares STOXX Europe 600 UCITS ETF",
    "EXXW.DE":"iShares MSCI Europe UCITS ETF",
    "ISPA.DE":"iShares STOXX Europe Select Dividend 30",
    "WS5X.MI":"WisdomTree EURO STOXX 50",
    "IUSA.MI":"iShares Core S&P 500 UCITS ETF Dist",
    "VUSA.MI":"Vanguard S&P 500 UCITS ETF Dist",
    "WSPX.MI":"WisdomTree S&P 500",
    "WSPE.MI":"WisdomTree S&P 500 EUR Hedged",
    "EQQQ.MI":"Invesco EQQQ Nasdaq-100 UCITS ETF",
    "WRTY.MI":"WisdomTree Russell 2000 UCITS ETF",
    "VAPX.MI":"Vanguard FTSE Developed Asia Pacific ex Japan",
    "JPNH.MI":"Amundi MSCI Japan UCITS ETF EUR Hedged",
    "IS3N.MI":"iShares MSCI EM Small Cap UCITS ETF",
    "WENT.MI":"WisdomTree Energy Transition Metals",
    "NTSZ.MI":"WisdomTree Eurozone Efficient Core UCITS ETF",
    "SMH.MI":"VanEck Semiconductor UCITS ETF",
    "XAIX.MI":"Xtrackers Artificial Intelligence & Big Data",
    "XDWT.MI":"Xtrackers MSCI World Swap UCITS ETF",
    "DFNS.MI":"VanEck Defense UCITS ETF",
    "QNTM.MI":"VanEck Quantum Computing UCITS ETF",
    "WHCS.MI":"WisdomTree Healthcare Innovation UCITS ETF",
    "RARE.MI":"VanEck Rare Earth & Strategic Metals",
    "IPRP.MI":"iShares European Property Yield UCITS ETF",
    "IWDP.MI":"iShares Developed Markets Property Yield",
    "IFFF.MI":"iShares MSCI Global Financials UCITS ETF",
    "DHS.MI":"WisdomTree US Equity Income UCITS ETF",
    "IUIT.MI":"iShares S&P 500 Information Technology",
    "ESGE.MI":"iShares MSCI World ESG Enhanced UCITS ETF",
    "WWRD.MI":"WisdomTree World",
    "NTSX.MI":"WisdomTree US Efficient Core UCITS ETF",
    "NTSG.MI":"WisdomTree Global Efficient Core UCITS ETF",
    "XREA.MI":"Xtrackers FTSE EPRA/NAREIT Dev Europe RE",
    "REET.MI":"iShares Global REIT UCITS ETF",
    "AIGA.MI":"WisdomTree Agriculture",
    "L2SP.MI":"Leverage Shares 2x S&P 500 ETP",
    "UC44.MI":"Leverage Shares 2x EUR USD ETP",
    "2LVE.MI":"Leverage Shares 2x EURO STOXX 50 ETP",
    "2NVD.MI":"Leverage Shares 2x NVIDIA ETP",
    "3USL.MI":"WisdomTree S&P 500 3x Daily Leveraged",
    "QQQ3.MI":"WisdomTree NASDAQ-100 3x Daily Leveraged",
    "3EUL.MI":"WisdomTree EURO STOXX 50 3x Daily Leveraged",
    "3NVD.MI":"Leverage Shares 3x NVIDIA ETP",
}

LEVA_TICKERS = {"L2SP.MI","UC44.MI","2LVE.MI","2NVD.MI","3USL.MI",
                "QQQ3.MI","3EUL.MI","3NVD.MI"}
MONETARIO_TICKERS = {"XEON.MI","SMART.MI"}

LEVEL_WEIGHTS = {
    "C1":(25,20,5,25,25,0), "C2":(25,20,5,25,25,0), "C3":(20,20,10,25,20,5),
    "C4":(20,20,10,25,20,5), "C5":(20,18,15,22,18,7), "C6":(18,18,18,22,17,7),
    "C7":(15,18,22,22,15,8), "C8":(12,18,28,22,12,8), "C9":(10,18,35,22,10,5),
    "A1":(25,20,5,25,25,0), "A2":(25,20,5,25,25,0), "A3":(20,20,10,25,20,5),
    "A4":(20,20,10,25,20,5), "A5":(18,18,18,22,17,7), "A6":(15,18,22,22,15,8),
    "A7":(12,18,28,22,12,8), "A8":(10,18,33,22,10,7), "A9":(8,18,38,22,8,6),
}

# ── YAHOO FINANCE ───────────────────────────────────────────────────────────
def fetch_yahoo(ticker, period="2y"):
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?interval=1d&range={period}")
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode())
        result = data["chart"]["result"]
        if not result: return None
        r0 = result[0]
        timestamps = r0.get("timestamp", [])
        closes = r0["indicators"]["quote"][0].get("close", [])
        closes = [c for c in closes if c is not None]
        if len(closes) < 10: return None
        # Yield
        yield_pct = 0.0
        try:
            sd = r0.get("summaryDetail", {})
            raw_y = sd.get("trailingAnnualDividendYield", {})
            if isinstance(raw_y, dict): raw_y = raw_y.get("raw", 0)
            yield_pct = round(float(raw_y or 0) * 100, 2)
        except Exception:
            pass
        # Date
        dates = []
        try:
            for ts in timestamps:
                if ts: dates.append(datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d"))
        except Exception:
            pass
        return {"closes": closes, "dates": dates, "yield_pct": yield_pct,
                "current_price": closes[-1]}
    except Exception as e:
        print(f"  ERR {ticker}: {e}")
        return None

# ── INDICATORI TECNICI ──────────────────────────────────────────────────────
def calc_sma(closes, period=200):
    if len(closes) < period: return None
    return round(sum(closes[-period:]) / period, 4)

def calc_kama(closes, period=10, fast=2, slow=30):
    if len(closes) < period + 1: return None
    fast_sc = 2 / (fast + 1); slow_sc = 2 / (slow + 1)
    kama = closes[period]
    for i in range(period + 1, len(closes)):
        direction = abs(closes[i] - closes[i - period])
        volatility = sum(abs(closes[j] - closes[j-1]) for j in range(i-period+1, i+1))
        er = direction / volatility if volatility else 0
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
        kama = kama + sc * (closes[i] - kama)
    return round(kama, 4)

def calc_momentum(closes, days):
    if len(closes) < days + 1: return None
    old = closes[-(days+1)]
    return round((closes[-1] - old) / old * 100, 2) if old else None

def calc_ret_1d(closes):
    if len(closes) < 2 or not closes[-2]: return None
    return round((closes[-1] - closes[-2]) / closes[-2] * 100, 3)

def calc_perf_1a(closes):
    if len(closes) < 253: return None
    old = closes[-253]
    return round((closes[-1] - old) / old * 100, 2) if old else None

def calc_score_for_level(sig, level_id):
    if not sig: return 0
    w = LEVEL_WEIGHTS.get(level_id, (20,20,15,25,15,5))
    w_sma, w_kama, w_m1, w_m3, w_m6, w_y = w
    s = 0
    price = sig.get("price"); sma = sig.get("sma200"); kama = sig.get("kama")
    if price and sma: s += w_sma if price > sma else 0
    if price and kama: s += w_kama if price > kama else 0
    m1 = sig.get("mom1m")
    if m1 is not None: s += round(max(0, min(w_m1, (m1/30)*w_m1)))
    m3 = sig.get("mom3m")
    if m3 is not None: s += round(max(0, min(w_m3, (m3/50)*w_m3)))
    m6 = sig.get("mom6m")
    if m6 is not None: s += round(max(0, min(w_m6, (m6/80)*w_m6)))
    y = sig.get("yield_pct", 0) or 0
    if y > 0 and w_y > 0: s += round(max(0, min(w_y, (y/8)*w_y)))
    return min(100, max(0, s))

def score_to_signal(score, price, sma200):
    above = price > sma200 if (price and sma200) else False
    if score >= 70 and above: return "BUY"
    if score >= 45 and above: return "HOLD"
    if score >= 25: return "WATCH"
    return "SELL"

# ── CLASSIFICATORE REGIME MACRO ─────────────────────────────────────────────
def classify_regime(proxy_data):
    """
    Classifica il regime macro dai 15 ETF proxy USA.
    Combina:
      1. Momentum 4W normalizzato × affinità scenario
      2. Segnali cross-asset (SPY vs TLT, GLD, VXX, HYG/LQD, UUP, EEM, TIP)
    Restituisce: scenario, confidence, scores, geo_signal
    """
    scores = {s: 0.0 for s in SCENARI}

    # 1. Momentum ETF proxy × affinità
    mom4w = {}
    for t, d in proxy_data.items():
        if t in ETF_PROXY and d and d.get("closes"):
            closes = d["closes"]
            if len(closes) >= 22:
                old = closes[-22]; now = closes[-1]
                if old: mom4w[t] = (now - old) / old * 100

    if mom4w:
        vals = list(mom4w.values())
        vmin, vmax = min(vals), max(vals)
        rng = vmax - vmin if vmax != vmin else 1
        for t, r in mom4w.items():
            norm = (r - vmin) / rng * 2 - 1  # normalizza -1 a +1
            for sc in SCENARI:
                affinity = ETF_PROXY[t].get(sc, 0)
                scores[sc] += norm * affinity * 15

    def get_ret4w(t):
        d = proxy_data.get(t)
        if not d or not d.get("closes"): return None
        c = d["closes"]
        if len(c) < 22: return None
        return (c[-1] - c[-22]) / c[-22] * 100 if c[-22] else None

    # 2. Cross-asset signals
    spy = get_ret4w("SPY"); tlt = get_ret4w("TLT"); gld = get_ret4w("GLD")
    vxx = get_ret4w("VXX"); hyg = get_ret4w("HYG"); lqd = get_ret4w("LQD")
    uup = get_ret4w("UUP"); eem = get_ret4w("EEM"); tip = get_ret4w("TIP")
    uso = get_ret4w("USO")

    # Equity vs Bond
    if spy is not None and tlt is not None:
        diff = spy - tlt
        if diff > 5:
            scores["goldilocks"] += 20; scores["reflazione"] += 10
        elif diff < -5:
            scores["risk_off"] += 20; scores["neutro"] += 10

    # Oro forte → inflazione/paura
    if gld is not None:
        if gld > 5:
            scores["reflazione"] += 15; scores["stagflazione"] += 10; scores["risk_off"] += 8
        elif gld < -3:
            scores["goldilocks"] += 10

    # Petrolio forte → reflazione/stagflazione
    if uso is not None:
        if uso > 8:
            scores["reflazione"] += 12; scores["stagflazione"] += 8
        elif uso < -8:
            scores["risk_off"] += 8

    # VXX forte → risk_off
    if vxx is not None:
        if vxx > 15:
            scores["risk_off"] += 25; scores["stagflazione"] += 5
        elif vxx < -10:
            scores["goldilocks"] += 15

    # HYG vs LQD → credit stress
    if hyg is not None and lqd is not None:
        diff = hyg - lqd
        if diff > 3:
            scores["goldilocks"] += 12; scores["reflazione"] += 8
        elif diff < -3:
            scores["risk_off"] += 15; scores["stagflazione"] += 8

    # Dollar forte → tightening → risk_off
    if uup is not None:
        if uup > 3:
            scores["risk_off"] += 10; scores["stagflazione"] += 8
        elif uup < -2:
            scores["reflazione"] += 10; scores["goldilocks"] += 5

    # EM vs USA → crescita globale
    if eem is not None and spy is not None:
        diff = eem - spy
        if diff > 3:
            scores["reflazione"] += 12
        elif diff < -5:
            scores["risk_off"] += 8; scores["stagflazione"] += 5

    # TIPS vs TLT → inflazione attesa
    if tip is not None and tlt is not None:
        diff = tip - tlt
        if diff > 2:
            scores["reflazione"] += 12; scores["stagflazione"] += 8
        elif diff < -2:
            scores["goldilocks"] += 10; scores["risk_off"] += 5

    # Normalizza a 100
    total = sum(scores.values())
    if total <= 0:
        norm_scores = {s: 20 for s in SCENARI}
    else:
        norm_scores = {s: round(scores[s] / total * 100, 1) for s in SCENARI}

    # Scenario dominante e confidence
    scenario = max(norm_scores, key=norm_scores.get)
    confidence = round(norm_scores[scenario])

    # Segnale geografico — momentum relativo (come raptor-signal)
    geo_signal = {"az_usa": 1.0, "az_europa": 1.0, "az_em": 1.0}
    vgk_r = get_ret4w("VGK"); spy_r = get_ret4w("SPY"); eem_r = get_ret4w("EEM")
    ewj_r = get_ret4w("EWJ")

    if spy_r is not None:
        # Europa vs USA: VGK momentum relativo a SPY
        if vgk_r is not None:
            diff_eu = vgk_r - spy_r  # positivo = Europa outperforma
            # Scala -10%/+10% → 0.5/1.5
            factor_eu = 1.0 + (diff_eu / 10) * 0.5
            geo_signal["az_europa"] = round(max(0.4, min(1.6, factor_eu)), 2)
            # USA inversamente correlato ma non speculare
            factor_usa = 1.0 - (diff_eu / 10) * 0.3
            geo_signal["az_usa"] = round(max(0.5, min(1.5, factor_usa)), 2)

        # EM vs USA: EEM momentum relativo a SPY
        if eem_r is not None:
            diff_em = eem_r - spy_r
            factor_em = 1.0 + (diff_em / 10) * 0.5
            geo_signal["az_em"] = round(max(0.3, min(1.7, factor_em)), 2)

    # In risk_off: penalizza EM e Europa indipendentemente dal momentum
    if scores.get("risk_off", 0) > 35:
        geo_signal["az_em"]    = round(geo_signal["az_em"] * 0.6, 2)
        geo_signal["az_europa"] = round(geo_signal["az_europa"] * 0.7, 2)

    return {
        "scenario": scenario,
        "confidence": confidence,
        "scores": norm_scores,
        "geo_signal": geo_signal,
        "aggiornato": datetime.date.today().isoformat(),
        "fonte": "autonomo_yahoo",
    }

def calc_pesi_override(scenario, confidence, geo_signal):
    """Applica confidence e geo_signal ai pesi_override base."""
    base = SCENARIO_PESI.get(scenario, SCENARIO_PESI["neutro"])
    neutro = SCENARIO_PESI["neutro"]
    # Scala tra neutro (confidence=50%) e pieno override (confidence=100%)
    t = max(0.0, min(1.0, (confidence - 50) / 50)) if confidence > 50 else 0.0
    pesi = {}
    for cat, v_base in base.items():
        v_neutro = neutro.get(cat, 1.0)
        pesi[cat] = round(v_neutro + t * (v_base - v_neutro), 3)
    # Applica segnale geografico
    for geo_cat, factor in geo_signal.items():
        if geo_cat in pesi:
            pesi[geo_cat] = round(pesi[geo_cat] * factor, 3)
    return pesi

# ── GESTIONE PORTAFOGLI MODELLO ─────────────────────────────────────────────
def get_price_at_date(closes, dates, target_date):
    """Restituisce il prezzo più vicino a target_date."""
    if not closes or not dates: return None
    target = str(target_date)
    for i, d in enumerate(dates):
        if d >= target:
            return closes[i]
    return closes[-1] if closes else None

def calc_total_return(price_entry, price_now, yield_pct, days):
    """Total return = price return + yield pro-rata."""
    if not price_entry or price_entry == 0: return 0.0
    price_ret = (price_now - price_entry) / price_entry * 100
    yield_ret = (yield_pct / 365) * days if yield_pct else 0.0
    return round(price_ret + yield_ret, 3)

def genera_composizione(livello_id, etf_data, alloc, n_etf_max, regime, pesi_override):
    """Genera composizione portafoglio per un livello al regime corrente."""
    lv_json = None
    try:
        with open(LEVELS_FILE) as f:
            levels = json.load(f)["levels"]
        lv_json = next((l for l in levels if l["id"] == livello_id), None)
    except Exception:
        pass

    pool = lv_json.get("etf_pool", []) if lv_json else []
    alloc_target = lv_json.get("alloc", alloc) if lv_json else alloc
    isDist = livello_id.startswith("C")
    gruppo = LIVELLO_GRUPPO.get(livello_id, "bilanciato")

    # Candidati con score
    candidati = []
    for t in pool:
        sig = etf_data.get(t)
        if not sig or not sig.get("price"): continue
        score = sig.get("score_by_level", {}).get(livello_id, sig.get("score", 0))
        if score <= 0: continue
        cat = ETF_CATEGORIA.get(t, "az_globale")
        peso_macro = pesi_override.get(cat, 1.0)
        if peso_macro <= 0: continue  # escluso dal regime
        # Boost tipo (Dist per C, Acc per A)
        tipo = ETF_TIPO.get(t, "Acc")
        boost = 10 if (isDist and tipo == "Dist") or (not isDist and tipo == "Acc") else 0
        score_eff = min(100, score * peso_macro + boost)
        candidati.append({"ticker": t, "score_eff": score_eff, "cat": cat,
                           "sig": sig, "tipo": tipo})

    # Ordina per score effettivo
    candidati.sort(key=lambda x: x["score_eff"], reverse=True)

    # Seleziona rispettando max 1 leva, max 2 per categoria
    selected = []; leva_n = 0; cat_count = {}
    for c in candidati:
        if len(selected) >= n_etf_max: break
        if c["ticker"] in LEVA_TICKERS:
            if leva_n >= 1: continue
            leva_n += 1
        else:
            cn = cat_count.get(c["cat"], 0)
            if cn >= 2: continue
            cat_count[c["cat"]] = cn + 1
        selected.append(c)

    if not selected: return []

    # Pesi proporzionali allo score dentro ogni categoria
    by_cat = {}
    for c in selected:
        by_cat.setdefault(c["cat"], []).append(c)

    pesi = {}
    for cat, items in by_cat.items():
        quota = alloc_target.get(cat, 0)
        if not quota: quota = 100 / len(selected)
        tot_score = sum(i["score_eff"] for i in items)
        for i in items:
            pesi[i["ticker"]] = quota * (i["score_eff"] / tot_score) if tot_score else quota / len(items)

    # Normalizza a 100
    tot = sum(pesi.values())
    if tot > 0:
        for t in pesi: pesi[t] = round(pesi[t] / tot * 100, 1)

    # Aggiusta arrotondamento
    comp = []
    for c in selected:
        t = c["ticker"]
        p = pesi.get(t, 0)
        price = c["sig"].get("price", 0)
        importo = round(CAPITALE_MODELLO * p / 100, 2)
        quote = round(importo / price, 4) if price else 0
        comp.append({
            "ticker": t,
            "nome": ETF_NOMI.get(t, t),
            "peso": p,
            "importo": importo,
            "price_entry": round(price, 4),
            "quote": quote,
            "yield_pct": c["sig"].get("yield_pct", 0),
        })

    # Aggiusta a 100%
    tot_p = sum(c["peso"] for c in comp)
    diff = round(100 - tot_p, 1)
    if diff != 0 and comp:
        comp[0]["peso"] = round(comp[0]["peso"] + diff, 1)
        comp[0]["importo"] = round(CAPITALE_MODELLO * comp[0]["peso"] / 100, 2)

    return comp

def trova_sostituto(ticker_uscito, composizione, etf_data, livello_id, pesi_override):
    """Trova sostituto per ETF uscito. Stessa categoria → altra cat → monetario."""
    cat_uscita = ETF_CATEGORIA.get(ticker_uscito, "az_globale")
    peso_libero = next((c["peso"] for c in composizione if c["ticker"] == ticker_uscito), 0)
    importo_libero = round(CAPITALE_MODELLO * peso_libero / 100, 2)

    # Ticker già in portafoglio
    in_ptf = {c["ticker"] for c in composizione}

    try:
        with open(LEVELS_FILE) as f:
            levels = json.load(f)["levels"]
        lv_json = next((l for l in levels if l["id"] == livello_id), None)
        pool = lv_json.get("etf_pool", []) if lv_json else []
    except Exception:
        pool = []

    # Candidati nella stessa categoria
    candidati = []
    for t in pool:
        if t in in_ptf or t == ticker_uscito: continue
        sig = etf_data.get(t)
        if not sig or not sig.get("price"): continue
        if ETF_CATEGORIA.get(t) != cat_uscita: continue
        score = sig.get("score_by_level", {}).get(livello_id, sig.get("score", 0))
        segnale = sig.get("segnale", "")
        if score > 30 and segnale in ("BUY", "HOLD"):
            candidati.append((t, score))

    if candidati:
        candidati.sort(key=lambda x: x[1], reverse=True)
        t_sost = candidati[0][0]
        price = etf_data[t_sost]["price"]
        return {
            "ticker": t_sost,
            "nome": ETF_NOMI.get(t_sost, t_sost),
            "peso": peso_libero,
            "importo": importo_libero,
            "price_entry": round(price, 4),
            "quote": round(importo_libero / price, 4) if price else 0,
            "yield_pct": etf_data[t_sost].get("yield_pct", 0),
        }

    # Tentativo 2: redistribuzione proporzionale nelle categorie con BUY
    candidati_altri = []
    for t in pool:
        if t in in_ptf or t == ticker_uscito: continue
        sig = etf_data.get(t)
        if not sig or not sig.get("price"): continue
        cat_t = ETF_CATEGORIA.get(t, "az_globale")
        if cat_t == cat_uscita: continue  # già provato
        score = sig.get("score_by_level", {}).get(livello_id, sig.get("score", 0))
        segnale = sig.get("segnale", "")
        peso_macro = pesi_override.get(cat_t, 1.0) if pesi_override else 1.0
        if score > 40 and segnale in ("BUY", "HOLD") and peso_macro > 0.5:
            candidati_altri.append((t, score * peso_macro))
    if candidati_altri:
        candidati_altri.sort(key=lambda x: x[1], reverse=True)
        t_sost2 = candidati_altri[0][0]
        price2 = etf_data[t_sost2]["price"]
        return {
            "ticker": t_sost2,
            "nome": ETF_NOMI.get(t_sost2, t_sost2),
            "peso": peso_libero,
            "importo": importo_libero,
            "price_entry": round(price2, 4),
            "quote": round(importo_libero / price2, 4) if price2 else 0,
            "yield_pct": etf_data[t_sost2].get("yield_pct", 0),
        }
    # Fallback finale: monetario
    mon = "XEON.MI" if "XEON.MI" in (etf_data or {}) else "SMART.MI"
    sig_mon = etf_data.get(mon, {})
    price_mon = sig_mon.get("price", 1) or 1
    return {
        "ticker": mon,
        "nome": ETF_NOMI.get(mon, mon),
        "peso": peso_libero,
        "importo": importo_libero,
        "price_entry": round(price_mon, 4),
        "quote": round(importo_libero / price_mon, 4),
        "yield_pct": sig_mon.get("yield_pct", 0),
    }

def aggiorna_versione(versione, etf_data, oggi):
    """Aggiorna prezzi e performance di una versione attiva. Controlla trigger drawdown."""
    triggers = []
    for c in versione["composizione"]:
        t = c["ticker"]
        sig = etf_data.get(t)
        if not sig or not sig.get("price"): continue
        price_now = sig["price"]
        price_entry = c.get("price_entry", price_now)
        days = (datetime.date.fromisoformat(oggi) -
                datetime.date.fromisoformat(versione["data_apertura"])).days
        c["price_now"] = round(price_now, 4)
        c["perf_pct"] = calc_total_return(price_entry, price_now, c.get("yield_pct", 0), days)
        c["perf_eur"] = round(c["importo"] * c["perf_pct"] / 100, 2)
        dd = (price_now - price_entry) / price_entry * 100 if price_entry else 0
        c["drawdown_pct"] = round(dd, 2)

        # Check trigger drawdown
        livello_id_ver = versione.get("livello_id","")
        gruppo_ver = LIVELLO_GRUPPO.get(livello_id_ver, "bilanciato")
        if dd <= -50:
            triggers.append({"ticker": t, "tipo": "drawdown_50", "dd": round(dd,2),
                             "motivo": f"{t} drawdown {dd:.1f}% — uscita immediata"})
        elif dd <= -30:
            triggers.append({"ticker": t, "tipo": "drawdown_30", "dd": round(dd,2),
                             "motivo": f"{t} drawdown {dd:.1f}% — ribilanciamento obbligatorio"})
        elif dd <= -20 and gruppo_ver == "aggressivo":
            triggers.append({"ticker": t, "tipo": "drawdown_20", "dd": round(dd,2),
                             "motivo": f"{t} drawdown {dd:.1f}% — ribilanciamento aggressivo"})
        elif dd <= -10:
            triggers.append({"ticker": t, "tipo": "drawdown_10_alert", "dd": round(dd,2),
                             "motivo": f"{t} drawdown {dd:.1f}% — alert"})

    # Performance totale versione
    tot_perf_eur = sum(c.get("perf_eur", 0) for c in versione["composizione"])
    versione["capitale_attuale"] = round(CAPITALE_MODELLO + tot_perf_eur, 2)
    versione["performance_pct"] = round(tot_perf_eur / CAPITALE_MODELLO * 100, 2)
    versione["performance_eur"] = round(tot_perf_eur, 2)
    versione["aggiornato"] = oggi
    return triggers

def check_persistenza(regime_corrente, livello_id, storia_regime):
    """Verifica se il nuovo regime è confermato per N settimane."""
    gruppo = LIVELLO_GRUPPO.get(livello_id, "bilanciato")
    pers = PERSISTENZA[gruppo]
    settimane = pers["settimane"]
    soglia = pers["soglia"]

    if not storia_regime or len(storia_regime) < settimane:
        return False

    ultimo_scenario = storia_regime[-1]["scenario"]
    if ultimo_scenario == regime_corrente["scenario"]:
        return False  # già nel regime corrente

    # Verifica ultime N settimane
    ultimi = storia_regime[-settimane:]
    stesso = sum(1 for r in ultimi if r["scenario"] == regime_corrente["scenario"])
    conf_ok = regime_corrente["confidence"] >= soglia
    return (stesso >= settimane) and conf_ok

def run_backtest(livello_id, etf_data_completo, alloc, n_etf_max,
                 regime_corrente, pesi_override):
    """
    Esegue il backtest dal 01/01/2025.
    Usa i prezzi storici degli ETF per ricostruire la storia versioni.
    Ritorna la lista delle versioni.
    """
    print(f"    Backtest {livello_id}...")

    # Genera composizione iniziale con prezzi al 01/01/2025
    comp_iniziale = []
    try:
        with open(LEVELS_FILE) as f:
            levels = json.load(f)["levels"]
        lv_json = next((l for l in levels if l["id"] == livello_id), None)
        pool = lv_json.get("etf_pool", []) if lv_json else []
        alloc_t = lv_json.get("alloc", alloc) if lv_json else alloc
    except Exception:
        pool = []; alloc_t = alloc

    # Prendi i top ETF per score base al 01/01/2025
    candidati_bt = []
    for t in pool:
        sig = etf_data_completo.get(t)
        if not sig or not sig.get("closes") or not sig.get("dates"): continue
        p = get_price_at_date(sig["closes"], sig["dates"], BACKTEST_START)
        if not p: continue
        score_bt = sig.get("score_by_level", {}).get(livello_id, sig.get("score", 30))
        cat = ETF_CATEGORIA.get(t, "az_globale")
        candidati_bt.append({"ticker": t, "cat": cat, "price_entry": round(p, 4),
                              "score": score_bt, "yield_pct": sig.get("yield_pct", 0)})

    candidati_bt.sort(key=lambda x: x["score"], reverse=True)
    selected_bt = []; leva_n = 0; cat_c = {}
    for c in candidati_bt:
        if len(selected_bt) >= n_etf_max: break
        if c["ticker"] in LEVA_TICKERS:
            if leva_n >= 1: continue
            leva_n += 1
        else:
            cn = cat_c.get(c["cat"], 0)
            if cn >= 2: continue
            cat_c[c["cat"]] = cn + 1
        selected_bt.append(c)

    if not selected_bt:
        return []

    # Calcola pesi proporzionali
    by_cat = {}
    for c in selected_bt: by_cat.setdefault(c["cat"], []).append(c)
    pesi_bt = {}
    for cat, items in by_cat.items():
        quota = alloc_t.get(cat, 100/len(selected_bt))
        tot_s = sum(i["score"] for i in items)
        for i in items:
            pesi_bt[i["ticker"]] = quota * (i["score"] / tot_s) if tot_s else quota / len(items)

    tot_p = sum(pesi_bt.values())
    comp_iniziale = []
    for c in selected_bt:
        t = c["ticker"]
        p = round(pesi_bt.get(t, 0) / tot_p * 100, 1) if tot_p else 0
        imp = round(CAPITALE_MODELLO * p / 100, 2)
        comp_iniziale.append({
            "ticker": t,
            "nome": ETF_NOMI.get(t, t),
            "peso": p,
            "importo": imp,
            "price_entry": c["price_entry"],
            "quote": round(imp / c["price_entry"], 4) if c["price_entry"] else 0,
            "yield_pct": c["yield_pct"],
        })

    # Prima versione dal 01/01/2025
    versioni = [{
        "versione": 1,
        "data_apertura": BACKTEST_START,
        "data_chiusura": None,
        "regime": "neutro",
        "confidence": 50,
        "trigger_apertura": "inizializzazione_backtest",
        "trigger_chiusura": None,
        "capitale_inizio": CAPITALE_MODELLO,
        "capitale_attuale": CAPITALE_MODELLO,
        "performance_pct": 0.0,
        "performance_eur": 0.0,
        "giorni_attivo": 0,
        "composizione": comp_iniziale,
        "etf_usciti_anticipati": [],
        "aggiornato": BACKTEST_START,
    }]

    return versioni

def gestisci_portafogli(portafogli_esistenti, etf_data, regime_corrente,
                        pesi_override, oggi, storia_regime):
    """
    Gestisce tutti i 18 portafogli modello.
    Per ogni livello:
      1. Aggiorna prezzi e performance versione attiva
      2. Controlla trigger drawdown
      3. Controlla cambio regime (con persistenza)
      4. Se trigger → chiude versione corrente e apre nuova
    """
    try:
        with open(LEVELS_FILE) as f:
            levels_data = json.load(f)["levels"]
    except Exception:
        print("  ERR: levels.json non trovato")
        return portafogli_esistenti

    portafogli = portafogli_esistenti or {}

    for lv in levels_data:
        lid = lv["id"]
        alloc = lv.get("alloc", {})
        n_max = lv.get("n_etf_max", 6)

        # Inizializza se non esiste
        if lid not in portafogli:
            print(f"    Init backtest {lid}")
            versioni = run_backtest(lid, etf_data, alloc, n_max,
                                    regime_corrente, pesi_override)
            # Aggiunge livello_id a ogni versione per il drawdown trigger
            for v in versioni:
                v["livello_id"] = lid
            portafogli[lid] = {
                "capitale_modello": CAPITALE_MODELLO,
                "versione_corrente": 1,
                "storia": versioni,
            }
            continue

        storia = portafogli[lid]["storia"]
        if not storia: continue

        # Versione attiva (ultima con data_chiusura=None)
        ver_attiva = next((v for v in reversed(storia) if v.get("data_chiusura") is None), None)
        if not ver_attiva: continue

        # 1. Aggiorna prezzi
        triggers_dd = aggiorna_versione(ver_attiva, etf_data, oggi)

        # Calcola giorni attivi
        try:
            days = (datetime.date.fromisoformat(oggi) -
                    datetime.date.fromisoformat(ver_attiva["data_apertura"])).days
            ver_attiva["giorni_attivo"] = days
        except Exception:
            pass

        # 2. Gestisci drawdown
        for trig in triggers_dd:
            t_out = trig["ticker"]
            # Rimuovi ETF dal portafoglio
            comp_before = [c for c in ver_attiva["composizione"]]
            ver_attiva["composizione"] = [c for c in ver_attiva["composizione"]
                                           if c["ticker"] != t_out]
            etf_uscito = next((c for c in comp_before if c["ticker"] == t_out), None)
            if etf_uscito:
                price_now = etf_data.get(t_out, {}).get("price", etf_uscito["price_entry"])
                perf = calc_total_return(etf_uscito["price_entry"], price_now,
                                         etf_uscito.get("yield_pct", 0), ver_attiva.get("giorni_attivo", 0))
                uscito_rec = {
                    "ticker": t_out,
                    "motivo": trig["tipo"],
                    "data": oggi,
                    "price_entry": etf_uscito["price_entry"],
                    "price_exit": round(price_now, 4),
                    "perf_pct": round(perf, 2),
                    "perf_eur": round(etf_uscito["importo"] * perf / 100, 2),
                    "drawdown_pct": trig["dd"],
                }
                ver_attiva.setdefault("etf_usciti_anticipati", []).append(uscito_rec)

                # Reinvesti (se non drawdown -50%: monetario obbligatorio)
                if trig["tipo"] == "drawdown_50":
                    sost = trova_sostituto(t_out, comp_before, etf_data, lid, pesi_override)
                    sost["ticker"] = "XEON.MI"  # forza monetario
                    sig_mon = etf_data.get("XEON.MI", {})
                    sost["price_entry"] = round(sig_mon.get("price", 1), 4)
                    sost["nome"] = ETF_NOMI.get("XEON.MI", "XEON.MI")
                else:
                    sost = trova_sostituto(t_out, comp_before, etf_data, lid, pesi_override)
                ver_attiva["composizione"].append(sost)

            # Se drawdown >= 30 su livelli aggressivi → nuova versione
            gruppo = LIVELLO_GRUPPO.get(lid, "bilanciato")
            if (trig["tipo"] in ("drawdown_30", "drawdown_50") or
                (trig["tipo"] == "drawdown_20" and gruppo == "aggressivo")):
                # Chiudi versione attiva
                ver_attiva["data_chiusura"] = oggi
                ver_attiva["trigger_chiusura"] = trig["motivo"]

                # Apri nuova versione
                n_ver = portafogli[lid]["versione_corrente"] + 1
                portafogli[lid]["versione_corrente"] = n_ver
                nuova_comp = genera_composizione(lid, etf_data, alloc, n_max,
                                                  regime_corrente["scenario"], pesi_override)
                cap_new_dd = ver_attiva.get("capitale_attuale", CAPITALE_MODELLO)
                storia.append({
                    "versione": n_ver,
                    "livello_id": lid,
                    "data_apertura": oggi,
                    "data_chiusura": None,
                    "regime": regime_corrente["scenario"],
                    "confidence": regime_corrente["confidence"],
                    "trigger_apertura": trig["motivo"],
                    "trigger_chiusura": None,
                    "capitale_inizio": cap_new_dd,
                    "capitale_attuale": cap_new_dd,
                    "performance_pct": 0.0,
                    "performance_eur": 0.0,
                    "giorni_attivo": 0,
                    "composizione": nuova_comp,
                    "etf_usciti_anticipati": [],
                    "aggiornato": oggi,
                })
                break  # una nuova versione per run

        # 3. Check cambio regime
        ver_attiva_now = next((v for v in reversed(storia)
                                if v.get("data_chiusura") is None), None)
        if not ver_attiva_now: continue

        if check_persistenza(regime_corrente, lid, storia_regime):
            regime_da = ver_attiva_now.get("regime", "neutro")
            regime_a  = regime_corrente["scenario"]
            if regime_da != regime_a:
                ver_attiva_now["data_chiusura"] = oggi
                ver_attiva_now["trigger_chiusura"] = f"cambio_regime {regime_da}→{regime_a}"

                n_ver = portafogli[lid]["versione_corrente"] + 1
                portafogli[lid]["versione_corrente"] = n_ver
                cap_new = ver_attiva_now.get("capitale_attuale", CAPITALE_MODELLO)
                nuova_comp = genera_composizione(lid, etf_data, alloc, n_max,
                                                  regime_a, pesi_override)
                storia.append({
                    "versione": n_ver,
                    "livello_id": lid,
                    "data_apertura": oggi,
                    "data_chiusura": None,
                    "regime": regime_a,
                    "confidence": regime_corrente["confidence"],
                    "trigger_apertura": f"cambio_regime {regime_da}→{regime_a}",
                    "trigger_chiusura": None,
                    "capitale_inizio": cap_new,
                    "capitale_attuale": cap_new,
                    "performance_pct": 0.0,
                    "performance_eur": 0.0,
                    "giorni_attivo": 0,
                    "composizione": nuova_comp,
                    "etf_usciti_anticipati": [],
                    "aggiornato": oggi,
                })

        # Performance totale cumulativa — capitale finale - capitale iniziale assoluto
        cap_finale = next((v.get("capitale_attuale", CAPITALE_MODELLO)
                           for v in reversed(storia)
                           if v.get("data_chiusura") is None), CAPITALE_MODELLO)
        perf_tot_eur = round(cap_finale - CAPITALE_MODELLO, 2)
        portafogli[lid]["performance_totale_eur"] = perf_tot_eur
        portafogli[lid]["performance_totale_pct"] = round(perf_tot_eur / CAPITALE_MODELLO * 100, 2)
        portafogli[lid]["capitale_attuale"] = cap_finale

    return portafogli

# ── MAIN ────────────────────────────────────────────────────────────────────
def main():
    oggi = datetime.date.today().isoformat()
    print(f"COMPASS fetch v2.0 — {oggi}")
    print(f"Universo: {len(ETF_UNIVERSE)} ETF + {len(ETF_PROXY)} proxy")

    # Carica dati esistenti
    existing = {}
    if OUT_FILE.exists():
        try:
            with open(OUT_FILE) as f:
                existing = json.load(f)
            print(f"  Dati esistenti: {existing.get('generated','?')}")
        except Exception:
            pass

    storia_regime = existing.get("storia_regime", [])
    portafogli_esistenti = existing.get("portafogli_modello", {})

    # ── 1. Scarica ETF PROXY per regime ──────────────────────────────────
    print(f"\n[1/4] Download {len(ETF_PROXY)} ETF proxy...")
    proxy_data = {}
    for t in ETF_PROXY:
        print(f"  {t}...", end=" ", flush=True)
        raw = fetch_yahoo(t, "1y")
        proxy_data[t] = raw
        print("OK" if raw else "ERR")
        time.sleep(0.3)

    # ── 2. Classifica regime ──────────────────────────────────────────────
    print("\n[2/4] Classificazione regime macro...")
    regime = classify_regime(proxy_data)
    pesi_override = calc_pesi_override(
        regime["scenario"], regime["confidence"], regime["geo_signal"])
    print(f"  Scenario: {regime['scenario']} ({regime['confidence']}%)")
    print(f"  Geo: EU={regime['geo_signal']['az_europa']} "
          f"USA={regime['geo_signal']['az_usa']} EM={regime['geo_signal']['az_em']}")

    # Aggiorna storia regime
    storia_regime = [r for r in storia_regime if r.get("data") != oggi]
    storia_regime.append({"data": oggi, "scenario": regime["scenario"],
                           "confidence": regime["confidence"]})
    storia_regime = storia_regime[-104:]  # max 2 anni

    # ── 3. Scarica ETF Compass ────────────────────────────────────────────
    print(f"\n[3/4] Download {len(ETF_UNIVERSE)} ETF Compass...")
    all_levels = list(LEVEL_WEIGHTS.keys())
    etf_data = {}
    success = 0; errors = 0

    # Riusa dati proxy già scaricati dove applicabile
    for ticker in ETF_UNIVERSE:
        print(f"  [{success+errors+1}/{len(ETF_UNIVERSE)}] {ticker}...", end=" ", flush=True)
        raw = fetch_yahoo(ticker, "2y")
        if not raw:
            errors += 1
            etf_data[ticker] = {
                "ticker": ticker, "nome": ETF_NOMI.get(ticker, ticker),
                "tipo": ETF_TIPO.get(ticker, "Acc"),
                "categoria": ETF_CATEGORIA.get(ticker, "az_globale"),
                "price": None, "ret_1d": None, "perf_1a": None, "yield_pct": 0.0,
                "sma200": None, "kama": None,
                "mom1m": None, "mom3m": None, "mom6m": None,
                "score": 0, "score_by_level": {lv: 0 for lv in all_levels},
                "segnale": "N/D", "prices_60": [], "closes": [], "dates": [],
            }
            print("ERR")
            time.sleep(0.3)
            continue

        closes = raw["closes"]
        dates  = raw.get("dates", [])
        price  = raw["current_price"]
        sma200 = calc_sma(closes, 200)
        kama   = calc_kama(closes)
        mom1m  = calc_momentum(closes, 21)
        mom3m  = calc_momentum(closes, 63)
        mom6m  = calc_momentum(closes, 126)
        ret_1d = calc_ret_1d(closes)
        perf1a = calc_perf_1a(closes)

        sig_base = {"price": price, "sma200": sma200, "kama": kama,
                    "mom1m": mom1m, "mom3m": mom3m, "mom6m": mom6m,
                    "yield_pct": raw["yield_pct"]}
        score_by_level = {lv: calc_score_for_level(sig_base, lv) for lv in all_levels}
        score_base = round((score_by_level["C5"] + score_by_level["A5"]) / 2)
        segnale = score_to_signal(score_base, price, sma200)

        etf_data[ticker] = {
            "ticker": ticker,
            "nome": ETF_NOMI.get(ticker, ticker),
            "tipo": ETF_TIPO.get(ticker, "Acc"),
            "categoria": ETF_CATEGORIA.get(ticker, "az_globale"),
            "price": round(price, 4),
            "ret_1d": ret_1d,
            "perf_1a": perf1a,
            "yield_pct": raw["yield_pct"],
            "sma200": sma200,
            "kama": kama,
            "mom1m": mom1m, "mom3m": mom3m, "mom6m": mom6m,
            "score": score_base,
            "score_by_level": score_by_level,
            "segnale": segnale,
            "prices_60": [round(c, 4) for c in closes[-60:]],
            "closes": [round(c, 4) for c in closes],
            "dates": dates,
        }
        success += 1
        print(f"OK score={score_base} {segnale}")
        time.sleep(0.4)

    # ── 4. Gestisci portafogli modello ────────────────────────────────────
    print(f"\n[4/4] Portafogli modello (backtest da {BACKTEST_START})...")
    portafogli = gestisci_portafogli(
        portafogli_esistenti, etf_data, regime,
        pesi_override, oggi, storia_regime)

    # ── Output ────────────────────────────────────────────────────────────
    # Rimuovi closes completi dall'output finale (troppo grandi)
    etf_out = {}
    for t, d in etf_data.items():
        etf_out[t] = {k: v for k, v in d.items() if k not in ("closes", "dates")}

    output = {
        "generated": datetime.datetime.utcnow().isoformat(),
        "version": "2.0",
        "total": len(ETF_UNIVERSE),
        "errors": errors,
        "success": success,
        "regime_macro": {
            **regime,
            "pesi_override": pesi_override,
        },
        "storia_regime": storia_regime,
        "portafogli_modello": portafogli,
        "etfs": etf_out,
    }

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

    size = OUT_FILE.stat().st_size / 1024
    print(f"\n✅ Done: {success} OK, {errors} errori → {OUT_FILE} ({size:.0f} KB)")
    print(f"   Regime: {regime['scenario']} ({regime['confidence']}%)")
    print(f"   Portafogli: {len(portafogli)} livelli")

if __name__ == "__main__":
    main()
