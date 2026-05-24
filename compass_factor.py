#!/usr/bin/env python3
"""
COMPASS — compass_factor.py v1.0
══════════════════════════════════
Strategia Factor Momentum — livelli C7/C8/C9 e A7/A8/A9
Obiettivo: battere IWMO.MI (iShares MSCI World Momentum Factor)

Differenze rispetto a compass_fetch.py:
  1. Score riformulato: momentum domina (mom3M 35%), RSI/ADX/AO integrati
  2. Rotation trigger: sostituisce ETF se candidato pool ha mom3M +15pt
  3. Regime calcolato dai dati reali giorno per giorno (no hardcoded)

Output: data/compass_factor.json
"""

import json, math, datetime, time, urllib.request
from pathlib import Path

# ── CONFIGURAZIONE ─────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
LEVELS_FILE = BASE_DIR / "data" / "levels.json"
OUT_FILE    = BASE_DIR / "data" / "compass_factor.json"
BACKTEST_START   = "2025-01-01"
CAPITALE_MODELLO = 100_000
BENCHMARK_TARGET = "IWMO.MI"

# Livelli gestiti da questa strategia
LIVELLI_FACTOR = ["C7", "C8", "C9", "A7", "A8", "A9"]

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

SCENARIO_PESI = {
    "goldilocks" : {"monetario":0.5,"obbligaz_ig":0.7,"hy":1.1,"em_bond":0.9,"az_globale":1.3,"az_europa":1.1,"az_usa":1.3,"az_em":1.2,"tematico":0.8,"leva":1.2,"multi_asset":1.1},
    "reflazione" : {"monetario":0.4,"obbligaz_ig":0.6,"hy":1.2,"em_bond":1.2,"az_globale":1.1,"az_europa":1.0,"az_usa":1.0,"az_em":1.3,"tematico":1.5,"leva":0.8,"multi_asset":1.0},
    "stagflazione":{"monetario":1.5,"obbligaz_ig":0.7,"hy":0.5,"em_bond":0.5,"az_globale":0.3,"az_europa":0.3,"az_usa":0.3,"az_em":0.2,"tematico":1.8,"leva":0.1,"multi_asset":0.5},
    "risk_off"   : {"monetario":2.0,"obbligaz_ig":1.5,"hy":0.3,"em_bond":0.3,"az_globale":0.3,"az_europa":0.3,"az_usa":0.4,"az_em":0.2,"tematico":0.6,"leva":0.0,"multi_asset":0.4},
    "neutro"     : {"monetario":1.0,"obbligaz_ig":1.0,"hy":1.0,"em_bond":1.0,"az_globale":1.0,"az_europa":1.0,"az_usa":1.0,"az_em":1.0,"tematico":1.0,"leva":1.0,"multi_asset":1.0},
}

TICKER_ALIAS = {
    "AGGH.DE":"EUNA.DE","IGLO.DE":"EUN3.DE","SEMB.DE":"IEMB.MI",
    "EUHA.MI":"EUHA.DE","IUIT.MI":"IUIT.L","SEMB.MI":"IEMB.MI",
    "FGEQ.MI":"FGEQ.F","IFSW.MI":"IBCZ.DE","IS07.MI":"IS07.DE",
    "FCRN.MI":"FCRN.DE","JNHD.MI":"JNHD.DE",
}

ETF_CATEGORIA = {
    "XEON.MI":"monetario","SMART.MI":"monetario","IU0E.MI":"monetario","XEOD.DE":"monetario",
    "IEAC.MI":"obbligaz_ig","IEAG.MI":"obbligaz_ig","IBTM.MI":"obbligaz_ig",
    "AGGH.DE":"obbligaz_ig","IGLO.DE":"obbligaz_ig","SEGA.MI":"obbligaz_ig",
    "VAGE.DE":"obbligaz_ig","EUNH.DE":"obbligaz_ig","IBGS.MI":"obbligaz_ig",
    "XUCS.DE":"obbligaz_ig","JNHD.DE":"obbligaz_ig","FGEQ.F":"obbligaz_ig",
    "IBGM.MI":"obbligaz_ig","XGSH.MI":"obbligaz_ig",
    "IHYU.MI":"hy","EUHI.MI":"hy","HYLD.MI":"hy","STHE.MI":"hy","EUHA.MI":"hy","EUHA.DE":"hy",
    "EMBE.MI":"em_bond","EMDV.MI":"em_bond","SEMB.DE":"em_bond","IEMB.MI":"em_bond",
    "SWDA.MI":"az_globale","VWCE.DE":"az_globale","CSSPX.MI":"az_globale",
    "ESGE.MI":"az_globale","WWRD.MI":"az_globale","NTSX.MI":"az_globale",
    "NTSG.MI":"az_globale","XREA.DE":"az_globale","VHYL.MI":"az_globale",
    "TDIV.MI":"az_globale","IWMO.MI":"az_globale","JPGL.MI":"az_globale",
    "IBCZ.DE":"az_globale","IS07.DE":"az_globale","FCRN.DE":"az_globale",
    "XDWT.MI":"az_globale",
    "MEUD.MI":"az_europa","EXX5.DE":"az_europa","EXV1.DE":"az_europa",
    "EXXW.DE":"az_europa","ISPA.DE":"az_europa","WS5X.MI":"az_europa",
    "IDVY.MI":"az_europa","EUDV.MI":"az_europa",
    "IUSA.MI":"az_usa","VUSA.MI":"az_usa","WSPX.MI":"az_usa",
    "WSPE.MI":"az_usa","EQQQ.MI":"az_usa","WRTY.MI":"az_usa",
    "VAPX.MI":"az_em","JPNH.MI":"az_em","IS3N.DE":"az_em",
    "WENT.MI":"az_em","NTSZ.MI":"az_em",
    "SMH.MI":"tematico","XAIX.MI":"tematico","XDWT.MI":"tematico",
    "DFNS.MI":"tematico","QNTM.MI":"tematico","WHCS.MI":"tematico",
    "RARE.MI":"tematico","IPRP.MI":"tematico","IWDP.MI":"tematico",
    "IFFF.MI":"tematico","DHS.MI":"tematico","IUIT.L":"tematico",
    "PHAU.MI":"tematico","PHAG.MI":"tematico","COPA.MI":"tematico",
    "CMOD.MI":"tematico","AIGA.MI":"tematico","XUTC.MI":"tematico",
    "3USL.MI":"leva","QQQ3.MI":"leva","3EUL.MI":"leva","3NVD.MI":"leva",
    "MACV.MI":"multi_asset","MODR.MI":"multi_asset","MAGR.MI":"multi_asset",
    "V20A.DE":"multi_asset","V40A.DE":"multi_asset","V60A.DE":"multi_asset","V80A.DE":"multi_asset",
}

ETF_TIPO = {
    "SWDA.MI":"Acc","VWCE.DE":"Acc","CSSPX.MI":"Acc","ESGE.MI":"Acc","NTSX.MI":"Acc",
    "NTSG.MI":"Acc","XREA.DE":"Acc","XAIX.MI":"Acc","DFNS.MI":"Acc","QNTM.MI":"Acc",
    "RARE.MI":"Acc","IUIT.L":"Acc","3USL.MI":"Acc","QQQ3.MI":"Acc","3EUL.MI":"Acc",
    "3NVD.MI":"Acc","PHAU.MI":"Acc","COPA.MI":"Acc","CMOD.MI":"Acc","IWMO.MI":"Acc",
    "JPGL.MI":"Acc","IBCZ.DE":"Acc","IS07.DE":"Acc","FCRN.DE":"Acc","WRTY.MI":"Acc",
    "WSPX.MI":"Acc","WSPE.MI":"Acc","WS5X.MI":"Acc","NTSZ.MI":"Acc","XDWT.MI":"Acc",
    "WWRD.MI":"Acc","XGSH.MI":"Acc","XUCS.DE":"Acc","HYLD.MI":"Acc","WENT.MI":"Acc",
    "VAPX.MI":"Dist","IDVY.MI":"Dist","EUDV.MI":"Dist","TDIV.MI":"Dist",
    "VHYL.MI":"Dist","DHS.MI":"Dist","WHCS.MI":"Dist","IPRP.MI":"Dist",
    "IWDP.MI":"Dist","IFFF.MI":"Dist","PHAG.MI":"Dist","EXX5.DE":"Dist",
    "EXV1.DE":"Dist","EXXW.DE":"Dist","ISPA.DE":"Dist","IUSA.MI":"Dist",
    "VUSA.MI":"Dist","EQQQ.MI":"Dist","MEUD.MI":"Acc","EXX5.DE":"Dist",
    "IS3N.DE":"Acc","JPNH.MI":"Dist","SMH.MI":"Acc","EUHA.DE":"Dist",
    "IHYU.MI":"Dist","EUHI.MI":"Dist","STHE.MI":"Dist","EMBE.MI":"Dist",
    "EMDV.MI":"Dist","IEAC.MI":"Dist","VAGE.DE":"Dist","XEON.MI":"Acc",
    "XEOD.DE":"Dist","XUTC.MI":"Dist",
}

ETF_NOMI = {
    "SWDA.MI":"iShares Core MSCI World","VWCE.DE":"Vanguard FTSE All-World Acc",
    "CSSPX.MI":"iShares Core S&P 500 Acc","IWMO.MI":"iShares MSCI World Momentum",
    "EQQQ.MI":"Invesco EQQQ Nasdaq-100","IUIT.L":"iShares S&P 500 IT Sector",
    "SMH.MI":"VanEck Semiconductor","XAIX.MI":"Xtrackers AI & Big Data",
    "DFNS.MI":"VanEck Defense","QNTM.MI":"VanEck Quantum Computing",
    "WHCS.MI":"WisdomTree Healthcare Innovation","RARE.MI":"VanEck Rare Earth",
    "3USL.MI":"WisdomTree S&P 500 3x Lev","QQQ3.MI":"WisdomTree Nasdaq 3x Lev",
    "3EUL.MI":"WisdomTree EuroStoxx 3x Lev","3NVD.MI":"Leverage Shares 3x NVIDIA",
    "PHAU.MI":"WisdomTree Physical Gold","PHAG.MI":"WisdomTree Physical Silver",
    "COPA.MI":"WisdomTree Copper","CMOD.MI":"iShares Diversified Commodity",
    "AIGA.MI":"WisdomTree Agriculture","JPGL.MI":"JPMorgan Global Equity Multi-Factor",
    "IBCZ.DE":"iShares STOXX World Multifactor","IS07.DE":"iShares STOXX World Multifactor EUR Hdg",
    "FCRN.DE":"iShares World Equity Factor Rotation","NTSX.MI":"WisdomTree US Efficient Core",
    "NTSG.MI":"WisdomTree Global Efficient Core","NTSZ.MI":"WisdomTree Eurozone Efficient Core",
    "WRTY.MI":"WisdomTree Russell 2000","WSPE.MI":"WisdomTree S&P 500 EUR Hdg",
    "WSPX.MI":"WisdomTree S&P 500","EXX5.DE":"iShares EURO STOXX 50",
    "EXV1.DE":"iShares STOXX Europe 600","EXXW.DE":"iShares MSCI Europe",
    "IS3N.DE":"iShares MSCI EM Small Cap","VAPX.MI":"Vanguard Dev Asia Pacific",
    "JPNH.MI":"Amundi MSCI Japan EUR Hdg","MEUD.MI":"SPDR MSCI Europe",
    "IUSA.MI":"iShares Core S&P 500 Dist","VUSA.MI":"Vanguard S&P 500 Dist",
    "VHYL.MI":"Vanguard FTSE All-World High Div","TDIV.MI":"VanEck Developed Markets Div",
    "IDVY.MI":"iShares Euro Dividend","EUDV.MI":"SPDR Euro Dividend Aristocrats",
    "DHS.MI":"WisdomTree US Equity Income","FGEQ.F":"Fidelity Global Quality Income",
    "IHYU.MI":"iShares USD High Yield EUR Hdg","EUHI.MI":"PIMCO Euro Short HY",
    "STHE.MI":"SPDR Bloomberg 0-3Y EUR HY","HYLD.MI":"iShares EUR High Yield",
    "EUHA.DE":"iShares EUR High Yield Corp","EMBE.MI":"iShares JPM EM Bond EUR Hdg",
    "EMDV.MI":"iShares JPM EM Local Govt Bond","IEAC.MI":"iShares Core EUR Corp Bond",
    "VAGE.DE":"Vanguard EUR Aggregate Bond","XEON.MI":"Amundi EUR Overnight",
    "XEOD.DE":"Xtrackers EUR Overnight Dist","XDWT.MI":"Xtrackers MSCI World Swap",
    "XUTC.MI":"Xtrackers MSCI USA IT 1D","IPRP.MI":"iShares European Property Yield",
    "IWDP.MI":"iShares Dev Markets Property","IFFF.MI":"iShares MSCI Global Financials",
    "ISPA.DE":"iShares STOXX Europe Select Div 30","WWRD.MI":"WisdomTree World",
    "WS5X.MI":"WisdomTree EURO STOXX 50","ESGE.MI":"iShares MSCI World ESG Enhanced",
    "XREA.DE":"Xtrackers FTSE EPRA/NAREIT Dev Europe",
}

LEVA_TICKERS = {"3USL.MI","QQQ3.MI","3EUL.MI","3NVD.MI"}

LIVELLO_GRUPPO = {
    "C7":"aggressivo","C8":"aggressivo","C9":"aggressivo",
    "A7":"aggressivo","A8":"aggressivo","A9":"aggressivo",
}

PERSISTENZA = {
    "aggressivo": {"settimane": 2, "soglia": 50},
}

# ── PESI SCORE FACTOR (differenziati per livello) ──────────────────────────
# Formato: (sma200, kama, mom1m, mom3m, mom6m, rsi, adx, ao)
# Totale = 100 punti
FACTOR_SCORE_WEIGHTS = {
    # C7/A7 — Dinamico: momentum forte ma con filtro trend
    "C7": {"sma200":10,"kama":10,"mom1m":15,"mom3m":35,"mom6m":20,"rsi":5,"adx":5,"ao":0},
    "A7": {"sma200":10,"kama":10,"mom1m":15,"mom3m":35,"mom6m":20,"rsi":5,"adx":5,"ao":0},
    # C8/A8 — Azionario: momentum puro, ADX conferma forza
    "C8": {"sma200":5,"kama":5,"mom1m":20,"mom3m":35,"mom6m":20,"rsi":5,"adx":10,"ao":0},
    "A8": {"sma200":5,"kama":5,"mom1m":20,"mom3m":35,"mom6m":20,"rsi":5,"adx":10,"ao":0},
    # C9/A9 — Trading: momentum ultra-aggressivo, AO per timing
    "C9": {"sma200":0,"kama":5,"mom1m":25,"mom3m":35,"mom6m":20,"rsi":5,"adx":5,"ao":5},
    "A9": {"sma200":0,"kama":5,"mom1m":25,"mom3m":35,"mom6m":20,"rsi":5,"adx":5,"ao":5},
}

# Soglia rotation trigger: se un candidato del pool ha mom3M
# superiore di questo valore a un ETF in portafoglio → rotazione
ROTATION_THRESHOLD = 15.0

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
        yield_pct = 0.0
        try:
            sd = r0.get("summaryDetail", {})
            raw_y = sd.get("trailingAnnualDividendYield", {})
            if isinstance(raw_y, dict): raw_y = raw_y.get("raw", 0)
            yield_pct = round(float(raw_y or 0) * 100, 2)
        except Exception:
            pass
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

def resolve_ticker(t):
    return TICKER_ALIAS.get(t, t)

# ── INDICATORI TECNICI ──────────────────────────────────────────────────────
def calc_sma(closes, period=200):
    if len(closes) < period: return None
    return round(sum(closes[-period:]) / period, 4)

def calc_kama(closes, period=10, fast=2, slow=30):
    if len(closes) < period + 1: return None
    fast_sc = 2/(fast+1); slow_sc = 2/(slow+1)
    kama = closes[period]
    for i in range(period+1, len(closes)):
        direction = abs(closes[i] - closes[i-period])
        volatility = sum(abs(closes[j]-closes[j-1]) for j in range(i-period+1, i+1))
        er = direction/volatility if volatility else 0
        sc = (er*(fast_sc-slow_sc)+slow_sc)**2
        kama = kama + sc*(closes[i]-kama)
    return round(kama, 4)

def calc_rsi(closes, period=14):
    if len(closes) < period+1: return None
    gains=[]; losses=[]
    for i in range(1, len(closes)):
        d = closes[i]-closes[i-1]
        gains.append(max(d,0)); losses.append(max(-d,0))
    if len(gains) < period: return None
    avg_g = sum(gains[-period:])/period
    avg_l = sum(losses[-period:])/period
    for i in range(len(gains)-period):
        avg_g = (avg_g*(period-1)+gains[i+period])/period
        avg_l = (avg_l*(period-1)+losses[i+period])/period
    if avg_l == 0: return 100.0
    return round(100 - 100/(1+avg_g/avg_l), 2)

def calc_adx(closes, period=14):
    if len(closes) < period*2+1: return None
    tr_list=[]; pdm_list=[]; ndm_list=[]
    for i in range(1, len(closes)):
        h=closes[i]; l=closes[i]; pc=closes[i-1]
        tr=max(h-l, abs(h-pc), abs(l-pc))
        pdm=max(h-closes[i-1],0) if h-closes[i-1]>closes[i-1]-l else 0
        ndm=max(closes[i-1]-l,0) if closes[i-1]-l>h-closes[i-1] else 0
        tr_list.append(tr); pdm_list.append(pdm); ndm_list.append(ndm)
    if len(tr_list) < period: return None
    atr=sum(tr_list[:period]); pdi=sum(pdm_list[:period]); ndi=sum(ndm_list[:period])
    dx_list=[]
    for i in range(period, len(tr_list)):
        atr=atr-atr/period+tr_list[i]
        pdi=pdi-pdi/period+pdm_list[i]
        ndi=ndi-ndi/period+ndm_list[i]
        pdi_p=100*pdi/atr if atr else 0
        ndi_p=100*ndi/atr if atr else 0
        denom=pdi_p+ndi_p
        dx=100*abs(pdi_p-ndi_p)/denom if denom else 0
        dx_list.append(dx)
    if len(dx_list) < period: return None
    return round(sum(dx_list[-period:])/period, 2)

def calc_ao(closes):
    if len(closes) < 34: return None
    return round(sum(closes[-5:])/5 - sum(closes[-34:])/34, 4)

def calc_momentum(closes, days):
    if len(closes) < days+1: return None
    old = closes[-(days+1)]
    return round((closes[-1]-old)/old*100, 2) if old else None

def calc_volatilita(equity_curve):
    if len(equity_curve) < 10: return None
    rend=[]
    for i in range(1, len(equity_curve)):
        if equity_curve[i-1] > 0:
            rend.append((equity_curve[i]-equity_curve[i-1])/equity_curve[i-1])
    if len(rend) < 5: return None
    media=sum(rend)/len(rend)
    var=sum((r-media)**2 for r in rend)/len(rend)
    return round(var**0.5*(252**0.5)*100, 2)

def calc_max_drawdown(equity_curve):
    if len(equity_curve) < 2: return None
    peak=equity_curve[0]; max_dd=0.0
    for v in equity_curve:
        if v > peak: peak=v
        dd=(v-peak)/peak*100 if peak > 0 else 0
        if dd < max_dd: max_dd=dd
    return round(max_dd, 2)

def calc_sharpe(equity_curve, risk_free=0.03):
    if len(equity_curve) < 10: return None
    rend=[]
    for i in range(1, len(equity_curve)):
        if equity_curve[i-1] > 0:
            rend.append((equity_curve[i]-equity_curve[i-1])/equity_curve[i-1])
    if len(rend) < 5: return None
    media=sum(rend)/len(rend)
    var=sum((r-media)**2 for r in rend)/len(rend)
    std=var**0.5
    if std == 0: return None
    return round((media-risk_free/252)/std*(252**0.5), 2)

# ── SCORE FACTOR ────────────────────────────────────────────────────────────
def calc_factor_score(closes, level_id, yield_pct=0):
    """
    Score factor momentum differenziato per livello.
    Integra: SMA200, KAMA, mom1M, mom3M, mom6M, RSI, ADX, AO
    """
    if not closes or len(closes) < 35: return 0
    w = FACTOR_SCORE_WEIGHTS.get(level_id,
        {"sma200":10,"kama":10,"mom1m":15,"mom3m":35,"mom6m":20,"rsi":5,"adx":5,"ao":0})

    price  = closes[-1]
    sma200 = calc_sma(closes, 200)
    kama   = calc_kama(closes)
    mom1m  = calc_momentum(closes, min(21, len(closes)-1))
    mom3m  = calc_momentum(closes, min(63, len(closes)-1))
    mom6m  = calc_momentum(closes, min(126, len(closes)-1))
    rsi14  = calc_rsi(closes, 14)
    adx14  = calc_adx(closes, 14)
    ao     = calc_ao(closes)

    s = 0

    # SMA200: prezzo > SMA → pieno punteggio
    if w["sma200"] > 0 and sma200:
        s += w["sma200"] if price > sma200 else 0

    # KAMA: prezzo > KAMA → pieno punteggio
    if w["kama"] > 0 and kama:
        s += w["kama"] if price > kama else 0

    # Mom 1M: scala lineare, max a +20%, penalty se negativo
    if w["mom1m"] > 0 and mom1m is not None:
        if mom1m >= 0:
            s += round(min(w["mom1m"], (mom1m/20)*w["mom1m"]))
        else:
            # Penalità proporzionale al ribasso
            s += round(max(-w["mom1m"]/2, (mom1m/20)*w["mom1m"]))

    # Mom 3M: scala lineare, max a +30% — cuore dello score
    if w["mom3m"] > 0 and mom3m is not None:
        if mom3m >= 0:
            s += round(min(w["mom3m"], (mom3m/30)*w["mom3m"]))
        else:
            s += round(max(-w["mom3m"]/2, (mom3m/30)*w["mom3m"]))

    # Mom 6M: scala lineare, max a +50%
    if w["mom6m"] > 0 and mom6m is not None:
        if mom6m >= 0:
            s += round(min(w["mom6m"], (mom6m/50)*w["mom6m"]))
        else:
            s += round(max(-w["mom6m"]/2, (mom6m/50)*w["mom6m"]))

    # RSI: ottimale 55-75 (momentum sano), penalità sopra 80 (ipercomprato)
    if w["rsi"] > 0 and rsi14 is not None:
        if 55 <= rsi14 <= 75:
            s += w["rsi"]
        elif 45 <= rsi14 < 55:
            s += round(w["rsi"] * 0.5)
        elif rsi14 > 75:
            s += round(w["rsi"] * 0.3)  # ipercomprato → rischio reversal
        # RSI < 45: 0 punti

    # ADX: forza del trend — ottimale > 25
    if w["adx"] > 0 and adx14 is not None:
        if adx14 >= 30:
            s += w["adx"]
        elif adx14 >= 20:
            s += round(w["adx"] * 0.6)
        elif adx14 >= 15:
            s += round(w["adx"] * 0.3)

    # AO: Awesome Oscillator positivo = momentum confermato
    if w["ao"] > 0 and ao is not None:
        s += w["ao"] if ao > 0 else 0

    return min(100, max(0, s))

def calc_factor_score_storico(ticker, target_date, level_id, etf_data_completo):
    """Score factor calcolato sui prezzi storici fino a target_date."""
    sig = etf_data_completo.get(ticker)
    if not sig: return 0
    closes = sig.get("closes", [])
    dates  = sig.get("dates", [])
    if not closes or not dates: return 0
    n = min(len(closes), len(dates))
    closes_hist = [closes[i] for i in range(n) if dates[i] <= target_date]
    if len(closes_hist) < 35: return 0
    yield_pct = sig.get("yield_pct", 0)
    return calc_factor_score(closes_hist, level_id, yield_pct)

# ── REGIME MACRO DAI DATI REALI ─────────────────────────────────────────────
def classify_regime_at_date(proxy_data_completo, target_date):
    """
    Classifica il regime macro usando i prezzi dei proxy
    fino a target_date — regime calcolato dai dati reali,
    non hardcoded.
    """
    scores = {s: 0.0 for s in SCENARI}

    def get_closes_to_date(ticker, tgt):
        d = proxy_data_completo.get(ticker)
        if not d: return []
        closes = d.get("closes", [])
        dates  = d.get("dates", [])
        n = min(len(closes), len(dates))
        return [closes[i] for i in range(n) if dates[i] <= tgt]

    def get_ret(ticker, days=22):
        cl = get_closes_to_date(ticker, target_date)
        if len(cl) < days+1: return None
        old = cl[-(days+1)]
        return (cl[-1]-old)/old*100 if old else None

    # Momentum 4W normalizzato × affinità scenario
    mom4w = {}
    for t in ETF_PROXY:
        r = get_ret(t, 22)
        if r is not None: mom4w[t] = r

    if mom4w:
        vals = list(mom4w.values())
        vmin, vmax = min(vals), max(vals)
        rng = vmax-vmin if vmax != vmin else 1
        for t, r in mom4w.items():
            norm = (r-vmin)/rng*2-1
            for sc in SCENARI:
                affinity = ETF_PROXY[t].get(sc, 0)
                scores[sc] += norm*affinity*15

    # Cross-asset signals
    spy=get_ret("SPY"); tlt=get_ret("TLT"); gld=get_ret("GLD")
    vxx=get_ret("VXX"); hyg=get_ret("HYG"); lqd=get_ret("LQD")
    uup=get_ret("UUP"); eem=get_ret("EEM"); tip=get_ret("TIP")
    uso=get_ret("USO")

    if spy is not None and tlt is not None:
        diff=spy-tlt
        if diff > 5: scores["goldilocks"]+=20; scores["reflazione"]+=10
        elif diff < -5: scores["risk_off"]+=20; scores["neutro"]+=10

    if gld is not None:
        if gld > 5: scores["reflazione"]+=15; scores["stagflazione"]+=10; scores["risk_off"]+=8
        elif gld < -3: scores["goldilocks"]+=10

    if uso is not None:
        if uso > 8: scores["reflazione"]+=12; scores["stagflazione"]+=8
        elif uso < -8: scores["risk_off"]+=8

    if vxx is not None:
        if vxx > 15: scores["risk_off"]+=25; scores["stagflazione"]+=5
        elif vxx < -10: scores["goldilocks"]+=15

    if hyg is not None and lqd is not None:
        diff=hyg-lqd
        if diff > 3: scores["goldilocks"]+=12; scores["reflazione"]+=8
        elif diff < -3: scores["risk_off"]+=15; scores["stagflazione"]+=8

    if uup is not None:
        if uup > 3: scores["risk_off"]+=10; scores["stagflazione"]+=8
        elif uup < -2: scores["reflazione"]+=10; scores["goldilocks"]+=5

    if eem is not None and spy is not None:
        diff=eem-spy
        if diff > 3: scores["reflazione"]+=12
        elif diff < -5: scores["risk_off"]+=8; scores["stagflazione"]+=5

    if tip is not None and tlt is not None:
        diff=tip-tlt
        if diff > 2: scores["reflazione"]+=12; scores["stagflazione"]+=8
        elif diff < -2: scores["goldilocks"]+=10; scores["risk_off"]+=5

    total=sum(scores.values())
    if total <= 0:
        norm_scores={s:20 for s in SCENARI}
    else:
        norm_scores={s:round(scores[s]/total*100,1) for s in SCENARI}

    scenario=max(norm_scores, key=norm_scores.get)
    confidence=round(norm_scores[scenario])
    return scenario, confidence

def calc_pesi_override(scenario, confidence):
    base=SCENARIO_PESI.get(scenario, SCENARIO_PESI["neutro"])
    neutro=SCENARIO_PESI["neutro"]
    t=max(0.0, min(1.0,(confidence-50)/50)) if confidence > 50 else 0.0
    return {cat: round(neutro.get(cat,1.0)+t*(v-neutro.get(cat,1.0)),3)
            for cat, v in base.items()}

# ── UTILITÀ PREZZI STORICI ──────────────────────────────────────────────────
def get_price_on_date(ticker, target_date, etf_data_completo):
    sig=etf_data_completo.get(ticker)
    if not sig: return None
    closes=sig.get("closes",[]); dates=sig.get("dates",[])
    n=min(len(closes), len(dates))
    best=None
    for i in range(n):
        if dates[i] <= target_date: best=closes[i]
        elif dates[i] > target_date: break
    return best

def get_all_trading_dates(etf_data_completo):
    date_set=set()
    for sig in etf_data_completo.values():
        for d in sig.get("dates",[]):
            if d >= BACKTEST_START: date_set.add(d)
    return sorted(date_set)

def calc_total_return(price_entry, price_now, yield_pct, days):
    if not price_entry or price_entry==0: return 0.0
    price_ret=(price_now-price_entry)/price_entry*100
    yield_ret=(yield_pct/365)*days if yield_pct else 0.0
    return round(price_ret+yield_ret, 3)

# ── SELEZIONE ETF FACTOR ────────────────────────────────────────────────────
def seleziona_etf_factor(pool, etf_data_completo, level_id, alloc_t,
                          n_etf_max, target_date, pesi_override=None):
    """
    Seleziona ETF usando factor score calcolato sui prezzi storici.
    Nessun limite di categoria rigido — il momentum decide.
    Max 1 ETF leva, max 2 per categoria come protezione minima.
    """
    isDist = level_id.startswith("C")
    candidati=[]

    for t in pool:
        sig=etf_data_completo.get(t)
        if not sig or not sig.get("closes"): continue
        price=get_price_on_date(t, target_date, etf_data_completo)
        if not price: continue

        score=calc_factor_score_storico(t, target_date, level_id, etf_data_completo)
        if score <= 10: continue  # soglia minima più bassa per factor

        cat=ETF_CATEGORIA.get(t, "az_globale")
        peso_macro=(pesi_override or {}).get(cat, 1.0)
        if peso_macro <= 0: continue

        tipo=ETF_TIPO.get(t, "Acc")
        # Per C: preferisce Dist, per A: preferisce Acc
        boost=5 if (isDist and tipo=="Dist") or (not isDist and tipo=="Acc") else 0
        # Boost extra per tematici nei livelli Trading (C9/A9)
        if level_id in ("C9","A9") and cat=="tematico":
            boost+=5

        score_eff=min(100, score*peso_macro+boost)
        candidati.append({
            "ticker":t, "score_eff":score_eff, "score_raw":score,
            "cat":cat, "price":price,
            "yield_pct":sig.get("yield_pct",0),
            "mom3m": calc_momentum(
                [sig["closes"][i] for i in range(min(len(sig["closes"]),len(sig["dates"])))
                 if sig["dates"][i] <= target_date][-64:] if sig.get("dates") else [],
                min(63, len([sig["closes"][i] for i in range(min(len(sig["closes"]),len(sig["dates"])))
                 if sig["dates"][i] <= target_date])-1)
            ) or 0,
        })

    # Ordina per score_eff decrescente — momentum decide
    candidati.sort(key=lambda x: x["score_eff"], reverse=True)

    selected=[]; leva_n=0; cat_c={}
    for c in candidati:
        if len(selected) >= n_etf_max: break
        if c["ticker"] in LEVA_TICKERS:
            if leva_n >= 1: continue
            leva_n+=1
        else:
            cn=cat_c.get(c["cat"],0)
            if cn >= 2: continue
            cat_c[c["cat"]]=cn+1
        selected.append(c)

    if not selected: return []

    # Pesi proporzionali allo score — no quote fisse per categoria
    # Usa alloc_t come cap massimo per categoria, non minimo
    tot_score=sum(c["score_eff"] for c in selected) or 1
    pesi_raw={c["ticker"]: c["score_eff"]/tot_score*100 for c in selected}

    # Cap per categoria (dalla alloc_t) — evita concentrazione eccessiva
    by_cat={}
    for c in selected: by_cat.setdefault(c["cat"],[]).append(c)
    pesi_finali={}
    for cat, items in by_cat.items():
        cap=alloc_t.get(cat, 100)
        tot_cat=sum(pesi_raw[i["ticker"]] for i in items)
        if tot_cat > cap and cap > 0:
            factor_riduzione=cap/tot_cat
            for i in items:
                pesi_finali[i["ticker"]]=pesi_raw[i["ticker"]]*factor_riduzione
        else:
            for i in items:
                pesi_finali[i["ticker"]]=pesi_raw[i["ticker"]]

    # Normalizza a 100
    tot_p=sum(pesi_finali.values()) or 1
    comp=[]
    for c in selected:
        t=c["ticker"]
        p=round(pesi_finali.get(t,0)/tot_p*100, 1)
        imp=round(CAPITALE_MODELLO*p/100, 2)
        comp.append({
            "ticker":t,
            "nome":ETF_NOMI.get(t,t),
            "peso":p,
            "importo":imp,
            "price_entry":round(c["price"],4),
            "quote":round(imp/c["price"],4) if c["price"] else 0,
            "yield_pct":c["yield_pct"],
            "score_entry":c["score_raw"],
            "mom3m_entry":round(c["mom3m"],2) if c["mom3m"] else 0,
        })

    # Aggiusta a 100%
    tot_p2=sum(c["peso"] for c in comp)
    diff=round(100-tot_p2, 1)
    if diff != 0 and comp: comp[0]["peso"]=round(comp[0]["peso"]+diff,1)
    return comp

# ── ROTATION TRIGGER ────────────────────────────────────────────────────────
def check_rotation_trigger(composizione, pool, etf_data_completo,
                            level_id, target_date, giorni_attivi):
    """
    Trigger 4 (Factor): sostituisce un ETF in portafoglio se esiste
    un candidato nel pool con mom3M superiore di ROTATION_THRESHOLD punti.
    Attivo solo dopo 30gg dalla versione.
    """
    if giorni_attivi < 30: return None

    in_ptf={c["ticker"]: c for c in composizione}

    def get_mom3m_storico(ticker):
        sig=etf_data_completo.get(ticker)
        if not sig: return None
        closes=sig.get("closes",[]); dates=sig.get("dates",[])
        n=min(len(closes),len(dates))
        cl=[closes[i] for i in range(n) if dates[i] <= target_date]
        return calc_momentum(cl, min(63, len(cl)-1))

    # Mom3M di tutti gli ETF in portafoglio
    mom_ptf={}
    for t in in_ptf:
        m=get_mom3m_storico(t)
        if m is not None: mom_ptf[t]=m

    if not mom_ptf: return None
    min_mom_ticker=min(mom_ptf, key=mom_ptf.get)
    min_mom=mom_ptf[min_mom_ticker]

    # Cerca candidato migliore nel pool
    migliore=None; migliore_mom=None; migliore_score=0

    for t in pool:
        if t in in_ptf: continue
        sig=etf_data_completo.get(t)
        if not sig or not sig.get("closes"): continue
        price=get_price_on_date(t, target_date, etf_data_completo)
        if not price: continue

        m=get_mom3m_storico(t)
        if m is None: continue

        # Deve battere il peggior ETF in ptf di ROTATION_THRESHOLD
        if m > min_mom + ROTATION_THRESHOLD:
            score=calc_factor_score_storico(t, target_date, level_id, etf_data_completo)
            if score > migliore_score:
                migliore=t; migliore_mom=m; migliore_score=score

    if not migliore: return None

    return {
        "out": min_mom_ticker,
        "in": migliore,
        "mom3m_out": round(min_mom,2),
        "mom3m_in": round(migliore_mom,2),
        "gap": round(migliore_mom-min_mom,2),
        "motivo": f"rotation: {min_mom_ticker} mom3M {min_mom:.1f}% → {migliore} mom3M {migliore_mom:.1f}% (gap +{migliore_mom-min_mom:.1f}%)"
    }

# ── BACKTEST FACTOR ─────────────────────────────────────────────────────────
def run_factor_backtest(level_id, etf_data_completo, alloc_t,
                        n_etf_max, proxy_data_completo):
    """
    Backtest giornaliero dal 01/01/2025.
    Regime calcolato dai dati reali ogni 5 giorni di trading.
    Trigger: drawdown, cambio regime, rotation momentum.
    """
    print(f"    Factor backtest {level_id}...")

    all_dates=get_all_trading_dates(etf_data_completo)
    if not all_dates: return []

    try:
        with open(LEVELS_FILE) as f:
            levels=json.load(f)["levels"]
        lv_json=next((l for l in levels if l["id"]==level_id), None)
        pool=lv_json.get("etf_pool",[]) if lv_json else []
        alloc_t=lv_json.get("alloc", alloc_t) if lv_json else alloc_t
    except Exception:
        pool=[]

    if not pool: return []

    pers=PERSISTENZA["aggressivo"]

    # Composizione iniziale
    comp_v1=seleziona_etf_factor(pool, etf_data_completo, level_id,
                                  alloc_t, n_etf_max, BACKTEST_START, None)
    if not comp_v1: return []

    versioni=[]
    ver_num=1
    cap_corrente=float(CAPITALE_MODELLO)
    equity_curve=[cap_corrente]
    equity_dates=[BACKTEST_START]

    versione_attiva={
        "versione":ver_num, "livello_id":level_id,
        "data_apertura":BACKTEST_START, "data_chiusura":None,
        "regime":"neutro", "confidence":50,
        "trigger_apertura":"inizializzazione_factor",
        "trigger_chiusura":None,
        "capitale_inizio":cap_corrente, "capitale_attuale":cap_corrente,
        "performance_pct":0.0, "performance_eur":0.0,
        "giorni_attivo":0, "composizione":comp_v1,
        "etf_usciti_anticipati":[], "aggiornato":BACKTEST_START,
        "_equity":[cap_corrente],
    }
    versioni.append(versione_attiva)

    # Stato regime
    ultimo_check=BACKTEST_START
    regime_attivo="neutro"; conf_attiva=50
    settimane_candidato=0; regime_candidato=None

    def apri_nuova_versione(cap_new, data, regime, conf, trigger_str, po):
        nonlocal ver_num, versione_attiva
        versione_attiva["data_chiusura"]=data
        versione_attiva["trigger_chiusura"]=trigger_str
        ver_num+=1
        nuova_comp=seleziona_etf_factor(pool, etf_data_completo, level_id,
                                        alloc_t, n_etf_max, data, po)
        if not nuova_comp: return
        scala=cap_new/CAPITALE_MODELLO if CAPITALE_MODELLO else 1
        for e in nuova_comp:
            e["importo"]=round(e["importo"]*scala,2)
            e["quote"]=round(e["importo"]/e["price_entry"],4) if e["price_entry"] else 0
        versione_attiva={
            "versione":ver_num, "livello_id":level_id,
            "data_apertura":data, "data_chiusura":None,
            "regime":regime, "confidence":conf,
            "trigger_apertura":trigger_str, "trigger_chiusura":None,
            "capitale_inizio":cap_new, "capitale_attuale":cap_new,
            "performance_pct":0.0, "performance_eur":0.0,
            "giorni_attivo":0, "composizione":nuova_comp,
            "etf_usciti_anticipati":[], "aggiornato":data,
            "_equity":[cap_new],
        }
        versioni.append(versione_attiva)

    for data_corrente in all_dates:
        if data_corrente <= BACKTEST_START: continue

        # Giorni attivi
        try:
            d_open=datetime.date.fromisoformat(versione_attiva["data_apertura"])
            d_curr=datetime.date.fromisoformat(data_corrente)
            versione_attiva["giorni_attivo"]=(d_curr-d_open).days
        except Exception:
            pass

        giorni_attivi=versione_attiva["giorni_attivo"]

        # ── Aggiorna prezzi + check drawdown ──────────────────────────
        tot_valore=0.0
        trigger_dd=None

        for etf in versione_attiva["composizione"]:
            t=etf["ticker"]
            if etf.get("importo",0) <= 0: continue
            price_now=get_price_on_date(t, data_corrente, etf_data_completo)
            if not price_now: price_now=etf.get("price_now", etf["price_entry"])

            price_entry=etf["price_entry"]
            y=etf.get("yield_pct",0)
            tr=calc_total_return(price_entry, price_now, y, giorni_attivi)
            perf_eur=round(etf["importo"]*tr/100, 2)
            dd=round((price_now-price_entry)/price_entry*100, 2) if price_entry else 0

            etf["price_now"]=round(price_now,4)
            etf["perf_pct"]=round(tr,3)
            etf["perf_eur"]=perf_eur
            etf["drawdown_pct"]=dd

            # Trigger drawdown — aggressivo
            if dd <= -40 and not trigger_dd:
                trigger_dd={"tipo":"drawdown_40","dd":dd,
                            "motivo":f"{t} drawdown {dd:.1f}% — uscita immediata"}
            elif dd <= -25 and not trigger_dd:
                trigger_dd={"tipo":"drawdown_25","dd":dd,
                            "motivo":f"{t} drawdown {dd:.1f}% — ribilanciamento obbligatorio"}
            elif dd <= -15 and not trigger_dd:
                trigger_dd={"tipo":"drawdown_15","dd":dd,
                            "motivo":f"{t} drawdown {dd:.1f}% — ribilanciamento aggressivo"}

            tot_valore+=etf["importo"]*(1+tr/100)

        perf_tot_eur=round(tot_valore-versione_attiva["capitale_inizio"],2)
        versione_attiva["capitale_attuale"]=round(tot_valore,2)
        versione_attiva["performance_eur"]=perf_tot_eur
        versione_attiva["performance_pct"]=round(
            perf_tot_eur/versione_attiva["capitale_inizio"]*100, 2)
        versione_attiva["aggiornato"]=data_corrente
        versione_attiva.setdefault("_equity",[]).append(round(tot_valore,2))
        versione_attiva.setdefault("_equity_dates",[]).append(data_corrente)
        equity_curve.append(round(tot_valore,2))
        equity_dates.append(data_corrente)

        # ── Check regime ogni 5 giorni ────────────────────────────────
        try:
            giorni_da_check=(datetime.date.fromisoformat(data_corrente)-
                             datetime.date.fromisoformat(ultimo_check)).days
        except Exception:
            giorni_da_check=0

        cambio_regime=None
        rotation=None

        if giorni_da_check >= 5:
            ultimo_check=data_corrente

            # Regime calcolato dai dati reali
            sc, conf=classify_regime_at_date(proxy_data_completo, data_corrente)

            if sc != regime_attivo:
                if sc == regime_candidato:
                    settimane_candidato+=1
                else:
                    regime_candidato=sc; settimane_candidato=1

                if (settimane_candidato >= pers["settimane"] and
                        conf >= pers["soglia"] and giorni_attivi >= 20):
                    cambio_regime={"da":regime_attivo,"a":sc,"confidence":conf}
                    regime_attivo=sc; conf_attiva=conf
                    settimane_candidato=0; regime_candidato=None
            else:
                settimane_candidato=0; regime_candidato=None

            # Rotation trigger (solo se nessun altro trigger)
            if not trigger_dd and not cambio_regime:
                rotation=check_rotation_trigger(
                    versione_attiva["composizione"], pool,
                    etf_data_completo, level_id, data_corrente, giorni_attivi)

        # ── Esegui ribilanciamento ────────────────────────────────────
        cap_new=versione_attiva["capitale_attuale"]
        po=calc_pesi_override(regime_attivo, conf_attiva)

        if trigger_dd and trigger_dd["tipo"]=="drawdown_40":
            # Sostituisci solo l'ETF crollo con monetario
            t_out=next((e["ticker"] for e in versione_attiva["composizione"]
                       if e.get("drawdown_pct",0) <= -40 and e.get("importo",0)>0), None)
            if t_out:
                etf_out=next(e for e in versione_attiva["composizione"] if e["ticker"]==t_out)
                versione_attiva["composizione"]=[e for e in versione_attiva["composizione"]
                                                  if e["ticker"]!=t_out]
                mon="XEON.MI"
                p_mon=get_price_on_date(mon, data_corrente, etf_data_completo) or 149.0
                versione_attiva["composizione"].append({
                    "ticker":mon,"nome":ETF_NOMI.get(mon,mon),
                    "peso":etf_out["peso"],"importo":etf_out["importo"],
                    "price_entry":round(p_mon,4),
                    "quote":round(etf_out["importo"]/p_mon,4),
                    "yield_pct":0,
                })
                versione_attiva.setdefault("etf_usciti_anticipati",[]).append({
                    "ticker":t_out,"motivo":"drawdown_40","data":data_corrente,
                    "price_entry":etf_out["price_entry"],
                    "price_exit":etf_out.get("price_now",etf_out["price_entry"]),
                    "perf_pct":etf_out.get("perf_pct",0),
                    "perf_eur":etf_out.get("perf_eur",0),
                    "drawdown_pct":etf_out.get("drawdown_pct",0),
                })

        elif trigger_dd and trigger_dd["tipo"] in ("drawdown_25","drawdown_15"):
            apri_nuova_versione(cap_new, data_corrente, regime_attivo,
                                conf_attiva, trigger_dd["motivo"], po)

        elif cambio_regime:
            apri_nuova_versione(cap_new, data_corrente, cambio_regime["a"],
                                cambio_regime["confidence"],
                                f"cambio_regime {cambio_regime['da']}→{cambio_regime['a']} "
                                f"(conf {cambio_regime['confidence']}%)", po)

        elif rotation:
            # Rotation: sostituisci solo l'ETF peggio, non reset totale
            t_out=rotation["out"]; t_in=rotation["in"]
            etf_out_obj=next((e for e in versione_attiva["composizione"]
                             if e["ticker"]==t_out), None)
            sig_in=etf_data_completo.get(t_in,{})
            price_in=get_price_on_date(t_in, data_corrente, etf_data_completo)

            if etf_out_obj and price_in:
                versione_attiva["etf_usciti_anticipati"].append({
                    "ticker":t_out,"motivo":"rotation_momentum","data":data_corrente,
                    "price_entry":etf_out_obj["price_entry"],
                    "price_exit":etf_out_obj.get("price_now",etf_out_obj["price_entry"]),
                    "perf_pct":etf_out_obj.get("perf_pct",0),
                    "perf_eur":etf_out_obj.get("perf_eur",0),
                    "drawdown_pct":etf_out_obj.get("drawdown_pct",0),
                    "mom3m_out":rotation["mom3m_out"],
                    "mom3m_in":rotation["mom3m_in"],
                    "replaced_by":t_in,
                })
                importo_libero=etf_out_obj["importo"]
                versione_attiva["composizione"]=[e for e in versione_attiva["composizione"]
                                                  if e["ticker"]!=t_out]
                score_in=calc_factor_score_storico(t_in, data_corrente, level_id, etf_data_completo)
                versione_attiva["composizione"].append({
                    "ticker":t_in,"nome":ETF_NOMI.get(t_in,t_in),
                    "peso":etf_out_obj["peso"],"importo":importo_libero,
                    "price_entry":round(price_in,4),
                    "quote":round(importo_libero/price_in,4),
                    "yield_pct":sig_in.get("yield_pct",0),
                    "score_entry":score_in,
                    "mom3m_entry":rotation["mom3m_in"],
                })

    # ── Rendimenti mensili ────────────────────────────────────────────
    rend_mensili={}
    if len(equity_curve) > 1:
        mesi_vals={}
        for d, v in zip(equity_dates, equity_curve):
            ym=d[:7]
            mesi_vals.setdefault(ym,[]).append(v)
        prev_val=CAPITALE_MODELLO
        for ym in sorted(mesi_vals.keys()):
            vals=mesi_vals[ym]
            if not vals: continue
            last_val=vals[-1]
            r=round((last_val-prev_val)/prev_val*100,2) if prev_val else 0
            rend_mensili[ym]=r
            prev_val=last_val

    # ── Diario movimenti ──────────────────────────────────────────────
    diario=[]
    for i in range(1, len(versioni)):
        v_old=versioni[i-1]; v_new=versioni[i]
        comp_old={e["ticker"]:e for e in v_old.get("composizione",[])}
        comp_new={e["ticker"]:e for e in v_new.get("composizione",[])}
        venduti=[{"ticker":t,"nome":e.get("nome",t),"peso":e.get("peso",0),
                  "importo":e.get("importo",0),"price_entry":e.get("price_entry",0),
                  "price_exit":e.get("price_now",e.get("price_entry",0)),
                  "perf_pct":e.get("perf_pct",0),"perf_eur":e.get("perf_eur",0)}
                 for t,e in comp_old.items() if t not in comp_new and e.get("importo",0)>0]
        acquistati=[{"ticker":t,"nome":e.get("nome",t),"peso":e.get("peso",0),
                     "importo":e.get("importo",0),"price_entry":e.get("price_entry",0),
                     "score_entry":e.get("score_entry",0),"mom3m_entry":e.get("mom3m_entry",0)}
                    for t,e in comp_new.items() if t not in comp_old and e.get("importo",0)>0]
        diario.append({
            "data":v_new.get("data_apertura"),
            "versione_da":v_old.get("versione"),
            "versione_a":v_new.get("versione"),
            "trigger":v_new.get("trigger_apertura","—"),
            "regime":v_new.get("regime","—"),
            "capitale_ribilanciamento":v_new.get("capitale_inizio",0),
            "venduti":venduti, "acquistati":acquistati,
        })

    # Metriche finali
    vol_tot=calc_volatilita(equity_curve)
    mdd_tot=calc_max_drawdown(equity_curve)
    sh_tot=calc_sharpe(equity_curve)

    for v in versioni:
        eq=v.pop("_equity",[])
        v.pop("_equity_dates",None)
        v["volatilita_ann"]=calc_volatilita(eq)
        v["max_drawdown"]=calc_max_drawdown(eq)
        v["sharpe"]=calc_sharpe(eq)
        v["_vol_tot"]=vol_tot
        v["_mdd_tot"]=mdd_tot
        v["_sharpe_tot"]=sh_tot
        v["_rend_mensili"]=rend_mensili
        v["_diario"]=diario

    n_ver=len(versioni)
    n_reb=n_ver-1
    cap_fin=versione_attiva.get("capitale_attuale",CAPITALE_MODELLO)
    perf_fin=round((cap_fin-CAPITALE_MODELLO)/CAPITALE_MODELLO*100,2)
    n_rot=sum(len(v.get("etf_usciti_anticipati",[])) for v in versioni
              if any(u.get("motivo")=="rotation_momentum"
                     for u in v.get("etf_usciti_anticipati",[])))

    vol_str=f", vol {vol_tot:.1f}%, MDD {mdd_tot:.1f}%, Sharpe {sh_tot:.2f}" if vol_tot else ""
    print(f"      → {n_ver} ver, {n_reb} rib, {n_rot} rot, perf {perf_fin:+.1f}%{vol_str}")

    return versioni

# ── MAIN ────────────────────────────────────────────────────────────────────
def main():
    oggi=datetime.date.today().isoformat()
    print(f"COMPASS Factor Strategy v1.0 — {oggi}")
    print(f"Livelli: {LIVELLI_FACTOR}")
    print(f"Benchmark target: {BENCHMARK_TARGET}")

    # Carica dati esistenti
    existing={}
    if OUT_FILE.exists():
        try:
            with open(OUT_FILE) as f:
                existing=json.load(f)
            print(f"  Dati esistenti: {existing.get('generated','?')}")
        except Exception:
            pass

    portafogli_esistenti=existing.get("portafogli_factor",{})
    run_number=existing.get("run_number",0)+1
    print(f"  Run number: {run_number}")

    # ── 1. Scarica ETF PROXY ──────────────────────────────────────────────
    print(f"\n[1/4] Download {len(ETF_PROXY)} ETF proxy...")
    proxy_data={}
    for t in ETF_PROXY:
        print(f"  {t}...", end=" ", flush=True)
        raw=fetch_yahoo(t, "2y")
        proxy_data[t]=raw
        print("OK" if raw else "ERR")
        time.sleep(0.3)

    # Classifica regime corrente
    regime_oggi, conf_oggi=classify_regime_at_date(proxy_data, oggi)
    po_oggi=calc_pesi_override(regime_oggi, conf_oggi)
    print(f"\n  Regime oggi: {regime_oggi} ({conf_oggi}%)")

    # ── 2. Scarica ETF universo factor ───────────────────────────────────
    # Raccoglie tutti i ticker unici dai pool dei livelli factor
    try:
        with open(LEVELS_FILE) as f:
            levels_data=json.load(f)["levels"]
    except Exception:
        print("ERR: levels.json non trovato")
        return

    ticker_set=set()
    for lv in levels_data:
        if lv["id"] in LIVELLI_FACTOR:
            ticker_set.update(lv.get("etf_pool",[]))
    # Aggiungi proxy-like tickers per benchmark
    ticker_set.add("IWMO.MI")
    ticker_set.add("MAGR.MI")
    ticker_set.add("XEON.MI")  # fallback monetario

    print(f"\n[2/4] Download {len(ticker_set)} ETF universo factor...")
    etf_data={}
    success=0; errors=0

    for i, ticker in enumerate(sorted(ticker_set)):
        yahoo_ticker=resolve_ticker(ticker)
        if yahoo_ticker is None:
            errors+=1; continue
        print(f"  [{i+1}/{len(ticker_set)}] {ticker}...", end=" ", flush=True)
        raw=fetch_yahoo(yahoo_ticker, "2y")
        if not raw:
            errors+=1
            etf_data[ticker]={"ticker":ticker,"closes":[],"dates":[],"yield_pct":0,"price":None}
            print("ERR")
            time.sleep(0.3)
            continue
        etf_data[ticker]={
            "ticker":ticker,
            "closes":raw["closes"],
            "dates":raw.get("dates",[]),
            "yield_pct":raw["yield_pct"],
            "price":round(raw["current_price"],4),
        }
        success+=1
        score_oggi=calc_factor_score(raw["closes"], "A8", raw["yield_pct"])
        print(f"OK score={score_oggi}")
        time.sleep(0.35)

    print(f"  Download: {success} OK, {errors} ERR")

    # Aggiungi proxy data per regime storico
    for t, d in proxy_data.items():
        if d and t not in etf_data:
            etf_data[t]=d

    # ── 3. Backtest factor per ogni livello ──────────────────────────────
    print(f"\n[3/4] Backtest factor (da {BACKTEST_START})...")

    portafogli={}
    for lv in levels_data:
        lid=lv["id"]
        if lid not in LIVELLI_FACTOR: continue

        alloc_t=lv.get("alloc",{})
        n_max=lv.get("n_etf_max",8)

        if lid in portafogli_esistenti:
            # Portafoglio esiste — aggiorna solo versione attiva
            storia=portafogli_esistenti[lid]["storia"]
            ver_att=next((v for v in reversed(storia) if v.get("data_chiusura") is None), None)
            if ver_att:
                # Aggiorna prezzi
                tot_v=0.0
                for etf in ver_att.get("composizione",[]):
                    t=etf["ticker"]
                    price_now=etf_data.get(t,{}).get("price")
                    if not price_now: continue
                    price_entry=etf["price_entry"]
                    giorni=(datetime.date.fromisoformat(oggi)-
                            datetime.date.fromisoformat(ver_att["data_apertura"])).days
                    tr=calc_total_return(price_entry, price_now,
                                        etf.get("yield_pct",0), giorni)
                    etf["price_now"]=round(price_now,4)
                    etf["perf_pct"]=round(tr,3)
                    etf["perf_eur"]=round(etf["importo"]*tr/100,2)
                    etf["drawdown_pct"]=round((price_now-price_entry)/price_entry*100,2) if price_entry else 0
                    tot_v+=etf["importo"]*(1+tr/100)
                ver_att["capitale_attuale"]=round(tot_v,2)
                ver_att["performance_eur"]=round(tot_v-ver_att["capitale_inizio"],2)
                ver_att["performance_pct"]=round(
                    (tot_v-ver_att["capitale_inizio"])/ver_att["capitale_inizio"]*100,2)
                ver_att["aggiornato"]=oggi

                # Check rotation trigger live
                rot=check_rotation_trigger(
                    ver_att["composizione"],
                    lv.get("etf_pool",[]), etf_data, lid, oggi,
                    (datetime.date.fromisoformat(oggi)-
                     datetime.date.fromisoformat(ver_att["data_apertura"])).days)

                portafogli[lid]=portafogli_esistenti[lid]
                cap_fin=ver_att["capitale_attuale"]
                perf_fin=round((cap_fin-CAPITALE_MODELLO)/CAPITALE_MODELLO*100,2)
                print(f"    {lid}: aggiornato, perf {perf_fin:+.1f}%"
                      + (f" | ROTATION suggerita: {rot['out']}→{rot['in']}" if rot else ""))

                # Aggiorna performance totale
                portafogli[lid]["performance_totale_pct"]=perf_fin
                portafogli[lid]["performance_totale_eur"]=round(cap_fin-CAPITALE_MODELLO,2)
                portafogli[lid]["capitale_attuale"]=cap_fin
                portafogli[lid]["rotation_suggerita"]=rot
                continue

        # Backtest da zero
        versioni=run_factor_backtest(lid, etf_data, alloc_t, n_max, proxy_data)
        if not versioni:
            print(f"    {lid}: nessuna versione generata")
            continue

        vol_tot=versioni[0].pop("_vol_tot",None)
        mdd_tot=versioni[0].pop("_mdd_tot",None)
        sh_tot=versioni[0].pop("_sharpe_tot",None)
        rend_m=versioni[0].pop("_rend_mensili",None)
        diario=versioni[0].pop("_diario",None)
        for v in versioni[1:]:
            v.pop("_vol_tot",None); v.pop("_mdd_tot",None)
            v.pop("_sharpe_tot",None); v.pop("_rend_mensili",None)
            v.pop("_diario",None)

        ver_att_fin=next((v for v in reversed(versioni) if v.get("data_chiusura") is None), None)
        cap_fin=ver_att_fin.get("capitale_attuale",CAPITALE_MODELLO) if ver_att_fin else CAPITALE_MODELLO
        perf_fin=round((cap_fin-CAPITALE_MODELLO)/CAPITALE_MODELLO*100,2)

        portafogli[lid]={
            "capitale_modello":CAPITALE_MODELLO,
            "versione_corrente":len(versioni),
            "storia":versioni,
            "performance_totale_pct":perf_fin,
            "performance_totale_eur":round(cap_fin-CAPITALE_MODELLO,2),
            "capitale_attuale":cap_fin,
            "volatilita_ann":vol_tot,
            "max_drawdown":mdd_tot,
            "sharpe":sh_tot,
            "rendimenti_mensili":rend_m or {},
            "diario_movimenti":diario or [],
            "benchmark_principale":BENCHMARK_TARGET,
            "rotation_suggerita":None,
        }

    # ── 4. Performance IWMO benchmark ────────────────────────────────────
    print(f"\n[4/4] Benchmark {BENCHMARK_TARGET}...")
    iwmo_data=etf_data.get(BENCHMARK_TARGET)
    iwmo_perf=None
    if iwmo_data and iwmo_data.get("closes") and iwmo_data.get("dates"):
        p_start=get_price_on_date(BENCHMARK_TARGET, BACKTEST_START, etf_data)
        p_now=iwmo_data["closes"][-1] if iwmo_data["closes"] else None
        if p_start and p_now:
            iwmo_perf=round((p_now-p_start)/p_start*100,2)
    print(f"  IWMO perf da {BACKTEST_START}: {iwmo_perf:+.1f}%" if iwmo_perf else "  IWMO: N/D")

    # Outperformance per ogni livello
    for lid, ptf in portafogli.items():
        ptf_perf=ptf.get("performance_totale_pct")
        if ptf_perf is not None and iwmo_perf is not None:
            ptf["outperformance_iwmo"]=round(ptf_perf-iwmo_perf,2)
            ptf["batte_iwmo"]=ptf_perf > iwmo_perf

    # ── Output ────────────────────────────────────────────────────────────
    # Score correnti per ogni ETF nel universo
    etf_scores={}
    for t, d in etf_data.items():
        if not d.get("closes") or not d.get("price"): continue
        scores_per_livello={}
        for lid in LIVELLI_FACTOR:
            scores_per_livello[lid]=calc_factor_score(d["closes"], lid, d.get("yield_pct",0))
        m3=calc_momentum(d["closes"], min(63, len(d["closes"])-1))
        m1=calc_momentum(d["closes"], min(21, len(d["closes"])-1))
        m6=calc_momentum(d["closes"], min(126, len(d["closes"])-1))
        rsi=calc_rsi(d["closes"])
        adx=calc_adx(d["closes"])
        ao=calc_ao(d["closes"])
        etf_scores[t]={
            "ticker":t,"nome":ETF_NOMI.get(t,t),
            "price":d["price"],
            "mom1m":m1,"mom3m":m3,"mom6m":m6,
            "rsi14":rsi,"adx14":adx,"ao":ao,
            "yield_pct":d.get("yield_pct",0),
            "scores":scores_per_livello,
        }

    output={
        "generated":datetime.datetime.utcnow().isoformat(),
        "version":"factor_1.0",
        "run_number":run_number,
        "strategy":"Factor Momentum",
        "livelli":LIVELLI_FACTOR,
        "benchmark_target":BENCHMARK_TARGET,
        "benchmark_perf_pct":iwmo_perf,
        "backtest_start":BACKTEST_START,
        "regime_oggi":{"scenario":regime_oggi,"confidence":conf_oggi},
        "rotation_threshold":ROTATION_THRESHOLD,
        "portafogli_factor":portafogli,
        "etf_scores":etf_scores,
    }

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE,"w",encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",",":"))

    size=OUT_FILE.stat().st_size/1024
    print(f"\n✅ Done → {OUT_FILE} ({size:.0f} KB)")
    print(f"   Run: {run_number} | Regime: {regime_oggi} ({conf_oggi}%)")
    print(f"   IWMO benchmark: {iwmo_perf:+.1f}%" if iwmo_perf else "   IWMO: N/D")
    print(f"\n   Risultati factor vs IWMO:")
    for lid, ptf in portafogli.items():
        perf=ptf.get("performance_totale_pct")
        out=ptf.get("outperformance_iwmo")
        batte=ptf.get("batte_iwmo",False)
        flag="✅" if batte else "❌"
        print(f"   {flag} {lid}: {perf:+.1f}% | vs IWMO: {out:+.1f}%" if perf and out else f"   {lid}: N/D")

if __name__ == "__main__":
    main()
