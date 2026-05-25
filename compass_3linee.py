#!/usr/bin/env python3
"""
COMPASS — compass_3linee.py v1.0
══════════════════════════════════
Strategia 3 Linee: Monetario / Obbligazionario / Azionario

Architettura:
  - 3 portafogli separati da €100.000 ciascuno
  - Linea M (Monetario): ottimizzazione yield, no rotation momentum
  - Linea O (Obbligazionario): rotation momentum + regime, score adattivo RSI
  - Linea A (Azionario): identica ad A7 factor, score adattivo RSI

Fase 1: portafogli separati e indipendenti
Fase 2 (futura): rotation capitale tra le 3 linee basata su regime + momentum

Output: data/compass_3linee.json
"""

import json, datetime, time, urllib.request
from pathlib import Path

# ── CONFIGURAZIONE ─────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).parent
LEVELS_FILE     = BASE_DIR / "data" / "levels.json"
OUT_FILE        = BASE_DIR / "data" / "compass_3linee.json"
BACKTEST_START  = "2025-01-01"
CAPITALE        = 100_000

LINEE = ["M", "O", "A"]

# ── COMPOSIZIONE STATICA LINEA O PER REGIME v2.0 ───────────────────────────
# Logica: cuscinetto anticiclico, non perde, cedola >=3% netto
# Nessuna rotation momentum — cambia solo su cambio regime robusto (conf>=65%)
# Floor mensile: -3% -> ribilanciamento difensivo automatico
# Pool: solo ETF obbligazionari puri, nessun azionario, nessun rischio cambio
# STHY.MI rimosso -> IHYU.MI (USD HY EUR hedged, no rischio cambio)
# JNHD.DE rimosso — era Giappone azionario, errore classificazione
COMPOSIZIONE_O_PER_REGIME = {
    "goldilocks": [
        {"ticker":"IHYU.MI","peso":30},
        {"ticker":"XUCS.DE","peso":30},
        {"ticker":"STHE.MI","peso":25},
        {"ticker":"IEAC.MI","peso":15},
    ],
    "reflazione": [
        {"ticker":"XUCS.DE","peso":35},
        {"ticker":"IHYU.MI","peso":25},
        {"ticker":"EMBE.MI","peso":25},
        {"ticker":"IEAC.MI","peso":15},
    ],
    "stagflazione": [
        {"ticker":"IBGS.MI","peso":40},
        {"ticker":"XUCS.DE","peso":35},
        {"ticker":"SEGA.MI","peso":25},
    ],
    "risk_off": [
        {"ticker":"IBGS.MI","peso":45},
        {"ticker":"XGSH.MI","peso":30},
        {"ticker":"XUCS.DE","peso":25},
    ],
    "neutro": [
        {"ticker":"XUCS.DE","peso":30},
        {"ticker":"IHYU.MI","peso":25},
        {"ticker":"IEAC.MI","peso":25},
        {"ticker":"IBGS.MI","peso":20},
    ],
}

# Yield stimati per cedola aggregata netta
YIELD_STIMATI_O = {
    "IHYU.MI": 4.5, "XUCS.DE": 4.2, "STHE.MI": 3.7,
    "IEAC.MI": 3.1, "EMBE.MI": 3.8, "IBGS.MI": 2.8,
    "SEGA.MI": 2.6, "XGSH.MI": 2.9,
}

# Trigger ribilanciamento linea O
O_CONF_MIN_REGIME   = 65    # Confidence minima per cambio regime
O_PERSIST_SETTIMANE = 3     # Settimane persistenza minima
O_FLOOR_MENSILE     = -3.0  # Floor mensile — se superato -> ribilanciamento difensivo
O_CEDOLA_MIN_NETTA  = 3.0   # Cedola aggregata minima netta %

LINEE_INFO = {
    "M": {
        "nome": "Monetario",
        "obiettivo": "Bunker — protezione assoluta, tasso overnight, no rotation",
        "benchmark": "XEON.MI",
        "n_etf_max": 3,
        "pool": ["XEON.MI", "SMART.MI", "XGSH.MI", "IBGS.MI", "IU0E.MI"],
        "colore": "#0EA5E9",
    },
    "O": {
        "nome": "Obbligazionario",
        "obiettivo": "Cuscinetto anticiclico — non perde, cedola >=3% netto, statico per regime",
        "benchmark": "IEAG.MI",
        "n_etf_max": 5,
        "pool": [
            "IHYU.MI", "XUCS.DE", "STHE.MI", "IEAC.MI",
            "EMBE.MI", "IBGS.MI", "SEGA.MI", "XGSH.MI",
        ],
        "colore": "#8B5CF6",
    },
    "A": {
        "nome": "Azionario",
        "obiettivo": "Motore — logica A7 Factor, Sharpe 1.92, score adattivo RSI",
        "benchmark": "IWMO.MI",
        "n_etf_max": 8,
        "pool": [
            "EUHA.DE", "HYLD.MI",
            "SWDA.MI", "VWCE.DE", "CSSPX.MI", "XDWT.MI", "EXXW.DE", "ESGE.MI",
            "EXX5.DE", "EXV1.DE",
            "IS3N.DE", "VAPX.MI", "JPNH.MI",
            "NTSX.MI", "NTSG.MI", "WRTY.MI", "NTSZ.MI",
            "PHAU.MI", "AIGA.MI", "RARE.MI", "COPA.MI", "CMOD.MI",
            "DFNS.MI", "SMH.MI", "IFFF.MI",
            "JPGL.MI", "IBCZ.DE", "IS07.DE", "FCRN.DE", "IWMO.MI",
            "IWQU.MI", "IEMO.MI", "IEQU.MI", "QDVB.DE",
            "XUTC.MI",
        ],
        "colore": "#DC2626",
    },
}

# ── RANGE ALLOCAZIONE PER REGIME (Fase 2 — futuro) ─────────────────────────
RANGE_REGIME = {
    "goldilocks":   {"M": (5,20),  "O": (15,35), "A": (50,80)},
    "reflazione":   {"M": (10,25), "O": (20,40), "A": (35,65)},
    "stagflazione": {"M": (30,60), "O": (25,45), "A": (0,25)},
    "risk_off":     {"M": (40,70), "O": (20,45), "A": (0,20)},
    "neutro":       {"M": (15,30), "O": (25,40), "A": (30,55)},
}

# ── ETF PROXY per regime ───────────────────────────────────────────────────
ETF_PROXY = {
    "SPY":{"goldilocks":0.9,"reflazione":0.7,"stagflazione":0.1,"risk_off":0.0,"neutro":0.5},
    "QQQ":{"goldilocks":0.9,"reflazione":0.5,"stagflazione":0.0,"risk_off":0.0,"neutro":0.4},
    "IWM":{"goldilocks":0.85,"reflazione":0.8,"stagflazione":0.1,"risk_off":0.0,"neutro":0.4},
    "VGK":{"goldilocks":0.8,"reflazione":0.75,"stagflazione":0.15,"risk_off":0.0,"neutro":0.4},
    "EEM":{"goldilocks":0.7,"reflazione":0.9,"stagflazione":0.2,"risk_off":0.0,"neutro":0.4},
    "EWJ":{"goldilocks":0.7,"reflazione":0.65,"stagflazione":0.2,"risk_off":0.2,"neutro":0.5},
    "TLT":{"goldilocks":0.4,"reflazione":0.1,"stagflazione":0.1,"risk_off":0.9,"neutro":0.5},
    "IEF":{"goldilocks":0.5,"reflazione":0.2,"stagflazione":0.2,"risk_off":0.8,"neutro":0.5},
    "HYG":{"goldilocks":0.85,"reflazione":0.7,"stagflazione":0.1,"risk_off":0.0,"neutro":0.5},
    "LQD":{"goldilocks":0.6,"reflazione":0.4,"stagflazione":0.2,"risk_off":0.3,"neutro":0.5},
    "TIP":{"goldilocks":0.5,"reflazione":0.85,"stagflazione":0.9,"risk_off":0.4,"neutro":0.5},
    "GLD":{"goldilocks":0.3,"reflazione":0.8,"stagflazione":0.9,"risk_off":0.9,"neutro":0.5},
    "USO":{"goldilocks":0.5,"reflazione":0.9,"stagflazione":0.85,"risk_off":0.2,"neutro":0.4},
    "VXX":{"goldilocks":0.0,"reflazione":0.0,"stagflazione":0.4,"risk_off":1.0,"neutro":0.2},
    "UUP":{"goldilocks":0.3,"reflazione":0.2,"stagflazione":0.6,"risk_off":0.7,"neutro":0.5},
}
SCENARI = ["goldilocks","reflazione","stagflazione","risk_off","neutro"]

# ── ETF METADATA ──────────────────────────────────────────────────────────
ETF_CATEGORIA = {
    # Monetario
    "XEON.MI":"monetario","SMART.MI":"monetario","IU0E.MI":"monetario","XEOD.DE":"monetario",
    "IBGS.MI":"obbligaz_ig","XGSH.MI":"obbligaz_ig",
    # Obbligazionario IG
    "IEAC.MI":"obbligaz_ig","IEAG.MI":"obbligaz_ig","JNHD.DE":"obbligaz_ig",
    "XUCS.DE":"obbligaz_ig","IBGM.MI":"obbligaz_ig","EUNH.DE":"obbligaz_ig","SEGA.MI":"obbligaz_ig",
    # HY
    "IHYU.MI":"hy","EUHI.MI":"hy","HYLD.MI":"hy","STHE.MI":"hy","STHY.MI":"hy","EUHA.DE":"hy",
    # EM Bond
    "EMBE.MI":"em_bond","EMDV.MI":"em_bond",
    # Azionario globale
    "SWDA.MI":"az_globale","VWCE.DE":"az_globale","CSSPX.MI":"az_globale",
    "ESGE.MI":"az_globale","NTSX.MI":"az_globale","NTSG.MI":"az_globale",
    "XDWT.MI":"az_globale","IWMO.MI":"az_globale","JPGL.MI":"az_globale",
    "IBCZ.DE":"az_globale","IS07.DE":"az_globale","FCRN.DE":"az_globale",
    "IWQU.MI":"az_globale",
    # Azionario Europa
    "EXX5.DE":"az_europa","EXV1.DE":"az_europa","EXXW.DE":"az_europa",
    "IEMO.MI":"az_europa","IEQU.MI":"az_europa",
    # Azionario USA
    "QDVB.DE":"az_usa",
    # Azionario EM/Asia
    "IS3N.DE":"az_em","VAPX.MI":"az_em","JPNH.MI":"az_em","NTSZ.MI":"az_em","WRTY.MI":"az_usa",
    # Tematici
    "SMH.MI":"tematico","DFNS.MI":"tematico","IFFF.MI":"tematico",
    "PHAU.MI":"tematico","AIGA.MI":"tematico","RARE.MI":"tematico",
    "COPA.MI":"tematico","CMOD.MI":"tematico","XUTC.MI":"tematico",
    # Multi-asset difensivo
    "VAGE.DE":"obbligaz_ig","EMBE.MI":"em_bond","EUHA.DE":"hy","JNHD.DE":"obbligaz_ig",
}

ETF_NOMI = {
    "XEON.MI":"Xtrackers II EUR Overnight Rate Swap UCITS ETF 1C","SMART.MI":"iShares EUR Ultrashort Bond",
    "IU0E.MI":"iShares EUR Ultrashort Bond Dist","XGSH.MI":"Xtrackers II Global Govt Bond EUR Hdg",
    "IBGS.MI":"iShares EUR Govt Bond 1-3yr",
    "IEAC.MI":"iShares Core EUR Corp Bond","IEAG.MI":"iShares Core EUR Aggregate Bond",
    "JNHD.DE":"JPMorgan EUR Corp Bond Research Enhanced","XUCS.DE":"Xtrackers USD Corp Bond EUR Hdg",
    "IBGM.MI":"iShares EUR Govt Bond 7-10yr","EUNH.DE":"iShares Core EUR Govt Bond",
    "SEGA.MI":"iShares EUR Govt Bond 1-5yr",
    "IHYU.MI":"iShares USD High Yield Corp Bond EUR Hdg","EUHI.MI":"PIMCO Euro Short HY",
    "HYLD.MI":"iShares EUR High Yield Corp Bond","STHE.MI":"SPDR Bloomberg 0-3Y EUR HY Corp Bond",
    "STHY.MI":"PIMCO US Short-Term High Yield Corp Bond UCITS ETF",
    "EMBE.MI":"iShares JPM EM Bond EUR Hdg","EMDV.MI":"iShares JPM EM Local Govt Bond",
    "SWDA.MI":"iShares Core MSCI World","VWCE.DE":"Vanguard FTSE All-World Acc",
    "CSSPX.MI":"iShares Core S&P 500 Acc","XDWT.MI":"Xtrackers MSCI World Swap",
    "EXXW.DE":"iShares MSCI Europe","EXX5.DE":"iShares EURO STOXX 50",
    "EXV1.DE":"iShares STOXX Europe 600","ESGE.MI":"iShares MSCI World ESG Enhanced",
    "IS3N.DE":"iShares MSCI EM Small Cap","VAPX.MI":"Vanguard Dev Asia Pacific",
    "JPNH.MI":"Amundi MSCI Japan EUR Hdg","NTSX.MI":"WisdomTree US Efficient Core",
    "NTSG.MI":"WisdomTree Global Efficient Core","NTSZ.MI":"WisdomTree Eurozone Efficient Core",
    "WRTY.MI":"WisdomTree Russell 2000","PHAU.MI":"WisdomTree Physical Gold",
    "AIGA.MI":"WisdomTree Agriculture","RARE.MI":"VanEck Rare Earth & Strategic Metals",
    "COPA.MI":"WisdomTree Copper","CMOD.MI":"iShares Diversified Commodity Swap",
    "DFNS.MI":"VanEck Defense UCITS ETF","SMH.MI":"VanEck Semiconductor UCITS ETF",
    "IFFF.MI":"iShares MSCI Global Financials","JPGL.MI":"JPMorgan Global Equity Multi-Factor",
    "IBCZ.DE":"iShares STOXX World Multifactor","IS07.DE":"iShares STOXX World Multifactor EUR Hdg",
    "FCRN.DE":"iShares World Equity Factor Rotation Active","IWMO.MI":"iShares MSCI World Momentum",
    "IWQU.MI":"iShares MSCI World Quality Factor","IEMO.MI":"iShares MSCI Europe Momentum Factor",
    "IEQU.MI":"iShares MSCI Europe Quality Factor","QDVB.DE":"iShares MSCI USA Quality Factor",
    "VAGE.DE":"Vanguard EUR Aggregate Bond","EUHA.DE":"iShares EUR High Yield Corp Bond",
    "XUTC.MI":"Xtrackers MSCI USA IT 1D",
}

ETF_TIPO = {
    "XEON.MI":"Acc","SMART.MI":"Acc","IU0E.MI":"Dist","XGSH.MI":"Acc","IBGS.MI":"Dist",
    "IEAC.MI":"Dist","IEAG.MI":"Dist","JNHD.DE":"Dist","XUCS.DE":"Acc",
    "IBGM.MI":"Dist","EUNH.DE":"Dist","SEGA.MI":"Dist",
    "IHYU.MI":"Dist","EUHI.MI":"Dist","HYLD.MI":"Acc","STHE.MI":"Dist","STHY.MI":"Dist",
    "EMBE.MI":"Dist","EMDV.MI":"Dist",
    "SWDA.MI":"Acc","VWCE.DE":"Acc","CSSPX.MI":"Acc","XDWT.MI":"Acc",
    "EXXW.DE":"Dist","EXX5.DE":"Dist","EXV1.DE":"Dist","ESGE.MI":"Acc",
    "IS3N.DE":"Acc","VAPX.MI":"Dist","JPNH.MI":"Dist",
    "NTSX.MI":"Acc","NTSG.MI":"Acc","NTSZ.MI":"Acc","WRTY.MI":"Acc",
    "PHAU.MI":"Acc","AIGA.MI":"Acc","RARE.MI":"Acc","COPA.MI":"Acc","CMOD.MI":"Acc",
    "DFNS.MI":"Acc","SMH.MI":"Acc","IFFF.MI":"Dist",
    "JPGL.MI":"Acc","IBCZ.DE":"Acc","IS07.DE":"Acc","FCRN.DE":"Acc","IWMO.MI":"Acc",
    "IWQU.MI":"Acc","IEMO.MI":"Acc","IEQU.MI":"Acc","QDVB.DE":"Acc",
    "VAGE.DE":"Dist","EUHA.DE":"Dist","JNHD.DE":"Dist","XUTC.MI":"Dist",
}

LEVA_TICKERS = set()  # nessuna leva nella strategia 3 linee

PERSISTENZA = {"settimane": 2, "soglia": 60}  # più conservativa di factor

# ── SCORE WEIGHTS per linea ────────────────────────────────────────────────
# Linea M: non usa score momentum — solo yield
# Linea O: momentum moderato, duration e credito pesano
# Linea A: identico ad A7 factor
SCORE_WEIGHTS = {
    "O": {"sma200":15,"kama":15,"mom1m":10,"mom3m":30,"mom6m":20,"rsi":5,"adx":5,"ao":0},
    "A": {"sma200":10,"kama":10,"mom1m":15,"mom3m":35,"mom6m":20,"rsi":5,"adx":5,"ao":0},
}

ROTATION_THRESHOLD = {"M": 999, "O": 12.0, "A": 15.0}  # M non ruota

# ── YAHOO FINANCE ──────────────────────────────────────────────────────────
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

# ── INDICATORI TECNICI ─────────────────────────────────────────────────────
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

def calc_total_return(price_entry, price_now, yield_pct, days):
    if not price_entry or price_entry == 0: return 0.0
    price_ret = (price_now-price_entry)/price_entry*100
    yield_ret = (yield_pct/365)*days if yield_pct else 0.0
    return round(price_ret+yield_ret, 3)

# ── SCORE ADATTIVO RSI ─────────────────────────────────────────────────────
def calc_rsi_excess_multiplier(rsi14):
    """
    Moltiplicatore momentum basato sul RSI.
    Zona 1 (RSI 45-72): 1.0 — momentum sano
    Zona 2 (RSI 73-82): 0.7 — eccesso moderato
    Zona 3 (RSI  > 82): 0.4 — eccesso estremo
    """
    if rsi14 is None: return 1.0
    if rsi14 > 82: return 0.4
    if rsi14 > 72: return 0.7
    if rsi14 >= 45: return 1.0
    return 0.8

def calc_score(closes, linea_id, yield_pct=0):
    """
    Score per selezione ETF.
    Linea M: basato su yield (non momentum)
    Linea O/A: momentum adattivo con penalità RSI eccesso
    """
    if not closes or len(closes) < 35: return 0

    # Linea M — score basato su yield e stabilità
    if linea_id == "M":
        if len(closes) < 5: return 0
        # Stabilità: bassa volatilità degli ultimi 30gg
        last30 = closes[-30:] if len(closes) >= 30 else closes
        rend = [(last30[i]-last30[i-1])/last30[i-1] for i in range(1,len(last30)) if last30[i-1]]
        if not rend: return 0
        vol = (sum(r**2 for r in rend)/len(rend))**0.5 * (252**0.5) * 100
        # Score: più bassa la volatilità meglio è, più alto il yield meglio è
        score_stab = max(0, 50 - vol*10)  # 0% vol → 50pt, 5% vol → 0pt
        score_yield = min(50, yield_pct * 10)  # 5% yield → 50pt
        return min(100, max(0, round(score_stab + score_yield)))

    w = SCORE_WEIGHTS.get(linea_id,
        {"sma200":10,"kama":10,"mom1m":15,"mom3m":35,"mom6m":20,"rsi":5,"adx":5,"ao":0})

    price  = closes[-1]
    sma200 = calc_sma(closes, 200)
    kama   = calc_kama(closes)
    mom1m  = calc_momentum(closes, min(21, len(closes)-1))
    mom3m  = calc_momentum(closes, min(63, len(closes)-1))
    mom6m  = calc_momentum(closes, min(126, len(closes)-1))
    rsi14  = calc_rsi(closes, 14)
    adx14  = calc_adx(closes, 14)

    mom_mult = calc_rsi_excess_multiplier(rsi14)
    s = 0

    if w["sma200"] > 0 and sma200:
        s += w["sma200"] if price > sma200 else 0
    if w["kama"] > 0 and kama:
        s += w["kama"] if price > kama else 0

    if w["mom1m"] > 0 and mom1m is not None:
        w1 = w["mom1m"] * mom_mult
        s += round(min(w1, (mom1m/20)*w1)) if mom1m >= 0 else round(max(-w1/2, (mom1m/20)*w1))

    if w["mom3m"] > 0 and mom3m is not None:
        w3 = w["mom3m"] * mom_mult
        s += round(min(w3, (mom3m/30)*w3)) if mom3m >= 0 else round(max(-w3/2, (mom3m/30)*w3))

    if w["mom6m"] > 0 and mom6m is not None:
        w6 = w["mom6m"] * mom_mult
        s += round(min(w6, (mom6m/50)*w6)) if mom6m >= 0 else round(max(-w6/2, (mom6m/50)*w6))

    if w["rsi"] > 0 and rsi14 is not None:
        if 55 <= rsi14 <= 72: s += w["rsi"]
        elif 45 <= rsi14 < 55: s += round(w["rsi"]*0.5)
        elif 73 <= rsi14 <= 82: s += round(w["rsi"]*0.3)

    if w["adx"] > 0 and adx14 is not None:
        if adx14 >= 30: s += w["adx"]
        elif adx14 >= 20: s += round(w["adx"]*0.6)
        elif adx14 >= 15: s += round(w["adx"]*0.3)

    return min(100, max(0, s))

def calc_score_storico(ticker, target_date, linea_id, etf_data):
    sig = etf_data.get(ticker)
    if not sig: return 0
    closes = sig.get("closes", [])
    dates  = sig.get("dates", [])
    if not closes or not dates: return 0
    n = min(len(closes), len(dates))
    closes_hist = [closes[i] for i in range(n) if dates[i] <= target_date]
    if len(closes_hist) < 35: return 0
    return calc_score(closes_hist, linea_id, sig.get("yield_pct", 0))

# ── REGIME DAI DATI REALI ──────────────────────────────────────────────────
def classify_regime_at_date(proxy_data, target_date):
    scores = {s: 0.0 for s in SCENARI}

    def get_ret(ticker, days=22):
        d = proxy_data.get(ticker)
        if not d: return None
        closes = d.get("closes", [])
        dates  = d.get("dates", [])
        n = min(len(closes), len(dates))
        cl = [closes[i] for i in range(n) if dates[i] <= target_date]
        if len(cl) < days+1: return None
        old = cl[-(days+1)]
        return (cl[-1]-old)/old*100 if old else None

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
                scores[sc] += norm * ETF_PROXY[t].get(sc, 0) * 15

    spy=get_ret("SPY"); tlt=get_ret("TLT"); gld=get_ret("GLD")
    vxx=get_ret("VXX"); hyg=get_ret("HYG"); lqd=get_ret("LQD")
    uup=get_ret("UUP"); eem=get_ret("EEM"); tip=get_ret("TIP")
    uso=get_ret("USO")

    if spy is not None and tlt is not None:
        diff=spy-tlt
        if diff>5: scores["goldilocks"]+=20; scores["reflazione"]+=10
        elif diff<-5: scores["risk_off"]+=20; scores["neutro"]+=10
    if gld is not None:
        if gld>5: scores["reflazione"]+=15; scores["stagflazione"]+=10; scores["risk_off"]+=8
        elif gld<-3: scores["goldilocks"]+=10
    if uso is not None:
        if uso>8: scores["reflazione"]+=12; scores["stagflazione"]+=8
        elif uso<-8: scores["risk_off"]+=8
    if vxx is not None:
        if vxx>15: scores["risk_off"]+=25; scores["stagflazione"]+=5
        elif vxx<-10: scores["goldilocks"]+=15
    if hyg is not None and lqd is not None:
        diff=hyg-lqd
        if diff>3: scores["goldilocks"]+=12; scores["reflazione"]+=8
        elif diff<-3: scores["risk_off"]+=15; scores["stagflazione"]+=8
    if uup is not None:
        if uup>3: scores["risk_off"]+=10; scores["stagflazione"]+=8
        elif uup<-2: scores["reflazione"]+=10; scores["goldilocks"]+=5
    if eem is not None and spy is not None:
        diff=eem-spy
        if diff>3: scores["reflazione"]+=12
        elif diff<-5: scores["risk_off"]+=8; scores["stagflazione"]+=5
    if tip is not None and tlt is not None:
        diff=tip-tlt
        if diff>2: scores["reflazione"]+=12; scores["stagflazione"]+=8
        elif diff<-2: scores["goldilocks"]+=10; scores["risk_off"]+=5

    total = sum(scores.values())
    if total <= 0:
        norm_scores = {s: 20 for s in SCENARI}
    else:
        norm_scores = {s: round(scores[s]/total*100, 1) for s in SCENARI}

    scenario = max(norm_scores, key=norm_scores.get)
    confidence = round(norm_scores[scenario])
    return scenario, confidence

# ── UTILITÀ ────────────────────────────────────────────────────────────────
def get_price_on_date(ticker, target_date, etf_data):
    sig = etf_data.get(ticker)
    if not sig: return None
    closes = sig.get("closes", [])
    dates  = sig.get("dates", [])
    n = min(len(closes), len(dates))
    best = None
    for i in range(n):
        if dates[i] <= target_date: best = closes[i]
        elif dates[i] > target_date: break
    return best

def get_all_trading_dates(etf_data):
    date_set = set()
    for sig in etf_data.values():
        for d in sig.get("dates", []):
            if d >= BACKTEST_START: date_set.add(d)
    return sorted(date_set)

# ── SELEZIONE ETF ──────────────────────────────────────────────────────────
def seleziona_etf_O(regime, etf_data, target_date, capitale):
    """
    Linea O v2.0 — composizione STATICA per regime.
    Nessuno score momentum — i bond non si selezionano per momentum.
    Logica: cuscinetto anticiclico, cedola >=3% netto, non perde.
    """
    regime_key = regime if regime in COMPOSIZIONE_O_PER_REGIME else "neutro"
    template = COMPOSIZIONE_O_PER_REGIME[regime_key]
    comp = []
    for item in template:
        t = item["ticker"]
        peso = item["peso"]
        sig = etf_data.get(t)
        if not sig or not sig.get("closes"): continue
        price = get_price_on_date(t, target_date, etf_data)
        if not price: continue
        importo = round(capitale * peso / 100, 2)
        yield_stimato = YIELD_STIMATI_O.get(t, 0)
        comp.append({
            "ticker": t,
            "nome": ETF_NOMI.get(t, t),
            "peso": float(peso),
            "importo": importo,
            "price_entry": round(price, 4),
            "quote": round(importo/price, 4) if price else 0,
            "yield_pct": yield_stimato,
            "score_entry": 0,
            "mom3m_entry": 0,
        })
    # Cedola aggregata attesa
    cedola_agg = sum(c["yield_pct"] * c["peso"] / 100 for c in comp)
    return comp, round(cedola_agg, 2)

def seleziona_etf(pool, linea_id, etf_data, n_max, target_date, regime=None, capitale=None):
    """
    Selezione ETF per una linea in una data storica.
    Linea M: score yield+stabilita
    Linea O: composizione statica per regime (v2.0)
    Linea A: score momentum adattivo RSI
    """
    if capitale is None: capitale = CAPITALE

    # Linea O — composizione statica
    if linea_id == "O":
        comp, _ = seleziona_etf_O(regime or "neutro", etf_data, target_date, capitale)
        return comp

    candidati = []
    for t in pool:
        sig = etf_data.get(t)
        if not sig or not sig.get("closes"): continue
        price = get_price_on_date(t, target_date, etf_data)
        if not price: continue

        score = calc_score_storico(t, target_date, linea_id, etf_data)
        if score <= 5: continue

        closes_hist = [sig["closes"][i] for i in range(min(len(sig["closes"]),len(sig["dates"])))
                       if sig["dates"][i] <= target_date]
        mom3m = calc_momentum(closes_hist[-64:], min(63, len(closes_hist)-1)) or 0

        candidati.append({
            "ticker": t, "score": score, "price": price,
            "yield_pct": sig.get("yield_pct", 0), "mom3m": mom3m,
        })

    candidati.sort(key=lambda x: x["score"], reverse=True)
    selected = candidati[:n_max]
    if not selected: return []

    tot = sum(c["score"] for c in selected) or 1
    comp = []
    for c in selected:
        p = round(c["score"]/tot*100, 1)
        imp = round(capitale*p/100, 2)
        comp.append({
            "ticker": c["ticker"],
            "nome": ETF_NOMI.get(c["ticker"], c["ticker"]),
            "peso": p, "importo": imp,
            "price_entry": round(c["price"], 4),
            "quote": round(imp/c["price"], 4) if c["price"] else 0,
            "yield_pct": c["yield_pct"],
            "score_entry": c["score"],
            "mom3m_entry": round(c["mom3m"], 2),
        })

    tot_p = sum(c["peso"] for c in comp)
    diff = round(100-tot_p, 1)
    if diff != 0 and comp:
        comp[0]["peso"] = round(comp[0]["peso"]+diff, 1)
        comp[0]["importo"] = round(capitale*comp[0]["peso"]/100, 2)
    return comp

# ── ROTATION TRIGGER ───────────────────────────────────────────────────────
def check_rotation(composizione, pool, etf_data, linea_id, target_date,
                   giorni_attivi, soglia, regime=None):
    """Rotation trigger: sostituisce l'ETF peggio se c'è candidato migliore di soglia."""
    if giorni_attivi < 30: return None
    if soglia >= 999: return None  # Linea M non ruota

    in_ptf = {c["ticker"]: c for c in composizione}

    def get_mom3m(ticker):
        sig = etf_data.get(ticker)
        if not sig: return None
        closes = sig.get("closes", [])
        dates  = sig.get("dates", [])
        n = min(len(closes), len(dates))
        cl = [closes[i] for i in range(n) if dates[i] <= target_date]
        return calc_momentum(cl, min(63, len(cl)-1))

    mom_ptf = {}
    for t in in_ptf:
        m = get_mom3m(t)
        if m is not None: mom_ptf[t] = m

    if not mom_ptf: return None
    min_ticker = min(mom_ptf, key=mom_ptf.get)
    min_mom = mom_ptf[min_ticker]

    migliore = None; migliore_mom = None; migliore_score = 0
    for t in pool:
        if t in in_ptf: continue
        if linea_id == "O" and regime in ("risk_off","stagflazione"):
            if ETF_CATEGORIA.get(t,"") == "hy": continue
        sig = etf_data.get(t)
        if not sig or not sig.get("closes"): continue
        price = get_price_on_date(t, target_date, etf_data)
        if not price: continue
        m = get_mom3m(t)
        if m is None: continue
        if m > min_mom + soglia:
            score = calc_score_storico(t, target_date, linea_id, etf_data)
            if score > migliore_score:
                migliore = t; migliore_mom = m; migliore_score = score

    if not migliore: return None
    return {
        "out": min_ticker, "in": migliore,
        "mom3m_out": round(min_mom, 2), "mom3m_in": round(migliore_mom, 2),
        "gap": round(migliore_mom-min_mom, 2),
        "motivo": f"rotation: {min_ticker} mom3M {min_mom:.1f}% → {migliore} mom3M {migliore_mom:.1f}% (gap +{migliore_mom-min_mom:.1f}%)"
    }

# ── BACKTEST ───────────────────────────────────────────────────────────────
def run_backtest_linea(linea_id, etf_data, proxy_data):
    """Backtest giornaliero per una singola linea."""
    info = LINEE_INFO[linea_id]
    pool  = info["pool"]
    n_max = info["n_etf_max"]
    soglia_rot = ROTATION_THRESHOLD[linea_id]

    print(f"    Backtest linea {linea_id} ({info['nome']})...")

    all_dates = get_all_trading_dates(etf_data)
    if not all_dates: return []

    comp_v1 = seleziona_etf(pool, linea_id, etf_data, n_max, BACKTEST_START, regime="neutro", capitale=CAPITALE)
    if not comp_v1: return []

    versioni = []
    ver_num = 1
    cap = float(CAPITALE)
    equity_curve = [cap]
    equity_dates = [BACKTEST_START]

    ver_attiva = {
        "versione": ver_num, "linea_id": linea_id,
        "data_apertura": BACKTEST_START, "data_chiusura": None,
        "regime": "neutro", "confidence": 50,
        "trigger_apertura": "inizializzazione",
        "trigger_chiusura": None,
        "capitale_inizio": cap, "capitale_attuale": cap,
        "performance_pct": 0.0, "performance_eur": 0.0,
        "giorni_attivo": 0, "composizione": comp_v1,
        "etf_usciti": [], "aggiornato": BACKTEST_START,
        "_equity": [cap],
    }
    versioni.append(ver_attiva)

    ultimo_check = BACKTEST_START
    regime_attivo = "neutro"; conf_attiva = 50
    settimane_cand = 0; regime_cand = None
    # Linea O usa soglie piu conservative per cambio regime
    pers = {"settimane": O_PERSIST_SETTIMANE, "soglia": O_CONF_MIN_REGIME} if linea_id == "O" else PERSISTENZA

    def apri_nuova(cap_new, data, regime, conf, trigger, reg=None):
        nonlocal ver_num, ver_attiva
        ver_attiva["data_chiusura"] = data
        ver_attiva["trigger_chiusura"] = trigger
        ver_num += 1
        # Passa il capitale reale e il regime — evita doppia scala
        nuova_comp = seleziona_etf(pool, linea_id, etf_data, n_max, data,
                                   regime=reg or regime, capitale=cap_new)
        if not nuova_comp: return
        # NON applicare scala — seleziona_etf gia usa cap_new come base
        ver_attiva = {
            "versione": ver_num, "linea_id": linea_id,
            "data_apertura": data, "data_chiusura": None,
            "regime": regime, "confidence": conf,
            "trigger_apertura": trigger, "trigger_chiusura": None,
            "capitale_inizio": cap_new, "capitale_attuale": cap_new,
            "performance_pct": 0.0, "performance_eur": 0.0,
            "giorni_attivo": 0, "composizione": nuova_comp,
            "etf_usciti": [], "aggiornato": data,
            "_equity": [cap_new],
        }
        versioni.append(ver_attiva)

    for data_corrente in all_dates:
        if data_corrente <= BACKTEST_START: continue

        try:
            d_open = datetime.date.fromisoformat(ver_attiva["data_apertura"])
            d_curr = datetime.date.fromisoformat(data_corrente)
            ver_attiva["giorni_attivo"] = (d_curr-d_open).days
        except Exception:
            pass

        giorni = ver_attiva["giorni_attivo"]
        tot_v = 0.0
        trigger_dd = None

        for etf in ver_attiva["composizione"]:
            t = etf["ticker"]
            if etf.get("importo", 0) <= 0: continue
            price_now = get_price_on_date(t, data_corrente, etf_data)
            if not price_now: price_now = etf.get("price_now", etf["price_entry"])
            pe = etf["price_entry"]
            y  = etf.get("yield_pct", 0)
            tr = calc_total_return(pe, price_now, y, giorni)
            dd = round((price_now-pe)/pe*100, 2) if pe else 0
            etf["price_now"] = round(price_now, 4)
            etf["perf_pct"]  = round(tr, 3)
            etf["perf_eur"]  = round(etf["importo"]*tr/100, 2)
            etf["drawdown_pct"] = dd

            # Soglie drawdown differenziate per linea
            # Linea O floor mensile -3% (cuscinetto non deve mai perdere)
            dd_soglia = {"M": -10, "O": -3, "A": -25}.get(linea_id, -20)
            if dd <= dd_soglia and not trigger_dd:
                trigger_dd = {"ticker": t, "dd": dd,
                              "motivo": f"{t} drawdown {dd:.1f}%"}

            tot_v += etf["importo"] * (1 + tr/100)

        perf_eur = round(tot_v - ver_attiva["capitale_inizio"], 2)
        ver_attiva["capitale_attuale"] = round(tot_v, 2)
        ver_attiva["performance_eur"] = perf_eur
        ver_attiva["performance_pct"] = round(
            perf_eur/ver_attiva["capitale_inizio"]*100, 2)
        ver_attiva["aggiornato"] = data_corrente
        ver_attiva.setdefault("_equity", []).append(round(tot_v, 2))
        equity_curve.append(round(tot_v, 2))
        equity_dates.append(data_corrente)

        # Check regime ogni 5 giorni
        try:
            giorni_check = (datetime.date.fromisoformat(data_corrente) -
                            datetime.date.fromisoformat(ultimo_check)).days
        except Exception:
            giorni_check = 0

        cambio_regime = None
        rotation = None

        if giorni_check >= 5:
            ultimo_check = data_corrente
            sc, conf = classify_regime_at_date(proxy_data, data_corrente)

            if sc != regime_attivo:
                if sc == regime_cand:
                    settimane_cand += 1
                else:
                    regime_cand = sc; settimane_cand = 1
                if (settimane_cand >= pers["settimane"] and
                        conf >= pers["soglia"] and giorni >= 20):
                    cambio_regime = {"da": regime_attivo, "a": sc, "confidence": conf}
                    regime_attivo = sc; conf_attiva = conf
                    settimane_cand = 0; regime_cand = None
            else:
                settimane_cand = 0; regime_cand = None

            # Rotation: solo linea A (M e O non fanno rotation momentum)
            if not trigger_dd and not cambio_regime and linea_id == "A":
                rotation = check_rotation(
                    ver_attiva["composizione"], pool, etf_data,
                    linea_id, data_corrente, giorni,
                    soglia_rot, regime_attivo)

        cap_new = ver_attiva["capitale_attuale"]

        if trigger_dd:
            # Sostituisce ETF in drawdown con il migliore disponibile
            t_out = trigger_dd["ticker"]
            etf_out = next((e for e in ver_attiva["composizione"]
                           if e["ticker"] == t_out), None)
            if etf_out:
                ver_attiva["composizione"] = [
                    e for e in ver_attiva["composizione"] if e["ticker"] != t_out]
                ver_attiva["etf_usciti"].append({
                    "ticker": t_out, "motivo": "drawdown",
                    "data": data_corrente,
                    "price_entry": etf_out["price_entry"],
                    "price_exit": etf_out.get("price_now", etf_out["price_entry"]),
                    "perf_pct": etf_out.get("perf_pct", 0),
                    "perf_eur": etf_out.get("perf_eur", 0),
                    "drawdown_pct": trigger_dd["dd"],
                })
                # Cerca sostituto
                candidati_sost = []
                for t in pool:
                    if t in {e["ticker"] for e in ver_attiva["composizione"]}: continue
                    if t == t_out: continue
                    if linea_id == "O" and regime_attivo in ("risk_off","stagflazione"):
                        if ETF_CATEGORIA.get(t,"") == "hy": continue
                    pr = get_price_on_date(t, data_corrente, etf_data)
                    if not pr: continue
                    sc_sost = calc_score_storico(t, data_corrente, linea_id, etf_data)
                    if sc_sost > 10:
                        candidati_sost.append((t, sc_sost, pr))
                if candidati_sost:
                    candidati_sost.sort(key=lambda x: x[1], reverse=True)
                    t_in, sc_in, pr_in = candidati_sost[0]
                    ver_attiva["composizione"].append({
                        "ticker": t_in, "nome": ETF_NOMI.get(t_in, t_in),
                        "peso": etf_out["peso"], "importo": etf_out["importo"],
                        "price_entry": round(pr_in, 4),
                        "quote": round(etf_out["importo"]/pr_in, 4),
                        "yield_pct": etf_data.get(t_in, {}).get("yield_pct", 0),
                        "score_entry": sc_in, "mom3m_entry": 0,
                    })

        elif cambio_regime:
            apri_nuova(cap_new, data_corrente, cambio_regime["a"],
                       cambio_regime["confidence"],
                       f"cambio_regime {cambio_regime['da']}→{cambio_regime['a']} "
                       f"(conf {cambio_regime['confidence']}%)",
                       cambio_regime["a"])

        elif rotation:
            t_out = rotation["out"]; t_in = rotation["in"]
            etf_out_obj = next((e for e in ver_attiva["composizione"]
                               if e["ticker"] == t_out), None)
            pr_in = get_price_on_date(t_in, data_corrente, etf_data)
            if etf_out_obj and pr_in:
                ver_attiva["etf_usciti"].append({
                    "ticker": t_out, "motivo": "rotation_momentum",
                    "data": data_corrente,
                    "price_entry": etf_out_obj["price_entry"],
                    "price_exit": etf_out_obj.get("price_now", etf_out_obj["price_entry"]),
                    "perf_pct": etf_out_obj.get("perf_pct", 0),
                    "perf_eur": etf_out_obj.get("perf_eur", 0),
                    "drawdown_pct": etf_out_obj.get("drawdown_pct", 0),
                    "mom3m_out": rotation["mom3m_out"],
                    "mom3m_in": rotation["mom3m_in"],
                    "replaced_by": t_in,
                })
                imp_libero = etf_out_obj["importo"]
                ver_attiva["composizione"] = [
                    e for e in ver_attiva["composizione"] if e["ticker"] != t_out]
                sc_in = calc_score_storico(t_in, data_corrente, linea_id, etf_data)
                m3_in = calc_momentum(
                    [etf_data[t_in]["closes"][i]
                     for i in range(min(len(etf_data[t_in]["closes"]),
                                       len(etf_data[t_in]["dates"])))
                     if etf_data[t_in]["dates"][i] <= data_corrente][-64:],
                    min(63, len([etf_data[t_in]["closes"][i]
                     for i in range(min(len(etf_data[t_in]["closes"]),
                                       len(etf_data[t_in]["dates"])))
                     if etf_data[t_in]["dates"][i] <= data_corrente])-1)
                ) if t_in in etf_data else 0
                ver_attiva["composizione"].append({
                    "ticker": t_in, "nome": ETF_NOMI.get(t_in, t_in),
                    "peso": etf_out_obj["peso"], "importo": imp_libero,
                    "price_entry": round(pr_in, 4),
                    "quote": round(imp_libero/pr_in, 4),
                    "yield_pct": etf_data.get(t_in, {}).get("yield_pct", 0),
                    "score_entry": sc_in, "mom3m_entry": round(m3_in or 0, 2),
                })

    # Rendimenti mensili
    rend_mensili = {}
    if len(equity_curve) > 1:
        mesi_vals = {}
        for d, v in zip(equity_dates, equity_curve):
            ym = d[:7]
            mesi_vals.setdefault(ym, []).append(v)
        prev = CAPITALE
        for ym in sorted(mesi_vals.keys()):
            vals = mesi_vals[ym]
            if not vals: continue
            last = vals[-1]
            r = round((last-prev)/prev*100, 2) if prev else 0
            rend_mensili[ym] = r
            prev = last

    # Diario movimenti
    diario = []
    for i in range(1, len(versioni)):
        v_old = versioni[i-1]; v_new = versioni[i]
        comp_old = {e["ticker"]: e for e in v_old.get("composizione", [])}
        comp_new = {e["ticker"]: e for e in v_new.get("composizione", [])}
        venduti = [{"ticker":t,"nome":e.get("nome",t),"peso":e.get("peso",0),
                    "importo":e.get("importo",0),"price_entry":e.get("price_entry",0),
                    "price_exit":e.get("price_now",e.get("price_entry",0)),
                    "perf_pct":e.get("perf_pct",0),"perf_eur":e.get("perf_eur",0)}
                   for t,e in comp_old.items()
                   if t not in comp_new and e.get("importo",0)>0]
        acquistati = [{"ticker":t,"nome":e.get("nome",t),"peso":e.get("peso",0),
                       "importo":e.get("importo",0),"price_entry":e.get("price_entry",0),
                       "score_entry":e.get("score_entry",0),"mom3m_entry":e.get("mom3m_entry",0)}
                      for t,e in comp_new.items()
                      if t not in comp_old and e.get("importo",0)>0]
        diario.append({
            "data": v_new.get("data_apertura"),
            "versione_da": v_old.get("versione"),
            "versione_a": v_new.get("versione"),
            "trigger": v_new.get("trigger_apertura","—"),
            "regime": v_new.get("regime","—"),
            "capitale_ribilanciamento": v_new.get("capitale_inizio",0),
            "venduti": venduti, "acquistati": acquistati,
        })

    vol_tot    = calc_volatilita(equity_curve)
    mdd_tot    = calc_max_drawdown(equity_curve)
    sharpe_tot = calc_sharpe(equity_curve)

    for v in versioni:
        eq = v.pop("_equity", [])
        v["volatilita_ann"] = calc_volatilita(eq)
        v["max_drawdown"]   = calc_max_drawdown(eq)
        v["sharpe"]         = calc_sharpe(eq)

    n_ver = len(versioni)
    n_reb = n_ver - 1
    cap_fin = ver_attiva.get("capitale_attuale", CAPITALE)
    perf_fin = round((cap_fin-CAPITALE)/CAPITALE*100, 2)
    n_rot = sum(len([u for u in v.get("etf_usciti",[])
                     if u.get("motivo")=="rotation_momentum"])
                for v in versioni)

    vol_str = f", vol {vol_tot:.1f}%, MDD {mdd_tot:.1f}%, Sharpe {sharpe_tot:.2f}" \
              if vol_tot else ""
    print(f"      → {n_ver} ver, {n_reb} rib, {n_rot} rot, perf {perf_fin:+.1f}%{vol_str}")

    return {
        "storia": versioni,
        "performance_totale_pct": perf_fin,
        "performance_totale_eur": round(cap_fin-CAPITALE, 2),
        "capitale_attuale": cap_fin,
        "volatilita_ann": vol_tot,
        "max_drawdown": mdd_tot,
        "sharpe": sharpe_tot,
        "rendimenti_mensili": rend_mensili,
        "diario_movimenti": diario,
        "versione_corrente": n_ver,
        "benchmark": info["benchmark"],
        "rotation_suggerita": None,
    }

# ── MAIN ───────────────────────────────────────────────────────────────────
def main():
    oggi = datetime.date.today().isoformat()
    print(f"COMPASS 3 Linee v1.0 — {oggi}")
    print(f"Linee: {LINEE} · Capitale: €{CAPITALE:,} per linea")

    existing = {}
    if OUT_FILE.exists():
        try:
            with open(OUT_FILE) as f:
                existing = json.load(f)
            print(f"  Dati esistenti: {existing.get('generated','?')}")
        except Exception:
            pass

    run_number = existing.get("run_number", 0) + 1
    portafogli_esistenti = existing.get("portafogli_3linee", {})

    # ── 1. Scarica proxy ──────────────────────────────────────────────────
    print(f"\n[1/4] Download {len(ETF_PROXY)} ETF proxy...")
    proxy_data = {}
    for t in ETF_PROXY:
        print(f"  {t}...", end=" ", flush=True)
        raw = fetch_yahoo(t, "2y")
        proxy_data[t] = raw
        print("OK" if raw else "ERR")
        time.sleep(0.3)

    sc_oggi, conf_oggi = classify_regime_at_date(proxy_data, oggi)
    print(f"\n  Regime oggi: {sc_oggi} ({conf_oggi}%)")

    # ── 2. Scarica ETF universo ────────────────────────────────────────────
    tutti_ticker = set()
    for info in LINEE_INFO.values():
        tutti_ticker.update(info["pool"])
        tutti_ticker.add(info["benchmark"])

    print(f"\n[2/4] Download {len(tutti_ticker)} ETF universo 3 linee...")
    etf_data = {}
    success = 0; errors = 0

    for i, ticker in enumerate(sorted(tutti_ticker)):
        print(f"  [{i+1}/{len(tutti_ticker)}] {ticker}...", end=" ", flush=True)
        raw = fetch_yahoo(ticker, "2y")
        if not raw:
            errors += 1
            etf_data[ticker] = {"ticker": ticker, "closes": [], "dates": [],
                                 "yield_pct": 0, "price": None}
            print("ERR")
        else:
            etf_data[ticker] = {
                "ticker": ticker,
                "closes": raw["closes"],
                "dates": raw.get("dates", []),
                "yield_pct": raw["yield_pct"],
                "price": round(raw["current_price"], 4),
            }
            success += 1
            score_oggi = calc_score(raw["closes"], "O", raw["yield_pct"])
            print(f"OK score={score_oggi}")
        time.sleep(0.35)

    print(f"  Download: {success} OK, {errors} ERR")

    # Aggiungi proxy al etf_data per regime storico
    for t, d in proxy_data.items():
        if d and t not in etf_data:
            etf_data[t] = d

    # ── 3. Backtest 3 linee ───────────────────────────────────────────────
    print(f"\n[3/4] Backtest 3 linee (da {BACKTEST_START})...")
    portafogli = {}

    for linea_id in LINEE:
        if linea_id in portafogli_esistenti:
            # Aggiorna solo la versione attiva
            ptf = portafogli_esistenti[linea_id]
            storia = ptf.get("storia", [])
            ver_att = next((v for v in reversed(storia)
                           if v.get("data_chiusura") is None), None)
            if ver_att:
                tot_v = 0.0
                for etf in ver_att.get("composizione", []):
                    t = etf["ticker"]
                    price_now = etf_data.get(t, {}).get("price")
                    if not price_now: continue
                    pe = etf["price_entry"]
                    giorni = (datetime.date.fromisoformat(oggi) -
                              datetime.date.fromisoformat(ver_att["data_apertura"])).days
                    tr = calc_total_return(pe, price_now, etf.get("yield_pct",0), giorni)
                    etf["price_now"] = round(price_now, 4)
                    etf["perf_pct"]  = round(tr, 3)
                    etf["perf_eur"]  = round(etf["importo"]*tr/100, 2)
                    etf["drawdown_pct"] = round((price_now-pe)/pe*100,2) if pe else 0
                    tot_v += etf["importo"] * (1+tr/100)
                ver_att["capitale_attuale"] = round(tot_v, 2)
                ver_att["performance_eur"]  = round(tot_v-ver_att["capitale_inizio"], 2)
                ver_att["performance_pct"]  = round(
                    (tot_v-ver_att["capitale_inizio"])/ver_att["capitale_inizio"]*100, 2)
                ver_att["aggiornato"] = oggi

                # Check rotation live
                info = LINEE_INFO[linea_id]
                rot = check_rotation(
                    ver_att["composizione"], info["pool"], etf_data,
                    linea_id, oggi,
                    (datetime.date.fromisoformat(oggi) -
                     datetime.date.fromisoformat(ver_att["data_apertura"])).days,
                    ROTATION_THRESHOLD[linea_id], sc_oggi)

                cap_fin = ver_att["capitale_attuale"]
                perf_fin = round((cap_fin-CAPITALE)/CAPITALE*100, 2)
                portafogli[linea_id] = portafogli_esistenti[linea_id]
                portafogli[linea_id]["performance_totale_pct"] = perf_fin
                portafogli[linea_id]["performance_totale_eur"] = round(cap_fin-CAPITALE,2)
                portafogli[linea_id]["capitale_attuale"] = cap_fin
                portafogli[linea_id]["rotation_suggerita"] = rot
                print(f"    Linea {linea_id}: aggiornato, perf {perf_fin:+.1f}%"
                      + (f" | ROTATION: {rot['out']}→{rot['in']}" if rot else ""))
                continue

        # Backtest da zero
        risultato = run_backtest_linea(linea_id, etf_data, proxy_data)
        if risultato:
            portafogli[linea_id] = risultato

    # ── 4. Benchmark performance ──────────────────────────────────────────
    print(f"\n[4/4] Benchmark performance...")
    benchmarks = {}
    for linea_id, info in LINEE_INFO.items():
        bm = info["benchmark"]
        sig = etf_data.get(bm)
        if not sig or not sig.get("closes") or not sig.get("dates"):
            benchmarks[bm] = {"ticker": bm, "perf_pct": None}
            continue
        p_start = get_price_on_date(bm, BACKTEST_START, etf_data)
        p_now   = sig["closes"][-1] if sig["closes"] else None
        perf = round((p_now-p_start)/p_start*100, 2) if p_start and p_now else None
        benchmarks[bm] = {"ticker": bm, "nome": ETF_NOMI.get(bm, bm), "perf_pct": perf}
        print(f"  {bm} ({info['nome']}): {'+' if perf and perf>=0 else ''}{perf:.1f}%"
              if perf else f"  {bm}: N/D")

    # Outperformance
    for linea_id, ptf in portafogli.items():
        bm = LINEE_INFO[linea_id]["benchmark"]
        bm_perf = benchmarks.get(bm, {}).get("perf_pct")
        ptf_perf = ptf.get("performance_totale_pct")
        if ptf_perf is not None and bm_perf is not None:
            ptf["outperformance_bm"] = round(ptf_perf - bm_perf, 2)
            ptf["batte_benchmark"] = ptf_perf > bm_perf

    # ── Output ────────────────────────────────────────────────────────────
    # Score correnti
    etf_scores = {}
    for t, d in etf_data.items():
        if not d.get("closes") or not d.get("price"): continue
        m3 = calc_momentum(d["closes"], min(63, len(d["closes"])-1))
        m1 = calc_momentum(d["closes"], min(21, len(d["closes"])-1))
        etf_scores[t] = {
            "ticker": t, "nome": ETF_NOMI.get(t, t),
            "price": d["price"],
            "mom1m": m1, "mom3m": m3,
            "yield_pct": d.get("yield_pct", 0),
            "score_M": calc_score(d["closes"], "M", d.get("yield_pct", 0)),
            "score_O": calc_score(d["closes"], "O", d.get("yield_pct", 0)),
            "score_A": calc_score(d["closes"], "A", d.get("yield_pct", 0)),
        }

    # Composizione ideale linea O per regime corrente
    comp_O_ideale, cedola_O_ideale = seleziona_etf_O(sc_oggi, etf_data, oggi, CAPITALE)
    composizione_ideale = {
        "O": {
            "regime": sc_oggi,
            "composizione": comp_O_ideale,
            "cedola_aggregata_netta_pct": cedola_O_ideale,
            "cedola_ok": cedola_O_ideale >= O_CEDOLA_MIN_NETTA,
            "logica": "Composizione statica per regime — non perde, cedola >=3% netto",
        }
    }

    output = {
        "generated": datetime.datetime.utcnow().isoformat(),
        "version": "3linee_2.0",
        "run_number": run_number,
        "strategy": "3 Linee — Monetario / Obbligazionario / Azionario",
        "fase": "1 — portafogli separati",
        "backtest_start": BACKTEST_START,
        "regime_oggi": {"scenario": sc_oggi, "confidence": conf_oggi},
        "range_regime": RANGE_REGIME,
        "composizione_O_per_regime": {
            sc: [{"ticker":e["ticker"],"peso":e["peso"],"yield_stimato":YIELD_STIMATI_O.get(e["ticker"],0)}
                 for e in etf_list]
            for sc, etf_list in COMPOSIZIONE_O_PER_REGIME.items()
        },
        "composizione_ideale": composizione_ideale,
        "portafogli_3linee": portafogli,
        "benchmarks": benchmarks,
        "etf_scores": etf_scores,
    }

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

    size = OUT_FILE.stat().st_size / 1024
    print(f"\n✅ Done → {OUT_FILE} ({size:.0f} KB)")
    print(f"   Run: {run_number} | Regime: {sc_oggi} ({conf_oggi}%)")
    print(f"\n   Risultati 3 Linee vs benchmark:")
    for lid, ptf in portafogli.items():
        perf = ptf.get("performance_totale_pct")
        out  = ptf.get("outperformance_bm")
        batte = ptf.get("batte_benchmark", False)
        flag = "✅" if batte else "❌"
        bm = LINEE_INFO[lid]["benchmark"]
        print(f"   {flag} Linea {lid} ({LINEE_INFO[lid]['nome']}): "
              f"{perf:+.1f}% | vs {bm}: {out:+.1f}pp"
              if perf is not None and out is not None
              else f"   Linea {lid}: N/D")

if __name__ == "__main__":
    main()
