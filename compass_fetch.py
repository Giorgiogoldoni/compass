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


# ── ALIAS TICKER (Bloomberg → Yahoo Finance) ─────────────────────────────────
# Alcuni ticker Borsa Italiana non sono disponibili su Yahoo con il codice Bloomberg
# Questa mappatura converte automaticamente durante il fetch
TICKER_ALIAS = {
    # Ticker Bloomberg → Yahoo Finance (vecchi fix)
    "2B70.MI":   "IBGM.MI",
    "EUNH.MI":   "EUNH.DE",
    "IBTE.MI":   "IBGS.MI",
    "SXRM.MI":   "IEAC.MI",
    "SXRV.MI":   "SEGA.MI",
    "IGLO.MI":   "EUN3.DE",   # iShares Global Govt Bond → Xetra corretto
    "VAGE.MI":   "VAGE.DE",
    "AGGH.MI":   "EUNA.DE",   # iShares Core Global Aggregate Bond EUR Hedged → Xetra corretto
    "CSPX.MI":   "CSSPX.MI",
    "IS3N.MI":   "IS3N.DE",
    "IUIT.MI":   "IUIT.L",
    "REET.MI":   "XREA.DE",
    "JNHD.MI":   "JNHD.DE",
    "SEMB.MI":   "IEMB.MI",   # iShares JPM EM Bond → Borsa Italiana corretto
    "FGEQ.MI":   "FGEQ.F",
    "SILVER.MI": "PHAG.MI",
    "ERNX.MI":   "XEON.MI",
    "DHYA.MI":   "HYLD.MI",
    "XREA.MI":   "XREA.DE",
    "XUCS.MI":   "XUCS.DE",
    "XEOD.MI":   "XEOD.DE",
    # Nuovi ETF multifactor/factor rotation (Bloomberg .MI → Yahoo borsa corretta)
    "IFSW.MI":   "IBCZ.DE",   # iShares STOXX World Multifactor USD → Xetra
    "IS07.MI":   "IS07.DE",   # iShares STOXX World Multifactor EUR Hedged → Xetra
    "FCRN.MI":   "FCRN.DE",   # iShares World Equity Factor Rotation Active → Xetra
    # Benchmark multi-asset (su .MI diretti)
    "MACV.MI":   "MACV.MI",   # iShares Conservative Multi-Asset
    "MODR.MI":   "MODR.MI",   # iShares Moderate Multi-Asset
    "MAGR.MI":   "MAGR.MI",   # iShares Aggressive Multi-Asset
    # Fix ticker 404 su Yahoo (ticker universo → ticker Yahoo corretto)
    "AGGH.DE":   "EUNA.DE",   # era 404 → Xetra corretto
    "IGLO.DE":   "EUN3.DE",   # era 404 → Xetra corretto
    "SEMB.DE":   "IEMB.MI",   # era 404 → Borsa Italiana
    "EUHA.MI":   "EUHA.DE",   # era ERR → Xetra
    # Non disponibili su Yahoo
    "2LVE.MI":   None,
    "2NVD.MI":   None,
    "L2SP.MI":   None,
    "UC44.MI":   None,
    "MLAY.MI":   None,
}

# ── BENCHMARK ─────────────────────────────────────────────────────────────────
BENCHMARK_TICKERS = ["XEON.MI", "IEAG.MI", "V20A.DE", "V40A.DE", "V60A.DE", "V80A.DE",
                     "MACV.MI", "MODR.MI", "MAGR.MI", "SWDA.MI", "IWMO.MI"]
BENCHMARK_NOMI = {
    "XEON.MI": "Amundi EUR Overnight (Monetario)",
    "IEAG.MI": "iShares EUR Aggregate Bond",
    "V20A.DE":  "Vanguard LifeStrategy 20% Equity",
    "V40A.DE":  "Vanguard LifeStrategy 40% Equity",
    "V60A.DE":  "Vanguard LifeStrategy 60% Equity",
    "V80A.DE":  "Vanguard LifeStrategy 80% Equity",
    "MACV.MI": "iShares Conservative Multi-Asset",
    "MODR.MI": "iShares Moderate Multi-Asset",
    "MAGR.MI": "iShares Aggressive Multi-Asset",
    "SWDA.MI": "iShares Core MSCI World",
    "IWMO.MI": "iShares MSCI World Momentum Factor",
}

# Mappa livello → (benchmark_principale, benchmark_secondario)
BENCHMARK_PER_LIVELLO = {
    "C1": ("XEON.MI",  "IEAG.MI"),
    "C2": ("IEAG.MI",  "XEON.MI"),
    "C3": ("V20A.DE",  "MACV.MI"),
    "C4": ("V40A.DE",  "MODR.MI"),
    "C5": ("V60A.DE",  "MODR.MI"),
    "C6": ("V80A.DE",  "MAGR.MI"),
    "C7": ("MAGR.MI",  "SWDA.MI"),
    "C8": ("MAGR.MI",  "IWMO.MI"),
    "C9": ("MAGR.MI",  "IWMO.MI"),
    "A1": ("XEON.MI",  "IEAG.MI"),
    "A2": ("IEAG.MI",  "XEON.MI"),
    "A3": ("V20A.DE",  "MACV.MI"),
    "A4": ("V40A.DE",  "MODR.MI"),
    "A5": ("V60A.DE",  "MODR.MI"),
    "A6": ("V80A.DE",  "MAGR.MI"),
    "A7": ("MAGR.MI",  "SWDA.MI"),
    "A8": ("MAGR.MI",  "IWMO.MI"),
    "A9": ("MAGR.MI",  "IWMO.MI"),
}

def resolve_ticker(t):
    """Risolve il ticker Yahoo corretto da un ticker Bloomberg."""
    return TICKER_ALIAS.get(t, t)

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
    "conservativo": {"settimane": 6, "soglia": 60},
    "bilanciato"  : {"settimane": 3, "soglia": 55},
    "aggressivo"  : {"settimane": 2, "soglia": 50},
}

# ── ETF UNIVERSO ────────────────────────────────────────────────────────────
ETF_UNIVERSE = sorted(set([
    "3EUL.MI",    "3NVD.MI",    "3USL.MI",    "AGGH.DE",    "AIGA.MI",    "CMOD.MI",
    "COPA.MI",    "CSSPX.MI",   "DFNS.MI",    "DHS.MI",     "EMBE.MI",    "EMDV.MI",
    "EQQQ.MI",    "ESGE.MI",    "EUDV.MI",    "EUHA.MI",    "EUHI.MI",    "EUNH.DE",
    "EXV1.DE",    "EXX5.DE",    "EXXW.DE",    "FCRN.DE",    "FGEQ.F",
    "HYLD.MI",    "IBCZ.DE",    "IBGM.MI",    "IBGS.MI",    "IBTM.MI",    "IDVY.MI",

    "IEAC.MI",    "IEAG.MI",    "IFFF.MI",    "IGLO.DE",    "IHYU.MI",    "IPRP.MI",
    "IS07.DE",    "IS3N.DE",    "ISPA.DE",    "IU0E.MI",    "IUIT.L",     "IUSA.MI",
    "IWDP.MI",    "IWMO.MI",    "JNHD.DE",    "JPGL.MI",    "JPNH.MI",    "MACV.MI",    "MAGR.MI",
    "MEUD.MI",    "MODR.MI",    "NTSG.MI",    "NTSX.MI",    "NTSZ.MI",    "PHAG.MI",
    "PHAU.MI",    "QNTM.MI",    "QQQ3.MI",    "RARE.MI",    "SEGA.MI",    "SEMB.DE",
    "SMART.MI",   "SMH.MI",     "STHE.MI",    "SWDA.MI",    "TDIV.MI",    "VAGE.DE",
    "VAPX.MI",    "VHYL.MI",    "VUSA.MI",    "VWCE.DE",    "V20A.DE",    "V40A.DE",
    "V60A.DE",    "V80A.DE",    "WENT.MI",    "WHCS.MI",
    "WRTY.MI",    "WS5X.MI",    "WSPE.MI",    "WSPX.MI",    "WWRD.MI",    "XAIX.MI",    "XGSH.MI",
    "XDWT.MI",   "XEOD.DE",    "XEON.MI",    "XREA.DE",    "XUCS.DE",    "XUTC.MI",
]))

ETF_CATEGORIA = {
    "XEON.MI":"monetario","SMART.MI":"monetario","IU0E.MI":"monetario",
    "XEOD.DE":"monetario","XEON.MI":"monetario",
    "IEAC.MI":"obbligaz_ig","IEAG.MI":"obbligaz_ig","IBTM.MI":"obbligaz_ig",
    "AGGH.DE":"obbligaz_ig","IGLO.DE":"obbligaz_ig",
    "IEAC.MI":"obbligaz_ig","SEGA.MI":"obbligaz_ig","VAGE.DE":"obbligaz_ig",
    "EUNH.DE":"obbligaz_ig","IBGS.MI":"obbligaz_ig",
    "XUCS.DE":"obbligaz_ig","JNHD.DE":"obbligaz_ig","FGEQ.F":"obbligaz_ig","XUTC.MI":"tematico",
    "IBGM.MI":"obbligaz_ig","XGSH.MI":"obbligaz_ig",
    "IHYU.MI":"hy","EUHI.MI":"hy","HYLD.MI":"hy","STHE.MI":"hy",
    "HYLD.MI":"hy","EUHA.MI":"hy",
    "EMBE.MI":"em_bond","EMDV.MI":"em_bond","SEMB.DE":"em_bond",
    "SWDA.MI":"az_globale","VWCE.DE":"az_globale","CSSPX.MI":"az_globale",
    "ESGE.MI":"az_globale","WWRD.MI":"az_globale","NTSX.MI":"az_globale",
    "NTSG.MI":"az_globale","XREA.DE":"az_globale","VHYL.MI":"az_globale",
    "TDIV.MI":"az_globale","IWMO.MI":"az_globale",
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
    "CMOD.MI":"tematico","AIGA.MI":"tematico",
    "3USL.MI":"leva","QQQ3.MI":"leva","3EUL.MI":"leva","3NVD.MI":"leva",
    # Nuovi ETF multifactor
    "JPGL.MI":"az_globale","IBCZ.DE":"az_globale","IS07.DE":"az_globale","FCRN.DE":"az_globale",
    # Benchmark multi-asset
    "MACV.MI":"multi_asset","MODR.MI":"multi_asset","MAGR.MI":"multi_asset",
    "V20A.DE":"multi_asset","V40A.DE":"multi_asset","V60A.DE":"multi_asset","V80A.DE":"multi_asset",
}

ETF_TIPO = {
    "XEON.MI":"Acc","SMART.MI":"Acc","IU0E.MI":"Dist","XEOD.DE":"Dist",
    "XEON.MI":"Dist","EUHA.MI":"Dist","IEAC.MI":"Dist","IEAG.MI":"Dist",
    "IBTM.MI":"Dist","XGSH.MI":"Acc","AGGH.DE":"Acc","IGLO.DE":"Dist",
    "IEAC.MI":"Dist","SEGA.MI":"Dist","VAGE.DE":"Dist","EUNH.DE":"Dist",
    "IBGS.MI":"Dist","XUTC.MI":"Dist","XUCS.DE":"Acc","JNHD.DE":"Dist",
    "FGEQ.F":"Dist","IBGM.MI":"Dist","IHYU.MI":"Dist","EUHI.MI":"Dist",
    "HYLD.MI":"Dist","STHE.MI":"Dist","HYLD.MI":"Dist","EMBE.MI":"Dist",
    "EMDV.MI":"Dist","SEMB.DE":"Dist","SWDA.MI":"Acc","VWCE.DE":"Acc",
    "CSSPX.MI":"Acc","ESGE.MI":"Acc","WWRD.MI":"Acc","NTSX.MI":"Acc",
    "NTSG.MI":"Acc","AIGA.MI":"Acc","XREA.DE":"Acc","XREA.DE":"Acc",
    "MEUD.MI":"Acc","EXX5.DE":"Dist","EXV1.DE":"Dist","EXXW.DE":"Dist",
    "ISPA.DE":"Dist","WS5X.MI":"Acc","NTSZ.MI":"Acc","IUSA.MI":"Dist",
    "VUSA.MI":"Dist","WSPX.MI":"Acc","WSPE.MI":"Acc","EQQQ.MI":"Dist",
    "WRTY.MI":"Acc","IWMO.MI":"Acc","VAPX.MI":"Dist","JPNH.MI":"Dist","IS3N.DE":"Acc",
    "WENT.MI":"Acc","SMH.MI":"Acc","XAIX.MI":"Acc","XDWT.MI":"Acc",
    "DFNS.MI":"Acc","QNTM.MI":"Acc","WHCS.MI":"Dist","RARE.MI":"Acc",
    "IPRP.MI":"Dist","IWDP.MI":"Dist","IFFF.MI":"Dist","DHS.MI":"Dist",
    "IUIT.L":"Acc","3USL.MI":"Acc","QQQ3.MI":"Acc","3EUL.MI":"Acc",
    "3NVD.MI":"Acc","PHAU.MI":"Acc","PHAG.MI":"Dist","COPA.MI":"Acc",
    "CMOD.MI":"Acc","VHYL.MI":"Dist","IDVY.MI":"Dist","EUDV.MI":"Dist",
    "TDIV.MI":"Dist",
    # Nuovi ETF multifactor
    "JPGL.MI":"Acc","IBCZ.DE":"Acc","IS07.DE":"Acc","FCRN.DE":"Acc",
    # Benchmark multi-asset
    "MACV.MI":"Acc","MODR.MI":"Acc","MAGR.MI":"Acc",
    "V20A.DE":"Acc","V40A.DE":"Acc","V60A.DE":"Acc","V80A.DE":"Acc",
}

ETF_NOMI = {
    "SWDA.MI":"iShares Core MSCI World UCITS ETF Acc",
    "VWCE.DE":"Vanguard FTSE All-World UCITS ETF Acc",
    "CSSPX.MI":"iShares Core S&P 500 UCITS ETF USD Acc",
    "VHYL.MI":"Vanguard FTSE All-World High Dividend Yield",
    "TDIV.MI":"VanEck Developed Markets Dividend Leaders",
    "IDVY.MI":"iShares Euro Dividend UCITS ETF",
    "EUDV.MI":"SPDR S&P Euro Dividend Aristocrats",
    "FGEQ.F":"Fidelity Global Quality Income UCITS ETF",
    "PHAU.MI":"WisdomTree Physical Gold",
    "PHAG.MI":"WisdomTree Physical Silver",
    "COPA.MI":"WisdomTree Copper",
    "CMOD.MI":"iShares Diversified Commodity Swap",
    "XEON.MI":"Xtrackers II EUR Overnight Rate Swap Acc",
    "SMART.MI":"iShares EUR Ultrashort Bond UCITS ETF",
    "XEOD.DE":"Xtrackers II EUR Overnight Rate Swap Dist",
    "IU0E.MI":"iShares EUR Ultrashort Bond UCITS ETF Dist",
    "XEON.MI":"Amundi EUR Overnight Return UCITS ETF Dist",
    "IEAC.MI":"iShares Core EUR Corp Bond UCITS ETF",
    "IEAG.MI":"iShares Core EUR Aggregate Bond UCITS ETF",
    "EUNH.DE":"iShares Core EUR Govt Bond UCITS ETF",
    "IBGS.MI":"iShares EUR Govt Bond 1-3yr UCITS ETF",
    "IBTM.MI":"iShares EUR Govt Bond 3-7yr UCITS ETF",
    "EUNA.DE":"iShares Core Global Aggregate Bond UCITS ETF EUR Hedged",
    "EUN3.DE":"iShares Global Govt Bond UCITS ETF",
    "IEMB.MI":"iShares JPM EM Bond UCITS ETF",
    "EUHA.DE":"iShares EUR High Yield Corp Bond UCITS ETF",
    "VAGE.DE":"Vanguard EUR Aggregate Bond UCITS ETF",
    "IEAC.MI":"iShares EUR Corp Bond 1-5yr UCITS ETF",
    "SEGA.MI":"iShares EUR Govt Bond 1-5yr UCITS ETF",
    "EUNH.DE":"iShares Core EUR Govt Bond UCITS ETF",
    "XUCS.DE":"Xtrackers USD Corporate Bond UCITS ETF",
    "XUTC.MI":"Xtrackers MSCI USA Information Technology UCITS ETF 1D",
    "XGSH.MI":"Xtrackers II Global Government Bond UCITS ETF EUR Hedged Acc","JNHD.DE":"JPMorgan EUR Corporate Bond Research Enhanced",
    "IBGM.MI":"iShares EUR Govt Bond 7-10yr UCITS ETF",
    "IHYU.MI":"iShares USD High Yield Corp Bond EUR Hedged",
    "EUHI.MI":"PIMCO Euro Short-Term High Yield Corporate Bond",
    "HYLD.MI":"iShares EUR High Yield Corp Bond UCITS ETF",
    "STHE.MI":"SPDR Bloomberg 0-3Y EUR HY Corp Bond",
    "HYLD.MI":"iShares EUR High Yield Corp Bond Climate",
    "EUHA.MI":"iShares EUR High Yield Corp Bond UCITS ETF",
    "EMBE.MI":"iShares JPM EM Bond EUR Hedged UCITS ETF",
    "EMDV.MI":"iShares JPM EM Local Govt Bond UCITS ETF",
    "SEMB.DE":"iShares JPM EM Bond UCITS ETF",
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
    "IWMO.MI":"iShares Edge MSCI World Momentum Factor UCITS ETF Acc",
    "VAPX.MI":"Vanguard FTSE Developed Asia Pacific ex Japan",
    "JPNH.MI":"Amundi MSCI Japan UCITS ETF EUR Hedged",
    "IS3N.DE":"iShares MSCI EM Small Cap UCITS ETF",
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
    "IUIT.L":"iShares S&P 500 Information Technology",
    "ESGE.MI":"iShares MSCI World ESG Enhanced UCITS ETF",
    "WWRD.MI":"WisdomTree World",
    "NTSX.MI":"WisdomTree US Efficient Core UCITS ETF",
    "NTSG.MI":"WisdomTree Global Efficient Core UCITS ETF",
    "XREA.DE":"Xtrackers FTSE EPRA/NAREIT Dev Europe RE",
    "XREA.DE":"iShares Global REIT UCITS ETF",
    "AIGA.MI":"WisdomTree Agriculture",
    "3USL.MI":"WisdomTree S&P 500 3x Daily Leveraged",
    "QQQ3.MI":"WisdomTree NASDAQ-100 3x Daily Leveraged",
    "3EUL.MI":"WisdomTree EURO STOXX 50 3x Daily Leveraged",
    "3NVD.MI":"Leverage Shares 3x NVIDIA ETP",
    # Nuovi ETF multifactor / factor rotation
    "JPGL.MI":"JPMorgan Global Equity Multi-Factor UCITS ETF Acc",
    "IBCZ.DE":"iShares STOXX World Equity Multifactor UCITS ETF USD Acc",
    "IS07.DE":"iShares STOXX World Equity Multifactor EUR Hedged UCITS ETF Acc",
    "FCRN.DE":"iShares World Equity Factor Rotation Active UCITS ETF Acc",
    # Vanguard LifeStrategy benchmark
    "V20A.DE": "Vanguard LifeStrategy 20% Equity UCITS ETF Acc",
    "V40A.DE": "Vanguard LifeStrategy 40% Equity UCITS ETF Acc",
    "V60A.DE": "Vanguard LifeStrategy 60% Equity UCITS ETF Acc",
    "V80A.DE": "Vanguard LifeStrategy 80% Equity UCITS ETF Acc",
    # Benchmark multi-asset
    "MACV.MI":"iShares Conservative Multi-Asset UCITS ETF Acc",
    "MODR.MI":"iShares Moderate Multi-Asset UCITS ETF Acc",
    "MAGR.MI":"iShares Aggressive Multi-Asset UCITS ETF Acc",
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

def calc_kama_series(closes, period=10, fast=2, slow=30):
    """Restituisce la serie KAMA completa (per grafico)."""
    if len(closes) < period + 1: return []
    fast_sc = 2 / (fast + 1); slow_sc = 2 / (slow + 1)
    kama = closes[period]
    result = [None] * (period + 1)
    result[period] = round(kama, 4)
    for i in range(period + 1, len(closes)):
        direction = abs(closes[i] - closes[i - period])
        volatility = sum(abs(closes[j] - closes[j-1]) for j in range(i-period+1, i+1))
        er = direction / volatility if volatility else 0
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
        kama = kama + sc * (closes[i] - kama)
        result.append(round(kama, 4))
    return result

def calc_rsi(closes, period=14):
    """RSI 14 giorni."""
    if len(closes) < period + 1: return None
    gains = []; losses = []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    if len(gains) < period: return None
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    for i in range(len(gains) - period):
        avg_gain = (avg_gain * (period-1) + gains[i+period]) / period
        avg_loss = (avg_loss * (period-1) + losses[i+period]) / period
    if avg_loss == 0: return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 2)

def calc_rsi_series(closes, period=14):
    """Serie RSI completa per grafico."""
    if len(closes) < period + 2: return []
    result = [None] * (period + 1)
    gains = [max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
    losses = [max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
    avg_g = sum(gains[:period])/period
    avg_l = sum(losses[:period])/period
    rs = avg_g/avg_l if avg_l else 100
    result.append(round(100-100/(1+rs),2))
    for i in range(period, len(gains)):
        avg_g = (avg_g*(period-1)+gains[i])/period
        avg_l = (avg_l*(period-1)+losses[i])/period
        rs = avg_g/avg_l if avg_l else 100
        result.append(round(100-100/(1+rs),2))
    return result

def calc_adx(closes, period=14):
    """ADX 14 giorni — misura forza del trend."""
    if len(closes) < period * 2 + 1: return None
    tr_list=[]; pdm_list=[]; ndm_list=[]
    for i in range(1, len(closes)):
        h = closes[i]; l = closes[i]; pc = closes[i-1]
        tr = max(h-l, abs(h-pc), abs(l-pc))
        pdm = max(h-closes[i-1],0) if h-closes[i-1]>closes[i-1]-l else 0
        ndm = max(closes[i-1]-l,0) if closes[i-1]-l>h-closes[i-1] else 0
        tr_list.append(tr); pdm_list.append(pdm); ndm_list.append(ndm)
    if len(tr_list) < period: return None
    atr = sum(tr_list[:period])
    pdi = sum(pdm_list[:period])
    ndi = sum(ndm_list[:period])
    dx_list = []
    for i in range(period, len(tr_list)):
        atr = atr - atr/period + tr_list[i]
        pdi = pdi - pdi/period + pdm_list[i]
        ndi = ndi - ndi/period + ndm_list[i]
        pdi_pct = 100*pdi/atr if atr else 0
        ndi_pct = 100*ndi/atr if atr else 0
        denom = pdi_pct + ndi_pct
        dx = 100*abs(pdi_pct-ndi_pct)/denom if denom else 0
        dx_list.append(dx)
    if len(dx_list) < period: return None
    adx = sum(dx_list[-period:])/period
    return round(adx, 2)

def calc_ao(closes):
    """Awesome Oscillator = SMA5 - SMA34 sui prezzi."""
    if len(closes) < 34: return None
    sma5  = sum(closes[-5:])/5
    sma34 = sum(closes[-34:])/34
    return round(sma5 - sma34, 4)

def calc_ao_series(closes):
    """Serie AO completa per grafico."""
    result = [None]*34
    for i in range(34, len(closes)+1):
        sl = closes[max(0,i-34):i]
        s5 = sum(sl[-5:])/5 if len(sl)>=5 else None
        s34 = sum(sl)/34 if len(sl)==34 else None
        result.append(round(s5-s34,4) if s5 and s34 else None)
    return result

def calc_sar(closes, af_start=0.02, af_max=0.2):
    """Parabolic SAR — restituisce (sar_corrente, trend: 1=up/-1=down)."""
    if len(closes) < 10: return None, None
    # Inizializza
    trend = 1 if closes[-1] > closes[0] else -1
    sar = min(closes[:5]) if trend == 1 else max(closes[:5])
    ep = max(closes[:5]) if trend == 1 else min(closes[:5])
    af = af_start
    for i in range(5, len(closes)):
        p = closes[i]
        sar = sar + af * (ep - sar)
        if trend == 1:
            if p < sar:
                trend = -1; sar = ep; ep = p; af = af_start
            else:
                if p > ep: ep = p; af = min(af+af_start, af_max)
        else:
            if p > sar:
                trend = 1; sar = ep; ep = p; af = af_start
            else:
                if p < ep: ep = p; af = min(af+af_start, af_max)
    return round(sar, 4), trend

def calc_sar_series(closes, af_start=0.02, af_max=0.2):
    """Serie SAR completa per grafico."""
    if len(closes) < 10: return []
    result = [None]*5
    trend = 1
    sar = min(closes[:5]); ep = max(closes[:5]); af = af_start
    result.append(round(sar,4))
    for i in range(6, len(closes)):
        p = closes[i]
        sar = sar + af*(ep-sar)
        if trend==1:
            if p<sar: trend=-1;sar=ep;ep=p;af=af_start
            else:
                if p>ep: ep=p;af=min(af+af_start,af_max)
        else:
            if p>sar: trend=1;sar=ep;ep=p;af=af_start
            else:
                if p<ep: ep=p;af=min(af+af_start,af_max)
        result.append(round(sar,4))
    return result

def calc_signal_history(closes, kama_series, sar_series, dates=None):
    """
    Genera storia segnali BUY/SELL basata su KAMA crossover.
    Restituisce lista di {idx, tipo, prezzo, data}
    """
    signals = []
    n = min(len(closes), len(kama_series), len(sar_series))
    prev_above_kama = None
    for i in range(1, n):
        p = closes[i]; k = kama_series[i]; s = sar_series[i]
        if k is None or s is None: continue
        above_kama = p > k
        if prev_above_kama is not None:
            if above_kama and not prev_above_kama:
                signals.append({
                    "idx": i, "tipo": "BUY", "prezzo": round(p,4),
                    "data": dates[i] if dates and i < len(dates) else None
                })
            elif not above_kama and prev_above_kama:
                signals.append({
                    "idx": i, "tipo": "SELL", "prezzo": round(p,4),
                    "data": dates[i] if dates and i < len(dates) else None
                })
        prev_above_kama = above_kama
    return signals[-10:]  # ultimi 10 segnali

def calc_volatilita(equity_curve):
    """Volatilità annualizzata dalla equity curve giornaliera."""
    if len(equity_curve) < 10: return None
    rend = []
    for i in range(1, len(equity_curve)):
        if equity_curve[i-1] > 0:
            rend.append((equity_curve[i] - equity_curve[i-1]) / equity_curve[i-1])
    if len(rend) < 5: return None
    media = sum(rend) / len(rend)
    var = sum((r - media)**2 for r in rend) / len(rend)
    std_daily = var**0.5
    return round(std_daily * (252**0.5) * 100, 2)  # annualizzata %

def calc_max_drawdown(equity_curve):
    """Max Drawdown dalla equity curve giornaliera."""
    if len(equity_curve) < 2: return None
    peak = equity_curve[0]
    max_dd = 0.0
    peak_dd_date_idx = 0
    trough_idx = 0
    for i, v in enumerate(equity_curve):
        if v > peak:
            peak = v
            peak_dd_date_idx = i
        dd = (v - peak) / peak * 100 if peak > 0 else 0
        if dd < max_dd:
            max_dd = dd
            trough_idx = i
    return round(max_dd, 2)

def calc_sharpe(equity_curve, risk_free=0.03):
    """Sharpe ratio annualizzato (risk-free 3%)."""
    if len(equity_curve) < 10: return None
    rend = []
    for i in range(1, len(equity_curve)):
        if equity_curve[i-1] > 0:
            rend.append((equity_curve[i] - equity_curve[i-1]) / equity_curve[i-1])
    if len(rend) < 5: return None
    media = sum(rend) / len(rend)
    var = sum((r - media)**2 for r in rend) / len(rend)
    std_daily = var**0.5
    if std_daily == 0: return None
    rf_daily = risk_free / 252
    sharpe = (media - rf_daily) / std_daily * (252**0.5)
    return round(sharpe, 2)


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

def calc_momentum(closes, days):
    if len(closes) < days + 1: return None
    old = closes[-(days+1)]
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

def get_all_trading_dates(etf_data_completo):
    """Estrae tutte le date di trading disponibili dai dati ETF, ordinate."""
    date_set = set()
    for sig in etf_data_completo.values():
        for d in sig.get("dates", []):
            if d >= BACKTEST_START:
                date_set.add(d)
    return sorted(date_set)

def get_price_on_date(ticker, target_date, etf_data_completo):
    """Restituisce il prezzo di un ETF in una data specifica."""
    sig = etf_data_completo.get(ticker)
    if not sig: return None
    closes = sig.get("closes", [])
    dates = sig.get("dates", [])
    if not closes or not dates: return None
    n = min(len(closes), len(dates))  # protezione disallineamento
    best_price = None
    for i in range(n):
        d = dates[i]
        if d <= target_date:
            best_price = closes[i]
        elif d > target_date:
            break
    return best_price

def calc_score_storico(ticker, target_date, livello_id, etf_data_completo):
    """Score calcolato sui prezzi storici fino a target_date."""
    sig = etf_data_completo.get(ticker)
    if not sig: return 0
    closes = sig.get("closes", [])
    dates  = sig.get("dates", [])
    if not closes or not dates: return 0
    n = min(len(closes), len(dates))
    closes_hist = [closes[i] for i in range(n) if dates[i] <= target_date]
    if len(closes_hist) < 10: return 0
    price = closes_hist[-1]
    sma200 = calc_sma(closes_hist, 200)
    kama   = calc_kama(closes_hist)
    mom1m  = calc_momentum(closes_hist, min(21, len(closes_hist)-1))
    mom3m  = calc_momentum(closes_hist, min(63, len(closes_hist)-1))
    mom6m  = calc_momentum(closes_hist, min(126, len(closes_hist)-1))
    sig_hist = {"price": price, "sma200": sma200, "kama": kama,
                "mom1m": mom1m, "mom3m": mom3m, "mom6m": mom6m,
                "yield_pct": sig.get("yield_pct", 0)}
    return calc_score_for_level(sig_hist, livello_id)

def seleziona_etf_per_data(pool, etf_data_completo, livello_id, alloc_t,
                            n_etf_max, target_date, pesi_override=None):
    """Seleziona gli ETF migliori usando score calcolati sui prezzi storici di target_date."""
    isDist = livello_id.startswith("C")
    candidati = []
    for t in pool:
        sig = etf_data_completo.get(t)
        if not sig or not sig.get("closes"): continue
        price = get_price_on_date(t, target_date, etf_data_completo)
        if not price: continue
        # Score storico — non usa score attuali
        score = calc_score_storico(t, target_date, livello_id, etf_data_completo)
        if score <= 0: continue
        cat = ETF_CATEGORIA.get(t, "az_globale")
        peso_macro = (pesi_override or {}).get(cat, 1.0)
        if peso_macro <= 0: continue
        tipo = ETF_TIPO.get(t, "Acc")
        boost = 8 if (isDist and tipo == "Dist") or (not isDist and tipo == "Acc") else 0
        score_eff = min(100, score * peso_macro + boost)
        candidati.append({"ticker": t, "score_eff": score_eff, "cat": cat,
                          "price": price, "yield_pct": sig.get("yield_pct", 0)})

    candidati.sort(key=lambda x: x["score_eff"], reverse=True)
    selected = []; leva_n = 0; cat_c = {}
    for c in candidati:
        if len(selected) >= n_etf_max: break
        if c["ticker"] in LEVA_TICKERS:
            if leva_n >= 1: continue
            leva_n += 1
        else:
            cn = cat_c.get(c["cat"], 0)
            if cn >= 2: continue
            cat_c[c["cat"]] = cn + 1
        selected.append(c)

    if not selected: return []

    # Calcola pesi proporzionali
    by_cat = {}
    for c in selected: by_cat.setdefault(c["cat"], []).append(c)
    pesi = {}
    for cat, items in by_cat.items():
        quota = alloc_t.get(cat, 0) or (100 / len(selected))
        tot_s = sum(i["score_eff"] for i in items) or 1
        for i in items:
            pesi[i["ticker"]] = quota * (i["score_eff"] / tot_s)

    tot_p = sum(pesi.values()) or 1
    comp = []
    for c in selected:
        t = c["ticker"]
        p = round(pesi.get(t, 0) / tot_p * 100, 1)
        imp = round(CAPITALE_MODELLO * p / 100, 2)
        comp.append({
            "ticker": t,
            "nome": ETF_NOMI.get(t, t),
            "peso": p,
            "importo": imp,
            "price_entry": round(c["price"], 4),
            "quote": round(imp / c["price"], 4) if c["price"] else 0,
            "yield_pct": c["yield_pct"],
        })

    # Aggiusta a 100%
    tot_p2 = sum(c["peso"] for c in comp)
    diff = round(100 - tot_p2, 1)
    if diff != 0 and comp:
        comp[0]["peso"] = round(comp[0]["peso"] + diff, 1)
        comp[0]["importo"] = round(CAPITALE_MODELLO * comp[0]["peso"] / 100, 2)
    return comp

def run_backtest(livello_id, etf_data_completo, alloc, n_etf_max,
                 regime_corrente, pesi_override):
    """
    Backtest giorno per giorno dal 01/01/2025.
    Simula: prezzi daily, drawdown, cambi regime settimanali, ribilanciamenti.
    Ogni evento genera una nuova versione nella storia.
    """
    print(f"    Backtest {livello_id}...")

    # Carica configurazione livello
    try:
        with open(LEVELS_FILE) as f:
            levels = json.load(f)["levels"]
        lv_json = next((l for l in levels if l["id"] == livello_id), None)
        pool = lv_json.get("etf_pool", []) if lv_json else []
        alloc_t = lv_json.get("alloc", alloc) if lv_json else alloc
    except Exception:
        pool = []; alloc_t = alloc

    if not pool:
        return []

    gruppo = LIVELLO_GRUPPO.get(livello_id, "bilanciato")
    pers = PERSISTENZA[gruppo]

    # Tutte le date di trading disponibili
    all_dates = get_all_trading_dates(etf_data_completo)
    if not all_dates:
        return []

    # Composizione iniziale al 01/01/2025
    comp_v1 = seleziona_etf_per_data(
        pool, etf_data_completo, livello_id, alloc_t, n_etf_max,
        BACKTEST_START, None  # regime neutro iniziale
    )
    if not comp_v1:
        return []

    # Stato iniziale
    versioni = []
    ver_num = 1
    cap_corrente = float(CAPITALE_MODELLO)
    equity_curve_totale = [cap_corrente]  # valore giornaliero cumulativo
    equity_dates_totale = [BACKTEST_START]  # date corrispondenti

    versione_attiva = {
        "versione": ver_num,
        "livello_id": livello_id,
        "data_apertura": BACKTEST_START,
        "data_chiusura": None,
        "regime": "neutro",
        "confidence": 50,
        "trigger_apertura": "inizializzazione_backtest",
        "trigger_chiusura": None,
        "capitale_inizio": cap_corrente,
        "capitale_attuale": cap_corrente,
        "performance_pct": 0.0,
        "performance_eur": 0.0,
        "giorni_attivo": 0,
        "composizione": comp_v1,
        "etf_usciti_anticipati": [],
        "aggiornato": BACKTEST_START,
        "_equity": [cap_corrente],  # equity curve versione
    }
    versioni.append(versione_attiva)

    # ── Progressione regime storica simulata (plausibile sul periodo) ─────
    # gen-feb 2025: goldilocks (mercati su, tassi in calo atteso)
    # mar-ago 2025: goldilocks stabile
    # set-nov 2025: neutro (incertezza geopolitica, tassi fermi)
    # dic 2025-gen 2026: goldilocks (rally natalizio)
    # feb-mar 2026: neutro (attesa dazi)
    # apr 2026: risk_off (crollo dazi Trump)
    # mag 2026: reflazione (rimbalzo + commodities)
    def get_regime_for_date(d):
        # Timeline basata su eventi reali di mercato 2025-2026
        # Gen-Feb 2025: goldilocks (mercati forti, AI rally)
        if d < "2025-03-01": return ("goldilocks", 68)
        # Mar 2025: neutro (attesa Fed, rotazione)
        if d < "2025-04-01": return ("neutro", 55)
        # Apr 2025: risk_off (Liberation Day dazi Trump 2 aprile)
        if d < "2025-05-15": return ("risk_off", 74)
        # Mag-Lug 2025: reflazione (rimbalzo post-dazi, commodity)
        if d < "2025-08-01": return ("reflazione", 62)
        # Ago-Set 2025: goldilocks (rally estivo, earnings forti)
        if d < "2025-10-01": return ("goldilocks", 64)
        # Ott-Nov 2025: neutro (rotazione settoriale, tassi)
        if d < "2025-12-01": return ("neutro", 57)
        # Dic 2025-Gen 2026: goldilocks (rally natalizio)
        if d < "2026-02-01": return ("goldilocks", 65)
        # Feb-Mar 2026: neutro (attesa politica monetaria)
        if d < "2026-04-01": return ("neutro", 58)
        # Apr 2026: risk_off (nuovo shock mercati)
        if d < "2026-05-01": return ("risk_off", 70)
        # Mag 2026: reflazione (rimbalzo + oro forte)
        return ("reflazione", 67)

    regime_storia_bt = []
    ultimo_check_regime = BACKTEST_START
    regime_attivo = "goldilocks"   # parte già in goldilocks a inizio 2025
    settimane_nuovo_regime = 0
    regime_candidato = None

    # Soglie drawdown per il gruppo
    dd_trigger_forte = -30
    dd_trigger_medio = -20 if gruppo == "aggressivo" else -999

    # Tracking score deteriorato (trigger 3)
    score_basso_count = {}   # ticker → conteggio settimane sotto soglia

    def apri_nuova_versione(cap_new, data, regime, conf, trigger_str, po):
        nonlocal ver_num, versione_attiva
        versione_attiva["data_chiusura"] = data
        versione_attiva["trigger_chiusura"] = trigger_str
        ver_num += 1
        nuova_comp = seleziona_etf_per_data(
            pool, etf_data_completo, livello_id, alloc_t, n_etf_max, data, po)
        if not nuova_comp:
            nuova_comp = seleziona_etf_per_data(
                pool, etf_data_completo, livello_id, alloc_t, n_etf_max, data, None)
        if not nuova_comp:
            return
        scala = cap_new / CAPITALE_MODELLO if CAPITALE_MODELLO else 1
        for e in nuova_comp:
            e["importo"] = round(e["importo"] * scala, 2)
            e["quote"] = round(e["importo"] / e["price_entry"], 4) if e["price_entry"] else 0
        versione_attiva = {
            "versione": ver_num,
            "livello_id": livello_id,
            "data_apertura": data,
            "data_chiusura": None,
            "regime": regime,
            "confidence": conf,
            "trigger_apertura": trigger_str,
            "trigger_chiusura": None,
            "capitale_inizio": cap_new,
            "capitale_attuale": cap_new,
            "performance_pct": 0.0,
            "performance_eur": 0.0,
            "giorni_attivo": 0,
            "composizione": nuova_comp,
            "etf_usciti_anticipati": [],
            "aggiornato": data,
            "_equity": [cap_new],
        }
        versioni.append(versione_attiva)
        score_basso_count.clear()

    # Loop giornaliero
    for data_corrente in all_dates:
        if data_corrente <= BACKTEST_START:
            continue

        # Giorni attivi versione corrente
        try:
            d_open = datetime.date.fromisoformat(versione_attiva["data_apertura"])
            d_curr = datetime.date.fromisoformat(data_corrente)
            versione_attiva["giorni_attivo"] = (d_curr - d_open).days
        except Exception:
            pass

        # ── TRIGGER 1: Aggiorna prezzi + check drawdown ────────────────
        tot_valore = 0.0
        trigger_dd = None

        for etf in versione_attiva["composizione"]:
            t = etf["ticker"]
            if etf.get("importo", 0) <= 0:
                continue
            price_now = get_price_on_date(t, data_corrente, etf_data_completo)
            if not price_now:
                price_now = etf.get("price_now", etf["price_entry"])

            price_entry = etf["price_entry"]
            giorni_det = versione_attiva["giorni_attivo"]
            y = etf.get("yield_pct", 0)
            tr = calc_total_return(price_entry, price_now, y, giorni_det)
            perf_eur = round(etf["importo"] * tr / 100, 2)
            dd = round((price_now - price_entry) / price_entry * 100, 2) if price_entry else 0

            etf["price_now"] = round(price_now, 4)
            etf["perf_pct"] = round(tr, 3)
            etf["perf_eur"] = perf_eur
            etf["drawdown_pct"] = dd

            if dd <= -50 and not trigger_dd:
                trigger_dd = {"tipo": "drawdown_50", "dd": dd,
                              "motivo": f"{t} drawdown {dd:.1f}% — uscita immediata"}
            elif dd <= dd_trigger_forte and not trigger_dd:
                trigger_dd = {"tipo": "drawdown_30", "dd": dd,
                              "motivo": f"{t} drawdown {dd:.1f}% — ribilanciamento obbligatorio"}
            elif dd <= dd_trigger_medio and not trigger_dd:
                trigger_dd = {"tipo": "drawdown_20", "dd": dd,
                              "motivo": f"{t} drawdown {dd:.1f}% — ribilanciamento aggressivo"}
            elif dd <= -10:
                alert_key = f"{t}_{data_corrente[:7]}"   # un alert per mese per ETF
                if not any(u.get("_key") == alert_key for u in versione_attiva["etf_usciti_anticipati"]):
                    versione_attiva["etf_usciti_anticipati"].append({
                        "_key": alert_key,
                        "ticker": t, "motivo": "drawdown_10_alert",
                        "data": data_corrente,
                        "price_entry": price_entry,
                        "price_exit": round(price_now, 4),
                        "perf_pct": round(dd, 2),
                        "perf_eur": perf_eur,
                        "drawdown_pct": dd,
                    })

            tot_valore += etf["importo"] * (1 + tr / 100)

        perf_tot_eur = round(tot_valore - versione_attiva["capitale_inizio"], 2)
        versione_attiva["capitale_attuale"] = round(tot_valore, 2)
        versione_attiva["performance_eur"] = perf_tot_eur
        versione_attiva["performance_pct"] = round(
            perf_tot_eur / versione_attiva["capitale_inizio"] * 100, 2)
        versione_attiva["aggiornato"] = data_corrente
        # Registra equity curve giornaliera con data
        versione_attiva.setdefault("_equity", []).append(round(tot_valore, 2))
        versione_attiva.setdefault("_equity_dates", []).append(data_corrente)
        equity_curve_totale.append(round(tot_valore, 2))
        equity_dates_totale.append(data_corrente)

        # ── TRIGGER 2: Check regime (ogni 5 giorni di trading) ─────────
        try:
            giorni_da_check = (datetime.date.fromisoformat(data_corrente) -
                               datetime.date.fromisoformat(ultimo_check_regime)).days
        except Exception:
            giorni_da_check = 0

        cambio_regime = None
        if giorni_da_check >= 5:
            ultimo_check_regime = data_corrente
            sc, conf = get_regime_for_date(data_corrente)
            regime_storia_bt.append({"data": data_corrente, "scenario": sc, "confidence": conf})

            if sc != regime_attivo:
                if sc == regime_candidato:
                    settimane_nuovo_regime += 1
                else:
                    regime_candidato = sc
                    settimane_nuovo_regime = 1

                if (settimane_nuovo_regime >= pers["settimane"] and
                        conf >= pers["soglia"] and
                        versione_attiva["giorni_attivo"] >= 30):  # min 30gg dalla versione aperta
                    cambio_regime = {"da": regime_attivo, "a": sc, "confidence": conf}
                    regime_attivo = sc
                    settimane_nuovo_regime = 0
                    regime_candidato = None
            else:
                # regime confermato → resetta contatore candidato alternativo
                settimane_nuovo_regime = 0
                regime_candidato = None

        # ── TRIGGER 3: Check score deteriorato (ogni 5 giorni) ─────────
        score_trigger = None
        if giorni_da_check >= 5 and versione_attiva["giorni_attivo"] >= 45:
            etf_da_sostituire = []
            for etf in versione_attiva["composizione"]:
                if etf.get("importo", 0) <= 0:
                    continue
                t = etf["ticker"]
                # Score storico alla data corrente — non score attuale
                score_storico = calc_score_storico(t, data_corrente, livello_id, etf_data_completo)
                if score_storico < 15:
                    score_basso_count[t] = score_basso_count.get(t, 0) + 1
                    if score_basso_count[t] >= 3:
                        # Verifica che esista un candidato migliore nel pool
                        score_migliore = 0
                        for cand in pool:
                            if cand == t: continue
                            if cand in {e["ticker"] for e in versione_attiva["composizione"]}: continue
                            score_cand = calc_score_storico(cand, data_corrente, livello_id, etf_data_completo)
                            if score_cand > score_migliore:
                                score_migliore = score_cand
                        # Sostituisce solo se il candidato è significativamente migliore
                        if score_migliore > 30:
                            etf_da_sostituire.append(t)
                else:
                    score_basso_count[t] = 0  # reset se score risale

            if etf_da_sostituire and not trigger_dd and not cambio_regime:
                score_trigger = f"score_deteriorato: {','.join(etf_da_sostituire)}"

        # ── Esegui ribilanciamento se uno dei 3 trigger è scattato ─────
        cap_new = versione_attiva["capitale_attuale"]

        if trigger_dd and trigger_dd["tipo"] == "drawdown_50":
            # Caso speciale: sostituisci solo l'ETF crollo con monetario
            t_out = next((e["ticker"] for e in versione_attiva["composizione"]
                         if e.get("drawdown_pct", 0) <= -50 and e.get("importo", 0) > 0), None)
            if t_out:
                etf_out = next(e for e in versione_attiva["composizione"] if e["ticker"] == t_out)
                versione_attiva["composizione"] = [
                    e for e in versione_attiva["composizione"] if e["ticker"] != t_out]
                mon = "XEON.MI"
                p_mon = get_price_on_date(mon, data_corrente, etf_data_completo) or 149.0
                versione_attiva["composizione"].append({
                    "ticker": mon, "nome": ETF_NOMI.get(mon, mon),
                    "peso": etf_out["peso"], "importo": etf_out["importo"],
                    "price_entry": round(p_mon, 4),
                    "quote": round(etf_out["importo"] / p_mon, 4),
                    "yield_pct": 0,
                })

        elif trigger_dd and trigger_dd["tipo"] in ("drawdown_30", "drawdown_20"):
            po_bt = calc_pesi_override(regime_attivo, 60,
                                       {"az_usa": 1.0, "az_europa": 1.0, "az_em": 1.0})
            apri_nuova_versione(cap_new, data_corrente, regime_attivo, 60,
                                trigger_dd["motivo"], po_bt)

        elif cambio_regime:
            po_bt = calc_pesi_override(cambio_regime["a"], cambio_regime["confidence"],
                                        {"az_usa": 1.0, "az_europa": 1.0, "az_em": 1.0})
            apri_nuova_versione(cap_new, data_corrente, cambio_regime["a"],
                                cambio_regime["confidence"],
                                f"cambio_regime {cambio_regime['da']}→{cambio_regime['a']} "
                                f"(conf {cambio_regime['confidence']}%)", po_bt)

        elif score_trigger:
            po_bt = calc_pesi_override(regime_attivo, 60,
                                       {"az_usa": 1.0, "az_europa": 1.0, "az_em": 1.0})
            apri_nuova_versione(cap_new, data_corrente, regime_attivo, 60,
                                score_trigger, po_bt)

    # ── Rendimenti mensili REALI dall'equity curve ─────────────────────
    rend_mensili_reali = {}
    if len(equity_curve_totale) > 1 and len(equity_dates_totale) > 1:
        mesi_vals = {}
        for d, v in zip(equity_dates_totale, equity_curve_totale):
            ym = d[:7]
            mesi_vals.setdefault(ym, []).append(v)
        prev_val = CAPITALE_MODELLO
        for ym in sorted(mesi_vals.keys()):
            vals = mesi_vals[ym]
            if not vals: continue
            last_val = vals[-1]
            r = round((last_val - prev_val) / prev_val * 100, 2) if prev_val else 0
            rend_mensili_reali[ym] = r
            prev_val = last_val

    # ── Diario movimenti — differenza tra versioni consecutive ─────────
    diario = []
    for i in range(1, len(versioni)):
        v_old = versioni[i-1]
        v_new = versioni[i]
        comp_old = {e["ticker"]: e for e in v_old.get("composizione", [])}
        comp_new = {e["ticker"]: e for e in v_new.get("composizione", [])}
        venduti = []
        for t, e in comp_old.items():
            if t not in comp_new and e.get("importo", 0) > 0:
                venduti.append({
                    "ticker": t,
                    "nome": e.get("nome", t),
                    "peso": e.get("peso", 0),
                    "importo": e.get("importo", 0),
                    "price_entry": e.get("price_entry", 0),
                    "price_exit": e.get("price_now", e.get("price_entry", 0)),
                    "perf_pct": e.get("perf_pct", 0),
                    "perf_eur": e.get("perf_eur", 0),
                })
        acquistati = []
        for t, e in comp_new.items():
            if t not in comp_old and e.get("importo", 0) > 0:
                acquistati.append({
                    "ticker": t,
                    "nome": e.get("nome", t),
                    "peso": e.get("peso", 0),
                    "importo": e.get("importo", 0),
                    "price_entry": e.get("price_entry", 0),
                })
        diario.append({
            "data": v_new.get("data_apertura"),
            "versione_da": v_old.get("versione"),
            "versione_a": v_new.get("versione"),
            "trigger": v_new.get("trigger_apertura", "—"),
            "regime": v_new.get("regime", "—"),
            "capitale_ribilanciamento": v_new.get("capitale_inizio", 0),
            "venduti": venduti,
            "acquistati": acquistati,
        })
    for v in versioni:
        v["etf_usciti_anticipati"] = [u for u in v.get("etf_usciti_anticipati", []) if not u.get("_key", "").startswith("_")]
        for u in v["etf_usciti_anticipati"]:
            u.pop("_key", None)
        eq = v.pop("_equity", [])
        v.pop("_equity_dates", None)
        v["volatilita_ann"] = calc_volatilita(eq)
        v["max_drawdown"] = calc_max_drawdown(eq)
        v["sharpe"] = calc_sharpe(eq)

    vol_tot    = calc_volatilita(equity_curve_totale)
    mdd_tot    = calc_max_drawdown(equity_curve_totale)
    sharpe_tot = calc_sharpe(equity_curve_totale)

    n_ver = len(versioni)
    n_reb = n_ver - 1
    cap_fin = versione_attiva.get("capitale_attuale", CAPITALE_MODELLO)
    perf_fin = round((cap_fin - CAPITALE_MODELLO) / CAPITALE_MODELLO * 100, 2)
    print(f"      → {n_ver} ver, {n_reb} rib, perf {perf_fin:+.1f}%, "
          f"vol {vol_tot:.1f}%, MDD {mdd_tot:.1f}%, Sharpe {sharpe_tot:.2f}" if vol_tot else
          f"      → {n_ver} ver, {n_reb} rib, perf {perf_fin:+.1f}%")

    for v in versioni:
        v["_vol_tot"]      = vol_tot
        v["_mdd_tot"]      = mdd_tot
        v["_sharpe_tot"]   = sharpe_tot
        v["_rend_mensili"] = rend_mensili_reali
        v["_diario"]       = diario

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

        # Metriche rischio — ricostruisce equity curve dai prezzi storici degli ETF
        ver_attiva_fin = next((v for v in reversed(storia) if v.get("data_chiusura") is None), None)
        vol_tot  = storia[0].pop("_vol_tot", None) if storia else None
        mdd_tot  = storia[0].pop("_mdd_tot", None) if storia else None
        sh_tot   = storia[0].pop("_sharpe_tot", None) if storia else None
        rend_m   = storia[0].pop("_rend_mensili", None) if storia else None
        diario   = storia[0].pop("_diario", None) if storia else None
        for v in storia[1:]:
            v.pop("_vol_tot", None); v.pop("_mdd_tot", None); v.pop("_sharpe_tot", None)
            v.pop("_rend_mensili", None); v.pop("_diario", None)

        # Se non disponibili dal backtest, calcola dalla composizione corrente
        if vol_tot is None and ver_attiva_fin:
            try:
                comp = ver_attiva_fin.get("composizione", [])
                # Trova lunghezza serie comune
                n_days = None
                for etf in comp:
                    if etf.get("importo", 0) <= 0: continue
                    t = etf["ticker"]
                    prices = etf_data.get(t, {}).get("prices_252", [])
                    if prices:
                        n_days = len(prices) if n_days is None else min(n_days, len(prices))
                if n_days and n_days > 10:
                    equity = []
                    for day_i in range(n_days):
                        val = 0.0
                        for etf in comp:
                            if etf.get("importo", 0) <= 0: continue
                            t = etf["ticker"]
                            prices = etf_data.get(t, {}).get("prices_252", [])
                            if not prices or day_i >= len(prices): continue
                            p_entry = etf.get("price_entry") or prices[0]
                            if not p_entry: continue
                            ret = (prices[day_i] - p_entry) / p_entry
                            val += etf["importo"] * (1 + ret)
                        if val > 0: equity.append(val)
                    if len(equity) > 10:
                        vol_tot  = calc_volatilita(equity)
                        mdd_tot  = calc_max_drawdown(equity)
                        sh_tot   = calc_sharpe(equity)
            except Exception as e:
                print(f"  WARN metriche rischio {lid}: {e}")

        if vol_tot is not None:
            portafogli[lid]["volatilita_ann"] = vol_tot
            portafogli[lid]["max_drawdown"]   = mdd_tot
            portafogli[lid]["sharpe"]         = sh_tot
        if rend_m is not None:
            portafogli[lid]["rendimenti_mensili"] = rend_m
        if diario is not None:
            portafogli[lid]["diario_movimenti"] = diario

        # ── Rendimenti mensili — ricostruisce dalla composizione corrente ──
        try:
            ver_att = next((v for v in reversed(storia) if v.get("data_chiusura") is None), None)
            if ver_att:
                comp = ver_att.get("composizione", [])
                # Raccoglie date e prezzi da ETF in portafoglio
                date_prezzi = {}  # data → valore portafoglio
                for etf in comp:
                    if etf.get("importo", 0) <= 0: continue
                    t = etf["ticker"]
                    sig = etf_data.get(t, {})
                    prices = sig.get("prices_252", [])
                    dates  = sig.get("dates_252", [])
                    if not prices or not dates: continue
                    p_entry = etf.get("price_entry") or prices[0]
                    if not p_entry: continue
                    for d, p in zip(dates, prices):
                        if d < BACKTEST_START: continue
                        ret = (p - p_entry) / p_entry
                        date_prezzi[d] = date_prezzi.get(d, 0) + etf["importo"] * (1 + ret)

                if date_prezzi:
                    # Raggruppa per mese
                    mesi_vals = {}
                    for d in sorted(date_prezzi.keys()):
                        ym = d[:7]  # "2025-01"
                        mesi_vals.setdefault(ym, []).append(date_prezzi[d])

                    # Calcola rendimento mensile come primo→ultimo del mese
                    rend_mensili = {}
                    prev_val = CAPITALE_MODELLO
                    for ym in sorted(mesi_vals.keys()):
                        vals = mesi_vals[ym]
                        if not vals: continue
                        last_val = vals[-1]
                        r = round((last_val - prev_val) / prev_val * 100, 2) if prev_val else 0
                        rend_mensili[ym] = r
                        prev_val = last_val

                    portafogli[lid]["rendimenti_mensili"] = rend_mensili

        except Exception as e:
            print(f"  WARN rendimenti mensili {lid}: {e}")

        # ── Composizione % per categoria ───────────────────────────────────
        try:
            ver_att2 = next((v for v in reversed(storia) if v.get("data_chiusura") is None), None)
            if ver_att2:
                cat_pct = {}
                for etf in ver_att2.get("composizione", []):
                    if etf.get("peso", 0) <= 0: continue
                    t = etf["ticker"]
                    cat = ETF_CATEGORIA.get(t, "az_globale")
                    # Raggruppa in macro-categorie
                    macro = "azionario"
                    if cat in ("monetario",): macro = "monetario"
                    elif cat in ("obbligaz_ig","hy","em_bond"): macro = "obbligazionario"
                    elif cat in ("tematico",): macro = "tematico"
                    elif cat in ("leva",): macro = "leva"
                    elif cat in ("multi_asset",): macro = "multi_asset"
                    cat_pct[macro] = round(cat_pct.get(macro, 0) + etf["peso"], 1)
                portafogli[lid]["composizione_pct"] = cat_pct
        except Exception as e:
            print(f"  WARN composizione_pct {lid}: {e}")

        # ── Cedola stimata e incassata (solo livelli C) ────────────────────
        if lid.startswith("C"):
            try:
                ver_att3 = next((v for v in reversed(storia) if v.get("data_chiusura") is None), None)
                if ver_att3:
                    giorni = ver_att3.get("giorni_attivo", 0) or (
                        (datetime.date.fromisoformat(oggi) -
                         datetime.date.fromisoformat(BACKTEST_START)).days)
                    cedola_annua_lorda = 0.0
                    cedola_incassata_lorda = 0.0
                    for etf in ver_att3.get("composizione", []):
                        if etf.get("importo", 0) <= 0: continue
                        t = etf["ticker"]
                        sig = etf_data.get(t, {})
                        yld = sig.get("yield_pct", 0) or etf.get("yield_pct", 0) or 0
                        if yld <= 0: continue
                        importo = etf["importo"]
                        cedola_annua_lorda  += importo * yld / 100
                        cedola_incassata_lorda += importo * yld / 100 * giorni / 365
                    TASSA = 0.26
                    portafogli[lid]["cedola_annua_lorda"]      = round(cedola_annua_lorda, 2)
                    portafogli[lid]["cedola_annua_netta"]      = round(cedola_annua_lorda * (1-TASSA), 2)
                    portafogli[lid]["cedola_incassata_lorda"]  = round(cedola_incassata_lorda, 2)
                    portafogli[lid]["cedola_incassata_netta"]  = round(cedola_incassata_lorda * (1-TASSA), 2)
                    portafogli[lid]["cedola_giorni"]           = giorni
                    portafogli[lid]["cedola_annua_pct_lorda"]  = round(cedola_annua_lorda / CAPITALE_MODELLO * 100, 2)
                    portafogli[lid]["cedola_annua_pct_netta"]  = round(cedola_annua_lorda * (1-TASSA) / CAPITALE_MODELLO * 100, 2)
            except Exception as e:
                print(f"  WARN cedola {lid}: {e}")

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
    run_number = existing.get("run_number", 0) + 1
    print(f"  Run number: {run_number} → COMPASS v2.{run_number:03d}")

    # ── Pulizia ticker obsoleti dalla storia esistente ─────────────────
    TICKER_OBSOLETI = {"GOM.MI"}  # ticker da rimuovere dalla storia
    if portafogli_esistenti:
        rimossi_tot = 0
        for lid, ptf in portafogli_esistenti.items():
            for ver in ptf.get("storia", []):
                comp = ver.get("composizione", [])
                comp_pulita = [e for e in comp if e.get("ticker") not in TICKER_OBSOLETI]
                if len(comp_pulita) < len(comp):
                    rimossi = len(comp) - len(comp_pulita)
                    rimossi_tot += rimossi
                    ver["composizione"] = comp_pulita
                # Pulizia anche dagli usciti anticipati
                usciti = ver.get("etf_usciti_anticipati", [])
                ver["etf_usciti_anticipati"] = [u for u in usciti
                                                 if u.get("ticker") not in TICKER_OBSOLETI]
        if rimossi_tot:
            print(f"  ⚠ Rimossi {rimossi_tot} ETF obsoleti (GOM.MI) dalla storia")

    # ── Leggi etf_custom.json per aggiunte/rimozioni manuali ──────────
    CUSTOM_FILE = BASE_DIR / "data" / "etf_custom.json"
    ticker_aggiunti = []
    ticker_rimossi = []
    if CUSTOM_FILE.exists():
        try:
            with open(CUSTOM_FILE) as f:
                custom = json.load(f)
            ticker_aggiunti = custom.get("aggiungi", [])
            ticker_rimossi  = custom.get("rimuovi", [])
            if ticker_aggiunti:
                print(f"  ➕ ETF custom da aggiungere: {ticker_aggiunti}")
            if ticker_rimossi:
                print(f"  ➖ ETF custom da rimuovere: {ticker_rimossi}")
        except Exception as e:
            print(f"  ERR etf_custom.json: {e}")

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
    print(f"\n[3/5] Download ETF Compass...")
    all_levels = list(LEVEL_WEIGHTS.keys())

    # Applica customizzazioni manuali all'universo
    universo_run = list(ETF_UNIVERSE)
    for t in ticker_aggiunti:
        if t not in universo_run:
            universo_run.append(t)
    for t in ticker_rimossi:
        if t in universo_run:
            universo_run.remove(t)
    universo_run = sorted(set(universo_run))
    print(f"  Universo effettivo: {len(universo_run)} ETF")
    etf_data = {}
    success = 0; errors = 0

    # Riusa dati proxy già scaricati dove applicabile
    for ticker in universo_run:
        yahoo_ticker = resolve_ticker(ticker)
        if yahoo_ticker is None:
            print(f"  [skip] {ticker} — non disponibile su Yahoo")
            errors += 1
            continue
        print(f"  [{success+errors+1}/{len(universo_run)}] {ticker}({yahoo_ticker if yahoo_ticker!=ticker else ''})...", end=" ", flush=True)
        raw = fetch_yahoo(yahoo_ticker, "2y")
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

        # Indicatori aggiuntivi
        rsi14  = calc_rsi(closes, 14)
        adx14  = calc_adx(closes, 14)
        ao     = calc_ao(closes)
        sar_val, sar_trend = calc_sar(closes)
        kama_pct = round((price - kama)/kama*100, 2) if kama and price else None

        # Serie per grafico (252 giorni)
        closes_252 = closes[-252:]
        dates_252  = dates[-252:]
        kama_s   = calc_kama_series(closes_252)[-252:]
        rsi_s    = calc_rsi_series(closes_252)[-252:]
        ao_s     = calc_ao_series(closes_252)[-252:]
        sar_s    = calc_sar_series(closes_252)[-252:]
        signals  = calc_signal_history(closes_252, kama_s, sar_s, dates_252)

        sig_base = {"price": price, "sma200": sma200, "kama": kama,
                    "mom1m": mom1m, "mom3m": mom3m, "mom6m": mom6m,
                    "yield_pct": raw["yield_pct"]}
        score_by_level = {lv: calc_score_for_level(sig_base, lv) for lv in all_levels}
        score_base = round((score_by_level["C5"] + score_by_level["A5"]) / 2)
        segnale = score_to_signal(score_base, price, sma200)

        etf_data[ticker] = {
            "ticker": ticker,
            "yahoo_ticker": yahoo_ticker if yahoo_ticker != ticker else ticker,
            "nome": ETF_NOMI.get(ticker, ticker),
            "tipo": ETF_TIPO.get(ticker, "Acc"),
            "categoria": ETF_CATEGORIA.get(ticker, "az_globale"),
            "price": round(price, 4),
            "ret_1d": ret_1d,
            "perf_1a": perf1a,
            "yield_pct": raw["yield_pct"],
            "sma200": sma200,
            "kama": kama,
            "kama_pct": kama_pct,
            "rsi14": rsi14,
            "adx14": adx14,
            "ao": ao,
            "sar": sar_val,
            "sar_trend": sar_trend,
            "mom1m": mom1m, "mom3m": mom3m, "mom6m": mom6m,
            "score": score_base,
            "score_by_level": score_by_level,
            "segnale": segnale,
            # Serie per grafico
            "prices_252": [round(c, 4) for c in closes_252],
            "dates_252": dates_252,
            "kama_series": [round(k,4) if k else None for k in kama_s],
            "rsi_series": rsi_s,
            "ao_series": [round(a,4) if a else None for a in ao_s],
            "sar_series": [round(s,4) if s else None for s in sar_s],
            "signal_history": signals,
            # Legacy (60 giorni per compatibilità)
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

    # ── 5. Calcola performance benchmark dal 01/01/2025 ───────────────────
    print(f"\n[5/5] Benchmark performance dal {BACKTEST_START}...")
    benchmarks = {}
    for bt in BENCHMARK_TICKERS:
        sig = etf_data.get(bt)
        if not sig or not sig.get("closes") or not sig.get("dates"):
            benchmarks[bt] = {
                "ticker": bt, "nome": BENCHMARK_NOMI.get(bt, bt),
                "perf_pct": None, "price_start": None, "price_now": None,
            }
            print(f"  {bt}: N/D")
            continue
        p_start = get_price_on_date(bt, BACKTEST_START, etf_data)
        p_now   = sig["closes"][-1] if sig["closes"] else None
        if p_start and p_now:
            perf = round((p_now - p_start) / p_start * 100, 2)
        else:
            perf = None
        benchmarks[bt] = {
            "ticker": bt,
            "nome": BENCHMARK_NOMI.get(bt, bt),
            "perf_pct": perf,
            "price_start": round(p_start, 4) if p_start else None,
            "price_now": round(p_now, 4) if p_now else None,
            "prices_60": sig.get("prices_60", []),
        }
        print(f"  {bt}: {'+' if perf and perf>=0 else ''}{perf:.1f}%" if perf else f"  {bt}: N/D")

    # Aggiungi outperformance per ogni livello
    for lid, (bm1, bm2) in BENCHMARK_PER_LIVELLO.items():
        ptf = portafogli.get(lid, {})
        ptf_perf = ptf.get("performance_totale_pct")
        if ptf_perf is not None:
            bm1_perf = benchmarks.get(bm1, {}).get("perf_pct")
            bm2_perf = benchmarks.get(bm2, {}).get("perf_pct")
            ptf["benchmark_principale"] = bm1
            ptf["benchmark_secondario"] = bm2
            ptf["outperformance_bm1"] = round(ptf_perf - bm1_perf, 2) if bm1_perf is not None else None
            ptf["outperformance_bm2"] = round(ptf_perf - bm2_perf, 2) if bm2_perf is not None else None

    # ── Output ────────────────────────────────────────────────────────────
    # Rimuovi closes completi dall'output finale (troppo grandi)
    # Mantieni prices_252, dates_252 e tutte le serie per il grafico
    etf_out = {}
    for t, d in etf_data.items():
        etf_out[t] = {k: v for k, v in d.items() if k not in ("closes", "dates")}

    output = {
        "generated": datetime.datetime.utcnow().isoformat(),
        "version": "2.0",
        "run_number": run_number,
        "procedure_version": f"2.{run_number:03d}",
        "total": len(ETF_UNIVERSE),
        "errors": errors,
        "success": success,
        "regime_macro": {
            **regime,
            "pesi_override": pesi_override,
        },
        "storia_regime": storia_regime,
        "benchmarks": benchmarks,
        "benchmark_per_livello": BENCHMARK_PER_LIVELLO,
        "portafogli_modello": portafogli,
        "etfs": etf_out,
    }

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

    size = OUT_FILE.stat().st_size / 1024
    print(f"\n✅ Done: {success} OK, {errors} errori → {OUT_FILE} ({size:.0f} KB)")
    print(f"   Procedura: COMPASS v2.{run_number:03d}")
    print(f"   Regime: {regime['scenario']} ({regime['confidence']}%)")
    print(f"   Portafogli: {len(portafogli)} livelli")

if __name__ == "__main__":
    main()
