#!/usr/bin/env python3
"""
COMPASS ETP — compass_etp.py v1.0
══════════════════════════════════════════════════════════════════════
Portafoglio ETP combinato: universo RAPTOR (25) + ETF FACTOR (50) = 75 ETF

Logica:
  SCORE_FINALE = SCORE_PRESENTE × peso_presente + SCORE_FUTURO × peso_futuro

  peso_futuro  = max(0, (prob_4w - 50) / 50 * 0.40)   ← dinamico
  peso_presente = 1 - peso_futuro

Motore PRESENTE:
  - mom3M 35pt + RSI adattivo + ADX + AO + regime reale 21 proxy
  - Eredita logica FACTOR

Motore FUTURO:
  - Usa forecast_regime() da compass_3linee.py (stessi 21 proxy)
  - Preferenze macro per categoria (ereditate da RAPTOR MACRO_PREF)
  - Penalizza ETF incompatibili col regime_4w
  - Premia ETF compatibili

Output: data/compass_etp.json
"""

import json, math, datetime, time, urllib.request
from pathlib import Path

# ── CONFIGURAZIONE ─────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent
OUT_FILE       = BASE_DIR / "data" / "compass_etp.json"
BACKTEST_START = "2024-01-01"
CAPITALE       = 100_000
BENCHMARK      = "IWMO.MI"
BENCHMARK2     = "VWCE.DE"
N_ETF_MAX      = 12
N_ETF_MIN      = 7

# ── UNIVERSO 75 ETF ────────────────────────────────────────────────────────
UNIVERSE = [
    # ── AZIONARIO GLOBALE (15) ─────────────────────────────────────────────
    {"t":"WWRD.MI", "n":"WT World",                        "cat":"az_globale", "sub":"GLOBAL",    "fonte":"RAPTOR"},
    {"t":"SWDA.MI", "n":"iShares Core MSCI World",         "cat":"az_globale", "sub":"GLOBAL",    "fonte":"FACTOR"},
    {"t":"VWCE.DE", "n":"Vanguard FTSE All-World",         "cat":"az_globale", "sub":"GLOBAL",    "fonte":"FACTOR"},
    {"t":"XDWT.MI", "n":"Xtrackers MSCI World Swap",       "cat":"az_globale", "sub":"GLOBAL",    "fonte":"FACTOR"},
    {"t":"ESGE.MI", "n":"iShares MSCI World ESG",          "cat":"az_globale", "sub":"GLOBAL",    "fonte":"FACTOR"},
    {"t":"IWMO.MI", "n":"iShares MSCI World Momentum",     "cat":"az_globale", "sub":"GLOBAL_F",  "fonte":"FACTOR"},
    {"t":"IWQU.MI", "n":"iShares MSCI World Quality",      "cat":"az_globale", "sub":"GLOBAL_F",  "fonte":"FACTOR"},
    {"t":"WOEE.DE", "n":"iShares World Enhanced Active",   "cat":"az_globale", "sub":"GLOBAL_F",  "fonte":"FACTOR"},
    {"t":"IFSW.MI", "n":"iShares STOXX World Multifactor", "cat":"az_globale", "sub":"GLOBAL_F",  "fonte":"FACTOR"},
    {"t":"JPGL.MI", "n":"JPMorgan Global Multi-Factor",    "cat":"az_globale", "sub":"GLOBAL_F",  "fonte":"FACTOR"},
    {"t":"FCRN.DE", "n":"iShares World Factor Rotation",   "cat":"az_globale", "sub":"GLOBAL_F",  "fonte":"FACTOR"},
    {"t":"NTSX.MI", "n":"WT US Efficient Core",            "cat":"az_globale", "sub":"GLOBAL_EC", "fonte":"RAPTOR"},
    {"t":"NTSG.MI", "n":"WT Global Efficient Core",        "cat":"az_globale", "sub":"GLOBAL_EC", "fonte":"ENTRAMBI"},
    {"t":"IBCZ.DE", "n":"iShares STOXX World Multifactor", "cat":"az_globale", "sub":"GLOBAL_F",  "fonte":"FACTOR"},
    {"t":"IS07.DE", "n":"iShares STOXX World MF EUR Hdg",  "cat":"az_globale", "sub":"GLOBAL_F",  "fonte":"FACTOR"},
    # ── AZIONARIO EUROPA (8) ───────────────────────────────────────────────
    {"t":"WS5X.MI", "n":"WT Euro Stoxx 50",                "cat":"az_europa",  "sub":"EU",        "fonte":"RAPTOR"},
    {"t":"SMEA.MI", "n":"iShares Europe Small Cap",        "cat":"az_europa",  "sub":"EU_SMALL",  "fonte":"RAPTOR"},
    {"t":"EXX5.DE", "n":"iShares EURO STOXX 50",           "cat":"az_europa",  "sub":"EU",        "fonte":"FACTOR"},
    {"t":"EXV1.DE", "n":"iShares STOXX Europe 600",        "cat":"az_europa",  "sub":"EU",        "fonte":"FACTOR"},
    {"t":"EXXW.DE", "n":"iShares MSCI Europe",             "cat":"az_europa",  "sub":"EU",        "fonte":"FACTOR"},
    {"t":"EUEE.DE", "n":"iShares Europe Enhanced Active",  "cat":"az_europa",  "sub":"EU_F",      "fonte":"FACTOR"},
    {"t":"IEMO.MI", "n":"iShares MSCI Europe Momentum",    "cat":"az_europa",  "sub":"EU_F",      "fonte":"FACTOR"},
    {"t":"IEQU.MI", "n":"iShares MSCI Europe Quality",     "cat":"az_europa",  "sub":"EU_F",      "fonte":"FACTOR"},
    # ── AZIONARIO USA (8) ──────────────────────────────────────────────────
    {"t":"WSPX.MI", "n":"WT S&P 500",                      "cat":"az_usa",     "sub":"US",        "fonte":"RAPTOR"},
    {"t":"WSPE.MI", "n":"WT S&P 500 EUR Hedged",           "cat":"az_usa",     "sub":"US_EUR",    "fonte":"RAPTOR"},
    {"t":"WNAS.MI", "n":"WT Nasdaq-100",                   "cat":"az_usa",     "sub":"TECH",      "fonte":"RAPTOR"},
    {"t":"CSSPX.MI","n":"iShares Core S&P 500",            "cat":"az_usa",     "sub":"US",        "fonte":"FACTOR"},
    {"t":"USEE.DE", "n":"iShares US Enhanced Active",      "cat":"az_usa",     "sub":"US_F",      "fonte":"FACTOR"},
    {"t":"QDVB.DE", "n":"iShares MSCI USA Quality",        "cat":"az_usa",     "sub":"US_F",      "fonte":"FACTOR"},
    {"t":"XUTC.MI", "n":"Xtrackers MSCI USA IT",           "cat":"az_usa",     "sub":"TECH",      "fonte":"FACTOR"},
    {"t":"WRTY.MI", "n":"WT Russell 2000 Efficient Core",  "cat":"az_usa",     "sub":"US_SMALL",  "fonte":"RAPTOR"},
    # ── AZIONARIO EM/ASIA (11) ─────────────────────────────────────────────
    {"t":"VFEM.MI", "n":"Vanguard FTSE EM",                "cat":"az_em",      "sub":"EM_BROAD",  "fonte":"RAPTOR"},
    {"t":"EIMI.MI", "n":"iShares MSCI EM",                 "cat":"az_em",      "sub":"EM_CORE",   "fonte":"RAPTOR"},
    {"t":"DXJF.MI", "n":"WisdomTree Japan EUR Hedged",             "cat":"az_em",      "sub":"JAPAN",     "fonte":"RAPTOR"},
    {"t":"XCHA.MI", "n":"iShares China",                   "cat":"az_em",      "sub":"CHINA",     "fonte":"RAPTOR"},
    {"t":"XASX.DE", "n":"iShares Asia Pacific",            "cat":"az_em",      "sub":"ASIA_PAC",  "fonte":"RAPTOR"},
    {"t":"EMEE.MI", "n":"iShares EM Enhanced Active",      "cat":"az_em",      "sub":"EM_F",      "fonte":"FACTOR"},
    {"t":"AXEE.MI", "n":"iShares Asia ex Japan Enhanced",  "cat":"az_em",      "sub":"ASIA_F",    "fonte":"FACTOR"},
    {"t":"IS3N.DE", "n":"iShares MSCI EM Small Cap",       "cat":"az_em",      "sub":"EM_SMALL",  "fonte":"FACTOR"},
    {"t":"VAPX.MI", "n":"Vanguard Dev Asia Pacific",       "cat":"az_em",      "sub":"ASIA_PAC",  "fonte":"FACTOR"},
    {"t":"JPNH.MI", "n":"Amundi MSCI Japan EUR Hdg",       "cat":"az_em",      "sub":"JAPAN",     "fonte":"FACTOR"},
    {"t":"NTSZ.MI", "n":"WT EM Efficient Core",            "cat":"az_em",      "sub":"EM_EC",     "fonte":"RAPTOR"},
    # ── TEMATICO (10) ──────────────────────────────────────────────────────
    {"t":"PHAU.MI", "n":"WT Physical Gold",                "cat":"tematico",   "sub":"GOLD",      "fonte":"ENTRAMBI"},
    {"t":"CRUD.MI", "n":"WT WTI Crude Oil",                "cat":"tematico",   "sub":"CRUDE",     "fonte":"RAPTOR"},
    {"t":"SMH.MI",  "n":"VanEck Semiconductor",            "cat":"tematico",   "sub":"TECH_T",    "fonte":"FACTOR"},
    {"t":"DFNS.MI", "n":"VanEck Defense",                  "cat":"tematico",   "sub":"DEFENSE",   "fonte":"FACTOR"},
    {"t":"IART.DE", "n":"iShares AI Innovation",           "cat":"tematico",   "sub":"AI",        "fonte":"FACTOR"},
    {"t":"RARE.MI", "n":"VanEck Rare Earth",               "cat":"tematico",   "sub":"MATERIALS", "fonte":"FACTOR"},
    {"t":"COPA.MI", "n":"WT Copper",                       "cat":"tematico",   "sub":"MATERIALS", "fonte":"FACTOR"},
    {"t":"CMOD.MI", "n":"iShares Commodity",               "cat":"tematico",   "sub":"COMMODITY", "fonte":"FACTOR"},
    {"t":"AIGA.MI", "n":"WT Agriculture",                  "cat":"tematico",   "sub":"COMMODITY", "fonte":"FACTOR"},
    {"t":"IFFF.MI", "n":"iShares MSCI Global Financials",  "cat":"tematico",   "sub":"FINANCIAL", "fonte":"FACTOR"},
    # ── HIGH YIELD (5) ─────────────────────────────────────────────────────
    {"t":"STHY.MI", "n":"PIMCO US ST HY USD",              "cat":"hy",         "sub":"US_HY",     "fonte":"RAPTOR"},
    {"t":"STHE.MI", "n":"PIMCO US ST HY EUR",              "cat":"hy",         "sub":"US_HY_EUR", "fonte":"ENTRAMBI"},
    {"t":"EUHI.MI", "n":"PIMCO Euro ST HY",                "cat":"hy",         "sub":"EU_HY",     "fonte":"RAPTOR"},
    {"t":"EUHA.DE", "n":"PIMCO Euro HY Acc",               "cat":"hy",         "sub":"EU_HY_A",   "fonte":"RAPTOR"},
    {"t":"IHYU.MI", "n":"iShares USD HY EUR Hdg",          "cat":"hy",         "sub":"US_HY_EUR", "fonte":"FACTOR"},
    # ── OBBLIGAZ. IG (8) ───────────────────────────────────────────────────
    {"t":"PJS1.MI", "n":"PIMCO Euro Short Mat",            "cat":"obbligaz_ig","sub":"EU_IG",     "fonte":"RAPTOR"},
    {"t":"XGIU.MI", "n":"iShares Euro Govt Bond",          "cat":"obbligaz_ig","sub":"EU_GOVT",   "fonte":"RAPTOR"},
    {"t":"IEAC.MI", "n":"iShares EUR Corp Bond",           "cat":"obbligaz_ig","sub":"EU_IG",     "fonte":"FACTOR"},
    {"t":"IBGS.MI", "n":"iShares EUR Govt Bond 1-3yr",     "cat":"obbligaz_ig","sub":"EU_GOVT",   "fonte":"FACTOR"},
    {"t":"SEGA.MI", "n":"iShares EUR Govt Bond 1-5yr",     "cat":"obbligaz_ig","sub":"EU_GOVT",   "fonte":"FACTOR"},
    {"t":"XGSH.MI", "n":"Xtrackers Global Govt EUR Hdg",   "cat":"obbligaz_ig","sub":"GLOBAL_IG", "fonte":"FACTOR"},
    {"t":"XUCS.DE", "n":"Xtrackers USD Corp EUR Hdg",      "cat":"obbligaz_ig","sub":"US_IG",     "fonte":"FACTOR"},
    {"t":"EUEB.MI", "n":"iShares EUR Corp Bond Enhanced",  "cat":"obbligaz_ig","sub":"EU_IG",     "fonte":"FACTOR"},
    # ── EM BOND (4) ────────────────────────────────────────────────────────
    {"t":"EMLI.MI", "n":"PIMCO EM Local Bond",             "cat":"em_bond",    "sub":"EM_LOCAL",  "fonte":"RAPTOR"},
    {"t":"EMBE.MI", "n":"iShares JPM EM Bond EUR Hdg",     "cat":"em_bond",    "sub":"EM_USD",    "fonte":"FACTOR"},
    {"t":"SEML.MI", "n":"iShares JPM EM Local Bond",       "cat":"em_bond",    "sub":"EM_LOCAL",  "fonte":"FACTOR"},
    {"t":"IEMB.MI", "n":"iShares JPM USD EM Bond",         "cat":"em_bond",    "sub":"EM_USD",    "fonte":"FACTOR"},
    # ── MONETARIO (3) ──────────────────────────────────────────────────────
    {"t":"XEON.MI", "n":"Xtrackers EUR Overnight",         "cat":"monetario",  "sub":"CASH",      "fonte":"RAPTOR"},
    {"t":"SMART.MI","n":"iShares EUR Ultrashort Bond",     "cat":"monetario",  "sub":"CASH",      "fonte":"FACTOR"},
    {"t":"IU0E.MI", "n":"iShares EUR Ultrashort Dist",     "cat":"monetario",  "sub":"CASH",      "fonte":"FACTOR"},
    # ── LEVA (3) — solo in regime favorevole ───────────────────────────────
    {"t":"3USL.MI", "n":"WT S&P 500 3x Lev",               "cat":"leva",       "sub":"LEVA_US",   "fonte":"FACTOR"},
    {"t":"QQQ3.MI", "n":"WT Nasdaq 3x Lev",                "cat":"leva",       "sub":"LEVA_US",   "fonte":"FACTOR"},
    {"t":"3NVD.MI", "n":"Leverage Shares 3x NVIDIA",       "cat":"leva",       "sub":"LEVA_TEMA", "fonte":"FACTOR"},
]

# ── PREFERENZE MACRO PER CATEGORIA (ereditate da RAPTOR + estese) ───────────
# Scenari FACTOR: goldilocks, euforia, reflazione, stagflazione, risk_off, neutro
MACRO_PREF_ETP = {
    "goldilocks": {
        "az_globale":1.3,"az_europa":1.1,"az_usa":1.3,"az_em":1.2,
        "tematico":0.8,"hy":1.1,"obbligaz_ig":0.3,"em_bond":0.6,
        "monetario":0.0,"leva":1.2
    },
    "euforia": {
        "az_globale":1.4,"az_europa":1.2,"az_usa":1.5,"az_em":1.3,
        "tematico":1.6,"hy":1.3,"obbligaz_ig":0.2,"em_bond":0.5,
        "monetario":0.0,"leva":1.8
    },
    "reflazione": {
        "az_globale":1.1,"az_europa":1.0,"az_usa":1.0,"az_em":1.3,
        "tematico":1.5,"hy":1.2,"obbligaz_ig":0.4,"em_bond":1.2,
        "monetario":0.0,"leva":0.8
    },
    "stagflazione": {
        "az_globale":0.3,"az_europa":0.3,"az_usa":0.3,"az_em":0.2,
        "tematico":1.8,"hy":0.5,"obbligaz_ig":0.9,"em_bond":0.5,
        "monetario":1.5,"leva":0.1
    },
    "risk_off": {
        "az_globale":0.3,"az_europa":0.2,"az_usa":0.4,"az_em":0.1,
        "tematico":0.5,"hy":0.2,"obbligaz_ig":1.5,"em_bond":0.2,
        "monetario":2.0,"leva":0.0
    },
    "neutro": {
        "az_globale":1.0,"az_europa":1.0,"az_usa":1.0,"az_em":1.0,
        "tematico":1.0,"hy":1.0,"obbligaz_ig":1.0,"em_bond":1.0,
        "monetario":1.0,"leva":1.0
    },
}

# Preferenze per sub-tipo (più granulare)
SUB_PREF_ETP = {
    "goldilocks": {
        "GLOBAL":1.0,"GLOBAL_F":1.2,"GLOBAL_EC":1.1,
        "EU":0.9,"EU_SMALL":0.8,"EU_F":1.0,
        "US":0.9,"US_EUR":0.7,"TECH":0.9,"US_SMALL":0.7,"US_F":1.0,
        "EM_BROAD":0.7,"EM_CORE":0.7,"EM_F":0.8,"EM_SMALL":0.6,
        "JAPAN":0.8,"CHINA":0.7,"ASIA_PAC":0.7,"ASIA_F":0.8,"EM_EC":0.7,
        "GOLD":0.4,"CRUDE":0.5,"TECH_T":0.9,"DEFENSE":0.7,"AI":0.9,
        "MATERIALS":0.6,"COMMODITY":0.5,"FINANCIAL":0.8,
        "US_HY":0.8,"US_HY_EUR":0.7,"EU_HY":0.9,"EU_HY_A":0.85,
        "EU_IG":0.3,"EU_GOVT":0.2,"GLOBAL_IG":0.3,"US_IG":0.4,
        "EM_LOCAL":0.7,"EM_USD":0.6,"CASH":0.0,
        "LEVA_US":1.2,"LEVA_TEMA":0.9,
    },
    "euforia": {
        "GLOBAL":0.9,"GLOBAL_F":1.3,"GLOBAL_EC":1.0,
        "EU":0.8,"EU_SMALL":0.9,"EU_F":1.1,
        "US":0.9,"US_EUR":0.6,"TECH":1.2,"US_SMALL":0.9,"US_F":1.1,
        "EM_BROAD":0.8,"EM_CORE":0.8,"EM_F":0.9,"EM_SMALL":0.7,
        "JAPAN":0.8,"CHINA":0.8,"ASIA_PAC":0.8,"ASIA_F":0.9,"EM_EC":0.8,
        "GOLD":0.3,"CRUDE":0.6,"TECH_T":1.3,"DEFENSE":0.6,"AI":1.4,
        "MATERIALS":0.5,"COMMODITY":0.4,"FINANCIAL":0.9,
        "US_HY":0.9,"US_HY_EUR":0.8,"EU_HY":0.9,"EU_HY_A":0.85,
        "EU_IG":0.2,"EU_GOVT":0.1,"GLOBAL_IG":0.2,"US_IG":0.3,
        "EM_LOCAL":0.6,"EM_USD":0.5,"CASH":0.0,
        "LEVA_US":1.8,"LEVA_TEMA":1.5,
    },
    "reflazione": {
        "GLOBAL":0.8,"GLOBAL_F":0.9,"GLOBAL_EC":0.8,
        "EU":0.8,"EU_SMALL":0.7,"EU_F":0.8,
        "US":0.7,"US_EUR":0.6,"TECH":0.6,"US_SMALL":0.7,"US_F":0.8,
        "EM_BROAD":1.0,"EM_CORE":1.0,"EM_F":1.1,"EM_SMALL":0.8,
        "JAPAN":0.7,"CHINA":0.8,"ASIA_PAC":0.8,"ASIA_F":0.9,"EM_EC":1.0,
        "GOLD":0.9,"CRUDE":1.0,"TECH_T":0.6,"DEFENSE":0.7,"AI":0.6,
        "MATERIALS":1.0,"COMMODITY":0.9,"FINANCIAL":0.7,
        "US_HY":0.8,"US_HY_EUR":0.7,"EU_HY":0.8,"EU_HY_A":0.8,
        "EU_IG":0.4,"EU_GOVT":0.3,"GLOBAL_IG":0.4,"US_IG":0.5,
        "EM_LOCAL":1.0,"EM_USD":0.9,"CASH":0.0,
        "LEVA_US":0.5,"LEVA_TEMA":1.0,
    },
    "stagflazione": {
        "GLOBAL":0.3,"GLOBAL_F":0.3,"GLOBAL_EC":0.3,
        "EU":0.3,"EU_SMALL":0.2,"EU_F":0.3,
        "US":0.3,"US_EUR":0.5,"TECH":0.2,"US_SMALL":0.2,"US_F":0.3,
        "EM_BROAD":0.2,"EM_CORE":0.2,"EM_F":0.2,"EM_SMALL":0.2,
        "JAPAN":0.3,"CHINA":0.2,"ASIA_PAC":0.3,"ASIA_F":0.2,"EM_EC":0.2,
        "GOLD":1.0,"CRUDE":1.0,"TECH_T":0.2,"DEFENSE":0.8,"AI":0.2,
        "MATERIALS":0.8,"COMMODITY":0.9,"FINANCIAL":0.3,
        "US_HY":0.4,"US_HY_EUR":0.5,"EU_HY":0.4,"EU_HY_A":0.4,
        "EU_IG":0.9,"EU_GOVT":0.8,"GLOBAL_IG":0.8,"US_IG":0.7,
        "EM_LOCAL":0.4,"EM_USD":0.3,"CASH":0.8,
        "LEVA_US":0.0,"LEVA_TEMA":0.0,
    },
    "risk_off": {
        "GLOBAL":0.2,"GLOBAL_F":0.2,"GLOBAL_EC":0.2,
        "EU":0.2,"EU_SMALL":0.1,"EU_F":0.2,
        "US":0.3,"US_EUR":0.6,"TECH":0.2,"US_SMALL":0.1,"US_F":0.2,
        "EM_BROAD":0.1,"EM_CORE":0.1,"EM_F":0.1,"EM_SMALL":0.1,
        "JAPAN":0.3,"CHINA":0.1,"ASIA_PAC":0.2,"ASIA_F":0.1,"EM_EC":0.1,
        "GOLD":1.0,"CRUDE":0.2,"TECH_T":0.2,"DEFENSE":0.9,"AI":0.2,
        "MATERIALS":0.3,"COMMODITY":0.3,"FINANCIAL":0.2,
        "US_HY":0.2,"US_HY_EUR":0.3,"EU_HY":0.2,"EU_HY_A":0.2,
        "EU_IG":1.0,"EU_GOVT":1.0,"GLOBAL_IG":0.9,"US_IG":0.8,
        "EM_LOCAL":0.1,"EM_USD":0.1,"CASH":1.0,
        "LEVA_US":0.0,"LEVA_TEMA":0.0,
    },
    "neutro": {k:1.0 for k in [
        "GLOBAL","GLOBAL_F","GLOBAL_EC","EU","EU_SMALL","EU_F",
        "US","US_EUR","TECH","US_SMALL","US_F",
        "EM_BROAD","EM_CORE","EM_F","EM_SMALL","JAPAN","CHINA","ASIA_PAC","ASIA_F","EM_EC",
        "GOLD","CRUDE","TECH_T","DEFENSE","AI","MATERIALS","COMMODITY","FINANCIAL",
        "US_HY","US_HY_EUR","EU_HY","EU_HY_A",
        "EU_IG","EU_GOVT","GLOBAL_IG","US_IG",
        "EM_LOCAL","EM_USD","CASH","LEVA_US","LEVA_TEMA",
    ]},
}

# Vincoli peso per categoria
MAX_PESO_CAT = {
    "az_globale":35,"az_europa":20,"az_usa":25,"az_em":20,
    "tematico":25,"hy":20,"obbligaz_ig":30,"em_bond":15,
    "monetario":70,"leva":15,
}

# Vincoli peso per ETF singolo
PESO_MAX_ETF = {"IART.DE":12, "3NVD.MI":8, "3USL.MI":12, "QQQ3.MI":12}

# Proxy per classificazione regime (21 — stessi di compass_3linee.py)
ETF_PROXY = {
    "SPY" :{"goldilocks":0.9,"reflazione":0.7,"stagflazione":0.1,"risk_off":0.0,"neutro":0.5},
    "QQQ" :{"goldilocks":0.9,"reflazione":0.5,"stagflazione":0.0,"risk_off":0.0,"neutro":0.4},
    "IWM" :{"goldilocks":0.85,"reflazione":0.8,"stagflazione":0.1,"risk_off":0.0,"neutro":0.4},
    "VGK" :{"goldilocks":0.8,"reflazione":0.75,"stagflazione":0.15,"risk_off":0.0,"neutro":0.4},
    "EEM" :{"goldilocks":0.7,"reflazione":0.9,"stagflazione":0.2,"risk_off":0.0,"neutro":0.4},
    "EWJ" :{"goldilocks":0.7,"reflazione":0.65,"stagflazione":0.2,"risk_off":0.2,"neutro":0.5},
    "TLT" :{"goldilocks":0.4,"reflazione":0.1,"stagflazione":0.1,"risk_off":0.9,"neutro":0.5},
    "IEF" :{"goldilocks":0.5,"reflazione":0.2,"stagflazione":0.2,"risk_off":0.8,"neutro":0.5},
    "HYG" :{"goldilocks":0.85,"reflazione":0.7,"stagflazione":0.1,"risk_off":0.0,"neutro":0.5},
    "LQD" :{"goldilocks":0.6,"reflazione":0.4,"stagflazione":0.2,"risk_off":0.3,"neutro":0.5},
    "TIP" :{"goldilocks":0.5,"reflazione":0.85,"stagflazione":0.9,"risk_off":0.4,"neutro":0.5},
    "GLD" :{"goldilocks":0.3,"reflazione":0.8,"stagflazione":0.9,"risk_off":0.9,"neutro":0.5},
    "USO" :{"goldilocks":0.5,"reflazione":0.9,"stagflazione":0.85,"risk_off":0.2,"neutro":0.4},
    "VXX" :{"goldilocks":0.0,"reflazione":0.0,"stagflazione":0.4,"risk_off":1.0,"neutro":0.2},
    "UUP" :{"goldilocks":0.3,"reflazione":0.2,"stagflazione":0.6,"risk_off":0.7,"neutro":0.5},
    "EZU" :{"goldilocks":0.80,"reflazione":0.75,"stagflazione":0.10,"risk_off":0.00,"neutro":0.40},
    "ACWX":{"goldilocks":0.80,"reflazione":0.85,"stagflazione":0.10,"risk_off":0.00,"neutro":0.40},
    "VEA" :{"goldilocks":0.75,"reflazione":0.80,"stagflazione":0.10,"risk_off":0.00,"neutro":0.40},
    "ACWI":{"goldilocks":0.85,"reflazione":0.80,"stagflazione":0.10,"risk_off":0.00,"neutro":0.45},
    "VT"  :{"goldilocks":0.85,"reflazione":0.80,"stagflazione":0.10,"risk_off":0.00,"neutro":0.45},
    "EURUSD=X":{"goldilocks":0.50,"reflazione":0.70,"stagflazione":0.40,"risk_off":0.20,"neutro":0.50},
}
SCENARI = ["goldilocks","euforia","reflazione","stagflazione","risk_off","neutro"]

# ── UTILITIES ──────────────────────────────────────────────────────────────
def fetch_yahoo(ticker, days=200):
    import urllib.request, json as _json, time as _time
    end   = int(__import__('datetime').datetime.utcnow().timestamp())
    start = end - days * 86400
    url   = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
             f"?interval=1d&period1={start}&period2={end}&events=history")
    headers = {"User-Agent":"Mozilla/5.0","Accept":"application/json"}
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as r:
                data   = _json.loads(r.read())
            result = data.get("chart",{}).get("result")
            if not result: _time.sleep(2); continue
            ts     = result[0]["timestamp"]
            q      = result[0]["indicators"]["quote"][0]
            adj    = result[0]["indicators"].get("adjclose",[{}])[0].get("adjclose",q["close"])
            dates  = [__import__('datetime').datetime.utcfromtimestamp(t).strftime("%Y-%m-%d") for t in ts]
            closes = [float(v) if v else None for v in adj]
            highs  = [float(v) if v else None for v in q["high"]]
            lows   = [float(v) if v else None for v in q["low"]]
            valid  = [(d,c,h,l) for d,c,h,l in zip(dates,closes,highs,lows) if c]
            if len(valid) < 60: return None
            d,c,h,l = zip(*valid)
            return {"dates":list(d),"closes":list(c),"highs":list(h),"lows":list(l)}
        except Exception as e:
            _time.sleep(2*attempt+1)
    return None

def get_price_on_date(closes, dates, target_date):
    for i in range(len(dates)-1, -1, -1):
        if dates[i] <= target_date: return closes[i]
    return None

def calc_momentum(closes, days):
    if len(closes) <= days: return None
    old = closes[-(days+1)]
    return round((closes[-1]-old)/old*100, 2) if old else None

def calc_rsi(closes, period=14):
    if len(closes) < period+2: return None
    gains=[]; losses=[]
    for i in range(1, len(closes)):
        d = closes[i]-closes[i-1]
        gains.append(max(d,0)); losses.append(max(-d,0))
    ag = sum(gains[:period])/period
    al = sum(losses[:period])/period
    for i in range(period, len(closes)-1):
        ag = (ag*(period-1)+gains[i])/period
        al = (al*(period-1)+losses[i])/period
    return round(100-100/(1+(ag/al if al else 100)), 1)

def calc_er(closes, period=10):
    if len(closes) <= period: return None
    direction  = abs(closes[-1]-closes[-period-1])
    volatility = sum(abs(closes[i]-closes[i-1]) for i in range(-period,0))
    return round(direction/volatility, 4) if volatility else 0

def calc_ao(closes):
    if len(closes) < 34: return None
    s5  = sum(closes[-5:])/5
    s34 = sum(closes[-34:])/34
    return round(s5-s34, 4)

def calc_adx_simple(closes, period=14):
    """ADX approssimato da closes."""
    if len(closes) < period*2: return None
    diffs = [abs(closes[i]-closes[i-1]) for i in range(-period,0)]
    return round(sum(diffs)/period/closes[-1]*100*10, 1)

def calc_max_drawdown(closes):
    peak = closes[0]; mdd = 0
    for c in closes:
        if c > peak: peak = c
        dd = (c-peak)/peak*100
        if dd < mdd: mdd = dd
    return round(mdd, 2)

def calc_sharpe(returns_list, rf=0.03):
    if not returns_list: return None
    import math
    mean_r = sum(returns_list)/len(returns_list)
    if len(returns_list) < 2: return None
    var    = sum((r-mean_r)**2 for r in returns_list)/(len(returns_list)-1)
    std    = math.sqrt(var) if var > 0 else None
    if not std: return None
    return round((mean_r*252 - rf) / (std*math.sqrt(252)), 2)

# ── CLASSIFICAZIONE REGIME ─────────────────────────────────────────────────
def classify_regime(proxy_data, target_date):
    scores = {s:0.0 for s in SCENARI}
    n_proxy = 0
    for ticker, pesi in ETF_PROXY.items():
        d = proxy_data.get(ticker)
        if not d: continue
        closes = d["closes"]; dates = d["dates"]
        n = min(len(closes),len(dates))
        cl = [closes[i] for i in range(n) if dates[i] <= target_date]
        if len(cl) < 22: continue
        mom4w = (cl[-1]-cl[-22])/cl[-22]*100 if cl[-22] else 0
        for sc in SCENARI:
            scores[sc] += mom4w * pesi.get(sc,0)
        n_proxy += 1
    if not n_proxy: return "neutro", 50
    min_s = min(scores.values())
    if min_s < 0:
        for sc in scores: scores[sc] -= min_s
    tot = sum(scores.values()) or 1
    probs = {sc: scores[sc]/tot*100 for sc in SCENARI}
    best  = max(probs, key=probs.get)
    return best, round(probs[best])

# ── FORECAST REGIME ────────────────────────────────────────────────────────
def forecast_regime_etp(proxy_data, target_date, regime_attuale):
    """Calcola forecast a 4 settimane con peso dinamico."""
    scores = {s:0.0 for s in SCENARI}

    def get_delta(ticker):
        d = proxy_data.get(ticker)
        if not d: return None
        closes = d["closes"]; dates = d["dates"]
        n = min(len(closes),len(dates))
        cl = [closes[i] for i in range(n) if dates[i] <= target_date]
        if len(cl) < 22: return None
        rec  = (cl[-1]-cl[-10])/cl[-10]*100 if len(cl)>=10 else None
        past = (cl[-11]-cl[-22])/cl[-22]*100 if len(cl)>=22 else None
        if rec is None or past is None: return None
        return rec - past

    def get_delta_diff(t1, t2):
        d1 = get_delta(t1); d2 = get_delta(t2)
        if d1 is None or d2 is None: return None
        return d1 - d2

    # Strato soft
    for ticker, pesi in ETF_PROXY.items():
        delta = get_delta(ticker)
        if delta is None: continue
        for sc in SCENARI:
            scores[sc] += delta * pesi.get(sc,0) * 10

    # Regole hard (R1-R25)
    REGOLE = [
        ("VXX",   ">",  8.0, {"risk_off":30}),
        ("VXX",   ">",  4.0, {"risk_off":15}),
        ("SPY-TLT",">", 6.0, {"goldilocks":25,"euforia":15}),
        ("SPY-TLT","<",-6.0, {"risk_off":25}),
        ("SPY-TLT","<",-3.0, {"risk_off":15}),
        ("HYG-LQD","<",-2.0, {"risk_off":20,"stagflazione":10}),
        ("HYG-LQD",">", 2.0, {"goldilocks":15,"euforia":10}),
        ("GLD",   ">",  4.0, {"reflazione":20,"risk_off":10}),
        ("GLD",   ">",  2.0, {"reflazione":10}),
        ("USO",   ">",  6.0, {"reflazione":25,"stagflazione":15}),
        ("USO",   "<", -6.0, {"risk_off":10,"neutro":10}),
        ("TIP-TLT",">", 2.0, {"reflazione":20,"stagflazione":10}),
        ("TIP-TLT","<",-2.0, {"goldilocks":15,"risk_off":5}),
        ("EEM-SPY",">", 3.0, {"reflazione":20}),
        ("EEM-SPY","<",-4.0, {"risk_off":15,"stagflazione":5}),
        ("UUP",   ">",  3.0, {"risk_off":15,"stagflazione":10}),
        ("UUP",   "<", -2.0, {"reflazione":15,"goldilocks":5}),
        ("EZU-SPY",">", 3.0, {"goldilocks":20,"reflazione":10}),
        ("EZU-SPY","<",-4.0, {"risk_off":15,"stagflazione":5}),
        ("ACWX-SPY",">",3.0, {"reflazione":20,"goldilocks":10}),
        ("ACWX-SPY","<",-4.0,{"risk_off":20,"stagflazione":5}),
        ("ACWI-SPY",">",3.0, {"reflazione":15,"goldilocks":10}),
        ("ACWI-SPY","<",-4.0,{"risk_off":15}),
        ("EURUSD=X",">",2.0, {"reflazione":15,"goldilocks":10}),
        ("EURUSD=X","<",-2.0,{"risk_off":10,"stagflazione":10}),
    ]

    n_attivate = 0
    for proxy, op, soglia, contributi in REGOLE:
        if "-" in proxy and proxy not in ETF_PROXY:
            parts = proxy.split("-")
            delta = get_delta_diff(parts[0], parts[1])
        else:
            delta = get_delta(proxy)
        if delta is None: continue
        attivata = delta > soglia if op==">" else delta < soglia
        if attivata:
            for sc, pts in contributi.items():
                scores[sc] += pts * 2
            n_attivate += 1

    # Normalizzazione
    min_s = min(scores.values())
    if min_s < 0:
        for sc in scores: scores[sc] -= min_s
    tot = sum(scores.values()) or 1
    probs = {sc: round(scores[sc]/tot*100, 1) for sc in SCENARI}

    regime_4w = max(probs, key=probs.get)
    prob_4w   = probs[regime_4w]

    # Segnale
    stesso = regime_4w == regime_attuale
    if stesso or prob_4w < 55:
        segnale = "HOLD"
    elif prob_4w < 65:
        segnale = "WATCH"
    else:
        segnale = "ROTATE"

    # Peso futuro dinamico
    peso_futuro  = max(0.0, (prob_4w - 50) / 50 * 0.40) if not stesso else 0.0
    peso_presente = 1.0 - peso_futuro

    return {
        "regime_4w":     regime_4w,
        "prob_4w":       prob_4w,
        "probs":         probs,
        "segnale":       segnale,
        "n_attivate":    n_attivate,
        "peso_futuro":   round(peso_futuro, 3),
        "peso_presente": round(peso_presente, 3),
    }

# ── SCORE PRESENTE ─────────────────────────────────────────────────────────
def calc_score_presente(etf, regime_oggi):
    closes = etf.get("closes", [])
    if not closes or len(closes) < 34: return 0

    mom3m  = calc_momentum(closes, 63) or 0
    mom1m  = calc_momentum(closes, 21) or 0
    rsi    = calc_rsi(closes) or 50
    er     = calc_er(closes) or 0
    ao     = calc_ao(closes) or 0

    # Score base momentum
    score = 0
    score += min(35, max(0, (mom3m + 20) / 40 * 35))
    score += min(15, max(0, (mom1m + 10) / 20 * 15))
    score += min(15, max(0, (rsi - 30) / 50 * 15))
    score += min(15, max(0, er / 0.4 * 15))
    if ao > 0: score += 10
    if ao > 0 and len(closes) >= 2:
        ao_prev = calc_ao(closes[:-1])
        if ao_prev and ao > ao_prev: score += 5

    # Moltiplicatore regime per categoria
    cat    = etf.get("cat", "az_globale")
    mult   = MACRO_PREF_ETP.get(regime_oggi, MACRO_PREF_ETP["neutro"]).get(cat, 1.0)
    sub    = etf.get("sub", "")
    sub_m  = SUB_PREF_ETP.get(regime_oggi, SUB_PREF_ETP["neutro"]).get(sub, 1.0)

    score = score * mult * sub_m

    # Penalità RSI ipercomprato
    if rsi > 82: score *= 0.5
    elif rsi > 75: score *= 0.75

    return min(100, round(score))

# ── SCORE FUTURO ──────────────────────────────────────────────────────────
def calc_score_futuro(etf, regime_4w, prob_4w):
    closes = etf.get("closes", [])
    if not closes: return 0

    cat   = etf.get("cat", "az_globale")
    sub   = etf.get("sub", "")
    mult  = MACRO_PREF_ETP.get(regime_4w, MACRO_PREF_ETP["neutro"]).get(cat, 1.0)
    sub_m = SUB_PREF_ETP.get(regime_4w, SUB_PREF_ETP["neutro"]).get(sub, 1.0)

    # Score base: preferenza macro per il regime futuro
    score = 50 * mult * sub_m

    # Bonus se momentum recente è nella direzione del regime futuro
    mom1m = calc_momentum(closes, 21) or 0
    if regime_4w in ("goldilocks","euforia") and mom1m > 0:
        score *= 1.1
    elif regime_4w in ("risk_off","stagflazione") and mom1m < 0:
        score *= 1.1

    return min(100, round(score))

# ── BACKTEST ───────────────────────────────────────────────────────────────
def run_backtest_etp(etf_data, proxy_data, backtest_start, oggi):
    import datetime as dt
    start_dt = dt.date.fromisoformat(backtest_start)
    end_dt   = dt.date.fromisoformat(oggi)
    capitale = float(CAPITALE)

    # Genera date di rebalancing (ogni 5 giorni lavorativi)
    all_dates = []
    for t, d in etf_data.items():
        all_dates.extend(d.get("dates",[]))
    all_dates = sorted(set(d for d in all_dates if d >= backtest_start and d <= oggi))
    rebal_dates = [all_dates[i] for i in range(0, len(all_dates), 5)]
    if all_dates and all_dates[-1] not in rebal_dates:
        rebal_dates.append(all_dates[-1])

    versioni = []
    composizione_attuale = []
    capitale_corrente    = capitale
    rendimenti_settimanali = {}
    storia_regime = []

    for idx, rdate in enumerate(rebal_dates):
        # Classifica regime
        regime_oggi, conf = classify_regime(proxy_data, rdate)
        storia_regime.append({"data": rdate, "regime": regime_oggi, "conf": conf})

        # Forecast
        fc = forecast_regime_etp(proxy_data, rdate, regime_oggi)

        # Score per ogni ETF
        candidati = []
        for etf in UNIVERSE:
            t = etf["t"]
            d = etf_data.get(t)
            if not d or not d.get("closes"): continue

            # Closes fino a rdate
            n = min(len(d["closes"]), len(d["dates"]))
            cl = [d["closes"][i] for i in range(n) if d["dates"][i] <= rdate]
            if len(cl) < 34: continue

            etf_snap = {**etf, "closes": cl}

            # Score presente e futuro
            sp = calc_score_presente(etf_snap, regime_oggi)
            sf = calc_score_futuro(etf_snap, fc["regime_4w"], fc["prob_4w"])

            # Score finale con pesi dinamici
            score_finale = sp * fc["peso_presente"] + sf * fc["peso_futuro"]

            # Vincolo leva: solo in euforia/goldilocks con confidence >= 50%
            if etf["cat"] == "leva":
                if regime_oggi not in ("goldilocks","euforia"):
                    score_finale *= 0.1
                elif conf < 50:
                    # Confidence bassa: dimezza il peso della leva
                    score_finale *= 0.5

            # Vincolo forecast: blocca leva in risk_off/stagflazione forecast
            if etf["cat"] == "leva" and fc["regime_4w"] in ("risk_off","stagflazione") and fc["segnale"] in ("ROTATE","WATCH"):
                score_finale = 0

            candidati.append({
                "ticker": t,
                "nome":   etf["n"],
                "cat":    etf["cat"],
                "sub":    etf["sub"],
                "score":  round(score_finale, 1),
                "sp":     sp,
                "sf":     sf,
                "price":  cl[-1],
            })

        if not candidati: continue

        # Selezione top N_ETF_MAX
        candidati.sort(key=lambda x: x["score"], reverse=True)

        # Diversificazione: max 2 per categoria
        selected = []; cat_count = {}
        for c in candidati:
            if len(selected) >= N_ETF_MAX: break
            n_cat = cat_count.get(c["cat"], 0)
            if n_cat >= 2: continue
            cat_count[c["cat"]] = n_cat + 1
            selected.append(c)

        if len(selected) < N_ETF_MIN: selected = candidati[:N_ETF_MAX]
        if not selected: continue

        # Garantisce almeno 1 ETF difensivo se non presente
        cats_sel = {c["cat"] for c in selected}
        if "obbligaz_ig" not in cats_sel and "monetario" not in cats_sel:
            difensivi = [c for c in candidati
                        if c["cat"] in ("obbligaz_ig","monetario","hy")
                        and c["ticker"] not in {s["ticker"] for s in selected}]
            if difensivi:
                difensivi.sort(key=lambda x: x["score"], reverse=True)
                if len(selected) >= N_ETF_MAX:
                    selected[-1] = difensivi[0]
                else:
                    selected.append(difensivi[0])

        # Pesi proporzionali allo score
        tot_score = sum(c["score"] for c in selected) or 1
        for c in selected:
            p = round(c["score"] / tot_score * 100, 1)
            # Applica cap ETF singolo
            max_p = PESO_MAX_ETF.get(c["ticker"], 100)
            # Applica cap categoria
            cat_max = MAX_PESO_CAT.get(c["cat"], 40)
            c["peso"] = min(p, max_p, cat_max)

        # Rinormalizza dopo i cap
        tot_peso = sum(c["peso"] for c in selected) or 1
        for c in selected:
            c["peso"] = round(c["peso"] / tot_peso * 100, 1)
            c["importo"] = round(capitale_corrente * c["peso"] / 100, 2)

        # Calcola rendimento portafoglio da data precedente a oggi
        if idx > 0 and composizione_attuale and rebal_dates[idx-1] < rdate:
            prev_date = rebal_dates[idx-1]
            ptf_ret = 0.0
            for pos in composizione_attuale:
                t = pos["ticker"]
                d = etf_data.get(t)
                if not d: continue
                n = min(len(d["closes"]), len(d["dates"]))
                p_prev = next((d["closes"][i] for i in range(n-1,-1,-1) if d["dates"][i] <= prev_date), None)
                p_now  = next((d["closes"][i] for i in range(n-1,-1,-1) if d["dates"][i] <= rdate), None)
                if p_prev and p_now and p_prev > 0:
                    ptf_ret += (p_now - p_prev) / p_prev * pos["peso"] / 100

            capitale_corrente = round(capitale_corrente * (1 + ptf_ret), 2)
            rendimenti_settimanali[rdate] = round(ptf_ret * 100, 4)

            for c in selected:
                c["importo"] = round(capitale_corrente * c["peso"] / 100, 2)

        composizione_attuale = selected
        versioni.append({
            "data":         rdate,
            "regime":       regime_oggi,
            "conf":         conf,
            "forecast":     fc,
            "composizione": selected,
            "capitale":     round(capitale_corrente, 2),
        })

    # Performance finale
    perf_tot = round((capitale_corrente - CAPITALE) / CAPITALE * 100, 2)
    perf_eur = round(capitale_corrente - CAPITALE, 2)

    # Equity curve mensile
    equity_mensile = []
    cap_tmp = float(CAPITALE)
    months_seen = set()
    for rdate, ret in sorted(rendimenti_settimanali.items()):
        cap_tmp = round(cap_tmp * (1 + ret/100), 2)
        month = rdate[:7]
        if month not in months_seen:
            equity_mensile.append({"mese": month, "valore": cap_tmp})
            months_seen.add(month)

    # MDD e Sharpe
    cap_series = [CAPITALE] + [v["capitale"] for v in versioni]
    peak = cap_series[0]; mdd = 0
    for c in cap_series:
        if c > peak: peak = c
        dd = (c-peak)/peak*100
        if dd < mdd: mdd = dd
    mdd = round(mdd, 2)

    rets = list(rendimenti_settimanali.values())
    sharpe = calc_sharpe(rets) if len(rets) > 4 else None

    # ── Metriche avanzate ─────────────────────────────────────────────
    # Sharpe 6M e 12M
    def sharpe_n(ret_list, n, rf=0.03/52):
        import math
        if len(ret_list) < n: return None
        w = ret_list[-n:]
        mean_r = sum(w)/len(w) - rf
        var = sum((r-sum(w)/len(w))**2 for r in w)/(len(w)-1) if len(w)>1 else 0
        std = math.sqrt(var) if var > 0 else 0
        return round(mean_r/std*math.sqrt(52), 2) if std > 0 else None

    rets_list = [rendimenti_settimanali[d] for d in sorted(rendimenti_settimanali)]
    sharpe_6m  = sharpe_n(rets_list, 26)
    sharpe_12m = sharpe_n(rets_list, 52)

    # Rolling Sharpe 13W
    def rolling_sharpe_series(ret_list, window=13, rf=0.03/52):
        import math
        result = []
        dates_s = sorted(rendimenti_settimanali.keys())
        for i in range(window, len(ret_list)+1):
            w = ret_list[i-window:i]
            mean_r = sum(w)/len(w) - rf
            var = sum((r-sum(w)/len(w))**2 for r in w)/(len(w)-1) if len(w)>1 else 0
            std = math.sqrt(var) if var > 0 else 0
            sh = round(mean_r/std*math.sqrt(52), 3) if std > 0 else 0
            d = dates_s[i-1] if i-1 < len(dates_s) else None
            result.append({"data": d, "sharpe": sh})
        return result

    rolling_sh = rolling_sharpe_series(rets_list, 13)

    # Drawdown series settimanale
    cap_series = [float(CAPITALE)]
    for d in sorted(rendimenti_settimanali.keys()):
        cap_series.append(cap_series[-1] * (1 + rendimenti_settimanali[d]/100))

    peak = cap_series[0]
    dd_series = []
    dates_w = sorted(rendimenti_settimanali.keys())
    for i, v in enumerate(cap_series[1:]):
        if v > peak: peak = v
        dd = round((v-peak)/peak*100, 3)
        dd_series.append({"data": dates_w[i], "dd": dd})

    # Rendimenti mensili strutturati per anno
    from collections import defaultdict
    rend_per_anno = defaultdict(dict)
    prev_val = float(CAPITALE)
    for e in equity_mensile:
        anno, mese = e['mese'].split('-')
        ret = round((e['valore']-prev_val)/prev_val*100, 2)
        rend_per_anno[anno][mese] = ret
        prev_val = e['valore']

    # Rendimento annuo
    rend_annuo = {}
    for anno, mesi in rend_per_anno.items():
        cum = 1.0
        for r in mesi.values():
            cum *= (1 + r/100)
        rend_annuo[anno] = round((cum-1)*100, 2)

    # Turnover medio
    turnovers = []
    for i in range(1, len(versioni)):
        prev_c = set(p['ticker'] for p in versioni[i-1].get('composizione',[]))
        curr_c = set(p['ticker'] for p in versioni[i].get('composizione',[]))
        changed = len(prev_c.symmetric_difference(curr_c))
        tot = max(len(prev_c), len(curr_c))
        if tot > 0: turnovers.append(changed/tot*100)
    turnover_medio = round(sum(turnovers)/len(turnovers), 1) if turnovers else 0

    # Performance per regime
    perf_per_regime = defaultdict(list)
    for v in versioni:
        regime = v.get('regime','neutro')
        data = v.get('data')
        if data and data in rendimenti_settimanali:
            perf_per_regime[regime].append(rendimenti_settimanali[data])
    perf_regime_summary = {
        reg: {
            "media_sett": round(sum(rets)/len(rets), 3),
            "n": len(rets),
            "positivi": sum(1 for r in rets if r > 0),
        }
        for reg, rets in perf_per_regime.items()
    }

    return {
        "performance_totale_pct": perf_tot,
        "performance_totale_eur": perf_eur,
        "capitale_attuale":       round(capitale_corrente, 2),
        "max_drawdown":           mdd,
        "sharpe":                 sharpe,
        "sharpe_6m":              sharpe_6m,
        "sharpe_12m":             sharpe_12m,
        "rolling_sharpe":         rolling_sh,
        "drawdown_series":        dd_series,
        "rend_per_anno":          dict(rend_per_anno),
        "rend_annuo":             rend_annuo,
        "turnover_medio":         turnover_medio,
        "perf_per_regime":        perf_regime_summary,
        "versioni":               versioni,
        "composizione_corrente":  composizione_attuale,
        "rendimenti_settimanali": rendimenti_settimanali,
        "equity_mensile":         equity_mensile,
        "storia_regime":          storia_regime[-20:],
        "n_rebalancing":          len(versioni),
    }

# ── MAIN ───────────────────────────────────────────────────────────────────
def main():
    import datetime as dt
    oggi = dt.date.today().isoformat()
    print(f"COMPASS ETP v1.0 — {oggi}")
    print(f"Universo: {len(UNIVERSE)} ETF | Benchmark: {BENCHMARK}")

    # Carica dati esistenti
    existing = {}
    if OUT_FILE.exists():
        try:
            existing = json.loads(OUT_FILE.read_text())
            run_number = existing.get("run_number", 0) + 1
            print(f"  Dati esistenti: {existing.get('generated','—')}")
        except:
            run_number = 1
    else:
        run_number = 1
    print(f"  Run number: {run_number}")

    # ── 1. Download proxy (21) ─────────────────────────────────────────────
    print(f"\n[1/4] Download {len(ETF_PROXY)} ETF proxy...")
    proxy_data = {}
    for ticker in ETF_PROXY:
        d = fetch_yahoo(ticker, days=200)
        if d:
            proxy_data[ticker] = d
            print(f"  {ticker}... OK")
        else:
            print(f"  {ticker}... ERR")
        time.sleep(0.3)

    # Regime oggi
    regime_oggi, conf_oggi = classify_regime(proxy_data, oggi)
    print(f"\n  Regime oggi: {regime_oggi} ({conf_oggi}%)")

    # ── 2. Download ETF universo (75) ──────────────────────────────────────
    tickers_unici = list({e["t"] for e in UNIVERSE})
    print(f"\n[2/4] Download {len(tickers_unici)} ETF universo...")
    etf_data = {}
    ok = 0; err = 0
    for i, ticker in enumerate(sorted(tickers_unici), 1):
        d = fetch_yahoo(ticker, days=400)
        if d:
            etf_data[ticker] = d
            mom3m = calc_momentum(d["closes"], 63)
            sp = calc_score_presente({**next(e for e in UNIVERSE if e["t"]==ticker), "closes":d["closes"]}, regime_oggi)
            print(f"  [{i}/{len(tickers_unici)}] {ticker}... OK score={sp}")
            ok += 1
        else:
            print(f"  [{i}/{len(tickers_unici)}] {ticker}... ERR")
            err += 1
        time.sleep(0.3)
    print(f"  Download: {ok} OK, {err} ERR")

    # ── 3. Backtest ────────────────────────────────────────────────────────
    print(f"\n[3/4] Backtest ETP (da {BACKTEST_START})...")
    risultato = run_backtest_etp(etf_data, proxy_data, BACKTEST_START, oggi)
    print(f"  Performance: {risultato['performance_totale_pct']:+.1f}% | "
          f"MDD: {risultato['max_drawdown']:.1f}% | "
          + ('Sharpe: ' + f"{risultato['sharpe']:.2f}" if risultato.get('sharpe') else 'Sharpe: —'))
    print(f"  Rebalancing eseguiti: {risultato['n_rebalancing']}")

    # ── 4. Benchmark ───────────────────────────────────────────────────────
    print(f"\n[4/4] Benchmark {BENCHMARK} + {BENCHMARK2}...")

    def calc_bm_perf(ticker, data):
        d = data.get(ticker)
        if not d: return None
        p_start = get_price_on_date(d["closes"], d["dates"], BACKTEST_START)
        p_end   = d["closes"][-1]
        return round((p_end-p_start)/p_start*100, 2) if p_start and p_end else None

    bm_perf  = calc_bm_perf(BENCHMARK, etf_data)
    bm2_perf = calc_bm_perf(BENCHMARK2, etf_data)

    if bm_perf:
        outperf = round(risultato["performance_totale_pct"] - bm_perf, 2)
        print(f"  {BENCHMARK}: {bm_perf:+.1f}% | Outperf: {outperf:+.1f}pp")
        risultato["benchmark"]        = BENCHMARK
        risultato["benchmark_perf"]   = bm_perf
        risultato["outperformance"]   = outperf
        risultato["batte_benchmark"]  = risultato["performance_totale_pct"] > bm_perf
    if bm2_perf:
        outperf2 = round(risultato["performance_totale_pct"] - bm2_perf, 2)
        print(f"  {BENCHMARK2}: {bm2_perf:+.1f}% | Outperf: {outperf2:+.1f}pp")
        risultato["benchmark2"]       = BENCHMARK2
        risultato["benchmark2_perf"]  = bm2_perf
        risultato["outperformance2"]  = outperf2

    # Correlazione con RAPTOR
    try:
        import urllib.request as _ur
        rp_data = json.loads(_ur.urlopen(
            "https://giorgiogoldoni.github.io/portafoglio/data/portfolio.json"
        ).read())
        rend_rp = {}
        for entry in rp_data.get("history", []):
            dt = entry.get("date")
            ptf = entry.get("portfolio", [])
            r1w = sum((p.get("weight",0)/100)*(p.get("ret_1w",0) or 0) for p in ptf)
            rend_rp[dt] = round(r1w, 4)

        rend_etp = risultato["rendimenti_settimanali"]
        comuni = sorted(set(rend_etp.keys()) & set(rend_rp.keys()))
        if len(comuni) >= 5:
            import math as _m
            e_vals = [rend_etp[d] for d in comuni]
            r_vals = [rend_rp[d]  for d in comuni]
            mean_e = sum(e_vals)/len(e_vals)
            mean_r = sum(r_vals)/len(r_vals)
            cov = sum((e-mean_e)*(r-mean_r) for e,r in zip(e_vals,r_vals))/(len(comuni)-1)
            std_e = _m.sqrt(sum((e-mean_e)**2 for e in e_vals)/(len(comuni)-1))
            std_r = _m.sqrt(sum((r-mean_r)**2 for r in r_vals)/(len(comuni)-1))
            corr  = round(cov/(std_e*std_r), 3) if std_e*std_r > 0 else None
            risultato["corr_raptor"] = corr
            risultato["corr_raptor_n"] = len(comuni)
            print(f"  Correlazione ETP-RAPTOR: {corr} (n={len(comuni)} settimane)")
    except Exception as ce:
        print(f"  Correlazione RAPTOR: ERR ({ce})")

    # Forecast corrente
    fc_oggi = forecast_regime_etp(proxy_data, oggi, regime_oggi)
    print(f"\n  Forecast: {fc_oggi['segnale']} → {fc_oggi['regime_4w']} "
          f"({fc_oggi['prob_4w']:.0f}%) | "
          f"peso_futuro={fc_oggi['peso_futuro']:.2f}")

    # ── Output ────────────────────────────────────────────────────────────
    output = {
        "generated":    datetime.datetime.utcnow().isoformat(),
        "version":      "etp_1.0",
        "run_number":   run_number,
        "strategy":     "COMPASS ETP — Presente + Futuro dinamico",
        "backtest_start": BACKTEST_START,
        "benchmark":    BENCHMARK,
        "n_etf_universo": len(UNIVERSE),
        "regime_oggi":  {"scenario": regime_oggi, "confidence": conf_oggi},
        "forecast":     fc_oggi,
        **risultato,
    }

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",",":"))

    size = OUT_FILE.stat().st_size / 1024
    print(f"\n✅ Done → {OUT_FILE} ({size:.0f} KB)")
    print(f"   Run: {run_number} | Regime: {regime_oggi} ({conf_oggi}%)")
    print(f"   Performance: {risultato['performance_totale_pct']:+.1f}% | "
          f"vs {BENCHMARK}: {risultato.get('outperformance',0):+.1f}pp | "
          f"MDD: {risultato['max_drawdown']:.1f}%")
    print(f"\n   Portafoglio corrente:")
    for pos in risultato["composizione_corrente"][:8]:
        print(f"   {pos['ticker']:<14} {pos['peso']:>5.1f}% | "
              f"score={pos['score']:.0f} (P={pos['sp']} F={pos['sf']})")

if __name__ == "__main__":
    main()
