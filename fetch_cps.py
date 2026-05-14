#!/usr/bin/env python3
"""
COMPASS — fetch_cps.py
Calcola segnali per 85 ETF usando KAMA, SMA200, Mom1M, Mom3M
Produce data/rwm_signals.json
"""

import json, os, time
from datetime import datetime, timezone
import yfinance as yf
import pandas as pd
import numpy as np

# ── ETF UNIVERSE ─────────────────────────────────────────────────
ETF_UNIVERSE = [
    "2B70.MI","2LVE.MI","2NVD.MI","3EUL.MI","3NVD.MI","3USL.MI",
    "AGGH.MI","AIGA.MI","CMOD.MI","COPA.MI","CSPX.MI","DFNS.MI",
    "DHS.MI","DHYA.MI","EMBE.MI","EMDV.MI","EQQQ.MI","ERNX.MI",
    "ESGE.MI","EUDV.MI","EUHA.MI","EUHI.MI","EUNH.MI","EXV1.DE",
    "EXX5.DE","EXXW.DE","FGEQ.MI","GOM.MI","HYLD.MI","IBTE.MI",
    "IBTM.MI","IDVY.MI","IEAC.MI","IEAG.MI","IFFF.MI","IGLO.MI",
    "IHYU.MI","IPRP.MI","IS3N.MI","ISPA.DE","IU0E.MI","IUIT.MI",
    "IUSA.MI","IWDP.MI","JNHD.MI","JPNH.MI","L2SP.MI","MEUD.MI",
    "NTSG.MI","NTSX.MI","NTSZ.MI","PHAU.MI","QNTM.MI","QQQ3.MI",
    "RARE.MI","REET.MI","SEMB.MI","SILVER.MI","SMART.MI","SMH.MI",
    "STHE.MI","SWDA.MI","SXRM.MI","SXRV.MI","TDIV.MI","UC44.MI",
    "VAGE.MI","VAPX.MI","VHYL.MI","VUSA.MI","VWCE.DE","WENT.MI",
    "WHCS.MI","WRTY.MI","WS5X.MI","WSPE.MI","WSPX.MI","WWRD.MI",
    "XAIX.MI","XDWT.MI","XEOD.MI","XEON.MI","XREA.MI","XUCS.MI",
    "XUTC.MI",
]

ETF_NAMES = {
    "2B70.MI":"iShares USD HY Bond EUR Hed",
    "2LVE.MI":"Amundi Leva 2x Euro Stoxx 50",
    "2NVD.MI":"GraniteShares 2x NVIDIA",
    "3EUL.MI":"WisdomTree EURO STOXX 50 3x",
    "3NVD.MI":"GraniteShares 3x NVIDIA",
    "3USL.MI":"WisdomTree S&P 500 3x",
    "AGGH.MI":"iShares Core Global Agg EUR Hed",
    "AIGA.MI":"iShares Global Aggregate ACC",
    "CMOD.MI":"iShares Diversified Commodity",
    "COPA.MI":"WisdomTree Copper",
    "CSPX.MI":"iShares Core S&P 500 Acc",
    "DFNS.MI":"VanEck Defence ETF",
    "DHS.MI":"WisdomTree Global Quality Div",
    "DHYA.MI":"iShares EUR HY Corp Bond EUR Hed",
    "EMBE.MI":"iShares JPM EM Bond EUR Hed",
    "EMDV.MI":"iShares JPM EM Local Bond",
    "EQQQ.MI":"Invesco NASDAQ-100",
    "ERNX.MI":"iShares EUR Ultrashort Bond Acc",
    "ESGE.MI":"iShares MSCI World ESG Enhanced",
    "EUDV.MI":"iShares EUR Dividend",
    "EUHA.MI":"PIMCO Euro High Yield Acc",
    "EUHI.MI":"PIMCO Euro ST High Yield Dist",
    "EUNH.MI":"iShares Core EUR Govt Bond",
    "EXV1.DE":"iShares STOXX Europe 600 Acc",
    "EXX5.DE":"iShares Core EURO STOXX 50",
    "EXXW.DE":"iShares MSCI World Acc",
    "FGEQ.MI":"Fidelity Global Quality Income",
    "GOM.MI":"iShares EUR Govt Bond 3-5yr",
    "HYLD.MI":"iShares EUR HY Corp Bond Dist",
    "IBTE.MI":"iShares EUR Govt Bond 0-1yr",
    "IBTM.MI":"iShares EUR Govt Bond 3-7yr",
    "IDVY.MI":"iShares EUR Dividend",
    "IEAC.MI":"iShares Core EUR Corp Bond",
    "IEAG.MI":"iShares Core EUR Aggregate Bond",
    "IFFF.MI":"iShares MSCI World Financials",
    "IGLO.MI":"iShares Global Govt Bond EUR Hed",
    "IHYU.MI":"iShares USD HY Corp Bond EUR Hed",
    "IPRP.MI":"iShares European Property Dist",
    "IS3N.MI":"iShares MSCI EM Asia",
    "ISPA.DE":"iShares STOXX EU Sel Div 30",
    "IU0E.MI":"Lyxor Smart Overnight Return",
    "IUIT.MI":"iShares S&P 500 IT Sector",
    "IUSA.MI":"iShares Core S&P 500 Dist",
    "IWDP.MI":"iShares Dev Markets Property",
    "JNHD.MI":"JPMorgan USD Ultra-Short Bond",
    "JPNH.MI":"Amundi MSCI Japan EUR Hedged",
    "L2SP.MI":"Amundi Leva 2x S&P 500",
    "MEUD.MI":"Lyxor MSCI EMU Dist",
    "NTSG.MI":"WisdomTree Global Efficient Core",
    "NTSX.MI":"WisdomTree US Efficient Core",
    "NTSZ.MI":"WisdomTree EM Efficient Core",
    "PHAU.MI":"WisdomTree Physical Gold",
    "QNTM.MI":"VanEck Quantum Computing",
    "QQQ3.MI":"WisdomTree NASDAQ-100 3x",
    "RARE.MI":"VanEck Rare Earth & Strategic",
    "REET.MI":"iShares Global REIT",
    "SEMB.MI":"iShares JPM EM Bond Dist",
    "SILVER.MI":"WisdomTree Physical Silver",
    "SMART.MI":"iShares EUR Ultrashort Bond Dist",
    "SMH.MI":"VanEck Semiconductor",
    "STHE.MI":"PIMCO US ST HY EUR Hed",
    "SWDA.MI":"iShares Core MSCI World Acc",
    "SXRM.MI":"iShares USD Treasury 7-10yr",
    "SXRV.MI":"iShares EUR Corp Bond 1-5yr",
    "TDIV.MI":"VanEck Morningstar Dev Div",
    "UC44.MI":"Amundi Leva 2x NASDAQ-100",
    "VAGE.MI":"Vanguard EUR Corporate Bond",
    "VAPX.MI":"Vanguard FTSE Asia Pacific",
    "VHYL.MI":"Vanguard FTSE All-World High Div",
    "VUSA.MI":"Vanguard S&P 500 Dist",
    "VWCE.DE":"Vanguard FTSE All-World Acc",
    "WENT.MI":"WisdomTree Emerging Markets",
    "WHCS.MI":"WisdomTree Healthcare",
    "WRTY.MI":"WisdomTree Russell 2000 EC",
    "WS5X.MI":"WisdomTree Euro Stoxx 50",
    "WSPE.MI":"WisdomTree S&P 500 EUR Hed",
    "WSPX.MI":"WisdomTree S&P 500",
    "WWRD.MI":"WisdomTree World Equity",
    "XAIX.MI":"Xtrackers AI & Big Data",
    "XDWT.MI":"Xtrackers MSCI World",
    "XEOD.MI":"Xtrackers EUR Overnight",
    "XEON.MI":"Xtrackers EUR Overnight Swap",
    "XREA.MI":"Xtrackers FTSE Developed REIT",
    "XUCS.MI":"Xtrackers USA Consumer Disc",
    "XUTC.MI":"Xtrackers USD Corp Bond",
}

# ── KAMA ─────────────────────────────────────────────────────────
def calcola_kama(close: pd.Series, n=10, fast=2, slow=30) -> pd.Series:
    fast_sc = 2 / (fast + 1)
    slow_sc = 2 / (slow + 1)
    kama = close.copy().astype(float)
    for i in range(n, len(close)):
        direction = abs(close.iloc[i] - close.iloc[i - n])
        volatility = sum(abs(close.iloc[j] - close.iloc[j - 1]) for j in range(i - n + 1, i + 1))
        er = direction / volatility if volatility != 0 else 0
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
        kama.iloc[i] = kama.iloc[i - 1] + sc * (close.iloc[i] - kama.iloc[i - 1])
    return kama

def calcola_segnale(score: int) -> str:
    if score == 4:   return "BUY"
    elif score == 3: return "HOLD"
    elif score == 2: return "WATCH"
    else:            return "SELL"

# ── MAIN ─────────────────────────────────────────────────────────
def main():
    print(f"[CPS] Avvio fetch_cps.py — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"[CPS] ETF da analizzare: {len(ETF_UNIVERSE)}")

    results = {}
    ok_count = 0
    fail_count = 0

    for ticker in ETF_UNIVERSE:
        try:
            df = yf.download(ticker, period="1y", interval="1d",
                             auto_adjust=True, progress=False)
            if df is None or len(df) < 63:
                print(f"  ✗ {ticker}: dati insufficienti ({len(df) if df is not None else 0} barre)")
                fail_count += 1
                continue

            close = df["Close"].squeeze().dropna()
            if len(close) < 63:
                fail_count += 1
                continue

            price     = float(close.iloc[-1])
            price_1d  = float(close.iloc[-2]) if len(close) >= 2  else price
            price_21d = float(close.iloc[-21]) if len(close) >= 21 else price
            price_63d = float(close.iloc[-63]) if len(close) >= 63 else price

            # Indicatori
            sma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else float(close.mean())
            kama   = float(calcola_kama(close).iloc[-1])
            mom1m  = (price / price_21d - 1) * 100
            mom3m  = (price / price_63d - 1) * 100
            ret1d  = (price / price_1d  - 1) * 100

            # Score 0-4
            s_sma200 = 1 if price > sma200 else 0
            s_kama   = 1 if price > kama   else 0
            s_mom1m  = 1 if mom1m > 0      else 0
            s_mom3m  = 1 if mom3m > 0      else 0
            score    = s_sma200 + s_kama + s_mom1m + s_mom3m
            segnale  = calcola_segnale(score)

            results[ticker] = {
                "ticker":   ticker,
                "nome":     ETF_NAMES.get(ticker, ticker),
                "price":    round(price, 4),
                "ret_1d":   round(ret1d, 3),
                "sma200":   round(sma200, 4),
                "kama":     round(kama, 4),
                "mom1m":    round(mom1m, 2),
                "mom3m":    round(mom3m, 2),
                "s_sma200": s_sma200,
                "s_kama":   s_kama,
                "s_mom1m":  s_mom1m,
                "s_mom3m":  s_mom3m,
                "score":    score,
                "segnale":  segnale,
                "prices_60": [round(float(v), 4) for v in close.tail(60).tolist()],
            }
            ok_count += 1
            print(f"  ✓ {ticker}: {segnale} ({score}/4) prezzo={price:.2f}")

        except Exception as e:
            print(f"  ✗ {ticker}: {e}")
            fail_count += 1

        time.sleep(0.15)

    buy   = sum(1 for v in results.values() if v["segnale"] in ("BUY","HOLD"))
    watch = sum(1 for v in results.values() if v["segnale"] == "WATCH")
    sell  = sum(1 for v in results.values() if v["segnale"] == "SELL")

    output = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "analyzed":  ok_count,
        "failed":    fail_count,
        "stats": {"buy_hold": buy, "watch": watch, "sell": sell},
        "etfs": results,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/rwm_signals.json", "w") as f:
        json.dump(output, f, separators=(",", ":"))

    print(f"\n[CPS] Completato: {ok_count} ok / {fail_count} falliti")
    print(f"[CPS] BUY/HOLD: {buy} · WATCH: {watch} · SELL: {sell}")
    print("[CPS] Scritto: data/rwm_signals.json")

if __name__ == "__main__":
    main()
