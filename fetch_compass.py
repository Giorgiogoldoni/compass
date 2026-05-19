#!/usr/bin/env python3
"""
fetch_compass.py
Scarica da Yahoo Finance i dati degli ETF nel pool Compass:
  - yield annuo (cedola)
  - rendimento prezzo 1 anno (52WeekChange)
  - classificazione Dist vs Acc
Produce:
  data/compass_etf.json   → dati per ETF
  data/levels.json        → aggiornato con etf_pool_dist / etf_pool_acc
"""

import json, time, datetime, os, sys
import yfinance as yf

# ── Percorsi ─────────────────────────────────────────────────────────
BASE   = os.path.dirname(os.path.abspath(__file__))
LEVELS = os.path.join(BASE, 'data', 'levels.json')
OUT    = os.path.join(BASE, 'data', 'compass_etf.json')

# ── Carica levels.json ────────────────────────────────────────────────
with open(LEVELS) as f:
    levels_data = json.load(f)

all_tickers = sorted({t for lv in levels_data['levels'] for t in lv.get('etf_pool', [])})
print(f"[compass] Ticker da analizzare: {len(all_tickers)}")

# ── Classificazione Dist/Acc per nome ────────────────────────────────
# Regole base su nome e ticker; verrà raffinata da Yahoo
DIST_HINTS = {
    'Dist','dist','Distribution','distribution',
    'Income','income','Dividend','dividend',
    'IDVY','VHYL','EUDV','TDIV','FGEQ','DHS','EMDV','STHE',
    'HYLD','EUHI','IHYU','DHYA','SEMB','EUHA',
    'IUSA','IPRP','IWDP','SILVER','PHAU','GOM',
    'EMBE','JNHD','COPA','AIGA','CMOD',
    '2B70','IBTE','IBTM','SXRM','SXRV','EUNH','AGGH',
    'IGLO','VAGE','ERNX','SMART','XEOD','IU0E',
}

def guess_dist(ticker, name=''):
    """Restituisce True se l'ETF è probabilmente a distribuzione."""
    t = ticker.split('.')[0]
    if t in DIST_HINTS: return True
    for hint in ('Dist','dist','Income','income','Dividend','dividend'):
        if hint in name: return True
    # Acc esplicito nel nome → Acc
    for hint in (' Acc',' acc','Accumulation','accumulation'):
        if hint in name: return False
    return None  # non determinato

# ── Fetch Yahoo ───────────────────────────────────────────────────────
results = {}
errors  = []

for i, ticker in enumerate(all_tickers):
    print(f"  [{i+1}/{len(all_tickers)}] {ticker}", end=' ', flush=True)
    try:
        tk = yf.Ticker(ticker)
        info = tk.info

        long_name  = info.get('longName') or info.get('shortName') or ticker
        quote_type = info.get('quoteType', '')

        # Yield (cedola annua %)
        raw_yield = (
            info.get('trailingAnnualDividendYield') or
            info.get('yield') or
            info.get('dividendYield') or 0
        )
        yield_pct = round(float(raw_yield) * 100, 2) if raw_yield else 0.0

        # Rendimento prezzo 1 anno (%)
        perf_1a_raw = info.get('52WeekChange') or info.get('ytdReturn') or None
        if perf_1a_raw is not None:
            perf_1a = round(float(perf_1a_raw) * 100, 2)
        else:
            # Fallback: calcola da history
            try:
                hist = tk.history(period='1y', auto_adjust=True)
                if len(hist) >= 2:
                    p0 = float(hist['Close'].iloc[0])
                    p1 = float(hist['Close'].iloc[-1])
                    perf_1a = round((p1/p0 - 1) * 100, 2) if p0 else None
                else:
                    perf_1a = None
            except Exception:
                perf_1a = None

        # Prezzo corrente
        price = info.get('regularMarketPrice') or info.get('currentPrice') or None
        if price:
            price = round(float(price), 4)

        # Dist/Acc
        # 1. Segnale da Yahoo (dividendo pagato = Dist)
        if yield_pct > 0.1:
            is_dist = True
        else:
            # 2. Fallback su nome
            gd = guess_dist(ticker, long_name)
            is_dist = gd if gd is not None else False

        results[ticker] = {
            'ticker'   : ticker,
            'nome'     : long_name,
            'tipo'     : 'Dist' if is_dist else 'Acc',
            'yield_pct': yield_pct,
            'perf_1a'  : perf_1a,
            'price'    : price,
        }
        flag = '🔴' if is_dist else '🔵'
        y_str = f"yield={yield_pct:.1f}%" if yield_pct else ''
        p_str = f"perf1a={perf_1a:.1f}%" if perf_1a is not None else ''
        print(f"{flag} {y_str} {p_str}")

    except Exception as e:
        print(f"❌ {e}")
        errors.append(ticker)
        results[ticker] = {'ticker': ticker, 'tipo': 'Acc', 'yield_pct': 0.0, 'perf_1a': None, 'price': None}

    time.sleep(0.4)  # rate limiting

# ── Salva compass_etf.json ───────────────────────────────────────────
out_data = {
    'generated'  : datetime.datetime.utcnow().isoformat(),
    'total'      : len(all_tickers),
    'errors'     : len(errors),
    'etfs'       : results,
}
os.makedirs(os.path.dirname(OUT), exist_ok=True)
with open(OUT, 'w') as f:
    json.dump(out_data, f, ensure_ascii=False, indent=2)
print(f"\n[compass] Salvato {OUT}")

# ── Aggiorna levels.json con etf_pool_dist / etf_pool_acc ────────────
for lv in levels_data['levels']:
    pool      = lv.get('etf_pool', [])
    pool_dist = [t for t in pool if results.get(t, {}).get('tipo') == 'Dist']
    pool_acc  = [t for t in pool if results.get(t, {}).get('tipo') == 'Acc']
    lv['etf_pool_dist'] = pool_dist
    lv['etf_pool_acc']  = pool_acc

with open(LEVELS, 'w') as f:
    json.dump(levels_data, f, ensure_ascii=False, indent=2)
print(f"[compass] levels.json aggiornato con etf_pool_dist / etf_pool_acc")

# ── Summary ──────────────────────────────────────────────────────────
dist_count = sum(1 for v in results.values() if v.get('tipo') == 'Dist')
acc_count  = len(results) - dist_count
print(f"\n[compass] Riepilogo: {dist_count} Dist · {acc_count} Acc · {len(errors)} errori")
if errors:
    print(f"  Errori: {errors}")
