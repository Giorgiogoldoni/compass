#!/usr/bin/env python3
"""
COMPASS Alert — compass_alert.py v1.0
══════════════════════════════════════
Controlla i cambi di segnale dopo ogni run e li stampa prominentemente.
Da aggiungere al workflow come ultimo step prima del commit.

Alert generati:
1. Forecast 3 Linee: HOLD→WATCH o HOLD→ROTATE
2. Rotation suggerita in Factor C9/A9
3. Ribilancio Fase 2 > soglia
4. ETP: cambio regime o forecast
5. Linea A: ROTATION eseguita
"""

import json, datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
SEP = "═" * 60

def load_json(path):
    try:
        return json.loads(path.read_text())
    except:
        return {}

def check_alerts():
    alerts = []
    warnings = []
    info = []

    # ── 3 LINEE ────────────────────────────────────────────────────
    d3l = load_json(BASE_DIR / "data" / "compass_3linee.json")
    if d3l:
        fc = d3l.get("forecast", {})
        segnale = fc.get("segnale", "HOLD")
        regime_4w = fc.get("regime_4w", "—")
        prob_4w = fc.get("prob_4w", 0)
        n_att = fc.get("n_attivate", 0)

        if segnale == "ROTATE":
            alerts.append(f"🔴 3 LINEE — ROTATE → {regime_4w.upper()} ({prob_4w:.0f}%) | {n_att}/25 regole")
        elif segnale == "WATCH":
            warnings.append(f"⚠️  3 LINEE — WATCH → {regime_4w} ({prob_4w:.0f}%) | {n_att}/25 regole")
        else:
            info.append(f"✅ 3 LINEE — HOLD | Regime: {d3l.get('regime_oggi',{}).get('scenario','—')}")

        # Fase 2 ribilancio
        f2 = d3l.get("fase2", {})
        if f2.get("ribilancio_necessario"):
            mov = f2.get("movimenti", {})
            totale_mov = sum(abs(v) for v in mov.values())
            if totale_mov > 50000:
                alerts.append(f"⚡ FASE 2 — Ribilancio €{totale_mov:,.0f} | {f2.get('motivo','—')}")
            else:
                warnings.append(f"⚡ FASE 2 — Ribilancio €{totale_mov:,.0f} | {f2.get('motivo','—')}")

        # Rotation Linea A
        ptf = d3l.get("portafogli_3linee", {})
        rot_A = ptf.get("A", {}).get("rotation_suggerita")
        if rot_A:
            warnings.append(f"🔄 ROTATION LINEA A — {rot_A.get('out','?')} → {rot_A.get('in','?')} (gap {rot_A.get('gap_pct',0):+.1f}%)")

    # ── FACTOR ─────────────────────────────────────────────────────
    dfc = load_json(BASE_DIR / "data" / "compass_factor.json")
    if dfc:
        livelli = dfc.get("portafogli", {})
        for lid, ptf in livelli.items():
            rot = ptf.get("rotation_suggerita")
            if rot and lid in ("C9", "A9"):
                alerts.append(f"🔄 FACTOR {lid} ROTATION — {rot.get('out','?')} → {rot.get('in','?')}")
            elif rot and lid in ("C8", "A8", "C7", "A7"):
                warnings.append(f"🔄 FACTOR {lid} ROTATION — {rot.get('out','?')} → {rot.get('in','?')}")
        perf_best = max(
            (ptf.get("performance_totale_pct", 0) or 0 for ptf in livelli.values()),
            default=0
        )
        info.append(f"✅ FACTOR — Regime: {dfc.get('regime_oggi',{}).get('scenario','—')} | Best: {perf_best:+.1f}%")

    # ── ETP ────────────────────────────────────────────────────────
    detp = load_json(BASE_DIR / "data" / "compass_etp.json")
    if detp:
        fc_etp = detp.get("forecast", {})
        segnale_etp = fc_etp.get("segnale", "HOLD")
        regime_4w_etp = fc_etp.get("regime_4w", "—")
        prob_4w_etp = fc_etp.get("prob_4w", 0)
        perf_etp = detp.get("performance_totale_pct", 0)
        mdd_etp = detp.get("max_drawdown", 0)

        if segnale_etp == "ROTATE":
            alerts.append(f"🔴 ETP — ROTATE → {regime_4w_etp.upper()} ({prob_4w_etp:.0f}%)")
        elif segnale_etp == "WATCH":
            warnings.append(f"⚠️  ETP — WATCH → {regime_4w_etp} ({prob_4w_etp:.0f}%)")
        else:
            info.append(f"✅ ETP — HOLD | Perf: {perf_etp:+.1f}% | MDD: {mdd_etp:.1f}%")

        # Alert drawdown ETP > -10%
        if mdd_etp < -10:
            alerts.append(f"🚨 ETP — MDD CRITICO: {mdd_etp:.1f}%")

    # ── STAMPA ─────────────────────────────────────────────────────
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{SEP}")
    print(f"COMPASS ALERT — {now}")
    print(SEP)

    if alerts:
        print(f"\n{'🚨 AZIONI RICHIESTE':}")
        for a in alerts:
            print(f"  {a}")

    if warnings:
        print(f"\n⚠️  AVVISI:")
        for w in warnings:
            print(f"  {w}")

    if info:
        print(f"\nℹ️  STATUS:")
        for i in info:
            print(f"  {i}")

    if not alerts and not warnings:
        print(f"\n  ✅ Nessuna azione richiesta — tutti i sistemi stabili")

    print(f"\n{SEP}\n")

    # Exit code: 0 sempre (non blocca il workflow)
    return len(alerts), len(warnings)

if __name__ == "__main__":
    n_alert, n_warn = check_alerts()
    print(f"Riepilogo: {n_alert} alert critici, {n_warn} avvisi")
