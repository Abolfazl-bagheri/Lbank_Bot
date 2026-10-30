"""
trendlines_breaks.py
SolAtlas-style multi-touch trendlines + break/retest (approximate).
Still WIP — not 1:1 with OpenMarket WASM.
"""
from __future__ import annotations

from io import BytesIO
from typing import Any

import numpy as np
import pandas as pd

SW = 3
ZONE = 0.005
MIN_TOUCH = 3
BRK_ATR = 0.3
RT_BARS = 18


def y_at(i0: float, p0: float, i1: float, p1: float, i: float) -> float:
    return p0 + (p1 - p0) * (i - i0) / max(i1 - i0, 1)


def detect_pivots(h: np.ndarray, l: np.ndarray, w: int = SW) -> tuple[list[int], list[int]]:
    ph, pl = [], []
    n = len(h)
    for i in range(w, n - w):
        if h[i] >= np.max(h[i - w : i + w + 1]) - 1e-12:
            ph.append(i)
        if l[i] <= np.min(l[i - w : i + w + 1]) + 1e-12:
            pl.append(i)
    return ph, pl


def find_trendlines(
    df: pd.DataFrame,
    max_res: int = 3,
    max_sup: int = 2,
) -> list[dict[str, Any]]:
    """Return list of resistance/support lines with touches, break, retest."""
    if df is None or len(df) < 40:
        return []
    o = df["o"].astype(float).values
    h = df["h"].astype(float).values
    l = df["l"].astype(float).values
    c = df["c"].astype(float).values
    n = len(df)
    tr = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    atr = pd.Series(np.concatenate([[float(tr[0])], tr.astype(float)])).rolling(14).mean().values
    ph, pl = detect_pivots(h, l)

    def build(idxs: list[int], prices: np.ndarray, kind: str, limit: int) -> list[dict]:
        cands = []
        for a in range(len(idxs)):
            for b in range(a + 1, len(idxs)):
                i0, i1 = idxs[a], idxs[b]
                if i1 - i0 < 8:
                    continue
                p0, p1 = float(prices[i0]), float(prices[i1])
                slope = (p1 - p0) / (i1 - i0)
                touches = []
                for i in idxs:
                    y = y_at(i0, p0, i1, p1, i)
                    if abs(prices[i] - y) / max(abs(y), 1e-9) <= ZONE:
                        touches.append(i)
                if len(touches) < MIN_TOUCH:
                    continue
                t0, t1 = touches[0], touches[-1]
                viol = 0
                for i in range(t0, t1 + 1):
                    y = y_at(i0, p0, i1, p1, i)
                    if kind == "res" and c[i] > y * (1 + ZONE * 2.5):
                        viol += 1
                    if kind == "sup" and c[i] < y * (1 - ZONE * 2.5):
                        viol += 1
                if viol / max(t1 - t0, 1) > 0.32:
                    continue
                y_now = y_at(i0, p0, i1, p1, n - 1)
                side_ok = (c[-1] <= y_now * (1 + ZONE)) if kind == "res" else (c[-1] >= y_now * (1 - ZONE))
                dist = abs(c[-1] - y_now) / max(c[-1], 1e-9)
                score = len(touches) * 20 + (18 if side_ok else -25) - dist * 40 + (t1 / n) * 8
                cands.append({
                    "kind": kind,
                    "i0": i0,
                    "p0": p0,
                    "i1": i1,
                    "p1": p1,
                    "touches": touches,
                    "n_touch": len(touches),
                    "y_now": y_now,
                    "slope": slope,
                    "score": score,
                    "side_ok": side_ok,
                })
        cands.sort(key=lambda x: (not x["side_ok"], -x["score"]))
        picked = []
        for x in cands:
            ok = True
            for p in picked:
                if abs(x["y_now"] - p["y_now"]) / max(abs(x["y_now"]), 1) < 0.01 and abs(x["slope"] - p["slope"]) < abs(x["slope"]) * 0.2 + 1e-6:
                    ok = False
                    break
            if ok:
                picked.append(x)
            if len(picked) >= limit:
                break
        out = []
        for L in picked:
            broken = retest = None
            start = max(L["i1"], L["touches"][-1]) + 1
            for i in range(start, n):
                y = y_at(L["i0"], L["p0"], L["i1"], L["p1"], i)
                thr = max((atr[i] * BRK_ATR) if atr[i] == atr[i] else y * 0.003, y * 0.002)
                if L["kind"] == "res":
                    if broken is None and c[i] > y + thr:
                        broken = i
                    elif broken is not None and retest is None and i - broken <= RT_BARS:
                        if l[i] <= y + thr * 0.5 and c[i] >= y - thr:
                            retest = i
                else:
                    if broken is None and c[i] < y - thr:
                        broken = i
                    elif broken is not None and retest is None and i - broken <= RT_BARS:
                        if h[i] >= y - thr * 0.5 and c[i] <= y + thr:
                            retest = i
            L["broken"] = broken
            L["retest"] = retest
            out.append(L)
        return out

    return build(ph, h, "res", max_res) + build(pl, l, "sup", max_sup)


def render_trendline_chart(
    df: pd.DataFrame,
    symbol: str,
    tf: str,
    gap_pct: float | None = None,
    max_bars: int = 120,
) -> bytes | None:
    """White chart: green/red candles, 1 solid RES + 1 solid SUP, rest dotted."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
    except ImportError:
        return None
    try:
        plot = df.tail(min(max_bars, len(df))).reset_index(drop=True)
        n = len(plot)
        if n < 30:
            return None
        o = plot["o"].astype(float).values
        h = plot["h"].astype(float).values
        l = plot["l"].astype(float).values
        c = plot["c"].astype(float).values
        lines = find_trendlines(plot, max_res=3, max_sup=2)

        # فقط یک خط اصلی پررنگ برای هر سمت
        primary_res = next((x for x in lines if x["kind"] == "res" and x.get("side_ok") and not x.get("broken")), None)
        if primary_res is None:
            primary_res = next((x for x in lines if x["kind"] == "res"), None)
        primary_sup = next((x for x in lines if x["kind"] == "sup" and x.get("side_ok") and not x.get("broken")), None)
        if primary_sup is None:
            primary_sup = next((x for x in lines if x["kind"] == "sup"), None)

        fig = plt.figure(figsize=(11.2, 5.6), dpi=140, facecolor="#ffffff")
        ax = fig.add_axes([0.07, 0.10, 0.70, 0.82])
        ax.set_facecolor("#ffffff")

        # عرض بدنه نسبت به تعداد کندل
        body_w = max(0.35, min(0.7, 28.0 / max(n, 1)))
        wick_w = max(0.6, body_w * 0.9)

        for i in range(n):
            up = c[i] >= o[i]
            col = "#26a69a" if up else "#ef5350"  # سبز صعود / قرمز نزول
            # سایه
            ax.plot([i, i], [l[i], h[i]], color=col, solid_capstyle="round", lw=wick_w, zorder=2)
            # بدنه
            bot = min(o[i], c[i])
            height = abs(c[i] - o[i])
            if height < 1e-15:
                height = (h[i] - l[i]) * 0.02 + 1e-15
            ax.add_patch(
                Rectangle(
                    (i - body_w / 2, bot),
                    body_w,
                    height,
                    facecolor=col,
                    edgecolor=col,
                    linewidth=0.4,
                    zorder=3,
                )
            )

        for L in lines:
            is_res = L["kind"] == "res"
            is_primary = (is_res and L is primary_res) or ((not is_res) and L is primary_sup)
            if is_res:
                col = "#d32f2f"
            else:
                col = "#2e7d32"
            if is_primary:
                ls, lw = "-", 2.15
            else:
                ls, lw = ":", 1.15  # نقطه‌چین
            y0 = y_at(L["i0"], L["p0"], L["i1"], L["p1"], 0)
            y1 = y_at(L["i0"], L["p0"], L["i1"], L["p1"], n - 1)
            ax.plot([0, n - 1], [y0, y1], color=col, lw=lw, ls=ls, solid_capstyle="round", zorder=4, alpha=0.95 if is_primary else 0.75)
            if is_primary:
                for ti in L["touches"]:
                    if 0 <= ti < n:
                        ax.scatter(
                            ti,
                            y_at(L["i0"], L["p0"], L["i1"], L["p1"], ti),
                            s=18,
                            c=col,
                            zorder=5,
                            edgecolors="#333",
                            lw=0.3,
                        )
            if is_primary and L.get("broken") is not None:
                bi = L["broken"]
                ax.annotate(
                    "BREAK",
                    xy=(bi, c[bi]),
                    xytext=(0, 8),
                    textcoords="offset points",
                    ha="center",
                    color="#555",
                    fontsize=7,
                    fontweight="bold",
                )
            if is_primary and L.get("retest") is not None:
                ri = L["retest"]
                ax.annotate(
                    "RETEST",
                    xy=(ri, c[ri]),
                    xytext=(0, -10),
                    textcoords="offset points",
                    ha="center",
                    color="#f9a825",
                    fontsize=7,
                    fontweight="bold",
                )

        # پنل روشن
        axp = fig.add_axes([0.80, 0.28, 0.18, 0.58])
        axp.set_facecolor("#f7f7f7")
        axp.set_xticks([])
        axp.set_yticks([])
        for s in axp.spines.values():
            s.set_color("#cccccc")
        gp = f"{gap_pct:.2f}%" if gap_pct is not None else "—"
        txt = [f"{symbol}", f"{tf}", f"gap≈{gp}", ""]
        if primary_res is not None:
            away = (primary_res["y_now"] - c[-1]) / c[-1] * 100
            txt += ["RES", f"{primary_res['y_now']:.6g}", f"{primary_res['n_touch']}t {away:+.1f}%", ""]
        if primary_sup is not None:
            away = (c[-1] - primary_sup["y_now"]) / c[-1] * 100
            txt += ["SUP", f"{primary_sup['y_now']:.6g}", f"{primary_sup['n_touch']}t {away:+.1f}%", ""]
        txt += ["1 solid / rest dotted", "WIP"]
        axp.text(
            0.08,
            0.96,
            "\n".join(txt),
            transform=axp.transAxes,
            va="top",
            ha="left",
            fontsize=8,
            color="#333333",
            family="monospace",
            linespacing=1.35,
        )

        ax.set_xlim(-1, n)
        # کمی پدینگ عمودی
        ymin, ymax = float(np.nanmin(l)), float(np.nanmax(h))
        pad = (ymax - ymin) * 0.04 + 1e-12
        ax.set_ylim(ymin - pad, ymax + pad)
        ax.tick_params(colors="#555555", labelsize=7)
        for s in ax.spines.values():
            s.set_color("#cccccc")
        ax.grid(True, color="#eeeeee", lw=0.6)
        ax.set_title(f"{symbol}  {tf}  ·  trendlines (beta)", color="#222222", fontsize=11, loc="left")
        buf = BytesIO()
        fig.savefig(buf, format="png", facecolor="#ffffff")
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception:
        return None
