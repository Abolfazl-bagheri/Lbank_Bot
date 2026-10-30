"""Auto trendlines + BREAK/RETEST/FAKE (reconstruction of SolAtlas 'Trendlines with Breaks').
Input: pandas DataFrame with columns o,h,l,c (optional: upper,lower for Bollinger)."""
import io
import numpy as np
import pandas as pd

DEFAULTS = dict(sw=3, touches=3, tol=0.5, brk=0.3, per_side=1, lb=250,
                retest=1, rt_bars=15, keep=3, horizon=24, wick_tol=2.0, max_age=0.4)


def line_y(L, i):
    return L["y0"] + L["sl"] * (i - L["a"])


def _clean(df):
    """Numeric, no NaN, consistent wicks. Keeps row count (indices stay valid)."""
    d = df.copy()
    for k in ("o", "h", "l", "c"):
        d[k] = pd.to_numeric(d[k], errors="coerce")
    d[["o", "h", "l", "c"]] = d[["o", "h", "l", "c"]].ffill().bfill()
    d["h"] = d[["o", "h", "l", "c"]].max(axis=1)
    d["l"] = d[["o", "h", "l", "c"]].min(axis=1)
    return d


def _pivots(arr, sw, s0, high):
    out = []
    for i in range(max(s0, sw), len(arr) - sw):
        v, ok = arr[i], True
        for k in range(1, sw + 1):
            lft, rgt = arr[i - k], arr[i + k]
            if (high and (lft >= v or rgt > v)) or (not high and (lft <= v or rgt < v)):
                ok = False
                break
        if ok:
            out.append(i)
    return out


def find_lines(df, auto=True, **kw):
    """Core engine -> (lines, info). auto: scale tol/brk to the symbol's own volatility."""
    P = {**DEFAULTS, **kw}
    df = _clean(df)
    h, l, c = (df[k].to_numpy(float) for k in ("h", "l", "c"))
    if auto:
        med = float(np.nanmedian(((h - l) / c * 100)[-P["lb"]:]))
        if "tol" not in kw:
            P["tol"] = min(0.5, max(0.04, med))
        if "brk" not in kw:
            P["brk"] = min(0.3, max(0.04, med * 0.6))
    n, sw = len(c), P["sw"]
    s0 = n - min(P["lb"], n)
    tol, brk = P["tol"] / 100, P["brk"] / 100
    ph, pl = _pivots(h, sw, s0, True), _pivots(l, sw, s0, False)
    lines = []
    for H, pv, px in ((True, ph[-16:], h), (False, pl[-16:], l)):
        cand = []
        for x in range(len(pv)):
            for y in range(x + 1, len(pv)):
                a, b = pv[x], pv[y]
                if b - a < sw * 2:
                    continue
                L = {"H": H, "a": a, "y0": px[a], "sl": (px[b] - px[a]) / (b - a)}
                tch = [k for k in pv if k >= a and (q := line_y(L, k)) > 0 and abs(px[k] - q) / q <= tol]
                if len(tch) < P["touches"]:
                    continue
                last = tch[-1]
                idx = np.arange(a, last + 1)
                yy = L["y0"] + L["sl"] * (idx - a)
                if (H and (c[idx] > yy * (1 + brk)).any()) or (not H and (c[idx] < yy * (1 - brk)).any()):
                    continue
                wt = P["wick_tol"] * tol   # wicks must respect the line too
                viol = (h[idx] > yy * (1 + wt)).sum() if H else (l[idx] < yy * (1 - wt)).sum()
                if viol > max(1, int(0.05 * len(idx))):   # allow a few outlier wicks
                    continue
                if (n - 1 - last) > P["max_age"] * min(P["lb"], n):   # last touch must be recent
                    continue
                L.update(tch=tch, last=last, br=-1, status="active", fake=None, rt=None, fakes=[])
                start = last + 1
                while True:                       # a faked break does not end the line: look for the next one
                    br = -1
                    for i in range(start, n):
                        q = line_y(L, i)
                        if (H and c[i] > q * (1 + brk)) or (not H and c[i] < q * (1 - brk)):
                            br = i
                            break
                    if br < 0:
                        break
                    fk = None
                    for i in range(br + 1, min(n - 1, br + P["horizon"]) + 1):
                        q = line_y(L, i)
                        if (H and c[i] < q * (1 - brk)) or (not H and c[i] > q * (1 + brk)):
                            fk = i
                            break
                    if fk is None:
                        L["br"] = br
                        break
                    L["fakes"].append((br, fk))
                    start = fk + 1
                if L["br"] >= 0:
                    L["status"] = "broken"
                    if P["retest"]:
                        for i in range(L["br"] + 1, min(n - 1, L["br"] + P["rt_bars"]) + 1):
                            q = line_y(L, i)
                            if (H and l[i] <= q * (1 + tol) and c[i] >= q) or (not H and h[i] >= q * (1 - tol) and c[i] <= q):
                                L["rt"], L["status"] = i, "retest"
                                break
                elif L["fakes"]:
                    L["status"], L["fake"] = "fake", L["fakes"][-1][1]
                L["kind"] = "static" if abs((line_y(L, last) - L["y0"]) / L["y0"]) < 2 * tol else "dynamic"
                cand.append(L)
        cand.sort(key=lambda L: (-len(L["tch"]), -L["last"]))
        kept = []
        for L in cand:
            if not any(len(set(L["tch"]) & set(K["tch"])) >= 2 for K in kept):
                kept.append(L)
        act = [L for L in kept if L["br"] < 0][:P["per_side"]]
        old = sorted([L for L in kept if L["br"] >= 0], key=lambda L: -L["br"])[:P["keep"]]
        lines += act + old
    return lines, {"s0": s0, "ph": ph, "pl": pl, "n": n}


def render_chart(df, lines, info, symbol, tf, note=""):
    """Returns PNG bytes (send with Telegram sendPhoto)."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    s0, n = info["s0"], info["n"]
    d = _clean(df).iloc[s0:].reset_index(drop=True)
    o, h, l, c = (d[k].to_numpy(float) for k in ("o", "h", "l", "c"))
    xs, BG = np.arange(len(d)), "#0b0d13"
    fig, ax = plt.subplots(figsize=(10, 5.6), dpi=130)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    flat = np.isclose(h, l, rtol=1e-9) & np.isclose(o, c, rtol=1e-9)   # bars with no trades
    col = np.where(flat, "#6b7385", np.where(c >= o, "#2de2a6", "#ff5b73"))
    vis = (h.max() - l.min()) or 1.0
    ax.vlines(xs, l, h, colors=col, linewidth=0.9)
    ax.bar(xs, np.maximum(np.abs(c - o), vis * 0.004), bottom=np.minimum(o, c) - np.where(np.abs(c - o) < vis * 0.004, vis * 0.002, 0),
           width=0.7, color=col)
    nflat = int(flat.sum())
    if nflat > 0.3 * len(d):
        note += f" · {nflat}/{len(d)} flat bars (illiquid, low confidence)"
    if {"upper", "lower"} <= set(d.columns):
        ax.plot(xs, d["upper"], color="#8b7bff", lw=0.7, alpha=0.45)
        ax.plot(xs, d["lower"], color="#8b7bff", lw=0.7, alpha=0.45)
        ax.fill_between(xs, d["lower"], d["upper"], color="#8b7bff", alpha=0.04)
    ext = max(6, len(d) // 25)
    for L in lines:
        clr = "#ffb547" if L["H"] else "#38d9ff"
        xa = L["a"] - s0
        xe = (L["br"] if L["br"] >= 0 else n - 1) - s0
        ya, ye = line_y(L, L["a"]), line_y(L, xe + s0)
        if L["br"] < 0:
            xe2 = len(d) - 1 + ext
            ax.plot([xa, xe2], [ya, line_y(L, xe2 + s0)], color=clr, lw=2, solid_capstyle="round")
            ax.text(xe2 + 0.5, line_y(L, xe2 + s0), f"{line_y(L, n - 1):.6g}", color=clr, fontsize=8, va="center")
        else:
            ax.plot([xa, xe], [ya, ye], color=clr, lw=2)
            ax.plot([xe, len(d) - 1], [ye, line_y(L, n - 1)], color=clr, lw=1.2, ls=(0, (5, 4)), alpha=0.55)
        ax.scatter([t - s0 for t in L["tch"]], [line_y(L, t) for t in L["tch"]], s=22, color=clr, zorder=5)
        def tag(i, txt, color, up):
            ax.annotate(txt, (i - s0, line_y(L, i)), xytext=(0, 12 if up else -14), textcoords="offset points",
                        ha="center", fontsize=8, fontweight="bold", color=color,
                        arrowprops=dict(arrowstyle="-", color=color, lw=0.6))
        bi = L["br"] if L["br"] >= 0 else (L["fakes"][-1][0] if L["fakes"] else -1)
        if bi >= 0:
            tag(bi, "BREAK ▲" if L["H"] else "BREAK ▼", "#ffffff", L["H"])
        if L["rt"] is not None:
            tag(L["rt"], "RETEST", "#38d9ff", not L["H"])
        if L["br"] < 0 and L["fakes"]:
            tag(L["fakes"][-1][1], "FAKE", "#ff5b73", not L["H"])
    ax.set_xlim(-1, len(d) + ext + 8)
    ax.yaxis.tick_right()
    ax.tick_params(colors="#8b92a8", labelsize=8)
    ax.grid(color="#ffffff", alpha=0.05)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.set_xticks([])
    ax.set_title(f"{symbol} · {tf} · Trendlines {note}", color="#eef0f8", fontsize=11, loc="left")
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=BG)
    plt.close(fig)
    return buf.getvalue()


# ---------- compatibility layer: same API the bot already calls ----------
def y_at(i0, p0, i1, p1, i):
    """y of the line through (i0,p0),(i1,p1) at x=i."""
    return p0 if i1 == i0 else p0 + (p1 - p0) * (i - i0) / (i1 - i0)


def find_trendlines(df, max_res=2, max_sup=1, **kw):
    """List of dicts: kind 'res'|'sup', broken (idx of last break, incl. faked ones) or None, i0,p0,i1,p1."""
    lines, _ = find_lines(df.reset_index(drop=True), **kw)
    res = [L for L in lines if L["H"]][:max_res]
    sup = [L for L in lines if not L["H"]][:max_sup]
    out = []
    for L in res + sup:
        broken = L["br"] if L["br"] >= 0 else (L["fakes"][-1][0] if L["fakes"] else None)
        out.append({**L, "kind": "res" if L["H"] else "sup", "broken": broken,
                    "i0": L["a"], "p0": L["y0"], "i1": L["last"], "p1": line_y(L, L["last"])})
    return out


def render_trendline_chart(df, symbol, tf, gap_pct=None, max_bars=180, **kw):
    df = df.reset_index(drop=True)
    lines, info = find_lines(df, lb=max_bars, **kw)
    note = f"· gap≈{gap_pct:.2f}%" if gap_pct is not None else ""
    return render_chart(df, lines, info, symbol, tf, note)


# ===================== Chart Lab (runs in GitHub Actions) =====================
import asyncio
import json
import os
import sys
import time

import requests

KBAR = {"15m": "15min", "1h": "1hr", "4h": "4hr"}
PERIOD = {"15m": 900, "1h": 3600, "4h": 14400}
SPOT_WS = "wss://www.lbkex.net/ws/V2/"


def _df(rows):
    if not rows:
        return None
    df = pd.DataFrame(rows).sort_values("ts").drop_duplicates("ts").reset_index(drop=True)
    s, sd = df["c"].rolling(20).mean(), df["c"].rolling(20).std(ddof=1)
    df["sma"], df["upper"], df["lower"] = s, s + 2 * sd, s - 2 * sd
    return df


def fetch_gate(symbol, tf, size=150):
    contract = symbol[:-4] + "_USDT" if symbol.endswith("USDT") and "_" not in symbol else symbol
    url = f"https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={contract}&interval={tf}&limit={size}"
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            print("gate HTTP", r.status_code, r.text[:120])
            return None
        data = r.json()
        return _df([{"ts": int(x["t"]), "o": float(x["o"]), "h": float(x["h"]), "l": float(x["l"]),
                     "c": float(x["c"]), "v": float(x.get("v") or 0)} for x in data]) if data else None
    except Exception as e:
        print("gate error", e)
        return None


async def fetch_spot_ws(symbol, tf, size=150):
    pair = symbol[:-4].lower() + "_usdt" if symbol.endswith("USDT") else symbol.lower()
    try:
        import websockets
        async with websockets.connect(SPOT_WS, ping_interval=20, close_timeout=5) as ws:
            await ws.send(json.dumps({"action": "request", "request": "kbar", "pair": pair, "kbar": KBAR[tf], "size": size}))
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
        data = msg.get("kbar") or msg.get("data") or []
        rows = []
        for x in data:
            if isinstance(x, dict):
                rows.append({"ts": int(x.get("t") or x.get("time") or 0), "o": float(x.get("o") or x.get("open") or 0),
                             "h": float(x.get("h") or x.get("high") or 0), "l": float(x.get("l") or x.get("low") or 0),
                             "c": float(x.get("c") or x.get("close") or 0), "v": float(x.get("v") or 0)})
            elif isinstance(x, (list, tuple)) and len(x) >= 5:
                rows.append({"ts": int(x[0]), "o": float(x[1]), "h": float(x[2]), "l": float(x[3]), "c": float(x[4]),
                             "v": float(x[5]) if len(x) > 5 else 0})
        return _df(rows)
    except Exception as e:
        print("spot_ws error", e)
        return None


def stats(df, tf):
    n = len(df)
    flat = int(((df.h == df.l) & (df.o == df.c)).sum())
    ts = df.ts.astype(int).tolist()
    return {"n": n, "flat_bars": flat, "flat_pct": round(flat / n * 100, 1),
            "missing_bars": int((ts[-1] - ts[0]) // PERIOD[tf] + 1 - n), "first_ts": ts[0], "last_ts": ts[-1],
            "last_close": float(df.c.iloc[-1])}


def main(symbol, tf, outdir="lab/out"):
    symbol = symbol.upper().strip()
    os.makedirs(outdir, exist_ok=True)
    res = {"symbol": symbol, "tf": tf, "generated": int(time.time()), "sources": {}}
    srcs = {"gate_futures": fetch_gate(symbol, tf), "lbank_spot_ws": asyncio.run(fetch_spot_ws(symbol, tf))}
    for name, df in srcs.items():
        if df is None or len(df) < 5:
            res["sources"][name] = {"error": "no data"}
            continue
        res["sources"][name] = {"stats": stats(df, tf),
                                "candles": df[["ts", "o", "h", "l", "c", "v"]].values.tolist()}
        try:
            open(f"{outdir}/{symbol}_{tf}_{name}.png", "wb").write(render_trendline_chart(df, symbol, tf, max_bars=180))
        except Exception as e:
            res["sources"][name]["render_error"] = str(e)
    with open(f"{outdir}/{symbol}_{tf}.json", "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False)
    print(json.dumps({k: (v.get("stats") or v.get("error")) for k, v in res["sources"].items()}, indent=1))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT", sys.argv[2] if len(sys.argv) > 2 else "1h")
