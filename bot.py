"""
LBank Gap Bot — slim multi-agent build (v2.0.0)
Scout → Filter → Analyst → Risk → Executor → Logger

- سیگنال: فقط متن (سِل + % تا UB + لینک TV LBank + لینک فیوچرز)
- احتمال گپ: متن + چارت BB (بدون خط‌روند)
- TRADE_MODE: off | paper | live
- state: فایل محلی + GitHub Contents API (.bot_state/)
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from datetime import datetime
from io import BytesIO
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

TEHRAN = ZoneInfo("Asia/Tehran")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "bot_state.json")
STATE_BAK = os.path.join(BASE_DIR, "bot_state.bak")

BOT_VERSION = "2.0.1"
CHANGELOG = {
    "2.0.0": [
        "ربات خلوت: فقط ۶ agent + گپ + احتمال‌گپ + اتوترید",
        "سیگنال فقط متن؛ چارت فقط روی احتمال گپ (BB)",
        "حذف زاویه / خط‌روند سیگنال / دکمه / کیفیت ورود",
        "ذخیره state روی GitHub بین ریست Actions",
    ],
}

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
CHAT_ID = os.environ.get("CHAT_ID", "").strip()

TIMEFRAMES = {"15m": "15min", "1h": "1hr", "4h": "4hr"}
TF_ORDER = {"15m": 0, "1h": 1, "4h": 2}
PERIOD_SEC = {"15m": 900, "1h": 3600, "4h": 14400}
OPEN_WINDOW_SEC = {"15m": 180, "1h": 240, "4h": 300}
PRE_ALERT_BEFORE = {"15m": 120, "1h": 180, "4h": 600}  # فقط اینقدر قبل از کندل
SIGNAL_SCAN_SEC = {"15m": 90, "1h": 150, "4h": 180}  # چند ثانیه اول کندل برای اسکن سیگنال

BB_PERIOD = 20
BB_STD = 2.0
PENETRATION_PCT = 0.5
MIN_OPEN_GAP_PCT = 0.5
TOP_GAINERS = 50
TOP_VOLUME = 30
ALWAYS_INCLUDE = ["BTCUSDT", "XAUTUSDT"]
ONLY_SELL = True

TRADE_MODE = os.environ.get("TRADE_MODE", "off").strip().lower()
if TRADE_MODE not in ("off", "paper", "live"):
    TRADE_MODE = "off"
AUTO_TRADE = os.environ.get("AUTO_TRADE", "0").strip() in ("1", "true", "True", "yes")
AUTO_TRADE_DRY_RUN = os.environ.get("AUTO_TRADE_DRY_RUN", "1").strip() not in ("0", "false", "False", "no")
LBANK_API_KEY = os.environ.get("LBANK_API_KEY", "").strip()
LBANK_API_SECRET = os.environ.get("LBANK_API_SECRET", "").strip()
TRADE_MARGIN_USDT = float(os.environ.get("TRADE_MARGIN_USDT", "1"))
TRADE_LEVERAGE = int(os.environ.get("TRADE_LEVERAGE", "10"))
TRADE_TP_PCT = float(os.environ.get("TRADE_TP_PCT", "30"))
TRADE_AVG_DOWN = True
TRADE_MAX_ADDS = int(os.environ.get("TRADE_MAX_ADDS", "15"))

FUTURES_TICKERS_URL = "https://lbkperp.lbank.com/cfd/openApi/v1/pub/marketData?productGroup=SwapU"
PERP_BASE = "https://lbkperp.lbank.com"
SPOT_WS = "wss://www.lbkex.net/ws/V2/"
GATE_INTERVAL = {"15m": "15m", "1h": "1h", "4h": "4h"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bot")

futures_last_map: dict = {}
kline_blacklist: dict = {}
sent_signals: set = set()
sent_pre_alerts: set = set()
open_trades: list = []
trade_history: list = []
pending_signals: list = []
stats = {"checked": 0, "signal_ok": 0, "skip_age": 0, "skip_no_gap": 0, "skip_dup": 0}
_last_trade_report_ts = 0
_last_github_push_ts = 0
last_known_version = None


def iran_now() -> datetime:
    return datetime.now(TEHRAN)


def trading_enabled() -> bool:
    return bool(AUTO_TRADE) or TRADE_MODE in ("paper", "live")


def is_paper_mode() -> bool:
    if TRADE_MODE == "paper":
        return True
    if TRADE_MODE == "live":
        return False
    return bool(AUTO_TRADE_DRY_RUN) or not LBANK_API_KEY


def is_blacklisted(symbol: str) -> bool:
    exp = kline_blacklist.get(symbol.upper())
    if not exp:
        return False
    if int(time.time()) > int(exp):
        kline_blacklist.pop(symbol.upper(), None)
        return False
    return True


def blacklist_add(symbol: str, hours: int = 24) -> None:
    kline_blacklist[symbol.upper()] = int(time.time()) + hours * 3600


def _github_repo_parts():
    repo = os.environ.get("GITHUB_REPOSITORY") or os.environ.get("STATE_GITHUB_REPO") or ""
    token = (os.environ.get("STATE_GITHUB_TOKEN") or os.environ.get("GITHUB_TOKEN") or "").strip()
    if "/" not in repo or not token:
        return None, None, None
    owner, name = repo.split("/", 1)
    return owner, name, token


def github_state_pull() -> bool:
    owner, name, token = _github_repo_parts()
    if not token:
        return False
    url = f"https://api.github.com/repos/{owner}/{name}/contents/.bot_state/bot_state.json"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    try:
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code != 200:
            return False
        data = r.json()
        content = base64.b64decode(data.get("content") or "").decode("utf-8")
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            f.write(content)
        log.info("github_state_pull OK")
        return True
    except Exception as e:
        log.warning("github_state_pull: %s", e)
        return False


def github_state_push(force: bool = False) -> bool:
    global _last_github_push_ts
    owner, name, token = _github_repo_parts()
    if not token or not os.path.exists(STATE_FILE):
        return False
    now = int(time.time())
    if not force and _last_github_push_ts and (now - _last_github_push_ts) < 120:
        return False
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            raw = f.read()
        b64 = base64.b64encode(raw.encode("utf-8")).decode("ascii")
        url = f"https://api.github.com/repos/{owner}/{name}/contents/.bot_state/bot_state.json"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
        sha = None
        gr = requests.get(url, headers=headers, timeout=20)
        if gr.status_code == 200:
            sha = (gr.json() or {}).get("sha")
        body = {"message": f"bot state {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}", "content": b64, "branch": os.environ.get("STATE_GITHUB_BRANCH") or "main"}
        if sha:
            body["sha"] = sha
        pr = requests.put(url, headers=headers, json=body, timeout=30)
        if pr.status_code in (200, 201):
            _last_github_push_ts = now
            log.info("github_state_push OK")
            return True
        log.warning("github_state_push HTTP %s", pr.status_code)
        return False
    except Exception as e:
        log.warning("github_state_push: %s", e)
        return False


def save_state() -> None:
    try:
        payload = {
            "open_trades": [t for t in open_trades if t.get("status") == "open"][-50:],
            "trade_history": trade_history[-200:],
            "pending": pending_signals[-300:],
            "kline_blacklist": kline_blacklist,
            "last_trade_report_ts": _last_trade_report_ts,
            "last_known_version": BOT_VERSION,
        }
        raw = json.dumps(payload, ensure_ascii=False, indent=2)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            f.write(raw)
        try:
            with open(STATE_BAK, "w", encoding="utf-8") as f:
                f.write(raw)
        except Exception:
            pass
        github_state_push(force=False)
    except Exception as e:
        log.warning("save_state: %s", e)


def load_state() -> None:
    global last_known_version, _last_trade_report_ts
    try:
        github_state_pull()
    except Exception:
        pass
    path = STATE_FILE if os.path.exists(STATE_FILE) else (STATE_BAK if os.path.exists(STATE_BAK) else None)
    if not path:
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        open_trades.clear()
        open_trades.extend(payload.get("open_trades") or [])
        trade_history.clear()
        trade_history.extend(payload.get("trade_history") or [])
        pending_signals.clear()
        pending_signals.extend(payload.get("pending") or [])
        kline_blacklist.clear()
        kline_blacklist.update(payload.get("kline_blacklist") or {})
        _last_trade_report_ts = int(payload.get("last_trade_report_ts") or 0)
        last_known_version = payload.get("last_known_version")
        log.info("State loaded open=%d hist=%d", len(open_trades), len(trade_history))
    except Exception as e:
        log.warning("load_state: %s", e)


def send_telegram_text(text: str) -> bool:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=15,
        )
        return r.status_code == 200
    except Exception as e:
        log.warning("tg text: %s", e)
        return False


def send_telegram_photo(image_bytes: bytes, caption: str) -> bool:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            data={"chat_id": CHAT_ID, "caption": caption[:1024], "parse_mode": "HTML"},
            files={"photo": ("chart.png", image_bytes, "image/png")},
            timeout=30,
        )
        return r.status_code == 200
    except Exception as e:
        log.warning("tg photo: %s", e)
        return False


def format_startup_message() -> str:
    global last_known_version
    lines = [f"✅ ربات روشن شد — <b>v{BOT_VERSION}</b>", f"🧠 agents · TRADE_MODE=<code>{TRADE_MODE}</code>"]
    if trading_enabled():
        lines.append(f"🤖 exec: {'PAPER' if is_paper_mode() else 'LIVE'} · {TRADE_MARGIN_USDT}U · {TRADE_LEVERAGE}x")
    if last_known_version and last_known_version != BOT_VERSION:
        lines.append(f"📦 {last_known_version} → {BOT_VERSION}")
        for item in CHANGELOG.get(BOT_VERSION) or []:
            lines.append(f"• {item}")
    last_known_version = BOT_VERSION
    return "\n".join(lines)


def refresh_futures_map() -> dict:
    global futures_last_map
    try:
        r = requests.get(FUTURES_TICKERS_URL, timeout=15)
        data = r.json().get("data") or []
    except Exception as e:
        log.error("tickers: %s", e)
        return futures_last_map
    out = {}
    for item in data:
        sym = (item.get("symbol") or "").upper()
        if not sym.endswith("USDT"):
            continue
        try:
            last = float(item.get("lastPrice") or item.get("last") or 0)
            open_p = float(item.get("openPrice") or item.get("open") or 0)
            vol = float(item.get("volume") or item.get("turnover") or 0)
        except Exception:
            continue
        if last > 0:
            out[sym] = {"last": last, "open": open_p, "vol": vol}
    if out:
        futures_last_map = out
    return futures_last_map


def get_top_movers(n: int = TOP_GAINERS) -> list:
    fmap = refresh_futures_map()
    rows = []
    for sym, rec in fmap.items():
        o, last = rec.get("open") or 0, rec.get("last") or 0
        if o <= 0 or last <= 0:
            continue
        rows.append({"symbol": sym, "change": (last - o) / o * 100.0, "vol": rec.get("vol") or 0})
    if not rows:
        return list(ALWAYS_INCLUDE)
    df = pd.DataFrame(rows)
    gainers = df.nlargest(n, "change")["symbol"].tolist()
    volumes = df.nlargest(TOP_VOLUME, "vol")["symbol"].tolist()
    symbols = list(dict.fromkeys(gainers + volumes + ALWAYS_INCLUDE))
    return [s for s in symbols if not is_blacklisted(s)]


def get_futures_last(symbol: str):
    rec = futures_last_map.get(symbol.upper())
    if rec and rec.get("last"):
        return float(rec["last"])
    refresh_futures_map()
    rec = futures_last_map.get(symbol.upper())
    return float(rec["last"]) if rec and rec.get("last") else None


def calc_bb(closes: np.ndarray, period: int = BB_PERIOD, std_mult: float = BB_STD):
    s = pd.Series(closes)
    sma = s.rolling(period).mean()
    std = s.rolling(period).std(ddof=1)
    return sma, sma + std_mult * std, sma - std_mult * std


def _df_from_ohlc_rows(rows: list) -> pd.DataFrame | None:
    if not rows:
        return None
    df = pd.DataFrame(rows).sort_values("ts").drop_duplicates("ts").reset_index(drop=True)
    if len(df) < BB_PERIOD + 2:
        return None
    sma, upper, lower = calc_bb(df["c"].astype(float).values)
    df["sma"], df["upper"], df["lower"] = sma.values, upper.values, lower.values
    return df


def fetch_klines_gate(symbol: str, tf: str, size: int = 80) -> pd.DataFrame | None:
    interval = GATE_INTERVAL.get(tf)
    if not interval:
        return None
    contract = symbol.upper()
    if contract.endswith("USDT") and "_" not in contract:
        contract = contract[:-4] + "_USDT"
    url = f"https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={contract}&interval={interval}&limit={size}"
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None
        data = r.json()
        if not isinstance(data, list) or not data:
            return None
        rows = [{"ts": int(x.get("t") or 0), "o": float(x.get("o") or 0), "h": float(x.get("h") or 0),
                 "l": float(x.get("l") or 0), "c": float(x.get("c") or 0), "v": float(x.get("v") or 0)} for x in data]
        return _df_from_ohlc_rows(rows)
    except Exception:
        return None


async def fetch_klines_spot_ws(symbol: str, tf: str, size: int = 80) -> pd.DataFrame | None:
    pair = symbol.lower()
    if pair.endswith("usdt") and "_" not in pair:
        pair = pair[:-4] + "_usdt"
    kbar = TIMEFRAMES.get(tf)
    if not kbar:
        return None
    try:
        import websockets
        async with websockets.connect(SPOT_WS, ping_interval=20, close_timeout=5) as ws:
            await ws.send(json.dumps({"action": "request", "request": "kbar", "pair": pair, "kbar": kbar, "size": size}))
            raw = await asyncio.wait_for(ws.recv(), timeout=12)
            msg = json.loads(raw)
            data = msg.get("kbar") or msg.get("data") or []
            if not data:
                return None
            rows = []
            for x in data:
                if isinstance(x, dict):
                    rows.append({"ts": int(x.get("t") or x.get("time") or 0), "o": float(x.get("o") or x.get("open") or 0),
                                 "h": float(x.get("h") or x.get("high") or 0), "l": float(x.get("l") or x.get("low") or 0),
                                 "c": float(x.get("c") or x.get("close") or 0), "v": float(x.get("v") or 0)})
                elif isinstance(x, (list, tuple)) and len(x) >= 5:
                    rows.append({"ts": int(x[0]), "o": float(x[1]), "h": float(x[2]), "l": float(x[3]), "c": float(x[4]),
                                 "v": float(x[5]) if len(x) > 5 else 0})
            return _df_from_ohlc_rows(rows)
    except Exception:
        return None


async def fetch_klines(symbol: str, tf: str, size: int = 80):
    df = await fetch_klines_spot_ws(symbol, tf, size)
    if df is not None and len(df) >= BB_PERIOD + 2:
        return df, "spot_ws"
    df = fetch_klines_gate(symbol, tf, size)
    if df is not None and len(df) >= BB_PERIOD + 2:
        return df, "gate_futures"
    return None, None


def is_erratic_wick_candle(row) -> bool:
    try:
        o, h, l, c = float(row["o"]), float(row["h"]), float(row["l"]), float(row["c"])
        body, rng = abs(c - o), h - l
        if rng <= 0:
            return False
        return body / rng < 0.15 and rng / max(c, 1e-12) > 0.08
    except Exception:
        return False


def check_signal(df: pd.DataFrame, symbol: str, tf: str):
    if len(df) < BB_PERIOD + 2:
        return None
    cur = df.iloc[-1]
    open_p, high, low, close = float(cur["o"]), float(cur["h"]), float(cur["l"]), float(cur["c"])
    if is_erratic_wick_candle(cur):
        stats["skip_no_gap"] += 1
        return None
    upper, lower = float(cur["upper"]), float(cur["lower"])
    if np.isnan(upper) or np.isnan(lower):
        return None
    stats["checked"] += 1
    candle_ts = int(cur["ts"])
    age_sec = int(time.time()) - candle_ts
    if age_sec < 0 or age_sec > OPEN_WINDOW_SEC.get(tf, 120):
        stats["skip_age"] += 1
        return None
    upper_pen = (high - upper) / upper * 100.0 if upper > 0 else 0.0
    side = None
    if open_p > upper and upper_pen >= PENETRATION_PCT:
        open_gap_pct = (open_p - upper) / open_p * 100.0 if open_p > 0 else 0.0
        if open_gap_pct < MIN_OPEN_GAP_PCT and symbol not in ALWAYS_INCLUDE:
            stats["skip_no_gap"] += 1
            return None
        if close < upper:
            stats["skip_no_gap"] += 1
            return None
        side = "SELL"
    if not side or (ONLY_SELL and side != "SELL"):
        stats["skip_no_gap"] += 1
        return None
    fut = get_futures_last(symbol)
    if fut is None or fut <= upper:
        stats["skip_no_gap"] += 1
        return None
    key = (symbol, tf, candle_ts, side)
    if key in sent_signals:
        stats["skip_dup"] += 1
        return None
    sent_signals.add(key)
    if len(sent_signals) > 4000:
        sent_signals.clear()
    stats["signal_ok"] += 1
    diff_pct = (fut - upper) / fut * 100.0 if fut else 0.0
    return {
        "side": side, "symbol": symbol, "tf": tf, "open": open_p, "high": high, "low": low, "close": close,
        "upper": upper, "lower": lower, "diff_pct": diff_pct, "fut_last": fut, "candle_ts": candle_ts, "age_sec": age_sec,
    }


def tv_lbank_url(symbol: str) -> str:
    return f"https://www.tradingview.com/chart/?symbol=LBANK%3A{symbol.upper()}.P"


def lbank_futures_url(symbol: str) -> str:
    s = symbol.lower()
    pair = s[:-4] + "_usdt" if s.endswith("usdt") else s
    return f"https://www.lbank.com/futures/{pair}"


def format_signal_message(sig: dict) -> str:
    sym = sig["symbol"]
    diff = float(sig.get("diff_pct") or 0)
    return (
        f"🔴 <b>سیگنال SELL</b>\n"
        f"<a href=\"{tv_lbank_url(sym)}\">{sym}</a> · {sig['tf']}\n"
        f"اختلاف تا UB: <b>{diff:.2f}%</b>\n"
        f"<a href=\"{lbank_futures_url(sym)}\">ال‌بانک فیوچرز</a>"
    )


def make_gap_chart(df: pd.DataFrame, symbol: str, tf: str, sig: dict | None = None) -> bytes | None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
    except ImportError:
        return None
    try:
        plot = df.tail(40).reset_index(drop=True)
        n = len(plot)
        if n < 8:
            return None
        o, h, l, c = plot["o"].astype(float).values, plot["h"].astype(float).values, plot["l"].astype(float).values, plot["c"].astype(float).values
        upper, lower, sma = plot["upper"].astype(float).values, plot["lower"].astype(float).values, plot["sma"].astype(float).values
        ub = float(upper[-1])
        entry = float((sig or {}).get("fut_last") or (sig or {}).get("open") or o[-1])
        gap_pct = (entry - ub) / entry * 100.0 if entry else 0.0
        fig, ax = plt.subplots(figsize=(9.5, 4.6), dpi=130, facecolor="#ffffff")
        x = np.arange(n)
        ax.fill_between(x, lower, upper, color="#90caf9", alpha=0.25)
        ax.plot(x, upper, color="#1976d2", lw=1.0)
        ax.plot(x, lower, color="#1976d2", lw=1.0)
        ax.plot(x, sma, color="#f9a825", lw=1.0)
        for i in range(n):
            color = "#26a69a" if c[i] >= o[i] else "#ef5350"
            ax.plot([i, i], [l[i], h[i]], color=color, lw=1.0)
            bot = min(o[i], c[i])
            ax.add_patch(Rectangle((i - 0.3, bot), 0.6, max(abs(c[i] - o[i]), 1e-12), facecolor=color, edgecolor=color))
        ax.axhline(ub, color="#1565c0", ls="--", lw=0.9)
        ax.set_title(f"{symbol} {tf}  |  gap≈{gap_pct:.2f}%", fontsize=11)
        ax.grid(True, color="#eeeeee", lw=0.6)
        buf = BytesIO()
        fig.savefig(buf, format="png", facecolor="#ffffff")
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        log.debug("chart: %s", e)
        return None


def trade_qty_from_margin(symbol: str, price: float) -> float | None:
    if price <= 0:
        return None
    qty = (TRADE_MARGIN_USDT * TRADE_LEVERAGE) / price
    if qty >= 1:
        qty = float(f"{qty:.4f}")
    elif qty >= 0.01:
        qty = float(f"{qty:.6f}")
    else:
        qty = float(f"{qty:.8f}")
    return qty if qty > 0 else None


def tp_price_for_sell(entry: float) -> float:
    return entry * (1.0 - (TRADE_TP_PCT / 100.0) / max(TRADE_LEVERAGE, 1))


def _perp_sign(params: dict) -> tuple[dict, dict]:
    ts = str(int(time.time() * 1000))
    echo = secrets.token_hex(16)
    payload = dict(params)
    payload.update({"api_key": LBANK_API_KEY, "timestamp": ts, "signature_method": "HmacSHA256", "echostr": echo})
    items = sorted((k, str(v)) for k, v in payload.items() if v is not None and k != "sign")
    query = "&".join(f"{k}={v}" for k, v in items)
    md5_upper = hashlib.md5(query.encode()).hexdigest().upper()
    payload["sign"] = hmac.new(LBANK_API_SECRET.encode(), md5_upper.encode(), hashlib.sha256).hexdigest().lower()
    headers = {"Content-Type": "application/json", "timestamp": ts, "signature_method": "HmacSHA256", "echostr": echo}
    return payload, headers


def _perp_post(path: str, params: dict) -> dict | None:
    if not LBANK_API_KEY or not LBANK_API_SECRET:
        return None
    body, headers = _perp_sign(params)
    try:
        r = requests.post(PERP_BASE.rstrip("/") + path, json=body, headers=headers, timeout=15)
        try:
            return r.json()
        except Exception:
            return {"raw": r.text[:300], "status_code": r.status_code}
    except Exception as e:
        log.warning("perp: %s", e)
        return None


def open_sell_trade(symbol: str, entry_price: float, tf: str, candle_ts: int | None = None) -> dict | None:
    if not trading_enabled():
        return None
    entry_price = float(entry_price)
    if entry_price <= 0:
        return None
    existing = next((t for t in open_trades if t.get("symbol") == symbol and t.get("status") == "open" and t.get("side") == "SELL"), None)
    if existing is not None:
        if not TRADE_AVG_DOWN:
            return None
        adds = int(existing.get("adds") or 0)
        if adds >= TRADE_MAX_ADDS:
            return None
        last_cts = int(existing.get("last_leg_candle_ts") or 0)
        cts = int(candle_ts or 0)
        if cts and last_cts and cts <= last_cts:
            return None
        avg = float(existing.get("entry") or 0)
        if avg > 0 and entry_price <= avg * 1.001:
            return None
        add_qty = trade_qty_from_margin(symbol, entry_price)
        if not add_qty:
            return None
        old_qty = float(existing.get("qty") or 0)
        new_qty = old_qty + add_qty
        new_margin = float(existing.get("margin") or TRADE_MARGIN_USDT) + TRADE_MARGIN_USDT
        new_entry = (avg * old_qty + entry_price * add_qty) / new_qty
        existing.update({"entry": new_entry, "qty": new_qty, "margin": new_margin, "tp_price": tp_price_for_sell(new_entry),
                         "adds": adds + 1, "last_leg_candle_ts": cts or last_cts, "tf": tf})
        send_telegram_text(f"📉 <b>پله PAPER</b> {symbol}\nمیانگین ≈ <code>{new_entry:.6g}</code> · {new_margin}U")
        save_state()
        return existing

    qty = trade_qty_from_margin(symbol, entry_price)
    if not qty:
        return None
    tp = tp_price_for_sell(entry_price)
    rec = {
        "symbol": symbol, "side": "SELL", "status": "open", "entry": entry_price, "qty": qty,
        "margin": TRADE_MARGIN_USDT, "leverage": TRADE_LEVERAGE, "tp_price": tp, "tf": tf,
        "opened_at": int(time.time()), "adds": 0,
        "entry_candle_ts": int(candle_ts or 0), "last_leg_candle_ts": int(candle_ts or 0),
        "dry_run": is_paper_mode(),
    }
    if is_paper_mode():
        rec["order_id"] = f"PAPER-{symbol}-{int(time.time())}"
        open_trades.append(rec)
        send_telegram_text(
            f"🤖 <b>ورود مجازی PAPER سِل</b>\n<b>{symbol}</b> · {tf}\n"
            f"ورود ≈ <code>{entry_price:.6g}</code>\nمارجین {TRADE_MARGIN_USDT}U · {TRADE_LEVERAGE}x\nTP ≈ <code>{tp:.6g}</code>"
        )
        save_state()
        return rec

    _perp_post("/cfd/openApi/v1/prv/setLeverage", {"symbol": symbol.upper(), "productGroup": "SwapU", "leverage": TRADE_LEVERAGE, "positionSide": "short"})
    data = _perp_post("/cfd/openApi/v1/prv/placeOrder", {
        "symbol": symbol.upper(), "productGroup": "SwapU", "side": "sell", "orderType": "market",
        "volume": qty, "positionSide": "short",
    })
    ok = isinstance(data, dict) and (data.get("success") is True or data.get("error_code") in (0, "0", None))
    if not ok:
        send_telegram_text(f"⚠️ LIVE ناموفق {symbol}\n<code>{str(data)[:160]}</code>")
        return None
    rec["order_id"] = str((data or {}).get("data") or data)[:80]
    rec["dry_run"] = False
    open_trades.append(rec)
    send_telegram_text(f"🤖 <b>LIVE سِل</b> {symbol}\nورود ≈ {entry_price:.6g}")
    save_state()
    return rec


def close_trade_record(rec: dict, px: float, reason: str) -> None:
    rec["status"] = "closed"
    rec["close_px"] = float(px)
    entry = float(rec["entry"])
    lev = float(rec.get("leverage") or TRADE_LEVERAGE)
    margin = float(rec.get("margin") or TRADE_MARGIN_USDT)
    margin_roi = ((entry - px) / entry * 100.0 if entry else 0.0) * lev
    pnl = margin * (margin_roi / 100.0)
    rec["pnl_usdt"] = pnl
    trade_history.append(dict(rec))
    send_telegram_text(
        f"✅ <b>کلوز {'PAPER' if rec.get('dry_run') else 'LIVE'}</b> {rec['symbol']}\n"
        f"{entry:.6g} → {px:.6g}\nROI ≈ <b>{margin_roi:.1f}%</b> · PnL <b>{pnl:+.3f}U</b> · {reason}"
    )
    save_state()


def manage_open_trades() -> None:
    if not trading_enabled() or not open_trades:
        return
    for rec in list(open_trades):
        if rec.get("status") != "open" or rec.get("side") != "SELL":
            continue
        px = get_futures_last(rec["symbol"])
        if not px:
            continue
        tp = float(rec.get("tp_price") or 0)
        if tp > 0 and px <= tp:
            close_trade_record(rec, px, "TP")
    open_trades[:] = [t for t in open_trades if t.get("status") == "open"]


def maybe_send_hourly_trade_report() -> None:
    global _last_trade_report_ts
    if not trading_enabled():
        return
    now = int(time.time())
    if _last_trade_report_ts == 0:
        _last_trade_report_ts = now
        return
    if now - _last_trade_report_ts < 3600:
        return
    _last_trade_report_ts = now
    closed = [t for t in trade_history if t.get("status") == "closed"]
    profit = sum(float(t.get("pnl_usdt") or 0) for t in closed if float(t.get("pnl_usdt") or 0) > 0)
    loss = sum(float(t.get("pnl_usdt") or 0) for t in closed if float(t.get("pnl_usdt") or 0) < 0)
    lines = [
        f"📊 <b>گزارش PAPER/LIVE</b> · {iran_now().strftime('%H:%M')}",
        f"بسته {len(closed)} · سود {profit:+.2f}U · ضرر {loss:+.2f}U · خالص {profit + loss:+.2f}U",
        f"باز: {len(open_trades)}",
    ]
    for t in open_trades[:12]:
        lines.append(f"• {t.get('symbol')} {float(t.get('entry') or 0):.6g} adds={t.get('adds', 0)}")
    send_telegram_text("\n".join(lines))
    save_state()


def timeframes_to_check_now() -> list:
    now = int(time.time())
    out = []
    for tf, p in PERIOD_SEC.items():
        if tf not in TIMEFRAMES:
            continue
        age = now % p
        win = SIGNAL_SCAN_SEC.get(tf, 60)
        if age < win:
            out.append(tf)
    out.sort(key=lambda x: TF_ORDER.get(x, 99))
    return out


def seconds_until_next_candle() -> int:
    now = int(time.time())
    waits = [p - (now % p) for p in PERIOD_SEC.values()]
    return max(1, min(waits) if waits else 30)


def pre_alert_tfs_now() -> list:
    """فقط وقتی تا باز شدن کندل بعدی بین ۱۵ثانیه تا BEFORE مانده؛ بعد از باز شدن صفر."""
    now = int(time.time())
    # اگر الان وقت سیگنال است، احتمال‌گپ نده
    if timeframes_to_check_now():
        return []
    out = []
    for tf, before in PRE_ALERT_BEFORE.items():
        p = PERIOD_SEC.get(tf)
        if not p or tf not in TIMEFRAMES:
            continue
        remaining = p - (now % p)
        # فقط داخل پنجره قبل از کندل
        if 15 <= remaining <= before:
            out.append(tf)
    out.sort(key=lambda x: TF_ORDER.get(x, 99))
    return out


async def process_symbol(symbol: str, tfs_to_check: list) -> None:
    now = int(time.time())
    for tf in sorted(tfs_to_check, key=lambda x: TF_ORDER.get(x, 99)):
        if tf not in TIMEFRAMES:
            continue
        period = PERIOD_SEC.get(tf, 900)
        expected_open = (now // period) * period
        df = None
        for attempt in range(3):
            df, src = await fetch_klines(symbol, tf, size=80)
            if df is None or len(df) < BB_PERIOD + 2:
                if attempt == 2:
                    blacklist_add(symbol, 12)
                await asyncio.sleep(0.7)
                continue
            if int(df.iloc[-1]["ts"]) >= expected_open:
                break
            await asyncio.sleep(0.7)
            df = None
        if df is None:
            continue

        try:
            from agents.context import SignalContext
            from agents.filter_agent import run_filter
            from agents.analyst import run_analyst
            from agents.risk import run_risk
            from agents.executor import run_executor
            from agents.logger_agent import run_logger
        except ImportError:
            sig = check_signal(df, symbol, tf)
            if not sig:
                continue
        else:
            fut = get_futures_last(symbol)
            ctx = SignalContext(symbol=symbol, tf=tf, df=df, fut_last=float(fut) if fut else None)
            fd = run_filter(ctx, is_blacklisted=is_blacklisted, is_erratic_row=is_erratic_wick_candle)
            ctx.add(fd)
            if not fd.ok:
                run_logger(ctx)
                continue
            ad = run_analyst(ctx, check_signal_fn=check_signal)
            ctx.add(ad)
            if not ad.ok:
                run_logger(ctx)
                continue
            sig = ctx.analysis
            if not sig:
                continue
            mode = TRADE_MODE
            if mode == "off" and AUTO_TRADE:
                mode = "paper" if is_paper_mode() else "live"
            if mode in ("paper", "live") and sig.get("side") == "SELL":
                rd = run_risk(ctx, only_sell=True, open_trades=open_trades)
                ctx.add(rd)
                if rd.ok:
                    entry = float(sig.get("fut_last") or sig.get("open") or 0)
                    rec = open_sell_trade(symbol, entry, tf, sig.get("candle_ts"))
                    log.info("EXEC %s %s entry=%s ok=%s", mode, symbol, entry, bool(rec))
                else:
                    log.info("RISK_BLOCK %s %s %s", symbol, tf, rd.reasons)
            run_logger(ctx)

        send_telegram_text(format_signal_message(sig))
        log.info("SIGNAL %s %s %s", sig["side"], symbol, tf)
        pending_signals.append({"symbol": symbol, "tf": tf, "candle_ts": sig["candle_ts"], "side": sig["side"]})
        await asyncio.sleep(0.05)


async def pre_alert_cycle(tfs: list | None = None) -> None:
    tfs = tfs or pre_alert_tfs_now()
    if not tfs:
        return
    symbols = get_top_movers(TOP_GAINERS)
    log.info("PRE_ALERT %s n=%d", tfs, len(symbols))
    for tf in tfs:
        for symbol in symbols:
            if is_blacklisted(symbol):
                continue
            try:
                df, _ = await fetch_klines(symbol, tf, size=60)
                if df is None or len(df) < BB_PERIOD + 2:
                    continue
                cur = df.iloc[-1]
                upper = float(cur["upper"])
                close = float(cur["c"])
                fut = get_futures_last(symbol)
                if not fut or upper <= 0:
                    continue
                px = fut if abs(fut - close) / close * 100 < 8 else close
                if px <= upper:
                    continue
                diff = (px - upper) / px * 100.0
                if diff < 1.0:
                    continue
                period = PERIOD_SEC[tf]
                next_open = (int(time.time()) // period + 1) * period
                key = (symbol, tf, next_open)
                if key in sent_pre_alerts:
                    continue
                sent_pre_alerts.add(key)
                cap = (
                    f"⚠️ <b>احتمال گپ</b> · {tf}\n"
                    f"<a href=\"{tv_lbank_url(symbol)}\">{symbol}</a>\n"
                    f"اختلاف تا UB ≈ <b>{diff:.2f}%</b>\n"
                    f"<a href=\"{lbank_futures_url(symbol)}\">ال‌بانک</a>"
                )
                img = make_gap_chart(df, symbol, tf, {"fut_last": px})
                if img:
                    send_telegram_photo(img, cap)
                else:
                    send_telegram_text(cap)
                await asyncio.sleep(0.12)
            except Exception as e:
                log.debug("pre %s: %s", symbol, e)


async def one_cycle(tfs: list | None = None) -> None:
    tfs = tfs or timeframes_to_check_now()
    if not tfs:
        return
    symbols = get_top_movers(TOP_GAINERS)
    log.info("cycle %s n=%d", tfs, len(symbols))
    sem = asyncio.Semaphore(6)

    async def one(sym):
        async with sem:
            try:
                await process_symbol(sym, tfs)
            except Exception as e:
                log.warning("%s: %s", sym, e)

    await asyncio.gather(*[one(s) for s in symbols])
    manage_open_trades()
    maybe_send_hourly_trade_report()
    for k in stats:
        stats[k] = 0
    save_state()


async def main() -> None:
    load_state()
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.error("TELEGRAM_TOKEN / CHAT_ID missing")
        return
    send_telegram_text(format_startup_message())
    log.info("started v%s mode=%s", BOT_VERSION, TRADE_MODE)
    save_state()
    while True:
        try:
            pre = pre_alert_tfs_now()
            if pre:
                await pre_alert_cycle(pre)
            tfs = timeframes_to_check_now()
            if tfs:
                log.info("سر کندل %s", tfs)
                await one_cycle(tfs)
                await asyncio.sleep(3)
            else:
                wait = seconds_until_next_candle()
                sleep_for = max(1, min(wait - 2, 60))
                log.info("خواب %ds", sleep_for)
                await asyncio.sleep(sleep_for)
        except Exception as e:
            log.error("loop: %s", e)
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        save_state()
        github_state_push(force=True)
