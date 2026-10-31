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
import re
import logging
import os
import secrets
import time
from datetime import datetime, timezone
from io import BytesIO
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

TEHRAN = ZoneInfo("Asia/Tehran")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "bot_state.json")
SIGNALS_FILE = os.path.join(BASE_DIR, "signals.json")
LIVE_TOPIC = os.environ.get("LIVE_TOPIC", "lbank-gap-b0b85cee29ce").strip()
STATE_BAK = os.path.join(BASE_DIR, "bot_state.bak")

BOT_VERSION = "2.4.1"
CHANGELOG = {
    "2.1.0": [
        "چارت احتمال‌گپ → استایل SolAtlas (خط‌روند شکست/بازتست) — هنوز ناقص",
    ],
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
OPEN_WINDOW_SEC = {"15m": 300, "1h": 420, "4h": 480}
PRE_ALERT_BEFORE = {"15m": 180, "1h": 360, "4h": 1200}  # 3m / 6m / 20m قبل از کندل
SIGNAL_SCAN_SEC = {"15m": 240, "1h": 360, "4h": 420}

BB_PERIOD = 20
BB_STD = 2.0
PENETRATION_PCT = 0.3
MIN_OPEN_GAP_PCT = 1.0
TOP_GAINERS = 60
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
TRADE_MARGIN_USDT = float(os.environ.get("TRADE_MARGIN_USDT", "10"))
TRADE_LEVERAGE = int(os.environ.get("TRADE_LEVERAGE", "20"))
TRADE_TP_PCT = float(os.environ.get("TRADE_TP_PCT", "50"))  # 50% سود مارجین → کلوز
TRADE_AVG_DOWN = True
TRADE_MAX_ADDS = int(os.environ.get("TRADE_MAX_ADDS", "20"))  # سقف کل پله
AVG_LEG_PERIOD_SEC = 900  # فقط یک پله در هر کندل ۱۵ دقیقه


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
_last_daily_report_date = ""
PAPER_START_BALANCE = float(os.getenv("PAPER_START_BALANCE", "1000"))
PAPER_STRATEGY_TAG = "v2_1000_10u_20x_tp50"  # برای ریست یک‌باره
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
    path = os.environ.get("STATE_GITHUB_PATH") or "bot_state.json"
    url = f"https://api.github.com/repos/{owner}/{name}/contents/{path}"
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


def github_push_file(local_path: str, remote_path: str, force: bool = False) -> bool:
    """آپلود یک فایل JSON به ریشهٔ ریپو (برای bot_state و signals)."""
    global _last_github_push_ts
    now = int(time.time())
    # rate-limit فقط برای state عادی؛ signals با force=True رد می‌شود
    if not force and _last_github_push_ts and now - _last_github_push_ts < 30:
        return False
    owner, name, token = _github_repo_parts()
    if not token or not owner or not name:
        log.warning("github_push skip: no token/repo")
        return False
    if not os.path.exists(local_path):
        return False
    try:
        with open(local_path, "r", encoding="utf-8") as f:
            raw = f.read()
        b64 = base64.b64encode(raw.encode("utf-8")).decode("ascii")
        url = f"https://api.github.com/repos/{owner}/{name}/contents/{remote_path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        sha = None
        gr = requests.get(url, headers=headers, timeout=20)
        if gr.status_code == 200:
            sha = (gr.json() or {}).get("sha")
        body = {
            "message": f"bot {remote_path} {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            "content": b64,
            "branch": os.environ.get("STATE_GITHUB_BRANCH") or "main",
        }
        if sha:
            body["sha"] = sha
        pr = requests.put(url, headers=headers, json=body, timeout=30)
        if pr.status_code in (200, 201):
            _last_github_push_ts = now
            log.info("github_push OK %s (%s bytes)", remote_path, len(raw))
            return True
        log.warning("github_push HTTP %s %s %s", remote_path, pr.status_code, (pr.text or "")[:160])
        return False
    except Exception as e:
        log.warning("github_push %s: %s", remote_path, e)
        return False


def github_state_push(force: bool = False) -> bool:
    path = os.environ.get("STATE_GITHUB_PATH") or "bot_state.json"
    return github_push_file(STATE_FILE, path, force=force)



def save_state(force_remote: bool = False) -> None:
    try:
        payload = {
            "open_trades": [t for t in open_trades if t.get("status") == "open"][-50:],
            "trade_history": trade_history[-200:],
            "pending": pending_signals[-300:],
            "kline_blacklist": kline_blacklist,
            "last_trade_report_ts": _last_trade_report_ts,
            "last_daily_report_date": _last_daily_report_date,
            "last_known_version": BOT_VERSION,
            "paper_strategy_tag": PAPER_STRATEGY_TAG,
        }
        raw = json.dumps(payload, ensure_ascii=False, indent=2)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            f.write(raw)
        try:
            with open(STATE_BAK, "w", encoding="utf-8") as f:
                f.write(raw)
        except Exception:
            pass
        github_state_push(force=force_remote)
    except Exception as e:
        log.warning("save_state: %s", e)


def load_state() -> None:
    global last_known_version, _last_trade_report_ts, _last_daily_report_date
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
        _last_daily_report_date = str(payload.get("last_daily_report_date") or "")
        last_known_version = payload.get("last_known_version")
        log.info("State loaded open=%d hist=%d file=%s", len(open_trades), len(trade_history), path)
        # ریست یک‌باره برای استراتژی جدید paper
        tag = str(payload.get("paper_strategy_tag") or "")
        if tag != PAPER_STRATEGY_TAG:
            log.info("PAPER_RESET strategy %s -> %s (clear trades)", tag or "none", PAPER_STRATEGY_TAG)
            open_trades.clear()
            trade_history.clear()
            pending_signals.clear()
    except Exception as e:
        log.warning("load_state: %s", e)


def push_live(ev: dict) -> None:
    """ارسال لحظه‌ای به ntfy برای داشبورد وب (جدا از تأخیر GitHub)."""
    if not LIVE_TOPIC:
        return
    try:
        requests.post(
            f"https://ntfy.sh/{LIVE_TOPIC}",
            data=json.dumps(ev, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
    except Exception:
        pass


def push_positions_live() -> None:
    """اسنپ‌شات پوزیشن‌های باز برای صفحه وب (ntfy + signals.json)."""
    try:
        rows = []
        for tr in open_trades:
            if tr.get("status") != "open":
                continue
            px = get_futures_last(tr.get("symbol") or "") or float(tr.get("entry") or 0)
            try:
                upnl, roi = _sell_upnl(tr, float(px))
            except Exception:
                upnl, roi = 0.0, 0.0
            rows.append({
                "symbol": tr.get("symbol"),
                "side": tr.get("side"),
                "entry": tr.get("entry"),
                "tp": tr.get("tp_price"),
                "adds": int(tr.get("adds") or 0),
                "pele": int(tr.get("adds") or 0) + 1,
                "margin": tr.get("margin"),
                "px": px,
                "pnl": round(float(upnl), 4),
                "roi": round(float(roi), 2),
                "tf": tr.get("tf"),
            })
        push_live({"t": "pos", "positions": rows, "n": len(rows), "ts": int(time.time())})
        # به‌روز کردن trades داخل signals.json بدون سیگنال جعلی
        log_signal_web("POS", "—", "—", 0.0)
    except Exception as e:
        log.debug("push_positions: %s", e)


def send_telegram_text(text: str) -> bool:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=15,
        )
        ok = r.status_code == 200
        if not ok:
            log.warning("tg text: %s", (r.text or "")[:180])
        else:
            try:
                plain = re.sub(r"<[^>]+>", "", text or "")
                push_live({"t": "log", "x": plain[:500], "ts": int(time.time())})
            except Exception:
                pass
        return ok
    except Exception as e:
        log.warning("tg text: %s", e)
        return False


def send_telegram_photo(image_bytes: bytes, caption: str) -> bool:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            data={"chat_id": CHAT_ID, "caption": (caption or "")[:1024], "parse_mode": "HTML"},
            files={"photo": ("chart.png", image_bytes, "image/png")},
            timeout=30,
        )
        ok = r.status_code == 200
        if not ok:
            log.warning("tg photo: %s", (r.text or "")[:180])
        else:
            try:
                plain = re.sub(r"<[^>]+>", "", caption or "")
                push_live({"t": "log", "x": plain[:500], "ts": int(time.time())})
            except Exception:
                pass
        return ok
    except Exception as e:
        log.warning("tg photo: %s", e)
        return False


def format_startup_message() -> str:
    global last_known_version
    lines = [f"✅ ربات روشن شد — <b>v{BOT_VERSION}</b>", f"🧠 agents · TRADE_MODE=<code>{TRADE_MODE}</code>"]
    if trading_enabled():
        lines.append(f"🤖 exec: {'PAPER' if is_paper_mode() else 'LIVE'} · {TRADE_MARGIN_USDT}U · {TRADE_LEVERAGE}x · گزارش هر ۱۵د")
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
    out = [s for s in symbols if not is_blacklisted(s)]
    log.info(
        "Movers: gainers=%d volume=%d unique=%d sample=%s",
        len(gainers), len(volumes), len(out), ",".join(out[:8]),
    )
    return out


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
        log.info("SKIP_SHORT %s %s bars=%s", symbol, tf, len(df) if df is not None else 0)
        return None
    cur = df.iloc[-1]
    open_p, high, low, close = float(cur["o"]), float(cur["h"]), float(cur["l"]), float(cur["c"])
    if is_erratic_wick_candle(cur):
        stats["skip_no_gap"] += 1
        log.info("SKIP_WICK %s %s open=%.6g high=%.6g low=%.6g", symbol, tf, open_p, high, low)
        return None
    upper, lower = float(cur["upper"]), float(cur["lower"])
    if np.isnan(upper) or np.isnan(lower):
        log.info("SKIP_BB_NAN %s %s", symbol, tf)
        return None
    stats["checked"] += 1
    candle_ts = int(cur["ts"])
    age_sec = int(time.time()) - candle_ts
    max_age = OPEN_WINDOW_SEC.get(tf, 120)
    if age_sec < 0 or age_sec > max_age:
        stats["skip_age"] += 1
        log.info(
            "SKIP_AGE %s %s | age=%ss > window=%ss | open=%.6g upper=%.6g",
            symbol, tf, age_sec, max_age, open_p, upper,
        )
        return None
    upper_pen = (high - upper) / upper * 100.0 if upper > 0 else 0.0
    open_gap_pct = (open_p - upper) / open_p * 100.0 if open_p > 0 else 0.0
    side = None
    if open_p > upper and open_gap_pct >= MIN_OPEN_GAP_PCT:
        side = "SELL"
    if not side or (ONLY_SELL and side != "SELL"):
        stats["skip_no_gap"] += 1
        log.info(
            "SKIP_NO_GAP %s %s | open=%.6g upper=%.6g gap=%.2f%% pen=%.2f%%",
            symbol, tf, open_p, upper, open_gap_pct, upper_pen,
        )
        return None
    fut = get_futures_last(symbol)
    if fut is not None and fut < upper * 0.995 and symbol not in ALWAYS_INCLUDE:
        stats["skip_no_gap"] += 1
        log.info("SKIP_FUT_BELOW_UB %s %s | fut=%.6g upper=%.6g", symbol, tf, fut, upper)
        return None
    if fut is None:
        fut = open_p
        log.info("FUT_FALLBACK %s %s use_open=%.6g", symbol, tf, open_p)
    key = (symbol, tf, candle_ts, side)
    if key in sent_signals:
        stats["skip_dup"] += 1
        return None
    sent_signals.add(key)
    if len(sent_signals) > 4000:
        sent_signals.clear()
    stats["signal_ok"] += 1
    diff_pct = (fut - upper) / fut * 100.0 if fut else 0.0
    log.info(
        "SIGNAL_CANDIDATE SELL %s %s | age=%ss open=%.6g upper=%.6g fut=%.6g gap=%.2f%%",
        symbol, tf, age_sec, open_p, upper, fut, diff_pct,
    )
    return {
        "side": side, "symbol": symbol, "tf": tf, "open": open_p, "high": high, "low": low, "close": close,
        "upper": upper, "lower": lower, "diff_pct": diff_pct, "fut_last": fut, "candle_ts": candle_ts, "age_sec": age_sec,
    }


def tv_lbank_url(symbol: str) -> str:
    s = (symbol or "").upper().replace("_", "").replace("-", "")
    return f"https://www.tradingview.com/chart/?symbol=LBANK%3A{s}.P"


def lbank_futures_url(symbol: str) -> str:
    # فرمت درست: btcusdt نه btc_usdt
    s = (symbol or "").lower().replace("_", "").replace("-", "")
    return f"https://www.lbank.com/futures/{s}"



def log_signal_web(kind: str, symbol: str, tf: str, gap: float) -> None:
    """برای داشبورد وب: signals.json را به‌روز و روی GitHub می‌نویسد."""
    try:
        data = {}
        if os.path.exists(SIGNALS_FILE):
            try:
                with open(SIGNALS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
            except Exception:
                data = {}
        sigs = list(data.get("signals") or [])[-199:]
        sigs.append({
            "kind": kind,
            "symbol": symbol,
            "tf": tf,
            "gap": round(float(gap or 0), 2),
            "ts": int(time.time()),
        })
        closed = [t for t in trade_history if t.get("status") == "closed"]
        pnl_list = [float(t.get("pnl_usdt") or 0) for t in closed]
        trades_out = []
        for tr in open_trades:
            if tr.get("status") != "open":
                continue
            px = get_futures_last(tr.get("symbol") or "") or float(tr.get("entry") or 0)
            try:
                upnl, _ = _sell_upnl(tr, float(px))
            except Exception:
                upnl = 0.0
            trades_out.append({
                "symbol": tr.get("symbol"),
                "entry": tr.get("entry"),
                "tp": tr.get("tp_price"),
                "adds": int(tr.get("adds") or 0),
                "margin": tr.get("margin"),
                "pnl": round(float(upnl), 4),
            })
        payload = {
            "mode": "PAPER" if is_paper_mode() else ("LIVE" if trading_enabled() else "OFF"),
            "version": BOT_VERSION,
            "updated_at": int(time.time()),
            "signals": sigs,
            "trades": trades_out,
            "closed": {
                "profit": round(sum(p for p in pnl_list if p > 0), 4),
                "loss": round(sum(p for p in pnl_list if p < 0), 4),
                "count": len(closed),
            },
        }
        with open(SIGNALS_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        github_push_file(SIGNALS_FILE, "signals.json", force=True)
    except Exception as e:
        log.warning("log_signal_web: %s", e)


def format_signal_message(sig: dict) -> str:
    sym = sig["symbol"]
    diff = float(sig.get("diff_pct") or 0)
    return (
        f"🔴 <b>سیگنال SELL</b>\n"
        f"<a href=\"{tv_lbank_url(sym)}\">{sym}</a> · {sig['tf']}\n"
        f"اختلاف تا UB: <b>{diff:.2f}%</b>\n"
        f"<a href=\"{lbank_futures_url(sym)}\">ال‌بانک فیوچرز</a>"
    )



def detect_fake_break_watch(df: pd.DataFrame) -> str | None:
    """حالت الف: اگر فیک‌بریک مقاومت اخیر → فقط هشدار watch SELL (بدون ورود)."""
    try:
        try:
            from trendlines_breaks import find_trendlines, y_at
        except ImportError:
            from agents.trendlines_breaks import find_trendlines, y_at
        lines = find_trendlines(df.tail(min(150, len(df))).reset_index(drop=True), max_res=2, max_sup=1)
        c = df["c"].astype(float).values
        n = len(df)
        for L in lines:
            if L.get("kind") != "res":
                continue
            bi = L.get("broken")
            if bi is None:
                continue
            age = n - 1 - int(bi)
            if age < 1 or age > 4:
                continue
            # بعد از break آیا کلوز دوباره زیر خط آمده؟
            yb = y_at(L["i0"], L["p0"], L["i1"], L["p1"], bi)
            reclaimed = False
            for i in range(int(bi) + 1, n):
                y = y_at(L["i0"], L["p0"], L["i1"], L["p1"], i)
                if c[i] < y * 0.998:
                    reclaimed = True
                    break
            if reclaimed:
                return f"⚠️ FAKE BREAK مقاومت · watch SELL (age={age})"
        return None
    except Exception:
        return None


def make_gap_chart(df: pd.DataFrame, symbol: str, tf: str, sig: dict | None = None) -> bytes | None:
    """احتمال‌گپ: چارت SolAtlas-style از trendlines_breaks (WIP)."""
    try:
        try:
            from trendlines_breaks import render_trendline_chart
        except ImportError:
            from agents.trendlines_breaks import render_trendline_chart
        entry = float((sig or {}).get("fut_last") or (sig or {}).get("open") or df.iloc[-1]["c"])
        ub = None
        if "upper" in df.columns:
            try:
                ub = float(df.iloc[-1]["upper"])
            except Exception:
                ub = None
        gap_pct = ((entry - ub) / entry * 100.0) if ub and entry and ub == ub else None
        return render_trendline_chart(df, symbol, tf, gap_pct=gap_pct, max_bars=180)
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
        log.info("PAPER_SKIP %s reason=trading_disabled", symbol)
        return None
    entry_price = float(entry_price)
    if entry_price <= 0:
        log.info("PAPER_SKIP %s reason=bad_entry", symbol)
        return None
    existing = next((t for t in open_trades if t.get("symbol") == symbol and t.get("status") == "open" and t.get("side") == "SELL"), None)
    if existing is not None:
        if not TRADE_AVG_DOWN:
            log.info("PAPER_SKIP %s reason=already_open_no_avg", symbol)
            return None
        adds = int(existing.get("adds") or 0)
        if adds >= TRADE_MAX_ADDS:
            log.info("PAPER_SKIP %s reason=max_adds", symbol)
            return None
        last_cts = int(existing.get("last_leg_candle_ts") or 0)
        cts = int(candle_ts or 0)
        if cts and last_cts and cts <= last_cts:
            log.info("PAPER_SKIP %s reason=same_candle_leg", symbol)
            return None
        avg = float(existing.get("entry") or 0)
        # پله بدون شرط گپ/جهت قیمت — فقط کندل جدید
        add_qty = trade_qty_from_margin(symbol, entry_price)
        if not add_qty:
            return None
        old_qty = float(existing.get("qty") or 0)
        new_qty = old_qty + add_qty
        new_margin = float(existing.get("margin") or TRADE_MARGIN_USDT) + TRADE_MARGIN_USDT
        new_entry = (avg * old_qty + entry_price * add_qty) / new_qty
        existing.update({"entry": new_entry, "qty": new_qty, "margin": new_margin, "tp_price": tp_price_for_sell(new_entry),
                         "adds": adds + 1,
                         "last_leg_candle_ts": (int(cts or last_cts or time.time()) // AVG_LEG_PERIOD_SEC) * AVG_LEG_PERIOD_SEC,
                         "tf": tf})
        log.info("PAPER_ADD %s avg=%.6g margin=%s adds=%s", symbol, new_entry, new_margin, adds + 1)
        save_state(force_remote=True)
        return existing

    qty = trade_qty_from_margin(symbol, entry_price)
    if not qty:
        return None
    tp = tp_price_for_sell(entry_price)
    rec = {
        "symbol": symbol, "side": "SELL", "status": "open", "entry": entry_price, "qty": qty,
        "margin": TRADE_MARGIN_USDT, "leverage": TRADE_LEVERAGE, "tp_price": tp, "tf": tf,
        "opened_at": int(time.time()), "adds": 0,
        "entry_candle_ts": int(candle_ts or 0),
        "last_leg_candle_ts": (int(candle_ts or time.time()) // AVG_LEG_PERIOD_SEC) * AVG_LEG_PERIOD_SEC,
        "dry_run": is_paper_mode(),
    }
    if is_paper_mode():
        rec["order_id"] = f"PAPER-{symbol}-{int(time.time())}"
        open_trades.append(rec)
        log.info("PAPER_OPEN %s %s entry=%.6g tp=%.6g", symbol, tf, entry_price, tp)
        save_state(force_remote=True)
        try:
            push_positions_live()
        except Exception:
            pass
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
    rec["closed_at"] = int(time.time())
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
    now = int(time.time())
    for rec in list(open_trades):
        if rec.get("status") != "open" or rec.get("side") != "SELL":
            continue
        sym = rec.get("symbol") or ""
        px = get_futures_last(sym)
        if not px:
            continue
        # کلوز فقط با رسیدن به ۵۰٪ سود مارجین (بدون استاپ)
        tp = float(rec.get("tp_price") or 0)
        if tp > 0 and px <= tp:
            close_trade_record(rec, px, "TP50")
            continue
        # اگر در ضرر است → در کندل بعدی همان TF یک پله ۱۰U بدون شرط گپ
        if not TRADE_AVG_DOWN or not is_paper_mode():
            continue
        pnl, _ = _sell_upnl(rec, float(px))
        if pnl >= 0:
            continue
        # پله فقط یک‌بار در هر کندل ۱۵دقیقه (نه هر کندل 1h/4h)
        period = AVG_LEG_PERIOD_SEC
        candle_open = (now // period) * period
        last_leg = int(rec.get("last_leg_candle_ts") or 0)
        if candle_open <= last_leg:
            continue
        tf = rec.get("tf") or "15m"
        adds = int(rec.get("adds") or 0)
        if adds >= TRADE_MAX_ADDS:
            continue
        add_qty = trade_qty_from_margin(sym, float(px))
        if not add_qty:
            continue
        old_qty = float(rec.get("qty") or 0)
        new_qty = old_qty + add_qty
        new_margin = float(rec.get("margin") or TRADE_MARGIN_USDT) + TRADE_MARGIN_USDT
        avg = float(rec.get("entry") or 0)
        new_entry = (avg * old_qty + float(px) * add_qty) / new_qty if new_qty else float(px)
        rec.update({
            "entry": new_entry,
            "qty": new_qty,
            "margin": new_margin,
            "tp_price": tp_price_for_sell(new_entry),
            "adds": adds + 1,
            "last_leg_candle_ts": candle_open,
        })
        log.info(
            "PAPER_AVG_LOSS %s tf=%s px=%.6g avg=%.6g pelea=%s margin=%.0f",
            sym, tf, px, new_entry, adds + 1, new_margin,
        )
        save_state(force_remote=True)
    open_trades[:] = [x for x in open_trades if x.get("status") == "open"]


def _sell_upnl(rec: dict, px: float) -> tuple[float, float]:
    """(pnl_usdt, margin_roi_pct) for open SELL."""
    entry = float(rec.get("entry") or 0)
    lev = float(rec.get("leverage") or TRADE_LEVERAGE)
    margin = float(rec.get("margin") or TRADE_MARGIN_USDT)
    if entry <= 0 or px <= 0:
        return 0.0, 0.0
    margin_roi = ((entry - px) / entry * 100.0) * lev
    pnl = margin * (margin_roi / 100.0)
    return pnl, margin_roi


def _closed_pnl_since(ts_from: int) -> tuple[float, float, float, int]:
    closed = [
        t for t in trade_history
        if t.get("status") == "closed" and int(t.get("closed_at") or t.get("opened_at") or 0) >= ts_from
    ]
    profit = sum(float(t.get("pnl_usdt") or 0) for t in closed if float(t.get("pnl_usdt") or 0) > 0)
    loss = sum(float(t.get("pnl_usdt") or 0) for t in closed if float(t.get("pnl_usdt") or 0) < 0)
    return profit, loss, profit + loss, len(closed)


def maybe_send_hourly_trade_report() -> None:
    global _last_trade_report_ts
    if not trading_enabled():
        return
    now = int(time.time())
    if _last_trade_report_ts == 0:
        _last_trade_report_ts = now
        return
    if now - _last_trade_report_ts < 900:
        return
    _last_trade_report_ts = now

    closed = [t for t in trade_history if t.get("status") == "closed"]
    profit = sum(float(t.get("pnl_usdt") or 0) for t in closed if float(t.get("pnl_usdt") or 0) > 0)
    loss = sum(float(t.get("pnl_usdt") or 0) for t in closed if float(t.get("pnl_usdt") or 0) < 0)
    net = profit + loss
    mode = "PAPER" if is_paper_mode() else "LIVE"

    open_upnl = 0.0
    open_lines = []
    for rec in open_trades[:15]:
        if rec.get("status") != "open":
            continue
        sym = rec.get("symbol") or "?"
        px = get_futures_last(sym) or 0.0
        pnl, roi = _sell_upnl(rec, float(px)) if px else (0.0, 0.0)
        open_upnl += pnl
        pele = int(rec.get("adds") or 0) + 1
        if pnl >= 0:
            mark = f"🟢 <b>{pnl:+.2f}U</b> ({roi:+.1f}%)"
        else:
            mark = f"🔴 <b>{pnl:+.2f}U</b> ({roi:+.1f}%)"
        open_lines.append(f"• {sym} · پله {pele} · {mark}")

    lines = [
        f"📊 <b>گزارش {mode}</b> · {iran_now().strftime('%H:%M')}",
        f"بسته: {len(closed)}",
        f"🟢 سود بسته (TP): <b>{profit:+.2f}U</b>",
        f"خالص بسته‌شده: <b>{profit:+.2f}U</b>",
        f"باز: {len(open_trades)} · uPnL: <b>{open_upnl:+.2f}U</b>",
    ]
    if open_lines:
        lines.append("────────")
        lines.extend(open_lines)
    send_telegram_text("\n".join(lines))
    save_state()
    try:
        log_signal_web("HEARTBEAT", "—", "—", 0.0)
    except Exception:
        pass


def maybe_send_daily_paper_report() -> None:
    """گزارش پایان‌روز: موجودی کاغذی بر اساس ترید ۲۴ساعت."""
    global _last_daily_report_date
    if not trading_enabled():
        return
    from datetime import timedelta
    now_ir = iran_now()
    # پنجره ۲۳:۵۰–۲۳:۵۹
    if not (now_ir.hour == 23 and now_ir.minute >= 50):
        return
    day_key = now_ir.strftime("%Y-%m-%d")
    if _last_daily_report_date == day_key:
        return
    _last_daily_report_date = day_key

    since = int(time.time()) - 24 * 3600
    profit, loss, net, cnt = _closed_pnl_since(since)
    open_upnl = 0.0
    for rec in open_trades:
        if rec.get("status") != "open":
            continue
        px = get_futures_last(rec.get("symbol") or "") or 0.0
        if px:
            pnl, _ = _sell_upnl(rec, float(px))
            open_upnl += pnl
    all_closed_net = sum(float(t.get("pnl_usdt") or 0) for t in trade_history if t.get("status") == "closed")
    balance = PAPER_START_BALANCE + all_closed_net + open_upnl
    mode = "PAPER" if is_paper_mode() else "LIVE"
    send_telegram_text(
        f"🌙 <b>گزارش روزانه {mode}</b>\n"
        f"📅 {day_key}\n"
        f"────────\n"
        f"در ۲۴ ساعت گذشته:\n"
        f"معاملات بسته: <b>{cnt}</b>\n"
        f"🟢 سود TP: <b>{profit:+.2f}U</b>\n"
        f"خالص ۲۴س: <b>{profit:+.2f}U</b>\n"
        f"uPnL باز: <b>{open_upnl:+.2f}U</b>\n"
        f"────────\n"
        f"💼 موجودی کاغذی تقریبی: <b>{balance:.2f}U</b>\n"
        f"(شروع {PAPER_START_BALANCE:.0f}U + بسته‌ها + باز)"
    )
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
    """چند دقیقه قبل از باز شدن هر TF (1h/4h زودتر). TFهایی که الان در پنجره سیگنال‌اند حذف می‌شوند."""
    now = int(time.time())
    signal_now = set(timeframes_to_check_now())
    out = []
    for tf, before in PRE_ALERT_BEFORE.items():
        if tf in signal_now:
            continue
        p = PERIOD_SEC.get(tf)
        if not p or tf not in TIMEFRAMES:
            continue
        remaining = p - (now % p)
        if 10 <= remaining <= before:
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
            log.info("NO_KLINE %s %s", symbol, tf)
            continue

        log.info("CHECK %s %s bars=%d", symbol, tf, len(df))
        sig = check_signal(df, symbol, tf)
        if not sig:
            continue

        # لاگ اختیاری agents (فیلتر سخت را برای ورود paper دور می‌زنیم)
        try:
            from agents.context import SignalContext
            from agents.logger_agent import run_logger
            fut = get_futures_last(symbol)
            ctx = SignalContext(symbol=symbol, tf=tf, df=df, fut_last=float(fut) if fut else None)
            ctx.analysis = sig
            run_logger(ctx)
        except Exception:
            pass

        # سیگنال متنی قطع — فقط لاگ + paper + وب داخلی
        log_signal_web("SELL", symbol, tf, float(sig.get("diff_pct") or 0))
        log.info("SIGNAL_SILENT %s %s %s gap=%.2f%%", sig["side"], symbol, tf, float(sig.get("diff_pct") or 0))

        # هر سیگنال SELL → دقیقاً یک ورود paper/live (پله فقط اگر از قبل باز باشد)
        if trading_enabled() and sig.get("side") == "SELL":
            entry = float(sig.get("fut_last") or sig.get("open") or 0)
            if entry > 0:
                rec = open_sell_trade(symbol, entry, tf, sig.get("candle_ts"))
                log.info("PAPER_ENTRY %s %s entry=%s ok=%s", symbol, tf, entry, bool(rec))
                save_state(force_remote=True)

        pending_signals.append({"symbol": symbol, "tf": tf, "candle_ts": sig["candle_ts"], "side": sig["side"]})
        await asyncio.sleep(0.05)


async def pre_alert_cycle(tfs: list | None = None) -> None:
    tfs = tfs or pre_alert_tfs_now()
    if not tfs:
        return
    symbols = get_top_movers(TOP_GAINERS)
    log.info("PRE_ALERT TFs=%s n=%d iran=%s", ",".join(tfs), len(symbols), iran_now().strftime("%H:%M:%S"))
    for tf in tfs:
        for symbol in symbols:
            if is_blacklisted(symbol):
                continue
            try:
                df, _ = await fetch_klines(symbol, tf, size=150)
                if df is None or len(df) < BB_PERIOD + 2:
                    log.info("PRE_SKIP_NO_KLINE %s %s", symbol, tf)
                    continue
                cur = df.iloc[-1]
                upper = float(cur["upper"])
                close = float(cur["c"])
                fut = get_futures_last(symbol)
                if not fut or upper <= 0:
                    log.info("PRE_SKIP_NO_FUT %s %s", symbol, tf)
                    continue
                px = fut if abs(fut - close) / close * 100 < 8 else close
                if px <= upper:
                    log.info("PRE_SKIP_BELOW_UB %s %s px=%.6g ub=%.6g", symbol, tf, px, upper)
                    continue
                diff = (px - upper) / px * 100.0
                if diff < MIN_OPEN_GAP_PCT:
                    log.info("PRE_SKIP_SMALL_GAP %s %s diff=%.2f%%", symbol, tf, diff)
                    continue
                period = PERIOD_SEC[tf]
                next_open = (int(time.time()) // period + 1) * period
                key = (symbol, tf, next_open)
                if key in sent_pre_alerts:
                    log.info("PRE_SKIP_DUP %s %s", symbol, tf)
                    continue
                sent_pre_alerts.add(key)
                log.info("PRE_ALERT_HIT %s %s diff=%.2f%% px=%.6g ub=%.6g", symbol, tf, diff, px, upper)
                cap = (
                    f"⚠️ <b>احتمال گپ</b> · {tf}\n"
                    f"<a href=\"{tv_lbank_url(symbol)}\">{symbol}</a>\n"
                    f"اختلاف تا UB ≈ <b>{diff:.2f}%</b>\n"
                    f"<a href=\"{lbank_futures_url(symbol)}\">ال‌بانک</a>"
                )
                fake_note = detect_fake_break_watch(df)
                if fake_note:
                    cap = cap + chr(10) + fake_note
                img = make_gap_chart(df, symbol, tf, {"fut_last": px})
                if img:
                    send_telegram_photo(img, cap)
                else:
                    send_telegram_text(cap)
                log_signal_web("GAP", symbol, tf, diff)
                push_live({"t": "sig", "kind": "GAP", "symbol": symbol, "tf": tf, "gap": round(float(diff), 2), "ts": int(time.time())})
                await asyncio.sleep(0.12)
            except Exception as e:
                log.debug("pre %s: %s", symbol, e)


async def one_cycle(tfs: list | None = None) -> None:
    tfs = tfs or timeframes_to_check_now()
    if not tfs:
        return
    symbols = get_top_movers(TOP_GAINERS)
    log.info("cycle TFs=%s n=%d iran=%s", ",".join(tfs), len(symbols), iran_now().strftime("%H:%M:%S"))
    sem = asyncio.Semaphore(12)

    async def one(sym):
        async with sem:
            try:
                await process_symbol(sym, tfs)
            except Exception as e:
                log.warning("%s: %s", sym, e)

    await asyncio.gather(*[one(s) for s in symbols])
    manage_open_trades()
    push_positions_live()
    maybe_send_hourly_trade_report()
    maybe_send_daily_paper_report()
    log.info(
        "STATS checked=%s signal=%s skip_age=%s skip_gap=%s skip_dup=%s open_trades=%s",
        stats.get("checked", 0), stats.get("signal_ok", 0),
        stats.get("skip_age", 0), stats.get("skip_no_gap", 0), stats.get("skip_dup", 0),
        len(open_trades),
    )
    for k in stats:
        stats[k] = 0
    save_state()


async def main() -> None:
    load_state()
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.error("TELEGRAM_TOKEN / CHAT_ID missing")
        return
    send_telegram_text(format_startup_message())
    mode_code = {"paper": 1, "live": 2}.get((TRADE_MODE or "off").lower(), 0)
    log.info(
        "started v%s mode_code=%s trading=%s is_paper=%s auto=%s",
        BOT_VERSION,
        mode_code,
        int(trading_enabled()),
        int(is_paper_mode()),
        int(bool(AUTO_TRADE)),
    )
    owner, name, token = _github_repo_parts()
    log.info(
        "state_remote repo=%s token=%s",
        f"{owner}/{name}" if owner and name else "NONE",
        "YES" if token else "NO",
    )
    save_state(force_remote=True)
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
                # نزدیک کندل زودتر بیدار شو؛ حداکثر ۳۰ث
                sleep_for = max(1, min(wait - 3, 30))
                pre2 = pre_alert_tfs_now()
                log.info("sleep=%ds next_candle=%ds pre=%s", sleep_for, wait, ",".join(pre2) or "-")
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
