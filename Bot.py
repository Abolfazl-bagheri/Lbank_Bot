"""
LBank Futures Bollinger Gap Signal Bot
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from io import BytesIO
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import websockets

TEHRAN = ZoneInfo("Asia/Tehran")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "bot_state.json")

# ========================= VERSION / CHANGELOG =========================
# هر بار اپدیت: BOT_VERSION را بالا ببر و یک خط در CHANGELOG[نسخه] اضافه کن.
BOT_VERSION = "1.9.0"
CHANGELOG = {
    "1.9.0": [
        "حالت ترکیبی: اتومات در گروه (گپ + احتمال گپ + زاویه 5m/15m/1h/4h)",
        "دکمه درخواستی → نتیجه فقط در پیوی همان نفر",
    ],
    "1.8.5": [
        "بهینه سرعت: Gate REST اول (بدون WS کند)، اسکن موازی بیشتر، لیست کوتاه‌تر",
        "BOT_MODE=auto | ondemand | hybrid",
    ],
    "1.8.4": [
        "منو و نتیجه با edit همان پیام (بدون اسپم پیام جدید)",
        "چند نفر هر کدام روی پیام خودشان کار می‌کنند",
    ],
    "1.8.3": [
        "نتایج اسکن به پیوی کاربر (گروه شلوغ نمی‌شود)",
        "session جدا per نفر — چند نفر همزمان بدون تداخل",
        "چت گروه: به پیام‌های معمولی جواب نمی‌دهد",
    ],
    "1.8.2": [
        "دکمه تک‌نماد: نام ارز → گپ یا زاویه → تایم‌فریم → اسکن همان نماد",
    ],
    "1.8.1": [
        "زاویه: تایم‌فریم ۵م و ۱۵م به دکمه و اسکن اضافه شد",
        "گپ: دکمه ۵م هم اضافه شد",
    ],
    "1.8.0": [
        "حالت درخواستی: دکمه‌های گپ و زاویه + انتخاب تایم‌فریم",
        "اسکن همیشگی خاموش — فقط وقتی دکمه بزنی اسکن می‌کند",
    ],
    "1.7.0": [
        "آماده‌باش بستن زاویه (EMA5/10/20 + خم UB) — فرمول EWZ/GRT",
        "فقط ۱س و ۴س | پیام دسته‌ای | سقف ۱۰ آلارم در روز | جدا از گپ",
    ],
    "1.6.1": [
        "محکم‌کاری محاسبه پله‌های سِل تا سیگنال به‌خاطر خطا قطع نشود",
    ],
    "1.6.0": [
        "حذف دکمه محاسبه سقف/کف",
        "پله‌های اضافه سِل (۲ و ۳) از سوئینگ + ATR روی همان تایم‌فریم",
        "هدف برگشت گپ (UB) در پیام سیگنال",
    ],
    "1.5.3": [
        "پیام سیگنال کوتاه: نوع · نماد · تایم · اختلاف% · Q",
        "احتمال گپ‌ها یکجا per تایم‌فریم (۱۵م / ۱س / ۴س)",
    ],
    "1.5.2": [
        "گزارش دور خلوت: بدون سیگنال فقط یک خط ضربان؛ نتیجه کندل قبلی فقط اگر چیزی بسته شده باشد",
    ],
    "1.5.1": [
        "رفع هشدار جعلی احتمال گپ (مثل Harmony): مقایسه فقط با قیمت همان منبع کندل، نه فیوچرز ال‌بانک روی BB گیت",
    ],
    "1.5.0": [
        "هشدار احتمال گپ کوتاه‌تر و فقط با اختلاف >۱٪ نسبت به UB",
        "زمان‌بندی: ۱۵م=۳د قبل | ۱س=۵د قبل | ۴س=۲۰د قبل",
        "لینک نماد مثل سیگنال + ظاهر متمایز پیام",
    ],
    "1.4.0": [
        "بلک‌لیست نمادهای NO_KLINE (۲۴س) — سرعت بالاتر، بدون چک تکراری بی‌فایده",
        "هشدار آماده‌باش گپ ۱۵م، ۲ دقیقه قبل از باز شدن کندل",
        "حداقل گپ اوپن ۰.۵٪ (حذف گپ‌های نوکی)",
        "امتیاز کیفیت ورود نرم‌تر + متن ریسک اسکالپ/نگه‌داشتن",
        "اعلان نسخه و تغییرات هنگام روشن شدن بعد از اپدیت",
    ],
    "1.3.0": [
        "امتیاز کیفیت ورود زیر هر سیگنال",
        "دکمه محاسبه سقف/کف (۴ روش) → ارسال به پیوی",
        "چارت سفید فشرده با برچسب UB / Prev High / Gap%",
    ],
    "1.2.0": [
        "فال‌بک کندل Gate / BingX برای نمادهای بدون اسپات",
        "گزارش دور + نتیجه کندل قبلی در یک پیام",
        "BTC و XAUT همیشه در واچ‌لیست",
    ],
}

TELEGRAM_TOKEN = os.environ.get(
    "TELEGRAM_TOKEN", "8750093707:AAEL73X5nl-uPgsWzLpF7bFdski4vGl3DP8"
)
CHAT_ID = os.environ.get("CHAT_ID", "-5426058105")

TIMEFRAMES = {
    "5m": "5min",
    "15m": "15min",
    "1h": "1hr",
    "4h": "4hr",
    "1d": "day",
}

BB_PERIOD = 20
BB_STD = 2
PENETRATION_PCT = 1.0
# برای بیت‌کوین و طلا نفوذ کوچک‌تر هم سیگنال بدهد (گپ طلای ۱۶:۳۰ با ۰.۴٪ رد شده بود)
PENETRATION_PCT_MAJOR = 0.25
# حداقل فاصلهٔ open تا UB (٪) — گپ‌های نوکی مثل Harmony ~0.2٪ حذف می‌شوند
MIN_OPEN_GAP_PCT = 0.5
# هشدار احتمال گپ: چند دقیقه قبل از باز شدن کندل (ثانیه)
PRE_ALERT_BEFORE = {
    "5m": 2 * 60,    # ۲ دقیقه قبل
    "15m": 3 * 60,   # ۳ دقیقه قبل
    "1h": 5 * 60,    # ۵ دقیقه قبل
    "4h": 20 * 60,   # ۲۰ دقیقه قبل
}
PRE_ALERT_MIN_PCT = 1.0  # فقط اگر اختلاف فیوچرز تا UB بالای ۱٪ باشد
TOP_GAINERS = 30
TOP_VOLUME = 50
# اسکن درخواستی: کمتر نماد = خیلی سریع‌تر
ONDEMAND_TOP_GAINERS = 18
ONDEMAND_TOP_VOLUME = 15
ONDEMAND_FETCH_CONCURRENCY = 18
SEND_CHART = True
ONLY_SELL = True
# hybrid = اتومات گروه + دکمه پیوی | ondemand | auto
BOT_MODE = os.environ.get("BOT_MODE", "hybrid").strip().lower()
ALWAYS_INCLUDE = ["BTCUSDT", "XAUTUSDT"]
# نمادهایی که NO_KLINE شدند تا این مدت دوباره چک نشوند (ثانیه) — ۲۴ ساعت
BLACKLIST_TTL_SEC = 24 * 3600

# ——— آماده‌باش بستن زاویه (جدا از گپ) ———
ANGLE_ENABLED = True
ANGLE_TFS = ["5m", "15m", "1h", "4h"]
ANGLE_EMA = (5, 10, 20)           # همان پیش‌فرض ال‌بانک
ANGLE_MIN_DIST_E10 = 1.8          # حداقل ٪ بالای EMA10
ANGLE_MIN_DIST_E20 = 2.8
ANGLE_MAX_DIST_E10 = 8.0
ANGLE_MAX_FROM_HIGH = 3.5         # حداکثر فاصله از سقف اخیر
ANGLE_MIN_SPREAD = 1.5            # ٪ فاصله EMA5−EMA20
ANGLE_DAILY_MAX = 25              # سقف آلارم زاویه در روز (اتومات گروه)
ANGLE_LOOKBACK_PUMP = 14          # کندل برای پامپ نزدیک UB

OPEN_WINDOW_SEC = {
    "5m": 120,
    "15m": 150,
    "1h": 180,
    "4h": 180,
    "1d": 600,
}

PERIOD_SEC = {
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1d": 24 * 60 * 60,
}

TF_ORDER = {"5m": 0, "15m": 1, "1h": 2, "4h": 3, "1d": 4}

FUTURES_TICKERS_URL = (
    "https://lbkperp.lbank.com/cfd/openApi/v1/pub/marketData?productGroup=SwapU"
)
SPOT_WS_URL = "wss://api.lbank.info/ws/V2/"

# fallback intervals for Gate / BingX when LBank spot has no pair
GATE_INTERVAL = {
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}
BINGX_INTERVAL = {
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}

sent_signals: set = set()
sent_pre_alerts: set = set()
sent_angle_alerts: set = set()
angle_alert_day: str | None = None
angle_alert_count_today: int = 0
pending_signals: list = []
last_daily_report_day = None
last_known_version: str | None = None
futures_last_map: dict = {}
# symbol -> unix time که بلک‌لیست شده
kline_blacklist: dict[str, int] = {}
# user_id -> {"step": ..., "symbol": ..., "mode": ...}  (جدا per نفر)
user_sessions: dict[str, dict] = {}
# حداکثر اسکن همزمان (چند نفر با هم)
scan_semaphore = asyncio.Semaphore(3)

daily = {"day": None, "by_tf": {}}

stats = {
    "checked": 0,
    "signal_ok": 0,
    "skip_age": 0,
    "skip_no_gap": 0,
    "skip_dup": 0,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bot")


def iran_now():
    return datetime.now(TEHRAN)


def iran_today() -> str:
    return iran_now().strftime("%Y-%m-%d")


def ensure_daily() -> dict:
    today = iran_today()
    if daily["day"] != today:
        daily["day"] = today
        daily["by_tf"] = {}
    return daily["by_tf"]


def tf_bucket(tf: str) -> dict:
    by = ensure_daily()
    if tf not in by:
        by[tf] = {"n": 0, "gap_win": 0}
    return by[tf]


def prune_blacklist() -> None:
    now = int(time.time())
    dead = [s for s, ts in kline_blacklist.items() if now - int(ts) >= BLACKLIST_TTL_SEC]
    for s in dead:
        del kline_blacklist[s]


def is_blacklisted(symbol: str) -> bool:
    if symbol in ALWAYS_INCLUDE:
        return False
    ts = kline_blacklist.get(symbol)
    if ts is None:
        return False
    if int(time.time()) - int(ts) >= BLACKLIST_TTL_SEC:
        kline_blacklist.pop(symbol, None)
        return False
    return True


def add_to_blacklist(symbol: str, reason: str = "NO_KLINE") -> None:
    if symbol in ALWAYS_INCLUDE:
        return
    if symbol in kline_blacklist:
        return
    kline_blacklist[symbol] = int(time.time())
    log.info("BLACKLIST +%s (%s) | total=%d", symbol, reason, len(kline_blacklist))


def save_state() -> None:
    try:
        prune_blacklist()
        payload = {
            "daily": daily,
            "last_daily_report_day": last_daily_report_day,
            "pending": pending_signals[-500:],
            "kline_blacklist": kline_blacklist,
            "last_known_version": BOT_VERSION,
            "sent_angle_alerts": list(sent_angle_alerts)[-400:],
            "angle_alert_day": angle_alert_day,
            "angle_alert_count_today": angle_alert_count_today,
        }
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning("save_state error: %s", e)


def load_state() -> None:
    global last_daily_report_day, last_known_version
    global angle_alert_day, angle_alert_count_today
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
        daily.update(payload.get("daily") or {})
        last_daily_report_day = payload.get("last_daily_report_day")
        last_known_version = payload.get("last_known_version")
        pending_signals[:] = payload.get("pending") or []
        bl = payload.get("kline_blacklist") or {}
        kline_blacklist.clear()
        for s, ts in bl.items():
            try:
                kline_blacklist[str(s)] = int(ts)
            except Exception:
                pass
        sent_angle_alerts.clear()
        for k in payload.get("sent_angle_alerts") or []:
            try:
                sent_angle_alerts.add(tuple(k) if isinstance(k, list) else k)
            except Exception:
                pass
        angle_alert_day = payload.get("angle_alert_day")
        try:
            angle_alert_count_today = int(payload.get("angle_alert_count_today") or 0)
        except Exception:
            angle_alert_count_today = 0
        prune_blacklist()
        log.info(
            "State loaded from %s | blacklist=%d | prev_ver=%s",
            STATE_FILE, len(kline_blacklist), last_known_version,
        )
    except Exception as e:
        log.warning("load_state error: %s", e)


def format_startup_message() -> str:
    """پیام روشن شدن؛ اگر نسخه عوض شده باشد changelog همان نسخه را هم می‌فرستد."""
    global last_known_version
    prev = last_known_version
    is_update = prev is not None and prev != BOT_VERSION
    lines = [f"✅ ربات روشن شد — <b>v{BOT_VERSION}</b>"]
    if is_update:
        lines.append(f"📦 اپدیت از <code>v{prev}</code> → <code>v{BOT_VERSION}</code>")
        lines.append("━━━━━━━━━━━━━━━━")
        lines.append(f"<b>تغییرات v{BOT_VERSION}:</b>")
        for item in CHANGELOG.get(BOT_VERSION) or ["—"]:
            lines.append(f"• {item}")
    elif prev is None:
        # اولین اجرا یا state بدون نسخه
        lines.append("━━━━━━━━━━━━━━━━")
        lines.append(f"<b>نسخه فعلی v{BOT_VERSION}:</b>")
        for item in CHANGELOG.get(BOT_VERSION) or ["—"]:
            lines.append(f"• {item}")
    return "\n".join(lines)


def timeframes_to_check_now() -> list:
    now = int(time.time())
    result = []
    for tf, period in PERIOD_SEC.items():
        candle_open = (now // period) * period
        age = now - candle_open
        window = OPEN_WINDOW_SEC.get(tf, 60)
        if 0 <= age <= window:
            result.append(tf)
    result.sort(key=lambda x: TF_ORDER.get(x, 99))
    return result


def seconds_until_next_candle() -> int:
    """کمینهٔ زمان تا باز شدن کندل بعدی یا شروع پنجرهٔ هشدار احتمال گپ."""
    now = int(time.time())
    waits = [((now // p) + 1) * p - now for p in PERIOD_SEC.values()]
    for tf, before in PRE_ALERT_BEFORE.items():
        p = PERIOD_SEC.get(tf)
        if not p:
            continue
        open_ts = (now // p) * p
        pre_start = open_ts + p - before
        if now < pre_start:
            waits.append(pre_start - now)
        elif now < open_ts + p:
            waits.append(min(25, open_ts + p - now))
    return max(1, min(waits))


def pre_alert_tfs_now() -> list[str]:
    """تایم‌فریم‌هایی که الان داخل پنجرهٔ هشدار احتمال گپ هستند."""
    now = int(time.time())
    out = []
    for tf, before in PRE_ALERT_BEFORE.items():
        p = PERIOD_SEC.get(tf)
        if not p:
            continue
        age = now - (now // p) * p
        if age >= (p - before):
            out.append(tf)
    out.sort(key=lambda x: TF_ORDER.get(x, 99))
    return out


def send_telegram_text(
    text: str,
    reply_markup: dict | None = None,
    chat_id: str | int | None = None,
) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id if chat_id is not None else CHAT_ID,
        "text": text[:4000],
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code != 200:
            log.warning("Telegram text error: %s", r.text[:200])
            return False
        return True
    except Exception as e:
        log.warning("Telegram text exception: %s", e)
        return False


def edit_telegram_message(
    chat_id: str | int,
    message_id: int,
    text: str,
    reply_markup: dict | None = None,
) -> bool:
    """همان پیام را عوض می‌کند — بدون پیام جدید."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text[:4000],
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code != 200:
            # اگر محتوا یکی باشد تلگرام خطا می‌دهد — بی‌ضرر
            err = r.text[:200]
            if "message is not modified" in err:
                return True
            log.warning("Telegram edit error: %s", err)
            # fallback: پیام جدید
            return send_telegram_text(text, reply_markup=reply_markup, chat_id=chat_id)
        return True
    except Exception as e:
        log.warning("Telegram edit exception: %s", e)
        return send_telegram_text(text, reply_markup=reply_markup, chat_id=chat_id)


def ui_reply(
    text: str,
    chat_id: str | int,
    message_id: int | None = None,
    reply_markup: dict | None = None,
) -> bool:
    if message_id is not None:
        return edit_telegram_message(chat_id, message_id, text, reply_markup)
    return send_telegram_text(text, reply_markup=reply_markup, chat_id=chat_id)


def send_telegram_photo(
    image_bytes: bytes,
    caption: str,
    reply_markup: dict | None = None,
    chat_id: str | int | None = None,
) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    files = {"photo": ("chart.png", image_bytes, "image/png")}
    data = {
        "chat_id": chat_id if chat_id is not None else CHAT_ID,
        "caption": caption,
        "parse_mode": "HTML",
    }
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    try:
        r = requests.post(url, data=data, files=files, timeout=30)
        if r.status_code != 200:
            log.warning("Telegram photo error: %s", r.text[:200])
            return False
        return True
    except Exception as e:
        log.warning("Telegram photo exception: %s", e)
        return False


def answer_callback_query(callback_id: str, text: str = "", show_alert: bool = False) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery"
    try:
        requests.post(
            url,
            json={
                "callback_query_id": callback_id,
                "text": text,
                "show_alert": show_alert,
            },
            timeout=10,
        )
    except Exception as e:
        log.warning("answerCallbackQuery: %s", e)


def liq_button_markup(symbol: str, tf: str) -> dict:
    # callback_data max 64 bytes
    data = f"liq:{symbol}:{tf}"
    return {
        "inline_keyboard": [
            [{"text": "📐 محاسبه سقف و کف", "callback_data": data}]
        ]
    }


def refresh_futures_map() -> dict:
    global futures_last_map
    try:
        r = requests.get(FUTURES_TICKERS_URL, timeout=15)
        data = r.json().get("data") or []
    except Exception as e:
        log.error("Futures tickers error: %s", e)
        return futures_last_map

    out = {}
    for item in data:
        sym = item.get("symbol") or ""
        if not sym.endswith("USDT"):
            continue
        try:
            last = float(item.get("lastPrice") or 0)
            open_p = float(item.get("openPrice") or 0)
            if last <= 0:
                continue
            vol = float(item.get("turnover") or item.get("volume") or 0)
            out[sym] = {"last": last, "open": open_p, "vol": vol}
        except (TypeError, ValueError):
            continue
    if out:
        futures_last_map = out
    return futures_last_map


def get_top_movers(n: int = TOP_GAINERS, vol_n: int | None = None) -> list:
    fmap = refresh_futures_map()
    rows = []
    for sym, rec in fmap.items():
        open_p = rec.get("open") or 0
        last = rec.get("last") or 0
        if open_p <= 0 or last <= 0:
            continue
        change = (last - open_p) / open_p * 100.0
        rows.append({
            "symbol": sym,
            "change": change,
            "vol": rec.get("vol") or 0,
        })

    if not rows:
        return list(ALWAYS_INCLUDE)

    df = pd.DataFrame(rows)
    vn = TOP_VOLUME if vol_n is None else vol_n
    gainers = df.nlargest(n, "change")["symbol"].tolist()
    volumes = df.nlargest(vn, "vol")["symbol"].tolist()
    symbols = list(dict.fromkeys(gainers + volumes + ALWAYS_INCLUDE))
    before = len(symbols)
    symbols = [s for s in symbols if not is_blacklisted(s)]
    skipped = before - len(symbols)
    log.info(
        "Movers: %d pump + %d volume + forced → %d unique (blacklist skip=%d, bl_size=%d)",
        len(gainers), len(volumes), len(symbols), skipped, len(kline_blacklist),
    )
    return symbols


def get_futures_last(symbol: str):
    rec = futures_last_map.get(symbol)
    if rec and rec.get("last"):
        return float(rec["last"])
    try:
        r = requests.get(FUTURES_TICKERS_URL, timeout=10)
        data = r.json().get("data") or []
        for item in data:
            if item.get("symbol") == symbol:
                val = float(item.get("lastPrice") or 0)
                if val > 0:
                    futures_last_map[symbol] = {
                        "last": val,
                        "open": float(item.get("openPrice") or 0),
                    }
                    return val
    except Exception as e:
        log.warning("futures last error %s: %s", symbol, e)
    return None


def futures_to_spot_pair(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"):
        return f"{s[:-4].lower()}_usdt"
    return symbol.lower()


def calc_bb(closes: np.ndarray, period: int = BB_PERIOD, std_mult: float = BB_STD):
    s = pd.Series(closes)
    sma = s.rolling(period).mean()
    std = s.rolling(period).std(ddof=1)
    return sma, sma + std_mult * std, sma - std_mult * std


def make_chart(df: pd.DataFrame, symbol: str, tf: str, signal: str, sig: dict | None = None):
    """White chart: candles + BB + Prev High + UB + Gap% (no trendlines)."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
    except ImportError:
        return None
    try:
        show = 48
        plot = df.tail(show).reset_index(drop=True)
        n = len(plot)
        if n < 8:
            return None
        o = plot["o"].astype(float).values
        h = plot["h"].astype(float).values
        l = plot["l"].astype(float).values
        c = plot["c"].astype(float).values
        upper = plot["upper"].astype(float).values
        lower = plot["lower"].astype(float).values
        sma = plot["sma"].astype(float).values

        prev_high = float(h[-2]) if n >= 2 else float(h[-1])
        ub = float(upper[-1])
        entry = None
        if sig:
            entry = sig.get("fut_last") or sig.get("open")
        if not entry:
            entry = float(o[-1])
        gap_pct = (float(entry) - ub) / float(entry) * 100.0 if entry else 0.0

        ymin = float(min(np.nanmin(l), np.nanmin(lower)))
        ymax = float(max(np.nanmax(h), np.nanmax(upper)))
        pad = (ymax - ymin) * 0.04
        ymin, ymax = ymin - pad, ymax + pad

        fig = plt.figure(figsize=(10.2, 5.0), dpi=140, facecolor="#ffffff")
        ax = fig.add_axes([0.07, 0.10, 0.90, 0.78])
        ax.set_facecolor("#ffffff")
        for sp in ax.spines.values():
            sp.set_color("#cccccc")
        ax.tick_params(colors="#444444", labelsize=8)
        ax.grid(True, color="#eeeeee", lw=0.65)

        x = list(range(n))
        ax.plot(x, upper, color="#e53935", lw=1.05, alpha=0.85)
        ax.plot(x, lower, color="#43a047", lw=0.95, alpha=0.8)
        ax.plot(x, sma, color="#f9a825", lw=0.8, alpha=0.75)

        width = 0.30
        for i in range(n):
            col = "#26a69a" if c[i] >= o[i] else "#ef5350"
            ax.plot([i, i], [l[i], h[i]], color=col, lw=0.95, zorder=3)
            bot = min(o[i], c[i])
            ht = max(abs(c[i] - o[i]), (ymax - ymin) * 0.001)
            ax.add_patch(
                Rectangle((i - width / 2, bot), width, ht, facecolor=col, edgecolor=col, lw=0, zorder=3)
            )

        ax.axhline(prev_high, color="#7e57c2", lw=1.0, ls="--", alpha=0.9)
        ax.text(
            n * 0.28, prev_high, f"  Prev High  {prev_high:.5g}",
            color="#5e35b1", fontsize=10, fontweight="bold", va="bottom",
            bbox=dict(boxstyle="round,pad=0.25", fc="#f3e5f5", ec="none", alpha=0.92), zorder=6,
        )
        ax.axhline(ub, color="#c62828", lw=1.15, ls=":")
        ax.text(
            n * 0.28, ub, f"  UB  {ub:.5g}",
            color="#c62828", fontsize=10, fontweight="bold", va="top",
            bbox=dict(boxstyle="round,pad=0.25", fc="#ffebee", ec="none", alpha=0.92), zorder=6,
        )

        ax.annotate(
            f"Gap {gap_pct:+.2f}%",
            xy=(n - 1, float(entry)),
            xytext=(n - 11, float(entry) + (ymax - ymin) * 0.05),
            fontsize=11, fontweight="bold",
            color="#c62828" if gap_pct > 0 else "#2e7d32",
            bbox=dict(boxstyle="round,pad=0.35", fc="#fff8e1", ec="#ffcc80", alpha=0.95),
            arrowprops=dict(arrowstyle="->", color="#ff9800", lw=1.2),
            zorder=7,
        )

        side = (signal or "").upper()
        price_col = "#e53935" if side == "SELL" else "#43a047"
        fig.text(0.07, 0.94, f"{symbol}", color="#212121", fontsize=13, fontweight="bold")
        fig.text(0.07, 0.905, f"{tf}  ·  {side}", color="#757575", fontsize=9)
        fig.text(0.96, 0.94, f"{float(entry):.6g}", color=price_col, fontsize=13, fontweight="bold", ha="right")

        ax.set_xlim(-0.8, n + 6)
        ax.set_ylim(ymin, ymax)

        buf = BytesIO()
        fig.savefig(buf, format="png", facecolor=fig.get_facecolor(), edgecolor="none")
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        log.warning("Chart error: %s", e)
        return None


# ========================= LIQUIDITY CHARTS (4 methods) =========================

def _swing_idxs(vals, kind: str, left: int = 2, right: int = 2):
    out = []
    n = len(vals)
    for i in range(left, n - right):
        w = vals[i - left : i + right + 1]
        if kind == "high" and vals[i] == max(w):
            out.append(i)
        if kind == "low" and vals[i] == min(w):
            out.append(i)
    return out


def _draw_candles_ax(ax, plot, ymin, ymax):
    from matplotlib.patches import Rectangle
    n = len(plot)
    o, h, l, c = plot["o"].values, plot["h"].values, plot["l"].values, plot["c"].values
    width = 0.32
    for i in range(n):
        col = "#26a69a" if c[i] >= o[i] else "#ef5350"
        ax.plot([i, i], [l[i], h[i]], color=col, lw=0.9, zorder=3)
        bot = min(o[i], c[i])
        ht = max(abs(c[i] - o[i]), (ymax - ymin) * 0.001)
        ax.add_patch(
            Rectangle((i - width / 2, bot), width, ht, facecolor=col, edgecolor=col, lw=0, zorder=3)
        )
    return n


def _style_white_ax(ax):
    ax.set_facecolor("#ffffff")
    for sp in ax.spines.values():
        sp.set_color("#cccccc")
    ax.tick_params(colors="#444444", labelsize=8)
    ax.grid(True, color="#eeeeee", lw=0.65)


def _fig_to_bytes(fig) -> bytes:
    buf = BytesIO()
    fig.savefig(buf, format="png", facecolor="#ffffff", bbox_inches="tight")
    import matplotlib.pyplot as plt
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def make_liquidity_charts(df: pd.DataFrame, symbol: str, tf: str) -> list[tuple[str, bytes]]:
    """Return list of (caption, png_bytes) for 4 liquidity methods."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    if df is None or len(df) < 30:
        return []

    work = df.tail(168).copy().reset_index(drop=True)
    if "v" not in work.columns:
        work["v"] = 1.0
    work["v"] = work["v"].astype(float).fillna(1.0)
    if "ts" in work.columns:
        work["dt"] = pd.to_datetime(work["ts"], unit="s", utc=True)
    elif "dt" not in work.columns:
        work["dt"] = pd.Timestamp.utcnow()

    show = min(60, len(work))
    plot = work.tail(show).reset_index(drop=True)
    off = len(work) - show
    px = float(work.iloc[-1]["c"])
    ymin0 = float(plot["l"].min())
    ymax0 = float(plot["h"].max())
    pad = (ymax0 - ymin0) * 0.06 or 1.0
    ymin0, ymax0 = ymin0 - pad, ymax0 + pad
    tol = px * 0.0008

    h = work["h"].astype(float).values
    l = work["l"].astype(float).values
    c = work["c"].astype(float).values
    o = work["o"].astype(float).values
    nfull = len(work)
    results: list[tuple[str, bytes]] = []

    # ----- 1) Equal H/L -----
    try:
        sh = _swing_idxs(h, "high")
        sl = _swing_idxs(l, "low")

        def cluster(idxs, vals):
            if not idxs:
                return []
            items = sorted([(i, float(vals[i])) for i in idxs], key=lambda x: x[1])
            clusters = []
            cur = {"idxs": [items[0][0]], "prices": [items[0][1]]}
            for i, p in items[1:]:
                if abs(p - float(np.mean(cur["prices"]))) <= tol:
                    cur["idxs"].append(i)
                    cur["prices"].append(p)
                else:
                    clusters.append(cur)
                    cur = {"idxs": [i], "prices": [p]}
            clusters.append(cur)
            out = []
            for cl in clusters:
                price = float(np.median(cl["prices"]))
                count = len(cl["idxs"])
                score = count * 10 + (25 if count >= 2 else 0)
                out.append({"price": price, "count": count, "score": score, "idxs": cl["idxs"]})
            return sorted(out, key=lambda x: -x["score"])

        hi_lv = [x for x in cluster(sh, h) if abs(x["price"] - px) / px <= 0.03][:3]
        lo_lv = [x for x in cluster(sl, l) if abs(x["price"] - px) / px <= 0.03][:3]

        fig, ax = plt.subplots(figsize=(10.2, 4.8), dpi=130, facecolor="#fff")
        _style_white_ax(ax)
        nn = _draw_candles_ax(ax, plot, ymin0, ymax0)
        for x in hi_lv:
            ax.axhline(x["price"], color="#e65100", lw=1.3)
            ax.text(
                1, x["price"], f"  EQH {x['price']:.5g} ({x['count']}x)",
                color="#e65100", fontsize=9, fontweight="bold", va="bottom",
                bbox=dict(boxstyle="round,pad=0.2", fc="#fff3e0", ec="none"),
            )
        for x in lo_lv:
            ax.axhline(x["price"], color="#1565c0", lw=1.3)
            ax.text(
                1, x["price"], f"  EQL {x['price']:.5g} ({x['count']}x)",
                color="#1565c0", fontsize=9, fontweight="bold", va="top",
                bbox=dict(boxstyle="round,pad=0.2", fc="#e3f2fd", ec="none"),
            )
        ax.set_xlim(-0.8, nn + 3)
        ax.set_ylim(ymin0, ymax0)
        fig.suptitle(f"1) Equal High/Low  ·  {symbol} {tf}", fontsize=12, fontweight="bold", x=0.08, ha="left")
        results.append((f"1️⃣ Equal H/L — <b>{symbol}</b> {tf}", _fig_to_bytes(fig)))
    except Exception as e:
        log.warning("liq method1: %s", e)

    # ----- 2) Session + PDH/PDL -----
    try:
        w2 = work.copy()
        w2["hour"] = pd.to_datetime(w2["dt"]).dt.hour
        w2["day"] = pd.to_datetime(w2["dt"]).dt.floor("D")
        days = sorted(w2["day"].unique())
        prev_day = days[-2] if len(days) >= 2 else days[-1]
        pdf = w2[w2["day"] == prev_day]
        pdh = float(pdf["h"].max()) if len(pdf) else px
        pdl = float(pdf["l"].min()) if len(pdf) else px
        recent = w2.tail(72)
        levels = [("PDH", pdh, "#e65100"), ("PDL", pdl, "#1565c0")]
        for name, hours, hc, lc in [
            ("Asia", range(0, 8), "#ff8f00", "#0277bd"),
            ("Lon", range(7, 16), "#ef6c00", "#0288d1"),
            ("NY", range(13, 22), "#d84315", "#01579b"),
        ]:
            part = recent[recent["hour"].isin(list(hours))]
            if len(part):
                levels.append((f"{name} H", float(part["h"].max()), hc))
                levels.append((f"{name} L", float(part["l"].min()), lc))

        fig, ax = plt.subplots(figsize=(10.2, 4.8), dpi=130, facecolor="#fff")
        _style_white_ax(ax)
        nn = _draw_candles_ax(ax, plot, ymin0, ymax0)
        for name, price, col in levels:
            if price < ymin0 - pad or price > ymax0 + pad:
                continue
            ax.axhline(price, color=col, lw=1.2, ls="--" if name.startswith("PD") else "-")
            ax.text(
                1, price, f"  {name} {price:.5g}",
                color=col, fontsize=8, fontweight="bold", va="bottom",
                bbox=dict(boxstyle="round,pad=0.2", fc="#fafafa", ec="none", alpha=0.9),
            )
        ax.set_xlim(-0.8, nn + 3)
        ax.set_ylim(ymin0, ymax0)
        fig.suptitle(f"2) Session + PDH/PDL  ·  {symbol} {tf}", fontsize=12, fontweight="bold", x=0.08, ha="left")
        results.append((f"2️⃣ Session/PDH — <b>{symbol}</b> {tf}", _fig_to_bytes(fig)))
    except Exception as e:
        log.warning("liq method2: %s", e)

    # ----- 3) Volume Profile -----
    try:
        prof = work.tail(72)
        bins = 40
        lo_p, hi_p = float(prof["l"].min()), float(prof["h"].max())
        edges = np.linspace(lo_p, hi_p, bins + 1)
        vol_at = np.zeros(bins)
        for _, row in prof.iterrows():
            i0 = int(np.searchsorted(edges, row["l"], side="right") - 1)
            i1 = int(np.searchsorted(edges, row["h"], side="right") - 1)
            i0 = max(0, min(bins - 1, i0))
            i1 = max(0, min(bins - 1, i1))
            if i1 < i0:
                i0, i1 = i1, i0
            span = i1 - i0 + 1
            for bi in range(i0, i1 + 1):
                vol_at[bi] += float(row["v"]) / span
        centers = (edges[:-1] + edges[1:]) / 2
        hvn_idx = []
        for bi in np.argsort(vol_at)[::-1]:
            if any(abs(centers[bi] - centers[j]) < (hi_p - lo_p) * 0.015 for j in hvn_idx):
                continue
            hvn_idx.append(int(bi))
            if len(hvn_idx) >= 3:
                break
        lvn_idx = []
        for bi in np.argsort(vol_at):
            if vol_at[bi] <= 0:
                continue
            if abs(centers[bi] - px) / px > 0.025:
                continue
            if any(abs(centers[bi] - centers[j]) < (hi_p - lo_p) * 0.02 for j in lvn_idx + hvn_idx):
                continue
            lvn_idx.append(int(bi))
            if len(lvn_idx) >= 2:
                break

        fig, ax = plt.subplots(figsize=(10.2, 4.8), dpi=130, facecolor="#fff")
        _style_white_ax(ax)
        nn = _draw_candles_ax(ax, plot, ymin0, ymax0)
        vmax = float(vol_at.max()) or 1.0
        for bi in range(bins):
            if centers[bi] < ymin0 or centers[bi] > ymax0:
                continue
            w = 8 * (vol_at[bi] / vmax)
            ax.barh(
                centers[bi], w, height=(hi_p - lo_p) / bins * 0.85,
                left=nn + 0.5, color="#90a4ae", alpha=0.45, zorder=1,
            )
        for bi in hvn_idx:
            ax.axhline(centers[bi], color="#6a1b9a", lw=1.4)
            ax.text(
                1, centers[bi], f"  HVN {centers[bi]:.5g}",
                color="#6a1b9a", fontsize=9, fontweight="bold", va="bottom",
                bbox=dict(boxstyle="round,pad=0.2", fc="#f3e5f5", ec="none"),
            )
        for bi in lvn_idx:
            ax.axhline(centers[bi], color="#00838f", lw=1.2, ls=":")
            ax.text(
                1, centers[bi], f"  LVN {centers[bi]:.5g}",
                color="#00838f", fontsize=9, fontweight="bold", va="top",
                bbox=dict(boxstyle="round,pad=0.2", fc="#e0f7fa", ec="none"),
            )
        ax.set_xlim(-0.8, nn + 10)
        ax.set_ylim(ymin0, ymax0)
        fig.suptitle(f"3) Volume Profile  ·  {symbol} {tf}", fontsize=12, fontweight="bold", x=0.08, ha="left")
        results.append((f"3️⃣ Volume Profile — <b>{symbol}</b> {tf}", _fig_to_bytes(fig)))
    except Exception as e:
        log.warning("liq method3: %s", e)

    # ----- 4) Liquidity sweeps -----
    try:
        sh2 = _swing_idxs(h, "high", 3, 3)
        sl2 = _swing_idxs(l, "low", 3, 3)
        sweeps_h, sweeps_l = [], []
        for i in range(5, nfull - 1):
            prior_lows = [l[j] for j in sl2 if i - 20 <= j < i]
            prior_highs = [h[j] for j in sh2 if i - 20 <= j < i]
            if prior_lows:
                m = min(prior_lows)
                if l[i] < m and c[i] > m:
                    sweeps_l.append({"i": i, "price": float(l[i])})
            if prior_highs:
                m = max(prior_highs)
                if h[i] > m and c[i] < m:
                    sweeps_h.append({"i": i, "price": float(h[i])})

        def top_sweep(sweeps, k=3):
            out = []
            for s in reversed(sweeps):
                if abs(s["price"] - px) / px > 0.03:
                    continue
                if any(abs(s["price"] - t["price"]) <= tol * 2 for t in out):
                    continue
                out.append(s)
                if len(out) >= k:
                    break
            return out

        th, tl = top_sweep(sweeps_h), top_sweep(sweeps_l)
        fig, ax = plt.subplots(figsize=(10.2, 4.8), dpi=130, facecolor="#fff")
        _style_white_ax(ax)
        nn = _draw_candles_ax(ax, plot, ymin0, ymax0)
        for s in th:
            ax.axhline(s["price"], color="#c62828", lw=1.35)
            ax.text(
                1, s["price"], f"  Sweep High {s['price']:.5g}",
                color="#c62828", fontsize=9, fontweight="bold", va="bottom",
                bbox=dict(boxstyle="round,pad=0.2", fc="#ffebee", ec="none"),
            )
            pi = s["i"] - off
            if 0 <= pi < nn:
                ax.scatter([pi], [float(plot["h"].iloc[pi])], color="#c62828", s=50, zorder=5, marker="v")
        for s in tl:
            ax.axhline(s["price"], color="#2e7d32", lw=1.35)
            ax.text(
                1, s["price"], f"  Sweep Low {s['price']:.5g}",
                color="#2e7d32", fontsize=9, fontweight="bold", va="top",
                bbox=dict(boxstyle="round,pad=0.2", fc="#e8f5e9", ec="none"),
            )
            pi = s["i"] - off
            if 0 <= pi < nn:
                ax.scatter([pi], [float(plot["l"].iloc[pi])], color="#2e7d32", s=50, zorder=5, marker="^")
        ax.set_xlim(-0.8, nn + 3)
        ax.set_ylim(ymin0, ymax0)
        fig.suptitle(f"4) Liquidity Sweep  ·  {symbol} {tf}", fontsize=12, fontweight="bold", x=0.08, ha="left")
        results.append((f"4️⃣ Sweep — <b>{symbol}</b> {tf}", _fig_to_bytes(fig)))
    except Exception as e:
        log.warning("liq method4: %s", e)

    return results


async def handle_liq_request(
    symbol: str,
    tf: str,
    user_id: int | None = None,
    group_chat_id: str | int | None = None,
) -> None:
    """ارسال ۴ چارت سقف/کف به پیوی کاربر (نه گروه)."""
    log.info("LIQ request %s %s user=%s", symbol, tf, user_id)
    target = user_id if user_id is not None else CHAT_ID

    # تست دسترسی پیوی: کاربر باید حداقل یک‌بار /start زده باشد
    ok = send_telegram_text(
        f"⏳ در حال محاسبه سقف/کف برای <b>{symbol}</b> ({tf}) …",
        chat_id=target,
    )
    if not ok and user_id is not None:
        # در گروه توضیح بده
        if group_chat_id is not None:
            send_telegram_text(
                "⚠️ برای دریافت نمودارها در پیوی، اول ربات را باز کن و <b>/start</b> بزن، "
                "بعد دوباره روی دکمه کلیک کن.",
                chat_id=group_chat_id,
            )
        return

    df, src = await fetch_klines(symbol, tf, size=120)
    if df is None or len(df) < 30:
        send_telegram_text(f"❌ دادهٔ کافی برای {symbol} {tf} نبود", chat_id=target)
        return
    if "v" not in df.columns or float(df["v"].fillna(0).sum()) <= 0:
        g = await asyncio.to_thread(fetch_klines_gate, symbol, tf, 120)
        if g is not None and "v" in g.columns:
            df = g
            src = "gate"
    charts = await asyncio.to_thread(make_liquidity_charts, df, symbol, tf)
    if not charts:
        send_telegram_text("❌ ساخت نمودار ناموفق بود", chat_id=target)
        return
    for cap, img in charts:
        send_telegram_photo(img, cap, chat_id=target)
        await asyncio.sleep(0.4)
    log.info("LIQ sent %d charts for %s %s src=%s → user %s", len(charts), symbol, tf, src, target)


async def telegram_callback_loop() -> None:
    """Poll Telegram for inline button presses."""
    offset = None
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    while True:
        try:
            params = {"timeout": 25, "allowed_updates": json.dumps(["callback_query"])}
            if offset is not None:
                params["offset"] = offset
            r = await asyncio.to_thread(requests.get, url, params=params, timeout=35)
            data = r.json() if r.status_code == 200 else {}
            for upd in data.get("result") or []:
                offset = upd["update_id"] + 1
                cq = upd.get("callback_query")
                if not cq:
                    continue
                cb_id = cq.get("id")
                raw = (cq.get("data") or "").strip()
                user = cq.get("from") or {}
                user_id = user.get("id")
                msg = cq.get("message") or {}
                group_chat_id = (msg.get("chat") or {}).get("id")

                answer_callback_query(cb_id, "ارسال به پیوی شما…")
                if not raw.startswith("liq:"):
                    continue
                parts = raw.split(":")
                if len(parts) != 3:
                    continue
                _, symbol, tf = parts
                symbol = symbol.upper()
                if tf not in TIMEFRAMES:
                    if user_id:
                        send_telegram_text(f"تایم‌فریم نامعتبر: {tf}", chat_id=user_id)
                    continue
                try:
                    await handle_liq_request(
                        symbol, tf, user_id=user_id, group_chat_id=group_chat_id
                    )
                except Exception as e:
                    log.warning("liq handle error: %s", e)
                    if user_id:
                        send_telegram_text(f"❌ خطا در محاسبه: {e}", chat_id=user_id)
        except Exception as e:
            log.warning("telegram poll: %s", e)
            await asyncio.sleep(3)


def entry_quality_score(df: pd.DataFrame, side: str) -> dict:
    """امتیاز کیفیت ورود 0-100 (آلارم ریسک). فقط راهنماست، نه فیلتر اجباری."""
    try:
        if df is None or len(df) < 25:
            return {"score": 50, "flag": "🟡", "label": "داده ناکافی", "reasons": ["· داده کم برای امتیازدهی"]}
        cur = df.iloc[-1]
        prev = df.iloc[-2]
        o, h, l, c = float(cur["o"]), float(cur["h"]), float(cur["l"]), float(cur["c"])
        ub = float(cur["upper"])
        sma = float(cur["sma"])
        po, ph, pl, pc = float(prev["o"]), float(prev["h"]), float(prev["l"]), float(prev["c"])
        reasons: list[str] = []
        score = 50

        if side == "SELL":
            gap_pct = (o - ub) / o * 100.0 if o > 0 else 0.0
            ext = (o - sma) / sma * 100.0 if sma else 0.0
            # گپ بزرگ‌تر = امتیاز بیشتر (mean-reversion قوی‌تر روی تایم بالا)
            if gap_pct >= 0.5:
                score += 10
                reasons.append(f"+ گپ نسبت به UB: {gap_pct:.2f}%")
            if gap_pct >= 1.5:
                score += 8
                reasons.append("+ گپ نسبتاً بزرگ")
            if gap_pct >= 4.0:
                score += 5
                reasons.append("+ گپ خیلی بزرگ (تارگت اسکالپ واضح‌تر)")

            prev_range = max(ph - pl, 1e-12)
            prev_wick = (ph - max(po, pc)) / prev_range
            if prev_wick >= 0.45:
                score += 10
                reasons.append(f"+ سایه بالای کندل قبلی قوی ({prev_wick:.0%})")
            elif prev_wick >= 0.30:
                score += 5
                reasons.append(f"+ سایه بالای کندل قبلی متوسط ({prev_wick:.0%})")
            else:
                score -= 3
                reasons.append(f"- سایه رد شدن قبلی ضعیف ({prev_wick:.0%})")

            cur_range = max(h - l, 1e-12)
            cur_wick = (h - max(o, c)) / cur_range
            if c < o and cur_wick >= 0.35:
                score += 10
                reasons.append("+ کندل فعلی در حال رد شدن از سقف")
            elif c > o and (c - o) / cur_range > 0.6:
                score -= 6
                reasons.append("- بدنه صعودی قوی — مناسب اسکالپ نه نگه‌داشتن")

            hs = _swing_idxs(df["h"].astype(float).values, "high")
            highs = [float(df["h"].iloc[i]) for i in hs if i < len(df) - 1]
            ref = max(h, ph)
            near = [x for x in highs if abs(x - ref) / max(ref, 1e-12) <= 0.002]
            if len(near) >= 2:
                score += 15
                reasons.append(f"+ نزدیک Equal High / سقف نقدینگی ({len(near)} لمس)")
            else:
                above = [x for x in highs if x >= o * 0.998]
                if above:
                    nearest = min(above)
                    dist = (nearest - o) / o * 100.0
                    if dist <= 0.25:
                        score += 10
                        reasons.append(f"+ نزدیک سوئینگ‌های (فاصله {dist:.2f}%)")
                    elif dist <= 0.6:
                        score += 4
                        reasons.append(f"+ نسبتاً نزدیک سوئینگ‌های ({dist:.2f}%)")
                    else:
                        score -= 5
                        reasons.append(f"- سقف نقدینگی دور است ({dist:.2f}%)")
                else:
                    score -= 4
                    reasons.append("- سوئینگ‌های بالای سر پیدا نشد")

            # مومنتوم: جریمه ملایم — پامپ ≠ گپ پر نمی‌شود؛ فقط ریسک نگه‌داشتن
            recent = df["c"].astype(float).tail(7).values
            if len(recent) >= 7:
                slope = (recent[-1] - recent[0]) / recent[0] * 100.0
                if slope > 2.5:
                    score -= 6
                    reasons.append(f"- موج صعودی تند ({slope:.1f}%) — بعد از پر شدن ممکن است دوباره بپرد")
                elif slope > 1.0:
                    score -= 3
                    reasons.append(f"- موج صعودی نسبتاً تند ({slope:.1f}%)")
                elif slope < 0:
                    score += 6
                    reasons.append(f"+ موج اخیر ضعیف/منفی ({slope:.1f}%)")
                else:
                    reasons.append(f"· شیب اخیر ملایم ({slope:.1f}%)")

            greens = sum(
                1 for i in range(-6, -1)
                if float(df.iloc[i]["c"]) > float(df.iloc[i]["o"])
            )
            if greens >= 4:
                score -= 4
                reasons.append(f"- {greens} کندل سبز اخیر — ریسک ادامه بعد از اسکالپ")
            elif greens <= 1:
                score += 5
                reasons.append("+ سبزهای اخیر کم است")

            if ext >= 1.5:
                score += 6
                reasons.append(f"+ فاصله از میانگین: {ext:.2f}%")
            if ext >= 4:
                score += 2
                reasons.append("+ کشیدگی زیاد از میانگین (تارگت پر شدن جذاب‌تر)")
        else:
            # LONG — آینه ساده
            lb = float(cur["lower"])
            gap_pct = (lb - o) / o * 100.0 if o > 0 else 0.0
            if gap_pct >= 0.5:
                score += 10
                reasons.append(f"+ گپ زیر LB: {gap_pct:.2f}%")
            prev_range = max(ph - pl, 1e-12)
            prev_wick = (min(po, pc) - pl) / prev_range
            if prev_wick >= 0.45:
                score += 10
                reasons.append("+ سایه پایین کندل قبلی قوی")
            elif prev_wick < 0.3:
                score -= 3
                reasons.append("- سایه رد شدن کف قبلی ضعیف")

        score = int(max(0, min(100, score)))
        if score >= 70:
            flag, label = "🟢", "مناسب‌تر برای ورود (اسکالپ تا پر شدن گپ)"
        elif score >= 50:
            flag, label = "🟡", "مخلوط — سایز کوچک؛ بعد از پر شدن مراقب ادامه موج باش"
        else:
            flag, label = "🔴", "ریسک بالاتر — مناسب اسکالپ سریع، نه نگه‌داشتن (ممکن است بعد از پر شدن دوباره بپرد)"
        # حداکثر ۴ دلیل برای پیام تلگرام
        reasons = reasons[:6]
        return {"score": score, "flag": flag, "label": label, "reasons": reasons}
    except Exception as e:
        log.warning("entry_quality_score: %s", e)
        return {"score": 50, "flag": "🟡", "label": "خطا در امتیازدهی", "reasons": []}


def check_signal(df: pd.DataFrame, symbol: str, tf: str, on_demand: bool = False):
    if len(df) < BB_PERIOD + 2:
        return None
    cur = df.iloc[-1]
    open_p = float(cur["o"])
    high = float(cur["h"])
    low = float(cur["l"])
    close = float(cur["c"])
    upper = float(cur["upper"])
    lower = float(cur["lower"])
    sma = float(cur["sma"])
    if np.isnan(upper) or np.isnan(lower):
        return None

    stats["checked"] += 1

    try:
        candle_ts = int(cur["ts"])
    except Exception:
        candle_ts = int(pd.Timestamp(cur["dt"]).timestamp())

    age_sec = int(time.time()) - candle_ts
    # در حالت درخواستی: کل طول کندل جاری مجاز است (نه فقط چند دقیقه اول)
    if on_demand:
        max_age = PERIOD_SEC.get(tf, 3600)
    else:
        max_age = OPEN_WINDOW_SEC.get(tf, 60)
    if age_sec < 0 or age_sec > max_age:
        stats["skip_age"] += 1
        return None

    upper_pen = (high - upper) / upper * 100.0 if upper > 0 else 0.0
    lower_pen = (lower - low) / lower * 100.0 if lower > 0 else 0.0
    min_pen = PENETRATION_PCT_MAJOR if symbol in ALWAYS_INCLUDE else PENETRATION_PCT

    side = None
    if open_p > upper and upper_pen >= min_pen:
        open_gap_pct = (open_p - upper) / open_p * 100.0 if open_p > 0 else 0.0
        min_gap = 0.0 if symbol in ALWAYS_INCLUDE else MIN_OPEN_GAP_PCT
        if open_gap_pct < min_gap:
            stats["skip_no_gap"] += 1
            log.info(
                "SKIP_TINY_GAP %s %s SELL | open_gap=%.3f%% < min=%.2f%%",
                symbol, tf, open_gap_pct, min_gap,
            )
            return None
        if low < upper:
            stats["skip_no_gap"] += 1
            log.info("SKIP_FILLED %s %s SELL | low=%.6g < upper=%.6g", symbol, tf, low, upper)
            return None
        side = "SELL"
    elif (not ONLY_SELL) and open_p < lower and lower_pen >= min_pen:
        open_gap_pct = (lower - open_p) / open_p * 100.0 if open_p > 0 else 0.0
        min_gap = 0.0 if symbol in ALWAYS_INCLUDE else MIN_OPEN_GAP_PCT
        if open_gap_pct < min_gap:
            stats["skip_no_gap"] += 1
            log.info(
                "SKIP_TINY_GAP %s %s LONG | open_gap=%.3f%% < min=%.2f%%",
                symbol, tf, open_gap_pct, min_gap,
            )
            return None
        if high > lower:
            stats["skip_no_gap"] += 1
            log.info("SKIP_FILLED %s %s LONG | high=%.6g > lower=%.6g", symbol, tf, high, lower)
            return None
        side = "LONG"

    if not side:
        stats["skip_no_gap"] += 1
        return None

    fut = get_futures_last(symbol)
    if fut is None:
        stats["skip_no_gap"] += 1
        log.info("SKIP_NO_FUT %s %s", symbol, tf)
        return None

    if ONLY_SELL and side != "SELL":
        stats["skip_no_gap"] += 1
        return None

    if side == "SELL" and fut <= upper:
        stats["skip_no_gap"] += 1
        log.info("SKIP_FUT_SIDE %s %s SELL | fut=%.6g <= upper=%.6g", symbol, tf, fut, upper)
        return None
    if side == "LONG" and fut >= lower:
        stats["skip_no_gap"] += 1
        log.info("SKIP_FUT_SIDE %s %s LONG | fut=%.6g >= lower=%.6g", symbol, tf, fut, lower)
        return None

    key = (symbol, tf, candle_ts, side)
    if not on_demand:
        if key in sent_signals:
            stats["skip_dup"] += 1
            return None
        sent_signals.add(key)
        if len(sent_signals) > 5000:
            sent_signals.clear()

    stats["signal_ok"] += 1
    exit_price = upper if side == "SELL" else lower
    if side == "SELL":
        diff_pct = (fut - exit_price) / fut * 100.0 if fut else 0.0
    else:
        diff_pct = (exit_price - fut) / fut * 100.0 if fut else 0.0

    quality = entry_quality_score(df, side)
    log.info(
        "QUALITY %s %s %s score=%s %s",
        side, symbol, tf, quality.get("score"), quality.get("label"),
    )

    ladders = {}
    if side == "SELL":
        ladders = compute_sell_add_levels(df, float(fut), float(upper))

    return {
        "side": side,
        "symbol": symbol,
        "tf": tf,
        "open": open_p,
        "high": high,
        "low": low,
        "close": close,
        "upper": upper,
        "lower": lower,
        "sma": sma,
        "upper_pen": upper_pen,
        "lower_pen": lower_pen,
        "diff_pct": diff_pct,
        "fut_last": fut,
        "candle_ts": candle_ts,
        "age_sec": age_sec,
        "q_score": quality.get("score", 50),
        "q_flag": quality.get("flag", "🟡"),
        "q_label": quality.get("label", ""),
        "q_reasons": quality.get("reasons") or [],
        "add2": ladders.get("add2"),
        "add3": ladders.get("add3"),
        "add2_pct": ladders.get("add2_pct"),
        "add3_pct": ladders.get("add3_pct"),
    }


def lbank_futures_link(symbol: str, tf: str) -> str:
    """لینک فیوچرز ال‌بانک؛ interval برای لود تایم‌فریم روی وب."""
    sym = symbol.upper()
    interval_map = {"5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}
    interval = interval_map.get(tf, "15m")
    # universal/https — روی موبایل اگر اپ نصب باشد ممکن است پیشنهاد Open in App بدهد
    return f"https://www.lbank.com/futures/{sym.lower()}?interval={interval}"


def compute_sell_add_levels(df: pd.DataFrame, entry: float, upper: float) -> dict:
    """پله‌های اضافه سِل بالای ورود: سوئینگ + ATR (برای میانگین‌گیری پله‌ای)."""
    out = {
        "entry": entry,
        "target_ub": upper,
        "add2": None,
        "add3": None,
        "add2_pct": None,
        "add3_pct": None,
    }
    if entry is None or entry <= 0 or df is None or len(df) < 25:
        return out
    try:
        highs = df["h"].astype(float).values
        lows = df["l"].astype(float).values
        closes = df["c"].astype(float).values
        # بدون کندل جاری
        h_hist = highs[:-1]
        l_hist = lows[:-1]
        c_hist = closes[:-1]
        if len(h_hist) < 20:
            return out

        swing20 = float(np.nanmax(h_hist[-20:]))
        swing50 = float(np.nanmax(h_hist[-min(50, len(h_hist)):]))
        # ATR تقریبی ۱۴
        prev_c = np.roll(c_hist, 1)
        prev_c[0] = c_hist[0]
        tr = np.maximum(
            h_hist - l_hist,
            np.maximum(np.abs(h_hist - prev_c), np.abs(l_hist - prev_c)),
        )
        atr = float(np.nanmean(tr[-14:])) if len(tr) >= 14 else float(np.nanmean(tr))
        if not np.isfinite(atr) or atr <= 0:
            atr = entry * 0.01

        # پله۲: اولین سطح معنی‌دار بالای ورود
        candidates2 = [swing20, entry + atr, entry * 1.01]
        above2 = [c for c in candidates2 if np.isfinite(c) and c > entry * 1.003]
        add2 = min(above2) if above2 else (entry + max(atr, entry * 0.005))

        # پله۳: سقف قوی‌تر
        candidates3 = [swing50, entry + 2 * atr, add2 + atr, entry * 1.02]
        above3 = [c for c in candidates3 if np.isfinite(c) and c > add2 * 1.003]
        add3 = max(above3) if above3 else (add2 + max(atr, entry * 0.005))

        if (add2 - entry) / entry < 0.003:
            add2 = entry + max(atr, entry * 0.005)
        if add3 <= add2:
            add3 = add2 + max(atr, entry * 0.005)

        out["add2"] = float(add2)
        out["add3"] = float(add3)
        out["add2_pct"] = (add2 - entry) / entry * 100.0
        out["add3_pct"] = (add3 - entry) / entry * 100.0
    except Exception as e:
        log.warning("compute_sell_add_levels: %s", e)
    return out


def format_signal_message(sig: dict) -> str:
    emoji = "🔴" if sig["side"] == "SELL" else "🟢"
    side_fa = "سِل" if sig["side"] == "SELL" else "لانگ"
    pen = sig.get("diff_pct")
    if pen is None:
        pen = sig["upper_pen"] if sig["side"] == "SELL" else sig["lower_pen"]
    symbol = sig["symbol"]
    link = lbank_futures_link(symbol, sig["tf"])
    q_score = sig.get("q_score", 50)
    lines = [
        f"{emoji} <b>{side_fa}</b> · "
        f'<a href="{link}"><b>{symbol}</b></a>',
        f"⏱ {sig['tf']} · 📏 {pen:.2f}% · Q{q_score}",
    ]
    entry = sig.get("fut_last") or sig.get("open")
    if sig.get("side") == "SELL" and entry:
        a2, a3 = sig.get("add2"), sig.get("add3")
        ub = sig.get("upper")
        lines.append(f"① ورود: <code>{entry:.6g}</code>")
        if a2:
            lines.append(
                f"② پله۲: <code>{a2:.6g}</code> (+{sig.get('add2_pct', 0):.2f}%)"
            )
        if a3:
            lines.append(
                f"③ پله۳: <code>{a3:.6g}</code> (+{sig.get('add3_pct', 0):.2f}%)"
            )
        if ub:
            lines.append(f"🎯 برگشت گپ UB: <code>{ub:.6g}</code>")
    return "\n".join(lines)


def score_signal(item: dict, close: float, high: float, low: float) -> dict:
    spot_open = float(item.get("open") or 0)
    fut_entry = float(item.get("fut_last") or 0)
    ratio = (fut_entry / spot_open) if spot_open > 0 and fut_entry > 0 else 1.0
    fut_low = low * ratio
    fut_high = high * ratio
    fut_now = get_futures_last(item["symbol"])

    if item["side"] == "SELL":
        gap_win = (fut_low <= item["upper"]) or (
            fut_now is not None and fut_now <= item["upper"]
        )
    else:
        gap_win = (fut_high >= item["lower"]) or (
            fut_now is not None and fut_now >= item["lower"]
        )
    item["evaluated"] = True
    item["close"] = close
    item["high"] = high
    item["low"] = low
    item["gap_win"] = bool(gap_win)
    b = tf_bucket(item["tf"])
    b["n"] += 1
    if gap_win:
        b["gap_win"] += 1
    return item


def evaluate_pending_for_df(symbol: str, tf: str, df) -> list:
    done = []
    now = int(time.time())
    period = PERIOD_SEC.get(tf, 300)
    for item in pending_signals:
        if item.get("evaluated") or item["symbol"] != symbol or item["tf"] != tf:
            continue
        if now < item["candle_ts"] + period:
            continue
        row = df[df["ts"] == item["candle_ts"]]
        if row.empty:
            continue
        r = row.iloc[0]
        done.append(score_signal(item, float(r["c"]), float(r["h"]), float(r["l"])))
    return done


def format_winrate_report(evaluated: list) -> str:
    """فقط وقتی سیگنال بسته‌شده وجود دارد؛ بدون جمع روز."""
    if not evaluated:
        return ""
    lines = ["📈 <b>نتیجه کندل قبلی</b>"]
    by_tf = {}
    for x in evaluated:
        by_tf.setdefault(x["tf"], []).append(x)
    for tf in sorted(by_tf, key=lambda t: TF_ORDER.get(t, 99)):
        items = by_tf[tf]
        n = len(items)
        gap_ok = sum(1 for i in items if i.get("gap_win"))
        lines.append(f"<b>{tf}</b>: گپ پر شد {gap_ok}/{n} ({gap_ok/n*100:.0f}%)")
        for x in items:
            g = "✅" if x.get("gap_win") else "❌"
            lines.append(f"{g} {x['symbol']} {x['side']}")
    lines.append(f"⏰ {iran_now().strftime('%H:%M:%S')} ایران")
    return "\n".join(lines)


def format_daily_report(day: str) -> str:
    by = daily.get("by_tf") or {}
    lines = ["🌙 <b>گزارش پایان روز</b>", f"📅 {day}", "━━━━━━━━━━━━━━━━"]
    total_n = total_gap = 0
    for tf in ["15m", "1h", "4h", "1d"]:
        b = by.get(tf) or {"n": 0, "gap_win": 0}
        n = b["n"]
        total_n += n
        total_gap += b.get("gap_win", 0)
        if n == 0:
            lines.append(f"{tf}: سیگنالی بسته نشد")
        else:
            lines.append(f"{tf}: گپ پر شد {b['gap_win']}/{n} ({b['gap_win']/n*100:.0f}%)")
    lines.append("━━━━━━━━━━━━━━━━")
    if total_n:
        lines.append(f"کل: گپ پر شد {total_gap}/{total_n} ({total_gap/total_n*100:.0f}%)")
    else:
        lines.append("امروز سیگنال بسته‌شده‌ای نبود")
    return "\n".join(lines)


def maybe_send_daily_report() -> None:
    global last_daily_report_day
    today = iran_today()
    if daily.get("day") and daily["day"] != today and last_daily_report_day != daily["day"]:
        send_telegram_text(format_daily_report(daily["day"]))
        last_daily_report_day = daily["day"]
        save_state()


def _df_from_ohlc_rows(rows: list) -> pd.DataFrame | None:
    if not rows or len(rows) < BB_PERIOD + 2:
        return None
    df = pd.DataFrame(rows).sort_values("ts").drop_duplicates("ts").reset_index(drop=True)
    df["dt"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    sma, upper, lower = calc_bb(df["c"].values)
    df["sma"] = sma.values
    df["upper"] = upper.values
    df["lower"] = lower.values
    return df


async def fetch_klines_ws(pair: str, kbar_type: str, size: int = 50):
    """کند — فقط وقتی REST جواب ندهد."""
    try:
        async with websockets.connect(SPOT_WS_URL, open_timeout=4, close_timeout=2) as ws:
            await ws.send(json.dumps({
                "action": "request", "request": "kbar",
                "kbar": kbar_type, "pair": pair, "size": str(size),
            }))
            records = None
            deadline = asyncio.get_event_loop().time() + 2.0
            while asyncio.get_event_loop().time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.8)
                    data = json.loads(raw)
                    if isinstance(data, dict) and "records" in data:
                        records = data["records"]
                        break
                except asyncio.TimeoutError:
                    continue
            if not records:
                return None
            rows = [{"ts": int(r[0]), "o": float(r[1]), "h": float(r[2]), "l": float(r[3]), "c": float(r[4])} for r in records]
            return _df_from_ohlc_rows(rows)
    except Exception as e:
        log.debug("WS kline error %s %s: %s", pair, kbar_type, e)
        return None


def fetch_klines_gate(symbol: str, tf: str, size: int = 50) -> pd.DataFrame | None:
    """Gate.io USDT-M futures — سریع (REST)."""
    interval = GATE_INTERVAL.get(tf)
    if not interval:
        return None
    contract = symbol.upper()
    if contract.endswith("USDT") and "_" not in contract:
        contract = contract[:-4] + "_USDT"
    url = (
        f"https://api.gateio.ws/api/v4/futures/usdt/candlesticks"
        f"?contract={contract}&interval={interval}&limit={size}"
    )
    try:
        r = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None
        data = r.json()
        if not isinstance(data, list) or not data:
            return None
        rows = [{
            "ts": int(x.get("t") or 0),
            "o": float(x.get("o") or 0),
            "h": float(x.get("h") or 0),
            "l": float(x.get("l") or 0),
            "c": float(x.get("c") or 0),
        } for x in data]
        return _df_from_ohlc_rows(rows)
    except Exception as e:
        log.debug("Gate kline error %s %s: %s", symbol, tf, e)
        return None


def fetch_klines_bingx(symbol: str, tf: str, size: int = 50) -> pd.DataFrame | None:
    interval = BINGX_INTERVAL.get(tf)
    if not interval:
        return None
    base = symbol.upper()
    pair = base[:-4] + "-USDT" if base.endswith("USDT") else base
    url = (
        f"https://open-api.bingx.com/openApi/swap/v3/quote/klines"
        f"?symbol={pair}&interval={interval}&limit={size}"
    )
    try:
        r = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None
        payload = r.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        if not data:
            return None
        rows = []
        for x in data:
            ts = int(x.get("time") or 0)
            if ts > 10_000_000_000:
                ts //= 1000
            rows.append({
                "ts": ts,
                "o": float(x.get("open") or 0),
                "h": float(x.get("high") or 0),
                "l": float(x.get("low") or 0),
                "c": float(x.get("close") or 0),
            })
        return _df_from_ohlc_rows(rows)
    except Exception as e:
        log.debug("BingX kline error %s %s: %s", symbol, tf, e)
        return None


async def fetch_klines(symbol: str, tf: str, size: int = 50, prefer_rest: bool = True):
    """
    پیش‌فرض: Gate → BingX → (اختیاری) WS ال‌بانک.
    WS برای هر نماد چند ثانیه طول می‌کشد و اسکن را خیلی کند می‌کرد.
    """
    df = await asyncio.to_thread(fetch_klines_gate, symbol, tf, size)
    if df is not None and len(df) >= BB_PERIOD + 2:
        return df, "gate_futures"

    df = await asyncio.to_thread(fetch_klines_bingx, symbol, tf, size)
    if df is not None and len(df) >= BB_PERIOD + 2:
        return df, "bingx_futures"

    if not prefer_rest:
        pair = futures_to_spot_pair(symbol)
        kbar = TIMEFRAMES.get(tf)
        if kbar:
            df = await fetch_klines_ws(pair, kbar, size=size)
            if df is not None and len(df) >= BB_PERIOD + 2:
                return df, "lbank_spot"

    log.info("NO_KLINE %s %s", symbol, tf)
    add_to_blacklist(symbol, reason=f"NO_KLINE:{tf}")
    return None, None


async def process_symbol(
    symbol: str,
    tfs_to_check: list,
    on_demand: bool = False,
    chat_id: str | int | None = None,
    chart_budget: list | None = None,
    collect_msgs: list | None = None,
) -> None:
    now = int(time.time())
    tfs_to_check = sorted(tfs_to_check, key=lambda x: TF_ORDER.get(x, 99))
    for tf in tfs_to_check:
        if tf not in TIMEFRAMES:
            continue
        period = PERIOD_SEC.get(tf, 300)
        expected_open = (now // period) * period
        df = None
        src = None
        attempts = 1 if on_demand else 3
        ksize = 45 if on_demand else 70
        for attempt in range(attempts):
            df, src = await fetch_klines(symbol, tf, size=ksize, prefer_rest=True)
            if df is None or len(df) < BB_PERIOD + 2:
                if not on_demand:
                    await asyncio.sleep(0.5)
                continue
            try:
                last_ts = int(df.iloc[-1]["ts"])
            except Exception:
                last_ts = int(pd.Timestamp(df.iloc[-1]["dt"]).timestamp())
            if on_demand or last_ts >= expected_open:
                break
            if not on_demand:
                log.info(
                    "WAIT_CANDLE %s %s | got_ts=%s expected=%s try=%d src=%s",
                    symbol, tf, last_ts, expected_open, attempt + 1, src,
                )
                await asyncio.sleep(0.5)
            df = None
        if df is None or len(df) < BB_PERIOD + 2:
            continue
        if not on_demand:
            evaluate_pending_for_df(symbol, tf, df)
        sig = check_signal(df, symbol, tf, on_demand=on_demand)
        if not sig:
            continue
        msg = format_signal_message(sig)
        log.info(
            "SIGNAL %s %s %s fut=%.6g src=%s on_demand=%s",
            sig["side"], symbol, tf, sig.get("fut_last") or 0, src, on_demand,
        )
        if not on_demand:
            pending_signals.append({
                "symbol": symbol,
                "tf": tf,
                "candle_ts": sig["candle_ts"],
                "side": sig["side"],
                "open": sig["open"],
                "upper": sig["upper"],
                "lower": sig["lower"],
                "fut_last": sig.get("fut_last"),
                "evaluated": False,
                "reported": False,
            })
        if collect_msgs is not None:
            # حالت edit: فقط متن جمع شود، پیام جدا نفرست
            collect_msgs.append(msg)
            await asyncio.sleep(0.02)
            continue
        do_chart = SEND_CHART
        if chart_budget is not None:
            if chart_budget[0] <= 0:
                do_chart = False
            else:
                chart_budget[0] -= 1
        if do_chart:
            img = make_chart(df, symbol, tf, sig["side"], sig)
            if img:
                send_telegram_photo(img, msg, chat_id=chat_id)
            else:
                send_telegram_text(msg, chat_id=chat_id)
        else:
            send_telegram_text(msg, chat_id=chat_id)
        await asyncio.sleep(0.05)


async def pre_alert_cycle(tfs: list[str] | None = None) -> None:
    """چند دقیقه قبل از باز شدن کندل: اگر فیوچرز >۱٪ بالای UB باشد، احتمال گپ بفرست."""
    tfs = tfs or pre_alert_tfs_now()
    if not tfs:
        return

    symbols = get_top_movers(TOP_GAINERS)
    if not symbols:
        return
    priority = [s for s in ALWAYS_INCLUDE if s in symbols]
    rest = [s for s in symbols if s not in ALWAYS_INCLUDE]
    symbols = priority + rest

    now = int(time.time())
    log.info("PRE_ALERT scan | TFs=%s | %d نماد", ",".join(tfs), len(symbols))
    sem = asyncio.Semaphore(8)
    # جمع‌آوری نتایج per تایم‌فریم → یک پیام واحد
    found: dict[str, list] = {tf: [] for tf in tfs}
    lock = asyncio.Lock()

    async def check_one(symbol: str, tf: str) -> None:
        async with sem:
            try:
                period = PERIOD_SEC[tf]
                before = PRE_ALERT_BEFORE.get(tf, 180)
                candle_open = (now // period) * period
                next_open = candle_open + period
                secs_left = next_open - now
                if secs_left < 5 or secs_left > before + 10:
                    return

                key = (symbol, tf, next_open)
                if key in sent_pre_alerts:
                    return
                df, src = await fetch_klines(symbol, tf, size=40)
                if df is None or len(df) < BB_PERIOD + 2:
                    return
                if "upper" not in df.columns:
                    return
                cur = df.iloc[-1]
                ub = float(cur["upper"])
                if np.isnan(ub) or ub <= 0:
                    return
                px = float(cur["c"])
                fut = get_futures_last(symbol)
                if fut and px > 0:
                    drift = abs(fut - px) / px * 100.0
                    if drift > 3.0:
                        log.info(
                            "PRE_SKIP_DRIFT %s %s | src=%s close=%.6g fut=%.6g drift=%.1f%%",
                            symbol, tf, src, px, fut, drift,
                        )
                if px <= ub:
                    return
                gap_pct = (px - ub) / px * 100.0
                if gap_pct < PRE_ALERT_MIN_PCT:
                    return

                sent_pre_alerts.add(key)
                if len(sent_pre_alerts) > 3000:
                    sent_pre_alerts.clear()
                async with lock:
                    found[tf].append((symbol, gap_pct))
                log.info(
                    "PRE_ALERT %s %s src=%s px=%.6g ub=%.6g +%.2f%% left=%ds",
                    symbol, tf, src, px, ub, gap_pct, secs_left,
                )
            except Exception as e:
                log.warning("pre_alert %s %s: %s", symbol, tf, e)

    tasks = [check_one(s, tf) for tf in tfs for s in symbols]
    await asyncio.gather(*tasks)

    total = 0
    for tf in tfs:
        items = found.get(tf) or []
        if not items:
            continue
        items.sort(key=lambda x: -x[1])
        lines = [f"🟣 <b>احتمال گپ · {tf}</b>"]
        for symbol, gap_pct in items:
            link = lbank_futures_link(symbol, tf)
            lines.append(f'• <a href="{link}"><b>{symbol}</b></a>  +{gap_pct:.2f}%')
        send_telegram_text("\n".join(lines))
        total += len(items)
    log.info("PRE_ALERT done | sent=%d across %d TF msgs", total, sum(1 for t in tfs if found.get(t)))


def _angle_reset_day_if_needed() -> None:
    global angle_alert_day, angle_alert_count_today
    day = iran_now().strftime("%Y-%m-%d")
    if angle_alert_day != day:
        angle_alert_day = day
        angle_alert_count_today = 0


def check_angle_setup(df: pd.DataFrame, symbol: str, tf: str) -> dict | None:
    """
    آماده‌باش بستن زاویه — فرمول EWZ/GRT:
    پامپ نزدیک UB + شیب UB/EMA5 در حال خنک شدن + هنوز بالای EMA10/20 + spread باز.
    """
    if df is None or len(df) < 35:
        return None
    try:
        c_s = df["c"].astype(float)
        h_s = df["h"].astype(float)
        l_s = df["l"].astype(float)
        e5 = c_s.ewm(span=ANGLE_EMA[0], adjust=False).mean()
        e10 = c_s.ewm(span=ANGLE_EMA[1], adjust=False).mean()
        e20 = c_s.ewm(span=ANGLE_EMA[2], adjust=False).mean()
        sma = c_s.rolling(BB_PERIOD).mean()
        std = c_s.rolling(BB_PERIOD).std(ddof=1)
        ub = sma + BB_STD * std

        i = len(df) - 1
        c = float(c_s.iloc[i])
        ema5 = float(e5.iloc[i])
        ema10 = float(e10.iloc[i])
        ema20 = float(e20.iloc[i])
        ub_v = float(ub.iloc[i])
        if c <= 0 or any(np.isnan(x) for x in (ema5, ema10, ema20, ub_v)):
            return None

        dist10 = (c - ema10) / c * 100.0
        dist20 = (c - ema20) / c * 100.0
        if dist10 < ANGLE_MIN_DIST_E10 or dist20 < ANGLE_MIN_DIST_E20:
            return None
        if dist10 > ANGLE_MAX_DIST_E10:
            return None

        # تاچ اخیر EMA10 نداشته باشد
        for j in range(max(0, i - 3), i + 1):
            if float(l_s.iloc[j]) <= float(e10.iloc[j]) * 1.002:
                return None

        hh = float(h_s.iloc[max(0, i - 7): i + 1].max())
        from_high = (hh - c) / hh * 100.0 if hh > 0 else 99.0
        if from_high > ANGLE_MAX_FROM_HIGH:
            return None

        # پامپ اخیر نزدیک UB
        pumped = False
        for j in range(max(0, i - ANGLE_LOOKBACK_PUMP), i + 1):
            u = float(ub.iloc[j])
            if u > 0 and float(h_s.iloc[j]) >= u * 0.988:
                pumped = True
                break
        if not pumped:
            return None

        def slope_pct(series, idx, bars):
            a = float(series.iloc[idx])
            b = float(series.iloc[idx - bars])
            if b == 0 or np.isnan(a) or np.isnan(b):
                return 0.0
            return (a - b) / b * 100.0

        ub_s5 = slope_pct(ub, i - 5, 5) if i >= 10 else 0.0
        ub_s3 = slope_pct(ub, i, 3)
        e5_s5 = slope_pct(e5, i - 5, 5) if i >= 10 else 0.0
        e5_s3 = slope_pct(e5, i, 3)
        spread = (ema5 - ema20) / c * 100.0
        if spread < ANGLE_MIN_SPREAD:
            return None

        cool_ub = ub_s5 >= 1.2 and ub_s3 < ub_s5 * 0.90 and ub_s3 > -1.0
        cool_e5 = e5_s5 >= 1.0 and e5_s3 < e5_s5 * 0.90 and e5_s3 > -1.2
        # رد شتاب کامل (شبیه FLUID غلط)
        if e5_s3 > 2.5 and ub_s3 > 2.5:
            return None
        if e5_s5 > 2.5 and e5_s3 > e5_s5 * 0.95:
            return None
        if ema5 < ema10 * 0.997:
            return None
        if not (cool_ub or cool_e5):
            return None

        try:
            candle_ts = int(df.iloc[i]["ts"])
        except Exception:
            candle_ts = int(pd.Timestamp(df.iloc[i]["dt"]).timestamp())

        return {
            "symbol": symbol,
            "tf": tf,
            "close": c,
            "ub": ub_v,
            "ema5": ema5,
            "ema10": ema10,
            "ema20": ema20,
            "dist10": dist10,
            "dist20": dist20,
            "from_high": from_high,
            "spread": spread,
            "ub_s5": ub_s5,
            "ub_s3": ub_s3,
            "e5_s5": e5_s5,
            "e5_s3": e5_s3,
            "candle_ts": candle_ts,
        }
    except Exception as e:
        log.debug("check_angle_setup %s %s: %s", symbol, tf, e)
        return None


def format_angle_batch(tf: str, items: list[dict]) -> str:
    lines = [f"⚠️ <b>آماده‌باش زاویه</b> · {tf}"]
    for it in items:
        sym = it["symbol"]
        link = lbank_futures_link(sym, tf)
        lines.append(
            f'• <a href="{link}"><b>{sym}</b></a> '
            f'ازسقف {it["from_high"]:.1f}% · '
            f'+E10 {it["dist10"]:.1f}% · +E20 {it["dist20"]:.1f}%\n'
            f'  🎯 E10 <code>{it["ema10"]:.6g}</code> · '
            f'E20 <code>{it["ema20"]:.6g}</code>'
        )
    return "\n".join(lines)


async def angle_alert_cycle(
    symbols: list[str],
    tfs: list[str] | None = None,
    on_demand: bool = False,
    chat_id: str | int | None = None,
    quiet: bool = False,
) -> tuple[int, list[str]]:
    """اسکن آماده‌باش زاویه. اگر quiet: پیام نفرست، متن‌ها را برگردان."""
    global angle_alert_count_today
    if not ANGLE_ENABLED:
        return 0, []
    tfs = [t for t in (tfs or ANGLE_TFS) if t in TIMEFRAMES]
    if not tfs:
        return 0, []
    if not on_demand:
        _angle_reset_day_if_needed()
        if angle_alert_count_today >= ANGLE_DAILY_MAX:
            log.info("ANGLE daily cap reached (%d)", ANGLE_DAILY_MAX)
            return 0, []

    by_tf: dict[str, list] = {tf: [] for tf in tfs}
    sem = asyncio.Semaphore(ONDEMAND_FETCH_CONCURRENCY)

    async def one(sym: str, tf: str) -> None:
        if is_blacklisted(sym):
            return
        async with sem:
            try:
                df, src = await fetch_klines(sym, tf, size=40, prefer_rest=True)
                if df is None or len(df) < 35:
                    return
                hit = check_angle_setup(df, sym, tf)
                if not hit:
                    return
                key = (sym, tf, hit["candle_ts"])
                if (not on_demand) and key in sent_angle_alerts:
                    return
                by_tf[tf].append(hit)
                log.info(
                    "ANGLE_CANDIDATE %s %s fromH=%.1f +E10=%.1f spr=%.1f src=%s",
                    sym, tf, hit["from_high"], hit["dist10"], hit["spread"], src,
                )
            except Exception as e:
                log.debug("angle %s %s: %s", sym, tf, e)

    tasks = [one(sym, tf) for sym in symbols for tf in tfs]
    await asyncio.gather(*tasks)

    sent_n = 0
    texts: list[str] = []
    for tf in tfs:
        items = by_tf.get(tf) or []
        if not items:
            continue
        if not on_demand:
            remain = ANGLE_DAILY_MAX - angle_alert_count_today
            if remain <= 0:
                break
            items = items[:remain]
        for it in items:
            key = (it["symbol"], tf, it["candle_ts"])
            sent_angle_alerts.add(key)
            if not on_demand:
                angle_alert_count_today += 1
            sent_n += 1
        if len(sent_angle_alerts) > 3000:
            sent_angle_alerts.clear()
        batch = format_angle_batch(tf, items)
        texts.append(batch)
        if not quiet:
            send_telegram_text(batch, chat_id=chat_id)
            await asyncio.sleep(0.3)

    if sent_n:
        log.info("ANGLE sent=%d on_demand=%s quiet=%s", sent_n, on_demand, quiet)
    return sent_n, texts


async def one_cycle(tfs: list | None = None) -> None:
    if tfs is None:
        tfs = timeframes_to_check_now()
    if not tfs:
        return
    tfs = sorted(tfs, key=lambda x: TF_ORDER.get(x, 99))
    symbols = get_top_movers(TOP_GAINERS)
    if not symbols:
        return
    # بیت‌کوین و طلا اول چک شوند تا از پنجرهٔ باز شدن کندل جا نمانند
    priority = [s for s in ALWAYS_INCLUDE if s in symbols]
    rest = [s for s in symbols if s not in ALWAYS_INCLUDE]
    symbols = priority + rest
    sem = asyncio.Semaphore(10)

    async def limited(sym: str) -> None:
        async with sem:
            try:
                await process_symbol(sym, tfs)
            except Exception as e:
                log.warning("خطا روی %s: %s", sym, e)

    await asyncio.gather(*(limited(sym) for sym in symbols))

    # بعد از گپ: اسکن آماده‌باش زاویه (۱س/۴س) روی همان چک‌لیست
    try:
        await angle_alert_cycle(symbols)  # returns (n, texts)
    except Exception as e:
        log.error("Angle cycle error: %s", e)

    evaluated = [x for x in pending_signals if x.get("evaluated") and not x.get("reported")]
    for x in evaluated:
        x["reported"] = True

    sig_n = stats["signal_ok"]
    tfs_label = ", ".join(tfs) if tfs else "—"
    clock = iran_now().strftime("%H:%M")

    if sig_n == 0 and not evaluated:
        # فقط ضربان — ربات زنده است، این دور چیزی نبود
        send_telegram_text(f"⚪️ دور <b>{tfs_label}</b> · سیگنالی نبود · {clock}")
    else:
        parts = []
        if sig_n > 0:
            parts.append(
                f"📊 دور <b>{tfs_label}</b>\n"
                f"✅ سیگنال جدید: <b>{sig_n}</b>"
            )
        wr = format_winrate_report(evaluated)
        if wr:
            if parts:
                parts.append("┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄")
            parts.append(wr)
        if parts:
            send_telegram_text("\n".join(parts))

    for k in stats:
        stats[k] = 0

    pending_signals[:] = [
        x for x in pending_signals
        if (not x.get("evaluated")) or (int(time.time()) - x["candle_ts"] < 48 * 3600)
    ]
    save_state()
    maybe_send_daily_report()


def main_menu_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "🔍 گپ", "callback_data": "mode:gap"},
                {"text": "⚠️ زاویه", "callback_data": "mode:angle"},
            ],
            [
                {"text": "📌 تک‌نماد", "callback_data": "mode:single"},
            ],
        ]
    }


def kind_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "🔍 گپ", "callback_data": "kind:gap"},
                {"text": "⚠️ زاویه", "callback_data": "kind:angle"},
            ],
            [{"text": "« بازگشت", "callback_data": "menu"}],
        ]
    }


def tf_keyboard(mode: str) -> dict:
    """mode = gap | angle"""
    if mode == "angle":
        rows = [
            [
                {"text": "5m", "callback_data": "scan:angle:5m"},
                {"text": "15m", "callback_data": "scan:angle:15m"},
            ],
            [
                {"text": "1h", "callback_data": "scan:angle:1h"},
                {"text": "4h", "callback_data": "scan:angle:4h"},
            ],
            [{"text": "همه (5m+15m+1h+4h)", "callback_data": "scan:angle:5m,15m,1h,4h"}],
            [{"text": "« بازگشت", "callback_data": "menu"}],
        ]
    else:
        rows = [
            [
                {"text": "5m", "callback_data": "scan:gap:5m"},
                {"text": "15m", "callback_data": "scan:gap:15m"},
            ],
            [
                {"text": "1h", "callback_data": "scan:gap:1h"},
                {"text": "4h", "callback_data": "scan:gap:4h"},
            ],
            [{"text": "همه (5m+15m+1h+4h)", "callback_data": "scan:gap:5m,15m,1h,4h"}],
            [{"text": "« بازگشت", "callback_data": "menu"}],
        ]
    return {"inline_keyboard": rows}


def normalize_symbol(text: str) -> str | None:
    """btc / btcusdt / BTC-USDT → BTCUSDT"""
    if not text:
        return None
    s = text.strip().upper().replace("-", "").replace("/", "").replace("_", "").replace(" ", "")
    if not s or len(s) > 20:
        return None
    if not s.endswith("USDT"):
        s = s + "USDT"
    # فقط حروف و عدد
    if not all(c.isalnum() for c in s):
        return None
    return s


def clear_session(user_id) -> None:
    user_sessions.pop(str(user_id), None)


def session_key(user_id) -> str:
    return str(user_id)


def reply_dest(user_id, group_chat_id=None):
    """ترجیح: پیوی کاربر. اگر نشد، همان چت."""
    return user_id if user_id is not None else group_chat_id


async def on_demand_gap_scan(
    tfs: list[str],
    chat_id: str | int,
    symbols: list[str] | None = None,
    message_id: int | None = None,
) -> None:
    for k in stats:
        stats[k] = 0
    if symbols is None:
        symbols = get_top_movers(ONDEMAND_TOP_GAINERS, vol_n=ONDEMAND_TOP_VOLUME)
        if not symbols:
            ui_reply("لیست نمادها خالی بود.", chat_id, message_id, main_menu_keyboard())
            return
        priority = [s for s in ALWAYS_INCLUDE if s in symbols]
        rest = [s for s in symbols if s not in ALWAYS_INCLUDE]
        symbols = priority + rest
    label_sym = symbols[0] if len(symbols) == 1 else f"{len(symbols)} نماد"
    t0 = time.time()
    ui_reply(
        f"🔍 در حال اسکن گپ…\n"
        f"<b>{label_sym}</b> · {', '.join(tfs)}\n⏳",
        chat_id,
        message_id,
    )
    collected: list[str] = []
    sem = asyncio.Semaphore(ONDEMAND_FETCH_CONCURRENCY)

    async def limited(sym: str) -> None:
        async with sem:
            try:
                await process_symbol(
                    sym,
                    tfs,
                    on_demand=True,
                    chat_id=chat_id,
                    collect_msgs=collected,
                )
            except Exception as e:
                log.warning("on_demand gap %s: %s", sym, e)

    await asyncio.gather(*(limited(s) for s in symbols))
    log.info("on_demand gap done in %.1fs symbols=%d", time.time() - t0, len(symbols))
    n = stats["signal_ok"]
    clock = iran_now().strftime("%H:%M")
    if n == 0:
        body = f"🔍 گپ · <b>{label_sym}</b> · {', '.join(tfs)}\nهیچ سیگنالی نبود · {clock}"
    else:
        # جمع متن‌ها در همان پیام (حداکثر طول تلگرام)
        lines = [f"🔍 گپ · <b>{label_sym}</b> · {n} سیگنال · {clock}", "━━━━━━━━"]
        for m in collected[:12]:
            # کوتاه‌تر برای جا شدن
            short = m.replace("\n\n", "\n").strip()
            if len(short) > 350:
                short = short[:347] + "…"
            lines.append(short)
            lines.append("┄┄┄")
        if len(collected) > 12:
            lines.append(f"… و {len(collected) - 12} مورد دیگر")
        body = "\n".join(lines)
    ui_reply(body, chat_id, message_id, main_menu_keyboard())
    for k in stats:
        stats[k] = 0


async def on_demand_angle_scan(
    tfs: list[str],
    chat_id: str | int,
    symbols: list[str] | None = None,
    message_id: int | None = None,
) -> None:
    if symbols is None:
        symbols = get_top_movers(ONDEMAND_TOP_GAINERS, vol_n=ONDEMAND_TOP_VOLUME)
        if not symbols:
            ui_reply("لیست نمادها خالی بود.", chat_id, message_id, main_menu_keyboard())
            return
        priority = [s for s in ALWAYS_INCLUDE if s in symbols]
        rest = [s for s in symbols if s not in ALWAYS_INCLUDE]
        symbols = priority + rest
    label_sym = symbols[0] if len(symbols) == 1 else f"{len(symbols)} نماد"
    t0 = time.time()
    ui_reply(
        f"⚠️ در حال اسکن زاویه…\n"
        f"<b>{label_sym}</b> · {', '.join(tfs)}\n⏳",
        chat_id,
        message_id,
    )
    n, texts = await angle_alert_cycle(
        symbols, tfs=tfs, on_demand=True, chat_id=chat_id, quiet=True
    )
    log.info("on_demand angle done in %.1fs symbols=%d hits=%d", time.time() - t0, len(symbols), n)
    clock = iran_now().strftime("%H:%M")
    if n == 0:
        extra = "\n(شرایط آماده‌باش برقرار نبود)" if len(symbols) == 1 else ""
        body = (
            f"⚠️ زاویه · <b>{label_sym}</b> · {', '.join(tfs)}\n"
            f"آماده‌باشی نبود{extra} · {clock}"
        )
    else:
        body = f"⚠️ زاویه · <b>{label_sym}</b> · {n} مورد · {clock}\n━━━━━━━━\n"
        body += "\n\n".join(texts)
    ui_reply(body, chat_id, message_id, main_menu_keyboard())


async def handle_callback(cq: dict) -> None:
    cb_id = cq.get("id")
    data = (cq.get("data") or "").strip()
    msg = cq.get("message") or {}
    chat = msg.get("chat") or {}
    group_id = chat.get("id") or CHAT_ID
    mid = msg.get("message_id")
    from_user = cq.get("from") or {}
    user_id = from_user.get("id")
    uid = session_key(user_id)
    answer_callback_query(cb_id)

    # درخواستی: UI و نتیجه در پیوی کاربر (گروه فقط اتومات می‌گیرد)
    dest = user_id if user_id is not None else group_id
    # اگر پیوی کار نکرد، روی همان پیام گروه edit می‌کنیم
    private_ok = True

    def show(text: str, markup: dict | None = None) -> bool:
        nonlocal private_ok
        ok = send_telegram_text(text, reply_markup=markup, chat_id=dest)
        if not ok and dest != group_id:
            private_ok = False
            ui_reply(
                text + "\n\n⚠️ اول در پیوی ربات /start بزن.",
                group_id,
                mid,
                markup,
            )
            return False
        return ok

    if data == "menu" or data == "start":
        clear_session(user_id)
        show("منوی درخواستی (نتیجه در پیوی):\nیکی را انتخاب کن:", main_menu_keyboard())
        return

    if data == "mode:single":
        user_sessions[uid] = {"step": "symbol", "ui_chat": dest, "ui_mid": None}
        show(
            "📌 نام ارز را <b>در پیوی</b> بفرست\n"
            "مثال: <code>btc</code> یا <code>EWZ</code>",
            {"inline_keyboard": [[{"text": "« بازگشت", "callback_data": "menu"}]]},
        )
        return

    if data == "mode:gap":
        clear_session(user_id)
        show("تایم‌فریم گپ را انتخاب کن:", tf_keyboard("gap"))
        return

    if data == "mode:angle":
        clear_session(user_id)
        show("تایم‌فریم زاویه را انتخاب کن:", tf_keyboard("angle"))
        return

    if data.startswith("kind:"):
        mode = data.split(":", 1)[1]
        sess = user_sessions.get(uid) or {}
        sym = sess.get("symbol")
        if not sym or mode not in ("gap", "angle"):
            show("جلسه منقضی شد. دوباره تک‌نماد را بزن.", main_menu_keyboard())
            clear_session(user_id)
            return
        user_sessions[uid] = {
            "step": "tf",
            "symbol": sym,
            "mode": mode,
            "ui_chat": dest,
            "ui_mid": None,
        }
        show(
            f"<b>{sym}</b> — تایم‌فریم {('زاویه' if mode == 'angle' else 'گپ')}:",
            tf_keyboard(mode),
        )
        return

    if data.startswith("scan:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        mode, tf_blob = parts[1], parts[2]
        tfs = [t.strip() for t in tf_blob.split(",") if t.strip() in TIMEFRAMES]
        if not tfs:
            show("تایم‌فریم نامعتبر.", main_menu_keyboard())
            return
        sess = user_sessions.get(uid) or {}
        single = None
        if sess.get("step") == "tf" and sess.get("symbol") and sess.get("mode") == mode:
            single = [sess["symbol"]]
        clear_session(user_id)
        # نتیجه فقط پیوی
        scan_chat = dest
        async with scan_semaphore:
            try:
                if mode == "gap":
                    await on_demand_gap_scan(
                        tfs, scan_chat, symbols=single, message_id=None
                    )
                elif mode == "angle":
                    await on_demand_angle_scan(
                        tfs, scan_chat, symbols=single, message_id=None
                    )
            except Exception as e:
                log.error("scan error: %s", e)
                send_telegram_text(f"خطا در اسکن: {e}", chat_id=scan_chat)
        return


async def handle_message(message: dict) -> None:
    chat = message.get("chat") or {}
    chat_id = chat.get("id") or CHAT_ID
    chat_type = chat.get("type") or "private"
    from_user = message.get("from") or {}
    user_id = from_user.get("id")
    uid = session_key(user_id)
    text = (message.get("text") or "").strip()
    is_private = chat_type == "private"

    if text in ("/start", "/menu", "منو", "menu"):
        clear_session(user_id)
        send_telegram_text(
            f"✅ ربات درخواستی — <b>v{BOT_VERSION}</b>\n"
            "منو و نتایج روی <b>همان یک پیام</b> عوض می‌شوند (بدون اسپم).",
            chat_id=chat_id,
            reply_markup=main_menu_keyboard(),
        )
        return

    sess = user_sessions.get(uid) or {}
    if sess.get("step") == "symbol":
        sym = normalize_symbol(text)
        ui_chat = sess.get("ui_chat") or chat_id
        ui_mid = sess.get("ui_mid")
        if not sym:
            ui_reply(
                "نام معتبر نیست. مثال: <code>btc</code> یا <code>ewz</code>",
                ui_chat,
                ui_mid,
                {"inline_keyboard": [[{"text": "« بازگشت", "callback_data": "menu"}]]},
            )
            return
        user_sessions[uid] = {
            "step": "kind",
            "symbol": sym,
            "ui_chat": ui_chat,
            "ui_mid": ui_mid,
        }
        ui_reply(
            f"نماد: <b>{sym}</b>\nچی را چک کنم؟",
            ui_chat,
            ui_mid,
            kind_keyboard(),
        )
        # پیام متنی کاربر را در گروه می‌تواند ادمین پاک کند؛ ما اسپم اضافه نمی‌کنیم
        return

    # گروه: سکوت
    if not is_private:
        return

    # پیوی: فقط اگر session نباشد یک منو
    send_telegram_text(
        "از دکمه‌ها استفاده کن:",
        chat_id=chat_id,
        reply_markup=main_menu_keyboard(),
    )


async def auto_scan_loop() -> None:
    """اسکن همیشگی → فقط گروه (CHAT_ID)."""
    last_scan_key = None
    last_pre_key = None
    log.info("AUTO loop started")
    while True:
        try:
            maybe_send_daily_report()
            tfs = timeframes_to_check_now()
            if tfs:
                now = int(time.time())
                scan_key = tuple(
                    (tf, (now // PERIOD_SEC[tf]) * PERIOD_SEC[tf]) for tf in sorted(tfs)
                )
                if scan_key == last_scan_key:
                    await asyncio.sleep(5)
                    continue
                await asyncio.sleep(1)
                try:
                    await one_cycle(tfs)
                except Exception as e:
                    log.error("Cycle error: %s", e)
                last_scan_key = scan_key
            else:
                pre_tfs = pre_alert_tfs_now()
                if pre_tfs:
                    now = int(time.time())
                    pre_key = tuple(
                        (tf, (now // PERIOD_SEC[tf]) * PERIOD_SEC[tf] + PERIOD_SEC[tf])
                        for tf in sorted(pre_tfs)
                    )
                    if pre_key != last_pre_key:
                        try:
                            await pre_alert_cycle(pre_tfs)
                        except Exception as e:
                            log.error("Pre-alert error: %s", e)
                        last_pre_key = pre_key
                    await asyncio.sleep(15)
                    continue
            wait = seconds_until_next_candle()
            sleep_for = max(5 if tfs else 1, wait - 2)
            next_iran = datetime.fromtimestamp(
                int(time.time()) + wait, TEHRAN
            ).strftime("%H:%M:%S")
            log.info("خواب %d ثانیه تا رویداد بعدی (~%s ایران)", sleep_for, next_iran)
            await asyncio.sleep(sleep_for)
        except Exception as e:
            log.error("auto_scan_loop: %s", e)
            await asyncio.sleep(10)


async def telegram_poll_loop() -> None:
    """دکمه‌های درخواستی → پیوی."""
    offset = 0
    log.info("Telegram poll loop started")
    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params={
                    "timeout": 25,
                    "offset": offset,
                    "allowed_updates": json.dumps(["message", "callback_query"]),
                },
                timeout=35,
            )
            data = r.json() if r.status_code == 200 else {}
            for upd in data.get("result") or []:
                offset = max(offset, int(upd["update_id"]) + 1)
                if "callback_query" in upd:
                    asyncio.create_task(handle_callback(upd["callback_query"]))
                elif "message" in upd:
                    asyncio.create_task(handle_message(upd["message"]))
        except Exception as e:
            log.warning("poll error: %s", e)
            await asyncio.sleep(3)
        await asyncio.sleep(0.2)


async def main() -> None:
    load_state()
    ensure_daily()
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.error("TELEGRAM_TOKEN / CHAT_ID تنظیم نشده")
        return

    mode = BOT_MODE
    if mode in ("auto", "continuous", "always"):
        send_telegram_text(
            format_startup_message()
            + "\n\nحالت: <b>فقط اتومات</b> (گروه)"
        )
        save_state()
        await auto_scan_loop()
        return

    if mode in ("ondemand", "manual", "request"):
        send_telegram_text(
            format_startup_message()
            + "\n\nحالت: <b>فقط درخواستی</b>\nنتیجه در پیوی — اول /start در پیوی",
            reply_markup=main_menu_keyboard(),
        )
        save_state()
        await telegram_poll_loop()
        return

    # پیش‌فرض: hybrid
    send_telegram_text(
        format_startup_message()
        + "\n\nحالت: <b>ترکیبی</b>\n"
        "• اتومات (گپ + احتمال گپ + زاویه) → <b>گروه</b>\n"
        "• دکمه دستی → <b>پیوی</b> همان نفر\n"
        "هر عضو یک‌بار در پیوی ربات /start بزند",
        reply_markup=main_menu_keyboard(),
    )
    save_state()
    log.info("HYBRID mode v%s — auto+ondemand", BOT_VERSION)
    await asyncio.gather(
        auto_scan_loop(),
        telegram_poll_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        save_state()
        log.info("Stopped by user")
