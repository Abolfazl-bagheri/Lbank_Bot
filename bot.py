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
BOT_VERSION = "1.7.5"
CHANGELOG = {
    "1.7.5": [
        "۱۵م زاویه شل‌تر (تست) | گپ فقط خلاصه | INNO فقط 🔴INNO",
    ],
    "1.7.4": [
        "هشدار تایم خبری (کلان + مرتبط با نماد) — ورود ممنوع در پنجره خبر",
        "علامت قرمز 🔴INNO روی ارزهای نوآوری/پرریسک",
    ],
    "1.7.3": [
        "زاویه سبک TA: جام ۱۵م بعد پامپ + گپ ۱س/۴س | هدف کلوز EMA10 | پله فقط با گپ بالاتر",
    ],
    "1.7.2": [
        "رفع آلارم غلط زاویه: فقط بعد از پامپ واقعی + از بالا به EMA؛ زیر میانگین‌ها دیگر سیگنال نمی‌دهد",
    ],
    "1.7.1": [
        "زنجیره زاویه: بستن اول/دوم در TF پایین → چک جام TF بالاتر (۱۵م→۱س→۴س)",
        "سطح A/B/C + رد سقف‌جدید/شتاب | زاویه فقط روی TF همان دور (سرعت)",
    ],
    "1.7.0": [
        "آماده‌باش بستن زاویه (EMA5/10/20 + خم UB) — فرمول",
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

import os
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

TIMEFRAMES = {
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
    "15m": 3 * 60,   # ۳ دقیقه قبل
    "1h": 5 * 60,    # ۵ دقیقه قبل
    "4h": 20 * 60,   # ۲۰ دقیقه قبل
}
PRE_ALERT_MIN_PCT = 1.0  # فقط اگر اختلاف فیوچرز تا UB بالای ۱٪ باشد
TOP_GAINERS = 30
TOP_VOLUME = 50
SEND_CHART = True
ONLY_SELL = True
ALWAYS_INCLUDE = ["BTCUSDT", "XAUTUSDT"]
# نمادهایی که NO_KLINE شدند تا این مدت دوباره چک نشوند (ثانیه) — ۲۴ ساعت
BLACKLIST_TTL_SEC = 24 * 3600

# ——— تایم خبری ———
NEWS_ENABLED = True
NEWS_PRE_ALERT_MIN = 45          # چند دقیقه قبل از خبر هشدار بده
NEWS_BLOCK_BEFORE_MIN = 15       # از چند دقیقه قبل ورود نکن
NEWS_BLOCK_AFTER_MIN = 20        # تا چند دقیقه بعد ورود نکن
NEWS_REFRESH_SEC = 3 * 3600      # هر چند وقت تقویم را تازه کن

# ——— منطقه نوآوری (INNO) — ریسک خیلی بالا ———
# دستی اضافه کن؛ به‌علاوه تشخیص خودکار از لeverage پایین (غیر سهام)
INNO_MANUAL = set()  # مثال: {"XXXUSDT", "YYYUSDT"}
INNO_FILE = os.path.join(BASE_DIR, "inno_symbols.txt")
# سهام/کالا که lev پایین دارند ولی INNO نیستند
NOT_INNO_BASES = {
    "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "AVAX", "DOT", "LINK",
    "LTC", "BCH", "NEAR", "ATOM", "UNI", "AAVE", "XAUT", "PAXG", "GOLD",
    "SILVER", "TSLA", "NVDA", "AAPL", "MSFT", "GOOGL", "META", "AMZN", "COIN",
    "MSTR", "SPY", "QQQ", "IWM", "NFLX", "AMD", "INTC", "BABA", "PLTR",
    "SUGAR", "WHEAT", "COTTON", "COCOA", "SOYBEAN", "CRCL", "HOOD",
}

# ——— آماده‌باش بستن زاویه (جدا از گپ) ———
ANGLE_ENABLED = True
ANGLE_TFS = ["15m", "1h", "4h"]   # زنجیره: ۱۵م→۱س→۴س
ANGLE_PARENT = {"15m": "1h", "1h": "4h", "4h": None}
ANGLE_EMA = (5, 10, 20)           # همان پیش‌فرض ال‌بانک
ANGLE_MIN_DIST_E10 = 1.8          # حداقل ٪ بالای EMA10 (آماده‌باش زود)
ANGLE_MIN_DIST_E20 = 2.8
ANGLE_MAX_DIST_E10 = 8.0
ANGLE_MAX_FROM_HIGH = 3.5         # حداکثر فاصله از سقف اخیر
ANGLE_MIN_SPREAD = 1.5            # ٪ فاصله EMA5−EMA20
ANGLE_DAILY_MAX = 12              # سقف آلارم زاویه در روز (ایران)
ANGLE_LOOKBACK_PUMP = 14          # کندل برای پامپ نزدیک UB
ANGLE_CLOSE1_PCT = 0.45           # نزدیکی low به EMA10 = بستن زاویه اول
ANGLE_CLOSE2_PCT = 0.55           # نزدیکی low به EMA20 = بستن زاویه دوم

OPEN_WINDOW_SEC = {
    "15m": 150,
    "1h": 180,
    "4h": 180,
    "1d": 600,
}

PERIOD_SEC = {
    "15m": 15 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1d": 24 * 60 * 60,
}

TF_ORDER = {"15m": 0, "1h": 1, "4h": 2, "1d": 3}

FUTURES_TICKERS_URL = (
    "https://lbkperp.lbank.com/cfd/openApi/v1/pub/marketData?productGroup=SwapU"
)
SPOT_WS_URL = "wss://api.lbank.info/ws/V2/"

# fallback intervals for Gate / BingX when LBank spot has no pair
GATE_INTERVAL = {
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}
BINGX_INTERVAL = {
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
instrument_lev: dict[str, int] = {}
inno_auto: set[str] = set()
news_events_cache: list[dict] = []
news_cache_ts: float = 0.0
sent_news_alerts: set = set()

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


def symbol_base(symbol: str) -> str:
    s = (symbol or "").upper().replace("USDT", "").replace("_", "")
    return s


def refresh_instrument_meta() -> None:
    """اهرم و کاندیدهای INNO از API ال‌بانک."""
    global instrument_lev, inno_auto
    try:
        r = requests.get(
            "https://lbkperp.lbank.com/cfd/openApi/v1/pub/instrument?productGroup=SwapU",
            timeout=15,
        )
        data = r.json().get("data") or []
    except Exception as e:
        log.warning("instrument meta: %s", e)
        return
    lev_map: dict[str, int] = {}
    auto: set[str] = set()
    for item in data:
        sym = (item.get("symbol") or "").upper()
        if not sym.endswith("USDT"):
            continue
        try:
            lev = int(float(item.get("maxLeverage") or 0))
        except Exception:
            lev = 0
        lev_map[sym] = lev
        base = symbol_base(sym)
        # lev خیلی پایین روی آلت غیرسهام ≈ منطقه پرریسک/نوآوری
        if 0 < lev <= 25 and base not in NOT_INNO_BASES and len(base) >= 2:
            auto.add(sym)
    instrument_lev = lev_map
    inno_auto = auto
    log.info("Instrument meta: %d symbols, auto-INNO candidates=%d", len(lev_map), len(auto))


def load_inno_manual() -> set[str]:
    out = set(x.upper() for x in INNO_MANUAL)
    if os.path.exists(INNO_FILE):
        try:
            with open(INNO_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip().upper()
                    if not line or line.startswith("#"):
                        continue
                    if not line.endswith("USDT"):
                        line = line + "USDT"
                    out.add(line)
        except Exception as e:
            log.warning("inno file: %s", e)
    return out


def is_inno(symbol: str) -> bool:
    sym = (symbol or "").upper()
    if sym in load_inno_manual():
        return True
    if not instrument_lev:
        refresh_instrument_meta()
    return sym in inno_auto


def inno_badge(symbol: str) -> str:
    if is_inno(symbol):
        return ' <b>🔴INNO</b>'
    return ""


# ——— تقویم خبری ———
# رویدادهای تکراری تقریبی + تلاش برای دریافت تقویم آنلاین
_MACRO_AFFECTS = {
    "default": ["BTCUSDT", "ETHUSDT", "XAUTUSDT"],
    "cpi": ["BTCUSDT", "ETHUSDT", "XAUTUSDT", "SOLUSDT"],
    "fomc": ["BTCUSDT", "ETHUSDT", "XAUTUSDT", "SOLUSDT", "BNBUSDT"],
    "nfp": ["BTCUSDT", "ETHUSDT", "XAUTUSDT"],
    "pce": ["BTCUSDT", "ETHUSDT", "XAUTUSDT"],
    "gdp": ["BTCUSDT", "ETHUSDT"],
}


def _parse_ff_day_html(html: str, day_label: str) -> list[dict]:
    """پارس ساده تقویم ForexFactory برای رویدادهای High impact."""
    import re
    from datetime import timedelta

    events = []
    # ردیف‌های جدول کلاسیک FF
    rows = re.findall(
        r'class="calendar__row[^"]*"[^>]*>(.*?)</tr>',
        html,
        re.I | re.S,
    )
    if not rows:
        # ساختار جدیدتر
        rows = re.findall(r'data-event-id="[^"]+"(.*?)</tr>', html, re.I | re.S)

    current_date = iran_today()
    for row in rows:
        if "high" not in row.lower() and "calendar__impact" not in row.lower():
            # فقط high
            if "icon--ff-impact-red" not in row and "impact-red" not in row.lower():
                if 'title="High"' not in row and "High Impact" not in row:
                    continue
        # currency
        cur_m = re.search(r'calendar__currency[^>]*>([A-Z]{3})', row)
        currency = cur_m.group(1) if cur_m else "USD"
        if currency not in ("USD", "EUR", "GBP", "JPY", "CNY"):
            continue
        title_m = re.search(r'calendar__event-title[^>]*>([^<]+)', row)
        if not title_m:
            title_m = re.search(r'calendar__event[^>]*>([^<]+)', row)
        title = (title_m.group(1).strip() if title_m else "High impact event")
        time_m = re.search(r'calendar__time[^>]*>([^<]+)', row)
        tstr = (time_m.group(1).strip() if time_m else "")
        # فقط USD برای کریپتو مهم‌تر است؛ EUR/GBP هم اثر دارند
        if currency != "USD" and not any(
            k in title.lower() for k in ("rate", "ecb", "boe", "interest")
        ):
            continue
        events.append({
            "title": title,
            "currency": currency,
            "time_str": tstr,
            "date": current_date,
            "source": "forexfactory",
        })
    return events


def _builtin_upcoming_macro() -> list[dict]:
    """رویدادهای کلان شناخته‌شدهٔ نزدیک (تقریبی ماهانه) — پشتیبان اگر اسکرپ شکست بخورد."""
    # تاریخ‌های مهم اکتبر ۲۰۲۶ از منابع عمومی
    known = [
        # (YYYY-MM-DD, HH:MM Iran roughly, title, tag)
        ("2026-10-14", "16:00", "US CPI (تورم آمریکا)", "cpi"),
        ("2026-10-15", "16:30", "US PPI / Retail Sales", "cpi"),
        ("2026-10-28", "21:30", "FOMC Rate Decision", "fomc"),
        ("2026-10-29", "16:30", "US PCE Price Index", "pce"),
        ("2026-11-06", "16:00", "Non-Farm Payrolls (NFP)", "nfp"),
    ]
    out = []
    now = iran_now()
    for date_s, hm, title, tag in known:
        try:
            y, m, d = map(int, date_s.split("-"))
            hh, mm = map(int, hm.split(":"))
            dt = datetime(y, m, d, hh, mm, tzinfo=TEHRAN)
        except Exception:
            continue
        if dt.timestamp() < time.time() - 3600:
            continue
        if dt.timestamp() > time.time() + 14 * 86400:
            continue
        out.append({
            "title": title,
            "currency": "USD",
            "ts": int(dt.timestamp()),
            "tag": tag,
            "source": "builtin",
            "affects": _MACRO_AFFECTS.get(tag, _MACRO_AFFECTS["default"]),
        })
    return out


def refresh_news_calendar(force: bool = False) -> list[dict]:
    global news_events_cache, news_cache_ts
    if not NEWS_ENABLED:
        return []
    now = time.time()
    if not force and news_events_cache and (now - news_cache_ts) < NEWS_REFRESH_SEC:
        return news_events_cache

    events: list[dict] = []
    events.extend(_builtin_upcoming_macro())

    # تلاش برای ForexFactory (ممکن است روی سرور بلاک شود)
    try:
        r = requests.get(
            "https://www.forexfactory.com/calendar?day=today",
            timeout=12,
            headers={"User-Agent": "Mozilla/5.0 (compatible; LBankBot/1.7)"},
        )
        if r.status_code == 200 and len(r.text) > 500:
            parsed = _parse_ff_day_html(r.text, "today")
            for p in parsed:
                events.append({
                    "title": p["title"],
                    "currency": p["currency"],
                    "ts": int(now),  # زمان دقیق در HTML پیچیده است — فقط هشدار روزانه
                    "tag": "high",
                    "source": "forexfactory",
                    "affects": _MACRO_AFFECTS["default"],
                    "all_day_hint": True,
                })
    except Exception as e:
        log.debug("FF calendar: %s", e)

    # یکتا بر اساس title+date
    seen = set()
    uniq = []
    for ev in events:
        key = (ev.get("title"), ev.get("ts") or ev.get("date"))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(ev)

    news_events_cache = uniq
    news_cache_ts = now
    log.info("News calendar refreshed: %d events", len(uniq))
    return uniq


def news_blocking_now(symbol: str | None = None) -> list[dict]:
    """رویدادهایی که الان پنجرهٔ ممنوعیت ورود دارند."""
    events = refresh_news_calendar()
    now = time.time()
    hit = []
    for ev in events:
        ts = ev.get("ts")
        if not ts:
            continue
        before = NEWS_BLOCK_BEFORE_MIN * 60
        after = NEWS_BLOCK_AFTER_MIN * 60
        if ts - before <= now <= ts + after:
            affects = ev.get("affects") or _MACRO_AFFECTS["default"]
            if symbol is None or symbol.upper() in affects or any(
                symbol_base(symbol) in symbol_base(a) for a in affects
            ):
                hit.append(ev)
            elif symbol and symbol.upper() in ("BTCUSDT", "ETHUSDT", "XAUTUSDT"):
                hit.append(ev)
    return hit


def news_upcoming(within_min: int = 180) -> list[dict]:
    events = refresh_news_calendar()
    now = time.time()
    out = []
    for ev in events:
        ts = ev.get("ts")
        if not ts:
            continue
        if 0 <= ts - now <= within_min * 60:
            out.append(ev)
    out.sort(key=lambda x: x.get("ts") or 0)
    return out


def format_news_alert(events: list[dict], prefix: str = "🚨 تایم خبری") -> str:
    lines = [f"<b>{prefix}</b> — تا اطلاع، ورود نکن / سایز را کم کن"]
    for ev in events[:8]:
        ts = ev.get("ts")
        try:
            tiran = datetime.fromtimestamp(ts, TEHRAN).strftime("%H:%M") if ts else "?"
        except Exception:
            tiran = "?"
        aff = ev.get("affects") or []
        aff_s = ", ".join(symbol_base(a) for a in aff[:4]) if aff else "بازار"
        lines.append(
            f"• {tiran} ایران · <b>{ev.get('title', 'خبر')}</b>\n"
            f"  اثر روی: {aff_s}"
        )
    return "\n".join(lines)


def maybe_send_news_prealerts() -> None:
    """۴۵ دقیقه قبل از خبر کلان، یک‌بار هشدار بده."""
    if not NEWS_ENABLED:
        return
    upcoming = news_upcoming(within_min=NEWS_PRE_ALERT_MIN + 5)
    for ev in upcoming:
        ts = ev.get("ts")
        if not ts:
            continue
        left = ts - time.time()
        if left > NEWS_PRE_ALERT_MIN * 60 or left < 0:
            continue
        key = ("pre", ev.get("title"), ts)
        if key in sent_news_alerts:
            continue
        sent_news_alerts.add(key)
        send_telegram_text(format_news_alert([ev], prefix="🚨 به‌زودی خبر مهم"))
        log.info("NEWS pre-alert: %s", ev.get("title"))


def news_line_for_symbol(symbol: str) -> str:
    hits = news_blocking_now(symbol)
    if not hits:
        # نزدیک بودن بدون بلاک کامل
        near = news_upcoming(within_min=NEWS_PRE_ALERT_MIN)
        near = [
            e for e in near
            if symbol.upper() in (e.get("affects") or _MACRO_AFFECTS["default"])
            or symbol.upper() in ("BTCUSDT", "ETHUSDT", "XAUTUSDT")
        ]
        if not near:
            return ""
        titles = ", ".join(e.get("title", "")[:40] for e in near[:2])
        return f"⚠️ نزدیک خبر: {titles}"
    titles = ", ".join(e.get("title", "")[:40] for e in hits[:2])
    return f"🚨 <b>تایم خبری — ورود نکن</b>: {titles}"


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
        "text": text,
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


def get_top_movers(n: int = TOP_GAINERS) -> list:
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
    gainers = df.nlargest(n, "change")["symbol"].tolist()
    volumes = df.nlargest(TOP_VOLUME, "vol")["symbol"].tolist()
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


def check_signal(df: pd.DataFrame, symbol: str, tf: str):
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
    interval_map = {"15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}
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
    """خلاصه گپ — بدون ورود/خروج/پله."""
    emoji = "🔴" if sig["side"] == "SELL" else "🟢"
    side_fa = "سِل" if sig["side"] == "SELL" else "لانگ"
    pen = sig.get("diff_pct")
    if pen is None:
        pen = sig["upper_pen"] if sig["side"] == "SELL" else sig["lower_pen"]
    symbol = sig["symbol"]
    link = lbank_futures_link(symbol, sig["tf"])
    q_score = sig.get("q_score", 50)
    badge = inno_badge(symbol)
    lines = [
        f"{emoji} <b>{side_fa}</b> · "
        f'<a href="{link}"><b>{symbol}</b></a>{badge}',
        f"⏱ {sig['tf']} · 📏 {pen:.2f}% · Q{q_score}",
    ]
    nl = news_line_for_symbol(symbol)
    if nl:
        lines.append(nl)
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
    try:
        async with websockets.connect(SPOT_WS_URL, open_timeout=10, close_timeout=3) as ws:
            await ws.send(json.dumps({
                "action": "request", "request": "kbar",
                "kbar": kbar_type, "pair": pair, "size": str(size),
            }))
            await ws.send(json.dumps({
                "action": "subscribe", "subscribe": "kbar",
                "kbar": kbar_type, "pair": pair,
            }))
            records = None
            live = None
            deadline = asyncio.get_event_loop().time() + 3
            while asyncio.get_event_loop().time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    data = json.loads(raw)
                    if isinstance(data, dict) and "records" in data:
                        records = data["records"]
                    if isinstance(data, dict) and data.get("type") == "kbar" and data.get("pair") == pair:
                        live = data.get("kbar")
                    if records is not None and live is not None:
                        break
                except asyncio.TimeoutError:
                    if records is not None:
                        break
            if not records:
                return None
            rows = [{"ts": int(r[0]), "o": float(r[1]), "h": float(r[2]), "l": float(r[3]), "c": float(r[4])} for r in records]
            df = _df_from_ohlc_rows(rows)
            if df is None:
                return None
            if live and isinstance(live, dict):
                df.loc[df.index[-1], "o"] = float(live.get("o", df.iloc[-1]["o"]))
                df.loc[df.index[-1], "h"] = float(live.get("h", df.iloc[-1]["h"]))
                df.loc[df.index[-1], "l"] = float(live.get("l", df.iloc[-1]["l"]))
                df.loc[df.index[-1], "c"] = float(live.get("c", df.iloc[-1]["c"]))
                sma, upper, lower = calc_bb(df["c"].values)
                df["sma"] = sma.values
                df["upper"] = upper.values
                df["lower"] = lower.values
            return df
    except Exception as e:
        log.debug("WS kline error %s %s: %s", pair, kbar_type, e)
        return None


def fetch_klines_gate(symbol: str, tf: str, size: int = 50) -> pd.DataFrame | None:
    """Gate.io USDT-M futures candlesticks — works when LBank has no spot pair."""
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
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None
        data = r.json()
        if not isinstance(data, list) or not data:
            return None
        rows = []
        for x in data:
            rows.append({
                "ts": int(x.get("t") or 0),
                "o": float(x.get("o") or 0),
                "h": float(x.get("h") or 0),
                "l": float(x.get("l") or 0),
                "c": float(x.get("c") or 0),
                "v": float(x.get("v") or x.get("sum") or 0),
            })
        return _df_from_ohlc_rows(rows)
    except Exception as e:
        log.debug("Gate kline error %s %s: %s", symbol, tf, e)
        return None


def fetch_klines_bingx(symbol: str, tf: str, size: int = 50) -> pd.DataFrame | None:
    interval = BINGX_INTERVAL.get(tf)
    if not interval:
        return None
    # BingX swap symbol form: MAGMA-USDT
    base = symbol.upper()
    if base.endswith("USDT"):
        pair = base[:-4] + "-USDT"
    else:
        pair = base
    url = (
        f"https://open-api.bingx.com/openApi/swap/v3/quote/klines"
        f"?symbol={pair}&interval={interval}&limit={size}"
    )
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
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


async def fetch_klines(symbol: str, tf: str, size: int = 50):
    """LBank spot WS → Gate futures → BingX futures."""
    pair = futures_to_spot_pair(symbol)
    kbar = TIMEFRAMES.get(tf)
    if kbar:
        df = await fetch_klines_ws(pair, kbar, size=size)
        if df is not None and len(df) >= BB_PERIOD + 2:
            return df, "lbank_spot"

    df = await asyncio.to_thread(fetch_klines_gate, symbol, tf, size)
    if df is not None and len(df) >= BB_PERIOD + 2:
        log.info("KLINE_SRC %s %s gate_futures", symbol, tf)
        return df, "gate_futures"

    df = await asyncio.to_thread(fetch_klines_bingx, symbol, tf, size)
    if df is not None and len(df) >= BB_PERIOD + 2:
        log.info("KLINE_SRC %s %s bingx_futures", symbol, tf)
        return df, "bingx_futures"

    log.info("NO_KLINE %s %s", symbol, tf)
    add_to_blacklist(symbol, reason=f"NO_KLINE:{tf}")
    return None, None


async def process_symbol(symbol: str, tfs_to_check: list) -> None:
    now = int(time.time())
    tfs_to_check = sorted(tfs_to_check, key=lambda x: TF_ORDER.get(x, 99))
    for tf in tfs_to_check:
        if tf not in TIMEFRAMES:
            continue
        period = PERIOD_SEC.get(tf, 300)
        expected_open = (now // period) * period
        df = None
        src = None
        for attempt in range(3):
            df, src = await fetch_klines(symbol, tf, size=80)
            if df is None or len(df) < BB_PERIOD + 2:
                await asyncio.sleep(1)
                continue
            try:
                last_ts = int(df.iloc[-1]["ts"])
            except Exception:
                last_ts = int(pd.Timestamp(df.iloc[-1]["dt"]).timestamp())
            if last_ts >= expected_open:
                break
            log.info(
                "WAIT_CANDLE %s %s | got_ts=%s expected=%s try=%d src=%s",
                symbol, tf, last_ts, expected_open, attempt + 1, src,
            )
            await asyncio.sleep(1)
            df = None
        if df is None or len(df) < BB_PERIOD + 2:
            continue
        evaluate_pending_for_df(symbol, tf, df)
        sig = check_signal(df, symbol, tf)
        if not sig:
            continue
        msg = format_signal_message(sig)
        log.info(
            "SIGNAL %s %s %s fut=%.6g src=%s",
            sig["side"], symbol, tf, sig.get("fut_last") or 0, src,
        )
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
        if SEND_CHART:
            img = make_chart(df, symbol, tf, sig["side"], sig)
            if img:
                send_telegram_photo(img, msg)
            else:
                send_telegram_text(msg)
        else:
            send_telegram_text(msg)
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


def _slope_pct(series, idx: int, bars: int) -> float:
    a = float(series.iloc[idx])
    b = float(series.iloc[idx - bars])
    if b == 0 or np.isnan(a) or np.isnan(b):
        return 0.0
    return (a - b) / b * 100.0


def _angle_metrics(df: pd.DataFrame) -> dict | None:
    """
    زاویه فقط وقتی معنا دارد که:
    ۱) اخیراً پامپ نزدیک UB بوده
    ۲) زاویه باز بوده (قیمت بالای EMAها با فاصله)
    ۳) الان از بالا دارد به EMA نزدیک/تاچ می‌کند — نه اینکه از قبل زیر میانگین‌ها باشد
    """
    if df is None or len(df) < 35:
        return None
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
    ema5, ema10, ema20 = float(e5.iloc[i]), float(e10.iloc[i]), float(e20.iloc[i])
    ub_v = float(ub.iloc[i])
    if c <= 0 or any(np.isnan(x) for x in (ema5, ema10, ema20, ub_v)):
        return None

    dist10 = (c - ema10) / c * 100.0
    dist20 = (c - ema20) / c * 100.0
    spread = (ema5 - ema20) / c * 100.0

    # ——— زمینهٔ پامپ + زاویهٔ باز در چند کندل قبل ———
    pumped = False
    for j in range(max(0, i - ANGLE_LOOKBACK_PUMP), i + 1):
        u = float(ub.iloc[j])
        if u > 0 and float(h_s.iloc[j]) >= u * 0.988:
            pumped = True
            break

    # در ۵–۱۰ کندل قبل باید حداقل یک‌بار خوب بالای EMA10 بوده باشد
    was_elevated = False
    max_prior_dist10 = -999.0
    for j in range(max(0, i - 10), i):
        cj, e10j = float(c_s.iloc[j]), float(e10.iloc[j])
        if cj <= 0 or e10j <= 0:
            continue
        d = (cj - e10j) / cj * 100.0
        if d > max_prior_dist10:
            max_prior_dist10 = d
        if d >= 1.5:
            was_elevated = True

    hh = float(h_s.iloc[max(0, i - 7): i + 1].max())
    prev_hh = float(h_s.iloc[max(0, i - 7): i].max()) if i > 0 else hh
    from_high = (hh - c) / hh * 100.0 if hh > 0 else 99.0
    new_high = float(h_s.iloc[i]) >= prev_hh * 0.998

    ub_s5 = _slope_pct(ub, i - 5, 5) if i >= 10 else 0.0
    ub_s3 = _slope_pct(ub, i, 3)
    e5_s5 = _slope_pct(e5, i - 5, 5) if i >= 10 else 0.0
    e5_s3 = _slope_pct(e5, i, 3)
    cool_ub = ub_s5 >= 1.0 and ub_s3 < ub_s5 * 0.90 and ub_s3 > -1.2
    cool_e5 = e5_s5 >= 0.8 and e5_s3 < e5_s5 * 0.90 and e5_s3 > -1.5
    hot = (e5_s3 > 2.5 and ub_s3 > 2.5) or (e5_s5 > 2.5 and e5_s3 > e5_s5 * 0.95)

    # تاچ EMA فقط اگر از بالا بیاید + زمینه پامپ/ارتفاع
    # close قیمت نباید عمیقاً زیر EMA باشد (مثل لیست غلط BTC/XAUT/…)
    touch1 = touch2 = False
    for j in range(max(0, i - 1), i + 1):
        lv, e10j, e20j = float(l_s.iloc[j]), float(e10.iloc[j]), float(e20.iloc[j])
        hj = float(h_s.iloc[j])
        if e10j > 0 and lv <= e10j * (1.0 + ANGLE_CLOSE1_PCT / 100.0) and hj >= e10j * 0.998:
            touch1 = True
        if e20j > 0 and lv <= e20j * (1.0 + ANGLE_CLOSE2_PCT / 100.0) and hj >= e20j * 0.998:
            touch2 = True

    context_ok = pumped and was_elevated and not hot

    # بستن زاویه اول: تاچ EMA10 از بالا، هنوز خیلی پایین‌تر نرفته
    close1 = (
        context_ok
        and touch1
        and -0.35 <= dist10 <= 1.8
        and dist20 >= -0.5
        and spread >= 0.8
    )
    # بستن زاویه دوم: تاچ EMA20 از بالا بعد از ارتفاع
    close2 = (
        context_ok
        and touch2
        and -0.5 <= dist20 <= 2.0
        and max_prior_dist10 >= 2.0
        and spread >= 0.6
    )

    cup_forming = (
        pumped
        and was_elevated
        and not hot
        and not new_high
        and (cool_ub or cool_e5)
        and from_high <= ANGLE_MAX_FROM_HIGH + 1.0
        and spread >= ANGLE_MIN_SPREAD * 0.8
        and dist10 >= 1.0
    )

    # آماده‌باش کلاسیک (سفت) — خنک شدن کامل
    ready_strict = (
        pumped
        and was_elevated
        and not hot
        and not new_high
        and (cool_ub or cool_e5)
        and ANGLE_MIN_DIST_E10 <= dist10 <= ANGLE_MAX_DIST_E10
        and dist20 >= ANGLE_MIN_DIST_E20
        and from_high <= ANGLE_MAX_FROM_HIGH
        and spread >= ANGLE_MIN_SPREAD
        and ema5 >= ema10 * 0.997
    )
    # سبک TA: پامپ + زاویه باز + بالای EMA10 (بدون خنک‌شدن کامل)
    cup_ta = (
        pumped
        and was_elevated
        and dist10 >= 1.2
        and dist20 >= 2.0
        and spread >= 1.8
        and from_high <= 5.5
        and ema5 >= ema10 * 0.995
        and c >= ema10 * 1.008
    )
    # ۱۵م شل‌تر (تست): فاصله کمتر، spread کمتر، سقف دورتر، hot شدید فقط رد
    cup_ta_15m = (
        pumped
        and was_elevated
        and dist10 >= 0.7
        and dist20 >= 1.2
        and spread >= 1.0
        and from_high <= 8.0
        and ema5 >= ema10 * 0.992
        and c >= ema10 * 1.003
        and not (e5_s3 > 4.0 and ub_s3 > 4.0)
    )
    touched_e10 = False
    for j in range(max(0, i - 2), i + 1):
        if float(l_s.iloc[j]) <= float(e10.iloc[j]) * (1.0 + ANGLE_CLOSE1_PCT / 100.0):
            touched_e10 = True
            break
    if touched_e10:
        ready_strict = False
        cup_ta = False
        cup_ta_15m = False

    ready_early = ready_strict or cup_ta

    try:
        candle_ts = int(df.iloc[i]["ts"])
    except Exception:
        candle_ts = int(pd.Timestamp(df.iloc[i]["dt"]).timestamp())

    return {
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
        "close1": close1,
        "close2": close2,
        "cup_forming": cup_forming,
        "cup_ta": cup_ta,
        "cup_ta_15m": cup_ta_15m,
        "ready_early": ready_early,
        "ready_strict": ready_strict,
        "new_high": new_high,
        "hot": hot,
        "pumped": pumped,
        "was_elevated": was_elevated,
        "candle_ts": candle_ts,
    }


def check_angle_setup(df: pd.DataFrame, symbol: str, tf: str) -> dict | None:
    """
    ready / close1 / close2
    روی ۱۵م: cup_ta_15m شل‌تر هم قبول می‌شود (تست).
    """
    try:
        m = _angle_metrics(df)
        if not m:
            return None
        if m["ready_early"]:
            event = "ready"
        elif tf == "15m" and m.get("cup_ta_15m"):
            event = "ready"
            m = {**m, "cup_ta": True, "ready_early": True}
        elif m["close2"]:
            event = "close2"
        elif m["close1"]:
            event = "close1"
        else:
            return None
        return {"symbol": symbol, "tf": tf, "event": event, **m}
    except Exception as e:
        log.debug("check_angle_setup %s %s: %s", symbol, tf, e)
        return None


def _df_has_sell_gap(df: pd.DataFrame) -> bool:
    """آیا کندل جاری/اخیر گپ سِل نسبت به UB دارد؟"""
    if df is None or len(df) < BB_PERIOD + 2 or "upper" not in df.columns:
        return False
    cur = df.iloc[-1]
    try:
        o = float(cur["o"])
        h = float(cur["h"])
        ub = float(cur["upper"])
    except Exception:
        return False
    if ub <= 0 or np.isnan(ub):
        return False
    open_gap = (o - ub) / ub * 100.0
    pen = (h - ub) / ub * 100.0 if h > ub else 0.0
    # گپ اوپن یا نفوذ قوی به بالای UB
    return open_gap >= MIN_OPEN_GAP_PCT or pen >= PENETRATION_PCT


def format_angle_batch(tf: str, items: list[dict]) -> str:
    lines = [f"⚠️ <b>زاویه / جام</b> · {tf}"]
    for it in items:
        sym = it["symbol"]
        link = lbank_futures_link(sym, tf)
        ev = it.get("event") or "ready"
        level = it.get("level") or "?"
        g1 = it.get("gap_1h")
        g4 = it.get("gap_4h")
        parent_cup = it.get("parent_cup")

        if ev == "close2":
            tag = "بستن زاویه <b>دوم</b>"
        elif ev == "close1":
            tag = "بستن زاویه <b>اول</b> (هدف رسیده)"
        elif it.get("cup_ta"):
            tag = "جام ۱۵م بعد پامپ — <b>ورود سِل</b>"
        else:
            tag = "آماده‌باش خنک‌شده"

        gap_txt = []
        if g1 is True:
            gap_txt.append("۱س گپ✓")
        elif g1 is False:
            gap_txt.append("۱س گپ✗")
        if g4 is True:
            gap_txt.append("۴س گپ✓")
        elif g4 is False:
            gap_txt.append("۴س گپ✗")
        gap_line = " · ".join(gap_txt) if gap_txt else ""

        scale = ""
        if g1 and g4:
            scale = "\n  📌 پله: مجاز (گپ ۱س+۴س) — اگر سقف جدید زد میانگین کم کن"
        elif g1 or g4:
            scale = "\n  📌 پله: فقط با احتیاط (یکی از گپ‌ها)"
        else:
            scale = "\n  📌 پله: نه — بدون گپ بالاتر اضافه نکن"

        extra = ""
        if parent_cup:
            extra = "\n  ⬆ تایم بالاتر: جام در حال شکل"
        if gap_line:
            extra += f"\n  🔗 {gap_line}"

        badge = inno_badge(sym)
        nl = news_line_for_symbol(sym)
        news_extra = f"\n  {nl}" if nl else ""
        lines.append(
            f'• <a href="{link}"><b>{sym}</b></a>{badge} [{level}] {tag}\n'
            f'  ازسقف {it["from_high"]:.1f}% · +E10 {it["dist10"]:.1f}% · +E20 {it["dist20"]:.1f}%\n'
            f'  🎯 هدف کلوز (زاویه۱) E10 <code>{it["ema10"]:.6g}</code>'
            f"{extra}{scale}{news_extra}"
        )
    return "\n".join(lines)


async def angle_alert_cycle(symbols: list[str], tfs: list[str] | None = None) -> int:
    """اسکن زاویه؛ فقط روی tfs داده‌شده (پیش‌فرض ANGLE_TFS). سقف روزانه."""
    global angle_alert_count_today
    if not ANGLE_ENABLED:
        return 0
    _angle_reset_day_if_needed()
    if angle_alert_count_today >= ANGLE_DAILY_MAX:
        log.info("ANGLE daily cap reached (%d)", ANGLE_DAILY_MAX)
        return 0

    scan_tfs = [t for t in (tfs or ANGLE_TFS) if t in ANGLE_TFS and t in TIMEFRAMES]
    if not scan_tfs:
        return 0

    by_tf: dict[str, list] = {tf: [] for tf in scan_tfs}
    sem = asyncio.Semaphore(8)

    async def one(sym: str, tf: str) -> None:
        if is_blacklisted(sym):
            return
        async with sem:
            try:
                df, src = await fetch_klines(sym, tf, size=50)
                if df is None or len(df) < 35:
                    return
                hit = check_angle_setup(df, sym, tf)
                if not hit:
                    return
                ev = hit.get("event") or "ready"
                key = (sym, tf, hit["candle_ts"], ev)
                if key in sent_angle_alerts:
                    return

                parent = ANGLE_PARENT.get(tf)
                hit["parent_tf"] = parent
                hit["parent_cup"] = False
                hit["gap_1h"] = None
                hit["gap_4h"] = None
                if parent and parent in TIMEFRAMES:
                    try:
                        pdf, _ = await fetch_klines(sym, parent, size=50)
                        pm = _angle_metrics(pdf) if pdf is not None else None
                        if pm and (pm.get("cup_forming") or pm.get("cup_ta")):
                            hit["parent_cup"] = True
                    except Exception:
                        pass
                # تأیید گپ تایم‌های بالاتر (سبک TA: پله با ۱س+۴س)
                try:
                    d1, _ = await fetch_klines(sym, "1h", size=40)
                    hit["gap_1h"] = _df_has_sell_gap(d1)
                except Exception:
                    hit["gap_1h"] = False
                try:
                    d4, _ = await fetch_klines(sym, "4h", size=40)
                    hit["gap_4h"] = _df_has_sell_gap(d4)
                except Exception:
                    hit["gap_4h"] = False

                # سبک TA روی ۱۵م: اگر جام هست ولی هیچ گپ بالاتری نیست → فقط واچ ضعیف
                if tf == "15m" and hit.get("cup_ta") and not hit["gap_1h"] and not hit["gap_4h"]:
                    hit["level"] = "A"
                elif hit["gap_1h"] and hit["gap_4h"] and ev in ("ready", "close1"):
                    hit["level"] = "C"
                elif (hit["gap_1h"] or hit["gap_4h"] or hit["parent_cup"]) and ev != "close2":
                    hit["level"] = "B"
                elif (ev == "ready" and hit["parent_cup"]) or (ev == "close2" and hit["parent_cup"]):
                    hit["level"] = "C"
                elif ev == "close2" or (ev == "close1" and hit["parent_cup"]):
                    hit["level"] = "B"
                else:
                    hit["level"] = "A"

                by_tf[tf].append(hit)
                log.info(
                    "ANGLE %s %s %s L=%s gap1h=%s gap4h=%s fromH=%.1f src=%s",
                    ev, sym, tf, hit["level"], hit["gap_1h"], hit["gap_4h"], hit["from_high"], src,
                )
            except Exception as e:
                log.debug("angle %s %s: %s", sym, tf, e)

    await asyncio.gather(*(one(sym, tf) for sym in symbols for tf in scan_tfs))

    sent_n = 0
    for tf in scan_tfs:
        items = by_tf.get(tf) or []
        if not items:
            continue
        items.sort(key=lambda x: {"C": 0, "B": 1, "A": 2}.get(x.get("level"), 9))
        remain = ANGLE_DAILY_MAX - angle_alert_count_today
        if remain <= 0:
            break
        items = items[:remain]
        for it in items:
            key = (it["symbol"], tf, it["candle_ts"], it.get("event") or "ready")
            sent_angle_alerts.add(key)
            angle_alert_count_today += 1
            sent_n += 1
        if len(sent_angle_alerts) > 3000:
            sent_angle_alerts.clear()
        send_telegram_text(format_angle_batch(tf, items))
        await asyncio.sleep(0.3)

    if sent_n:
        log.info("ANGLE sent=%d today=%d/%d TFs=%s", sent_n, angle_alert_count_today, ANGLE_DAILY_MAX, scan_tfs)
    return sent_n


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

    # بعد از گپ: زاویه فقط روی TFهای همین دور (نه همهٔ ANGLE_TFS هر بار)
    try:
        angle_tfs = [t for t in tfs if t in ANGLE_TFS]
        if angle_tfs:
            await angle_alert_cycle(symbols, tfs=angle_tfs)
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


async def main() -> None:
    load_state()
    ensure_daily()
    try:
        refresh_instrument_meta()
    except Exception as e:
        log.warning("instrument meta on start: %s", e)
    try:
        refresh_news_calendar(force=True)
    except Exception as e:
        log.warning("news calendar on start: %s", e)
    send_telegram_text(format_startup_message())
    # خلاصه اخبار نزدیک (اگر باشد)
    try:
        near = [e for e in refresh_news_calendar() if e.get("ts") and e["ts"] > time.time()]
        near = sorted(near, key=lambda x: x["ts"])[:5]
        if near:
            send_telegram_text(format_news_alert(near, prefix="📅 اخبار مهم پیش‌رو"))
    except Exception as e:
        log.debug("startup news: %s", e)
    save_state()  # ثبت نسخه فعلی
    last_scan_key = None
    last_pre_key = None
    last_news_refresh = time.time()
    while True:
        maybe_send_daily_report()
        try:
            maybe_send_news_prealerts()
            if time.time() - last_news_refresh > NEWS_REFRESH_SEC:
                refresh_news_calendar(force=True)
                last_news_refresh = time.time()
        except Exception as e:
            log.debug("news loop: %s", e)
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
                await asyncio.sleep(20)
                continue
        wait = seconds_until_next_candle()
        sleep_for = max(5 if tfs else 1, wait - 2)
        next_iran = datetime.fromtimestamp(int(time.time()) + wait, TEHRAN).strftime("%H:%M:%S")
        log.info("خواب %d ثانیه تا رویداد بعدی (~%s ایران)", sleep_for, next_iran)
        await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        save_state()
        log.info("Stopped by user")
