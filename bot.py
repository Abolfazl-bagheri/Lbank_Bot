"""
LBank Futures Bollinger Gap Signal Bot
"""

from __future__ import annotations

import asyncio
import json
import hashlib
import hmac
import secrets
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
BOT_VERSION = "1.12.4"
CHANGELOG = {
    "1.12.0": [
        "اتوترید سِل: مارجین ۱ USDT · اهرم ۱۰ · تی‌پی ۳۰٪ روی مارجین · بدون استاپ",
        "پیش‌فرض DRY_RUN=true تا کلید API و تست امن انجام شود",
    ],
    "1.11.1": [
        "خط استاتیک حمایت/مقاومت: کف گذشته≈سقف الان یا سقف گذشته≈کف الان",
        "بک‌گراند چارت ساختار سفید",
    ],
    "1.11.0": [
        "سیگنال = یک پیام: شات خط‌روند + کپشن (ارز / % تا UB-LB / سل|لانگ / TF)",
        "حذف کیفیت ورود، تگ کندل، UBسواری، ازداخل‌باند از پیام سیگنال",
        "گپ نزولی (لانگ) فعال | گزارش دور بدون duplicate",
    ],
    "1.10.2": [
        "خط‌روند اجباری نرم‌تر — چارت دیگر بدون خط نمی‌رود (fallback سقف‌های اخیر)",
    ],
    "1.10.1": [
        "رفع گپ جعلی (مثل RAM): دیگر فیوچرز با مقیاس غلط با UB اسپات مقایسه نمی‌شود",
        "گپ فقط با همان منبع کندل (اسپات/گیت) سنجیده می‌شود؛ drift بالا = رد",
        "چارت ساختار ضعیف (err>4٪) ارسال نمی‌شود",
    ],
    "1.10.0": [
        "رفع باگ حیاتی: structure_for_signal تابع ناموجود صدا می‌زد → خط هرگز کشیده نمی‌شد",
        "حالا از fetch_klines واقعی استفاده می‌کند و خط‌روند/کانال روی چارت می‌آید",
    ],
    "1.9.9": [
        "خط‌روند/کانال با الگوریتم جدید: سایهٔ غالب اخیر (درس DOGE/WIF/LINK)",
        "رسم با polyfit واقعی + همهٔ نقاط لمس برچسب‌دار — دیگر خط قدیمی اشتباه نیست",
    ],
    "1.9.8": [
        "فقط چارت خط‌روند/کانال — شات گپ و BB حذف کامل",
        "اگر کانال دقیق باشد روی همان چارت رسم می‌شود",
    ],
    "1.9.7": [
        "رفع قطع چارت: همیشه روی سیگنال و احتمال‌گپ عکس می‌فرستد (خط‌روند/کانال یا BB)",
    ],
    "1.9.6": [
        "کانال منظم نزولی + clone سقف/کف روی سیگنال و احتمال‌گپ",
        "احتمال‌گپ با قیمت فیوچرز (نه فقط اسپات) | چارت ساختار همیشه اگر کانال/خط باشد",
    ],
    "1.9.5": [
        "زاویه موقتاً خاموش (ANGLE_ENABLED=False) — شلوغی کمتر",
        "شات گپ/BB حذف شد؛ فقط چارت خط‌روند (ساختار) ارسال می‌شود",
    ],
    "1.9.2": [
        "پیام زاویه: گپ 1h/4h ✅❌ + جام ✅ / UB باز 🟠",
    ],
    "1.9.1": [
        "اسکرین گپ+BB و خط‌روند جدا | احتمال‌گپ با چارت",
        "زاویه فقط وضعیت گپ + چارت BB/EMA | حذف دکمه ساختار",
    ],
    "1.9.0": [
        "۳ مدل خط HH↓ HH↑ LL↑ — قوی اگر قله را تاچ کند",
        "فیلتر شدوی یه‌ویی DOS | چارت ساختار روی سیگنال قوی | پیام خلاصه",
    ],
    "1.8.1": [
        "ساختار روی TF بالاتر: 15m→1h/4h | 1h→4h/1d | 4h→1d/1w",
    ],
    "1.8.0": [
        "احتمال گپ: سطح ریسک + فاصله مقاومت + دکمه 📐 ساختار per نماد",
    ],
    "1.7.9": [
        "اخبار فقط روز خبر (۹ صبح) — حذف پیام استارت تکراری",
    ],
    "1.7.8": [
        "گپ با سطح ریسک: قوی/متوسط/ضعیف + فاصله تا مقاومت",
        "دکمه 📐 ساختار — خط‌روند و مقاومت افقی درخواستی",
    ],
    "1.7.7": [
        "علامت گپ: 📥از‌داخل‌باند / 🟠سواری‌UB / 🗡️کندل‌قوی",
        "اخبار: یک نوتیف روزانه ساعت ۹ صبح ایران + اثر روی ارزها",
    ],
    "1.7.6": [
        "زاویه زودتر: خم شدن نوک UB (قبل از بستن EMA) | بستن زاویه=دیر حذف از ۱۵م | واچ برای نوک تیز",
    ],
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
ONLY_SELL = False

# ========================= AUTO TRADE (LBank Futures) =========================
# کلیدها را فقط از env بگذار (هرگز داخل فایل هاردکد نکن)
#   LBANK_API_KEY / LBANK_API_SECRET
# یا در GitHub Secrets همین نام‌ها
AUTO_TRADE = os.environ.get("AUTO_TRADE", "0").strip() in ("1", "true", "True", "yes")
AUTO_TRADE_DRY_RUN = os.environ.get("AUTO_TRADE_DRY_RUN", "1").strip() not in ("0", "false", "False", "no")
LBANK_API_KEY = os.environ.get("LBANK_API_KEY", "").strip()
LBANK_API_SECRET = os.environ.get("LBANK_API_SECRET", "").strip()
TRADE_MARGIN_USDT = float(os.environ.get("TRADE_MARGIN_USDT", "1"))   # مارجین به تتر
TRADE_LEVERAGE = int(os.environ.get("TRADE_LEVERAGE", "10"))           # اهرم
TRADE_TP_PCT = float(os.environ.get("TRADE_TP_PCT", "30"))             # سود روی مارجین ٪
TRADE_ONLY_SELL = True   # طبق درخواست: فقط سِل
TRADE_NO_SL = True       # بدون استاپ‌لاس
TRADE_AVG_DOWN = True    # در ضرر، با گپ سِل بعدی +۱U میانگین کن
TRADE_MAX_ADDS = int(os.environ.get("TRADE_MAX_ADDS", "15"))  # سقف دفعات میانگین
# با اهرم ۱۰، تی‌پی ۳۰٪ مارجین ≈ ۳٪ حرکت قیمت به نفع سِل
PERP_BASE = "https://lbkperp.lbank.com"
STATE_BAK = os.path.join(BASE_DIR, "bot_state.bak")

ALWAYS_INCLUDE = ["BTCUSDT", "XAUTUSDT"]
# نمادهایی که NO_KLINE شدند تا این مدت دوباره چک نشوند (ثانیه) — ۲۴ ساعت
BLACKLIST_TTL_SEC = 24 * 3600

# ——— تایم خبری ———
NEWS_ENABLED = True
NEWS_PRE_ALERT_MIN = 45          # چند دقیقه قبل از خبر (فقط روی سیگنال، نه اسپم جدا)
NEWS_BLOCK_BEFORE_MIN = 15
NEWS_BLOCK_AFTER_MIN = 20
NEWS_REFRESH_SEC = 3 * 3600
NEWS_DAILY_HOUR = 9              # نوتیف روزانه اخبار — ساعت ایران
NEWS_DAILY_MINUTE = 0

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
# موقتاً خاموش — شلوغی پیام؛ برای برگشت True کن
ANGLE_ENABLED = False
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
    "1w": "7d",
}
BINGX_INTERVAL = {
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
    "1w": "1w",
}

sent_signals: set = set()
sent_pre_alerts: set = set()
sent_angle_alerts: set = set()
angle_alert_day: str | None = None
angle_alert_count_today: int = 0
pending_signals: list = []
_last_cycle_report_key = None
reported_eval_ids: set = set()  # (symbol, tf, candle_ts) already in cycle report
open_trades: list = []  # پوزیشن‌های اتوترید باز
trade_history: list = []  # کلوزشده‌ها برای گزارش ساعتی
_last_trade_report_ts = 0
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
last_news_daily_day: str | None = None  # آخرین روز ارسال نوتیف ۹ صبح

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


def _futures_change_24h(symbol: str) -> float | None:
    """درصد تغییر ۲۴س از تیکر فیوچرز ال‌بانک (اگر باشد)."""
    try:
        refresh_futures_map()
        item = None
        # futures_last_map فقط last است؛ از API دوباره بخوان
        r = requests.get(FUTURES_TICKERS_URL, timeout=12)
        data = r.json().get("data") or []
        for it in data:
            if (it.get("symbol") or "").upper() == symbol.upper():
                # فیلدهای رایج
                for k in ("change", "changeRate", "priceChangePercent", "chg"):
                    if it.get(k) is not None:
                        v = float(it[k])
                        # بعضی APIها کسری‌اند (0.05 = 5%)
                        if abs(v) < 1.5:
                            v *= 100.0
                        return v
                last = float(it.get("lastPrice") or 0)
                open24 = float(it.get("openPrice") or it.get("open24h") or 0)
                if last > 0 and open24 > 0:
                    return (last - open24) / open24 * 100.0
        return None
    except Exception as e:
        log.debug("chg24 %s: %s", symbol, e)
        return None


def _events_for_iran_day(day: str | None = None) -> list[dict]:
    """فقط رویدادهایی که تاریخ ایران‌شان همان روز است (نه کل هفته)."""
    day = day or iran_today()
    events = refresh_news_calendar(force=False)
    out = []
    for ev in events:
        ts = ev.get("ts")
        if not ts:
            continue
        # all_day_hint از FF بدون ساعت واقعی — رد (قبلاً هر روز نویز می‌داد)
        if ev.get("all_day_hint") and ev.get("source") == "forexfactory":
            continue
        try:
            d = datetime.fromtimestamp(int(ts), TEHRAN).strftime("%Y-%m-%d")
        except Exception:
            continue
        if d == day:
            out.append(ev)
    out.sort(key=lambda x: x.get("ts") or 0)
    return out


def format_daily_news_brief() -> str | None:
    """خلاصهٔ اخبار همان روز ایران. اگر خبری نباشد None."""
    today = iran_today()
    day_events = _events_for_iran_day(today)
    if not day_events:
        return None

    lines = [f"📅 <b>اخبار امروز</b> · {today}", "────────────"]
    for ev in day_events[:10]:
        ts = ev.get("ts")
        try:
            tiran = datetime.fromtimestamp(ts, TEHRAN).strftime("%H:%M") if ts else "—"
        except Exception:
            tiran = "—"
        title = ev.get("title") or "خبر"
        aff = ev.get("affects") or _MACRO_AFFECTS.get("default") or []
        aff_s = ", ".join(sorted({symbol_base(a) for a in aff[:6]})) or "بازار"
        lines.append(f"• <b>{tiran}</b> · {title}\n  اثر محتمل: {aff_s}")

    lines.append("────────────")
    lines.append("<b>اثر بازار (۲۴س اخیر)</b>")
    watch = ["BTCUSDT", "ETHUSDT", "XAUTUSDT", "SOLUSDT", "XRPUSDT"]
    for sym in watch:
        chg = _futures_change_24h(sym)
        if chg is None:
            continue
        arrow = "🟢" if chg >= 0 else "🔴"
        lines.append(f"{arrow} {symbol_base(sym)}  {chg:+.2f}%")
    lines.append("────────────")
    lines.append("یک‌بار در روز · فقط روزهایی که خبر هست")
    return "\n".join(lines)


def maybe_send_daily_news() -> None:
    """ساعت ۹ صبح ایران به بعد؛ فقط اگر برای امروز خبر واقعی باشد."""
    global last_news_daily_day
    if not NEWS_ENABLED:
        return
    now = iran_now()
    today = now.strftime("%Y-%m-%d")
    if last_news_daily_day == today:
        return
    if now.hour < NEWS_DAILY_HOUR:
        return
    try:
        msg = format_daily_news_brief()
        if not msg:
            # روز بدون خبر — علامت بزن تا دوباره چک/اسپم نشود
            last_news_daily_day = today
            log.info("No macro news for %s — skip daily brief", today)
            save_state()
            return
        send_telegram_text(msg)
        last_news_daily_day = today
        log.info("Daily news brief sent for %s", today)
        save_state()
    except Exception as e:
        log.warning("daily news: %s", e)


def maybe_send_news_prealerts() -> None:
    """۴۵ دقیقه قبل و نزدیک لحظهٔ خبرهای مهم → پیام جدا در تلگرام."""
    global sent_news_alerts
    if not NEWS_ENABLED:
        return
    try:
        # پنجره: از NEWS_PRE_ALERT_MIN قبل تا چند دقیقه بعد از شروع
        events = news_upcoming(within_min=NEWS_PRE_ALERT_MIN + 5)
        # رویدادهایی که همین الان در پنجره بلاک هستند هم اضافه
        now = time.time()
        for ev in refresh_news_calendar():
            ts = ev.get("ts")
            if not ts:
                continue
            if -5 * 60 <= ts - now <= NEWS_PRE_ALERT_MIN * 60:
                if ev not in events:
                    events.append(ev)
        if not events:
            return
        # فقط با impact بالا یا تگ‌های مهم
        important = []
        for ev in events:
            title = (ev.get("title") or "").lower()
            tag = (ev.get("tag") or "").lower()
            impact = str(ev.get("impact") or "").lower()
            if impact in ("high", "3", "red", "★★★", "★★★★"):
                important.append(ev)
                continue
            keys = ("cpi", "fomc", "nfp", "pce", "rate", "fed", "تورم", "بهره", "payroll", "beige")
            if any(k in title or k in tag for k in keys):
                important.append(ev)
                continue
            # اگر از builtin آمده و عنوان دارد
            if ev.get("source") == "builtin" or tag in ("cpi", "fomc", "nfp", "pce"):
                important.append(ev)
        if not important:
            # اگر impact مشخص نیست ولی در تقویم امروز با عنوان قوی است
            important = [e for e in events if e.get("title")]
        # dedupe by title+hour
        to_send = []
        for ev in important:
            ts = int(ev.get("ts") or 0)
            key = (ev.get("title", "")[:60], ts // 60)
            if key in sent_news_alerts:
                continue
            # فقط وقتی کمتر از PRE_ALERT دقیقه مانده یا تا ۲۰ دقیقه بعد از خبر
            if ts and not (-NEWS_BLOCK_AFTER_MIN * 60 <= ts - now <= NEWS_PRE_ALERT_MIN * 60):
                continue
            sent_news_alerts.add(key)
            to_send.append(ev)
        if not to_send:
            return
        if len(sent_news_alerts) > 200:
            sent_news_alerts.clear()
        msg = format_news_alert(to_send, prefix="🚨 خبر مهم نزدیک است")
        send_telegram_text(msg)
        log.info("NEWS prealert sent: %s", [e.get("title") for e in to_send])
        save_state()
    except Exception as e:
        log.warning("news prealert: %s", e)


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


def _github_repo_parts():
    """owner, repo از GITHUB_REPOSITORY یا STATE_GITHUB_REPO."""
    repo = (os.environ.get("STATE_GITHUB_REPO") or os.environ.get("GITHUB_REPOSITORY") or "").strip()
    token = (os.environ.get("STATE_GITHUB_TOKEN") or os.environ.get("GITHUB_TOKEN") or "").strip()
    if not repo or not token or "/" not in repo:
        return None, None, None
    owner, name = repo.split("/", 1)
    return owner, name, token


def github_state_pull() -> bool:
    """دانلود bot_state.json از مسیر .bot_state/ در ریپو (پایدار بین جاب‌ها)."""
    owner, name, token = _github_repo_parts()
    if not token:
        return False
    url = f"https://api.github.com/repos/{owner}/{name}/contents/.bot_state/bot_state.json"
    try:
        r = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=25,
        )
        if r.status_code == 404:
            log.info("github_state_pull: no remote state yet")
            return False
        if r.status_code != 200:
            log.warning("github_state_pull HTTP %s %s", r.status_code, r.text[:200])
            return False
        data = r.json()
        import base64
        content = base64.b64decode(data.get("content") or "").decode("utf-8")
        payload = json.loads(content)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        try:
            with open(STATE_BAK, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
        log.info("github_state_pull OK sha=%s", (data.get("sha") or "")[:12])
        return True
    except Exception as e:
        log.warning("github_state_pull: %s", e)
        return False


_last_github_push_ts = 0

def github_state_push(force: bool = False) -> bool:
    """آپلود state به GitHub تا جاب بعدی بتواند بخواند."""
    global _last_github_push_ts
    owner, name, token = _github_repo_parts()
    if not token:
        return False
    now = int(time.time())
    # حداکثر هر ۲ دقیقه یک‌بار (مگر force) تا rate limit نخوریم
    if not force and _last_github_push_ts and (now - _last_github_push_ts) < 120:
        return False
    if not os.path.exists(STATE_FILE):
        return False
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            raw = f.read()
        import base64
        b64 = base64.b64encode(raw.encode("utf-8")).decode("ascii")
        url = f"https://api.github.com/repos/{owner}/{name}/contents/.bot_state/bot_state.json"
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
            "message": f"bot state {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
            "content": b64,
            "branch": os.environ.get("STATE_GITHUB_BRANCH") or "main",
        }
        if sha:
            body["sha"] = sha
        pr = requests.put(url, headers=headers, json=body, timeout=30)
        if pr.status_code in (200, 201):
            _last_github_push_ts = now
            log.info("github_state_push OK")
            return True
        log.warning("github_state_push HTTP %s %s", pr.status_code, pr.text[:300])
        return False
    except Exception as e:
        log.warning("github_state_push: %s", e)
        return False


def save_state() -> None:
    try:
        prune_blacklist()
        payload = {
            "daily": daily,
            "last_daily_report_day": last_daily_report_day,
            "pending": pending_signals[-500:],
            "open_trades": [t for t in open_trades if t.get("status") == "open"][-50:],
            "trade_history": trade_history[-200:],
            "last_trade_report_ts": _last_trade_report_ts,
            "last_cycle_report_key": list(_last_cycle_report_key) if _last_cycle_report_key else None,
            "reported_eval_ids": [list(x) for x in list(reported_eval_ids)[-800:]],
            "kline_blacklist": kline_blacklist,
            "last_known_version": BOT_VERSION,
            "sent_angle_alerts": list(sent_angle_alerts)[-400:],
            "angle_alert_day": angle_alert_day,
            "angle_alert_count_today": angle_alert_count_today,
            "last_news_daily_day": last_news_daily_day,
        }
        raw = json.dumps(payload, ensure_ascii=False, indent=2)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            f.write(raw)
        try:
            with open(STATE_BAK, "w", encoding="utf-8") as f:
                f.write(raw)
        except Exception:
            pass
        # همگام با گیت‌هاب (اگر GITHUB_TOKEN باشد)
        try:
            github_state_push(force=False)
        except Exception as e:
            log.debug("github push skip: %s", e)
    except Exception as e:
        log.warning("save_state error: %s", e)


def load_state() -> None:
    global last_daily_report_day, last_known_version, last_news_daily_day
    global angle_alert_day, angle_alert_count_today
    # اول از گیت‌هاب (پایدار بین سرورهای Actions)
    try:
        github_state_pull()
    except Exception as e:
        log.debug("github pull: %s", e)
    payload = None
    for path_try in (STATE_FILE, STATE_BAK):
        if not os.path.exists(path_try):
            continue
        try:
            with open(path_try, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if payload:
                log.info("state loaded from %s", path_try)
                break
        except Exception as e:
            log.warning("load_state %s: %s", path_try, e)
            payload = None
    if not payload:
        return
    try:
        daily.update(payload.get("daily") or {})
        last_daily_report_day = payload.get("last_daily_report_day")
        last_known_version = payload.get("last_known_version")
        pending_signals[:] = payload.get("pending") or []
        ot = payload.get("open_trades") or []
        if isinstance(ot, list):
            open_trades[:] = [t for t in ot if isinstance(t, dict)]
        th = payload.get("trade_history") or []
        if isinstance(th, list):
            trade_history[:] = [t for t in th if isinstance(t, dict)]
        global _last_trade_report_ts, _last_cycle_report_key, reported_eval_ids
        try:
            _last_trade_report_ts = int(payload.get("last_trade_report_ts") or 0)
        except Exception:
            _last_trade_report_ts = 0
        try:
            lck = payload.get("last_cycle_report_key")
            if isinstance(lck, list) and len(lck) >= 1:
                # (tfs_label, tuple of triples)
                tfs_l = lck[0]
                triples = lck[1] if len(lck) > 1 else []
                if isinstance(triples, list):
                    _last_cycle_report_key = (tfs_l, tuple(tuple(x) if isinstance(x, list) else x for x in triples))
                else:
                    _last_cycle_report_key = tuple(lck)
            else:
                _last_cycle_report_key = None
        except Exception:
            _last_cycle_report_key = None
        try:
            reported_eval_ids.clear()
            for item in payload.get("reported_eval_ids") or []:
                if isinstance(item, (list, tuple)) and len(item) >= 3:
                    reported_eval_ids.add((str(item[0]), str(item[1]), int(item[2])))
        except Exception:
            pass
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
        last_news_daily_day = payload.get("last_news_daily_day")
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
    if AUTO_TRADE:
        mode = "DRY_RUN (تست)" if AUTO_TRADE_DRY_RUN or not LBANK_API_KEY else "LIVE"
        lines.append(
            f"🤖 اتوترید سِل: {mode} · {TRADE_MARGIN_USDT}U · {TRADE_LEVERAGE}x · TP {TRADE_TP_PCT}% مارجین · بدون SL"
        )
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


# ——— ساختار: خط‌روند + مقاومت افقی (سبک CATE) ———
STRUCT_STRONG_PCT = 1.5    # فاصله تا مقاومت ≤ این → قوی
STRUCT_MEDIUM_PCT = 4.0    # تا این → متوسط؛ بیشتر → ضعیف / کشف قیمت
STRUCT_HZ_TOL = 0.012      # تلورانس هم‌تراز بودن سقف/کف برای افقی
STRUCT_SWING_W = 3         # همسایه چپ/راست برای سوئینگ
# ساختار روی TF بالاتر از سیگنال
# 15m->1h+4h | 1h->4h+1d | 4h->1d+1w | 1d->1w
STRUCT_HTF = {
    "15m": ["1h", "4h"],
    "1h": ["4h", "1d"],
    "4h": ["1d", "1w"],
    "1d": ["1w"],
}


def _swing_highs(df: pd.DataFrame, w: int = STRUCT_SWING_W) -> list[dict]:
    out = []
    if df is None or len(df) < w * 2 + 3:
        return out
    hs = df["h"].astype(float).values
    ts = df["ts"].astype(int).values if "ts" in df.columns else None
    for i in range(w, len(df) - w):
        h = float(hs[i])
        if all(h >= float(hs[i + k]) for k in range(-w, w + 1) if k != 0):
            out.append({
                "i": i,
                "h": h,
                "ts": int(ts[i]) if ts is not None else i,
            })
    return out


def _swing_lows(df: pd.DataFrame, w: int = STRUCT_SWING_W) -> list[dict]:
    out = []
    if df is None or len(df) < w * 2 + 3:
        return out
    ls = df["l"].astype(float).values
    ts = df["ts"].astype(int).values if "ts" in df.columns else None
    for i in range(w, len(df) - w):
        l = float(ls[i])
        if all(l <= float(ls[i + k]) for k in range(-w, w + 1) if k != 0):
            out.append({
                "i": i,
                "l": l,
                "ts": int(ts[i]) if ts is not None else i,
            })
    return out


def is_erratic_wick_candle(row) -> bool:
    """شدوی یه‌ویی دوطرفه مثل DOS — حذف از سیگنال."""
    try:
        o = float(row["o"]); h = float(row["h"]); l = float(row["l"]); c = float(row["c"])
    except Exception:
        return False
    rng = h - l
    if rng <= 0 or c <= 0:
        return False
    body = abs(c - o)
    body_frac = body / rng
    uw = h - max(o, c)
    lw = min(o, c) - l
    range_pct = rng / c * 100.0
    if body_frac < 0.35 and uw / rng > 0.30 and lw / rng > 0.20 and range_pct >= 8.0:
        return True
    if body_frac < 0.30 and range_pct >= 15.0:
        return True
    return False


def _dominant_wick_swings(
    df: pd.DataFrame,
    kind: str,
    left: int = 4,
    right: int = 4,
    min_sep_sec: int = 10 * 3600,
    min_protrude_pct: float = 0.12,
) -> list[dict]:
    """
    سایهٔ غالب (درس DOGE/WIF):
    - fractal در همسایگی left/right
    - بیرون‌زدگی نسبت به میانهٔ پنجره
    - فاصلهٔ زمانی حداقل بین نقاط
    """
    if df is None or len(df) < left + right + 5:
        return []
    hs = df["h"].astype(float).values
    ls = df["l"].astype(float).values
    ts = df["ts"].astype(int).values if "ts" in df.columns else np.arange(len(df))
    out: list[dict] = []
    for i in range(left, len(df) - right):
        if kind == "h":
            val = float(hs[i])
            window = [float(hs[j]) for j in range(i - left, i + right + 1) if j != i]
            if val < max(window) - 1e-15:
                continue
            med = float(np.median(window))
            if med <= 0 or (val - med) / med * 100.0 < min_protrude_pct:
                continue
            item = {"i": i, "ts": int(ts[i]), "h": val, "l": float(ls[i])}
        else:
            val = float(ls[i])
            window = [float(ls[j]) for j in range(i - left, i + right + 1) if j != i]
            if val > min(window) + 1e-15:
                continue
            med = float(np.median(window))
            if med <= 0 or (med - val) / med * 100.0 < min_protrude_pct:
                continue
            item = {"i": i, "ts": int(ts[i]), "h": float(hs[i]), "l": val}
        if out and item["ts"] - out[-1]["ts"] < min_sep_sec:
            if kind == "h" and item["h"] > out[-1]["h"]:
                out[-1] = item
            elif kind == "l" and item["l"] < out[-1]["l"]:
                out[-1] = item
            continue
        out.append(item)
    return out


def _fit_desc_segment(pts: list[dict], key: str, last_ts: int, min_pts: int = 2, max_pts: int = 6):
    """زنجیره نزولی اخیر — نرم‌تر از قبل (۲ نقطه هم قبول)."""
    best = None
    n = len(pts)
    if n < min_pts:
        return None
    for L in range(min_pts, min(max_pts, n) + 1):
        for s in range(max(0, n - L - 6), n - L + 1):
            seg = pts[s:s + L]
            # اکثر نقاط نزولی باشند (نه لزوما همه)
            if L >= 3:
                desc = sum(1 for i in range(L - 1) if seg[i + 1][key] <= seg[i][key] * 1.002)
                if desc < max(1, L - 2):
                    continue
            else:
                if seg[-1][key] > seg[0][key] * 1.005:
                    continue
            xs = np.array([p["ts"] for p in seg], dtype=float)
            ys = np.array([p[key] for p in seg], dtype=float)
            if abs(xs[-1] - xs[0]) < 1:
                continue
            m, b = np.polyfit(xs, ys, 1)
            if m > 1e-12:  # صعودی خالص رد
                continue
            pred = m * xs + b
            res = float(np.mean(np.abs(ys - pred) / np.maximum(np.abs(pred), 1e-15) * 100.0))
            if res > 2.8:
                continue
            age_h = (last_ts - seg[-1]["ts"]) / 3600.0
            if age_h > 120:
                continue
            span_h = (seg[-1]["ts"] - seg[0]["ts"]) / 3600.0
            if span_h < 3:
                continue
            sc = res + 0.02 * age_h - 0.08 * L
            if best is None or sc < best["sc"]:
                best = {"pts": seg, "m": float(m), "b": float(b), "res": res, "sc": sc, "key": key}
    return best


def _simple_highs_line(df: pd.DataFrame) -> dict | None:
    """
    خط مقاومت اضطراری: ۲–۴ سقف اخیر (حتی اگر fractal کامل نباشد).
    تقریباً همیشه برای چارت چیزی برمی‌گرداند.
    """
    if df is None or len(df) < 20:
        return None
    try:
        d = df.iloc[-min(len(df), 150):].copy()
        hs = d["h"].astype(float).values
        ts = d["ts"].astype(int).values
        last_ts = int(ts[-1])
        peak_h = float(hs[-min(6, len(hs)):].max())
        # local peaks با پنجره کوچک
        peaks = []
        w = 2
        for i in range(w, len(d) - w):
            if hs[i] >= max(hs[i - w:i + w + 1]) - 1e-15:
                peaks.append({"i": i, "ts": int(ts[i]), "h": float(hs[i]), "l": float(d.iloc[i]["l"])})
        if len(peaks) < 2:
            # دو سقف مطلق در نیمه اخیر
            mid = len(d) // 3
            i1 = int(np.argmax(hs[: max(mid, 5)]))
            i2 = mid + int(np.argmax(hs[mid:]))
            if i2 <= i1:
                i2 = len(d) - 2
            peaks = [
                {"i": i1, "ts": int(ts[i1]), "h": float(hs[i1]), "l": float(d.iloc[i1]["l"])},
                {"i": i2, "ts": int(ts[i2]), "h": float(hs[i2]), "l": float(d.iloc[i2]["l"])},
            ]
        # آخرین ۳–۵ سقف نزدیک به قیمت فعلی
        peaks = [p for p in peaks if last_ts - p["ts"] <= 10 * 86400]
        if len(peaks) < 2:
            return None
        # ترجیح نزولی: از بین ترکیب‌های انتهایی
        best = None
        for L in range(2, min(5, len(peaks)) + 1):
            seg = peaks[-L:]
            xs = np.array([p["ts"] for p in seg], dtype=float)
            ys = np.array([p["h"] for p in seg], dtype=float)
            if abs(xs[-1] - xs[0]) < 60:
                continue
            m, b = np.polyfit(xs, ys, 1)
            pred = m * xs + b
            res = float(np.mean(np.abs(ys - pred) / np.maximum(np.abs(pred), 1e-15) * 100.0))
            uy = float(m * last_ts + b)
            err = abs(uy - peak_h) / max(peak_h, 1e-15) * 100.0
            sc = res + 0.15 * max(0, m * 1e8) + 0.05 * err  # شیب صعودی جریمه
            if best is None or sc < best["sc"]:
                best = {
                    "pts": seg, "m": float(m), "b": float(b), "res": res,
                    "sc": sc, "uy": uy, "err": err, "peak_h": peak_h,
                }
        if not best:
            return None
        pts = best["pts"]
        touches = [{"ts": p["ts"], "h": p["h"], "l": p.get("l", p["h"])} for p in pts]
        err = best["err"]
        if err <= 2.5 and best["m"] <= 0:
            risk, risk_fa, risk_emoji = "strong", "قوی", "🟢"
        elif err <= 5.0:
            risk, risk_fa, risk_emoji = "medium", "متوسط", "🟡"
        else:
            risk, risk_fa, risk_emoji = "weak", "ضعیف", "⚪"
        tl = {
            "kind": "HH↓" if best["m"] <= 0 else "HH↑",
            "err": float(err),
            "n3": max(0, len(pts) - 2),
            "score": float(err),
            "p1": touches[0],
            "p2": touches[-1],
            "slope": best["m"],
            "intercept": best["b"],
            "line_now": float(best["uy"]),
            "proj": float(best["uy"]),
            "thirds": touches[1:-1],
            "touches": touches,
            "m": best["m"],
            "b": best["b"],
        }
        return {
            "risk": risk,
            "risk_fa": risk_fa,
            "risk_emoji": risk_emoji,
            "res_price": float(best["uy"]),
            "res_dist_pct": float(err),
            "res_kind": tl["kind"],
            "horizontals": [],
            "tl": tl,
            "tls": [tl],
            "channel": False,
            "note": f"خط سقف · {len(pts)} نقطه · err={err:.1f}%",
        }
    except Exception as e:
        log.debug("_simple_highs_line: %s", e)
        return None


def detect_desc_channel(df: pd.DataFrame, price: float) -> dict | None:
    """کانال نزولی از سایه‌های غالب (نرم‌تر)."""
    if df is None or len(df) < 35 or not price or price <= 0:
        return None
    try:
        d = df.iloc[-min(len(df), 200):].copy()
        last_ts = int(d.iloc[-1]["ts"])
        peak_h = float(d["h"].astype(float).iloc[-min(12, len(d)):].max())
        span = max(1, last_ts - int(d.iloc[0]["ts"]))
        bar_sec = span / max(len(d) - 1, 1)
        sep = int(max(4 * 3600, min(16 * 3600, bar_sec * 6)))
        sh = _dominant_wick_swings(d, "h", left=2, right=2, min_sep_sec=sep, min_protrude_pct=0.05)
        sl = _dominant_wick_swings(d, "l", left=2, right=2, min_sep_sec=sep, min_protrude_pct=0.05)
        max_age = int(10 * 86400)
        sh = [x for x in sh if last_ts - x["ts"] <= max_age]
        sl = [x for x in sl if last_ts - x["ts"] <= max_age]
        if len(sh) < 2 or len(sl) < 2:
            return None
        uh = _fit_desc_segment(sh, "h", last_ts, min_pts=2, max_pts=6)
        ul = _fit_desc_segment(sl, "l", last_ts, min_pts=2, max_pts=6)
        if not uh or not ul:
            return None
        ratio = min(abs(uh["m"]), abs(ul["m"])) / max(abs(uh["m"]), abs(ul["m"]), 1e-20)
        if ratio < 0.15:
            return None
        uy = uh["m"] * last_ts + uh["b"]
        ly = ul["m"] * last_ts + ul["b"]
        if uy <= ly:
            return None
        width = (uy - ly) / max(price, 1e-15)
        if width < 0.005 or width > 0.55:
            return None
        err = abs(uy - peak_h) / max(peak_h, 1e-15) * 100.0
        pos = (price - ly) / max(uy - ly, 1e-15)
        if err <= 2.5 and pos >= 0.45:
            risk, risk_fa, risk_emoji = "strong", "قوی", "🟢"
        elif err <= 5.0 and pos >= 0.25:
            risk, risk_fa, risk_emoji = "medium", "متوسط", "🟡"
        else:
            risk, risk_fa, risk_emoji = "weak", "ضعیف", "⚪"
        p1h, p2h = uh["pts"][0], uh["pts"][-1]
        p1l, p2l = ul["pts"][0], ul["pts"][-1]
        upper_tl = {
            "kind": "CH↑", "err": float(err), "n3": max(0, len(uh["pts"]) - 2),
            "score": float(err),
            "p1": {"ts": p1h["ts"], "h": p1h["h"], "l": p1h.get("l", p1h["h"])},
            "p2": {"ts": p2h["ts"], "h": p2h["h"], "l": p2h.get("l", p2h["h"])},
            "slope": uh["m"], "intercept": uh["b"],
            "line_now": float(uy), "proj": float(uy),
            "thirds": [{"ts": p["ts"], "h": p["h"]} for p in uh["pts"][1:-1][:4]],
            "touches": [{"ts": p["ts"], "h": p["h"]} for p in uh["pts"]],
            "m": uh["m"], "b": uh["b"],
        }
        lower_tl = {
            "kind": "CH↓", "err": float(ul["res"]), "n3": max(0, len(ul["pts"]) - 2),
            "score": float(ul["res"]),
            "p1": {"ts": p1l["ts"], "h": p1l.get("h", p1l["l"]), "l": p1l["l"]},
            "p2": {"ts": p2l["ts"], "h": p2l.get("h", p2l["l"]), "l": p2l["l"]},
            "slope": ul["m"], "intercept": ul["b"],
            "line_now": float(ly), "proj": float(ly),
            "thirds": [{"ts": p["ts"], "l": p["l"]} for p in ul["pts"][1:-1][:4]],
            "touches": [{"ts": p["ts"], "l": p["l"]} for p in ul["pts"]],
            "m": ul["m"], "b": ul["b"],
        }
        return {
            "risk": risk, "risk_fa": risk_fa, "risk_emoji": risk_emoji,
            "res_price": float(uy), "res_dist_pct": float(err), "res_kind": "CH↓",
            "horizontals": [], "tl": upper_tl, "tls": [upper_tl, lower_tl],
            "channel": True, "channel_pos": float(pos),
            "note": f"کانال نزولی · سقف err={err:.1f}% · pos={pos:.0%}",
        }
    except Exception as e:
        log.debug("detect_desc_channel: %s", e)
        return None


def detect_desc_resistance(df: pd.DataFrame, price: float) -> dict | None:
    """مقاومت نزولی از سایه‌های غالب — در صورت شکست از خط ساده استفاده می‌کند."""
    if df is None or len(df) < 25 or not price or price <= 0:
        return None
    try:
        d = df.iloc[-min(len(df), 200):].copy()
        last_ts = int(d.iloc[-1]["ts"])
        peak_h = float(d["h"].astype(float).iloc[-min(8, len(d)):].max())
        span = max(1, last_ts - int(d.iloc[0]["ts"]))
        bar_sec = span / max(len(d) - 1, 1)
        sep = int(max(4 * 3600, min(16 * 3600, bar_sec * 6)))
        sh = _dominant_wick_swings(d, "h", left=2, right=2, min_sep_sec=sep, min_protrude_pct=0.05)
        sh = [x for x in sh if last_ts - x["ts"] <= 10 * 86400]
        uh = _fit_desc_segment(sh, "h", last_ts, min_pts=2, max_pts=6) if len(sh) >= 2 else None
        if uh:
            uy = uh["m"] * last_ts + uh["b"]
            err = abs(uy - peak_h) / max(peak_h, 1e-15) * 100.0
            if err <= 6.0:
                if err <= 2.5:
                    risk, risk_fa, risk_emoji = "strong", "قوی", "🟢"
                elif err <= 4.5:
                    risk, risk_fa, risk_emoji = "medium", "متوسط", "🟡"
                else:
                    risk, risk_fa, risk_emoji = "weak", "ضعیف", "⚪"
                pts = uh["pts"]
                touches = [{"ts": p["ts"], "h": p["h"], "l": p.get("l", p["h"])} for p in pts]
                tl = {
                    "kind": "HH↓", "err": float(err), "n3": max(0, len(pts) - 2),
                    "score": float(err), "p1": touches[0], "p2": touches[-1],
                    "slope": uh["m"], "intercept": uh["b"],
                    "line_now": float(uy), "proj": float(uy),
                    "thirds": touches[1:-1], "touches": touches,
                    "m": uh["m"], "b": uh["b"],
                }
                return {
                    "risk": risk, "risk_fa": risk_fa, "risk_emoji": risk_emoji,
                    "res_price": float(uy), "res_dist_pct": float(err), "res_kind": "HH↓",
                    "horizontals": [], "tl": tl, "tls": [tl], "channel": False,
                    "note": f"مقاومت نزولی · {len(pts)} لمس · err={err:.1f}%",
                }
        return _simple_highs_line(d)
    except Exception as e:
        log.debug("detect_desc_resistance: %s", e)
        return _simple_highs_line(df)



def detect_static_sr(df: pd.DataFrame, tol_pct: float = 1.2, recent_bars: int = 12) -> list[dict]:
    """
    خط افقی استاتیک (فلipped S/R):
      - RES←LOW : کف قوی گذشته ≈ سقف اخیر  → مقاومت افقی
      - SUP←HIGH: سقف قوی گذشته ≈ کف اخیر  → حمایت افقی
    فقط وقتی اختلاف نقاط ≤ tol_pct٪ باشد.
    """
    out: list[dict] = []
    if df is None or len(df) < 30:
        return out
    try:
        d = df.iloc[-min(len(df), 200):].copy().reset_index(drop=True)
        hs = d["h"].astype(float).values
        ls = d["l"].astype(float).values
        ts = d["ts"].astype(int).values
        w = 2
        sh, sl = [], []
        for i in range(w, len(d) - w):
            if hs[i] >= max(hs[i - w: i + w + 1]) - 1e-15:
                sh.append(i)
            if ls[i] <= min(ls[i - w: i + w + 1]) + 1e-15:
                sl.append(i)
        recent_start = max(0, len(d) - recent_bars)
        # past LOW ≈ current HIGH
        now_highs = [i for i in sh if i >= recent_start] + ([len(d) - 1] if len(d) else [])
        for i in now_highs:
            hi = float(hs[i])
            if hi <= 0:
                continue
            for j in sl:
                if j >= i - 2:
                    continue
                lo = float(ls[j])
                err = abs(hi - lo) / hi * 100.0
                if err <= tol_pct:
                    out.append({
                        "kind": "RES←LOW",
                        "price": float((hi + lo) / 2.0),
                        "err": float(err),
                        "past_ts": int(ts[j]),
                        "now_ts": int(ts[i]),
                        "past_p": lo,
                        "now_p": hi,
                    })
        # past HIGH ≈ current LOW
        now_lows = [i for i in sl if i >= recent_start] + ([len(d) - 1] if len(d) else [])
        for i in now_lows:
            lo = float(ls[i])
            if lo <= 0:
                continue
            for j in sh:
                if j >= i - 2:
                    continue
                hi = float(hs[j])
                err = abs(hi - lo) / lo * 100.0
                if err <= tol_pct:
                    out.append({
                        "kind": "SUP←HIGH",
                        "price": float((hi + lo) / 2.0),
                        "err": float(err),
                        "past_ts": int(ts[j]),
                        "now_ts": int(ts[i]),
                        "past_p": hi,
                        "now_p": lo,
                    })
        out.sort(key=lambda x: x["err"])
        uniq = []
        for m in out:
            if any(abs(m["price"] - u["price"]) / max(m["price"], 1e-15) < 0.008 for u in uniq):
                continue
            uniq.append(m)
            if len(uniq) >= 3:
                break
        return uniq
    except Exception as e:
        log.debug("detect_static_sr: %s", e)
        return []


def analyze_structure(df: pd.DataFrame, price: float) -> dict:
    """اول کانال، بعد مقاومت؛ + خط استاتیک افقی (کف↔سقف)."""
    empty = {
        "risk": "weak", "risk_fa": "ضعیف", "risk_emoji": "⚪",
        "res_price": None, "res_dist_pct": None, "res_kind": None,
        "horizontals": [], "tl": None, "tls": [], "channel": False,
        "note": "بدون ساختار",
    }
    if df is None or len(df) < 20 or price is None or price <= 0:
        return empty
    try:
        statics = detect_static_sr(df, tol_pct=1.2, recent_bars=12)
        ch = detect_desc_channel(df, price)
        res = detect_desc_resistance(df, price)
        rank = {"strong": 0, "medium": 1, "weak": 2}
        candidates = [x for x in (ch, res) if x and (x.get("tls") or x.get("tl"))]
        if not candidates:
            simple = _simple_highs_line(df)
            result = simple if simple else dict(empty)
        else:
            candidates.sort(
                key=lambda x: (
                    rank.get(x.get("risk"), 9),
                    float(x.get("res_dist_pct") or 99),
                    0 if x.get("channel") else 1,
                )
            )
            result = dict(candidates[0])
        # همیشه استاتیک را بچسبان (حتی اگر ترندلاین نبود)
        if statics:
            result["horizontals"] = statics
            kinds = " · ".join(f"{s['kind']}({s['err']:.1f}%)" for s in statics[:2])
            note = result.get("note") or ""
            result["note"] = (note + " · " if note and note != "بدون ساختار" else "") + f"استاتیک: {kinds}"
            # اگر فقط استاتیک داریم
            if not (result.get("tls") or result.get("tl")):
                best = statics[0]
                result["res_price"] = best["price"]
                result["res_dist_pct"] = best["err"]
                result["res_kind"] = best["kind"]
                result["risk"] = "strong" if best["err"] <= 0.6 else ("medium" if best["err"] <= 1.2 else "weak")
                result["risk_fa"] = {"strong": "قوی", "medium": "متوسط", "weak": "ضعیف"}[result["risk"]]
                result["risk_emoji"] = {"strong": "🟢", "medium": "🟡", "weak": "⚪"}[result["risk"]]
        return result
    except Exception as e:
        log.debug("analyze_structure: %s", e)
        return _simple_highs_line(df) or empty


async def structure_for_signal(symbol: str, signal_tf: str, price: float):
    """Structure on signal TF + higher TFs. Uses real fetch_klines (not a missing helper)."""
    empty = {
        "risk": "weak",
        "risk_fa": "ضعیف",
        "risk_emoji": "⚪",
        "res_price": None,
        "res_dist_pct": None,
        "res_kind": None,
        "horizontals": [],
        "tl": None,
        "tls": [],
        "channel": False,
        "note": "ساختار HTF در دسترس نبود",
        "struct_tf": None,
    }
    # تایم خود سیگنال + بالاتر (فقط TFهای پشتیبانی‌شده)
    htf_list = []
    if signal_tf in TIMEFRAMES:
        htf_list.append(signal_tf)
    for t in (STRUCT_HTF.get(signal_tf) or ["4h", "1d"]):
        if t in TIMEFRAMES and t not in htf_list:
            htf_list.append(t)
    # 1w فقط اگر در TIMEFRAMES باشد
    best = None
    rank = {"strong": 0, "medium": 1, "weak": 2}
    for htf in htf_list:
        try:
            df, src = await fetch_klines(symbol, htf, size=200)
            if df is None or len(df) < 30:
                log.debug("structure skip %s %s: no df", symbol, htf)
                continue
            st = analyze_structure(df, price)
            if not (st.get("tls") or st.get("tl") or st.get("channel")):
                log.debug("structure empty %s %s: %s", symbol, htf, st.get("note"))
                continue
            st["struct_tf"] = htf
            st["struct_src"] = src
            dist = st.get("res_dist_pct")
            dist_key = float(dist) if dist is not None else 999.0
            ch_pen = 0 if st.get("channel") else 0.25
            cand = (rank.get(st.get("risk"), 2) + ch_pen, dist_key, st, df, htf)
            if best is None or cand[0] < best[0] or (cand[0] == best[0] and cand[1] < best[1]):
                best = cand
        except Exception as e:
            log.warning("structure_for_signal %s %s: %s", symbol, htf, e)
    if best is None:
        return empty, None, signal_tf
    st, df, htf = best[2], best[3], best[4]
    st["note"] = (st.get("note") or "") + f" · TF:{htf}"
    return st, df, htf



def make_structure_chart(df: pd.DataFrame, symbol: str, tf: str, st: dict) -> bytes | None:
    """چارت ساده با خط‌روند نزولی + مقاومت‌های افقی."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from matplotlib.patches import Rectangle
        from datetime import datetime as dtmod

        if df is None or len(df) < 20:
            return None
        # از اولین نقطهٔ لمس تا الان (حداکثر ~۱۸۰ کندل)
        n = min(len(df), 180)
        d = df.iloc[-n:].copy()
        try:
            lines0 = st.get("tls") or ([] if not st.get("tl") else [st["tl"]])
            t_min = None
            for tl0 in lines0:
                for p in (tl0.get("touches") or [tl0.get("p1"), tl0.get("p2")]):
                    if p and p.get("ts"):
                        t_min = int(p["ts"]) if t_min is None else min(t_min, int(p["ts"]))
            if t_min:
                d2 = df[df["ts"].astype(int) >= t_min - PERIOD_SEC.get(tf, 3600) * 8]
                if d2 is not None and len(d2) >= 25:
                    d = d2.copy()
        except Exception:
            pass
        fig, ax = plt.subplots(figsize=(13, 6.5), facecolor="#ffffff")
        ax.set_facecolor("#ffffff")

        for _, row in d.iterrows():
            try:
                tsv = int(row["ts"])
                t = dtmod.fromtimestamp(tsv, TEHRAN)
            except Exception:
                continue
            o, h, l, c = float(row["o"]), float(row["h"]), float(row["l"]), float(row["c"])
            color = "#26a69a" if c >= o else "#ef5350"
            ax.plot([t, t], [l, h], color=color, lw=0.9, zorder=2)
            bottom = min(o, c)
            height = max(abs(c - o), (h - l) * 0.01)
            ax.add_patch(Rectangle(
                (mdates.date2num(t) - 0.3 * (PERIOD_SEC.get(tf, 3600) / 86400), bottom),
                0.6 * (PERIOD_SEC.get(tf, 3600) / 86400),
                height,
                facecolor=color, edgecolor=color, zorder=3,
            ))

        # BB حذف شد — فقط خط‌روند/کانال

        # خط استاتیک افقی (حمایت/مقاومت flipped)
        hz_colors = {"RES←LOW": "#e65100", "SUP←HIGH": "#1565c0", "HZ": "#f57c00"}
        for hz in (st.get("horizontals") or [])[:3]:
            lv = float(hz.get("price") or 0)
            if lv <= 0:
                continue
            kind = hz.get("kind") or "HZ"
            col = hz_colors.get(kind, "#e65100")
            ax.axhline(lv, color=col, lw=2.2, alpha=0.95, zorder=4)
            ax.text(
                0.01, lv,
                f" {kind} {lv:.6g}" + (f" ({hz['err']:.2f}%)" if hz.get("err") is not None else ""),
                transform=ax.get_yaxis_transform(),
                color=col, fontsize=9, va="bottom", fontweight="bold",
            )
            # نقاط گذشته و فعلی
            try:
                if hz.get("past_ts") and hz.get("past_p"):
                    pt = dtmod.fromtimestamp(int(hz["past_ts"]), TEHRAN)
                    ax.scatter([pt], [float(hz["past_p"])], c="#1976d2", s=65, zorder=7, edgecolors="#000", lw=0.6)
                if hz.get("now_ts") and hz.get("now_p"):
                    nt = dtmod.fromtimestamp(int(hz["now_ts"]), TEHRAN)
                    ax.scatter([nt], [float(hz["now_p"])], c="#c62828", s=65, zorder=7, edgecolors="#000", lw=0.6)
            except Exception:
                pass

        # خط‌روند / کانال — رسم با m*t+b (polyfit واقعی) + همهٔ نقاط لمس
        lines = st.get("tls") or ([] if not st.get("tl") else [st["tl"]])
        # اگر ساختار خالی بود روی همین df خط اضطراری بساز
        if not lines:
            try:
                px0 = float(d.iloc[-1]["c"])
                forced = analyze_structure(d, px0)
                if forced:
                    st = {**st, **forced}
                    lines = forced.get("tls") or ([forced["tl"]] if forced.get("tl") else [])
            except Exception:
                pass
        colors = {
            "HH↓": "#f9a825", "HH↑": "#2e7d32", "LL↑": "#7b1fa2",
            "CH↑": "#ef6c00", "CH↓": "#00838f",
        }
        t_end = int(d.iloc[-1]["ts"]) + PERIOD_SEC.get(tf, 3600) * 6
        for tl in lines[:3]:
            if not tl.get("p1") or not tl.get("p2"):
                continue
            kind = tl.get("kind") or ""
            m = tl.get("m", tl.get("slope"))
            b = tl.get("b", tl.get("intercept"))
            p1, p2 = tl["p1"], tl["p2"]
            if m is None or b is None:
                y1 = float(p1.get("h", p1.get("l", 0)))
                y2 = float(p2.get("h", p2.get("l", 0)))
                dt = float(p2["ts"]) - float(p1["ts"])
                if abs(dt) < 1:
                    continue
                m = (y2 - y1) / dt
                b = y1 - float(m) * float(p1["ts"])
            if m is None:
                continue
            if b is None:
                y1 = float(p1.get("h", p1.get("l", 0)))
                b = y1 - float(m) * float(p1["ts"])
            t0 = int(p1["ts"])
            xs = np.linspace(t0, t_end, 80)
            ys = float(m) * xs + float(b)
            xd = [dtmod.fromtimestamp(int(x), TEHRAN) for x in xs]
            col = colors.get(kind, "#ffca28")
            err = tl.get("err")
            lab = ("Upper CH" if kind == "CH↑" else "Lower CH" if kind == "CH↓"
                   else "DESC RES" if kind == "HH↓" else kind)
            if err is not None:
                lab += f" {err:.1f}%"
            ax.plot(xd, ys, color=col, lw=2.6, label=lab, zorder=5)
            touches = tl.get("touches") or ([p1] + list(tl.get("thirds") or []) + [p2])
            for i, th in enumerate(touches):
                try:
                    is_high = kind in ("CH↑", "HH↓", "HH↑")
                    ty = float(th.get("h") if is_high else th.get("l", th.get("h", 0)))
                    tt = dtmod.fromtimestamp(int(th["ts"]), TEHRAN)
                    ax.scatter([tt], [ty], c=col, s=70, zorder=7, edgecolors="white", lw=0.9)
                    ax.annotate(
                        ("%d\n%.5g" % (i + 1, ty)),
                        (tt, ty),
                        textcoords="offset points",
                        xytext=(0, 10 if is_high else -14),
                        color=col, fontsize=7.5, ha="center", fontweight="bold",
                    )
                except Exception:
                    pass

        if st.get("channel") and len(lines) >= 2:
            try:
                u = next((x for x in lines if x.get("kind") == "CH↑"), lines[0])
                lo = next((x for x in lines if x.get("kind") == "CH↓"), lines[1])
                mu, bu = u.get("m", u.get("slope")), u.get("b", u.get("intercept"))
                ml, bl = lo.get("m", lo.get("slope")), lo.get("b", lo.get("intercept"))
                if bu is None:
                    bu = float(u["p1"].get("h", 0)) - float(mu) * float(u["p1"]["ts"])
                if bl is None:
                    bl = float(lo["p1"].get("l", 0)) - float(ml) * float(lo["p1"]["ts"])
                t0 = min(int(u["p1"]["ts"]), int(lo["p1"]["ts"]))
                xs = np.linspace(t0, t_end, 80)
                yu = float(mu) * xs + float(bu)
                yl = float(ml) * xs + float(bl)
                xd = [dtmod.fromtimestamp(int(x), TEHRAN) for x in xs]
                ax.fill_between(xd, yl, yu, color="#42a5f5", alpha=0.12, zorder=1)
            except Exception:
                pass

        price = float(d.iloc[-1]["c"])
        t_last = dtmod.fromtimestamp(int(d.iloc[-1]["ts"]), TEHRAN)
        ax.scatter([t_last], [price], c="#111", s=40, zorder=7, edgecolors="#fff", lw=0.8)
        res = st.get("res_price")
        if res:
            ax.axhline(res, color="#f9a825", lw=1.2, ls="--", alpha=0.85)
            ax.annotate(
                f"res {res:.6g} (+{st.get('res_dist_pct', 0):.1f}%)",
                xy=(t_last, res), xytext=(-80, 10),
                textcoords="offset points", color="#f57f17", fontsize=8,
            )

        ax.set_title(
            f"{symbol} {tf} | {st.get('risk', '?')} | {st.get('res_kind', '')} | err={st.get('res_dist_pct')}",
            color="#111", fontsize=11,
        )
        ax.tick_params(colors="#333")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H", tz=TEHRAN))
        ax.grid(True, alpha=0.25, color="#bbb")
        for sp in ax.spines.values():
            sp.set_color("#999")
        fig.autofmt_xdate(rotation=25)
        plt.tight_layout()
        buf = BytesIO()
        fig.savefig(buf, format="png", dpi=120, facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        log.warning("make_structure_chart: %s", e)
        return None


def make_angle_chart(df: pd.DataFrame, symbol: str, tf: str, it: dict | None = None) -> bytes | None:
    """چارت زاویه: کندل + BB + EMA5/10/20."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from matplotlib.patches import Rectangle
        from datetime import datetime as dtmod
        from io import BytesIO

        if df is None or len(df) < 25:
            return None
        d = df.iloc[-min(len(df), 60):].copy()
        cser = d["c"].astype(float)
        d["ema5"] = cser.ewm(span=5, adjust=False).mean()
        d["ema10"] = cser.ewm(span=10, adjust=False).mean()
        d["ema20"] = cser.ewm(span=20, adjust=False).mean()
        sma = cser.rolling(BB_PERIOD).mean()
        std = cser.rolling(BB_PERIOD).std(ddof=0)
        d["sma"] = sma
        d["upper"] = sma + BB_STD * std
        d["lower"] = sma - BB_STD * std

        fig, ax = plt.subplots(figsize=(12, 6), facecolor="#0e1117")
        ax.set_facecolor("#0e1117")
        for _, row in d.iterrows():
            try:
                t = dtmod.fromtimestamp(int(row["ts"]), TEHRAN)
            except Exception:
                continue
            o, h, l, c = float(row["o"]), float(row["h"]), float(row["l"]), float(row["c"])
            color = "#26a69a" if c >= o else "#ef5350"
            ax.plot([t, t], [l, h], color=color, lw=0.9, zorder=2)
            bottom = min(o, c)
            height = max(abs(c - o), (h - l) * 0.01)
            ax.add_patch(Rectangle(
                (mdates.date2num(t) - 0.3 * (PERIOD_SEC.get(tf, 3600) / 86400), bottom),
                0.6 * (PERIOD_SEC.get(tf, 3600) / 86400),
                height, facecolor=color, edgecolor=color, zorder=3,
            ))
        xs, uys, lys, e5, e10, e20 = [], [], [], [], [], []
        for _, row in d.iterrows():
            if pd.isna(row.get("upper")):
                continue
            try:
                t = dtmod.fromtimestamp(int(row["ts"]), TEHRAN)
            except Exception:
                continue
            xs.append(t)
            uys.append(float(row["upper"]))
            lys.append(float(row["lower"]))
            e5.append(float(row["ema5"]))
            e10.append(float(row["ema10"]))
            e20.append(float(row["ema20"]))
        if xs:
            ax.plot(xs, uys, color="#7e57c2", lw=1.3, label="UB", zorder=4)
            ax.plot(xs, lys, color="#7e57c2", lw=1.0, alpha=0.5, label="LB", zorder=4)
            ax.plot(xs, e5, color="#ffca28", lw=1.2, label="EMA5", zorder=5)
            ax.plot(xs, e10, color="#29b6f6", lw=1.2, label="EMA10", zorder=5)
            ax.plot(xs, e20, color="#ef5350", lw=1.2, label="EMA20", zorder=5)
        ax.set_title(f"{symbol} {tf} · Angle / BB", color="white", fontsize=12)
        ax.tick_params(colors="#aaa")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M", tz=TEHRAN))
        ax.legend(loc="upper left", facecolor="#1a1d24", labelcolor="white", fontsize=8)
        ax.grid(True, alpha=0.12)
        for s in ax.spines.values():
            s.set_color("#333")
        plt.tight_layout()
        buf = BytesIO()
        fig.savefig(buf, format="png", dpi=120, facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        log.debug("make_angle_chart: %s", e)
        return None



def gap_context_flags(df: pd.DataFrame, side: str = "SELL") -> dict:
    """
    منشأ گپ + کندل قوی قبلی (الگوی LUMIA):
      inside  → از داخل باند پامپ شده → پرشدن محتمل‌تر 📥
      ride    → چند کندل روی/بالای UB → سواری 🟠
      strong  → کندل قبلی بدنه≥۲/۳ و سایه بالا≤۱/۳ → احتمال شکست قله بعد 🗡️
    """
    out = {"inside": False, "ride": False, "strong_prev": False, "tags": []}
    if df is None or len(df) < 3 or side != "SELL":
        return out
    try:
        cur = df.iloc[-1]
        prev = df.iloc[-2]
        cur_o = float(cur["o"])
        cur_ub = float(cur["upper"])
        prev_o = float(prev["o"])
        prev_h = float(prev["h"])
        prev_l = float(prev["l"])
        prev_c = float(prev["c"])
        prev_ub = float(prev["upper"])
        prev_lb = float(prev["lower"])

        # کندل قوی قبلی (سبز، بدنه پر، سایه بالا کم)
        rng = prev_h - prev_l
        if rng > 0 and prev_c >= prev_o:
            body = prev_c - prev_o
            uw = prev_h - prev_c
            if body >= rng * (2.0 / 3.0) and uw <= rng * (1.0 / 3.0):
                out["strong_prev"] = True
                out["tags"].append("🗡️کندل‌قوی→اول‌قله")

        # سواری: اوپن فعلی و قبلی هر دو بالای UB خودشان
        ride = False
        if (
            np.isfinite(cur_ub) and cur_ub > 0 and cur_o > cur_ub * 1.002
            and np.isfinite(prev_ub) and prev_ub > 0 and prev_o > prev_ub * 1.001
        ):
            ride = True
        # یا ۳ از ۴ کندل اخیر اوپن بالای UB
        above_n = 0
        look = min(5, len(df) - 1)
        for j in range(-look, 0):
            row = df.iloc[j]
            o, ub = float(row["o"]), float(row["upper"])
            if np.isfinite(ub) and ub > 0 and o > ub * 1.001:
                above_n += 1
        if above_n >= 3:
            ride = True

        # منشأ داخل باند: در ۱–۶ کندل قبل اوپنی زیر UB (ترجیحاً زیر mid)
        inside = False
        for j in range(-2, max(-7, -len(df)), -1):
            row = df.iloc[j]
            o = float(row["o"])
            ub = float(row["upper"])
            lb = float(row["lower"])
            if not np.isfinite(ub) or ub <= 0:
                continue
            if o <= ub * 0.999:
                inside = True
                break
            if np.isfinite(lb) and o < (ub + lb) / 2:
                inside = True
                break

        if ride:
            out["ride"] = True
            out["tags"].append("🟠سواری‌UB")
        elif inside:
            out["inside"] = True
            out["tags"].append("📥از‌داخل‌باند")
        elif np.isfinite(prev_ub) and prev_o <= prev_ub:
            out["inside"] = True
            out["tags"].append("📥از‌داخل‌باند")
    except Exception as e:
        log.debug("gap_context: %s", e)
    return out


def check_signal(df: pd.DataFrame, symbol: str, tf: str):
    if len(df) < BB_PERIOD + 2:
        return None
    cur = df.iloc[-1]
    open_p = float(cur["o"])
    high = float(cur["h"])
    low = float(cur["l"])
    close = float(cur["c"])
    if is_erratic_wick_candle(cur):
        stats["skip_no_gap"] += 1
        log.info("SKIP_ERRATIC %s %s | wick-spike candle", symbol, tf)
        return None
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

    # HTF structure later in process_symbol
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
        "struct_tf": None,
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
    """کپشن سیگنال: فقط ارز / % تا باند / سل|لانگ / TF."""
    emoji = "🔴" if sig["side"] == "SELL" else "🟢"
    side_fa = "سِل" if sig["side"] == "SELL" else "لانگ"
    pen = sig.get("diff_pct")
    if pen is None:
        pen = sig.get("upper_pen") if sig["side"] == "SELL" else sig.get("lower_pen")
    if pen is None:
        pen = 0.0
    symbol = sig["symbol"]
    link = lbank_futures_link(symbol, sig["tf"])
    badge = inno_badge(symbol)
    # یک خط جمع‌وجور برای کپشن عکس
    return (
        f'{emoji} <b>{side_fa}</b> · <a href="{link}"><b>{symbol}</b></a>{badge}\n'
        f'⏱ <b>{sig["tf"]}</b> · 📏 <b>{pen:.2f}%</b> تا {"UB" if sig["side"] == "SELL" else "LB"}'
    )

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



# ========================= LBank Futures Trade Client =========================
def _perp_sign(params: dict) -> tuple[dict, dict]:
    """امضای HmacSHA256 طبق داک قرارداد ال‌بانک."""
    ts = str(int(time.time() * 1000))
    echo = secrets.token_hex(16)  # 32 chars
    payload = dict(params)
    payload["api_key"] = LBANK_API_KEY
    payload["timestamp"] = ts
    payload["signature_method"] = "HmacSHA256"
    payload["echostr"] = echo
    # sort by key
    items = sorted((k, str(v)) for k, v in payload.items() if v is not None and k != "sign")
    query = "&".join(f"{k}={v}" for k, v in items)
    md5_upper = hashlib.md5(query.encode("utf-8")).hexdigest().upper()
    sign = hmac.new(
        LBANK_API_SECRET.encode("utf-8"),
        md5_upper.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest().lower()
    payload["sign"] = sign
    headers = {
        "Content-Type": "application/json",
        "timestamp": ts,
        "signature_method": "HmacSHA256",
        "echostr": echo,
    }
    return payload, headers


def _perp_post(path: str, params: dict) -> dict | None:
    if not LBANK_API_KEY or not LBANK_API_SECRET:
        log.warning("AUTO_TRADE: API key/secret missing")
        return None
    body, headers = _perp_sign(params)
    url = PERP_BASE.rstrip("/") + path
    try:
        r = requests.post(url, json=body, headers=headers, timeout=15)
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text, "status_code": r.status_code}
        log.info("PERP POST %s -> %s %s", path, r.status_code, str(data)[:300])
        return data if isinstance(data, dict) else {"data": data}
    except Exception as e:
        log.warning("PERP POST %s error: %s", path, e)
        return None


def trade_qty_from_margin(symbol: str, price: float) -> float | None:
    """حجم پایه از مارجین ۱ USDT و اهرم ۱۰ → notional = margin * leverage."""
    if not price or price <= 0:
        return None
    notional = TRADE_MARGIN_USDT * TRADE_LEVERAGE  # e.g. 10 USDT
    qty = notional / price
    # گرد کردن ساده
    if qty >= 1:
        qty = float(f"{qty:.4f}")
    elif qty >= 0.01:
        qty = float(f"{qty:.6f}")
    else:
        qty = float(f"{qty:.8f}")
    return qty if qty > 0 else None


def tp_price_for_sell(entry: float) -> float:
    """سِل: سود ۳۰٪ روی مارجین با اهرم L → حرکت قیمت ≈ TP/L."""
    move = (TRADE_TP_PCT / 100.0) / max(TRADE_LEVERAGE, 1)
    return entry * (1.0 - move)


def open_sell_trade(symbol: str, entry_price: float, tf: str, candle_ts: int | None = None) -> dict | None:
    """سِل ۱U @ اهرم ۱۰. میانگین فقط روی کندل بعدی (یک پله در هر candle_ts)."""
    if not AUTO_TRADE:
        return None
    entry_price = float(entry_price)
    if entry_price <= 0:
        return None

    # پوزیشن باز هم‌نماد؟
    existing = None
    for t in open_trades:
        if t.get("symbol") == symbol and t.get("status") == "open" and t.get("side") == "SELL":
            existing = t
            break

    if existing is not None:
        if not TRADE_AVG_DOWN:
            log.info("AUTO_TRADE skip duplicate open %s", symbol)
            return None
        adds = int(existing.get("adds") or 0)
        if adds >= TRADE_MAX_ADDS:
            log.info("AUTO_TRADE max adds reached %s (%d)", symbol, adds)
            return None
        # فقط یک پله در هر کندل (۱۵م / ۱س / …) — نه چندبار داخل همان کندل
        last_cts = int(existing.get("last_leg_candle_ts") or existing.get("entry_candle_ts") or 0)
        if candle_ts is not None:
            try:
                cts = int(candle_ts)
            except Exception:
                cts = 0
            if cts and last_cts and cts <= last_cts:
                log.info(
                    "AUTO_TRADE skip avg same-candle %s cts=%s last=%s",
                    symbol, cts, last_cts,
                )
                return None
        # فقط اگر در ضرر (برای سِل: قیمت فعلی بالاتر از میانگین ورود)
        avg = float(existing.get("entry") or 0)
        if avg > 0 and entry_price <= avg * 1.001:
            log.info("AUTO_TRADE skip avg %s (not in loss entry=%.6g px=%.6g)", symbol, avg, entry_price)
            return None
        add_qty = trade_qty_from_margin(symbol, entry_price)
        if not add_qty:
            return None
        old_qty = float(existing.get("qty") or 0)
        old_margin = float(existing.get("margin") or TRADE_MARGIN_USDT)
        new_qty = old_qty + add_qty
        new_margin = old_margin + TRADE_MARGIN_USDT
        # میانگین وزنی ورود
        new_entry = (avg * old_qty + entry_price * add_qty) / new_qty if new_qty > 0 else entry_price
        new_tp = tp_price_for_sell(new_entry)
        existing["entry"] = float(new_entry)
        existing["qty"] = float(new_qty)
        existing["margin"] = float(new_margin)
        existing["tp_price"] = float(new_tp)
        existing["adds"] = adds + 1
        existing["last_add_at"] = int(time.time())
        existing["last_add_px"] = float(entry_price)
        existing["tf"] = tf
        if candle_ts:
            existing["last_leg_candle_ts"] = int(candle_ts)
        legs = existing.get("legs") or []
        legs.append({
            "px": entry_price, "qty": add_qty, "ts": int(time.time()),
            "tf": tf, "candle_ts": int(candle_ts) if candle_ts else None,
        })
        existing["legs"] = legs[-20:]
        if not (AUTO_TRADE_DRY_RUN or not LBANK_API_KEY):
            # تلاش سفارش واقعی اضافه
            sym = symbol.upper()
            _perp_post("/cfd/openApi/v1/prv/placeOrder", {
                "symbol": sym,
                "productGroup": "SwapU",
                "side": "sell",
                "orderType": "market",
                "volume": add_qty,
                "leverage": TRADE_LEVERAGE,
                "positionSide": "short",
            })
        tag = "DRY" if existing.get("dry_run") or AUTO_TRADE_DRY_RUN or not LBANK_API_KEY else "LIVE"
        send_telegram_text(
            f"📉 <b>میانگین سِل ({tag})</b> {symbol}\n"
            f"پله {existing['adds']}+۱ · این ورود <code>{entry_price:.6g}</code>\n"
            f"میانگین جدید <code>{new_entry:.6g}</code> · مارجین کل <b>{new_margin:.0f}U</b>\n"
            f"حجم کل ≈ <code>{new_qty}</code> · TP جدید <code>{new_tp:.6g}</code>\n"
            f"(هدف همچنان ۳۰٪ سود روی مارجین کل)"
        )
        log.info(
            "AUTO_TRADE AVG %s adds=%d entry=%.6g tp=%.6g margin=%.1f",
            symbol, existing["adds"], new_entry, new_tp, new_margin,
        )
        save_state()
        try:
            github_state_push(force=True)
        except Exception:
            pass
        return existing

    # پوزیشن جدید
    qty = trade_qty_from_margin(symbol, entry_price)
    if not qty:
        log.warning("AUTO_TRADE bad qty %s px=%s", symbol, entry_price)
        return None
    tp = tp_price_for_sell(entry_price)
    rec = {
        "symbol": symbol,
        "side": "SELL",
        "tf": tf,
        "entry": float(entry_price),
        "qty": float(qty),
        "margin": float(TRADE_MARGIN_USDT),
        "leverage": TRADE_LEVERAGE,
        "tp_price": float(tp),
        "tp_pct_margin": TRADE_TP_PCT,
        "opened_at": int(time.time()),
        "status": "open",
        "dry_run": AUTO_TRADE_DRY_RUN or not LBANK_API_KEY,
        "order_id": None,
        "adds": 0,
        "entry_candle_ts": int(candle_ts) if candle_ts else 0,
        "last_leg_candle_ts": int(candle_ts) if candle_ts else 0,
        "legs": [{"px": entry_price, "qty": qty, "ts": int(time.time()), "tf": tf,
                  "candle_ts": int(candle_ts) if candle_ts else None}],
    }
    if AUTO_TRADE_DRY_RUN or not LBANK_API_KEY:
        rec["order_id"] = f"DRY-{symbol}-{int(time.time())}"
        open_trades.append(rec)
        send_telegram_text(
            f"🤖 <b>اتوترید سِل (DRY)</b>\n"
            f"<b>{symbol}</b> · {tf}\n"
            f"ورود ≈ <code>{entry_price:.6g}</code>\n"
            f"حجم ≈ <code>{qty}</code> · مارجین {TRADE_MARGIN_USDT}U · اهرم {TRADE_LEVERAGE}x\n"
            f"TP ≈ <code>{tp:.6g}</code> (۳۰٪ روی مارجین)\n"
            f"میانگین در ضرر: روشن · بدون استاپ"
        )
        log.info("AUTO_TRADE DRY open SELL %s qty=%s entry=%s tp=%s", symbol, qty, entry_price, tp)
        save_state()
        return rec

    _perp_post("/cfd/openApi/v1/prv/setLeverage", {
        "symbol": symbol if symbol.endswith("USDT") else symbol + "USDT",
        "leverage": TRADE_LEVERAGE,
        "productGroup": "SwapU",
    })
    sym = symbol.upper()
    if not sym.endswith("USDT"):
        sym = sym + "USDT"
    data = _perp_post("/cfd/openApi/v1/prv/placeOrder", {
        "symbol": sym,
        "productGroup": "SwapU",
        "side": "sell",
        "orderType": "market",
        "volume": qty,
        "leverage": TRADE_LEVERAGE,
        "positionSide": "short",
    })
    ok = False
    oid = None
    if isinstance(data, dict):
        if data.get("result") in (True, "true", "True") or data.get("error_code") in (0, "0"):
            ok = True
            d = data.get("data") or {}
            oid = d.get("orderId") or d.get("order_id") or d.get("id")
        else:
            log.warning("AUTO_TRADE live place failed: %s", data)
            send_telegram_text(
                f"⚠️ اتوترید زنده ناموفق برای <b>{symbol}</b>\n"
                f"<code>{str(data)[:400]}</code>"
            )
            return None
    if not ok:
        return None
    rec["order_id"] = oid or f"LIVE-{symbol}-{int(time.time())}"
    rec["dry_run"] = False
    open_trades.append(rec)
    send_telegram_text(
        f"🤖 <b>اتوترید سِل باز شد</b>\n"
        f"<b>{symbol}</b> · qty={qty} · entry≈{entry_price:.6g}\n"
        f"TP≈{tp:.6g} · اهرم {TRADE_LEVERAGE}x · مارجین {TRADE_MARGIN_USDT}U"
    )
    save_state()
    return rec



def close_sell_trade(rec: dict, px: float, reason: str) -> None:
    rec["status"] = "closed"
    rec["closed_at"] = int(time.time())
    rec["close_px"] = float(px)
    rec["close_reason"] = reason
    entry = float(rec["entry"])
    lev = float(rec.get("leverage") or TRADE_LEVERAGE)
    margin = float(rec.get("margin") or TRADE_MARGIN_USDT)
    # ROI روی مارجین برای سِل
    price_move_pct = (entry - px) / entry * 100.0 if entry else 0.0
    margin_roi = price_move_pct * lev
    pnl_usdt = margin * (margin_roi / 100.0)
    rec["margin_roi"] = margin_roi
    rec["pnl_usdt"] = pnl_usdt
    trade_history.append(dict(rec))
    if len(trade_history) > 500:
        del trade_history[:-400]
    tag = "DRY" if rec.get("dry_run") or not LBANK_API_KEY else "LIVE"
    if rec.get("dry_run") or not LBANK_API_KEY:
        send_telegram_text(
            f"✅ <b>کلوز سِل ({tag})</b> {rec['symbol']}\n"
            f"ورود {entry:.6g} → خروج {px:.6g}\n"
            f"ROI مارجین ≈ <b>{margin_roi:.1f}%</b> · PnL ≈ <b>{pnl_usdt:+.3f}U</b> · {reason}"
        )
        log.info("AUTO_TRADE %s close %s roi=%.1f%% pnl=%.3fU", tag, rec["symbol"], margin_roi, pnl_usdt)
        save_state()
        return
    sym = rec["symbol"].upper()
    data = _perp_post("/cfd/openApi/v1/prv/closePosition", {
        "symbol": sym,
        "productGroup": "SwapU",
        "side": "buy",  # close short
        "orderType": "market",
        "volume": rec.get("qty"),
        "positionSide": "short",
    })
    send_telegram_text(
        f"✅ <b>کلوز سِل (LIVE)</b> {rec['symbol']}\n"
        f"ورود {entry:.6g} → خروج {px:.6g}\n"
        f"ROI مارجین ≈ <b>{margin_roi:.1f}%</b> · PnL ≈ <b>{pnl_usdt:+.3f}U</b> · {reason}\n"
        f"<code>{str(data)[:200] if data else 'no resp'}</code>"
    )



def trade_report_stats() -> dict:
    """جمع سود/ضرر فرضی از تاریخچه + شناور باز."""
    closed = [t for t in trade_history if t.get("status") == "closed"]
    wins = [t for t in closed if float(t.get("pnl_usdt") or 0) > 0]
    losses = [t for t in closed if float(t.get("pnl_usdt") or 0) < 0]
    profit = sum(float(t.get("pnl_usdt") or 0) for t in wins)
    loss = sum(float(t.get("pnl_usdt") or 0) for t in losses)  # منفی
    net_closed = profit + loss
    # شناور باز
    unreal = 0.0
    open_n = 0
    for rec in open_trades:
        if rec.get("status") != "open" or rec.get("side") != "SELL":
            continue
        open_n += 1
        px = get_futures_last(rec["symbol"])
        if not px or px <= 0:
            continue
        entry = float(rec["entry"])
        lev = float(rec.get("leverage") or TRADE_LEVERAGE)
        margin = float(rec.get("margin") or TRADE_MARGIN_USDT)
        price_move_pct = (entry - px) / entry * 100.0 if entry else 0.0
        unreal += margin * (price_move_pct * lev / 100.0)
    open_rows = []
    for rec in open_trades:
        if rec.get("status") != "open" or rec.get("side") != "SELL":
            continue
        px = get_futures_last(rec["symbol"])
        entry = float(rec.get("entry") or 0)
        lev = float(rec.get("leverage") or TRADE_LEVERAGE)
        margin = float(rec.get("margin") or TRADE_MARGIN_USDT)
        u = 0.0
        if px and px > 0 and entry > 0:
            price_move_pct = (entry - float(px)) / entry * 100.0
            u = margin * (price_move_pct * lev / 100.0)
        open_rows.append({
            "symbol": rec.get("symbol"),
            "entry": entry,
            "tp": float(rec.get("tp_price") or 0),
            "margin": margin,
            "adds": int(rec.get("adds") or 0),
            "unreal": u,
            "px": float(px) if px else 0.0,
        })
    return {
        "closed_n": len(closed),
        "win_n": len(wins),
        "loss_n": len(losses),
        "profit": profit,
        "loss": loss,
        "net_closed": net_closed,
        "open_n": open_n,
        "unreal": unreal,
        "net_all": net_closed + unreal,
        "open_rows": open_rows,
    }


def maybe_send_hourly_trade_report(force: bool = False) -> None:
    """اولین گزارش ~۱۰ دقیقه بعد از استارت؛ بعد هر ۱ ساعت."""
    global _last_trade_report_ts
    if not AUTO_TRADE:
        return
    now = int(time.time())
    FIRST_DELAY = 600
    INTERVAL = 3600
    if not force:
        if _last_trade_report_ts == 0:
            _last_trade_report_ts = now
            log.info("AUTO_TRADE report timer armed (first in %ds)", FIRST_DELAY)
            save_state()
            return
        elapsed = now - _last_trade_report_ts
        # بعد از arm یا بعد از گزارش قبلی: حداقل FIRST_DELAY برای اول، INTERVAL برای بعدی
        # اگر trade_history خالی و open خالی و هنوز در پنجرهٔ اول هستیم از arm
        # ساده‌ترین قانون پایدار:
        #   همیشه INTERVAL بین دو ارسال؛ ولی اولین arm فقط FIRST_DELAY صبر می‌کند
        # با متغیر کمکی در state سخت است — از INTERVAL استفاده می‌کنیم برای همه
        # و FIRST_DELAY فقط برای arm اولیه:
        #   arm: _last=now
        #   if elapsed < INTERVAL: return  → اولین بعد از ۱ ساعت می‌آید
        # کاربر گزارش می‌خواست هر ۱ ساعت — INTERVAL=3600 کافی است
        # برای اینکه زودتر بفهمد روشن است: FIRST_DELAY برای بار اول
        # تشخیص بار اول: اگر در trade_history هیچ چیزی با key report_sent نباشد
        # استفاده از: اگر elapsed >= FIRST_DELAY و _trade_reports_sent == 0
        if not hasattr(maybe_send_hourly_trade_report, "_sent_count"):
            maybe_send_hourly_trade_report._sent_count = 0
        sent = maybe_send_hourly_trade_report._sent_count
        need = FIRST_DELAY if sent == 0 else INTERVAL
        if elapsed < need:
            return

    st = trade_report_stats()
    mode = "DRY" if AUTO_TRADE_DRY_RUN or not LBANK_API_KEY else "LIVE"
    try:
        iran = datetime.now(TEHRAN).strftime("%H:%M")
    except Exception:
        iran = datetime.utcnow().strftime("%H:%M UTC")
    if st["closed_n"] == 0 and st["open_n"] == 0:
        msg = (
            f"📊 <b>گزارش ساعتی اتوترید ({mode})</b>\n"
            f"تا این لحظه معامله‌ای ثبت نشده.\n"
            f"⏰ {iran} ایران"
        )
    else:
        lines = [
            f"📊 <b>گزارش ساعتی اتوترید ({mode})</b>",
            "━━━━━━━━━━━━━━━━",
            f"بسته‌شده: <b>{st['closed_n']}</b> (✅{st['win_n']} · ❌{st['loss_n']})",
            f"سود تحقق‌یافته: <b>+{st['profit']:.3f}U</b>",
            f"ضرر تحقق‌یافته: <b>{st['loss']:.3f}U</b>",
            f"خالص بسته: <b>{st['net_closed']:+.3f}U</b>",
            f"باز: <b>{st['open_n']}</b> · شناور: <b>{st['unreal']:+.3f}U</b>",
            f"خالص کل تا الان: <b>{st['net_all']:+.3f}U</b>",
        ]
        rows = st.get("open_rows") or []
        if rows:
            lines.append("────────")
            lines.append("📌 <b>ارزهای شناور (آماده میانگین):</b>")
            for r in rows[:20]:
                ad = int(r.get("adds") or 0)
                leg = f"پله {ad + 1}" if ad else "پله ۱"
                lines.append(
                    f"• <b>{r['symbol']}</b> {leg} · میانگین <code>{r['entry']:.6g}</code> "
                    f"· مارجین {r['margin']:.0f}U · uPnL <b>{r['unreal']:+.2f}U</b>"
                )
        lines.append("━━━━━━━━━━━━━━━━")
        lines.append(f"⏰ {iran} ایران")
        msg = "\n".join(lines)

    send_telegram_text(msg)
    _last_trade_report_ts = now
    if not hasattr(maybe_send_hourly_trade_report, "_sent_count"):
        maybe_send_hourly_trade_report._sent_count = 0
    maybe_send_hourly_trade_report._sent_count += 1
    save_state()
    log.info(
        "HOURLY_TRADE_REPORT #%d net=%.3f closed=%d open=%d",
        maybe_send_hourly_trade_report._sent_count, st["net_all"], st["closed_n"], st["open_n"],
    )



def manage_open_trades() -> None:
    """هر دور: اگر قیمت به TP سِل رسید کلوز کن (بدون استاپ)."""
    if not AUTO_TRADE or not open_trades:
        return
    for rec in list(open_trades):
        if rec.get("status") != "open":
            continue
        if rec.get("side") != "SELL":
            continue
        sym = rec["symbol"]
        px = get_futures_last(sym)
        if not px or px <= 0:
            continue
        tp = float(rec.get("tp_price") or 0)
        if tp > 0 and px <= tp:
            close_sell_trade(rec, px, "TP 30% مارجین")
    # پاکسازی بسته‌شده‌های قدیمی از رم (state جدا ذخیره می‌شود)
    open_trades[:] = [t for t in open_trades if t.get("status") == "open" or (int(time.time()) - int(t.get("closed_at") or 0) < 3600)]



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
        try:
            px = float(sig.get("fut_last") or sig["open"])
            st, st_df, st_tf = await structure_for_signal(symbol, tf, px)
            sig["struct_risk"] = st.get("risk")
            sig["struct_risk_fa"] = st.get("risk_fa")
            sig["struct_risk_emoji"] = st.get("risk_emoji")
            sig["struct_res_price"] = st.get("res_price")
            sig["struct_res_dist_pct"] = st.get("res_dist_pct")
            sig["struct_res_kind"] = st.get("res_kind")
            sig["struct_note"] = st.get("note")
            sig["struct_tf"] = st_tf or st.get("struct_tf")
            sig["_st"] = st
            sig["_st_df"] = st_df
        except Exception as e:
            log.debug("HTF structure %s %s: %s", symbol, tf, e)
        caption = format_signal_message(sig)
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
        st_df = sig.pop("_st_df", None)
        st_obj = sig.pop("_st", None) or {}
        st_tf = sig.get("struct_tf") or tf
        # یک پیام: شات خط‌روند/کانال + کپشن خلاصه (بدون متن جدا)
        sent = False
        if SEND_CHART:
            try:
                chart_df = st_df if st_df is not None and len(st_df) >= 30 else df
                chart_tf = st_tf if st_df is not None and len(st_df) >= 30 else tf
                chart_st = st_obj or {}
                if not (chart_st.get("tls") or chart_st.get("tl") or chart_st.get("channel")):
                    try:
                        px = float(chart_df.iloc[-1]["c"])
                        chart_st = analyze_structure(chart_df, px) or {}
                    except Exception:
                        pass
                st_img = make_structure_chart(chart_df, symbol, chart_tf, chart_st)
                if st_img:
                    send_telegram_photo(st_img, caption)
                    sent = True
                else:
                    log.warning("NO_CHART %s %s (structure chart failed)", symbol, tf)
            except Exception as e:
                log.warning("structure chart send: %s", e)
        if not sent:
            send_telegram_text(caption)
        # اتوترید: فقط سِل
        try:
            if AUTO_TRADE and sig.get("side") == "SELL":
                entry = float(sig.get("fut_last") or sig.get("open") or 0)
                if entry > 0:
                    open_sell_trade(
                        symbol, entry, tf,
                        candle_ts=sig.get("candle_ts"),
                    )
        except Exception as e:
            log.warning("AUTO_TRADE open error %s: %s", symbol, e)
        await asyncio.sleep(0.05)



def price_aligned_with_klines(symbol: str, kline_close: float, max_drift_pct: float = 8.0):
    """
    قیمتی که با کندل‌های BB هم‌مقیاس باشد.
    اگر last فیوچرز با close اسپات اختلاف وحشتناک داشت (مثل RAM 8.6 در برابر 0.033)
    فیوچرز را دور می‌ریزیم و فقط close کندل را برمی‌گردانیم.
    """
    if not kline_close or kline_close <= 0:
        return kline_close, "kline", None
    fut = get_futures_last(symbol)
    if not fut or fut <= 0:
        return kline_close, "kline", None
    drift = abs(fut - kline_close) / kline_close * 100.0
    if drift > max_drift_pct:
        return kline_close, "kline_reject_fut", drift
    return fut, "futures", drift


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
                lb = float(cur["lower"]) if "lower" in cur.index or "lower" in df.columns else float("nan")
                if np.isnan(ub) or ub <= 0:
                    return
                px = float(cur["c"])
                check_px, px_src, drift = price_aligned_with_klines(symbol, px, max_drift_pct=8.0)
                if px_src == "kline_reject_fut":
                    log.info(
                        "PRE_SKIP_DRIFT %s %s | src=%s close=%.6g fut_drift=%.1f%%",
                        symbol, tf, src, px, drift or 0,
                    )
                side = None
                gap_pct = 0.0
                if check_px > ub:
                    gap_pct = (check_px - ub) / check_px * 100.0
                    if gap_pct >= PRE_ALERT_MIN_PCT:
                        side = "SELL"
                elif (not np.isnan(lb)) and lb > 0 and check_px < lb:
                    gap_pct = (lb - check_px) / check_px * 100.0 if check_px > 0 else 0.0
                    if gap_pct >= PRE_ALERT_MIN_PCT:
                        side = "LONG"
                if not side:
                    return

                sent_pre_alerts.add(key)
                if len(sent_pre_alerts) > 3000:
                    sent_pre_alerts.clear()
                try:
                    st, _, _ = await structure_for_signal(symbol, tf, float(check_px))
                except Exception:
                    st = analyze_structure(df, float(check_px))
                async with lock:
                    found[tf].append((symbol, gap_pct, side, st or {}))
                log.info(
                    "PRE_ALERT %s %s %s +%.2f%% left=%ds",
                    side, symbol, tf, gap_pct, secs_left,
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
        # فقط شات + کپشن خلاصه برای هر مورد (بدون لیست متنی جدا)
        for item in items[:8]:
            symbol = item[0]
            gap_pct = item[1]
            side = item[2] if len(item) > 2 and item[2] in ("SELL", "LONG") else "SELL"
            st = item[3] if len(item) > 3 else {}
            try:
                if not SEND_CHART:
                    continue
                df2, _ = await fetch_klines(symbol, tf, size=100)
                if df2 is None or len(df2) < 30:
                    continue
                px = float(df2.iloc[-1]["c"])
                st2, st_df, st_tf = await structure_for_signal(symbol, tf, px)
                st_use = st2 or st or {}
                chart_df = st_df if st_df is not None and len(st_df) >= 30 else df2
                chart_tf = st_tf or tf
                if not (st_use.get("tls") or st_use.get("tl") or st_use.get("channel")):
                    try:
                        st_use = analyze_structure(chart_df, px) or {}
                    except Exception:
                        pass
                st_img = make_structure_chart(chart_df, symbol, chart_tf, st_use)
                emoji = "🔴" if side == "SELL" else "🟢"
                side_fa = "سِل" if side == "SELL" else "لانگ"
                band = "UB" if side == "SELL" else "LB"
                link = lbank_futures_link(symbol, tf)
                cap = (
                    f'🟣 احتمال گپ · {emoji} <b>{side_fa}</b> · '
                    f'<a href="{link}"><b>{symbol}</b></a>\n'
                    f'⏱ <b>{tf}</b> · 📏 <b>{gap_pct:.1f}%</b> تا {band}'
                )
                if st_img:
                    send_telegram_photo(st_img, cap)
                else:
                    send_telegram_text(cap)
                await asyncio.sleep(0.15)
            except Exception as e:
                log.warning("pre chart %s: %s", symbol, e)
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
    # شتاب دوباره (مثل MUBARAK سبزهای پشت‌سرهم) — نوک دوباره تیز شده
    reaccel = e5_s3 > e5_s5 * 1.05 and e5_s3 > 1.2 and ub_s3 > ub_s5 * 0.98

    touched_e10 = False
    for j in range(max(0, i - 2), i + 1):
        if float(l_s.iloc[j]) <= float(e10.iloc[j]) * (1.0 + ANGLE_CLOSE1_PCT / 100.0):
            touched_e10 = True
            break

    touch1 = touch2 = False
    for j in range(max(0, i - 1), i + 1):
        lv, e10j, e20j = float(l_s.iloc[j]), float(e10.iloc[j]), float(e20.iloc[j])
        hj = float(h_s.iloc[j])
        if e10j > 0 and lv <= e10j * (1.0 + ANGLE_CLOSE1_PCT / 100.0) and hj >= e10j * 0.998:
            touch1 = True
        if e20j > 0 and lv <= e20j * (1.0 + ANGLE_CLOSE2_PCT / 100.0) and hj >= e20j * 0.998:
            touch2 = True

    context_ok = pumped and was_elevated and not hot
    close1 = context_ok and touch1 and -0.35 <= dist10 <= 1.8 and dist20 >= -0.5 and spread >= 0.8
    close2 = (
        context_ok and touch2 and -0.5 <= dist20 <= 2.0
        and max_prior_dist10 >= 2.0 and spread >= 0.6
    )

    cup_forming = (
        pumped and was_elevated and not hot and not new_high
        and (cool_ub or cool_e5)
        and from_high <= ANGLE_MAX_FROM_HIGH + 1.0
        and spread >= ANGLE_MIN_SPREAD * 0.8
        and dist10 >= 1.0
    )

    ready_strict = (
        pumped and was_elevated and not hot and not new_high
        and (cool_ub or cool_e5)
        and ANGLE_MIN_DIST_E10 <= dist10 <= ANGLE_MAX_DIST_E10
        and dist20 >= ANGLE_MIN_DIST_E20
        and from_high <= ANGLE_MAX_FROM_HIGH
        and spread >= ANGLE_MIN_SPREAD
        and ema5 >= ema10 * 0.997
        and not touched_e10
    )

    # ★ بزنگاه: نوک UB از تیز به خم (۱–۲ کندل قبل از تاچ EMA10)
    # ub_s5 هنوز صعودی بوده، ub_s3 کند شده، قیمت بالای E10، نزدیک سقف
    ub_bend = (
        pumped
        and was_elevated
        and not touched_e10
        and not reaccel
        and not new_high
        and dist10 >= 1.0
        and dist20 >= 1.5
        and spread >= 1.2
        and from_high <= 4.5
        and ub_s5 >= 1.2
        and ub_s3 < ub_s5 * 0.88
        and ub_s3 > -0.8
        and c >= ema10 * 1.01
        and ema5 >= ema10 * 0.995
    )

    # واچ: چسبیده به نوک UB ولی هنوز تیز به بالا (ورود نه)
    near_ub = ub_v > 0 and c >= ub_v * 0.972
    watch_tip = (
        pumped
        and was_elevated
        and not touched_e10
        and near_ub
        and dist10 >= 1.2
        and spread >= 1.0
        and (hot or ub_s3 >= 1.5 or (ub_s5 >= 1.5 and ub_s3 >= ub_s5 * 0.92))
    )

    cup_ta = (
        pumped and was_elevated and not touched_e10 and not reaccel
        and dist10 >= 1.2 and dist20 >= 2.0 and spread >= 1.8
        and from_high <= 5.5 and ema5 >= ema10 * 0.995 and c >= ema10 * 1.008
        and not hot
    )
    cup_ta_15m = (
        pumped and was_elevated and not touched_e10 and not reaccel
        and dist10 >= 0.9 and dist20 >= 1.4 and spread >= 1.1
        and from_high <= 6.5 and ema5 >= ema10 * 0.992 and c >= ema10 * 1.005
        and not (e5_s3 > 3.5 and ub_s3 > 3.5)
        and (ub_bend or cool_ub or cool_e5 or ub_s3 < ub_s5 * 0.95)
    )

    ready_early = ready_strict or cup_ta or ub_bend

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
        "ub_bend": ub_bend,
        "watch_tip": watch_tip,
        "ready_early": ready_early,
        "ready_strict": ready_strict,
        "new_high": new_high,
        "hot": hot,
        "reaccel": reaccel,
        "pumped": pumped,
        "was_elevated": was_elevated,
        "candle_ts": candle_ts,
    }


def check_angle_setup(df: pd.DataFrame, symbol: str, tf: str) -> dict | None:
    """
    اولویت:
      bend  = خم شدن نوک UB (زود — بزنگاه)
      ready = آماده‌باش خنک / جام
      watch = نوک هنوز تیز — فقط واچ
    روی ۱۵م بستن زاویه (close1/2) ارسال نمی‌شود چون از نظر تو دیر است.
    """
    try:
        m = _angle_metrics(df)
        if not m:
            return None
        if m.get("ub_bend"):
            event = "bend"
        elif m.get("ready_strict") or m.get("ready_early"):
            event = "ready"
        elif tf == "15m" and m.get("cup_ta_15m"):
            event = "ready"
            m = {**m, "cup_ta": True, "ready_early": True}
        elif m.get("watch_tip"):
            event = "watch"
        elif tf != "15m" and m.get("close2"):
            event = "close2"  # فقط تایم بالاتر؛ برچسب دیر
        elif tf != "15m" and m.get("close1"):
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
    """گپ ۱س/۴س + جام / UB باز — فقط فارسی + اموجی."""
    lines = [f"⚠️ <b>زاویه</b> · {tf}"]
    for it in items:
        sym = it["symbol"]
        link = lbank_futures_link(sym, tf)
        g1 = it.get("gap_1h")
        g4 = it.get("gap_4h")
        parts = []

        if g1 is True:
            parts.append("گپ 1h ✅")
        elif g1 is False:
            parts.append("گپ 1h ❌")

        if g4 is True:
            parts.append("گپ 4h ✅")
        elif g4 is False:
            parts.append("گپ 4h ❌")

        # جام در حال شکل / خم UB  |  نوک تیز = UB باز (سواری)
        has_cup = bool(
            it.get("cup_ta")
            or it.get("cup_forming")
            or it.get("cup_ta_15m")
            or it.get("parent_cup")
            or it.get("ub_bend")
            or it.get("event") == "bend"
        )
        ub_open = bool(it.get("watch_tip") or it.get("event") == "watch")
        if ub_open:
            parts.append("UB باز 🟠")
        elif has_cup:
            parts.append("جام ✅")

        if not parts:
            parts.append("بدون داده گپ")

        lines.append(
            f'· <a href="{link}"><b>{sym}</b></a> · ' + " · ".join(parts)
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

                if ev == "watch":
                    hit["level"] = "A"  # فقط واچ
                elif ev in ("close1", "close2"):
                    hit["level"] = "A"  # دیر
                elif ev == "bend" and (hit["gap_1h"] or hit["gap_4h"]):
                    hit["level"] = "C"
                elif ev == "bend":
                    hit["level"] = "B"
                elif hit["gap_1h"] and hit["gap_4h"] and ev == "ready":
                    hit["level"] = "C"
                elif (hit["gap_1h"] or hit["gap_4h"] or hit["parent_cup"]) and ev == "ready":
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
        for it in items[:6]:
            try:
                sym = it["symbol"]
                df, _ = await fetch_klines(sym, tf, size=50)
                if df is None:
                    continue
                img = make_angle_chart(df, sym, tf, it)
                if img:
                    send_telegram_photo(img, f"⚠️ {sym} · {tf}")
                await asyncio.sleep(0.12)
            except Exception as e:
                log.debug("angle chart: %s", e)
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

    # فقط مواردی که هنوز در گزارش دور نیامده‌اند
    evaluated = []
    for x in pending_signals:
        if not x.get("evaluated"):
            continue
        if x.get("reported"):
            continue
        eid = (str(x.get("symbol")), str(x.get("tf")), int(x.get("candle_ts") or 0))
        if eid in reported_eval_ids:
            x["reported"] = True
            continue
        evaluated.append(x)

    for x in evaluated:
        x["reported"] = True
        eid = (str(x.get("symbol")), str(x.get("tf")), int(x.get("candle_ts") or 0))
        reported_eval_ids.add(eid)

    if len(reported_eval_ids) > 2000:
        # نگه داشتن آخرین‌ها
        reported_eval_ids.clear()

    sig_n = int(stats.get("signal_ok") or 0)
    tfs_label = ", ".join(tfs) if tfs else "—"
    clock = iran_now().strftime("%H:%M")

    # گزارش دور فقط یک‌بار برای این مجموعه سیگنال
    report_key = (tfs_label, tuple(sorted((x.get("symbol"), x.get("tf"), x.get("candle_ts")) for x in evaluated)))
    global _last_cycle_report_key
    if evaluated and report_key != _last_cycle_report_key:
        wr = format_winrate_report(evaluated)
        if wr:
            send_telegram_text(wr)
            _last_cycle_report_key = report_key
            log.info("cycle report sent | evaluated=%d sig_n=%d", len(evaluated), sig_n)
    elif sig_n == 0 and not evaluated:
        log.info("دور %s · سیگنالی نبود · %s (بدون پیام تلگرام)", tfs_label, clock)

    for k in stats:
        stats[k] = 0

    pending_signals[:] = [
        x for x in pending_signals
        if (not x.get("evaluated")) or (int(time.time()) - x["candle_ts"] < 48 * 3600)
    ]
    save_state()
    maybe_send_daily_report()
    try:
        manage_open_trades()
        maybe_send_hourly_trade_report()
    except Exception as _te:
        log.debug("auto-trade after cycle: %s", _te)


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
    if AUTO_TRADE:
        log.info(
            "AUTO_TRADE enabled dry=%s key=%s",
            AUTO_TRADE_DRY_RUN or not LBANK_API_KEY,
            "yes" if LBANK_API_KEY else "no",
        )
        # تایمر گزارش را مسلح کن
        maybe_send_hourly_trade_report(force=False)
    # اخبار فقط با maybe_send_daily_news (۹ صبح · فقط روز خبر) — نه هنگام استارت
    save_state()
    last_scan_key = None
    last_pre_key = None
    last_news_refresh = time.time()
    while True:
        # اتوترید: مدیریت پوزیشن + گزارش ساعتی (حتی وقتی سر کندل نیست)
        try:
            manage_open_trades()
            maybe_send_hourly_trade_report()
        except Exception as _te:
            log.debug("auto-trade loop: %s", _te)
        maybe_send_daily_report()
        try:
            maybe_send_daily_news()
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
            # قبل از شروع قفل کن تا دوبار پشت‌هم همان کندل اجرا نشود
            last_scan_key = scan_key
            await asyncio.sleep(1)
            try:
                await one_cycle(tfs)
            except Exception as e:
                log.error("Cycle error: %s", e)
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
