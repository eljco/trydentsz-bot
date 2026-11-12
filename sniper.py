"""
CS2 Sniper v3.1 - high-throughput edition
- Adaptive rate limiting (reads CSFloat's x-ratelimit headers, backs off on 429, speeds back up)
- Several price-sliced "newest" feeds + a "best_deal" feed, extra pages when the market is busy
- Float-aware pre-filter using CSFloat's own reference price -> far fewer sales-history calls
- Unverified alerts instead of silently dropping listings when sales data is rate limited
- Discord posting runs in a background thread so alerts never slow down scanning
- Optional status channel (DISCORD_WEBHOOK_STATUS) that shows exactly why listings did / didn't alert
v3.1: listings up to 2h old are checked on every feed, guns up to $50, volume-oriented profit thresholds,
      deagle added, only sales from the last 30 days count for the profit check.
"""
import json
import os
import itertools
import queue
import signal
import statistics
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import quote

import requests

# ================= CONFIG =================
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "").strip()
CSFLOAT_API_KEY = os.environ.get("CSFLOAT_API_KEY", "").strip()
SEND_TEST_ALERT = os.environ.get("SEND_TEST_ALERT", "false").lower() == "true"
STATUS_HOOK_ENV = os.environ.get("DISCORD_WEBHOOK_STATUS", "").strip()

WEBHOOKS = {
    "main": DISCORD_WEBHOOK,
    "cheap": os.environ.get("DISCORD_WEBHOOK_CHEAP", "").strip() or DISCORD_WEBHOOK,
    "knives": os.environ.get("DISCORD_WEBHOOK_KNIVES", "").strip() or DISCORD_WEBHOOK,
    "gloves": os.environ.get("DISCORD_WEBHOOK_GLOVES", "").strip() or DISCORD_WEBHOOK,
    "ouncetra": os.environ.get("DISCORD_WEBHOOK_OUNCETRA", "").strip() or DISCORD_WEBHOOK,
}

if not DISCORD_WEBHOOK or not CSFLOAT_API_KEY:
    print("Missing DISCORD_WEBHOOK or CSFLOAT_API_KEY environment variables.")
    sys.exit(1)

# --- loop timing ---
# RUN_SECONDS > 0 -> GitHub Actions mode (poll that long, exit). RUN_SECONDS = 0 -> run forever (VPS).
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "1500"))
BOUNDED = RUN_SECONDS > 0
POLL_INTERVAL = 0.5            # target seconds per full cycle; the rate limiter slows this down if needed
# Listings older than this are ignored (checked on every feed except the sticker-craft feed).
# 2 hours: catches deals that were missed or that the best_deal feed keeps surfacing.
MAX_LISTING_AGE = int(os.environ.get("MAX_LISTING_AGE_HOURS", "2")) * 60 * 60

# Status reports (why things did/didn't alert). Always-on: hourly to status channel (or every 6h to main).
# Actions mode: one report at the end of each run, only if a status webhook is set.
STATUS_HOOK = STATUS_HOOK_ENV or (DISCORD_WEBHOOK if not BOUNDED else "")
STATUS_EVERY_MIN = 60 if STATUS_HOOK_ENV else 360
FAIL_ALERT_AFTER = 0 if BOUNDED else 10

# --- feeds ---
# ONE request per poll, rotating through this plan. The limiter spreads requests evenly over CSFloat's
# rate-limit window, so we never burn the allowance. "best_deal" keeps showing already-discounted listings
# until they sell, so it finds deals with far fewer requests than "most_recent".
# (sort, min_price_cents, max_price_cents)
# Weighted towards the $0-$50 range where the liquid guns live; knives/gloves get one slice up to ~$260.
FEED_PLAN = [
    ("best_deal", 0, 200),
    ("most_recent", 0, 200),
    ("best_deal", 200, 600),
    ("most_recent", 200, 600),
    ("best_deal", 600, 1500),
    ("most_recent", 600, 1500),
    ("best_deal", 0, 1500),
]
FEED_DISABLED = set()
FEED_I = 0
# --- craft hunter: dedicated queries for listings carrying the most valuable premium stickers ---
# The bot LEARNS sticker ids from listings it sees, then every CRAFT_FEED_EVERY-th request asks CSFloat for the
# CHEAPEST listings carrying one of the top stickers (cheapest skin + expensive sticker = lowest SP).
# Crafts are rare and hard to sell, so they get a smaller share of requests than before (1 in 5).
# Sticker crafts are OFF by default: they tie up lots of money, are hard to sell and flooded the alerts.
# Set ENABLE_STICKER_ALERTS=true (env) to turn crafts, the craft hunter and "stickers ~free" alerts back on.
ENABLE_STICKER_ALERTS = os.environ.get("ENABLE_STICKER_ALERTS", "false").lower() == "true"
CRAFT_FEED_EVERY = 5 if ENABLE_STICKER_ALERTS else 0
CRAFT_TOP_N = 20                  # how many of the most valuable learned stickers to rotate through
CRAFT_MIN_STICKER_CENTS = 300     # ignore stickers worth less than $3
EXTRA_STICKER_IDS = []            # optional: add CSFloat sticker ids you always want checked, e.g. [123, 456]
STICKER_FILE = os.environ.get("STICKER_FILE", "sticker_ids.json")
EXCLUDE_SOUVENIRS = True          # souvenirs are skipped completely (no alerts of any kind)
STICKER_ITEM_MIN_VALUE = 5.0      # '~free stickers' alerts need at least this much sticker value ($)
STICKER_ITEM_MIN_DISCOUNT = 5.0   # stickered item priced this far under the CLEAN skin price = stickers ~free
MAX_SALES_CALLS_PER_POLL = 15  # cap on sales-history lookups per cycle (cached per skin for 15 min)

# --- what to look for ---
# Liquid weapons. (DEALS_ANY_WEAPON below keeps the general scanner open for every gun anyway.)
TARGET_WEAPONS = ["ak-47", "m4a4", "m4a1-s", "mac-10", "awp",
                  "glock", "usp", "desert eagle", "p250", "five-seven", "tec-9", "cz75", "p2000", "dual berettas", "r8 revolver"]
# Volume strategy: MANY small flips (~40/day at ~0.40 EUR each) instead of a few big ones.
# 150 EUR budget / 40 buys = ~$4.30 average buy, so the cap is low and cheap skins are the main target.
MAX_SKIN_PRICE = 15.00
MIN_SKIN_PRICE = 0.50
ENABLE_KNIVES_GLOVES = False      # too expensive for the budget; set True to scan them again

PREMIUM_STICKERS = [
    "katowice 2014", "katowice 2015", "cologne 2015", "krakow 2017",
    "boston 2018", "london 2018", "cologne 2016", "mlg columbus 2016",
]
# Sticker crafts are harder to sell than plain skins, so only VERY low SP (0-3.3%) is worth an alert.
STICKER_MAX_SP = 3.3
MAX_SP_BY_COUNT = {1: STICKER_MAX_SP, 2: STICKER_MAX_SP, 3: STICKER_MAX_SP, 4: STICKER_MAX_SP}
STICKER_MAX_PRICE = 300.00   # 4x old-tournament crafts cost far more than $50
# Generic craft rule: ANY gun (not only TARGET_WEAPONS) with valuable stickers applied cheaply
GENERIC_CRAFT_MIN_STICKER_VALUE = 10.0   # $ total sticker value (CSFloat reference prices)
GENERIC_CRAFT_MAX_SP = STICKER_MAX_SP               # % SP

# Charms attached to a gun are part of its value: they count in the discount AND the profit estimate.
CHARM_VALUE_FACTOR = 1.0          # share of the charm's value counted (lower it, e.g. 0.7, to be more cautious)
CHARM_FALLBACK_MAX_USD = 1.00     # if CSFloat gives no charm price, use (float est. - base) but only up to this
ALLOW_CHARMS_ON_GUNS = False
ENABLE_SKIN_PATTERNS = False
FAIR_PREMIUM_PCT = 25.0
FAIR_SLACK = 0.30

# --- general "deal" scanner ---
ENABLE_DEALS = True
DEALS_ANY_WEAPON = False         # only TARGET_WEAPONS (AK, M4s, MAC-10, AWP, pistols)
DEAL_MIN_DISCOUNT = 2.5           # % below float-adjusted reference price (cheap pre-filter, no API call)
PRED_CAP = 1.00                   # CSFloat's float-adjusted price is NEVER trusted above the base price
                                  # (it over-values extreme floats nobody actually pays for). It can only LOWER the estimate.
DEAL_MIN_SALES = 3
DEAL_UNVERIFIED_DISCOUNT = 7.0    # sales data unavailable -> still alert at this % below reference
PREMIUM_UNVERIFIED_DISCOUNT = 12.0
RETRY_MAX = 3                     # polls to wait for sales data before falling back

SELL_FEE = 0.02
# Nobody pays the full reference price: you realistically list a bit under it. All profit numbers (and the
# alert thresholds) use this undercut. Set to 0.0 to go back to "sell at full reference price".
SELL_UNDERCUT_PCT = 1.3
# How resale value is estimated. The old rule used the LOWER of CSFloat's reference price and the recent
# sales median, which under-estimates liquid skins you can really sell at/above the reference.
RESALE_CAPPED_BY_MEDIAN = False   # True = old, pessimistic behaviour
RESALE_BOOST_PCT = 0.0            # e.g. 2.0 if you routinely sell ~2% above CSFloat's reference price
MIN_STABILITY = 3
# Profit targets (USD). Goal ~ $18 (~16 EUR) per day: ~20 buys at ~$0.90 average. "GOOD" is the volume tier,
# "STRONG" is the ~$0.80+ tier you really want.
STRONG_MIN_PROFIT = 0.60
STRONG_MIN_PCT = 8.0
STRONG_MIN_STABILITY = 5
GOOD_MIN_PROFIT = 0.35
GOOD_MIN_PCT = 6.0
SALES_MAX_AGE_DAYS = 30           # only sales this recent count for avg/median/stability (old one-off sales can't fool the check)
MAX_KNIFE_PRICE = 259.00
MAX_GLOVE_PRICE = 259.00
PREMIUM_MIN_SALES = 3
PREMIUM_STRONG_PROFIT = 5.00
PREMIUM_GOOD_PROFIT = 2.00
PATTERN_DEPENDENT = []

BLUE_SKINS = [
    "ice coaled", "liquid", "blue laminate", "frontside misty", "blue phosphor", "desolate space",
    "water elemental", "blue fissure", "blueprint", "cobalt disruption", "blue ply", "undertow",
    "atheris", "fever dream", "cortex", "cyrex", "bunsen burner", "glacier mesh", "ice cap",
    "sapphire", "azure", "blue steel", "oceanic", "hydroponic",
]
OUNCETRA_MAX_PRICE = 15.00
OUNCETRA_MIN_DISCOUNT = 0.5

CHEAP_MAX_PRICE = 1.50
CHEAP_MIN_PROFIT = 0.15
BREAKEVEN_MIN_PROFIT = 0.30       # "SMALL PROFIT" tier
BREAKEVEN_MIN_STABILITY = 4

ENABLE_SKINPORT = True
SKINPORT_REFRESH = 330

CHARM_PATTERNS = {
    111, 123, 1234, 12345, 321, 1122, 2233, 2345, 6767, 67,
    69, 6969, 420, 42069, 4545, 8989, 2323, 12223,
    222, 333, 444, 555, 666, 777, 888, 999, 1337, 404,
}
SKIN_PATTERNS = {111, 123, 321, 67, 69, 420, 222, 333, 444, 555, 666, 777, 888, 999, 404}

CSFLOAT = "https://csfloat.com/api/v1"
SEEN_FILE = os.environ.get("SEEN_FILE", "seen_ids.json")
MAX_SEEN = 20000
REALERT_ON_DROP_PCT = 2.0    # a listing is re-evaluated if its price drops by at least this much

# ================= STATE =================
session = requests.Session()
SEEN = {}                    # listing_id -> [unix time, price_cents]
HISTORY_CACHE = {}           # name -> (ts, sales)
SALES_CACHE_TTL = 900
PRICE_SOURCES = {}
REJ = Counter()
HTTP = Counter()
RETRIES = {}
STATS = {"polls": 0, "listings": 0, "alerts": 0, "errors": 0, "saturated": 0, "post_fail": 0, "blocked_s": 0, "craft_polls": 0}
STICKER_DB = {}               # sticker id -> [name, reference price cents]
CRAFT_I = 0
POLL_N = 0
CRAFT_DISABLED = False
LAST_REJ = None
NEAR = []                    # best-looking listings that were rejected: (discount, name, price, reason, id)
SALES_BUDGET = 0
LAST_ALERT = 0.0
RUNNING = True
ALERTQ = queue.PriorityQueue()   # (priority, seq, job): 0 = plain deals / system, 1 = sticker crafts
_SEQ = itertools.count()
_REF_LOGGED = False


def rej(reason):
    """Count a rejection and remember the reason (used for the near-miss report)."""
    global LAST_REJ
    REJ[reason] += 1
    LAST_REJ = reason


def log(*a):
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), *a, flush=True)


def _stop(*_):
    global RUNNING
    RUNNING = False
    log("Stopping...")


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def sleep_interruptible(sec):
    end = time.time() + sec
    while RUNNING and time.time() < end:
        time.sleep(min(1, max(0, end - time.time())))


# ================= PERSISTENCE =================
def load_seen():
    try:
        with open(SEEN_FILE) as f:
            for k, v in json.load(f).items():
                SEEN[k] = v if isinstance(v, list) else [v, 0]
        log(f"Loaded {len(SEEN)} seen listing ids")
    except Exception:
        pass


def save_seen():
    try:
        if len(SEEN) > MAX_SEEN:
            for k, _ in sorted(SEEN.items(), key=lambda kv: kv[1][0])[: len(SEEN) - MAX_SEEN]:
                SEEN.pop(k, None)
        with open(SEEN_FILE, "w") as f:
            json.dump(SEEN, f)
    except Exception as e:
        log("Could not save seen file:", e)


def load_stickers():
    try:
        with open(STICKER_FILE) as f:
            STICKER_DB.update(json.load(f))
        log(f"Loaded {len(STICKER_DB)} learned sticker ids")
    except Exception:
        pass


def save_stickers():
    try:
        with open(STICKER_FILE, "w") as f:
            json.dump(STICKER_DB, f)
    except Exception as e:
        log("Could not save sticker file:", e)


def learn_stickers(listings):
    changed = False
    for l in listings:
        for st in (l.get("item") or {}).get("stickers") or []:
            n = (st.get("name") or "").lower()
            if not any(p in n for p in PREMIUM_STICKERS):
                continue
            sid = st.get("stickerId") or st.get("sticker_id")
            if not sid:
                continue
            price = (st.get("reference") or {}).get("price") or 0
            sid, old = str(sid), STICKER_DB.get(str(sid))
            if not old or (price and abs(price - old[1]) > max(50, old[1] * 0.1)):
                STICKER_DB[sid] = [st.get("name"), price]
                changed = True
    if changed:
        save_stickers()


def next_craft_id():
    global CRAFT_I
    ids = [str(i) for i in EXTRA_STICKER_IDS]
    top = sorted(((k, v) for k, v in STICKER_DB.items() if v[1] >= CRAFT_MIN_STICKER_CENTS),
                 key=lambda kv: kv[1][1], reverse=True)[:CRAFT_TOP_N]
    ids += [k for k, _ in top if k not in ids]
    if not ids:
        return None
    CRAFT_I += 1
    return ids[CRAFT_I % len(ids)]


def is_seen(lid, price_cents):
    e = SEEN.get(lid)
    if e is None:
        return False
    old = e[1] if len(e) > 1 else 0
    if price_cents and old and price_cents < old * (1 - REALERT_ON_DROP_PCT / 100.0):
        return False   # price dropped -> evaluate again
    return True


# ================= DISCORD (background thread) =================
def _post(payload, webhook):
    for _ in range(5):
        try:
            r = session.post(webhook, json=payload, timeout=10)
        except Exception as e:
            log("Discord error:", e)
            time.sleep(2)
            continue
        if r.status_code == 429:
            try:
                wait = float(r.json().get("retry_after", 2))
            except Exception:
                wait = 2
            time.sleep(wait + 0.3)
            continue
        if r.status_code >= 300:
            log(f"Discord webhook error {r.status_code}: {r.text[:200]}")
            return False
        return True
    return False


def discord_worker():
    global LAST_ALERT
    while True:
        _, _, job = ALERTQ.get()
        try:
            payload, webhook, label = job
            ok = _post(payload, webhook)
            if not ok and label:   # retry with a minimal embed (no fields) so the alert is never lost
                emb = (payload.get("embeds") or [{}])[0]
                mini = {"username": payload.get("username"), "embeds": [{k: v for k, v in {
                    "title": (emb.get("title") or "Alert")[:250], "url": emb.get("url"),
                    "description": (emb.get("description") or "")[:1500], "color": emb.get("color")}.items()
                    if v is not None}]}
                ok = _post(mini, webhook)
            if not ok and label:
                STATS["post_fail"] += 1
                log(f"!! ALERT COULD NOT BE DELIVERED: {label}")
            if ok:
                if label:
                    STATS["alerts"] += 1
                    LAST_ALERT = time.time()
                    log(f"ALERT {label}")
            time.sleep(0.4)
        finally:
            ALERTQ.task_done()


def enqueue(payload, webhook, label=None, prio=0):
    if webhook:
        ALERTQ.put((prio, next(_SEQ), (payload, webhook, label)))


def flush_queue(timeout=60):
    end = time.time() + timeout
    while ALERTQ.unfinished_tasks and time.time() < end:
        time.sleep(0.5)


def money(x):
    return f"${x:,.2f}" if x is not None else "?"


def pct(x):
    return f"{x:+.1f}%" if x is not None else "?"


def fmt_date(iso):
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%d/%m %H:%M")
    except Exception:
        return "?"


def simple_message(text, color=3447003, webhook=None):
    enqueue({"username": "trydentsz cs2 bot 🔥",
             "embeds": [{"description": text, "color": color}]}, webhook or DISCORD_WEBHOOK)


# ================= PRICE SOURCES =================
def fetch_skinport():
    r = session.get(
        "https://api.skinport.com/v1/items",
        params={"app_id": 730, "currency": "USD"},
        headers={"Accept-Encoding": "br, gzip", "User-Agent": "trydentsz-sniper/3.1"},
        timeout=40,
    )
    r.raise_for_status()
    out = {}
    for it in r.json():
        mp = it.get("min_price")
        if mp:
            out[it["market_hash_name"]] = {
                "price": float(mp),
                "qty": it.get("quantity"),
                "median": it.get("median_price"),
                "url": it.get("item_page") or "https://skinport.com",
            }
    return out


def register_source(name, fetch_fn, every):
    PRICE_SOURCES[name] = {"fetch": fetch_fn, "data": {}, "ts": 0, "every": every}


def refresh_sources():
    now = time.time()
    for name, src in PRICE_SOURCES.items():
        if now - src["ts"] < src["every"]:
            continue
        src["ts"] = now
        try:
            src["data"] = src["fetch"]()
            log(f"{name}: loaded {len(src['data'])} prices")
        except Exception as e:
            log(f"{name} refresh failed (keeping old data): {e}")


def cheapest_elsewhere(name):
    res = []
    for sname, src in PRICE_SOURCES.items():
        d = src["data"].get(name)
        if d:
            res.append((sname, d["price"], d.get("url")))
    return sorted(res, key=lambda x: x[1])


# ================= ADAPTIVE RATE LIMITING =================
def _hnum(h, *keys):
    for k in keys:
        v = h.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return None


class Limiter:
    """Spaces requests, honours x-ratelimit-* / Retry-After headers, slows down after 429s, recovers over time."""

    def __init__(self, name, base_gap):
        self.name = name
        self.base_gap = base_gap
        self.gap = base_gap
        self.blocked_until = 0.0
        self.last = 0.0
        self.remaining = None
        self.limit = None
        self.reset_ts = None
        self.streak = 0

    def wait_needed(self):
        now = time.time()
        w = max(self.blocked_until - now, self.last + self.gap - now, 0.0)
        if self.remaining is not None and self.reset_ts and self.reset_ts > now:
            window = self.reset_ts - now
            avail = self.remaining - 3            # keep a small reserve
            if avail <= 0:
                w = max(w, window)                # allowance used up -> wait for the reset
            else:
                w = max(w, self.last + window / avail - now)   # spread what is left evenly
        return w

    def update(self, r):
        now = time.time()
        h = r.headers
        rem = _hnum(h, "x-ratelimit-remaining", "ratelimit-remaining")
        lim = _hnum(h, "x-ratelimit-limit", "ratelimit-limit")
        rst = _hnum(h, "x-ratelimit-reset", "ratelimit-reset")
        if rem is not None:
            self.remaining = rem
        if lim is not None:
            self.limit = lim
        if rst is not None:
            self.reset_ts = rst if rst > 1e9 else now + rst
        if r.status_code == 429:
            ra = _hnum(h, "retry-after")
            if ra is None and self.reset_ts and self.reset_ts > now:
                ra = self.reset_ts - now
            self.blocked_until = now + min(ra or 30, 3700)
            STATS["blocked_s"] += min(ra or 30, 3700)
            self.gap = min(self.gap * 1.5, 5.0)
            self.streak = 0
            log(f"[{self.name}] 429 - paused {min(ra or 30, 3700):.0f}s")
        else:
            self.streak += 1
            if self.streak >= 40 and self.gap > self.base_gap:
                self.gap = max(self.base_gap, self.gap * 0.8)
                self.streak = 0


LIST_LIM = Limiter("listings", 0.35)
HIST_LIM = Limiter("history", 0.6)


def api_get(path, params, lim, max_wait):
    """Returns Response, or None if we'd have to wait longer than max_wait."""
    w = lim.wait_needed()
    if w > max_wait:
        return None
    if w > 0:
        sleep_interruptible(w)
    lim.last = time.time()
    r = session.get(
        f"{CSFLOAT}{path}",
        params=params,
        headers={"Authorization": CSFLOAT_API_KEY,
                 "User-Agent": "trydentsz-sniper/3.1",
                 "Accept": "application/json"},
        timeout=15,
    )
    lim.update(r)
    HTTP[r.status_code] += 1
    return r


# ================= SALES HISTORY =================
def get_sales(name, budgeted=True):
    """List of sales, or None if unavailable (budget used up / rate limited / error)."""
    global SALES_BUDGET
    c = HISTORY_CACHE.get(name)
    if c and time.time() - c[0] < SALES_CACHE_TTL:
        return c[1]
    if budgeted:
        if SALES_BUDGET <= 0:
            return None
        SALES_BUDGET -= 1
    try:
        r = api_get(f"/history/{quote(name)}/sales", None, HIST_LIM, 3 if budgeted else 15)
    except Exception as e:
        log("sales fetch failed:", e)
        return None
    if r is None or r.status_code != 200:
        return None
    try:
        data = r.json()
    except Exception:
        return None
    sales = data if isinstance(data, list) else data.get("data", [])
    if len(HISTORY_CACHE) > 1500:
        HISTORY_CACHE.clear()
    HISTORY_CACHE[name] = (time.time(), sales)
    return sales


def _sale_is_recent(s):
    """True if the sale is within SALES_MAX_AGE_DAYS (sales without a parsable date are kept)."""
    if not SALES_MAX_AGE_DAYS:
        return True
    created = s.get("created_at")
    if not created:
        return True
    try:
        dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() <= SALES_MAX_AGE_DAYS * 86400
    except Exception:
        return True


def summarize_sales(sales):
    clean = []
    for s in sales or []:
        item = s.get("item") or {}
        if item.get("stickers") or not s.get("price"):
            continue
        if not _sale_is_recent(s):
            continue
        clean.append(s)
    clean.sort(key=lambda s: s.get("created_at") or "", reverse=True)
    clean = clean[:10]
    prices = [s["price"] / 100.0 for s in clean]
    floats = [(s.get("item") or {}).get("float_value") for s in clean]
    floats = [f for f in floats if f is not None]
    if not prices:
        return None
    avg = statistics.mean(prices)
    cv = statistics.pstdev(prices) / avg if len(prices) >= 3 and avg else 0
    stability = max(0, min(10, round(10 - cv * 30)))
    return {
        "n": len(prices),
        "avg": avg,
        "median": statistics.median(prices),
        "avg_float": statistics.mean(floats) if floats else None,
        "min": min(prices), "max": max(prices),
        "stability": stability,
        "last3": [(s["price"] / 100.0, (s.get("item") or {}).get("float_value"), s.get("created_at"))
                  for s in clean[:3]],
    }


# ================= CHECKS =================
def check_pattern(pattern_num, exact_set, max_value=None, generic=True):
    if pattern_num is None:
        return False
    try:
        n = int(pattern_num)
    except (TypeError, ValueError):
        return False
    if max_value is not None and n > max_value:
        return False
    p_str = str(n)
    if n in exact_set:
        return True
    if not generic:
        return False
    if len(p_str) >= 3 and p_str == p_str[::-1]:
        return True
    if len(p_str) >= 3 and len(set(p_str)) == 1:
        return True
    return False


def check_float(v):
    if v is None:
        return None
    if 0 < v <= 0.0003:
        return f"Ultra-Low Float ({v})"
    if 0.9995 <= v <= 0.99999:
        return f"Ultra-High Float ({v})"
    return None


def check_stickers(item):
    found = []
    for st in item.get("stickers") or []:
        n = (st.get("name") or "").lower()
        if any(p in n for p in PREMIUM_STICKERS):
            found.append(st.get("name"))
    return found or None


def sticker_value(item):
    return sum(((s.get("reference") or {}).get("price") or 0) for s in item.get("stickers") or []) / 100.0


def is_souvenir(name, item):
    return bool(item.get("is_souvenir")) or name.startswith("souvenir ")


def get_base_price(listing):
    base = (listing.get("reference") or {}).get("base_price")
    return base / 100.0 if base else None


def get_pred_price(listing):
    pred = (listing.get("reference") or {}).get("predicted_price")
    return pred / 100.0 if pred else None


def sell_estimate(listing, item, price):
    """(list_price, you_receive_after_fee, profit, ref_value) if sold SELL_UNDERCUT_PCT under the reference price."""
    base = get_base_price(listing)
    if not base:
        return None
    pred = get_pred_price(listing)
    ref = min(pred, base * PRED_CAP) if pred else base
    ref_eff = ref + charm_bonus(listing, item)
    sell = ref_eff * (1 - SELL_UNDERCUT_PCT / 100.0)
    got = sell * (1 - SELL_FEE)
    return sell, got, got - price, ref_eff


def fair_price_ok(listing, price):
    base = get_base_price(listing)
    if base is None:
        return False, None
    return price <= base * (1 + FAIR_PREMIUM_PCT / 100.0) + FAIR_SLACK, base


def compute_sp(listing, item):
    for key in ("sticker_percentage", "sp"):
        v = listing.get(key, item.get(key))
        if isinstance(v, (int, float)):
            return float(v) * 100 if v <= 1 else float(v)
    price = listing.get("price")
    base = (listing.get("reference") or {}).get("base_price")
    if price is None or base is None:
        return None
    total = sum(((s.get("reference") or {}).get("price") or 0) for s in item.get("stickers") or [])
    if total <= 0:
        return None
    return max(price - base, 0) / total * 100


def charm_bonus(listing, item):
    """Value (USD, already discounted by CHARM_VALUE_FACTOR) of a charm attached to a gun."""
    keys = item.get("keychains") or []
    if not keys and item.get("keychain_pattern") is None:
        return 0.0
    val = sum(((k.get("reference") or {}).get("price") or 0) for k in keys) / 100.0
    if val <= 0:   # no charm price from CSFloat -> small, capped part of its float estimate
        base, pred = get_base_price(listing), get_pred_price(listing)
        if base and pred and pred > base:
            diff = pred - base
            if diff <= CHARM_FALLBACK_MAX_USD and diff / base <= 0.15:
                val = diff
    return val * CHARM_VALUE_FACTOR


def is_standalone_charm(name, item=None):
    return name.startswith("charm |")   # guns WITH a charm attached are still normal guns


def get_charm_pattern(item):
    if item.get("keychain_pattern") is not None:
        return item.get("keychain_pattern")
    for k in item.get("keychains") or []:
        if k.get("pattern") is not None:
            return k.get("pattern")
    return None


def is_blue_skin(name):
    n = (name or "").lower()
    return any(k in n for k in BLUE_SKINS)


def item_category(name_orig):
    n = (name_orig or "").lower()
    if "gloves" in n or "hand wraps" in n:
        return "gloves"
    if n.startswith("★"):
        return "knives"
    return "gun"


def is_fresh(listing):
    created = listing.get("created_at")
    if not created:
        return True
    try:
        dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() <= MAX_LISTING_AGE
    except Exception:
        return True


# ================= EMBED =================
def build_embed(title, listing, item, price, trigger_text, color, verdict=None):
    name = item.get("market_hash_name") or "Unknown"
    float_val = item.get("float_value")
    seed = item.get("paint_seed")
    lid = listing.get("id")
    base = get_base_price(listing)
    pred = get_pred_price(listing)
    sales = summarize_sales(get_sales(name, budgeted=False))
    others = cheapest_elsewhere(name)

    discount = ((price - base) / base * 100) if base else None
    vs_avg = ((price - sales["avg"]) / sales["avg"] * 100) if sales else None

    seller = listing.get("seller") or {}
    stats = seller.get("statistics") or {}
    seller_txt = seller.get("username") or "?"
    if stats.get("total_trades") is not None:
        seller_txt += f" ({stats['total_trades']} trades)"

    float_q = ""
    if sales and sales["avg_float"] and float_val is not None:
        r = float_val / sales["avg_float"] if sales["avg_float"] else 1
        float_q = "🟢 Float Excellent" if r < 0.75 else ("🟡 Float Average" if r < 1.25 else "🔴 Float High")

    est = sell_estimate(listing, item, price)
    head = verdict or "📌 ALERT"
    desc = [f"**{head}**"]
    line2 = []
    if base and price < base:
        line2.append(f"Save {money(base - price)}")
    if float_q:
        line2.append(float_q)
    if line2:
        desc.append(" · ".join(line2))
    if est:
        sell_p, got, prof, _ = est
        desc.append(f"💵 Sell ~{money(sell_p)} ({SELL_UNDERCUT_PCT:g}% under) → you get **{money(got)}** "
                    f"after {SELL_FEE * 100:g}% fee → **{money(prof)}** profit ({prof / price * 100:+.1f}%)")
    desc.append(f"\n📌 **Trigger:** {trigger_text}")

    fields = [
        {"name": "💰 Listing price", "value": money(price), "inline": True},
        {"name": "📊 Market price", "value": money(base), "inline": True},
        {"name": "📉 Discount", "value": pct(discount), "inline": True},
        {"name": "🎲 Float", "value": f"`{float_val:.6f}`" if float_val is not None else "?", "inline": True},
        {"name": "🌱 Seed", "value": f"`{seed}`" if seed is not None else "?", "inline": True},
        {"name": "👤 Seller", "value": seller_txt, "inline": True},
    ]
    if est:
        sell_p, got, prof, ref_v = est
        stick_note = "\n_Skin value only, stickers not counted._" if item.get("stickers") else ""
        fields.append({"name": "💵 If you sell it", "value":
                       f"Reference: {money(ref_v)}\nList at {money(sell_p)} ({SELL_UNDERCUT_PCT:g}% under)\n"
                       f"You get: **{money(got)}** after {SELL_FEE * 100:g}% fee\n"
                       f"Profit: **{money(prof)}** ({prof / price * 100:+.1f}%)" + stick_note,
                       "inline": False})
    if pred:
        note = f"\n⚠️ +{(pred / base - 1) * 100:.0f}% vs base - ignored" if base and pred > base * 1.05 else ""
        fields.append({"name": "🎯 CSFloat float est.", "value": money(pred) + note, "inline": True})
    if sales:
        fields += [
            {"name": f"📈 Avg {sales['n']} sales", "value": money(sales["avg"]), "inline": True},
            {"name": "🎲 Avg float",
             "value": f"`{sales['avg_float']:.6f}`" if sales["avg_float"] else "?", "inline": True},
            {"name": "📊 vs Avg", "value": pct(vs_avg), "inline": True},
        ]
        label = "🟢 Stable" if sales["stability"] >= 8 else ("🟡 Moderate" if sales["stability"] >= 5 else "🔴 Volatile")
        fields.append({"name": "📊 Stability",
                       "value": f"{label} {sales['stability']}/10\n"
                                f"Range {money(sales['min'])} – {money(sales['max'])} · {sales['n']} sales",
                       "inline": False})

    if others:
        lines = []
        for sname, p, url in others:
            diff = (price - p) / p * 100
            tag = "✅ listing is cheaper" if price <= p else "⚠️ cheaper here"
            link = f"[{sname}]({url})" if url else sname
            lines.append(f"{link}: **{money(p)}** ({diff:+.1f}% vs listing) {tag}")
        lines.append("_Other sites show lowest price for the skin, not float-matched._")
        fields.append({"name": "🏪 Price comparison", "value": "\n".join(lines)[:1000], "inline": False})
    else:
        fields.append({"name": "🏪 Price comparison", "value": "No other-site price found.", "inline": False})

    if sales and sales["last3"]:
        rows = []
        for i, (sp_, sf, sd) in enumerate(sales["last3"], 1):
            rel = (sp_ - price) / price * 100 if price else 0
            fl = f"`{sf:.6f}`" if sf is not None else "?"
            rows.append(f"**#{i}** {money(sp_)} · {fl} → {rel:+.0f}% vs listing · {fmt_date(sd)}")
        fields.append({"name": f"🕐 Last {len(rows)} sales (w/o stickers)", "value": "\n".join(rows), "inline": False})

    if lid:
        fields.append({"name": "🛒 BUY NOW", "value": f"[Open on CSFloat](https://csfloat.com/item/{lid})",
                       "inline": False})

    embed = {
        "title": f"🚨 {title} — {name}",
        "url": f"https://csfloat.com/item/{lid}" if lid else None,
        "description": "\n".join(desc),
        "color": color,
        "fields": fields,
        "footer": {"text": f"Cloud Sniper v3.1 • {datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M:%S')} UTC"},
    }
    for f in fields:
        f["value"] = str(f["value"])[:1024] or "?"
    embed = {k: v for k, v in embed.items() if v is not None}
    icon = item.get("icon_url")
    if icon:
        embed["thumbnail"] = {"url": f"https://community.cloudflare.steamstatic.com/economy/image/{icon}/360fx360f"}
    return {"username": "trydentsz cs2 bot 🔥", "embeds": [embed]}


# ================= LISTING EVALUATION =================
def evaluate(listing):
    """Returns (title, trigger_text, color, verdict[, channel]) or None or "RETRY"."""
    item = listing.get("item") or {}
    name_orig = item.get("market_hash_name") or ""
    name = name_orig.lower()
    price = (listing.get("price") or 0) / 100.0
    if price <= 0:
        return None
    float_val = item.get("float_value")
    skin_pattern = item.get("paint_seed")
    charm_item = is_standalone_charm(name, item)
    charm_pattern = get_charm_pattern(item)
    is_target = any(w in name for w in TARGET_WEAPONS) or charm_item
    cat = item_category(name_orig)
    if EXCLUDE_SOUVENIRS and is_souvenir(name, item):
        rej("souvenir (excluded)")
        return None
    if not ENABLE_STICKER_ALERTS and item.get("stickers"):
        rej("stickered item (sticker alerts off)")
        return None

    # 1. rare charm pattern
    if is_target and (charm_item or ALLOW_CHARMS_ON_GUNS) and charm_pattern is not None \
            and check_pattern(charm_pattern, CHARM_PATTERNS):
        return ("RARE CHARM PATTERN", f"Matched charm pattern: {charm_pattern}", 16711680, "💎 RARE CHARM")

    # 2. sticker crafts
    if cat == "gun" and not charm_item and not is_souvenir(name, item) and price <= STICKER_MAX_PRICE \
            and item.get("stickers"):
        sp = compute_sp(listing, item)
        matched = check_stickers(item) if is_target else None
        if matched:
            n_st = len(matched)
            limit = MAX_SP_BY_COUNT.get(min(n_st, max(MAX_SP_BY_COUNT)))
            if sp is None:
                rej("craft: SP not computable")
            elif sp <= limit:
                return (f"PREMIUM STICKER CRAFT ({n_st}x)",
                        f"{sp:.1f}% SP (limit {limit:.1f}%) | Stickers: {', '.join(matched)}",
                        16711680, f"🏷️ {n_st}x STICKER CRAFT")
            else:
                rej("craft: SP too high")
        sv = sticker_value(item)
        if sp is not None and sv >= GENERIC_CRAFT_MIN_STICKER_VALUE and sp <= GENERIC_CRAFT_MAX_SP:
            names = ", ".join(s.get("name", "?") for s in item.get("stickers") or [])
            return ("STICKER CRAFT (LOW SP)",
                    f"{sp:.1f}% SP on {money(sv)} of stickers | {names[:200]}",
                    16711680, "🏷️ CHEAP STICKERS")

    # 3. rare gun pattern
    if (is_target and ENABLE_SKIN_PATTERNS and not charm_item and price <= MAX_SKIN_PRICE
            and check_pattern(skin_pattern, SKIN_PATTERNS, max_value=1000, generic=False)):
        ok, base = fair_price_ok(listing, price)
        if ok:
            return ("RARE SKIN PATTERN", f"Pattern {skin_pattern} at fair price (base ${base:.2f})",
                    16711680, "🌱 RARE PATTERN")

    # 4. extreme float (any gun)
    if cat == "gun" and not charm_item and price <= MAX_SKIN_PRICE:
        msg = check_float(float_val)
        if msg:
            ok, base = fair_price_ok(listing, price)
            if ok:
                return ("EXTREME FLOAT", f"{msg} | paying ${price:.2f} vs base ${base:.2f}",
                        16711680, "🎯 EXTREME FLOAT")

    # 5a. blue gun skins
    if cat == "gun" and is_blue_skin(name) and not is_souvenir(name, item) \
            and not item.get("stickers") and not charm_item and MIN_SKIN_PRICE <= price <= OUNCETRA_MAX_PRICE:
        b = get_base_price(listing)
        if b:
            pr = get_pred_price(listing)
            d = (b - price) / b * 100
            if pr:
                r2 = min(pr, b * PRED_CAP)
                d = max(d, (r2 - price) / r2 * 100)
            if d >= OUNCETRA_MIN_DISCOUNT:
                return ("BLUE SKIN", f"{d:.1f}% below market ({money(b)})",
                        3447003, "💙 BLUE SKIN DEAL", "ouncetra")
            rej("blue skin: not below market enough")
        else:
            rej("blue skin: no base price")

    # 5. deal vs market
    premium = cat in ("knives", "gloves")
    if premium and not ENABLE_KNIVES_GLOVES:
        rej("knife/glove (disabled)")
        return None
    max_price = {"knives": MAX_KNIFE_PRICE, "gloves": MAX_GLOVE_PRICE}.get(cat, MAX_SKIN_PRICE)
    if not ENABLE_DEALS or charm_item or not (DEALS_ANY_WEAPON or is_target or premium):
        return None
    if not (MIN_SKIN_PRICE <= price <= max_price):
        rej("price out of range")
        return None
    if item.get("stickers"):
        has_st = True
    else:
        has_st = False
    if premium and any(k in name for k in PATTERN_DEPENDENT):
        return None
    base = get_base_price(listing)
    if not base:
        rej("no base price")
        return None
    pred = get_pred_price(listing)
    ref = min(pred, base * PRED_CAP) if pred else base   # float-adjusted reference
    bonus = charm_bonus(listing, item)
    ref_eff = ref + bonus                       # gun value + charm value = CSFloat "Final Price"
    disc = (ref_eff - price) / ref_eff * 100
    charm_note = f" | incl. {money(bonus)} charm bonus" if bonus > 0 else ""
    cheap = price <= CHEAP_MAX_PRICE and not premium
    if disc < DEAL_MIN_DISCOUNT:
        rej("below reference < %.1f%%" % DEAL_MIN_DISCOUNT)
        return None
    net_fast = ref_eff * (1 - SELL_UNDERCUT_PCT / 100.0) * (1 - SELL_FEE) - price
    if net_fast < (CHEAP_MIN_PROFIT if cheap else 0):
        rej("no edge after fees (pre-check)")
        return None

    channel_fallback = "cheap" if cheap else (cat if premium else "main")
    if has_st:
        sv = sticker_value(item)
        if sv < STICKER_ITEM_MIN_VALUE:
            rej("stickers too cheap to matter")
            return None
        if disc >= STICKER_ITEM_MIN_DISCOUNT and net_fast >= (CHEAP_MIN_PROFIT if cheap else GOOD_MIN_PROFIT):
            return ("STICKERS ~FREE", f"{disc:.0f}% below the CLEAN-skin reference ({money(ref)}) with "
                    f"{money(sv)} of stickers included | est. profit after fees {money(net_fast)}",
                    16766720, "🏷️ STICKERED ITEM BELOW CLEAN PRICE", channel_fallback)
        rej("has stickers (not a craft)")
        return None
    sales = get_sales(name_orig)
    if sales is None:
        need = PREMIUM_UNVERIFIED_DISCOUNT if premium else DEAL_UNVERIFIED_DISCOUNT
        if disc >= need:
            return ("DEAL (UNVERIFIED)", f"{disc:.0f}% below float-adjusted market - sales data unavailable",
                    16766720, "⚠️ UNVERIFIED DEAL", channel_fallback)
        if RETRIES.get(listing.get("id"), 0) < RETRY_MAX - 1:
            return "RETRY"
        need_profit = CHEAP_MIN_PROFIT if cheap else (PREMIUM_GOOD_PROFIT if premium else GOOD_MIN_PROFIT)
        if net_fast >= need_profit and net_fast / price * 100 >= GOOD_MIN_PCT:
            return ("DEAL (REFERENCE ONLY)",
                    f"Est. profit after fees {money(net_fast)} ({net_fast / price * 100:+.0f}%) vs CSFloat "
                    f"reference {money(ref)} | {disc:.0f}% below - no sales data, verify before buying{charm_note}",
                    16766720, "⚠️ UNVERIFIED DEAL (reference only)", channel_fallback)
        rej("no sales data + small edge")
        return None
    s = summarize_sales(sales)
    min_sales = PREMIUM_MIN_SALES if premium else DEAL_MIN_SALES
    if not s or s["n"] < min_sales:
        rej("too few recent sales")
        return None
    if s["stability"] < MIN_STABILITY:
        rej("too volatile")
        return None

    if s["median"] < price:   # sanity check: recent sales are mostly BELOW what we'd pay
        rej("recent sales median below listing price")
        return None
    resale = (min(ref, s["median"]) if RESALE_CAPPED_BY_MEDIAN else ref) * (1 + RESALE_BOOST_PCT / 100.0) + bonus
    net = resale * (1 - SELL_UNDERCUT_PCT / 100.0) * (1 - SELL_FEE) - price
    net_pct = net / price * 100
    trig = (f"Est. net profit after {SELL_UNDERCUT_PCT:g}% undercut + {SELL_FEE * 100:g}% fee: {money(net)} ({net_pct:+.0f}%) "
            f"| resale est. {money(resale * (1 - SELL_UNDERCUT_PCT / 100.0))} (recent median {money(s['median'])}) | {disc:.0f}% below market{charm_note}")
    if premium:
        if net >= PREMIUM_STRONG_PROFIT and net_pct >= STRONG_MIN_PCT \
                and s["stability"] >= STRONG_MIN_STABILITY:
            return ("GREAT DEAL", trig, 5763719, "⭐ STRONG BUY", cat)
        if net >= PREMIUM_GOOD_PROFIT and net_pct >= GOOD_MIN_PCT:
            return ("GOOD DEAL", trig, 16766720, "👍 GOOD DEAL", cat)
        rej("premium profit too low")
        return None
    if cheap:
        if net >= CHEAP_MIN_PROFIT:
            return ("GOOD DEAL", trig, 16766720, "👍 NICE DEAL (cheap item)", "cheap")
        rej("cheap profit too low")
        return None
    if net >= STRONG_MIN_PROFIT and net_pct >= STRONG_MIN_PCT and s["stability"] >= STRONG_MIN_STABILITY:
        return ("GREAT DEAL", trig, 5763719, "⭐ STRONG BUY", "main")
    if net >= GOOD_MIN_PROFIT and net_pct >= GOOD_MIN_PCT:
        return ("GOOD DEAL", trig, 16766720, "👍 GOOD DEAL", "main")
    if net >= BREAKEVEN_MIN_PROFIT and s["stability"] >= BREAKEVEN_MIN_STABILITY:
        return ("LOW MARGIN", trig, 9807270, "⚖️ SMALL PROFIT", "main")
    rej("profit too low")
    return None


# ================= POLL =================
def fetch_feed(sort, lo, hi, extra=None, tag=None):
    """Returns (listings or None, sleep_override, is_error). Waiting for the limiter is NOT an error."""
    global _REF_LOGGED
    params = {"limit": 50, "sort_by": sort, "type": "buy_now"}
    if lo:
        params["min_price"] = lo
    if hi:
        params["max_price"] = hi
    if extra:
        params.update(extra)
    try:
        r = api_get("/listings", params, LIST_LIM, 120)
    except Exception as e:
        log("Request exception:", e)
        return None, 15, True
    if r is None:
        return None, max(LIST_LIM.wait_needed(), 1), False
    if r.status_code == 429:
        return None, max(LIST_LIM.blocked_until - time.time(), 1), False
    if r.status_code == 400 and sort == "best_deal":
        FEED_DISABLED.add(sort)
        log(f"best_deal sort rejected by CSFloat ({r.text[:150]!r}) - feed disabled, using most_recent only")
        return None, 1, False
    if r.status_code == 400 and extra and "stickers" in extra:
        globals()["CRAFT_DISABLED"] = True
        log(f"CSFloat rejected the sticker-filter query ({r.text[:150]!r}) - craft hunter disabled")
        return None, 1, False
    if r.status_code != 200:
        log(f"API error {r.status_code}: {r.text[:200]!r}")
        low = r.text.lower()
        return None, (120 if ("just a moment" in low or "cloudflare" in low) else 30), True
    try:
        data = r.json()
    except Exception:
        log("Bad JSON from CSFloat")
        return None, 15, True
    batch = data if isinstance(data, list) else data.get("data", [])
    if batch and not _REF_LOGGED:
        _REF_LOGGED = True
        log("Sample reference keys:", sorted((batch[0].get("reference") or {}).keys()),
            "| rate limit headers:", {k: v for k, v in r.headers.items() if "ratelimit" in k.lower()})
    for l in batch:
        l["_feed"] = tag or sort
    if sort == "most_recent" and len(batch) >= 50 \
            and all(not is_seen(l.get("id"), l.get("price")) for l in batch if l.get("id")):
        STATS["saturated"] += 1   # whole page was new -> listings appeared faster than we can fetch
    return batch, None, False


def next_feed():
    global FEED_I
    for _ in range(len(FEED_PLAN)):
        f = FEED_PLAN[FEED_I % len(FEED_PLAN)]
        FEED_I += 1
        if f[0] not in FEED_DISABLED:
            return f
    return None


def choose_feed():
    """Mostly the rotating plan; every CRAFT_FEED_EVERY-th request is a targeted sticker query."""
    global POLL_N
    POLL_N += 1
    if CRAFT_FEED_EVERY and not CRAFT_DISABLED and POLL_N % CRAFT_FEED_EVERY == 0:
        sid = next_craft_id()
        if sid:
            STATS["craft_polls"] += 1
            return ("lowest_price", 0, int(STICKER_MAX_PRICE * 100), {"stickers": sid}, "craft")
    f = next_feed()
    return (f + (None, None)) if f else None


def poll_once():
    global SALES_BUDGET, LAST_REJ
    SALES_BUDGET = MAX_SALES_CALLS_PER_POLL
    feed = choose_feed()
    if not feed:
        return False, 60
    listings, override, is_err = fetch_feed(*feed)
    if listings is None:
        return (not is_err), override
    learn_stickers(listings)
    all_listings = {l["id"]: l for l in listings if l.get("id")}

    now = time.time()
    changed = False
    ordered = sorted(all_listings.values(), key=lambda l: l.get("created_at") or "", reverse=True)
    ordered.sort(key=lambda l: bool((l.get("item") or {}).get("stickers")))   # stable: plain skins first
    for listing in ordered:
        lid = listing["id"]
        price_cents = listing.get("price") or 0
        if is_seen(lid, price_cents):
            continue
        # Age filter on every feed except the sticker-craft hunter (stale listings are not worth a sales lookup)
        if listing.get("_feed") != "craft" and not is_fresh(listing):
            SEEN[lid] = [now, price_cents]
            changed = True
            continue
        STATS["listings"] += 1
        LAST_REJ = None
        try:
            hit = evaluate(listing)
        except Exception as e:
            log("Error evaluating listing:", repr(e))
            hit = None
        if hit == "RETRY":
            RETRIES[lid] = RETRIES.get(lid, 0) + 1
            if RETRIES[lid] < RETRY_MAX:
                rej("retry later (sales unavailable)")
                continue
            hit = None
        RETRIES.pop(lid, None)
        if not hit and LAST_REJ and price_cents <= 26000:
            b, pr = get_base_price(listing), get_pred_price(listing)
            r_ = (min(pr, b * PRED_CAP) if (pr and b) else b)
            if r_ and (r_ - price_cents / 100.0) / r_ * 100 >= 5:
                NEAR.append(((r_ - price_cents / 100.0) / r_ * 100,
                             (listing.get("item") or {}).get("market_hash_name"), price_cents / 100.0,
                             LAST_REJ, lid))
                if len(NEAR) > 60:
                    NEAR.sort(reverse=True)
                    del NEAR[15:]
        SEEN[lid] = [now, price_cents]
        changed = True
        if hit:
            title, trig, color, verdict = hit[:4]
            channel = hit[4] if len(hit) > 4 else "main"
            item = listing.get("item") or {}
            price = price_cents / 100.0
            try:
                payload = build_embed(title, listing, item, price, trig, color, verdict)
                enqueue(payload, WEBHOOKS.get(channel) or DISCORD_WEBHOOK,
                        f"[{channel}] {title} {item.get('market_hash_name')} ${price:.2f}",
                        prio=1 if item.get("stickers") else 0)
            except Exception as e:
                log("Error building alert:", repr(e))
    if len(RETRIES) > 2000:
        RETRIES.clear()
    if changed:
        save_seen()
    return True, None


# ================= STATUS =================
def status_report(final=False):
    total_req = sum(HTTP.values()) or 1
    bad = HTTP[429] + HTTP[403] + HTTP[503] + HTTP[502]
    unhealthy = STATS["polls"] > 0 and (STATS["errors"] / STATS["polls"] > 0.3 or bad / total_req > 0.2
                                        or STATS["listings"] < 30)
    hook = STATUS_HOOK or (DISCORD_WEBHOOK if unhealthy else "")
    if not hook:
        return
    now = time.time()
    last = f"{(now - LAST_ALERT) / 60:.0f} min ago" if LAST_ALERT else "none yet"
    top = "\n".join(f"• {k}: {v}" for k, v in REJ.most_common(8)) or "-"
    http = ", ".join(f"{k}: {v}" for k, v in sorted(HTTP.items())) or "-"
    NEAR.sort(reverse=True)
    near = "\n".join(f"• {d:.0f}% under ref · {n} · {money(p)} · rejected: {why} · "
                     f"[open](https://csfloat.com/item/{i})" for d, n, p, why, i in NEAR[:5]) or "-"
    NEAR.clear()
    txt = (f"{'⚠️ **UNHEALTHY** ' if unhealthy else ''}💓 **Sniper status** "
           f"({'run finished' if final else 'alive'})\n"
           f"Polls: {STATS['polls']} · new listings checked: {STATS['listings']} · "
           f"alerts: {STATS['alerts']} · last alert: {last}\n"
           f"HTTP: {http}\n"
           f"Missed-window events: {STATS['saturated']} · time paused by rate limits: {STATS['blocked_s']:.0f}s · "
           f"undeliverable alerts: {STATS['post_fail']}\n"
           f"Craft queries: {STATS['craft_polls']} · learned premium sticker ids: {len(STICKER_DB)}\n"
           f"Listings allowance: {LIST_LIM.remaining} of {LIST_LIM.limit} left, resets in "
           f"{max(0, (LIST_LIM.reset_ts or 0) - now):.0f}s · requests this run: {sum(HTTP.values())}\n"
           f"**Why listings did NOT alert:**\n{top}\n"
           f"**Best listings that were rejected (5%+ under ref):**\n{near}")
    simple_message(txt[:4000], 15158332 if unhealthy else 3447003, hook)
    REJ.clear()


# ================= MAIN LOOP =================
def main():
    log("🚀 Cloud Sniper v3.1 starting")
    load_seen()
    load_stickers()
    threading.Thread(target=discord_worker, daemon=True).start()
    if ENABLE_SKINPORT:
        register_source("Skinport", fetch_skinport, SKINPORT_REFRESH)

    if SEND_TEST_ALERT:
        for ch, hook in WEBHOOKS.items():
            simple_message(f"✅ **TEST ALERT:** sniper is alive — this is the **{ch}** channel.", 5763719, hook)
        if STATUS_HOOK_ENV:
            simple_message("✅ **TEST ALERT:** this is the **status** channel.", 5763719, STATUS_HOOK_ENV)

    fails = 0
    warned = False
    successes = 0
    last_status = time.time()
    deadline = time.time() + RUN_SECONDS if BOUNDED else None

    while RUNNING:
        if deadline and time.time() >= deadline:
            break
        loop_start = time.time()
        try:
            refresh_sources()
            ok, override = poll_once()
        except Exception as e:
            log("Unexpected error:", repr(e))
            ok, override = False, 20

        STATS["polls"] += 1
        if ok:
            successes += 1
            if warned:
                simple_message("✅ Bot recovered, polling normally again.", 5763719, STATUS_HOOK or None)
                warned = False
            fails = 0
        else:
            fails += 1
            STATS["errors"] += 1
            if FAIL_ALERT_AFTER and fails >= FAIL_ALERT_AFTER and not warned:
                simple_message(f"⚠️ {fails} polls in a row failed (rate limit / block / API down). "
                               f"Still retrying with backoff.", 15158332, STATUS_HOOK or None)
                warned = True

        if not BOUNDED and time.time() - last_status >= STATUS_EVERY_MIN * 60:
            status_report()
            last_status = time.time()

        if override:
            delay = override
        elif not ok:
            delay = min(300, POLL_INTERVAL * (2 ** min(fails, 6)))
        else:
            delay = max(0.5, POLL_INTERVAL - (time.time() - loop_start))
        end = time.time() + delay
        if deadline:
            end = min(end, deadline)
        sleep_interruptible(end - time.time())

    save_seen()
    log(f"Run finished: {STATS['polls']} polls, {successes} ok, {STATS['listings']} listings checked, "
        f"{STATS['alerts']} alerts. HTTP: {dict(HTTP)}")
    log("Why listings did not alert:", dict(REJ))
    status_report(final=True)
    flush_queue(60)
    if BOUNDED and successes == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
