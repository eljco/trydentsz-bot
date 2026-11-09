"""
CS2 Sniper - 24/7 edition
- Runs forever in a loop (no GitHub Actions cron gaps), survives errors, backs off on rate limits
- Scans CSFloat for new listings, compares against Skinport + CSFloat sale history
- Posts "FLOAT SNIPER"-style embeds to Discord
"""
import json
import os
import signal
import statistics
import sys
import time
from datetime import datetime, timezone
from urllib.parse import quote

import requests

# ================= CONFIG =================
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "").strip()
CSFLOAT_API_KEY = os.environ.get("CSFLOAT_API_KEY", "").strip()
SEND_TEST_ALERT = os.environ.get("SEND_TEST_ALERT", "false").lower() == "true"

if not DISCORD_WEBHOOK or not CSFLOAT_API_KEY:
    print("Missing DISCORD_WEBHOOK or CSFLOAT_API_KEY environment variables.")
    sys.exit(1)

# --- loop timing ---
# GitHub Actions mode: RUN_SECONDS > 0 -> poll for that long, then exit (cron starts the next run).
# Local/VPS mode: RUN_SECONDS = 0 -> run forever.
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "270"))
BOUNDED = RUN_SECONDS > 0
POLL_INTERVAL = 8            # seconds between CSFloat polls
# Max listing age to consider (10 hours). The seen-ids cache prevents repeat alerts.
MAX_LISTING_AGE = 10 * 60 * 60
HEARTBEAT_HOURS = 0 if BOUNDED else 6   # only for always-on mode
FAIL_ALERT_AFTER = 0 if BOUNDED else 10 # only for always-on mode (0 = off)

# --- what to look for ---
TARGET_WEAPONS = ["awp", "m4a4", "m4a1-s", "glock", "usp", "ak-47", "mac-10", "galil"]
MAX_SKIN_PRICE = 35.00       # USD
MIN_SKIN_PRICE = 0.10

PREMIUM_STICKERS = [
    "katowice 2014", "katowice 2015", "cologne 2015", "krakow 2017",
    "boston 2018", "london 2018", "cologne 2016", "mlg columbus 2016",
]
MAX_SP = 4.2                 # sticker crafts: max % of sticker value you pay above base

ALLOW_CHARMS_ON_GUNS = False
ENABLE_SKIN_PATTERNS = False
FAIR_PREMIUM_PCT = 25.0
FAIR_SLACK = 0.30

# --- NEW: general "great deal" scanner (like the Five-SeveN example) ---
ENABLE_DEALS = True
DEALS_ANY_WEAPON = True      # False = only TARGET_WEAPONS
DEAL_MIN_DISCOUNT_VS_BASE = 8.0   # % below CSFloat base price (cheap pre-filter)
DEAL_MIN_DISCOUNT_VS_AVG = 5.0    # % below average of last sales (must pass to alert)
DEAL_STRONG_VS_AVG = 10.0         # >= this = "STRONG BUY", otherwise "GOOD DEAL"
DEAL_MIN_SALES = 3                # need at least this many recent sales (liquidity check)

# --- price comparison sources ---
ENABLE_SKINPORT = True
SKINPORT_REFRESH = 330       # seconds (Skinport caches for 5 min, limit 8 req / 5 min)
# SkinSwap: no verified public endpoint yet - see register_source() at the bottom.

CHARM_PATTERNS = {
    111, 123, 1234, 12345, 321, 1122, 2233, 2345, 6767, 67,
    69, 6969, 420, 42069, 4545, 8989, 2323, 12223,
    222, 333, 444, 555, 666, 777, 888, 999, 1337, 404,
}
SKIN_PATTERNS = {111, 123, 321, 67, 69, 420, 222, 333, 444, 555, 666, 777, 888, 999, 404}

CSFLOAT = "https://csfloat.com/api/v1"
SEEN_FILE = os.environ.get("SEEN_FILE", "seen_ids.json")
MAX_SEEN = 5000

# ================= STATE =================
session = requests.Session()
SEEN = {}                    # listing_id -> unix time
HISTORY_CACHE = {}           # name -> (ts, sales)
PRICE_SOURCES = {}           # source name -> {"fetch": fn, "data": {}, "ts": 0, "every": secs}
STATS = {"polls": 0, "listings": 0, "alerts": 0, "errors": 0}
RUNNING = True


def log(*a):
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), *a, flush=True)


def _stop(*_):
    global RUNNING
    RUNNING = False
    log("Stopping...")


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


# ================= PERSISTENCE =================
def load_seen():
    try:
        with open(SEEN_FILE) as f:
            SEEN.update(json.load(f))
        log(f"Loaded {len(SEEN)} seen listing ids")
    except Exception:
        pass


def save_seen():
    try:
        if len(SEEN) > MAX_SEEN:
            for k, _ in sorted(SEEN.items(), key=lambda kv: kv[1])[: len(SEEN) - MAX_SEEN]:
                SEEN.pop(k, None)
        with open(SEEN_FILE, "w") as f:
            json.dump(SEEN, f)
    except Exception as e:
        log("Could not save seen file:", e)


# ================= DISCORD =================
def post_discord(payload):
    for _ in range(4):
        try:
            r = session.post(DISCORD_WEBHOOK, json=payload, timeout=10)
        except Exception as e:
            log("Discord error:", e)
            time.sleep(2)
            continue
        if r.status_code == 429:
            try:
                wait = float(r.json().get("retry_after", 2))
            except Exception:
                wait = 2
            time.sleep(wait + 0.5)
            continue
        if r.status_code >= 300:
            log(f"Discord webhook error {r.status_code}: {r.text[:200]}")
            return False
        return True
    return False


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


def simple_message(text, color=3447003):
    post_discord({"username": "trydentsz cs2 bot 🔥",
                  "embeds": [{"description": text, "color": color}]})


# ================= PRICE SOURCES =================
def fetch_skinport():
    r = session.get(
        "https://api.skinport.com/v1/items",
        params={"app_id": 730, "currency": "USD"},
        headers={"Accept-Encoding": "br, gzip", "User-Agent": "trydentsz-sniper/2.0"},
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
    """
    Add a marketplace. fetch_fn() must return {market_hash_name: {"price": float, "url": str, "qty": int|None}}
    Use this to plug in SkinSwap (or any other site) once you have its API details.
    """
    PRICE_SOURCES[name] = {"fetch": fetch_fn, "data": {}, "ts": 0, "every": every}


def refresh_sources():
    now = time.time()
    for name, src in PRICE_SOURCES.items():
        if now - src["ts"] < src["every"]:
            continue
        src["ts"] = now  # set first so failures don't hammer the API
        try:
            src["data"] = src["fetch"]()
            log(f"{name}: loaded {len(src['data'])} prices")
        except Exception as e:
            log(f"{name} refresh failed (keeping old data): {e}")


def cheapest_elsewhere(name):
    """Returns list of (source, price, url) for every source that has this item."""
    res = []
    for sname, src in PRICE_SOURCES.items():
        d = src["data"].get(name)
        if d:
            res.append((sname, d["price"], d.get("url")))
    return sorted(res, key=lambda x: x[1])


# ================= CSFLOAT HELPERS =================
def csfloat_get(path, params=None, timeout=15):
    return session.get(
        f"{CSFLOAT}{path}",
        params=params,
        headers={"Authorization": CSFLOAT_API_KEY,
                 "User-Agent": "trydentsz-sniper/2.0",
                 "Accept": "application/json"},
        timeout=timeout,
    )


def get_sales(name):
    c = HISTORY_CACHE.get(name)
    if c and time.time() - c[0] < 600:
        return c[1]
    try:
        r = csfloat_get(f"/history/{quote(name)}/sales")
        if r.status_code != 200:
            return []
        data = r.json()
        sales = data if isinstance(data, list) else data.get("data", [])
    except Exception as e:
        log("sales fetch failed:", e)
        return []
    if len(HISTORY_CACHE) > 500:
        HISTORY_CACHE.clear()
    HISTORY_CACHE[name] = (time.time(), sales)
    return sales


def summarize_sales(sales):
    """Last 10 sales without stickers -> averages, stability, last 3."""
    clean = []
    for s in sales:
        item = s.get("item") or {}
        if item.get("stickers"):
            continue
        if not s.get("price"):
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
    stability = max(0, min(10, round(10 - cv * 40)))
    return {
        "n": len(prices),
        "avg": avg,
        "avg_float": statistics.mean(floats) if floats else None,
        "min": min(prices), "max": max(prices),
        "stability": stability,
        "last3": [(s["price"] / 100.0, (s.get("item") or {}).get("float_value"), s.get("created_at"))
                  for s in clean[:3]],
    }


# ================= CHECKS (unchanged logic) =================
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


def is_souvenir(name, item):
    return bool(item.get("is_souvenir")) or name.startswith("souvenir ")


def get_base_price(listing):
    base = (listing.get("reference") or {}).get("base_price")
    return base / 100.0 if base else None


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


def is_standalone_charm(name, item=None):
    if name.startswith("charm |"):
        return True
    return bool(item) and item.get("keychain_pattern") is not None


def get_charm_pattern(item):
    if item.get("keychain_pattern") is not None:
        return item.get("keychain_pattern")
    for k in item.get("keychains") or []:
        if k.get("pattern") is not None:
            return k.get("pattern")
    return None


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
    sales = summarize_sales(get_sales(name))
    others = cheapest_elsewhere(name)

    discount = ((price - base) / base * 100) if base else None
    vs_avg = ((price - sales["avg"]) / sales["avg"] * 100) if sales else None

    seller = listing.get("seller") or {}
    stats = seller.get("statistics") or {}
    seller_txt = seller.get("username") or "?"
    if stats.get("total_trades") is not None:
        seller_txt += f" ({stats['total_trades']} trades)"

    # float quality line
    float_q = ""
    if sales and sales["avg_float"] and float_val is not None:
        r = float_val / sales["avg_float"] if sales["avg_float"] else 1
        float_q = "🟢 Float Excellent" if r < 0.75 else ("🟡 Float Average" if r < 1.25 else "🔴 Float High")

    head = verdict or "📌 ALERT"
    desc = [f"**{head}**"]
    line2 = []
    if base and price < base:
        line2.append(f"Save {money(base - price)}")
    if float_q:
        line2.append(float_q)
    if line2:
        desc.append(" · ".join(line2))
    desc.append(f"\n📌 **Trigger:** {trigger_text}")

    fields = [
        {"name": "💰 Listing price", "value": money(price), "inline": True},
        {"name": "📊 Market price", "value": money(base), "inline": True},
        {"name": "📉 Discount", "value": pct(discount), "inline": True},
        {"name": "🎲 Float", "value": f"`{float_val:.6f}`" if float_val is not None else "?", "inline": True},
        {"name": "🌱 Seed", "value": f"`{seed}`" if seed is not None else "?", "inline": True},
        {"name": "👤 Seller", "value": seller_txt, "inline": True},
    ]
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

    # price comparison across other sites
    if others:
        lines = []
        for sname, p, url in others:
            diff = (price - p) / p * 100
            tag = "✅ listing is cheaper" if price <= p else "⚠️ cheaper here"
            link = f"[{sname}]({url})" if url else sname
            lines.append(f"{link}: **{money(p)}** ({diff:+.1f}% vs listing) {tag}")
        lines.append("_Other sites show lowest price for the skin, not float-matched._")
        fields.append({"name": "🏪 Price comparison", "value": "\n".join(lines)[:1000], "inline": False})
        if price <= others[0][1]:
            fields.append({"name": "✅ Best price", "value": "Cheapest vs all tracked sites.", "inline": False})
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
        "footer": {"text": f"Cloud Sniper • 24/7 • {datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M:%S')} UTC"},
    }
    icon = item.get("icon_url")
    if icon:
        embed["thumbnail"] = {"url": f"https://community.cloudflare.steamstatic.com/economy/image/{icon}/360fx360f"}
    return {"username": "trydentsz cs2 bot 🔥", "embeds": [embed]}


# ================= LISTING EVALUATION =================
def evaluate(listing):
    """Returns (title, trigger_text, color, verdict) or None."""
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

    # 1. rare charm pattern
    if is_target and (charm_item or ALLOW_CHARMS_ON_GUNS) and charm_pattern is not None \
            and check_pattern(charm_pattern, CHARM_PATTERNS):
        return ("RARE CHARM PATTERN", f"Matched charm pattern: {charm_pattern}", 16711680, "💎 RARE CHARM")

    # 2. premium sticker craft
    if is_target and not is_souvenir(name, item) and price <= MAX_SKIN_PRICE:
        matched = check_stickers(item)
        if matched:
            sp = compute_sp(listing, item)
            if sp is not None and sp <= MAX_SP:
                return ("PREMIUM STICKER CRAFT", f"{sp:.1f}% SP | Stickers: {', '.join(matched)}",
                        16711680, "🏷️ STICKER CRAFT")

    # 3. rare gun pattern
    if (is_target and ENABLE_SKIN_PATTERNS and not charm_item and price <= MAX_SKIN_PRICE
            and check_pattern(skin_pattern, SKIN_PATTERNS, max_value=1000, generic=False)):
        ok, base = fair_price_ok(listing, price)
        if ok:
            return ("RARE SKIN PATTERN", f"Pattern {skin_pattern} at fair price (base ${base:.2f})",
                    16711680, "🌱 RARE PATTERN")

    # 4. extreme float
    if is_target and not charm_item and price <= MAX_SKIN_PRICE:
        msg = check_float(float_val)
        if msg:
            ok, base = fair_price_ok(listing, price)
            if ok:
                return ("EXTREME FLOAT", f"{msg} | paying ${price:.2f} vs base ${base:.2f}",
                        16711680, "🎯 EXTREME FLOAT")

    # 5. great deal vs market (works on any skin if DEALS_ANY_WEAPON)
    if ENABLE_DEALS and not charm_item and MIN_SKIN_PRICE <= price <= MAX_SKIN_PRICE \
            and (DEALS_ANY_WEAPON or is_target) and not item.get("stickers"):
        base = get_base_price(listing)
        if base and (base - price) / base * 100 >= DEAL_MIN_DISCOUNT_VS_BASE:
            s = summarize_sales(get_sales(name_orig))
            if s and s["n"] >= DEAL_MIN_SALES:
                below_avg = (s["avg"] - price) / s["avg"] * 100
                if below_avg >= DEAL_MIN_DISCOUNT_VS_AVG:
                    strong = below_avg >= DEAL_STRONG_VS_AVG
                    return ("GREAT DEAL",
                            f"{below_avg:.0f}% below avg sale price, {(base - price) / base * 100:.0f}% below market",
                            16766720 if not strong else 5763719,
                            "⭐ STRONG BUY" if strong else "👍 GOOD DEAL")
    return None


# ================= POLL =================
def poll_once():
    """Returns (ok, sleep_override)."""
    try:
        r = csfloat_get("/listings", params={"limit": 50, "sort_by": "most_recent", "type": "buy_now"})
    except Exception as e:
        log("Request exception:", e)
        return False, 15

    if r.status_code == 429:
        wait = float(r.headers.get("Retry-After", 60))
        log(f"Rate limited, sleeping {wait:.0f}s")
        return False, wait
    if r.status_code != 200:
        body = r.text[:200].replace("\n", " ")
        log(f"API error {r.status_code}: {body}")
        if "just a moment" in r.text.lower() or "cloudflare" in r.text.lower():
            return False, 120
        return False, 30
    try:
        data = r.json()
    except Exception:
        log("Bad JSON from CSFloat")
        return False, 15

    listings = data if isinstance(data, list) else data.get("data", [])
    now = time.time()
    new_seen = False
    for listing in listings:
        lid = listing.get("id")
        if not lid or lid in SEEN:
            continue
        SEEN[lid] = now
        new_seen = True
        if not is_fresh(listing):
            continue
        STATS["listings"] += 1
        try:
            hit = evaluate(listing)
            if hit:
                title, trig, color, verdict = hit
                item = listing.get("item") or {}
                price = (listing.get("price") or 0) / 100.0
                payload = build_embed(title, listing, item, price, trig, color, verdict)
                if post_discord(payload):
                    STATS["alerts"] += 1
                    log(f"ALERT: {title} {item.get('market_hash_name')} ${price:.2f}")
        except Exception as e:
            log("Error evaluating listing:", repr(e))
    if new_seen:
        save_seen()
    return True, None


# ================= MAIN LOOP =================
def main():
    log("🚀 Cloud Sniper 24/7 starting")
    load_seen()
    if ENABLE_SKINPORT:
        register_source("Skinport", fetch_skinport, SKINPORT_REFRESH)

    if SEND_TEST_ALERT:
        simple_message("✅ **TEST ALERT:** sniper is alive and posting to Discord.", 5763719)

    fails = 0
    warned = False
    successes = 0
    last_beat = time.time()
    deadline = time.time() + RUN_SECONDS if BOUNDED else None

    while RUNNING:
        if deadline and time.time() >= deadline:
            break
        loop_start = time.time()
        try:
            refresh_sources()
            ok, override = poll_once()
        except Exception as e:  # nothing should ever kill the loop
            log("Unexpected error:", repr(e))
            ok, override = False, 20

        STATS["polls"] += 1
        if ok:
            successes += 1
            if warned:
                simple_message("✅ Bot recovered, polling normally again.", 5763719)
                warned = False
            fails = 0
        else:
            fails += 1
            STATS["errors"] += 1
            if FAIL_ALERT_AFTER and fails >= FAIL_ALERT_AFTER and not warned:
                simple_message(f"⚠️ {fails} polls in a row failed (rate limit / block / API down). "
                               f"Still retrying with backoff.", 15158332)
                warned = True

        if HEARTBEAT_HOURS and time.time() - last_beat >= HEARTBEAT_HOURS * 3600:
            simple_message(f"💓 Alive — {STATS['polls']} polls, {STATS['listings']} new listings checked, "
                           f"{STATS['alerts']} alerts, {STATS['errors']} errors.")
            last_beat = time.time()

        # sleep: normal interval, or exponential backoff while failing
        if override:
            delay = override
        elif not ok:
            delay = min(300, POLL_INTERVAL * (2 ** min(fails, 5)))
        else:
            delay = max(1, POLL_INTERVAL - (time.time() - loop_start))
        end = time.time() + delay
        if deadline:
            end = min(end, deadline)
        while RUNNING and time.time() < end:
            time.sleep(min(1, end - time.time()))

    save_seen()
    log(f"Run finished: {STATS['polls']} polls, {successes} ok, {STATS['alerts']} alerts.")
    # red job if every poll failed, so blocks/403s aren't hidden behind a green check
    if BOUNDED and successes == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
