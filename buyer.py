"""
buyer.py - execution layer for sniper.py: auto buy-now + auto buy orders on CSFloat.

BUYER_MODE (env):
  off  - nothing happens, the sniper behaves exactly as before
  dry  - DEFAULT. Runs every check and the full budget accounting, posts "would buy" messages, spends nothing
  live - real purchases and real buy orders

Rules (all tunable via env, see the CONFIG block):
  * only items with recent, verified sales data (never the "unverified" alerts)
  * liquidity gate: enough sales in the last few days AND a very recent sale
  * net profit after the 2% fee (and the sniper's undercut) must be >= BUY_MIN_PROFIT_USD
  * hard daily budget (EUR -> USD), money is reserved BEFORE every call and persisted to buyer_state.json,
    which the workflow caches between runs
  * open buy orders count against the budget until they are cancelled, filled or expired
  * NEW: every purchase goes into a ledger (buyer_state.json -> "inv"). After the 7-day trade lock the bot lists
    the item at CSFloat's reference price (the float-adjusted "predicted price" of the sell menu) minus 1 cent,
    keeps it 1 cent under the reference, notices the sale and tracks the ~8 days until the money is usable again.
    Only items the bot bought itself are ever listed - nothing else in your inventory is touched.
  * NEW: rolling bankroll cap (CAPITAL_EUR): money tied up in unsold / not yet credited items + open orders
    can never exceed it, no matter what the daily budget says.

AUTOLIST_MODE (env): off | notify (DEFAULT, only posts what it WOULD list, lists nothing) | live (real listings)
"""
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


def _num(name, default):
    try:
        return float(os.environ.get(name, "").strip() or default)
    except ValueError:
        return float(default)


# ================= CONFIG =================
MODE = os.environ.get("BUYER_MODE", "dry").strip().lower()
if MODE not in ("off", "dry", "live"):
    MODE = "dry"                      # an unknown value must never become "live"
ACTIVE = MODE != "off"
LIVE = MODE == "live"

DAILY_BUDGET_EUR = _num("DAILY_BUDGET_EUR", 8.0)
EUR_USD_RATE = _num("EUR_USD_RATE", 1.10)          # CSFloat prices are USD; keep this a bit LOW to stay under budget
BUDGET_CENTS = int(DAILY_BUDGET_EUR * EUR_USD_RATE * 100)
# rolling bankroll: everything bought and not yet back as usable balance (14 days per cycle) must stay under this
CAPITAL_EUR = _num("CAPITAL_EUR", 116.0)
CAPITAL_CENTS = int(CAPITAL_EUR * EUR_USD_RATE * 100)

# "bomboclat deal" budget: extra money per day that only an exceptionally good deal may use, on top of the daily budget.
# A bomboclat deal may use what is left of the daily budget PLUS this pool and may cost up to BOMBOCLAT_MAX_ITEM_EUR.
BOMBOCLAT_BUDGET_EUR = _num("BOMBOCLAT_BUDGET_EUR", 5.0)
BOMBOCLAT_BUDGET_CENTS = int(BOMBOCLAT_BUDGET_EUR * EUR_USD_RATE * 100)
BOMBOCLAT_MAX_ITEM_CENTS = int(_num("BOMBOCLAT_MAX_ITEM_EUR", 12.0) * EUR_USD_RATE * 100)
BOMBOCLAT_MIN_NET_USD = _num("BOMBOCLAT_MIN_NET_USD", 0.40)   # est. net profit needed ...
BOMBOCLAT_MIN_ROI = _num("BOMBOCLAT_MIN_ROI", 0.05)           # ... and at least this fraction of the price (stops thin margins on big items)
# open buy orders do NOT count against the daily budget: the money only counts once an order fills.
# They still count against the bankroll cap, and this limits how many can be open at the same time.
MAX_OPEN_ORDERS = int(_num("BUY_MAX_OPEN_ORDERS", 12))

# auto-listing
AUTOLIST_MODE = os.environ.get("AUTOLIST_MODE", "notify").strip().lower()
if AUTOLIST_MODE not in ("off", "notify", "live"):
    AUTOLIST_MODE = "notify"          # an unknown value must never become "live"
LOCK_DAYS = _num("TRADE_LOCK_DAYS", 7.0)            # Steam trade protection on the item we bought
CREDIT_DAYS = _num("CREDIT_DAYS", 7.0)              # sale -> money usable (green "Credit Trades Now" banner shows with <24h left)
LIST_UNDERCUT_CENTS = int(_num("LIST_UNDERCUT_CENTS", 1))
LIST_ALLOW_LOSS = os.environ.get("LIST_ALLOW_LOSS", "true").strip().lower() == "true"   # list even if below cost
REPRICE_EVERY_MIN = _num("REPRICE_EVERY_MIN", 60.0)  # how often a listing is re-checked against the reference
MAX_ITEM_USD = _num("BUY_MAX_ITEM_USD", 3.00)      # no single purchase / order above this
MAX_ITEM_CENTS = int(MAX_ITEM_USD * 100)
MIN_PROFIT_USD = _num("BUY_MIN_PROFIT_USD", 0.03)  # net, after the 2% fee and the sniper's SELL_UNDERCUT_USD
# "min": resale = lower of CSFloat's reference and the recent sales median (careful). "ref": same maths as the alerts.
RESALE_BASIS = os.environ.get("BUY_RESALE_BASIS", "min").strip().lower()

# liquidity gate (sales without stickers, from CSFloat's sales history)
LIQ_WINDOW_DAYS = _num("LIQ_WINDOW_DAYS", 3.0)
LIQ_MIN_SALES = int(_num("LIQ_MIN_SALES", 10))             # at least this many sales inside the window ...
LIQ_MAX_LAST_SALE_HOURS = _num("LIQ_MAX_LAST_SALE_HOURS", 8.0)   # ... and the latest one at most this old

ORDER_TTL_HOURS = _num("BUY_ORDER_TTL_HOURS", 6.0)         # our own open buy orders are cancelled after this long
MAX_ACTIONS_PER_RUN = int(_num("BUY_MAX_ACTIONS_PER_RUN", 60))
COOLDOWN_AFTER_UNKNOWN = 30        # seconds without trading after an unclear result
MAX_UNKNOWN_PER_RUN = 3            # that many unclear results in one run -> trading disabled until the next run
MAINTAIN_EVERY = 300               # seconds between order reconcile / expiry / inventory checks
BUDGET_TZ = os.environ.get("BUDGET_TZ", "UTC").strip() or "UTC"   # the daily budget resets at midnight in this zone
STATE_FILE = os.environ.get("BUYER_STATE_FILE", "buyer_state.json")
TRADES_HOOK = os.environ.get("DISCORD_WEBHOOK_TRADES", "").strip()   # falls back to the main webhook

VERIFIED_TITLES = {"GREAT DEAL", "GOOD DEAL", "LOW MARGIN"}   # evaluate() results that went through the sales check

# ================= STATE =================
M = None                 # the sniper module (set by start())
STATE = {}
ACCT = {}                # STATE["live"] or STATE["dry"], so a dry run can never touch the live counters
RUN = {"bought": 0, "ordered": 0, "cancelled": 0, "filled": 0, "unknown": 0, "actions": 0,
       "disabled": False, "pause_until": 0.0, "balance": None}
SKIPS = Counter()
_last_maint = 0.0


def _today():
    tz = timezone.utc
    if ZoneInfo is not None:
        try:
            tz = ZoneInfo(BUDGET_TZ)
        except Exception:
            tz = timezone.utc
    return datetime.now(tz).strftime("%Y-%m-%d")


def _new_acct(day):
    return {"day": day, "spent": 0, "orders": {}, "bought": {}, "unknown": [], "bal0": None, "bn": None}


def _load():
    existed = True
    try:
        with open(STATE_FILE) as f:
            st = json.load(f)
    except Exception:
        st, existed = {}, False
    if not isinstance(st, dict):
        st, existed = {}, False
    for k in ("live", "dry"):
        a = st.get(k)
        if not isinstance(a, dict):
            a = _new_acct(_today())
        fresh = _new_acct("")
        for key, default in fresh.items():
            a.setdefault(key, default)
        st[k] = a
    if not isinstance(st.get("inv"), dict):
        st["inv"] = {}
    return st, existed


def save():
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(STATE, f)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        M.log("buyer: could not save state:", e)
        if LIVE:                       # a budget that cannot be remembered must not be spent
            RUN["disabled"] = True


def _roll():
    day = _today()
    if ACCT.get("day") != day:
        ACCT["day"] = day
        ACCT["spent"] = 0
        ACCT["bal0"] = None
        ACCT["bn"] = None
        cutoff = time.time() - 3 * 86400
        ACCT["bought"] = {k: v for k, v in ACCT["bought"].items() if v > cutoff}
        M.log(f"buyer: new budget day {day}")
        save()


def reserved():
    return sum(int(o.get("cents", 0)) for o in ACCT["orders"].values())


def committed():
    """What counts against the DAILY budget: money actually spent. Unfilled buy orders are not included."""
    return int(ACCT["spent"])


def _notify(text, color=3447003):
    try:
        M.simple_message(text[:3900], color, TRADES_HOOK or None)
    except Exception as e:
        M.log("buyer: notify failed:", e)


def _usd(cents):
    return f"${cents / 100:.2f}"


def _skip(reason):
    SKIPS[reason.split(" (")[0]] += 1
    M.rej("autobuy: " + reason.split(" (")[0])
    return f"\n🤖 not auto-bought: {reason}"


def _gate(cents, bomboclat=False, quiet=False):
    """None if we may spend `cents` right now, else the reason. bomboclat=True uses the bigger limits."""
    if RUN["disabled"]:
        return "auto-buy disabled for this run"
    if time.time() < RUN["pause_until"]:
        return "cooling down after an error"
    if RUN["actions"] >= MAX_ACTIONS_PER_RUN:
        return "per-run action limit"
    if cents > (BOMBOCLAT_MAX_ITEM_CENTS if bomboclat else MAX_ITEM_CENTS):
        return f"above max item price {_usd(BOMBOCLAT_MAX_ITEM_CENTS if bomboclat else MAX_ITEM_CENTS)}"
    if tied_cents() + reserved() + cents > CAPITAL_CENTS:
        return "bankroll tied up (waiting for sales / credits)"
    if committed() + cents > BUDGET_CENTS + (BOMBOCLAT_BUDGET_CENTS if bomboclat else 0):
        if not quiet and ACCT.get("bn") != ACCT["day"]:
            ACCT["bn"] = ACCT["day"]
            save()
            _notify(f"⛔ **Daily budget reached** [{MODE}]: {_usd(committed())} of {_usd(BUDGET_CENTS)} "
                    f"has been spent (open buy orders only count once they fill). Auto-buy pauses until the day rolls over ({BUDGET_TZ}).",
                    16766720)
        return "daily budget"
    return None


# ================= HELPERS =================
def _parse_ts(iso):
    try:
        return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def liquidity(sales):
    """(ok, sales_in_window, hours_since_last_sale) - sales with stickers are ignored."""
    now = time.time()
    ts = []
    for s in sales or []:
        if (s.get("item") or {}).get("stickers") or not s.get("price"):
            continue
        t = _parse_ts(s.get("created_at") or "")
        if t:
            ts.append(t)
    n = sum(1 for t in ts if now - t <= LIQ_WINDOW_DAYS * 86400)
    last_h = (now - max(ts)) / 3600 if ts else None
    ok = n >= LIQ_MIN_SALES and last_h is not None and last_h <= LIQ_MAX_LAST_SALE_HOURS
    return ok, n, last_h


def _liq_text(n, last_h):
    last = f"{last_h:.1f}h ago" if last_h is not None else "never"
    return f"{n} sales/{LIQ_WINDOW_DAYS:g}d, last {last}"


def _net(price_usd, ref_value, median):
    """Net profit if resold SELL_UNDERCUT_USD under the resale basis, after the sell fee."""
    basis = min(ref_value, median) if (RESALE_BASIS == "min" and median) else ref_value
    return (basis - M.SELL_UNDERCUT_USD) * (1 - M.SELL_FEE) - price_usd, basis


def _call(method, path, payload=None, params=None):
    """-> (outcome, http_status, data). outcome: 'ok' | 'rejected' (definitely not executed) | 'unknown'."""
    try:
        r = M.session.request(
            method, f"{M.CSFLOAT}{path}", json=payload, params=params,
            headers={"Authorization": M.CSFLOAT_API_KEY, "Content-Type": "application/json",
                     "User-Agent": "trydentsz-sniper/3.1", "Accept": "application/json"},
            timeout=15)
    except Exception as e:                      # timeout / connection reset: it may or may not have gone through
        return "unknown", 0, repr(e)
    try:
        data = r.json()
    except Exception:
        data = (r.text or "")[:300]
    if 200 <= r.status_code < 300:
        return "ok", r.status_code, data
    if r.status_code >= 500:
        return "unknown", r.status_code, data
    return "rejected", r.status_code, data


def _msg(data):
    if isinstance(data, dict):
        return str(data.get("message") or data.get("error") or data)[:200]
    return str(data)[:200]


def _extract_list(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("orders", "data", "buy_orders", "results"):
            if isinstance(data.get(k), list):
                return data[k]
    return None


def _extract_id(data):
    if isinstance(data, dict):
        for k in ("id", "order_id"):
            if data.get(k) is not None:
                return str(data[k])
        for k in ("order", "data", "buy_order"):
            if isinstance(data.get(k), dict):
                rid = _extract_id(data[k])
                if rid:
                    return rid
    return None


def _order_state(o):
    st = str(o.get("state") or o.get("status") or "").lower()
    if st in ("filled", "fulfilled", "completed", "complete", "done"):
        return "filled"
    if st in ("cancelled", "canceled", "expired", "closed", "deleted"):
        return "gone"
    return "open"


def _after_reject(status, data, what):
    text = _msg(data)
    M.log(f"buyer: {what} rejected ({status}): {text}")
    low = text.lower()
    if status in (401, 403):
        RUN["disabled"] = True
        _notify(f"🛑 **Auto-buy disabled for this run**: CSFloat answered {status} ({text}). "
                f"Check that the API key is allowed to trade.", 15158332)
    elif status == 429:
        RUN["pause_until"] = time.time() + 60
    elif "balance" in low or "funds" in low:
        RUN["pause_until"] = time.time() + 900
        _notify(f"⚠️ Not enough CSFloat balance for {what}: {text}", 16766720)
    # any other 4xx is normally a lost race (listing sold or price changed): nothing else to do


def _after_unknown(what, status):
    RUN["unknown"] += 1
    RUN["pause_until"] = time.time() + COOLDOWN_AFTER_UNKNOWN
    if RUN["unknown"] >= MAX_UNKNOWN_PER_RUN:
        RUN["disabled"] = True
    ACCT["unknown"] = (ACCT["unknown"] + [{"what": what, "ts": int(time.time()), "status": status}])[-20:]
    save()
    _notify(f"⚠️ **Result unknown** for {what} (HTTP {status or 'no response'}). The amount stays counted "
            f"against today's budget. Check your CSFloat account before assuming anything.", 16766720)


# ================= BUY NOW =================
def on_deal(listing, hit):
    """Called by sniper.poll_once for every alert. Returns text for the alert's trigger line ('' = nothing to say)."""
    if not ACTIVE:
        return ""
    try:
        return _on_deal(listing, hit)
    except Exception as e:
        M.log("buyer: error in on_deal:", repr(e))
        return ""


def _on_deal(listing, hit):
    title = hit[0]
    channel = hit[4] if len(hit) > 4 else "main"
    if title not in VERIFIED_TITLES or channel not in ("main", "cheap"):
        SKIPS["alert type not eligible"] += 1       # unverified / pattern / float / blue / knife / glove alerts
        return ""
    _roll()
    item = listing.get("item") or {}
    name = item.get("market_hash_name") or ""
    lid = listing.get("id")
    cents = int(listing.get("price") or 0)
    if not (lid and name and cents > 0):
        return ""
    if item.get("stickers") or M.is_souvenir(name.lower(), item):
        return _skip("stickers or souvenir")
    if lid in ACCT["bought"]:
        return _skip("already bought this listing")
    gate = _gate(cents, bomboclat=True, quiet=True)      # loosest limits first; the real limit is decided once the profit is known
    if gate:
        return _skip(gate)

    sales = M.get_sales(name, budgeted=False)       # cached by evaluate() a moment ago
    if sales is None:
        return _skip("no sales data")
    ok, n, last_h = liquidity(sales)
    if not ok:
        return _skip(f"illiquid ({_liq_text(n, last_h)})")
    s = M.summarize_sales(sales)
    price = cents / 100.0
    if not s or s["median"] < price:
        return _skip("recent sales median below the price")
    est = M.sell_estimate(listing, item, price)
    if not est:
        return _skip("no reference price")
    net, basis = _net(price, est[3], s["median"])
    if net < MIN_PROFIT_USD:
        return _skip(f"net {net:+.2f} below minimum {MIN_PROFIT_USD:.2f}")
    bomboclat = net >= BOMBOCLAT_MIN_NET_USD and net >= BOMBOCLAT_MIN_ROI * price
    gate = _gate(cents, bomboclat=bomboclat)
    if gate:
        extra = "" if bomboclat else f" (net {net:+.2f}, a bomboclat deal needs +{BOMBOCLAT_MIN_NET_USD:.2f} and {BOMBOCLAT_MIN_ROI:.0%})"
        return _skip(gate + extra)
    return _buy_now(lid, name, cents, net, basis, n, last_h, item, (listing.get("seller") or {}).get("steam_id"), bomboclat)


def _buy_now(lid, name, cents, net, basis, n, last_h, item=None, seller=None, bomboclat=False):
    RUN["actions"] += 1
    ACCT["spent"] += cents                 # reserve first: if anything below goes wrong the money still counts
    ACCT["bought"][lid] = time.time()
    save()
    link = f"https://csfloat.com/item/{lid}"
    detail = (f"{name} for **{_usd(cents)}** · est. net **+${net:.2f}** (resale basis {_usd(int(basis * 100))}) · "
              f"{_liq_text(n, last_h)}\n[open listing]({link})")
    left = f"Budget: {_usd(committed())} / {_usd(BUDGET_CENTS)} spent today"
    if bomboclat:
        left += f" (💣 bomboclat pool allowed up to +{_usd(BOMBOCLAT_BUDGET_CENTS)})"
    tag = "💣 BOMBOCLAT DEAL " if bomboclat else ""
    if not LIVE:
        RUN["bought"] += 1
        _notify(f"🧪 **DRY RUN, would buy** {tag}{detail}\n{left}", 10070709)
        return "\n🤖 DRY RUN: would have bought this"
    outcome, status, data = _call("POST", "/listings/buy", {"contract_ids": [lid], "total_price": cents})
    if outcome == "ok":
        RUN["bought"] += 1
        _inv_add(lid, name, cents, (item or {}).get("float_value"), seller)
        _notify(f"🛒 **{tag}BOUGHT** {detail}\n{left}\nTrade lock ends ~{_fmt_ts(time.time() + LOCK_DAYS * 86400)} "
                f"(accept the seller's Steam trade offer!)", 5763719)
        return "\n🤖 AUTO-BOUGHT"
    if outcome == "rejected":
        ACCT["spent"] -= cents
        ACCT["bought"].pop(lid, None)
        save()
        _after_reject(status, data, f"buy {name}")
        return f"\n🤖 buy failed ({status}): {_msg(data)}"
    _after_unknown(f"buy {name} ({_usd(cents)})", status)
    return "\n🤖 buy result unknown, check CSFloat"


# ================= BUY ORDERS =================
def on_gap(name, lid, ask, base, top):
    """Called by sniper.bo_scan_once with the lowest listing and the highest existing buy order."""
    if not ACTIVE:
        return
    try:
        _on_gap(name, ask, base, top)
    except Exception as e:
        M.log("buyer: error in on_gap:", repr(e))


def _on_gap(name, ask, base, top):
    _roll()
    if any(o.get("name") == name for o in ACCT["orders"].values()):
        return                                       # one open order per skin (our own bid is the new top bid)
    cents = int(round((top + M.BO_BID_STEP) * 100))
    price = cents / 100.0
    ref = min(ask, base) if base else ask
    # cheap pre-check on the most optimistic numbers: most scans stop here, no extra requests, no noise
    if (ref - M.SELL_UNDERCUT_USD) * (1 - M.SELL_FEE) - price < MIN_PROFIT_USD:
        return
    gate = _gate(cents)
    if gate:
        _skip(gate)
        return
    if len(ACCT["orders"]) >= MAX_OPEN_ORDERS:
        _skip("max open buy orders")
        return
    sales = M.get_sales(name, budgeted=False)
    if sales is None:
        _skip("no sales data")
        return
    ok, n, last_h = liquidity(sales)
    if not ok:
        _skip(f"illiquid ({_liq_text(n, last_h)})")
        return
    s = M.summarize_sales(sales)
    if not s:
        _skip("no recent sales")
        return
    net, basis = _net(price, ref, s["median"])
    if net < MIN_PROFIT_USD:
        _skip(f"net {net:+.2f} below minimum {MIN_PROFIT_USD:.2f}")
        return
    _place_order(name, cents, net, basis, n, last_h, top, ask)


def _place_order(name, cents, net, basis, n, last_h, top, ask):
    RUN["actions"] += 1
    stamp = int(time.time() * 1000)
    key = f"dry-{stamp}" if not LIVE else f"pending-{stamp}"
    ACCT["orders"][key] = {"name": name, "cents": cents, "ts": time.time()}     # reserve first
    save()
    detail = (f"{name}: bid **{_usd(cents)}** (top bid {_usd(int(round(top * 100)))}, lowest listing "
              f"{_usd(int(round(ask * 100)))}) · est. net **+${net:.2f}** · {_liq_text(n, last_h)}")
    left = f"Budget: {_usd(committed())} / {_usd(BUDGET_CENTS)} spent today"
    if not LIVE:
        RUN["ordered"] += 1
        _notify(f"🧪 **DRY RUN, would place buy order** {detail}\n{left}", 10070709)
        return
    outcome, status, data = _call("POST", "/buy-orders", {"market_hash_name": name, "max_price": cents, "quantity": 1})
    if outcome == "ok":
        oid = _extract_id(data)
        if oid:
            ACCT["orders"][oid] = ACCT["orders"].pop(key)
        RUN["ordered"] += 1
        save()
        _notify(f"🧾 **BUY ORDER PLACED** {detail}\n{left}", 5763719)
    elif outcome == "rejected":
        ACCT["orders"].pop(key, None)
        save()
        _after_reject(status, data, f"buy order {name}")
    else:
        _after_unknown(f"buy order {name} ({_usd(cents)})", status)


# ================= MAINTENANCE (reconcile / expire / balance) =================
def maintain(force=False):
    """Self-throttled; called from the sniper's main loop."""
    global _last_maint
    if not ACTIVE or M is None:
        return
    now = time.time()
    if not force and now - _last_maint < MAINTAIN_EVERY:
        return
    _last_maint = now
    try:
        _roll()
        if LIVE:
            _check_balance()
            _reconcile()
        _expire_orders()
        _trim_orders()
        if LIVE and AUTOLIST_MODE != "off":
            _inventory_cycle()
        save()
    except Exception as e:
        M.log("buyer: maintenance error:", repr(e))


def _check_balance():
    outcome, _, data = _call("GET", "/me")
    if outcome != "ok" or not isinstance(data, dict):
        return
    user = data.get("user") if isinstance(data.get("user"), dict) else data
    bal = user.get("balance")
    if not isinstance(bal, (int, float)):
        return
    bal = int(bal)
    RUN["balance"] = bal
    if ACCT.get("bal0") is None:
        ACCT["bal0"] = bal
    elif ACCT["bal0"] - bal > 2 * BUDGET_CENTS:
        # backstop: far more money left the account today than this bot's counters can explain
        RUN["disabled"] = True
        _notify(f"🛑 **Auto-buy disabled**: CSFloat balance fell from {_usd(ACCT['bal0'])} to {_usd(bal)} today, "
                f"more than twice the daily budget. Check the account.", 15158332)


def _reconcile():
    """Compare our tracked buy orders with what CSFloat says is still open."""
    if not ACCT["orders"]:
        return
    outcome, status, data = _call("GET", "/me/buy-orders", params={"page": 0, "limit": 100, "order": "desc"})
    remote = _extract_list(data) if outcome == "ok" else None
    if remote is None:
        M.log(f"buyer: could not read open buy orders ({outcome} {status}), keeping them reserved")
        return
    remote = [o for o in remote if isinstance(o, dict)]
    by_id = {str(o["id"]): o for o in remote if o.get("id") is not None}
    for oid, o in list(ACCT["orders"].items()):
        if oid.startswith("pending-"):       # order whose id we could not read: try to find it by skin + price
            for r in remote:
                rname = r.get("market_hash_name") or (r.get("item") or {}).get("market_hash_name")
                rprice = r.get("max_price") or r.get("price")
                if r.get("id") is not None and rname == o["name"] and rprice == o["cents"] \
                        and str(r["id"]) not in ACCT["orders"]:
                    ACCT["orders"][str(r["id"])] = ACCT["orders"].pop(oid)
                    break
            continue
        if oid.startswith("dry-"):
            continue
        r = by_id.get(oid)
        state = _order_state(r) if r is not None else "filled"   # vanished = assume filled (the safe direction)
        if state == "open":
            continue
        ACCT["orders"].pop(oid, None)
        if state == "filled":
            ACCT["spent"] += int(o["cents"])
            RUN["filled"] += 1
            _inv_add(f"bo-{oid}", o["name"], int(o["cents"]), None)
            _notify(f"✅ **Buy order no longer open (counted as filled)**: {o['name']} up to {_usd(o['cents'])}", 5763719)


def _expire_orders():
    ttl = ORDER_TTL_HOURS * 3600
    now = time.time()
    for oid, o in list(ACCT["orders"].items()):
        if now - float(o.get("ts", now)) < ttl:
            continue
        if not LIVE or oid.startswith("dry-"):
            ACCT["orders"].pop(oid, None)
            RUN["cancelled"] += 1
            continue
        if oid.startswith("pending-"):
            # never learned its id and it is past its time: assume it filled, count it as spent
            ACCT["orders"].pop(oid, None)
            ACCT["spent"] += int(o["cents"])
            continue
        outcome, status, data = _call("DELETE", f"/buy-orders/{oid}")
        if outcome == "ok":
            ACCT["orders"].pop(oid, None)
            RUN["cancelled"] += 1
            M.log(f"buyer: cancelled stale buy order {o['name']}")
        elif status == 404:                       # already gone: most likely filled
            ACCT["orders"].pop(oid, None)
            ACCT["spent"] += int(o["cents"])
        else:
            M.log(f"buyer: could not cancel {oid} ({outcome} {status}), will retry")



def _trim_orders():
    """Orders do not reserve daily budget, so once the day's spending leaves no room for an order, cancel it
    (otherwise it could still fill and push the day over budget)."""
    for oid, o in list(ACCT["orders"].items()):
        if oid.startswith("pending-") or committed() + int(o["cents"]) <= BUDGET_CENTS:
            continue
        if not LIVE or oid.startswith("dry-"):
            ACCT["orders"].pop(oid, None)
            RUN["cancelled"] += 1
            continue
        outcome, status, data = _call("DELETE", f"/buy-orders/{oid}")
        if outcome == "ok":
            ACCT["orders"].pop(oid, None)
            RUN["cancelled"] += 1
            M.log(f"buyer: cancelled buy order {o['name']} (no daily budget left for it)")
        elif status == 404:                       # already gone: most likely filled
            ACCT["orders"].pop(oid, None)
            ACCT["spent"] += int(o["cents"])
        else:
            M.log(f"buyer: could not cancel {oid} ({outcome} {status}), will retry")


# ================= INVENTORY LEDGER / AUTO-LIST / SALES =================
# statuses: bought (waiting for the trade lock / listing) -> listed -> sold (waiting for the money) -> credited
ACTIVE_INV = ("bought", "listed", "sold")


def _inv():
    return STATE.setdefault("inv", {})


def tied_cents():
    return sum(int(v.get("cost", 0)) for v in _inv().values() if v.get("status") in ACTIVE_INV)


def _inv_counts():
    c = Counter(v.get("status") for v in _inv().values() if v.get("status") in ACTIVE_INV)
    return f"{c['bought']} locked/unlisted, {c['listed']} listed, {c['sold']} awaiting credit"


def _fmt_ts(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%d %b %H:%M UTC")


def _inv_add(key, name, cents, flt, seller=None):
    if not LIVE:
        return
    _inv()[str(key)] = {"name": name, "cost": int(cents), "float": flt, "bought": time.time(),
                        "status": "bought", "retry": 0, "fails": 0, "seller": str(seller) if seller else None}
    save()


def _norm_inv(raw):
    """Inventory rows differ a bit between API versions: flat item or {"item": {...}, "reference": {...}}."""
    if not isinstance(raw, dict):
        return None
    it = raw.get("item") if isinstance(raw.get("item"), dict) else raw
    ref = raw.get("reference") or it.get("reference") or {}
    aid = it.get("asset_id") or raw.get("asset_id")
    if aid is None:
        return None
    return {"asset_id": str(aid), "name": it.get("market_hash_name"), "float": it.get("float_value"),
            "listing_id": raw.get("listing_id") or it.get("listing_id"),
            "pred": ref.get("predicted_price"), "base": ref.get("base_price"),
            "tradable": it.get("tradable", raw.get("tradable"))}


def _float_eq(a, b):
    try:
        return abs(float(a) - float(b)) < 1e-9
    except (TypeError, ValueError):
        return False


def _match(e, rows, used):
    for r in rows:
        if r["asset_id"] in used or r["name"] != e["name"]:
            continue
        if e.get("float") is not None and not _float_eq(r["float"], e["float"]):
            continue
        return r
    return None


def _list_price(pred, base):
    """CSFloat's sell menu reference = the float-adjusted predicted price (falls back to the base price)."""
    basis = pred or base
    if not basis:
        return None, None
    return max(int(basis) - LIST_UNDERCUT_CENTS, 1), int(basis)


def _net_cents(price_cents, cost):
    return int(price_cents * (1 - M.SELL_FEE)) - int(cost)


ACCEPTS_FILE = os.environ.get("STEAM_ACCEPTS_FILE", "steam_accepts.json")


def _read_accepts():
    """steam_accept.js records when it accepted the seller's offer: the 7-day lock starts then, not at purchase."""
    try:
        with open(ACCEPTS_FILE) as f:
            acc = json.load(f)
    except Exception:
        return
    for k, a in (acc or {}).items():
        v = _inv().get(k)
        if v and isinstance(a, dict) and not v.get("accepted_ts") and a.get("ts"):
            v["accepted_ts"] = float(a["ts"])


def _inventory_cycle():
    now = time.time()
    inv = _inv()
    for k, v in list(inv.items()):                  # forget finished items after 30 days
        if v.get("status") == "credited" and now - v.get("credited_ts", now) > 30 * 86400:
            inv.pop(k, None)
    _read_accepts()
    due_list = [k for k, v in inv.items() if v.get("status") == "bought"
                and now >= (v.get("accepted_ts") or v["bought"]) + LOCK_DAYS * 86400 and now >= v.get("retry", 0)]
    listed = [k for k, v in inv.items() if v.get("status") == "listed"]
    sold = [k for k, v in inv.items() if v.get("status") == "sold"]

    if listed:
        _sales_scan(listed, now)
    for k in sold:
        v = inv[k]
        if now >= v.get("credit_ts", 0):
            v["status"], v["credited_ts"] = "credited", now
            got = _net_cents(v.get("list_price", 0), v["cost"])
            _notify(f"💶 **Money should be usable now**: {v['name']} (sold {_usd(v.get('list_price', 0))}, est. profit "
                    f"{'+' if got >= 0 else '-'}{_usd(abs(got))}). If CSFloat still shows the green verify/credit "
                    f"button for it, press it. {_inv_counts()}", 5763719)
    for k in listed:                                # re-check price / state of what is still listed
        v = inv[k]
        if v.get("status") == "listed" and now - v.get("chk", 0) >= REPRICE_EVERY_MIN * 60:
            _check_listed(k, v, now)
    if due_list and AUTOLIST_MODE in ("notify", "live"):
        _list_due(due_list, now)
    save()


def _list_due(keys, now):
    inv = _inv()
    outcome, status, data = _call("GET", "/me/inventory")
    rows = _extract_list(data) if outcome == "ok" else None
    if rows is None and outcome == "ok" and isinstance(data, dict):
        rows = _extract_list(data.get("inventory")) if "inventory" in data else None
    if rows is None:
        M.log(f"buyer: could not read inventory ({outcome} {status})")
        return
    rows = [r for r in (_norm_inv(x) for x in rows) if r]
    used = {v.get("asset_id") for v in inv.values() if v.get("asset_id")}
    for k in keys:
        e = inv[k]
        r = _match(e, rows, used)
        if r is None:
            e["retry"] = now + 1800                 # not in the inventory yet (trade offer not accepted / still moving)
            if now > e["bought"] + 21 * 86400 and not e.get("lost_note"):
                e["lost_note"] = True
                _notify(f"⚠️ {e['name']} bought {_fmt_ts(e['bought'])} is still not in your inventory. "
                        f"Did you accept the seller's trade offer?", 16766720)
            continue
        used.add(r["asset_id"])
        if r.get("listing_id"):                     # already in the stall (listed by hand?) -> just track it
            e.update(status="listed", asset_id=r["asset_id"], list_id=str(r["listing_id"]), chk=now,
                     listed_ts=now, list_price=e.get("list_price", 0))
            continue
        if r.get("tradable") in (0, False, "0"):
            e["retry"] = now + 1800                 # Steam still says locked
            continue
        price, basis = _list_price(r["pred"], r["base"])
        if price is None:
            e["retry"] = now + 1800
            continue
        net = _net_cents(price, e["cost"])
        loss = f"\n⚠️ below cost: est. {_usd(net)}" if net < 0 else ""
        if net < 0 and not LIST_ALLOW_LOSS:
            e["retry"] = now + 6 * 3600
            _notify(f"⏸️ Not listing {e['name']}: reference {_usd(basis)} would lose money (cost {_usd(e['cost'])}). "
                    f"Retrying in 6h.", 16766720)
            continue
        info = (f"{e['name']} for **{_usd(price)}** (reference {_usd(basis)} - {LIST_UNDERCUT_CENTS}c) · "
                f"cost {_usd(e['cost'])} · est. net **{'+' if net >= 0 else '-'}{_usd(abs(net))}** after 2% fee{loss}")
        if AUTOLIST_MODE != "live":
            e["retry"] = now + 6 * 3600
            _notify(f"🧪 **WOULD LIST** {info}\nAUTOLIST_MODE=notify, nothing was listed.", 10070709)
            continue
        oc, st, d = _call("POST", "/listings", {"asset_id": r["asset_id"], "type": "buy_now", "price": price,
                                                "description": "", "private": False})
        if oc == "ok":
            lid = _extract_id(d)
            e.update(status="listed", asset_id=r["asset_id"], list_id=lid, list_price=price, chk=now, listed_ts=now)
            _notify(f"🏷️ **LISTED** {info}", 5763719)
            if not lid:
                M.log("buyer: listing created but no id in the answer:", str(d)[:200])
        elif oc == "rejected":
            e["fails"] = e.get("fails", 0) + 1
            e["retry"] = now + (1800 if e["fails"] < 4 else 6 * 3600)
            M.log(f"buyer: listing {e['name']} rejected ({st}): {_msg(d)}")
            if e["fails"] in (1, 4):
                _notify(f"⚠️ Could not list {e['name']} ({st}): {_msg(d)}. Retrying later.", 16766720)
        else:
            e["retry"] = now + 300                  # unclear: next round the inventory shows a listing_id if it worked
            M.log(f"buyer: listing {e['name']} unclear ({st})")


def _trade_cid(t):
    return str(t.get("contract_id") or (t.get("contract") or {}).get("id") or "")


def _sales_scan(keys, now):
    inv = _inv()
    oc, st, data = _call("GET", "/me/trades", params={"role": "seller", "state": "pending,verified",
                                                      "limit": 100, "page": 0})
    trades = _extract_list(data) if oc == "ok" else None
    if trades is None and oc == "ok" and isinstance(data, dict):
        trades = _extract_list(data.get("trades")) if "trades" in data else None
    if trades is None:
        return
    by = {_trade_cid(t): t for t in trades if isinstance(t, dict) and _trade_cid(t)}
    for k in keys:
        v = inv[k]
        if v.get("list_id") and str(v["list_id"]) in by:
            _mark_sold(v, now)


def _mark_sold(v, now):
    v["status"], v["sold_ts"], v["credit_ts"] = "sold", now, now + CREDIT_DAYS * 86400
    net = _net_cents(v.get("list_price", 0), v["cost"])
    held = (now - v.get("listed_ts", now)) / 3600
    _notify(f"💰 **SOLD** {v['name']} for {_usd(v.get('list_price', 0))} after {held:.1f}h listed · est. net "
            f"{'+' if net >= 0 else '-'}{_usd(abs(net))}\n"
            f"➡️ **Accept the sale on CSFloat and send the Steam trade offer now** (the bot cannot do that).\n"
            f"Money usable ~{_fmt_ts(v['credit_ts'])}. {_inv_counts()}", 5763719)


def _check_listed(k, v, now):
    v["chk"] = now
    if not v.get("list_id"):
        return
    oc, st, data = _call("GET", f"/listings/{v['list_id']}")
    if oc == "ok" and isinstance(data, dict):
        state = str(data.get("state") or data.get("status") or "").lower()
        if state == "sold":
            _mark_sold(v, now)
            return
        if state in ("", "listed", "active"):
            ref = data.get("reference") or {}
            new, basis = _list_price(ref.get("predicted_price"), ref.get("base_price"))
            cur = data.get("price")
            changed = bool(new) and isinstance(cur, (int, float)) and int(cur) != new
            if changed and (LIST_ALLOW_LOSS or _net_cents(new, v["cost"]) >= 0):
                oc2, st2, d2 = _call("PATCH", f"/listings/{v['list_id']}", {"price": new})
                if oc2 == "ok":
                    v["list_price"] = new
                    M.log(f"buyer: repriced {v['name']} {int(cur)} -> {new}")
            else:
                v["list_price"] = int(cur) if isinstance(cur, (int, float)) else v.get("list_price", 0)
            return
    if (oc == "rejected" and st == 404) or (oc == "ok" and isinstance(data, dict)):
        # gone / not "listed": relist it unless the trades scan already caught it as sold
        v.update(status="bought", retry=0, fails=0)
        v.pop("list_id", None)
        v.pop("asset_id", None)
        _notify(f"⚠️ Listing of {v['name']} is gone and no sale was found. The bot will list it again.", 16766720)


# ================= LIFECYCLE =================
def bo_price_ceiling(default_max):
    """The buy-order scanner only needs skins we are allowed to buy."""
    return min(default_max, MAX_ITEM_USD) if ACTIVE else default_max


def start(module):
    global M, STATE, ACCT
    M = module
    if not ACTIVE:
        M.log("buyer: BUYER_MODE=off")
        return
    STATE, existed = _load()
    ACCT = STATE["live" if LIVE else "dry"]
    M.log(f"buyer: mode={MODE} · budget {_usd(BUDGET_CENTS)}/day (EUR {DAILY_BUDGET_EUR:g} at {EUR_USD_RATE:g}) · "
          f"max item {_usd(MAX_ITEM_CENTS)} · min net +${MIN_PROFIT_USD:.2f} · "
          f"liquidity >= {LIQ_MIN_SALES} sales in {LIQ_WINDOW_DAYS:g}d, last sale <= {LIQ_MAX_LAST_SALE_HOURS:g}h")
    if LIVE and not existed:
        _notify("⚠️ **Auto-buy (live): no budget state found.** Fine on the very first live run. Otherwise the cache "
                "was lost and today's counter restarted from zero.", 16766720)
    _roll()
    maintain(force=True)
    M.log(status_line())


def finish():
    if ACTIVE and M is not None and ACCT:
        save()
        M.log(status_line())


def status_line():
    if not ACTIVE:
        return "Auto-buy: off"
    skips = ", ".join(f"{k}: {v}" for k, v in SKIPS.most_common(4)) or "-"
    bal = f" · balance {_usd(RUN['balance'])}" if RUN["balance"] is not None else ""
    inv = _inv_counts()
    return (f"Auto-buy [{MODE}] bankroll {_usd(tied_cents())}/{_usd(CAPITAL_CENTS)} tied ({inv}) autolist={AUTOLIST_MODE}{' DISABLED' if RUN['disabled'] else ''}: {_usd(ACCT['spent'])} spent + "
            f"{_usd(reserved())} in open orders (not counted until filled) of {_usd(BUDGET_CENTS)} today{bal} · this run: {RUN['bought']} buys, "
            f"{RUN['ordered']} orders, {RUN['filled']} filled, {RUN['cancelled']} cancelled, {RUN['unknown']} unclear "
            f"· skipped: {skips}")
