"""
trends.py - market index + daily charts (daily / weekly / monthly) posted to its own Discord channel.

How it works
  * Every listing the sniper sees carries CSFloat's reference (base) price for that skin. Each time a skin is seen again
    (15 min to 6 h later) its price change is recorded. Once per hour the average change over all skins becomes one point
    of a market index (start = 100). Up/down breadth (how many skins rose vs fell) is stored too.
  * The index and its raw data live inside buyer_state.json (key "trends"), so no extra cache file is needed.
  * Once a day (TRENDS_POST_HOUR, in BUDGET_TZ) three charts are posted: last 24 h, last 7 days, last 30 days.
    History starts on the first run with this file, so weekly / monthly fill up over time.
Needs BUYER_MODE live or dry (the state file belongs to the buyer). Webhook: DISCORD_WEBHOOK_TRENDS.
"""
import io
import json
import os
import time
from datetime import datetime, timezone

import requests

import buyer as _buyer

try:
    from zoneinfo import ZoneInfo
except Exception:                       # pragma: no cover
    ZoneInfo = None

HOOK = os.environ.get("DISCORD_WEBHOOK_TRENDS", "").strip()
TZ_NAME = os.environ.get("BUDGET_TZ", "UTC").strip() or "UTC"
POST_HOUR = int(float(os.environ.get("TRENDS_POST_HOUR", "8")))
FORCE = os.environ.get("TRENDS_FORCE", "false").strip().lower() == "true"

MIN_GAP = 15 * 60            # a skin must be seen this much later to count as a new observation
MAX_GAP = 6 * 3600           # ... but not later than this (then the change is no longer "hourly")
MAX_NAMES = 3000
KEEP_DAYS = 35
MIN_POINTS_PER_HOUR = 15     # fewer price changes than this in an hour -> no index point for that hour
CLIP = 0.10                  # single-skin changes are clipped to +-10% so one odd skin cannot move the index

_last_check = 0.0
_forced_done = False


def _tz():
    if ZoneInfo is not None:
        try:
            return ZoneInfo(TZ_NAME)
        except Exception:
            pass
    return timezone.utc


def _st():
    st = _buyer.STATE.setdefault("trends", {})
    st.setdefault("last", {})       # name -> [base_price_cents, ts]
    st.setdefault("acc", {})        # hour bucket -> [relative price changes]
    st.setdefault("series", [])     # [hour_ts, index, n_changes, n_up, n_down]
    st.setdefault("posted_day", "")
    st.setdefault("since", None)
    return st


def _active():
    return bool(getattr(_buyer, "ACTIVE", False)) and isinstance(_buyer.STATE, dict)


def observe(listings):
    if not _active():
        return
    st = _st()
    now = time.time()
    last = st["last"]
    hb = str(int(now // 3600 * 3600))
    acc = st["acc"].setdefault(hb, [])
    for l in listings:
        base = (l.get("reference") or {}).get("base_price")
        name = (l.get("item") or {}).get("market_hash_name")
        if not base or not name or base <= 0:
            continue
        prev = last.get(name)
        if prev is None:
            last[name] = [base, now]
            continue
        dt = now - prev[1]
        if dt < MIN_GAP:
            continue
        if dt <= MAX_GAP and prev[0] > 0:
            r = base / prev[0] - 1
            if abs(r) <= 0.5:
                acc.append(max(-CLIP, min(CLIP, r)))
        last[name] = [base, now]
    if len(last) > MAX_NAMES:
        for k, _ in sorted(last.items(), key=lambda kv: kv[1][1])[:len(last) - MAX_NAMES]:
            last.pop(k, None)


def flush():
    """Turn finished hours into index points."""
    if not _active():
        return
    st = _st()
    now = time.time()
    cur = int(now // 3600 * 3600)
    series = st["series"]
    idx = series[-1][1] if series else 100.0
    for hb in sorted(st["acc"], key=int):
        h = int(hb)
        if h >= cur:
            continue
        rs = st["acc"].pop(hb)
        if len(rs) < MIN_POINTS_PER_HOUR:
            continue
        idx *= 1 + sum(rs) / len(rs)
        up = sum(1 for r in rs if r > 0.0005)
        down = sum(1 for r in rs if r < -0.0005)
        series.append([h, round(idx, 4), len(rs), up, down])
        st["since"] = st["since"] or h
    cutoff = now - KEEP_DAYS * 86400
    st["series"] = [p for p in series if p[0] >= cutoff]


def _window(series, hours):
    cutoff = time.time() - hours * 3600
    pts = [p for p in series if p[0] >= cutoff]
    return pts


def _chart(pts, hours, label):
    """-> (png_bytes, pct_change, up_share, down_share, hours_of_data) or None"""
    if len(pts) < 3:
        return None
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    tz = _tz()
    xs = [datetime.fromtimestamp(p[0], tz) for p in pts]
    base = pts[0][1]
    ys = [p[1] / base * 100 for p in pts]
    pct = ys[-1] - 100
    color = "#3ba55d" if pct >= 0 else "#ed4245"
    fig, ax = plt.subplots(figsize=(8, 3.6), dpi=130)
    fig.patch.set_facecolor("#1e1f22")
    ax.set_facecolor("#2b2d31")
    ax.axhline(100, color="#80848e", lw=0.8, ls="--")
    ax.plot(xs, ys, color=color, lw=2)
    ax.fill_between(xs, ys, 100, color=color, alpha=0.18)
    arrow = "▲" if pct >= 0 else "▼"
    ax.set_title(f"{label}   {arrow} {pct:+.2f}%", color="white", fontsize=13, loc="left")
    ax.tick_params(colors="#b5bac1", labelsize=8)
    for s in ax.spines.values():
        s.set_color("#404249")
    ax.grid(color="#404249", alpha=0.5, lw=0.5)
    fmt = "%H:%M" if hours <= 24 else "%d %b"
    ax.xaxis.set_major_formatter(mdates.DateFormatter(fmt, tz=tz))
    fig.autofmt_xdate()
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", facecolor=fig.get_facecolor())
    plt.close(fig)
    ups = sum(p[3] for p in pts)
    downs = sum(p[4] for p in pts)
    tot = max(1, sum(p[2] for p in pts))
    have_h = (pts[-1][0] - pts[0][0]) / 3600
    return buf.getvalue(), pct, ups / tot, downs / tot, have_h


def post(now_reason=""):
    if not HOOK or not _active():
        return False
    flush()
    st = _st()
    specs = [(24, "Last 24 hours", "daily"), (24 * 7, "Last 7 days", "weekly"), (24 * 30, "Last 30 days", "monthly")]
    embeds, files = [], {}
    for hours, label, key in specs:
        res = _chart(_window(st["series"], hours), hours, f"CS2 skin market · {label}")
        if not res:
            embeds.append({"title": f"{label}", "description": "Not enough data yet, the index is still collecting.",
                           "color": 9807270})
            continue
        png, pct, up, down, have_h = res
        note = ""
        if have_h < hours * 0.9:
            note = f"\nOnly {have_h / 24:.1f} days of data so far."
        fname = f"{key}.png"
        files[f"files[{len(files)}]"] = (fname, png, "image/png")
        embeds.append({"title": f"{'📈' if pct >= 0 else '📉'} {label}: {pct:+.2f}%",
                       "description": f"Skins that rose: {up:.0%} · fell: {down:.0%} (of all price changes seen).{note}",
                       "color": 5763719 if pct >= 0 else 15158332, "image": {"url": f"attachment://{fname}"}})
    payload = {"content": "**Market trends** (average change of CSFloat reference prices over the skins the bot sees)",
               "embeds": embeds}
    try:
        r = requests.post(HOOK, data={"payload_json": json.dumps(payload)}, files=files or None, timeout=40)
        return 200 <= r.status_code < 300
    except Exception as e:
        _buyer.M.log("trends: post failed:", repr(e))
        return False


def maybe_post():
    """Called from the sniper's main loop."""
    global _last_check, _forced_done
    if not _active():
        return
    now = time.time()
    if now - _last_check < 60:
        return
    _last_check = now
    flush()
    st = _st()
    local = datetime.now(_tz())
    day = local.strftime("%Y-%m-%d")
    due = local.hour >= POST_HOUR and st["posted_day"] != day
    if FORCE and not _forced_done:
        _forced_done = True
        due = True
    if due and now - st.get("try_ts", 0) > 600:
        st["try_ts"] = now
        if post():
            st["posted_day"] = day
        _buyer.save()
