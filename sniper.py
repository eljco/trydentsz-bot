import os
import sys
import time
from datetime import datetime, timezone

import requests

# ---------------- CONFIG (secrets come from GitHub Actions) ----------------
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "").strip()
CSFLOAT_API_KEY = os.environ.get("CSFLOAT_API_KEY", "").strip()
SEND_TEST_ALERT = os.environ.get("SEND_TEST_ALERT", "false").lower() == "true"

if not DISCORD_WEBHOOK or not CSFLOAT_API_KEY:
    print("Missing DISCORD_WEBHOOK or CSFLOAT_API_KEY environment variables.")
    sys.exit(1)

TARGET_WEAPONS = ["awp", "m4a4", "m4a1-s", "glock", "usp", "ak-47", "mac-10", "galil"]

PREMIUM_STICKERS = [
    "katowice 2014", "katowice 2015", "cologne 2015", "krakow 2017",
    "boston 2018", "london 2018", "cologne 2016", "mlg columbus 2016",
]

MAX_SKIN_PRICE = 35.00  # in the currency CSFloat returns (USD); label below says EUR

# Sticker crafts only alert if you pay at most this % of the sticker value (lower = better deal)
MAX_SP = 4.2

# Charm patterns: only alert on standalone charms (not charms attached to guns)
ALLOW_CHARMS_ON_GUNS = False

# Gun skin pattern alerts (uses the same pattern list). Off by default.
ENABLE_SKIN_PATTERNS = False

# Set DEBUG_DUMP=true in the workflow to print one listing's raw sticker/price fields
DEBUG_DUMP = os.environ.get("DEBUG_DUMP", "false").lower() == "true"
_dumped = False
_sp_warned = False

EXACT_PATTERNS = {
    111, 123, 1234, 12345, 321, 1122, 2233, 2345, 6767, 67,
    69, 6969, 420, 42069, 4545, 8989, 2323, 12223,
    222, 333, 444, 555, 666, 777, 888, 999, 1337, 404,
}

POLLS_PER_RUN = 5
POLL_INTERVAL = 10          # seconds between polls
MAX_LISTING_AGE = 6 * 60    # only alert on listings newer than 6 min (avoids duplicates across runs)

SEEN_IDS = set()


# ---------------- DISCORD ----------------
def send_discord_alert(title, skin_name, price, float_val, identifier, details, listing_id=None):
    fields = [
        {"name": "💰 Price", "value": f"${price:.2f}", "inline": True},
        {"name": "✨ Float", "value": f"{float_val}", "inline": True},
        {"name": "🎯 Match Info", "value": f"`{identifier}`", "inline": True},
    ]
    if listing_id:
        fields.append({"name": "🔗 Listing", "value": f"https://csfloat.com/item/{listing_id}", "inline": False})

    payload = {
        "username": "trydentsz cs2 bot 🔥",
        "embeds": [{
            "title": f"🚨 {title}",
            "description": f"**{skin_name}**\n\n📌 **Trigger Details:** {details}",
            "color": 16711680,
            "fields": fields,
            "footer": {"text": "Cloud Sniper • Monster Edition 24/7"},
        }],
    }
    try:
        r = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
        if r.status_code >= 300:
            print(f"Discord webhook error {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"Failed to send webhook: {e}")


# ---------------- CHECKS ----------------
def check_pattern(pattern_num):
    if pattern_num is None:
        return False
    try:
        n = int(pattern_num)
    except (TypeError, ValueError):
        return False
    p_str = str(n)
    if n in EXACT_PATTERNS:
        return True
    if len(p_str) >= 3 and p_str == p_str[::-1]:  # palindrome
        return True
    if len(p_str) >= 3 and len(set(p_str)) == 1:  # repeaters like 7777
        return True
    return False


def check_float(float_val):
    if float_val is None:
        return None
    if 0 < float_val <= 0.0003:
        return f"Ultra-Low Float ({float_val})"
    if 0.9995 <= float_val <= 0.99999:
        return f"Ultra-High Float ({float_val})"
    return None


def check_stickers(item_data):
    stickers = item_data.get("stickers") or []
    found = []
    for sticker in stickers:
        s_name = (sticker.get("name") or "").lower()
        if any(prem in s_name for prem in PREMIUM_STICKERS):
            found.append(sticker.get("name"))
    return found or None


def compute_sp(listing, item_data):
    """
    Sticker percentage = (price paid above the skin's base price) / (total sticker value) * 100.
    Matches CSFloat's 'SP' badge. Returns None if it can't be calculated.
    """
    # Use a direct value if CSFloat ever exposes one
    for key in ("sticker_percentage", "sp"):
        v = listing.get(key, item_data.get(key))
        if isinstance(v, (int, float)):
            return float(v) * 100 if v <= 1 else float(v)

    price = listing.get("price")
    base = (listing.get("reference") or {}).get("base_price")
    if price is None or base is None:
        return None

    sticker_total = 0
    for s in item_data.get("stickers") or []:
        ref_price = (s.get("reference") or {}).get("price")
        if ref_price:
            sticker_total += ref_price
    if sticker_total <= 0:
        return None

    return max(price - base, 0) / sticker_total * 100


def is_standalone_charm(name):
    return name.startswith("charm |")


def get_charm_pattern(item_data):
    """CSFloat exposes charms as 'keychains'; each has a 'pattern'."""
    if item_data.get("keychain_pattern") is not None:  # standalone charm items
        return item_data.get("keychain_pattern")
    for k in item_data.get("keychains") or []:
        p = k.get("pattern")
        if p is not None:
            return p
    return None


def is_fresh(listing):
    created = listing.get("created_at")
    if not created:
        return True
    try:
        dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - dt).total_seconds()
        return age <= MAX_LISTING_AGE
    except Exception:
        return True


# ---------------- POLL ----------------
def poll_csfloat_api():
    """Returns True if the API call succeeded, False otherwise."""
    url = "https://csfloat.com/api/v1/listings"
    params = {"limit": 50, "sort_by": "most_recent", "type": "buy_now"}
    headers = {
        "Authorization": CSFLOAT_API_KEY,  # raw key, no "Bearer"
        "User-Agent": "trydentsz-sniper/1.0",
        "Accept": "application/json",
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
    except Exception as e:
        print(f"Request exception: {e}")
        return False

    print(f"API Response Status: {response.status_code}")
    if response.status_code != 200:
        body = response.text[:300].replace("\n", " ")
        print(f"API Error Body: {body}")
        if "cloudflare" in response.text.lower() or "just a moment" in response.text.lower():
            print("-> Looks like a Cloudflare block of GitHub's IP range, not an auth problem.")
        return False

    try:
        data = response.json()
    except Exception:
        print("Response was not valid JSON.")
        return False

    listings = data if isinstance(data, list) else data.get("data", [])

    global _dumped
    if DEBUG_DUMP and not _dumped:
        for l in listings:
            if (l.get("item") or {}).get("stickers"):
                import json
                it = l["item"]
                print("DEBUG price:", l.get("price"), "reference:", l.get("reference"))
                print("DEBUG stickers:", json.dumps(it.get("stickers"))[:600])
                print("DEBUG item keys:", list(it.keys()))
                _dumped = True
                break

    for listing in listings:
        listing_id = listing.get("id")
        if listing_id in SEEN_IDS or not is_fresh(listing):
            continue

        item_data = listing.get("item", {}) or {}
        name = (item_data.get("market_hash_name") or "").lower()
        charm_pattern = get_charm_pattern(item_data)

        is_target = any(w in name for w in TARGET_WEAPONS) or is_standalone_charm(name)
        if not is_target:
            continue

        price = (listing.get("price") or 0) / 100.0
        float_val = item_data.get("float_value")
        skin_pattern = item_data.get("paint_seed")  # pattern template (paint_index is the skin ID)

        alerted = False

        # 1. Rare charm pattern (no price cap)
        charm_ok = is_standalone_charm(name) or ALLOW_CHARMS_ON_GUNS
        if charm_ok and charm_pattern is not None and check_pattern(charm_pattern):
            send_discord_alert("RARE CHARM PATTERN FOUND!", name, price, float_val,
                               charm_pattern, f"Matched charm pattern: {charm_pattern}", listing_id)
            alerted = True

        # 2. Premium sticker crafts (price cap AND SP cap)
        if not alerted:
            matched = check_stickers(item_data)
            if matched and price <= MAX_SKIN_PRICE:
                sp = compute_sp(listing, item_data)
                if sp is None:
                    global _sp_warned
                    if not _sp_warned:
                        print("Could not compute SP for a sticker listing - skipping those. "
                              "Run with DEBUG_DUMP=true and send me the output.")
                        _sp_warned = True
                elif sp <= MAX_SP:
                    s = ", ".join(matched)
                    send_discord_alert("PREMIUM STICKER CRAFT FOUND!", name, price, float_val,
                                       f"{sp:.1f}% SP", f"{sp:.1f}% SP | Stickers: {s}", listing_id)
                    alerted = True

        # 3. Rare skin pattern (off by default)
        if (not alerted and ENABLE_SKIN_PATTERNS and not is_standalone_charm(name)
                and check_pattern(skin_pattern) and price <= MAX_SKIN_PRICE):
            send_discord_alert("RARE SKIN PATTERN FOUND!", name, price, float_val,
                               skin_pattern, f"Matched pattern template: {skin_pattern}", listing_id)
            alerted = True

        # 4. Extreme float
        if not alerted:
            float_msg = check_float(float_val)
            if float_msg and price <= MAX_SKIN_PRICE:
                send_discord_alert("EXTREME FLOAT ANOMALY!", name, price, float_val,
                                   skin_pattern if skin_pattern is not None else "N/A",
                                   f"{float_msg} under limit", listing_id)
                alerted = True

        if alerted and listing_id:
            SEEN_IDS.add(listing_id)

    return True


if __name__ == "__main__":
    print("🚀 Cloud Sniper Bot (Authenticated Monster Edition) starting poll loop...")

    successes = 0
    for i in range(POLLS_PER_RUN):
        if poll_csfloat_api():
            successes += 1
        if i < POLLS_PER_RUN - 1:
            time.sleep(POLL_INTERVAL)

    if SEND_TEST_ALERT:
        print("🚀 Sending manual test alert...")
        send_discord_alert(
            "TEST ALERT: SNIPER IS ALIVE!",
            "AK-47 | Frontside Misty (Field-Tested)",
            14.72, 0.191136, "414",
            "Manual test trigger - checking Discord connection!",
        )

    # Make the job go red if every poll failed, so 403s aren't hidden behind a green check
    if successes == 0:
        print("All polls failed.")
        sys.exit(1)
