
import time
import requests
import os

# Your Discord Webhook URL
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1569335516392988756/DCKSmOW9pS6rb7ncwC3eL56q_BcKAFjMfjx1atcxf5T2_GUG7lelOmEq2MPpLxfsHV9g"

# Target Weapons
TARGET_WEAPONS = ["awp", "m4a4", "m4a1-s", "glock", "usp", "ak-47", "mac-10", "galil"]

# Premium Stickers for <= 4.2% SP
PREMIUM_STICKERS = [
    "katowice 2014", "katowice 2015", "cologne 2015", "krakow 2017", 
    "boston 2018", "london 2018", "cologne 2016", "mlg columbus 2016"
]

# Exact Patterns & Charms
EXACT_PATTERNS = {
    111, 123, 1234, 12345, 321, 1122, 2233, 2345, 6767, 67, 
    69, 6969, 420, 42069, 4545, 8989, 2323, 12223,
    222, 333, 444, 555, 666, 777, 888, 999, 1337, 404
}

def send_discord_alert(title, skin_name, price, float_val, pattern, details):
    payload = {
        "username": "trydentsz cs2 bot 🔥",
        "embeds": [{
            "title": f"🚨 {title}",
            "description": f"**{skin_name}**\n\n📌 **Trigger Details:** {details}",
            "color": 16711680,
            "fields": [
                {"name": "💰 Price", "value": f"${price}", "inline": True},
                {"name": "✨ Float", "value": f"{float_val}", "inline": True},
                {"name": "🔢 Pattern", "value": f"`{pattern}`", "inline": True}
            ],
            "footer": {"text": "Cloud Sniper • Running 24/7 on GitHub Actions"}
        }]
    }
    try:
        requests.post(DISCORD_WEBHOOK, json=payload)
    except Exception as e:
        print(f"Failed to send webhook: {e}")

def check_pattern(pattern_num):
    if not pattern_num:
        return False
    p_str = str(pattern_num)
    if int(pattern_num) in EXACT_PATTERNS:
        return True
    if len(p_str) >= 3 and p_str == p_str[::-1]: # Palindrome
        return True
    if len(set(p_str)) == 1: # Repeaters like 7777
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

def poll_csfloat_api():
    """Scans CSFloat public listings API"""
    url = "https://csfloat.com/api/v1/listings?limit=25"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"API Error: {response.status_code}")
            return
        
        listings = response.json()
        if not isinstance(listings, list):
            return

        for item in listings:
            item_data = item.get("item", {})
            name = item_data.get("market_hash_name", "").lower()
            
            # Filter weapons/charms
            is_target = any(w in name for w in TARGET_WEAPONS) or item.get("charm") is not None
            if not is_target:
                continue

            price = item.get("price", 0) / 100.0
            float_val = item_data.get("float_value")
            pattern = item_data.get("paint_index")
            charm_pattern = item.get("charm", {}).get("pattern_index")
            
            active_pattern = charm_pattern or pattern

            # 1. Check Patterns / Charms
            if check_pattern(active_pattern):
                send_discord_alert("RARE PATTERN / CHARM FOUND!", name, price, float_val, active_pattern, f"Matched pattern: {active_pattern}")
                continue

            # 2. Check Float Extremes
            float_msg = check_float(float_val)
            if float_msg:
                send_discord_alert("EXTREME FLOAT ANOMALY!", name, price, float_val, pattern or "N/A", float_msg)
                continue

    except Exception as e:
        print(f"Polling exception: {e}")

if __name__ == "__main__":
    print("🚀 Cloud Sniper Bot starting poll loop...")
    # Runs a few iterations per workflow trigger
    for _ in range(5):
        poll_csfloat_api()
        time.sleep(10)
