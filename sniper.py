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

# Max price limit for gun skins/sticker crafts (Euro)
MAX_SKIN_PRICE = 40.00

# Exact Patterns & Charms
EXACT_PATTERNS = {
    111, 123, 1234, 12345, 321, 1122, 2233, 2345, 6767, 67, 
    69, 6969, 420, 42069, 4545, 8989, 2323, 12223,
    222, 333, 444, 555, 666, 777, 888, 999, 1337, 404
}

def send_discord_alert(title, skin_name, price, float_val, identifier, details):
    payload = {
        "username": "trydentsz cs2 bot 🔥",
        "embeds": [{
            "title": f"🚨 {title}",
            "description": f"**{skin_name}**\n\n📌 **Trigger Details:** {details}",
            "color": 16711680,
            "fields": [
                {"name": "💰 Price", "value": f"€{price:.2f}", "inline": True},
                {"name": "✨ Float", "value": f"{float_val}", "inline": True},
                {"name": "🎯 Match Info", "value": f"`{identifier}`", "inline": True}
            ],
            "footer": {"text": "Cloud Sniper • Monster Edition 24/7"}
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

def check_stickers(item_data):
    """Scans item stickers array for premium tournament stickers"""
    stickers = item_data.get("stickers", [])
    if not stickers:
        return None
    
    found_stickers = []
    for sticker in stickers:
        s_name = sticker.get("name", "").lower()
        for prem in PREMIUM_STICKERS:
            if prem in s_name:
                found_stickers.append(sticker.get("name"))
                break
    return found_stickers if found_stickers else None

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
            
            skin_pattern = item_data.get("paint_index")
            charm_data = item.get("charm")
            charm_pattern = charm_data.get("pattern_index") if charm_data else None

            # 1. Check Charms (No price cap restriction)
            if charm_pattern and check_pattern(charm_pattern):
                send_discord_alert("RARE CHARM PATTERN FOUND!", name, price, float_val, charm_pattern, f"Matched charm pattern ID: {charm_pattern}")
                continue

            # 2. Check Premium Sticker Crafts (Enforcing <= €40 price limit)
            matched_stickers = check_stickers(item_data)
            if matched_stickers and price <= MAX_SKIN_PRICE:
                sticker_list_str = ", ".join(matched_stickers)
                send_discord_alert("PREMIUM STICKER CRAFT FOUND!", name, price, float_val, sticker_list_str, f"Found high-tier stickers under €40 limit! Stickers: {sticker_list_str}")
                continue

            # 3. Check Gun Skin Patterns (Enforcing <= €40 price limit)
            if skin_pattern and check_pattern(skin_pattern) and price <= MAX_SKIN_PRICE:
                send_discord_alert("RARE SKIN PATTERN FOUND!", name, price, float_val, skin_pattern, f"Matched pattern template under €40: {skin_pattern}")
                continue

            # 4. Check Float Extremes (Enforcing <= €40 price limit)
            float_msg = check_float(float_val)
            if float_msg and price <= MAX_SKIN_PRICE:
                send_discord_alert("EXTREME FLOAT ANOMALY!", name, price, float_val, skin_pattern or "N/A", f"{float_msg} under €40 limit")
                continue

    except Exception as e:
        print(f"Polling exception: {e}")

if __name__ == "__main__":
    print("🚀 Cloud Sniper Bot (Monster Edition) starting poll loop...")
    for _ in range(5):
        poll_csfloat_api()
        time.sleep(10)
if __name__ == "__main__":
    print("🚀 Forcing manual test alert...")
    # Send a fake test notification right now to verify Discord webhook
    send_discord_alert(
        "TEST ALERT: SNIPER IS ALIVE!", 
        "AK-47 | Frontside Misty (Field-Tested)", 
        14.72, 
        0.191136, 
        "414", 
        "Manual test trigger - checking Discord connection!"
    )
