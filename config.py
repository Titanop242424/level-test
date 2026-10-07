# -*- coding: utf-8 -*-
"""
Configuration module for FreeFire Level Up Bot & Web System
Supports .env, config.json, or environment variables.
"""
import os
import json

# Try to load python-dotenv if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")

# Load from config.json if exists
config_json = {}
if os.path.exists(CONFIG_FILE):
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config_json = json.load(f)
    except Exception:
        config_json = {}

def get_config(key: str, default: str = "") -> str:
    val = os.environ.get(key)
    if val is not None and str(val).strip():
        return str(val).strip()
    if key in config_json and str(config_json[key]).strip():
        return str(config_json[key]).strip()
    return default

# MongoDB Configuration
# User can set their MongoDB Cluster connection string:
# Example: mongodb+srv://<username>:<password>@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority
MONGO_URI = get_config("MONGO_URI", get_config("LIVE_MONGO_URI", ""))
DB_NAME = get_config("DB_NAME", "levelup_system_db")

# Free Fire Game Proxy (Optional: specify Indian/BD proxy to bypass BR_FFI_BLOCKED_NOT_IND_REGION_LOGIN when VPS is abroad)
# Example: http://user:pass@indian-proxy:port or socks5://indian-proxy:1080
FF_PROXY = get_config("FF_PROXY", get_config("PROXY_URL", get_config("HTTP_PROXY", "")))
AUTO_PROXY_BD_IND = get_config("AUTO_PROXY_BD_IND", "true").lower() in ("true", "1", "yes")

# JWT & Security
JWT_SECRET = get_config("JWT_SECRET", "super_secret_cyber_levelup_key_2026_@#!OB55")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_DAYS = int(get_config("JWT_EXPIRATION_DAYS", "15"))

# Initial Admin Credentials (Created if no admin exists)
ADMIN_USERNAME = get_config("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = get_config("ADMIN_PASSWORD", "admin1234")

# Server Configuration
WEB_HOST = get_config("WEB_HOST", "0.0.0.0")
WEB_PORT = int(get_config("WEB_PORT", "20335"))

# Support / Contact Links (defaults from screenshots)
TELEGRAM_CHANNEL = get_config("TELEGRAM_CHANNEL", "https://t.me/+fWms1r9krpZkNjE1")
TELEGRAM_GROUP = get_config("TELEGRAM_GROUP", "https://t.me/lvlgroupff")
TELEGRAM_SUPPORT = get_config("TELEGRAM_SUPPORT", "https://t.me/supprotby")
UPI_ID = get_config("UPI_ID", "silentkiller@upi")
UPI_NAME = get_config("UPI_NAME", "LEVEL UP SERVICE")
QR_CODE_URL = get_config("QR_CODE_URL", "./static/image.png")
BINANCE_ID = get_config("BINANCE_ID", "1231529920")

# Automated UPI Payment Gateway (FamPay API)
PAYMENT_API_KEY = get_config("PAYMENT_API_KEY", "JAISSU-e96a9a1fffb2d71b562d9107")
PAYMENT_API_BASE = get_config("PAYMENT_API_BASE", "https://wfx-jaissu-pay.vercel.app")
PAYMENT_MERCHANT_TYPE = get_config("PAYMENT_MERCHANT_TYPE", "fampay")

# ==============================================================================
# Matchmaking & Game Wait Delays (Manage directly here in config.py)
# When a match is found and entered in game, wait this many seconds before
# searching and entering the next match:
# ==============================================================================
BR_MATCH_WAIT_SECONDS = 150.0   # BR (Battle Royale) delay after match entered (180s)
LW_MATCH_WAIT_SECONDS = 10.0    # LW (Lone Wolf) delay after match entered (15s)

# StartMatch queue retry interval (seconds between search requests while waiting in queue)
BR_QUEUE_SEARCH_INTERVAL = 3.0  # BR queue retry interval (seconds)
LW_QUEUE_SEARCH_INTERVAL = 2.0  # LW queue retry interval (seconds)

# Helper function to read config overrides while ignoring outdated legacy values (like 2.0s)
def _resolve_config_float(key: str, default: float, legacy_defaults: list = None) -> float:
    raw = get_config(key, "")
    if raw:
        if legacy_defaults and raw in [str(x) for x in legacy_defaults]:
            return default
        try:
            return float(raw)
        except Exception:
            pass
    return default

# Matchmaking Delays exported for the bot
BR_NEW_MATCH_DELAY = _resolve_config_float("BR_NEW_MATCH_DELAY", BR_MATCH_WAIT_SECONDS, ["2.0", "2", "2.5"])
LW_NEW_MATCH_DELAY = _resolve_config_float("LW_NEW_MATCH_DELAY", LW_MATCH_WAIT_SECONDS, ["2.0", "2"])
BR_START_MATCH_INTERVAL = _resolve_config_float("BR_START_MATCH_INTERVAL", BR_QUEUE_SEARCH_INTERVAL, ["180.0", "180"])
LW_START_MATCH_INTERVAL = _resolve_config_float("LW_START_MATCH_INTERVAL", LW_QUEUE_SEARCH_INTERVAL, ["30.0", "30"])

print(f"[Config] Initialized. Mongo configured: {'YES' if MONGO_URI else 'NO (Local fallback store active until Mongo URI is provided)'}")
print(f"[Config] Match wait times -> BR: {BR_NEW_MATCH_DELAY}s | LW: {LW_NEW_MATCH_DELAY}s (managed in config.py)")

