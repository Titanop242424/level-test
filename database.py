# -*- coding: utf-8 -*-
"""
Database layer with native MongoDB Cluster support and seamless local fallback.
Handles Users, JWT Auth, bcrypt password hashing, Plans, Orders, and Settings.
"""
import os
import json
import time
import uuid
import datetime
import re
from typing import Dict, List, Any, Optional, Tuple

import bcrypt
import jwt
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError, PyMongoError

from config import (
    MONGO_URI, DB_NAME, JWT_SECRET, JWT_ALGORITHM, JWT_EXPIRATION_DAYS,
    ADMIN_USERNAME, ADMIN_PASSWORD, TELEGRAM_CHANNEL, TELEGRAM_GROUP,
    TELEGRAM_SUPPORT, UPI_ID, UPI_NAME, QR_CODE_URL, BINANCE_ID
)

DEFAULT_PLANS = [
    {
        "id": "plan_6h",
        "name": "6 Hours Sprint",
        "duration_hours": 6,
        "duration_text": "6 hours",
        "price_inr": 30,
        "price_usd": 0.30,
        "slots": 3,
        "badge": "Trial",
        "popular": False,
        "features": [
            "3 Concurrent Accounts",
            "6 Hours Non-Stop Leveling",
            "Fast Matchmaking & EXP Grinding",
            "Anti-Ban Protection (OB55 Safe)",
            "24/7 Bot Runtime",
            "Telegram Support"
        ]
    },
    {
        "id": "plan_24h",
        "name": "24 Hours Turbo",
        "duration_hours": 24,
        "duration_text": "24 hours (1 Day)",
        "price_inr": 70,
        "price_usd": 0.85,
        "slots": 5,
        "badge": "Popular",
        "popular": True,
        "features": [
            "5 Concurrent Accounts",
            "24 Hours Turbo Leveling",
            "Priority Matchmaking Queue",
            "Auto Reconnect Watchdog",
            "Real-Time EXP Webhook",
            "24/7 VIP Support"
        ]
    },
    {
        "id": "plan_3d",
        "name": "3 Days Pro",
        "duration_hours": 72,
        "duration_text": "3 Days (72 Hours)",
        "price_inr": 180,
        "price_usd": 2.15,
        "slots": 8,
        "badge": "Value",
        "popular": False,
        "features": [
            "8 Concurrent Accounts",
            "72 Hours Continuous Grinding",
            "Multi-Region Auto Balancing",
            "Max EXP Yield per Match",
            "OB55 Protocol Optimization",
            "Dedicated Support Agent"
        ]
    },
    {
        "id": "plan_7d",
        "name": "7 Days Champion",
        "duration_hours": 168,
        "duration_text": "7 Days (1 Week)",
        "price_inr": 350,
        "price_usd": 4.20,
        "slots": 12,
        "badge": "Best Seller",
        "popular": False,
        "features": [
            "12 Concurrent Accounts",
            "168 Hours Non-Stop Grinding",
            "Fast Level 10-60 Boost",
            "Automatic Device Randomizer",
            "Zero Downtime Failover",
            "Priority Telegram Hotline"
        ]
    },
    {
        "id": "plan_30d",
        "name": "30 Days Dominator",
        "duration_hours": 720,
        "duration_text": "30 Days (1 Month)",
        "price_inr": 1200,
        "price_usd": 14.50,
        "slots": 25,
        "badge": "Supreme",
        "popular": False,
        "features": [
            "25 Concurrent Accounts",
            "Full Month Unlimited EXP",
            "Dedicated Match Session Worker",
            "Custom Device Spoofing",
            "Instant Slot Reallocation",
            "Personal VIP Account Manager"
        ]
    }
]

class DatabaseManager:
    def __init__(self):
        self.is_mongo = False
        self.client: Optional[MongoClient] = None
        self.db = None
        self.fallback_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "levelup_local_db.json")
        self.fallback_data: Dict[str, Any] = {
            "users": [],
            "plans": DEFAULT_PLANS,
            "orders": [],
            "accounts_meta": [],
            "settings": {
                "upi_id": UPI_ID,
                "upi_name": UPI_NAME,
                "qr_code_url": QR_CODE_URL,
                "binance_id": BINANCE_ID,
                "telegram_channel": TELEGRAM_CHANNEL,
                "telegram_group": TELEGRAM_GROUP,
                "telegram_support": TELEGRAM_SUPPORT,
                "announcement": "Welcome to Level Up Bot! Instant activation after admin approval."
            }
        }
        # In-memory caching for lightning fast responses
        self._plans_cache: Optional[List[Dict[str, Any]]] = None
        self._plans_cache_time: float = 0.0

        self._settings_cache: Optional[Dict[str, Any]] = None
        self._settings_cache_time: float = 0.0

        self._users_cache: Optional[List[Dict[str, Any]]] = None
        self._users_cache_time: float = 0.0

        self._user_by_id_cache: Dict[str, Tuple[Dict[str, Any], float]] = {}
        self._user_by_uname_cache: Dict[str, Tuple[Dict[str, Any], float]] = {}

        self._accounts_meta_cache: Optional[List[Dict[str, Any]]] = None
        self._accounts_meta_cache_time: float = 0.0

        self.init_db()

    def invalidate_users_cache(self):
        self._users_cache = None
        self._users_cache_time = 0.0
        self._user_by_id_cache.clear()
        self._user_by_uname_cache.clear()
        self._accounts_meta_cache = None
        self._accounts_meta_cache_time = 0.0

    def invalidate_plans_cache(self):
        self._plans_cache = None
        self._plans_cache_time = 0.0

    def invalidate_settings_cache(self):
        self._settings_cache = None
        self._settings_cache_time = 0.0

    def invalidate_accounts_meta_cache(self):
        self._accounts_meta_cache = None
        self._accounts_meta_cache_time = 0.0

    def init_db(self):
        # 1. Try to connect to MongoDB cluster if URI is provided
        uri = MONGO_URI.strip()
        if uri:
            try:
                print(f"[Database] Connecting to MongoDB Cluster: {uri[:25]}***...")
                self.client = MongoClient(
                    uri,
                    serverSelectionTimeoutMS=4000,
                    connectTimeoutMS=4000,
                    socketTimeoutMS=5000,
                    maxPoolSize=50,
                    minPoolSize=5
                )
                # Verify connection
                self.client.admin.command('ping')
                self.db = self.client[DB_NAME]
                self.is_mongo = True
                print(f"\033[92m[+] Successfully connected to MongoDB Cluster (Database: {DB_NAME})!\033[0m")
                self._seed_mongo()
                return
            except Exception as e:
                print(f"\033[93m[!] Could not connect to MongoDB Cluster ({e}). Switching to local fallback store.\033[0m")
                self.is_mongo = False
        else:
            print("[Database] No MONGO_URI provided in config. Using local persistent JSON store.")

        # 2. Local fallback storage
        self._load_fallback()
        self._seed_fallback()

    def _load_fallback(self):
        if os.path.exists(self.fallback_file):
            try:
                with open(self.fallback_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        self.fallback_data.update(data)
            except Exception as e:
                print(f"[-] Error loading local fallback DB: {e}")

    def _save_fallback(self):
        try:
            tmp = self.fallback_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.fallback_data, f, indent=2)
            os.replace(tmp, self.fallback_file)
        except Exception as e:
            print(f"[-] Error saving local fallback DB: {e}")

    # ==================== SEEDING ====================
    def _seed_mongo(self):
        try:
            local_data = {}
            if os.path.exists(self.fallback_file):
                try:
                    with open(self.fallback_file, "r", encoding="utf-8") as f:
                        local_data = json.load(f)
                except Exception:
                    local_data = {}

            # Seed / Migrate Users
            if self.db.users.count_documents({}) == 0:
                if local_data.get("users"):
                    self.db.users.insert_many(local_data["users"])
                    print(f"\033[92m[+] Migrated {len(local_data['users'])} users to MongoDB Cluster!\033[0m")
                else:
                    admin_user = self.get_user_by_username(ADMIN_USERNAME)
                    if not admin_user:
                        pwd_hash = self.hash_password(ADMIN_PASSWORD)
                        self.db.users.insert_one({
                            "id": str(uuid.uuid4()),
                            "username": ADMIN_USERNAME,
                            "password_hash": pwd_hash,
                            "role": "admin",
                            "slots": 999,
                            "plan": "Administrator (Infinite)",
                            "plan_expires_at": None,
                            "telegram": "@SIL3NT_KILLER",
                            "status": "active",
                            "created_at": time.time()
                        })
                        print(f"\033[92m[+] Default Admin user created in MongoDB: {ADMIN_USERNAME}\033[0m")

            # Seed / Migrate Plans
            if self.db.plans.count_documents({}) == 0:
                plans_to_seed = local_data.get("plans") or DEFAULT_PLANS
                self.db.plans.insert_many(plans_to_seed)
                print(f"[Database] Seeded {len(plans_to_seed)} plans into MongoDB.")

            # Seed / Migrate Orders
            if self.db.orders.count_documents({}) == 0 and local_data.get("orders"):
                self.db.orders.insert_many(local_data["orders"])
                print(f"[Database] Migrated {len(local_data['orders'])} orders into MongoDB.")

            # Seed / Migrate Accounts Meta
            if self.db.accounts_meta.count_documents({}) == 0 and local_data.get("accounts_meta"):
                self.db.accounts_meta.insert_many(local_data["accounts_meta"])
                print(f"[Database] Migrated {len(local_data['accounts_meta'])} accounts meta into MongoDB.")

            # Seed / Migrate Settings
            if self.db.settings.count_documents({}) == 0:
                sett = local_data.get("settings") or {
                    "upi_id": UPI_ID,
                    "upi_name": UPI_NAME,
                    "qr_code_url": QR_CODE_URL,
                    "binance_id": BINANCE_ID,
                    "telegram_channel": TELEGRAM_CHANNEL,
                    "telegram_group": TELEGRAM_GROUP,
                    "telegram_support": TELEGRAM_SUPPORT,
                    "announcement": "Welcome to Level Up Bot! Instant activation after admin approval."
                }
                self.db.settings.insert_one(sett)
                print("[Database] Seeded settings into MongoDB.")
        except Exception as e:
            print(f"[-] MongoDB seeding error: {e}")

    def _seed_fallback(self):
        # Plans
        if not self.fallback_data.get("plans"):
            self.fallback_data["plans"] = DEFAULT_PLANS

        # Admin
        users = self.fallback_data.get("users", [])
        has_admin = any(u.get("role") == "admin" for u in users)
        if not has_admin:
            pwd_hash = self.hash_password(ADMIN_PASSWORD)
            users.append({
                "id": str(uuid.uuid4()),
                "username": ADMIN_USERNAME,
                "password_hash": pwd_hash,
                "role": "admin",
                "slots": 999,
                "plan": "Administrator (Infinite)",
                "plan_expires_at": None,
                "telegram": "@SIL3NT_KILLER",
                "status": "active",
                "created_at": time.time()
            })
            self.fallback_data["users"] = users
            self._save_fallback()
            print(f"\033[92m[+] Default Admin user created in local DB: {ADMIN_USERNAME}\033[0m")

    # ==================== PASSWORD & JWT ====================
    @staticmethod
    def hash_password(password: str) -> str:
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")

    @staticmethod
    def verify_password(password: str, hashed: str) -> bool:
        try:
            return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
        except Exception:
            return False

    def generate_token(self, user_dict: Dict[str, Any]) -> str:
        payload = {
            "user_id": user_dict["id"],
            "username": user_dict["username"],
            "role": user_dict.get("role", "user"),
            "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=JWT_EXPIRATION_DAYS),
            "iat": datetime.datetime.now(datetime.timezone.utc)
        }
        return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            return payload
        except Exception:
            return None

    # ==================== USER MANAGEMENT ====================
    def register_user(self, username: str, password: str, telegram: str = "") -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        username = str(username).strip()
        if len(username) < 3 or len(username) > 32:
            return False, "Username must be between 3 and 32 characters", None
        if not re.match(r'^[a-zA-Z0-9_\-\.@]+$', username):
            return False, "Username can only contain letters, numbers, hyphens, and underscores", None

        if len(password) < 6:
            return False, "Password must be at least 6 characters long", None

        # Check existing
        existing = self.get_user_by_username(username)
        if existing:
            return False, "Username already exists. Please sign in.", None

        # Role determination: if no users exist at all, make first user admin
        role = "user"
        slots = 0  # 0 slots until buying plan or approved by admin
        plan = "Free Tier (No active plan)"

        user_doc = {
            "id": str(uuid.uuid4()),
            "username": username,
            "password_hash": self.hash_password(password),
            "role": role,
            "slots": slots,
            "plan": plan,
            "plan_expires_at": None,
            "telegram": telegram.strip(),
            "status": "active",
            "created_at": time.time()
        }

        if self.is_mongo:
            try:
                self.db.users.insert_one(user_doc)
            except Exception as e:
                return False, f"Database error: {e}", None
        else:
            self.fallback_data["users"].append(user_doc)
            self._save_fallback()

        self.invalidate_users_cache()
        clean_user = self._clean_user(user_doc)
        token = self.generate_token(user_doc)
        clean_user["token"] = token
        return True, "User registered successfully", clean_user

    def authenticate_user(self, username: str, password: str) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        username = username.strip()
        user = self.get_user_by_username(username)
        if not user:
            return False, "Invalid username or password", None

        if user.get("status") == "banned":
            return False, "This account has been suspended by administration.", None

        if not self.verify_password(password, user.get("password_hash", "")):
            return False, "Invalid username or password", None

        token = self.generate_token(user)
        clean = self._clean_user(user)
        clean["token"] = token
        return True, "Authentication successful", clean

    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        uname = username.strip().lower()
        now = time.time()
        if uname in self._user_by_uname_cache:
            cached_u, exp = self._user_by_uname_cache[uname]
            if now < exp:
                return cached_u

        user = None
        if self.is_mongo:
            try:
                user = self.db.users.find_one({"username": {"$regex": f"^{username.strip()}$", "$options": "i"}})
            except Exception:
                pass
        if not user:
            for u in self.fallback_data.get("users", []):
                if u.get("username", "").lower() == uname:
                    user = u
                    break

        if user:
            self._user_by_uname_cache[uname] = (user, now + 5.0)
            if user.get("id"):
                self._user_by_id_cache[str(user["id"])] = (user, now + 5.0)
        return user

    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        uid_str = str(user_id)
        now = time.time()
        if uid_str in self._user_by_id_cache:
            cached_u, exp = self._user_by_id_cache[uid_str]
            if now < exp:
                return cached_u

        user = None
        if self.is_mongo:
            try:
                user = self.db.users.find_one({"id": uid_str})
            except Exception:
                pass
        if not user:
            for u in self.fallback_data.get("users", []):
                if str(u.get("id")) == uid_str:
                    user = u
                    break

        if user:
            self._user_by_id_cache[uid_str] = (user, now + 5.0)
            if user.get("username"):
                self._user_by_uname_cache[str(user["username"]).strip().lower()] = (user, now + 5.0)
        return user

    def get_all_users(self) -> List[Dict[str, Any]]:
        now = time.time()
        if self._users_cache is not None and (now - self._users_cache_time) < 5.0:
            return self._users_cache

        result = []
        if self.is_mongo:
            try:
                cursor = self.db.users.find().sort("created_at", -1)
                for u in cursor:
                    result.append(self._clean_user(u))
            except Exception:
                pass
        if not result:
            for u in self.fallback_data.get("users", []):
                result.append(self._clean_user(u))
        result.sort(key=lambda x: x.get("created_at", 0), reverse=True)

        self._users_cache = result
        self._users_cache_time = now
        return result

    def update_user(self, user_id: str, updates: Dict[str, Any]) -> bool:
        # Don't allow updating id or password_hash directly via this method
        safe_updates = {k: v for k, v in updates.items() if k not in ["id", "password_hash"]}
        ok = False
        if self.is_mongo:
            try:
                self.db.users.update_one({"id": str(user_id)}, {"$set": safe_updates})
                ok = True
            except Exception as e:
                print(f"[-] Mongo update_user error: {e}")
                ok = False
        else:
            for u in self.fallback_data.get("users", []):
                if str(u.get("id")) == str(user_id):
                    u.update(safe_updates)
                    self._save_fallback()
                    ok = True
                    break
        if ok:
            self.invalidate_users_cache()
        return ok

    def count_users(self) -> int:
        if self.is_mongo:
            try:
                return self.db.users.count_documents({})
            except Exception:
                pass
        return len(self.fallback_data.get("users", []))

    def _clean_user(self, user_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Strip password_hash and mongo _id for safe API response."""
        res = dict(user_doc)
        res.pop("password_hash", None)
        res.pop("_id", None)
        # Check plan expiry and calculate remaining time
        expiry = res.get("plan_expires_at")
        now = time.time()
        if expiry and isinstance(expiry, (int, float)):
            if now > expiry:
                res["plan_expired"] = True
                res["remaining_seconds"] = 0
                res["remaining_time_str"] = "Expired"
                res["plan"] = "Expired"
                if res.get("role") != "admin":
                    res["slots"] = 0
            else:
                res["plan_expired"] = False
                rem = max(0, int(expiry - now))
                res["remaining_seconds"] = rem
                days = rem // 86400
                hours = (rem % 86400) // 3600
                mins = (rem % 3600) // 60
                secs = rem % 60
                if days > 0:
                    res["remaining_time_str"] = f"{days}d {hours}h {mins}m"
                elif hours > 0:
                    res["remaining_time_str"] = f"{hours}h {mins}m {secs}s"
                else:
                    res["remaining_time_str"] = f"{mins}m {secs}s"
                # If plan was set to Expired or default but expiry is active in future, show active plan name
                current_plan = res.get("plan", "")
                if current_plan in ["Expired", "Free Tier", "Free Tier (No active plan)", None, ""]:
                    # Find latest approved order plan name if available
                    user_orders = self.get_user_orders(res.get("id", ""))
                    approved_order = next((o for o in user_orders if o.get("status") == "approved"), None)
                    res["plan"] = approved_order.get("plan_name", "Active VIP Plan") if approved_order else "Active VIP Plan"
            try:
                res["plan_expires_at_formatted"] = datetime.datetime.fromtimestamp(expiry).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                res["plan_expires_at_formatted"] = str(expiry)
        else:
            if res.get("role") == "admin":
                res["plan_expired"] = False
                res["remaining_seconds"] = None
                res["remaining_time_str"] = "Unlimited (Admin)"
                res["plan_expires_at_formatted"] = "Lifetime (Admin)"
                res["plan"] = "Administrator (Infinite)"
                res["slots"] = 999
            else:
                res["plan_expired"] = True
                res["remaining_seconds"] = 0
                res["remaining_time_str"] = "No Active Plan"
                res["plan_expires_at_formatted"] = "None"
                res["plan"] = "Free Tier (No active plan)"
                res["slots"] = 0
        return res

    # ==================== ACCOUNT METADATA & OWNERSHIP ====================
    def link_account_to_user(self, user_id: str, account_info: Dict[str, Any]):
        """Associate a FreeFire account UID with the user who created it."""
        username = str(account_info.get("username", "")).strip()
        if not username:
            u = self.get_user_by_id(user_id)
            if u:
                username = u.get("username", "")

        uid_str = str(account_info.get("uid", "")).strip()
        acc_doc = {
            "id": str(uuid.uuid4()),
            "user_id": str(user_id),
            "username": username,
            "uid": uid_str,
            "token": str(account_info.get("token", "")),
            "nickname": str(account_info.get("nickname") or f"Account_{uid_str[:6]}"),
            "level": int(account_info.get("level", 1) or 1),
            "exp": int(account_info.get("exp", 0) or 0),
            "region": str(account_info.get("region", "IND")),
            "is_admin": bool(account_info.get("is_admin", False)),
            "created_at": time.time()
        }
        if self.is_mongo:
            try:
                self.db.accounts_meta.delete_many({"uid": acc_doc["uid"]})
                self.db.accounts_meta.insert_one(acc_doc)
            except Exception as e:
                print(f"[-] Mongo link_account error: {e}")
        else:
            accs = self.fallback_data.setdefault("accounts_meta", [])
            self.fallback_data["accounts_meta"] = [a for a in accs if a.get("uid") != acc_doc["uid"]]
            self.fallback_data["accounts_meta"].append(acc_doc)
            self._save_fallback()

        self.invalidate_accounts_meta_cache()

    def update_account_profile(self, uid: str, nickname: Optional[str] = None, level: Optional[int] = None, exp: Optional[int] = None, in_game_id: Optional[str] = None):
        """Update live nickname/level/exp on the stored account metadata."""
        uid_str = str(uid).strip()
        updates = {}
        if nickname: updates["nickname"] = str(nickname)
        if level is not None and level > 0: updates["level"] = int(level)
        if exp is not None and exp > 0: updates["exp"] = int(exp)
        if in_game_id: updates["in_game_id"] = str(in_game_id)
        if not updates: return
        if self.is_mongo:
            try:
                conds = [{"uid": uid_str}, {"in_game_id": uid_str}]
                if in_game_id: conds.append({"uid": str(in_game_id).strip()})
                self.db.accounts_meta.update_many({"$or": conds}, {"$set": updates})
            except Exception as e:
                print(f"[-] Mongo update_account_profile error: {e}")
        else:
            for a in self.fallback_data.get("accounts_meta", []):
                if a.get("uid") == uid_str or a.get("in_game_id") == uid_str or (in_game_id and a.get("uid") == str(in_game_id).strip()):
                    a.update(updates)
            self._save_fallback()
        self.invalidate_accounts_meta_cache()

    def upgrade_token_account_meta(self, token: str, account_id: str, nickname: Optional[str] = None,
                                   level: Optional[int] = None, exp: Optional[int] = None,
                                   region: Optional[str] = None):
        """When an account logs in via token, upgrade placeholder uid (tok_...) to real game account_id."""
        tok = str(token).strip()
        acc_id_str = str(account_id).strip()
        if not tok or not acc_id_str:
            return

        updates = {"uid": acc_id_str, "account_id": acc_id_str, "display_uid": acc_id_str}
        if nickname: updates["nickname"] = str(nickname)
        if level is not None and level > 0: updates["level"] = int(level)
        if exp is not None and exp > 0: updates["exp"] = int(exp)
        if region: updates["region"] = str(region)

        tok_10 = f"tok_{tok[:10]}"
        tok_20 = f"tok_{tok[:20]}"

        if self.is_mongo:
            try:
                self.db.accounts_meta.update_many(
                    {"$or": [
                        {"token": tok},
                        {"uid": tok_10},
                        {"uid": tok_20},
                        {"uid": acc_id_str}
                    ]},
                    {"$set": updates}
                )
            except Exception as e:
                print(f"[-] Mongo upgrade_token_account_meta error: {e}")
        else:
            for a in self.fallback_data.get("accounts_meta", []):
                if a.get("token") == tok or a.get("uid") in [tok_10, tok_20, acc_id_str]:
                    a.update(updates)
            self._save_fallback()
        self.invalidate_accounts_meta_cache()

    def get_user_account_uids(self, user_id: str) -> List[str]:
        all_meta = self.get_all_accounts_meta()
        uid_str = str(user_id)
        return [str(a.get("uid")) for a in all_meta if str(a.get("user_id")) == uid_str and a.get("uid")]

    def get_user_accounts_meta(self, user_id: str) -> List[Dict[str, Any]]:
        """Get full account docs owned by a specific user using cached accounts meta."""
        all_meta = self.get_all_accounts_meta()
        uid_str = str(user_id)
        return [dict(a) for a in all_meta if str(a.get("user_id")) == uid_str]

    def get_all_accounts_meta(self) -> List[Dict[str, Any]]:
        """Get all accounts linked across all users with enriched owner info."""
        now = time.time()
        if self._accounts_meta_cache is not None and (now - self._accounts_meta_cache_time) < 5.0:
            return self._accounts_meta_cache

        accounts = []
        if self.is_mongo:
            try:
                cursor = self.db.accounts_meta.find().sort("created_at", -1)
                for a in cursor:
                    a.pop("_id", None)
                    accounts.append(a)
            except Exception:
                pass
        else:
            accounts = [dict(a) for a in self.fallback_data.get("accounts_meta", [])]

        # Enrich with owner username & telegram (cached)
        users_map = {u["id"]: u for u in self.get_all_users()}
        for a in accounts:
            u = users_map.get(a.get("user_id"))
            if u:
                a["owner_username"] = u.get("username", a.get("username", "Unknown"))
                a["owner_telegram"] = u.get("telegram", "")
            else:
                a["owner_username"] = a.get("username", "Unknown")
                a["owner_telegram"] = ""
        accounts.sort(key=lambda x: x.get("created_at", 0), reverse=True)

        self._accounts_meta_cache = accounts
        self._accounts_meta_cache_time = now
        return accounts

    def get_account_meta(self, uid: str) -> Optional[Dict[str, Any]]:
        """Find account metadata by uid, in_game_id, account_id, or token."""
        uid_str = str(uid).strip()
        if not uid_str:
            return None
        all_meta = self.get_all_accounts_meta()
        for a in all_meta:
            if str(a.get("uid", "")).strip() == uid_str:
                return a
            if str(a.get("in_game_id", "")).strip() == uid_str:
                return a
            if str(a.get("account_id", "")).strip() == uid_str:
                return a
            tok = str(a.get("token", "")).strip()
            if tok and (tok == uid_str or f"tok_{tok[:10]}" == uid_str or f"tok_{tok[:20]}" == uid_str):
                return a
        return None

    def unlink_account(self, uid: str):
        uid_str = str(uid).strip()
        if not uid_str:
            return
        if self.is_mongo:
            try:
                self.db.accounts_meta.delete_many({
                    "$or": [
                        {"uid": uid_str},
                        {"in_game_id": uid_str},
                        {"account_id": uid_str},
                        {"token": uid_str}
                    ]
                })
            except Exception as e:
                print(f"[-] Mongo unlink_account error: {e}")
        else:
            self.fallback_data["accounts_meta"] = [
                a for a in self.fallback_data.get("accounts_meta", [])
                if str(a.get("uid", "")).strip() != uid_str
                and str(a.get("in_game_id", "")).strip() != uid_str
                and str(a.get("account_id", "")).strip() != uid_str
                and str(a.get("token", "")).strip() != uid_str
            ]
            self._save_fallback()
        self.invalidate_accounts_meta_cache()

    def delete_user(self, user_id: str) -> Tuple[bool, str]:
        """Delete user and all their linked accounts."""
        user = self.get_user_by_id(user_id)
        if not user:
            return False, "User not found"

        all_users = self.get_all_users()
        if user.get("role") == "admin":
            admin_count = len([u for u in all_users if u.get("role") == "admin"])
            if admin_count <= 1:
                return False, "Cannot delete the sole administrator account."

        # Unlink all user accounts
        uids = self.get_user_account_uids(user_id)
        for u in uids:
            self.unlink_account(u)

        if self.is_mongo:
            try:
                self.db.users.delete_one({"id": str(user_id)})
                self.db.orders.delete_many({"user_id": str(user_id)})
                self.invalidate_users_cache()
                return True, f"User '{user.get('username')}' deleted successfully."
            except Exception as e:
                return False, f"Database error: {e}"
        else:
            self.fallback_data["users"] = [u for u in self.fallback_data.get("users", []) if str(u.get("id")) != str(user_id)]
            self.fallback_data["orders"] = [o for o in self.fallback_data.get("orders", []) if str(o.get("user_id")) != str(user_id)]
            self._save_fallback()
            self.invalidate_users_cache()
            return True, f"User '{user.get('username')}' deleted successfully."

    def reset_user_password(self, user_id: str, new_password: str) -> Tuple[bool, str]:
        """Reset password for user."""
        if len(new_password) < 4:
            return False, "Password must be at least 4 characters long."
        pwd_hash = self.hash_password(new_password)
        if self.is_mongo:
            try:
                self.db.users.update_one({"id": str(user_id)}, {"$set": {"password_hash": pwd_hash}})
                return True, "Password reset successfully."
            except Exception as e:
                return False, f"Database error: {e}"
        for u in self.fallback_data.get("users", []):
            if str(u.get("id")) == str(user_id):
                u["password_hash"] = pwd_hash
                self._save_fallback()
                return True, "Password reset successfully."
        return False, "User not found."

    def toggle_user_status(self, user_id: str, status: str) -> Tuple[bool, str]:
        """Toggle active/banned status."""
        if status not in ["active", "banned"]:
            return False, "Invalid status. Use 'active' or 'banned'."
        user = self.get_user_by_id(user_id)
        if not user:
            return False, "User not found"
        if user.get("role") == "admin" and status == "banned":
            return False, "Cannot ban an administrator account."

        if self.is_mongo:
            try:
                self.db.users.update_one({"id": str(user_id)}, {"$set": {"status": status}})
                return True, f"User '{user.get('username')}' is now {status}."
            except Exception as e:
                return False, f"Database error: {e}"
        for u in self.fallback_data.get("users", []):
            if str(u.get("id")) == str(user_id):
                u["status"] = status
                self._save_fallback()
                return True, f"User '{user.get('username')}' is now {status}."
        return False, "User not found."

    # ==================== PLANS ====================
    def get_plans(self) -> List[Dict[str, Any]]:
        plans = []
        if self.is_mongo:
            try:
                cursor = self.db.plans.find()
                for p in cursor:
                    p.pop("_id", None)
                    plans.append(p)
            except Exception:
                pass
        if not plans:
            plans = self.fallback_data.get("plans", DEFAULT_PLANS)

        return plans

    def get_plan_by_id(self, plan_id: str) -> Optional[Dict[str, Any]]:
        plans = self.get_plans()
        for p in plans:
            if p.get("id") == plan_id:
                return p
        return None

    def update_plan(self, plan_id: str, updates: Dict[str, Any]) -> Tuple[bool, str]:
        allowed = {"name", "duration_hours", "duration_text", "price_inr", "price_usd", "slots", "badge", "popular", "features"}
        clean_updates = {k: v for k, v in updates.items() if k in allowed}
        if not clean_updates:
            return False, "No valid plan fields to update"

        if "price_inr" in clean_updates:
            try: clean_updates["price_inr"] = float(clean_updates["price_inr"])
            except Exception: pass
        if "price_usd" in clean_updates:
            try: clean_updates["price_usd"] = float(clean_updates["price_usd"])
            except Exception: pass
        if "slots" in clean_updates:
            try: clean_updates["slots"] = int(clean_updates["slots"])
            except Exception: pass
        if "duration_hours" in clean_updates:
            try: clean_updates["duration_hours"] = int(clean_updates["duration_hours"])
            except Exception: pass
        if "popular" in clean_updates:
            clean_updates["popular"] = bool(clean_updates["popular"])

        if self.is_mongo:
            try:
                res = self.db.plans.update_one({"id": plan_id}, {"$set": clean_updates})
                if res.matched_count == 0:
                    # If not yet seeded in mongo, copy from fallback and update
                    plans = self.get_plans()
                    for p in plans:
                        if p.get("id") == plan_id:
                            p.update(clean_updates)
                            self.db.plans.replace_one({"id": plan_id}, p, upsert=True)
                            self.invalidate_plans_cache()
                            return True, "Plan updated successfully"
                    return False, "Plan not found"
                self.invalidate_plans_cache()
                return True, "Plan updated successfully"
            except Exception as e:
                return False, f"Database error: {e}"
        else:
            plans = self.fallback_data.setdefault("plans", list(DEFAULT_PLANS))
            for p in plans:
                if p.get("id") == plan_id:
                    p.update(clean_updates)
                    self._save_fallback()
                    self.invalidate_plans_cache()
                    return True, "Plan updated successfully"
            return False, "Plan not found"

    def get_expired_users(self) -> List[Dict[str, Any]]:
        now = time.time()
        expired = []
        all_users = self.get_all_users()
        for u in all_users:
            if u.get("role") == "admin":
                continue
            expiry = u.get("plan_expires_at")
            if expiry and isinstance(expiry, (int, float)) and now > expiry:
                expired.append(u)
        return expired

    # ==================== ORDERS & PLAN PURCHASES ====================
    def create_order(self, user_id: str, username: str, plan_id: str,
                     payment_method: str, transaction_id: str, user_telegram: str = "",
                     custom_order_id: Optional[str] = None, initial_status: str = "pending") -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        plan = self.get_plan_by_id(plan_id)
        if not plan:
            return False, "Selected plan not found", None

        txn = transaction_id.strip()
        if len(txn) < 4 or len(txn) > 64:
            return False, "Please enter a valid Transaction / UTR reference number", None

        # Anti-spam: Check if user already has 2 pending orders waiting for approval (only if creating a pending order)
        if initial_status == "pending":
            user_orders = self.get_user_orders(user_id)
            pending_count = sum(1 for o in user_orders if o.get("status") == "pending")
            if pending_count >= 2:
                return False, "You already have pending order(s) awaiting review. Please wait for admin approval before submitting new requests.", None

        # Anti-spam: Check duplicate transaction / UTR reference across all orders
        all_orders = self.get_all_orders()
        clean_txn = txn.lower().strip()
        for o in all_orders:
            if str(o.get("transaction_id", "")).strip().lower() == clean_txn:
                return False, "This Transaction / UTR reference has already been submitted.", None

        order_id = str(custom_order_id).strip() if custom_order_id else f"ORD-{int(time.time())}-{random_str(4).upper()}"

        order_doc = {
            "id": order_id,
            "user_id": str(user_id),
            "username": username,
            "plan_id": plan["id"],
            "plan_name": plan["name"],
            "duration_hours": plan["duration_hours"],
            "slots": plan["slots"],
            "price_inr": plan["price_inr"],
            "price_usd": plan["price_usd"],
            "payment_method": payment_method or "UPI",
            "transaction_id": txn,
            "user_telegram": user_telegram or "",
            "status": initial_status,  # 'pending', 'approved', 'rejected'
            "created_at": time.time(),
            "approved_at": None,
            "admin_remarks": ""
        }

        if self.is_mongo:
            try:
                self.db.orders.insert_one(order_doc)
            except Exception as e:
                return False, f"Failed to save order: {e}", None
        else:
            self.fallback_data.setdefault("orders", []).append(order_doc)
            self._save_fallback()

        clean_order = dict(order_doc)
        clean_order.pop("_id", None)
        return True, "Plan purchase request submitted successfully! Admin will verify and activate shortly.", clean_order

    def get_user_orders(self, user_id: str) -> List[Dict[str, Any]]:
        orders = []
        if self.is_mongo:
            try:
                cursor = self.db.orders.find({"user_id": str(user_id)}).sort("created_at", -1)
                for o in cursor:
                    o.pop("_id", None)
                    orders.append(o)
                return orders
            except Exception:
                pass
        orders = [dict(o) for o in self.fallback_data.get("orders", []) if str(o.get("user_id")) == str(user_id)]
        orders.sort(key=lambda x: x.get("created_at", 0), reverse=True)
        return orders

    def get_all_orders(self) -> List[Dict[str, Any]]:
        orders = []
        if self.is_mongo:
            try:
                cursor = self.db.orders.find().sort("created_at", -1)
                for o in cursor:
                    o.pop("_id", None)
                    orders.append(o)
                return orders
            except Exception:
                pass
        orders = [dict(o) for o in self.fallback_data.get("orders", [])]
        orders.sort(key=lambda x: x.get("created_at", 0), reverse=True)
        return orders

    def approve_order(self, order_id: str, admin_username: str, remarks: str = "") -> Tuple[bool, str]:
        order = self._find_order(order_id)
        if not order:
            return False, "Order not found"

        if order.get("status") == "approved":
            return False, "Order is already approved"

        user = self.get_user_by_id(order["user_id"])
        if not user:
            return False, "User not found for this order"

        # Calculate new plan expiry and slots
        duration_sec = order.get("duration_hours", 24) * 3600
        current_expiry = user.get("plan_expires_at")
        now = time.time()

        if current_expiry and current_expiry > now:
            new_expiry = current_expiry + duration_sec
        else:
            new_expiry = now + duration_sec

        plan_id = order.get("plan_id")
        plan_obj = self.get_plan_by_id(plan_id) if plan_id else None
        plan_slots = int(order.get("slots") or (plan_obj.get("slots") if plan_obj else 3))
        plan_name = order.get("plan_name") or (plan_obj.get("name") if plan_obj else "Premium Plan")

        # If user currently has an active plan that hasn't expired yet, keep higher slots or upgrade
        if current_expiry and current_expiry > now and user.get("slots", 0) > 0:
            new_slots = max(int(user.get("slots", 0)), plan_slots)
        else:
            new_slots = plan_slots

        # Update User
        self.update_user(user["id"], {
            "slots": new_slots,
            "plan": plan_name,
            "plan_expires_at": new_expiry
        })

        # Update Order
        now_ts = time.time()
        order_update = {
            "status": "approved",
            "approved_at": now_ts,
            "admin_remarks": remarks or f"Approved by {admin_username}"
        }

        if self.is_mongo:
            try:
                self.db.orders.update_one({"id": order_id}, {"$set": order_update})
            except Exception as e:
                return False, f"Database error: {e}"
        else:
            for o in self.fallback_data.get("orders", []):
                if o.get("id") == order_id:
                    o.update(order_update)
                    self._save_fallback()
                    break

        return True, f"Order {order_id} approved! User '{user.get('username')}' now has {new_slots} slots until {datetime.datetime.fromtimestamp(new_expiry).strftime('%Y-%m-%d %H:%M:%S')}."

    def reject_order(self, order_id: str, admin_username: str, remarks: str = "") -> Tuple[bool, str]:
        order = self._find_order(order_id)
        if not order:
            return False, "Order not found"

        order_update = {
            "status": "rejected",
            "approved_at": time.time(),
            "admin_remarks": remarks or f"Declined by {admin_username}"
        }

        if self.is_mongo:
            try:
                self.db.orders.update_one({"id": order_id}, {"$set": order_update})
            except Exception as e:
                return False, f"Database error: {e}"
        else:
            for o in self.fallback_data.get("orders", []):
                if o.get("id") == order_id:
                    o.update(order_update)
                    self._save_fallback()
                    break

        return True, f"Order {order_id} rejected."

    def _find_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        if self.is_mongo:
            try:
                return self.db.orders.find_one({"id": order_id})
            except Exception:
                pass
        for o in self.fallback_data.get("orders", []):
            if o.get("id") == order_id:
                return o
        return None

    # ==================== SYSTEM SETTINGS ====================
    def get_settings(self) -> Dict[str, Any]:
        now = time.time()
        if self._settings_cache is not None and (now - self._settings_cache_time) < 60.0:
            return self._settings_cache

        s = None
        if self.is_mongo:
            try:
                found = self.db.settings.find_one()
                if found:
                    found.pop("_id", None)
                    s = found
            except Exception:
                pass
        if not s:
            s = self.fallback_data.get("settings", {
                "upi_id": UPI_ID,
                "upi_name": UPI_NAME,
                "qr_code_url": QR_CODE_URL,
                "binance_id": BINANCE_ID,
                "telegram_channel": TELEGRAM_CHANNEL,
                "telegram_group": TELEGRAM_GROUP,
                "telegram_support": TELEGRAM_SUPPORT,
                "announcement": "Welcome to Level Up Bot! Instant activation after admin approval."
            })

        self._settings_cache = s
        self._settings_cache_time = now
        return s

    def update_settings(self, updates: Dict[str, Any]) -> bool:
        ok = False
        if self.is_mongo:
            try:
                self.db.settings.update_one({}, {"$set": updates}, upsert=True)
                ok = True
            except Exception as e:
                print(f"[-] Mongo settings update error: {e}")
                ok = False
        else:
            self.fallback_data.setdefault("settings", {}).update(updates)
            self._save_fallback()
            ok = True

        if ok:
            self.invalidate_settings_cache()
        return ok


def random_str(length: int = 4) -> str:
    import random
    import string
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


# Global Singleton Instance
db = DatabaseManager()
