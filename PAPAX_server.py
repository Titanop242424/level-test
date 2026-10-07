# -*- coding: utf-8 -*-
"""
FreeFire Level Up Bot - Professional Web Dashboard & Real-Time EXP Tracker
Embedded Async Web Server (aiohttp) with MongoDB Cluster Support, JWT Auth,
Multi-Tenant Account Management, Plan Billing & Dedicated Admin System.
"""

import asyncio
import json
import os
import time
from typing import Dict, List, Any, Optional
from aiohttp import web
import aiohttp

from config import (
    WEB_HOST, WEB_PORT,
    PAYMENT_API_KEY, PAYMENT_API_BASE, PAYMENT_MERCHANT_TYPE
)
from database import db, random_str

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN_CACHE_FILE = os.path.join(BASE_DIR, "token_cache.json")
DEVICES_FILE = os.path.join(BASE_DIR, "devices.json")
ACCOUNTS_FILE = os.path.join(BASE_DIR, "accounts.json")


_token_cache_memo = {}
_token_cache_memo_time = 0.0

def _get_parsed_token_cache() -> dict:
    global _token_cache_memo, _token_cache_memo_time
    now = time.time()
    if _token_cache_memo and (now - _token_cache_memo_time) < 4.0:
        return _token_cache_memo
    if os.path.exists(TOKEN_CACHE_FILE):
        try:
            with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
                c = json.load(f)
                if isinstance(c, dict):
                    _token_cache_memo = c
                    _token_cache_memo_time = now
                    return c
        except Exception:
            pass
    return {}


def resolve_account_info_for_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Look up real in-game account info (account_id, nickname, level, exp, region, likes)
    for an authentication token from memory or cached token_cache.json.
    """
    if not token:
        return None
    tok = str(token).strip()
    if tok.lower().startswith("bearer "):
        tok = tok[7:].strip()
    tok_10 = f"tok_{tok[:10]}"
    tok_20 = f"tok_{tok[:20]}"

    # 1. Check memory bot_state
    for k in [tok, tok_20, tok_10]:
        acc = bot_state.accounts.get(k)
        if acc and acc.get("account_id") and not str(acc.get("account_id")).startswith("tok_"):
            return acc
        cred = bot_state.account_credentials.get(k)
        if cred and cred.get("account_id") and not str(cred.get("account_id")).startswith("tok_"):
            return cred

    for acc in bot_state.accounts.values():
        if isinstance(acc, dict):
            if acc.get("token") == tok or acc.get("auth_token") == tok:
                return acc

    # 2. Check cached token_cache.json
    cache = _get_parsed_token_cache()
    if cache:
        for k in [tok_20, tok_10, tok]:
            if k in cache and isinstance(cache[k], dict):
                return cache[k]
        for entry in cache.values():
            if isinstance(entry, dict):
                e_tok = str(entry.get("access_token") or entry.get("auth_token") or entry.get("token") or "")
                if e_tok and (e_tok == tok or e_tok.startswith(tok[:20])):
                    return entry

    return None


class BotState:
    def __init__(self):
        self.accounts: Dict[str, Dict[str, Any]] = {}
        self.logs: List[Dict[str, Any]] = []
        self.max_logs = 200
        self.total_matches = 0
        self.total_matches_started = 0
        self.total_gained_exp = 0
        self.start_time = time.time()
        self.account_workers: Dict[str, asyncio.Task] = {}
        # Map auth_uid / auth_token-prefix -> worker key (for cancellation)
        self.worker_aliases: Dict[str, str] = {}
        self.account_aliases: Dict[str, str] = {}
        self.refresh_callbacks: Dict[str, Any] = {}
        self.account_credentials: Dict[str, Dict[str, Any]] = {}
        self.match_activity_times: Dict[str, float] = {}
        self.auth_to_game_id: Dict[str, str] = {}
        self.game_to_auth_id: Dict[str, str] = {}
        self.account_token_map: Dict[str, str] = {}

    def link_alias(self, alias: str, target: str):
        if alias and target:
            a_str = str(alias).strip()
            t_str = str(target).strip()
            if a_str and t_str:
                self.account_aliases[a_str] = t_str
                self.account_aliases[t_str] = a_str
                self.auth_to_game_id[a_str] = t_str
                self.game_to_auth_id[t_str] = a_str

    def get_all_aliases(self, ident: str) -> set:
        aliases = set()
        if not ident:
            return aliases
        i_str = str(ident).strip()
        if not i_str:
            return aliases
        aliases.add(i_str)
        # Direct aliases
        if i_str in self.account_aliases:
            aliases.add(str(self.account_aliases[i_str]))
        if i_str in self.auth_to_game_id:
            aliases.add(str(self.auth_to_game_id[i_str]))
        if i_str in self.game_to_auth_id:
            aliases.add(str(self.game_to_auth_id[i_str]))
        if i_str in self.worker_aliases:
            aliases.add(str(self.worker_aliases[i_str]))
        # Credentials lookup
        cred = self.account_credentials.get(i_str)
        if cred:
            for k in ["account_id", "auth_uid", "auth_token"]:
                v = str(cred.get(k, "")).strip()
                if v:
                    aliases.add(v)
                    if k == "auth_token":
                        aliases.add(f"tok_{v[:10]}")
                        aliases.add(f"tok_{v[:20]}")
        # Live accounts lookup
        acc = self.get_account(i_str)
        if acc:
            for k in ["uid", "account_id", "auth_uid", "display_uid"]:
                v = str(acc.get(k, "")).strip()
                if v:
                    aliases.add(v)
        # Token clean lookup
        if i_str.startswith("tok_"):
            tok_clean = i_str[4:]
            cached_info = resolve_account_info_for_token(tok_clean)
            if cached_info:
                for k in ["account_id", "auth_uid", "open_id"]:
                    v = str(cached_info.get(k, "")).strip()
                    if v:
                        aliases.add(v)
        return aliases

    def touch_match_activity(self, uid: str):
        now = time.time()
        uid_str = str(uid).strip()
        if not uid_str:
            return
        self.match_activity_times[uid_str] = now
        acc = self.get_account(uid_str)
        if acc:
            if acc.get("account_id"):
                self.match_activity_times[str(acc["account_id"]).strip()] = now
            if acc.get("auth_uid"):
                self.match_activity_times[str(acc["auth_uid"]).strip()] = now
            if acc.get("uid"):
                self.match_activity_times[str(acc["uid"]).strip()] = now

    def get_match_activity_time(self, uid: str) -> float:
        uid_str = str(uid).strip()
        if not uid_str:
            return time.time()
        if uid_str in self.match_activity_times:
            return self.match_activity_times[uid_str]
        acc = self.get_account(uid_str)
        if acc:
            for k in [str(acc.get("account_id", "")).strip(), str(acc.get("auth_uid", "")).strip(), str(acc.get("uid", "")).strip()]:
                if k and k in self.match_activity_times:
                    return self.match_activity_times[k]
        now = time.time()
        self.match_activity_times[uid_str] = now
        return now

    def reset_match_activity_time(self, uid: str):
        uid_str = str(uid).strip()
        if uid_str:
            self.match_activity_times[uid_str] = time.time()

    def get_account(self, uid: str) -> Optional[Dict[str, Any]]:
        if not uid:
            return None
        uid_str = str(uid).strip()
        if uid_str in self.accounts:
            return self.accounts[uid_str]
        alias = self.account_aliases.get(uid_str)
        if alias and alias in self.accounts:
            return self.accounts[alias]
        w_alias = self.worker_aliases.get(uid_str)
        if w_alias and w_alias in self.accounts:
            return self.accounts[w_alias]
        cred = self.account_credentials.get(uid_str)
        if cred:
            for k in [str(cred.get("account_id", "")), str(cred.get("auth_uid", "")), f"tok_{str(cred.get('auth_token', ''))[:20]}", f"tok_{str(cred.get('auth_token', ''))[:10]}", str(cred.get('auth_token', ''))]:
                if k and k in self.accounts:
                    return self.accounts[k]
        for acc_k, acc_v in self.accounts.items():
            if uid_str in [str(acc_v.get("uid", "")), str(acc_v.get("account_id", "")), str(acc_v.get("auth_uid", "")), str(acc_v.get("display_uid", ""))]:
                return acc_v
            tok_val = str(acc_v.get("token", "") or acc_v.get("auth_token", "") or "")
            if tok_val and (uid_str == f"tok_{tok_val[:10]}" or uid_str == f"tok_{tok_val[:20]}" or uid_str == tok_val):
                return acc_v

        # Token cache fallback if queried by token prefix
        if uid_str.startswith("tok_") or len(uid_str) >= 20:
            token_clean = uid_str[4:] if uid_str.startswith("tok_") else uid_str
            cached_info = resolve_account_info_for_token(token_clean)
            if cached_info and cached_info.get("account_id"):
                acc_id_str = str(cached_info["account_id"])
                if acc_id_str in self.accounts:
                    return self.accounts[acc_id_str]
        return None

    def log(self, message: str, level: str = "info", uid: Optional[str] = None, category: Optional[str] = None):
        if not category:
            msg_lower = (message or "").lower()
            if any(k in msg_lower for k in ["match", "lone wolf", "udp", "battle royale", "startmatch", "searching", "finish", "idle"]):
                category = "matches"
            elif any(k in msg_lower for k in ["exp", "level", "like", "profile", "gain"]):
                category = "exp"
            elif any(k in msg_lower for k in ["account", "login", "auth", "token", "cache", "device", "purged", "removed", "delete", "limit reached", "slot", "gateway"]):
                category = "accounts"
            else:
                category = "system"

        entry = {
            "time": time.strftime("%H:%M:%S"),
            "level": level,
            "message": message,
            "uid": uid,
            "category": category
        }
        self.logs.append(entry)
        if len(self.logs) > self.max_logs:
            self.logs.pop(0)

    def register_account(self, uid: str, nickname: str, region: str, level: int, exp: int, likes: int = 0, alt_uid: Optional[str] = None, mode: str = "AUTO"):
        uid_str = str(uid).strip()
        acc = self.get_account(uid_str)
        if not acc and alt_uid:
            acc = self.get_account(str(alt_uid).strip())

        if alt_uid:
            self.link_alias(str(alt_uid).strip(), uid_str)

        if not acc:
            acc = {
                "uid": uid_str,
                "account_id": uid_str,
                "auth_uid": str(alt_uid).strip() if alt_uid else uid_str,
                "display_uid": str(alt_uid).strip() if alt_uid else uid_str,
                "nickname": nickname or f"Player_{uid_str[:6]}",
                "region": region or "IND",
                "mode": mode or "AUTO",
                "level": level or 1,
                "initial_exp": exp or 0,
                "current_exp": exp or 0,
                "gained_exp": 0,
                "likes": likes or 0,
                "status": "ONLINE",
                "matches_played": 0,
                "matches_started": 0,
                "active_matches": 0,
                "last_match_time": None,
                "last_updated": time.strftime("%H:%M:%S")
            }
            self.accounts[uid_str] = acc
            if alt_uid:
                self.accounts[str(alt_uid).strip()] = acc
        else:
            if nickname:
                acc["nickname"] = nickname
            if region:
                acc["region"] = region
            if level and level > 0:
                acc["level"] = level
            if acc.get("initial_exp", 0) == 0 and exp > 0:
                acc["initial_exp"] = exp
            if exp and exp > 0:
                acc["current_exp"] = exp
                acc["gained_exp"] = max(0, exp - acc.get("initial_exp", exp))
            if likes:
                acc["likes"] = likes
            if mode:
                acc["mode"] = mode
            acc["status"] = "ONLINE"
            acc["last_updated"] = time.strftime("%H:%M:%S")

        self.accounts[uid_str] = acc
        if alt_uid:
            alt_str = str(alt_uid).strip()
            self.accounts[alt_str] = acc
            self.link_alias(uid_str, alt_str)

        self.recalc_totals()
        return acc

    def update_exp(self, uid: str, current_exp: int, level: Optional[int] = None):
        uid_str = str(uid).strip()
        acc = self.get_account(uid_str)
        if acc:
            old_exp = acc.get("current_exp", 0)
            if acc.get("initial_exp", 0) == 0 and current_exp > 0:
                acc["initial_exp"] = current_exp
            acc["current_exp"] = current_exp
            if level is not None and level > 0:
                acc["level"] = level
            acc["gained_exp"] = max(0, current_exp - acc.get("initial_exp", current_exp))
            acc["last_updated"] = time.strftime("%H:%M:%S")
            diff = current_exp - old_exp
            if diff > 0 and old_exp > 0:
                self.log(
                    f"Account {acc['nickname']} ({acc.get('uid', uid_str)}) gained +{diff} EXP! "
                    f"Total Gained: +{acc['gained_exp']}", "success", uid_str
                )
            self.recalc_totals()

    def update_status(self, uid: str, status: str, active_matches: Optional[int] = None):
        uid_str = str(uid).strip()
        acc = self.get_account(uid_str)
        if acc:
            acc["status"] = status
            if active_matches is not None:
                acc["active_matches"] = active_matches
            acc["last_updated"] = time.strftime("%H:%M:%S")

    def increment_match_started(self, uid: str):
        uid_str = str(uid).strip()
        self.touch_match_activity(uid_str)
        self.total_matches_started += 1
        acc = self.get_account(uid_str)
        if acc:
            acc["matches_started"] = acc.get("matches_started", 0) + 1
            acc["active_matches"] = acc.get("active_matches", 0) + 1
            acc["status"] = "IN_MATCH"
            acc["last_updated"] = time.strftime("%H:%M:%S")

    def increment_match(self, uid: str):
        uid_str = str(uid).strip()
        self.touch_match_activity(uid_str)
        self.total_matches += 1
        acc = self.get_account(uid_str)
        if acc:
            acc["matches_played"] = acc.get("matches_played", 0) + 1
            if acc.get("active_matches", 0) > 0:
                acc["active_matches"] -= 1
            acc["status"] = "IN_MATCH" if acc.get("active_matches", 0) > 0 else "ONLINE"
            acc["last_match_time"] = time.strftime("%H:%M:%S")
            acc["last_updated"] = time.strftime("%H:%M:%S")
            self.log(
                f"Account {acc['nickname']} finished Match #{acc['matches_played']}",
                "success", uid_str
            )

    def get_account_level(self, uid: str) -> int:
        acc = self.get_account(str(uid).strip())
        if acc and acc.get("level"):
            try:
                return int(acc.get("level", 1) or 1)
            except Exception:
                return 1
        return 1

    def recalc_totals(self):
        unique_accs = {id(a): a for a in self.accounts.values()}.values()
        self.total_gained_exp = sum(acc.get("gained_exp", 0) for acc in unique_accs)


bot_state = BotState()

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "index.html")


# ==================== AUTH HELPERS ====================
def get_auth_token_from_request(request: web.Request) -> Optional[str]:
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:].strip()
    # Or from cookie
    return request.cookies.get("token")


def get_current_user(request: web.Request) -> Optional[Dict[str, Any]]:
    token = get_auth_token_from_request(request)
    if not token:
        return None
    payload = db.verify_token(token)
    if not payload:
        return None
    user = db.get_user_by_id(payload.get("user_id"))
    if not user or user.get("status") == "banned":
        return None
    return user


def require_auth(handler):
    async def wrapper(request: web.Request, *args, **kwargs):
        user = get_current_user(request)
        if not user:
            return web.json_response({"status": "error", "error": "Unauthorized. Please sign in."}, status=401)
        request["user"] = user
        return await handler(request, *args, **kwargs)
    return wrapper


def require_admin(handler):
    async def wrapper(request: web.Request, *args, **kwargs):
        user = get_current_user(request)
        if not user:
            return web.json_response({"status": "error", "error": "Unauthorized. Please sign in."}, status=401)
        if user.get("role") != "admin":
            return web.json_response({"status": "error", "error": "Forbidden: Administrator privileges required."}, status=403)
        request["user"] = user
        return await handler(request, *args, **kwargs)
    return wrapper


# ==================== SECURITY & ANTI-BOT RATE LIMITER ====================
def get_client_ip(request: web.Request) -> str:
    cf = request.headers.get("CF-Connecting-IP")
    if cf:
        return cf.strip()
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    if request.remote:
        return str(request.remote).strip()
    return "127.0.0.1"


class SecurityRateLimiter:
    """In-memory thread-safe sliding window rate limiter."""
    def __init__(self):
        self._records: Dict[Tuple[str, str], List[float]] = {}
        self._lock = asyncio.Lock()
        self._last_prune = time.time()

    async def is_allowed(self, ip: str, action: str, limit: int, window: float) -> bool:
        now = time.time()
        key = (str(ip).strip(), str(action).strip())
        async with self._lock:
            if now - self._last_prune > 60.0:
                self._prune_all(now)

            timestamps = self._records.get(key, [])
            timestamps = [t for t in timestamps if (now - t) < window]
            if len(timestamps) >= limit:
                self._records[key] = timestamps
                return False

            timestamps.append(now)
            self._records[key] = timestamps
            return True

    def _prune_all(self, now: float):
        self._last_prune = now
        dead_keys = []
        for k, ts_list in self._records.items():
            valid = [t for t in ts_list if (now - t) < 3600.0]
            if valid:
                self._records[k] = valid
            else:
                dead_keys.append(k)
        for k in dead_keys:
            self._records.pop(k, None)


rate_limiter = SecurityRateLimiter()


@web.middleware
async def security_rate_limit_middleware(request: web.Request, handler):
    path = request.path
    if path.startswith("/api/"):
        ip = get_client_ip(request)
        # Global burst protection: max 120 API requests per minute per IP
        if not await rate_limiter.is_allowed(ip, "global_api", limit=80, window=60.0):
            return web.json_response({
                "status": "error",
                "error": "Rate limit exceeded. Your IP is sending requests too quickly. Please slow down."
            }, status=429)
    return await handler(request)



# ==================== PURGE CACHE UTILITIES ====================
def _purge_token_cache(account_uid: str, auth_uid: Optional[str] = None,
                        auth_token: Optional[str] = None) -> int:
    """Remove all cache entries related to this account. Returns count removed."""
    removed = 0
    if not os.path.exists(TOKEN_CACHE_FILE) or os.path.getsize(TOKEN_CACHE_FILE) == 0:
        return 0
    try:
        with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        if not isinstance(cache, dict):
            return 0

        keys_to_remove = set()
        for key, entry in list(cache.items()):
            if not isinstance(entry, dict):
                continue
            entry_acc_id = str(entry.get("account_id", ""))
            entry_auth_uid = str(entry.get("auth_uid", ""))
            entry_auth_tok = entry.get("auth_token", "")

            if key == account_uid or entry_acc_id == account_uid:
                keys_to_remove.add(key)
            if auth_uid and (key == auth_uid or entry_auth_uid == auth_uid
                             or key == f"tok_{auth_uid}"):
                keys_to_remove.add(key)
            if auth_token and (key == f"tok_{auth_token[:20]}"
                                or entry_auth_tok == auth_token):
                keys_to_remove.add(key)

        for k in keys_to_remove:
            if k in cache:
                del cache[k]
                removed += 1

        if removed:
            tmp = TOKEN_CACHE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(cache, f, indent=2)
            os.replace(tmp, TOKEN_CACHE_FILE)
    except Exception as e:
        print(f"[-] _purge_token_cache error: {e}")
    return removed


def _purge_devices(account_uid: str, auth_uid: Optional[str] = None) -> int:
    """Remove device mapping for this account. Returns count removed."""
    removed = 0
    if not os.path.exists(DEVICES_FILE) or os.path.getsize(DEVICES_FILE) == 0:
        return 0
    try:
        with open(DEVICES_FILE, "r", encoding="utf-8") as f:
            devices = json.load(f)
        if not isinstance(devices, dict):
            return 0

        keys_to_remove = set()
        for key in list(devices.keys()):
            if key == account_uid or (auth_uid and key == auth_uid):
                keys_to_remove.add(key)

        for k in keys_to_remove:
            if k in devices:
                del devices[k]
                removed += 1

        if removed:
            tmp = DEVICES_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(devices, f, indent=4)
            os.replace(tmp, DEVICES_FILE)
    except Exception as e:
        print(f"[-] _purge_devices error: {e}")
    return removed


# ==================== COMPLETE ACCOUNT PURGE & PLAN EXPIRY ====================
def purge_account_completely(account_identifier: str, reason: str = "") -> bool:
    """
    Completely and atomically removes an account from:
    1. bot_state.account_workers (stops running task)
    2. bot_state.accounts (memory dashboard state)
    3. bot_state.account_credentials
    4. accounts.json (ensuring it is removed from accounts.json)
    5. token_cache.json (ensuring all aliases are purged)
    6. devices.json (removes device mapping)
    7. Database accounts_meta (unlinks from user)
    """
    uid = str(account_identifier).strip()
    if not uid:
        return False

    cred = bot_state.account_credentials.get(uid, {})
    auth_uid = str(cred.get("auth_uid", "") or "")
    auth_token = str(cred.get("auth_token", "") or "")
    account_id = str(cred.get("account_id", uid) or uid)

    aliases = {uid, account_id}
    if auth_uid: aliases.add(auth_uid)
    if auth_token:
        aliases.add(auth_token)
        aliases.add(f"tok_{auth_token[:20]}")
        aliases.add(f"tok_{auth_token[:10]}")

    if uid in bot_state.account_aliases:
        aliases.add(bot_state.account_aliases[uid])
    if uid in bot_state.worker_aliases:
        aliases.add(bot_state.worker_aliases[uid])

    # Also search accounts.json to see if this account matches any uid or token in the file
    if os.path.exists(ACCOUNTS_FILE):
        try:
            with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
            if isinstance(existing, list):
                new_list = []
                for acc in existing:
                    acc_uid = str(acc.get("uid", ""))
                    acc_tok = str(acc.get("token", ""))
                    match = False
                    if acc_uid and (acc_uid in aliases or acc_uid == uid or acc_uid == account_id or acc_uid == auth_uid):
                        match = True
                        aliases.add(acc_uid)
                    if acc_tok and (acc_tok in aliases or acc_tok == auth_token or f"tok_{acc_tok[:20]}" in aliases):
                        match = True
                        aliases.add(acc_tok)
                        aliases.add(f"tok_{acc_tok[:20]}")
                    if not match:
                        new_list.append(acc)
                tmp = ACCOUNTS_FILE + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(new_list, f, indent=2)
                os.replace(tmp, ACCOUNTS_FILE)
        except Exception as e:
            print(f"[-] accounts.json purge error: {e}")

    # 1. Stop workers (avoid self-cancellation if current task triggers purge)
    cur_task = None
    try:
        cur_task = asyncio.current_task()
    except Exception:
        cur_task = None

    for alias in list(aliases):
        wk = bot_state.worker_aliases.get(alias, alias)
        task = bot_state.account_workers.get(wk)
        if task and task is not cur_task and not task.done():
            task.cancel()
        bot_state.account_workers.pop(wk, None)

    for wk in list(bot_state.account_workers.keys()):
        if wk in aliases:
            t = bot_state.account_workers.pop(wk, None)
            if t and t is not cur_task and not t.done():
                t.cancel()

    for alias in list(aliases):
        bot_state.worker_aliases.pop(alias, None)

    # 2. Memory state
    for alias in list(aliases):
        bot_state.accounts.pop(alias, None)
        bot_state.account_credentials.pop(alias, None)
        bot_state.account_aliases.pop(alias, None)
        bot_state.match_activity_times.pop(alias, None)
    bot_state.recalc_totals()

    # 3. Synchronize token_cache.json
    for a in list(aliases):
        _purge_token_cache(a, None, None)
    if account_id:
        _purge_token_cache(account_id, auth_uid or None, auth_token or None)

    # 4. Purge devices.json
    for a in list(aliases):
        _purge_devices(a, None)
    if account_id:
        _purge_devices(account_id, auth_uid or None)

    # 5. Database unlink
    for a in list(aliases):
        db.unlink_account(a)

    log_msg = f"Account {uid} completely purged" + (f" ({reason})" if reason else "")
    bot_state.log(log_msg, "warning", uid)
    return True


async def check_and_purge_expired_plans():
    """
    Checks all non-admin users for expired plans.
    If a user's plan is expired:
    - Finds all accounts belonging to that user
    - Automatically removes them from accounts.json, token_cache.json, devices.json, memory
    - Resets slots to 0 and logs the event
    """
    now = time.time()
    try:
        all_users = db.get_all_users()
        for u in all_users:
            if u.get("role") == "admin":
                continue
            plan_expires_at = u.get("plan_expires_at")
            if plan_expires_at and isinstance(plan_expires_at, (int, float)) and now > plan_expires_at:
                user_id = u["id"]
                username = u.get("username", "User")
                owned_meta = db.get_user_accounts_meta(user_id)
                if owned_meta:
                    bot_state.log(
                        f"User '{username}' plan expired. Auto-removing {len(owned_meta)} accounts from accounts.json & token_cache.json...",
                        "warning"
                    )
                    for acc in owned_meta:
                        acc_uid = str(acc.get("uid", ""))
                        if acc_uid:
                            purge_account_completely(acc_uid, reason=f"Plan expired for {username}")

                # Reset user slots and plan info whether they had accounts or not!
                db.update_user(user_id, {
                    "slots": 0,
                    "plan": "Expired",
                    "plan_expires_at": None
                })
    except Exception as e:
        print(f"[-] check_and_purge_expired_plans error: {e}")


async def expired_plans_watchdog():
    """Background watchdog periodically checking for expired user plans."""
    while True:
        try:
            await asyncio.sleep(20)
            await check_and_purge_expired_plans()
        except asyncio.CancelledError:
            break
        except Exception as e:
            await asyncio.sleep(20)


# ==================== HTTP HANDLERS: FRONTEND ====================
async def handle_index(request: web.Request) -> web.Response:
    if os.path.exists(TEMPLATE_PATH):
        with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
            content = f.read()
    else:
        content = "<h1>templates/index.html not found!</h1>"
    return web.Response(text=content, content_type="text/html", charset="utf-8")


# ==================== HTTP HANDLERS: AUTH ====================
async def handle_auth_register(request: web.Request) -> web.Response:
    try:
        ip = get_client_ip(request)
        # Max 5 registrations per 15 minutes per IP
        if not await rate_limiter.is_allowed(ip, "register", limit=1, window=900.0):
            return web.json_response({
                "status": "error",
                "error": "Registration rate limit reached for your IP. Please try again after 15 minutes."
            }, status=429)

        data = await request.json()
        # Honeypot verification: reject bot scrapers that auto-populate hidden inputs
        if data.get("website") or data.get("company") or data.get("bot_check"):
            return web.json_response({"status": "error", "error": "Bot check verification failed"}, status=400)

        username = str(data.get("username", "")).strip()
        password = str(data.get("password", "")).strip()
        telegram = str(data.get("telegram", "")).strip()

        success, msg, user_data = db.register_user(username, password, telegram)
        if not success:
            return web.json_response({"status": "error", "error": msg}, status=400)

        response = web.json_response({"status": "ok", "message": msg, "user": user_data})
        if user_data and "token" in user_data:
            response.set_cookie("token", user_data["token"], max_age=86400 * 15, httponly=False)
        return response
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


async def handle_auth_login(request: web.Request) -> web.Response:
    try:
        ip = get_client_ip(request)
        # Max 12 login attempts per 2 minutes per IP (brute-force defense)
        if not await rate_limiter.is_allowed(ip, "login", limit=12, window=120.0):
            return web.json_response({
                "status": "error",
                "error": "Too many failed login attempts. Please wait 2 minutes before trying again."
            }, status=429)

        data = await request.json()
        username = str(data.get("username", "")).strip()
        password = str(data.get("password", "")).strip()

        success, msg, user_data = db.authenticate_user(username, password)
        if not success:
            return web.json_response({"status": "error", "error": msg}, status=401)

        response = web.json_response({"status": "ok", "message": msg, "user": user_data})
        if user_data and "token" in user_data:
            response.set_cookie("token", user_data["token"], max_age=86400 * 15, httponly=False)
        return response
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


async def handle_auth_me(request: web.Request) -> web.Response:
    user = get_current_user(request)
    if not user:
        return web.json_response({"status": "error", "error": "Not authenticated"}, status=401)

    clean = db._clean_user(user)
    # Calculate user's active accounts and slots
    owned_uids = db.get_user_account_uids(user["id"])
    clean["accounts_count"] = len(owned_uids)
    clean["available_slots"] = max(0, clean.get("slots", 0) - len(owned_uids))
    clean["is_mongo"] = db.is_mongo
    return web.json_response({"status": "ok", "user": clean})


async def handle_auth_logout(request: web.Request) -> web.Response:
    response = web.json_response({"status": "ok", "message": "Logged out successfully"})
    response.del_cookie("token")
    return response


# ==================== HTTP HANDLERS: CORE BOT & STATS ====================
def build_aggregated_accounts(user: Optional[Dict[str, Any]], is_admin: bool) -> List[Dict[str, Any]]:
    """
    Consistently aggregate accounts with strict ownership isolation:
    - Admin: All accounts in DB metadata, accounts.json, and live running accounts.
    - Regular User: ONLY accounts belonging to this specific user!
    - Unauthenticated: Empty list.
    """
    if not user:
        return []

    if is_admin:
        db_meta = db.get_all_accounts_meta()
    else:
        db_meta = db.get_user_accounts_meta(user["id"])

    # If regular user, build comprehensive set of keys they own
    user_owned_keys = set()
    if not is_admin:
        for m in db_meta:
            for k in ["uid", "account_id", "display_uid", "auth_uid", "in_game_id", "token"]:
                val = str(m.get(k, "")).strip()
                if val and val.lower() != "none":
                    user_owned_keys.add(val)
                    user_owned_keys.update(bot_state.get_all_aliases(val))
                    if k == "token":
                        user_owned_keys.add(f"tok_{val[:10]}")
                        user_owned_keys.add(f"tok_{val[:20]}")
            m_uid = str(m.get("uid", "")).strip()
            if m_uid:
                user_owned_keys.update(bot_state.get_all_aliases(m_uid))
            in_game = str(m.get("in_game_id", "")).strip()
            if in_game:
                user_owned_keys.update(bot_state.get_all_aliases(in_game))

    json_accounts = []
    if os.path.exists(ACCOUNTS_FILE):
        try:
            with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                content = json.load(f)
                if isinstance(content, list):
                    json_accounts = content
        except Exception:
            json_accounts = []

    results: List[Dict[str, Any]] = []
    seen_keys: set = set()

    def get_cand_keys(obj: Dict[str, Any]) -> set:
        keys = set()
        for k in ["uid", "display_uid", "account_id", "auth_uid", "in_game_id", "token"]:
            v = str(obj.get(k, "")).strip()
            if v and v.lower() != "none":
                keys.add(v)
                if k == "token":
                    keys.add(f"tok_{v[:10]}")
                    keys.add(f"tok_{v[:20]}")
        return keys

    def add_card(ident: str, meta: Optional[Dict] = None, live_data: Optional[Dict] = None):
        nonlocal results, seen_keys
        cand_keys = {ident}
        if meta:
            cand_keys.update(get_cand_keys(meta))
        if live_data:
            cand_keys.update(get_cand_keys(live_data))

        # Expand all known aliases from bot_state and cache
        expanded_cands = set(cand_keys)
        for ck in list(cand_keys):
            expanded_cands.update(bot_state.get_all_aliases(ck))
        cand_keys = expanded_cands

        live = live_data or bot_state.get_account(ident)
        tok_val = str((meta.get("token") if meta else "") or "")
        token_info = None
        if tok_val or ident.startswith("tok_") or (meta and meta.get("token")):
            t_to_search = tok_val or (meta.get("token") if meta else "") or (ident[4:] if ident.startswith("tok_") else ident)
            token_info = resolve_account_info_for_token(t_to_search)
            if token_info:
                acc_id_res = str(token_info.get("account_id") or "")
                if acc_id_res:
                    cand_keys.add(acc_id_res)
                    cand_keys.update(bot_state.get_all_aliases(acc_id_res))
                    cand_keys.add(f"tok_{t_to_search[:10]}")
                    cand_keys.add(f"tok_{t_to_search[:20]}")
                    cand_keys.add(t_to_search)
                    if not live:
                        live = bot_state.get_account(acc_id_res)

        # STRICT ISOLATION: Non-admin users can ONLY see their own accounts!
        if not is_admin:
            if not cand_keys.intersection(user_owned_keys):
                return

        # Check if already added (after candidate expansion)
        if cand_keys.intersection(seen_keys):
            return

        is_worker_running = (ident in bot_state.account_workers) or (tok_val and f"tok_{tok_val[:20]}" in bot_state.account_workers) or (tok_val and f"tok_{tok_val[:10]}" in bot_state.account_workers)
        if token_info and token_info.get("account_id"):
            real_acc_id = str(token_info["account_id"])
            if real_acc_id in bot_state.account_workers:
                is_worker_running = True

        display_uid = None
        if token_info and token_info.get("account_id"):
            display_uid = str(token_info["account_id"])
        elif live and live.get("account_id") and not str(live.get("account_id")).startswith("tok_"):
            display_uid = str(live["account_id"])
        elif meta and meta.get("uid") and not str(meta.get("uid")).startswith("tok_"):
            display_uid = str(meta["uid"])
        elif live and live.get("uid") and not str(live.get("uid")).startswith("tok_"):
            display_uid = str(live["uid"])
        else:
            display_uid = (meta.get("uid") if meta else None) or (live.get("display_uid") if live else None) or (live.get("auth_uid") if live else None) or ident

        nickname = (live.get("nickname") if live else None) or (token_info.get("nickname") if token_info else None) or (meta.get("nickname") if meta else None)
        if not nickname or nickname.startswith("Account_tok_") or nickname.startswith("User_tok_") or nickname.startswith("Direct_tok_") or nickname.startswith("Token_"):
            if token_info and token_info.get("nickname"):
                nickname = token_info["nickname"]
            elif live and live.get("nickname"):
                nickname = live["nickname"]
            else:
                nickname = f"Player_{str(display_uid)[:6]}"

        level = (live.get("level") if live and live.get("level", 0) > 1 else None) or (token_info.get("level") if token_info and token_info.get("level", 0) > 1 else None) or (meta.get("level") if meta else None) or 1
        current_exp = (live.get("current_exp") if live else None) or (token_info.get("exp") if token_info else None) or (meta.get("exp") if meta else None) or 0
        initial_exp = (live.get("initial_exp") if live else None) or current_exp or (meta.get("exp") if meta else None) or 0
        region = (live.get("region") if live else None) or (token_info.get("region") if token_info else None) or (meta.get("region") if meta else None) or "IND"
        mode = (live.get("mode") if live else None) or (meta.get("mode") if meta else None) or "AUTO"
        likes = (live.get("likes") if live else None) or (token_info.get("likes") if token_info else None) or (meta.get("likes") if meta else None) or 0

        status = (live.get("status") if live else None) or ("ONLINE" if is_worker_running else "STANDBY")

        card = {
            "uid": str(display_uid),
            "display_uid": str(display_uid),
            "nickname": nickname,
            "region": region,
            "mode": mode,
            "level": level,
            "initial_exp": initial_exp,
            "current_exp": current_exp,
            "gained_exp": (live.get("gained_exp") if live else None) or max(0, current_exp - initial_exp),
            "likes": likes,
            "status": status,
            "matches_played": (live.get("matches_played") if live else None) or 0,
            "matches_started": (live.get("matches_started") if live else None) or 0,
            "active_matches": (live.get("active_matches") if live else None) or 0,
            "last_match_time": (live.get("last_match_time") if live else None),
            "last_updated": (live.get("last_updated") if live else "Connected"),
            "owner_username": (meta.get("owner_username") if meta else None) or (user.get("username") if user else "Admin"),
            "owner_telegram": meta.get("owner_telegram", "") if meta else "",
            "owner_id": (meta.get("user_id") if meta else None) or (user.get("id") if user else "admin"),
        }

        seen_keys.update(cand_keys)
        seen_keys.update(get_cand_keys(card))
        if live:
            seen_keys.update(get_cand_keys(live))
        results.append(card)

    # 1. Accounts linked in DB
    for m in db_meta:
        m_uid = str(m.get("uid", "")).strip()
        if m_uid:
            add_card(m_uid, meta=m)

    # 2. Accounts in accounts.json (Only add if admin or matches user's accounts)
    for j in json_accounts:
        j_uid = str(j.get("uid", "")).strip()
        j_tok = str(j.get("token", "")).strip()
        ident = j_uid or (f"tok_{j_tok[:10]}" if j_tok else "")
        if ident:
            if is_admin or ident in user_owned_keys or (j_tok and f"tok_{j_tok[:10]}" in user_owned_keys):
                add_card(ident, meta=j)

    # 3. Live running accounts in bot_state.accounts (Only add if admin or matches user's accounts)
    for k_acc, live in list(bot_state.accounts.items()):
        k_str = str(k_acc).strip()
        if k_str:
            if is_admin or k_str in user_owned_keys:
                add_card(k_str, live_data=live)

    results.sort(key=lambda x: x.get("gained_exp", 0), reverse=True)
    return results


def is_account_active(acc: Dict[str, Any]) -> bool:
    if not acc:
        return False
    st = str(acc.get("status", "")).upper()
    if any(s in st for s in ["STANDBY", "OFFLINE", "STOPPED", "ERROR", "DISCONNECTED", "EXPIRED"]):
        return False
    if any(k in st for k in ["SEARCHING", "IN_MATCH", "ONLINE", "WAITING", "RUNNING", "CONNECT"]):
        return True
    if int(acc.get("active_matches", 0) or 0) > 0:
        return True
    return False


async def handle_get_stats(request: web.Request) -> web.Response:
    user = get_current_user(request)
    is_admin = bool(user and user.get("role") == "admin")

    # User details & remaining plan time
    user_clean = db._clean_user(user) if user else {}

    accounts = build_aggregated_accounts(user, is_admin)
    total_accounts = len(accounts)
    active_accounts = len([a for a in accounts if is_account_active(a)])

    if user and not is_admin:
        # ==================== REGULAR USER VIEW ====================
        matches_done = sum(a.get("matches_played", 0) for a in accounts)
        matches_started = sum(a.get("matches_started", 0) for a in accounts)
        if matches_started < matches_done:
            matches_started = matches_done + sum(a.get("active_matches", 0) for a in accounts)
        total_gained = sum(a.get("gained_exp", 0) for a in accounts)

        user_slots = user_clean.get("slots", 0)
        available_slots = max(0, user_slots - total_accounts)

        # Include this user's accounts plus direct json accounts in logs
        owned_uids = {str(a.get("uid")) for a in accounts if a.get("uid")}
        for a in accounts:
            if a.get("display_uid"):
                owned_uids.add(str(a.get("display_uid")))

        user_logs = [l for l in bot_state.logs if (not l.get("uid")) or (str(l.get("uid")) in owned_uids)]

        return web.json_response({
            "total_accounts": total_accounts,
            "active_accounts": active_accounts,
            "matches_started": matches_started,
            "matches_done": matches_done,
            "total_matches": matches_done,
            "total_gained_exp": total_gained,
            "accounts": accounts,
            "logs": user_logs[-60:],
            "uptime": int(time.time() - bot_state.start_time),
            "user_slots": user_slots,
            "available_slots": available_slots,
            "is_admin": False,
            "is_mongo": db.is_mongo,
            "plan_name": user_clean.get("plan") or user.get("plan", "Free Tier"),
            "plan_expires_at": user_clean.get("plan_expires_at"),
            "plan_expires_at_formatted": user_clean.get("plan_expires_at_formatted", "None"),
            "remaining_seconds": user_clean.get("remaining_seconds", 0),
            "remaining_time_str": user_clean.get("remaining_time_str", "No Active Plan"),
            "plan_expired": user_clean.get("plan_expired", False)
        })
    else:
        # ==================== ADMIN GLOBAL VIEW ====================
        matches_done = bot_state.total_matches or sum(a.get("matches_played", 0) for a in accounts)
        matches_started = bot_state.total_matches_started or sum(a.get("matches_started", 0) for a in accounts)
        if matches_started < matches_done:
            matches_started = matches_done + sum(a.get("active_matches", 0) for a in accounts)
        total_gained = bot_state.total_gained_exp or sum(a.get("gained_exp", 0) for a in accounts)

        return web.json_response({
            "total_accounts": total_accounts,
            "active_accounts": active_accounts,
            "matches_started": matches_started,
            "matches_done": matches_done,
            "total_matches": matches_done,
            "total_gained_exp": total_gained,
            "accounts": accounts,
            "logs": bot_state.logs[-100:],
            "uptime": int(time.time() - bot_state.start_time),
            "user_slots": 999 if is_admin else 0,
            "available_slots": 999 if is_admin else 0,
            "is_admin": bool(is_admin),
            "is_mongo": db.is_mongo,
            "plan_name": "Administrator (Infinite)",
            "plan_expires_at": None,
            "plan_expires_at_formatted": "Lifetime (Admin)",
            "remaining_seconds": None,
            "remaining_time_str": "Unlimited (Admin)",
            "plan_expired": False
        })


@require_auth
async def handle_add_account(request: web.Request) -> web.Response:
    try:
        user = request["user"]
        data = await request.json()

        # Plan & Slot enforcement for non-admin users
        if user.get("role") != "admin":
            now = time.time()
            plan_expires_at = user.get("plan_expires_at")
            if not plan_expires_at or (isinstance(plan_expires_at, (int, float)) and now > plan_expires_at):
                return web.json_response({
                    "status": "error",
                    "error": "Your subscription plan has expired or is inactive. Please buy or renew a plan in the 'Buy Plan' tab to add accounts!"
                }, status=403)

            user_clean = db._clean_user(user)
            user_slots = user_clean.get("slots", 0)
            if user_slots <= 0:
                return web.json_response({
                    "status": "error",
                    "error": "You do not have any active account slots. Please purchase a Plan in the 'Buy Plan' tab to unlock slots."
                }, status=403)

            owned_uids = db.get_user_account_uids(user["id"])
            if len(owned_uids) >= user_slots:
                return web.json_response({
                    "status": "error",
                    "error": f"Slot limit reached ({len(owned_uids)}/{user_slots} used). Please purchase a Plan in the Buy tab to unlock more slots!"
                }, status=403)

        # Load existing accounts.json
        existing = []
        if os.path.exists(ACCOUNTS_FILE):
            try:
                with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            except Exception:
                existing = []

        added_uid = ""
        reg = str(data.get("region", "IND")).strip().upper() or "IND"
        data["region"] = reg

        mode = str(data.get("mode", "AUTO")).strip().upper() or "AUTO"
        if mode not in ["AUTO", "BR", "LW"]:
            mode = "AUTO"
        data["mode"] = mode

        if "uid" in data and "password" in data:
            uid = str(data["uid"]).strip()
            pwd = str(data["password"]).strip()
            if not uid or not pwd:
                return web.json_response({"status": "error", "error": "UID and Password are required"}, status=400)
            existing = [acc for acc in existing if str(acc.get("uid")) != uid]
            existing.append({"uid": uid, "password": pwd, "region": reg, "mode": mode})
            added_uid = uid
        elif "token" in data:
            token = str(data["token"]).strip().strip('"').strip("'")
            if token.lower().startswith("bearer "):
                token = token[7:].strip()
            if not token:
                return web.json_response({"status": "error", "error": "Token is required"}, status=400)
            existing = [acc for acc in existing if acc.get("token") != token]
            existing.append({"token": token, "region": reg, "mode": mode})

            # Auto-resolve real account details from token cache
            cached_info = resolve_account_info_for_token(token)
            if cached_info and cached_info.get("account_id"):
                added_uid = str(cached_info["account_id"])
                real_nickname = cached_info.get("nickname") or f"Player_{added_uid}"
                real_level = cached_info.get("level", 1)
                real_exp = cached_info.get("exp", 0)
                real_region = cached_info.get("region", reg)
            else:
                added_uid = f"tok_{token[:10]}"
                real_nickname = f"Token_{token[:8]}"
                real_level = 1
                real_exp = 0
                real_region = reg
        else:
            return web.json_response({"status": "error", "error": "Invalid payload"}, status=400)

        tmp = ACCOUNTS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
        os.replace(tmp, ACCOUNTS_FILE)

        # Link account to user in DB
        is_admin_user = (user.get("role") == "admin")
        db.link_account_to_user(user["id"], {
            "uid": added_uid,
            "token": data.get("token", ""),
            "nickname": real_nickname if 'real_nickname' in locals() else f"User_{added_uid[:6]}",
            "level": real_level if 'real_level' in locals() else 1,
            "exp": real_exp if 'real_exp' in locals() else 0,
            "region": reg,
            "mode": mode,
            "username": user.get("username", ""),
            "is_admin": is_admin_user
        })

        bot_state.log(f"New account {added_uid} ({reg} | {mode}) added by {user.get('username')}", "success")

        if "on_account_added" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_account_added"](data))

        return web.json_response({"status": "ok", "message": f"Account {added_uid} added successfully!"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


@require_auth
async def handle_delete_account(request: web.Request) -> web.Response:
    try:
        user = request["user"]
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        if not uid:
            return web.json_response({"status": "error", "error": "UID is required"}, status=400)

        # Check ownership unless admin
        if user.get("role") != "admin":
            owned_meta = db.get_user_accounts_meta(user["id"])
            user_owned = set()
            for m in owned_meta:
                for k in ["uid", "account_id", "display_uid", "auth_uid", "in_game_id", "token"]:
                    v = str(m.get(k, "")).strip()
                    if v and v.lower() != "none":
                        user_owned.add(v)
                        user_owned.update(bot_state.get_all_aliases(v))
                        if k == "token":
                            user_owned.add(f"tok_{v[:10]}")
                            user_owned.add(f"tok_{v[:20]}")
                m_uid = str(m.get("uid", "")).strip()
                if m_uid:
                    user_owned.update(bot_state.get_all_aliases(m_uid))
                in_game = str(m.get("in_game_id", "")).strip()
                if in_game:
                    user_owned.update(bot_state.get_all_aliases(in_game))

            # Check all aliases of target uid
            all_target_aliases = {uid}
            all_target_aliases.update(bot_state.get_all_aliases(uid))
            for m in owned_meta:
                m_u = str(m.get("uid", "")).strip()
                m_in = str(m.get("in_game_id", "")).strip()
                m_tok = str(m.get("token", "")).strip()
                if uid in [m_u, m_in, m_tok, f"tok_{m_tok[:10]}", f"tok_{m_tok[:20]}"]:
                    if m_u: all_target_aliases.add(m_u)
                    if m_in: all_target_aliases.add(m_in)

            if not all_target_aliases.intersection(user_owned):
                return web.json_response({
                    "status": "error",
                    "error": "You do not have permission to delete this account. Only the account owner or admin can remove it."
                }, status=403)

        success = purge_account_completely(uid, reason=f"Deleted by {user.get('username')}")
        if not success:
            return web.json_response({"status": "error", "error": "Failed to delete account"}, status=500)

        # Notify callbacks if present
        if "on_account_deleted" in bot_state.refresh_callbacks:
            try:
                cb = bot_state.refresh_callbacks["on_account_deleted"]
                if asyncio.iscoroutinefunction(cb):
                    asyncio.create_task(cb([uid]))
                else:
                    cb([uid])
            except Exception:
                pass

        return web.json_response({
            "status": "ok",
            "message": f"Account {uid} cleanly terminated and removed from accounts.json & cache."
        })
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


@require_auth
async def handle_refresh_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        if "on_refresh_account" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_refresh_account"](uid))
        return web.json_response({"status": "ok", "message": f"Refresh triggered for {uid}"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


# ==================== HTTP HANDLERS: PLANS & ORDERS ====================
async def handle_get_plans(request: web.Request) -> web.Response:
    plans = db.get_plans()
    return web.json_response(
        {"status": "ok", "plans": plans},
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate, max-age=0",
            "Pragma": "no-cache"
        }
    )


@require_auth
async def handle_create_order(request: web.Request) -> web.Response:
    try:
        user = request["user"]
        ip = get_client_ip(request)

        # Anti-spam: Max 3 orders per 10 minutes per IP/user
        throttle_key = f"{ip}_{user['id']}"
        if not await rate_limiter.is_allowed(throttle_key, "order", limit=3, window=600.0):
            return web.json_response({
                "status": "error",
                "error": "Plan purchase request rate limit exceeded. Please wait a few minutes before submitting another request."
            }, status=429)

        data = await request.json()
        # Honeypot verification
        if data.get("website") or data.get("company") or data.get("bot_check"):
            return web.json_response({"status": "error", "error": "Bot verification failed"}, status=400)

        plan_id = str(data.get("plan_id", "")).strip()
        txn_id = str(data.get("transaction_id", "")).strip()
        payment_method = str(data.get("payment_method", "UPI")).strip()
        telegram = str(data.get("user_telegram", user.get("telegram", ""))).strip()

        success, msg, order = db.create_order(
            user_id=user["id"],
            username=user["username"],
            plan_id=plan_id,
            payment_method=payment_method,
            transaction_id=txn_id,
            user_telegram=telegram
        )

        if not success:
            return web.json_response({"status": "error", "error": msg}, status=400)

        bot_state.log(f"New Plan Order [{order['id']}] submitted by {user['username']} for {order['plan_name']}", "info")
        return web.json_response({"status": "ok", "message": msg, "order": order})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


@require_auth
async def handle_get_my_orders(request: web.Request) -> web.Response:
    user = request["user"]
    orders = db.get_user_orders(user["id"])
    return web.json_response({"status": "ok", "orders": orders})


# ==================== AUTOMATED PAYMENT GATEWAY (FAMPAY) ====================
@require_auth
async def handle_payment_generate_qr(request: web.Request) -> web.Response:
    """
    Calls FamPay order generation API to register an order and return styled QR.
    The secret PAYMENT_API_KEY is kept confidential on the backend.
    """
    try:
        user = request["user"]
        ip = get_client_ip(request)

        # Anti-spam rate limiting: max 8 QR gens per 10 minutes
        if not await rate_limiter.is_allowed(f"{ip}_{user['id']}", "pay_qr", limit=8, window=600.0):
            return web.json_response({
                "status": "error",
                "error": "QR generation rate limit reached. Please wait a few minutes before trying again."
            }, status=429)

        data = await request.json()
        plan_id = str(data.get("plan_id", "")).strip()
        plan = db.get_plan_by_id(plan_id)
        if not plan:
            return web.json_response({"status": "error", "error": "Selected plan not found"}, status=400)

        amount = int(plan.get("price_inr", 10))
        # Unique clean order ID for FamPay: e.g. ORD1728234859ABC
        order_id = f"ORD{int(time.time())}{random_str(3).upper()}"

        gen_url = f"{PAYMENT_API_BASE.rstrip('/')}/gen"
        params = {
            "key": PAYMENT_API_KEY,
            "amount": str(amount),
            "order_id": order_id,
            "type": PAYMENT_MERCHANT_TYPE
        }

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
            async with session.get(gen_url, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    return web.json_response({
                        "status": "error",
                        "error": f"Payment gateway service unavailable: {text[:100]}"
                    }, status=502)
                res_data = await resp.json()

        if not res_data.get("success"):
            return web.json_response({
                "status": "error",
                "error": res_data.get("message", "Failed to generate payment QR code")
            }, status=400)

        # Proxied image URL streams normal clean QR (without styled_url)
        qr_stream_url = f"/api/payment/qr-image?order_id={order_id}&amount={amount}"

        return web.json_response({
            "status": "ok",
            "order_id": order_id,
            "amount": amount,
            "plan_name": plan.get("name"),
            "qr_url": qr_stream_url,
            "gateway_url": res_data.get("url", "")
        })
    except Exception as e:
        return web.json_response({"status": "error", "error": f"QR generation error: {str(e)}"}, status=500)


async def handle_payment_qr_image(request: web.Request) -> web.Response:
    """Streams the normal clean QR code directly (without styled layout)."""
    try:
        order_id = request.query.get("order_id", "").strip()
        amount = request.query.get("amount", "10").strip()
        if not order_id:
            return web.Response(status=400, text="Missing order_id")

        qr_url = f"{PAYMENT_API_BASE.rstrip('/')}/qr"
        params = {
            "key": PAYMENT_API_KEY,
            "amount": amount,
            "order_id": order_id,
            "type": PAYMENT_MERCHANT_TYPE
        }
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.get(qr_url, params=params) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    return web.Response(
                        body=content,
                        content_type="image/png",
                        headers={"Cache-Control": "public, max-age=600"}
                    )
                return web.Response(status=resp.status, text="Failed to stream QR image")
    except Exception as e:
        return web.Response(status=500, text=str(e))


@require_auth
async def handle_payment_verify(request: web.Request) -> web.Response:
    """
    Verifies UTR via FamPay verify API. If verified: instantly activates user plan!
    """
    try:
        user = request["user"]
        ip = get_client_ip(request)

        # Rate limit verification attempts: max 6 attempts per 5 minutes
        if not await rate_limiter.is_allowed(f"{ip}_{user['id']}", "pay_verify", limit=6, window=300.0):
            return web.json_response({
                "status": "error",
                "error": "Too many verification attempts. Please wait 5 minutes before trying again."
            }, status=429)

        data = await request.json()
        order_id = str(data.get("order_id", "")).strip()
        plan_id = str(data.get("plan_id", "")).strip()
        utr = str(data.get("utr", "")).strip()
        tg_handle = str(data.get("user_telegram", user.get("telegram", ""))).strip()

        if not utr or len(utr) < 6:
            return web.json_response({
                "status": "error",
                "error": "Please enter a valid 12-digit UPI UTR number from your payment receipt."
            }, status=400)

        if not order_id:
            return web.json_response({"status": "error", "error": "Missing order ID for verification"}, status=400)

        plan = db.get_plan_by_id(plan_id)
        if not plan:
            return web.json_response({"status": "error", "error": "Selected plan not found"}, status=400)

        # Check if this UTR was already redeemed on our platform
        all_orders = db.get_all_orders()
        clean_utr = utr.lower().strip()
        for o in all_orders:
            if str(o.get("transaction_id", "")).strip().lower() == clean_utr:
                return web.json_response({
                    "status": "error",
                    "error": "This UTR / Transaction ID has already been redeemed."
                }, status=400)

        # Call auto verify API: /verify
        verify_url = f"{PAYMENT_API_BASE.rstrip('/')}/verify"
        params = {
            "key": PAYMENT_API_KEY,
            "utr": utr,
            "order_id": order_id,
            "type": PAYMENT_MERCHANT_TYPE
        }

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.get(verify_url, params=params) as resp:
                try:
                    verify_res = await resp.json()
                except Exception:
                    text = await resp.text()
                    return web.json_response({
                        "status": "error",
                        "error": f"Verification server response invalid: {text[:100]}"
                    }, status=502)

        # Check verification state
        is_verified = bool(verify_res.get("verified") is True or (verify_res.get("status") == "success" and verify_res.get("verified")))
        if not is_verified:
            err_msg = verify_res.get("message") or "UTR verification failed. Please ensure payment is completed and enter the exact 12-digit UTR."
            return web.json_response({"status": "error", "error": err_msg}, status=400)

        # SUCCESS: Create order and immediately approve it!
        success, msg, order = db.create_order(
            user_id=user["id"],
            username=user["username"],
            plan_id=plan_id,
            payment_method="UPI (Auto-Verified)",
            transaction_id=utr,
            user_telegram=tg_handle,
            custom_order_id=order_id,
            initial_status="pending"
        )
        if not success or not order:
            return web.json_response({"status": "error", "error": msg}, status=400)

        # Approve and allocate plan/slots instantly
        appr_ok, appr_msg = db.approve_order(order["id"], admin_username="AUTO_GATEWAY", remarks=f"Auto verified via FamPay (UTR: {utr})")
        if not appr_ok:
            return web.json_response({"status": "error", "error": f"Order activation error: {appr_msg}"}, status=500)

        bot_state.log(f"⚡ AUTO-PAYMENT VERIFIED: User '{user['username']}' unlocked '{plan['name']}' via UTR {utr}!", "success")

        # Fetch fresh user details
        fresh_user = db.get_user_by_id(user["id"])
        clean_user = db._clean_user(fresh_user) if fresh_user else None

        return web.json_response({
            "status": "ok",
            "verified": True,
            "message": f"Payment verified! Your '{plan['name']}' plan has been activated instantly.",
            "order": order,
            "user": clean_user
        })
    except Exception as e:
        return web.json_response({"status": "error", "error": f"Verification error: {str(e)}"}, status=500)


# ==================== HTTP HANDLERS: PUBLIC SETTINGS ====================
async def handle_qrcode_image(request: web.Request) -> web.Response:
    qr_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "qrcode.jpg")
    if os.path.exists(qr_path):
        with open(qr_path, "rb") as f:
            content = f.read()
        return web.Response(body=content, content_type="image/jpeg")
    return web.Response(status=404, text="QR Code image not found")


async def handle_get_public_settings(request: web.Request) -> web.Response:
    settings = db.get_settings()
    return web.json_response({
        "status": "ok",
        "settings": {
            "upi_id": settings.get("upi_id", "silentkiller@upi"),
            "upi_name": settings.get("upi_name", "LEVEL UP SERVICE"),
            "qr_code_url": settings.get("qr_code_url") or "/static/qrcode.jpg",
            "binance_id": settings.get("binance_id", "1231529920"),
            "telegram_channel": settings.get("telegram_channel", "https://t.me/SILENT_API"),
            "telegram_group": settings.get("telegram_group", "https://t.me/SILENTLIKEGROUP"),
            "telegram_support": settings.get("telegram_support", "https://t.me/SIL3NT_KILLER"),
            "announcement": settings.get("announcement", "Instant activation after admin approval."),
            "is_mongo": db.is_mongo
        }
    })


# ==================== HTTP HANDLERS: ADMIN PANEL ====================
@require_admin
async def handle_admin_overview(request: web.Request) -> web.Response:
    users = db.get_all_users()
    orders = db.get_all_orders()
    pending_orders = [o for o in orders if o.get("status") == "pending"]
    approved_orders = [o for o in orders if o.get("status") == "approved"]
    total_rev_inr = sum(o.get("price_inr", 0) for o in approved_orders)
    total_rev_usd = sum(o.get("price_usd", 0) for o in approved_orders)

    all_accounts = build_aggregated_accounts(None, is_admin=True)
    total_active_accounts = len([a for a in all_accounts if is_account_active(a)])

    return web.json_response({
        "status": "ok",
        "overview": {
            "total_users": len(users),
            "total_orders": len(orders),
            "pending_orders_count": len(pending_orders),
            "approved_orders_count": len(approved_orders),
            "total_revenue_inr": total_rev_inr,
            "total_revenue_usd": total_rev_usd,
            "total_active_accounts": total_active_accounts,
            "total_matches": bot_state.total_matches,
            "is_mongo": db.is_mongo
        }
    })


@require_admin
async def handle_admin_get_orders(request: web.Request) -> web.Response:
    orders = db.get_all_orders()
    return web.json_response({"status": "ok", "orders": orders})


@require_admin
async def handle_admin_order_action(request: web.Request) -> web.Response:
    try:
        user = request["user"]
        data = await request.json()
        order_id = str(data.get("order_id", "")).strip()
        action = str(data.get("action", "")).strip().lower()
        remarks = str(data.get("remarks", "")).strip()

        if action == "approve":
            success, msg = db.approve_order(order_id, user.get("username", "admin"), remarks)
            if success:
                bot_state.log(f"Order {order_id} APPROVED by admin {user.get('username')}", "success")
        elif action == "reject":
            success, msg = db.reject_order(order_id, user.get("username", "admin"), remarks)
            if success:
                bot_state.log(f"Order {order_id} REJECTED by admin {user.get('username')}", "warning")
        else:
            return web.json_response({"status": "error", "error": "Invalid action. Use 'approve' or 'reject'"}, status=400)

        if not success:
            return web.json_response({"status": "error", "error": msg}, status=400)

        return web.json_response({"status": "ok", "message": msg})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


@require_admin
async def handle_admin_get_users(request: web.Request) -> web.Response:
    users = db.get_all_users()
    for u in users:
        u_clean = db._clean_user(u)
        u_accounts = db.get_user_accounts_meta(u["id"])
        is_admin_user = (u.get("role") == "admin")
        clean_slots = 999 if is_admin_user else u_clean.get("slots", 0)
        u["slots"] = clean_slots
        u["plan"] = u_clean.get("plan", u.get("plan", "Free Tier"))
        u["plan_expired"] = u_clean.get("plan_expired", False)
        u["remaining_time_str"] = u_clean.get("remaining_time_str", "None")
        u["accounts_count"] = len(u_accounts)
        u["slots_used"] = len(u_accounts)
        u["slots_available"] = 999 if is_admin_user else max(0, clean_slots - len(u_accounts))
        u["account_uids"] = [str(a.get("uid")) for a in u_accounts]
        u["total_gained_exp"] = sum((bot_state.get_account(str(a.get("uid"))) or {}).get("gained_exp", 0) for a in u_accounts)
        u["total_matches"] = sum((bot_state.get_account(str(a.get("uid"))) or {}).get("matches_played", 0) for a in u_accounts)
    return web.json_response({"status": "ok", "users": users})


@require_admin
async def handle_admin_get_accounts(request: web.Request) -> web.Response:
    all_meta = db.get_all_accounts_meta()

    # Read accounts.json to know what is directly saved in file
    json_accounts = []
    if os.path.exists(ACCOUNTS_FILE):
        try:
            with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                content = json.load(f)
                if isinstance(content, list):
                    json_accounts = content
        except Exception:
            json_accounts = []

    # Read token_cache.json keys
    cached_keys = set()
    if os.path.exists(TOKEN_CACHE_FILE):
        try:
            with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
                c = json.load(f)
                if isinstance(c, dict):
                    cached_keys = set(str(k) for k in c.keys())
        except Exception:
            pass

    json_uids = {str(a.get("uid")) for a in json_accounts if a.get("uid")}
    json_tokens = {str(a.get("token")) for a in json_accounts if a.get("token")}

    accounts = []
    seen_identifiers = set()

    for m in all_meta:
        uid_str = str(m.get("uid"))
        tok = str(m.get("token", ""))
        cached_info = resolve_account_info_for_token(tok) if tok else None

        display_uid = uid_str
        if (display_uid.startswith("tok_") or not display_uid.isdigit()) and cached_info and cached_info.get("account_id"):
            display_uid = str(cached_info["account_id"])

        seen_identifiers.add(uid_str)
        seen_identifiers.add(display_uid)
        if tok:
            seen_identifiers.add(tok)
            seen_identifiers.add(f"tok_{tok[:10]}")
            seen_identifiers.add(f"tok_{tok[:20]}")

        live = bot_state.get_account(display_uid) or bot_state.get_account(uid_str) or {}
        is_worker_running = (display_uid in bot_state.account_workers) or (uid_str in bot_state.account_workers) or (tok and f"tok_{tok[:20]}" in bot_state.account_workers) or (tok and f"tok_{tok[:10]}" in bot_state.account_workers)
        status = live.get("status") or ("ONLINE" if is_worker_running else "STANDBY")

        in_json = (uid_str in json_uids) or (display_uid in json_uids) or (tok and tok in json_tokens)
        in_cache = (uid_str in cached_keys) or (display_uid in cached_keys) or (tok and (f"tok_{tok[:20]}" in cached_keys or f"tok_{tok[:10]}" in cached_keys))

        owner = m.get("owner_username", "Unknown")
        is_admin_acc = (owner.lower() == "admin") or bool(m.get("is_admin"))
        source = "admin" if is_admin_acc else "user"

        nickname = live.get("nickname") or (cached_info.get("nickname") if cached_info else None) or m.get("nickname") or f"Account_{display_uid[:6]}"
        if nickname.startswith("Account_tok_") or nickname.startswith("User_tok_"):
            if cached_info and cached_info.get("nickname"):
                nickname = cached_info["nickname"]

        level = live.get("level") or (cached_info.get("level") if cached_info else None) or m.get("level", 1)
        current_exp = live.get("current_exp") or (cached_info.get("exp") if cached_info else None) or m.get("exp", 0)
        region = live.get("region") or (cached_info.get("region") if cached_info else None) or m.get("region", "IND")

        acc = {
            "uid": display_uid,
            "display_uid": display_uid,
            "nickname": nickname,
            "region": region,
            "level": level,
            "current_exp": current_exp,
            "gained_exp": live.get("gained_exp", 0),
            "matches_played": live.get("matches_played", 0),
            "matches_started": live.get("matches_started", 0),
            "active_matches": live.get("active_matches", 0),
            "status": status,
            "owner_username": owner,
            "owner_telegram": m.get("owner_telegram", ""),
            "owner_id": m.get("user_id", ""),
            "created_at": m.get("created_at", 0),
            "source": source,
            "in_accounts_json": in_json,
            "in_token_cache": in_cache
        }
        accounts.append(acc)

    # Check for accounts present in accounts.json but NOT in DB metadata!
    for j_acc in json_accounts:
        j_uid = str(j_acc.get("uid", "")).strip()
        j_tok = str(j_acc.get("token", "")).strip()
        cached_info = resolve_account_info_for_token(j_tok) if j_tok else None

        if cached_info and cached_info.get("account_id"):
            ident = str(cached_info["account_id"])
        else:
            ident = j_uid or f"tok_{j_tok[:10]}"

        if j_uid and j_uid in seen_identifiers:
            continue
        if ident in seen_identifiers:
            continue
        if j_tok and (j_tok in seen_identifiers or f"tok_{j_tok[:10]}" in seen_identifiers or f"tok_{j_tok[:20]}" in seen_identifiers):
            continue

        seen_identifiers.add(ident)
        live = bot_state.get_account(ident) or {}
        is_worker_running = (ident in bot_state.account_workers) or (j_tok and f"tok_{j_tok[:20]}" in bot_state.account_workers) or (j_tok and f"tok_{j_tok[:10]}" in bot_state.account_workers)
        status = live.get("status") or ("ONLINE" if is_worker_running else "STANDBY")
        in_cache = (j_uid and j_uid in cached_keys) or (j_tok and (f"tok_{j_tok[:20]}" in cached_keys or f"tok_{j_tok[:10]}" in cached_keys))

        nickname = live.get("nickname") or (cached_info.get("nickname") if cached_info else None) or f"Player_{ident[:6]}"
        level = live.get("level") or (cached_info.get("level") if cached_info else None) or 1
        current_exp = live.get("current_exp") or (cached_info.get("exp") if cached_info else None) or 0
        region = live.get("region") or (cached_info.get("region") if cached_info else None) or "IND"

        acc = {
            "uid": ident,
            "display_uid": ident,
            "nickname": nickname,
            "region": region,
            "level": level,
            "current_exp": current_exp,
            "gained_exp": live.get("gained_exp", 0),
            "matches_played": live.get("matches_played", 0),
            "matches_started": live.get("matches_started", 0),
            "active_matches": live.get("active_matches", 0),
            "status": status,
            "owner_username": "Admin / Direct accounts.json",
            "owner_telegram": "",
            "owner_id": "admin",
            "created_at": 0,
            "source": "admin",
            "in_accounts_json": True,
            "in_token_cache": in_cache
        }
        accounts.append(acc)

    accounts.sort(key=lambda x: x.get("gained_exp", 0), reverse=True)

    admin_count = len([a for a in accounts if a.get("source") == "admin"])
    user_count = len([a for a in accounts if a.get("source") == "user"])
    json_count = len([a for a in accounts if a.get("in_accounts_json")])

    return web.json_response({
        "status": "ok",
        "accounts": accounts,
        "admin_count": admin_count,
        "user_count": user_count,
        "json_count": json_count,
        "total_count": len(accounts)
    })


@require_admin
async def handle_admin_update_plans(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        if "plan_id" in data:
            plan_id = str(data["plan_id"]).strip()
            success, msg = db.update_plan(plan_id, data)
            if not success:
                return web.json_response({"status": "error", "error": msg}, status=400)
            bot_state.log(f"Admin updated pricing for plan '{plan_id}'", "success")
            return web.json_response({"status": "ok", "message": msg, "plans": db.get_plans()})
        elif "plans" in data and isinstance(data["plans"], list):
            for p in data["plans"]:
                pid = p.get("id")
                if pid:
                    db.update_plan(pid, p)
            bot_state.log("Admin updated all plan pricing and slots", "success")
            return web.json_response({"status": "ok", "message": "All plans updated successfully!", "plans": db.get_plans()})
        else:
            return web.json_response({"status": "error", "error": "Invalid payload. Provide 'plan_id' or 'plans' array."}, status=400)
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


@require_admin
async def handle_admin_delete_user(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        user_id = str(data.get("user_id", "")).strip()
        if not user_id:
            return web.json_response({"status": "error", "error": "user_id is required"}, status=400)
        success, msg = db.delete_user(user_id)
        if not success:
            return web.json_response({"status": "error", "error": msg}, status=400)
        bot_state.log(f"Admin deleted user {user_id}: {msg}", "warning")
        return web.json_response({"status": "ok", "message": msg})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


@require_admin
async def handle_admin_reset_password(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        user_id = str(data.get("user_id", "")).strip()
        new_pass = str(data.get("new_password", "")).strip()
        if not user_id or not new_pass:
            return web.json_response({"status": "error", "error": "user_id and new_password are required"}, status=400)
        success, msg = db.reset_user_password(user_id, new_pass)
        if not success:
            return web.json_response({"status": "error", "error": msg}, status=400)
        return web.json_response({"status": "ok", "message": msg})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


@require_admin
async def handle_admin_toggle_user_status(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        user_id = str(data.get("user_id", "")).strip()
        status = str(data.get("status", "")).strip().lower()
        success, msg = db.toggle_user_status(user_id, status)
        if not success:
            return web.json_response({"status": "error", "error": msg}, status=400)
        return web.json_response({"status": "ok", "message": msg})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


@require_admin
async def handle_admin_update_user(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        user_id = str(data.get("user_id", "")).strip()
        if not user_id:
            return web.json_response({"status": "error", "error": "user_id is required"}, status=400)

        updates = {}
        PLAN_HOURS_INFO = {
            6: (3, "6 Hours Sprint"),
            24: (5, "24 Hours Turbo"),
            72: (8, "3 Days Pro"),
            168: (12, "7 Days Champion"),
            720: (25, "30 Days Dominator"),
        }
        if "add_hours" in data:
            add_h = int(data["add_hours"])
            add_sec = add_h * 3600
            if add_sec > 0:
                target_user = db.get_user_by_id(user_id)
                current_expiry = target_user.get("plan_expires_at") if target_user else None
                now = time.time()
                base = current_expiry if (current_expiry and current_expiry > now) else now
                updates["plan_expires_at"] = base + add_sec
                if add_h in PLAN_HOURS_INFO:
                    def_slots, def_plan = PLAN_HOURS_INFO[add_h]
                    if "slots" not in data or int(data.get("slots", 0)) <= 0:
                        updates["slots"] = def_slots
                    if "plan" not in updates and target_user and target_user.get("plan") in ["Expired", "Free Tier", "Free Tier (No active plan)", None, ""]:
                        updates["plan"] = def_plan
                elif "plan" not in updates and target_user and target_user.get("plan") in ["Expired", "Free Tier", "Free Tier (No active plan)", None, ""]:
                    updates["plan"] = "Active VIP Plan"

        if "role" in data and data["role"] in ["user", "admin"]:
            updates["role"] = data["role"]
        if "status" in data and data["status"] in ["active", "banned"]:
            updates["status"] = data["status"]
        if "plan" in data and str(data["plan"]).strip():
            updates["plan"] = str(data["plan"]).strip()
        if "slots" in data:
            updates["slots"] = max(0, int(data["slots"]))

        success = db.update_user(user_id, updates)
        if not success:
            return web.json_response({"status": "error", "error": "Failed to update user"}, status=500)

        return web.json_response({"status": "ok", "message": "User updated successfully"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


@require_admin
async def handle_admin_get_settings(request: web.Request) -> web.Response:
    settings = db.get_settings()
    settings["is_mongo"] = db.is_mongo
    return web.json_response({"status": "ok", "settings": settings})


@require_admin
async def handle_admin_update_settings(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        updates = {}
        for key in ["upi_id", "upi_name", "qr_code_url", "binance_id", "telegram_channel", "telegram_group", "telegram_support", "announcement"]:
            if key in data:
                updates[key] = str(data[key]).strip()

        db.update_settings(updates)
        return web.json_response({"status": "ok", "message": "System settings saved successfully!"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


@require_admin
async def handle_admin_test_mongo(request: web.Request) -> web.Response:
    """Allows testing and saving a MongoDB Cluster URI directly from Admin Panel."""
    try:
        from pymongo import MongoClient
        data = await request.json()
        uri = str(data.get("mongo_uri", "")).strip()
        if not uri:
            return web.json_response({"status": "error", "error": "MongoDB URI cannot be empty"})

        client = MongoClient(uri, serverSelectionTimeoutMS=4000, connectTimeoutMS=4000)
        client.admin.command('ping')

        # If success, update db instance and config.json
        from config import CONFIG_FILE, DB_NAME
        config_data = {}
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                config_data = json.load(f)
        config_data["MONGO_URI"] = uri
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=2)

        # Re-initialize db
        db.client = client
        db.db = client[DB_NAME]
        db.is_mongo = True
        db._seed_mongo()

        return web.json_response({"status": "ok", "message": "Successfully connected to MongoDB Cluster! Mongo is now active."})
    except Exception as e:
        return web.json_response({"status": "error", "error": f"Connection failed: {str(e)}"})


# ==================== SERVER BOOTSTRAP ====================
async def start_web_dashboard(host: str = WEB_HOST, port: int = WEB_PORT):
    app = web.Application(middlewares=[security_rate_limit_middleware])

    # Public Routes
    app.router.add_get("/", handle_index)
    app.router.add_get("/login", handle_index)
    app.router.add_get("/admin", handle_index)
    app.router.add_get("/plans", handle_index)
    app.router.add_get("/history", handle_index)
    app.router.add_get("/contact", handle_index)

    # Auth APIs
    app.router.add_post("/api/auth/register", handle_auth_register)
    app.router.add_post("/api/auth/login", handle_auth_login)
    app.router.add_get("/api/auth/me", handle_auth_me)
    app.router.add_post("/api/auth/logout", handle_auth_logout)

    # Bot & Account APIs
    app.router.add_get("/api/stats", handle_get_stats)
    app.router.add_post("/api/account/add", handle_add_account)
    app.router.add_post("/api/account/delete", handle_delete_account)
    app.router.add_post("/api/account/refresh", handle_refresh_account)

    # Plans & Orders APIs
    app.router.add_get("/api/plans", handle_get_plans)
    app.router.add_post("/api/plans/order", handle_create_order)
    app.router.add_get("/api/plans/my-orders", handle_get_my_orders)
    app.router.add_get("/api/settings/public", handle_get_public_settings)

    # Automated Payment Gateway APIs (FamPay)
    app.router.add_post("/api/payment/generate-qr", handle_payment_generate_qr)
    app.router.add_get("/api/payment/qr-image", handle_payment_qr_image)
    app.router.add_post("/api/payment/verify", handle_payment_verify)

    # Admin APIs
    app.router.add_get("/api/admin/overview", handle_admin_overview)
    app.router.add_get("/api/admin/orders", handle_admin_get_orders)
    app.router.add_post("/api/admin/orders/action", handle_admin_order_action)
    app.router.add_get("/api/admin/users", handle_admin_get_users)
    app.router.add_post("/api/admin/users/update", handle_admin_update_user)
    app.router.add_post("/api/admin/users/delete", handle_admin_delete_user)
    app.router.add_post("/api/admin/users/reset-password", handle_admin_reset_password)
    app.router.add_post("/api/admin/users/toggle-status", handle_admin_toggle_user_status)
    app.router.add_get("/api/admin/accounts", handle_admin_get_accounts)
    app.router.add_post("/api/admin/plans/update", handle_admin_update_plans)
    app.router.add_get("/api/admin/settings", handle_admin_get_settings)
    app.router.add_post("/api/admin/settings", handle_admin_update_settings)
    app.router.add_post("/api/admin/test-mongo", handle_admin_test_mongo)

    # Static & Asset Routes
    app.router.add_get("/static/qrcode.jpg", handle_qrcode_image)
    app.router.add_get("/api/qrcode", handle_qrcode_image)
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
    if os.path.exists(static_dir):
        app.router.add_static("/static/", static_dir)

    # Start Background Watchdogs
    asyncio.create_task(expired_plans_watchdog())

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"\033[92m[+] Web Dashboard running on http://localhost:{port}\033[0m")
    if db.is_mongo:
        print("\033[92m[+] Database Backend: MongoDB Cluster ACTIVE\033[0m")
    else:
        print("\033[93m[!] Database Backend: Local Storage (Set MONGO_URI in config.json or Admin Panel to activate cluster)\033[0m")