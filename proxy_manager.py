# -*- coding: utf-8 -*-
"""
Indian & Regional Proxy Manager for Free Fire OB55 Bot
Automatically fetches, validates, and rotates Indian (and BD) HTTP proxies
to bypass BR_FFI_BLOCKED_NOT_IND_REGION_LOGIN when deploying the bot on non-Indian VPS servers.
"""

import os
import json
import time
import random
import asyncio
import logging
from typing import List, Dict, Optional, Tuple, Any
from urllib.parse import urlparse
import httpx

# Setup logger
logger = logging.getLogger("ProxyManager")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(BASE_DIR, "ind_proxies_cache.json")


class IndianProxyManager:
    def __init__(self, manual_proxy: str = ""):
        self.manual_proxy = manual_proxy.strip() if manual_proxy else ""
        self._verified_proxies: List[str] = []
        self._current_working_proxy: Optional[str] = None
        self._failed_proxies: set = set()
        self._is_refreshing: bool = False
        self._host_country: Optional[str] = None
        self._is_foreign_host: Optional[bool] = None
        self._lock = asyncio.Lock()
        
        # Load from cache file on startup
        self._load_cache()

    def _load_cache(self):
        """Loads cached verified proxies from disk."""
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    proxies = data.get("proxies", [])
                    # Filter out old or invalid entries
                    valid = [p.strip() for p in proxies if p and p.strip().startswith("http")]
                    if valid:
                        self._verified_proxies = valid
                        self._current_working_proxy = valid[0]
                        print(f"\033[96m[PROXY-MANAGER] Loaded {len(valid)} cached Indian proxies from disk.\033[0m")
            except Exception as e:
                logger.warning(f"Error loading proxy cache: {e}")

    def _save_cache(self):
        """Saves current verified proxies to disk."""
        try:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump({
                    "updated_at": time.time(),
                    "proxies": self._verified_proxies[:20]
                }, f, indent=2)
        except Exception as e:
            logger.warning(f"Error saving proxy cache: {e}")

    async def detect_host_location(self) -> str:
        """Detects host public IP and country to determine if VPS is abroad."""
        if self._host_country is not None:
            return self._host_country

        check_endpoints = [
            "http://ip-api.com/json",
            "https://ipapi.co/json/",
            "https://api.country.is/"
        ]
        
        for ep in check_endpoints:
            try:
                async with httpx.AsyncClient(timeout=4.0, verify=False) as client:
                    resp = await client.get(ep)
                    if resp.status_code == 200:
                        data = resp.json()
                        country = data.get("countryCode") or data.get("country") or ""
                        if country:
                            self._host_country = str(country).upper()
                            self._is_foreign_host = (self._host_country not in ["IN", "INDIA", "BD", "BANGLADESH"])
                            if self._is_foreign_host:
                                print(f"\033[93m[PROXY-MANAGER] Detected host region: {self._host_country} (Foreign VPS). Auto-proxy for IND will activate.\033[0m")
                            else:
                                print(f"\033[92m[PROXY-MANAGER] Detected host region: {self._host_country} (Subcontinent / Local).\033[0m")
                            return self._host_country
            except Exception:
                continue

        # If detection fails, assume foreign host on VPS to be safe
        self._host_country = "UNKNOWN"
        self._is_foreign_host = True
        return self._host_country

    def is_foreign_host(self) -> bool:
        if self._is_foreign_host is None:
            return False
        return self._is_foreign_host

    def set_foreign_host(self, foreign: bool = True):
        self._is_foreign_host = foreign

    async def _scrape_proxyscrape(self, country: str = "IN") -> List[str]:
        """Scrapes Proxyscrape v3 & v2 for free proxies."""
        proxies = []
        urls = [
            f"https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&country={country}&proxy_format=protocolipport&format=text",
            f"https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=6000&country={country}&ssl=all&anonymity=all"
        ]
        for url in urls:
            try:
                async with httpx.AsyncClient(timeout=6.0, verify=False) as c:
                    r = await c.get(url)
                    if r.status_code == 200:
                        lines = r.text.splitlines()
                        for line in lines:
                            l = line.strip()
                            if not l:
                                continue
                            if not l.startswith("http://") and not l.startswith("https://"):
                                if "://" in l:
                                    continue  # Skip socks if httpx socksio isn't configured
                                l = f"http://{l}"
                            if l.startswith("http://") or l.startswith("https://"):
                                proxies.append(l)
            except Exception:
                pass
        return proxies

    async def _scrape_geonode(self) -> List[str]:
        """Scrapes Geonode API for Indian HTTP proxies."""
        proxies = []
        url = "https://proxylist.geonode.com/api/proxy-list?country=IN&limit=50&page=1&sort_by=lastChecked&sort_type=desc"
        try:
            async with httpx.AsyncClient(timeout=6.0, verify=False) as c:
                r = await c.get(url)
                if r.status_code == 200:
                    data = r.json().get("data", [])
                    for item in data:
                        ip = item.get("ip")
                        port = item.get("port")
                        protocols = item.get("protocols", ["http"])
                        if ip and port and ("http" in protocols or "https" in protocols):
                            proxies.append(f"http://{ip}:{port}")
        except Exception:
            pass
        return proxies

    async def _scrape_github_lists(self) -> List[str]:
        """Scrapes fast GitHub proxy lists."""
        proxies = []
        urls = [
            "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt",
            "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt"
        ]
        for url in urls:
            try:
                async with httpx.AsyncClient(timeout=5.0, verify=False) as c:
                    r = await c.get(url)
                    if r.status_code == 200:
                        lines = r.text.splitlines()
                        for line in lines[:80]:
                            l = line.strip()
                            if l and ":" in l and not l.startswith("#"):
                                if not l.startswith("http://") and not l.startswith("https://"):
                                    l = f"http://{l}"
                                proxies.append(l)
            except Exception:
                pass
        return proxies

    async def scrape_all_candidates(self) -> List[str]:
        """Gathers proxy candidates from all available free scrapers concurrently."""
        tasks = [
            self._scrape_proxyscrape("IN"),
            self._scrape_geonode(),
            self._scrape_github_lists()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_candidates = []
        for res in results:
            if isinstance(res, list):
                all_candidates.extend(res)

        # De-duplicate while preserving order, remove previously failed proxies
        unique = []
        seen = set()
        for p in all_candidates:
            p_clean = p.strip()
            if p_clean and p_clean not in seen and p_clean not in self._failed_proxies:
                seen.add(p_clean)
                unique.append(p_clean)

        return unique

    async def validate_proxy(self, proxy: str, timeout: float = 3.5) -> Optional[Tuple[str, float]]:
        """Tests a proxy to verify connectivity and speed."""
        t0 = time.time()
        # Test against Garena endpoint or lightweight IP checker
        test_url = "https://clientbp.ppmainecoonghj.com/GetLoginData"
        try:
            async with httpx.AsyncClient(proxy=proxy, timeout=timeout, verify=False) as client:
                resp = await client.post(test_url, data=b"", headers={"User-Agent": "UnityPlayer/2018.4.12f1"})
                # Any HTTP response from Garena (even 503 or 400 from Garena load balancer) indicates successful connection
                latency = round(time.time() - t0, 2)
                return proxy, latency
        except Exception:
            # Fallback test via httpbin or ipify
            try:
                async with httpx.AsyncClient(proxy=proxy, timeout=timeout, verify=False) as client:
                    resp = await client.get("http://api.ipify.org?format=json")
                    if resp.status_code == 200:
                        latency = round(time.time() - t0, 2)
                        return proxy, latency
            except Exception:
                pass
        return None

    async def refresh_pool(self, min_proxies: int = 3, max_candidates_to_check: int = 40):
        """Refreshes verified proxy pool by validating candidates in parallel."""
        async with self._lock:
            if self._is_refreshing:
                return
            self._is_refreshing = True

        try:
            print("\033[93m[PROXY-MANAGER] Refreshing Indian proxy pool... Scraping free Indian proxies...\033[0m")
            candidates = await self.scrape_all_candidates()
            print(f"\033[96m[PROXY-MANAGER] Scraped {len(candidates)} candidates. Validating top {max_candidates_to_check}...\033[0m")

            # Shuffle candidates slightly to spread load
            subset = candidates[:max_candidates_to_check]
            random.shuffle(subset)

            sem = asyncio.Semaphore(20)

            async def _check_with_sem(p):
                async with sem:
                    return await self.validate_proxy(p)

            results = await asyncio.gather(*[_check_with_sem(p) for p in subset], return_exceptions=True)
            alive = []
            for res in results:
                if isinstance(res, tuple) and res is not None:
                    alive.append(res)

            # Sort by lowest latency
            alive.sort(key=lambda x: x[1])

            new_verified = [p for p, lat in alive]
            for p, lat in alive:
                print(f"\033[92m[PROXY-MANAGER] Verified working proxy: {p} (latency: {lat}s)\033[0m")

            if new_verified:
                for p in new_verified:
                    if p not in self._verified_proxies:
                        self._verified_proxies.append(p)
                if not self._current_working_proxy:
                    self._current_working_proxy = self._verified_proxies[0]
                self._save_cache()
                print(f"\033[92m[PROXY-MANAGER] Pool refreshed successfully. Total active proxies: {len(self._verified_proxies)}\033[0m")
            else:
                print("\033[91m[PROXY-MANAGER] Warning: No proxies from this batch passed validation. Retrying with next batch...\033[0m")
        except Exception as e:
            logger.error(f"[PROXY-MANAGER] Error during pool refresh: {e}")
        finally:
            self._is_refreshing = False

    async def get_working_proxy(self) -> Optional[str]:
        """Returns the best available working Indian proxy."""
        # 1. Check if user configured a manual proxy in .env
        if self.manual_proxy:
            return self.manual_proxy

        # 2. Return current verified proxy if available
        if self._current_working_proxy and self._current_working_proxy in self._verified_proxies:
            return self._current_working_proxy

        if self._verified_proxies:
            self._current_working_proxy = self._verified_proxies[0]
            return self._current_working_proxy

        # 3. If pool is empty, immediately refresh pool
        await self.refresh_pool()
        if self._verified_proxies:
            self._current_working_proxy = self._verified_proxies[0]
            return self._current_working_proxy

        return None

    def mark_proxy_failed(self, proxy: str, reason: str = ""):
        """Marks a proxy as failed, rotates to the next one, and schedules refresh if pool is low."""
        if not proxy:
            return
        self._failed_proxies.add(proxy)
        if proxy in self._verified_proxies:
            self._verified_proxies.remove(proxy)
        print(f"\033[91m[PROXY-MANAGER] Proxy failed ({reason}): {proxy}. Removed from pool. Remaining: {len(self._verified_proxies)}\033[0m")
        
        if self._verified_proxies:
            self._current_working_proxy = self._verified_proxies[0]
        else:
            self._current_working_proxy = None
            # Trigger background refresh
            asyncio.create_task(self.refresh_pool())

    def mark_proxy_success(self, proxy: str):
        """Promotes a proxy as the current working proxy."""
        if proxy and proxy not in self._verified_proxies:
            self._verified_proxies.insert(0, proxy)
        self._current_working_proxy = proxy
        self._save_cache()

    async def post(
        self,
        url: str,
        headers: Dict[str, str],
        data: Any,
        timeout: float = 14.0,
        max_retries: int = 5
    ) -> Optional[httpx.Response]:
        """
        Executes an HTTP POST request through verified Indian proxies with automatic retry and rotation.
        """
        for attempt in range(max_retries):
            proxy = await self.get_working_proxy()
            if not proxy:
                print("\033[91m[PROXY-MANAGER] No Indian proxies available in pool. Retrying scrape...\033[0m")
                await self.refresh_pool()
                proxy = await self.get_working_proxy()
                if not proxy:
                    print("\033[91m[PROXY-MANAGER] Failed to acquire any working Indian proxy.\033[0m")
                    return None

            try:
                print(f"\033[96m[PROXY-MANAGER] Routing POST via Indian proxy ({attempt+1}/{max_retries}): {proxy}\033[0m")
                async with httpx.AsyncClient(proxy=proxy, timeout=timeout, verify=False) as client:
                    resp = await client.post(url, headers=headers, data=data)
                    
                    # Check if Garena still blocked with BR_FFI_BLOCKED_NOT_IND_REGION_LOGIN
                    if resp.status_code == 400 and b"BR_FFI_BLOCKED_NOT_IND_REGION_LOGIN" in resp.content:
                        print(f"\033[93m[PROXY-MANAGER] Proxy {proxy} was flagged by Garena as non-IND. Discarding...\033[0m")
                        self.mark_proxy_failed(proxy, reason="Garena NOT_IND_REGION_LOGIN")
                        continue

                    if resp.status_code == 200:
                        self.mark_proxy_success(proxy)
                        return resp
                    else:
                        print(f"\033[93m[PROXY-MANAGER] Proxy {proxy} returned status {resp.status_code} (body: {resp.content[:100]})\033[0m")
                        # If 502/503/504 proxy gateway errors, rotate
                        if resp.status_code in [502, 503, 504, 403, 407]:
                            self.mark_proxy_failed(proxy, reason=f"HTTP {resp.status_code}")
                            continue
                        return resp

            except (httpx.ProxyError, httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
                self.mark_proxy_failed(proxy, reason=type(e).__name__)
                continue
            except Exception as e:
                print(f"\033[91m[PROXY-MANAGER] Proxy exception: {type(e).__name__}: {e}\033[0m")
                self.mark_proxy_failed(proxy, reason=str(e))
                continue

        return None


# Global singleton instance configured with any user override from config/env
_proxy_manager_instance: Optional[IndianProxyManager] = None

def get_proxy_manager() -> IndianProxyManager:
    global _proxy_manager_instance
    if _proxy_manager_instance is None:
        from config import FF_PROXY
        _proxy_manager_instance = IndianProxyManager(manual_proxy=FF_PROXY)
    return _proxy_manager_instance
