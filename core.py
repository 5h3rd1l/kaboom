from termcolor import colored
import curses
import httpx
import trio
import os
from argparse import ArgumentParser
from datetime import datetime
import time
import importlib
import pkgutil
import re
import sys
import random
import json
import concurrent.futures
import threading
from functools import lru_cache
from itertools import cycle

try:
    from free_verify_proxy import VerifyProxyLists
    from free_verify_proxy.proxy import ProxyScraper
    PROXY_SUPPORT = True
except ImportError:
    ProxyScraper = None
    PROXY_SUPPORT = False
    print(colored("Warning: free-verify-proxy not installed. Proxy support disabled.", "yellow"))
    print(colored("Install with: pip install free-verify-proxy", "yellow"))

try:
    from pyfiglet import Figlet
except ImportError:
    Figlet = None

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
except ImportError:
    Console = None
    Panel = None
    Table = None


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kaboom.instruments import TrioProgress

__version__ = "1"
APP_NAME = "KABOOM"
APP_DIR = os.path.dirname(os.path.abspath(__file__))
PROXY_STATE_FILE = os.path.join(APP_DIR, ".proxy_fetch_state.json")
PROXY_DEBUG_LOG_FILE = os.path.join(APP_DIR, ".proxy_debug.log")
PROXY_DEBUG = False
MAX_REPEAT = None  # No max limit
DEFAULT_REPEAT_INTERVAL = 5
MIN_REPEAT_INTERVAL = 1
STATUS_LABELS = {
    "accepted": "Accepted",
    "rate_limited": "Rate limited",
    "error": "Failed",
    "not_sent": "No action",
    "running": "Running",
    "pending": "Pending",
}
TUI_BODY_TOP = 12
TUI_BANNER_ROWS = 5
TUI_BANNER_TEXT = "KABOOM"
TUI_CREDIT_TEXT = "github.com/5h3rd1l/kaboom"
TUI_BANNER_FONT_NAME = "slant"
TUI_BANNER_COLORS = [1, 4, 3, 6, 7]
TUI_STEPS = [
    ("phones", "Phones"),
    ("modules", "Modules"),
    ("settings", "Settings"),
    ("run", "Run"),
    ("results", "Results"),
]
TUI_TITLE_STEPS = {
    "Start": None,
    "Fetch Proxies": None,
    "Phone Source": "phones",
    "Phone Numbers": "phones",
    "Import Phone Numbers": "phones",
    "Select File": "phones",
    "Import Summary": "phones",
    "Manage Phone Number": "phones",
    "Clear Numbers": "phones",
    "Invalid File": "phones",
    "Modules": "modules",
    "Single Module": "modules",
    "Custom Modules": "modules",
    "Invalid Selection": "modules",
    "Run Settings": "settings",
    "Run Summary": "settings",
    "Invalid Input": "settings",
    "Running": "run",
    "Waiting": "run",
    "Complete": "run",
    "Results": "results",
}

# Proxy Configuration
PROXY_COUNTRIES = ["all"]
PROXY_PROTOCOLS = ["http", "https"]
PROXY_ANONYMITY = ["all"]
PROXY_VERIFY_THREADS = 200
PROXY_VERIFY_TIMEOUT = (2, 2)
PROXY_FALLBACK_TIMEOUT = (5, 5)
PROXY_MAX_CANDIDATES = 120
PROXY_TARGET_COUNT = 20
MAX_PROXY_FAILURES = 3
PROXY_RETRY_DELAY = 0.5
PROXY_DISABLE_SECONDS = 1800
PROXY_HTTPX_VERIFY_TIMEOUT = 4
PROXY_TEST_URLS = [
    "https://api.ipify.org/",
    "https://checkip.amazonaws.com/",
    "https://httpbin.org/ip",
]
REQUEST_DELAY_MIN = 1
REQUEST_DELAY_MAX = 3
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 5


class ProxyManager:
    """Manages proxy rotation and verification"""
    
    def __init__(self):
        self.proxies = []
        self.proxy_records = {}
        self.proxy_cycle = None
        self.proxy_failures = {}
        self.last_refresh = 0
        self.initialized = False
        self.enabled = PROXY_SUPPORT
        self.fetch_stats = self._empty_fetch_stats()
        self.fetch_abort = None
        self.fetch_lock = threading.Lock()

    def _empty_fetch_stats(self):
        return {
            "phase": "idle",
            "candidates": 0,
            "verified": 0,
            "failed": 0,
            "sources": 0,
        }

    def load_cached(self):
        """Load previously fetched proxies from local state."""
        state = load_proxy_fetch_state()
        records = state.get("proxies") or []
        if not isinstance(records, list):
            return False
        loaded_records = {}
        for item in records:
            record = proxy_record_from_state(item)
            if record:
                loaded_records[record["url"]] = record
        if not loaded_records:
            return False
        self.proxy_records = loaded_records
        self._rebuild_proxy_rotation()
        try:
            self.last_refresh = float(state.get("last_fetched") or 0)
        except (TypeError, ValueError):
            self.last_refresh = 0
        self.initialized = True
        return True
    
    async def initialize(self, force_refresh=False, verbose=True, abort_event=None):
        """Fetch and verify proxies once at startup"""
        if not self.enabled:
            if verbose:
                print(colored("\n⚠️  Proxy support not available. Install free-verify-proxy to enable.", "yellow"))
            self.initialized = True
            return False
        
        if self.initialized and not force_refresh:
            return True
        
        if verbose:
            print(colored("\n🔄 Initializing proxy system...", "cyan"))
            print(colored("   This may take 10-20 seconds while verifying proxies...", "yellow"))
        
        loaded_records = None
        try:
            # Run the verification in a thread pool since it's CPU/IO heavy
            self._verbose = verbose
            self.fetch_abort = abort_event
            self.fetch_stats = self._empty_fetch_stats()
            existing_records = dict(self.proxy_records)
            verified_records = await trio.to_thread.run_sync(self._fetch_verified_proxies)
            self.last_refresh = time.time()
            
            if verified_records:
                loaded_records = self._merge_record_maps(existing_records, verified_records)
                self.proxy_records = loaded_records
                self._rebuild_proxy_rotation()
                self.initialized = True
                save_proxy_fetch_state(self.last_refresh, self.proxy_records.values())
                
                if verbose:
                    print(colored(f"\n✅ Successfully loaded {len(self.proxies)} verified proxies!", "green"))
                    if self.proxies:
                        print(colored(f"   First few proxies: {', '.join(self.proxies[:3])}", "dim"))
                return True
            else:
                if existing_records:
                    self.proxy_records = existing_records
                    self._rebuild_proxy_rotation()
                    self.initialized = True
                    return True
                self.initialized = True
                if verbose:
                    print(colored("\n⚠️  Warning: No verified proxies found. Running without proxy support.", "yellow"))
                return False
                
        except Exception as e:
            error_msg = str(e)
            # Handle the 'dim' color error gracefully
            if verbose and 'dim' in error_msg:
                print(colored("\n⚠️  Note: Rich text formatting not fully available.", "yellow"))
            if loaded_records:
                self.proxy_records = loaded_records
                self._rebuild_proxy_rotation()
                self.initialized = True
                return True
            if verbose:
                print(colored(f"\n⚠️  Continuing without proxy support...", "yellow"))
            self.initialized = True
            return False
        finally:
            self._verbose = True
            self.fetch_abort = None
    
    def _fetch_verified_proxies(self):
        """Synchronous proxy fetching function (runs in thread pool)"""
        try:
            verify_proxy_lists = VerifyProxyLists()
            proxy_candidates = self._fetch_fast_proxy_candidates()
            verified_records = {}

            if proxy_candidates:
                verified_records = self._verify_proxy_candidates(
                    proxy_candidates,
                    PROXY_TARGET_COUNT,
                )

            if not self._fetch_cancelled() and len(verified_records) < PROXY_TARGET_COUNT:
                self.fetch_stats["phase"] = "full fetch"
                fallback_proxies = self._fetch_library_verified_proxies(verify_proxy_lists)
                merged_candidates = self._merge_proxy_records(verified_records.values(), fallback_proxies)
                remaining = [
                    proxy_data for proxy_data in merged_candidates
                    if normalize_proxy_url(proxy_data.get("proxy")) not in verified_records
                ]
                top_up = self._verify_proxy_candidates(
                    remaining,
                    PROXY_TARGET_COUNT - len(verified_records),
                )
                verified_records = self._merge_record_maps(verified_records, top_up)
            return verified_records
            
        except Exception as e:
            if getattr(self, "_verbose", True):
                print(colored(f"Proxy verification error: {e}", "red"))
            return {}

    def _fetch_library_verified_proxies(self, verify_proxy_lists):
        if self._fetch_cancelled():
            return []
        return verify_proxy_lists.get_verifyProxyLists(
            countryCodes=PROXY_COUNTRIES,
            protocols=PROXY_PROTOCOLS,
            anonymityLevels=PROXY_ANONYMITY,
            number_of_threads=PROXY_VERIFY_THREADS,
            timeout=PROXY_FALLBACK_TIMEOUT,
        )

    def _merge_proxy_records(self, *proxy_groups):
        merged = []
        seen = set()
        for group in proxy_groups:
            for proxy_data in group or []:
                proxy = proxy_host_port((proxy_data.get("proxy") or proxy_data.get("url")) if isinstance(proxy_data, dict) else proxy_data)
                if not proxy or proxy in seen:
                    continue
                if isinstance(proxy_data, dict):
                    item = dict(proxy_data)
                    item["proxy"] = proxy
                else:
                    item = {"proxy": proxy}
                merged.append(item)
                seen.add(proxy)
        return merged

    def _fetch_fast_proxy_candidates(self):
        if self._fetch_cancelled():
            return []
        if ProxyScraper is None:
            return []

        scraper = ProxyScraper()
        fast_sources = [
            ("proxyscrape", scraper.get_proxyscrape),
            ("geonode", scraper.get_geonode_proxy),
            ("lumiproxy", scraper.get_lumiproxy_proxys),
            ("proxy-list.download", scraper.get_proxy_list),
        ]
        candidates = []
        self.fetch_stats["sources"] = len(fast_sources)
        self.fetch_stats["phase"] = "collecting"
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(fast_sources)) as executor:
            futures = {executor.submit(source): source_name for source_name, source in fast_sources}
            for future in concurrent.futures.as_completed(futures):
                source_name = futures[future]
                try:
                    result = future.result()
                except Exception:
                    continue
                if result:
                    for item in result:
                        if isinstance(item, dict):
                            item.setdefault("source", source_name)
                    candidates.extend(result)

        filtered = self._filter_proxy_candidates(candidates)
        random.shuffle(filtered)
        self.fetch_stats["candidates"] = len(filtered)
        return filtered[:PROXY_MAX_CANDIDATES]

    def _filter_proxy_candidates(self, candidates):
        filtered = []
        seen = set()
        countries = {country.upper() for country in PROXY_COUNTRIES}
        protocols = {protocol.lower() for protocol in PROXY_PROTOCOLS}
        anonymity_levels = {level.lower() for level in PROXY_ANONYMITY}

        for proxy_data in candidates:
            proxy = proxy_host_port(proxy_data.get("proxy"))
            if not proxy or proxy in seen:
                continue
            country = str(proxy_data.get("countryCode", "")).upper()
            protocol = str(proxy_data.get("protocol", "")).lower()
            anonymity = str(proxy_data.get("anonymityLevel", "")).lower()
            if countries and "ALL" not in countries and country not in countries:
                continue
            if protocols and protocol not in protocols:
                continue
            if anonymity_levels and "all" not in anonymity_levels and not any(level in anonymity for level in anonymity_levels):
                continue
            item = dict(proxy_data)
            item["proxy"] = proxy
            filtered.append(item)
            seen.add(proxy)
        return filtered

    def _verify_proxy_candidates(self, candidates, target_count):
        verified = {}
        if not candidates or target_count <= 0:
            return verified
        self.fetch_stats["phase"] = "verifying"
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=PROXY_VERIFY_THREADS)
        try:
            future_to_proxy = {
                executor.submit(self._verify_proxy_with_httpx, proxy_data): proxy_data
                for proxy_data in candidates
            }
            for future in concurrent.futures.as_completed(future_to_proxy):
                if self._fetch_cancelled():
                    break
                proxy_data = future_to_proxy[future]
                try:
                    record = future.result()
                    if record:
                        verified[record["url"]] = record
                        with self.fetch_lock:
                            self.proxy_records[record["url"]] = record
                            self._rebuild_proxy_rotation()
                        self.fetch_stats["verified"] += 1
                        if len(verified) >= target_count:
                            break
                    else:
                        self.fetch_stats["failed"] += 1
                except Exception:
                    self.fetch_stats["failed"] += 1
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        return verified

    def _fetch_cancelled(self):
        return bool(self.fetch_abort and self.fetch_abort.is_set())

    def _verify_proxy_with_httpx(self, proxy_data):
        proxy_url = normalize_proxy_url(proxy_data.get("proxy") if isinstance(proxy_data, dict) else proxy_data)
        if not proxy_url:
            return None
        errors = []
        for test_url in PROXY_TEST_URLS:
            try:
                with httpx.Client(proxy=proxy_url, timeout=PROXY_HTTPX_VERIFY_TIMEOUT, follow_redirects=True) as client:
                    response = client.get(test_url)
                if response.status_code == 200:
                    return make_proxy_record(proxy_url, proxy_data, verified=True)
                errors.append(f"{test_url}: HTTP {response.status_code}")
            except Exception as exc:
                errors.append(f"{test_url}: {module_exception_reason(exc)}")
        record = make_proxy_record(proxy_url, proxy_data, verified=False)
        record["last_error"] = errors[-1] if errors else "verification failed"
        proxy_debug_log(f"{proxy_url} verification failed: {record['last_error']}")
        return None

    def _merge_record_maps(self, *record_maps):
        merged = {}
        for record_map in record_maps:
            if isinstance(record_map, dict):
                iterable = record_map.values()
            else:
                iterable = record_map or []
            for record in iterable:
                normalized = proxy_record_from_state(record)
                if not normalized:
                    continue
                existing = merged.get(normalized["url"])
                if existing:
                    normalized["successes"] = max(existing.get("successes", 0), normalized.get("successes", 0))
                    normalized["failures"] = min(existing.get("failures", 0), normalized.get("failures", 0))
                merged[normalized["url"]] = normalized
        return merged

    def _rebuild_proxy_rotation(self):
        now = time.time()
        active_records = [
            record for record in self.proxy_records.values()
            if float(record.get("disabled_until") or 0) <= now
        ]
        active_records.sort(
            key=lambda record: (
                record.get("failures", 0),
                -record.get("successes", 0),
                -float(record.get("last_verified") or 0),
            )
        )
        self.proxies = [record["url"] for record in active_records]
        self.proxy_cycle = cycle(self.proxies) if self.proxies else None
        self.proxy_failures = {proxy: self.proxy_records[proxy].get("failures", 0) for proxy in self.proxies}
    
    def get_next_proxy(self):
        """Get the next proxy in rotation"""
        self._rebuild_proxy_rotation()
        if not self.proxies or not self.proxy_cycle:
            return None
        
        # Try to find a working proxy (not failed too many times)
        for _ in range(len(self.proxies)):
            proxy = next(self.proxy_cycle)
            if self.proxy_failures.get(proxy, 0) < MAX_PROXY_FAILURES:
                return proxy
        
        # If all proxies have failed too many times, reset failures
        return None
    
    def mark_success(self, proxy):
        """Mark a proxy as successful"""
        proxy = normalize_proxy_url(proxy)
        record = self.proxy_records.get(proxy)
        if record:
            record["successes"] = int(record.get("successes", 0)) + 1
            record["failures"] = max(0, int(record.get("failures", 0)) - 1)
            record["disabled_until"] = 0
            record["last_error"] = ""
            self._rebuild_proxy_rotation()
            save_proxy_fetch_state(self.last_refresh or time.time(), self.proxy_records.values())
    
    def mark_failure(self, proxy, reason=""):
        """Mark a proxy as failed"""
        proxy = normalize_proxy_url(proxy)
        record = self.proxy_records.get(proxy)
        if record:
            record["failures"] = int(record.get("failures", 0)) + 1
            record["last_error"] = reason or "request failed"
            proxy_debug_log(f"{proxy} failed: {record['last_error']}")
            if record["failures"] >= MAX_PROXY_FAILURES:
                record["disabled_until"] = time.time() + PROXY_DISABLE_SECONDS
                proxy_debug_log(f"{proxy} disabled until {datetime.fromtimestamp(record['disabled_until'])}")
            self._rebuild_proxy_rotation()
            save_proxy_fetch_state(self.last_refresh or time.time(), self.proxy_records.values())
    
    def get_stats(self):
        """Get proxy statistics"""
        if not self.proxy_records:
            return "No proxies loaded"
        now = time.time()
        active = sum(1 for record in self.proxy_records.values() if float(record.get("disabled_until") or 0) <= now)
        disabled = len(self.proxy_records) - active
        if disabled:
            return f"{active}/{len(self.proxy_records)} proxies active, {disabled} disabled"
        return f"{active}/{len(self.proxy_records)} proxies active"


# Global proxy manager instance
proxy_manager = ProxyManager()


def load_proxy_fetch_state():
    try:
        with open(PROXY_STATE_FILE, "r", encoding="utf-8") as file:
            state = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    if not isinstance(state, dict):
        return {}
    return state


def save_proxy_fetch_state(timestamp, proxies):
    records = []
    seen = set()
    for proxy in proxies:
        record = proxy_record_from_state(proxy)
        if not record or record["url"] in seen:
            continue
        records.append(record)
        seen.add(record["url"])
    state = {
        "last_fetched": float(timestamp),
        "count": len(records),
        "proxies": records,
    }
    try:
        with open(PROXY_STATE_FILE, "w", encoding="utf-8") as file:
            json.dump(state, file, indent=2)
            file.write("\n")
    except OSError:
        pass


def proxy_debug_log(message):
    if not PROXY_DEBUG:
        return
    try:
        with open(PROXY_DEBUG_LOG_FILE, "a", encoding="utf-8") as file:
            file.write(f"{datetime.now().isoformat(timespec='seconds')} {message}\n")
    except OSError:
        pass


def proxy_last_fetched_details(now=None):
    state = load_proxy_fetch_state()
    proxies = state.get("proxies")
    if not isinstance(proxies, list) or not proxies:
        return "Never fetched"
    count = state.get("count")
    if isinstance(count, int) and count <= 0:
        return "Never fetched"
    timestamp = state.get("last_fetched")
    if not timestamp:
        return "Never fetched"
    try:
        timestamp = float(timestamp)
    except (TypeError, ValueError):
        return "Never fetched"
    now = time.time() if now is None else now
    seconds = max(0, int(now - timestamp))
    if seconds < 60:
        relative = "just now" if seconds < 5 else f"{seconds}s ago"
    elif seconds < 3600:
        minutes = seconds // 60
        relative = f"{minutes}min ago"
    elif seconds < 86400:
        hours = seconds // 3600
        relative = f"{hours}hr{'s' if hours != 1 else ''} ago"
    else:
        days = seconds // 86400
        relative = f"{days}day{'s' if days != 1 else ''} ago"
    fetched_at = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    count_text = f" ({count} loaded)" if isinstance(count, int) else ""
    return f"{relative} at {fetched_at}{count_text}"


def normalize_proxy_url(proxy_url):
    if not proxy_url:
        return None
    proxy_url = str(proxy_url).strip()
    if not proxy_url:
        return None
    if "://" not in proxy_url:
        proxy_url = f"http://{proxy_url}"
    return proxy_url


def proxy_record_from_state(item):
    if isinstance(item, str):
        return make_proxy_record(item)
    if not isinstance(item, dict):
        return None
    proxy_url = normalize_proxy_url(item.get("url") or item.get("proxy"))
    if not proxy_url:
        return None
    now = time.time()
    return {
        "url": proxy_url,
        "host_port": proxy_host_port(proxy_url),
        "source": str(item.get("source") or "unknown"),
        "last_verified": float(item.get("last_verified") or now),
        "successes": int(item.get("successes") or 0),
        "failures": int(item.get("failures") or 0),
        "disabled_until": float(item.get("disabled_until") or 0),
        "last_error": str(item.get("last_error") or ""),
    }


def make_proxy_record(proxy_url, source_data=None, verified=True):
    proxy_url = normalize_proxy_url(proxy_url)
    if not proxy_url:
        return None
    source_data = source_data if isinstance(source_data, dict) else {}
    return {
        "url": proxy_url,
        "host_port": proxy_host_port(proxy_url),
        "source": str(source_data.get("source") or source_data.get("provider") or "free-verify-proxy"),
        "last_verified": time.time() if verified else 0,
        "successes": 0,
        "failures": 0 if verified else 1,
        "disabled_until": 0,
        "last_error": "",
    }


def proxy_host_port(proxy_url):
    if not proxy_url:
        return None
    proxy_url = str(proxy_url).strip()
    if not proxy_url:
        return None
    if "://" in proxy_url:
        proxy_url = proxy_url.split("://", 1)[1]
    return proxy_url.strip("/")


def create_httpx_client_with_proxy(proxy_url=None):
    """Create an httpx client with optional proxy support"""
    if proxy_url:
        proxy_url = normalize_proxy_url(proxy_url)
        # For httpx, proxies are set via the 'proxy' parameter (singular)
        return httpx.AsyncClient(
            timeout=10,
            proxy=proxy_url,  # Note: 'proxy' not 'proxies'
            follow_redirects=True
        )
    return httpx.AsyncClient(timeout=10, follow_redirects=True)


async def launch_module_with_proxy(module, phone, client, out, use_proxy=True):
    """Launch a module directly first, then retry failed results with proxies."""
    name = getattr(module, "__name__", str(module))

    def fallback_result(reason, rate_limited=False):
        return {
            "name": name,
            "domain": name,
            "frequent_rate_limit": rate_limited,
            "rateLimit": rate_limited,
            "sent": False,
            "error": not rate_limited,
            "reason": reason,
        }

    async def run_module_once(client_to_use):
        local_out = []
        try:
            await module(phone, client_to_use, local_out)
        except Exception as exc:
            error_str = str(exc).lower()
            rate_limited = any(token in error_str for token in ("rate limit", "429", "too many requests"))
            reason = "Rate limited" if rate_limited else module_exception_reason(exc)
            return fallback_result(reason, rate_limited)
        if not local_out:
            return fallback_result("module returned no result")
        return local_out[-1]

    # Add random delay to avoid rate limiting
    delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
    await trio.sleep(delay)

    direct_result = await run_module_once(client)
    direct_result["transport"] = "direct"
    direct_result["proxy"] = ""
    direct_result["proxy_retries"] = 0
    if result_status(direct_result) == "accepted" or not use_proxy:
        out.append(direct_result)
        return

    if not proxy_manager.enabled or not proxy_manager.proxies:
        out.append(direct_result)
        return

    last_result = direct_result
    for attempt in range(MAX_RETRIES):
        proxy = None
        client_to_use = None
        try:
            proxy = proxy_manager.get_next_proxy()
            if not proxy:
                break
            try:
                client_to_use = create_httpx_client_with_proxy(proxy)
            except ValueError:
                proxy_manager.mark_failure(proxy, "invalid proxy URL")
                continue
            client_to_use.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
            })

            proxy_result = await run_module_once(client_to_use)
            proxy_result["transport"] = "proxy"
            proxy_result["proxy"] = proxy
            proxy_result["proxy_retries"] = attempt + 1
            last_result = proxy_result
            if result_status(proxy_result) == "accepted":
                proxy_manager.mark_success(proxy)
                out.append(proxy_result)
                return
            proxy_manager.mark_failure(proxy, proxy_result.get("reason") or status_label(result_status(proxy_result)))
            if attempt < MAX_RETRIES - 1:
                await trio.sleep(PROXY_RETRY_DELAY)
        finally:
            if client_to_use is not None:
                await client_to_use.aclose()

    out.append(last_result)
    return


def import_submodules(package, recursive=True):
    """Get all the submodules"""
    if isinstance(package, str):
        package = importlib.import_module(package)
    results = {}
    for loader, name, is_pkg in pkgutil.walk_packages(package.__path__):
        full_name = package.__name__ + '.' + name
        results[full_name] = importlib.import_module(full_name)
        if recursive and is_pkg:
            results.update(import_submodules(full_name))
    return results


def get_functions(modules,args=None):
    """Transform the modules objects to functions"""
    websites = []

    for module in modules:
        if len(module.split(".")) > 3 :
            modu = modules[module]
            site = module.split(".")[-1]
            websites.append(modu.__dict__[site])
    return websites


def module_category(site):
    parts = getattr(site, "__module__", "").split(".")
    if len(parts) >= 3 and parts[-2] != "modules":
        return parts[-2].title()
    return "Other"


def grouped_site_names(websites):
    groups = {}
    for site in websites:
        groups.setdefault(module_category(site), []).append(site.__name__)
    return {category: sorted(names) for category, names in sorted(groups.items())}


def status_label(status):
    return STATUS_LABELS.get(status, status.replace("_", " ").title())


def result_status(result):
    if result.get("sent"):
        return "accepted"
    if result.get("rateLimit"):
        return "rate_limited"
    if result.get("error"):
        return "error"
    return "not_sent"


def module_exception_reason(exc):
    if isinstance(exc, httpx.ReadTimeout):
        return "request timed out after 10 seconds"
    if isinstance(exc, httpx.ConnectTimeout):
        return "connection timed out after 10 seconds"
    if isinstance(exc, httpx.TimeoutException):
        return "request timed out"
    return str(exc) or type(exc).__name__


def credit():
    """Print Credit"""
    print(APP_NAME)


def clear_terminal():
    print("\033[H\033[J", end="")


def print_banner():
    clear_terminal()
    print(colored(APP_NAME, "cyan", attrs=["bold"]))
    print(colored("=" * 48, "cyan"))
    print("Run controlled OTP request tests across configured service modules.")
    print("Press Ctrl-C anytime to stop.\n")


def normalize_phone(raw_phone):
    digits = "".join(ch for ch in raw_phone if ch.isdigit())
    if len(digits) < 10:
        raise ValueError("phone number must contain at least 10 digits")
    return f"92{digits[-10:]}"


def parse_phone_numbers_detailed(phone_input, is_file=None):
    """Parse phone numbers and keep invalid entries for UI summaries."""
    phone_numbers = []
    invalid_entries = []
    duplicate_entries = []
    seen = set()

    def add_numbers(raw_numbers):
        for num in raw_numbers:
            num = num.strip()
            if not num:
                continue
            try:
                normalized = normalize_phone(num)
                if normalized in seen:
                    duplicate_entries.append((num, normalized))
                    continue
                seen.add(normalized)
                phone_numbers.append(normalized)
            except ValueError as e:
                invalid_entries.append((num, str(e)))

    if is_file is None:
        is_file = phone_input.strip().endswith('.txt')

    if is_file:
        try:
            with open(phone_input.strip(), 'r') as file:
                for line in file:
                    add_numbers(line.split(','))
        except FileNotFoundError:
            raise ValueError(f"File not found: {phone_input}")
        except Exception as e:
            raise ValueError(f"Error reading file: {e}")
    else:
        for line in phone_input.splitlines():
            add_numbers(line.split(','))

    if not phone_numbers:
        raise ValueError("No valid phone numbers found")

    return phone_numbers, invalid_entries, duplicate_entries


def parse_phone_numbers(phone_input):
    """Parse phone numbers from comma-separated string or file path"""
    phone_numbers, invalid_entries, duplicate_entries = parse_phone_numbers_detailed(phone_input)
    for num, reason in invalid_entries:
        print(colored(f"Warning: Invalid number '{num}' - {reason}", "yellow"))
    for _, normalized in duplicate_entries:
        print(colored(f"Warning: Duplicate number skipped: {normalized}", "yellow"))
    return phone_numbers


def prompt_text(message):
    try:
        return input(message).strip()
    except EOFError:
        raise SystemExit("input aborted")


def prompt_int(message, default, minimum=None, maximum=None):
    value = prompt_text(message)
    if not value:
        return default
    try:
        number = int(value)
    except ValueError:
        raise SystemExit("value must be a number")
    if minimum is not None and number < minimum:
        raise SystemExit(f"value must be at least {minimum}")
    if maximum is not None and number > maximum:
        raise SystemExit(f"value must be at most {maximum}")
    return number


def prompt_choice(message, choices, default):
    normalized = {choice.lower(): choice for choice in choices}
    value = prompt_text(message).lower()
    if not value:
        return default
    if value not in normalized:
        raise SystemExit(f"choose one of: {', '.join(choices)}")
    return normalized[value]


def list_site_names(websites):
    return sorted(site.__name__ for site in websites)


def select_site_interactive(websites):
    names = list_site_names(websites)
    print(colored("\nAvailable sites", "cyan", attrs=["bold"]))
    for index, name in enumerate(names, start=1):
        print(f"{index:2}. {name}")

    selected = prompt_text("\nSite number or name: ")
    if not selected:
        raise SystemExit("site selection required")
    if selected.isdigit():
        index = int(selected)
        if index < 1 or index > len(names):
            raise SystemExit("site number out of range")
        selected = names[index-1]
    if selected not in names:
        raise SystemExit(f"unknown site: {selected}")
    return selected


def site_label(name):
    return name


def tui_setup(stdscr):
    try:
        curses.curs_set(0)
    except curses.error:
        pass
    if curses.has_colors():
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_CYAN, -1)
        curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_CYAN)
        curses.init_pair(3, curses.COLOR_YELLOW, -1)
        curses.init_pair(4, curses.COLOR_GREEN, -1)
        curses.init_pair(5, curses.COLOR_RED, -1)
        curses.init_pair(6, curses.COLOR_MAGENTA, -1)
        curses.init_pair(7, curses.COLOR_BLUE, -1)
        curses.init_pair(8, curses.COLOR_WHITE, -1)
    stdscr.keypad(True)


def tui_start():
    stdscr = curses.initscr()
    curses.noecho()
    curses.cbreak()
    tui_setup(stdscr)
    return stdscr


def tui_stop(stdscr):
    try:
        stdscr.keypad(False)
        curses.nocbreak()
        curses.noecho()
        curses.curs_set(1)
    except curses.error:
        pass
    curses.endwin()


def tui_attr(pair, fallback=0):
    if curses.has_colors():
        return curses.color_pair(pair)
    return fallback


def tui_addnstr(stdscr, y, x, text, maxlen=None, attr=0):
    height, width = stdscr.getmaxyx()
    if y < 0 or y >= height or x < 0 or x >= width:
        return
    safe_width = width - x - 1
    if safe_width <= 0:
        return
    if maxlen is None:
        maxlen = safe_width
    maxlen = max(0, min(maxlen, safe_width))
    try:
        stdscr.addnstr(y, x, text, maxlen, attr)
    except curses.error:
        pass


def tui_banner_frame():
    return int(time.monotonic())


@lru_cache(maxsize=1)
def tui_banner_renderer():
    if Figlet is None:
        return None
    return Figlet(font=TUI_BANNER_FONT_NAME, width=200, justify="left")


@lru_cache(maxsize=1)
def tui_banner_art():
    renderer = tui_banner_renderer()
    if renderer is None:
        return None
    lines = renderer.renderText(TUI_BANNER_TEXT).rstrip("\n").splitlines()
    lines = [line.rstrip() for line in lines if line.strip()]
    if not lines:
        return None
    common_indent = min((len(line) - len(line.lstrip(" ")) for line in lines), default=0)
    lines = [line[common_indent:] for line in lines]
    banner_width = max(len(line) for line in lines)
    return tuple(line.ljust(banner_width) for line in lines[:TUI_BANNER_ROWS])


def tui_draw_banner(stdscr, y=0):
    height, width = stdscr.getmaxyx()
    lines = tui_banner_art()
    if not lines:
        word = TUI_BANNER_TEXT
        glyph_width = 5
        total_width = len(word) * glyph_width + (len(word) - 1) * 1
        start_x = max(0, (width - total_width) // 2)
        for row in range(TUI_BANNER_ROWS):
            x = start_x
            for index, char in enumerate(word):
                glyph = {
                    "O": [" ███ ", "█   █", "█   █", "█   █", " ███ "],
                    "T": ["█████", "  █  ", "  █  ", "  █  ", "  █  "],
                    "P": ["████ ", "█   █", "████ ", "█    ", "█    "],
                    "E": ["█████", "█    ", "████ ", "█    ", "█████"],
                    "S": [" ████", "█    ", " ███ ", "    █", "████ "],
                    "R": ["████ ", "█   █", "████ ", "█ █  ", "█  ██"],
                }.get(char, ["█████"] * TUI_BANNER_ROWS)
                color_pair = TUI_BANNER_COLORS[index % len(TUI_BANNER_COLORS)]
                attr = tui_attr(color_pair)
                if row in (1, 2, 3):
                    attr |= curses.A_BOLD
                else:
                    attr |= curses.A_DIM
                tui_addnstr(stdscr, y + row, x, glyph[row], glyph_width, attr)
                x += glyph_width + 1
        return

    total_width = max(len(line) for line in lines)
    if total_width > width - 4:
        compact = TUI_BANNER_TEXT
        start_x = max(0, (width - len(compact)) // 2)
        tui_addnstr(stdscr, y + 2, start_x, compact, len(compact), tui_attr(1) | curses.A_BOLD)
        credit_x = max(0, (width - len(TUI_CREDIT_TEXT)) // 2)
        tui_addnstr(stdscr, y + 4, credit_x, TUI_CREDIT_TEXT, len(TUI_CREDIT_TEXT), curses.A_DIM)
        return

    start_x = max(0, (width - total_width) // 2)
    frame = tui_banner_frame()
    accent_row = frame % max(1, len(lines))

    for row, line in enumerate(lines):
        color_pair = TUI_BANNER_COLORS[row % len(TUI_BANNER_COLORS)]
        attr = tui_attr(color_pair) | curses.A_BOLD
        if row != accent_row:
            attr |= curses.A_DIM
        tui_addnstr(stdscr, y + row, start_x, line, total_width, attr)

    glow_y = y + len(lines)
    if glow_y < height - 1:
        underline = "-" * total_width
        tui_addnstr(stdscr, glow_y, start_x, underline, total_width, tui_attr(1) | curses.A_DIM)
        sparkle_x = start_x + (frame * 3 % max(1, total_width))
        if sparkle_x < width - 1:
            sparkle_attr = tui_attr(TUI_BANNER_COLORS[frame % len(TUI_BANNER_COLORS)]) | curses.A_BOLD
            tui_addnstr(stdscr, glow_y, sparkle_x, "*", 1, sparkle_attr)
    credit_y = y + len(lines) + 1
    if credit_y < height - 1:
        credit_x = max(0, (width - len(TUI_CREDIT_TEXT)) // 2)
        tui_addnstr(stdscr, credit_y, credit_x, TUI_CREDIT_TEXT, len(TUI_CREDIT_TEXT), curses.A_DIM)


def tui_step_for_title(title):
    return TUI_TITLE_STEPS.get(title)


def tui_draw_steps(stdscr, y, current_step):
    if current_step is None:
        return
    height, width = stdscr.getmaxyx()
    if y < 0 or y >= height - 1:
        return
    labels = []
    for step_id, label in TUI_STEPS:
        if step_id == current_step:
            labels.append(f"[{label}]")
        else:
            labels.append(label)
    line = "  >  ".join(labels)
    start_x = max(0, (width - len(line)) // 2)
    x = start_x
    for index, (step_id, label) in enumerate(TUI_STEPS):
        text = f"[{label}]" if step_id == current_step else label
        attr = tui_attr(4) | curses.A_BOLD if step_id == current_step else curses.A_DIM
        tui_addnstr(stdscr, y, x, text, len(text), attr)
        x += len(text)
        if index < len(TUI_STEPS) - 1:
            connector = "  >  "
            tui_addnstr(stdscr, y, x, connector, len(connector), curses.A_DIM)
            x += len(connector)


def tui_header(stdscr, title, subtitle=None, step=None, footer=None):
    stdscr.erase()
    height, width = stdscr.getmaxyx()
    tui_draw_banner(stdscr, 0)
    active_step = step if step is not None else tui_step_for_title(title)
    tui_draw_steps(stdscr, TUI_BANNER_ROWS + 3, active_step)
    if subtitle:
        tui_addnstr(stdscr, TUI_BANNER_ROWS + 4, 2, subtitle, max(0, width - 4), tui_attr(1))
    if footer is None:
        footer = "Up/Down or j/k move  Enter select  q quit"
    tui_addnstr(stdscr, height - 1, 0, footer.ljust(width), width, curses.A_DIM)


def tui_draw_table(stdscr, y, rows, widths, attrs=None):
    height, width = stdscr.getmaxyx()
    attrs = attrs or [0] * len(rows)
    for index, row in enumerate(rows):
        if y + index >= height - 1:
            break
        cells = []
        for cell, cell_width in zip(row, widths):
            text = str(cell)
            if len(text) > cell_width:
                text = text[: max(0, cell_width - 1)] + "…"
            cells.append(text.ljust(cell_width))
        tui_addnstr(stdscr, y + index, 2, "  ".join(cells), max(0, width - 4), attrs[index])


def tui_menu(stdscr, title, options, subtitle=None, start_index=0):
    if not options:
        raise SystemExit("no options available")
    index = max(0, min(start_index, len(options) - 1))
    offset = 0
    while True:
        tui_header(stdscr, title, subtitle)
        height, width = stdscr.getmaxyx()
        list_top = TUI_BODY_TOP
        visible_rows = max(1, height - list_top - 2)
        if index < offset:
            offset = index
        if index >= offset + visible_rows:
            offset = index - visible_rows + 1

        for row, item_index in enumerate(range(offset, min(len(options), offset + visible_rows))):
            y = list_top + row
            label, description, value = options[item_index]
            marker = "> " if item_index == index else "  "
            label_width = min(28, max(16, width // 3))
            line = f"{marker}{label:<{label_width}}"
            if description:
                line = f"{line} {description}"
            attr = curses.A_REVERSE if item_index == index else curses.A_NORMAL
            tui_addnstr(stdscr, y, 2, line.ljust(width - 4), max(0, width - 4), attr)

        stdscr.refresh()
        stdscr.timeout(120)
        key = stdscr.getch()
        if key in (ord("q"), 27):
            raise SystemExit("aborted")
        if key in (curses.KEY_UP, ord("k")):
            index = (index - 1) % len(options)
        elif key in (curses.KEY_DOWN, ord("j")):
            index = (index + 1) % len(options)
        elif key in (curses.KEY_ENTER, 10, 13):
            return options[index][2]


def tui_input(stdscr, title, prompt, default=""):
    value = default
    cursor = len(value)
    try:
        curses.curs_set(1)
    except curses.error:
        pass
    stdscr.nodelay(False)
    stdscr.timeout(120)
    while True:
        tui_header(stdscr, title)
        height, width = stdscr.getmaxyx()
        y = max(TUI_BODY_TOP, height // 2 - 1)
        tui_addnstr(stdscr, y, 4, prompt, max(0, width - 8), tui_attr(1) | curses.A_BOLD)
        if default:
            tui_addnstr(stdscr, y + 1, 4, f"Default: {default}", max(0, width - 8), curses.A_DIM)

        input_x = 6
        visible_width = max(8, width - input_x - 2)
        scroll = 0
        if cursor >= visible_width:
            scroll = cursor - visible_width + 1
        elif cursor < scroll:
            scroll = cursor
        display = value[scroll:scroll + visible_width]
        tui_addnstr(stdscr, y + 3, 4, "> ", 2, curses.A_BOLD)
        tui_addnstr(stdscr, y + 3, input_x, " " * visible_width, visible_width, curses.A_NORMAL)
        tui_addnstr(stdscr, y + 3, input_x, display, visible_width, curses.A_BOLD)
        cursor_x = input_x + max(0, cursor - scroll)
        try:
            stdscr.move(y + 3, min(width - 1, cursor_x))
        except curses.error:
            pass
        stdscr.refresh()

        key = stdscr.getch()
        if key == -1:
            continue
        if key in (curses.KEY_ENTER, 10, 13):
            try:
                curses.curs_set(0)
            except curses.error:
                pass
            stdscr.timeout(-1)
            return value.strip() or default
        if key in (curses.KEY_BACKSPACE, 127, 8):
            if cursor > 0:
                value = value[:cursor - 1] + value[cursor:]
                cursor -= 1
            continue
        if key == curses.KEY_DC:
            if cursor < len(value):
                value = value[:cursor] + value[cursor + 1:]
            continue
        if key == curses.KEY_LEFT:
            cursor = max(0, cursor - 1)
            continue
        if key == curses.KEY_RIGHT:
            cursor = min(len(value), cursor + 1)
            continue
        if key == curses.KEY_HOME:
            cursor = 0
            continue
        if key == curses.KEY_END:
            cursor = len(value)
            continue
        if 32 <= key <= 126:
            value = value[:cursor] + chr(key) + value[cursor:]
            cursor += 1


def tui_notice(stdscr, title, lines, options):
    menu_options = [(label, "", value) for label, value in options]
    subtitle = "  ".join(lines)
    return tui_menu(stdscr, title, menu_options, subtitle=subtitle)


def tui_start_menu(stdscr):
    proxy_status = "available" if proxy_manager.enabled else "unavailable"
    if proxy_manager.enabled and proxy_manager.proxies:
        proxy_status = f"{proxy_manager.get_stats()} loaded"
    return tui_menu(
        stdscr,
        "Start",
        [
            ("Continue", "Go to phone and module setup.", "continue"),
            ("Fetch proxies", f"Last fetched: {proxy_last_fetched_details()}  Status: {proxy_status}", "fetch"),
        ],
        subtitle="Choose how to start this session.",
    )


async def tui_fetch_proxies(stdscr):
    if not proxy_manager.enabled:
        tui_notice(
            stdscr,
            "Fetch Proxies",
            ["Proxy support is unavailable. Install free-verify-proxy to enable it."],
            [("Back", "back")],
        )
        return False

    started = time.time()
    done_event = threading.Event()
    stop_event = threading.Event()
    fetch_state = {"loaded": False, "error": None}

    def worker():
        try:
            fetch_state["loaded"] = trio.run(proxy_manager.initialize, True, False, stop_event)
        except Exception as exc:
            fetch_state["error"] = exc
        finally:
            done_event.set()

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    try:
        curses.curs_set(0)
    except curses.error:
        pass

    while True:
        elapsed = int(time.time() - started)
        stats = proxy_manager.fetch_stats
        subtitle = (
            f"{stats.get('phase', 'working')}   "
            f"Candidates {stats.get('candidates', 0)}   "
            f"Verified {stats.get('verified', 0)}   "
            f"Failed {stats.get('failed', 0)}   "
            f"{elapsed}s elapsed"
        )
        tui_header(stdscr, "Fetch Proxies", subtitle, footer="Enter continue  Esc back")
        height, width = stdscr.getmaxyx()
        y = max(TUI_BODY_TOP, height // 2 - 1)
        tui_addnstr(
            stdscr,
            y,
            4,
            "Collecting candidates, verifying them with the same HTTP client used at runtime.",
            max(0, width - 8),
            tui_attr(3) | curses.A_BOLD,
        )
        tui_addnstr(
            stdscr,
            y + 2,
            4,
            "Press Enter to continue with the proxies collected so far.",
            max(0, width - 8),
            curses.A_DIM,
        )
        if done_event.is_set():
            break
        stdscr.timeout(120)
        key = stdscr.getch()
        if key in (ord("q"), 27):
            stop_event.set()
            break
        if key in (curses.KEY_ENTER, 10, 13, ord("c"), ord("C")):
            stop_event.set()
            break

    if not done_event.is_set():
        # Give the worker a short window to flush any in-flight result updates.
        done_event.wait(1.0)

    if fetch_state["error"] is not None:
        tui_notice(
            stdscr,
            "Fetch Proxies",
            [f"Proxy fetch failed: {module_exception_reason(fetch_state['error'])}"],
            [("Continue", "continue")],
        )
        return bool(proxy_manager.proxies)

    stats = proxy_manager.get_stats()
    if proxy_manager.proxies:
        status_line = f"Loaded {len(proxy_manager.proxies)} verified proxies."
    else:
        status_line = "No verified proxies were loaded."
    tui_notice(
        stdscr,
        "Fetch Proxies",
        [status_line, stats, f"Last fetched: {proxy_last_fetched_details()}"],
        [("Continue", "continue")],
    )
    return bool(proxy_manager.proxies)


def tui_dashboard(stdscr, websites):
    groups = grouped_site_names(websites)
    tui_header(
        stdscr,
        "Dashboard",
        f"{len(websites)} modules available   "
        + ", ".join(f"{category}: {len(names)}" for category, names in groups.items()),
    )
    height, width = stdscr.getmaxyx()
    tui_addnstr(
        stdscr,
        max(TUI_BODY_TOP + 1, height // 2),
        4,
        "Select how to provide phone numbers.",
        max(0, width - 8),
        tui_attr(4) | curses.A_BOLD,
    )
    stdscr.refresh()


def tui_show_import_summary(stdscr, phone_numbers, invalid_entries):
    lines = [f"Imported {len(phone_numbers)} valid number(s)."]
    if invalid_entries:
        lines.append(f"Skipped {len(invalid_entries)} invalid entr{'y' if len(invalid_entries) == 1 else 'ies'}.")
    tui_notice(stdscr, "Import Summary", lines, [("Continue", "continue")])


def phone_list_has(phone_numbers, phone, ignore_index=None):
    for index, existing in enumerate(phone_numbers):
        if ignore_index is not None and index == ignore_index:
            continue
        if existing == phone:
            return True
    return False


def tui_draw_phone_workspace(stdscr, phone_numbers, selected_index, input_text, message="", message_attr=0):
    footer = "Enter add/open  Up/Down select  F import  E edit  R remove  C clear  Q quit"
    tui_header(stdscr, "Phone Numbers", f"{len(phone_numbers)} number(s) ready", footer=footer)
    height, width = stdscr.getmaxyx()
    list_top = TUI_BODY_TOP
    input_y = max(list_top + 6, height - 6)
    action_y = input_y - 2
    list_bottom = max(list_top + 1, action_y - 1)
    visible_rows = max(1, list_bottom - list_top)
    continue_selected = phone_numbers and selected_index == len(phone_numbers)

    tui_addnstr(stdscr, list_top - 1, 2, "Added numbers", max(0, width - 4), tui_attr(1) | curses.A_BOLD)
    if not phone_numbers:
        tui_addnstr(
            stdscr,
            list_top,
            4,
            "No phone numbers added yet. Type a number below in 923210001234 format and press Enter.",
            max(0, width - 8),
            curses.A_DIM,
        )
    else:
        offset = 0
        selected_phone_index = min(selected_index, len(phone_numbers) - 1)
        if selected_phone_index >= visible_rows:
            offset = selected_phone_index - visible_rows + 1
        for row, index in enumerate(range(offset, min(len(phone_numbers), offset + visible_rows))):
            marker = ">" if index == selected_index else " "
            line = f"{marker} {index + 1:2}. {phone_numbers[index]}"
            attr = curses.A_REVERSE if index == selected_index else curses.A_NORMAL
            tui_addnstr(stdscr, list_top + row, 4, line.ljust(width - 8), max(0, width - 8), attr)
        if len(phone_numbers) > visible_rows:
            tui_addnstr(
                stdscr,
                list_bottom,
                4,
                f"{len(phone_numbers) - visible_rows} more number(s). Use Up/Down to review.",
                max(0, width - 8),
                curses.A_DIM,
            )

    continue_label = " Continue "
    continue_attr = curses.A_REVERSE | curses.A_BOLD if continue_selected else curses.A_BOLD
    if not phone_numbers:
        continue_attr = curses.A_DIM
    tui_addnstr(stdscr, action_y, 4, continue_label, len(continue_label), continue_attr)

    tui_addnstr(stdscr, input_y, 2, "Add phone number (923210001234)", max(0, width - 4), tui_attr(1) | curses.A_BOLD)
    tui_addnstr(stdscr, input_y + 1, 4, "> " + input_text, max(0, width - 8), curses.A_BOLD)
    if message:
        tui_addnstr(stdscr, input_y + 3, 2, message, max(0, width - 4), message_attr)
    try:
        curses.curs_set(1)
        stdscr.move(input_y + 1, min(width - 2, 6 + len(input_text)))
    except curses.error:
        pass
    stdscr.refresh()


def tui_collect_phone_numbers(stdscr):
    phone_numbers = []
    selected_index = 0
    input_text = ""
    message = ""
    message_attr = curses.A_DIM

    while True:
        if phone_numbers:
            selected_index = max(0, min(selected_index, len(phone_numbers)))
        else:
            selected_index = 0

        tui_draw_phone_workspace(stdscr, phone_numbers, selected_index, input_text, message, message_attr)
        stdscr.timeout(120)
        key = stdscr.getch()

        if key in (ord("q"), ord("Q"), 27):
            raise SystemExit("aborted")
        if key in (curses.KEY_UP, ord("k")) and phone_numbers:
            selected_index = (selected_index - 1) % (len(phone_numbers) + 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")) and phone_numbers:
            selected_index = (selected_index + 1) % (len(phone_numbers) + 1)
            continue
        if key in (curses.KEY_BACKSPACE, 127, 8):
            input_text = input_text[:-1]
            continue
        if key in (10, 13, curses.KEY_ENTER):
            if input_text.strip():
                try:
                    phone = normalize_phone(input_text)
                except ValueError as exc:
                    message = str(exc)
                    message_attr = tui_attr(5) | curses.A_BOLD
                    continue
                if phone_list_has(phone_numbers, phone):
                    message = f"Duplicate number: {phone}"
                    message_attr = tui_attr(5) | curses.A_BOLD
                    continue
                phone_numbers.append(phone)
                selected_index = len(phone_numbers) - 1
                input_text = ""
                message = f"Added {phone}"
                message_attr = tui_attr(4) | curses.A_BOLD
                continue
            if phone_numbers and selected_index == len(phone_numbers):
                try:
                    curses.curs_set(0)
                except curses.error:
                    pass
                stdscr.timeout(-1)
                return phone_numbers
            if phone_numbers:
                current = phone_numbers[selected_index]
                number_action = tui_menu(
                    stdscr,
                    "Manage Phone Number",
                    [
                        ("Edit", current, "edit"),
                        ("Remove", current, "remove"),
                        ("Back", "", "back"),
                    ],
                    subtitle=f"Selected: {current}",
                )
                if number_action == "edit":
                    replacement = tui_input(stdscr, "Edit Phone Number", "Replacement phone number", current)
                    try:
                        normalized = normalize_phone(replacement)
                    except ValueError as exc:
                        message = str(exc)
                        message_attr = tui_attr(5) | curses.A_BOLD
                        continue
                    if phone_list_has(phone_numbers, normalized, ignore_index=selected_index):
                        message = f"Duplicate number: {normalized}"
                        message_attr = tui_attr(5) | curses.A_BOLD
                        continue
                    phone_numbers[selected_index] = normalized
                    message = f"Updated {current} to {phone_numbers[selected_index]}"
                    message_attr = tui_attr(4) | curses.A_BOLD
                elif number_action == "remove":
                    removed = phone_numbers.pop(selected_index)
                    selected_index = max(0, min(selected_index, len(phone_numbers)))
                    message = f"Removed {removed}"
                    message_attr = tui_attr(4) | curses.A_BOLD
                continue
            message = "Enter a phone number before adding."
            message_attr = tui_attr(3) | curses.A_BOLD
            continue
        if key in (ord("f"), ord("F")):
            file_path = tui_input(stdscr, "Import Phone Numbers", "File path")
            try:
                imported, invalid_entries, duplicate_entries = parse_phone_numbers_detailed(file_path, is_file=True)
            except ValueError as exc:
                message = str(exc)
                message_attr = tui_attr(5) | curses.A_BOLD
                continue
            for phone in imported:
                if not phone_list_has(phone_numbers, phone):
                    phone_numbers.append(phone)
            selected_index = max(0, len(phone_numbers) - 1)
            skipped = []
            if invalid_entries:
                skipped.append(f"{len(invalid_entries)} invalid")
            if duplicate_entries:
                skipped.append(f"{len(duplicate_entries)} duplicate")
            skipped_text = f" Skipped {', '.join(skipped)}." if skipped else ""
            message = f"Imported {len(imported)} unique number(s).{skipped_text}"
            message_attr = tui_attr(4) | curses.A_BOLD
            continue
        if key in (ord("e"), ord("E")):
            if not phone_numbers or selected_index == len(phone_numbers):
                message = "No number selected to edit."
                message_attr = tui_attr(3) | curses.A_BOLD
                continue
            current = phone_numbers[selected_index]
            replacement = tui_input(stdscr, "Edit Phone Number", "Replacement phone number", current)
            try:
                normalized = normalize_phone(replacement)
            except ValueError as exc:
                message = str(exc)
                message_attr = tui_attr(5) | curses.A_BOLD
                continue
            if phone_list_has(phone_numbers, normalized, ignore_index=selected_index):
                message = f"Duplicate number: {normalized}"
                message_attr = tui_attr(5) | curses.A_BOLD
                continue
            phone_numbers[selected_index] = normalized
            message = f"Updated {current} to {phone_numbers[selected_index]}"
            message_attr = tui_attr(4) | curses.A_BOLD
            continue
        if key in (ord("r"), ord("R")):
            if not phone_numbers or selected_index == len(phone_numbers):
                message = "No number selected to remove."
                message_attr = tui_attr(3) | curses.A_BOLD
                continue
            removed = phone_numbers.pop(selected_index)
            selected_index = max(0, min(selected_index, len(phone_numbers)))
            message = f"Removed {removed}"
            message_attr = tui_attr(4) | curses.A_BOLD
            continue
        if key in (ord("c"), ord("C")):
            if not phone_numbers:
                message = "The list is already empty."
                message_attr = tui_attr(3) | curses.A_BOLD
                continue
            decision = tui_notice(
                stdscr,
                "Clear Numbers",
                [f"Remove all {len(phone_numbers)} phone number(s) from this run?"],
                [("Clear list", "clear"), ("Back", "back")],
            )
            if decision == "clear":
                phone_numbers = []
                selected_index = 0
                message = "Phone number list cleared."
                message_attr = tui_attr(4) | curses.A_BOLD
            continue
        if 32 <= key <= 126:
            input_text += chr(key)


def tui_select_modules(stdscr, websites):
    names = list_site_names(websites)
    mode = tui_menu(
        stdscr,
        "Modules",
        [
            ("All modules", f"Run all {len(names)} configured modules.", ("all", None)),
            ("Single module", "Choose one service module.", ("single", None)),
            ("Custom selection", "Select multiple modules by category.", ("custom", None)),
        ],
    )
    selected_mode, _ = mode
    if selected_mode == "all":
        return names
    if selected_mode == "single":
        site_options = [
            (site_label(name), module_category(next(site for site in websites if site.__name__ == name)), name)
            for name in names
        ]
        return [tui_menu(stdscr, "Single Module", site_options)]

    selected = set()
    by_category = {}
    for site in websites:
        by_category.setdefault(module_category(site), []).append(site)

    while True:
        selected_count = len(selected)
        options = []
        for category in sorted(by_category):
            category_sites = sorted(by_category[category], key=lambda site: site.__name__)
            chosen = sum(1 for site in category_sites if site.__name__ in selected)
            options.append((category, f"{chosen}/{len(category_sites)} selected", ("category", category)))
        options.extend([
            ("Continue", f"{selected_count} module(s) selected", ("continue", None)),
            ("Clear selection", "Remove all selected modules.", ("clear", None)),
        ])
        action, value = tui_menu(stdscr, "Custom Modules", options, subtitle="Select categories to toggle modules.")
        if action == "continue":
            if selected:
                return sorted(selected)
            tui_notice(stdscr, "Invalid Selection", ["Select at least one module."], [("Try again", "again")])
            continue
        if action == "clear":
            selected.clear()
            continue

        category_sites = sorted(by_category[value], key=lambda site: site.__name__)
        while True:
            category_options = [
                (
                    f"[{'x' if site.__name__ in selected else ' '}] {site.__name__}",
                    "",
                    ("toggle", site.__name__),
                )
                for site in category_sites
            ]
            category_options.append(("Back", f"{len(selected)} total selected", ("back", None)))
            category_action, site_name = tui_menu(
                stdscr,
                value,
                category_options,
                subtitle="Enter toggles a module.",
            )
            if category_action == "back":
                break
            if site_name in selected:
                selected.remove(site_name)
            else:
                selected.add(site_name)


def tui_run_settings(stdscr):
    while True:
        raw_repeat = tui_input(stdscr, "Run Settings", "Number of rounds", "1")
        try:
            repeat = int(raw_repeat)
        except ValueError:
            tui_notice(stdscr, "Invalid Input", ["Rounds must be a number."], [("Try again", "again")])
            continue
        if repeat < 1:
            tui_notice(stdscr, "Invalid Input", ["Rounds must be at least 1."], [("Try again", "again")])
            continue
        break

    while True:
        raw_interval = tui_input(
            stdscr,
            "Run Settings",
            f"Seconds between rounds, default {DEFAULT_REPEAT_INTERVAL}",
            str(DEFAULT_REPEAT_INTERVAL),
        )
        try:
            repeat_interval = int(raw_interval)
        except ValueError:
            tui_notice(stdscr, "Invalid Input", ["Interval must be a number."], [("Try again", "again")])
            continue
        if repeat_interval < MIN_REPEAT_INTERVAL:
            tui_notice(
                stdscr,
                "Invalid Input",
                [f"Interval must be at least {MIN_REPEAT_INTERVAL} seconds."],
                [("Try again", "again")],
            )
            continue
        break
    return repeat, repeat_interval


def tui_result_status(result):
    return result_status(result)


def tui_status_attr(status):
    if status == "accepted":
        return tui_attr(4) | curses.A_BOLD
    if status == "rate_limited":
        return tui_attr(3) | curses.A_BOLD
    if status == "error":
        return tui_attr(5) | curses.A_BOLD
    if status == "running":
        return tui_attr(1) | curses.A_BOLD
    return curses.A_DIM


def tui_draw_run(stdscr, phone, websites, states, round_number, total_rounds, start_time, phone_index=None, total_phones=None, done=False):
    title = "Complete" if done else "Running"
    phone_info = f"Phone {phone}"
    if phone_index is not None and total_phones is not None:
        phone_info = f"Phone {phone_index}/{total_phones}: {phone}"
    tui_header(stdscr, title, f"Round {round_number}/{total_rounds}   {phone_info}")
    height, width = stdscr.getmaxyx()
    elapsed = round(time.time() - start_time, 1)
    site_names = [site.__name__ for site in websites]
    completed = sum(1 for name in site_names if states[name]["status"] not in ("pending", "running"))
    accepted = sum(1 for name in site_names if states[name]["status"] == "accepted")
    limited = sum(1 for name in site_names if states[name]["status"] == "rate_limited")
    errors = sum(1 for name in site_names if states[name]["status"] == "error")

    stats_y = TUI_BODY_TOP
    summary = (
        f"Done {completed}/{len(site_names)}   "
        f"Accepted {accepted}   Rate limited {limited}   Failed {errors}   Elapsed {elapsed}s"
    )
    tui_addnstr(stdscr, stats_y, 2, summary, max(0, width - 4), curses.A_BOLD)

    bar_width = max(10, width - 8)
    filled = int(bar_width * completed / max(1, len(site_names)))
    bar = "[" + "#" * filled + "-" * (bar_width - filled) + "]"
    tui_addnstr(stdscr, stats_y + 2, 2, bar, max(0, width - 4), tui_attr(1))

    list_top = stats_y + 4
    visible_rows = max(1, height - list_top - 3)
    table_width = max(20, width - 4)
    module_width = min(22, max(12, table_width // 4))
    status_width = 14
    duration_width = 9
    reason_width = max(12, table_width - module_width - status_width - duration_width - 6)
    header = [("Module", "Status", "Duration", "Reason")]
    tui_draw_table(stdscr, list_top, header, [module_width, status_width, duration_width, reason_width], [curses.A_BOLD])
    for row, name in enumerate(site_names[: max(0, visible_rows - 1)]):
        state = states[name]
        status = state["status"]
        reason = state.get("reason") or ""
        duration = state.get("duration")
        duration_text = f"{duration:.1f}s" if duration is not None else "-"
        tui_draw_table(
            stdscr,
            list_top + row + 1,
            [(name, status_label(status), duration_text, reason or "-")],
            [module_width, status_width, duration_width, reason_width],
            [tui_status_attr(status)],
        )

    rendered = max(0, visible_rows - 1)
    if len(site_names) > rendered:
        tui_addnstr(
            stdscr,
            height - 3,
            2,
            f"... {len(site_names) - rendered} more modules",
            max(0, width - 4),
            curses.A_DIM,
        )

    footer = "Ctrl-C stop"
    if done:
        footer = "Enter continue"
    tui_addnstr(stdscr, height - 1, 0, footer.ljust(width), width, curses.A_DIM)
    stdscr.refresh()


async def launch_module_tracked(module, phone, client, out, states):
    name = getattr(module, "__name__", str(module))
    states[name]["status"] = "running"
    started = time.time()
    local_out = []
    
    await launch_module_with_proxy(module, phone, client, local_out, use_proxy=True)
    
    result = local_out[-1] if local_out else {
        "name": name,
        "domain": name,
        "frequent_rate_limit": False,
        "rateLimit": False,
        "sent": False,
        "error": True,
        "reason": "module returned no result",
    }
    out.append(result)
    states[name]["status"] = tui_result_status(result)
    states[name]["reason"] = result.get("reason", "")
    states[name]["duration"] = time.time() - started


async def launch_module_tracked_multi(module, phone, client, out, states):
    name = getattr(module, "__name__", str(module))
    key = (phone, name)
    states[key]["status"] = "running"
    started = time.time()
    local_out = []
    
    await launch_module_with_proxy(module, phone, client, local_out, use_proxy=True)
    
    result = local_out[-1] if local_out else {
        "name": name,
        "domain": name,
        "frequent_rate_limit": False,
        "rateLimit": False,
        "sent": False,
        "error": True,
        "reason": "module returned no result",
    }
    result["phone"] = phone
    out.append(result)
    states[key]["status"] = tui_result_status(result)
    states[key]["reason"] = result.get("reason", "")
    states[key]["duration"] = time.time() - started


def tui_draw_run_batch(stdscr, phone_numbers, websites, states, round_number, total_rounds, start_time, done=False):
    title = "Complete" if done else "Running"
    tui_header(stdscr, title, f"Round {round_number}/{total_rounds}   Phones running together: {len(phone_numbers)}")
    height, width = stdscr.getmaxyx()
    elapsed = round(time.time() - start_time, 1)
    total = len(phone_numbers) * len(websites)
    completed = sum(1 for state in states.values() if state["status"] not in ("pending", "running"))
    accepted = sum(1 for state in states.values() if state["status"] == "accepted")
    limited = sum(1 for state in states.values() if state["status"] == "rate_limited")
    errors = sum(1 for state in states.values() if state["status"] == "error")

    stats_y = TUI_BODY_TOP
    active = sum(1 for state in states.values() if state["status"] == "running")
    summary = (
        f"Done {completed}/{total}   Running {active}   "
        f"Accepted {accepted}   Rate limited {limited}   Failed {errors}   Elapsed {elapsed}s"
    )
    tui_addnstr(stdscr, stats_y, 2, summary, max(0, width - 4), curses.A_BOLD)

    bar_width = max(10, width - 8)
    filled = int(bar_width * completed / max(1, total))
    bar = "[" + "#" * filled + "-" * (bar_width - filled) + "]"
    tui_addnstr(stdscr, stats_y + 2, 2, bar, max(0, width - 4), tui_attr(1))

    rows = []
    attrs = []
    for phone in phone_numbers:
        for site in websites:
            name = site.__name__
            state = states[(phone, name)]
            duration = state.get("duration")
            rows.append((
                phone,
                name,
                status_label(state["status"]),
                f"{duration:.1f}s" if duration is not None else "-",
                state.get("reason") or "-",
            ))
            attrs.append(tui_status_attr(state["status"]))

    list_top = stats_y + 4
    visible_rows = max(1, height - list_top - 3)
    table_width = max(30, width - 4)
    phone_width = 16
    module_width = min(18, max(10, table_width // 5))
    status_width = 14
    duration_width = 9
    reason_width = max(10, table_width - phone_width - module_width - status_width - duration_width - 8)
    widths = [phone_width, module_width, status_width, duration_width, reason_width]
    tui_draw_table(stdscr, list_top, [("Phone", "Module", "Status", "Duration", "Reason")], widths, [curses.A_BOLD])
    rendered_rows = rows[: max(0, visible_rows - 1)]
    tui_draw_table(stdscr, list_top + 1, rendered_rows, widths, attrs[:len(rendered_rows)])

    if len(rows) > len(rendered_rows):
        tui_addnstr(
            stdscr,
            height - 3,
            2,
            f"... {len(rows) - len(rendered_rows)} more checks",
            max(0, width - 4),
            curses.A_DIM,
        )

    footer = "Ctrl-C stop"
    if done:
        footer = "Round complete"
    tui_addnstr(stdscr, height - 1, 0, footer.ljust(width), width, curses.A_DIM)
    stdscr.refresh()


async def run_round_tui(stdscr, phone, websites, round_number, total_rounds, phone_index=None, total_phones=None, pause_on_done=True):
    start_time = time.time()
    out = []
    states = {
        website.__name__: {"status": "pending", "reason": "", "duration": None}
        for website in websites
    }
    client = httpx.AsyncClient(timeout=10)

    async def renderer():
        while True:
            tui_draw_run(stdscr, phone, websites, states, round_number, total_rounds, start_time, phone_index, total_phones)
            await trio.sleep(0.1)

    try:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(renderer)
            for website in websites:
                nursery.start_soon(launch_module_tracked, website, phone, client, out, states)
            while any(state["status"] in ("pending", "running") for state in states.values()):
                await trio.sleep(0.1)
            nursery.cancel_scope.cancel()
    finally:
        await client.aclose()

    tui_draw_run(stdscr, phone, websites, states, round_number, total_rounds, start_time, phone_index, total_phones, done=True)
    if pause_on_done:
        stdscr.nodelay(False)
        stdscr.getch()
    else:
        await trio.sleep(1)
    return out


async def run_round_tui_batch(stdscr, phone_numbers, websites, round_number, total_rounds):
    start_time = time.time()
    out = []
    states = {
        (phone, website.__name__): {"status": "pending", "reason": "", "duration": None}
        for phone in phone_numbers
        for website in websites
    }
    client = httpx.AsyncClient(timeout=10)

    async def renderer():
        while True:
            tui_draw_run_batch(stdscr, phone_numbers, websites, states, round_number, total_rounds, start_time)
            await trio.sleep(0.1)

    try:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(renderer)
            for phone in phone_numbers:
                for website in websites:
                    nursery.start_soon(launch_module_tracked_multi, website, phone, client, out, states)
            while any(state["status"] in ("pending", "running") for state in states.values()):
                await trio.sleep(0.1)
            nursery.cancel_scope.cancel()
    finally:
        await client.aclose()

    tui_draw_run_batch(stdscr, phone_numbers, websites, states, round_number, total_rounds, start_time, done=True)
    await trio.sleep(1)
    return out


async def tui_wait_between_rounds(stdscr, seconds, next_round, total_rounds, current_phone=None, total_phones=None):
    started = time.time()
    stdscr.nodelay(True)
    stdscr.timeout(120)
    while True:
        remaining = max(0, int(seconds - (time.time() - started)))
        subtitle = f"Next round {next_round}/{total_rounds}"
        if current_phone and total_phones:
            subtitle += f"   Phone {current_phone}/{total_phones}"
        tui_header(stdscr, "Waiting", subtitle)
        height, width = stdscr.getmaxyx()
        tui_addnstr(
            stdscr,
            max(TUI_BODY_TOP + 2, height // 2),
            4,
            f"Waiting {remaining} seconds. Press Ctrl-C to stop.",
            max(0, width - 8),
            tui_attr(3) | curses.A_BOLD,
        )
        stdscr.refresh()
        if remaining <= 0:
            break
        await trio.sleep(0.25)
    stdscr.nodelay(False)
    stdscr.timeout(-1)


def summarize_results(results):
    summary = {"accepted": 0, "rate_limited": 0, "error": 0, "not_sent": 0}
    for result in results:
        status = result_status(result)
        summary[status] = summary.get(status, 0) + 1
    return summary


def tui_results_summary(stdscr, results, phone_count, module_count, repeat):
    summary = summarize_results(results)
    direct_successes = sum(1 for result in results if result_status(result) == "accepted" and result.get("transport") == "direct")
    proxy_successes = sum(1 for result in results if result_status(result) == "accepted" and result.get("transport") == "proxy")
    proxy_retries = sum(int(result.get("proxy_retries") or 0) for result in results)
    actions = [("Run again", "again"), ("New run", "new"), ("Exit", "exit")]
    index = 0
    while True:
        tui_header(stdscr, "Results", "Run complete. Review totals and choose what to do next.")
        height, width = stdscr.getmaxyx()
        y = TUI_BODY_TOP
        total = len(results)
        tui_addnstr(stdscr, y, 2, "Summary", max(0, width - 4), tui_attr(1) | curses.A_BOLD)
        rows = [
            ("Phones", phone_count),
            ("Modules", module_count),
            ("Rounds", repeat),
            ("Total checks", total),
            ("Accepted", summary.get("accepted", 0)),
            ("Rate limited", summary.get("rate_limited", 0)),
            ("Failed", summary.get("error", 0)),
            ("Proxy retries", proxy_retries),
            ("Direct OK", direct_successes),
            ("Proxy OK", proxy_successes),
            ("No action", summary.get("not_sent", 0)),
        ]
        left_width = max(24, (width - 8) // 3)
        for item_index, (label, value) in enumerate(rows):
            row = item_index % 4 + 1
            column = item_index // 4
            x = 4 + (left_width * column)
            attr = curses.A_NORMAL
            if label == "Accepted":
                attr = tui_attr(4) | curses.A_BOLD
            elif label == "Rate limited":
                attr = tui_attr(3) | curses.A_BOLD
            elif label == "Failed":
                attr = tui_attr(5) | curses.A_BOLD
            tui_addnstr(stdscr, y + row, x, f"{label:<14} {value}", max(0, width - x - 2), attr)

        reason_counts = {}
        for result in results:
            status = result_status(result)
            if status not in ("error", "rate_limited"):
                continue
            reason = result.get("reason") or "-"
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        action_y = max(TUI_BODY_TOP, height - len(actions) - 1)
        reason_y = y + 6
        if reason_y < action_y - 1:
            tui_addnstr(stdscr, reason_y, 2, "Top issues", max(0, width - 4), tui_attr(1) | curses.A_BOLD)
            max_issue_rows = max(0, action_y - reason_y - 1)
            top_issues = sorted(reason_counts.items(), key=lambda item: item[1], reverse=True)[:max_issue_rows]
            for row, (reason, count) in enumerate(top_issues, start=1):
                tui_addnstr(stdscr, reason_y + row, 4, f"{count}x {reason}", max(0, width - 8), curses.A_DIM)

        for row, (label, value) in enumerate(actions):
            attr = curses.A_REVERSE | curses.A_BOLD if row == index else curses.A_NORMAL
            tui_addnstr(stdscr, action_y + row, 4, label.ljust(18), 18, attr)
        stdscr.refresh()
        stdscr.timeout(120)
        key = stdscr.getch()
        if key in (ord("q"), 27):
            return "exit"
        if key in (curses.KEY_UP, ord("k")):
            index = (index - 1) % len(actions)
        elif key in (curses.KEY_DOWN, ord("j")):
            index = (index + 1) % len(actions)
        elif key in (curses.KEY_ENTER, 10, 13):
            return actions[index][1]


async def run_tui(stdscr, phone_numbers, websites, repeat, repeat_interval):
    while True:
        all_results = []
        for round_number in range(1, repeat + 1):
            round_results = await run_round_tui_batch(
                stdscr,
                phone_numbers,
                websites,
                round_number,
                repeat,
            )
            all_results.extend(round_results)
            if round_number < repeat:
                await tui_wait_between_rounds(stdscr, repeat_interval, round_number + 1, repeat)

        action = tui_results_summary(stdscr, all_results, len(phone_numbers), len(websites), repeat)
        if action == "again":
            continue
        return action


def tui_config(stdscr, websites):
    tui_setup(stdscr)

    source = tui_menu(
        stdscr,
        "Phone Source",
        [
            ("Enter number(s)", "Type one or more numbers manually.", "manual"),
            ("Select file", "Load numbers from a .txt file.", "file"),
        ],
        subtitle="Choose how to provide the phone numbers.",
    )

    if source == "manual":
        phone_numbers = tui_collect_phone_numbers(stdscr)
    else:
        while True:
            file_path = tui_input(stdscr, "Select File", "Path to .txt file")
            try:
                phone_numbers, invalid_entries, duplicate_entries = parse_phone_numbers_detailed(file_path, is_file=True)
            except ValueError as exc:
                tui_notice(stdscr, "Invalid File", [str(exc)], [("Try again", "again")])
                continue
            lines = [f"Imported {len(phone_numbers)} unique number(s)."]
            if invalid_entries:
                lines.append(f"Skipped {len(invalid_entries)} invalid entr{'y' if len(invalid_entries) == 1 else 'ies'}.")
            if duplicate_entries:
                lines.append(f"Skipped {len(duplicate_entries)} duplicate entr{'y' if len(duplicate_entries) == 1 else 'ies'}.")
            tui_notice(stdscr, "Import Summary", lines, [("Continue", "continue")])
            break

    selected_names = tui_select_modules(stdscr, websites)
    repeat, repeat_interval = tui_run_settings(stdscr)
    tui_notice(
        stdscr,
        "Run Summary",
        [
            f"Phone numbers: {len(phone_numbers)}",
            f"Modules: {len(selected_names)}",
            f"Rounds: {repeat}",
            "Execution: all phone numbers run simultaneously each round",
            f"Interval: {repeat_interval}s between repeated rounds",
        ],
        [("Start run", "start")],
    )
    return {
        "phone_numbers": phone_numbers,
        "selected_sites": selected_names,
        "repeat": repeat,
        "repeat_interval": repeat_interval,
        "clear_screen": True,
    }


def print_result(data,phone,start_time,websites,clear_screen, phone_index=None, total_phones=None):
    def print_color(text,color):
        return(colored(text,color))

    phone_header = f"   {phone}"
    if phone_index and total_phones:
        phone_header = f"Phone {phone_index}/{total_phones}: {phone}"

    if clear_screen:
        print("\033[H\033[J")

    if Console and Table and Panel:
        console = Console()
        table = Table(title=phone_header)
        table.add_column("Module", style="cyan", no_wrap=True)
        table.add_column("Status", no_wrap=True)
        table.add_column("Reason")
        for result in data:
            status = result_status(result)
            style = {
                "accepted": "green",
                "rate_limited": "yellow",
                "error": "red",
                "not_sent": "magenta",
            }.get(status, "white")
            table.add_row(
                result.get("domain", result.get("name", "unknown")),
                f"[{style}]{status_label(status)}[/{style}]",
                result.get("reason", "-"),
            )
        elapsed = round(time.time() - start_time, 2)
        console.print(Panel.fit(APP_NAME, style="cyan"))
        console.print(table)
        console.print(f"{len(websites)} modules checked in {elapsed} seconds.")
        console.print("Accepted means the API responded successfully; it does not confirm OTP delivery.", style="dim")
        return

    description = print_color("[+] Accepted","green") + "," + print_color(" [x] Rate limited","yellow") + "," + print_color(" [!] Failed","red")
    print("*" * (len(phone_header) + 6))
    print(phone_header)
    print("*" * (len(phone_header) + 6))
    credit()

    for results in data:
        reason = results.get("reason")
        suffix = f" - {reason}" if reason else ""
        if results["rateLimit"]==True:
            websiteprint = print_color("[x] " + results["domain"] + suffix, "yellow")
            print(websiteprint)
        elif results["sent"] == False and results["error"] == False:
            websiteprint = print_color("[-] " + results["domain"] + suffix, "magenta")
            print(websiteprint)
        elif results["sent"] == True and results["error"] == False:
            websiteprint = print_color("[+] " + results["domain"] + suffix, "green")
            print(websiteprint)
        elif results["error"] == True:
            websiteprint = print_color("[!] " + results["domain"] + suffix, "red")
            print(websiteprint)

    print("\n" + description)
    print(str(len(websites)) + " websites checked in " +
          str(round(time.time() - start_time, 2)) + " seconds")
    print("Note: Accepted means the API responded successfully; it does not confirm OTP delivery.")


async def launch_module(module,phone, client, out):
    await launch_module_with_proxy(module, phone, client, out, use_proxy=True)


async def launch_module_for_phone(module, phone, client, out):
    local_out = []
    await launch_module(module, phone, client, local_out)
    result = local_out[-1] if local_out else {
        "name": getattr(module, "__name__", str(module)),
        "domain": getattr(module, "__name__", str(module)),
        "frequent_rate_limit": False,
        "rateLimit": False,
        "sent": False,
        "error": True,
        "reason": "module returned no result",
    }
    result["phone"] = phone
    out.append(result)


async def run_round(phone, websites, clear_screen, round_number=None, total_rounds=None, phone_index=None, total_phones=None):
    start_time = time.time()
    client = httpx.AsyncClient(timeout=10)
    out = []
    instrument = TrioProgress(len(websites))

    if round_number is not None and total_rounds is not None:
        if phone_index and total_phones:
            print(f"\nPhone {phone_index}/{total_phones} - Round {round_number}/{total_rounds}")
        else:
            print(f"\nRound {round_number}/{total_rounds}")

    trio.lowlevel.add_instrument(instrument)
    try:
        async with trio.open_nursery() as nursery:
            for website in websites:
                nursery.start_soon(launch_module, website, phone, client, out)
    finally:
        trio.lowlevel.remove_instrument(instrument)
        await client.aclose()

    print_result(out, phone, start_time, websites, clear_screen, phone_index, total_phones)


def print_batch_result(data, phone_numbers, start_time, websites, clear_screen, round_number=None, total_rounds=None):
    if clear_screen:
        print("\033[H\033[J")
    title = f"Round {round_number}/{total_rounds}" if round_number and total_rounds else "Run results"

    if Console and Table and Panel:
        console = Console()
        table = Table(title=title)
        table.add_column("Phone", style="cyan", no_wrap=True)
        table.add_column("Module", no_wrap=True)
        table.add_column("Status", no_wrap=True)
        table.add_column("Reason")
        for result in data:
            status = result_status(result)
            style = {
                "accepted": "green",
                "rate_limited": "yellow",
                "error": "red",
                "not_sent": "magenta",
            }.get(status, "white")
            table.add_row(
                result.get("phone", "-"),
                result.get("domain", result.get("name", "unknown")),
                f"[{style}]{status_label(status)}[/{style}]",
                result.get("reason", "-"),
            )
        elapsed = round(time.time() - start_time, 2)
        console.print(Panel.fit(APP_NAME, style="cyan"))
        console.print(table)
        console.print(f"{len(phone_numbers)} phone(s), {len(websites)} module(s), {len(data)} checks in {elapsed} seconds.")
        console.print("Accepted means the API responded successfully; it does not confirm OTP delivery.", style="dim")
        return

    credit()
    print(title)
    for result in data:
        print(f"{result.get('phone', '-')} {result.get('domain', result.get('name', 'unknown'))}: {status_label(result_status(result))}")
    print(f"{len(data)} checks completed in {round(time.time() - start_time, 2)} seconds")


async def run_round_batch(phone_numbers, websites, clear_screen, round_number=None, total_rounds=None):
    start_time = time.time()
    client = httpx.AsyncClient(timeout=10)
    out = []

    if round_number is not None and total_rounds is not None:
        print(f"\nRound {round_number}/{total_rounds} - running {len(phone_numbers)} phone(s) together")

    try:
        async with trio.open_nursery() as nursery:
            for phone in phone_numbers:
                for website in websites:
                    nursery.start_soon(launch_module_for_phone, website, phone, client, out)
    finally:
        await client.aclose()

    print_batch_result(out, phone_numbers, start_time, websites, clear_screen, round_number, total_rounds)


async def maincore():
    parser = ArgumentParser(description=f"{APP_NAME} v{__version__}")
    parser.add_argument("phone", nargs='?', metavar='PHONE', help="Target phone number(s) - comma separated or path to .txt file")
    parser.add_argument("--no-clear", default=False, required=False,action="store_true",dest="noclear",help="Do not clear the terminal to display the results")
    parser.add_argument("--site", default=None, required=False,action="store",dest="site",help="Check only one site")
    parser.add_argument("--repeat", default=1, required=False, type=int, help=f"Run multiple controlled rounds (no max limit)")
    parser.add_argument("--repeat-interval", default=DEFAULT_REPEAT_INTERVAL, required=False, type=int, help=f"Seconds between repeat rounds, default {DEFAULT_REPEAT_INTERVAL}, minimum {MIN_REPEAT_INTERVAL}")
    parser.add_argument("--no-proxy", default=False, action="store_true", help="Disable proxy support")
    parser.add_argument("--proxy-debug", default=False, action="store_true", help="Write proxy verification/runtime failures to .proxy_debug.log")

    args = parser.parse_args()
    global PROXY_DEBUG
    PROXY_DEBUG = args.proxy_debug
    if args.no_proxy:
        proxy_manager.enabled = False
    elif proxy_manager.enabled:
        proxy_manager.load_cached()
    
    interactive = args.phone is None
    use_tui = interactive and sys.stdin.isatty() and sys.stdout.isatty()

    clear_screen = not args.noclear
    onlysite = args.site
    repeat = args.repeat
    repeat_interval = args.repeat_interval

    # Import Modules
    modules = import_submodules("kaboom.modules")

    websites = get_functions(modules, args)

    if not use_tui:
        # Initialize proxy system for command-line runs. TUI users choose this from the start screen.
        if not args.no_proxy and PROXY_SUPPORT:
            await proxy_manager.initialize()
        elif not args.no_proxy and not PROXY_SUPPORT:
            print(colored("\n⚠️  free-verify-proxy not installed. Install with: pip install free-verify-proxy", "yellow"))
        elif args.no_proxy:
            print(colored("\n⚠️  Proxy support disabled by user", "yellow"))

    if use_tui:
        stdscr = tui_start()
        try:
            while True:
                startup_action = tui_start_menu(stdscr)
                if startup_action == "continue":
                    break
                if startup_action == "fetch":
                    await tui_fetch_proxies(stdscr)
            while True:
                config = tui_config(stdscr, websites)
                phone_numbers = config["phone_numbers"]
                selected_sites = config["selected_sites"]
                repeat = config["repeat"]
                repeat_interval = config["repeat_interval"]
                clear_screen = config["clear_screen"]

                if repeat < 1:
                    raise SystemExit("--repeat must be at least 1")
                if repeat > 1 and repeat_interval < MIN_REPEAT_INTERVAL:
                    raise SystemExit(f"--repeat-interval must be at least {MIN_REPEAT_INTERVAL} seconds")

                selected_websites = [site for site in websites if site.__name__ in selected_sites]
                if not selected_websites:
                    raise SystemExit("No modules selected")

                action = await run_tui(stdscr, phone_numbers, selected_websites, repeat, repeat_interval)
                if action != "new":
                    break
            return
        finally:
            tui_stop(stdscr)
    elif interactive:
        print_banner()
        phone_input = prompt_text("Phone number(s) (comma separated or path to .txt file): ")
        try:
            phone_numbers = parse_phone_numbers(phone_input)
        except ValueError as exc:
            raise SystemExit(str(exc))

        print(colored("\nRun mode", "cyan", attrs=["bold"]))
        print("1. All modules")
        print("2. Pick one module")
        mode = prompt_choice("Choose mode [1]: ", {"1", "2"}, "1")

        if mode == "1":
            pass
        elif mode == "2":
            onlysite = select_site_interactive(websites)

        # No max limit on rounds
        while True:
            repeat_input = prompt_text(f"\nNumber of rounds (any positive integer) [1]: ")
            if not repeat_input:
                repeat = 1
                break
            try:
                repeat = int(repeat_input)
                if repeat < 1:
                    print(colored("Rounds must be at least 1", "red"))
                    continue
                break
            except ValueError:
                print(colored("Please enter a valid number", "red"))
        
        if repeat > 1:
            repeat_interval = prompt_int(
                f"Seconds between rounds, default {DEFAULT_REPEAT_INTERVAL}, min {MIN_REPEAT_INTERVAL} [{DEFAULT_REPEAT_INTERVAL}]: ",
                DEFAULT_REPEAT_INTERVAL,
                MIN_REPEAT_INTERVAL,
            )
        clear_screen = True
    else:
        phone_input = args.phone
        try:
            phone_numbers = parse_phone_numbers(phone_input)
        except ValueError as exc:
            raise SystemExit(str(exc))

    if repeat < 1:
        raise SystemExit("--repeat must be at least 1")
    if repeat > 1 and repeat_interval < MIN_REPEAT_INTERVAL:
        raise SystemExit(f"--repeat-interval must be at least {MIN_REPEAT_INTERVAL} seconds")

    if onlysite:
        onlysite=[onlysite]
        websites = [site for site in websites if site.__name__  in onlysite]
        if not websites:
            raise SystemExit(f"Unknown site: {onlysite[0]}")

    if interactive and not use_tui:
        print(colored("\nReview", "cyan", attrs=["bold"]))
        print(f"Phone numbers: {len(phone_numbers)} loaded")
        print(f"First phone: {phone_numbers[0]}")
        print(f"Modules: {', '.join(list_site_names(websites))}")
        print(f"Rounds: {repeat}")
        print("Execution: all phone numbers run simultaneously each round")
        if repeat > 1:
            print(f"Interval: {repeat_interval} seconds")
        confirm = prompt_choice("Start? y/N: ", {"y", "n"}, "n")
        if confirm != "y":
            raise SystemExit("aborted")

    for round_number in range(1, repeat + 1):
        await run_round_batch(phone_numbers, websites, clear_screen, round_number, repeat)
        if round_number < repeat:
            print(f"\nWaiting {repeat_interval} seconds before next round. Press Ctrl-C to stop.")
            await trio.sleep(repeat_interval)


def main():
    try:
        trio.run(maincore)
    except KeyboardInterrupt:
        print("\nStopped.")
    except Exception as e:
        print(colored(f"\nError: {e}", "red"))
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
