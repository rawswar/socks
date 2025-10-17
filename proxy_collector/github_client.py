from __future__ import annotations

import json
import logging
import random
import threading
import time
from base64 import b64decode
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode

import requests
from requests import Response
from requests.exceptions import RequestException

from .models import GitHubRateInfo


class RateLimiter:
    """Thread-safe rate limiter to prevent exceeding API rate limits."""
    
    def __init__(
        self,
        requests_per_minute: int,
        min_interval: float,
        max_interval: float,
    ) -> None:
        self.rpm = max(0, requests_per_minute)
        self.min_interval = max(0.0, min_interval)
        self.max_interval = max(self.min_interval, max_interval)
        self.request_times: Deque[float] = deque()
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__ + ".RateLimiter")
        self.random = random.Random()
    
    def wait_if_needed(self) -> None:
        """Wait if necessary to respect rate limits and request intervals."""
        interval = None
        while True:
            with self.lock:
                now = time.time()

                # Clean up timestamps older than 60 seconds
                while self.request_times and now - self.request_times[0] >= 60.0:
                    self.request_times.popleft()

                if interval is None:
                    interval = self._next_interval()

                sleep_time = 0.0
                reason = ""

                if self.request_times:
                    elapsed = now - self.request_times[-1]
                    if elapsed < interval:
                        sleep_time = interval - elapsed
                        reason = f"request interval ({interval:.2f}s)"

                if self.rpm > 0 and len(self.request_times) >= self.rpm:
                    earliest = self.request_times[0]
                    rpm_wait = max(0.0, 60.0 - (now - earliest))
                    if rpm_wait > sleep_time:
                        reason = "requests per minute limit"
                    sleep_time = max(sleep_time, rpm_wait)

                if sleep_time <= 0:
                    self.request_times.append(now)
                    return

            self.logger.info(
                "Rate limiter sleeping %.2fs due to %s",
                sleep_time,
                reason or "rate limiting",
            )
            time.sleep(sleep_time)
            # Loop and re-check conditions with the same interval

    def _next_interval(self) -> float:
        if self.max_interval <= self.min_interval:
            return self.min_interval
        return self.random.uniform(self.min_interval, self.max_interval)

    def reset(self) -> None:
        with self.lock:
            self.request_times.clear()


class GitHubClient:
    def __init__(
        self,
        base_url: str,
        token: Optional[str],
        *,
        request_timeout: int,
        max_retries: int,
        requests_per_minute: int = 5,
        min_request_interval: float = 6.0,
        max_request_interval: float = 10.0,
        secondary_rate_limit_cooldown: float = 300.0,
        initial_backoff: float = 60.0,
        max_backoff: float = 3600.0,
        backoff_jitter_ratio: float = 0.1,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.timeout = request_timeout
        self.max_retries = max_retries
        self.secondary_rate_limit_cooldown = secondary_rate_limit_cooldown
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_jitter_ratio = backoff_jitter_ratio
        self.logger = logging.getLogger(__name__ + ".GitHubClient")
        self.lock = threading.RLock()
        self.rate_info = GitHubRateInfo(remaining=5000, reset_timestamp=time.time())
        self.cooldown_until = 0.0
        self.etag_cache: Dict[str, Dict[str, Any]] = {}
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            requests_per_minute=requests_per_minute,
            min_interval=min_request_interval,
            max_interval=max_request_interval,
        )

        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "socks5-proxy-collector",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
            self.logger.info("GitHub token configured for authenticated requests")
        else:
            self.logger.warning("No GitHub token configured - using unauthenticated requests")
        self.session.headers.update(headers)

    def _cache_key(self, url: str, params: Optional[Dict[str, Any]]) -> str:
        if not params:
            return url
        return f"{url}?{urlencode(sorted(params.items()))}"

    def _respect_rate_limits(self) -> None:
        while True:
            with self.lock:
                now = time.time()
                reset_wait = self.rate_info.reset_timestamp if self.rate_info.remaining <= 0 else 0.0
                wait_until = max(self.cooldown_until, reset_wait)
            if wait_until and wait_until > now:
                sleep_for = wait_until - now
                self.logger.info("Rate limit cooldown active, sleeping for %.2fs", sleep_for)
                time.sleep(max(0.0, sleep_for))
                continue
            break

    def _update_rate_info(self, response: Response) -> None:
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset = response.headers.get("X-RateLimit-Reset")
        limit = response.headers.get("X-RateLimit-Limit")
        now = time.time()
        with self.lock:
            if remaining is not None:
                try:
                    self.rate_info.remaining = int(remaining)
                except ValueError:
                    pass
            if reset is not None:
                try:
                    reset_ts = float(reset)
                    if reset_ts >= now:
                        self.rate_info.reset_timestamp = reset_ts
                except ValueError:
                    pass
            if limit is not None:
                try:
                    self.rate_info.limit = int(limit)
                except ValueError:
                    pass
            if self.rate_info.remaining <= 0 and self.rate_info.reset_timestamp > now:
                self.cooldown_until = self.rate_info.reset_timestamp
            else:
                self.cooldown_until = 0.0

    def _log_rate_status(self) -> None:
        with self.lock:
            remaining = self.rate_info.remaining
            limit = self.rate_info.limit
            reset_timestamp = self.rate_info.reset_timestamp
        reset_in = max(0.0, reset_timestamp - time.time())
        limit_text = f"/{limit}" if limit is not None else ""
        self.logger.info(
            "GitHub rate limit remaining: %s%s (resets in %.0fs)",
            remaining,
            limit_text,
            reset_in,
        )

    def _maybe_cache_response(self, cache_key: str, response: Response, payload: Any) -> None:
        etag = response.headers.get("ETag")
        if etag:
            with self.lock:
                self.etag_cache[cache_key] = {"etag": etag, "payload": payload}

    def _cached_payload(self, cache_key: str) -> Optional[Any]:
        with self.lock:
            cached = self.etag_cache.get(cache_key)
            if cached:
                return cached.get("payload")
        return None

    def _build_headers(
        self,
        cache_key: str,
    ) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        with self.lock:
            cached = self.etag_cache.get(cache_key)
            if cached and "etag" in cached:
                headers["If-None-Match"] = cached["etag"]
        return headers

    def _raw_url_from_html(self, html_url: Optional[str]) -> Optional[str]:
        if not html_url:
            return None
        prefix = "https://github.com/"
        if not html_url.startswith(prefix):
            return None
        remainder = html_url[len(prefix) :]
        segments = remainder.split("/")
        if len(segments) < 5 or segments[2] != "blob":
            return None
        owner, repo, _, ref, *path_parts = segments
        if not owner or not repo or not ref or not path_parts:
            return None
        path = "/".join(path_parts)
        if not path:
            return None
        if "?" in path:
            path = path.split("?", 1)[0]
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{ref}/{path}"

    def _decode_base64_content(self, encoded: str) -> Optional[str]:
        try:
            normalized = "".join(encoded.splitlines())
            decoded_bytes = b64decode(normalized)
            return decoded_bytes.decode("utf-8", errors="replace")
        except (ValueError, UnicodeDecodeError) as exc:
            self.logger.debug("Failed to decode base64 content: %s", exc)
            return None

    def _detect_secondary_rate_limit(self, response: Response) -> bool:
        """Detect if the response indicates a secondary rate limit."""
        try:
            text = response.text.lower()
            return (
                "secondary rate limit" in text
                or "abuse detection" in text
                or "you have exceeded a secondary rate limit" in text
            )
        except Exception:
            return False
    
    def _calculate_exponential_backoff(self, attempt: int) -> float:
        """Calculate exponential backoff with jitter."""
        delay = min(self.initial_backoff * (2 ** attempt), self.max_backoff)
        jitter = random.uniform(0, delay * self.backoff_jitter_ratio)
        return delay + jitter
    
    def _handle_rate_limit_response(self, response: Response, attempt: int = 0) -> bool:
        """Handle rate limit responses including secondary rate limits."""
        # Check for secondary rate limit
        if self._detect_secondary_rate_limit(response):
            wait_for = self.secondary_rate_limit_cooldown
            self.logger.warning(
                "Secondary rate limit detected. Cooling down for %.2fs before retrying.",
                wait_for,
            )
            with self.lock:
                self.cooldown_until = max(self.cooldown_until, time.time() + wait_for)
            self.rate_limiter.reset()
            return True
        
        # Check Retry-After header
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                wait_for = float(retry_after)
                with self.lock:
                    self.cooldown_until = max(self.cooldown_until, time.time() + wait_for)
                self.logger.warning("GitHub requested retry after %.2fs", wait_for)
                self.rate_limiter.reset()
                return True
            except ValueError:
                pass
        
        # Check X-RateLimit-Reset header
        reset = response.headers.get("X-RateLimit-Reset")
        if reset:
            try:
                reset_ts = float(reset)
                with self.lock:
                    self.cooldown_until = max(self.cooldown_until, reset_ts)
                wait_for = max(0.0, reset_ts - time.time())
                self.logger.warning(
                    "GitHub rate limit hit (remaining: %s); waiting %.2fs",
                    response.headers.get("X-RateLimit-Remaining", "unknown"),
                    wait_for,
                )
                self.rate_limiter.reset()
                return True
            except ValueError:
                pass
        
        return False

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        raw: bool = False,
    ) -> Optional[Any]:
        cache_key = self._cache_key(url, params)
        for attempt in range(self.max_retries + 1):
            # Apply rate limiting before making the request
            self._respect_rate_limits()
            self.rate_limiter.wait_if_needed()
            
            headers = self._build_headers(cache_key)
            request_headers = dict(self.session.headers)
            request_headers.update(headers)
            if raw:
                request_headers["Accept"] = "application/vnd.github.v3.raw"
            else:
                request_headers["Accept"] = "application/vnd.github+json"
            
            try:
                response = self.session.request(
                    method,
                    url,
                    params=params,
                    timeout=self.timeout,
                    headers=request_headers,
                )
            except RequestException as exc:
                backoff_time = self._calculate_exponential_backoff(attempt)
                self.logger.warning(
                    "GitHub request error (%s): %s (attempt %d/%d, retrying in %.2fs)",
                    url,
                    exc,
                    attempt + 1,
                    self.max_retries + 1,
                    backoff_time,
                )
                time.sleep(backoff_time)
                continue

            self._update_rate_info(response)

            if response.status_code == 304:
                cached_payload = self._cached_payload(cache_key)
                if cached_payload is not None:
                    self.logger.debug("Cache hit for %s", cache_key)
                    return cached_payload
                self.logger.debug("Received 304 but cache empty for %s", cache_key)
                return None

            if response.status_code == 429:
                if self._handle_rate_limit_response(response, attempt):
                    continue
            
            if response.status_code == 403:
                if self._handle_rate_limit_response(response, attempt):
                    continue

            if 500 <= response.status_code < 600:
                backoff_time = self._calculate_exponential_backoff(attempt)
                self.logger.warning(
                    "GitHub server error %d for %s (attempt %d/%d, retrying in %.2fs)",
                    response.status_code,
                    url,
                    attempt + 1,
                    self.max_retries + 1,
                    backoff_time,
                )
                time.sleep(backoff_time)
                continue

            if response.status_code >= 400:
                self.logger.error(
                    "GitHub request failed (%s) with status %d: %s",
                    url,
                    response.status_code,
                    response.text[:500] if response.text else "",
                )
                return None

            payload: Any
            if raw:
                payload = response.text
            else:
                try:
                    payload = response.json()
                except json.JSONDecodeError:
                    self.logger.error("Failed to decode JSON for %s", url)
                    return None

            self._maybe_cache_response(cache_key, response, payload)
            return payload
        
        self.logger.error("Exceeded retry attempts (%d) for %s", self.max_retries, url)
        return None

    def search_code(
        self,
        query: str,
        *,
        per_page: int,
        max_pages: int,
    ) -> Optional[Dict[str, Any]]:
        endpoint = f"{self.base_url}/search/code"
        aggregated_items = []
        had_response = False
        for page in range(1, max_pages + 1):
            params = {
                "q": query,
                "per_page": per_page,
                "page": page,
            }
            payload = self._request("GET", endpoint, params=params)
            if payload is None:
                break
            had_response = True
            items = payload.get("items", [])
            aggregated_items.extend(items)
            incomplete = payload.get("incomplete_results", False)
            self.logger.info(
                "Fetched %d items for query '%s' (page %d)",
                len(items),
                query,
                page,
            )
            if not items or incomplete:
                break
        if not had_response:
            return None
        return {"items": aggregated_items}

    def fetch_file_content(self, download_url: str) -> Optional[str]:
        return self._request("GET", download_url, raw=True)

    def fetch_content_from_search_item(self, item: Dict[str, Any]) -> Optional[str]:
        """
        Fetch file content from a GitHub search result item using a fallback strategy:
        1. Contents API (most reliable) - uses item['url']
        2. HTML URL conversion - converts github.com URL to raw.githubusercontent.com
        3. Default branch fallback - constructs raw URL using default_branch
        
        This avoids using blob SHA in raw URLs which causes 404 errors.
        """
        repository = item.get("repository") or {}
        repo_name = repository.get("full_name", "")
        path = item.get("path")
        repo_label = repo_name or "unknown-repo"
        path_label = path or "unknown-path"
        
        self.logger.debug(
            "Attempting to fetch content for %s/%s",
            repo_label,
            path_label,
        )

        if not path:
            self.logger.debug("Skipping search item without path for repository '%s'", repo_label)
            return None

        # Strategy 1: Contents API (most reliable)
        api_url = item.get("url")
        if api_url:
            payload = self._request("GET", api_url)
            if isinstance(payload, dict):
                content_blob = payload.get("content")
                encoding = (payload.get("encoding") or "").lower()
                if content_blob:
                    if encoding == "base64" or not encoding:
                        decoded = self._decode_base64_content(content_blob)
                        if decoded is not None:
                            self.logger.debug(
                                "Retrieved content via Contents API for %s/%s (size=%d)",
                                repo_label,
                                path_label,
                                len(decoded),
                            )
                            return decoded
                download_url = payload.get("download_url")
                if download_url:
                    content = self.fetch_file_content(download_url)
                    if content is not None:
                        self.logger.debug(
                            "Retrieved content via Contents API download_url for %s/%s (size=%d)",
                            repo_label,
                            path_label,
                            len(content),
                        )
                        return content
            
            self.logger.debug("Contents API failed for %s/%s", repo_label, path_label)

        # Strategy 2: Convert HTML URL to raw URL
        html_url = item.get("html_url")
        if html_url:
            raw_url = self._raw_url_from_html(html_url)
            if raw_url:
                content = self.fetch_file_content(raw_url)
                if content is not None:
                    self.logger.debug(
                        "Retrieved content via HTML->raw conversion for %s/%s (size=%d)",
                        repo_label,
                        path_label,
                        len(content),
                    )
                    return content
            self.logger.debug("HTML->raw conversion failed for %s/%s", repo_label, path_label)

        # Strategy 3: Use default_branch as fallback (NOT blob SHA)
        default_branch = repository.get("default_branch")
        if repo_name and default_branch:
            raw_url = f"https://raw.githubusercontent.com/{repo_name}/{default_branch}/{path}"
            content = self.fetch_file_content(raw_url)
            if content is not None:
                self.logger.debug(
                    "Retrieved content via default_branch for %s/%s (size=%d)",
                    repo_label,
                    path_label,
                    len(content),
                )
                return content
            self.logger.debug("Default branch fallback failed for %s/%s", repo_label, path_label)

        # Last resort: try download_url if present
        download_url = item.get("download_url")
        if download_url:
            content = self.fetch_file_content(download_url)
            if content is not None:
                self.logger.debug(
                    "Retrieved content via download_url for %s/%s (size=%d)",
                    repo_label,
                    path_label,
                    len(content),
                )
                return content

        self.logger.debug(
            "Failed to retrieve content for %s/%s using all strategies",
            repo_label,
            path_label,
        )
        return None
