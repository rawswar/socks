from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests
from requests import Response
from requests.exceptions import RequestException

from .models import GitHubRateInfo


class GitHubClient:
    def __init__(
        self,
        base_url: str,
        token: Optional[str],
        *,
        request_timeout: int,
        max_retries: int,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.timeout = request_timeout
        self.max_retries = max_retries
        self.logger = logging.getLogger(__name__ + ".GitHubClient")
        self.lock = threading.RLock()
        self.rate_info = GitHubRateInfo(remaining=5000, reset_timestamp=time.time())
        self.cooldown_until = 0.0
        self.etag_cache: Dict[str, Dict[str, Any]] = {}

        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "socks5-proxy-collector",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self.session.headers.update(headers)

    def _cache_key(self, url: str, params: Optional[Dict[str, Any]]) -> str:
        if not params:
            return url
        return f"{url}?{urlencode(sorted(params.items()))}"

    def _respect_rate_limits(self) -> None:
        with self.lock:
            now = time.time()
            wait_until = max(self.cooldown_until, self.rate_info.reset_timestamp if self.rate_info.remaining <= 0 else 0.0)
        if wait_until and wait_until > now:
            sleep_for = wait_until - now
            self.logger.info("Rate limit reached, sleeping for %.2fs", sleep_for)
            time.sleep(max(0.0, sleep_for))

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

    def _handle_rate_limit_response(self, response: Response) -> bool:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                wait_for = float(retry_after)
                with self.lock:
                    self.cooldown_until = max(self.cooldown_until, time.time() + wait_for)
                self.logger.warning("GitHub requested retry after %.2fs", wait_for)
                return True
            except ValueError:
                pass
        reset = response.headers.get("X-RateLimit-Reset")
        if reset:
            try:
                reset_ts = float(reset)
                with self.lock:
                    self.cooldown_until = max(self.cooldown_until, reset_ts)
                wait_for = max(0.0, reset_ts - time.time())
                self.logger.warning("GitHub rate limit hit; waiting %.2fs", wait_for)
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
        backoff = 1.0
        for attempt in range(self.max_retries + 1):
            self._respect_rate_limits()
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
                self.logger.warning("GitHub request error (%s): %s", url, exc)
                time.sleep(min(backoff, 30.0))
                backoff *= 2
                continue

            self._update_rate_info(response)

            if response.status_code == 304:
                cached_payload = self._cached_payload(cache_key)
                if cached_payload is not None:
                    self.logger.debug("Cache hit for %s", cache_key)
                    return cached_payload
                self.logger.debug("Received 304 but cache empty for %s", cache_key)
                return None

            if response.status_code == 403 and self._handle_rate_limit_response(response):
                continue

            if 500 <= response.status_code < 600:
                self.logger.warning(
                    "GitHub server error %s for %s (attempt %d)",
                    response.status_code,
                    url,
                    attempt + 1,
                )
                time.sleep(min(backoff, 30.0))
                backoff *= 2
                continue

            if response.status_code >= 400:
                self.logger.error(
                    "GitHub request failed (%s) with status %d: %s",
                    url,
                    response.status_code,
                    response.text,
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
        self.logger.error("Exceeded retry attempts for %s", url)
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
