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

    def _effective_rpm(self) -> float:
        if self.rpm <= 0:
            return 0.0
        if self.rpm >= 8:
            return self.random.uniform(8.0, 12.0)
        return float(self.rpm)

    def _base_interval(self) -> float:
        effective_rpm = self._effective_rpm()
        if effective_rpm <= 0:
            return max(0.0, self.min_interval)
        interval = 60.0 / effective_rpm
        interval = max(self.min_interval, interval)
        interval = min(self.max_interval, interval)
        return interval

    def _next_interval(self) -> float:
        base = self._base_interval()
        if base <= 0.0:
            return max(0.0, min(self.min_interval, self.max_interval))
        jitter_ratio = self.random.uniform(0.05, 0.15)
        interval = base * (1.0 + jitter_ratio)
        return min(self.max_interval, max(self.min_interval, interval))

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
        requests_per_minute: int = 10,
        min_request_interval: float = 5.0,
        max_request_interval: float = 12.0,
        secondary_rate_limit_cooldown: float = 300.0,
        initial_backoff: float = 60.0,
        max_backoff: float = 3600.0,
        backoff_jitter_ratio: float = 0.1,
        negative_cache_ttl: float = 24 * 3600,
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
        self.negative_cache_ttl = max(0.0, negative_cache_ttl)
        self._negative_cache: Dict[str, float] = {}
        self._metrics_lock = threading.RLock()
        self._metrics: Dict[str, float] = {
            "requests": 0,
            "http_404": 0,
            "secondary_rate_limit_hits": 0,
            "secondary_rate_limit_cooldown_seconds": 0.0,
            "cache_hits": 0,
            "negative_cache_hits": 0,
        }

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

    def _record_request_status(self, status_code: int) -> None:
        with self._metrics_lock:
            self._metrics["requests"] += 1
            if status_code == 404:
                self._metrics["http_404"] += 1

    def _record_rate_limit_cooldown(self, wait_for: float) -> None:
        with self._metrics_lock:
            self._metrics["secondary_rate_limit_hits"] += 1
            self._metrics["secondary_rate_limit_cooldown_seconds"] += max(0.0, wait_for)

    def get_metrics(self) -> Dict[str, Any]:
        with self._metrics_lock:
            return dict(self._metrics)

    def _maybe_cache_response(self, cache_key: str, response: Response, payload: Any) -> None:
        etag = response.headers.get("ETag")
        last_modified = response.headers.get("Last-Modified")
        if etag or last_modified:
            with self.lock:
                cached_entry = self.etag_cache.setdefault(cache_key, {})
                if etag:
                    cached_entry["etag"] = etag
                if last_modified:
                    cached_entry["last_modified"] = last_modified
                cached_entry["payload"] = payload
                cached_entry["cached_at"] = time.time()

    def _cached_payload(self, cache_key: str) -> Optional[Any]:
        with self.lock:
            cached = self.etag_cache.get(cache_key)
            if cached:
                payload = cached.get("payload")
                if payload is not None:
                    with self._metrics_lock:
                        self._metrics["cache_hits"] = self._metrics.get("cache_hits", 0) + 1
                return payload
        return None

    def _build_headers(
        self,
        cache_key: str,
    ) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        with self.lock:
            cached = self.etag_cache.get(cache_key)
            if cached:
                etag = cached.get("etag")
                last_modified = cached.get("last_modified")
                if etag:
                    headers["If-None-Match"] = etag
                if last_modified:
                    headers["If-Modified-Since"] = last_modified
        return headers

    def _negative_cache_key(self, repository: str, path: str, ref: Optional[str]) -> Optional[str]:
        if not repository or not path:
            return None
        normalized_ref = ref or ""
        return f"{repository}:{path}:{normalized_ref}"

    def _is_negatively_cached(self, repository: str, path: str, ref: Optional[str]) -> bool:
        key = self._negative_cache_key(repository, path, ref)
        if not key:
            return False
        with self.lock:
            expiry = self._negative_cache.get(key)
            if expiry is None:
                return False
            if expiry < time.time():
                self._negative_cache.pop(key, None)
                return False
            with self._metrics_lock:
                self._metrics["negative_cache_hits"] = self._metrics.get("negative_cache_hits", 0) + 1
            return True

    def _mark_negative_cache(self, repository: str, path: str, ref: Optional[str]) -> None:
        key = self._negative_cache_key(repository, path, ref)
        if not key or self.negative_cache_ttl <= 0:
            return
        with self.lock:
            self._negative_cache[key] = time.time() + self.negative_cache_ttl

    def _clear_negative_cache(self, repository: str, path: str, ref: Optional[str]) -> None:
        key = self._negative_cache_key(repository, path, ref)
        if not key:
            return
        with self.lock:
            self._negative_cache.pop(key, None)

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
            self._record_rate_limit_cooldown(wait_for)
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
                self._record_rate_limit_cooldown(wait_for)
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
                self._record_rate_limit_cooldown(wait_for)
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
        accept: Optional[str] = None,
        return_status: bool = False,
    ) -> Any:
        cache_key = self._cache_key(url, params)
        for attempt in range(self.max_retries + 1):
            # Apply rate limiting before making the request
            self._respect_rate_limits()
            self.rate_limiter.wait_if_needed()
            
            headers = self._build_headers(cache_key)
            request_headers = dict(self.session.headers)
            request_headers.update(headers)
            effective_accept = accept
            if effective_accept is None:
                effective_accept = "application/vnd.github.v3.raw" if raw else "application/vnd.github+json"
            request_headers["Accept"] = effective_accept
            
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

            self._record_request_status(response.status_code)
            self._update_rate_info(response)

            if response.status_code == 304:
                cached_payload = self._cached_payload(cache_key)
                if cached_payload is not None:
                    self.logger.debug("Cache hit for %s", cache_key)
                    return (cached_payload, 304) if return_status else cached_payload
                self.logger.debug("Received 304 but cache empty for %s", cache_key)
                return (None, 304) if return_status else None

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
                return (None, response.status_code) if return_status else None

            payload: Any
            if raw:
                payload = response.text
            else:
                try:
                    payload = response.json()
                except json.JSONDecodeError:
                    self.logger.error("Failed to decode JSON for %s", url)
                    return (None, response.status_code) if return_status else None

            self._maybe_cache_response(cache_key, response, payload)
            return (payload, response.status_code) if return_status else payload
        
        self.logger.error("Exceeded retry attempts (%d) for %s", self.max_retries, url)
        return (None, None) if return_status else None

    def search_code(
        self,
        query: str,
        *,
        per_page: int,
        max_pages: int,
        sort: Optional[str] = None,
        order: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        endpoint = f"{self.base_url}/search/code"
        aggregated_items: List[Dict[str, Any]] = []
        had_response = False
        fallback_to_default = False
        page = 1
        while page <= max_pages:
            params: Dict[str, Any] = {
                "q": query,
                "per_page": per_page,
                "page": page,
            }
            if not fallback_to_default and sort:
                params["sort"] = sort
            if not fallback_to_default and order:
                params["order"] = order

            payload, status = self._request(
                "GET",
                endpoint,
                params=params,
                accept="application/vnd.github.v3.text-match+json",
                return_status=True,
            )
            if payload is None:
                if status == 422 and not fallback_to_default and (sort or order):
                    self.logger.debug(
                        "Search API rejected sort=%s order=%s; retrying with default ordering",
                        sort,
                        order,
                    )
                    fallback_to_default = True
                    continue
                break

            had_response = True
            items = payload.get("items", [])
            aggregated_items.extend(items)
            incomplete = payload.get("incomplete_results", False)
            self.logger.info(
                "Fetched %d items for query '%s' (page %d%s)",
                len(items),
                query,
                page,
                " fallback" if fallback_to_default else "",
            )
            if not items or incomplete:
                break
            page += 1
        if not had_response:
            return None
        return {"items": aggregated_items, "incomplete_results": payload.get("incomplete_results", False)}

    def fetch_file_content(self, download_url: str, *, return_status: bool = False) -> Any:
        return self._request("GET", download_url, raw=True, return_status=return_status)

    def fetch_content_from_search_item(self, item: Dict[str, Any]) -> Optional[str]:
        """Fetch file content with layered fallbacks and negative caching."""
        repository = item.get("repository") or {}
        repo_name = repository.get("full_name", "")
        path = item.get("path")
        repo_label = repo_name or "unknown-repo"
        path_label = path or "unknown-path"
        default_branch = repository.get("default_branch")
        blob_sha = item.get("sha")

        self.logger.debug("Attempting to fetch content for %s/%s", repo_label, path_label)

        if not path:
            self.logger.debug("Skipping search item without path for repository '%s'", repo_label)
            return None

        candidate_refs = [ref for ref in (blob_sha, default_branch) if ref]
        if candidate_refs and all(self._is_negatively_cached(repo_name, path, ref) for ref in candidate_refs):
            self.logger.debug(
                "Skipping %s/%s because all refs are present in negative cache",
                repo_label,
                path_label,
            )
            return None

        def _on_success(ref: Optional[str]) -> None:
            for candidate in {ref, blob_sha, default_branch}:
                if candidate:
                    self._clear_negative_cache(repo_name, path, candidate)

        def _on_not_found(ref: Optional[str]) -> None:
            marker = ref or blob_sha or default_branch
            if marker:
                self._mark_negative_cache(repo_name, path, marker)

        # Strategy 1: Contents API (most reliable)
        api_url = item.get("url")
        if api_url and not (blob_sha and self._is_negatively_cached(repo_name, path, blob_sha)):
            payload, status = self._request("GET", api_url, return_status=True)
            if isinstance(payload, dict):
                content_blob = payload.get("content")
                encoding = (payload.get("encoding") or "").lower()
                if content_blob:
                    if encoding in {"", "base64"}:
                        decoded = self._decode_base64_content(content_blob)
                        if decoded is not None:
                            self.logger.debug(
                                "Retrieved content via Contents API for %s/%s (size=%d)",
                                repo_label,
                                path_label,
                                len(decoded),
                            )
                            _on_success(blob_sha)
                            return decoded
                download_url = payload.get("download_url")
                if download_url:
                    text, download_status = self.fetch_file_content(download_url, return_status=True)
                    if text is not None:
                        self.logger.debug(
                            "Retrieved content via Contents API download_url for %s/%s (size=%d)",
                            repo_label,
                            path_label,
                            len(text),
                        )
                        _on_success(blob_sha or default_branch)
                        return text
                    if download_status == 404:
                        _on_not_found(default_branch or blob_sha)
            if status == 404:
                _on_not_found(blob_sha)
            else:
                self.logger.debug("Contents API yielded status %s for %s/%s", status, repo_label, path_label)

        # Strategy 2: Convert HTML URL to raw URL
        html_url = item.get("html_url")
        if html_url and not (default_branch and self._is_negatively_cached(repo_name, path, default_branch)):
            raw_url = self._raw_url_from_html(html_url)
            if raw_url:
                text, raw_status = self.fetch_file_content(raw_url, return_status=True)
                if text is not None:
                    self.logger.debug(
                        "Retrieved content via HTML->raw conversion for %s/%s (size=%d)",
                        repo_label,
                        path_label,
                        len(text),
                    )
                    _on_success(default_branch)
                    return text
                if raw_status == 404:
                    _on_not_found(default_branch)
            self.logger.debug("HTML->raw conversion failed for %s/%s", repo_label, path_label)

        # Strategy 3: Use default_branch as fallback (NOT blob SHA)
        if repo_name and default_branch and not self._is_negatively_cached(repo_name, path, default_branch):
            raw_url = f"https://raw.githubusercontent.com/{repo_name}/{default_branch}/{path}"
            text, raw_status = self.fetch_file_content(raw_url, return_status=True)
            if text is not None:
                self.logger.debug(
                    "Retrieved content via default_branch for %s/%s (size=%d)",
                    repo_label,
                    path_label,
                    len(text),
                )
                _on_success(default_branch)
                return text
            if raw_status == 404:
                _on_not_found(default_branch)
            self.logger.debug("Default branch fallback failed for %s/%s with status %s", repo_label, path_label, raw_status)

        # Last resort: try download_url if present
        download_url = item.get("download_url")
        if download_url:
            text, download_status = self.fetch_file_content(download_url, return_status=True)
            if text is not None:
                self.logger.debug(
                    "Retrieved content via download_url for %s/%s (size=%d)",
                    repo_label,
                    path_label,
                    len(text),
                )
                _on_success(default_branch or blob_sha)
                return text
            if download_status == 404:
                _on_not_found(default_branch or blob_sha)

        self.logger.debug(
            "Failed to retrieve content for %s/%s using all strategies",
            repo_label,
            path_label,
        )
        return None
