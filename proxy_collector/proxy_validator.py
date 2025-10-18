from __future__ import annotations

import json
import logging
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

import requests
from requests.exceptions import (
    ConnectTimeout,
    HTTPError,
    ProxyError,
    ReadTimeout,
    RequestException,
    SSLError,
)

from .models import ActiveProxy


@dataclass(frozen=True)
class EndpointConfig:
    url: str
    expected_ip_key: str


@dataclass(frozen=True)
class ValidationAttempt:
    success: bool
    proxy: Optional[ActiveProxy] = None
    failure_reason: Optional[str] = None


class ValidationResult(Sequence[ActiveProxy]):
    def __init__(
        self,
        active: List[ActiveProxy],
        *,
        failures_by_reason: Dict[str, int],
        egress_ips: Dict[str, int],
        classified: Dict[str, List[ActiveProxy]],
        total_candidates: int,
    ) -> None:
        self._active = list(active)
        self.failures_by_reason = dict(
            sorted(failures_by_reason.items(), key=lambda item: (-item[1], item[0]))
        )
        self.egress_ips = dict(sorted(egress_ips.items(), key=lambda item: (-item[1], item[0])))
        self.classified = {
            name: list(sorted(proxies, key=lambda proxy: proxy.latency))
            for name, proxies in classified.items()
        }
        self.total_candidates = total_candidates

    def __iter__(self) -> Iterator[ActiveProxy]:
        return iter(self._active)

    def __len__(self) -> int:
        return len(self._active)

    def __getitem__(self, index: int) -> ActiveProxy:
        return self._active[index]

    @property
    def active(self) -> List[ActiveProxy]:
        return list(self._active)

    @property
    def success_count(self) -> int:
        return len(self._active)


class ProxyValidator:
    def __init__(
        self,
        *,
        max_workers: int,
        timeout: float,
        test_url: Optional[str] = None,
        expected_ip_key: Optional[str] = None,
        protocols: Optional[Sequence[str]] = None,
        endpoints: Optional[Sequence[Dict[str, str]]] = None,
        connect_timeout: Optional[float] = None,
        read_timeout: Optional[float] = None,
    ) -> None:
        self.max_workers = max(1, int(max_workers))
        self.timeout = float(timeout)
        self.connect_timeout = float(connect_timeout or self.timeout)
        self.read_timeout = float(read_timeout or self.timeout)
        if self.read_timeout < self.connect_timeout:
            self.read_timeout = self.connect_timeout
        if self.timeout < self.read_timeout:
            self.timeout = self.read_timeout
        self.logger = logging.getLogger(__name__ + ".ProxyValidator")
        self.protocols = self._normalise_protocols(protocols)
        self.endpoints = self._prepare_endpoints(endpoints, test_url, expected_ip_key)
        self._reference_ips: Dict[str, str] = {}
        self._reference_lock = threading.Lock()

    @staticmethod
    def _normalise_protocols(protocols: Optional[Sequence[str]]) -> List[Tuple[str, str]]:
        mapping = {
            "socks5": ("socks5", "socks5h"),
            "socks5h": ("socks5", "socks5h"),
            "socks4": ("socks4", "socks4a"),
            "socks4a": ("socks4", "socks4a"),
            "http": ("http", "http"),
            "https": ("https", "https"),
        }
        resolved: List[Tuple[str, str]] = []
        seen: Set[str] = set()
        values = protocols or ["socks5"]
        for value in values:
            candidate = (value or "").strip().lower()
            if not candidate:
                continue
            entry = mapping.get(candidate)
            if entry is None:
                continue
            label, scheme = entry
            if label in seen:
                continue
            seen.add(label)
            resolved.append((label, scheme))
        if not resolved:
            resolved.append(("socks5", "socks5h"))
        return resolved

    def _prepare_endpoints(
        self,
        endpoints: Optional[Sequence[Dict[str, str]]],
        test_url: Optional[str],
        expected_ip_key: Optional[str],
    ) -> List[EndpointConfig]:
        prepared: List[EndpointConfig] = []
        if endpoints:
            for entry in endpoints:
                if not isinstance(entry, dict):
                    continue
                url = entry.get("url")
                key = entry.get("expected_ip_key") or expected_ip_key
                if isinstance(url, str) and isinstance(key, str):
                    prepared.append(EndpointConfig(url=url, expected_ip_key=key))
        if not prepared and isinstance(test_url, str) and isinstance(expected_ip_key, str):
            prepared.append(EndpointConfig(url=test_url, expected_ip_key=expected_ip_key))
        if not prepared:
            prepared.append(
                EndpointConfig(url="https://api.ipify.org?format=json", expected_ip_key="ip")
            )
        unique: List[EndpointConfig] = []
        seen: Set[Tuple[str, str]] = set()
        for endpoint in prepared:
            key = (endpoint.url, endpoint.expected_ip_key)
            if key in seen:
                continue
            seen.add(key)
            unique.append(endpoint)
        return unique

    def _get_reference_ip(self, endpoint: EndpointConfig) -> Optional[str]:
        with self._reference_lock:
            cached = self._reference_ips.get(endpoint.url)
            if cached:
                return cached
        try:
            response = requests.get(
                endpoint.url,
                timeout=(self.connect_timeout, self.read_timeout),
            )
            response.raise_for_status()
            payload = response.json()
            reference_ip = payload.get(endpoint.expected_ip_key)
            if isinstance(reference_ip, str) and reference_ip:
                with self._reference_lock:
                    self._reference_ips[endpoint.url] = reference_ip
                return reference_ip
        except (RequestException, ValueError, json.JSONDecodeError) as exc:
            self.logger.debug("Unable to resolve reference IP for %s: %s", endpoint.url, exc)
        return None

    @staticmethod
    def _build_proxy_map(scheme: str, host: str, port: int) -> Optional[Dict[str, str]]:
        if not scheme:
            return None
        uri = f"{scheme}://{host}:{port}"
        return {"http": uri, "https": uri}

    def _classify(self, endpoint: EndpointConfig, observed_ip: str) -> str:
        reference = self._get_reference_ip(endpoint)
        if not reference:
            return "unknown"
        if observed_ip == reference:
            return "transparent"
        return "anonymous"

    def _attempt_endpoint(
        self,
        host: str,
        port: int,
        protocol_label: str,
        endpoint: EndpointConfig,
        proxies: Dict[str, str],
    ) -> ValidationAttempt:
        start = time.perf_counter()
        try:
            response = requests.get(
                endpoint.url,
                proxies=proxies,
                timeout=(self.connect_timeout, self.read_timeout),
            )
            response.raise_for_status()
        except ConnectTimeout:
            return ValidationAttempt(False, failure_reason="connect_timeout")
        except ReadTimeout:
            return ValidationAttempt(False, failure_reason="read_timeout")
        except ProxyError:
            return ValidationAttempt(False, failure_reason="proxy_error")
        except SSLError:
            return ValidationAttempt(False, failure_reason="ssl_error")
        except HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else "unknown"
            return ValidationAttempt(False, failure_reason=f"http_{status_code}")
        except RequestException:
            return ValidationAttempt(False, failure_reason="request_error")
        latency = time.perf_counter() - start
        try:
            payload = response.json()
        except (ValueError, json.JSONDecodeError):
            return ValidationAttempt(False, failure_reason="invalid_json")
        observed_ip = payload.get(endpoint.expected_ip_key)
        if not isinstance(observed_ip, str) or not observed_ip:
            return ValidationAttempt(False, failure_reason="missing_egress_ip")
        classification = self._classify(endpoint, observed_ip)
        proxy = ActiveProxy(
            host=host,
            port=port,
            latency=latency,
            protocol=protocol_label,
            egress_ip=observed_ip,
            endpoint=endpoint.url,
            classification=classification,
        )
        return ValidationAttempt(True, proxy=proxy)

    def _validate_candidate(
        self,
        candidate: Tuple[str, int],
        shutdown_event: Optional[threading.Event],
    ) -> ValidationAttempt:
        host, port = candidate
        reason_counter: Counter[str] = Counter()
        for protocol_label, scheme in self.protocols:
            if shutdown_event and shutdown_event.is_set():
                return ValidationAttempt(False, failure_reason="shutdown")
            proxies = self._build_proxy_map(scheme, host, port)
            if proxies is None:
                reason_counter["unsupported_protocol"] += 1
                continue
            for endpoint in self.endpoints:
                if shutdown_event and shutdown_event.is_set():
                    return ValidationAttempt(False, failure_reason="shutdown")
                attempt = self._attempt_endpoint(host, port, protocol_label, endpoint, proxies)
                if attempt.success:
                    return attempt
                if attempt.failure_reason:
                    reason_counter[attempt.failure_reason] += 1
        if reason_counter:
            failure_reason = reason_counter.most_common(1)[0][0]
        else:
            failure_reason = "unknown"
        return ValidationAttempt(False, failure_reason=failure_reason)

    def _record_attempt(
        self,
        attempt: ValidationAttempt,
        results: List[ActiveProxy],
        failures: Counter[str],
        egress_counter: Counter[str],
        classified_map: Dict[str, List[ActiveProxy]],
        progress_callback: Optional[Callable[[List[ActiveProxy]], None]],
    ) -> None:
        if attempt.success and attempt.proxy:
            results.append(attempt.proxy)
            egress_key = attempt.proxy.egress_ip or "unknown"
            egress_counter[egress_key] += 1
            classification_key = attempt.proxy.classification or "unclassified"
            classified_map[classification_key].append(attempt.proxy)
            if progress_callback:
                snapshot = sorted(results, key=lambda proxy: proxy.latency)
                progress_callback(snapshot)
            return
        failures[attempt.failure_reason or "unknown"] += 1

    def validate(
        self,
        candidates: Iterable[Tuple[str, int]],
        shutdown_event: Optional[threading.Event] = None,
        progress_callback: Optional[Callable[[List[ActiveProxy]], None]] = None,
    ) -> ValidationResult:
        unique_candidates = list(dict.fromkeys(candidates))
        total_candidates = len(unique_candidates)
        if total_candidates == 0:
            if progress_callback:
                progress_callback([])
            return ValidationResult(
                [],
                failures_by_reason={},
                egress_ips={},
                classified={},
                total_candidates=0,
            )

        results: List[ActiveProxy] = []
        failures: Counter[str] = Counter()
        egress_counter: Counter[str] = Counter()
        classified_map: Dict[str, List[ActiveProxy]] = defaultdict(list)

        iterator = iter(unique_candidates)
        pending: Dict[Future[ValidationAttempt], Tuple[str, int]] = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            def _submit_next() -> None:
                if shutdown_event and shutdown_event.is_set():
                    return
                try:
                    candidate = next(iterator)
                except StopIteration:
                    return
                future = executor.submit(self._validate_candidate, candidate, shutdown_event)
                pending[future] = candidate

            for _ in range(min(self.max_workers, total_candidates)):
                _submit_next()

            while pending:
                future = next(as_completed(list(pending)))
                candidate = pending.pop(future)
                try:
                    attempt = future.result()
                except Exception as exc:  # pylint: disable=broad-except
                    host, port = candidate
                    self.logger.error(
                        "Validator task failed for %s:%s (%s)",
                        host,
                        port,
                        exc,
                    )
                    attempt = ValidationAttempt(False, failure_reason="executor_error")
                self._record_attempt(
                    attempt,
                    results,
                    failures,
                    egress_counter,
                    classified_map,
                    progress_callback,
                )
                _submit_next()

        results.sort(key=lambda proxy: proxy.latency)
        if progress_callback:
            progress_callback(list(results))

        classified = {name: proxies for name, proxies in classified_map.items() if proxies}
        # Remove placeholder classification if it is the only bucket with empty metadata
        if "unclassified" in classified and all(
            proxy.classification for proxy in classified["unclassified"]
        ):
            classified.pop("unclassified", None)

        return ValidationResult(
            results,
            failures_by_reason=dict(failures),
            egress_ips=dict(egress_counter),
            classified=classified,
            total_candidates=total_candidates,
        )
