from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from requests.exceptions import RequestException

from .models import ActiveProxy


class ProxyValidator:
    def __init__(
        self,
        *,
        max_workers: int,
        timeout: int,
        test_url: str,
        expected_ip_key: str,
    ) -> None:
        self.max_workers = max_workers
        self.timeout = timeout
        self.test_url = test_url
        self.expected_ip_key = expected_ip_key
        self.logger = logging.getLogger(__name__ + ".ProxyValidator")
        self._reference_ip: Optional[str] = None
        self._reference_lock = threading.Lock()

    def _fetch_reference_ip(self) -> Optional[str]:
        with self._reference_lock:
            if self._reference_ip is not None:
                return self._reference_ip
            try:
                response = requests.get(self.test_url, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                reference_ip = data.get(self.expected_ip_key)
                if isinstance(reference_ip, str):
                    self._reference_ip = reference_ip
                    self.logger.info("Detected reference IP %s", reference_ip)
                    return reference_ip
            except (RequestException, ValueError, json.JSONDecodeError) as exc:
                self.logger.warning("Unable to determine reference IP: %s", exc)
            return None

    def _validate_single(self, candidate: Tuple[str, int]) -> Optional[ActiveProxy]:
        host, port = candidate
        proxies = {
            "http": f"socks5h://{host}:{port}",
            "https": f"socks5h://{host}:{port}",
        }
        start = time.perf_counter()
        try:
            response = requests.get(
                self.test_url,
                proxies=proxies,
                timeout=self.timeout,
            )
            response.raise_for_status()
            latency = time.perf_counter() - start
            payload = response.json()
            observed_ip = payload.get(self.expected_ip_key)
            if not isinstance(observed_ip, str) or not observed_ip:
                self.logger.debug("Proxy %s:%s missing observed IP", host, port)
                return None
            reference_ip = self._fetch_reference_ip()
            if reference_ip and observed_ip == reference_ip:
                self.logger.debug(
                    "Proxy %s:%s is not anonymous (observed IP %s)",
                    host,
                    port,
                    observed_ip,
                )
                return None
            proxy = ActiveProxy(host=host, port=port, latency=latency)
            self.logger.debug("Proxy %s:%s validated with latency %.3fs", host, port, latency)
            return proxy
        except (RequestException, ValueError, json.JSONDecodeError) as exc:
            self.logger.debug("Proxy %s:%s validation error: %s", host, port, exc)
            return None

    def validate(self, candidates: Iterable[Tuple[str, int]]) -> List[ActiveProxy]:
        candidates_list = list(candidates)
        if not candidates_list:
            return []
        results: List[ActiveProxy] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_proxy: Dict[Future[Optional[ActiveProxy]], Tuple[str, int]] = {}
            for candidate in candidates_list:
                future = executor.submit(self._validate_single, candidate)
                future_to_proxy[future] = candidate
            for future in as_completed(future_to_proxy):
                proxy = future_to_proxy[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as exc:
                    self.logger.error("Validator task failed for %s:%s (%s)", proxy[0], proxy[1], exc)
        results.sort(key=lambda proxy: proxy.latency)
        return results
