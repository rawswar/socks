from __future__ import annotations

import ipaddress
import json
import logging
import random
import re
import socket
import struct
import threading
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from concurrent.futures import Future, ThreadPoolExecutor, as_completed

import requests
from requests import Response
from requests.exceptions import (ConnectTimeout, HTTPError, ProxyError, ReadTimeout,
                                 RequestException, SSLError)

from .models import ProxyClassification, ProxyProtocol, ProxyValidationSummary

LOGGER_NAME = __name__ + ".ProxyValidator"
HTTP_CONNECT_PROBE = (
    b"CONNECT example.com:443 HTTP/1.1\r\n"
    b"Host: example.com:443\r\n"
    b"User-Agent: proxy-validator/1.0\r\n"
    b"Proxy-Connection: Keep-Alive\r\n\r\n"
)
SOCKS4_TARGET_IP = socket.inet_aton("1.1.1.1")
SOCKS4_TARGET_PORT = 80
IPV4_PATTERN = re.compile(r"(?<!\d)(?:\d{1,3}\.){3}\d{1,3}(?!\d)")
BOUNDED_QUEUE_MULTIPLIER = 4
USER_AGENT = "proxy-validator/1.0"


@dataclass(frozen=True)
class ProtocolDetectionResult:
    protocol: ProxyProtocol
    requires_auth: bool
    status: str
    latency: Optional[float]
    error: Optional[str]


@dataclass(frozen=True)
class EndpointVerificationResult:
    category: str
    latency: Optional[float]
    endpoint: Optional[str]
    egress_ip: Optional[str]
    error: Optional[str]


class _QueueFullError(RuntimeError):
    pass


class _BoundedExecutor:
    def __init__(self, max_workers: int, queue_capacity: int) -> None:
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._semaphore = threading.Semaphore(queue_capacity)

    def submit(self, fn, *args, **kwargs) -> Future:
        if not self._semaphore.acquire(blocking=False):
            raise _QueueFullError("validation queue is saturated")

        def _wrapped(*wrapped_args, **wrapped_kwargs):
            try:
                return fn(*wrapped_args, **wrapped_kwargs)
            finally:
                self._semaphore.release()

        return self._executor.submit(_wrapped, *args, **kwargs)

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)


class ProxyValidator:
    def __init__(
        self,
        *,
        max_workers: int,
        connect_timeout: float,
        total_timeout: float,
        endpoints: Sequence[str],
        max_endpoints: int,
        endpoint_attempts: int,
        allow_authenticated: bool,
    ) -> None:
        self.max_workers = max(1, max_workers)
        self.connect_timeout = min(max(connect_timeout, 2.0), 5.0)
        self.total_timeout = max(total_timeout, self.connect_timeout)
        self.allow_authenticated = allow_authenticated

        sanitized_endpoints = [endpoint.strip() for endpoint in endpoints if endpoint and endpoint.strip()]
        if not sanitized_endpoints:
            raise ValueError("Validator endpoints cannot be empty")
        self.endpoints: List[str] = sanitized_endpoints
        self.max_endpoints = max(1, min(max_endpoints, len(self.endpoints)))
        self.endpoint_attempts = max(1, endpoint_attempts)

        self.logger = logging.getLogger(LOGGER_NAME)
        self._reference_ip: Optional[str] = None
        self._reference_lock = threading.Lock()
        queue_capacity = max(self.max_workers, self.max_workers * BOUNDED_QUEUE_MULTIPLIER)
        self._queue_capacity = queue_capacity

    def validate(
        self,
        candidates: Iterable[Tuple[str, int]],
        shutdown_event: Optional[threading.Event] = None,
        progress_callback: Optional[Callable[[ProxyValidationSummary], None]] = None,
    ) -> ProxyValidationSummary:
        summary = ProxyValidationSummary()
        candidate_list = list(candidates)
        if not candidate_list:
            return summary

        executor = _BoundedExecutor(self.max_workers, self._queue_capacity)
        futures: Dict[Future[ProxyClassification], Tuple[str, int]] = {}
        try:
            for host, port in candidate_list:
                if shutdown_event and shutdown_event.is_set():
                    self.logger.info(
                        "Shutdown requested; stopped scheduling remaining validation tasks"
                    )
                    break
                try:
                    future = executor.submit(self._validate_candidate, host, int(port))
                    futures[future] = (host, int(port))
                except _QueueFullError:
                    self.logger.debug("Validation queue full; dropping candidate %s:%s", host, port)

            for future in as_completed(futures):
                host, port = futures[future]
                try:
                    classification = future.result()
                except Exception as exc:  # pylint: disable=broad-except
                    self.logger.error("Validator task failed for %s:%s (%s)", host, port, exc)
                    continue
                summary.record(classification)
                if progress_callback:
                    progress_callback(summary.snapshot())
                if shutdown_event and shutdown_event.is_set():
                    self.logger.info("Shutdown requested; draining remaining validation results")
        finally:
            executor.shutdown(wait=True)

        if progress_callback:
            progress_callback(summary.snapshot())
        self._log_summary(summary)
        return summary

    def _validate_candidate(self, host: str, port: int) -> ProxyClassification:
        deadline = time.monotonic() + self.total_timeout
        try:
            detection = self._detect_protocol(host, port, deadline)

            latency = detection.latency
            endpoint: Optional[str] = None
            egress_ip: Optional[str] = None
            verified_at: Optional[str] = None
            category = self._category_from_detection(detection)
            error = detection.error
            if detection.status in {"requires_auth", "handshake_fail", "dial_timeout", "http_connect_only", "socks4"} and error is None:
                error = detection.status
            requires_auth = detection.requires_auth
            protocol_value = detection.protocol.value

            if detection.status == "requires_auth" and not self.allow_authenticated:
                self.logger.debug("Proxy %s:%s requires authentication; skipping verification", host, port)

            if detection.status == "socks5":
                verification = self._verify_endpoints(host, port, deadline)
                category = verification.category
                latency = verification.latency or latency
                endpoint = verification.endpoint
                egress_ip = verification.egress_ip
                error = verification.error or error
                if category == "validated":
                    verified_at = datetime.now(timezone.utc).isoformat()

            return ProxyClassification(
                host=host,
                port=port,
                protocol=protocol_value,
                requires_auth=requires_auth,
                category=category,
                latency=latency,
                endpoint=endpoint,
                egress_ip=egress_ip,
                verified_at=verified_at,
                error=error,
            )
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.debug("Unexpected validation error for %s:%s: %s", host, port, exc)
            return ProxyClassification(
                host=host,
                port=port,
                protocol=ProxyProtocol.UNKNOWN.value,
                requires_auth=False,
                category="other",
                latency=None,
                endpoint=None,
                egress_ip=None,
                verified_at=None,
                error=exc.__class__.__name__,
            )

    def _detect_protocol(self, host: str, port: int, deadline: float) -> ProtocolDetectionResult:
        socks5_result = self._attempt_socks5(host, port, deadline)
        if socks5_result.status in {"socks5", "requires_auth", "dial_timeout"}:
            return socks5_result
        fallback_error = socks5_result.error

        socks4_result = self._attempt_socks4(host, port, deadline)
        if socks4_result.status in {"socks4", "dial_timeout"}:
            return socks4_result
        if not fallback_error:
            fallback_error = socks4_result.error

        http_result = self._attempt_http_connect(host, port, deadline)
        if http_result.status in {"http_connect_only", "dial_timeout"}:
            return http_result

        return ProtocolDetectionResult(
            protocol=ProxyProtocol.UNKNOWN,
            requires_auth=False,
            status="handshake_fail",
            latency=socks5_result.latency,
            error=http_result.error or fallback_error or socks5_result.error,
        )

    def _attempt_socks5(self, host: str, port: int, deadline: float) -> ProtocolDetectionResult:
        try:
            with closing(self._open_socket(host, port, deadline)) as sock:
                start = time.perf_counter()
                sock.sendall(b"\x05\x01\x00")
                reply = sock.recv(2)
                handshake_latency = time.perf_counter() - start
                if len(reply) < 2:
                    return ProtocolDetectionResult(
                        protocol=ProxyProtocol.UNKNOWN,
                        requires_auth=False,
                        status="handshake_fail",
                        latency=handshake_latency,
                        error="short_reply",
                    )
                version, method = reply[0], reply[1]
                if version != 0x05:
                    return ProtocolDetectionResult(
                        protocol=ProxyProtocol.UNKNOWN,
                        requires_auth=False,
                        status="handshake_fail",
                        latency=handshake_latency,
                        error=f"unexpected_version_{version:02x}",
                    )
                if method == 0x00:
                    return ProtocolDetectionResult(
                        protocol=ProxyProtocol.SOCKS5,
                        requires_auth=False,
                        status="socks5",
                        latency=handshake_latency,
                        error=None,
                    )
                if method == 0x02:
                    return ProtocolDetectionResult(
                        protocol=ProxyProtocol.SOCKS5,
                        requires_auth=True,
                        status="requires_auth",
                        latency=handshake_latency,
                        error=None,
                    )
                return ProtocolDetectionResult(
                    protocol=ProxyProtocol.SOCKS5,
                    requires_auth=False,
                    status="handshake_fail",
                    latency=handshake_latency,
                    error=f"unsupported_method_{method:02x}",
                )
        except TimeoutError:
            return ProtocolDetectionResult(
                protocol=ProxyProtocol.UNKNOWN,
                requires_auth=False,
                status="dial_timeout",
                latency=None,
                error="connect_timeout",
            )
        except (OSError, socket.timeout) as exc:
            error_label = exc.__class__.__name__
            status = "dial_timeout" if isinstance(exc, socket.timeout) else "handshake_fail"
            return ProtocolDetectionResult(
                protocol=ProxyProtocol.UNKNOWN,
                requires_auth=False,
                status=status,
                latency=None,
                error=error_label,
            )

    def _attempt_socks4(self, host: str, port: int, deadline: float) -> ProtocolDetectionResult:
        try:
            with closing(self._open_socket(host, port, deadline)) as sock:
                start = time.perf_counter()
                payload = b"\x04\x01" + struct.pack(">H", SOCKS4_TARGET_PORT) + SOCKS4_TARGET_IP + b"\x00"
                sock.sendall(payload)
                reply = sock.recv(8)
                handshake_latency = time.perf_counter() - start
                if len(reply) < 2:
                    return ProtocolDetectionResult(
                        protocol=ProxyProtocol.UNKNOWN,
                        requires_auth=False,
                        status="handshake_fail",
                        latency=handshake_latency,
                        error="short_reply",
                    )
                vn, status_byte = reply[0], reply[1]
                if vn != 0x00:
                    return ProtocolDetectionResult(
                        protocol=ProxyProtocol.UNKNOWN,
                        requires_auth=False,
                        status="handshake_fail",
                        latency=handshake_latency,
                        error=f"unexpected_vn_{vn:02x}",
                    )
                if status_byte == 0x5A:
                    return ProtocolDetectionResult(
                        protocol=ProxyProtocol.SOCKS4,
                        requires_auth=False,
                        status="socks4",
                        latency=handshake_latency,
                        error=None,
                    )
                return ProtocolDetectionResult(
                    protocol=ProxyProtocol.SOCKS4,
                    requires_auth=False,
                    status="handshake_fail",
                    latency=handshake_latency,
                    error=f"status_{status_byte:02x}",
                )
        except TimeoutError:
            return ProtocolDetectionResult(
                protocol=ProxyProtocol.UNKNOWN,
                requires_auth=False,
                status="dial_timeout",
                latency=None,
                error="connect_timeout",
            )
        except (OSError, socket.timeout) as exc:
            error_label = exc.__class__.__name__
            status = "dial_timeout" if isinstance(exc, socket.timeout) else "handshake_fail"
            return ProtocolDetectionResult(
                protocol=ProxyProtocol.UNKNOWN,
                requires_auth=False,
                status=status,
                latency=None,
                error=error_label,
            )

    def _attempt_http_connect(self, host: str, port: int, deadline: float) -> ProtocolDetectionResult:
        try:
            with closing(self._open_socket(host, port, deadline)) as sock:
                start = time.perf_counter()
                sock.sendall(HTTP_CONNECT_PROBE)
                reply = sock.recv(64)
                handshake_latency = time.perf_counter() - start
                header = reply.decode("latin1", errors="ignore")
                if header.startswith("HTTP/1."):
                    status_line = header.splitlines()[0] if header else ""
                    return ProtocolDetectionResult(
                        protocol=ProxyProtocol.HTTP_CONNECT,
                        requires_auth=False,
                        status="http_connect_only",
                        latency=handshake_latency,
                        error=status_line or None,
                    )
                return ProtocolDetectionResult(
                    protocol=ProxyProtocol.UNKNOWN,
                    requires_auth=False,
                    status="handshake_fail",
                    latency=handshake_latency,
                    error="no_http_header",
                )
        except TimeoutError:
            return ProtocolDetectionResult(
                protocol=ProxyProtocol.UNKNOWN,
                requires_auth=False,
                status="dial_timeout",
                latency=None,
                error="connect_timeout",
            )
        except (OSError, socket.timeout) as exc:
            error_label = exc.__class__.__name__
            status = "dial_timeout" if isinstance(exc, socket.timeout) else "handshake_fail"
            return ProtocolDetectionResult(
                protocol=ProxyProtocol.UNKNOWN,
                requires_auth=False,
                status=status,
                latency=None,
                error=error_label,
            )

    def _verify_endpoints(self, host: str, port: int, deadline: float) -> EndpointVerificationResult:
        proxies = {
            "http": f"socks5h://{host}:{port}",
            "https": f"socks5h://{host}:{port}",
        }
        endpoints = self._select_endpoints()
        last_error = None

        for endpoint in endpoints:
            for _attempt in range(self.endpoint_attempts):
                remaining = self._deadline_remaining(deadline)
                if remaining <= 0:
                    return EndpointVerificationResult(
                        category="endpoint_blocked",
                        latency=None,
                        endpoint=None,
                        egress_ip=None,
                        error=last_error or "deadline_exceeded",
                    )
                timeout = self._request_timeout(deadline)
                headers = {"User-Agent": USER_AGENT}
                start = time.perf_counter()
                try:
                    response = requests.get(endpoint, proxies=proxies, timeout=timeout, headers=headers)
                    response.raise_for_status()
                    egress_ip = self._extract_ip_from_response(response)
                    if not egress_ip:
                        last_error = "no_egress_ip"
                        continue
                    if not self._is_public_ip(egress_ip):
                        last_error = "non_public_ip"
                        continue
                    reference_ip = self._fetch_reference_ip()
                    if reference_ip and reference_ip == egress_ip:
                        last_error = "reference_ip_match"
                        continue
                    latency = time.perf_counter() - start
                    return EndpointVerificationResult(
                        category="validated",
                        latency=latency,
                        endpoint=endpoint,
                        egress_ip=egress_ip,
                        error=None,
                    )
                except (ConnectTimeout, ReadTimeout):
                    last_error = "timeout"
                    continue
                except SSLError:
                    last_error = "tls_error"
                    continue
                except HTTPError as exc:
                    last_error = f"http_{exc.response.status_code if exc.response else 'error'}"
                    continue
                except ProxyError as exc:
                    last_error = exc.__class__.__name__
                    continue
                except RequestException as exc:
                    last_error = exc.__class__.__name__
                    continue
                except json.JSONDecodeError:
                    last_error = "invalid_json"
                    continue
        if last_error == "tls_error":
            return EndpointVerificationResult(
                category="tls_fail",
                latency=None,
                endpoint=None,
                egress_ip=None,
                error="tls_error",
            )
        return EndpointVerificationResult(
            category="endpoint_blocked",
            latency=None,
            endpoint=None,
            egress_ip=None,
            error=last_error or "endpoint_blocked",
        )

    def _select_endpoints(self) -> List[str]:
        if self.max_endpoints >= len(self.endpoints):
            shuffled = list(self.endpoints)
            random.shuffle(shuffled)
            return shuffled
        return random.sample(self.endpoints, self.max_endpoints)

    def _request_timeout(self, deadline: float) -> Tuple[float, float]:
        remaining = max(0.1, self._deadline_remaining(deadline))
        connect_timeout = min(self.connect_timeout, remaining)
        return (connect_timeout, remaining)

    def _deadline_remaining(self, deadline: float) -> float:
        return deadline - time.monotonic()

    def _open_socket(self, host: str, port: int, deadline: float) -> socket.socket:
        remaining = self._deadline_remaining(deadline)
        if remaining <= 0:
            raise TimeoutError("deadline exceeded before connect")
        timeout = min(self.connect_timeout, remaining)
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.settimeout(timeout)
        return sock

    def _extract_ip_from_response(self, response: Response) -> Optional[str]:
        try:
            data = response.json()
        except (ValueError, json.JSONDecodeError):
            data = None
        if isinstance(data, dict):
            for value in data.values():
                candidate = self._normalize_ip(value)
                if candidate and self._is_public_ip(candidate):
                    return candidate
        text_candidate = self._normalize_ip(response.text)
        if text_candidate and self._is_public_ip(text_candidate):
            return text_candidate
        return None

    def _normalize_ip(self, value: object) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, (list, dict)):
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            ip_obj = ipaddress.ip_address(text)
            return str(ip_obj)
        except ValueError:
            match = IPV4_PATTERN.search(text)
            if match:
                candidate = match.group(0)
                try:
                    ipaddress.ip_address(candidate)
                    return candidate
                except ValueError:
                    return None
        return None

    def _is_public_ip(self, value: str) -> bool:
        try:
            ip_obj = ipaddress.ip_address(value)
        except ValueError:
            return False
        return ip_obj.is_global

    def _fetch_reference_ip(self) -> Optional[str]:
        with self._reference_lock:
            if self._reference_ip is not None:
                return self._reference_ip
            headers = {"User-Agent": USER_AGENT}
            for endpoint in self.endpoints:
                try:
                    response = requests.get(endpoint, timeout=self.connect_timeout, headers=headers)
                    response.raise_for_status()
                    detected = self._extract_ip_from_response(response)
                    if detected:
                        if self._is_public_ip(detected):
                            self._reference_ip = detected
                            self.logger.info("Detected reference IP %s via %s", detected, endpoint)
                            return detected
                except RequestException as exc:
                    self.logger.debug("Reference IP lookup failed via %s: %s", endpoint, exc)
                except json.JSONDecodeError as exc:
                    self.logger.debug("Reference IP parse error via %s: %s", endpoint, exc)
            return None

    def _category_from_detection(self, detection: ProtocolDetectionResult) -> str:
        if detection.status == "socks5":
            return "handshake_ok"
        if detection.status == "requires_auth":
            return "requires_auth"
        if detection.status == "dial_timeout":
            return "dial_timeout"
        if detection.status == "http_connect_only":
            return "http_connect_only"
        return "handshake_fail"

    def _log_summary(self, summary: ProxyValidationSummary) -> None:
        success = summary.success_count
        self.logger.info("Validation completed | active=%d | candidates=%d", success, len(summary.classified))
        if summary.category_counts:
            ordered = [f"{category}={count}" for category, count in summary.category_counts.items()]
            self.logger.info("Validation categories: %s", ", ".join(ordered))
        if summary.protocol_counts:
            ordered = [f"{protocol}={count}" for protocol, count in summary.protocol_counts.items()]
            self.logger.info("Protocol detection: %s", ", ".join(ordered))
        if summary.failure_reasons:
            top_causes = summary.failure_reasons.most_common(5)
            formatted = ", ".join(f"{reason}={count}" for reason, count in top_causes)
            self.logger.info("Top failure reasons: %s", formatted)


__all__ = [
    "ProxyValidator",
]
