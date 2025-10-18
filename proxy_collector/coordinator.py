from __future__ import annotations

import contextlib
import ipaddress
import logging
import re
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from .config import load_config
from .github_client import GitHubClient
from .models import ActiveProxy, ProxyValidationSummary
from .proxy_validator import ProxyValidator
from .query_strategy import QueryStrategyGenerator
from .result_publisher import ResultPublisher

PLACEHOLDER_HOSTS = {"0.0.0.0", "1.2.3.4", "255.255.255.255"}

PROXY_PATTERNS = [
    re.compile(r"\b((?:\d{1,3}\.){3}\d{1,3}):(\d{2,5})\b"),
    re.compile(r"\b(?:https?|socks5?|socks4)://((?:\d{1,3}\.){3}\d{1,3}):(\d{2,5})\b", re.IGNORECASE),
    re.compile(r"\b(?:https?|socks5?|socks4)://[^:@\s]+:[^@]+@((?:\d{1,3}\.){3}\d{1,3}):(\d{2,5})", re.IGNORECASE),
    re.compile(r"\bip\s*[:=]\s*((?:\d{1,3}\.){3}\d{1,3})\s+(?:port|p)\s*[:=]\s*(\d{2,5})", re.IGNORECASE),
    re.compile(
        r"""["']ip["']\s*:\s*["']((?:\d{1,3}\.){3}\d{1,3})["']\s*,\s*["'](?:port|p)["']\s*:\s*["']?(\d{2,5})["']?""",
        re.IGNORECASE,
    ),
]


class Coordinator:
    def __init__(
        self,
        config_overrides: Optional[Dict[str, object]] = None,
    ) -> None:
        self.config = load_config(config_overrides)
        self._termination_reason: Optional[str] = None
        self.shutdown_event = threading.Event()
        self._latest_candidates_snapshot: Set[Tuple[str, int]] = set()
        self._partial_summary = ProxyValidationSummary()
        self._last_flush_timestamp: float = time.time()
        self._metrics: Dict[str, float] = {
            "queries_executed": 0,
            "files_attempted": 0,
            "files_successful": 0,
            "files_with_proxies": 0,
        }

        coordinator_cfg = self.config["coordinator"]
        self.max_runtime_seconds = coordinator_cfg.get("max_runtime_seconds")
        self.runtime_threshold_seconds = coordinator_cfg.get("runtime_shutdown_threshold_seconds", 60)
        self.flush_interval_seconds = coordinator_cfg.get("flush_interval_seconds", 300)
        self.max_files_per_query = coordinator_cfg.get("max_files_per_query", 20)

        self._configure_logging()
        github_cfg = self.config["github"]
        self.github_client = GitHubClient(
            base_url=github_cfg["base_url"],
            token=github_cfg.get("token"),
            request_timeout=github_cfg["request_timeout"],
            max_retries=github_cfg["max_retries"],
            requests_per_minute=github_cfg.get("requests_per_minute", 5),
            min_request_interval=github_cfg.get("min_request_interval_seconds", 6.0),
            max_request_interval=github_cfg.get("max_request_interval_seconds", 10.0),
            secondary_rate_limit_cooldown=github_cfg.get("secondary_rate_limit_cooldown", 300.0),
            initial_backoff=github_cfg.get("initial_backoff_seconds", 60.0),
            max_backoff=github_cfg.get("max_backoff_seconds", 3600.0),
            backoff_jitter_ratio=github_cfg.get("backoff_jitter_ratio", 0.1),
        )
        validator_cfg = self.config["validator"]
        self.validator = ProxyValidator(
            max_workers=validator_cfg["max_workers"],
            connect_timeout=validator_cfg["connect_timeout"],
            total_timeout=validator_cfg["total_timeout"],
            endpoints=validator_cfg["endpoints"],
            max_endpoints=validator_cfg.get("max_endpoints", 2),
            endpoint_attempts=validator_cfg.get("endpoint_attempts", 2),
            allow_authenticated=validator_cfg.get("allow_authenticated", False),
        )
        publisher_cfg = self.config["publisher"]
        self.publisher = ResultPublisher(
            output_dir=publisher_cfg["output_dir"],
            txt_filename=publisher_cfg["txt_filename"],
            active_json_filename=publisher_cfg["active_json_filename"],
            classified_json_filename=publisher_cfg["classified_json_filename"],
            zip_filename=publisher_cfg["zip_filename"],
            partial_txt_filename=publisher_cfg["partial_txt_filename"],
            partial_json_filename=publisher_cfg["partial_json_filename"],
        )
        self.query_generator = QueryStrategyGenerator()
        self.logger = logging.getLogger(__name__ + ".Coordinator")

    def _configure_logging(self) -> None:
        logging_cfg = self.config.get("logging", {})
        level_name = str(logging_cfg.get("level", "INFO")).upper()
        level = getattr(logging, level_name, logging.INFO)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

        root_logger = logging.getLogger()
        root_logger.setLevel(level)

        if not any(isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler) for handler in root_logger.handlers):
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(level)
            stream_handler.setFormatter(formatter)
            root_logger.addHandler(stream_handler)

        log_dir = Path(logging_cfg.get("directory", "logs"))
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / logging_cfg.get("filename", "app.log")
        max_bytes = int(logging_cfg.get("max_bytes", 2 * 1024 * 1024))
        backup_count = int(logging_cfg.get("backup_count", 5))

        has_file_handler = any(
            isinstance(handler, RotatingFileHandler) and Path(getattr(handler, "baseFilename", "")) == log_file
            for handler in root_logger.handlers
        )
        if not has_file_handler:
            file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

    def _extract_proxies_from_text(self, text: str) -> Set[Tuple[str, int]]:
        proxies: Set[Tuple[str, int]] = set()
        for pattern in PROXY_PATTERNS:
            for match in pattern.finditer(text):
                groups = match.groups()
                ip_value: Optional[str] = None
                port_value: Optional[str] = None
                for value in groups:
                    if value is None:
                        continue
                    normalized = value.strip().strip("\"'")
                    normalized = normalized.rstrip(",;")
                    if normalized and ip_value is None and self._is_valid_ip(normalized):
                        ip_value = normalized
                        continue
                    if normalized and port_value is None and self._is_valid_port(normalized):
                        port_value = normalized
                if ip_value and port_value:
                    if self._is_unlikely_socks_port(port_value, match.group(0)):
                        continue
                    proxies.add((ip_value, int(port_value)))
        return proxies

    @staticmethod
    def _is_valid_ip(host: str) -> bool:
        try:
            ip_obj = ipaddress.IPv4Address(host)
        except ipaddress.AddressValueError:
            return False
        if str(ip_obj) in PLACEHOLDER_HOSTS:
            return False
        return not (
            ip_obj.is_private
            or ip_obj.is_loopback
            or ip_obj.is_reserved
            or ip_obj.is_multicast
            or ip_obj.is_unspecified
            or ip_obj.is_link_local
        )

    @staticmethod
    def _is_valid_port(port: str) -> bool:
        try:
            value = int(port.strip("\"'"))
            return 1 <= value <= 65535
        except (ValueError, AttributeError):
            return False

    @staticmethod
    def _is_unlikely_socks_port(port: str, match_text: str) -> bool:
        try:
            port_value = int(port)
        except ValueError:
            return True
        if port_value in (80, 443):
            normalized = match_text.lower()
            if "socks" not in normalized:
                return True
        return False

    def _time_remaining(self, start_time: float) -> Optional[float]:
        if not self.max_runtime_seconds:
            return None
        return self.max_runtime_seconds - (time.time() - start_time)

    def _mark_shutdown(self, reason: str) -> None:
        if self.shutdown_event.is_set():
            return
        self.shutdown_event.set()
        self._termination_reason = reason
        if reason == "timeout":
            self.logger.info("Reached max runtime, publishing and exiting gracefully.")
        elif reason.startswith("signal:"):
            sig_name = reason.split(":", 1)[1]
            self.logger.info("Caught termination signal %s, publishing and exiting gracefully.", sig_name)
        self._maybe_flush_partial(self._latest_candidates_snapshot, self._partial_summary, force=True)

    def _should_stop_accepting_work(self, start_time: float) -> bool:
        if self.shutdown_event.is_set():
            return True
        remaining = self._time_remaining(start_time)
        if remaining is not None and remaining <= self.runtime_threshold_seconds:
            self.logger.warning(
                "Runtime budget nearly exhausted (%.1fs remaining); preparing graceful shutdown",
                max(0.0, remaining),
            )
            self._mark_shutdown("timeout")
            return True
        return False

    @contextlib.contextmanager
    def _signal_handler(self):
        handlers = []
        if threading.current_thread() is not threading.main_thread():
            yield
            return

        def _handler(signum, _frame) -> None:
            sig_name = signal.Signals(signum).name
            self.logger.warning("Caught termination signal %s", sig_name)
            self._mark_shutdown(f"signal:{sig_name}")

        for sig in (signal.SIGINT, signal.SIGTERM):
            previous = signal.getsignal(sig)
            handlers.append((sig, previous))
            signal.signal(sig, _handler)
        try:
            yield
        finally:
            for sig, previous in handlers:
                signal.signal(sig, previous)

    def _maybe_flush_partial(
        self,
        candidates: Iterable[Tuple[str, int]],
        summary: ProxyValidationSummary,
        *,
        force: bool = False,
    ) -> None:
        now = time.time()
        if not force:
            if self.flush_interval_seconds <= 0:
                force = True
            elif (now - self._last_flush_timestamp) < self.flush_interval_seconds and not self.shutdown_event.is_set():
                return
        try:
            self.publisher.write_partial_results(candidates, summary)
            self._last_flush_timestamp = now
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.debug("Unable to flush partial results: %s", exc)

    def _harvest_from_query(self, query: str, start_time: float) -> Set[Tuple[str, int]]:
        if self.shutdown_event.is_set():
            return set()
        github_cfg = self.config["github"]
        payload = self.github_client.search_code(
            query,
            per_page=github_cfg["per_page"],
            max_pages=github_cfg["max_pages"],
        )
        if payload is None:
            return set()

        candidates: Set[Tuple[str, int]] = set()
        items = payload.get("items", [])
        limited_items = items[: self.max_files_per_query]
        if len(items) > self.max_files_per_query:
            self.logger.debug(
                "Limiting processing to %d of %d items for query '%s'",
                self.max_files_per_query,
                len(items),
                query,
            )

        for item in limited_items:
            if self.shutdown_event.is_set():
                break
            repository = item.get("repository") or {}
            repo_name = repository.get("full_name", "unknown-repo")
            path = item.get("path", "unknown-path")
            self.logger.debug(
                "Processing file %s/%s for query '%s'",
                repo_name,
                path,
                query,
            )

            content = self.github_client.fetch_content_from_search_item(item)
            self._metrics["files_attempted"] += 1
            if content is None:
                self.logger.debug("No content retrieved for %s/%s", repo_name, path)
                continue
            self._metrics["files_successful"] += 1

            extracted = self._extract_proxies_from_text(content)
            if extracted:
                candidates.update(extracted)
                self._metrics["files_with_proxies"] += 1
                sample = [f"{host}:{port}" for host, port in list(extracted)[:3]]
                if sample:
                    self.logger.info(
                        "Extracted %d proxies from %s/%s (sample: %s)",
                        len(extracted),
                        repo_name,
                        path,
                        ", ".join(sample),
                    )
                else:
                    self.logger.info(
                        "Extracted %d proxies from %s/%s",
                        len(extracted),
                        repo_name,
                        path,
                    )

        return candidates

    def _collect_candidates(self, queries: Iterable[str], concurrency: int, start_time: float) -> Set[Tuple[str, int]]:
        aggregated: Set[Tuple[str, int]] = set()
        self._latest_candidates_snapshot = aggregated

        if concurrency <= 1:
            for query in queries:
                if self._should_stop_accepting_work(start_time):
                    self.logger.info("Stopping further query scheduling due to graceful shutdown trigger")
                    break
                self._metrics["queries_executed"] += 1
                result = self._harvest_from_query(query, start_time)
                aggregated.update(result)
                self._latest_candidates_snapshot = aggregated
                self.logger.info("Query '%s' produced %d unique proxies", query, len(result))
                self._maybe_flush_partial(aggregated, self._partial_summary)
            return aggregated

        query_iter = iter(queries)
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures_to_query = {}

            def _submit_next() -> None:
                if self._should_stop_accepting_work(start_time):
                    return
                try:
                    next_query = next(query_iter)
                except StopIteration:
                    return
                self._metrics["queries_executed"] += 1
                futures_to_query[executor.submit(self._harvest_from_query, next_query, start_time)] = next_query

            for _ in range(concurrency):
                _submit_next()

            for future in as_completed(futures_to_query):
                query = futures_to_query.pop(future)
                try:
                    result = future.result()
                    aggregated.update(result)
                    self._latest_candidates_snapshot = aggregated
                    self.logger.info("Query '%s' produced %d unique proxies", query, len(result))
                except Exception as exc:  # pylint: disable=broad-except
                    self.logger.error("Query task failed for '%s': %s", query, exc)
                self._maybe_flush_partial(aggregated, self._partial_summary)
                _submit_next()
        return aggregated

    def _log_kpis(
        self,
        elapsed_seconds: float,
        candidates: Set[Tuple[str, int]],
        summary: ProxyValidationSummary,
    ) -> None:
        github_metrics = self.github_client.get_metrics()
        message = (
            "Run KPIs | queries=%d | files_success=%d/%d | files_with_proxies=%d | candidates=%d | validated=%d | "
            "secondary_hits=%d | secondary_cooldown=%.1fs | timed_out=%s"
        )
        timed_out = self._termination_reason == "timeout"
        self.logger.info(
            message,
            int(self._metrics["queries_executed"]),
            int(self._metrics["files_successful"]),
            int(self._metrics["files_attempted"]),
            int(self._metrics["files_with_proxies"]),
            len(candidates),
            summary.success_count,
            github_metrics.get("secondary_rate_limit_hits", 0),
            github_metrics.get("secondary_rate_limit_cooldown_seconds", 0.0),
            "yes" if timed_out else "no",
        )
        self.logger.info(
            "Pipeline finished in %.2fs | %d candidates | %d active proxies | 404 responses=%d",
            elapsed_seconds,
            len(candidates),
            summary.success_count,
            github_metrics.get("http_404", 0),
        )

    def run(self) -> List[ActiveProxy]:
        start_time = time.time()
        self._last_flush_timestamp = start_time
        coordinator_cfg = self.config["coordinator"]
        max_queries = coordinator_cfg["max_queries"]
        search_concurrency = coordinator_cfg["search_concurrency"]

        queries = self.query_generator.generate(limit=max_queries)
        self.logger.info("Starting collection with %d queries", len(queries))

        validation_summary = ProxyValidationSummary()
        self._partial_summary = ProxyValidationSummary()
        candidates: Set[Tuple[str, int]] = set()

        with self._signal_handler():
            candidates = self._collect_candidates(queries, search_concurrency, start_time)
            self.logger.info("Harvested %d unique proxy candidates", len(candidates))
            self._latest_candidates_snapshot = candidates
            self._maybe_flush_partial(candidates, self._partial_summary, force=True)

            def _on_validator_progress(summary: ProxyValidationSummary) -> None:
                self._partial_summary = summary
                self._maybe_flush_partial(candidates, summary)

            validation_summary = self.validator.validate(
                candidates,
                shutdown_event=self.shutdown_event,
                progress_callback=_on_validator_progress,
            )
            self._partial_summary = validation_summary
            self._maybe_flush_partial(candidates, validation_summary, force=True)

            if validation_summary.success_count:
                self.publisher.publish(validation_summary, candidates=candidates)
            else:
                self.logger.warning("No active proxies validated; skipping publication")

        elapsed = time.time() - start_time
        self._log_kpis(elapsed, candidates, validation_summary)
        return list(validation_summary.active_proxies)
