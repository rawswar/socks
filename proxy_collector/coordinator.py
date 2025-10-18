from __future__ import annotations

import contextlib
import ipaddress
import logging
import re
import signal
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

from .config import load_config
from .github_client import GitHubClient
from .models import ActiveProxy
from .proxy_validator import ProxyValidator, ValidationResult
from .query_strategy import QueryStrategyGenerator, QuerySpec
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

TEXT_MATCH_IPV4_PATTERN = re.compile(r"\b((?:\d{1,3}\.){3}\d{1,3}):(\d{2,5})\b")
HIGH_PRIORITY_FILENAMES = {"socks5.txt", "proxies-socks5.txt", "socks5_raw.txt"}
NEGATIVE_PATH_TOKENS = {"doc", "docs", "locale", "po", "i18n"}
SUPPORTED_EXTENSIONS = {"txt", "csv", "json", "yaml", "yml", "conf", "md"}
PATH_PRIORITY_KEYWORDS = {"proxy", "proxies", "list", "socks5"}


@dataclass(frozen=True)
class ScoredSearchItem:
    score: float
    ipv4_hits: int
    size: int
    item: Dict[str, Any]


class Coordinator:
    def __init__(
        self,
        config_overrides: Optional[Dict[str, object]] = None,
    ) -> None:
        self.config = load_config(config_overrides)
        self._termination_reason: Optional[str] = None
        self.shutdown_event = threading.Event()
        self._last_validation_report: Optional[ValidationResult] = None
        self._latest_candidates_snapshot: Set[Tuple[str, int]] = set()
        self._partial_active: List[ActiveProxy] = []
        self._last_flush_timestamp: float = time.time()
        self._metrics: Dict[str, float] = {
            "queries_executed": 0,
            "files_attempted": 0,
            "files_successful": 0,
            "files_with_proxies": 0,
            "downloads_attempted": 0,
            "downloads_successful": 0,
            "downloads_skipped": 0,
        }
        self._metrics_lock = threading.Lock()
        self._query_metrics: Dict[str, Dict[str, float]] = defaultdict(lambda: {
            "search_hits": 0,
            "eligible_hits": 0,
            "downloads": 0,
            "proxies": 0,
        })
        self._failed_repositories: Dict[str, int] = defaultdict(int)
        self._seen_blob_shas: Set[str] = set()
        self._watchdog_streak = 0
        self._using_fallback_queries = False

        coordinator_cfg = self.config["coordinator"]
        self.max_runtime_seconds = coordinator_cfg.get("max_runtime_seconds")
        self.runtime_threshold_seconds = coordinator_cfg.get("runtime_shutdown_threshold_seconds", 60)
        self.flush_interval_seconds = coordinator_cfg.get("flush_interval_seconds", 300)
        self.max_files_per_query = coordinator_cfg.get("max_files_per_query", 20)
        self.max_downloads_per_query = max(1, int(coordinator_cfg.get("max_downloads_per_query", 10)))
        self.download_concurrency = max(1, int(coordinator_cfg.get("download_concurrency", 4)))
        self.download_queue_limit = max(1, int(coordinator_cfg.get("download_queue_limit", 64)))
        self.score_threshold = float(coordinator_cfg.get("score_threshold", 3.0))
        self.sampling_ratio = float(coordinator_cfg.get("sampling_ratio", 0.05))
        self.sampling_threshold = max(1, int(coordinator_cfg.get("sampling_full_extraction_threshold", 5)))
        self.watchdog_threshold = max(1, int(coordinator_cfg.get("watchdog_zero_result_threshold", 5)))

        self._configure_logging()
        github_cfg = self.config["github"]
        self.search_sort = github_cfg.get("search_sort", "indexed")
        self.search_order = github_cfg.get("search_order", "desc")
        self.github_client = GitHubClient(
            base_url=github_cfg["base_url"],
            token=github_cfg.get("token"),
            request_timeout=github_cfg["request_timeout"],
            max_retries=github_cfg["max_retries"],
            requests_per_minute=github_cfg.get("requests_per_minute", 10),
            min_request_interval=github_cfg.get("min_request_interval_seconds", 5.0),
            max_request_interval=github_cfg.get("max_request_interval_seconds", 12.0),
            secondary_rate_limit_cooldown=github_cfg.get("secondary_rate_limit_cooldown", 300.0),
            initial_backoff=github_cfg.get("initial_backoff_seconds", 60.0),
            max_backoff=github_cfg.get("max_backoff_seconds", 3600.0),
            backoff_jitter_ratio=github_cfg.get("backoff_jitter_ratio", 0.1),
            negative_cache_ttl=github_cfg.get("negative_cache_ttl_seconds", 24 * 3600),
        )
        validator_cfg = self.config["validator"]
        self.validator = ProxyValidator(
            max_workers=int(validator_cfg["max_workers"]),
            timeout=float(validator_cfg["timeout"]),
            test_url=validator_cfg.get("test_url"),
            expected_ip_key=validator_cfg.get("expected_ip_key"),
            protocols=validator_cfg.get("protocols"),
            endpoints=validator_cfg.get("endpoints"),
            connect_timeout=validator_cfg.get("connect_timeout"),
            read_timeout=validator_cfg.get("read_timeout"),
        )
        publisher_cfg = self.config["publisher"]
        self.publisher = ResultPublisher(
            output_dir=publisher_cfg["output_dir"],
            txt_filename=publisher_cfg["txt_filename"],
            json_filename=publisher_cfg["json_filename"],
            zip_filename=publisher_cfg["zip_filename"],
            partial_txt_filename=publisher_cfg["partial_txt_filename"],
            partial_json_filename=publisher_cfg["partial_json_filename"],
            active_json_filename=publisher_cfg["active_json_filename"],
            classified_json_filename=publisher_cfg["classified_json_filename"],
        )
        self.query_generator = QueryStrategyGenerator()
        self.logger = logging.getLogger(__name__ + ".Coordinator")

    def _increment_metric(self, key: str, delta: float = 1.0) -> None:
        with self._metrics_lock:
            self._metrics[key] = self._metrics.get(key, 0) + delta

    def _update_query_metric(self, query: str, key: str, delta: float) -> None:
        with self._metrics_lock:
            metrics = self._query_metrics[query]
            metrics[key] = metrics.get(key, 0) + delta

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
    def _ipv4_hit_score(hit_count: int) -> float:
        if hit_count >= 20:
            return 5.0
        if hit_count >= 10:
            return 4.0
        if hit_count >= 5:
            return 3.0
        if hit_count >= 3:
            return 2.0
        if hit_count >= 1:
            return 1.0
        return 0.0

    def _score_search_item(self, item: Dict[str, Any]) -> ScoredSearchItem:
        score = 0.0
        name = (item.get("name") or "").lower()
        path = (item.get("path") or "").lower()

        if name in HIGH_PRIORITY_FILENAMES or path.split("/")[-1] in HIGH_PRIORITY_FILENAMES:
            score += 3.0
        elif any(keyword in path for keyword in PATH_PRIORITY_KEYWORDS):
            score += 1.5

        if any(token in path.split("/") for token in NEGATIVE_PATH_TOKENS):
            score -= 3.0

        extension = ""
        if "." in path:
            extension = path.rsplit(".", 1)[-1]
            if extension in SUPPORTED_EXTENSIONS:
                score += 1.0

        language = (item.get("language") or "").lower()
        if language in {"text", "markdown"}:
            score += 1.0

        size_value = item.get("size")
        size = 0
        if isinstance(size_value, int):
            size = size_value
        elif isinstance(size_value, str) and size_value.isdigit():
            size = int(size_value)
        if size:
            if 1024 <= size <= 200_000:
                score += 2.0
            else:
                score -= 2.0

        text_matches = item.get("text_matches") or []
        ipv4_hits = 0
        for match in text_matches:
            fragment = match.get("fragment") or ""
            ipv4_hits += len(TEXT_MATCH_IPV4_PATTERN.findall(fragment))
        score += self._ipv4_hit_score(ipv4_hits)

        return ScoredSearchItem(score=score, ipv4_hits=ipv4_hits, size=size, item=item)

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

    def _extract_with_sampling(self, content: str) -> Tuple[Set[Tuple[str, int]], bool]:
        if not content:
            return set(), False
        lines = content.splitlines()
        if not lines:
            return set(), False
        ratio = max(0.01, min(self.sampling_ratio, 1.0))
        if ratio >= 1.0:
            full = self._extract_proxies_from_text(content)
            return full, True
        step = max(1, int(1.0 / ratio))
        sampled_lines = [line for idx, line in enumerate(lines) if idx % step == 0]
        if not sampled_lines:
            sampled_lines = lines[: min(len(lines), 10)]
        sample_text = "\n".join(sampled_lines)
        sample_proxies = self._extract_proxies_from_text(sample_text)
        if len(sample_proxies) >= self.sampling_threshold:
            full_proxies = self._extract_proxies_from_text(content)
            return full_proxies, True
        return sample_proxies, False

    def _download_and_extract(self, candidate: ScoredSearchItem) -> Set[Tuple[str, int]]:
        item = candidate.item
        repository = item.get("repository") or {}
        repo_name = repository.get("full_name", "unknown-repo")
        path = item.get("path", "unknown-path")

        self._increment_metric("files_attempted")
        self._increment_metric("downloads_attempted")

        content = self.github_client.fetch_content_from_search_item(item)
        if content is None:
            with self._metrics_lock:
                self._failed_repositories[repo_name] += 1
            self._increment_metric("downloads_skipped")
            return set()

        self._increment_metric("files_successful")
        self._increment_metric("downloads_successful")

        proxies, escalated = self._extract_with_sampling(content)
        if proxies:
            self._increment_metric("files_with_proxies")
            sample_list = ", ".join(f"{host}:{port}" for host, port in list(proxies)[:3])
            self.logger.info(
                "Extracted %d proxies from %s/%s (score=%.1f sample:%s)",
                len(proxies),
                repo_name,
                path,
                candidate.score,
                "full" if escalated else "sample",
            )
            if sample_list:
                self.logger.debug("Sample proxies from %s/%s: %s", repo_name, path, sample_list)
            return proxies

        if not escalated:
            self.logger.debug(
                "Sampling found no proxies for %s/%s (score=%.1f)",
                repo_name,
                path,
                candidate.score,
            )
        self._increment_metric("downloads_skipped")
        return set()

    def _process_scored_items(
        self,
        query_key: str,
        candidates: Sequence[ScoredSearchItem],
    ) -> Tuple[Set[Tuple[str, int]], int, int]:
        if not candidates:
            return set(), 0, 0
        limit = min(len(candidates), self.download_queue_limit)
        selected = list(candidates[:limit])
        aggregated: Set[Tuple[str, int]] = set()
        processed = 0
        proxies_found = 0

        max_workers = max(1, self.download_concurrency)
        iterator = iter(selected)
        futures: Dict[Any, ScoredSearchItem] = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            def _submit_next() -> None:
                try:
                    candidate = next(iterator)
                except StopIteration:
                    return
                futures[executor.submit(self._download_and_extract, candidate)] = candidate

            for _ in range(min(max_workers, limit)):
                _submit_next()

            for future in as_completed(futures):
                candidate = futures.pop(future)
                try:
                    proxies = future.result()
                except Exception as exc:  # pylint: disable=broad-except
                    repository = candidate.item.get("repository") or {}
                    repo_name = repository.get("full_name", "unknown-repo")
                    path = candidate.item.get("path", "unknown-path")
                    self.logger.error(
                        "Download worker failed for %s/%s: %s",
                        repo_name,
                        path,
                        exc,
                    )
                    proxies = set()
                processed += 1
                self._update_query_metric(query_key, "downloads", 1)
                if proxies:
                    aggregated.update(proxies)
                    proxies_found += len(proxies)
                    self._update_query_metric(query_key, "proxies", len(proxies))
                _submit_next()

        return aggregated, processed, proxies_found

    def _next_query_spec(
        self,
        primary_iter: Iterator[QuerySpec],
        fallback_iter: Iterator[QuerySpec],
        used: Set[str],
    ) -> QuerySpec:
        while True:
            iterator = fallback_iter if self._using_fallback_queries else primary_iter
            spec = next(iterator)
            if spec.query in used:
                continue
            used.add(spec.query)
            return spec

    def _update_watchdog(self, result_count: int) -> None:
        if result_count > 0:
            self._watchdog_streak = 0
            return
        self._watchdog_streak += 1
        if (
            self.watchdog_threshold > 0
            and self._watchdog_streak >= self.watchdog_threshold
            and not self._using_fallback_queries
        ):
            self._using_fallback_queries = True
            self.logger.warning(
                "Watchdog triggered fallback query lane after %d empty results",
                self._watchdog_streak,
            )

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
        self._maybe_flush_partial(self._latest_candidates_snapshot, self._partial_active, force=True)

    def _shutdown_threshold_seconds(self) -> float:
        configured = self.runtime_threshold_seconds
        if configured is None:
            return 60.0
        return max(60.0, float(configured))

    def _should_stop_accepting_work(self, start_time: float) -> bool:
        if self.shutdown_event.is_set():
            return True
        remaining = self._time_remaining(start_time)
        if remaining is not None and remaining <= self._shutdown_threshold_seconds():
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
        active: Iterable[ActiveProxy],
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
            self.publisher.write_partial_results(candidates, list(active))
            self._last_flush_timestamp = now
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.debug("Unable to flush partial results: %s", exc)

    def _harvest_from_query(self, spec: QuerySpec, start_time: float) -> Set[Tuple[str, int]]:
        if self.shutdown_event.is_set():
            return set()
        github_cfg = self.config["github"]
        payload = self.github_client.search_code(
            spec.query,
            per_page=github_cfg["per_page"],
            max_pages=github_cfg["max_pages"],
            sort=spec.sort or self.search_sort,
            order=spec.order or self.search_order,
        )
        if payload is None:
            return set()

        items = payload.get("items", [])
        self._update_query_metric(spec.query, "search_hits", len(items))

        scored: List[ScoredSearchItem] = []
        for item in items:
            if self.shutdown_event.is_set():
                break
            blob_sha = (item.get("sha") or "").lower()
            if blob_sha and blob_sha in self._seen_blob_shas:
                continue
            scored_item = self._score_search_item(item)
            if scored_item.score < self.score_threshold:
                continue
            self._update_query_metric(spec.query, "eligible_hits", 1)
            scored.append(scored_item)

        if not scored:
            self.logger.info(
                "Query '%s' (lane=%s) returned %d results with no eligible candidates",
                spec.query,
                spec.lane,
                len(items),
            )
            return set()

        scored.sort(key=lambda entry: (-entry.score, -entry.ipv4_hits, entry.size))
        limit = min(
            len(scored),
            self.max_downloads_per_query,
            self.max_files_per_query,
            self.download_queue_limit,
        )
        shortlisted = scored[:limit]
        for candidate in shortlisted:
            blob_sha = (candidate.item.get("sha") or "").lower()
            if blob_sha:
                self._seen_blob_shas.add(blob_sha)

        preview = ", ".join(
            f"{candidate.item.get('path', 'unknown')}:{candidate.score:.1f}" for candidate in shortlisted[:3]
        )
        self.logger.info(
            "Query '%s' (lane=%s) shortlisted %d/%d candidates (preview: %s)",
            spec.query,
            spec.lane,
            len(shortlisted),
            len(items),
            preview or "n/a",
        )

        candidates, processed, proxies_found = self._process_scored_items(spec.query, shortlisted)
        if processed:
            self.logger.debug(
                "Query '%s' processed %d candidates yielding %d unique proxies (%d total detections)",
                spec.query,
                processed,
                len(candidates),
                proxies_found,
            )
        return candidates

    def _collect_candidates(self, max_queries: int, concurrency: int, start_time: float) -> Set[Tuple[str, int]]:
        aggregated: Set[Tuple[str, int]] = set()
        self._latest_candidates_snapshot = aggregated

        primary_iter = self.query_generator.iterate("primary")
        fallback_iter = self.query_generator.iterate("fallback")
        used_queries: Set[str] = set()

        if concurrency <= 1:
            for _ in range(max_queries):
                if self._should_stop_accepting_work(start_time):
                    self.logger.info("Stopping further query scheduling due to graceful shutdown trigger")
                    break
                spec = self._next_query_spec(primary_iter, fallback_iter, used_queries)
                self._increment_metric("queries_executed")
                try:
                    result = self._harvest_from_query(spec, start_time)
                except Exception as exc:  # pylint: disable=broad-except
                    self.logger.error("Query task failed for '%s': %s", spec.query, exc)
                    self._update_watchdog(0)
                    continue
                aggregated.update(result)
                self._latest_candidates_snapshot = aggregated
                self.logger.info(
                    "Query '%s' (lane=%s) produced %d unique proxies",
                    spec.query,
                    spec.lane,
                    len(result),
                )
                self._maybe_flush_partial(aggregated, self._partial_active)
                self._update_watchdog(len(result))
            return aggregated

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures_to_spec: Dict[Any, QuerySpec] = {}

            def _submit_next() -> None:
                if self._should_stop_accepting_work(start_time):
                    return
                if len(used_queries) >= max_queries:
                    return
                spec = self._next_query_spec(primary_iter, fallback_iter, used_queries)
                self._increment_metric("queries_executed")
                futures_to_spec[executor.submit(self._harvest_from_query, spec, start_time)] = spec

            for _ in range(min(concurrency, max_queries)):
                _submit_next()

            while futures_to_spec:
                for future in as_completed(list(futures_to_spec)):
                    spec = futures_to_spec.pop(future)
                    try:
                        result = future.result()
                        aggregated.update(result)
                        self._latest_candidates_snapshot = aggregated
                        self.logger.info(
                            "Query '%s' (lane=%s) produced %d unique proxies",
                            spec.query,
                            spec.lane,
                            len(result),
                        )
                        self._update_watchdog(len(result))
                    except Exception as exc:  # pylint: disable=broad-except
                        self.logger.error("Query task failed for '%s': %s", spec.query, exc)
                        self._update_watchdog(0)
                    self._maybe_flush_partial(aggregated, self._partial_active)
                    _submit_next()
                    break
        return aggregated

    def _log_kpis(
        self,
        elapsed_seconds: float,
        candidates: Set[Tuple[str, int]],
        validation_report: Optional[ValidationResult],
    ) -> None:
        github_metrics = self.github_client.get_metrics()
        active_snapshot = list(validation_report) if validation_report is not None else list(self._partial_active)
        summary = (
            "Run KPIs | queries=%d | downloads=%d/%d | files_with_proxies=%d | candidates=%d | validated=%d | "
            "secondary_hits=%d | secondary_cooldown=%.1fs | timed_out=%s"
        )
        timed_out = self._termination_reason == "timeout"
        downloads_attempted = int(self._metrics["downloads_attempted"])
        downloads_successful = int(self._metrics["downloads_successful"])
        downloads_skipped = int(self._metrics["downloads_skipped"])
        self.logger.info(
            summary,
            int(self._metrics["queries_executed"]),
            downloads_successful,
            downloads_attempted,
            int(self._metrics["files_with_proxies"]),
            len(candidates),
            len(active_snapshot),
            github_metrics.get("secondary_rate_limit_hits", 0),
            github_metrics.get("secondary_rate_limit_cooldown_seconds", 0.0),
            "yes" if timed_out else "no",
        )
        signal_ratio = 0.0
        if self._metrics["files_successful"]:
            signal_ratio = self._metrics["files_with_proxies"] / max(1.0, self._metrics["files_successful"])
        self.logger.info(
            "Signal ratio: %.1f%% | files_success=%d/%d | downloads_skipped=%d",
            signal_ratio * 100.0,
            int(self._metrics["files_with_proxies"]),
            int(self._metrics["files_successful"]),
            downloads_skipped,
        )
        http_404 = github_metrics.get("http_404", 0)
        request_total = github_metrics.get("requests", 0)
        if request_total:
            self.logger.info(
                "HTTP 404 ratio: %.2f%% (%d/%d)",
                (http_404 / request_total) * 100.0,
                http_404,
                request_total,
            )
        self.logger.info(
            "Pipeline finished in %.2fs | %d candidates | %d active proxies",
            elapsed_seconds,
            len(candidates),
            len(active_snapshot),
        )

        if validation_report is not None:
            failure_total = sum(validation_report.failures_by_reason.values())
            self.logger.info(
                "Validator summary | attempts=%d | success=%d | failures=%d | unique_egress=%d",
                validation_report.total_candidates,
                len(validation_report),
                failure_total,
                len(validation_report.egress_ips),
            )
            if validation_report.failures_by_reason:
                failure_summary = ", ".join(
                    f"{reason}={count}" for reason, count in validation_report.failures_by_reason.items()
                )
                self.logger.info("Validation failures by reason: %s", failure_summary)
            if validation_report.egress_ips:
                egress_preview = ", ".join(
                    f"{ip}={count}" for ip, count in list(validation_report.egress_ips.items())[:5]
                )
                self.logger.info("Egress coverage detail: %s", egress_preview)
            if validation_report.classified:
                class_breakdown = ", ".join(
                    f"{name}={len(proxies)}" for name, proxies in validation_report.classified.items()
                )
                self.logger.info("Proxy classifications: %s", class_breakdown)

        top_queries = sorted(
            self._query_metrics.items(),
            key=lambda item: (item[1].get("proxies", 0), item[1].get("downloads", 0)),
            reverse=True,
        )[:3]
        if top_queries:
            breakdown = ", ".join(
                f"{query}: proxies={int(metrics.get('proxies', 0))} downloads={int(metrics.get('downloads', 0))}"
                for query, metrics in top_queries
            )
            self.logger.info("Top query contributions: %s", breakdown)

        if self._failed_repositories:
            top_failures = sorted(
                self._failed_repositories.items(),
                key=lambda item: item[1],
                reverse=True,
            )[:5]
            failure_summary = ", ".join(f"{repo}:{count}" for repo, count in top_failures)
            self.logger.info("Repositories with repeated failures: %s", failure_summary)

        termination_note = self._termination_reason or "completed"
        self.logger.info(
            "Termination summary | graceful_timeout=%s | reason=%s | shutdown_event=%s",
            "yes" if timed_out else "no",
            termination_note,
            "set" if self.shutdown_event.is_set() else "clear",
        )

    def run(self) -> List[ActiveProxy]:
        start_time = time.time()
        self._last_flush_timestamp = start_time
        coordinator_cfg = self.config["coordinator"]
        max_queries = coordinator_cfg["max_queries"]
        search_concurrency = coordinator_cfg["search_concurrency"]

        self.logger.info("Starting collection with up to %d queries", max_queries)
        self._watchdog_streak = 0
        self._using_fallback_queries = False
        self._last_validation_report = None

        candidates: Set[Tuple[str, int]] = set()
        validation_report: Optional[ValidationResult] = None

        try:
            with self._signal_handler():
                candidates = self._collect_candidates(max_queries, search_concurrency, start_time)
                self._latest_candidates_snapshot = candidates
                self.logger.info("Harvested %d unique proxy candidates", len(candidates))
                self._maybe_flush_partial(candidates, self._partial_active, force=True)

                def _on_validator_progress(results: List[ActiveProxy]) -> None:
                    self._partial_active = list(results)
                    self._maybe_flush_partial(candidates, self._partial_active)

                validation_report = self.validator.validate(
                    candidates,
                    shutdown_event=self.shutdown_event,
                    progress_callback=_on_validator_progress,
                )
                self._last_validation_report = validation_report
                validated_now = list(validation_report)
                self._partial_active = validated_now
                self._maybe_flush_partial(candidates, self._partial_active, force=True)
                if not validated_now:
                    self.logger.warning("No active proxies validated; proceeding with empty publication")
        finally:
            publish_candidates = (
                sorted(self._latest_candidates_snapshot)
                if self._latest_candidates_snapshot
                else sorted(candidates)
            )
            published_active = list(validation_report) if validation_report is not None else list(self._partial_active)
            try:
                self.publisher.publish(
                    published_active,
                    candidates=publish_candidates,
                    validation_report=validation_report,
                )
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.error("Failed to publish results: %s", exc)

        elapsed = time.time() - start_time
        final_candidates = self._latest_candidates_snapshot or candidates
        self._log_kpis(elapsed, final_candidates, validation_report)
        return list(validation_report) if validation_report is not None else list(self._partial_active)
