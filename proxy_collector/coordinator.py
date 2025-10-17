from __future__ import annotations

import ipaddress
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Optional, Set, Tuple

from .config import load_config
from .github_client import GitHubClient
from .models import ActiveProxy
from .proxy_validator import ProxyValidator
from .query_strategy import QueryStrategyGenerator
from .result_publisher import ResultPublisher

MAX_FILES_PER_QUERY = 20

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
            timeout=validator_cfg["timeout"],
            test_url=validator_cfg["test_url"],
            expected_ip_key=validator_cfg["expected_ip_key"],
        )
        publisher_cfg = self.config["publisher"]
        self.publisher = ResultPublisher(
            output_dir=publisher_cfg["output_dir"],
            txt_filename=publisher_cfg["txt_filename"],
            json_filename=publisher_cfg["json_filename"],
            zip_filename=publisher_cfg["zip_filename"],
        )
        self.query_generator = QueryStrategyGenerator()
        self.logger = logging.getLogger(__name__ + ".Coordinator")

    def _configure_logging(self) -> None:
        level_name = self.config["logging"].get("level", "INFO")
        level = getattr(logging, str(level_name).upper(), logging.INFO)
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=level,
                format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            )
        else:
            logging.getLogger().setLevel(level)

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
                    proxies.add((ip_value, int(port_value)))
        return proxies

    @staticmethod
    def _is_valid_ip(host: str) -> bool:
        try:
            ip_obj = ipaddress.IPv4Address(host)
        except ipaddress.AddressValueError:
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

    def _harvest_from_query(self, query: str) -> Set[Tuple[str, int]]:
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
        limited_items = items[:MAX_FILES_PER_QUERY]
        if len(items) > MAX_FILES_PER_QUERY:
            self.logger.debug(
                "Limiting processing to %d of %d items for query '%s'",
                MAX_FILES_PER_QUERY,
                len(items),
                query,
            )

        for item in limited_items:
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
            if content is None:
                self.logger.debug("No content retrieved for %s/%s", repo_name, path)
                continue

            self.logger.debug(
                "File %s/%s size: %d bytes",
                repo_name,
                path,
                len(content),
            )

            extracted = self._extract_proxies_from_text(content)
            self.logger.debug(
                "Found %d proxy candidates in %s/%s",
                len(extracted),
                repo_name,
                path,
            )

            if extracted:
                candidates.update(extracted)
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

    def _collect_candidates(self, queries: Iterable[str], concurrency: int) -> Set[Tuple[str, int]]:
        aggregated: Set[Tuple[str, int]] = set()

        if concurrency <= 1:
            for query in queries:
                result = self._harvest_from_query(query)
                aggregated.update(result)
                self.logger.info("Query '%s' produced %d unique proxies", query, len(result))
            return aggregated

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            future_map = {executor.submit(self._harvest_from_query, query): query for query in queries}
            for future in as_completed(future_map):
                query = future_map[future]
                try:
                    result = future.result()
                    aggregated.update(result)
                    self.logger.info("Query '%s' produced %d unique proxies", query, len(result))
                except Exception as exc:
                    self.logger.error("Query task failed for '%s': %s", query, exc)
        return aggregated

    def run(self) -> List[ActiveProxy]:
        start_time = time.time()
        coordinator_cfg = self.config["coordinator"]
        max_queries = coordinator_cfg["max_queries"]
        search_concurrency = coordinator_cfg["search_concurrency"]

        queries = self.query_generator.generate(limit=max_queries)
        self.logger.info("Starting collection with %d queries", len(queries))
        candidates = self._collect_candidates(queries, search_concurrency)
        self.logger.info("Harvested %d unique proxy candidates", len(candidates))

        active_proxies = self.validator.validate(candidates)
        self.logger.info("Validated %d active proxies", len(active_proxies))

        if active_proxies:
            self.publisher.publish(active_proxies)
        else:
            self.logger.warning("No active proxies validated; skipping publication")

        elapsed = time.time() - start_time
        self.logger.info(
            "Pipeline finished in %.2fs | %d candidates | %d active proxies",
            elapsed,
            len(candidates),
            len(active_proxies),
        )
        return active_proxies
