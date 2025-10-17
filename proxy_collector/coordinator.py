from __future__ import annotations

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

_PROXY_PATTERN = re.compile(r"\b((?:\d{1,3}\.){3}\d{1,3}):(\d{2,5})\b")


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
        for match in _PROXY_PATTERN.finditer(text):
            host, port_str = match.groups()
            if self._is_valid_ip(host) and self._is_valid_port(port_str):
                proxies.add((host, int(port_str)))
        return proxies

    @staticmethod
    def _is_valid_ip(host: str) -> bool:
        octets = host.split(".")
        if len(octets) != 4:
            return False
        try:
            return all(0 <= int(octet) <= 255 for octet in octets)
        except ValueError:
            return False

    @staticmethod
    def _is_valid_port(port: str) -> bool:
        try:
            value = int(port)
            return 1 <= value <= 65535
        except ValueError:
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
        for item in payload.get("items", []):
            download_url = item.get("download_url")
            if not download_url:
                continue
            content = self.github_client.fetch_file_content(download_url)
            if not content:
                continue
            extracted = self._extract_proxies_from_text(content)
            if extracted:
                self.logger.debug(
                    "Query '%s' yielded %d candidates from %s",
                    query,
                    len(extracted),
                    download_url,
                )
                candidates.update(extracted)
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
