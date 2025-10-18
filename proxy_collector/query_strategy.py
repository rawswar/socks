from __future__ import annotations

import itertools
import random
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Optional, Sequence


@dataclass(frozen=True)
class QuerySpec:
    """Container for a fully-qualified GitHub search query."""

    query: str
    sort: Optional[str] = "indexed"
    order: Optional[str] = "desc"
    lane: str = "primary"


class QueryStrategyGenerator:
    """Generate high-signal GitHub code search queries for SOCKS5 discovery."""

    def __init__(self, seed: Optional[int] = None) -> None:
        self.random = random.Random(seed)
        self._primary_pool = self._build_primary_pool()
        self._fallback_pool = self._build_fallback_pool()

    def generate(self, limit: Optional[int] = None, mode: str = "primary") -> List[QuerySpec]:
        if limit is None or limit <= 0:
            raise ValueError("Query generation requires a positive limit")
        iterator = self.iterate(mode)
        return [next(iterator) for _ in range(limit)]

    def iterate(self, mode: str = "primary") -> Iterator[QuerySpec]:
        pool: Sequence[QuerySpec]
        if mode == "primary":
            pool = self._primary_pool
        elif mode == "fallback":
            pool = self._fallback_pool
        else:
            raise ValueError(f"Unsupported query mode: {mode}")
        while True:
            shuffled = list(pool)
            self.random.shuffle(shuffled)
            for spec in shuffled:
                yield spec

    def batched(self, batch_size: int, limit: Optional[int] = None, mode: str = "primary") -> Iterable[List[QuerySpec]]:
        if batch_size <= 0:
            raise ValueError("Batch size must be positive")
        iterator = self.iterate(mode)
        batch: List[QuerySpec] = []
        produced = 0
        while limit is None or produced < limit:
            batch.append(next(iterator))
            produced += 1
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _with_filters(self, core: str) -> str:
        suffix = (
            "size:>200 size:<200000 "
            "-filename:README* -path:doc -path:docs -path:po "
            "-filename:*.po -filename:*.rst -filename:CHANGELOG* -filename:NEWS*"
        )
        return f"{core} {suffix}".strip()

    def _dedupe(self, queries: Iterable[str], lane: str) -> List[QuerySpec]:
        seen = set()
        specs: List[QuerySpec] = []
        for raw_query in queries:
            normalized = " ".join(raw_query.split())
            if normalized in seen:
                continue
            seen.add(normalized)
            specs.append(QuerySpec(normalized, lane=lane))
        return specs

    def _build_primary_pool(self) -> List[QuerySpec]:
        filenames = [
            "filename:socks5.txt",
            "filename:proxies-socks5.txt",
            "filename:SOCKS5_RAW.txt",
        ]
        path_focus = [
            "path:proxy",
            "path:proxies",
            "path:list",
            "in:path socks5",
        ]
        semantic_phrases = [
            '"socks5" "ip:port"',
            '"socks5 proxies"',
            '"proxy list" socks5',
            '"socks5" "proxy list"',
            '"socks5h"',
        ]
        extensions = [
            "extension:txt",
            "extension:csv",
            "extension:json",
            "extension:yaml",
            "extension:conf",
            "language:Text",
            "language:Markdown",
        ]

        primary_queries = set()

        for filename, semantic in itertools.product(filenames, semantic_phrases[:3]):
            primary_queries.add(self._with_filters(f"{filename} {semantic}"))
        for filename in filenames:
            primary_queries.add(self._with_filters(f"{filename} in:path \"ip:port\""))
        for clause in path_focus:
            primary_queries.add(self._with_filters(f"{clause} \"socks5\" extension:txt"))
        for ext in extensions:
            primary_queries.add(self._with_filters(f'"socks5" "proxy list" {ext}'))
        primary_queries.add(self._with_filters('"socks5" "ip:port" in:file extension:txt'))
        primary_queries.add(self._with_filters('"SOCKS5_RAW" extension:txt'))
        primary_queries.add(self._with_filters('"socks5h" extension:txt'))
        primary_queries.add(self._with_filters('filename:proxies-socks5.txt OR filename:SOCKS5_RAW.txt'))
        primary_queries.add(self._with_filters('in:path socks5 extension:csv'))

        return self._dedupe(primary_queries, lane="primary")

    def _build_fallback_pool(self) -> List[QuerySpec]:
        fallback_semantics = [
            '"socks5" "fresh"',
            '"premium" "socks5"',
            '"daily" "socks5" "proxy"',
        ]
        countries = [
            "usa",
            "uk",
            "germany",
            "china",
            "japan",
            "france",
            "brazil",
            "india",
        ]
        fallback_queries = set()
        for country, phrase in itertools.product(countries, fallback_semantics):
            fallback_queries.add(self._with_filters(f'{phrase} {country} extension:txt'))
            fallback_queries.add(self._with_filters(f'"proxy list" "socks5" {country} extension:csv'))
        fallback_queries.add(self._with_filters('"socks5" fork:true extension:txt'))
        fallback_queries.add(self._with_filters('"socks5" language:YAML in:file'))
        fallback_queries.add(self._with_filters('"working" "socks5" "proxy list"'))

        return self._dedupe(fallback_queries, lane="fallback")
