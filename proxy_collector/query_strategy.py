from __future__ import annotations

import itertools
import random
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Optional, Sequence

FILENAME_CLAUSES = [
    "filename:socks5.txt",
    "filename:SOCKS5_RAW.txt",
    "filename:proxies-socks5.txt",
    "filename:socks5_list.txt",
]

FILENAME_OR_GROUP = "(" + " OR ".join(FILENAME_CLAUSES) + ")"

PATH_CLAUSES = [
    "path:proxy",
    "path:proxies",
    "path:list",
    "in:path socks5",
]

SEMANTIC_PRIMARY = [
    '"socks5" "ip:port"',
    '"proxy list" socks5',
    '"socks5 proxies"',
]

SEMANTIC_SUPPORT = [
    '"socks5" "proxy list"',
    '"fresh" "socks5"',
    '"daily" "socks5"',
    "socks5h",
]

EXTENSION_CLAUSES = [
    "extension:txt",
    "extension:csv",
    "extension:json",
    "extension:yaml",
    "extension:yml",
    "extension:conf",
]

LANGUAGE_CLAUSES = [
    "language:Text",
    "language:Markdown",
]

FALLBACK_SEMANTICS = [
    '"premium" "socks5"',
    '"fresh" "socks5"',
    '"daily" "socks5" "proxy"',
    '"working" "socks5"',
]

COUNTRY_HINTS = [
    "usa",
    "uk",
    "germany",
    "france",
    "netherlands",
    "brazil",
    "mexico",
    "japan",
    "india",
    "singapore",
]


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

    def _compose_query(self, *parts: str) -> str:
        tokens = [token.strip() for token in parts if token and token.strip()]
        if not tokens:
            raise ValueError("Cannot compose an empty GitHub search query")
        core = " ".join(tokens)
        return self._with_filters(core)

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
        assembled: List[str] = []

        def add(*parts: str) -> None:
            assembled.append(self._compose_query(*parts))

        for filename, phrase in itertools.product(FILENAME_CLAUSES, SEMANTIC_PRIMARY):
            add(filename, phrase)
        for filename, ext in itertools.product(FILENAME_CLAUSES, EXTENSION_CLAUSES):
            add(filename, '"socks5"', ext)
        for filename in FILENAME_CLAUSES:
            add(filename, '"ip:port"')
            add(filename, '"socks5"', "in:file")

        for phrase in itertools.chain(SEMANTIC_PRIMARY, SEMANTIC_SUPPORT):
            add(FILENAME_OR_GROUP, phrase)
        add(FILENAME_OR_GROUP, '"proxy list"', 'socks5')
        add(FILENAME_OR_GROUP, "extension:txt")

        for path_clause in PATH_CLAUSES:
            for phrase in SEMANTIC_PRIMARY:
                add(path_clause, phrase)
            for ext in EXTENSION_CLAUSES:
                add(path_clause, '"socks5"', ext)
            for language in LANGUAGE_CLAUSES:
                add(path_clause, '"socks5"', language)

        for ext in EXTENSION_CLAUSES:
            add('"socks5" "proxy list"', ext)
            add('"socks5" "ip:port"', ext)
        for language in LANGUAGE_CLAUSES:
            add('"socks5" "proxy list"', language)
            add('"socks5 proxies"', language)

        add('"socks5h"', "extension:txt")
        add('"socks5h"', '"ip:port"')
        add('"socks5"', '"credential"', "extension:txt")
        add('"socks5"', '"auth"', '"proxy"', "extension:txt")

        return self._dedupe(assembled, lane="primary")

    def _build_fallback_pool(self) -> List[QuerySpec]:
        assembled: List[str] = []

        def add(*parts: str) -> None:
            assembled.append(self._compose_query(*parts))

        for country, phrase in itertools.product(COUNTRY_HINTS, FALLBACK_SEMANTICS):
            add(phrase, country, "extension:txt")
            add(phrase, country, "language:Text")
        for phrase in FALLBACK_SEMANTICS:
            add(phrase, "extension:csv")
            add(phrase, "extension:json")
        add('"socks5"', "fork:true", "extension:txt")
        add('"socks5"', "fork:true", "extension:csv")
        add('"socks5"', '"rotating"', "extension:txt")
        add('"residential"', '"socks5"', "extension:txt")
        add('"rotating"', '"socks5"', "extension:csv")
        add('"datacenter"', '"socks5"', "extension:txt")
        add('"private"', '"socks5"', "extension:txt")

        for language in LANGUAGE_CLAUSES:
            add('"socks5 list"', language)

        add('"working"', '"socks5"', '"proxy list"')
        add('"elite"', '"socks5"', "extension:txt")
        add('"anonymous"', '"socks5"', "extension:txt")

        regions = ["asia", "europe", "america", "africa"]
        for region in regions:
            add('"socks5" "proxy list"', region, "extension:txt")

        return self._dedupe(assembled, lane="fallback")
