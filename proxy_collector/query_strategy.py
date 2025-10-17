from __future__ import annotations

import itertools
import random
from typing import Iterable, Iterator, List, Optional


class QueryStrategyGenerator:
    def __init__(self, seed: Optional[int] = None) -> None:
        self.random = random.Random(seed)
        self.keyword_groups: List[List[str]] = [
            ["socks5", "proxy list"],
            ["socks5", "fresh proxies"],
            ["socks5", "ip:port"],
            ["socks5", "elite proxy"],
            ["socks5", "updated"],
        ]
        self.languages = ["Markdown", "Text", "JSON", "YAML"]
        self.qualifiers = [
            "in:file",
            "fork:true",
            "size:>200",
        ]
        self.recency_filters = [
            "pushed:>2024-01-01",
            "pushed:>2023-01-01",
        ]

    def generate(self, limit: Optional[int] = None) -> List[str]:
        if limit is None:
            raise ValueError("Query generation requires an explicit limit to avoid infinite sequences")
        return list(itertools.islice(self._iter_queries(), 0, limit))

    def _iter_queries(self) -> Iterator[str]:
        templates = [
            "{keywords} {qualifier} language:{language} {recency}",
            "{keywords} {qualifier} language:{language}",
            "{keywords} {recency}",
        ]
        for group in self.keyword_groups:
            keywords = " ".join(f'"{token}"' for token in group)
            for language, qualifier, recency, template in itertools.product(
                self.languages,
                self.qualifiers,
                self.recency_filters,
                templates,
            ):
                query = template.format(
                    keywords=keywords,
                    language=language,
                    qualifier=qualifier,
                    recency=recency,
                )
                yield query.strip()

        fallback_templates = [
            '"socks5" "{country}"',
            '"socks5" "{country}" "proxy"',
        ]
        countries = ["usa", "uk", "germany", "russia", "japan", "france"]
        while True:
            country = self.random.choice(countries)
            template = self.random.choice(fallback_templates)
            yield template.format(country=country)

    def batched(self, batch_size: int, limit: Optional[int] = None) -> Iterable[List[str]]:
        batch: List[str] = []
        produced = 0
        for query in self._iter_queries():
            if limit is not None and produced >= limit:
                break
            batch.append(query)
            produced += 1
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
