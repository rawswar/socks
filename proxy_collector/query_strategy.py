from __future__ import annotations

import random
from typing import Iterable, Iterator, List, Optional


class QueryStrategyGenerator:
    def __init__(self, seed: Optional[int] = None) -> None:
        self.random = random.Random(seed)
        self.high_quality_queries: List[str] = [
            '"socks5" "proxy" language:Text pushed:>2024-01-01',
            '"socks5" "list" in:file size:>200',
            '"proxy list" "socks5" language:Markdown',
            '"socks5" language:JSON pushed:>2024-01-01',
            '"socks5 proxy" fork:true size:>500',
            '"fresh proxies" "socks5" language:Text',
            '"socks5" "ip:port" in:file',
            '"elite proxy" "socks5" language:Markdown',
            '"socks5 proxies" "updated" pushed:>2023-01-01',
            '"proxy" "socks5" language:YAML in:file',
            '"socks5" language:Text size:>1000',
            '"working socks5" "proxy list"',
        ]
    
    def generate(self, limit: Optional[int] = None) -> List[str]:
        if limit is None:
            raise ValueError("Query generation requires an explicit limit to avoid infinite sequences")
        
        queries: List[str] = []
        for query in self._iter_queries():
            queries.append(query)
            if len(queries) >= limit:
                break
        
        return queries
    
    def _iter_queries(self) -> Iterator[str]:
        for query in self.high_quality_queries:
            yield query
        
        fallback_templates = [
            '"socks5" "{country}" "proxy"',
            '"proxy list" "socks5" "{country}"',
        ]
        countries = ["usa", "uk", "germany", "china", "japan", "france"]
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
