from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class ProxyProtocol(str, Enum):
    SOCKS5 = "socks5"
    SOCKS4 = "socks4"
    HTTP_CONNECT = "http"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ActiveProxy:
    host: str
    port: int
    protocol: str
    latency: float
    egress_ip: str
    endpoint: str
    verified_at: str


@dataclass(frozen=True)
class ProxyClassification:
    host: str
    port: int
    protocol: str
    requires_auth: bool
    category: str
    latency: Optional[float]
    endpoint: Optional[str]
    egress_ip: Optional[str]
    verified_at: Optional[str]
    error: Optional[str]


@dataclass(frozen=True)
class GitHubRateInfo:
    remaining: int
    reset_timestamp: float
    limit: Optional[int] = None


@dataclass
class ProxyValidationSummary:
    active_proxies: List[ActiveProxy] = field(default_factory=list)
    classified: List[ProxyClassification] = field(default_factory=list)
    category_counts: Counter = field(default_factory=Counter)
    protocol_counts: Counter = field(default_factory=Counter)
    failure_reasons: Counter = field(default_factory=Counter)

    def record(self, classification: ProxyClassification) -> None:
        self.classified.append(classification)
        self.protocol_counts[classification.protocol] += 1
        self.category_counts[classification.category] += 1
        if classification.error:
            self.failure_reasons[classification.error] += 1
        if self._is_validated_socks5(classification):
            self.active_proxies.append(
                ActiveProxy(
                    host=classification.host,
                    port=classification.port,
                    protocol=classification.protocol,
                    latency=classification.latency or 0.0,
                    egress_ip=classification.egress_ip or "",
                    endpoint=classification.endpoint or "",
                    verified_at=classification.verified_at or "",
                )
            )

    def snapshot(self) -> ProxyValidationSummary:
        clone = ProxyValidationSummary()
        clone.active_proxies.extend(self.active_proxies)
        clone.classified.extend(self.classified)
        clone.category_counts.update(self.category_counts)
        clone.protocol_counts.update(self.protocol_counts)
        clone.failure_reasons.update(self.failure_reasons)
        return clone

    @property
    def success_count(self) -> int:
        return len(self.active_proxies)

    @staticmethod
    def _is_validated_socks5(classification: ProxyClassification) -> bool:
        if classification.category != "validated":
            return False
        if classification.protocol != ProxyProtocol.SOCKS5.value:
            return False
        if classification.requires_auth:
            return False
        if classification.latency is None:
            return False
        if not classification.egress_ip or not classification.endpoint or not classification.verified_at:
            return False
        return True
