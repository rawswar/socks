from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ActiveProxy:
    host: str
    port: int
    latency: float
    protocol: str = "socks5"
    egress_ip: Optional[str] = None
    endpoint: Optional[str] = None
    classification: Optional[str] = None


@dataclass
class GitHubRateInfo:
    remaining: int
    reset_timestamp: float
    limit: Optional[int] = None
