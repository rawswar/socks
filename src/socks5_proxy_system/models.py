from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ActiveProxy:
    host: str
    port: int
    latency: float


@dataclass
class GitHubRateInfo:
    remaining: int
    reset_timestamp: float
    limit: Optional[int] = None
