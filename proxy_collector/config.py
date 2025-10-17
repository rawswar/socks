import os
from copy import deepcopy
from typing import Any, Dict, Optional


DEFAULT_CONFIG: Dict[str, Any] = {
    "github": {
        "base_url": "https://api.github.com",
        "search_endpoint": "/search/code",
        "per_page": 50,
        "max_pages": 3,
        "token": None,
        "request_timeout": 10,
        "max_retries": 2,
        "requests_per_minute": 5,
        "min_request_interval_seconds": 6.0,
        "max_request_interval_seconds": 10.0,
        "secondary_rate_limit_cooldown": 300.0,
        "initial_backoff_seconds": 60.0,
        "max_backoff_seconds": 3600.0,
        "backoff_jitter_ratio": 0.1,
    },
    "coordinator": {
        "max_queries": 12,
        "search_concurrency": 1,
        "query_batch_size": 10,
    },
    "validator": {
        "max_workers": 64,
        "timeout": 8,
        "test_url": "https://api.ipify.org?format=json",
        "expected_ip_key": "ip",
    },
    "publisher": {
        "output_dir": "output",
        "txt_filename": "proxies.txt",
        "json_filename": "proxies.json",
        "zip_filename": "proxies.zip",
    },
    "logging": {
        "level": "INFO",
    },
}


def _deep_update(target: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            target[key] = _deep_update(dict(target[key]), value)
        else:
            target[key] = value
    return target


def load_config(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    config = deepcopy(DEFAULT_CONFIG)
    token = os.getenv("GITHUB_TOKEN")
    if token:
        config["github"]["token"] = token

    if overrides:
        config = _deep_update(config, overrides)

    env_max_queries = os.getenv("PROXY_MAX_QUERIES")
    if env_max_queries:
        try:
            config["coordinator"]["max_queries"] = int(env_max_queries)
        except ValueError:
            pass

    env_concurrency = os.getenv("PROXY_SEARCH_CONCURRENCY")
    if env_concurrency:
        try:
            config["coordinator"]["search_concurrency"] = int(env_concurrency)
        except ValueError:
            pass

    env_validator_workers = os.getenv("PROXY_VALIDATOR_WORKERS")
    if env_validator_workers:
        try:
            config["validator"]["max_workers"] = int(env_validator_workers)
        except ValueError:
            pass

    env_requests_per_minute = os.getenv("PROXY_REQUESTS_PER_MINUTE")
    if env_requests_per_minute:
        try:
            config["github"]["requests_per_minute"] = max(0, int(env_requests_per_minute))
        except ValueError:
            pass

    env_min_delay = os.getenv("PROXY_REQUEST_MIN_DELAY")
    if env_min_delay:
        try:
            config["github"]["min_request_interval_seconds"] = max(0.0, float(env_min_delay))
        except ValueError:
            pass

    env_max_delay = os.getenv("PROXY_REQUEST_MAX_DELAY")
    if env_max_delay:
        try:
            config["github"]["max_request_interval_seconds"] = max(0.0, float(env_max_delay))
        except ValueError:
            pass

    env_secondary_cooldown = os.getenv("PROXY_SECONDARY_RATE_LIMIT_COOLDOWN")
    if env_secondary_cooldown:
        try:
            config["github"]["secondary_rate_limit_cooldown"] = max(0.0, float(env_secondary_cooldown))
        except ValueError:
            pass

    env_initial_backoff = os.getenv("PROXY_INITIAL_BACKOFF_SECONDS")
    if env_initial_backoff:
        try:
            config["github"]["initial_backoff_seconds"] = max(0.0, float(env_initial_backoff))
        except ValueError:
            pass

    env_max_backoff = os.getenv("PROXY_MAX_BACKOFF_SECONDS")
    if env_max_backoff:
        try:
            config["github"]["max_backoff_seconds"] = max(0.0, float(env_max_backoff))
        except ValueError:
            pass

    env_max_retries = os.getenv("PROXY_GITHUB_MAX_RETRIES")
    if env_max_retries:
        try:
            config["github"]["max_retries"] = max(0, int(env_max_retries))
        except ValueError:
            pass

    min_interval = config["github"]["min_request_interval_seconds"]
    max_interval = config["github"]["max_request_interval_seconds"]
    if max_interval < min_interval:
        config["github"]["max_request_interval_seconds"] = float(min_interval)

    return config
