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
        "max_retries": 3,
    },
    "coordinator": {
        "max_queries": 50,
        "search_concurrency": 5,
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

    return config
