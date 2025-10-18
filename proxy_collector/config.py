import os
from copy import deepcopy
from typing import Any, Dict, Iterable, Optional, Tuple


DEFAULT_CONFIG: Dict[str, Any] = {
    "github": {
        "base_url": "https://api.github.com",
        "search_endpoint": "/search/code",
        "per_page": 50,
        "max_pages": 3,
        "token": None,
        "request_timeout": 10,
        "max_retries": 2,
        "requests_per_minute": 10,
        "min_request_interval_seconds": 5.0,
        "max_request_interval_seconds": 12.0,
        "secondary_rate_limit_cooldown": 300.0,
        "initial_backoff_seconds": 60.0,
        "max_backoff_seconds": 3600.0,
        "backoff_jitter_ratio": 0.1,
        "search_sort": "indexed",
        "search_order": "desc",
        "negative_cache_ttl_seconds": 24 * 3600,
    },
    "coordinator": {
        "max_queries": 12,
        "search_concurrency": 1,
        "query_batch_size": 10,
        "max_files_per_query": 20,
        "max_downloads_per_query": 10,
        "download_concurrency": 4,
        "download_queue_limit": 64,
        "score_threshold": 3.0,
        "sampling_ratio": 0.05,
        "sampling_full_extraction_threshold": 5,
        "watchdog_zero_result_threshold": 5,
        "max_runtime_seconds": None,
        "runtime_shutdown_threshold_seconds": 60,
        "flush_interval_seconds": 300,
    },
    "validator": {
        "max_workers": 64,
        "timeout": 8.0,
        "connect_timeout": 4.0,
        "read_timeout": 8.0,
        "test_url": "https://api.ipify.org?format=json",
        "expected_ip_key": "ip",
        "protocols": ["socks5", "socks4", "http"],
        "endpoints": [],
    },
    "publisher": {
        "output_dir": ".",
        "txt_filename": "proxies.txt",
        "json_filename": "proxies.json",
        "zip_filename": "proxies.zip",
        "partial_txt_filename": "proxies.partial.txt",
        "partial_json_filename": "proxies.partial.json",
        "active_json_filename": "active_proxies.json",
        "classified_json_filename": "classified_proxies.json",
    },
    "logging": {
        "level": "INFO",
        "directory": "logs",
        "filename": "app.log",
        "max_bytes": 2 * 1024 * 1024,
        "backup_count": 5,
    },
}


def _deep_update(target: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            target[key] = _deep_update(dict(target[key]), value)
        else:
            target[key] = value
    return target


def _apply_config_value(config: Dict[str, Any], path: Iterable[str], value: Any) -> None:
    keys = list(path)
    target = config
    for key in keys[:-1]:
        target = target.setdefault(key, {})
    target[keys[-1]] = value


def _env_int(name: str, minimum: Optional[int] = None) -> Optional[int]:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    if minimum is not None:
        parsed = max(minimum, parsed)
    return parsed


def _env_float(name: str, minimum: Optional[float] = None) -> Optional[float]:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    if minimum is not None:
        parsed = max(minimum, parsed)
    return parsed


def _env_str(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    return value.strip()


def _apply_first_env(
    config: Dict[str, Any],
    env_keys: Iterable[str],
    path: Tuple[str, ...],
    parser,
) -> None:
    for env_name in env_keys:
        parsed = parser(env_name)
        if parsed is not None:
            _apply_config_value(config, path, parsed)
            break


def load_config(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    config = deepcopy(DEFAULT_CONFIG)

    token = os.getenv("GITHUB_TOKEN")
    if token:
        config["github"]["token"] = token

    if overrides:
        config = _deep_update(config, overrides)

    # Coordinator overrides
    _apply_first_env(
        config,
        ("PROXY_MAX_QUERIES", "MAX_QUERIES"),
        ("coordinator", "max_queries"),
        lambda key: _env_int(key, minimum=1),
    )
    _apply_first_env(
        config,
        ("PROXY_SEARCH_CONCURRENCY", "SEARCH_CONCURRENCY"),
        ("coordinator", "search_concurrency"),
        lambda key: _env_int(key, minimum=1),
    )
    _apply_first_env(
        config,
        ("PROXY_MAX_FILES_PER_QUERY", "MAX_FILES_PER_QUERY"),
        ("coordinator", "max_files_per_query"),
        lambda key: _env_int(key, minimum=1),
    )
    _apply_first_env(
        config,
        ("PROXY_MAX_RUNTIME_SECONDS", "MAX_RUNTIME_SECONDS"),
        ("coordinator", "max_runtime_seconds"),
        lambda key: _env_int(key, minimum=60),
    )
    _apply_first_env(
        config,
        ("FLUSH_INTERVAL_SECONDS", "PROXY_FLUSH_INTERVAL_SECONDS"),
        ("coordinator", "flush_interval_seconds"),
        lambda key: _env_int(key, minimum=60),
    )

    # GitHub overrides
    _apply_first_env(
        config,
        ("REQUESTS_PER_MINUTE", "PROXY_REQUESTS_PER_MINUTE"),
        ("github", "requests_per_minute"),
        lambda key: _env_int(key, minimum=0),
    )
    _apply_first_env(
        config,
        ("MAX_PAGES", "PROXY_GITHUB_MAX_PAGES"),
        ("github", "max_pages"),
        lambda key: _env_int(key, minimum=1),
    )
    _apply_first_env(
        config,
        ("COOLDOWN", "PROXY_SECONDARY_RATE_LIMIT_COOLDOWN"),
        ("github", "secondary_rate_limit_cooldown"),
        lambda key: _env_float(key, minimum=0.0),
    )
    _apply_first_env(
        config,
        ("PROXY_INITIAL_BACKOFF_SECONDS",),
        ("github", "initial_backoff_seconds"),
        lambda key: _env_float(key, minimum=0.0),
    )
    _apply_first_env(
        config,
        ("PROXY_MAX_BACKOFF_SECONDS",),
        ("github", "max_backoff_seconds"),
        lambda key: _env_float(key, minimum=0.0),
    )
    _apply_first_env(
        config,
        ("PROXY_GITHUB_MAX_RETRIES", "GITHUB_MAX_RETRIES"),
        ("github", "max_retries"),
        lambda key: _env_int(key, minimum=0),
    )

    # Logging overrides
    _apply_first_env(
        config,
        ("LOG_LEVEL", "PROXY_LOG_LEVEL"),
        ("logging", "level"),
        _env_str,
    )

    # Validator overrides
    _apply_first_env(
        config,
        ("VALIDATION_MAX_WORKERS", "PROXY_VALIDATOR_WORKERS", "VALIDATOR_WORKERS"),
        ("validator", "max_workers"),
        lambda key: _env_int(key, minimum=1),
    )
    _apply_first_env(
        config,
        ("VALIDATION_TIMEOUT", "PROXY_VALIDATOR_TIMEOUT"),
        ("validator", "timeout"),
        lambda key: _env_float(key, minimum=0.1),
    )
    _apply_first_env(
        config,
        ("VALIDATION_TIMEOUT_CONNECT", "VALIDATION_CONNECT_TIMEOUT"),
        ("validator", "connect_timeout"),
        lambda key: _env_float(key, minimum=0.1),
    )
    _apply_first_env(
        config,
        ("VALIDATION_TIMEOUT_READ", "VALIDATION_READ_TIMEOUT"),
        ("validator", "read_timeout"),
        lambda key: _env_float(key, minimum=0.1),
    )

    _apply_first_env(
        config,
        ("PROXY_REQUEST_MIN_DELAY",),
        ("github", "min_request_interval_seconds"),
        lambda key: _env_float(key, minimum=0.0),
    )
    _apply_first_env(
        config,
        ("PROXY_REQUEST_MAX_DELAY",),
        ("github", "max_request_interval_seconds"),
        lambda key: _env_float(key, minimum=0.0),
    )

    # Ensure interval bounds remain valid
    min_interval = config["github"]["min_request_interval_seconds"]
    max_interval = config["github"]["max_request_interval_seconds"]
    if max_interval < min_interval:
        config["github"]["max_request_interval_seconds"] = float(min_interval)

    max_files = config["coordinator"]["max_files_per_query"]
    if max_files < 1:
        config["coordinator"]["max_files_per_query"] = 1

    flush_interval = config["coordinator"]["flush_interval_seconds"]
    if flush_interval < 60:
        config["coordinator"]["flush_interval_seconds"] = 60

    connect_timeout = float(config["validator"]["connect_timeout"])
    read_timeout = float(config["validator"]["read_timeout"])
    total_timeout = float(config["validator"]["timeout"])
    if read_timeout < connect_timeout:
        config["validator"]["read_timeout"] = float(connect_timeout)
        read_timeout = float(connect_timeout)
    if total_timeout < read_timeout:
        config["validator"]["timeout"] = float(read_timeout)

    return config
