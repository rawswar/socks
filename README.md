# SOCKS5 Proxy Collector and Validator

This project implements an automated pipeline for discovering, validating, and publishing SOCKS5 proxies sourced from public GitHub repositories.

## Components

The system is composed of five primary components:

1. **Coordinator** – Orchestrates the end-to-end workflow (collect → validate → publish) and manages concurrency.
2. **QueryStrategyGenerator** – Produces high-signal search queries for the GitHub code search API.
3. **GitHubClient** – Handles rate-aware GitHub API requests with ETag caching and retry logic.
4. **ProxyValidator** – Performs anonymous SOCKS5 connectivity checks concurrently and records latency.
5. **ResultPublisher** – Emits validated proxies in multiple formats (`.txt`, `.json`, `.zip`).

## Running the Pipeline

```bash
python main.py --config path/to/config.json
```

The configuration file is optional. When omitted, sensible defaults defined in `proxy_collector.config` are used. Environment variables such as `GITHUB_TOKEN`, `PROXY_MAX_QUERIES`, `PROXY_SEARCH_CONCURRENCY`, and `PROXY_VALIDATOR_WORKERS` can be used to override key settings at runtime.

## Output

Validated proxies are written to the `output/` directory:

- `proxies.txt` – plain list of `ip:port` pairs
- `proxies.json` – structured details including latency
- `proxies.zip` – archive containing both text and JSON outputs

## Dependencies

Install the runtime dependencies with:

```bash
pip install -r requirements.txt
```

The project depends on `requests` and `PySocks` for SOCKS5 support.
