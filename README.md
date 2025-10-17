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

The configuration file is optional. When omitted, sensible defaults defined in `proxy_collector.config` are used. 

### Environment Variables

The following environment variables can be used to override key settings at runtime:

**GitHub API Configuration:**
- `GITHUB_TOKEN` – GitHub personal access token (recommended for higher rate limits)
- `PROXY_MAX_QUERIES` – Maximum number of search queries to execute (default: 12)
- `PROXY_SEARCH_CONCURRENCY` – Concurrent query execution threads (default: 1)
- `PROXY_GITHUB_MAX_RETRIES` – Maximum retry attempts for failed requests (default: 2)
- `PROXY_REQUESTS_PER_MINUTE` – Maximum requests per minute (default: 5)
- `PROXY_REQUEST_MIN_DELAY` – Minimum delay between requests in seconds (default: 6.0)
- `PROXY_REQUEST_MAX_DELAY` – Maximum delay between requests in seconds (default: 10.0)
- `PROXY_SECONDARY_RATE_LIMIT_COOLDOWN` – Cooldown time for secondary rate limits (default: 300.0)
- `PROXY_INITIAL_BACKOFF_SECONDS` – Initial backoff time for exponential backoff (default: 60.0)
- `PROXY_MAX_BACKOFF_SECONDS` – Maximum backoff time (default: 3600.0)

**Validator Configuration:**
- `PROXY_VALIDATOR_WORKERS` – Concurrent validation threads (default: 64)

### Rate Limiting

The system implements conservative rate limiting to avoid triggering GitHub's secondary rate limits:

- **Sequential execution:** Queries are executed sequentially (concurrency=1) to prevent request bursts
- **Request throttling:** 5 requests per minute with 6-10 second randomized delays between requests
- **Exponential backoff:** Failed requests use exponential backoff with jitter
- **Secondary rate limit detection:** Automatically detects and handles secondary rate limits with 5-minute cooldowns
- **Token support:** Using a GitHub token increases rate limits from 10 to 30 requests per minute

To avoid rate limit issues, it's strongly recommended to:
1. Set a `GITHUB_TOKEN` environment variable
2. Keep `PROXY_MAX_QUERIES` at 10-15 or lower
3. Use sequential execution (concurrency=1)

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
