# GitHub Client Tests

## Test Coverage

This directory contains unit tests for the GitHub client, specifically focusing on the file content fetching strategies that were fixed to avoid 404 errors from using blob SHAs in raw URLs.

## Running Tests

```bash
python -m unittest tests.test_github_client -v
```

## Test Cases

1. **test_contents_api_success_base64_response** - Verifies Contents API with base64 encoding works
2. **test_html_raw_fallback_success** - Verifies HTML->raw URL conversion fallback
3. **test_default_branch_fallback_success** - Verifies default_branch fallback strategy
4. **test_no_blob_sha_in_raw_urls** - Ensures blob SHA is never used in raw URLs (critical!)
5. **test_all_strategies_fail** - Verifies graceful failure when all strategies fail
6. **test_download_url_last_resort** - Verifies download_url is used as last resort
7. **test_raw_url_from_html_conversion** - Tests HTML URL to raw URL conversion logic
8. **test_contents_api_with_download_url** - Tests Contents API returning download_url

## Key Fix

The main fix implemented was to change the file fetching priority to:
1. Contents API (most reliable)
2. HTML-to-raw URL conversion
3. Default branch fallback

**Previously**, the code tried to use `item['sha']` (which is a blob SHA) to construct raw URLs, causing mass 404 errors. The fix explicitly avoids using blob SHA and prioritizes the Contents API instead.
