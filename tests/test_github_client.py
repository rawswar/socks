import unittest
from unittest.mock import Mock, MagicMock, patch
from proxy_collector.github_client import GitHubClient


class TestGitHubClientFetchStrategies(unittest.TestCase):
    """
    Test suite for GitHub file content fetching with proper fallback strategies.
    
    These tests verify that the client:
    1. Prioritizes Contents API (most reliable)
    2. Falls back to HTML-to-raw URL conversion
    3. Uses default_branch as last resort
    4. Does NOT use blob SHA in raw URLs (which causes 404s)
    """

    def setUp(self):
        """Set up test client with mocked session."""
        self.client = GitHubClient(
            base_url="https://api.github.com",
            token="test_token",
            request_timeout=10,
            max_retries=0,
            requests_per_minute=60,
            min_request_interval=0.0,
            max_request_interval=0.0,
        )
        # Mock the rate limiter to avoid delays in tests
        self.client.rate_limiter.wait_if_needed = Mock()
        self.client._respect_rate_limits = Mock()

    def _create_search_item(
        self,
        path="proxies.txt",
        repo_name="user/repo",
        default_branch="main",
        sha="abc123blob",
        html_url=None,
        api_url=None,
        download_url=None,
    ):
        """Helper to create a mock search result item."""
        if html_url is None:
            html_url = f"https://github.com/{repo_name}/blob/{default_branch}/{path}"
        if api_url is None:
            api_url = f"https://api.github.com/repos/{repo_name}/contents/{path}"
        
        return {
            "path": path,
            "sha": sha,
            "url": api_url,
            "html_url": html_url,
            "download_url": download_url,
            "repository": {
                "full_name": repo_name,
                "default_branch": default_branch,
            },
        }

    @patch.object(GitHubClient, "_request")
    def test_contents_api_success_base64_response(self, mock_request):
        """Test Case A: Contents API succeeds with base64-encoded JSON response."""
        # Arrange
        item = self._create_search_item()
        expected_content = "192.168.1.1:8080\n192.168.1.2:8081\n"
        import base64
        encoded = base64.b64encode(expected_content.encode()).decode()
        
        mock_request.return_value = (
            {
                "content": encoded,
                "encoding": "base64",
            },
            200,
        )
        
        # Act
        result = self.client.fetch_content_from_search_item(item)
        
        # Assert
        self.assertEqual(result, expected_content)
        self.assertEqual(mock_request.call_count, 1)

    @patch.object(GitHubClient, "fetch_file_content")
    @patch.object(GitHubClient, "_request")
    def test_html_raw_fallback_success(self, mock_request, mock_fetch):
        """Test Case B: Contents API fails, HTML->raw conversion succeeds."""
        # Arrange
        item = self._create_search_item(
            html_url="https://github.com/user/repo/blob/main/proxies.txt"
        )
        expected_content = "192.168.1.1:8080\n"
        
        # Mock: Contents API fails, raw URL succeeds
        mock_request.return_value = (None, 500)  # API fails
        mock_fetch.return_value = (expected_content, 200)  # Raw URL succeeds
        
        # Act
        result = self.client.fetch_content_from_search_item(item)
        
        # Assert
        self.assertEqual(result, expected_content)
        
        # Verify raw URL was constructed correctly (no /blob/)
        mock_fetch.assert_called()
        raw_url = mock_fetch.call_args[0][0]
        self.assertIn("raw.githubusercontent.com", raw_url)
        self.assertNotIn("/blob/", raw_url)
        self.assertIn("user/repo/main/proxies.txt", raw_url)

    @patch.object(GitHubClient, "fetch_file_content")
    @patch.object(GitHubClient, "_request")
    def test_default_branch_fallback_success(self, mock_request, mock_fetch):
        """Test Case C: Only default_branch fallback succeeds."""
        # Arrange
        item = self._create_search_item(
            html_url=None,  # No HTML URL
            default_branch="master",
        )
        expected_content = "192.168.1.1:8080\n"
        
        # Mock: Contents API fails, no HTML URL, default branch succeeds
        mock_request.return_value = (None, 500)  # API fails
        mock_fetch.return_value = (expected_content, 200)  # Default branch succeeds
        
        # Act
        result = self.client.fetch_content_from_search_item(item)
        
        # Assert
        self.assertEqual(result, expected_content)
        
        # Verify correct URL was constructed with default_branch
        mock_fetch.assert_called()
        raw_url = mock_fetch.call_args[0][0]
        self.assertIn("raw.githubusercontent.com", raw_url)
        self.assertIn("user/repo/master/proxies.txt", raw_url)

    @patch.object(GitHubClient, "fetch_file_content")
    @patch.object(GitHubClient, "_request")
    def test_no_blob_sha_in_raw_urls(self, mock_request, mock_fetch):
        """Test that blob SHA is NOT used to construct raw URLs."""
        # Arrange
        item = self._create_search_item(
            sha="abc123blobsha",  # This is a blob SHA
            html_url=None,
            default_branch="main",
        )
        
        # Mock: All methods fail (we're testing what's NOT called)
        mock_request.return_value = (None, 500)
        mock_fetch.return_value = (None, 404)
        
        # Act
        result = self.client.fetch_content_from_search_item(item)
        
        # Assert
        self.assertIsNone(result)
        
        # Verify blob SHA was never used in any URL
        for call in mock_fetch.call_args_list:
            url = call[0][0] if call[0] else ""
            self.assertNotIn("abc123blobsha", url)

    @patch.object(GitHubClient, "fetch_file_content")
    @patch.object(GitHubClient, "_request")
    def test_all_strategies_fail(self, mock_request, mock_fetch):
        """Test that None is returned when all strategies fail."""
        # Arrange
        item = self._create_search_item()
        
        # Mock: Everything fails
        mock_request.return_value = (None, 404)
        mock_fetch.return_value = (None, 404)
        
        # Act
        result = self.client.fetch_content_from_search_item(item)
        
        # Assert
        self.assertIsNone(result)

    @patch.object(GitHubClient, "fetch_file_content")
    @patch.object(GitHubClient, "_request")
    def test_download_url_last_resort(self, mock_request, mock_fetch):
        """Test that download_url is used as last resort."""
        # Arrange
        download_url = "https://raw.githubusercontent.com/user/repo/main/proxies.txt"
        item = self._create_search_item(
            html_url=None,
            download_url=download_url,
        )
        expected_content = "192.168.1.1:8080\n"
        
        # Mock: API fails, no HTML, default branch fails, download_url succeeds
        mock_request.return_value = (None, 404)
        mock_fetch.side_effect = [(None, 404), (expected_content, 200)]  # First call fails, second succeeds
        
        # Act
        result = self.client.fetch_content_from_search_item(item)
        
        # Assert
        self.assertEqual(result, expected_content)
        
        # Verify download_url was used
        calls = [call[0][0] for call in mock_fetch.call_args_list]
        self.assertIn(download_url, calls)

    def test_raw_url_from_html_conversion(self):
        """Test HTML URL to raw URL conversion logic."""
        # Test valid conversion
        html_url = "https://github.com/owner/repo/blob/main/path/to/file.txt"
        raw_url = self.client._raw_url_from_html(html_url)
        expected = "https://raw.githubusercontent.com/owner/repo/main/path/to/file.txt"
        self.assertEqual(raw_url, expected)
        
        # Test with query parameters (should be stripped)
        html_url = "https://github.com/owner/repo/blob/main/file.txt?ref=abc"
        raw_url = self.client._raw_url_from_html(html_url)
        expected = "https://raw.githubusercontent.com/owner/repo/main/file.txt"
        self.assertEqual(raw_url, expected)
        
        # Test invalid URLs
        self.assertIsNone(self.client._raw_url_from_html(None))
        self.assertIsNone(self.client._raw_url_from_html(""))
        self.assertIsNone(self.client._raw_url_from_html("https://example.com/file.txt"))
        self.assertIsNone(self.client._raw_url_from_html("https://github.com/owner/repo"))

    @patch.object(GitHubClient, "_request")
    def test_contents_api_with_download_url(self, mock_request):
        """Test Contents API returns download_url which is fetched."""
        # Arrange
        item = self._create_search_item()
        expected_content = "192.168.1.1:8080\n"
        download_url = "https://raw.githubusercontent.com/user/repo/main/proxies.txt"
        
        # Mock: API returns JSON with download_url
        mock_request.side_effect = [
            ({"download_url": download_url}, 200),  # First call returns API response
            (expected_content, 200),  # Second call fetches the download_url
        ]
        
        # Act
        result = self.client.fetch_content_from_search_item(item)
        
        # Assert
        self.assertEqual(result, expected_content)
        self.assertEqual(mock_request.call_count, 2)


if __name__ == "__main__":
    unittest.main()
