import time
import jwt
from unittest.mock import patch, MagicMock
from indexd.auth.drivers import request_auth_cache


def test_request_auth_cache_no_token_with_authorization_header():
    """
    Test caching behavior when no token is provided, but an Authorization header is present.
    The cache key should include the Authorization header, and subsequent calls with the same header
    should hit the cache.
    """
    calls = []

    @request_auth_cache(maximum_ttl_seconds=2)
    def dummy_func(x, token=None):
        calls.append(x)
        return x * 2

    # Mock flask.request.headers.get to return a specific Authorization header
    with patch("flask.request") as mock_request:
        mock_headers = {"Authorization": "Bearer test.jwt.token"}
        mock_request.headers = MagicMock()
        mock_request.headers.get = MagicMock(return_value=mock_headers["Authorization"])

        # Call without token, should use Authorization header in cache key
        result1 = dummy_func(5)
        assert result1 == 10
        assert calls == [5]

        # Second call, should use cache
        result2 = dummy_func(5)
        assert result2 == 10
        assert calls == [5]  # No new call, cache hit

        # Change Authorization header, should miss cache
        mock_request.headers.get = MagicMock(return_value="Bearer another.token")
        result3 = dummy_func(5)
        assert result3 == 10
        assert calls == [5, 5]  # New call, cache miss due to header change


def test_request_auth_cache_basic_auth_header():
    """
    Test caching behavior with Basic Auth header.
    The cache key should include the Basic Auth header, and subsequent calls with the same header
    should hit the cache.
    If the Basic Auth header changes, it should miss the cache.
    """
    calls = []

    @request_auth_cache(maximum_ttl_seconds=2)
    def dummy_func(x, token=None):
        calls.append(x)
        return x + 1

    with patch("flask.request") as mock_request:
        # Simulate Basic Auth header
        mock_headers = {"Authorization": "Basic dXNlcjpwYXNz"}
        mock_request.headers = MagicMock()
        mock_request.headers.get = MagicMock(return_value=mock_headers["Authorization"])

        # First call, should cache
        result1 = dummy_func(7)
        assert result1 == 8
        assert calls == [7]

        # Second call, should use cache
        result2 = dummy_func(7)
        assert result2 == 8
        assert calls == [7]

        # Change Basic Auth header, should miss cache
        mock_request.headers.get = MagicMock(return_value="Basic dXNlcjoxMjM0NQ==")
        result3 = dummy_func(7)
        assert result3 == 8
        assert calls == [7, 7]


def test_request_auth_cache_token_timeout(monkeypatch):
    """
    Test caching behavior with a token that has a TTL.
    The cache should expire after the TTL, and subsequent calls should hit the cache again.
    If the token is provided, it should be used to determine the cache expiration.
    If no token is provided, the maximum TTL should be used.
    """
    calls = []

    @request_auth_cache(maximum_ttl_seconds=5)
    def dummy_func(x, token=None):
        calls.append(x)
        return x * 2

    with patch("flask.request") as mock_request:

        # Simulate a JWT token with exp field 2 seconds from now
        exp_time = int(time.time()) + 2
        payload = {"exp": exp_time}
        secret = "your-secret-key"
        token = jwt.encode(payload, secret, algorithm="HS256")

        mock_headers = {"Authorization": f"Bearer {token}"}
        mock_request.headers = MagicMock()
        mock_request.headers.get = MagicMock(return_value=mock_headers["Authorization"])

        # First call, should cache
        result1 = dummy_func(5)
        assert result1 == 10
        assert calls == [5]

        # Second call, should use cache
        result2 = dummy_func(5)
        assert result2 == 10
        assert calls == [5]

        # Simulate time passing beyond TTL
        original_time = time.time()
        monkeypatch.setattr(time, "time", lambda: original_time + 3)

        # Third call, cache not should expire
        result3 = dummy_func(5)
        assert result3 == 10
        assert calls == [5]

        monkeypatch.setattr(time, "time", lambda: original_time + 10)

        # Fourth call, cache not should expire
        result4 = dummy_func(5)
        assert result4 == 10
        assert calls == [5, 5]


def test_request_auth_cache_token_with_expiry(monkeypatch):
    """
    Test caching behavior with a JWT token that has an 'exp' field.
    The cache should respect the token's expiration time.
    If the token is provided, it should be used to determine the cache expiration.
    If no token is provided, the maximum TTL should be used.
    """

    calls = []

    # Simulate a JWT token with exp field 2 seconds from now

    exp_time = int(time.time()) + 2
    payload = {"exp": exp_time}
    secret = "your-secret-key"
    token = jwt.encode(payload, secret, algorithm="HS256")

    @request_auth_cache(maximum_ttl_seconds=5)
    def dummy_func(x, token=None):
        calls.append(x)
        return x * 3

    with patch("flask.request") as mock_request:
        mock_request.headers = MagicMock()
        mock_request.headers.get = MagicMock(return_value=None)

        # First call, should cache
        result1 = dummy_func(4, token=token)
        assert result1 == 12
        assert calls == [4]

        # Second call, within token expiry, should use cache
        result2 = dummy_func(4, token=token)
        assert result2 == 12
        assert calls == [4]

        # Simulate time passing beyond token expiry but within cache TTL
        original_time = time.time()
        monkeypatch.setattr(time, "time", lambda: original_time + 3)

        # Third call, token expired, cache should miss
        result3 = dummy_func(4, token=token)
        assert result3 == 12
        assert calls == [4, 4]
