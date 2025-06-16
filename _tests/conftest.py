"""
Shared pytest fixtures and configuration for Valkey tests
"""

import asyncio
import sys
import pytest
import pytest_asyncio

# ! Windows: Use SelectorEventLoopPolicy for pytest-asyncio compatibility
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from unittest.mock import patch
import socket
import time

# Add this fixture to provide event_loop - use the built-in function event loop
@pytest_asyncio.fixture
def event_loop(request):
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session", autouse=True)
def ensure_valkey_server():
    """Ensure Valkey/Redis server is running on localhost:6379 or 127.0.0.1:6379 before tests."""
    import socket
    def is_port_open(host, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            try:
                s.connect((host, port))
                return True
            except Exception:
                return False
    running = is_port_open("localhost", 6379) or is_port_open("127.0.0.1", 6379)
    if not running:
        raise RuntimeError("No Redis/Valkey server running on localhost:6379. Please start docker-redis-1.")
    yield

from valkey.asyncio import Valkey
from valkey.exceptions import TimeoutError, ValkeyError

from app.core.valkey_core.config import ValkeyConfig
from app.core.valkey_core.client import ValkeyClient

@pytest_asyncio.fixture
async def valkey_client(event_loop):  # Make sure to accept event_loop parameter
    """Yields a connected production ValkeyClient instance for integration tests with metrics disabled."""
    # * Disable Prometheus metrics to avoid duplicate timeseries errors
    old_metrics = getattr(ValkeyConfig, "VALKEY_METRICS_ENABLED", None)
    ValkeyConfig.VALKEY_METRICS_ENABLED = False
    client = ValkeyClient()
    await client.get_client()  # Ensure connection is established
    try:
        yield client
    finally:
        await client.shutdown()
        if old_metrics is not None:
            ValkeyConfig.VALKEY_METRICS_ENABLED = old_metrics

from app.core.valkey_core.cache.valkey_cache import ValkeyCache
from app.core.valkey_core.limiting.rate_limit import check_rate_limit

# Remove event_loop parameter from flush_valkey since it doesn't need it directly
@pytest_asyncio.fixture(autouse=True)
async def flush_valkey(valkey_client):
    """Flushes the Valkey database before and after each test for isolation."""
    await valkey_client.flushdb()
    yield
    await valkey_client.flushdb()

@pytest_asyncio.fixture
async def valkey_cache(event_loop):
    """Pre-configured ValkeyCache instance for tests. Cleans up test namespace after each test."""
    # Pass event_loop if ValkeyCache supports it, otherwise just use event_loop in fixture
    try:
        cache = ValkeyCache(loop=event_loop)
    except TypeError:
        cache = ValkeyCache()
    yield cache
    await cache.clear_namespace("test_")

@pytest.fixture
def rate_limit_checker():
    """Rate limit checker function with default values (Valkey)"""
    async def checker(endpoint, identifier, limit=10, window=60):
        return await check_rate_limit(endpoint, identifier, limit, window)
    return checker

@pytest.fixture(autouse=True)
def reset_valkey_config():
    """Reset ValkeyConfig between tests"""
    original_attrs = {k: v for k, v in vars(ValkeyConfig).items() if not k.startswith('__')}
    yield
    # Restore only the attributes that were present originally
    for k in list(vars(ValkeyConfig).keys()):
        if not k.startswith('__'):
            if k in original_attrs:
                setattr(ValkeyConfig, k, original_attrs[k])
            else:
                delattr(ValkeyConfig, k)

@pytest.fixture
def mock_time():
    """Mock time module for time-sensitive tests"""
    with patch("time.time") as mock_time:
        mock_time.return_value = 0
        yield mock_time
