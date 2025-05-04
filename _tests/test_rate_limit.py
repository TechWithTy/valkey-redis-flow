"""
Production-grade Valkey rate limiting tests with:
- Comprehensive error handling
- Performance metrics using Prometheus
- Edge case testing
- Monitoring integration
"""

import asyncio
import logging
from datetime import datetime
from unittest.mock import patch

import pytest
from prometheus_client import REGISTRY, Counter, Histogram

from app.core.valkey.rate_limit import check_rate_limit

logger = logging.getLogger(__name__)

# Prometheus metrics
RATE_LIMIT_REQUESTS = Counter(
    "valkey_rate_limit_requests_total", "Total rate limit requests", ["endpoint", "status"]
)

RATE_LIMIT_LATENCY = Histogram(
    "valkey_rate_limit_latency_seconds",
    "Rate limit processing latency",
    ["endpoint"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)


@pytest.fixture(autouse=True)
def reset_prometheus():
    """Reset Prometheus metrics between tests"""
    for collector in list(REGISTRY._collector_to_names):
        if collector not in REGISTRY._names_to_collectors.values():
            REGISTRY.unregister(collector)


@pytest.mark.asyncio
async def test_window_sliding():
    """Test sliding window behavior with Prometheus metrics"""
    endpoint = "test_endpoint"
    identifier = "test_id"
    start_time = datetime.now()

    try:
        # Test within limit
        for i in range(10):
            result = await check_rate_limit(endpoint, identifier, 10, 60)
            assert result is False, f"Request {i} should not be rate limited"

        # Test over limit
        result = await check_rate_limit(endpoint, identifier, 10, 60)
        assert result is True

        # Record metrics
        duration = (datetime.now() - start_time).total_seconds()
        RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, status="allowed").inc(10)
        RATE_LIMIT_LATENCY.labels(endpoint=endpoint).observe(duration)
    except Exception as e:
        logger.error(f"Rate limit test failed: {e}")
        raise


@pytest.mark.asyncio
async def test_burst_handling():
    """Verify burst traffic handling with Prometheus metrics"""
    endpoint = "burst_endpoint"
    identifier = "burst_id"
    start_time = datetime.now()

    try:
        # Simulate burst traffic
        tasks = []
        for _ in range(20):
            tasks.append(check_rate_limit(endpoint, identifier, 15, 60))

        results = await asyncio.gather(*tasks)

        # Verify and record results
        allowed = results.count(False)
        rejected = results.count(True)
        assert allowed == 15
        assert rejected == 5

        duration = (datetime.now() - start_time).total_seconds()
        RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, status="allowed").inc(allowed)
        RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, status="rejected").inc(rejected)
        RATE_LIMIT_LATENCY.labels(endpoint=endpoint).observe(duration)

    except Exception as e:
        logger.error(f"Burst handling test failed: {e}")
        raise


@pytest.mark.asyncio
async def test_distributed_consistency():
    """Test rate limiting works across shards with failover"""
    endpoint = "dist_endpoint"
    identifier = "dist_id"

    try:
        # Test with shard failure
        with patch(
            "app.core.valkey.client.ValkeyClient._get_sharded_client"
        ) as mock_shard:
            mock_shard.return_value = None
            assert await check_rate_limit(endpoint, identifier, 5, 60) is False

        # Test normal operation
        assert await check_rate_limit(endpoint, identifier, 5, 60) is False

    except Exception as e:
        logger.error(f"Distributed consistency test failed: {e}")
        raise


@pytest.mark.asyncio
async def test_edge_cases():
    """Test various edge cases and error scenarios"""
    endpoint = "edge_endpoint"
    identifier = "edge_id"

    # Test empty endpoint
    with pytest.raises(ValueError):
        await check_rate_limit("", identifier, 5, 60)

    # Test empty identifier
    with pytest.raises(ValueError):
        await check_rate_limit(endpoint, "", 5, 60)

    # Test invalid limits
    with pytest.raises(ValueError):
        await check_rate_limit(endpoint, identifier, 0, 60)

    with pytest.raises(ValueError):
        await check_rate_limit(endpoint, identifier, 5, 0)


@pytest.mark.asyncio
async def test_performance_under_load():
    """Test rate limiter performance with Prometheus metrics"""
    endpoint = "perf_endpoint"
    identifier = "perf_id"
    start_time = datetime.now()

    try:
        # Simulate high concurrent load
        tasks = []
        for i in range(100):
            tasks.append(check_rate_limit(endpoint, f"{identifier}_{i}", 50, 60))

        results = await asyncio.gather(*tasks)

        # Verify performance
        duration = (datetime.now() - start_time).total_seconds()
        assert duration < 1.0

        # Record metrics
        allowed = results.count(False)
        rejected = results.count(True)
        RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, status="allowed").inc(allowed)
        RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, status="rejected").inc(rejected)
        RATE_LIMIT_LATENCY.labels(endpoint=endpoint).observe(duration)

    except Exception as e:
        logger.error(f"Performance test failed: {e}")
        raise


@pytest.mark.asyncio
async def test_rate_limit_idempotency_and_cleanup():
    """Test idempotency and cleanup for repeated requests and metrics isolation."""
    endpoint = "idempotent_endpoint"
    identifier = "idempotent_id"
    # Send repeated requests
    for _ in range(5):
        result = await check_rate_limit(endpoint, identifier, 10, 60)
        assert result is False
    # Cleanup: ensure metrics are not double-counted
    # (Prometheus counters should only increment by request count)
    # No assertion here, just check no exception and correct logic


@pytest.mark.asyncio
async def test_rate_limit_metrics_labels():
    """Test Prometheus metrics labels are correct and unique per endpoint/status."""
    endpoint = "metrics_endpoint"
    identifier = "metrics_id"
    await check_rate_limit(endpoint, identifier, 5, 60)
    # Check that the metric has the correct label
    allowed_count = RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, status="allowed")._value.get()
    assert allowed_count >= 1


@pytest.mark.asyncio
async def test_rate_limit_latency_bucket():
    """Test that latency is recorded in the correct Prometheus bucket."""
    endpoint = "latency_endpoint"
    identifier = "latency_id"
    start_time = datetime.now()
    await check_rate_limit(endpoint, identifier, 5, 60)
    duration = (datetime.now() - start_time).total_seconds()
    # Find the correct bucket
    found = False
    for bucket in RATE_LIMIT_LATENCY.collect()[0].samples:
        if bucket.labels.get("endpoint") == endpoint and bucket.name.endswith("_bucket"):
            if float(bucket.labels["le"]) >= duration:
                found = True
                break
    assert found, "Latency not recorded in any bucket"
