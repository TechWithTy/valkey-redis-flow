"""
Production-grade Valkey performance tests with:
- Comprehensive metrics using Prometheus
- Failover scenario testing
- Circuit breaker integration
- Performance benchmarking
"""

import asyncio
import logging
import time
from datetime import datetime
from unittest.mock import patch

import pytest
from prometheus_client import REGISTRY, Counter, Gauge, Histogram

from app.core.valkey.client import client as valkey_client

logger = logging.getLogger(__name__)

# Prometheus metrics
VALKEY_OPS_METRICS = {
    "throughput": Gauge("valkey_ops_per_sec", "Operations per second", ["operation"]),
    "latency": Histogram(
        "valkey_op_latency",
        "Operation latency seconds",
        ["operation"],
        buckets=[0.001, 0.01, 0.1, 0.5],
    ),
    "errors": Counter("valkey_op_errors", "Operation errors", ["operation", "type"]),
}


@pytest.fixture(autouse=True)
def reset_prometheus():
    """Reset Prometheus metrics between tests"""
    for collector in list(REGISTRY._collector_to_names):
        if collector not in REGISTRY._names_to_collectors.values():
            REGISTRY.unregister(collector)


@pytest.mark.asyncio
async def test_failover_scenarios():
    """Test performance during failover scenarios"""
    client = valkey_client

    with patch("valkey.asyncio.cluster.ValkeyCluster.execute_command") as mock_exec:
        mock_exec.side_effect = [
            Exception("Node down"),
            "OK",
            Exception("Cluster reconfigured"),
            "OK",
        ]

        start = time.time()
        try:
            await client.set("failover_key", "value")
            VALKEY_OPS_METRICS["errors"].labels(operation="set", type="failover").inc()
        except Exception:
            pass
        duration = time.time() - start
        VALKEY_OPS_METRICS["latency"].labels(operation="set").observe(duration)

        # Should recover and succeed
        assert await client.set("failover_key", "value") == "OK"


@pytest.mark.asyncio
async def test_circuit_breaker_performance():
    """Test performance with circuit breaker engaged"""
    client = valkey_client

    with patch(
        "app.core.valkey.circuit_breaker.CircuitBreaker.is_open", return_value=True
    ):
        start = time.time()
        try:
            await client.set("cb_key", "value")
        except Exception as e:
            VALKEY_OPS_METRICS["errors"].labels(
                operation="set", type="circuit_breaker"
            ).inc()
            latency = time.time() - start
            VALKEY_OPS_METRICS["latency"].labels(operation="circuit_breaker").observe(
                latency
            )


@pytest.mark.asyncio
async def test_throughput_under_stress():
    """Measure throughput under simulated stress"""
    client = valkey_client
    ops = 0
    start = time.time()

    while time.time() - start < 5:  # Run for 5 seconds
        try:
            await client.set(f"stress_{ops}", "value")
            ops += 1
        except Exception as e:
            VALKEY_OPS_METRICS["errors"].labels(operation="set", type="stress").inc()

    throughput = ops / (time.time() - start)
    VALKEY_OPS_METRICS["throughput"].labels(operation="set").set(throughput)

    assert throughput > 5000  # Minimum ops/sec threshold


@pytest.mark.asyncio
async def test_latency_distribution():
    """Measure latency distribution under load"""
    client = valkey_client
    latencies = []

    for i in range(100):
        start = time.time()
        await client.set(f"latency_{i}", "value")
        latency = time.time() - start
        latencies.append(latency)
        VALKEY_OPS_METRICS["latency"].labels(operation="set").observe(latency)

    avg = sum(latencies) / len(latencies)
    assert avg < 0.01  # 10ms avg latency threshold
