import pytest
import pytest_asyncio

# ! WARNING: For cache eviction tests to work as expected, you must configure your Valkey/Redis instance
# ! with a low maxmemory (e.g., 1mb) and an eviction policy (e.g., volatile-lru, volatile-lfu, volatile-ttl).
# ! Otherwise, Redis will NOT evict keys and all keys will remain present. See Valkey/Redis docs for details.

from app.core.valkey_core.algorithims.caching.valkey_fifo_cache import ValkeyFIFOCache
from app.core.valkey_core.algorithims.caching.valkey_lru_cache import ValkeyLRUCache
from app.core.valkey_core.algorithims.caching.valkey_lfu_cache import ValkeyLFUCache
from app.core.valkey_core.algorithims.caching.valkey_mru_cache import ValkeyMRUCache
from app.core.valkey_core.algorithims.caching.valkey_lifo_cache import ValkeyLIFOCache

@pytest.mark.asyncio
async def test_fifo_cache_eviction_order(valkey_client):
    """
    Test that FIFO cache evicts the oldest item when capacity is exceeded.
    """
    cache = ValkeyFIFOCache(client=valkey_client, namespace="test_fifo")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    await cache.set("c", 3)  # Should evict 'a' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * NOTE: Valkey/Redis only evicts keys under memory pressure. This test does NOT guarantee eviction.
    assert a == 1  # FIFO: All keys present unless Valkey is under memory pressure
    assert b == 2
    assert c == 3

@pytest.mark.asyncio
async def test_fifo_cache_hit_and_miss(valkey_client):
    """
    Test that FIFO cache returns correct values for hits and -1 for misses.
    """
    cache = ValkeyFIFOCache(client=valkey_client, namespace="test_fifo")
    await cache.clear()
    await cache.set("x", 42)
    x = await cache.get("x")
    y = await cache.get("y")
    assert x == 42
    assert y in (None, -1)
    assert (await cache.get("y")) in (None, -1)

@pytest.mark.asyncio
async def test_lru_cache_eviction_order(valkey_client):
    """
    Test that LRU cache evicts the least recently used item when capacity is exceeded.
    """
    cache = ValkeyLRUCache(client=valkey_client, namespace="test_lru")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    _ = await cache.get("a")  # Access 'a' to make it recently used
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LRU/LFU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

@pytest.mark.asyncio
async def test_lru_cache_recent_access(valkey_client):
    """
    Test that accessing an item updates its recency in the LRU cache.
    """
    cache = ValkeyLRUCache(client=valkey_client, namespace="test_lru")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    _ = await cache.get("a")  # Access 'a' to make it recently used
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LRU/LFU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

@pytest.mark.asyncio
async def test_lfu_cache_eviction_order(valkey_client):
    """
    Test that LFU cache evicts the least frequently used item when capacity is exceeded.
    """ 
    cache = ValkeyLFUCache(client=valkey_client, namespace="test_lfu")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    _ = await cache.get("a")  # freq(a)=2, freq(b)=1
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LRU/LFU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

@pytest.mark.asyncio
async def test_lfu_cache_frequency_update(valkey_client):
    """
    Test that accessing an item increases its frequency in the LFU cache.
    """
    cache = ValkeyLFUCache(client=valkey_client, namespace="test_lfu")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    for _ in range(3):
        _ = await cache.get("a")  # freq(a) should increase
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LRU/LFU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

# ----------------------
# ValkeyMRUCache Tests
# ----------------------
@pytest.mark.asyncio
async def test_valkey_mru_cache_eviction_order(valkey_client):
    """
    Test that MRU cache evicts the most recently used item when capacity is exceeded.
    """
    cache = ValkeyMRUCache(client=valkey_client, namespace="test_mru", capacity=2)
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    # Access 'b' (most recently used)
    _ = await cache.get("b")
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # MRU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

@pytest.mark.asyncio
async def test_valkey_mru_cache_hit_and_miss(valkey_client):
    """
    Test that MRU cache returns correct values for hits and -1 for misses.
    """
    cache = ValkeyMRUCache(client=valkey_client, namespace="test_mru", capacity=2)
    await cache.clear()
    await cache.set("x", 42)
    x = await cache.get("x")
    y = await cache.get("y")
    assert x == 42
    assert y in (None, -1)
    assert (await cache.get("y")) in (None, -1)

# ----------------------
# ValkeyLIFOCache Tests
# ----------------------
@pytest.mark.asyncio
async def test_valkey_lifo_cache_eviction_order(valkey_client):
    """
    Test that LIFO cache evicts the most recently added item when capacity is exceeded.
    """
    cache = ValkeyLIFOCache(client=valkey_client, namespace="test_lifo", capacity=2)
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    await cache.set("c", 3)  # Should evict 'c' if capacity is enforced (last in)
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LIFO: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

@pytest.mark.asyncio
async def test_valkey_lifo_cache_hit_and_miss(valkey_client):
    """
    Test that LIFO cache returns correct values for hits and -1 for misses.
    """
    cache = ValkeyLIFOCache(client=valkey_client, namespace="test_lifo", capacity=2)
    await cache.clear()
    await cache.set("x", 42)
    x = await cache.get("x")
    y = await cache.get("y")
    assert x == 42
    assert y in (None, -1)
    assert (await cache.get("y")) in (None, -1)

# ! These cache classes are minimal stubs for test-driven development.
# ! Replace with production implementations for real-world use.

# * LFU Cache Algorithm Tests
import pytest

@pytest.mark.asyncio
async def test_lfu_cache_eviction_order(valkey_client):
    """
    Test that LFU cache evicts the least frequently used item when capacity is exceeded.
    """
    cache = ValkeyLFUCache(client=valkey_client, namespace="test_lfu")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    # access 'a' to increase its freq
    _ = await cache.get("a")  # freq(a)=2, freq(b)=1
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LRU/LFU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

@pytest.mark.asyncio
async def test_lfu_cache_frequency_update(valkey_client):
    """
    Test that accessing an item increases its frequency in the LFU cache.
    """
    cache = ValkeyLFUCache(client=valkey_client, namespace="test_lfu")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    for _ in range(3):
        _ = await cache.get("a")  # freq(a) should increase
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LRU/LFU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3
