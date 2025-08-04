"""
VALKEY Cache Utilities

Core VALKEY functionality including:
- Connection management
- Basic caching operations
- Cache statistics
"""

import hashlib
import json
import logging
import asyncio
from collections.abc import Callable
from typing import Any

from ..client import client as valkey_client

logger = logging.getLogger(__name__)

class ValkeyCache:
    """
    Async wrapper for VALKEY cache operations. Provides get, set, delete, and composite cache methods.
    Reuses the core async functions for all logic.
    """
    def __init__(self, client=valkey_client):
        # Accepts either a ValkeyClient (wrapper) or a raw async client
        self._client = client

    async def _get_raw_client(self):
        # If self._client is a ValkeyClient, get the underlying async client
        if hasattr(self._client, 'get_client') and callable(self._client.get_client):
            return await self._client.get_client()
        return self._client

    async def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the cache"""
        try:
            raw_client = await self._get_raw_client()
            value = await raw_client.get(key)
            if value is None:
                logger.debug(f"Cache miss for key: {key}")
                return default
                
            logger.debug(f"Cache hit for key: {key}")
            if isinstance(value, bytes):
                return value.decode('utf-8')
            return value
        except Exception as e:
            logger.warning(f"Error retrieving from VALKEY cache: {str(e)}")
            return default

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set a value in the cache with optional TTL"""
        try:
            raw_client = await self._get_raw_client()
            await raw_client.set(key, value, ex=ttl)
            logger.debug(f"Cache set for key: {key}")
        except Exception as e:
            logger.warning(f"Error setting VALKEY cache: {str(e)}")

    async def delete(self, key: str) -> bool:
        """Delete a key from the cache"""
        try:
            raw_client = await self._get_raw_client()
            result = await raw_client.delete(key)
            success = bool(result)
            logger.debug(f"Cache delete for key: {key}, success: {success}")
            return success
        except Exception as e:
            logger.warning(f"Error deleting from VALKEY cache: {str(e)}")
            return False

    async def get_or_set(self, key: str, func: Callable[[], Any], ttl: int | None = None) -> Any:
        """Get a value from cache or compute and store it if not found"""
        return await get_or_set_cache(key, func, ttl)

    def cache_result(self, ttl: int | None = None, key_prefix: str = ""):
        """Decorator for caching function results"""
        return cache_result(ttl, key_prefix)


async def get_cached_result(key: str, default: Any = None) -> Any:
    """
    Get a result from the VALKEY cache.
    Args:
        key: The cache key to retrieve
        default: Value to return if key is not found or error occurs
    Returns:
        Cached value, or default if not found
    """
    try:
        value = await valkey_client.get(key)
        if value is None:
            logger.debug(f"Cache miss for key: {key}")
            return default
            
        logger.debug(f"Cache hit for key: {key}")
        return value
    except Exception as e:
        logger.warning(f"Error retrieving from VALKEY cache: {str(e)}")
        return default


async def invalidate_cache_key(key: str) -> bool:
    """
    Invalidate a specific cache key in VALKEY.
    Args:
        key: The cache key to invalidate
    Returns:
        True if the key was found and deleted, False otherwise
    """
    try:
        result = bool(await valkey_client.delete(key))
        logger.debug(f"Cache invalidate for key: {key}, success: {result}")
        return result
    except Exception as e:
        logger.warning(f"Error invalidating VALKEY cache: {str(e)}")
        return False


async def get_or_set_cache(
    key: str, func: Callable[[], Any], ttl: int | None = None
) -> Any:
    """
    Get a value from VALKEY, or compute and store it if not found.
    Args:
        key: The cache key to retrieve or store
        func: Function to call if the key is not in the cache
        ttl: Optional cache expiration (Time To Live) in seconds
    Returns:
        The cached or computed value
    """
    try:
        value = await valkey_client.get(key)
        if value is not None:
            logger.debug(f"Cache hit for key: {key}")
            return value
            
        logger.debug(f"Cache miss for key: {key}")
        result = await func() if asyncio.iscoroutinefunction(func) else func()
        await valkey_client.set(key, result, ex=ttl)
        return result
    except Exception as e:
        logger.error(f"Error computing or caching result in VALKEY: {str(e)}")
        raise


def cache_result(ttl: int | None = None, key_prefix: str = ""):
    """
    Decorator that caches the result of a function based on its arguments using VALKEY.
    Args:
        ttl: Optional cache expiration (Time To Live) in seconds
        key_prefix: Optional prefix for the cache key
    Returns:
        Decorated function that uses VALKEY caching
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        async def wrapper(*args, **kwargs):
            key_args = json.dumps(args, sort_keys=True, default=str)
            key_kwargs = json.dumps(kwargs, sort_keys=True, default=str)
            raw_key = f"{key_prefix}:{func.__name__}:{key_args}:{key_kwargs}"
            key = hashlib.md5(raw_key.encode()).hexdigest()
            
            value = await get_cached_result(key)
            if value is not None:
                return value
                
            result = await func(*args, **kwargs)
            await valkey_client.set(key, result, ex=ttl)
            return result

        return wrapper

    return decorator
