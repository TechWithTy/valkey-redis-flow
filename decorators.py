"""
Decorators for instrumenting Valkey/Redis operations with Prometheus metrics.
"""
import time
import functools
import inspect
import asyncio
from typing import Any, Callable, Dict, Optional, Union, TypeVar, cast

from app.core.prometheus.metrics import get_cache_count, get_cache_latency

# Type variable for function return type
T = TypeVar('T')

def track_valkey_metrics(operation: str):
    """
    Decorator that tracks timing and outcome of Redis/Valkey operations.
    
    Args:
        operation: The operation name ('hit', 'miss', 'set', 'delete')
    
    Usage:
        @track_valkey_metrics('get')
        async def get_from_cache(key):
            # Your function implementation
            return value
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        is_async = asyncio.iscoroutinefunction(func)
        
        if is_async:
            @functools.wraps(func)
            async def wrapper_async(*args: Any, **kwargs: Any) -> T:
                cache_type = "valkey"  # Default cache type
                
                # Try to extract cache_type from self or args if available
                if args and hasattr(args[0], '_metrics_namespace'):
                    cache_type = getattr(args[0], '_metrics_namespace', 'valkey')
                
                start_time = time.time()
                try:
                    result = await func(*args, **kwargs)
                    # Track hit or miss based on result for get operations
                    if operation == 'get':
                        actual_op = 'hit' if result is not None else 'miss'
                        get_cache_count().labels(cache_type, actual_op).inc()
                    else:
                        # For set, delete, etc. operations
                        get_cache_count().labels(cache_type, operation).inc()
                    
                    return result
                finally:
                    # Always track operation latency
                    elapsed = time.time() - start_time
                    get_cache_latency().labels(cache_type, operation).observe(elapsed)
            
            return cast(Callable[..., T], wrapper_async)
        else:
            @functools.wraps(func)
            def wrapper_sync(*args: Any, **kwargs: Any) -> T:
                cache_type = "valkey"
                
                # Try to extract cache_type from self or args if available
                if args and hasattr(args[0], '_metrics_namespace'):
                    cache_type = getattr(args[0], '_metrics_namespace', 'valkey')
                
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    # Track hit or miss based on result for get operations
                    if operation == 'get':
                        actual_op = 'hit' if result is not None else 'miss'
                        get_cache_count().labels(cache_type, actual_op).inc()
                    else:
                        # For set, delete, etc. operations
                        get_cache_count().labels(cache_type, operation).inc()
                    
                    return result
                finally:
                    # Always track operation latency
                    elapsed = time.time() - start_time
                    get_cache_latency().labels(cache_type, operation).observe(elapsed)
            
            return wrapper_sync
    
    return decorator
