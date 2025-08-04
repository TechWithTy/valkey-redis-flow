"""
Valkey client initialization and configuration.

Follows best practices for:
- Connection pooling
- Timeout handling
- Error recovery
- Sharding support
- Structured Valkey exception handling
- Distributed locking
"""

import asyncio
import json
import logging
import time
from typing import Any

from valkey.asyncio import Valkey, ValkeyCluster
from valkey.backoff import (
    ConstantBackoff,
    DecorrelatedJitterBackoff,
    ExponentialBackoff,
)
from valkey.retry import Retry
from .exceptions.exceptions import TimeoutError, ValkeyError

from .config import ValkeyConfig
from .exceptions.exceptions import handle_valkey_exceptions
from .decorators import track_valkey_metrics
from ..prometheus.metrics import get_cache_count, get_cache_latency, get_cache_hit_ratio

VALKEY_CLUSTER = ValkeyConfig.VALKEY_CLUSTER
VALKEY_DB = ValkeyConfig.VALKEY_DB
VALKEY_HOST = ValkeyConfig.VALKEY_HOST
VALKEY_MAX_CONNECTIONS = ValkeyConfig.VALKEY_MAX_CONNECTIONS
VALKEY_PASSWORD = ValkeyConfig.VALKEY_PASSWORD
VALKEY_PORT = ValkeyConfig.VALKEY_PORT
VALKEY_SOCKET_CONNECT_TIMEOUT = ValkeyConfig.VALKEY_SOCKET_CONNECT_TIMEOUT
VALKEY_SOCKET_TIMEOUT = ValkeyConfig.VALKEY_SOCKET_TIMEOUT

logger = logging.getLogger(__name__)

# Default timeout constants (in seconds)
DEFAULT_CONNECTION_TIMEOUT = 5.0
DEFAULT_SOCKET_TIMEOUT = 10.0
DEFAULT_COMMAND_TIMEOUT = 5.0


class ValkeyLock:
    """
    Distributed lock using Valkey's built-in lock mechanism.
    Usage:
        async with ValkeyLock(client, name, timeout=5):
            # critical section
    """
    def __init__(self, client, name: str, timeout: float | None = None, sleep: float = 0.1, blocking: bool = True, blocking_timeout: float | None = None, thread_local: bool = False):
        # thread_local=False to avoid _thread._local issues in async
        self._lock = client._client.lock(
            name,
            timeout=timeout,
            sleep=sleep,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

    async def __aenter__(self):
        acquired = await self._lock.acquire()
        if not acquired:
            raise TimeoutError("Could not acquire Valkey lock")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._lock.release()


class ValkeyClient:
    """
    Valkey client wrapper with connection management and utilities.

    Handles:
    - Connection pooling
    - Automatic reconnections
    - Timeout handling
    - Sharding support
    - Structured Valkey exception handling
    - Distributed locking (see lock method)
    """

    def __init__(self):
        """Initialize with automatic cluster detection"""
        self._client = None
        self._cluster_mode = VALKEY_CLUSTER
        self._metrics_task = None
        self._metrics_enabled = ValkeyConfig.VALKEY_METRICS_ENABLED
        self._metrics_namespace = getattr(
            ValkeyConfig, "REDIS_METRICS_NAMESPACE", "valkey"
        )

    async def get_client(self) -> Valkey | ValkeyCluster:
        """
        Returns configured client based on settings
        - Auto-reconnects if needed
        - Supports both cluster and sharded modes
        """
        if self._cluster_mode:
            return await self._get_cluster_client()
        return await self._get_sharded_client()

    async def _get_cluster_client(self) -> ValkeyCluster:
        """Get a cluster Valkey client based on configuration"""
        if not self._client:
            # Choose backoff strategy based on config
            backoff_type = ValkeyConfig.VALKEY_RETRY_BACKOFF_TYPE
            if backoff_type == "exponential":
                backoff = ExponentialBackoff(
                    base=ValkeyConfig.VALKEY_RETRY_BACKOFF_BASE,
                    cap=ValkeyConfig.VALKEY_RETRY_BACKOFF_CAP,
                )
            elif backoff_type == "jitter":
                backoff = DecorrelatedJitterBackoff(
                    base=ValkeyConfig.VALKEY_RETRY_BACKOFF_BASE,
                    cap=ValkeyConfig.VALKEY_RETRY_BACKOFF_CAP,
                )
            else:
                backoff = ConstantBackoff(ValkeyConfig.VALKEY_RETRY_BACKOFF_BASE)
            retry = Retry(
                backoff,
                retries=ValkeyConfig.VALKEY_RETRY_ATTEMPTS,
                supported_errors=(TimeoutError, ValkeyError),
            )
            self._client = ValkeyCluster(
                host=VALKEY_HOST,
                port=VALKEY_PORT,
                password=VALKEY_PASSWORD,
                db=VALKEY_DB,
                socket_timeout=VALKEY_SOCKET_TIMEOUT,
                socket_connect_timeout=VALKEY_SOCKET_CONNECT_TIMEOUT,
                max_connections=VALKEY_MAX_CONNECTIONS,
                ssl=ValkeyConfig.VALKEY_SSL,
                ssl_cert_reqs=ValkeyConfig.VALKEY_SSL_CERT_REQS,
                ssl_ca_certs=ValkeyConfig.VALKEY_SSL_CA_CERTS,
                ssl_keyfile=ValkeyConfig.VALKEY_SSL_KEYFILE,
                ssl_certfile=ValkeyConfig.VALKEY_SSL_CERTFILE,
                retry=retry,
                cluster_error_retry_attempts=ValkeyConfig.VALKEY_RETRY_ATTEMPTS,
            )
        return self._client

    async def _get_sharded_client(self) -> Valkey:
        """Get a sharded Valkey client based on configuration"""
        if not self._client:
            backoff_type = ValkeyConfig.VALKEY_RETRY_BACKOFF_TYPE
            if backoff_type == "exponential":
                backoff = ExponentialBackoff(
                    base=ValkeyConfig.VALKEY_RETRY_BACKOFF_BASE,
                    cap=ValkeyConfig.VALKEY_RETRY_BACKOFF_CAP,
                )
            elif backoff_type == "jitter":
                backoff = DecorrelatedJitterBackoff(
                    base=ValkeyConfig.VALKEY_RETRY_BACKOFF_BASE,
                    cap=ValkeyConfig.VALKEY_RETRY_BACKOFF_CAP,
                )
            else:
                backoff = ConstantBackoff(ValkeyConfig.VALKEY_RETRY_BACKOFF_BASE)
            retry = Retry(
                backoff,
                retries=ValkeyConfig.VALKEY_RETRY_ATTEMPTS,
                supported_errors=(TimeoutError, ValkeyError),
            )
            self._client = Valkey(
                host=VALKEY_HOST,
                port=VALKEY_PORT,
                password=VALKEY_PASSWORD,
                db=VALKEY_DB,
                socket_timeout=VALKEY_SOCKET_TIMEOUT,
                socket_connect_timeout=VALKEY_SOCKET_CONNECT_TIMEOUT,
                max_connections=VALKEY_MAX_CONNECTIONS,
                ssl=ValkeyConfig.VALKEY_SSL,
                ssl_cert_reqs=ValkeyConfig.VALKEY_SSL_CERT_REQS,
                ssl_ca_certs=ValkeyConfig.VALKEY_SSL_CA_CERTS,
                ssl_keyfile=ValkeyConfig.VALKEY_SSL_KEYFILE,
                ssl_certfile=ValkeyConfig.VALKEY_SSL_CERTFILE,
                retry=retry,
            )
        return self._client

    async def shutdown(self):
        """Cleanly shutdown Valkey client"""
        if self._client:
            await self._client.close()
            self._client = None
        if self._metrics_task:
            self._metrics_task.cancel()

    async def __aenter__(self):
        if not await self.is_healthy():
            raise ConnectionError("Valkey connection failed")
        return self

    @staticmethod
    def _maybe_json_decode(value: str | bytes) -> Any:
        """
        Safely decode JSON if value looks like JSON, else return as-is.
        Handles bytes by decoding to str first.
        """
        if not value:
            return None
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        v = value.strip()
        # Accepts JSON primitives, objects, arrays
        if v[:1] in ('"', '{', '[', 't', 'f', 'n') or v.replace('.', '', 1).isdigit():
            try:
                return json.loads(v)
            except Exception:
                pass
        return value

    @track_valkey_metrics('get')
    async def get(self, key: str, timeout: float = None, wrap_http_exception: bool = True) -> Any:
        if timeout is None:
            timeout = ValkeyConfig.VALKEY_COMMAND_TIMEOUT

        async def _action():
            logger.debug(f"Valkey get operation for key: {key}")
            # Remove 'timeout' from direct call to backend client
            value = await (await self.get_client()).get(key)
            
            # Don't try to update cache hit ratio metric here
            # We'll leave this to the background metrics collector
                    
            return self._maybe_json_decode(value)

        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.get", wrap_http_exception=wrap_http_exception
        )

    @track_valkey_metrics('set')
    async def set(
        self,
        key: str,
        value: Any,
        ex: int | None = None,
        timeout: float = None,
    ) -> bool:
        if timeout is None:
            timeout = ValkeyConfig.VALKEY_COMMAND_TIMEOUT

        async def _action():
            logger.debug(f"Valkey set operation for key: {key}, ttl: {ex or 0}")
            # Remove 'timeout' from direct call to backend client
            result = await (await self.get_client()).set(
                key, json.dumps(value), ex=ex
            )
            return result

        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.set"
        )

    @track_valkey_metrics('delete')
    async def delete(self, *keys: str, timeout: float = DEFAULT_COMMAND_TIMEOUT) -> int:
        async def _action():
            logger.debug(f"Valkey delete operation for keys: {keys}")
            # Remove 'timeout' from direct call to backend client (see debugging_tests.md)
            return await (await self.get_client()).delete(*keys)

        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.delete"
        )

    @track_valkey_metrics('delete')
    async def delete_many(self, keys: list[str], timeout: float = DEFAULT_COMMAND_TIMEOUT) -> int:
        """Delete multiple keys at once"""
        async def _action():
            logger.debug(f"Valkey delete_many operation for keys: {keys}")
            return await (await self.get_client()).delete(*keys)

        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.delete_many"
        )

    async def is_healthy(self) -> bool:
        try:
            return await (await self.get_client()).ping()
        except (ValkeyError, TimeoutError):
            return False

    async def incr(self, key: str, timeout: float = DEFAULT_COMMAND_TIMEOUT) -> int:
        async def _action():
            logger.debug(f"Valkey incr operation for key: {key}")
            value = await (await self.get_client()).incr(key)
            return value

        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.incr"
        )

    async def expire(
        self, key: str, ex: int, timeout: float = DEFAULT_COMMAND_TIMEOUT
    ) -> bool:
        async def _action():
            logger.debug(f"Valkey expire operation for key: {key}, ttl: {ex}")
            result = await (await self.get_client()).expire(key, ex)
            return result

        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.expire"
        )

    async def ttl(self, key: str, timeout: float = DEFAULT_COMMAND_TIMEOUT) -> int:
        async def _action():
            logger.debug(f"Valkey ttl operation for key: {key}")
            value = await (await self.get_client()).ttl(key)
            return value

        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.ttl"
        )

    async def flushdb(self):
        """
        Flush the current Valkey database (for test isolation).
        """
        async def _action():
            logger.debug("Valkey flushdb operation")
            client = await self.get_client()
            result = await client.flushdb()
            return result

        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.flushdb"
        )

    async def exists(self, key: str, timeout: float = DEFAULT_COMMAND_TIMEOUT) -> bool:
        async def _action():
            logger.debug(f"Valkey exists operation for key: {key}")
            exists = await (await self.get_client()).exists(key)
            return exists == 1

        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.exists"
        )

    async def pipeline(self):
        async def _action():
            logger.debug("Valkey pipeline operation")
            client = await self.get_client()
            return client.pipeline()

        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.pipeline"
        )

    async def pubsub(self):
        async def _action():
            logger.debug("Valkey pubsub operation")
            client = await self.get_client()
            return client.pubsub()

        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.pubsub"
        )

    async def publish(self, channel: str, message: str):
        """
        Publish a message to a channel.
        """
        async def _action():
            logger.debug(f"Valkey publish operation for channel: {channel}")
            client = await self.get_client()
            return await client.publish(channel, message)
        
        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.publish"
        )
        
    async def scan(self, match: str = "*") -> list[str]:
        """
        Asynchronously scan for all keys matching the pattern.
        Uses the underlying Redis/Valkey SCAN command.
        """
        async def _action():
            logger.debug(f"Valkey scan operation for pattern: {match}")
            cursor = 0
            keys = []
            while True:
                cursor, batch = await (await self.get_client()).scan(cursor=cursor, match=match)
                keys.extend(batch)
                if cursor == 0:
                    break
            return keys
            
        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.scan"
        )

    async def lrem(self, key: str, count: int, value: str) -> int:
        """
        Remove elements from a list (like Redis LREM).
        """
        async def _action():
            logger.debug(f"Valkey lrem operation for key: {key}, count: {count}")
            result = await (await self.get_client()).lrem(key, count, value)
            return result
            
        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.lrem"
        )

    async def rpush(self, key: str, value: str) -> int:
        """
        Append a value to a list (like Redis RPUSH).
        """
        async def _action():
            logger.debug(f"Valkey rpush operation for key: {key}")
            result = await (await self.get_client()).rpush(key, value)
            return result
            
        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.rpush"
        )

    async def llen(self, key: str) -> int:
        """
        Get the length of a list (like Redis LLEN).
        """
        async def _action():
            logger.debug(f"Valkey llen operation for key: {key}")
            result = await (await self.get_client()).llen(key)
            return result
            
        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.llen"
        )

    async def rpop(self, key: str) -> str | None:
        """
        Remove and get the last element in a list (like Redis RPOP).
        """
        async def _action():
            logger.debug(f"Valkey rpop operation for key: {key}")
            result = await (await self.get_client()).rpop(key)
            return result
            
        return await handle_valkey_exceptions(
            _action, logger=logger, endpoint="valkey.rpop"
        )
        
    @property
    def conn(self):
        """Return the underlying Valkey/ValkeyCluster connection (sync, may be None if not initialized)."""
        return self._client

    async def aconn(self):
        """Return the underlying Valkey/ValkeyCluster connection, initializing if needed (async)."""
        return await self.get_client()

    def lock(self, name: str, timeout: float | None = None, sleep: float = 0.1, blocking: bool = True, blocking_timeout: float | None = None, thread_local: bool = True):
        """
        Acquire a distributed lock using Valkey's built-in locking.
        Usage:
            async with client.lock("resource_key", timeout=5):
                ...
        """
        return ValkeyLock(self, name, timeout, sleep, blocking, blocking_timeout, thread_local)


# Factory for Valkey client instance
def get_valkey_client():
    """
    Returns a new ValkeyClient instance for the current event loop.
    Use this in async code to avoid event loop issues with singletons.
    """
    return ValkeyClient()

# todo: Remove global singleton if not needed for legacy compatibility
import warnings
warnings.warn("The global 'client' singleton is deprecated. Use get_valkey_client() instead.", DeprecationWarning)
client = get_valkey_client()

# Module-level functions for convenience
get_client = client.get_client
shutdown = client.shutdown
get = client.get
set = client.set
delete = client.delete
is_healthy = client.is_healthy
incr = client.incr
expire = client.expire
ttl = client.ttl
exists = client.exists
pipeline = client.pipeline
pubsub = client.pubsub
lock = client.lock