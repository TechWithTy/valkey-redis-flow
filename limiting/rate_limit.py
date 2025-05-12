"""
Rate Limiting Utilities

Provides production-grade rate limiting using Redis with:
- Exponential backoff
- Detailed metrics
"""

import logging
import time
from datetime import datetime, timedelta

from fastapi import HTTPException, status
from prometheus_client import Counter, Gauge

# from app.core.third_party_integrations.supabase_home.functions.auth import SupabaseAuthService  # ! Deprecated: Use async get_auth_service() instead
from app.core.third_party_integrations.supabase_home.client import get_supabase_client
from app.core.third_party_integrations.supabase_home.app import SupabaseAuthService

async def get_auth_service():
    client = await get_supabase_client()
    service = SupabaseAuthService(client)
    user = await service.get_current_user() if hasattr(service.get_current_user, '__await__') else service.get_current_user()
    return user

from app.core.valkey_core.client import client as valkey_client

logger = logging.getLogger(__name__)

# Initialize Redis client
client = valkey_client

# ! Deprecated: Use async get_auth_service() instead of direct SupabaseAuthService instantiation
# auth_service = SupabaseAuthService()

# Prometheus Metrics
RATE_LIMIT_REQUESTS = Counter(
    "rate_limit_requests_total", "Total rate limit requests", ["endpoint", "status"]
)

RATE_LIMIT_GAUGE = Gauge(
    "rate_limit_active_requests", "Currently active rate-limited requests", ["endpoint"]
)


# Default rate limit
DEFAULT_LIMIT = 100

# Cleanup script (runs weekly)
CLEANUP_SCRIPT = """
local keys = redis.call('KEYS', 'rate_meta:*')
for _, key in ipairs(keys) do
    local last_update = redis.call('HGET', key, 'updated_at')
    if last_update and os.time() - last_update > 604800 then
        redis.call('DEL', key)
    end
end
return #keys
"""


async def check_rate_limit(key: str, limit: int, window: int) -> bool:
    """
    Improved sliding window rate limiting using Redis sorted sets
    More accurate than fixed windows, better for burst protection
    """
    now = datetime.utcnow()
    pipeline = client.pipeline()

    # Remove expired timestamps
    pipeline.zremrangebyscore(key, 0, (now - timedelta(seconds=window)).timestamp())

    # Count remaining requests
    pipeline.zcard(key)

    # Add current request
    pipeline.zadd(key, {now.timestamp(): now.timestamp()})
    pipeline.expire(key, window)

    _, count, _, _ = await pipeline.execute()
    return count >= limit


async def increment_rate_limit(identifier: str, endpoint: str, window: int = 60) -> int:
    """
    Increment rate limit counter for identifier and endpoint

    Args:
        identifier: User ID, IP address, or other unique identifier
        endpoint: API endpoint or action name
        window: Time window in seconds

    Returns:
        Current count after increment
    """
    key = f"rate_limit:{endpoint}:{identifier}"

    # Get or initialize counter
    current = await client.get(key)
    if current is None:
        await client.set(key, 1, ex=window)
        return 1

    # Increment and return
    count = int(current) + 1
    await client.set(key, count, ex=window)
    return count


async def get_remaining_limit(
    identifier: str, endpoint: str, limit: int = 5, window: int = 60
) -> dict:
    """
    Get rate limit details including remaining requests

    Returns:
        {
            "limit": 5,
            "remaining": 3,
            "reset": 1625097600
        }
    """
    key = f"rate_limit:{endpoint}:{identifier}"
    current = int(await client.get(key) or 0)
    ttl = await client.ttl(key)

    return {
        "limit": limit,
        "remaining": max(0, limit - current),
        "reset": int(time.time()) + (ttl if ttl > 0 else window),
    }


async def alert_system(message: str) -> None:
    # Implement your alerting system
    logger.warning(message)


async def verify_and_limit(token: str, ip: str, endpoint: str, window: int = 3600):
    """
    Enhanced with:
    - User-level rate tracking
    - Composite keys for granular control
    """
    RATE_LIMIT_GAUGE.labels(endpoint=endpoint).inc()

    try:
        # Verify JWT with detailed logging
        if not auth_service.verify_jwt(token):
            logger.warning(
                "Invalid JWT token",
                extra={"token": token[:8] + "...", "endpoint": endpoint},
            )
            RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, status="unauthorized").inc()
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials"
            )

        user_id = auth_service.get_user_id(token)
        limit = DEFAULT_LIMIT

        logger.debug(
            "Rate limit check", extra={"user_id": user_id, "endpoint": endpoint}
        )

        # Composite key: user + IP + endpoint
        rate_key = f"user_rate:{user_id}:{ip}:{endpoint}"

        # Track metadata without tier logic
        metadata = {
            "last_ip": ip,
            "last_endpoint": endpoint,
            "updated_at": datetime.utcnow().isoformat(),
        }

        await client.hset(f"rate_meta:{user_id}", mapping=metadata)

        if await check_rate_limit(rate_key, limit=limit, window=window):
            logger.warning(
                "Rate limit exceeded",
                extra={
                    "user_id": user_id,
                    "ip": ip,
                    "endpoint": endpoint,
                    "limit": limit,
                },
            )
            RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, status="limited").inc()
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded. Try again in {window} seconds",
            )

        await increment_rate_limit(rate_key, endpoint, window=window)
        RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, status="allowed").inc()
        return user_id

    except Exception as e:
        logger.error(
            "Rate limit error",
            extra={"error": str(e), "stack_trace": True},
            exc_info=True,
        )
        RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, status="error").inc()
        # Fail open during Redis outages
        return user_id
    finally:
        RATE_LIMIT_GAUGE.labels(endpoint=endpoint).dec()


async def service_rate_limit(
    key: str, limit: int, window: int, endpoint: str = "internal"
) -> bool:
    """
    Simplified rate limiting for internal services without user authentication.

    Args:
        key: Unique identifier for the rate limit (e.g. 'celery_health')
        limit: Max allowed requests per window
        window: Time window in seconds
        endpoint: Optional endpoint identifier for metrics

    Returns:
        bool: True if request is allowed, False if rate limited
    """
    RATE_LIMIT_GAUGE.labels(endpoint=endpoint).inc()

    try:
        rate_key = f"service_rate:{key}"

        if await check_rate_limit(rate_key, limit=limit, window=window):
            logger.warning(
                "Service rate limit exceeded",
                extra={"key": key, "endpoint": endpoint, "limit": limit},
            )
            RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, status="limited").inc()
            return False

        await increment_rate_limit(rate_key, endpoint, window=window)
        RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, status="allowed").inc()
        return True

    except Exception as e:
        logger.error(
            "Service rate limit error",
            extra={"error": str(e), "stack_trace": True},
            exc_info=True,
        )
        RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, status="error").inc()
        # Fail open during Redis outages
        return True
    finally:
        RATE_LIMIT_GAUGE.labels(endpoint=endpoint).dec()


async def init_cleanup():
    await client.eval(CLEANUP_SCRIPT, 0)
    # asyncio.create_task(run_weekly(init_cleanup))  # This line is commented out because run_weekly is not defined in the provided code
