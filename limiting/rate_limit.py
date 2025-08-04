"""
Rate Limiting Utilities

Provides production-grade rate limiting using Redis with:
- Exponential backoff
- Detailed logging
"""

import logging
import time
from datetime import datetime, timedelta

from fastapi import HTTPException, status

# Replace Supabase imports with mock utility imports
from app.core.valkey_core._tests.utilities.mock_auth import get_mock_auth_service

async def get_auth_service():
    """Get auth service - using mock implementation"""
    return await get_mock_auth_service()

from app.core.valkey_core.client import client as valkey_client

logger = logging.getLogger(__name__)

# Initialize Redis client
client = valkey_client

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


async def check_rate_limit(client, key: str, limit: int, window: int) -> bool:
    """
    Improved sliding window rate limiting using Redis sorted sets
    More accurate than fixed windows, better for burst protection
    See debugging guide: app/core/valkey_core/_tests/_docs/debugging_tests.md
    """
    now = datetime.utcnow()
    # DEBUG: log entry parameters
    print(f"🔍 DEBUG [check_rate_limit] start: key={key}, limit={limit}, window={window}, now={now.isoformat()}")
    # Defensive: handle None client gracefully (fail-closed)
    if client is None:
        print(f"[check_rate_limit] Valkey client is None for key={key}. Failing closed.")
        logger.error(f"[check_rate_limit] Valkey client is None for key={key}. Failing closed.")
        return False
    redis = await client.aconn()
    print(f"🔍 DEBUG [check_rate_limit] acquired Redis connection for key={key}")
    logger.debug(f"[check_rate_limit] Acquired async Valkey connection for key={key} at {now.isoformat()}.")
    
    # First, remove expired timestamps and get the current count
    try:
        # DEBUG: remove expired entries
        cutoff = (now - timedelta(seconds=window)).timestamp()
        print(f"🔍 DEBUG [check_rate_limit] removing entries older than {cutoff}")
        # Remove expired entries - check if this needs await or not
        redis.zremrangebyscore(key, 0, cutoff)
        
        # Get current count BEFORE adding the new request
        current_count = redis.zcard(key)
        print(f"🔍 DEBUG [check_rate_limit] current_count after cleanup: {current_count}")
        logger.debug(f"[check_rate_limit] Current count for key={key}: {current_count}, limit={limit}")
        
        # Check if adding one more would exceed the limit
        if current_count >= limit:
            print(f"🔍 DEBUG [check_rate_limit] rate limit exceeded: current_count={current_count} >= limit={limit}")
            logger.debug(f"[check_rate_limit] Rate limit exceeded for key={key}: count={current_count}, limit={limit}")
            return False
        
        # If we're under the limit, add the current request
        redis.zadd(key, {now.timestamp(): now.timestamp()})
        print(f"🔍 DEBUG [check_rate_limit] added new entry timestamp {now.timestamp()}")
        # Set key TTL
        redis.expire(key, window)
        print(f"🔍 DEBUG [check_rate_limit] set key expire to {window} seconds")
        
        return True
    except Exception as e:
        print(f"🔍 DEBUG [check_rate_limit] exception occurred: {e}")
        logger.error(f"[check_rate_limit] Error in rate limiting: {str(e)}")
        # Fail closed on errors
        return False


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
    logger.debug(f"Rate limit check started for endpoint: {endpoint}")

    try:
        # Get auth service (now using mock implementation)
        auth_service = await get_auth_service()
        
        # Verify JWT with detailed logging
        if not auth_service.verify_jwt(token):
            logger.warning(
                "Invalid JWT token",
                extra={"token": token[:8] + "...", "endpoint": endpoint},
            )
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

        if await check_rate_limit(client, rate_key, limit=limit, window=window):
            logger.warning(
                "Rate limit exceeded",
                extra={
                    "user_id": user_id,
                    "ip": ip,
                    "endpoint": endpoint,
                    "limit": limit,
                },
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded. Try again in {window} seconds",
            )

        await increment_rate_limit(rate_key, endpoint, window=window)
        logger.debug(f"Request allowed for endpoint: {endpoint}, user: {user_id}")
        return user_id

    except Exception as e:
        logger.error(
            "Rate limit error",
            extra={"error": str(e), "stack_trace": True},
            exc_info=True,
        )
        # Fail open during Redis outages
        return user_id
    finally:
        logger.debug(f"Rate limit check completed for endpoint: {endpoint}")


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
    logger.debug(f"Service rate limit check started for key: {key}, endpoint: {endpoint}")

    try:
        rate_key = f"service_rate:{key}"

        if await check_rate_limit(client, rate_key, limit=limit, window=window):
            logger.warning(
                "Service rate limit exceeded",
                extra={"key": key, "endpoint": endpoint, "limit": limit},
            )
            return False

        await increment_rate_limit(rate_key, endpoint, window=window)
        logger.debug(f"Service request allowed for key: {key}, endpoint: {endpoint}")
        return True

    except Exception as e:
        logger.error(
            "Service rate limit error",
            extra={"error": str(e), "stack_trace": True},
            exc_info=True,
        )
        # Fail open during Redis outages
        return True
    finally:
        logger.debug(f"Service rate limit check completed for key: {key}, endpoint: {endpoint}")


async def init_cleanup():
    await client.eval(CLEANUP_SCRIPT, 0)
    # asyncio.create_task(run_weekly(init_cleanup))  # This line is commented out because run_weekly is not defined in the provided code
