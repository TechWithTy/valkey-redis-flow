"""
Centralized exception handling with HTTP status codes.

Defines:
- log_and_raise_valkey_exception utility
- handle_valkey_exceptions async utility
"""
import logging
from fastapi import HTTPException
from valkey import ValkeyError
from valkey.exceptions import (
    AskError,
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    AuthorizationError,
    BusyLoadingError,
    ChildDeadlockedError,
    ClusterCrossSlotError,
    ClusterDownError,
    ClusterError,
    ConnectionError,
    DataError,
    ExecAbortError,
    InvalidResponse,
    LockError,
    LockNotOwnedError,
    MasterDownError,
    MaxConnectionsError,
    ModuleError,
    MovedError,
    NoPermissionError,
    NoScriptError,
    OutOfMemoryError,
    PubSubError,
    ReadOnlyError,
    ResponseError,
    SlotNotCoveredError,
    TimeoutError,
    TryAgainError,
    ValkeyClusterException,
    ValkeyError,
    WatchError,
)

logger = logging.getLogger(__name__)


def log_and_raise_valkey_exception(
    logger, error_type: str, *args, log_message=None, **kwargs
):
    """
    Logs an error message and raises a Valkey-specific HTTP exception.
    Args:
        logger: The logger instance to use for logging
        error_type: One of 'connection', 'timeout', 'auth', 'response', 'api', or any Valkey exception class name (e.g., 'BusyLoadingError')
        *args: Arguments to pass to the exception constructor
        log_message: Optional custom message to log (defaults to error class message)
        **kwargs: Keyword arguments to pass to the exception constructor
    Raises:
        ValkeyError: An instance of the specified Valkey exception class
    """
    error_map = {
        "connection": ConnectionError,
        "timeout": TimeoutError,
        "auth": AuthenticationError,
        "authorization": AuthorizationError,
        "response": ResponseError,
        "cluster": ValkeyClusterException,
        "api": ValkeyError,
        # Add direct mappings for all Valkey exception class names
        "BusyLoadingError": BusyLoadingError,
        "InvalidResponse": InvalidResponse,
        "DataError": DataError,
        "PubSubError": PubSubError,
        "WatchError": WatchError,
        "NoScriptError": NoScriptError,
        "OutOfMemoryError": OutOfMemoryError,
        "ExecAbortError": ExecAbortError,
        "ReadOnlyError": ReadOnlyError,
        "NoPermissionError": NoPermissionError,
        "ModuleError": ModuleError,
        "LockError": LockError,
        "LockNotOwnedError": LockNotOwnedError,
        "ChildDeadlockedError": ChildDeadlockedError,
        "AuthenticationWrongNumberOfArgsError": AuthenticationWrongNumberOfArgsError,
        "ClusterError": ClusterError,
        "ClusterDownError": ClusterDownError,
        "AskError": AskError,
        "TryAgainError": TryAgainError,
        "ClusterCrossSlotError": ClusterCrossSlotError,
        "MovedError": MovedError,
        "MasterDownError": MasterDownError,
        "SlotNotCoveredError": SlotNotCoveredError,
        "MaxConnectionsError": MaxConnectionsError,
    }
    exc_class = error_map.get(error_type, ValkeyError)
    exception = exc_class(*args, **kwargs)
    message = log_message or str(exception)
    logger.error(f"Valkey Exception [{exc_class.__name__}]: {message}")
    raise exception


async def handle_valkey_exceptions(
    func,
    *,
    endpoint: str | None = None,
    logger=None,
    current_user: dict | None = None,
    extra_context: dict | None = None,
    VALKEY_ERRORS_MAPPING: dict | None = None,
    wrap_http_exception: bool = True,
):
    """
    Utility to wrap Valkey logic with robust exception-to-HTTP mapping and logging.

    Usage (async):
        result = await handle_valkey_exceptions(
            lambda: some_async_func(...),
            endpoint="/valkey/op",
            logger=logger,
            current_user=current_user,
            extra_context={...},
            VALKEY_ERRORS_MAPPING={...}
        )
    Args:
        func: The async function or lambda to execute.
        endpoint: Optional endpoint string for context/logging.
        logger: Optional logger instance.
        current_user: Optional dict of user info.
        extra_context: Optional dict for additional context (e.g., request_id).
        VALKEY_ERRORS_MAPPING: Optional dict mapping Valkey exceptions to HTTP response codes.
    Returns:
        The result of func, or raises mapped HTTPException on error (if wrap_http_exception is True), otherwise raises the original exception.
    """
    if VALKEY_ERRORS_MAPPING is None:
        VALKEY_ERRORS_MAPPING = {
            AuthenticationError: 401,
            AuthorizationError: 403,
            ConnectionError: 503,
            TimeoutError: 504,
            BusyLoadingError: 503,
            ResponseError: 502,
            DataError: 400,
            PubSubError: 500,
            WatchError: 409,
            NoScriptError: 500,
            OutOfMemoryError: 507,
            ExecAbortError: 500,
            ReadOnlyError: 403,
            NoPermissionError: 403,
            ModuleError: 500,
            LockError: 423,
            LockNotOwnedError: 423,
            ChildDeadlockedError: 500,
            AuthenticationWrongNumberOfArgsError: 400,
            ValkeyClusterException: 500,
            ClusterError: 500,
            ClusterDownError: 503,
            AskError: 502,
            TryAgainError: 503,
            ClusterCrossSlotError: 400,
            MovedError: 502,
            MasterDownError: 503,
            SlotNotCoveredError: 503,
            MaxConnectionsError: 429,
        }
    try:
        return await func()
    except tuple(VALKEY_ERRORS_MAPPING.keys()) as exc:
        status_code = getattr(exc, "status_code", None) or VALKEY_ERRORS_MAPPING.get(type(exc), 500)
        error_type = type(exc).__name__
        detail = {
            "error": error_type,
            "message": str(exc),
            "endpoint": endpoint,
            "user": current_user,
            "context": extra_context,
            "details": getattr(exc, "details", None),
        }
        if logger:
            logger.error(f"[Valkey] Exception at {endpoint}: {detail}")
        if wrap_http_exception:
            raise HTTPException(status_code=status_code, detail=detail) from exc
        else:
            raise exc
    except Exception as exc:
        detail = {
            "error": "UnhandledException",
            "message": str(exc),
            "endpoint": endpoint,
            "user": current_user,
            "context": extra_context,
        }
        if logger:
            logger.error(f"[Valkey] Unhandled exception at {endpoint}: {detail}")
        if wrap_http_exception:
            raise HTTPException(status_code=500, detail=detail) from exc
        else:
            raise exc
