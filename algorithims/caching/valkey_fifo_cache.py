from typing import Any

from app.core.valkey_core.client import ValkeyClient

class ValkeyFIFOCache:
    """
    FIFO cache adapter using Valkey/Redis with volatile-ttl policy.
    """
    def __init__(self, client=None, namespace: str = "fifo", default_ttl: int = 3600):
        self.client = client or ValkeyClient.get_default()
        self.namespace = namespace
        self.default_ttl = default_ttl

    def _key(self, key: str) -> str:
        return f"{self.namespace}:{key}"

    async def get(self, key: str) -> Any:
        return await self.client.get(self._key(key))

    async def set(self, key: str, value: Any, ttl: int | None = None):
        ttl = ttl or self.default_ttl
        await self.client.set(self._key(key), value, ex=ttl)

    async def delete(self, key: str):
        await self.client.delete(self._key(key))

    async def clear(self):
        # ! Use SCAN for safety in production, not KEYS
        keys = await self.client.scan(f"{self.namespace}:*")
        if keys:
            await self.client.delete(*keys)
