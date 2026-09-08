import time
import json
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class KeyValueItem:
    def __init__(self, value: str, expires_at: Optional[float] = None):
        self.value = value
        self.expires_at = expires_at

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

class NotificationKeyValueStore:
    """
    Lightweight Key-Value store with TTL support for ephemeral notification delivery state.
    Compatible with Redis semantics.
    """
    backend = "IN_MEMORY"

    def __init__(self):
        self._store: Dict[str, KeyValueItem] = {}
        self._lists: Dict[str, list] = {}

    def ping(self) -> bool:
        return True

    def push_recent(self, list_key: str, value: Any, maxlen: int = 500):
        """Prepend to a capped 'recent items' list (newest first)."""
        lst = self._lists.setdefault(list_key, [])
        lst.insert(0, value)
        del lst[maxlen:]

    def recent(self, list_key: str, limit: int = 50) -> list:
        return list(self._lists.get(list_key, [])[:limit])

    def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        expires_at = time.time() + ex if ex else None
        serialized = json.dumps(value) if not isinstance(value, str) else value
        self._store[key] = KeyValueItem(value=serialized, expires_at=expires_at)
        return True

    def get(self, key: str) -> Optional[Any]:
        self._clean_expired()
        item = self._store.get(key)
        if not item or item.is_expired():
            if key in self._store:
                del self._store[key]
            return None
        try:
            return json.loads(item.value)
        except Exception:
            return item.value

    def delete(self, key: str) -> bool:
        if key in self._store:
            del self._store[key]
            return True
        return False

    def keys(self, pattern: str = "*") -> list:
        self._clean_expired()
        return list(self._store.keys())

    def _clean_expired(self):
        now = time.time()
        expired_keys = [k for k, item in self._store.items() if item.expires_at and item.expires_at < now]
        for k in expired_keys:
            del self._store[k]

    def count(self) -> int:
        self._clean_expired()
        return len(self._store)


class RedisKeyValueStore:
    """
    Real Redis implementation (redis-py) with the same interface as NotificationKeyValueStore.
    - set/get with native Redis TTL (EX)
    - 'recent' lists via LPUSH + LTRIM so notifications survive server restarts
    """
    backend = "REDIS"
    KEY_PREFIX = "notification:"

    def __init__(self, url: str):
        import redis  # imported lazily so the in-memory mode never needs the package
        self.url = url
        self._r = redis.Redis.from_url(url, decode_responses=True, socket_connect_timeout=3, socket_timeout=3)
        try:
            self._r.ping()
        except Exception as e:
            raise RuntimeError(
                f"Cannot reach Redis at {url}: {e}. Start it with `docker compose up -d` "
                "or unset USE_REAL_REDIS to use the in-memory store."
            ) from e
        logger.info("RedisKeyValueStore connected to %s", url)

    def ping(self) -> bool:
        try:
            return bool(self._r.ping())
        except Exception:
            return False

    def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        serialized = json.dumps(value) if not isinstance(value, str) else value
        return bool(self._r.set(key, serialized, ex=ex))

    def get(self, key: str) -> Optional[Any]:
        raw = self._r.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return raw

    def delete(self, key: str) -> bool:
        return bool(self._r.delete(key))

    def keys(self, pattern: str = "*") -> list:
        return list(self._r.scan_iter(match=pattern))

    def count(self) -> int:
        """Number of live notification keys (those expire via TTL)."""
        return sum(1 for _ in self._r.scan_iter(match=self.KEY_PREFIX + "*"))

    def push_recent(self, list_key: str, value: Any, maxlen: int = 500):
        pipe = self._r.pipeline()
        pipe.lpush(list_key, json.dumps(value))
        pipe.ltrim(list_key, 0, maxlen - 1)
        pipe.execute()

    def recent(self, list_key: str, limit: int = 50) -> list:
        return [json.loads(x) for x in self._r.lrange(list_key, 0, limit - 1)]


def _build_store():
    from src.common.config import settings
    if settings.USE_REAL_REDIS:
        return RedisKeyValueStore(settings.REDIS_URL)
    return NotificationKeyValueStore()


# USE_REAL_REDIS=true -> real Redis; otherwise the in-memory store (default, used by tests)
kv_store = _build_store()
