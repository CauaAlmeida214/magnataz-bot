from __future__ import absolute_import
"""
Caching system for OB CASH 3.0.

Provides in-memory and disk-based caching for market data to reduce
API calls and improve performance.
"""

import time
import hashlib
import pickle
from pathlib import Path
from typing import Optional, Tuple, Any, Dict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import threading

from obcash3.config.settings import CACHE_DIR
from obcash3.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class CacheEntry:
    """Single cache entry with metadata."""
    data: Any
    timestamp: datetime
    ttl_seconds: int
    source: str

    def is_valid(self) -> bool:
        """Check if cache entry hasn't expired."""
        age = datetime.now() - self.timestamp
        return age < timedelta(seconds=self.ttl_seconds)


class MemoryCache:
    """In-memory LRU cache with TTL."""

    def __init__(self, max_size: int = 100, default_ttl: int = 300):
        """
        Initialize memory cache.

        Args:
            max_size: Maximum number of entries
            default_ttl: Default time-to-live in seconds
        """
        self._cache: Dict[str, CacheEntry] = {}
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._lock = threading.RLock()

    def _make_key(self, symbol: str, interval: str, source: str,
                  extra: Optional[Dict[str, Any]] = None) -> str:
        """Generate a unique cache key."""
        key_parts = [symbol, interval, source]
        if extra:
            extra_str = str(sorted(extra.items()))
            key_parts.append(extra_str)
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()[:16]

    def get(self, symbol: str, interval: str, source: str,
            extra: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        """Retrieve data from cache if valid."""
        with self._lock:
            key = self._make_key(symbol, interval, source, extra)
            entry = self._cache.get(key)

            if entry and entry.is_valid():
                logger.debug("Cache HIT: %s %s %s", symbol, interval, source)
                return entry.data
            elif entry:
                logger.debug("Cache EXPIRED: %s %s %s", symbol, interval, source)
                del self._cache[key]

            return None

    def set(self, symbol: str, interval: str, source: str, data: Any,
            ttl: Optional[int] = None, extra: Optional[Dict[str, Any]] = None) -> None:
        """Store data in cache."""
        with self._lock:
            key = self._make_key(symbol, interval, source, extra)
            entry = CacheEntry(
                data=data,
                timestamp=datetime.now(),
                ttl_seconds=ttl or self._default_ttl,
                source=source
            )
            self._cache[key] = entry

            # Evict old entries if over size limit
            if len(self._cache) > self._max_size:
                oldest_key = min(
                    self._cache,
                    key=lambda k: self._cache[k].timestamp
                )
                del self._cache[oldest_key]

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            logger.info("Memory cache cleared")

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total = len(self._cache)
            expired = sum(1 for e in self._cache.values() if not e.is_valid())
            return {
                "total_entries": total,
                "valid_entries": total - expired,
                "expired_entries": expired,
                "max_size": self._max_size,
            }


class DiskCache:
    """Persistent disk-based cache with TTL."""

    def __init__(self, cache_dir: str | Path = CACHE_DIR, default_ttl: int = 600):
        """
        Initialize disk cache.

        Args:
            cache_dir: Directory to store cache files
            default_ttl: Default TTL in seconds
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.default_ttl = default_ttl

    def _make_path(self, symbol: str, interval: str, source: str,
                   extra: Optional[Dict[str, Any]] = None) -> Path:
        """Generate cache file path."""
        key_parts = [symbol, interval, source]
        if extra:
            extra_str = str(sorted(extra.items()))
            key_parts.append(extra_str)
        key_str = "|".join(key_parts)
        filename = hashlib.md5(key_str.encode()).hexdigest()[:16] + ".pkl"
        return self.cache_dir / source / filename

    def get(self, symbol: str, interval: str, source: str,
            extra: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        """Retrieve data from disk cache."""
        path = self._make_path(symbol, interval, source, extra)

        if not path.exists():
            return None

        try:
            with open(path, "rb") as f:
                entry: CacheEntry = pickle.load(f)

            if entry.is_valid():
                logger.debug("DiskCache HIT: %s %s %s", symbol, interval, source)
                return entry.data
            else:
                logger.debug("DiskCache EXPIRED: %s %s %s", symbol, interval, source)
                path.unlink(missing_ok=True)
                return None
        except Exception as e:
            logger.warning("DiskCache ERROR reading %s: %s", path, e)
            return None

    def set(self, symbol: str, interval: str, source: str, data: Any,
            ttl: Optional[int] = None, extra: Optional[Dict[str, Any]] = None) -> None:
        """Store data to disk cache."""
        path = self._make_path(symbol, interval, source, extra)
        path.parent.mkdir(parents=True, exist_ok=True)

        try:
            entry = CacheEntry(
                data=data,
                timestamp=datetime.now(),
                ttl_seconds=ttl or self.default_ttl,
                source=source
            )
            with open(path, "wb") as f:
                pickle.dump(entry, f, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            logger.warning("DiskCache ERROR writing %s: %s", path, e)

    def clear(self) -> None:
        """Clear all disk cache files."""
        if self.cache_dir.exists():
            import shutil
            shutil.rmtree(self.cache_dir)
            self.cache_dir.mkdir()
            logger.info("Disk cache cleared")

    def stats(self) -> Dict[str, Any]:
        """Get disk cache statistics."""
        if not self.cache_dir.exists():
            return {"total_files": 0}

        files = list(self.cache_dir.rglob("*.pkl"))
        total_size = sum(f.stat().st_size for f in files if f.is_file())
        return {
            "total_files": len(files),
            "total_size_mb": total_size / (1024 * 1024),
        }


class CacheManager:
    """Combined cache manager with both memory and disk layers."""

    def __init__(self, memory_ttl: int = 300, disk_ttl: int = 600):
        """
        Initialize cache manager.

        Args:
            memory_ttl: TTL for memory cache in seconds
            disk_ttl: TTL for disk cache in seconds
        """
        self.memory = MemoryCache(default_ttl=memory_ttl)
        self.disk = DiskCache(default_ttl=disk_ttl)
        self._lock = threading.Lock()

    def get(self, symbol: str, interval: str, source: str,
            extra: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        """
        Retrieve from cache (memory first, then disk).

        Returns None if not found or expired.
        """
        # Try memory cache first
        data = self.memory.get(symbol, interval, source, extra)
        if data is not None:
            return data

        # Try disk cache
        data = self.disk.get(symbol, interval, source, extra)
        if data is not None:
            # Populate memory cache for faster future access
            self.memory.set(symbol, interval, source, data)
            return data

        return None

    def set(self, symbol: str, interval: str, source: str, data: Any,
            ttl: Optional[int] = None, extra: Optional[Dict[str, Any]] = None) -> None:
        """Store in both memory and disk caches."""
        self.memory.set(symbol, interval, source, data, ttl, extra)
        self.disk.set(symbol, interval, source, data, ttl, extra)

    def clear_all(self) -> None:
        """Clear both memory and disk caches."""
        self.memory.clear()
        self.disk.clear()

    def stats(self) -> Dict[str, Any]:
        """Combined statistics."""
        return {
            "memory": self.memory.stats(),
            "disk": self.disk.stats(),
        }
