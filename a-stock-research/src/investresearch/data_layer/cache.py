"""简单JSON文件缓存 - 减少重复API调用"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from pathlib import Path
from typing import Any

from ..core.logging import get_logger

logger = get_logger("cache")


class FileCache:
    """基于JSON文件的缓存

    用法:
        cache = FileCache(cache_dir="data/cache")
        cache.set("stock_info_600519", {"name": "贵州茅台"}, ttl=86400)
        data = cache.get("stock_info_600519")
    """

    def __init__(self, cache_dir: str = "data/cache", default_ttl: int = 86400) -> None:
        self._cache_dir = Path(cache_dir)
        self._default_ttl = default_ttl
        self._lock = threading.Lock()
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def _key_to_path(self, key: str) -> Path:
        """将缓存key转为文件路径 (MD5 hash)"""
        h = hashlib.md5(key.encode()).hexdigest()
        return self._cache_dir / f"{h}.json"

    def get(self, key: str) -> Any | None:
        """获取缓存值，过期返回None"""
        path = self._key_to_path(key)
        if not path.exists():
            return None

        try:
            with open(path, "r", encoding="utf-8") as f:
                entry = json.load(f)

            # 检查TTL
            if entry.get("expires_at", 0) < time.time():
                path.unlink(missing_ok=True)
                return None

            return entry["value"]

        except (json.JSONDecodeError, KeyError, OSError):
            return None

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """设置缓存值"""
        ttl = ttl if ttl is not None else self._default_ttl
        path = self._key_to_path(key)

        entry = {
            "key": key,
            "value": value,
            "expires_at": time.time() + ttl,
            "created_at": time.time(),
        }

        with self._lock:
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(entry, f, ensure_ascii=False, default=str)
            except OSError as e:
                logger.warning(f"缓存写入失败: {e}")

    def delete(self, key: str) -> None:
        """删除缓存"""
        path = self._key_to_path(key)
        path.unlink(missing_ok=True)

    def clear(self) -> int:
        """清空所有缓存，返回删除文件数"""
        count = 0
        for path in self._cache_dir.glob("*.json"):
            path.unlink(missing_ok=True)
            count += 1
        return count
