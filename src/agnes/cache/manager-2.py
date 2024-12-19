from typing import Dict, Any, Optional, Union, List, Callable
import asyncio
import json
import redis.asyncio as redis
from datetime import datetime, timedelta
import logging
import hashlib
from dataclasses import dataclass
from enum import Enum
import pickle
import time

class CacheBackend(Enum):
    MEMORY = "memory"
    REDIS = "redis"
    FILE = "file"

@dataclass
class CacheConfig:
    backend: CacheBackend
    ttl: int = 300  # 5 minutes
    max_size: int = 1000
    serializer: str = "json"
    namespace: str = "agnes"

class CacheItem:
    def __init__(self,
                 key: str,
                 value: Any,
                 ttl: Optional[int] = None,
                 created_at: Optional[datetime] = None):
        self.key = key
        self.value = value
        self.ttl = ttl
        self.created_at = created_at or datetime.utcnow()
        self.accessed_at = self.created_at
        self.access_count = 0
    
    def is_expired(self) -> bool:
        """Check if cache item is expired"""
        if not self.ttl:
            return False
        return (datetime.utcnow() - self.created_at).total_seconds() > self.ttl
    
    def access(self):
        """Update item access statistics"""
        self.accessed_at = datetime.utcnow()
        self.access_count += 1

class CacheSerializer:
    @staticmethod
    def serialize(value: Any, format: str = "json") -> bytes:
        """Serialize value to bytes"""
        if format == "json":
            return json.dumps(value).encode()
        elif format == "pickle":
            return pickle.dumps(value)
        else:
            raise ValueError(f"Unknown serialization format: {format}")
    
    @staticmethod
    def deserialize(data: bytes, format: str = "json") -> Any:
        """Deserialize value from bytes"""
        if format == "json":
            return json.loads(data.decode())
        elif format == "pickle":
            return pickle.loads(data)
        else:
            raise ValueError(f"Unknown serialization format: {format}")

class MemoryCache:
    def __init__(self, config: CacheConfig):
        self.config = config
        self.items: Dict[str, CacheItem] = {}
        self.lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        async with self.lock:
            item = self.items.get(key)
            if not item:
                return None
            
            if item.is_expired():
                del self.items[key]
                return None
            
            item.access()
            return item.value
    
    async def set(self,
                  key: str,
                  value: Any,
                  ttl: Optional[int] = None):
        """Set cache value"""
        async with self.lock:
            # Check cache size limit
            if len(self.items) >= self.config.max_size:
                self._evict_items()
            
            self.items[key] = CacheItem(
                key=key,
                value=value,
                ttl=ttl or self.config.ttl
            )
    
    async def delete(self, key: str):
        """Delete cache value"""
        async with self.lock:
            if key in self.items:
                del self.items[key]
    
    async def clear(self):
        """Clear all cache items"""
        async with self.lock:
            self.items.clear()
    
    def _evict_items(self):
        """Evict items when cache is full"""
        # Remove expired items first
        expired = [
            key for key, item in self.items.items()
            if item.is_expired()
        ]
        for key in expired:
            del self.items[key]
        
        # If still need to evict, remove least recently used
        if len(self.items) >= self.config.max_size:
            sorted_items = sorted(
                self.items.items(),
                key=lambda x: x[1].accessed_at
            )
            to_remove = len(self.items) - self.config.max_size + 1
            for key, _ in sorted_items[:to_remove]:
                del self.items[key]

class RedisCache:
    def __init__(self, config: CacheConfig, redis_url: str):
        self.config = config
        self.redis = redis.Redis.from_url(redis_url)
        self.serializer = CacheSerializer()
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        full_key = f"{self.config.namespace}:{key}"
        data = await self.redis.get(full_key)
        if data:
            return self.serializer.deserialize(
                data,
                self.config.serializer
            )
        return None
    
    async def set(self,
                  key: str,
                  value: Any,
                  ttl: Optional[int] = None):
        """Set cache value"""
        full_key = f"{self.config.namespace}:{key}"
        data = self.serializer.serialize(
            value,
            self.config.serializer
        )
        
        if ttl is None:
            ttl = self.config.ttl
        
        await self.redis.set(
            full_key,
            data,
            ex=ttl
        )
    
    async def delete(self, key: str):
        """Delete cache value"""
        full_key = f"{self.config.namespace}:{key}"
        await self.redis.delete(full_key)
    
    async def clear(self):
        """Clear all cache items"""
        pattern = f"{self.config.namespace}:*"
        keys = await self.redis.keys(pattern)
        if keys:
            await self.redis.delete(*keys)

class CacheManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.backends: Dict[str, Union[MemoryCache, RedisCache]] = {}
        self.logger = logging.getLogger(__name__)
        self._setup_backends()
    
    def _setup_backends(self):
        """Setup cache backends"""
        for name, backend_config in self.config['backends'].items():
            cache_config = CacheConfig(
                backend=CacheBackend(backend_config['type']),
                ttl=backend_config.get('ttl', 300),
                max_size=backend_config.get('max_size', 1000),
                serializer=backend_config.get('serializer', 'json'),
                namespace=backend_config.get('namespace', 'agnes')
            )
            
            if cache_config.backend == CacheBackend.MEMORY:
                self.backends[name] = MemoryCache(cache_config)
            elif cache_config.backend == CacheBackend.REDIS:
                self.backends[name] = RedisCache(
                    cache_config,
                    backend_config['url']
                )
    
    async def get(self,
                  key: str,
                  backend: str = "default") -> Optional[Any]:
        """Get value from cache"""
        cache = self.backends.get(backend)
        if not cache:
            raise ValueError(f"Cache backend {backend} not found")
        
        try:
            return await cache.get(key)
        except Exception as e:
            self.logger.error(f"Error getting cache value: {e}")
            return None
    
    async def set(self,
                  key: str,
                  value: Any,
                  ttl: Optional[int] = None,
                  backend: str = "default"):
        """Set cache value"""
        cache = self.backends.get(backend)
        if not cache:
            raise ValueError(f"Cache backend {backend} not found")
        
        try:
            await cache.set(key, value, ttl)
        except Exception as e:
            self.logger.error(f"Error setting cache value: {e}")
    
    async def delete(self, key: str, backend: str = "default"):
        """Delete cache value"""
        cache = self.backends.get(backend)
        if not cache:
            raise ValueError(f"Cache backend {backend} not found")
        
        try:
            await cache.delete(key)
        except Exception as e:
            self.logger.error(f"Error deleting cache value: {e}")
    
    async def clear(self, backend: str = "default"):
        """Clear cache"""
        cache = self.backends.get(backend)
        if not cache:
            raise ValueError(f"Cache backend {backend} not found")
        
        try:
            await cache.clear()
        except Exception as e:
            self.logger.error(f"Error clearing cache: {e}")

class CacheDecorator:
    def __init__(self,
                 manager: CacheManager,
                 ttl: Optional[int] = None,
                 backend: str = "default"):
        self.manager = manager
        self.ttl = ttl
        self.backend = backend
    
    def __call__(self, func: Callable):
        """Decorator for caching function results"""
        async def wrapper(*args, **kwargs):
            # Generate cache key
            key = self._generate_key(func.__name__, args, kwargs)
            
            # Try to get from cache
            result = await self.manager.get(key, self.backend)
            if result is not None:
                return result
            
            # Execute function
            result = await func(*args, **kwargs)
            
            # Cache result
            await self.manager.set(
                key,
                result,
                self.ttl,
                self.backend
            )
            
            return result
        
        return wrapper
    
    def _generate_key(self,
                     func_name: str,
                     args: tuple,
                     kwargs: dict) -> str:
        """Generate cache key from function call"""
        key_parts = [func_name]
        
        # Add args
        for arg in args:
            key_parts.append(str(arg))
        
        # Add kwargs
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={v}")
        
        # Generate hash
        key_str = ":".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()
