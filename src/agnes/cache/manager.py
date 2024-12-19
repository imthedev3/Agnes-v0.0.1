from typing import Dict, Any, Optional, Union, List
import asyncio
import json
import time
from dataclasses import dataclass
import redis.asyncio as redis
from functools import wraps
import hashlib
import pickle

@dataclass
class CacheConfig:
    backend: str  # 'redis', 'memory', 'distributed'
    ttl: int  # seconds
    max_size: Optional[int] = None
    servers: Optional[List[str]] = None
    namespace: str = "agnes"

class CacheItem:
    def __init__(self, value: Any, ttl: int):
        self.value = value
        self.expiry = time.time() + ttl
    
    def is_expired(self) -> bool:
        return time.time() > self.expiry

class MemoryCache:
    def __init__(self, max_size: Optional[int] = None):
        self.cache: Dict[str, CacheItem] = {}
        self.max_size = max_size
    
    async def get(self, key: str) -> Optional[Any]:
        if key not in self.cache:
            return None
        
        item = self.cache[key]
        if item.is_expired():
            del self.cache[key]
            return None
        
        return item.value
    
    async def set(self, key: str, value: Any, ttl: int):
        if self.max_size and len(self.cache) >= self.max_size:
            # Remove oldest item
            oldest_key = min(self.cache.keys(), 
                           key=lambda k: self.cache[k].expiry)
            del self.cache[oldest_key]
        
        self.cache[key] = CacheItem(value, ttl)
    
    async def delete(self, key: str):
        if key in self.cache:
            del self.cache[key]
    
    async def clear(self):
        self.cache.clear()

class RedisCache:
    def __init__(self, redis_client: redis.Redis, namespace: str):
        self.redis = redis_client
        self.namespace = namespace
    
    def _make_key(self, key: str) -> str:
        return f"{self.namespace}:{key}"
    
    async def get(self, key: str) -> Optional[Any]:
        value = await self.redis.get(self._make_key(key))
        if value is None:
            return None
        return pickle.loads(value)
    
    async def set(self, key: str, value: Any, ttl: int):
        await self.redis.setex(
            self._make_key(key),
            ttl,
            pickle.dumps(value)
        )
    
    async def delete(self, key: str):
        await self.redis.delete(self._make_key(key))
    
    async def clear(self):
        pattern = f"{self.namespace}:*"
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(
                cursor,
                match=pattern,
                count=100
            )
            if keys:
                await self.redis.delete(*keys)
            if cursor == 0:
                break

class DistributedCache:
    def __init__(self, servers: List[str], namespace: str):
        self.servers = servers
        self.namespace = namespace
        self.clients: Dict[str, redis.Redis] = {}
        
        for server in servers:
            host, port = server.split(':')
            self.clients[server] = redis.Redis(
                host=host,
                port=int(port)
            )
    
    def _get_server(self, key: str) -> redis.Redis:
        """Get responsible server for key using consistent hashing"""
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        server_index = hash_value % len(self.servers)
        server = self.servers[server_index]
        return self.clients[server]
    
    async def get(self, key: str) -> Optional[Any]:
        client = self._get_server(key)
        value = await client.get(f"{self.namespace}:{key}")
        if value is None:
            return None
        return pickle.loads(value)
    
    async def set(self, key: str, value: Any, ttl: int):
        client = self._get_server(key)
        await client.setex(
            f"{self.namespace}:{key}",
            ttl,
            pickle.dumps(value)
        )
    
    async def delete(self, key: str):
        client = self._get_server(key)
        await client.delete(f"{self.namespace}:{key
