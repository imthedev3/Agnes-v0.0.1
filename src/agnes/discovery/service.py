from typing import Dict, Any, Optional, List, Callable
import asyncio
import json
import consul.aio
import etcd3.client
from dataclasses import dataclass
import aiohttp
import socket
import time
import hashlib
from random import shuffle
import dns.resolver
from urllib.parse import urlparse

@dataclass
class ServiceInstance:
    id: str
    name: str
    host: str
    port: int
    metadata: Dict[str, Any]
    health_check: str
    last_check: float
    status: str = "unknown"

class ServiceRegistry:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config.get('type', 'consul')
        self.client = None
        self.local_services: Dict[str, ServiceInstance] = {}
        self.health_check_interval = config.get('health_check_interval', 30)
        self._setup_client()
    
    def _setup_client(self):
        """Setup service registry client"""
        if self.type == 'consul':
            self.client = consul.aio.Consul(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 8500)
            )
        elif self.type == 'etcd':
            self.client = etcd3.client.Etcd3Client(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 2379)
            )
    
    async def register(self, instance: ServiceInstance):
        """Register service instance"""
        if self.type == 'consul':
            await self.client.agent.service.register(
                name=instance.name,
                service_id=instance.id,
                address=instance.host,
                port=instance.port,
                tags=[],
                meta=instance.metadata,
                check={
                    "http": instance.health_check,
                    "interval": f"{self.health_check_interval}s"
                }
            )
        elif self.type == 'etcd':
            key = f"/services/{instance.name}/{instance.id}"
            value = json.dumps({
                "id": instance.id,
                "name": instance.name,
                "host": instance.host,
                "port": instance.port,
                "metadata": instance.metadata,
                "health_check": instance.health_check,
                "last_check": time.time()
            })
            await self.client.put(key, value, lease=self.health_check_interval)
        
        self.local_services[instance.id] = instance
    
    async def deregister(self, instance_id: str):
        """Deregister service instance"""
        if self.type == 'consul':
            await self.client.agent.service.deregister(instance_id)
        elif self.type == 'etcd':
            instance = self.local_services.get(instance_id)
            if instance:
                key = f"/services/{instance.name}/{instance_id}"
                await self.client.delete(key)
        
        if instance_id in self.local_services:
            del self.local_services[instance_id]
    
    async def get_services(self, name: str) -> List[ServiceInstance]:
        """Get all instances of a service"""
        instances = []
        
        if self.type == 'consul':
            services = await self.client.health.service(name)
            for service in services[1]:
                instance = ServiceInstance(
                    id=service['Service']['ID'],
                    name=service['Service']['Service'],
                    host=service['Service']['Address'],
                    port=service['Service']['Port'],
                    metadata=service['Service']['Meta'],
                    health_check=service['Checks'][0]['HTTP'],
                    last_check=time.time(),
                    status=service['Checks'][0]['Status']
                )
                instances.append(instance)
        
        elif self.type == 'etcd':
            prefix = f"/services/{name}/"
            async for item in self.client.get_prefix(prefix):
                data = json.loads(item.value)
                instance = ServiceInstance(**data)
                instances.append(instance)
        
        return instances
    
    async def watch_services(self, 
                           name: str, 
                           callback: Callable[[List[ServiceInstance]], None]):
        """Watch service changes"""
        if self.type == 'consul':
            index = None
            while True:
                try:
                    index, services = await self.client.health.service(
                        name,
                        index=index
                    )
                    instances = []
                    for service in services:
                        instance = ServiceInstance(
                            id=service['Service']['ID'],
                            name=service['Service']['Service'],
                            host=service['Service']['Address'],
                            port=service['Service']['Port'],
                            metadata=service['Service']['Meta'],
                            health_check=service['Checks'][0]['HTTP'],
                            last_check=time.time(),
                            status=service['Checks'][0]['Status']
                        )
                        instances.append(instance)
                    await callback(instances)
                except Exception as e:
                    print(f"Error watching services: {e}")
                    await asyncio.sleep(5)
        
        elif self.type == 'etcd':
            prefix = f"/services/{name}/"
            async for event in self.client.watch_prefix(prefix):
                try:
                    instances = []
                    async for item in self.client.get_prefix(prefix):
                        data = json.loads(item.value)
                        instance = ServiceInstance(**data)
                        instances.append(instance)
                    await callback(instances)
                except Exception as e:
                    print(f"Error watching services: {e}")

class ServiceDiscovery:
    def __init__(self, registry: ServiceRegistry):
        self.registry = registry
        self.cache: Dict[str, List[ServiceInstance]] = {}
        self.cache_time: Dict[str, float] = {}
        self.cache_ttl = 60  # seconds
        self.load_balancers: Dict[str, 'LoadBalancer'] = {}
    
    async def get_service(self, 
                         name: str, 
                         strategy: str = "round_robin") -> Optional[ServiceInstance]:
        """Get service instance using load balancing"""
        if name not in self.load_balancers:
            self.load_balancers[name] = LoadBalancer(strategy)
        
        instances = await self._get_cached_instances(name)
        if not instances:
            return None
        
        return self.load_balancers[name].choose(instances)
    
    async def _get_cached_instances(self, 
                                  name: str) -> List[ServiceInstance]:
        """Get cached service instances"""
        now = time.time()
        if (name not in self.cache or 
            now - self.cache_time.get(name, 0) > self.cache_ttl):
            instances = await self.registry.get_services(name)
            self.cache[name] = instances
            self.cache_time[name] = now
        
        return self.cache.get(name, [])

class LoadBalancer:
    def __init__(self, strategy: str = "round_robin"):
        self.strategy = strategy
        self.current_index = 0
        self.weights: Dict[str, int] = {}
    
    def choose(self, instances: List[ServiceInstance]) -> ServiceInstance:
        """Choose instance based on strategy"""
        if not instances:
            return None
        
        if self.strategy == "round_robin":
            return self._round_robin(instances)
        elif self.strategy == "random":
            return self._random(instances)
        elif self.strategy == "weighted":
            return self._weighted(instances)
        elif self.strategy == "least_connections":
            return self._least_connections(instances)
        else:
            raise ValueError(f"Unknown load balancing strategy: {self.strategy}")
    
    def _round_robin(self, 
                    instances: List[ServiceInstance]) -> ServiceInstance:
        """Round-robin selection"""
        instance = instances[self.current_index % len(instances)]
        self.current_index += 1
        return instance
    
    def _random(self, instances: List[ServiceInstance]) -> ServiceInstance:
        """Random selection"""
        shuffle(instances)
        return instances[0]
    
    def _weighted(self, instances: List[ServiceInstance]) -> ServiceInstance:
        """Weighted selection"""
        total_weight = sum(
            instance.metadata.get('weight', 1)
            for instance in instances
        )
        point = (self.current_index % total_weight) + 1
        
        current_weight = 0
        for instance in instances:
            current_weight += instance.metadata.get('weight', 1)
            if current_weight >= point:
                self.current_index += 1
                return instance
        
        return instances[0]
    
    def _least_connections(self, 
                          instances: List[ServiceInstance]) -> ServiceInstance:
        """Least connections selection"""
        return min(
            instances,
            key=lambda x: x.metadata.get('connections', 0)
        )

class ServiceHealthCheck:
    def __init__(self, registry: ServiceRegistry):
        self.registry = registry
        self.http_client = aiohttp.ClientSession()
    
    async def check_health(self, instance: ServiceInstance):
        """Check service health"""
        try:
            async with self.http_client.get(instance.health_check) as response:
                instance.status = "passing" if response.status == 200 else "critical"
        except Exception:
            instance.status = "critical"
        
        instance.last_check = time.time()
    
    async def start_health_checks(self):
        """Start periodic health checks"""
        while True:
            for instance in self.registry.local_services.values():
                await self.check_health(instance)
            
            await asyncio.sleep(
                self.registry.health_check_interval
            )
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.http_client.close()
