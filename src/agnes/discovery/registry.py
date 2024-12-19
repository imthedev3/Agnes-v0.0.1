from typing import Dict, Any, List, Optional, Union, Callable
import asyncio
import json
import consul.aio
import etcd3.client
import logging
from datetime import datetime
import socket
import aiohttp
import uuid
from dataclasses import dataclass
import random
from enum import Enum

class ServiceStatus(Enum):
    UP = "up"
    DOWN = "down"
    UNKNOWN = "unknown"

@dataclass
class ServiceEndpoint:
    id: str
    name: str
    host: str
    port: int
    metadata: Dict[str, Any]
    status: ServiceStatus
    last_check: datetime
    health_check_url: Optional[str] = None
    weight: int = 100
    version: Optional[str] = None

class ServiceRegistry:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config['type']
        self.services: Dict[str, List[ServiceEndpoint]] = {}
        self.watchers: Dict[str, List[Callable]] = {}
        self.logger = logging.getLogger(__name__)
        self._setup_backend()
    
    def _setup_backend(self):
        """Setup registry backend"""
        if self.type == 'consul':
            self.client = consul.aio.Consul(
                host=self.config['consul_host'],
                port=self.config['consul_port']
            )
        elif self.type == 'etcd':
            self.client = etcd3.client(
                host=self.config['etcd_host'],
                port=self.config['etcd_port']
            )
    
    async def register_service(self,
                             name: str,
                             host: str,
                             port: int,
                             metadata: Dict[str, Any] = None,
                             health_check_url: Optional[str] = None,
                             weight: int = 100,
                             version: Optional[str] = None) -> ServiceEndpoint:
        """Register service endpoint"""
        endpoint = ServiceEndpoint(
            id=str(uuid.uuid4()),
            name=name,
            host=host,
            port=port,
            metadata=metadata or {},
            status=ServiceStatus.UNKNOWN,
            last_check=datetime.utcnow(),
            health_check_url=health_check_url,
            weight=weight,
            version=version
        )
        
        if self.type == 'consul':
            await self._register_consul(endpoint)
        elif self.type == 'etcd':
            await self._register_etcd(endpoint)
        
        if name not in self.services:
            self.services[name] = []
        self.services[name].append(endpoint)
        
        # Start health check
        if health_check_url:
            asyncio.create_task(
                self._health_check_loop(endpoint)
            )
        
        await self._notify_watchers(name, self.services[name])
        return endpoint
    
    async def deregister_service(self,
                                name: str,
                                endpoint_id: str):
        """Deregister service endpoint"""
        if name not in self.services:
            return
        
        self.services[name] = [
            ep for ep in self.services[name]
            if ep.id != endpoint_id
        ]
        
        if self.type == 'consul':
            await self._deregister_consul(endpoint_id)
        elif self.type == 'etcd':
            await self._deregister_etcd(endpoint_id)
        
        await self._notify_watchers(name, self.services[name])
    
    async def get_service(self,
                         name: str,
                         version: Optional[str] = None) -> List[ServiceEndpoint]:
        """Get service endpoints"""
        if name not in self.services:
            if self.type == 'consul':
                await self._load_consul(name)
            elif self.type == 'etcd':
                await self._load_etcd(name)
        
        endpoints = self.services.get(name, [])
        if version:
            endpoints = [
                ep for ep in endpoints
                if ep.version == version
            ]
        
        return endpoints
    
    async def watch_service(self,
                          name: str,
                          callback: Callable[[List[ServiceEndpoint]], None]):
        """Watch service changes"""
        if name not in self.watchers:
            self.watchers[name] = []
        
        self.watchers[name].append(callback)
        
        if self.type == 'consul':
            await self._watch_consul(name)
        elif self.type == 'etcd':
            await self._watch_etcd(name)
    
    async def _health_check_loop(self, endpoint: ServiceEndpoint):
        """Health check loop"""
        while True:
            try:
                status = await self._check_health(endpoint)
                if status != endpoint.status:
                    endpoint.status = status
                    endpoint.last_check = datetime.utcnow()
                    await self._notify_watchers(
                        endpoint.name,
                        self.services[endpoint.name]
                    )
            except Exception as e:
                self.logger.error(
                    f"Health check failed for {endpoint.name}: {e}"
                )
            
            await asyncio.sleep(
                self.config.get('health_check_interval', 30)
            )
    
    async def _check_health(self,
                          endpoint: ServiceEndpoint) -> ServiceStatus:
        """Check endpoint health"""
        if not endpoint.health_check_url:
            return ServiceStatus.UNKNOWN
        
        try:
            url = f"http://{endpoint.host}:{endpoint.port}{endpoint.health_check_url}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        return ServiceStatus.UP
                    else:
                        return ServiceStatus.DOWN
        except:
            return ServiceStatus.DOWN
    
    async def _notify_watchers(self,
                             name: str,
                             endpoints: List[ServiceEndpoint]):
        """Notify service watchers"""
        if name in self.watchers:
            for callback in self.watchers[name]:
                try:
                    await callback(endpoints)
                except Exception as e:
                    self.logger.error(
                        f"Error in service watcher callback: {e}"
                    )

class ServiceDiscovery:
    def __init__(self, registry: ServiceRegistry):
        self.registry = registry
        self.logger = logging.getLogger(__name__)
    
    async def get_endpoint(self,
                          name: str,
                          version: Optional[str] = None) -> Optional[ServiceEndpoint]:
        """Get service endpoint using load balancing"""
        endpoints = await self.registry.get_service(name, version)
        
        # Filter healthy endpoints
        healthy = [
            ep for ep in endpoints
            if ep.status != ServiceStatus.DOWN
        ]
        
        if not healthy:
            return None
        
        # Weighted random selection
        total_weight = sum(ep.weight for ep in healthy)
        r = random.uniform(0, total_weight)
        upto = 0
        
        for endpoint in healthy:
            if upto + endpoint.weight >= r:
                return endpoint
            upto += endpoint.weight
        
        return healthy[-1]
    
    async def get_all_endpoints(self,
                              name: str,
                              version: Optional[str] = None) -> List[ServiceEndpoint]:
        """Get all healthy service endpoints"""
        endpoints = await self.registry.get_service(name, version)
        return [
            ep for ep in endpoints
            if ep.status != ServiceStatus.DOWN
        ]

class ServiceClient:
    def __init__(self,
                 discovery: ServiceDiscovery,
                 timeout: int = 30):
        self.discovery = discovery
        self.timeout = timeout
        self.session = aiohttp.ClientSession()
    
    async def close(self):
        """Close client"""
        await self.session.close()
    
    async def request(self,
                     service: str,
                     method: str,
                     path: str,
                     version: Optional[str] = None,
                     **kwargs) -> aiohttp.ClientResponse:
        """Make request to service"""
        endpoint = await self.discovery.get_endpoint(service, version)
        if not endpoint:
            raise ValueError(f"No healthy endpoints for service {service}")
        
        url = f"http://{endpoint.host}:{endpoint.port}{path}"
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        
        return await self.session.request(
            method,
            url,
            timeout=timeout,
            **kwargs
        )
