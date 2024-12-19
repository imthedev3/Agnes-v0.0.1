from typing import Dict, Any, List, Optional, Callable
import asyncio
import random
import time
from dataclasses import dataclass
from enum import Enum
import aiohttp
import socket

class BalancingStrategy(Enum):
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    IP_HASH = "ip_hash"
    RANDOM = "random"

@dataclass
class ServerConfig:
    host: str
    port: int
    weight: int = 1
    max_connections: int = 100
    health_check_path: str = "/health"

@dataclass
class ServerStatus:
    server: ServerConfig
    active: bool = True
    current_connections: int = 0
    last_check_time: float = 0
    failed_checks: int = 0

class LoadBalancer:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.servers: List[ServerStatus] = []
        self.current_index = 0
        self.strategy = BalancingStrategy(config.get("strategy", "round_robin"))
        self.session = aiohttp.ClientSession()
    
    async def initialize(self):
        """Initialize load balancer with servers"""
        for server_config in self.config["servers"]:
            server = ServerConfig(**server_config)
            self.servers.append(ServerStatus(server=server))
        
        # Start health checks
        asyncio.create_task(self._health_check_loop())
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.session.close()
    
    async def get_next_server(self, 
                            client_ip: Optional[str] = None) -> ServerStatus:
        """Get next available server based on strategy"""
        active_servers = [s for s in self.servers if s.active]
        if not active_servers:
            raise Exception("No active servers available")
        
        if self.strategy == BalancingStrategy.ROUND_ROBIN:
            server = self._round_robin(active_servers)
        elif self.strategy == BalancingStrategy.LEAST_CONNECTIONS:
            server = self._least_connections(active_servers)
        elif self.strategy == BalancingStrategy.WEIGHTED_ROUND_ROBIN:
            server = self._weighted_round_robin(active_servers)
        elif self.strategy == BalancingStrategy.IP_HASH:
            server = self._ip_hash(active_servers, client_ip)
        elif self.strategy == BalancingStrategy.RANDOM:
            server = random.choice(active_servers)
        else:
            raise ValueError(f"Unknown balancing strategy: {self.strategy}")
        
        return server
    
    def _round_robin(self, servers: List[ServerStatus]) -> ServerStatus:
        """Simple round-robin selection"""
        server = servers[self.current_index % len(servers)]
        self.current_index += 1
        return server
    
    def _least_connections(self, 
                          servers: List[ServerStatus]) -> ServerStatus:
        """Select server with least active connections"""
        return min(servers, key=lambda s: s.current_connections)
    
    def _weighted_round_robin(self, 
                            servers: List[ServerStatus]) -> ServerStatus:
        """Weighted round-robin selection"""
        total_weight = sum(s.server.weight for s in servers)
        point = (self.current_index % total_weight) + 1
        
        current_weight = 0
        for server in servers:
            current_weight += server.server.weight
            if current_weight >= point:
                self.current_index += 1
                return server
        
        return servers[0]
    
    def _ip_hash(self, 
                 servers: List[ServerStatus], 
                 client_ip: Optional[str]) -> ServerStatus:
        """Select server based on client IP hash"""
        if not client_ip:
            return self._round_robin(servers)
        
        hash_value = sum(ord(c) for c in client_ip)
        return servers[hash_value % len(servers)]
    
    async def _health_check_loop(self):
        """Continuously check server health"""
        while True:
            tasks = [
                self._check_server_health(server)
                for server in self.servers
            ]
            await asyncio.gather(*tasks)
            await asyncio.sleep(self.config.get("health_check_interval", 30))
    
    async def _check_server_health(self, server: ServerStatus):
        """Check health of individual server"""
        url = f"http://{server.server.host}:{server.server.port}" \
              f"{server.server.health_check_path}"
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    server.active = True
                    server.failed_checks = 0
                else:
                    self._handle_failed_health_check(server)
        except Exception:
            self._handle_failed_health_check(server)
        
        server.last_check_time = time.time()
    
    def _handle_failed_health_check(self, server: ServerStatus):
        """Handle failed health check"""
        server.failed_checks += 1
        if server.failed_checks >= self.config.get("max_failed_checks", 3):
            server.active = False

class LoadBalancerProxy:
    def __init__(self, balancer: LoadBalancer):
        self.balancer = balancer
    
    async def handle_request(self, 
                           request: Any, 
                           client_ip: Optional[str] = None) -> Any:
        """Handle incoming request"""
        server = await self.balancer.get_next_server(client_ip)
        
        try:
            server.current_connections += 1
            return await self._forward_request(server, request)
        finally:
            server.current_connections -= 1
    
    async def _forward_request(self, 
                             server: ServerStatus, 
                             request: Any) -> Any:
        """Forward request to selected server"""
        url = f"http://{server.server.host}:{server.server.port}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=request.method,
                    url=f"{url}{request.path_qs}",
                    headers=request.headers,
                    data=await request.read()
                ) as response:
                    return await response.read()
        except Exception as e:
            # Handle connection error
            server.active = False
            raise Exception(f"Forward request failed: {str(e)}")
