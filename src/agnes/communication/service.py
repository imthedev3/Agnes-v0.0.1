from typing import Dict, Any, Optional, Callable, List
import asyncio
import json
import aio_pika
import grpc
import redis.asyncio as redis
from google.protobuf import message
from dataclasses import dataclass

@dataclass
class ServiceConfig:
    name: str
    version: str
    protocol: str  # 'grpc', 'amqp', 'redis'
    host: str
    port: int
    credentials: Optional[Dict[str, str]] = None

class ServiceRegistry:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.services: Dict[str, ServiceConfig] = {}
        self._redis_client = redis.Redis(
            host=config['redis_host'],
            port=config['redis_port']
        )
    
    async def register_service(self, service: ServiceConfig):
        """Register service in registry"""
        service_key = f"service:{service.name}:{service.version}"
        service_data = {
            "name": service.name,
            "version": service.version,
            "protocol": service.protocol,
            "host": service.host,
            "port": service.port
        }
        
        await self._redis_client.hset(
            service_key,
            mapping=service_data
        )
        await self._redis_client.expire(service_key, 60)  # TTL: 60 seconds
        
        self.services[service.name] = service
    
    async def get_service(self, name: str, version: str) -> Optional[ServiceConfig]:
        """Get service from registry"""
        service_key = f"service:{name}:{version}"
        service_data = await self._redis_client.hgetall(service_key)
        
        if not service_data:
            return None
        
        return ServiceConfig(
            name=service_data[b'name'].decode(),
            version=service_data[b'version'].decode(),
            protocol=service_data[b'protocol'].decode(),
            host=service_data[b'host'].decode(),
            port=int(service_data[b'port'])
        )
    
    async def heartbeat(self):
        """Send heartbeat for registered services"""
        while True:
            for service in self.services.values():
                await self.register_service(service)
            await asyncio.sleep(30)

class ServiceCommunicator:
    def __init__(self, config: Dict[str, Any], registry: ServiceRegistry):
        self.config = config
        self.registry = registry
        self._amqp_connection = None
        self._grpc_channels: Dict[str, grpc.aio.Channel] = {}
        self._redis_client = redis.Redis(
            host=config['redis_host'],
            port=config['redis_port']
        )
    
    async def initialize(self):
        """Initialize communicator"""
        # Connect to RabbitMQ
        self._amqp_connection = await aio_pika.connect_robust(
            self.config['amqp_url']
        )
    
    async def call_service(self,
                          service_name: str,
                          version: str,
                          method: str,
                          data: Any) -> Any:
        """Call remote service"""
        service = await self.registry.get_service(service_name, version)
        if not service:
            raise ValueError(f"Service {service_name}:{version} not found")
        
        if service.protocol == 'grpc':
            return await self._call_grpc(service, method, data)
        elif service.protocol == 'amqp':
            return await self._call_amqp(service, method, data)
        elif service.protocol == 'redis':
            return await self._call_redis(service, method, data)
        else:
            raise ValueError(f"Unsupported protocol: {service.protocol}")
    
    async def _call_grpc(self,
                        service: ServiceConfig,
                        method: str,
                        data: message.Message) -> Any:
        """Call gRPC service"""
        channel_key = f"{service.name}:{service.version}"
        
        if channel_key not in self._grpc_channels:
            self._grpc_channels[channel_key] = grpc.aio.insecure_channel(
                f"{service.host}:{service.port}"
            )
        
        channel = self._grpc_channels[channel_key]
        stub_class = self._get_stub_class(service.name)
        stub = stub_class(channel)
        
        try:
            response = await getattr(stub, method)(data)
            return response
        except grpc.RpcError as e:
            raise Exception(f"gRPC call failed: {e}")
    
    async def _call_amqp(self,
                        service: ServiceConfig,
                        method: str,
                        data: Dict[str, Any]) -> Any:
        """Call service via AMQP"""
        async with self._amqp_connection.channel() as channel:
            # Declare response queue
            response_queue = await channel.declare_queue(
                "",
                exclusive=True,
                auto_delete=True
            )
            
            # Create message
            message = aio_pika.Message(
                body=json.dumps({
                    "method": method,
                    "data": data,
                    "reply_to": response_queue.name
                }).encode(),
                reply_to=response_queue.name
            )
            
            # Send message
            await channel.default_exchange.publish(
                message,
                routing_key=f"{service.name}.{method}"
            )
            
            # Wait for response
            try:
                async with asyncio.timeout(30):
                    response = await response_queue.get()
                    return json.loads(response.body.decode())
            except asyncio.TimeoutError:
                raise Exception("Service call timed out")
    
    async def _call_redis(self,
                         service: ServiceConfig,
                         method: str,
                         data: Dict[str, Any]) -> Any:
        """Call service via Redis"""
        # Generate unique request ID
        request_id = f"{service.name}:{method}:{asyncio.current_task().get_name()}"
        
        # Publish request
        await self._redis_client.publish(
            f"{service.name}.{method}",
            json.dumps({
                "request_id": request_id,
                "data": data
            })
        )
        
        # Wait for response
        try:
            async with asyncio.timeout(30):
                response = await self._redis_client.blpop(request_id, timeout=30)
                if response:
                    return json.loads(response[1].decode())
                raise Exception("No response received")
        except asyncio.TimeoutError:
            raise Exception("Service call timed out")
        finally:
            await self._redis_client.delete(request_id)
    
    def _get_stub_class(self, service_name: str) -> Any:
        """Get gRPC stub class for service"""
        # Import and return appropriate stub class
        # This should be implemented based on your gRPC service definitions
        pass
