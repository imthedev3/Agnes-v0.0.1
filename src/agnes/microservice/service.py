from typing import Dict, Any, List, Optional, Union, Type, Callable
import asyncio
import aiohttp
import logging
from dataclasses import dataclass
from enum import Enum
import json
import time
from datetime import datetime
import consul.aio
import grpcio
import grpcio_health
import prometheus_client as prom
import opentelemetry
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
import nats
from circuitbreaker import circuit
import jwt
import msgpack
import aiormq
import aio_pika

class ServiceState(Enum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"

@dataclass
class ServiceConfig:
    name: str
    version: str
    host: str
    port: int
    discovery: Dict[str, Any]
    metrics: Dict[str, Any]
    tracing: Dict[str, Any]
    logging: Dict[str, Any]
    auth: Dict[str, Any]
    dependencies: List[str]

class Microservice:
    def __init__(self, config: ServiceConfig):
        self.config = config
        self.state = ServiceState.STOPPED
        self.logger = logging.getLogger(config.name)
        
        # Initialize components
        self.discovery = ServiceDiscovery(config.discovery)
        self.metrics = MetricsManager(config.metrics)
        self.tracer = TracingManager(config.tracing)
        self.transport = TransportManager()
        self.health = HealthManager()
        
        # Setup service registry
        self.endpoints = {}
        self.event_handlers = {}
        
        # Initialize communication channels
        self.nats = None
        self.rmq = None
        
        # Setup circuit breaker
        self.circuit_breaker = CircuitBreaker()
    
    async def start(self):
        """Start microservice"""
        try:
            self.state = ServiceState.STARTING
            self.logger.info(f"Starting service {self.config.name}")
            
            # Initialize components
            await self._init_components()
            
            # Register with service discovery
            await self.discovery.register(
                self.config.name,
                self.config.host,
                self.config.port
            )
            
            # Start health checks
            await self.health.start()
            
            # Start metrics collection
            self.metrics.start()
            
            # Initialize communication
            await self._init_communication()
            
            self.state = ServiceState.RUNNING
            self.logger.info(f"Service {self.config.name} started")
        
        except Exception as e:
            self.logger.error(f"Failed to start service: {e}")
            await self.stop()
            raise
    
    async def stop(self):
        """Stop microservice"""
        self.state = ServiceState.STOPPING
        self.logger.info(f"Stopping service {self.config.name}")
        
        try:
            # Stop communication
            await self._stop_communication()
            
            # Deregister from service discovery
            await self.discovery.deregister(self.config.name)
            
            # Stop health checks
            await self.health.stop()
            
            # Stop metrics collection
            self.metrics.stop()
            
            self.state = ServiceState.STOPPED
            self.logger.info(f"Service {self.config.name} stopped")
        
        except Exception as e:
            self.logger.error(f"Error stopping service: {e}")
            raise
    
    def endpoint(self,
                path: str,
                method: str = "GET",
                auth_required: bool = False):
        """Decorator for registering HTTP endpoints"""
        def decorator(handler):
            self.endpoints[f"{method}:{path}"] = {
                "handler": handler,
                "auth_required": auth_required
            }
            return handler
        return decorator
    
    def event_handler(self, event_type: str):
        """Decorator for registering event handlers"""
        def decorator(handler):
            self.event_handlers[event_type] = handler
            return handler
        return decorator
    
    async def call_service(self,
                          service: str,
                          endpoint: str,
                          method: str = "GET",
                          data: Any = None,
                          timeout: int = 30) -> Any:
        """Call another microservice"""
        # Get service instance from discovery
        instance = await self.discovery.get_service(service)
        if not instance:
            raise ServiceNotFoundError(f"Service {service} not found")
        
        # Build request URL
        url = f"http://{instance['host']}:{instance['port']}{endpoint}"
        
        # Create trace context
        with self.tracer.start_span(
            f"call_service_{service}_{endpoint}"
        ) as span:
            try:
                # Call service with circuit breaker
                return await self.circuit_breaker.call(
                    self._make_http_request,
                    url=url,
                    method=method,
                    data=data,
                    timeout=timeout
                )
            
            except Exception as e:
                span.record_exception(e)
                raise
    
    async def publish_event(self,
                          event_type: str,
                          data: Any):
        """Publish event to message broker"""
        if self.nats:
            # Publish to NATS
            await self.nats.publish(
                f"{self.config.name}.{event_type}",
                msgpack.packb(data)
            )
        
        if self.rmq:
            # Publish to RabbitMQ
            exchange = await self.rmq.get_exchange(
                f"{self.config.name}.events"
            )
            await exchange.publish(
                aio_pika.Message(
                    body=msgpack.packb(data)
                ),
                routing_key=event_type
            )
    
    async def _init_components(self):
        """Initialize service components"""
        # Setup logging
        logging.config.dictConfig(self.config.logging)
        
        # Initialize tracing
        self.tracer.init_tracer(
            self.config.name,
            self.config.tracing
        )
        
        # Setup metrics
        self.metrics.init_metrics(self.config.name)
        
        # Initialize health checks
        self.health.add_check(
            "startup",
            self._check_startup
        )
        for dep in self.config.dependencies:
            self.health.add_check(
                f"dependency_{dep}",
                lambda: self._check_dependency(dep)
            )
    
    async def _init_communication(self):
        """Initialize communication channels"""
        # Connect to NATS if configured
        if self.config.get('nats'):
            self.nats = await nats.connect(
                self.config['nats']['url']
            )
            
            # Subscribe to events
            for event_type, handler in self.event_handlers.items():
                await self.nats.subscribe(
                    f"*.{event_type}",
                    cb=lambda msg: self._handle_nats_event(
                        msg,
                        handler
                    )
                )
        
        # Connect to RabbitMQ if configured
        if self.config.get('rabbitmq'):
            self.rmq = await aio_pika.connect_robust(
                self.config['rabbitmq']['url']
            )
            
            # Setup exchanges and queues
            channel = await self.rmq.channel()
            exchange = await channel.declare_exchange(
                f"{self.config.name}.events",
                aio_pika.ExchangeType.TOPIC
            )
            
            # Setup event handlers
            for event_type, handler in self.event_handlers.items():
                queue = await channel.declare_queue(
                    f"{self.config.name}.{event_type}"
                )
                await queue.bind(
                    exchange,
                    routing_key=event_type
                )
                await queue.consume(
                    lambda msg: self._handle_rmq_event(
                        msg,
                        handler
                    )
                )
    
    async def _stop_communication(self):
        """Stop communication channels"""
        if self.nats:
            await self.nats.close()
        
        if self.rmq:
            await self.rmq.close()
    
    async def _make_http_request(self,
                               url: str,
                               method: str,
                               data: Any = None,
                               timeout: int = 30) -> Any:
        """Make HTTP request to another service"""
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method,
                url,
                json=data,
                timeout=timeout,
                headers=self._get_trace_headers()
            ) as response:
                return await response.json()
    
    def _get_trace_headers(self) -> Dict[str, str]:
        """Get tracing headers for requests"""
        return {
            'X-Trace-ID': trace.get_current_span().get_span_context().trace_id,
            'X-Span-ID': trace.get_current_span().get_span_context().span_id
        }
    
    async def _check_startup(self) -> bool:
        """Startup health check"""
        return self.state == ServiceState.RUNNING
    
    async def _check_dependency(self, service: str) -> bool:
        """Check dependency health"""
        try:
            instance = await self.discovery.get_service(service)
            if not instance:
                return False
            
            # Make health check request
            url = f"http://{instance['host']}:{instance['port']}/health"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as response:
                    return response.status == 200
        
        except Exception:
            return False
    
    async def _handle_nats_event(self,
                               msg: nats.aio.msg.Msg,
                               handler: Callable):
        """Handle NATS event"""
        try:
            data = msgpack.unpackb(msg.data)
            await handler(data)
            await msg.ack()
        
        except Exception as e:
            self.logger.error(f"Error handling NATS event: {e}")
            await msg.nak()
    
    async def _handle_rmq_event(self,
                              msg: aio_pika.IncomingMessage,
                              handler: Callable):
        """Handle RabbitMQ event"""
        try:
            async with msg.process():
                data = msgpack.unpackb(msg.body)
                await handler(data)
        
        except Exception as e:
            self.logger.error(f"Error handling RMQ event: {e}")
            await msg.reject()

class ServiceDiscovery:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.consul = consul.aio.Consul(
            host=config['consul']['host'],
            port=config['consul']['port']
        )
    
    async def register(self,
                      name: str,
                      host: str,
                      port: int):
        """Register service with Consul"""
        await self.consul.agent.service.register(
            name=name,
            service_id=f"{name}-{host}-{port}",
            address=host,
            port=port,
            check={
                "http": f"http://{host}:{port}/health",
                "interval": "10s",
                "timeout": "5s"
            }
        )
    
    async def deregister(self, name: str):
        """Deregister service from Consul"""
        await self.consul.agent.service.deregister(
            f"{name}-{self.config['host']}-{self.config['port']}"
        )
    
    async def get_service(self,
                         name: str) -> Optional[Dict[str, Any]]:
        """Get service instance from Consul"""
        index, services = await self.consul.health.service(
            name,
            passing=True
        )
        
        if services:
            service = services[0]['Service']
            return {
                'host': service['Address'],
                'port': service['Port']
            }
        
        return None

class MetricsManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.metrics = {}
    
    def init_metrics(self, service_name: str):
        """Initialize service metrics"""
        self.metrics['requests_total'] = prom.Counter(
            'service_requests_total',
            'Total requests processed',
            ['service', 'endpoint', 'method', 'status']
        )
        
        self.metrics['request_duration_seconds'] = prom.Histogram(
            'service_request_duration_seconds',
            'Request duration in seconds',
            ['service', 'endpoint', 'method']
        )
        
        self.metrics['dependencies_up'] = prom.Gauge(
            'service_dependencies_up',
            'Dependency status',
            ['service', 'dependency']
        )
    
    def start(self):
        """Start metrics server"""
        if self.config.get('prometheus', {}).get('enabled', False):
            prom.start_http_server(
                self.config['prometheus']['port']
            )
    
    def stop(self):
        """Stop metrics collection"""
        pass

class TracingManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.tracer = None
    
    def init_tracer(self,
                   service_name: str,
                   config: Dict[str, Any]):
        """Initialize OpenTelemetry tracer"""
        if not config.get('enabled', False):
            return
        
        # Setup Jaeger exporter
        jaeger_exporter = JaegerExporter(
            agent_host_name=config['jaeger']['host'],
            agent_port=config['jaeger']['port']
        )
        
        # Configure tracer
        trace.set_tracer_provider(
            trace.TracerProvider(
                resource=Resource.create({
                    "service.name": service_name
                })
            )
        )
        
        # Add Jaeger exporter
        span_processor = BatchSpanProcessor(jaeger_exporter)
        trace.get_tracer_provider().add_span_processor(
            span_processor
        )
        
        self.tracer = trace.get_tracer(service_name)
    
    def start_span(self,
                  name: str,
                  context: Optional[Dict] = None):
        """Start new trace span"""
        return self.tracer.start_as_current_span(
            name,
            context=context
        )

class HealthManager:
    def __init__(self):
        self.checks = {}
        self.is_healthy = True
        self._check_task = None
    
    def add_check(self,
                 name: str,
                 check: Callable[[], bool]):
        """Add health check"""
        self.checks[name] = check
    
    async def start(self):
        """Start health checking"""
        self._check_task = asyncio.create_task(
            self._run_checks()
        )
    
    async def stop(self):
        """Stop health checking"""
        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
    
    async def _run_checks(self):
        """Run health checks periodically"""
        while True:
            try:
                results = {}
                
                for name, check in self.checks.items():
                    try:
                        results[name] = await check()
                    except Exception as e:
                        results[name] = False
                        logging.error(
                            f"Health check {name} failed: {e}"
                        )
                
                self.is_healthy = all(results.values())
            
            except Exception as e:
                logging.error(f"Error running health checks: {e}")
                self.is_healthy = False
            
            await asyncio.sleep(10)

class CircuitBreaker:
    def __init__(self):
        self.states = {}
    
    @circuit
    async def call(self,
                  func: Callable,
                  *args,
                  **kwargs):
        """Call function with circuit breaker"""
        return await func(*args, **kwargs)

class ServiceNotFoundError(Exception):
    pass
