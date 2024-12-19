from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import time
import asyncio
import psutil
import prometheus_client as prom
from prometheus_client.core import CollectorRegistry
from prometheus_client import start_http_server
import functools

@dataclass
class MetricConfig:
    name: str
    type: str  # counter, gauge, histogram, summary
    description: str
    labels: Optional[List[str]] = None
    buckets: Optional[List[float]] = None  # for histogram
    quantiles: Optional[List[float]] = None  # for summary

class MetricsCollector:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.metrics: Dict[str, Any] = {}
    
    def create_metric(self, config: MetricConfig):
        """Create a new metric based on configuration"""
        metric_types = {
            'counter': prom.Counter,
            'gauge': prom.Gauge,
            'histogram': prom.Histogram,
            'summary': prom.Summary
        }
        
        if config.type not in metric_types:
            raise ValueError(f"Unsupported metric type: {config.type}")
        
        kwargs = {
            'name': config.name,
            'documentation': config.description,
            'registry': self.registry
        }
        
        if config.labels:
            kwargs['labelnames'] = config.labels
        
        if config.type == 'histogram' and config.buckets:
            kwargs['buckets'] = config.buckets
        
        if config.type == 'summary' and config.quantiles:
            kwargs['quantiles'] = config.quantiles
        
        self.metrics[config.name] = metric_types[config.type](**kwargs)
        return self.metrics[config.name]
    
    def get_metric(self, name: str) -> Optional[Any]:
        """Get existing metric by name"""
        return self.metrics.get(name)
    
    def measure_time(self, metric_name: str):
        """Decorator to measure function execution time"""
        def decorator(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = await func(*args, **kwargs)
                    self.metrics[metric_name].observe(time.time() - start_time)
                    return result
                except Exception as e:
                    self.metrics[f"{metric_name}_errors"].inc()
                    raise e
            
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    self.metrics[metric_name].observe(time.time() - start_time)
                    return result
                except Exception as e:
                    self.metrics[f"{metric_name}_errors"].inc()
                    raise e
            
            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
        return decorator

class SystemMetricsCollector:
    def __init__(self, collector: MetricsCollector):
        self.collector = collector
        self._setup_metrics()
    
    def _setup_metrics(self):
        """Setup system metrics"""
        metrics = [
            MetricConfig(
                name="system_cpu_usage",
                type="gauge",
                description="CPU usage percentage"
            ),
            MetricConfig(
                name="system_memory_usage",
                type="gauge",
                description="Memory usage in bytes"
            ),
            MetricConfig(
                name="system_disk_usage",
                type="gauge",
                description="Disk usage percentage",
                labels=["path"]
            ),
            MetricConfig(
                name="system_network_bytes",
                type="counter",
                description="Network bytes transferred",
                labels=["direction"]
            )
        ]
        
        for metric_config in metrics:
            self.collector.create_metric(metric_config)
    
    async def collect_metrics(self):
        """Collect system metrics"""
        while True:
            # CPU metrics
            cpu_percent = psutil.cpu_percent()
            self.collector.get_metric("system_cpu_usage").set(cpu_percent)
            
            # Memory metrics
            memory = psutil.virtual_memory()
            self.collector.get_metric("system_memory_usage").set(memory.used)
            
            # Disk metrics
            for partition in psutil.disk_partitions():
                usage = psutil.disk_usage(partition.mountpoint)
                self.collector.get_metric("system_disk_usage").labels(
                    partition.mountpoint
                ).set(usage.percent)
            
            # Network metrics
            net_io = psutil.net_io_counters()
            self.collector.get_metric("system_network_bytes").labels(
                "sent"
            ).inc(net_io.bytes_sent)
            self.collector.get_metric("system_network_bytes").labels(
                "received"
            ).inc(net_io.bytes_recv)
            
            await asyncio.sleep(15)  # Collect every 15 seconds

class ApplicationMetricsCollector:
    def __init__(self, collector: MetricsCollector):
        self.collector = collector
        self._setup_metrics()
    
    def _setup_metrics(self):
        """Setup application metrics"""
        metrics = [
            MetricConfig(
                name="request_duration_seconds",
                type="histogram",
                description="Request duration in seconds",
                labels=["method", "endpoint"],
                buckets=[0.1, 0.5, 1.0, 2.0, 5.0]
            ),
            MetricConfig(
                name="request_count",
                type="counter",
                description="Total request count",
                labels=["method", "endpoint", "status"]
            ),
            MetricConfig(
                name="active_connections",
                type="gauge",
                description="Number of active connections"
            ),
            MetricConfig(
                name="error_count",
                type="counter",
                description="Total error count",
                labels=["type"]
            )
        ]
        
        for metric_config in metrics:
            self.collector.create_metric(metric_config)

class MetricsServer:
    def __init__(self, 
                 collector: MetricsCollector, 
                 host: str = '0.0.0.0', 
                 port: int = 9090):
        self.collector = collector
        self.host = host
        self.port = port
    
    async def start(self):
        """Start metrics server"""
        start_http_server(
            port=self.port,
            addr=self.host,
            registry=self.collector.registry
        )

class MonitoringManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.collector = MetricsCollector()
        self.system_collector = SystemMetricsCollector(self.collector)
        self.app_collector = ApplicationMetricsCollector(self.collector)
        self.server = MetricsServer(
            self.collector,
            host=config.get('host', '0.0.0.0'),
            port=config.get('port', 9090)
        )
    
    async def start(self):
        """Start monitoring system"""
        # Start metrics server
        await self.server.start()
        
        # Start system metrics collection
        asyncio.create_task(self.system_collector.collect_metrics())
    
    def instrument_endpoint(self, method: str, endpoint: str):
        """Decorator for instrumenting API endpoints"""
        def decorator(func):
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                request_duration = self.collector.get_metric(
                    "request_duration_seconds"
                )
                request_count = self.collector.get_metric("request_count")
                active_connections = self.collector.get_metric(
                    "active_connections"
                )
                
                start_time = time.time()
                active_connections.inc()
                
                try:
                    result = await func(*args, **kwargs)
                    request_duration.labels(
                        method=method,
                        endpoint=endpoint
                    ).observe(time.time() - start_time)
                    request_count.labels(
                        method=method,
                        endpoint=endpoint,
                        status="success"
                    ).inc()
                    return result
                except Exception as e:
                    request_count.labels(
                        method=method,
                        endpoint=endpoint,
                        status="error"
                    ).inc()
                    self.collector.get_metric("error_count").labels(
                        type=type(e).__name__
                    ).inc()
                    raise
                finally:
                    active_connections.dec()
            
            return wrapper
        return decorator
