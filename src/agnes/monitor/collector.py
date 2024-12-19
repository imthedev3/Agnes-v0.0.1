from typing import Dict, Any, List, Optional, Union
import asyncio
import aiohttp
import psutil
import prometheus_client as prom
from prometheus_client import Counter, Gauge, Histogram, Summary
import logging
from datetime import datetime, timedelta
import json
import socket
import aiomysql
import aioredis
import elasticsearch
from dataclasses import dataclass
from enum import Enum
import netifaces
import docker
import kubernetes
import statsd
import telegraf
import os

class MetricType(Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"

class MetricLevel(Enum):
    SYSTEM = "system"
    APPLICATION = "application"
    BUSINESS = "business"

@dataclass
class Metric:
    name: str
    type: MetricType
    level: MetricLevel
    value: float
    labels: Dict[str, str]
    timestamp: datetime
    description: str = ""

class MetricsCollector:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.storage = MetricsStorage(config['storage'])
        self.logger = logging.getLogger(__name__)
        
        # Initialize Prometheus metrics
        self.metrics = {}
        self._setup_metrics()
        
        # Initialize exporters
        self.exporters = []
        self._setup_exporters()
        
        # System info
        self.hostname = socket.gethostname()
        self.ip_address = self._get_ip_address()
    
    def _setup_metrics(self):
        """Setup Prometheus metrics"""
        # System metrics
        self.metrics['system_cpu_usage'] = Gauge(
            'system_cpu_usage',
            'System CPU usage percentage',
            ['cpu']
        )
        
        self.metrics['system_memory_usage'] = Gauge(
            'system_memory_usage',
            'System memory usage in bytes',
            ['type']
        )
        
        self.metrics['system_disk_usage'] = Gauge(
            'system_disk_usage',
            'System disk usage in bytes',
            ['device', 'mountpoint']
        )
        
        self.metrics['system_network_io'] = Counter(
            'system_network_io_bytes',
            'System network IO in bytes',
            ['interface', 'direction']
        )
        
        # Application metrics
        self.metrics['http_requests_total'] = Counter(
            'http_requests_total',
            'Total HTTP requests',
            ['method', 'endpoint', 'status']
        )
        
        self.metrics['http_request_duration_seconds'] = Histogram(
            'http_request_duration_seconds',
            'HTTP request duration in seconds',
            ['method', 'endpoint']
        )
        
        self.metrics['active_users'] = Gauge(
            'active_users',
            'Number of active users'
        )
        
        self.metrics['db_connections'] = Gauge(
            'db_connections',
            'Number of database connections',
            ['database']
        )
        
        self.metrics['cache_hits'] = Counter(
            'cache_hits_total',
            'Total cache hits',
            ['cache']
        )
        
        self.metrics['cache_misses'] = Counter(
            'cache_misses_total',
            'Total cache misses',
            ['cache']
        )
        
        # Business metrics
        self.metrics['business_transactions'] = Counter(
            'business_transactions_total',
            'Total business transactions',
            ['type', 'status']
        )
        
        self.metrics['business_errors'] = Counter(
            'business_errors_total',
            'Total business errors',
            ['type', 'code']
        )
    
    def _setup_exporters(self):
        """Setup metric exporters"""
        if self.config.get('prometheus', {}).get('enabled', False):
            self.exporters.append(PrometheusExporter(
                self.config['prometheus']
            ))
        
        if self.config.get('statsd', {}).get('enabled', False):
            self.exporters.append(StatsdExporter(
                self.config['statsd']
            ))
        
        if self.config.get('elasticsearch', {}).get('enabled', False):
            self.exporters.append(ElasticsearchExporter(
                self.config['elasticsearch']
            ))
    
    async def collect_metrics(self):
        """Collect all metrics"""
        try:
            # Collect system metrics
            await self._collect_system_metrics()
            
            # Collect application metrics
            await self._collect_application_metrics()
            
            # Collect business metrics
            await self._collect_business_metrics()
            
            # Store metrics
            await self._store_metrics()
            
            # Export metrics
            await self._export_metrics()
        
        except Exception as e:
            self.logger.error(f"Failed to collect metrics: {e}")
    
    async def _collect_system_metrics(self):
        """Collect system metrics"""
        # CPU metrics
        cpu_percent = psutil.cpu_percent(interval=1, percpu=True)
        for i, percent in enumerate(cpu_percent):
            self.metrics['system_cpu_usage'].labels(cpu=f"cpu{i}").set(percent)
        
        # Memory metrics
        memory = psutil.virtual_memory()
        self.metrics['system_memory_usage'].labels(type='total').set(memory.total)
        self.metrics['system_memory_usage'].labels(type='used').set(memory.used)
        self.metrics['system_memory_usage'].labels(type='free').set(memory.free)
        
        # Disk metrics
        for partition in psutil.disk_partitions():
            usage = psutil.disk_usage(partition.mountpoint)
            self.metrics['system_disk_usage'].labels(
                device=partition.device,
                mountpoint=partition.mountpoint
            ).set(usage.used)
        
        # Network metrics
        net_io = psutil.net_io_counters(pernic=True)
        for interface, counters in net_io.items():
            self.metrics['system_network_io'].labels(
                interface=interface,
                direction='sent'
            ).inc(counters.bytes_sent)
            self.metrics['system_network_io'].labels(
                interface=interface,
                direction='received'
            ).inc(counters.bytes_recv)
    
    async def _collect_application_metrics(self):
        """Collect application metrics"""
        # Database metrics
        pool = aiomysql.Pool  # Your database pool
        self.metrics['db_connections'].labels(
            database='mysql'
        ).set(pool.size())
        
        # Cache metrics
        redis = aioredis.Redis  # Your Redis client
        info = await redis.info()
        self.metrics['cache_hits'].labels(
            cache='redis'
        ).inc(int(info['keyspace_hits']))
        self.metrics['cache_misses'].labels(
            cache='redis'
        ).inc(int(info['keyspace_misses']))
    
    async def _collect_business_metrics(self):
        """Collect business metrics"""
        # Example business metrics
        pass
    
    async def _store_metrics(self):
        """Store collected metrics"""
        metrics = []
        
        for metric in self.metrics.values():
            if isinstance(metric, (Counter, Gauge)):
                metrics.append(Metric(
                    name=metric._name,
                    type=MetricType.COUNTER if isinstance(metric, Counter) \
                        else MetricType.GAUGE,
                    level=MetricLevel.SYSTEM,
                    value=metric._value.get(),
                    labels=metric._labelnames,
                    timestamp=datetime.utcnow()
                ))
        
        await self.storage.store_metrics(metrics)
    
    async def _export_metrics(self):
        """Export metrics to configured exporters"""
        for exporter in self.exporters:
            try:
                await exporter.export(self.metrics)
            except Exception as e:
                self.logger.error(f"Failed to export metrics: {e}")
    
    def _get_ip_address(self) -> str:
        """Get primary IP address"""
        interfaces = netifaces.interfaces()
        
        for interface in interfaces:
            addresses = netifaces.ifaddresses(interface)
            if netifaces.AF_INET in addresses:
                for addr in addresses[netifaces.AF_INET]:
                    ip = addr['addr']
                    if not ip.startswith('127.'):
                        return ip
        
        return '127.0.0.1'

class MetricsStorage:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config['type']
        self._setup_storage()
    
    def _setup_storage(self):
        """Setup storage backend"""
        if self.type == 'elasticsearch':
            self.es = elasticsearch.AsyncElasticsearch(
                self.config['elasticsearch_url']
            )
    
    async def store_metrics(self, metrics: List[Metric]):
        """Store metrics"""
        if self.type == 'elasticsearch':
            await self._store_elasticsearch(metrics)
    
    async def _store_elasticsearch(self, metrics: List[Metric]):
        """Store metrics in Elasticsearch"""
        bulk_data = []
        
        for metric in metrics:
            bulk_data.extend([
                {
                    "index": {
                        "_index": f"metrics-{metric.timestamp:%Y.%m.%d}"
                    }
                },
                {
                    "name": metric.name,
                    "type": metric.type.value,
                    "level": metric.level.value,
                    "value": metric.value,
                    "labels": metric.labels,
                    "timestamp": metric.timestamp.isoformat()
                }
            ])
        
        if bulk_data:
            await self.es.bulk(body=bulk_data)

class PrometheusExporter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.port = config.get('port', 9090)
        
        # Start Prometheus HTTP server
        prom.start_http_server(self.port)
    
    async def export(self, metrics: Dict[str, Any]):
        """Export metrics to Prometheus"""
        # Metrics are automatically exposed via HTTP server
        pass

class StatsdExporter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = statsd.StatsClient(
            host=config.get('host', 'localhost'),
            port=config.get('port', 8125)
        )
    
    async def export(self, metrics: Dict[str, Any]):
        """Export metrics to StatsD"""
        for name, metric in metrics.items():
            if isinstance(metric, Counter):
                self.client.incr(name, int(metric._value.get()))
            elif isinstance(metric, Gauge):
                self.client.gauge(name, float(metric._value.get()))

class ElasticsearchExporter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.es = elasticsearch.AsyncElasticsearch(
            config['url']
        )
    
    async def export(self, metrics: Dict[str, Any]):
        """Export metrics to Elasticsearch"""
        bulk_data = []
        timestamp = datetime.utcnow()
        
        for name, metric in metrics.items():
            bulk_data.extend([
                {
                    "index": {
                        "_index": f"metrics-{timestamp:%Y.%m.%d}"
                    }
                },
                {
                    "name": name,
                    "value": float(metric._value.get()),
                    "type": metric.__class__.__name__.lower(),
                    "timestamp": timestamp.isoformat()
                }
            ])
        
        if bulk_data:
            await self.es.bulk(body=bulk_data)
