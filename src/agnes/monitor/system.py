from typing import Dict, Any, List, Optional, Union, Callable
import asyncio
import psutil
import platform
import json
from datetime import datetime
import aiohttp
import logging
from dataclasses import dataclass
from enum import Enum
import socket
import aiodocker
import uuid
from collections import defaultdict

class MetricType(Enum):
    CPU = "cpu"
    MEMORY = "memory"
    DISK = "disk"
    NETWORK = "network"
    PROCESS = "process"
    CUSTOM = "custom"

class AlertLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class MetricThreshold:
    warning: float
    error: float
    critical: float
    duration: int = 60  # seconds
    frequency: int = 3  # minimum occurrences

@dataclass
class Metric:
    name: str
    type: MetricType
    value: float
    timestamp: datetime
    labels: Optional[Dict[str, str]] = None

@dataclass
class Alert:
    id: str
    metric: str
    level: AlertLevel
    message: str
    value: float
    threshold: float
    timestamp: datetime
    labels: Optional[Dict[str, str]] = None

class SystemMonitor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.hostname = socket.gethostname()
        self.docker = None
        self.thresholds: Dict[str, MetricThreshold] = {}
        self.history: Dict[str, List[Metric]] = defaultdict(list)
        self.alert_handlers: List[Callable[[Alert], Any]] = []
        self.logger = logging.getLogger(__name__)
    
    async def start(self):
        """Start monitoring"""
        self.docker = aiodocker.Docker()
        
        while True:
            try:
                await self._collect_metrics()
                await asyncio.sleep(
                    self.config.get('collection_interval', 10)
                )
            except Exception as e:
                self.logger.error(f"Error collecting metrics: {e}")
                await asyncio.sleep(5)
    
    async def stop(self):
        """Stop monitoring"""
        if self.docker:
            await self.docker.close()
    
    async def _collect_metrics(self):
        """Collect system metrics"""
        timestamp = datetime.utcnow()
        
        # Collect CPU metrics
        cpu_metrics = await self._collect_cpu_metrics(timestamp)
        for metric in cpu_metrics:
            await self._process_metric(metric)
        
        # Collect memory metrics
        memory_metrics = await self._collect_memory_metrics(timestamp)
        for metric in memory_metrics:
            await self._process_metric(metric)
        
        # Collect disk metrics
        disk_metrics = await self._collect_disk_metrics(timestamp)
        for metric in disk_metrics:
            await self._process_metric(metric)
        
        # Collect network metrics
        network_metrics = await self._collect_network_metrics(timestamp)
        for metric in network_metrics:
            await self._process_metric(metric)
        
        # Collect process metrics
        process_metrics = await self._collect_process_metrics(timestamp)
        for metric in process_metrics:
            await self._process_metric(metric)
        
        # Collect Docker metrics
        if self.docker:
            docker_metrics = await self._collect_docker_metrics(timestamp)
            for metric in docker_metrics:
                await self._process_metric(metric)
    
    async def _collect_cpu_metrics(self, timestamp: datetime) -> List[Metric]:
        """Collect CPU metrics"""
        metrics = []
        
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        metrics.append(Metric(
            name="system_cpu_usage",
            type=MetricType.CPU,
            value=cpu_percent,
            timestamp=timestamp,
            labels={'host': self.hostname}
        ))
        
        # CPU load
        load1, load5, load15 = psutil.getloadavg()
        metrics.extend([
            Metric(
                name="system_load_1",
                type=MetricType.CPU,
                value=load1,
                timestamp=timestamp,
                labels={'host': self.hostname}
            ),
            Metric(
                name="system_load_5",
                type=MetricType.CPU,
                value=load5,
                timestamp=timestamp,
                labels={'host': self.hostname}
            ),
            Metric(
                name="system_load_15",
                type=MetricType.CPU,
                value=load15,
                timestamp=timestamp,
                labels={'host': self.hostname}
            )
        ])
        
        return metrics
    
    async def _collect_memory_metrics(self, timestamp: datetime) -> List[Metric]:
        """Collect memory metrics"""
        metrics = []
        
        memory = psutil.virtual_memory()
        metrics.extend([
            Metric(
                name="system_memory_total",
                type=MetricType.MEMORY,
                value=memory.total,
                timestamp=timestamp,
                labels={'host': self.hostname}
            ),
            Metric(
                name="system_memory_used",
                type=MetricType.MEMORY,
                value=memory.used,
                timestamp=timestamp,
                labels={'host': self.hostname}
            ),
            Metric(
                name="system_memory_percent",
                type=MetricType.MEMORY,
                value=memory.percent,
                timestamp=timestamp,
                labels={'host': self.hostname}
            )
        ])
        
        swap = psutil.swap_memory()
        metrics.extend([
            Metric(
                name="system_swap_total",
                type=MetricType.MEMORY,
                value=swap.total,
                timestamp=timestamp,
                labels={'host': self.hostname}
            ),
            Metric(
                name="system_swap_used",
                type=MetricType.MEMORY,
                value=swap.used,
                timestamp=timestamp,
                labels={'host': self.hostname}
            ),
            Metric(
                name="system_swap_percent",
                type=MetricType.MEMORY,
                value=swap.percent,
                timestamp=timestamp,
                labels={'host': self.hostname}
            )
        ])
        
        return metrics
    
    async def _collect_disk_metrics(self, timestamp: datetime) -> List[Metric]:
        """Collect disk metrics"""
        metrics = []
        
        for partition in psutil.disk_partitions():
            try:
                usage = psutil.disk_usage(partition.mountpoint)
                metrics.extend([
                    Metric(
                        name="system_disk_total",
                        type=MetricType.DISK,
                        value=usage.total,
                        timestamp=timestamp,
                        labels={
                            'host': self.hostname,
                            'device': partition.device,
                            'mountpoint': partition.mountpoint
                        }
                    ),
                    Metric(
                        name="system_disk_used",
                        type=MetricType.DISK,
                        value=usage.used,
                        timestamp=timestamp,
                        labels={
                            'host': self.hostname,
                            'device': partition.device,
                            'mountpoint': partition.mountpoint
                        }
                    ),
                    Metric(
                        name="system_disk_percent",
                        type=MetricType.DISK,
                        value=usage.percent,
                        timestamp=timestamp,
                        labels={
                            'host': self.hostname,
                            'device': partition.device,
                            'mountpoint': partition.mountpoint
                        }
                    )
                ])
            except Exception as e:
                self.logger.error(
                    f"Error collecting disk metrics for {partition.mountpoint}: {e}"
                )
        
        return metrics
    
    async def _collect_network_metrics(self, timestamp: datetime) -> List[Metric]:
        """Collect network metrics"""
        metrics = []
        
        net_io = psutil.net_io_counters()
        metrics.extend([
            Metric(
                name="system_network_bytes_sent",
                type=MetricType.NETWORK,
                value=net_io.bytes_sent,
                timestamp=timestamp,
                labels={'host': self.hostname}
            ),
            Metric(
                name="system_network_bytes_recv",
                type=MetricType.NETWORK,
                value=net_io.bytes_recv,
                timestamp=timestamp,
                labels={'host': self.hostname}
            ),
            Metric(
                name="system_network_packets_sent",
                type=MetricType.NETWORK,
                value=net_io.packets_sent,
                timestamp=timestamp,
                labels={'host': self.hostname}
            ),
            Metric(
                name="system_network_packets_recv",
                type=MetricType.NETWORK,
                value=net_io.packets_recv,
                timestamp=timestamp,
                labels={'host': self.hostname}
            )
        ])
        
        return metrics
    
    async def _collect_process_metrics(self, timestamp: datetime) -> List[Metric]:
        """Collect process metrics"""
        metrics = []
        
        for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
            try:
                metrics.extend([
                    Metric(
                        name="system_process_cpu",
                        type=MetricType.PROCESS,
                        value=proc.info['cpu_percent'],
                        timestamp=timestamp,
                        labels={
                            'host': self.hostname,
                            'pid': str(proc.info['pid']),
                            'name': proc.info['name']
                        }
                    ),
                    Metric(
                        name="system_process_memory",
                        type=MetricType.PROCESS,
                        value=proc.info['memory_percent'],
                        timestamp=timestamp,
                        labels={
                            'host': self.hostname,
                            'pid': str(proc.info['pid']),
                            'name': proc.info['name']
                        }
                    )
                ])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        return metrics
    
    async def _collect_docker_metrics(self, timestamp: datetime) -> List[Metric]:
        """Collect Docker metrics"""
        metrics = []
        
        try:
            containers = await self.docker.containers.list()
            for container in containers:
                stats = await container.stats(stream=False)
                if stats:
                    stats = stats[0]  # Get first stats entry
                    
                    # CPU stats
                    cpu_delta = stats['cpu_stats']['cpu_usage']['total_usage'] - \
                               stats['precpu_stats']['cpu_usage']['total_usage']
                    system_delta = stats['cpu_stats']['system_cpu_usage'] - \
                                 stats['precpu_stats']['system_cpu_usage']
                    cpu_percent = 0.0
                    if system_delta > 0:
                        cpu_percent = (cpu_delta / system_delta) * 100.0
                    
                    metrics.append(Metric(
                        name="docker_cpu_usage",
                        type=MetricType.CPU,
                        value=cpu_percent,
                        timestamp=timestamp,
                        labels={
                            'host': self.hostname,
                            'container': container.id[:12],
                            'name': container.name
                        }
                    ))
                    
                    # Memory stats
                    mem_usage = stats['memory_stats']['usage']
                    mem_limit = stats['memory_stats']['limit']
                    mem_percent = (mem_usage / mem_limit) * 100.0
                    
                    metrics.append(Metric(
                        name="docker_memory_usage",
                        type=MetricType.MEMORY,
                        value=mem_percent,
                        timestamp=timestamp,
                        labels={
                            'host': self.hostname,
                            'container': container.id[:12],
                            'name': container.name
                        }
                    ))
        
        except Exception as e:
            self.logger.error(f"Error collecting Docker metrics: {e}")
        
        return metrics
    
    async def _process_metric(self, metric: Metric):
        """Process collected metric"""
        # Store metric in history
        self.history[metric.name].append(metric)
        
        # Cleanup old metrics
        cutoff = datetime.utcnow() - timedelta(
            hours=self.config.get('history_retention_hours', 24)
        )
        self.history[metric.name] = [
            m for m in self.history[metric.name]
            if m.timestamp > cutoff
        ]
        
        # Check thresholds
        if metric.name in self.thresholds:
            await self._check_thresholds(metric)
    
    async def _check_thresholds(self, metric: Metric):
        """Check metric thresholds and generate alerts"""
        threshold = self.thresholds[metric.name]
        recent_metrics = [
            m for m in self.history[metric.name]
            if (datetime.utcnow() - m.timestamp).total_seconds() <= threshold.duration
        ]
        
        if len(recent_metrics) < threshold.frequency:
            return
        
        violations = [
            m for m in recent_metrics
            if m.value >= threshold.critical
        ]
        if len(violations) >= threshold.frequency:
            await self._generate_alert(
                metric,
                AlertLevel.CRITICAL,
                threshold.critical
            )
            return
        
        violations = [
            m for m in recent_metrics
            if m.value >= threshold.error
        ]
        if len(violations) >= threshold.frequency:
            await self._generate_alert(
                metric,
                AlertLevel.ERROR,
                threshold.error
            )
            return
        
        violations = [
            m for m in recent_metrics
            if m.value >= threshold.warning
        ]
        if len(violations) >= threshold.frequency:
            await self._generate_alert(
                metric,
                AlertLevel.WARNING,
                threshold.warning
            )
    
    async def _generate_alert(self,
                            metric: Metric,
                            level: AlertLevel,
                            threshold: float):
        """Generate and send alert"""
        alert = Alert(
            id=str(uuid.uuid4()),
            metric=metric.name,
            level=level,
            message=f"{metric.name} exceeds {level.value} threshold",
            value=metric.value,
            threshold=threshold,
            timestamp=datetime.utcnow(),
            labels=metric.labels
        )
        
        for handler in self.alert_handlers:
            try:
                await handler(alert)
            except Exception as e:
                self.logger.error(f"Error in alert handler: {e}")
    
    def add_threshold(self,
                     metric: str,
                     warning: float,
                     error: float,
                     critical: float,
                     duration: int = 60,
                     frequency: int = 3):
        """Add metric threshold"""
        self.thresholds[metric] = MetricThreshold(
            warning=warning,
            error=error,
            critical=critical,
            duration=duration,
            frequency=frequency
        )
    
    def add_alert_handler(self, handler: Callable[[Alert], Any]):
        """Add alert handler"""
        self.alert_handlers.append(handler)

class MetricStore:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._setup_storage()
    
    def _setup_storage(self):
        """Setup metric storage"""
        storage_type = self.config.get('storage', {}).get('type', 'memory')
        if storage_type == 'elasticsearch':
            self.storage = ElasticsearchMetricStorage(
                self.config['storage']
            )
        else:
            self.storage = MemoryMetricStorage()
    
    async def store_metric(self, metric: Metric):
        """Store metric"""
        await self.storage.store(metric)
    
    async def query_metrics(self,
                          name: str,
                          start_time: datetime,
                          end_time: datetime,
                          labels: Optional[Dict[str, str]] = None) -> List[Metric]:
        """Query metrics"""
        return await self.storage.query(name, start_time, end_time, labels)

class MemoryMetricStorage:
    def __init__(self):
        self.metrics: Dict[str, List[Metric]] = defaultdict(list)
    
    async def store(self, metric: Metric):
        """Store metric in memory"""
        self.metrics[metric.name].append(metric)
    
    async def query(self,
                   name: str,
                   start_time: datetime,
                   end_time: datetime,
                   labels: Optional[Dict[str, str]] = None) -> List[Metric]:
        """Query metrics from memory"""
        metrics = self.metrics.get(name, [])
        return [
            m for m in metrics
            if start_time <= m.timestamp <= end_time and
            (not labels or all(
                m.labels.get(k) == v
                for k, v in labels.items()
            ))
        ]

class ElasticsearchMetricStorage:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = AsyncElasticsearch([config['url']])
    
    async def store(self, metric: Metric):
        """Store metric in Elasticsearch"""
        await self.client.index(
            index=f"metrics-{metric.name}",
            body={
                'timestamp': metric.timestamp.isoformat(),
                'value': metric.value,
                'type': metric.type.value,
                'labels': metric.labels or {}
            }
        )
    
    async def query(self,
                   name: str,
                   start_time: datetime,
                   end_time: datetime,
                   labels: Optional[Dict[str, str]] = None) -> List[Metric]:
        """Query metrics from Elasticsearch"""
        query = {
            'bool': {
                'must': [
                    {
                        'range': {
                            'timestamp': {
                                'gte': start_time.isoformat(),
                                'lte': end_time.isoformat()
                            }
                        }
                    }
                ]
            }
        }
        
        if labels:
            for key, value in labels.items():
                query['bool']['must'].append({
                    'term': {
                        f'labels.{key}': value
                    }
                })
        
        result = await self.client.search(
            index=f"metrics-{name}",
            body={'query': query}
        )
        
        return [
            Metric(
                name=name,
                type=MetricType(hit['_source']['type']),
                value=hit['_source']['value'],
                timestamp=datetime.fromisoformat(
                    hit['_source']['timestamp']
                ),
                labels=hit['_source']['labels']
            )
            for hit in result['hits']['hits']
        ]
