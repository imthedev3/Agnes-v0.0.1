import logging
import json
from typing import Dict, Any, Optional
from datetime import datetime
import asyncio
from elasticsearch import AsyncElasticsearch
from opencensus.ext.prometheus import prometheus_metrics
from prometheus_client import Counter, Histogram, Gauge

class MetricsCollector:
    def __init__(self):
        # Request metrics
        self.request_counter = Counter(
            'agnes_requests_total',
            'Total requests processed',
            ['endpoint', 'status']
        )
        
        self.request_latency = Histogram(
            'agnes_request_latency_seconds',
            'Request latency in seconds',
            ['endpoint']
        )
        
        # System metrics
        self.memory_usage = Gauge(
            'agnes_memory_usage_bytes',
            'Memory usage in bytes'
        )
        
        self.cpu_usage = Gauge(
            'agnes_cpu_usage_percent',
            'CPU usage percentage'
        )
        
        # Model metrics
        self.model_inference_time = Histogram(
            'agnes_model_inference_seconds',
            'Model inference time in seconds',
            ['model_type']
        )
        
        self.model_error_counter = Counter(
            'agnes_model_errors_total',
            'Total model errors',
            ['model_type', 'error_type']
        )

class ElasticSearchHandler:
    def __init__(self, config: Dict[str, Any]):
        self.es = AsyncElasticsearch([config['elasticsearch_url']])
        self.index_prefix = config.get('index_prefix', 'agnes-logs-')
    
    async def store_log(self, log_data: Dict[str, Any]):
        """Store log entry in Elasticsearch"""
        index_name = f"{self.index_prefix}{datetime.now():%Y.%m.%d}"
        await self.es.index(
            index=index_name,
            document=log_data
        )
    
    async def close(self):
        """Close Elasticsearch connection"""
        await self.es.close()

class StructuredLogger:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.metrics = MetricsCollector()
        self.es_handler = ElasticSearchHandler(config)
        
        # Setup logging
        logging.basicConfig(
            level=config.get('log_level', logging.INFO),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('agnes')
    
    async def log_event(self,
                       event_type: str,
                       data: Dict[str, Any],
                       context: Optional[Dict[str, Any]] = None):
        """Log structured event"""
        timestamp = datetime.utcnow().isoformat()
        
        log_entry = {
            'timestamp': timestamp,
            'event_type': event_type,
            'data': data,
            'context': context or {}
        }
        
        # Log to file
        self.logger.info(json.dumps(log_entry))
        
        # Store in Elasticsearch
        await self.es_handler.store_log(log_entry)
        
        # Update metrics
        self._update_metrics(event_type, data)
    
    def _update_metrics(self, event_type: str, data: Dict[str, Any]):
        """Update Prometheus metrics"""
        if event_type == 'request':
            self.metrics.request_counter.labels(
                endpoint=data.get('endpoint', 'unknown'),
                status=data.get('status', 'unknown')
            ).inc()
            
            if 'latency' in data:
                self.metrics.request_latency.labels(
                    endpoint=data.get('endpoint', 'unknown')
                ).observe(data['latency'])
        
        elif event_type == 'model_inference':
            self.metrics.model_inference_time.labels(
                model_type=data.get('model_type', 'unknown')
            ).observe(data.get('inference_time', 0))

class PerformanceMonitor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.metrics = MetricsCollector()
        self._monitoring = False
    
    async def start_monitoring(self):
        """Start performance monitoring"""
        self._monitoring = True
        asyncio.create_task(self._monitor_loop())
    
    async def stop_monitoring(self):
        """Stop performance monitoring"""
        self._monitoring = False
    
    async def _monitor_loop(self):
        """Continuous monitoring loop"""
        while self._monitoring:
            # Update system metrics
            self._update_system_metrics()
            
            # Wait for next monitoring interval
            await asyncio.sleep(self.config.get('monitoring_interval', 60))
    
    def _update_system_metrics(self):
        """Update system metrics"""
        import psutil
        
        # Update memory usage
        memory = psutil.virtual_memory()
        self.metrics.memory_usage.set(memory.used)
        
        # Update CPU usage
        self.metrics.cpu_usage.set(psutil.cpu_percent())
