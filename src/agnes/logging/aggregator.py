from typing import Dict, Any, Optional, List, Union
import asyncio
import logging
import json
from datetime import datetime
import aiohttp
import elasticsearch
from elasticsearch import AsyncElasticsearch
import structlog
from dataclasses import dataclass
import socket
import sys
import traceback
import gzip
import io
import uuid

@dataclass
class LogConfig:
    level: str
    format: str
    elastic_hosts: List[str]
    index_pattern: str
    batch_size: int = 100
    flush_interval: int = 5
    retention_days: int = 30

class LogFormatter:
    def __init__(self):
        self.hostname = socket.gethostname()
    
    def format(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Format log record"""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "hostname": self.hostname,
            "level": record.get("level", "INFO"),
            "logger": record.get("logger_name", "root"),
            "message": record.get("event"),
            "context": record.get("context", {}),
            "exception": record.get("exception"),
            "trace_id": record.get("trace_id"),
            "span_id": record.get("span_id"),
            "service": record.get("service", "agnes"),
            "environment": record.get("environment", "production")
        }

class ElasticsearchHandler:
    def __init__(self, config: LogConfig):
        self.config = config
        self.client = AsyncElasticsearch(config.elastic_hosts)
        self.buffer: List[Dict[str, Any]] = []
        self.lock = asyncio.Lock()
        self.formatter = LogFormatter()
    
    async def initialize(self):
        """Initialize Elasticsearch handler"""
        # Create index template
        template = {
            "index_patterns": [self.config.index_pattern + "*"],
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "index.lifecycle.name": "logs_policy",
                "index.lifecycle.rollover_alias": self.config.index_pattern
            },
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "level": {"type": "keyword"},
                    "logger": {"type": "keyword"},
                    "message": {"type": "text"},
                    "context": {"type": "object"},
                    "exception": {"type": "text"},
                    "trace_id": {"type": "keyword"},
                    "span_id": {"type": "keyword"},
                    "service": {"type": "keyword"},
                    "environment": {"type": "keyword"},
                    "hostname": {"type": "keyword"}
                }
            }
        }
        
        await self.client.indices.put_template(
            name="logs_template",
            body=template
        )
        
        # Start background tasks
        asyncio.create_task(self._periodic_flush())
    
    async def handle(self, record: Dict[str, Any]):
        """Handle log record"""
        formatted_record = self.formatter.format(record)
        
        async with self.lock:
            self.buffer.append(formatted_record)
            
            if len(self.buffer) >= self.config.batch_size:
                await self._flush()
    
    async def _flush(self):
        """Flush buffered records to Elasticsearch"""
        if not self.buffer:
            return
        
        async with self.lock:
            records = self.buffer
            self.buffer = []
        
        try:
            actions = [
                {
                    "_index": f"{self.config.index_pattern}-"
                             f"{datetime.utcnow():%Y.%m.%d}",
                    "_source": record
                }
                for record in records
            ]
            
            await self.client.bulk(body=actions)
        
        except Exception as e:
            print(f"Error flushing logs to Elasticsearch: {e}")
            # Requeue failed records
            async with self.lock:
                self.buffer.extend(records)
    
    async def _periodic_flush(self):
        """Periodically flush buffer"""
        while True:
            await asyncio.sleep(self.config.flush_interval)
            await self._flush()

class LogAggregator:
    def __init__(self, config: LogConfig):
        self.config = config
        self.handlers: List[Any] = []
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging configuration"""
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                self.add_context_processor,
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            wrapper_class=structlog.BoundLogger,
            cache_logger_on_first_use=True,
        )
    
    def add_context_processor(self, 
                            logger: str,
                            method_name: str,
                            event_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Add additional context to log records"""
        event_dict.update({
            "trace_id": event_dict.get("trace_id", str(uuid.uuid4())),
            "service": self.config.get("service_name", "agnes"),
            "environment": self.config.get("environment", "production")
        })
        return event_dict
    
    async def add_handler(self, handler: Any):
        """Add log handler"""
        self.handlers.append(handler)
    
    def get_logger(self, name: str) -> structlog.BoundLogger:
        """Get logger instance"""
        return structlog.get_logger(name)

class LogRotator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.max_size = config.get("max_size", 100 * 1024 * 1024)  # 100MB
        self.backup_count = config.get("backup_count", 5)
    
    async def rotate_logs(self, file_path: str):
        """Rotate log file if needed"""
        try:
            if not await self._should_rotate(file_path):
                return
            
            # Rotate existing backup files
            for i in range(self.backup_count - 1, 0, -1):
                src = f"{file_path}.{i}.gz"
                dst = f"{file_path}.{i + 1}.gz"
                
                try:
                    await asyncio.to_thread(self._rename_file, src, dst)
                except FileNotFoundError:
                    continue
            
            # Compress current log file
            await self._compress_file(file_path, f"{file_path}.1.gz")
            
            # Create new empty log file
            open(file_path, 'w').close()
        
        except Exception as e:
            print(f"Error rotating logs: {e}")
    
    async def _should_rotate(self, file_path: str) -> bool:
        """Check if log file should be rotated"""
        try:
            size = (await asyncio.to_thread(
                lambda: os.path.getsize(file_path)
            ))
            return size >= self.max_size
        except FileNotFoundError:
            return False
    
    def _rename_file(self, src: str, dst: str):
        """Rename file if it exists"""
        try:
            os.rename(src, dst)
        except FileNotFoundError:
            pass
    
    async def _compress_file(self, src_path: str, dst_path: str):
        """Compress log file"""
        try:
            with open(src_path, 'rb') as f_in:
                with gzip.open(dst_path, 'wb') as f_out:
                    await asyncio.to_thread(
                        lambda: f_out.write(f_in.read())
                    )
        except Exception as e:
            print(f"Error compressing log file: {e}")

class LogAnalyzer:
    def __init__(self, elastic_client: AsyncElasticsearch):
        self.client = elastic_client
    
    async def analyze_errors(self, 
                           time_range: str = "1h") -> Dict[str, Any]:
        """Analyze error logs"""
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"level": "ERROR"}},
                        {"range": {
                            "timestamp": {
                                "gte": f"now-{time_range}"
                            }
                        }}
                    ]
                }
            },
            "aggs": {
                "errors_by_service": {
                    "terms": {"field": "service"}
                },
                "errors_by_type": {
                    "terms": {"field": "context.error_type"}
                }
            },
            "size": 0
        }
        
        result = await self.client.search(
            index="logs-*",
            body=query
        )
        
        return {
            "total_errors": result["hits"]["total"]["value"],
            "errors_by_service": {
                bucket["key"]: bucket["doc_count"]
                for bucket in result["aggregations"]
                              ["errors_by_service"]["buckets"]
            },
            "errors_by_type": {
                bucket["key"]: bucket["doc_count"]
                for bucket in result["aggregations"]
                              ["errors_by_type"]["buckets"]
            }
        }
    
    async def get_recent_errors(self, 
                              limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent error logs"""
        query = {
            "query": {
                "match": {"level": "ERROR"}
            },
            "sort": [{"timestamp": "desc"}],
            "size": limit
        }
        
        result = await self.client.search(
            index="logs-*",
            body=query
        )
        
        return [hit["_source"] for hit in result["hits"]["hits"]]
