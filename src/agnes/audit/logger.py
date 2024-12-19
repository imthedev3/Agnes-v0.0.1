from typing import Dict, Any, List, Optional, Union
import asyncio
import json
from datetime import datetime
import logging
import uuid
from dataclasses import dataclass
from enum import Enum
from elasticsearch import AsyncElasticsearch
import aiomysql
import aioredis
import aiokafka
import socket
import hashlib
import jwt

class AuditAction(Enum):
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    LOGIN = "login"
    LOGOUT = "logout"
    EXECUTE = "execute"
    DEPLOY = "deploy"
    CONFIG = "config"
    ADMIN = "admin"

class AuditLevel(Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class AuditEvent:
    id: str
    timestamp: datetime
    action: AuditAction
    level: AuditLevel
    user_id: str
    resource_type: str
    resource_id: str
    details: Dict[str, Any]
    metadata: Dict[str, Any]
    status: str
    ip_address: str
    user_agent: str
    session_id: Optional[str] = None
    error: Optional[str] = None

class AuditLogger:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.hostname = socket.gethostname()
        self.storage = AuditStorage(config['storage'])
        self.cache = AuditCache(config['cache'])
        self.logger = logging.getLogger(__name__)
    
    async def log(self,
                 action: AuditAction,
                 user_id: str,
                 resource_type: str,
                 resource_id: str,
                 details: Dict[str, Any],
                 level: AuditLevel = AuditLevel.INFO,
                 metadata: Dict[str, Any] = None,
                 status: str = "success",
                 ip_address: str = None,
                 user_agent: str = None,
                 session_id: str = None,
                 error: str = None):
        """Log audit event"""
        event = AuditEvent(
            id=str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            action=action,
            level=level,
            user_id=user_id,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details,
            metadata=metadata or {},
            status=status,
            ip_address=ip_address or "",
            user_agent=user_agent or "",
            session_id=session_id,
            error=error
        )
        
        # Add system metadata
        event.metadata.update({
            'hostname': self.hostname,
            'environment': self.config.get('environment', 'production')
        })
        
        # Store event
        try:
            await self.storage.store(event)
            
            # Cache for real-time alerts
            if level in [AuditLevel.WARNING, AuditLevel.ERROR, AuditLevel.CRITICAL]:
                await self.cache.store_alert(event)
            
            # Send to Kafka if enabled
            if self.config.get('kafka', {}).get('enabled', False):
                await self._send_to_kafka(event)
        
        except Exception as e:
            self.logger.error(f"Failed to store audit event: {e}")
            raise
    
    async def _send_to_kafka(self, event: AuditEvent):
        """Send event to Kafka"""
        producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=self.config['kafka']['brokers']
        )
        await producer.start()
        
        try:
            key = f"{event.resource_type}:{event.resource_id}".encode()
            value = json.dumps({
                'id': event.id,
                'timestamp': event.timestamp.isoformat(),
                'action': event.action.value,
                'level': event.level.value,
                'user_id': event.user_id,
                'resource_type': event.resource_type,
                'resource_id': event.resource_id,
                'details': event.details,
                'metadata': event.metadata,
                'status': event.status,
                'ip_address': event.ip_address,
                'user_agent': event.user_agent,
                'session_id': event.session_id,
                'error': event.error
            }).encode()
            
            await producer.send_and_wait(
                self.config['kafka']['topic'],
                value=value,
                key=key
            )
        
        finally:
            await producer.stop()
    
    async def query(self,
                   start_time: datetime,
                   end_time: datetime,
                   filters: Dict[str, Any] = None,
                   page: int = 1,
                   size: int = 100) -> List[AuditEvent]:
        """Query audit events"""
        return await self.storage.query(
            start_time,
            end_time,
            filters,
            page,
            size
        )
    
    async def get_user_activity(self,
                              user_id: str,
                              start_time: datetime,
                              end_time: datetime) -> List[AuditEvent]:
        """Get user activity"""
        return await self.storage.query(
            start_time,
            end_time,
            {'user_id': user_id}
        )
    
    async def get_resource_history(self,
                                 resource_type: str,
                                 resource_id: str) -> List[AuditEvent]:
        """Get resource history"""
        return await self.storage.query(
            None,
            None,
            {
                'resource_type': resource_type,
                'resource_id': resource_id
            }
        )

class AuditStorage:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config['type']
        self._setup_storage()
    
    def _setup_storage(self):
        """Setup storage backend"""
        if self.type == 'elasticsearch':
            self.client = AsyncElasticsearch([
                self.config['elasticsearch_url']
            ])
        elif self.type == 'mysql':
            self.pool = aiomysql.create_pool(**self.config['mysql'])
    
    async def store(self, event: AuditEvent):
        """Store audit event"""
        if self.type == 'elasticsearch':
            await self._store_elasticsearch(event)
        elif self.type == 'mysql':
            await self._store_mysql(event)
    
    async def _store_elasticsearch(self, event: AuditEvent):
        """Store event in Elasticsearch"""
        await self.client.index(
            index=f"audit-{event.timestamp.strftime('%Y.%m')}",
            body={
                'timestamp': event.timestamp.isoformat(),
                'action': event.action.value,
                'level': event.level.value,
                'user_id': event.user_id,
                'resource_type': event.resource_type,
                'resource_id': event.resource_id,
                'details': event.details,
                'metadata': event.metadata,
                'status': event.status,
                'ip_address': event.ip_address,
                'user_agent': event.user_agent,
                'session_id': event.session_id,
                'error': event.error
            }
        )
    
    async def _store_mysql(self, event: AuditEvent):
        """Store event in MySQL"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO audit_events (
                        id, timestamp, action, level, user_id,
                        resource_type, resource_id, details, metadata,
                        status, ip_address, user_agent, session_id, error
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    event.id,
                    event.timestamp,
                    event.action.value,
                    event.level.value,
                    event.user_id,
                    event.resource_type,
                    event.resource_id,
                    json.dumps(event.details),
                    json.dumps(event.metadata),
                    event.status,
                    event.ip_address,
                    event.user_agent,
                    event.session_id,
                    event.error
                ))
                await conn.commit()
    
    async def query(self,
                   start_time: Optional[datetime],
                   end_time: Optional[datetime],
                   filters: Dict[str, Any] = None,
                   page: int = 1,
                   size: int = 100) -> List[AuditEvent]:
        """Query audit events"""
        if self.type == 'elasticsearch':
            return await self._query_elasticsearch(
                start_time, end_time, filters, page, size
            )
        elif self.type == 'mysql':
            return await self._query_mysql(
                start_time, end_time, filters, page, size
            )

class AuditCache:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.redis = aioredis.Redis.from_url(
            config['redis_url']
        )
    
    async def store_alert(self,
                         event: AuditEvent,
                         ttl: int = 3600):
        """Store event alert in cache"""
        key = f"audit:alert:{event.id}"
        value = json.dumps({
            'id': event.id,
            'timestamp': event.timestamp.isoformat(),
            'action': event.action.value,
            'level': event.level.value,
            'user_id': event.user_id,
            'resource_type': event.resource_type,
            'resource_id': event.resource_id,
            'details': event.details,
            'status': event.status,
            'error': event.error
        })
        
        await self.redis.set(key, value, ex=ttl)
        
        # Add to sorted set for time-based queries
        score = int(event.timestamp.timestamp())
        await self.redis.zadd(
            'audit:alerts',
            {event.id: score}
        )
    
    async def get_recent_alerts(self,
                              count: int = 100) -> List[Dict[str, Any]]:
        """Get recent alerts"""
        # Get recent alert IDs
        alert_ids = await self.redis.zrevrange(
            'audit:alerts',
            0,
            count - 1
        )
        
        alerts = []
        for alert_id in alert_ids:
            alert_data = await self.redis.get(
                f"audit:alert:{alert_id.decode()}"
            )
            if alert_data:
                alerts.append(json.loads(alert_data))
        
        return alerts
