from typing import Dict, Any, List, Optional, Union, Callable
import asyncio
import hashlib
import json
from datetime import datetime
import aiohttp
import aiomysql
from elasticsearch import AsyncElasticsearch
import redis.asyncio as redis
from dataclasses import dataclass
import logging
from enum import Enum
import time

class SyncStrategy(Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFF = "diff"

@dataclass
class SyncTask:
    source: str
    target: str
    strategy: SyncStrategy
    query: Dict[str, Any]
    mapping: Dict[str, str]
    filters: Optional[List[Dict[str, Any]]] = None
    transform: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    batch_size: int = 1000
    schedule: Optional[str] = None

class DataSource:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config['type']
        self.client = None
    
    async def connect(self):
        """Connect to data source"""
        if self.type == 'mysql':
            self.client = await aiomysql.create_pool(**self.config['connection'])
        elif self.type == 'elasticsearch':
            self.client = AsyncElasticsearch([self.config['connection']])
        elif self.type == 'redis':
            self.client = redis.Redis(**self.config['connection'])
    
    async def disconnect(self):
        """Disconnect from data source"""
        if self.client:
            if self.type == 'mysql':
                self.client.close()
                await self.client.wait_closed()
            elif self.type == 'elasticsearch':
                await self.client.close()
            elif self.type == 'redis':
                await self.client.close()
    
    async def fetch_data(self,
                        query: Dict[str, Any],
                        batch_size: int = 1000) -> AsyncIterator[List[Dict[str, Any]]]:
        """Fetch data from source"""
        if self.type == 'mysql':
            async with self.client.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query['sql'], query.get('params', []))
                    while True:
                        rows = await cursor.fetchmany(batch_size)
                        if not rows:
                            break
                        yield [dict(zip(cursor.column_names, row)) for row in rows]
        
        elif self.type == 'elasticsearch':
            search_after = None
            while True:
                body = {**query, 'size': batch_size}
                if search_after:
                    body['search_after'] = search_after
                
                result = await self.client.search(body=body)
                hits = result['hits']['hits']
                if not hits:
                    break
                
                yield [hit['_source'] for hit in hits]
                search_after = hits[-1]['sort']
        
        elif self.type == 'redis':
            keys = await self.client.keys(query['pattern'])
            for i in range(0, len(keys), batch_size):
                batch_keys = keys[i:i + batch_size]
                pipeline = self.client.pipeline()
                for key in batch_keys:
                    pipeline.get(key)
                values = await pipeline.execute()
                yield [
                    {'key': key.decode(), 'value': json.loads(value)}
                    for key, value in zip(batch_keys, values)
                    if value is not None
                ]

class DataTarget:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config['type']
        self.client = None
    
    async def connect(self):
        """Connect to data target"""
        if self.type == 'mysql':
            self.client = await aiomysql.create_pool(**self.config['connection'])
        elif self.type == 'elasticsearch':
            self.client = AsyncElasticsearch([self.config['connection']])
        elif self.type == 'redis':
            self.client = redis.Redis(**self.config['connection'])
    
    async def disconnect(self):
        """Disconnect from data target"""
        if self.client:
            if self.type == 'mysql':
                self.client.close()
                await self.client.wait_closed()
            elif self.type == 'elasticsearch':
                await self.client.close()
            elif self.type == 'redis':
                await self.client.close()
    
    async def write_data(self,
                        data: List[Dict[str, Any]],
                        mapping: Dict[str, str]):
        """Write data to target"""
        if self.type == 'mysql':
            if not data:
                return
            
            columns = list(mapping.values())
            placeholders = ', '.join(['%s'] * len(columns))
            sql = f"INSERT INTO {self.config['table']} ({', '.join(columns)}) VALUES ({placeholders})"
            
            values = [
                [row[mapping[col]] for col in columns]
                for row in data
            ]
            
            async with self.client.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.executemany(sql, values)
                await conn.commit()
        
        elif self.type == 'elasticsearch':
            if not data:
                return
            
            actions = []
            for row in data:
                doc = {
                    mapping[key]: value
                    for key, value in row.items()
                    if key in mapping
                }
                actions.append({
                    '_index': self.config['index'],
                    '_source': doc
                })
            
            await self.client.bulk(body=actions)
        
        elif self.type == 'redis':
            if not data:
                return
            
            pipeline = self.client.pipeline()
            for row in data:
                key = row[mapping['key']]
                value = {
                    mapping[k]: v
                    for k, v in row.items()
                    if k in mapping and k != 'key'
                }
                pipeline.set(key, json.dumps(value))
            await pipeline.execute()

class SyncEngine:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.sources: Dict[str, DataSource] = {}
        self.targets: Dict[str, DataTarget] = {}
        self.tasks: Dict[str, SyncTask] = {}
        self.logger = logging.getLogger(__name__)
        self._setup_connections()
    
    def _setup_connections(self):
        """Setup data sources and targets"""
        for name, config in self.config['sources'].items():
            self.sources[name] = DataSource(config)
        
        for name, config in self.config['targets'].items():
            self.targets[name] = DataTarget(config)
    
    async def initialize(self):
        """Initialize sync engine"""
        for source in self.sources.values():
            await source.connect()
        
        for target in self.targets.values():
            await target.connect()
    
    async def cleanup(self):
        """Cleanup resources"""
        for source in self.sources.values():
            await source.disconnect()
        
        for target in self.targets.values():
            await target.disconnect()
    
    def add_task(self, name: str, task: SyncTask):
        """Add sync task"""
        self.tasks[name] = task
    
    async def run_task(self, name: str):
        """Run sync task"""
        task = self.tasks[name]
        source = self.sources[task.source]
        target = self.targets[task.target]
        
        try:
            self.logger.info(f"Starting sync task: {name}")
            start_time = time.time()
            
            async for batch in source.fetch_data(
                task.query,
                task.batch_size
            ):
                if task.filters:
                    batch = self._apply_filters(batch, task.filters)
                
                if task.transform:
                    batch = [task.transform(item) for item in batch]
                
                await target.write_data(batch, task.mapping)
            
            duration = time.time() - start_time
            self.logger.info(
                f"Completed sync task: {name} in {duration:.2f} seconds"
            )
        
        except Exception as e:
            self.logger.error(f"Error in sync task {name}: {e}")
            raise
    
    def _apply_filters(self,
                      data: List[Dict[str, Any]],
                      filters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply filters to data"""
        result = []
        for item in data:
            if self._match_filters(item, filters):
                result.append(item)
        return result
    
    def _match_filters(self,
                      item: Dict[str, Any],
                      filters: List[Dict[str, Any]]) -> bool:
        """Check if item matches filters"""
        for filter in filters:
            field = filter['field']
            op = filter['op']
            value = filter['value']
            
            if field not in item:
                return False
            
            if op == 'eq' and item[field] != value:
                return False
            elif op == 'ne' and item[field] == value:
                return False
            elif op == 'gt' and item[field] <= value:
                return False
            elif op == 'lt' and item[field] >= value:
                return False
            elif op == 'in' and item[field] not in value:
                return False
            elif op == 'nin' and item[field] in value:
                return False
        
        return True

class DataValidator:
    def __init__(self):
        self.validators: Dict[str, Callable] = {}
    
    def add_validator(self,
                     name: str,
                     validator: Callable[[Dict[str, Any]], bool]):
        """Add data validator"""
        self.validators[name] = validator
    
    def validate(self,
                data: List[Dict[str, Any]],
                validators: List[str]) -> List[Dict[str, Any]]:
        """Validate data with specified validators"""
        result = []
        for item in data:
            valid = True
            for validator_name in validators:
                validator = self.validators.get(validator_name)
                if validator and not validator(item):
                    valid = False
                    break
            if valid:
                result.append(item)
        return result

class DataTransformer:
    def __init__(self):
        self.transformers: Dict[str, Callable] = {}
    
    def add_transformer(self,
                       name: str,
                       transformer: Callable[[Dict[str, Any]], Dict[str, Any]]):
        """Add data transformer"""
        self.transformers[name] = transformer
    
    def transform(self,
                 data: List[Dict[str, Any]],
                 transformers: List[str]) -> List[Dict[str, Any]]:
        """Transform data with specified transformers"""
        result = data
        for transformer_name in transformers:
            transformer = self.transformers.get(transformer_name)
            if transformer:
                result = [transformer(item) for item in result]
        return result
