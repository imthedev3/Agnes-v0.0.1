from typing import Dict, Any, List, Optional, Union
import asyncio
from elasticsearch import AsyncElasticsearch
from datetime import datetime
import logging
from dataclasses import dataclass
import json
from enum import Enum
import re

class SearchIndex(Enum):
    PRODUCTS = "products"
    USERS = "users"
    DOCUMENTS = "documents"
    LOGS = "logs"

@dataclass
class SearchResult:
    id: str
    score: float
    source: Dict[str, Any]
    highlights: Optional[Dict[str, List[str]]] = None

class SearchEngine:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = AsyncElasticsearch([config['elasticsearch_url']])
        self.logger = logging.getLogger(__name__)
        self.index_configs = self._load_index_configs()
    
    def _load_index_configs(self) -> Dict[str, Dict[str, Any]]:
        """Load index configurations"""
        configs = {}
        for index in SearchIndex:
            config_file = f"config/indices/{index.value}.json"
            try:
                with open(config_file) as f:
                    configs[index.value] = json.load(f)
            except FileNotFoundError:
                self.logger.warning(f"Index config not found: {config_file}")
        return configs
    
    async def initialize(self):
        """Initialize search engine"""
        for index_name, config in self.index_configs.items():
            if not await self.client.indices.exists(index=index_name):
                await self.client.indices.create(
                    index=index_name,
                    body=config
                )
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.client.close()
    
    async def index_document(self,
                           index: SearchIndex,
                           document: Dict[str, Any],
                           doc_id: Optional[str] = None):
        """Index document"""
        try:
            await self.client.index(
                index=index.value,
                id=doc_id,
                body=document,
                refresh=True
            )
        except Exception as e:
            self.logger.error(f"Error indexing document: {e}")
            raise
    
    async def bulk_index(self,
                        index: SearchIndex,
                        documents: List[Dict[str, Any]]):
        """Bulk index documents"""
        actions = []
        for doc in documents:
            action = {
                '_index': index.value,
                '_source': doc
            }
            if 'id' in doc:
                action['_id'] = doc.pop('id')
            actions.append(action)
        
        try:
            await self.client.bulk(body=actions, refresh=True)
        except Exception as e:
            self.logger.error(f"Error bulk indexing documents: {e}")
            raise
    
    async def search(self,
                    index: SearchIndex,
                    query: str,
                    filters: Optional[Dict[str, Any]] = None,
                    sort: Optional[List[Dict[str, str]]] = None,
                    page: int = 1,
                    size: int = 10) -> List[SearchResult]:
        """Search documents"""
        try:
            body = {
                'query': self._build_query(query, filters),
                'from': (page - 1) * size,
                'size': size,
                'highlight': {
                    'fields': {
                        '*': {}
                    }
                }
            }
            
            if sort:
                body['sort'] = sort
            
            response = await self.client.search(
                index=index.value,
                body=body
            )
            
            return [
                SearchResult(
                    id=hit['_id'],
                    score=hit['_score'],
                    source=hit['_source'],
                    highlights=hit.get('highlight')
                )
                for hit in response['hits']['hits']
            ]
        
        except Exception as e:
            self.logger.error(f"Error searching documents: {e}")
            raise
    
    def _build_query(self,
                    query: str,
                    filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Build Elasticsearch query"""
        must = [
            {
                'multi_match': {
                    'query': query,
                    'fields': ['*'],
                    'type': 'best_fields',
                    'fuzziness': 'AUTO'
                }
            }
        ]
        
        if filters:
            for field, value in filters.items():
                if isinstance(value, list):
                    must.append({'terms': {field: value}})
                else:
                    must.append({'term': {field: value}})
        
        return {
            'bool': {
                'must': must
            }
        }
    
    async def suggest(self,
                     index: SearchIndex,
                     field: str,
                     prefix: str,
                     size: int = 5) -> List[str]:
        """Get suggestions for field"""
        try:
            body = {
                'suggest': {
                    'suggestions': {
                        'prefix': prefix,
                        'completion': {
                            'field': field,
                            'size': size
                        }
                    }
                }
            }
            
            response = await self.client.search(
                index=index.value,
                body=body
            )
            
            suggestions = []
            for option in response['suggest']['suggestions'][0]['options']:
                suggestions.append(option['text'])
            
            return suggestions
        
        except Exception as e:
            self.logger.error(f"Error getting suggestions: {e}")
            raise
    
    async def delete_document(self,
                            index: SearchIndex,
                            doc_id: str):
        """Delete document"""
        try:
            await self.client.delete(
                index=index.value,
                id=doc_id,
                refresh=True
            )
        except Exception as e:
            self.logger.error(f"Error deleting document: {e}")
            raise
    
    async def update_document(self,
                            index: SearchIndex,
                            doc_id: str,
                            updates: Dict[str, Any]):
        """Update document"""
        try:
            await self.client.update(
                index=index.value,
                id=doc_id,
                body={'doc': updates},
                refresh=True
            )
        except Exception as e:
            self.logger.error(f"Error updating document: {e}")
            raise

class SearchAnalyzer:
    def __init__(self, engine: SearchEngine):
        self.engine = engine
    
    def analyze_text(self, text: str) -> List[str]:
        """Analyze text into tokens"""
        # Simple tokenization for example
        return re.findall(r'\w+', text.lower())
    
    def calculate_relevance(self,
                          query_tokens: List[str],
                          document_tokens: List[str]) -> float:
        """Calculate document relevance score"""
        query_set = set(query_tokens)
        doc_set = set(document_tokens)
        
        intersection = query_set.intersection(doc_set)
        union = query_set.union(doc_set)
        
        return len(intersection) / len(union)

class SearchIndexer:
    def __init__(self, engine: SearchEngine):
        self.engine = engine
        self.batch_size = 100
        self.queue: List[Dict[str, Any]] = []
    
    async def add_to_queue(self,
                          index: SearchIndex,
                          document: Dict[str, Any]):
        """Add document to indexing queue"""
        self.queue.append({
            'index': index,
            'document': document
        })
        
        if len(self.queue) >= self.batch_size:
            await self.flush_queue()
    
    async def flush_queue(self):
        """Flush indexing queue"""
        if not self.queue:
            return
        
        # Group documents by index
        index_groups: Dict[SearchIndex, List[Dict[str, Any]]] = {}
        for item in self.queue:
            if item['index'] not in index_groups:
                index_groups[item['index']] = []
            index_groups[item['index']].append(item['document'])
        
        # Bulk index each group
        for index, documents in index_groups.items():
            await self.engine.bulk_index(index, documents)
        
        self.queue.clear()
