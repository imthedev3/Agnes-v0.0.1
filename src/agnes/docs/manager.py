from typing import Dict, Any, List, Optional, Union, BinaryIO
import asyncio
import aiofiles
from datetime import datetime
import uuid
from enum import Enum
import logging
import json
import yaml
from pathlib import Path
import mimetypes
import hashlib
from dataclasses import dataclass
import aioboto3
from elasticsearch import AsyncElasticsearch
import markdown
import frontmatter
import re
from PIL import Image
import io

class DocumentType(Enum):
    MARKDOWN = "markdown"
    PDF = "pdf"
    IMAGE = "image"
    SPREADSHEET = "spreadsheet"
    DOCUMENT = "document"
    CODE = "code"
    BINARY = "binary"

class DocumentStatus(Enum):
    DRAFT = "draft"
    PUBLISHED = "published"
    ARCHIVED = "archived"

@dataclass
class Document:
    id: str
    title: str
    content: Union[str, bytes]
    type: DocumentType
    status: DocumentStatus
    version: int
    created_at: datetime
    updated_at: datetime
    author: str
    tags: List[str]
    metadata: Dict[str, Any]
    path: Optional[str] = None
    parent_id: Optional[str] = None
    mime_type: Optional[str] = None
    size: Optional[int] = None
    checksum: Optional[str] = None

class DocumentManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.storage = DocumentStorage(config['storage'])
        self.search = DocumentSearch(config['search'])
        self.logger = logging.getLogger(__name__)
    
    async def create_document(self,
                            title: str,
                            content: Union[str, bytes],
                            type: DocumentType,
                            author: str,
                            tags: List[str] = None,
                            metadata: Dict[str, Any] = None,
                            path: Optional[str] = None,
                            parent_id: Optional[str] = None) -> Document:
        """Create new document"""
        doc = Document(
            id=str(uuid.uuid4()),
            title=title,
            content=content,
            type=type,
            status=DocumentStatus.DRAFT,
            version=1,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            author=author,
            tags=tags or [],
            metadata=metadata or {},
            path=path,
            parent_id=parent_id
        )
        
        # Determine mime type and size
        if isinstance(content, bytes):
            doc.mime_type = mimetypes.guess_type(title)[0]
            doc.size = len(content)
            doc.checksum = hashlib.sha256(content).hexdigest()
        else:
            doc.mime_type = 'text/plain'
            doc.size = len(content.encode())
            doc.checksum = hashlib.sha256(content.encode()).hexdigest()
        
        # Store document
        await self.storage.store_document(doc)
        
        # Index for search
        await self.search.index_document(doc)
        
        return doc
    
    async def update_document(self,
                            doc_id: str,
                            title: Optional[str] = None,
                            content: Optional[Union[str, bytes]] = None,
                            tags: Optional[List[str]] = None,
                            metadata: Optional[Dict[str, Any]] = None) -> Document:
        """Update existing document"""
        doc = await self.storage.get_document(doc_id)
        if not doc:
            raise ValueError(f"Document {doc_id} not found")
        
        if title:
            doc.title = title
        if content:
            doc.content = content
            if isinstance(content, bytes):
                doc.size = len(content)
                doc.checksum = hashlib.sha256(content).hexdigest()
            else:
                doc.size = len(content.encode())
                doc.checksum = hashlib.sha256(content.encode()).hexdigest()
        if tags:
            doc.tags = tags
        if metadata:
            doc.metadata.update(metadata)
        
        doc.version += 1
        doc.updated_at = datetime.utcnow()
        
        await self.storage.store_document(doc)
        await self.search.index_document(doc)
        
        return doc
    
    async def get_document(self,
                          doc_id: str,
                          version: Optional[int] = None) -> Optional[Document]:
        """Get document by ID"""
        return await self.storage.get_document(doc_id, version)
    
    async def delete_document(self, doc_id: str):
        """Delete document"""
        await self.storage.delete_document(doc_id)
        await self.search.delete_document(doc_id)
    
    async def search_documents(self,
                             query: str,
                             doc_type: Optional[DocumentType] = None,
                             tags: Optional[List[str]] = None,
                             author: Optional[str] = None,
                             status: Optional[DocumentStatus] = None,
                             path: Optional[str] = None,
                             page: int = 1,
                             size: int = 10) -> List[Document]:
        """Search documents"""
        return await self.search.search_documents(
            query, doc_type, tags, author, status, path, page, size
        )
    
    async def get_document_versions(self,
                                  doc_id: str) -> List[Document]:
        """Get document version history"""
        return await self.storage.get_document_versions(doc_id)
    
    async def publish_document(self, doc_id: str) -> Document:
        """Publish document"""
        doc = await self.storage.get_document(doc_id)
        if not doc:
            raise ValueError(f"Document {doc_id} not found")
        
        doc.status = DocumentStatus.PUBLISHED
        doc.updated_at = datetime.utcnow()
        
        await self.storage.store_document(doc)
        await self.search.index_document(doc)
        
        return doc
    
    async def archive_document(self, doc_id: str) -> Document:
        """Archive document"""
        doc = await self.storage.get_document(doc_id)
        if not doc:
            raise ValueError(f"Document {doc_id} not found")
        
        doc.status = DocumentStatus.ARCHIVED
        doc.updated_at = datetime.utcnow()
        
        await self.storage.store_document(doc)
        await self.search.index_document(doc)
        
        return doc

class DocumentStorage:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config['type']
        self.session = aioboto3.Session()
    
    async def store_document(self, doc: Document):
        """Store document"""
        if self.type == 's3':
            await self._store_s3(doc)
        else:
            await self._store_file(doc)
    
    async def _store_s3(self, doc: Document):
        """Store document in S3"""
        bucket = self.config['bucket']
        key = f"documents/{doc.id}/v{doc.version}"
        
        async with self.session.client('s3') as s3:
            # Store content
            content = io.BytesIO(
                doc.content if isinstance(doc.content, bytes)
                else doc.content.encode()
            )
            await s3.upload_fileobj(
                content,
                bucket,
                f"{key}/content",
                ExtraArgs={'ContentType': doc.mime_type}
            )
            
            # Store metadata
            metadata = {
                'id': doc.id,
                'title': doc.title,
                'type': doc.type.value,
                'status': doc.status.value,
                'version': doc.version,
                'created_at': doc.created_at.isoformat(),
                'updated_at': doc.updated_at.isoformat(),
                'author': doc.author,
                'tags': doc.tags,
                'metadata': doc.metadata,
                'path': doc.path,
                'parent_id': doc.parent_id,
                'mime_type': doc.mime_type,
                'size': doc.size,
                'checksum': doc.checksum
            }
            await s3.put_object(
                Bucket=bucket,
                Key=f"{key}/metadata.json",
                Body=json.dumps(metadata),
                ContentType='application/json'
            )
    
    async def _store_file(self, doc: Document):
        """Store document in filesystem"""
        base_path = Path(self.config['path']) / doc.id / f"v{doc.version}"
        base_path.mkdir(parents=True, exist_ok=True)
        
        # Store content
        content_path = base_path / "content"
        async with aiofiles.open(content_path, 'wb') as f:
            await f.write(
                doc.content if isinstance(doc.content, bytes)
                else doc.content.encode()
            )
        
        # Store metadata
        metadata = {
            'id': doc.id,
            'title': doc.title,
            'type': doc.type.value,
            'status': doc.status.value,
            'version': doc.version,
            'created_at': doc.created_at.isoformat(),
            'updated_at': doc.updated_at.isoformat(),
            'author': doc.author,
            'tags': doc.tags,
            'metadata': doc.metadata,
            'path': doc.path,
            'parent_id': doc.parent_id,
            'mime_type': doc.mime_type,
            'size': doc.size,
            'checksum': doc.checksum
        }
        metadata_path = base_path / "metadata.json"
        async with aiofiles.open(metadata_path, 'w') as f:
            await f.write(json.dumps(metadata))
    
    async def get_document(self,
                          doc_id: str,
                          version: Optional[int] = None) -> Optional[Document]:
        """Get document"""
        if self.type == 's3':
            return await self._get_s3(doc_id, version)
        else:
            return await self._get_file(doc_id, version)
    
    async def delete_document(self, doc_id: str):
        """Delete document"""
        if self.type == 's3':
            await self._delete_s3(doc_id)
        else:
            await self._delete_file(doc_id)
    
    async def get_document_versions(self,
                                  doc_id: str) -> List[Document]:
        """Get document versions"""
        if self.type == 's3':
            return await self._get_versions_s3(doc_id)
        else:
            return await self._get_versions_file(doc_id)

class DocumentSearch:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = AsyncElasticsearch([config['elasticsearch_url']])
    
    async def index_document(self, doc: Document):
        """Index document for search"""
        await self.client.index(
            index='documents',
            id=doc.id,
            body={
                'title': doc.title,
                'content': doc.content if isinstance(doc.content, str)
                else None,
                'type': doc.type.value,
                'status': doc.status.value,
                'version': doc.version,
                'created_at': doc.created_at.isoformat(),
                'updated_at': doc.updated_at.isoformat(),
                'author': doc.author,
                'tags': doc.tags,
                'metadata': doc.metadata,
                'path': doc.path,
                'parent_id': doc.parent_id
            }
        )
    
    async def delete_document(self, doc_id: str):
        """Delete document from search index"""
        await self.client.delete(
            index='documents',
            id=doc_id
        )
    
    async def search_documents(self,
                             query: str,
                             doc_type: Optional[DocumentType] = None,
                             tags: Optional[List[str]] = None,
                             author: Optional[str] = None,
                             status: Optional[DocumentStatus] = None,
                             path: Optional[str] = None,
                             page: int = 1,
                             size: int = 10) -> List[Document]:
        """Search documents"""
        must = [
            {
                'multi_match': {
                    'query': query,
                    'fields': ['title^2', 'content'],
                    'type': 'best_fields'
                }
            }
        ]
        
        if doc_type:
            must.append({'term': {'type': doc_type.value}})
        if tags:
            must.append({'terms': {'tags': tags}})
        if author:
            must.append({'term': {'author': author}})
        if status:
            must.append({'term': {'status': status.value}})
        if path:
            must.append({'prefix': {'path': path}})
        
        body = {
            'query': {
                'bool': {
                    'must': must
                }
            },
            'from': (page - 1) * size,
            'size': size,
            'sort': [
                {'_score': 'desc'},
                {'updated_at': 'desc'}
            ]
        }
        
        result = await self.client.search(
            index='documents',
            body=body
        )
        
        return [
            await self._hit_to_document(hit)
            for hit in result['hits']['hits']
        ]
    
    async def _hit_to_document(self, hit: Dict[str, Any]) -> Document:
        """Convert search hit to document"""
        source = hit['_source']
        doc = Document(
            id=hit['_id'],
            title=source['title'],
            content=source.get('content', ''),
            type=DocumentType(source['type']),
            status=DocumentStatus(source['status']),
            version=source['version'],
            created_at=datetime.fromisoformat(source['created_at']),
            updated_at=datetime.fromisoformat(source['updated_at']),
            author=source['author'],
            tags=source['tags'],
            metadata=source['metadata'],
            path=source.get('path'),
            parent_id=source.get('parent_id')
        )
        return doc
