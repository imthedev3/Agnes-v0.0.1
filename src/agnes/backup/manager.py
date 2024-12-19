from typing import Dict, Any, List, Optional, Union
import asyncio
import aiomysql
import aioredis
import boto3
import botocore
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from enum import Enum
import tempfile
import os
import shutil
import gzip
import tarfile
import json
import hashlib
import aiofiles
from pathlib import Path

class BackupType(Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"
    SNAPSHOT = "snapshot"

class BackupStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    VERIFIED = "verified"

@dataclass
class Backup:
    id: str
    type: BackupType
    source: str
    destination: str
    status: BackupStatus
    size: Optional[int] = None
    checksum: Optional[str] = None
    metadata: Dict[str, Any] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None

class BackupManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.storage = BackupStorage(config['storage'])
        self.logger = logging.getLogger(__name__)
        self._setup_clients()
    
    def _setup_clients(self):
        """Setup backup clients"""
        # S3 client
        if self.config['storage']['type'] == 's3':
            self.s3 = boto3.client('s3')
        
        # MySQL connection pool
        if 'mysql' in self.config['sources']:
            self.mysql_pool = aiomysql.create_pool(
                **self.config['sources']['mysql']
            )
        
        # Redis connection
        if 'redis' in self.config['sources']:
            self.redis = aioredis.Redis.from_url(
                self.config['sources']['redis']['url']
            )
    
    async def create_backup(self,
                          type: BackupType,
                          source: str,
                          destination: str,
                          metadata: Dict[str, Any] = None) -> Backup:
        """Create new backup"""
        backup = Backup(
            id=hashlib.sha256(
                f"{source}:{destination}:{datetime.utcnow()}"
                .encode()
            ).hexdigest()[:12],
            type=type,
            source=source,
            destination=destination,
            status=BackupStatus.PENDING,
            metadata=metadata or {}
        )
        
        try:
            backup.started_at = datetime.utcnow()
            backup.status = BackupStatus.RUNNING
            
            # Perform backup based on source type
            if source.startswith('mysql://'):
                await self._backup_mysql(backup)
            elif source.startswith('redis://'):
                await self._backup_redis(backup)
            elif source.startswith('file://'):
                await self._backup_files(backup)
            else:
                raise ValueError(f"Unsupported source: {source}")
            
            # Verify backup
            if self.config.get('verify', {}).get('enabled', False):
                await self._verify_backup(backup)
            
            backup.completed_at = datetime.utcnow()
            backup.status = BackupStatus.COMPLETED
        
        except Exception as e:
            backup.status = BackupStatus.FAILED
            backup.error = str(e)
            self.logger.error(f"Backup failed: {e}")
            raise
        
        finally:
            # Store backup metadata
            await self.storage.store(backup)
        
        return backup
    
    async def restore_backup(self,
                           backup_id: str,
                           destination: Optional[str] = None) -> bool:
        """Restore backup"""
        backup = await self.storage.get(backup_id)
        if not backup:
            raise ValueError(f"Backup not found: {backup_id}")
        
        try:
            # Download backup if needed
            if backup.destination.startswith('s3://'):
                local_path = await self._download_from_s3(backup)
            else:
                local_path = backup.destination.replace('file://', '')
            
            # Restore based on source type
            if backup.source.startswith('mysql://'):
                await self._restore_mysql(backup, local_path)
            elif backup.source.startswith('redis://'):
                await self._restore_redis(backup, local_path)
            elif backup.source.startswith('file://'):
                await self._restore_files(backup, local_path)
            
            return True
        
        except Exception as e:
            self.logger.error(f"Restore failed: {e}")
            raise
        
        finally:
            # Cleanup temporary files
            if 'local_path' in locals():
                try:
                    os.remove(local_path)
                except:
                    pass
    
    async def _backup_mysql(self, backup: Backup):
        """Backup MySQL database"""
        db_name = backup.source.split('/')[-1]
        
        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            dump_file = os.path.join(temp_dir, f"{db_name}.sql")
            
            # Dump database
            async with self.mysql_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
                    if not await cursor.fetchone():
                        raise ValueError(f"Database not found: {db_name}")
            
            process = await asyncio.create_subprocess_exec(
                'mysqldump',
                f"--host={self.config['sources']['mysql']['host']}",
                f"--user={self.config['sources']['mysql']['user']}",
                f"--password={self.config['sources']['mysql']['password']}",
                db_name,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                raise RuntimeError(
                    f"mysqldump failed: {stderr.decode()}"
                )
            
            # Compress dump
            async with aiofiles.open(dump_file, 'wb') as f:
                await f.write(gzip.compress(stdout))
            
            # Calculate checksum
            backup.checksum = await self._calculate_checksum(dump_file)
            backup.size = os.path.getsize(dump_file)
            
            # Store backup
            if backup.destination.startswith('s3://'):
                await self._upload_to_s3(dump_file, backup)
            else:
                dest_path = backup.destination.replace('file://', '')
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                shutil.copy2(dump_file, dest_path)
    
    async def _backup_redis(self, backup: Backup):
        """Backup Redis database"""
        with tempfile.TemporaryDirectory() as temp_dir:
            dump_file = os.path.join(temp_dir, 'dump.rdb')
            
            # Save Redis database
            await self.redis.save()
            
            # Copy dump file
            redis_dir = self.config['sources']['redis'].get(
                'dir',
                '/var/lib/redis'
            )
            shutil.copy2(
                os.path.join(redis_dir, 'dump.rdb'),
                dump_file
            )
            
            # Compress dump
            compressed_file = f"{dump_file}.gz"
            with open(dump_file, 'rb') as f_in:
                with gzip.open(compressed_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # Calculate checksum
            backup.checksum = await self._calculate_checksum(compressed_file)
            backup.size = os.path.getsize(compressed_file)
            
            # Store backup
            if backup.destination.startswith('s3://'):
                await self._upload_to_s3(compressed_file, backup)
            else:
                dest_path = backup.destination.replace('file://', '')
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                shutil.copy2(compressed_file, dest_path)
    
    async def _backup_files(self, backup: Backup):
        """Backup files"""
        source_path = backup.source.replace('file://', '')
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create tar archive
            archive_path = os.path.join(temp_dir, 'backup.tar.gz')
            
            with tarfile.open(archive_path, 'w:gz') as tar:
                tar.add(source_path, arcname='.')
            
            # Calculate checksum
            backup.checksum = await self._calculate_checksum(archive_path)
            backup.size = os.path.getsize(archive_path)
            
            # Store backup
            if backup.destination.startswith('s3://'):
                await self._upload_to_s3(archive_path, backup)
            else:
                dest_path = backup.destination.replace('file://', '')
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                shutil.copy2(archive_path, dest_path)
    
    async def _calculate_checksum(self, file_path: str) -> str:
        """Calculate file checksum"""
        hasher = hashlib.sha256()
        
        async with aiofiles.open(file_path, 'rb') as f:
            while chunk := await f.read(8192):
                hasher.update(chunk)
        
        return hasher.hexdigest()
    
    async def _upload_to_s3(self,
                           file_path: str,
                           backup: Backup):
        """Upload file to S3"""
        bucket = backup.destination.split('/')[2]
        key = '/'.join(backup.destination.split('/')[3:])
        
        self.s3.upload_file(
            file_path,
            bucket,
            key,
            ExtraArgs={
                'Metadata': {
                    'checksum': backup.checksum,
                    'original_source': backup.source,
                    'backup_type': backup.type.value,
                    'created_at': backup.started_at.isoformat()
                }
            }
        )
    
    async def _download_from_s3(self,
                               backup: Backup) -> str:
        """Download file from S3"""
        bucket = backup.destination.split('/')[2]
        key = '/'.join(backup.destination.split('/')[3:])
        
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            self.s3.download_file(
                bucket,
                key,
                temp_file.name
            )
            return temp_file.name

class BackupStorage:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config['type']
        self._setup_storage()
    
    def _setup_storage(self):
        """Setup storage backend"""
        if self.type == 'mysql':
            self.pool = aiomysql.create_pool(
                **self.config['mysql']
            )
    
    async def store(self, backup: Backup):
        """Store backup metadata"""
        if self.type == 'mysql':
            await self._store_mysql(backup)
    
    async def get(self, backup_id: str) -> Optional[Backup]:
        """Get backup metadata"""
        if self.type == 'mysql':
            return await self._get_mysql(backup_id)
    
    async def list(self,
                  source: Optional[str] = None,
                  type: Optional[BackupType] = None,
                  status: Optional[BackupStatus] = None,
                  start_date: Optional[datetime] = None,
                  end_date: Optional[datetime] = None) -> List[Backup]:
        """List backups"""
        if self.type == 'mysql':
            return await self._list_mysql(
                source, type, status, start_date, end_date
            )
