from typing import Dict, Any, List, Optional, Union, Callable
import asyncio
import aiocron
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from enum import Enum
import json
import aioredis
import aiomysql
import aiohttp
import uuid
import pytz
from croniter import croniter

class JobType(Enum):
    REPORT = "report"
    BACKUP = "backup"
    MAINTENANCE = "maintenance"
    NOTIFICATION = "notification"
    CLEANUP = "cleanup"
    CUSTOM = "custom"

class JobPriority(Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class Job:
    id: str
    type: JobType
    name: str
    schedule: str
    handler: str
    priority: JobPriority
    params: Dict[str, Any]
    metadata: Dict[str, Any]
    enabled: bool = True
    status: JobStatus = JobStatus.PENDING
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    error: Optional[str] = None
    retries: int = 0
    max_retries: int = 3
    timeout: int = 300
    created_at: datetime = datetime.utcnow()

class SchedulerManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.jobs: Dict[str, Job] = {}
        self.running_jobs: Dict[str, asyncio.Task] = {}
        self.handlers: Dict[str, Callable] = {}
        self.storage = JobStorage(config['storage'])
        self.lock = JobLock(config['lock'])
        self.logger = logging.getLogger(__name__)
        self.timezone = pytz.timezone(config.get('timezone', 'UTC'))
    
    async def start(self):
        """Start scheduler"""
        self.logger.info("Starting scheduler")
        
        # Load jobs from storage
        jobs = await self.storage.load_jobs()
        for job in jobs:
            await self.add_job(job)
        
        # Start job scheduler
        asyncio.create_task(self._scheduler_loop())
    
    async def stop(self):
        """Stop scheduler"""
        self.logger.info("Stopping scheduler")
        
        # Cancel running jobs
        for job_id, task in self.running_jobs.items():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self.running_jobs.clear()
    
    async def add_job(self, job: Job):
        """Add new job"""
        if job.id in self.jobs:
            raise ValueError(f"Job {job.id} already exists")
        
        # Validate schedule
        if not croniter.is_valid(job.schedule):
            raise ValueError(f"Invalid schedule: {job.schedule}")
        
        # Calculate next run time
        job.next_run = croniter(
            job.schedule,
            datetime.now(self.timezone)
        ).get_next(datetime)
        
        self.jobs[job.id] = job
        await self.storage.store_job(job)
        
        self.logger.info(f"Added job: {job.id}")
    
    async def remove_job(self, job_id: str):
        """Remove job"""
        if job_id not in self.jobs:
            raise ValueError(f"Job {job_id} not found")
        
        # Cancel if running
        if job_id in self.running_jobs:
            self.running_jobs[job_id].cancel()
            try:
                await self.running_jobs[job_id]
            except asyncio.CancelledError:
                pass
            del self.running_jobs[job_id]
        
        del self.jobs[job_id]
        await self.storage.delete_job(job_id)
        
        self.logger.info(f"Removed job: {job_id}")
    
    async def update_job(self, job: Job):
        """Update job"""
        if job.id not in self.jobs:
            raise ValueError(f"Job {job.id} not found")
        
        # Validate schedule
        if not croniter.is_valid(job.schedule):
            raise ValueError(f"Invalid schedule: {job.schedule}")
        
        # Calculate next run time
        job.next_run = croniter(
            job.schedule,
            datetime.now(self.timezone)
        ).get_next(datetime)
        
        self.jobs[job.id] = job
        await self.storage.store_job(job)
        
        self.logger.info(f"Updated job: {job.id}")
    
    def register_handler(self,
                        name: str,
                        handler: Callable):
        """Register job handler"""
        self.handlers[name] = handler
    
    async def _scheduler_loop(self):
        """Main scheduler loop"""
        while True:
            try:
                now = datetime.now(self.timezone)
                
                # Find jobs to run
                for job in self.jobs.values():
                    if job.enabled and job.next_run <= now:
                        # Start job if not already running
                        if job.id not in self.running_jobs:
                            self.running_jobs[job.id] = asyncio.create_task(
                                self._run_job(job)
                            )
                
                # Clean up completed jobs
                completed = []
                for job_id, task in self.running_jobs.items():
                    if task.done():
                        completed.append(job_id)
                for job_id in completed:
                    del self.running_jobs[job_id]
                
                await asyncio.sleep(1)
            
            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(5)
    
    async def _run_job(self, job: Job):
        """Run job"""
        # Acquire lock
        if not await self.lock.acquire(job.id):
            self.logger.warning(f"Job {job.id} is already running")
            return
        
        try:
            self.logger.info(f"Starting job: {job.id}")
            job.status = JobStatus.RUNNING
            job.last_run = datetime.now(self.timezone)
            
            # Get handler
            handler = self.handlers.get(job.handler)
            if not handler:
                raise ValueError(f"Handler not found: {job.handler}")
            
            # Run handler with timeout
            try:
                async with asyncio.timeout(job.timeout):
                    await handler(job.params)
                
                job.status = JobStatus.COMPLETED
                job.error = None
                job.retries = 0
            
            except asyncio.TimeoutError:
                job.status = JobStatus.FAILED
                job.error = "Job timed out"
                await self._handle_failure(job)
            
            except Exception as e:
                job.status = JobStatus.FAILED
                job.error = str(e)
                await self._handle_failure(job)
            
            # Calculate next run time
            job.next_run = croniter(
                job.schedule,
                datetime.now(self.timezone)
            ).get_next(datetime)
            
            # Update storage
            await self.storage.store_job(job)
        
        finally:
            # Release lock
            await self.lock.release(job.id)
    
    async def _handle_failure(self, job: Job):
        """Handle job failure"""
        job.retries += 1
        
        if job.retries < job.max_retries:
            # Retry after delay
            delay = min(300, 30 * (2 ** (job.retries - 1)))  # Exponential backoff
            job.next_run = datetime.now(self.timezone) + timedelta(seconds=delay)
            
            self.logger.info(
                f"Job {job.id} failed, retry {job.retries}/{job.max_retries} "
                f"in {delay} seconds"
            )
        else:
            self.logger.error(
                f"Job {job.id} failed after {job.max_retries} retries"
            )
            
            # Send notification
            await self._send_failure_notification(job)
    
    async def _send_failure_notification(self, job: Job):
        """Send job failure notification"""
        if not self.config.get('notifications', {}).get('enabled', False):
            return
        
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(
                    self.config['notifications']['webhook_url'],
                    json={
                        'job_id': job.id,
                        'job_name': job.name,
                        'job_type': job.type.value,
                        'error': job.error,
                        'retries': job.retries,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                )
        except Exception as e:
            self.logger.error(f"Failed to send notification: {e}")

class JobStorage:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config['type']
        self._setup_storage()
    
    def _setup_storage(self):
        """Setup storage backend"""
        if self.type == 'mysql':
            self.pool = aiomysql.create_pool(**self.config['mysql'])
        elif self.type == 'redis':
            self.redis = aioredis.Redis.from_url(
                self.config['redis_url']
            )
    
    async def store_job(self, job: Job):
        """Store job"""
        if self.type == 'mysql':
            await self._store_mysql(job)
        elif self.type == 'redis':
            await self._store_redis(job)
    
    async def load_jobs(self) -> List[Job]:
        """Load all jobs"""
        if self.type == 'mysql':
            return await self._load_mysql()
        elif self.type == 'redis':
            return await self._load_redis()
    
    async def delete_job(self, job_id: str):
        """Delete job"""
        if self.type == 'mysql':
            await self._delete_mysql(job_id)
        elif self.type == 'redis':
            await self._delete_redis(job_id)

class JobLock:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.redis = aioredis.Redis.from_url(
            config['redis_url']
        )
    
    async def acquire(self,
                     job_id: str,
                     timeout: int = 60) -> bool:
        """Acquire job lock"""
        key = f"lock:job:{job_id}"
        return await self.redis.set(
            key,
            "1",
            ex=timeout,
            nx=True
        )
    
    async def release(self, job_id: str):
        """Release job lock"""
        key = f"lock:job:{job_id}"
        await self.redis.delete(key)
