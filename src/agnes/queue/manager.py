from typing import Dict, Any, List, Optional, Union, Callable
import asyncio
import redis.asyncio as redis
import json
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from enum import Enum
import uuid
import traceback
import signal
import functools
import time
import pickle

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"

class TaskPriority(Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3

@dataclass
class Task:
    id: str
    name: str
    payload: Dict[str, Any]
    priority: TaskPriority
    status: TaskStatus
    queue: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    retries: int = 0
    max_retries: int = 3
    retry_delay: int = 60
    timeout: Optional[int] = None
    result: Optional[Any] = None

class TaskQueue:
    def __init__(self, redis_client: redis.Redis, name: str):
        self.redis = redis_client
        self.name = name
        self.processing_key = f"{name}:processing"
        self.failed_key = f"{name}:failed"
    
    async def push(self, task: Task):
        """Push task to queue"""
        await self.redis.zadd(
            self.name,
            {
                json.dumps({
                    'id': task.id,
                    'name': task.name,
                    'payload': task.payload,
                    'priority': task.priority.value,
                    'created_at': task.created_at.isoformat()
                }): task.priority.value
            }
        )
    
    async def pop(self) -> Optional[Task]:
        """Pop task from queue"""
        result = await self.redis.zpopmax(self.name)
        if not result:
            return None
        
        data = json.loads(result[0][0])
        task = Task(
            id=data['id'],
            name=data['name'],
            payload=data['payload'],
            priority=TaskPriority(data['priority']),
            status=TaskStatus.PENDING,
            queue=self.name,
            created_at=datetime.fromisoformat(data['created_at'])
        )
        
        # Move to processing set
        await self.redis.hset(
            self.processing_key,
            task.id,
            json.dumps(data)
        )
        
        return task
    
    async def complete(self, task: Task):
        """Mark task as completed"""
        await self.redis.hdel(self.processing_key, task.id)
    
    async def fail(self, task: Task):
        """Mark task as failed"""
        await self.redis.hdel(self.processing_key, task.id)
        await self.redis.hset(
            self.failed_key,
            task.id,
            json.dumps({
                'task': {
                    'id': task.id,
                    'name': task.name,
                    'payload': task.payload,
                    'priority': task.priority.value
                },
                'error': task.error,
                'failed_at': datetime.utcnow().isoformat()
            })
        )
    
    async def retry(self, task: Task):
        """Retry failed task"""
        await self.redis.hdel(self.failed_key, task.id)
        await self.push(task)

class TaskWorker:
    def __init__(self,
                 queue: TaskQueue,
                 handlers: Dict[str, Callable],
                 config: Dict[str, Any]):
        self.queue = queue
        self.handlers = handlers
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.running = False
        self._current_task: Optional[Task] = None
    
    async def start(self):
        """Start worker"""
        self.running = True
        self.logger.info(f"Starting worker for queue {self.queue.name}")
        
        # Setup signal handlers
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_event_loop().add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(self.shutdown(s))
            )
        
        while self.running:
            try:
                await self._process_next_task()
                await asyncio.sleep(0.1)  # Prevent busy loop
            except Exception as e:
                self.logger.error(f"Error processing task: {e}")
                await asyncio.sleep(1)  # Back off on error
    
    async def _process_next_task(self):
        """Process next task from queue"""
        task = await self.queue.pop()
        if not task:
            return
        
        self._current_task = task
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.utcnow()
        
        try:
            handler = self.handlers.get(task.name)
            if not handler:
                raise ValueError(f"No handler found for task {task.name}")
            
            if task.timeout:
                async with asyncio.timeout(task.timeout):
                    task.result = await handler(task.payload)
            else:
                task.result = await handler(task.payload)
            
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.utcnow()
            await self.queue.complete(task)
        
        except Exception as e:
            task.error = str(e)
            if task.retries < task.max_retries:
                task.retries += 1
                task.status = TaskStatus.RETRYING
                await asyncio.sleep(task.retry_delay)
                await self.queue.retry(task)
            else:
                task.status = TaskStatus.FAILED
                await self.queue.fail(task)
            
            self.logger.error(
                f"Task {task.id} failed: {e}\n{traceback.format_exc()}"
            )
        
        finally:
            self._current_task = None
    
    async def shutdown(self, signal=None):
        """Shutdown worker"""
        self.logger.info(f"Shutting down worker (signal={signal})")
        self.running = False
        
        if self._current_task:
            # Wait for current task to complete
            try:
                async with asyncio.timeout(30):
                    while self._current_task:
                        await asyncio.sleep(0.1)
            except asyncio.TimeoutError:
                self.logger.warning(
                    "Timeout waiting for current task to complete"
                )

class QueueManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.redis = redis.Redis.from_url(config['redis_url'])
        self.queues: Dict[str, TaskQueue] = {}
        self.workers: Dict[str, List[TaskWorker]] = {}
        self.handlers: Dict[str, Callable] = {}
        self.logger = logging.getLogger(__name__)
    
    def register_handler(self, name: str, handler: Callable):
        """Register task handler"""
        self.handlers[name] = handler
    
    def register_queue(self, name: str):
        """Register task queue"""
        if name not in self.queues:
            self.queues[name] = TaskQueue(self.redis, name)
    
    async def enqueue(self,
                     queue: str,
                     name: str,
                     payload: Dict[str, Any],
                     priority: TaskPriority = TaskPriority.NORMAL,
                     timeout: Optional[int] = None) -> Task:
        """Enqueue new task"""
        if queue not in self.queues:
            raise ValueError(f"Queue {queue} not registered")
        
        task = Task(
            id=str(uuid.uuid4()),
            name=name,
            payload=payload,
            priority=priority,
            status=TaskStatus.PENDING,
            queue=queue,
            created_at=datetime.utcnow(),
            timeout=timeout
        )
        
        await self.queues[queue].push(task)
        return task
    
    async def start_workers(self):
        """Start queue workers"""
        for queue_name, queue_config in self.config['queues'].items():
            self.register_queue(queue_name)
            
            workers = []
            for _ in range(queue_config['workers']):
                worker = TaskWorker(
                    self.queues[queue_name],
                    self.handlers,
                    queue_config
                )
                workers.append(worker)
                asyncio.create_task(worker.start())
            
            self.workers[queue_name] = workers
            self.logger.info(
                f"Started {len(workers)} workers for queue {queue_name}"
            )
    
    async def shutdown(self):
        """Shutdown all workers"""
        for queue_name, workers in self.workers.items():
            self.logger.info(f"Shutting down workers for queue {queue_name}")
            for worker in workers:
                await worker.shutdown()

class TaskScheduler:
    def __init__(self, manager: QueueManager):
        self.manager = manager
        self.schedules: Dict[str, Dict[str, Any]] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
    
    def add_schedule(self,
                    name: str,
                    task: str,
                    payload: Dict[str, Any],
                    schedule: str,
                    queue: str = "default",
                    priority: TaskPriority = TaskPriority.NORMAL):
        """Add scheduled task"""
        self.schedules[name] = {
            'task': task,
            'payload': payload,
            'schedule': schedule,
            'queue': queue,
            'priority': priority,
            'last_run': None
        }
    
    async def start(self):
        """Start scheduler"""
        self.running = True
        while self.running:
            now = datetime.utcnow()
            
            for name, schedule in self.schedules.items():
                if self._should_run(schedule, now):
                    try:
                        await self.manager.enqueue(
                            schedule['queue'],
                            schedule['task'],
                            schedule['payload'],
                            schedule['priority']
                        )
                        schedule['last_run'] = now
                    except Exception as e:
                        self.logger.error(
                            f"Error scheduling task {name}: {e}"
                        )
            
            await asyncio.sleep(1)
    
    def _should_run(self,
                    schedule: Dict[str, Any],
                    now: datetime) -> bool:
        """Check if schedule should run"""
        if not schedule['last_run']:
            return True
        
        interval = schedule['schedule']
        if isinstance(interval, int):
            # Interval in seconds
            return (now - schedule['last_run']).total_seconds() >= interval
        else:
            # Cron-style schedule
            # Implementation left as exercise
            return False
    
    async def shutdown(self):
        """Shutdown scheduler"""
        self.running = False
