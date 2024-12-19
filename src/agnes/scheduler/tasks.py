from typing import Dict, Any, Optional, List, Callable, Union
import asyncio
import datetime
import croniter
import json
from dataclasses import dataclass
import uuid
import logging
from enum import Enum
import traceback
import signal

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class TaskDefinition:
    name: str
    cron: str
    func: Callable
    args: tuple = ()
    kwargs: Dict[str, Any] = None
    timeout: Optional[int] = None
    retry_count: int = 0
    retry_delay: int = 60
    tags: List[str] = None

@dataclass
class TaskInstance:
    id: str
    definition: TaskDefinition
    status: TaskStatus
    created_at: datetime.datetime
    started_at: Optional[datetime.datetime] = None
    completed_at: Optional[datetime.datetime] = None
    result: Any = None
    error: Optional[str] = None
    retry_count: int = 0

class TaskScheduler:
    def __init__(self):
        self.tasks: Dict[str, TaskDefinition] = {}
        self.instances: Dict[str, TaskInstance] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        self._load_persistent_tasks()
    
    def schedule(self, task: TaskDefinition):
        """Schedule a new task"""
        self.tasks[task.name] = task
        self._save_task(task)
    
    def unschedule(self, name: str):
        """Unschedule a task"""
        if name in self.tasks:
            del self.tasks[name]
            self._remove_task(name)
    
    async def start(self):
        """Start task scheduler"""
        self.running = True
        await self._schedule_loop()
    
    async def stop(self):
        """Stop task scheduler"""
        self.running = False
        # Wait for running tasks to complete
        running_tasks = [
            instance for instance in self.instances.values()
            if instance.status == TaskStatus.RUNNING
        ]
        if running_tasks:
            await asyncio.gather(
                *(self._wait_for_task(instance) for instance in running_tasks)
            )
    
    async def _schedule_loop(self):
        """Main scheduling loop"""
        while self.running:
            now = datetime.datetime.now()
            
            for task in self.tasks.values():
                if self._should_run_task(task, now):
                    await self._create_and_run_task(task)
            
            # Sleep until next minute
            next_minute = (now + datetime.timedelta(minutes=1)
                         ).replace(second=0, microsecond=0)
            await asyncio.sleep(
                (next_minute - datetime.datetime.now()).total_seconds()
            )
    
    def _should_run_task(self, 
                        task: TaskDefinition, 
                        now: datetime.datetime) -> bool:
        """Check if task should run"""
        cron = croniter.croniter(task.cron, now)
        previous_run = cron.get_prev(datetime.datetime)
        next_run = cron.get_next(datetime.datetime)
        
        return previous_run <= now < next_run
    
    async def _create_and_run_task(self, task: TaskDefinition):
        """Create and run task instance"""
        instance = TaskInstance(
            id=str(uuid.uuid4()),
            definition=task,
            status=TaskStatus.PENDING,
            created_at=datetime.datetime.now()
        )
        
        self.instances[instance.id] = instance
        asyncio.create_task(self._run_task(instance))
    
    async def _run_task(self, instance: TaskInstance):
        """Run task instance"""
        instance.status = TaskStatus.RUNNING
        instance.started_at = datetime.datetime.now()
        
        try:
            if instance.definition.timeout:
                async with asyncio.timeout(instance.definition.timeout):
                    result = await self._execute_task(instance)
            else:
                result = await self._execute_task(instance)
            
            instance.status = TaskStatus.COMPLETED
            instance.result = result
        
        except Exception as e:
            instance.status = TaskStatus.FAILED
            instance.error = str(e)
            self.logger.error(
                f"Task {instance.definition.name} failed: {e}\n"
                f"{traceback.format_exc()}"
            )
            
            # Handle retry
            if (instance.retry_count < instance.definition.retry_count):
                instance.retry_count += 1
                await asyncio.sleep(instance.definition.retry_delay)
                await self._run_task(instance)
        
        finally:
            instance.completed_at = datetime.datetime.now()
            self._save_instance(instance)
    
    async def _execute_task(self, instance: TaskInstance) -> Any:
        """Execute task function"""
        func = instance.definition.func
        args = instance.definition.args or ()
        kwargs = instance.definition.kwargs or {}
        
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            return await asyncio.to_thread(func, *args, **kwargs)
    
    async def _wait_for_task(self, instance: TaskInstance):
        """Wait for task to complete"""
        while instance.status == TaskStatus.RUNNING:
            await asyncio.sleep(1)
    
    def _load_persistent_tasks(self):
        """Load tasks from persistent storage"""
        try:
            with open('tasks.json', 'r') as f:
                data = json.load(f)
                for task_data in data:
                    self.tasks[task_data['name']] = TaskDefinition(**task_data)
        except FileNotFoundError:
            pass
    
    def _save_task(self, task: TaskDefinition):
        """Save task to persistent storage"""
        try:
            tasks_data = []
            if os.path.exists('tasks.json'):
                with open('tasks.json', 'r') as f:
                    tasks_data = json.load(f)
            
            tasks_data.append({
                'name': task.name,
                'cron': task.cron,
                'timeout': task.timeout,
                'retry_count': task.retry_count,
                'retry_delay': task.retry_delay,
                'tags': task.tags
            })
            
            with open('tasks.json', 'w') as f:
                json.dump(tasks_data, f)
        except Exception as e:
            self.logger.error(f"Error saving task: {e}")
    
    def _remove_task(self, name: str):
        """Remove task from persistent storage"""
        try:
            if os.path.exists('tasks.json'):
                with open('tasks.json', 'r') as f:
                    tasks_data = json.load(f)
                
                tasks_data = [
                    task for task in tasks_data
                    if task['name'] != name
                ]
                
                with open('tasks.json', 'w') as f:
                    json.dump(tasks_data, f)
        except Exception as e:
            self.logger.error(f"Error removing task: {e}")
    
    def _save_instance(self, instance: TaskInstance):
        """Save task instance result"""
        try:
            with open('task_history.json', 'a') as f:
                data = {
                    'id': instance.id,
                    'name': instance.definition.name,
                    'status': instance.status.value,
                    'created_at': instance.created_at.isoformat(),
                    'started_at': instance.started_at.isoformat() 
                        if instance.started_at else None,
                    'completed_at': instance.completed_at.isoformat() 
                        if instance.completed_at else None,
                    'result': instance.result,
                    'error': instance.error,
                    'retry_count': instance.retry_count
                }
                f.write(json.dumps(data) + '\n')
        except Exception as e:
            self.logger.error(f"Error saving task instance: {e}")

class TaskManager:
    def __init__(self, scheduler: TaskScheduler):
        self.scheduler = scheduler
    
    def add_task(self,
                 name: str,
                 func: Callable,
                 cron: str,
                 **kwargs):
        """Add new task"""
        task = TaskDefinition(
            name=name,
            func=func,
            cron=cron,
            **kwargs
        )
        self.scheduler.schedule(task)
    
    def remove_task(self, name: str):
        """Remove task"""
        self.scheduler.unschedule(name)
    
    def get_task_status(self, 
                       task_id: str) -> Optional[TaskInstance]:
        """Get task instance status"""
        return self.scheduler.instances.get(task_id)
    
    def get_task_history(self, 
                        name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get task execution history"""
        history = []
        try:
            with open('task_history.json', 'r') as f:
                for line in f:
                    data = json.loads(line)
                    if not name or data['name'] == name:
                        history.append(data)
        except FileNotFoundError:
            pass
        
        return history
    
    async def cancel_task(self, task_id: str):
        """Cancel running task"""
        instance = self.scheduler.instances.get(task_id)
        if instance and instance.status == TaskStatus.RUNNING:
            # Send cancellation signal
            instance.status = TaskStatus.CANCELLED
            # Wait for task to complete
            await self.scheduler._wait_for_task(instance)
