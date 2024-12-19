from typing import Dict, Any, List, Optional, Union, Callable
import asyncio
from datetime import datetime
import json
import yaml
from dataclasses import dataclass
import logging
from enum import Enum
import uuid
import networkx as nx

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

class WorkflowStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class TaskDefinition:
    name: str
    handler: str
    inputs: Dict[str, Any]
    outputs: List[str]
    retry_count: int = 0
    timeout: Optional[int] = None
    dependencies: List[str] = None

@dataclass
class TaskInstance:
    id: str
    definition: TaskDefinition
    status: TaskStatus
    workflow_id: str
    inputs: Dict[str, Any]
    outputs: Dict[str, Any]
    error: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    retry_count: int = 0

@dataclass
class WorkflowDefinition:
    name: str
    tasks: List[TaskDefinition]
    timeout: Optional[int] = None
    on_failure: str = "fail"  # fail, continue, retry
    max_retries: int = 0
    variables: Dict[str, Any] = None

@dataclass
class WorkflowInstance:
    id: str
    definition: WorkflowDefinition
    status: WorkflowStatus
    tasks: Dict[str, TaskInstance]
    variables: Dict[str, Any]
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error: Optional[str] = None

class WorkflowEngine:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.workflows: Dict[str, WorkflowInstance] = {}
        self.task_handlers: Dict[str, Callable] = {}
        self.logger = logging.getLogger(__name__)
    
    def register_handler(self, name: str, handler: Callable):
        """Register task handler"""
        self.task_handlers[name] = handler
    
    async def create_workflow(self,
                            definition: WorkflowDefinition,
                            variables: Dict[str, Any] = None) -> WorkflowInstance:
        """Create new workflow instance"""
        workflow = WorkflowInstance(
            id=str(uuid.uuid4()),
            definition=definition,
            status=WorkflowStatus.PENDING,
            tasks={},
            variables=variables or {}
        )
        
        # Create task instances
        for task_def in definition.tasks:
            task = TaskInstance(
                id=str(uuid.uuid4()),
                definition=task_def,
                status=TaskStatus.PENDING,
                workflow_id=workflow.id,
                inputs={},
                outputs={}
            )
            workflow.tasks[task_def.name] = task
        
        self.workflows[workflow.id] = workflow
        return workflow
    
    async def start_workflow(self, workflow_id: str):
        """Start workflow execution"""
        workflow = self.workflows.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow {workflow_id} not found")
        
        workflow.status = WorkflowStatus.RUNNING
        workflow.start_time = datetime.utcnow()
        
        try:
            # Build dependency graph
            graph = self._build_dependency_graph(workflow)
            
            # Execute tasks in order
            for task_names in nx.topological_generations(graph):
                # Execute tasks in current level in parallel
                tasks = [
                    self._execute_task(workflow.tasks[name])
                    for name in task_names
                ]
                await asyncio.gather(*tasks)
                
                # Check workflow status
                if workflow.status != WorkflowStatus.RUNNING:
                    break
            
            if workflow.status == WorkflowStatus.RUNNING:
                workflow.status = WorkflowStatus.COMPLETED
        
        except Exception as e:
            workflow.status = WorkflowStatus.FAILED
            workflow.error = str(e)
            self.logger.error(f"Workflow {workflow_id} failed: {e}")
        
        finally:
            workflow.end_time = datetime.utcnow()
    
    def _build_dependency_graph(self, workflow: WorkflowInstance) -> nx.DiGraph:
        """Build task dependency graph"""
        graph = nx.DiGraph()
        
        # Add all tasks
        for task in workflow.tasks.values():
            graph.add_node(task.definition.name)
        
        # Add dependencies
        for task in workflow.tasks.values():
            if task.definition.dependencies:
                for dep in task.definition.dependencies:
                    graph.add_edge(dep, task.definition.name)
        
        # Check for cycles
        if not nx.is_directed_acyclic_graph(graph):
            raise ValueError("Workflow contains circular dependencies")
        
        return graph
    
    async def _execute_task(self, task: TaskInstance):
        """Execute single task"""
        if task.status == TaskStatus.COMPLETED:
            return
        
        task.status = TaskStatus.RUNNING
        task.start_time = datetime.utcnow()
        
        try:
            # Prepare task inputs
            self._prepare_task_inputs(task)
            
            # Execute task handler
            handler = self.task_handlers.get(task.definition.handler)
            if not handler:
                raise ValueError(
                    f"Handler {task.definition.handler} not found"
                )
            
            if task.definition.timeout:
                async with asyncio.timeout(task.definition.timeout):
                    task.outputs = await handler(task.inputs)
            else:
                task.outputs = await handler(task.inputs)
            
            task.status = TaskStatus.COMPLETED
        
        except Exception as e:
            task.error = str(e)
            if task.retry_count < task.definition.retry_count:
                task.retry_count += 1
                await self._execute_task(task)
            else:
                task.status = TaskStatus.FAILED
                workflow = self.workflows[task.workflow_id]
                if workflow.definition.on_failure == "fail":
                    workflow.status = WorkflowStatus.FAILED
                    workflow.error = str(e)
        
        finally:
            task.end_time = datetime.utcnow()
    
    def _prepare_task_inputs(self, task: TaskInstance):
        """Prepare task input variables"""
        workflow = self.workflows[task.workflow_id]
        
        for name, value in task.definition.inputs.items():
            if isinstance(value, str) and value.startswith("$"):
                # Reference to workflow variable
                var_name = value[1:]
                task.inputs[name] = workflow.variables.get(var_name)
            elif isinstance(value, str) and value.startswith("@"):
                # Reference to another task's output
                parts = value[1:].split(".")
                if len(parts) != 2:
                    raise ValueError(f"Invalid task reference: {value}")
                task_name, output_name = parts
                if task_name not in workflow.tasks:
                    raise ValueError(f"Referenced task not found: {task_name}")
                source_task = workflow.tasks[task_name]
                if source_task.status != TaskStatus.COMPLETED:
                    raise ValueError(
                        f"Referenced task not completed: {task_name}"
                    )
                task.inputs[name] = source_task.outputs.get(output_name)
            else:
                task.inputs[name] = value
    
    async def cancel_workflow(self, workflow_id: str):
        """Cancel workflow execution"""
        workflow = self.workflows.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow {workflow_id} not found")
        
        workflow.status = WorkflowStatus.CANCELLED
        workflow.end_time = datetime.utcnow()
    
    def get_workflow_status(self, workflow_id: str) -> Optional[WorkflowInstance]:
        """Get workflow status"""
        return self.workflows.get(workflow_id)
    
    def get_task_status(self,
                       workflow_id: str,
                       task_name: str) -> Optional[TaskInstance]:
        """Get task status"""
        workflow = self.workflows.get(workflow_id)
        if not workflow:
            return None
        return workflow.tasks.get(task_name)

class WorkflowRegistry:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.definitions: Dict[str, WorkflowDefinition] = {}
    
    def load_from_file(self, path: str):
        """Load workflow definitions from file"""
        with open(path) as f:
            if path.endswith('.yaml') or path.endswith('.yml'):
                data = yaml.safe_load(f)
            else:
                data = json.load(f)
        
        for workflow_data in data['workflows']:
            self.register_workflow(
                self._create_definition(workflow_data)
            )
    
    def _create_definition(self,
                          data: Dict[str, Any]) -> WorkflowDefinition:
        """Create workflow definition from data"""
        tasks = []
        for task_data in data['tasks']:
            task = TaskDefinition(
                name=task_data['name'],
                handler=task_data['handler'],
                inputs=task_data.get('inputs', {}),
                outputs=task_data.get('outputs', []),
                retry_count=task_data.get('retry_count', 0),
                timeout=task_data.get('timeout'),
                dependencies=task_data.get('dependencies', [])
            )
            tasks.append(task)
        
        return WorkflowDefinition(
            name=data['name'],
            tasks=tasks,
            timeout=data.get('timeout'),
            on_failure=data.get('on_failure', 'fail'),
            max_retries=data.get('max_retries', 0),
            variables=data.get('variables', {})
        )
    
    def register_workflow(self, definition: WorkflowDefinition):
        """Register workflow definition"""
        self.definitions[definition.name] = definition
    
    def get_workflow(self, name: str) -> Optional[WorkflowDefinition]:
        """Get workflow definition"""
        return self.definitions.get(name)
