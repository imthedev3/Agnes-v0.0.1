from typing import Dict, Any, List, Optional, Union, Callable
import asyncio
import yaml
import docker
import kubernetes
from kubernetes import client, config
from datetime import datetime
import logging
from dataclasses import dataclass
from enum import Enum
import aioboto3
import aiofiles
import tempfile
import os
import shutil
import git
import jinja2
import hashlib

class DeploymentType(Enum):
    DOCKER = "docker"
    KUBERNETES = "kubernetes"
    SERVERLESS = "serverless"

class DeploymentStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"

@dataclass
class Deployment:
    id: str
    name: str
    type: DeploymentType
    environment: str
    version: str
    config: Dict[str, Any]
    status: DeploymentStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = None
    logs: List[str] = None

class DeploymentManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.docker = docker.from_env()
        self.session = aioboto3.Session()
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader('templates/deploy')
        )
        self.logger = logging.getLogger(__name__)
        
        # Setup Kubernetes if enabled
        if self.config.get('kubernetes', {}).get('enabled', False):
            if 'kubeconfig' in self.config['kubernetes']:
                config.load_kube_config(
                    self.config['kubernetes']['kubeconfig']
                )
            else:
                config.load_incluster_config()
            self.k8s = client.ApiClient()
    
    async def deploy(self,
                    name: str,
                    type: DeploymentType,
                    environment: str,
                    version: str,
                    config: Dict[str, Any]) -> Deployment:
        """Start new deployment"""
        deployment = Deployment(
            id=hashlib.sha256(
                f"{name}:{environment}:{version}".encode()
            ).hexdigest()[:12],
            name=name,
            type=type,
            environment=environment,
            version=version,
            config=config,
            status=DeploymentStatus.PENDING,
            created_at=datetime.utcnow(),
            logs=[]
        )
        
        try:
            if type == DeploymentType.DOCKER:
                await self._deploy_docker(deployment)
            elif type == DeploymentType.KUBERNETES:
                await self._deploy_kubernetes(deployment)
            elif type == DeploymentType.SERVERLESS:
                await self._deploy_serverless(deployment)
            
            deployment.status = DeploymentStatus.COMPLETED
            deployment.completed_at = datetime.utcnow()
        
        except Exception as e:
            deployment.status = DeploymentStatus.FAILED
            deployment.error = str(e)
            deployment.completed_at = datetime.utcnow()
            self.logger.error(f"Deployment failed: {e}")
            
            # Attempt rollback
            try:
                await self.rollback(deployment)
            except Exception as e:
                self.logger.error(f"Rollback failed: {e}")
        
        return deployment
    
    async def _deploy_docker(self, deployment: Deployment):
        """Deploy Docker container"""
        deployment.started_at = datetime.utcnow()
        deployment.status = DeploymentStatus.RUNNING
        
        # Pull image
        self.logger.info(f"Pulling image {deployment.config['image']}")
        self.docker.images.pull(
            deployment.config['image'],
            tag=deployment.version
        )
        
        # Prepare container config
        container_config = {
            'image': f"{deployment.config['image']}:{deployment.version}",
            'name': f"{deployment.name}-{deployment.environment}",
            'detach': True,
            'environment': deployment.config.get('env', {}),
            'ports': deployment.config.get('ports', {}),
            'volumes': deployment.config.get('volumes', {}),
            'network': deployment.config.get('network'),
            'restart_policy': {'Name': 'always'}
        }
        
        # Stop and remove existing container
        try:
            container = self.docker.containers.get(container_config['name'])
            container.stop()
            container.remove()
        except docker.errors.NotFound:
            pass
        
        # Create and start new container
        self.logger.info(f"Starting container {container_config['name']}")
        container = self.docker.containers.run(**container_config)
        
        # Wait for container to be healthy
        for _ in range(30):
            container.reload()
            if container.status == 'running':
                return
            await asyncio.sleep(1)
        
        raise RuntimeError("Container failed to start")
    
    async def _deploy_kubernetes(self, deployment: Deployment):
        """Deploy to Kubernetes"""
        deployment.started_at = datetime.utcnow()
        deployment.status = DeploymentStatus.RUNNING
        
        # Prepare Kubernetes manifests
        manifests = []
        for template_name in deployment.config['templates']:
            template = self.template_env.get_template(
                f"kubernetes/{template_name}"
            )
            manifest = yaml.safe_load(template.render(
                name=deployment.name,
                environment=deployment.environment,
                version=deployment.version,
                config=deployment.config
            ))
            manifests.append(manifest)
        
        # Apply manifests
        for manifest in manifests:
            if manifest['kind'] == 'Deployment':
                api = client.AppsV1Api(self.k8s)
                try:
                    api.patch_namespaced_deployment(
                        name=manifest['metadata']['name'],
                        namespace=deployment.environment,
                        body=manifest
                    )
                except kubernetes.client.rest.ApiException as e:
                    if e.status == 404:
                        api.create_namespaced_deployment(
                            namespace=deployment.environment,
                            body=manifest
                        )
                    else:
                        raise
            
            elif manifest['kind'] == 'Service':
                api = client.CoreV1Api(self.k8s)
                try:
                    api.patch_namespaced_service(
                        name=manifest['metadata']['name'],
                        namespace=deployment.environment,
                        body=manifest
                    )
                except kubernetes.client.rest.ApiException as e:
                    if e.status == 404:
                        api.create_namespaced_service(
                            namespace=deployment.environment,
                            body=manifest
                        )
                    else:
                        raise
        
        # Wait for deployment to be ready
        api = client.AppsV1Api(self.k8s)
        for _ in range(60):
            deployment_status = api.read_namespaced_deployment_status(
                name=f"{deployment.name}-{deployment.environment}",
                namespace=deployment.environment
            )
            
            if (deployment_status.status.ready_replicas or 0) == \
               deployment_status.status.replicas:
                return
            
            await asyncio.sleep(5)
        
        raise RuntimeError("Deployment failed to become ready")
    
    async def _deploy_serverless(self, deployment: Deployment):
        """Deploy serverless functions"""
        deployment.started_at = datetime.utcnow()
        deployment.status = DeploymentStatus.RUNNING
        
        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Clone repository
            repo = git.Repo.clone_from(
                deployment.config['repository'],
                temp_dir,
                branch=deployment.version
            )
            
            # Prepare serverless config
            template = self.template_env.get_template(
                'serverless/serverless.yml'
            )
            serverless_config = template.render(
                name=deployment.name,
                environment=deployment.environment,
                version=deployment.version,
                config=deployment.config
            )
            
            async with aiofiles.open(
                os.path.join(temp_dir, 'serverless.yml'),
                'w'
            ) as f:
                await f.write(serverless_config)
            
            # Deploy using Serverless Framework
            process = await asyncio.create_subprocess_exec(
                'serverless',
                'deploy',
                '--stage', deployment.environment,
                cwd=temp_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                raise RuntimeError(
                    f"Serverless deployment failed: {stderr.decode()}"
                )
            
            deployment.logs.append(stdout.decode())
    
    async def rollback(self, deployment: Deployment):
        """Rollback failed deployment"""
        self.logger.info(f"Rolling back deployment {deployment.id}")
        
        try:
            if deployment.type == DeploymentType.DOCKER:
                await self._rollback_docker(deployment)
            elif deployment.type == DeploymentType.KUBERNETES:
                await self._rollback_kubernetes(deployment)
            elif deployment.type == DeploymentType.SERVERLESS:
                await self._rollback_serverless(deployment)
            
            deployment.status = DeploymentStatus.ROLLED_BACK
        
        except Exception as e:
            self.logger.error(f"Rollback failed: {e}")
            raise
    
    async def _rollback_docker(self, deployment: Deployment):
        """Rollback Docker deployment"""
        # Get previous version
        previous_version = await self._get_previous_version(
            deployment.name,
            deployment.environment
        )
        
        if not previous_version:
            raise ValueError("No previous version found")
        
        # Deploy previous version
        await self.deploy(
            deployment.name,
            DeploymentType.DOCKER,
            deployment.environment,
            previous_version,
            deployment.config
        )
    
    async def _rollback_kubernetes(self, deployment: Deployment):
        """Rollback Kubernetes deployment"""
        api = client.AppsV1Api(self.k8s)
        
        # Rollback deployment
        api.create_namespaced_deployment_rollback(
            name=f"{deployment.name}-{deployment.environment}",
            namespace=deployment.environment,
            body={
                'name': f"{deployment.name}-{deployment.environment}",
                'rollbackTo': {'revision': 0}
            }
        )
    
    async def _rollback_serverless(self, deployment: Deployment):
        """Rollback serverless deployment"""
        # Get previous version
        previous_version = await self._get_previous_version(
            deployment.name,
            deployment.environment
        )
        
        if not previous_version:
            raise ValueError("No previous version found")
        
        # Deploy previous version
        await self.deploy(
            deployment.name,
            DeploymentType.SERVERLESS,
            deployment.environment,
            previous_version,
            deployment.config
        )
    
    async def _get_previous_version(self,
                                  name: str,
                                  environment: str) -> Optional[str]:
        """Get previous deployed version"""
        # Implementation depends on version tracking system
        # Could be stored in database, S3, etc.
        pass

class DeploymentMonitor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    async def check_health(self,
                          deployment: Deployment) -> bool:
        """Check deployment health"""
        if deployment.type == DeploymentType.DOCKER:
            return await self._check_docker_health(deployment)
        elif deployment.type == DeploymentType.KUBERNETES:
            return await self._check_kubernetes_health(deployment)
        elif deployment.type == DeploymentType.SERVERLESS:
            return await self._check_serverless_health(deployment)
    
    async def collect_metrics(self,
                            deployment: Deployment) -> Dict[str, Any]:
        """Collect deployment metrics"""
        if deployment.type == DeploymentType.DOCKER:
            return await self._collect_docker_metrics(deployment)
        elif deployment.type == DeploymentType.KUBERNETES:
            return await self._collect_kubernetes_metrics(deployment)
        elif deployment.type == DeploymentType.SERVERLESS:
            return await self._collect_serverless_metrics(deployment)
