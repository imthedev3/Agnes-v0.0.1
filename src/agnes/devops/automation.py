from typing import Dict, Any, List, Optional, Callable
import asyncio
import yaml
import docker
import kubernetes
from kubernetes import client, config
import paramiko
import ansible_runner
from datetime import datetime
import jinja2
from dataclasses import dataclass
import subprocess
import logging

@dataclass
class DeploymentConfig:
    name: str
    image: str
    version: str
    replicas: int
    resources: Dict[str, Any]
    env_vars: Dict[str, str]
    volumes: List[Dict[str, Any]]

class AutomationManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.docker_client = docker.from_env()
        self.setup_kubernetes()
        self.logger = logging.getLogger(__name__)
        
    def setup_kubernetes(self):
        """Setup Kubernetes client"""
        try:
            config.load_incluster_config()
        except kubernetes.config.ConfigException:
            config.load_kube_config()
        
        self.k8s_apps = client.AppsV1Api()
        self.k8s_core = client.CoreV1Api()
    
    async def deploy_service(self, deployment: DeploymentConfig):
        """Deploy service to Kubernetes"""
        # Create deployment
        deployment_manifest = self._create_deployment_manifest(deployment)
        
        try:
            await self.k8s_apps.create_namespaced_deployment(
                namespace=self.config['namespace'],
                body=deployment_manifest
            )
        except kubernetes.client.rest.ApiException as e:
            if e.status == 409:  # Already exists
                await self.k8s_apps.patch_namespaced_deployment(
                    name=deployment.name,
                    namespace=self.config['namespace'],
                    body=deployment_manifest
                )
            else:
                raise
    
    def _create_deployment_manifest(self, 
                                  deployment: DeploymentConfig) -> Dict[str, Any]:
        """Create Kubernetes deployment manifest"""
        return {
            'apiVersion': 'apps/v1',
            'kind': 'Deployment',
            'metadata': {
                'name': deployment.name,
                'labels': {
                    'app': deployment.name
                }
            },
            'spec': {
                'replicas': deployment.replicas,
                'selector': {
                    'matchLabels': {
                        'app': deployment.name
                    }
                },
                'template': {
                    'metadata': {
                        'labels': {
                            'app': deployment.name
                        }
                    },
                    'spec': {
                        'containers': [{
                            'name': deployment.name,
                            'image': f"{deployment.image}:{deployment.version}",
                            'env': [
                                {
                                    'name': k,
                                    'value': v
                                } for k, v in deployment.env_vars.items()
                            ],
                            'resources': deployment.resources,
                            'volumeMounts': deployment.volumes
                        }]
                    }
                }
            }
        }

class AnsibleAutomation:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.inventory_path = config['inventory_path']
        self.playbook_path = config['playbook_path']
    
    async def run_playbook(self, 
                          playbook: str, 
                          extra_vars: Optional[Dict[str, Any]] = None):
        """Run Ansible playbook"""
        result = await asyncio.to_thread(
            ansible_runner.run,
            playbook=playbook,
            inventory=self.inventory_path,
            extravars=extra_vars or {}
        )
        
        if result.status != 'successful':
            raise Exception(f"Playbook execution failed: {result.stderr}")
        
        return result.stats

class InfrastructureManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.terraform_path = config['terraform_path']
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader('templates')
        )
    
    async def apply_infrastructure(self, 
                                 template: str, 
                                 variables: Dict[str, Any]):
        """Apply infrastructure changes"""
        # Generate Terraform configuration
        tf_config = self._generate_terraform_config(template, variables)
        
        # Write configuration
        with open(f"{self.terraform_path}/main.tf", 'w') as f:
            f.write(tf_config)
        
        # Initialize and apply
        await self._run_terraform_command('init')
        await self._run_terraform_command('apply', '-auto-approve')
    
    def _generate_terraform_config(self, 
                                 template: str, 
                                 variables: Dict[str, Any]) -> str:
        """Generate Terraform configuration from template"""
        template = self.template_env.get_template(template)
        return template.render(**variables)
    
    async def _run_terraform_command(self, *args):
        """Run Terraform command"""
        process = await asyncio.create_subprocess_exec(
