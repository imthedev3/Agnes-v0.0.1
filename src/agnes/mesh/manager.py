from typing import Dict, Any, List, Optional
import yaml
import kubernetes
from kubernetes import client, config
from kubernetes.client.rest import ApiException

class ServiceMeshManager:
    def __init__(self, kubeconfig_path: Optional[str] = None):
        if kubeconfig_path:
            config.load_kube_config(kubeconfig_path)
        else:
            config.load_incluster_config()
        
        self.v1 = client.CoreV1Api()
        self.custom_objects = client.CustomObjectsApi()
    
    async def apply_mesh_config(self, config_path: str):
        """Apply service mesh configuration"""
        with open(config_path) as f:
            mesh_config = yaml.safe_load_all(f)
            
            for resource in mesh_config:
                try:
                    await self._apply_resource(resource)
                except ApiException as e:
                    print(f"Error applying resource: {e}")
    
    async def _apply_resource(self, resource: Dict[str, Any]):
        """Apply Kubernetes resource"""
        group = "networking.istio.io"
        version = "v1alpha3"
        namespace = resource.get("metadata", {}).get("namespace", "default")
        
        try:
            await self.custom_objects.create_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=resource["kind"].lower() + "s",
                body=resource
            )
        except ApiException as e:
            if e.status == 409:  # Already exists
                await self.custom_objects.patch_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=resource["kind"].lower() + "s",
                    name=resource["metadata"]["name"],
                    body=resource
                )
    
    async def get_mesh_status(self) -> Dict[str, Any]:
        """Get service mesh status"""
        try:
            # Get Istio pods
            istio_pods = self.v1.list_namespaced_pod(
                namespace="istio-system",
                label_selector="app=istiod"
            )
            
            # Get virtual services
            virtual_services = self.custom_objects.list_cluster_custom_object(
                group="networking.istio.io",
                version="v1alpha3",
                plural="virtualservices"
            )
            
            return {
                "istiod_status": self._get_pods_status(istio_pods.items),
                "virtual_services": len(virtual_services["items"]),
                "mesh_health": self._check_mesh_health(istio_pods.items)
            }
        
        except ApiException as e:
            print(f"Error getting mesh status: {e}")
            return {"error": str(e)}
    
    def _get_pods_status(self, pods: List[Any]) -> Dict[str, int]:
        """Get pod status counts"""
        status_count = {
            "Running": 0,
            "Pending": 0,
            "Failed": 0
        }
        
        for pod in pods:
            status = pod.status.phase
            status_count[status] = status_count.get(status, 0) + 1
        
        return status_count
    
    def _check_mesh_health(self, pods: List[Any]) -> str:
        """Check overall mesh health"""
        running_pods = sum(1 for pod in pods 
                         if pod.status.phase == "Running")
        
        if running_pods == 0:
            return "Critical"
        elif running_pods < len(pods):
            return "Degraded"
        return "Healthy"
