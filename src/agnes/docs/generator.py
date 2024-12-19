from typing import Dict, Any, List, Optional, Union, Type
import inspect
import yaml
import json
import re
from dataclasses import dataclass, field
import markdown2
import jinja2
import asyncio
from enum import Enum
import typing
from docstring_parser import parse as parse_docstring

@dataclass
class EndpointParameter:
    name: str
    type: str
    description: str
    required: bool = True
    default: Any = None
    enum: Optional[List[str]] = None
    example: Any = None

@dataclass
class EndpointResponse:
    status_code: int
    description: str
    schema: Dict[str, Any]
    examples: Optional[List[Dict[str, Any]]] = None

@dataclass
class EndpointDoc:
    path: str
    method: str
    summary: str
    description: str
    tags: List[str]
    parameters: List[EndpointParameter]
    request_body: Optional[Dict[str, Any]] = None
    responses: Dict[int, EndpointResponse] = field(default_factory=dict)
    security: Optional[List[str]] = None
    deprecated: bool = False

class OpenAPIGenerator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.endpoints: List[EndpointDoc] = []
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader('templates')
        )
    
    def generate_from_router(self, router: Any):
        """Generate API documentation from router"""
        for route in router.routes:
            endpoint_doc = self._parse_route(route)
            if endpoint_doc:
                self.endpoints.append(endpoint_doc)
    
    def _parse_route(self, route: Any) -> Optional[EndpointDoc]:
        """Parse route information"""
        handler = route.handler
        if not hasattr(handler, "__doc__"):
            return None
        
        docstring = parse_docstring(handler.__doc__)
        signature = inspect.signature(handler)
        
        # Parse parameters
        parameters = []
        for name, param in signature.parameters.items():
            if name == 'self' or name == 'request':
                continue
            
            param_doc = next(
                (p for p in docstring.params if p.arg_name == name),
                None
            )
            
            parameters.append(EndpointParameter(
                name=name,
                type=self._get_type_hint(param.annotation),
                description=param_doc.description if param_doc else "",
                required=param.default == inspect.Parameter.empty,
                default=param.default if param.default != inspect.Parameter.empty else None
            ))
        
        # Parse responses
        responses = {}
        for line in docstring.returns.description.split('\n'):
            match = re.match(r'(\d{3}):\s*(.*)', line)
            if match:
                status_code = int(match.group(1))
                description = match.group(2)
                responses[status_code] = EndpointResponse(
                    status_code=status_code,
                    description=description,
                    schema=self._get_return_schema(handler)
                )
        
        return EndpointDoc(
            path=route.path,
            method=route.method.upper(),
            summary=docstring.short_description,
            description=docstring.long_description or "",
            tags=self._get_tags(route),
            parameters=parameters,
            responses=responses
        )
    
    def _get_type_hint(self, annotation: Any) -> str:
        """Get type hint string representation"""
        if annotation == inspect.Parameter.empty:
            return "any"
        
        if hasattr(annotation, "__origin__"):
            if annotation.__origin__ == Union:
                types = [self._get_type_hint(arg) for arg in annotation.__args__]
                return " | ".join(types)
            elif annotation.__origin__ == List:
                return f"array[{self._get_type_hint(annotation.__args__[0])}]"
            elif annotation.__origin__ == Dict:
                return "object"
        
        if isinstance(annotation, type):
            if issubclass(annotation, Enum):
                return "enum"
            return annotation.__name__.lower()
        
        return "any"
    
    def _get_return_schema(self, handler: Any) -> Dict[str, Any]:
        """Get return value schema"""
        return_annotation = inspect.signature(handler).return_annotation
        if return_annotation == inspect.Parameter.empty:
            return {}
        
        if hasattr(return_annotation, "__annotations__"):
            return {
                name: self._get_type_hint(hint)
                for name, hint in return_annotation.__annotations__.items()
            }
        
        return {"type": self._get_type_hint(return_annotation)}
    
    def _get_tags(self, route: Any) -> List[str]:
        """Get endpoint tags"""
        tags = []
        if hasattr(route, "tags"):
            tags.extend(route.tags)
        return tags or ["default"]
    
    def generate_openapi_spec(self) -> Dict[str, Any]:
        """Generate OpenAPI specification"""
        return {
            "openapi": "3.0.0",
            "info": {
                "title": self.config.get("title", "API Documentation"),
                "version": self.config.get("version", "1.0.0"),
                "description": self.config.get("description", "")
            },
            "servers": [
                {
                    "url": server
                } for server in self.config.get("servers", [])
            ],
            "paths": self._generate_paths(),
            "components": {
                "schemas": self._generate_schemas(),
                "securitySchemes": self.config.get("security_schemes", {})
            }
        }
    
    def _generate_paths(self) -> Dict[str, Any]:
        """Generate paths documentation"""
        paths = {}
        for endpoint in self.endpoints:
            if endpoint.path not in paths:
                paths[endpoint.path] = {}
            
            paths[endpoint.path][endpoint.method.lower()] = {
                "summary": endpoint.summary,
                "description": endpoint.description,
                "tags": endpoint.tags,
                "parameters": [
                    {
                        "name": param.name,
                        "in": "query",  # Simplified, should detect from route
                        "description": param.description,
                        "required": param.required,
                        "schema": {
                            "type": param.type,
                            "default": param.default,
                            "enum": param.enum
                        }
                    } for param in endpoint.parameters
                ],
                "responses": {
                    str(code): {
                        "description": response.description,
                        "content": {
                            "application/json": {
                                "schema": response.schema
                            }
                        }
                    }
                    for code, response in endpoint.responses.items()
                }
            }
        
        return paths
    
    def _generate_schemas(self) -> Dict[str, Any]:
        """Generate component schemas"""
        schemas = {}
        for endpoint in self.endpoints:
            if endpoint.request_body:
                self._add_schema_from_dict(
                    schemas,
                    f"{endpoint.path.replace('/', '_')}Request",
                    endpoint.request_body
                )
            
            for response in endpoint.responses.values():
                self._add_schema_from_dict(
                    schemas,
                    f"{endpoint.path.replace('/', '_')}Response",
                    response.schema
                )
        
        return schemas
    
    def _add_schema_from_dict(self, 
                             schemas: Dict[str, Any],
                             name: str,
                             schema: Dict[str, Any]):
        """Add schema to components"""
        if not schema:
            return
        
        schemas[name] = {
            "type": "object",
            "properties": {
                key: {"type": value} if isinstance(value, str)
                else value
                for key, value in schema.items()
            }
        }
    
    def generate_html(self) -> str:
        """Generate HTML documentation"""
        template = self.template_env.get_template('api_docs.html.j2')
        return template.render(
            spec=self.generate_openapi_spec(),
            config=self.config
        )
    
    def save_documentation(self):
        """Save API documentation"""
        # Save OpenAPI spec
        with open('api_spec.yaml', 'w') as f:
            yaml.dump(self.generate_openapi_spec(), f)
        
        # Save HTML docs
        with open('api_docs.html', 'w') as f:
            f.write(self.generate_html())

class MarkdownGenerator:
    def __init__(self, openapi_generator: OpenAPIGenerator):
        self.generator = openapi_generator
    
    def generate_markdown(self) -> str:
        """Generate Markdown documentation"""
        docs = []
        
        # Add header
        spec = self.generator.generate_openapi_spec()
        docs.append(f"# {spec['info']['title']}")
        docs.append(f"Version: {spec['info']['version']}\n")
        docs.append(spec['info']['description'] + "\n")
        
        # Add endpoints
        for endpoint in self.generator.endpoints:
            docs.append(self._generate_endpoint_doc(endpoint))
        
        return "\n".join(docs)
    
    def _generate_endpoint_doc(self, endpoint: EndpointDoc) -> str:
        """Generate endpoint documentation"""
        docs = []
        
        # Endpoint header
        docs.append(f"## {endpoint.method} {endpoint.path}")
        docs.append(f"{endpoint.description}\n")
        
        # Parameters
        if endpoint.parameters:
            docs.append("### Parameters")
            docs.append("| Name | Type | Required | Description |")
            docs.append("|------|------|----------|-------------|")
            for param in endpoint.parameters:
                docs.append(
                    f"| {param.name} | {param.type} | "
                    f"{'Yes' if param.required else 'No'} | "
                    f"{param.description} |"
                )
            docs.append("")
        
        # Request body
        if endpoint.request_body:
            docs.append("### Request Body")
            docs.append("```json")
            docs.append(json.dumps(endpoint.request_body, indent=2))
            docs.append("```\n")
        
        # Responses
        docs.append("### Responses")
        for code, response in endpoint.responses.items():
            docs.append(f"#### {code}: {response.description}")
            if response.schema:
                docs.append("```json")
                docs.append(json.dumps(response.schema, indent=2))
                docs.append("```")
            if response.examples:
                docs.append("\nExample:")
                docs.append("```json")
                docs.append(json.dumps(response.examples[0], indent=2))
                docs.append("```")
            docs.append("")
        
        return "\n".join(docs)

class DocumentationServer:
    def __init__(self, 
                 config: Dict[str, Any],
                 openapi_generator: OpenAPIGenerator):
        self.config = config
        self.generator = openapi_generator
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader('templates')
        )
    
    async def serve_docs(self):
        """Serve API documentation"""
        from aiohttp import web
        
        app = web.Application()
        app.router.add_get("/", self.handle_index)
        app.router.add_get("/spec", self.handle_spec)
        app.router.add_get("/markdown", self.handle_markdown)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(
            runner,
            self.config.get("host", "localhost"),
            self.config.get("port", 8080)
        )
        await site.start()
    
    async def handle_index(self, request):
        """Handle index page request"""
        return web.Response(
            text=self.generator.generate_html(),
            content_type="text/html"
        )
    
    async def handle_spec(self, request):
        """Handle OpenAPI spec request"""
        return web.json_response(
            self.generator.generate_openapi_spec()
        )
    
    async def handle_markdown(self, request):
        """Handle Markdown docs request"""
        markdown_generator = MarkdownGenerator(self.generator)
        return web.Response(
            text=markdown_generator.generate_markdown(),
            content_type="text/markdown"
        )
