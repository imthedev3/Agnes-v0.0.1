from typing import Dict, Any, List, Optional, Union, Callable
import asyncio
import aiohttp
from aiohttp import web
import json
import yaml
import logging
from datetime import datetime
import jwt
from dataclasses import dataclass
import re
import ratelimit
import cachetools
import prometheus_client
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

@dataclass
class Route:
    id: str
    path: str
    methods: List[str]
    service: str
    endpoint: str
    strip_prefix: bool = True
    rewrite_path: Optional[str] = None
    middleware: List[str] = None
    rate_limit: Optional[Dict[str, Any]] = None
    cache: Optional[Dict[str, Any]] = None
    timeout: int = 30
    retry: Optional[Dict[str, Any]] = None
    circuit_breaker: Optional[Dict[str, Any]] = None
    authentication: Optional[Dict[str, Any]] = None
    cors: Optional[Dict[str, Any]] = None
    transform: Optional[Dict[str, Any]] = None

class APIGateway:
    def __init__(self,
                 config: Dict[str, Any],
                 discovery: ServiceDiscovery):
        self.config = config
        self.discovery = discovery
        self.routes: Dict[str, Route] = {}
        self.middleware: Dict[str, Callable] = {}
        self.rate_limiters: Dict[str, ratelimit.RateLimiter] = {}
        self.cache = cachetools.TTLCache(
            maxsize=1000,
            ttl=300
        )
        self.logger = logging.getLogger(__name__)
        self.tracer = trace.get_tracer(__name__)
        self._setup_metrics()
    
    def _setup_metrics(self):
        """Setup Prometheus metrics"""
        self.request_duration = prometheus_client.Histogram(
            'gateway_request_duration_seconds',
            'Request duration in seconds',
            ['method', 'path', 'status']
        )
        self.request_total = prometheus_client.Counter(
            'gateway_requests_total',
            'Total requests',
            ['method', 'path']
        )
        self.error_total = prometheus_client.Counter(
            'gateway_errors_total',
            'Total errors',
            ['method', 'path', 'error']
        )
    
    async def start(self):
        """Start API gateway"""
        app = web.Application(
            middlewares=[
                self._error_middleware,
                self._auth_middleware,
                self._cors_middleware
            ]
        )
        
        # Setup routes
        app.router.add_route(
            '*', '/{path:.*}',
            self._handle_request
        )
        
        # Setup metrics endpoint
        app.router.add_get(
            '/metrics',
            self._handle_metrics
        )
        
        # Start server
        runner = web.AppRunner(app)
        await runner.setup()
        
        site = web.TCPSite(
            runner,
            self.config['host'],
            self.config['port']
        )
        await site.start()
    
    def add_route(self, route: Route):
        """Add API route"""
        self.routes[route.id] = route
        
        # Setup rate limiter
        if route.rate_limit:
            self.rate_limiters[route.id] = ratelimit.RateLimiter(
                rate=route.rate_limit['rate'],
                period=route.rate_limit['period']
            )
    
    def add_middleware(self,
                      name: str,
                      middleware: Callable):
        """Add middleware"""
        self.middleware[name] = middleware
    
    @web.middleware
    async def _error_middleware(self,
                              request: web.Request,
                              handler: Callable) -> web.Response:
        """Error handling middleware"""
        try:
            return await handler(request)
        except web.HTTPException as e:
            return web.json_response(
                {
                    'error': e.reason
                },
                status=e.status
            )
        except Exception as e:
            self.logger.error(f"Request error: {e}")
            return web.json_response(
                {
                    'error': 'Internal server error'
                },
                status=500
            )
    
    @web.middleware
    async def _auth_middleware(self,
                             request: web.Request,
                             handler: Callable) -> web.Response:
        """Authentication middleware"""
        route = self._get_route(request)
        if not route or not route.authentication:
            return await handler(request)
        
        auth = route.authentication
        auth_type = auth.get('type', 'jwt')
        
        if auth_type == 'jwt':
            token = request.headers.get('Authorization')
            if not token:
                raise web.HTTPUnauthorized(
                    reason='Missing authorization token'
                )
            
            try:
                payload = jwt.decode(
                    token.replace('Bearer ', ''),
                    auth['secret'],
                    algorithms=['HS256']
                )
                request['user'] = payload
            except jwt.InvalidTokenError:
                raise web.HTTPUnauthorized(
                    reason='Invalid token'
                )
        
        return await handler(request)
    
    @web.middleware
    async def _cors_middleware(self,
                             request: web.Request,
                             handler: Callable) -> web.Response:
        """CORS middleware"""
        route = self._get_route(request)
        if not route or not route.cors:
            return await handler(request)
        
        cors = route.cors
        headers = {
            'Access-Control-Allow-Origin': cors.get(
                'allow_origin',
                '*'
            ),
            'Access-Control-Allow-Methods': ','.join(
                cors.get(
                    'allow_methods',
                    ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']
                )
            ),
            'Access-Control-Allow-Headers': ','.join(
                cors.get(
                    'allow_headers',
                    ['Content-Type', 'Authorization']
                )
            ),
            'Access-Control-Max-Age': str(
                cors.get('max_age', 86400)
            )
        }
        
        if request.method == 'OPTIONS':
            return web.Response(headers=headers)
        
        response = await handler(request)
        response.headers.extend(headers)
        return response
    
    def _get_route(self,
                   request: web.Request) -> Optional[Route]:
        """Get matching route for request"""
        path = request.path
        method = request.method
        
        for route in self.routes.values():
            if (
                method in route.methods and
                re.match(route.path, path)
            ):
                return route
        
        return None
    
    async def _handle_request(self,
                            request: web.Request) -> web.Response:
        """Handle API request"""
        route = self._get_route(request)
        if not route:
            raise web.HTTPNotFound()
        
        # Rate limiting
        if route.id in self.rate_limiters:
            limiter = self.rate_limiters[route.id]
            if not limiter.try_acquire():
                raise web.HTTPTooManyRequests()
        
        # Caching
        if route.cache and request.method == 'GET':
            cache_key = f"{route.id}:{request.path}:{request.query_string}"
            cached = self.cache.get(cache_key)
            if cached:
                return web.Response(**cached)
        
        # Start span
        with self.tracer.start_as_current_span(
            f"{request.method} {route.path}"
        ) as span:
            # Inject trace context
            carrier = {}
            TraceContextTextMapPropagator().inject(carrier)
            
            # Prepare request
            service_request = await self._prepare_request(
                request,
                route
            )
            
            # Execute middleware
            if route.middleware:
                for name in route.middleware:
                    middleware = self.middleware.get(name)
                    if middleware:
                        service_request = await middleware(
                            service_request
                        )
            
            # Get service endpoint
            endpoint = await self.discovery.get_endpoint(
                route.service
            )
            if not endpoint:
                raise web.HTTPServiceUnavailable()
            
            # Make request to service
            start_time = datetime.utcnow()
            try:
                service_response = await self._make_request(
                    endpoint,
                    service_request,
                    route
                )
                
                # Transform response
                if route.transform:
                    service_response = await self._transform_response(
                        service_response,
                        route.transform
                    )
                
                # Cache response
                if route.cache and request.method == 'GET':
                    self.cache[cache_key] = {
                        'body': service_response.body,
                        'status': service_response.status,
                        'headers': dict(service_response.headers)
                    }
                
                return service_response
            
            finally:
                duration = (
                    datetime.utcnow() - start_time
                ).total_seconds()
                
                # Record metrics
                self.request_duration.labels(
                    request.method,
                    route.path,
                    service_response.status
                ).observe(duration)
                
                self.request_total.labels(
                    request.method,
                    route.path
                ).inc()
    
    async def _prepare_request(self,
                             request: web.Request,
                             route: Route) -> web.Request:
        """Prepare service request"""
        # Build path
        path = request.path
        if route.strip_prefix:
            path = re.sub(f"^{route.path}", '', path)
        if route.rewrite_path:
            path = route.rewrite_path
        
        # Build headers
        headers = dict(request.headers)
        headers['X-Forwarded-For'] = request.remote
        headers['X-Forwarded-Proto'] = request.scheme
        headers['X-Forwarded-Host'] = request.host
        
        # Build request
        return web.Request(
            method=request.method,
            path=path,
            headers=headers,
            query_string=request.query_string,
            body=await request.read()
        )
    
    async def _make_request(self,
                          endpoint: ServiceEndpoint,
                          request: web.Request,
                          route: Route) -> web.Response:
        """Make request to service"""
        url = f"http://{endpoint.host}:{endpoint.port}{request.path}"
        if request.query_string:
            url = f"{url}?{request.query_string}"
        
        # Setup timeout
        timeout = aiohttp.ClientTimeout(
            total=route.timeout
        )
        
        # Setup retry
        retry_options = route.retry or {}
        max_retries = retry_options.get('max_attempts', 3)
        retry_delay = retry_options.get('delay', 1)
        
        # Make request with retry
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(
                        request.method,
                        url,
                        headers=request.headers,
                        data=request.body,
                        timeout=timeout
                    ) as response:
                        return web.Response(
                            body=await response.read(),
                            status=response.status,
                            headers=response.headers
                        )
            
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                
                await asyncio.sleep(
                    retry_delay * (attempt + 1)
                )
    
    async def _transform_response(self,
                                response: web.Response,
                                transform: Dict[str, Any]) -> web.Response:
        """Transform service response"""
        # Parse response
        try:
            data = json.loads(response.body)
        except:
            return response
        
        # Apply transformations
        if 'rename_fields' in transform:
            for old_name, new_name in transform['rename_fields'].items():
                if old_name in data:
                    data[new_name] = data.pop(old_name)
        
        if 'remove_fields' in transform:
            for field in transform['remove_fields']:
                data.pop(field, None)
        
        if 'add_fields' in transform:
            data.update(transform['add_fields'])
        
        # Return transformed response
        return web.json_response(
            data,
            status=response.status,
            headers=response.headers
        )
    
    async def _handle_metrics(self,
                            request: web.Request) -> web.Response:
        """Handle metrics endpoint"""
        metrics = prometheus_client.generate_latest()
        return web.Response(
            body=metrics,
            content_type='text/plain'
        )
