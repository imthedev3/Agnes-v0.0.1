from typing import Dict, Any, Optional, List
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
import asyncio
import jwt
from datetime import datetime, timedelta

class APIGateway:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.app = FastAPI(title="AGNES API Gateway")
        self.setup_middleware()
        self.setup_routes()
        self.clients: Dict[str, httpx.AsyncClient] = {}
    
    def setup_middleware(self):
        """Setup API gateway middleware"""
        # Setup CORS
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=self.config.get("cors_origins", ["*"]),
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Add rate limiting middleware
        self.app.add_middleware(
            RateLimitMiddleware,
            limit=self.config.get("rate_limit", 100),
            window=self.config.get("rate_window", 60)
        )
    
    def setup_routes(self):
        """Setup API routes"""
        @self.app.get("/health")
        async def health_check():
            return {"status": "healthy"}
        
        @self.app.post("/api/{service}/{path:path}")
        async def proxy_request(service: str, path: str, request: Request):
            return await self._handle_request(service, path, request)
    
    async def _handle_request(self, 
                            service: str, 
                            path: str, 
                            request: Request) -> Response:
        """Handle incoming API request"""
        # Get service configuration
        service_config = self.config["services"].get(service)
        if not service_config:
            return JSONResponse(
                status_code=404,
                content={"error": f"Service {service} not found"}
            )
        
        # Get or create client
        client = await self._get_client(service)
        
        # Forward request
        try:
            # Build target URL
            target_url = f"{service_config['url']}/{path}"
            
            # Get request body
            body = await request.body()
            
            # Forward request with headers
            response = await client.request(
                method=request.method,
                url=target_url,
                content=body,
                headers=dict(request.headers),
                timeout=service_config.get("timeout", 30)
            )
            
            return Response(
                content=response.content,
                status_code=response.status_code,
                headers=dict(response.headers)
            )
        
        except Exception as e:
            return JSONResponse(
                status_code=500,
                content={"error": str(e)}
            )
    
    async def _get_client(self, service: str) -> httpx.AsyncClient:
        """Get or create HTTP client for service"""
        if service not in self.clients:
            self.clients[service] = httpx.AsyncClient()
        return self.clients[service]

class RateLimitMiddleware:
    def __init__(self, limit: int, window: int):
        self.limit = limit
        self.window = window
        self.requests: Dict[str, List[datetime]] = {}
    
    async def __call__(self, request: Request, call_next):
        # Get client IP
        client_ip = request.client.host
        
        # Check rate limit
        if not self._check_rate_limit(client_ip):
            return JSONResponse(
                status_code=429,
                content={"error": "Rate limit exceeded"}
            )
        
        # Process request
        response = await call_next(request)
        return response
    
    def _check_rate_limit(self, client_ip: str) -> bool:
        """Check if request is within rate limit"""
        now = datetime.utcnow()
        
        # Initialize or clean old requests
        if client_ip not in self.requests:
            self.requests[client_ip] = []
        else:
            # Remove old requests
            threshold = now - timedelta(seconds=self.window)
            self.requests[client_ip] = [
                req for req in self.requests[client_ip]
                if req > threshold
            ]
        
        # Check limit
        if len(self.requests[client_ip]) >= self.limit:
            return False
        
        # Add new request
        self.requests[client_ip].append(now)
        return True
