from typing import Dict, Any, List, Optional, Union
import asyncio
import jwt
import bcrypt
import base64
import secrets
import hashlib
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import aioredis
import aiomysql
import aiohttp
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import re
import ipaddress
from urllib.parse import urlparse
import ssl
import certifi

class SecurityLevel(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AuthType(Enum):
    PASSWORD = "password"
    TOKEN = "token"
    APIKEY = "apikey"
    OAUTH = "oauth"
    JWT = "jwt"
    CERTIFICATE = "certificate"

@dataclass
class SecurityEvent:
    id: str
    type: str
    level: SecurityLevel
    source: str
    target: str
    action: str
    status: str
    timestamp: datetime
    metadata: Dict[str, Any]
    details: str

class SecurityManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.storage = SecurityStorage(config['storage'])
        self.cache = aioredis.Redis.from_url(config['cache']['redis_url'])
        self.logger = logging.getLogger(__name__)
        
        # Initialize encryption
        self.fernet = Fernet(config['encryption']['key'].encode())
        
        # Load security policies
        self.policies = config.get('policies', {})
        
        # Initialize rate limiters
        self.rate_limiters = {}
        for name, policy in config.get('rate_limits', {}).items():
            self.rate_limiters[name] = RateLimiter(
                self.cache,
                policy['limit'],
                policy['window']
            )
    
    async def authenticate(self,
                         auth_type: AuthType,
                         credentials: Dict[str, Any]) -> bool:
        """Authenticate request"""
        try:
            if auth_type == AuthType.PASSWORD:
                return await self._authenticate_password(credentials)
            elif auth_type == AuthType.TOKEN:
                return await self._authenticate_token(credentials)
            elif auth_type == AuthType.APIKEY:
                return await self._authenticate_apikey(credentials)
            elif auth_type == AuthType.OAUTH:
                return await self._authenticate_oauth(credentials)
            elif auth_type == AuthType.JWT:
                return await self._authenticate_jwt(credentials)
            elif auth_type == AuthType.CERTIFICATE:
                return await self._authenticate_certificate(credentials)
            
            return False
        
        except Exception as e:
            await self.log_security_event(
                "authentication_error",
                SecurityLevel.HIGH,
                credentials.get('username', 'unknown'),
                'auth_system',
                'authenticate',
                'failed',
                {'error': str(e)}
            )
            raise
    
    async def _authenticate_password(self,
                                   credentials: Dict[str, Any]) -> bool:
        """Authenticate with username/password"""
        username = credentials.get('username')
        password = credentials.get('password')
        
        if not username or not password:
            return False
        
        # Check rate limit
        if not await self.rate_limiters['login'].check(username):
            await self.log_security_event(
                "rate_limit_exceeded",
                SecurityLevel.HIGH,
                username,
                'auth_system',
                'login',
                'blocked',
                {'reason': 'rate_limit'}
            )
            return False
        
        # Get stored password hash
        user = await self.storage.get_user(username)
        if not user:
            return False
        
        # Verify password
        if bcrypt.checkpw(
            password.encode(),
            user['password_hash'].encode()
        ):
            await self.log_security_event(
                "login_success",
                SecurityLevel.LOW,
                username,
                'auth_system',
                'login',
                'success',
                {}
            )
            return True
        
        await self.log_security_event(
            "login_failed",
            SecurityLevel.MEDIUM,
            username,
            'auth_system',
            'login',
            'failed',
            {'reason': 'invalid_password'}
        )
        return False
    
    async def _authenticate_token(self,
                                credentials: Dict[str, Any]) -> bool:
        """Authenticate with token"""
        token = credentials.get('token')
        if not token:
            return False
        
        # Check token in cache
        token_data = await self.cache.get(f"token:{token}")
        if not token_data:
            return False
        
        token_data = json.loads(token_data)
        
        # Check if token is expired
        if datetime.fromtimestamp(token_data['expires_at']) < datetime.utcnow():
            await self.cache.delete(f"token:{token}")
            return False
        
        return True
    
    async def _authenticate_apikey(self,
                                 credentials: Dict[str, Any]) -> bool:
        """Authenticate with API key"""
        api_key = credentials.get('api_key')
        if not api_key:
            return False
        
        # Get API key data
        key_data = await self.storage.get_apikey(api_key)
        if not key_data:
            return False
        
        # Check if key is active
        if not key_data['active']:
            return False
        
        # Check if key has expired
        if key_data['expires_at'] and \
           datetime.fromtimestamp(key_data['expires_at']) < datetime.utcnow():
            return False
        
        # Update last used timestamp
        await self.storage.update_apikey_usage(api_key)
        
        return True
    
    async def authorize(self,
                       user: str,
                       resource: str,
                       action: str) -> bool:
        """Authorize action"""
        try:
            # Get user roles
            roles = await self.storage.get_user_roles(user)
            
            # Get resource permissions
            permissions = await self.storage.get_resource_permissions(
                resource,
                action
            )
            
            # Check if any role has required permission
            for role in roles:
                if role in permissions['allowed_roles']:
                    await self.log_security_event(
                        "authorization_success",
                        SecurityLevel.LOW,
                        user,
                        resource,
                        action,
                        'success',
                        {'roles': roles}
                    )
                    return True
            
            await self.log_security_event(
                "authorization_failed",
                SecurityLevel.MEDIUM,
                user,
                resource,
                action,
                'failed',
                {'roles': roles, 'required': permissions['allowed_roles']}
            )
            return False
        
        except Exception as e:
            await self.log_security_event(
                "authorization_error",
                SecurityLevel.HIGH,
                user,
                resource,
                action,
                'error',
                {'error': str(e)}
            )
            raise
    
    async def encrypt(self, data: Union[str, bytes]) -> str:
        """Encrypt data"""
        if isinstance(data, str):
            data = data.encode()
        return self.fernet.encrypt(
