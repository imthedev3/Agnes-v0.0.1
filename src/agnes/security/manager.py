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
        return self.fernet.encrypt(data).decode()
    
    async def decrypt(self, encrypted: str) -> bytes:
        """Decrypt data"""
        return self.fernet.decrypt(encrypted.encode())
    
    def hash_password(self, password: str) -> str:
        """Hash password"""
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode(), salt).decode()
    
    async def validate_password(self, password: str) -> bool:
        """Validate password strength"""
        policy = self.policies['password']
        
        if len(password) < policy['min_length']:
            return False
        
        if policy['require_uppercase'] and \
           not any(c.isupper() for c in password):
            return False
        
        if policy['require_lowercase'] and \
           not any(c.islower() for c in password):
            return False
        
        if policy['require_numbers'] and \
           not any(c.isdigit() for c in password):
            return False
        
        if policy['require_special'] and \
           not any(c in policy['special_chars'] for c in password):
            return False
        
        return True
    
    async def generate_token(self,
                           user: str,
                           expires_in: int = 3600) -> str:
        """Generate authentication token"""
        token = secrets.token_urlsafe(32)
        
        await self.cache.set(
            f"token:{token}",
            json.dumps({
                'user': user,
                'created_at': datetime.utcnow().timestamp(),
                'expires_at': (datetime.utcnow() + \
                             timedelta(seconds=expires_in)).timestamp()
            }),
            ex=expires_in
        )
        
        return token
    
    async def generate_apikey(self,
                            name: str,
                            user: str,
                            expires_in: Optional[int] = None) -> str:
        """Generate API key"""
        key = f"ak_{secrets.token_urlsafe(32)}"
        
        await self.storage.store_apikey({
            'key': key,
            'name': name,
            'user': user,
            'created_at': datetime.utcnow().timestamp(),
            'expires_at': (datetime.utcnow() + \
                          timedelta(seconds=expires_in)).timestamp() \
                          if expires_in else None,
            'active': True
        })
        
        return key
    
    async def validate_input(self,
                           input_type: str,
                           value: str) -> bool:
        """Validate user input"""
        policy = self.policies['input'].get(input_type)
        if not policy:
            return True
        
        if 'max_length' in policy and \
           len(value) > policy['max_length']:
            return False
        
        if 'pattern' in policy and \
           not re.match(policy['pattern'], value):
            return False
        
        if 'blacklist' in policy and \
           any(bad in value.lower() for bad in policy['blacklist']):
            return False
        
        return True
    
    async def validate_ip(self, ip: str) -> bool:
        """Validate IP address"""
        try:
            addr = ipaddress.ip_address(ip)
            
            # Check if IP is in blacklist
            if any(addr in ipaddress.ip_network(net) \
                  for net in self.policies['ip']['blacklist']):
                return False
            
            # Check if IP is in whitelist
            if self.policies['ip']['whitelist'] and \
               not any(addr in ipaddress.ip_network(net) \
                      for net in self.policies['ip']['whitelist']):
                return False
            
            return True
        
        except ValueError:
            return False
    
    async def validate_url(self, url: str) -> bool:
        """Validate URL"""
        try:
            parsed = urlparse(url)
            
            # Check scheme
            if parsed.scheme not in self.policies['url']['allowed_schemes']:
                return False
            
            # Check domain
            if any(domain in parsed.netloc \
                  for domain in self.policies['url']['blacklist']):
                return False
            
            return True
        
        except Exception:
            return False
    
    async def log_security_event(self,
                               event_type: str,
                               level: SecurityLevel,
                               source: str,
                               target: str,
                               action: str,
                               status: str,
                               metadata: Dict[str, Any],
                               details: str = ""):
        """Log security event"""
        event = SecurityEvent(
            id=f"evt_{secrets.token_hex(8)}",
            type=event_type,
            level=level,
            source=source,
            target=target,
            action=action,
            status=status,
            timestamp=datetime.utcnow(),
            metadata=metadata,
            details=details
        )
        
        # Store event
        await self.storage.store_event(event)
        
        # Send alerts for high severity events
        if level in [SecurityLevel.HIGH, SecurityLevel.CRITICAL]:
            await self._send_security_alert(event)
    
    async def _send_security_alert(self, event: SecurityEvent):
        """Send security alert"""
        if not self.config.get('alerts', {}).get('enabled', False):
            return
        
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(
                    self.config['alerts']['webhook_url'],
                    json={
                        'event_id': event.id,
                        'type': event.type,
                        'level': event.level.value,
                        'source': event.source,
                        'target': event.target,
                        'action': event.action,
                        'status': event.status,
                        'timestamp': event.timestamp.isoformat(),
                        'metadata': event.metadata,
                        'details': event.details
                    }
                )
        except Exception as e:
            self.logger.error(f"Failed to send security alert: {e}")

class RateLimiter:
    def __init__(self,
                 redis: aioredis.Redis,
                 limit: int,
                 window: int):
        self.redis = redis
        self.limit = limit
        self.window = window
    
    async def check(self, key: str) -> bool:
        """Check rate limit"""
        current = await self.redis.incr(f"ratelimit:{key}")
        
        if current == 1:
            await self.redis.expire(f"ratelimit:{key}", self.window)
        
        return current <= self.limit

class SecurityStorage:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config['type']
        self._setup_storage()
    
    def _setup_storage(self):
        """Setup storage backend"""
        if self.type == 'mysql':
            self.pool = aiomysql.create_pool(
                **self.config['mysql']
            )
