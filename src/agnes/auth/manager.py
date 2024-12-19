from typing import Dict, Any, List, Optional, Union, Set
import asyncio
import jwt
import bcrypt
import uuid
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from enum import Enum
import json
import aioredis
import aiomysql
from elasticsearch import AsyncElasticsearch

class AuthType(Enum):
    PASSWORD = "password"
    TOKEN = "token"
    OAUTH = "oauth"
    LDAP = "ldap"
    SSO = "sso"

class Permission(Enum):
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"

@dataclass
class User:
    id: str
    username: str
    email: str
    full_name: str
    password_hash: Optional[str]
    roles: List[str]
    permissions: Set[Permission]
    auth_type: AuthType
    metadata: Dict[str, Any]
    active: bool
    created_at: datetime
    last_login: Optional[datetime]

@dataclass
class Role:
    id: str
    name: str
    description: str
    permissions: Set[Permission]
    metadata: Dict[str, Any]
    created_at: datetime

@dataclass
class AuthToken:
    token: str
    user_id: str
    expires_at: datetime
    refresh_token: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class AuthManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.storage = AuthStorage(config['storage'])
        self.cache = AuthCache(config['cache'])
        self.logger = logging.getLogger(__name__)
    
    async def register_user(self,
                          username: str,
                          email: str,
                          password: str,
                          full_name: str,
                          roles: List[str] = None,
                          auth_type: AuthType = AuthType.PASSWORD,
                          metadata: Dict[str, Any] = None) -> User:
        """Register new user"""
        # Check if username/email exists
        if await self.storage.get_user_by_username(username):
            raise ValueError("Username already exists")
        if await self.storage.get_user_by_email(email):
            raise ValueError("Email already exists")
        
        # Hash password
        password_hash = None
        if auth_type == AuthType.PASSWORD:
            password_hash = bcrypt.hashpw(
                password.encode(),
                bcrypt.gensalt()
            ).decode()
        
        # Create user
        user = User(
            id=str(uuid.uuid4()),
            username=username,
            email=email,
            full_name=full_name,
            password_hash=password_hash,
            roles=roles or [],
            permissions=set(),
            auth_type=auth_type,
            metadata=metadata or {},
            active=True,
            created_at=datetime.utcnow(),
            last_login=None
        )
        
        # Calculate permissions
        user.permissions = await self._calculate_permissions(user.roles)
        
        # Store user
        await self.storage.store_user(user)
        
        return user
    
    async def authenticate(self,
                         username: str,
                         password: str) -> AuthToken:
        """Authenticate user with password"""
        user = await self.storage.get_user_by_username(username)
        if not user:
            raise ValueError("Invalid username or password")
        
        if not user.active:
            raise ValueError("User account is disabled")
        
        if user.auth_type != AuthType.PASSWORD:
            raise ValueError("Invalid authentication method")
        
        if not bcrypt.checkpw(
            password.encode(),
            user.password_hash.encode()
        ):
            raise ValueError("Invalid username or password")
        
        # Update last login
        user.last_login = datetime.utcnow()
        await self.storage.store_user(user)
        
        # Generate tokens
        return await self._generate_tokens(user)
    
    async def authenticate_oauth(self,
                               provider: str,
                               token: str) -> AuthToken:
        """Authenticate user with OAuth"""
        # Verify OAuth token with provider
        user_info = await self._verify_oauth_token(provider, token)
        
        # Get or create user
        user = await self.storage.get_user_by_email(user_info['email'])
        if not user:
            user = await self.register_user(
                username=user_info['username'],
                email=user_info['email'],
                password='',
                full_name=user_info['name'],
                auth_type=AuthType.OAUTH,
                metadata={
                    'oauth_provider': provider
                }
            )
        
        # Update last login
        user.last_login = datetime.utcnow()
        await self.storage.store_user(user)
        
        # Generate tokens
        return await self._generate_tokens(user)
    
    async def verify_token(self, token: str) -> User:
        """Verify JWT token"""
        try:
            # Check cache first
            cached_user = await self.cache.get_user(token)
            if cached_user:
                return cached_user
            
            # Verify token
            payload = jwt.decode(
                token,
                self.config['jwt_secret'],
                algorithms=['HS256']
            )
            
            # Get user
            user = await self.storage.get_user(payload['sub'])
            if not user:
                raise ValueError("User not found")
            
            if not user.active:
                raise ValueError("User account is disabled")
            
            # Cache user
            await self.cache.set_user(token, user)
            
            return user
        
        except jwt.InvalidTokenError:
            raise ValueError("Invalid token")
    
    async def refresh_token(self,
                          refresh_token: str) -> AuthToken:
        """Refresh JWT token"""
        try:
            # Verify refresh token
            payload = jwt.decode(
                refresh_token,
                self.config['jwt_secret'],
                algorithms=['HS256']
            )
            
            if payload['type'] != 'refresh':
                raise ValueError("Invalid token type")
            
            # Get user
            user = await self.storage.get_user(payload['sub'])
            if not user:
                raise ValueError("User not found")
            
            if not user.active:
                raise ValueError("User account is disabled")
            
            # Generate new tokens
            return await self._generate_tokens(user)
        
        except jwt.InvalidTokenError:
            raise ValueError("Invalid refresh token")
    
    async def create_role(self,
                         name: str,
                         description: str,
                         permissions: Set[Permission],
                         metadata: Dict[str, Any] = None) -> Role:
        """Create new role"""
        # Check if role exists
        if await self.storage.get_role_by_name(name):
            raise ValueError("Role already exists")
        
        # Create role
        role = Role(
            id=str(uuid.uuid4()),
            name=name,
            description=description,
            permissions=permissions,
            metadata=metadata or {},
            created_at=datetime.utcnow()
        )
        
        # Store role
        await self.storage.store_role(role)
        
        return role
    
    async def assign_role(self,
                         user_id: str,
                         role_name: str):
        """Assign role to user"""
        user = await self.storage.get_user(user_id)
        if not user:
            raise ValueError("User not found")
        
        role = await self.storage.get_role_by_name(role_name)
        if not role:
            raise ValueError("Role not found")
        
        if role_name not in user.roles:
            user.roles.append(role_name)
            user.permissions = await self._calculate_permissions(user.roles)
            await self.storage.store_user(user)
            
            # Invalidate cache
            await self.cache.delete_user_permissions(user_id)
    
    async def remove_role(self,
                         user_id: str,
                         role_name: str):
        """Remove role from user"""
        user = await self.storage.get_user(user_id)
        if not user:
            raise ValueError("User not found")
        
        if role_name in user.roles:
            user.roles.remove(role_name)
            user.permissions = await self._calculate_permissions(user.roles)
            await self.storage.store_user(user)
            
            # Invalidate cache
            await self.cache.delete_user_permissions(user_id)
    
    async def check_permission(self,
                             user_id: str,
                             permission: Permission) -> bool:
        """Check if user has permission"""
        # Check cache first
        cached = await self.cache.get_user_permissions(user_id)
        if cached is not None:
            return permission in cached
        
        # Get user permissions
        user = await self.storage.get_user(user_id)
        if not user:
            return False
        
        # Cache permissions
        await self.cache.set_user_permissions(
            user_id,
            user.permissions
        )
        
        return permission in user.permissions
    
    async def _calculate_permissions(self,
                                   roles: List[str]) -> Set[Permission]:
        """Calculate user permissions from roles"""
        permissions = set()
        
        for role_name in roles:
            role = await self.storage.get_role_by_name(role_name)
            if role:
                permissions.update(role.permissions)
        
        return permissions
    
    async def _generate_tokens(self, user: User) -> AuthToken:
        """Generate JWT tokens"""
        now = datetime.utcnow()
        
        # Access token
        access_token = jwt.encode(
            {
                'sub': user.id,
                'type': 'access',
                'roles': user.roles,
                'permissions': [p.value for p in user.permissions],
                'iat': now,
                'exp': now + timedelta(
                    minutes=self.config['access_token_lifetime']
                )
            },
            self.config['jwt_secret'],
            algorithm='HS256'
        )
        
        # Refresh token
        refresh_token = jwt.encode(
            {
                'sub': user.id,
                'type': 'refresh',
                'iat': now,
                'exp': now + timedelta(
                    days=self.config['refresh_token_lifetime']
                )
            },
            self.config['jwt_secret'],
            algorithm='HS256'
        )
        
        return AuthToken(
            token=access_token,
            user_id=user.id,
            expires_at=now + timedelta(
                minutes=self.config['access_token_lifetime']
            ),
            refresh_token=refresh_token
        )
    
    async def _verify_oauth_token(self,
                                provider: str,
                                token: str) -> Dict[str, Any]:
        """Verify OAuth token with provider"""
        if provider == 'google':
            return await self._verify_google_token(token)
        elif provider == 'github':
            return await self._verify_github_token(token)
        else:
            raise ValueError(f"Unsupported OAuth provider: {provider}")

class AuthStorage:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config['type']
        self._setup_storage()
    
    def _setup_storage(self):
        """Setup storage backend"""
        if self.type == 'mysql':
            self.pool = aiomysql.create_pool(**self.config['mysql'])
        elif self.type == 'elasticsearch':
            self.client = AsyncElasticsearch([self.config['elasticsearch_url']])
    
    async def store_user(self, user: User):
        """Store user"""
        if self.type == 'mysql':
            await self._store_user_mysql(user)
        elif self.type == 'elasticsearch':
            await self._store_user_elasticsearch(user)
    
    async def store_role(self, role: Role):
        """Store role"""
        if self.type == 'mysql':
            await self._store_role_mysql(role)
        elif self.type == 'elasticsearch':
            await self._store_role_elasticsearch(role)
    
    async def get_user(self, user_id: str) -> Optional[User]:
        """Get user by ID"""
        if self.type == 'mysql':
            return await self._get_user_mysql(user_id)
        elif self.type == 'elasticsearch':
            return await self._get_user_elasticsearch(user_id)
    
    async def get_user_by_username(self,
                                 username: str) -> Optional[User]:
        """Get user by username"""
        if self.type == 'mysql':
            return await self._get_user_by_username_mysql(username)
        elif self.type == 'elasticsearch':
            return await self._get_user_by_username_elasticsearch(username)
    
    async def get_user_by_email(self,
                               email: str) -> Optional[User]:
        """Get user by email"""
        if self.type == 'mysql':
            return await self._get_user_by_email_mysql(email)
        elif self.type == 'elasticsearch':
            return await self._get_user_by_email_elasticsearch(email)
    
    async def get_role_by_name(self,
                              name: str) -> Optional[Role]:
        """Get role by name"""
        if self.type == 'mysql':
            return await self._get_role_by_name_mysql(name)
        elif self.type == 'elasticsearch':
            return await self._get_role_by_name_elasticsearch(name)

class AuthCache:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.redis = aioredis.Redis.from_url(
            config['redis_url']
        )
    
    async def get_user(self, token: str) -> Optional[User]:
        """Get cached user"""
        data = await self.redis.get(f"user:{token}")
        if data:
            return User(**json.loads(data))
        return None
    
    async def set_user(self,
                      token: str,
                      user: User,
                      ttl: int = 300):
        """Cache user"""
        await self.redis.set(
            f"user:{token}",
            json.dumps(user.__dict__),
            ex=ttl
        )
    
    async def get_user_permissions(self,
                                 user_id: str) -> Optional[Set[Permission]]:
        """Get cached user permissions"""
        data = await self.redis.get(f"permissions:{user_id}")
        if data:
            return {
                Permission(p)
                for p in json.loads(data)
            }
        return None
    
    async def set_user_permissions(self,
                                 user_id: str,
                                 permissions: Set[Permission],
                                 ttl: int = 300):
        """Cache user permissions"""
        await self.redis.set(
            f"permissions:{user_id}",
            json.dumps([p.value for p in permissions]),
            ex=ttl
        )
    
    async def delete_user_permissions(self, user_id: str):
        """Delete cached user permissions"""
        await self.redis.delete(f"permissions:{user_id}")
