from typing import Dict, Any, List, Optional, Union, Set
from dataclasses import dataclass
import jwt
import bcrypt
import asyncio
import time
from enum import Enum
import uuid
from datetime import datetime, timedelta

class PermissionLevel(Enum):
    NONE = 0
    READ = 1
    WRITE = 2
    ADMIN = 3

@dataclass
class Permission:
    resource: str
    level: PermissionLevel
    conditions: Optional[Dict[str, Any]] = None

@dataclass
class Role:
    name: str
    permissions: List[Permission]
    description: str = ""
    metadata: Dict[str, Any] = None

class PermissionManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.roles: Dict[str, Role] = {}
        self.user_roles: Dict[str, Set[str]] = {}
        self.cache: Dict[str, Dict[str, bool]] = {}
        self.cache_ttl = config.get('cache_ttl', 300)  # 5 minutes
    
    def add_role(self, role: Role):
        """Add new role"""
        self.roles[role.name] = role
    
    def remove_role(self, role_name: str):
        """Remove role"""
        if role_name in self.roles:
            del self.roles[role_name]
            # Remove role from all users
            for user_roles in self.user_roles.values():
                user_roles.discard(role_name)
    
    def assign_role(self, user_id: str, role_name: str):
        """Assign role to user"""
        if role_name not in self.roles:
            raise ValueError(f"Role {role_name} does not exist")
        
        if user_id not in self.user_roles:
            self.user_roles[user_id] = set()
        
        self.user_roles[user_id].add(role_name)
        self._clear_user_cache(user_id)
    
    def revoke_role(self, user_id: str, role_name: str):
        """Revoke role from user"""
        if user_id in self.user_roles:
            self.user_roles[user_id].discard(role_name)
            self._clear_user_cache(user_id)
    
    def check_permission(self,
                        user_id: str,
                        resource: str,
                        level: PermissionLevel,
                        context: Optional[Dict[str, Any]] = None) -> bool:
        """Check if user has permission"""
        cache_key = f"{user_id}:{resource}:{level.value}"
        
        # Check cache
        if cache_key in self.cache:
            cache_time, result = self.cache[cache_key]
            if time.time() - cache_time < self.cache_ttl:
                return result
        
        # Get user roles
        user_roles = self.user_roles.get(user_id, set())
        
        # Check each role's permissions
        for role_name in user_roles:
            role = self.roles[role_name]
            for permission in role.permissions:
                if (permission.resource == resource or 
                    permission.resource == "*"):
                    if permission.level.value >= level.value:
                        if self._check_conditions(permission, context):
                            # Cache result
                            self.cache[cache_key] = (time.time(), True)
                            return True
        
        # Cache negative result
        self.cache[cache_key] = (time.time(), False)
        return False
    
    def _check_conditions(self,
                         permission: Permission,
                         context: Optional[Dict[str, Any]]) -> bool:
        """Check permission conditions"""
        if not permission.conditions:
            return True
        
        if not context:
            return False
        
        for key, value in permission.conditions.items():
            if key not in context:
                return False
            
            if isinstance(value, list):
                if context[key] not in value:
                    return False
            elif context[key] != value:
                return False
        
        return True
    
    def _clear_user_cache(self, user_id: str):
        """Clear user's permission cache"""
        keys_to_remove = [
            key for key in self.cache.keys()
            if key.startswith(f"{user_id}:")
        ]
        for key in keys_to_remove:
            del self.cache[key]

class RBACManager:
    def __init__(self, permission_manager: PermissionManager):
        self.permission_manager = permission_manager
    
    def create_policy(self,
                     role: str,
                     resources: List[str],
                     level: PermissionLevel,
                     conditions: Optional[Dict[str, Any]] = None):
        """Create new RBAC policy"""
        permissions = [
            Permission(
                resource=resource,
                level=level,
                conditions=conditions
            )
            for resource in resources
        ]
        
        role = Role(
            name=role,
            permissions=permissions
        )
        
        self.permission_manager.add_role(role)
    
    def update_policy(self,
                     role: str,
                     resources: List[str],
                     level: PermissionLevel,
                     conditions: Optional[Dict[str, Any]] = None):
        """Update existing RBAC policy"""
        self.permission_manager.remove_role(role)
        self.create_policy(role, resources, level, conditions)
    
    def delete_policy(self, role: str):
        """Delete RBAC policy"""
        self.permission_manager.remove_role(role)
    
    def list_policies(self) -> List[Role]:
        """List all RBAC policies"""
        return list(self.permission_manager.roles.values())

class TokenManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.secret = config['jwt_secret']
        self.algorithm = config.get('jwt_algorithm', 'HS256')
        self.access_token_ttl = config.get('access_token_ttl', 3600)  # 1 hour
        self.refresh_token_ttl = config.get('refresh_token_ttl', 86400 * 7)  # 7 days
    
    def create_access_token(self,
                          user_id: str,
                          roles: List[str],
                          additional_claims: Optional[Dict[str, Any]] = None) -> str:
        """Create access token"""
        now = datetime.utcnow()
        claims = {
            "sub": user_id,
            "roles": roles,
            "type": "access",
            "iat": now,
            "exp": now + timedelta(seconds=self.access_token_ttl)
        }
        
        if additional_claims:
            claims.update(additional_claims)
        
        return jwt.encode(claims, self.secret, algorithm=self.algorithm)
    
    def create_refresh_token(self, user_id: str) -> str:
        """Create refresh token"""
        now = datetime.utcnow()
        claims = {
            "sub": user_id,
            "type": "refresh",
            "iat": now,
            "exp": now + timedelta(seconds=self.refresh_token_ttl)
        }
        
        return jwt.encode(claims, self.secret, algorithm=self.algorithm)
    
    def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify and decode token"""
        try:
            claims = jwt.decode(
                token,
                self.secret,
                algorithms=[self.algorithm]
            )
            return claims
        except jwt.ExpiredSignatureError:
            raise ValueError("Token has expired")
        except jwt.InvalidTokenError:
            raise ValueError("Invalid token")
