from typing import Dict, Any, Optional
import hashlib
import hmac
import base64
from cryptography.fernet import Fernet
from dataclasses import dataclass

@dataclass
class SecurityConfig:
    encryption_key: str
    max_request_size: int
    rate_limit: int
    allowed_ips: list
    security_headers: Dict[str, str]

class SecurityManager:
    def __init__(self, config: SecurityConfig):
        self.config = config
        self.cipher_suite = Fernet(config.encryption_key.encode())
        
    async def encrypt_data(self, data: str) -> str:
        """Encrypt sensitive data"""
        encrypted_data = self.cipher_suite.encrypt(data.encode())
        return base64.b64encode(encrypted_data).decode()
    
    async def decrypt_data(self, encrypted_data: str) -> str:
        """Decrypt sensitive data"""
        decoded_data = base64.b64decode(encrypted_data.encode())
        decrypted_data = self.cipher_suite.decrypt(decoded_data)
        return decrypted_data.decode()
    
    async def generate_hmac(self, data: str, key: str) -> str:
        """Generate HMAC for data integrity"""
        hmac_obj = hmac.new(
            key.encode(),
            msg=data.encode(),
            digestmod=hashlib.sha256
        )
        return hmac_obj.hexdigest()
    
    async def verify_hmac(self, data: str, signature: str, key: str) -> bool:
        """Verify HMAC signature"""
        expected_signature = await self.generate_hmac(data, key)
        return hmac.compare_digest(signature, expected_signature)

class RequestSanitizer:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
    async def sanitize_input(self, 
                            input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize input data"""
        sanitized = {}
        for key, value in input_data.items():
            if isinstance(value, str):
                sanitized[key] = self._sanitize_string(value)
            elif isinstance(value, dict):
                sanitized[key] = await self.sanitize_input(value)
            elif isinstance(value, list):
                sanitized[key] = [
                    await self.sanitize_input(item) 
                    if isinstance(item, dict) 
                    else self._sanitize_string(item) 
                    if isinstance(item, str) 
                    else item 
                    for item in value
                ]
            else:
                sanitized[key] = value
        return sanitized
    
    def _sanitize_string(self, value: str) -> str:
        """Sanitize string values"""
        # Remove potential XSS
        value = value.replace("<script>", "").replace("</script>", "")
        # Remove SQL injection attempts
        value = value.replace("'", "").replace('"', "")
        # Add more sanitization as needed
        return value

class AccessControl:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.rate_limiter = RateLimiter(config.get("rate_limit", {}))
        
    async def check_access(self, 
                          request: Dict[str, Any],
                          token: str) -> bool:
        """Check access permissions"""
        # Verify token
        if not await self._verify_token(token):
            return False
        
        # Check rate limit
        if not await self.rate_limiter.check_limit(request):
            return False
        
        # Check permissions
        if not await self._check_permissions(request, token):
            return False
        
        return True
    
    async def _verify_token(self, token: str) -> bool:
        """Verify authentication token"""
        # Implement token verification logic
        return True
    
    async def _check_permissions(self, 
                               request: Dict[str, Any],
                               token: str) -> bool:
        """Check user permissions"""
        # Implement permission checking logic
        return True

class RateLimiter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.limits = {}
        
    async def check_limit(self, request: Dict[str, Any]) -> bool:
        """Check rate limits"""
        # Implement rate limiting logic
        return True
