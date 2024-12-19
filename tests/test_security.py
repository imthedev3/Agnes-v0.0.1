import pytest
from agnes.safety.security import (
    SecurityManager,
    RequestSanitizer,
    AccessControl,
    SecurityConfig
)

@pytest.fixture
def security_config():
    return SecurityConfig(
        encryption_key="your-test-key-here",
        max_request_size=1024*1024,
        rate_limit=60,
        allowed_ips=["127.0.0.1"],
        security_headers={
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY"
        }
    )

@pytest.fixture
def security_manager(security_config):
    return SecurityManager(security_config)

async def test_encryption(security_manager):
    original_data = "sensitive_data"
    encrypted = await security_manager.encrypt_data(original_data)
    decrypted = await security_manager.decrypt_data(encrypted)
    assert decrypted == original_data

@pytest.fixture
def request_sanitizer():
    config = {
        "max_length": 1000,
        "allowed_tags": []
    }
    return RequestSanitizer(config)

async def test_input_sanitization(request_sanitizer):
    input_data = {
        "text": "Hello <script>alert('xss')</script>",
        "nested": {
            "sql": "SELECT * FROM users WHERE 1=1--"
        }
    }
    sanitized = await request_sanitizer.sanitize_input(input_data)
    assert "<script>" not in sanitized["text"]
    assert "--" not in sanitized["nested"]["sql"]
