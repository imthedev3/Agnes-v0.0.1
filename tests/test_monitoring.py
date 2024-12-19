import pytest
from agnes.monitoring.logger import StructuredLogger, MetricsCollector
from agnes.config.manager import ConfigManager, ConfigurationSource

@pytest.fixture
def config():
    return {
        'log_level': 'INFO',
        'elasticsearch_url': 'http://localhost:9200',
        'index_prefix': 'test-logs-'
    }

@pytest.fixture
def logger(config):
    return StructuredLogger(config)

async def test_log_event(logger):
    event_data = {
        'endpoint': '/api/test',
        'status': 'success',
        'latency': 0.1
    }
    
    await logger.log_event('request', event_data)
    # Add assertions for log storage and metrics

@pytest.fixture
def config_sources():
    return {
        'main': ConfigurationSource(
            type='file',
            path='tests/fixtures/config.yaml',
            format='yaml'
        ),
        'local': ConfigurationSource(
            type='file',
            path='tests/fixtures/config.local.yaml',
            format='yaml',
            required=False
        )
    }

@pytest.fixture
def config_manager(config_sources):
    return ConfigManager(config_sources)

def test_config_loading(config_manager):
    assert config_manager.get('version') == '1.0'
    assert 'logging' in config_manager.config
    
def test_config_reload(config_manager):
    config_manager.reload_all()
    # Add assertions for reloaded configuration
