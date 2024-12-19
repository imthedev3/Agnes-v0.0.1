from typing import Dict, Any, Optional
import yaml
import json
from pathlib import Path
from dataclasses import dataclass
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

@dataclass
class ConfigurationSource:
    type: str
    path: str
    format: str
    required: bool = True
    auto_reload: bool = False

class ConfigManager:
    def __init__(self, 
                 sources: Dict[str, ConfigurationSource],
                 defaults: Optional[Dict[str, Any]] = None):
        self.sources = sources
        self.defaults = defaults or {}
        self.config = {}
        self.observers = []
        
        # Load initial configuration
        self.reload_all()
        
        # Setup auto-reload if needed
        self._setup_auto_reload()
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self.config.get(key, default)
    
    def reload_all(self):
        """Reload all configuration sources"""
        config = self.defaults.copy()
        
        for name, source in self.sources.items():
            try:
                source_config = self._load_source(source)
                config.update(source_config)
            except Exception as e:
                if source.required:
                    raise ConfigurationError(
                        f"Failed to load required configuration source {name}: {str(e)}"
                    )
        
        self.config = config
    
    def _load_source(self, source: ConfigurationSource) -> Dict[str, Any]:
        """Load configuration from source"""
        path = Path(source.path)
        
        if not path.exists():
            raise ConfigurationError(f"Configuration path does not exist: {path}")
        
        if source.format == 'yaml':
            return self._load_yaml(path)
        elif source.format == 'json':
            return self._load_json(path)
        else:
            raise ConfigurationError(f"Unsupported configuration format: {source.format}")
    
    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        """Load YAML configuration file"""
        with open(path, 'r') as f:
            return yaml.safe_load(f)
    
    def _load_json(self, path: Path) -> Dict[str, Any]:
        """Load JSON configuration file"""
        with open(path, 'r') as f:
            return json.load(f)
    
    def _setup_auto_reload(self):
        """Setup configuration auto-reload"""
        class ConfigFileHandler(FileSystemEventHandler):
            def __init__(self, config_manager):
                self.config_manager = config_manager
            
            def on_modified(self, event):
                if not event.is_directory:
                    self.config_manager.reload_all()
        
        # Setup file watchers for auto-reload sources
        for source in self.sources.values():
            if source.auto_reload:
                path = Path(source.path).parent
                observer = Observer()
                observer.schedule(ConfigFileHandler(self), path, recursive=False)
                observer.start()
                self.observers.append(observer)
    
    def __del__(self):
        """Cleanup observers"""
        for observer in self.observers:
            observer.stop()
            observer.join()

class EnvironmentConfig:
    def __init__(self, prefix: str = 'AGNES_'):
        self.prefix = prefix
    
    def get_env_config(self) -> Dict[str, Any]:
        """Get configuration from environment variables"""
        import os
        
        config = {}
        for key, value in os.environ.items():
            if key.startswith(self.prefix):
                config_key = key[len(self.prefix):].lower()
                config[config_key] = self._parse_env_value(value)
        
        return config
    
    def _parse_env_value(self, value: str) -> Any:
        """Parse environment variable value"""
        # Try to parse as JSON
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            # Return as string if not valid JSON
            return value

class ConfigurationError(Exception):
    """Configuration error exception"""
    pass
