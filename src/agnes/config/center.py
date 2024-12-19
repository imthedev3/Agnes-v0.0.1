from typing import Dict, Any, Optional, List, Union
import asyncio
import json
import yaml
from datetime import datetime
import aiohttp
import consul.aio
import etcd3.client
from dataclasses import dataclass
import zookeeper
import threading
import watchdog.observers
import watchdog.events
from cryptography.fernet import Fernet
import hashlib

@dataclass
class ConfigItem:
    key: str
    value: Any
    version: int
    last_modified: datetime
    encrypted: bool = False

class ConfigStore:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config.get('type', 'consul')
        self.client = None
        self._setup_client()
    
    def _setup_client(self):
        """Setup configuration store client"""
        if self.type == 'consul':
            self.client = consul.aio.Consul(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 8500)
            )
        elif self.type == 'etcd':
            self.client = etcd3.client.Etcd3Client(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 2379)
            )
        else:
            raise ValueError(f"Unsupported config store type: {self.type}")
    
    async def get(self, key: str) -> Optional[ConfigItem]:
        """Get configuration value"""
        if self.type == 'consul':
            index, data = await self.client.kv.get(key)
            if data:
                return ConfigItem(
                    key=key,
                    value=json.loads(data['Value'].decode()),
                    version=data['ModifyIndex'],
                    last_modified=datetime.fromtimestamp(
                        data['ModifyIndex']
                    ),
                    encrypted=data.get('Flags', 0) == 1
                )
        elif self.type == 'etcd':
            value = await self.client.get(key)
            if value:
                metadata = value.metadata
                return ConfigItem(
                    key=key,
                    value=json.loads(value[0].decode()),
                    version=metadata.version,
                    last_modified=datetime.fromtimestamp(
                        metadata.create_revision
                    ),
                    encrypted=metadata.flags == 1
                )
        return None
    
    async def put(self, 
                  key: str, 
                  value: Any, 
                  encrypted: bool = False) -> ConfigItem:
        """Put configuration value"""
        encoded_value = json.dumps(value).encode()
        
        if self.type == 'consul':
            await self.client.kv.put(
                key,
                encoded_value,
                flags=1 if encrypted else 0
            )
            return await self.get(key)
        
        elif self.type == 'etcd':
            result = await self.client.put(
                key,
                encoded_value,
                prev_kv=True,
                flags=1 if encrypted else 0
            )
            return await self.get(key)
    
    async def delete(self, key: str):
        """Delete configuration value"""
        if self.type == 'consul':
            await self.client.kv.delete(key)
        elif self.type == 'etcd':
            await self.client.delete(key)
    
    async def list(self, prefix: str = "") -> List[ConfigItem]:
        """List configuration values"""
        items = []
        
        if self.type == 'consul':
            index, data = await self.client.kv.get(prefix, recurse=True)
            if data:
                for item in data:
                    items.append(ConfigItem(
                        key=item['Key'],
                        value=json.loads(item['Value'].decode()),
                        version=item['ModifyIndex'],
                        last_modified=datetime.fromtimestamp(
                            item['ModifyIndex']
                        ),
                        encrypted=item.get('Flags', 0) == 1
                    ))
        
        elif self.type == 'etcd':
            async for item in self.client.get_prefix(prefix):
                items.append(ConfigItem(
                    key=item.key.decode(),
                    value=json.loads(item.value.decode()),
                    version=item.metadata.version,
                    last_modified=datetime.fromtimestamp(
                        item.metadata.create_revision
                    ),
                    encrypted=item.metadata.flags == 1
                ))
        
        return items

class ConfigWatcher:
    def __init__(self, store: ConfigStore, callback: callable):
        self.store = store
        self.callback = callback
        self.watching = False
        self._watch_tasks = {}
    
    async def start(self, prefix: str = ""):
        """Start watching configuration changes"""
        self.watching = True
        
        if self.store.type == 'consul':
            asyncio.create_task(self._watch_consul(prefix))
        elif self.store.type == 'etcd':
            asyncio.create_task(self._watch_etcd(prefix))
    
    async def stop(self):
        """Stop watching configuration changes"""
        self.watching = False
        for task in self._watch_tasks.values():
            task.cancel()
    
    async def _watch_consul(self, prefix: str):
        """Watch Consul changes"""
        index = None
        while self.watching:
            try:
                index, data = await self.store.client.kv.get(
                    prefix,
                    index=index,
                    recurse=True
                )
                if data:
                    for item in data:
                        key = item['Key']
                        config_item = await self.store.get(key)
                        if config_item:
                            await self.callback(config_item)
            except Exception as e:
                print(f"Error watching Consul: {e}")
                await asyncio.sleep(5)
    
    async def _watch_etcd(self, prefix: str):
        """Watch etcd changes"""
        async for event in self.store.client.watch_prefix(prefix):
            if not self.watching:
                break
            
            try:
                key = event.key.decode()
                config_item = await self.store.get(key)
                if config_item:
                    await self.callback(config_item)
            except Exception as e:
                print(f"Error watching etcd: {e}")

class ConfigEncryption:
    def __init__(self, key: bytes):
        self.fernet = Fernet(key)
    
    def encrypt(self, value: str) -> str:
        """Encrypt configuration value"""
        return self.fernet.encrypt(value.encode()).decode()
    
    def decrypt(self, value: str) -> str:
        """Decrypt configuration value"""
        return self.fernet.decrypt(value.encode()).decode()

class ConfigurationCenter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.store = ConfigStore(config['store'])
        self.encryption = ConfigEncryption(
            config['encryption']['key'].encode()
        ) if config.get('encryption') else None
        self.cache: Dict[str, ConfigItem] = {}
        self.watchers: List[ConfigWatcher] = []
    
    async def initialize(self):
        """Initialize configuration center"""
        # Load initial configuration
        items = await self.store.list()
        for item in items:
            self.cache[item.key] = item
        
        # Setup watchers
        watcher = ConfigWatcher(
            self.store,
            self._handle_config_change
        )
        await watcher.start()
        self.watchers.append(watcher)
    
    async def get_config(self, 
                        key: str, 
                        default: Any = None) -> Optional[Any]:
        """Get configuration value"""
        if key in self.cache:
            item = self.cache[key]
            value = item.value
            
            if item.encrypted and self.encryption:
                value = self.encryption.decrypt(value)
            
            return value
        
        # Try to get from store
        item = await self.store.get(key)
        if item:
            self.cache[key] = item
            value = item.value
            
            if item.encrypted and self.encryption:
                value = self.encryption.decrypt(value)
            
            return value
        
        return default
    
    async def set_config(self,
                        key: str,
                        value: Any,
                        encrypt: bool = False) -> ConfigItem:
        """Set configuration value"""
        if encrypt and self.encryption:
            value = self.encryption.encrypt(str(value))
        
        item = await self.store.put(key, value, encrypted=encrypt)
        self.cache[key] = item
        return item
    
    async def delete_config(self, key: str):
        """Delete configuration value"""
        await self.store.delete(key)
        if key in self.cache:
            del self.cache[key]
    
    async def _handle_config_change(self, item: ConfigItem):
        """Handle configuration change"""
        self.cache[item.key] = item
