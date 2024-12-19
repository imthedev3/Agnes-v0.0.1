from typing import Dict, Any, List
from dataclasses import dataclass

@dataclass
class NeuralConfig:
    model_type: str
    layers: int
    hidden_size: int
    attention_heads: int

@dataclass
class MemoryConfig:
    storage_type: str
    cache_size: int
    index_type: str
    persistence: Dict[str, Any]

@dataclass
class SafetyConfig:
    monitors: List[Dict[str, Any]]
    sandbox: Dict[str, Any]

@dataclass
class AgnesConfig:
    version: str
    engine: Dict[str, Any]
    neural_models: Dict[str, NeuralConfig]
    memory: MemoryConfig
    safety: SafetyConfig
