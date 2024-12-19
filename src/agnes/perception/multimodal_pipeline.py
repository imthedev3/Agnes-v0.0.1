from typing import Dict, Any, List, Union
import torch
import torch.nn as nn
from dataclasses import dataclass
from PIL import Image
import numpy as np

@dataclass
class ModalityConfig:
    input_size: int
    hidden_size: int
    output_size: int
    dropout: float

class ModalityEncoder(nn.Module):
    def __init__(self, config: ModalityConfig):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(config.input_size, config.hidden_size),
            nn.ReLU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden_size, config.output_size)
        )
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.encoder(x)

class MultiModalFusion(nn.Module):
    def __init__(self, modality_dims: Dict[str, int], fusion_dim: int):
        super().__init__()
        self.modality_dims = modality_dims
        self.fusion_dim = fusion_dim
        
        # Create projection layers for each modality
        self.projections = nn.ModuleDict({
            modality: nn.Linear(dim, fusion_dim)
            for modality, dim in modality_dims.items()
        })
        
        # Multi-head attention for fusion
        self.attention = nn.MultiheadAttention(
            embed_dim=fusion_dim,
            num_heads=8
        )
        
        # Final fusion layer
        self.fusion_layer = nn.Sequential(
            nn.Linear(fusion_dim, fusion_dim),
            nn.ReLU(),
            nn.Linear(fusion_dim, fusion_dim)
        )
    
    def forward(self, 
                modality_outputs: Dict[str, torch.Tensor]) -> torch.Tensor:
        # Project each modality to common space
        projected_features = []
        for modality, features in modality_outputs.items():
            if modality in self.projections:
                projected = self.projections[modality](features)
                projected_features.append(projected)
        
        # Stack features for attention
        stacked_features = torch.stack(projected_features, dim=0)
        
        # Apply self-attention
        attended_features, _ = self.attention(
            stacked_features,
            stacked_features,
            stacked_features
        )
        
        # Mean pool across modalities
        fused = torch.mean(attended_features, dim=0)
        
        # Final fusion
        output = self.fusion_layer(fused)
        return output

class MultiModalPipeline:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._setup_encoders()
        self._setup_fusion()
    
    def _setup_encoders(self):
        """Setup encoders for each modality"""
        self.encoders = {}
        for modality, cfg in self.config['modalities'].items():
            self.encoders[modality] = ModalityEncoder(ModalityConfig(**cfg))
    
    def _setup_fusion(self):
        """Setup fusion module"""
        modality_dims = {
            modality: cfg['output_size']
            for modality, cfg in self.config['modalities'].items()
        }
        self.fusion_module = MultiModalFusion(
            modality_dims,
            self.config['fusion_dim']
        )
    
    async def process(self, 
                     inputs: Dict[str, Any]) -> Dict[str, torch.Tensor]:
        """Process multi-modal inputs"""
        # Encode each modality
        encoded_features = {}
        for modality, encoder in self.encoders.items():
            if modality in inputs:
                features = await self._preprocess_modality(
                    modality,
                    inputs[modality]
                )
                encoded_features[modality] = encoder(features)
        
        # Fuse modalities
        fused_output = self.fusion_module(encoded_features)
        
        return {
            "modality_features": encoded_features,
            "fused_output": fused_output
        }
    
    async def _preprocess_modality(self, 
                                 modality: str, 
                                 data: Any) -> torch.Tensor:
        """Preprocess different modality inputs"""
        if modality == "text":
            # Implement text preprocessing
            return torch.randn(1, self.config['modalities']['text']['input_size'])
        elif modality == "image":
            # Implement image preprocessing
            return torch.randn(1, self.config['modalities']['image']['input_size'])
        elif modality == "audio":
            # Implement audio preprocessing
            return torch.randn(1, self.config['modalities']['audio']['input_size'])
        else:
            raise ValueError(f"Unknown modality: {modality}")
