import pytest
import torch
from agnes.perception.multimodal_pipeline import (
    MultiModalPipeline,
    ModalityEncoder,
    MultiModalFusion
)

@pytest.fixture
def config():
    return {
        "modalities": {
            "text": {
                "input_size": 768,
                "hidden_size": 512,
                "output_size": 256,
                "dropout": 0.1
            },
            "image": {
                "input_size": 2048,
                "hidden_size": 1024,
                "output_size": 256,
                "dropout": 0.1
            }
        },
        "fusion_dim": 256
    }

@pytest.fixture
def pipeline(config):
    return MultiModalPipeline(config)

async def test_modality_encoding(pipeline):
    # Test text encoding
    text_input = torch.randn(1, 768)
    image_input = torch.randn(1, 2048)
    
    inputs = {
        "text": text_input,
        "image": image_input
    }
    
    results = await pipeline.process(inputs)
    assert "modality_features" in results
    assert "fused_output" in results
    
    assert results["modality_features"]["text"].shape == (1, 256)
    assert results["modality_features"]["image"].shape == (1, 256)
    assert results["fused_output"].shape == (1, 256)
