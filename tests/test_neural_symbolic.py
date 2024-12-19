import pytest
import torch
from agnes.brain.neural_symbolic import NeuralSymbolicProcessor
from agnes.brain.knowledge_graph import KnowledgeGraphManager, Entity, Relation

@pytest.fixture
def neural_symbolic_config():
    return {
        "input_size": 768,
        "hidden_size": 512,
        "output_size": 256,
        "rules": {
            "rule1": {"condition": "test", "action": "test"}
        },
        "knowledge_base": {
            "entities": [],
            "relations": []
        }
    }

@pytest.fixture
def processor(neural_symbolic_config):
    return NeuralSymbolicProcessor(neural_symbolic_config)

async def test_neural_processing(processor):
    input_data = {"text": "test input"}
    result = await processor.process(input_data)
    assert "neural_output" in result
    assert isinstance(result["neural_output"], torch.Tensor)

@pytest.fixture
def knowledge_graph():
    config = {
        "max_entities": 1000,
        "max_relations": 5000
    }
    return KnowledgeGraphManager(config)

async def test_knowledge_graph_operations(knowledge_graph):
    # Test entity addition
    entity1 = Entity(id="e1", type="test", attributes={"name": "Entity 1"})
    entity2 = Entity(id="e2", type="test", attributes={"name": "Entity 2"})
    
    assert await knowledge_graph.add_entity(entity1)
    assert await knowledge_graph.add_entity(entity2)
    
    # Test relation addition
    relation = Relation(source="e1", target="e2", type="test", weight=1.0)
    assert await knowledge_graph.add_relation(relation)
    
    # Test path finding
    path = await knowledge_graph.find_path("e1", "e2")
    assert path == ["e1", "e2"]
