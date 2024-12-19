import pytest
from agnes.core.engine import AgnesEngine, EngineConfig

@pytest.fixture
def engine():
    config = EngineConfig(
        memory_size=1000,
        max_planning_depth=5,
        safety_checks=["input", "output"],
        neural_models={"test": "mock"},
        compute_resources={}
    )
    return AgnesEngine(config)

async def test_engine_initialization(engine):
    assert isinstance(engine, AgnesEngine)

async def test_basic_processing(engine):
    result = await engine.process({"test": "data"})
    assert isinstance(result, dict)
