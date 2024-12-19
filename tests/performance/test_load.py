import pytest
import asyncio
from agnes.core.engine import AgnesEngine
from agnes.core.types import Request

@pytest.mark.performance
class TestLoadPerformance:
    @pytest.fixture
    async def engine(self, config_manager):
        engine = AgnesEngine(config_manager.config)
        await engine.initialize()
        yield engine
        await engine.shutdown()
    
    async def test_concurrent_requests(self, engine, mock_request_context):
        num_requests = 100
        
        async def make_request():
            request = Request(
                type="test",
                data={"message": "Test"},
                context=mock_request_context
            )
            return await engine.process_request(request)
        
        tasks = [make_request() for _ in range(num_requests)]
        responses = await asyncio.gather(*tasks)
        
        assert len(responses) == num_requests
        assert all(r.status == "success" for r in responses)
