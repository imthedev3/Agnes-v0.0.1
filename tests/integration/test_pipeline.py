import pytest
from agnes.core.pipeline import Pipeline
from agnes.core.types import Request, Response

@pytest.mark.integration
class TestPipeline:
    @pytest.fixture
    async def pipeline(self, agnes_engine):
        """Create a test pipeline."""
        pipeline = Pipeline(agnes_engine.config)
        await pipeline.initialize()
        yield pipeline
        await pipeline.shutdown()
    
    async def test_basic_request_processing(self, pipeline, mock_request_context):
        request = Request(
            type="test",
            data={"message": "Hello, World!"},
            context=mock_request_context
        )
        
        response = await pipeline.process(request)
        
        assert isinstance(response, Response)
        assert response.status == "success"
    
    async def test_pipeline_error_handling(self, pipeline, mock_request_context):
        request = Request(
            type="invalid",
            data={},
            context=mock_request_context
        )
        
        with pytest.raises(ValueError):
            await pipeline.process(request)
