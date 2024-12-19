from typing import Dict, Any, Optional
from agnes.core.engine import AgnesEngine
from agnes.safety.validator import RequestValidator
from agnes.monitoring.logger import APILogger

class AgnesService:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.engine = AgnesEngine(config.get("engine", {}))
        self.validator = RequestValidator(config.get("validation", {}))
        self.logger = APILogger(config.get("logging", {}))
    
    async def process_request(self, 
                            request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process incoming API request"""
        # Log request
        await self.logger.log_request(request_data)
        
        try:
            # Validate request
            validated_data = await self.validator.validate_request(request_data)
            
            # Process with engine
            result = await self.engine.process(validated_data)
            
            # Log response
            await self.logger.log_response(result)
            
            return result
        except Exception as e:
            # Log error
            await self.logger.log_error(str(e))
            raise
    
    async def train_model(self, 
                         training_config: Dict[str, Any]) -> Dict[str, Any]:
        """Handle model training request"""
        await self.logger.log_request(training_config)
        
        try:
            # Validate training config
            validated_config = await self.validator.validate_training_config(
                training_config
            )
            
            # Initialize training
            training_result = await self.engine.train(validated_config)
            
            await self.logger.log_response(training_result)
            return training_result
        except Exception as e:
            await self.logger.log_error(str(e))
            raise
