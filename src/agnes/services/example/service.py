from agnes.microservice.service import Microservice, ServiceConfig
from aiohttp import web
import logging
import json

class ExampleService(Microservice):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(ServiceConfig(**config))
        self.logger = logging.getLogger(__name__)
    
    @Microservice.endpoint("/hello", method="GET")
    async def hello(self, request: web.Request) -> web.Response:
        """Simple hello endpoint"""
        return web.json_response({
            "message": "Hello from Example Service!"
        })
    
    @Microservice.endpoint("/echo", method="POST", auth_required=True)
    async def echo(self, request: web.Request) -> web.Response:
        """Echo received data"""
        data = await request.json()
        return web.json_response(data)
    
    @Microservice.event_handler("user.created")
    async def handle_user_created(self, data: Dict[str, Any]):
        """Handle user creation event"""
        self.logger.info(f"New user created: {data}")
        # Process user creation event
        await self.publish_event(
            "user.welcomed",
            {
                "user_id": data["id"],
                "message": "Welcome to our service!"
            }
        )

async def create_service():
    """Create example service"""
    # Load config
    with open("config/services/example.yaml") as f:
        config = yaml.safe_load(f)
    
    # Create service
    service = ExampleService(config)
    
    # Start service
    await service.start()
    
    return service

if __name__ == "__main__":
    # Run service
    asyncio.run(create_service())
