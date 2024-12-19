from typing import Dict, Any, Optional, Set, Callable
import asyncio
import json
import aiohttp
from aiohttp import web
import jwt
from dataclasses import dataclass
import logging
from datetime import datetime
import uuid
from enum import Enum

class MessageType(Enum):
    CONNECT = "connect"
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    MESSAGE = "message"
    ERROR = "error"

@dataclass
class WebSocketClient:
    id: str
    socket: web.WebSocketResponse
    user_id: Optional[str] = None
    subscriptions: Set[str] = None
    connected_at: datetime = None
    last_ping: datetime = None

    def __post_init__(self):
        if self.subscriptions is None:
            self.subscriptions = set()
        if self.connected_at is None:
            self.connected_at = datetime.utcnow()

class WebSocketManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.clients: Dict[str, WebSocketClient] = {}
        self.handlers: Dict[str, Callable] = {}
        self.logger = logging.getLogger(__name__)
        self.ping_interval = config.get('ping_interval', 30)
        self.jwt_secret = config['jwt_secret']
    
    async def handle_connection(self, request: web.Request) -> web.WebSocketResponse:
        """Handle new WebSocket connection"""
        ws = web.WebSocketResponse(
            heartbeat=self.ping_interval,
            autoping=True
        )
        await ws.prepare(request)
        
        client = WebSocketClient(
            id=str(uuid.uuid4()),
            socket=ws
        )
        self.clients[client.id] = client
        
        try:
            await self._handle_client_messages(client)
        except Exception as e:
            self.logger.error(f"Error handling client messages: {e}")
        finally:
            await self._disconnect_client(client)
        
        return ws
    
    async def _handle_client_messages(self, client: WebSocketClient):
        """Handle messages from client"""
        async for msg in client.socket:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    message_type = MessageType(data.get('type'))
                    
                    if message_type == MessageType.CONNECT:
                        await self._handle_connect(client, data)
                    elif message_type == MessageType.SUBSCRIBE:
                        await self._handle_subscribe(client, data)
                    elif message_type == MessageType.UNSUBSCRIBE:
                        await self._handle_unsubscribe(client, data)
                    elif message_type == MessageType.MESSAGE:
                        await self._handle_message(client, data)
                
                except Exception as e:
                    await self._send_error(client, str(e))
            
            elif msg.type == aiohttp.WSMsgType.ERROR:
                self.logger.error(
                    f"WebSocket connection closed with error: {ws.exception()}"
                )
    
    async def _handle_connect(self, client: WebSocketClient, data: Dict[str, Any]):
        """Handle client connection"""
        try:
            token = data.get('token')
            if token:
                payload = jwt.decode(
                    token,
                    self.jwt_secret,
                    algorithms=['HS256']
                )
                client.user_id = payload['sub']
            
            await self._send_message(
                client,
                {
                    'type': 'connect',
                    'client_id': client.id,
                    'message': 'Connected successfully'
                }
            )
        
        except jwt.InvalidTokenError:
            await self._send_error(client, "Invalid authentication token")
    
    async def _handle_subscribe(self, client: WebSocketClient, data: Dict[str, Any]):
        """Handle subscription request"""
        channels = data.get('channels', [])
        for channel in channels:
            client.subscriptions.add(channel)
        
        await self._send_message(
            client,
            {
                'type': 'subscribe',
                'channels': list(client.subscriptions)
            }
        )
    
    async def _handle_unsubscribe(self, client: WebSocketClient, data: Dict[str, Any]):
        """Handle unsubscribe request"""
        channels = data.get('channels', [])
        for channel in channels:
            client.subscriptions.discard(channel)
        
        await self._send_message(
            client,
            {
                'type': 'unsubscribe',
                'channels': list(client.subscriptions)
            }
        )
    
    async def _handle_message(self, client: WebSocketClient, data: Dict[str, Any]):
        """Handle client message"""
        message_type = data.get('message_type')
        handler = self.handlers.get(message_type)
        
        if handler:
            try:
                response = await handler(client, data)
                if response:
                    await self._send_message(client, response)
            except Exception as e:
                await self._send_error(client, str(e))
        else:
            await self._send_error(
                client,
                f"Unknown message type: {message_type}"
            )
    
    async def broadcast(self,
                       channel: str,
                       message: Dict[str, Any],
                       exclude_client: Optional[str] = None):
        """Broadcast message to channel subscribers"""
        for client in self.clients.values():
            if (client.id != exclude_client and
                channel in client.subscriptions):
                await self._send_message(client, message)
    
    async def _send_message(self,
                           client: WebSocketClient,
                           message: Dict[str, Any]):
        """Send message to client"""
        try:
            await client.socket.send_json(message)
        except Exception as e:
            self.logger.error(f"Error sending message to client: {e}")
            await self._disconnect_client(client)
    
    async def _send_error(self, client: WebSocketClient, error: str):
        """Send error message to client"""
        await self._send_message(
            client,
            {
                'type': 'error',
                'message': error
            }
        )
    
    async def _disconnect_client(self, client: WebSocketClient):
        """Disconnect client"""
        if client.id in self.clients:
            del self.clients[client.id]
            try:
                await client.socket.close()
            except:
                pass
    
    def register_handler(self,
                        message_type: str,
                        handler: Callable[[WebSocketClient, Dict[str, Any]], Any]):
        """Register message handler"""
        self.handlers[message_type] = handler
    
    async def start_ping(self):
        """Start ping monitoring"""
        while True:
            await asyncio.sleep(self.ping_interval)
            await self._check_clients()
    
    async def _check_clients(self):
        """Check client connections"""
        now = datetime.utcnow()
        disconnected_clients = []
        
        for client in self.clients.values():
            if (client.last_ping and
                (now - client.last_ping).seconds > self.ping_interval * 2):
                disconnected_clients.append(client)
        
        for client in disconnected_clients:
            await self._disconnect_client(client)

class WebSocketServer:
    def __init__(self, app: web.Application, config: Dict[str, Any]):
        self.app = app
        self.config = config
        self.manager = WebSocketManager(config)
        self.setup_routes()
    
    def setup_routes(self):
        """Setup WebSocket routes"""
        self.app.router.add_get(
            '/ws',
            self.manager.handle_connection
        )
    
    async def start(self):
        """Start WebSocket server"""
        asyncio.create_task(self.manager.start_ping())
    
    async def stop(self):
        """Stop WebSocket server"""
        for client in list(self.manager.clients.values()):
            await self.manager._disconnect_client(client)
