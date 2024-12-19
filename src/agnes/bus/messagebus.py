from typing import Dict, Any, List, Optional, Union, Callable
import asyncio
import json
import logging
from datetime import datetime
import uuid
from enum import Enum
import redis.asyncio as redis
from dataclasses import dataclass
import aio_pika
import pickle

class MessagePriority(Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3

@dataclass
class Message:
    id: str
    topic: str
    payload: Dict[str, Any]
    priority: MessagePriority
    created_at: datetime
    headers: Optional[Dict[str, str]] = None
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None

class MessageBus:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.channel = None
        self.exchange = None
        self.queues: Dict[str, aio_pika.Queue] = {}
        self.handlers: Dict[str, List[Callable]] = {}
        self.logger = logging.getLogger(__name__)
    
    async def connect(self):
        """Connect to message broker"""
        self.connection = await aio_pika.connect_robust(
            self.config['rabbitmq_url']
        )
        
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=10)
        
        self.exchange = await self.channel.declare_exchange(
            "agnes",
            aio_pika.ExchangeType.TOPIC,
            durable=True
        )
    
    async def disconnect(self):
        """Disconnect from message broker"""
        if self.connection:
            await self.connection.close()
    
    async def publish(self,
                     topic: str,
                     payload: Dict[str, Any],
                     priority: MessagePriority = MessagePriority.NORMAL,
                     headers: Optional[Dict[str, str]] = None,
                     correlation_id: Optional[str] = None,
                     reply_to: Optional[str] = None):
        """Publish message to topic"""
        message = Message(
            id=str(uuid.uuid4()),
            topic=topic,
            payload=payload,
            priority=priority,
            created_at=datetime.utcnow(),
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to
        )
        
        try:
            await self.exchange.publish(
                aio_pika.Message(
                    body=json.dumps({
                        'id': message.id,
                        'topic': message.topic,
                        'payload': message.payload,
                        'priority': message.priority.value,
                        'created_at': message.created_at.isoformat(),
                        'headers': message.headers,
                        'correlation_id': message.correlation_id,
                        'reply_to': message.reply_to
                    }).encode(),
                    content_type='application/json',
                    priority=message.priority.value,
                    message_id=message.id,
                    correlation_id=message.correlation_id,
                    reply_to=message.reply_to,
                    headers=message.headers
                ),
                routing_key=topic
            )
        except Exception as e:
            self.logger.error(f"Error publishing message: {e}")
            raise
    
    async def subscribe(self,
                       topic: str,
                       handler: Callable[[Message], Any],
                       queue_name: Optional[str] = None):
        """Subscribe to topic"""
        if topic not in self.handlers:
            self.handlers[topic] = []
        
        self.handlers[topic].append(handler)
        
        if not queue_name:
            queue_name = f"agnes.{topic}"
        
        if queue_name not in self.queues:
            queue = await self.channel.declare_queue(
                queue_name,
                durable=True,
                arguments={
                    'x-max-priority': 10
                }
            )
            await queue.bind(self.exchange, topic)
            self.queues[queue_name] = queue
            
            asyncio.create_task(
                self._process_queue(queue)
            )
    
    async def _process_queue(self, queue: aio_pika.Queue):
        """Process messages from queue"""
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        data = json.loads(message.body.decode())
                        msg = Message(
                            id=data['id'],
                            topic=data['topic'],
                            payload=data['payload'],
                            priority=MessagePriority(data['priority']),
                            created_at=datetime.fromisoformat(
                                data['created_at']
                            ),
                            headers=data['headers'],
                            correlation_id=data['correlation_id'],
                            reply_to=data['reply_to']
                        )
                        
                        handlers = self.handlers.get(msg.topic, [])
                        for handler in handlers:
                            try:
                                await handler(msg)
                            except Exception as e:
                                self.logger.error(
                                    f"Error in message handler: {e}"
                                )
                    
                    except Exception as e:
                        self.logger.error(
                            f"Error processing message: {e}"
                        )

class EventBus:
    def __init__(self, message_bus: MessageBus):
        self.message_bus = message_bus
        self.handlers: Dict[str, List[Callable]] = {}
    
    async def publish_event(self,
                          event_type: str,
                          data: Dict[str, Any],
                          priority: MessagePriority = MessagePriority.NORMAL):
        """Publish event"""
        await self.message_bus.publish(
            f"event.{event_type}",
            data,
            priority
        )
    
    async def subscribe_event(self,
                            event_type: str,
                            handler: Callable[[Dict[str, Any]], Any]):
        """Subscribe to event"""
        async def event_handler(message: Message):
            await handler(message.payload)
        
        await self.message_bus.subscribe(
            f"event.{event_type}",
            event_handler
        )

class CommandBus:
    def __init__(self, message_bus: MessageBus):
        self.message_bus = message_bus
        self.handlers: Dict[str, Callable] = {}
    
    async def send_command(self,
                          command: str,
                          data: Dict[str, Any],
                          priority: MessagePriority = MessagePriority.NORMAL) -> Optional[Any]:
        """Send command and wait for response"""
        reply_queue = await self.message_bus.channel.declare_queue(
            "",
            exclusive=True,
            auto_delete=True
        )
        
        correlation_id = str(uuid.uuid4())
        
        future = asyncio.Future()
        
        async def response_handler(message: aio_pika.IncomingMessage):
            if message.correlation_id == correlation_id:
                future.set_result(
                    json.loads(message.body.decode())
                )
        
        await reply_queue.consume(response_handler)
        
        await self.message_bus.publish(
            f"command.{command}",
            data,
            priority,
            correlation_id=correlation_id,
            reply_to=reply_queue.name
        )
        
        try:
            return await asyncio.wait_for(future, timeout=30)
        except asyncio.TimeoutError:
            self.logger.error(f"Command {command} timed out")
            return None
        finally:
            await reply_queue.delete()
    
    async def register_handler(self,
                             command: str,
                             handler: Callable[[Dict[str, Any]], Any]):
        """Register command handler"""
        async def command_handler(message: Message):
            try:
                result = await handler(message.payload)
                
                if message.reply_to:
                    await self.message_bus.channel.default_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(result).encode(),
                            correlation_id=message.correlation_id
                        ),
                        routing_key=message.reply_to
                    )
            
            except Exception as e:
                self.logger.error(f"Error handling command: {e}")
                if message.reply_to:
                    await self.message_bus.channel.default_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps({
                                'error': str(e)
                            }).encode(),
                            correlation_id=message.correlation_id
                        ),
                        routing_key=message.reply_to
                    )
        
        await self.message_bus.subscribe(
            f"command.{command}",
            command_handler
        )
