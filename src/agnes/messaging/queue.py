from typing import Dict, Any, Optional, Callable, List
import asyncio
import json
import aio_pika
from dataclasses import dataclass
import logging
from datetime import datetime
import uuid

@dataclass
class QueueConfig:
    name: str
    durable: bool = True
    auto_delete: bool = False
    arguments: Optional[Dict[str, Any]] = None
    retry_count: int = 3
    retry_delay: int = 5000  # milliseconds
    dlq_suffix: str = "_dlq"

@dataclass
class ExchangeConfig:
    name: str
    type: str = "direct"
    durable: bool = True
    auto_delete: bool = False
    arguments: Optional[Dict[str, Any]] = None

class MessageQueue:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.channel = None
        self.queues: Dict[str, aio_pika.Queue] = {}
        self.exchanges: Dict[str, aio_pika.Exchange] = {}
        self.logger = logging.getLogger(__name__)
    
    async def connect(self):
        """Connect to RabbitMQ"""
        self.connection = await aio_pika.connect_robust(
            self.config['url']
        )
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=1)
    
    async def declare_queue(self, config: QueueConfig):
        """Declare queue with given configuration"""
        # Declare main queue
        queue = await self.channel.declare_queue(
            name=config.name,
            durable=config.durable,
            auto_delete=config.auto_delete,
            arguments={
                **(config.arguments or {}),
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": f"{config.name}{config.dlq_suffix}"
            }
        )
        
        # Declare DLQ
        dlq = await self.channel.declare_queue(
            name=f"{config.name}{config.dlq_suffix}",
            durable=config.durable,
            auto_delete=config.auto_delete,
            arguments=config.arguments
        )
        
        self.queues[config.name] = queue
        self.queues[f"{config.name}{config.dlq_suffix}"] = dlq
        
        return queue
    
    async def declare_exchange(self, config: ExchangeConfig):
        """Declare exchange with given configuration"""
        exchange = await self.channel.declare_exchange(
            name=config.name,
            type=config.type,
            durable=config.durable,
            auto_delete=config.auto_delete,
            arguments=config.arguments
        )
        
        self.exchanges[config.name] = exchange
        return exchange
    
    async def publish(self,
                     exchange: str,
                     routing_key: str,
                     message: Dict[str, Any],
                     headers: Optional[Dict[str, Any]] = None):
        """Publish message to exchange"""
        if exchange not in self.exchanges:
            raise ValueError(f"Exchange {exchange} not declared")
        
        exchange_obj = self.exchanges[exchange]
        
        try:
            await exchange_obj.publish(
                aio_pika.Message(
                    body=json.dumps(message).encode(),
                    headers=headers or {},
                    message_id=str(uuid.uuid4()),
                    timestamp=datetime.utcnow().timestamp(),
                    content_type="application/json"
                ),
                routing_key=routing_key
            )
        except Exception as e:
            self.logger.error(f"Error publishing message: {e}")
            raise
    
    async def consume(self,
                     queue: str,
                     callback: Callable[[Dict[str, Any], Dict[str, Any]], Any],
                     error_callback: Optional[Callable[[Exception], Any]] = None):
        """Consume messages from queue"""
        if queue not in self.queues:
            raise ValueError(f"Queue {queue} not declared")
        
        queue_obj = self.queues[queue]
        
        async def _process_message(message: aio_pika.IncomingMessage):
            async with message.process():
                try:
                    body = json.loads(message.body.decode())
                    await callback(body, message.headers)
                except Exception as e:
                    if error_callback:
                        await error_callback(e)
                    
                    retry_count = message.headers.get('x-retry-count', 0)
                    if retry_count < self.config['retry_count']:
                        # Retry message
                        await self.publish(
                            message.exchange or "",
                            message.routing_key,
                            body,
                            {
                                **message.headers,
                                'x-retry-count': retry_count + 1,
                                'x-original-error': str(e)
                            }
                        )
                    else:
                        # Move to DLQ
                        await self.publish(
                            "",
                            f"{queue}{self.config['dlq_suffix']}",
                            body,
                            {
                                **message.headers,
                                'x-error': str(e),
                                'x-original-routing-key': message.routing_key
                            }
                        )
        
        await queue_obj.consume(_process_message)
    
    async def process_dlq(self,
                         queue: str,
                         max_retry: Optional[int] = None):
        """Process messages in DLQ"""
        dlq_name = f"{queue}{self.config['dlq_suffix']}"
        if dlq_name not in self.queues:
            raise ValueError(f"DLQ {dlq_name} not declared")
        
        dlq = self.queues[dlq_name]
        
        while True:
            message = await dlq.get()
            if message is None:
                break
            
            try:
                # Get original routing information
                routing_key = message.headers.get('x-original-routing-key', queue)
                retry_count = message.headers.get('x-retry-count', 0)
                
                if max_retry and retry_count >= max_retry:
                    # Permanently failed
                    self.logger.error(
                        f"Message permanently failed: {message.body.decode()}"
                    )
                    continue
                
                # Republish to original queue
                await self.publish(
                    "",
                    routing_key,
                    json.loads(message.body.decode()),
                    {
                        **message.headers,
                        'x-retry-count': retry_count + 1,
                        'x-reprocessed-at': datetime.utcnow().isoformat()
                    }
                )
                
                await message.ack()
            
            except Exception as e:
                self.logger.error(f"Error processing DLQ message: {e}")
                await message.reject()

class MessageBroker:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.queue = MessageQueue(config)
        self.subscribers: Dict[str, List[Callable]] = {}
    
    async def initialize(self):
        """Initialize message broker"""
        await self.queue.connect()
        
        # Setup exchanges and queues
        for exchange_config in self.config['exchanges']:
            await self.queue.declare_exchange(ExchangeConfig(**exchange_config))
        
        for queue_config in self.config['queues']:
            await self.queue.declare_queue(QueueConfig(**queue_config))
    
    async def publish(self,
                     topic: str,
                     message: Dict[str, Any],
                     headers: Optional[Dict[str, Any]] = None):
        """Publish message to topic"""
        await self.queue.publish(
            self.config['default_exchange'],
            topic,
            message,
            headers
        )
    
    async def subscribe(self,
                       topic: str,
                       callback: Callable[[Dict[str, Any], Dict[str, Any]], Any]):
        """Subscribe to topic"""
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        
        self.subscribers[topic].append(callback)
        
        await self.queue.consume(
            topic,
            callback
        )
