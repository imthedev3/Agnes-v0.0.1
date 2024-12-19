from typing import Dict, Any, List, Optional, Union
import asyncio
import aiohttp
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
from datetime import datetime
import logging
from dataclasses import dataclass
from enum import Enum
import jinja2
import markdown
import aiofcm
from twilio.rest import Client as TwilioClient
import aioredis
import telegram
import discord
import slack_sdk.web.async_client as slack
import boto3

class NotificationType(Enum):
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    SLACK = "slack"
    DISCORD = "discord"
    TELEGRAM = "telegram"
    WEBHOOK = "webhook"

class NotificationPriority(Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class Notification:
    id: str
    type: NotificationType
    priority: NotificationPriority
    recipient: str
    subject: str
    content: str
    template: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = None
    status: str = "pending"
    sent_at: Optional[datetime] = None
    error: Optional[str] = None

class NotificationSender:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader('templates/notifications')
        )
        self.logger = logging.getLogger(__name__)
        self.cache = aioredis.Redis.from_url(config['cache']['redis_url'])
        self._setup_clients()
    
    def _setup_clients(self):
        """Setup notification clients"""
        # Email
        if self.config['email']['enabled']:
            self.email_config = self.config['email']
        
        # SMS (Twilio)
        if self.config['sms']['enabled']:
            self.twilio = TwilioClient(
                self.config['sms']['account_sid'],
                self.config['sms']['auth_token']
            )
        
        # Push (Firebase)
        if self.config['push']['enabled']:
            self.fcm = aiofcm.FCM(self.config['push']['server_key'])
        
        # Slack
        if self.config['slack']['enabled']:
            self.slack = slack.AsyncWebClient(
                token=self.config['slack']['bot_token']
            )
        
        # Discord
        if self.config['discord']['enabled']:
            self.discord = discord.Client()
        
        # Telegram
        if self.config['telegram']['enabled']:
            self.telegram = telegram.Bot(
                token=self.config['telegram']['bot_token']
            )
    
    async def send(self, notification: Notification):
        """Send notification"""
        try:
            # Rate limiting check
            if not await self._check_rate_limit(notification):
                raise ValueError("Rate limit exceeded")
            
            # Prepare content
            if notification.template:
                notification.content = await self._render_template(
                    notification.template,
                    notification.data or {}
                )
            
            # Send based on type
            if notification.type == NotificationType.EMAIL:
                await self._send_email(notification)
            elif notification.type == NotificationType.SMS:
                await self._send_sms(notification)
            elif notification.type == NotificationType.PUSH:
                await self._send_push(notification)
            elif notification.type == NotificationType.SLACK:
                await self._send_slack(notification)
            elif notification.type == NotificationType.DISCORD:
                await self._send_discord(notification)
            elif notification.type == NotificationType.TELEGRAM:
                await self._send_telegram(notification)
            elif notification.type == NotificationType.WEBHOOK:
                await self._send_webhook(notification)
            
            notification.status = "sent"
            notification.sent_at = datetime.utcnow()
            
            # Store in cache for deduplication
            await self._store_sent(notification)
        
        except Exception as e:
            notification.status = "failed"
            notification.error = str(e)
            self.logger.error(f"Failed to send notification: {e}")
            raise
    
    async def _send_email(self, notification: Notification):
        """Send email notification"""
        msg = MIMEMultipart('alternative')
        msg['Subject'] = notification.subject
        msg['From'] = self.email_config['from_address']
        msg['To'] = notification.recipient
        
        # Plain text and HTML versions
        text_part = MIMEText(notification.content, 'plain')
        html_part = MIMEText(
            markdown.markdown(notification.content),
            'html'
        )
        
        msg.attach(text_part)
        msg.attach(html_part)
        
        async with aiosmtplib.SMTP(
            hostname=self.email_config['smtp_host'],
            port=self.email_config['smtp_port'],
            use_tls=self.email_config['use_tls']
        ) as smtp:
            if self.email_config['username']:
                await smtp.login(
                    self.email_config['username'],
                    self.email_config['password']
                )
            
            await smtp.send_message(msg)
    
    async def _send_sms(self, notification: Notification):
        """Send SMS notification"""
        self.twilio.messages.create(
            body=notification.content,
            to=notification.recipient,
            from_=self.config['sms']['from_number']
        )
    
    async def _send_push(self, notification: Notification):
        """Send push notification"""
        message = aiofcm.Message(
            device_token=notification.recipient,
            notification=aiofcm.Notification(
                title=notification.subject,
                body=notification.content
            ),
            data=notification.data
        )
        
        await self.fcm.send_message(message)
    
    async def _send_slack(self, notification: Notification):
        """Send Slack notification"""
        await self.slack.chat_postMessage(
            channel=notification.recipient,
            text=notification.content,
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": notification.content
                    }
                }
            ]
        )
    
    async def _send_discord(self, notification: Notification):
        """Send Discord notification"""
        webhook = discord.Webhook.from_url(
            notification.recipient,
            adapter=discord.AsyncWebhookAdapter(aiohttp.ClientSession())
        )
        
        embed = discord.Embed(
            title=notification.subject,
            description=notification.content
        )
        
        await webhook.send(embed=embed)
    
    async def _send_telegram(self, notification: Notification):
        """Send Telegram notification"""
        await self.telegram.send_message(
            chat_id=notification.recipient,
            text=notification.content,
            parse_mode='Markdown'
        )
    
    async def _send_webhook(self, notification: Notification):
        """Send webhook notification"""
        async with aiohttp.ClientSession() as session:
            await session.post(
                notification.recipient,
                json={
                    'id': notification.id,
                    'subject': notification.subject,
                    'content': notification.content,
                    'data': notification.data,
                    'metadata': notification.metadata
                },
                headers={
                    'Content-Type': 'application/json',
                    'X-Agnes-Signature': self._generate_signature(notification)
                }
            )
    
    async def _render_template(self,
                             template_name: str,
                             data: Dict[str, Any]) -> str:
        """Render notification template"""
        template = self.template_env.get_template(template_name)
        return template.render(**data)
    
    async def _check_rate_limit(self,
                              notification: Notification) -> bool:
        """Check notification rate limit"""
        key = f"ratelimit:{notification.type.value}:{notification.recipient}"
        count = await self.cache.incr(key)
        
        if count == 1:
            await self.cache.expire(
                key,
                self.config['rate_limits'].get(
                    notification.type.value,
                    3600
                )
            )
        
        max_count = self.config['rate_limits'].get(
            f"{notification.type.value}_count",
            100
        )
        
        return count <= max_count
    
    async def _store_sent(self, notification: Notification):
        """Store sent notification for deduplication"""
        key = f"sent:{notification.id}"
        await self.cache.set(
            key,
            json.dumps({
                'id': notification.id,
                'type': notification.type.value,
                'recipient': notification.recipient,
                'subject': notification.subject,
                'sent_at': notification.sent_at.isoformat()
            }),
            ex=self.config['dedup_ttl']
        )
    
    def _generate_signature(self,
                          notification: Notification) -> str:
        """Generate webhook signature"""
        import hmac
        import hashlib
        
        message = f"{notification.id}:{notification.recipient}"
        signature = hmac.new(
            self.config['webhook']['secret'].encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
        
        return signature

class NotificationBatcher:
    def __init__(self,
                 sender: NotificationSender,
                 config: Dict[str, Any]):
        self.sender = sender
        self.config = config
        self.batches: Dict[str, List[Notification]] = {}
        self.logger = logging.getLogger(__name__)
    
    async def add(self, notification: Notification):
        """Add notification to batch"""
        key = f"{notification.type.value}:{notification.recipient}"
        
        if key not in self.batches:
            self.batches[key] = []
        
        self.batches[key].append(notification)
        
        # Send batch if threshold reached
        if len(self.batches[key]) >= self.config['batch_size']:
            await self._send_batch(key)
    
    async def _send_batch(self, key: str):
        """Send notification batch"""
        if key not in self.batches:
            return
        
        notifications = self.batches[key]
        if not notifications:
            return
        
        try:
            # Combine notifications
            combined = self._combine_notifications(notifications)
            
            # Send combined notification
            await self.sender.send(combined)
            
            # Clear batch
            self.batches[key] = []
        
        except Exception as e:
            self.logger.error(f"Failed to send notification batch: {e}")
    
    def _combine_notifications(self,
                             notifications: List[Notification]) -> Notification:
        """Combine multiple notifications"""
        first = notifications[0]
        
        # Combine content based on type
        if first.type == NotificationType.EMAIL:
            content = self._combine_email_content(notifications)
        else:
            content = "\n\n".join(n.content for n in notifications)
        
        return Notification(
            id=f"batch-{datetime.utcnow().timestamp()}",
            type=first.type,
            priority=max(n.priority for n in notifications),
            recipient=first.recipient,
            subject=f"Combined Notification ({len(notifications)})",
            content=content,
            metadata={
                'batch_size': len(notifications),
                'original_ids': [n.id for n in notifications]
            }
        )
    
    def _combine_email_content(self,
                             notifications: List[Notification]) -> str:
        """Combine email notifications"""
        parts = []
        for notification in notifications:
            parts.append(f"## {notification.subject}\n\n{notification.content}")
        
        return "\n\n---\n\n".join(parts)
