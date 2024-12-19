from typing import Dict, Any, List, Optional, Union, Callable
import asyncio
import json
from datetime import datetime
import aiohttp
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dataclasses import dataclass
import logging
from enum import Enum
import jinja2
import yaml

class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class AlertStatus(Enum):
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"

@dataclass
class Alert:
    id: str
    name: str
    severity: AlertSeverity
    status: AlertStatus
    message: str
    source: str
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None

class AlertRule:
    def __init__(self,
                 name: str,
                 condition: str,
                 severity: AlertSeverity,
                 message_template: str):
        self.name = name
        self.condition = condition
        self.severity = severity
        self.message_template = message_template
        self.template = jinja2.Template(message_template)
    
    def evaluate(self, context: Dict[str, Any]) -> Optional[str]:
        """Evaluate alert condition"""
        try:
            if eval(self.condition, {"context": context}):
                return self.template.render(context)
            return None
        except Exception as e:
            logging.error(f"Error evaluating alert rule {self.name}: {e}")
            return None

class AlertNotifier:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader('templates')
        )
    
    async def notify(self, alert: Alert, channels: List[str]):
        """Send alert notifications"""
        for channel in channels:
            if channel == 'email':
                await self._send_email(alert)
            elif channel == 'slack':
                await self._send_slack(alert)
            elif channel == 'webhook':
                await self._send_webhook(alert)
    
    async def _send_email(self, alert: Alert):
        """Send email notification"""
        template = self.template_env.get_template('alert_email.html')
        html_content = template.render(alert=alert)
        
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"[{alert.severity.value.upper()}] {alert.name}"
        msg['From'] = self.config['email']['from']
        msg['To'] = ', '.join(self.config['email']['recipients'])
        
        msg.attach(MIMEText(html_content, 'html'))
        
        try:
            with smtplib.SMTP(
                self.config['email']['smtp_host'],
                self.config['email']['smtp_port']
            ) as server:
                server.starttls()
                server.login(
                    self.config['email']['username'],
                    self.config['email']['password']
                )
                server.send_message(msg)
        except Exception as e:
            logging.error(f"Error sending email notification: {e}")
    
    async def _send_slack(self, alert: Alert):
        """Send Slack notification"""
        template = self.template_env.get_template('alert_slack.json')
        payload = template.render(alert=alert)
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.config['slack']['webhook_url'],
                    json=json.loads(payload)
                ) as response:
                    if response.status != 200:
                        logging.error(
                            f"Error sending Slack notification: "
                            f"{await response.text()}"
                        )
        except Exception as e:
            logging.error(f"Error sending Slack notification: {e}")
    
    async def _send_webhook(self, alert: Alert):
        """Send webhook notification"""
        payload = {
            "id": alert.id,
            "name": alert.name,
            "severity": alert.severity.value,
            "status": alert.status.value,
            "message": alert.message,
            "source": alert.source,
            "timestamp": alert.timestamp.isoformat(),
            "metadata": alert.metadata
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.config['webhook']['url'],
                    json=payload,
                    headers=self.config['webhook'].get('headers', {})
                ) as response:
                    if response.status not in (200, 201):
                        logging.error(
                            f"Error sending webhook notification: "
                            f"{await response.text()}"
                        )
        except Exception as e:
            logging.error(f"Error sending webhook notification: {e}")

class AlertManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.rules: Dict[str, AlertRule] = {}
        self.alerts: Dict[str, Alert] = {}
        self.notifier = AlertNotifier(config)
        self._load_rules()
    
    def _load_rules(self):
        """Load alert rules from configuration"""
        for rule_config in self.config['rules']:
            rule = AlertRule(
                name=rule_config['name'],
                condition=rule_config['condition'],
                severity=AlertSeverity(rule_config['severity']),
                message_template=rule_config['message']
            )
            self.rules[rule.name] = rule
    
    async def evaluate_rules(self, context: Dict[str, Any]):
        """Evaluate all alert rules"""
        for rule in self.rules.values():
            message = rule.evaluate(context)
            if message:
                await self.create_alert(
                    name=rule.name,
                    severity=rule.severity,
                    message=message,
                    source=context.get('source', 'system'),
                    metadata=context
                )
    
    async def create_alert(self,
                          name: str,
                          severity: AlertSeverity,
                          message: str,
                          source: str,
                          metadata: Optional[Dict[str, Any]] = None):
        """Create new alert"""
        alert = Alert(
            id=f"{name}_{datetime.utcnow().timestamp()}",
            name=name,
            severity=severity,
            status=AlertStatus.ACTIVE,
            message=message,
            source=source,
            timestamp=datetime.utcnow(),
            metadata=metadata
        )
        
        self.alerts[alert.id] = alert
        
        # Send notifications
        channels = self._get_notification_channels(severity)
        await self.notifier.notify(alert, channels)
    
    def _get_notification_channels(self, 
                                 severity: AlertSeverity) -> List[str]:
        """Get notification channels for severity level"""
        channels = []
        for channel, config in self.config['notifications'].items():
            if severity.value in config['severities']:
                channels.append(channel)
        return channels
    
    async def acknowledge_alert(self,
                              alert_id: str,
                              user: str):
        """Acknowledge alert"""
        if alert_id not in self.alerts:
            raise ValueError(f"Alert {alert_id} not found")
        
        alert = self.alerts[alert_id]
        alert.status = AlertStatus.ACKNOWLEDGED
        alert.acknowledged_by = user
        alert.acknowledged_at = datetime.utcnow()
    
    async def resolve_alert(self, alert_id: str):
        """Resolve alert"""
        if alert_id not in self.alerts:
            raise ValueError(f"Alert {alert_id} not found")
        
        alert = self.alerts[alert_id]
        alert.status = AlertStatus.RESOLVED
        alert.resolved_at = datetime.utcnow()
    
    def get_active_alerts(self,
                         severity: Optional[AlertSeverity] = None) -> List[Alert]:
        """Get active alerts"""
        alerts = [
            alert for alert in self.alerts.values()
            if alert.status == AlertStatus.ACTIVE
        ]
        
        if severity:
            alerts = [
                alert for alert in alerts
                if alert.severity == severity
            ]
        
        return sorted(
            alerts,
            key=lambda x: x.timestamp,
            reverse=True
        )
    
    async def cleanup_resolved_alerts(self,
                                    days: int = 30):
        """Cleanup old resolved alerts"""
        cutoff = datetime.utcnow() - timedelta(days=days)
        alerts_to_remove = [
            alert_id
            for alert_id, alert in self.alerts.items()
            if (alert.status == AlertStatus.RESOLVED and
                alert.resolved_at < cutoff)
        ]
        
        for alert_id in alerts_to_remove:
            del self.alerts[alert_id]
