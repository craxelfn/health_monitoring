"""Sends notifications via email/SMS"""
import logging
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class NotificationSender:
    """Sends notifications via email and SMS"""
    
    def __init__(self, 
                 smtp_host: Optional[str] = None,
                 smtp_port: int = 587,
                 smtp_user: Optional[str] = None,
                 smtp_password: Optional[str] = None,
                 email_from: Optional[str] = None,
                 email_to: Optional[str] = None,
                 sms_enabled: bool = False):
        """
        Initialize notification sender
        Args:
            smtp_host: SMTP server host
            smtp_port: SMTP server port
            smtp_user: SMTP username
            smtp_password: SMTP password
            email_from: Sender email address
            email_to: Recipient email address
            sms_enabled: Whether SMS is enabled (placeholder for future implementation)
        """
        self.smtp_host = smtp_host or os.getenv('SMTP_HOST')
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user or os.getenv('SMTP_USER')
        self.smtp_password = smtp_password or os.getenv('SMTP_PASSWORD')
        self.email_from = email_from or os.getenv('EMAIL_FROM', 'alerts@healthmonitoring.com')
        self.email_to = email_to or os.getenv('EMAIL_TO', 'admin@healthmonitoring.com')
        self.sms_enabled = sms_enabled
        
        if self.smtp_host:
            logger.info(f"Notification sender initialized with SMTP: {self.smtp_host}")
        else:
            logger.warning("SMTP not configured. Notifications will be logged only.")
    
    def send_email(self, subject: str, body: str, html_body: Optional[str] = None) -> bool:
        """
        Send email notification
        Args:
            subject: Email subject
            body: Plain text body
            html_body: Optional HTML body
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.smtp_host:
            logger.warning("SMTP not configured. Email not sent.")
            return False
        
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.email_from
            msg['To'] = self.email_to
            
            # Add plain text part
            part1 = MIMEText(body, 'plain')
            msg.attach(part1)
            
            # Add HTML part if provided
            if html_body:
                part2 = MIMEText(html_body, 'html')
                msg.attach(part2)
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.smtp_user and self.smtp_password:
                    server.starttls()
                    server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"Email sent successfully: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}", exc_info=True)
            return False
    
    def send_sms(self, message: str, phone_number: Optional[str] = None) -> bool:
        """
        Send SMS notification (placeholder for future implementation)
        Args:
            message: SMS message
            phone_number: Phone number (optional)
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.sms_enabled:
            logger.debug("SMS not enabled")
            return False
        
        # Placeholder for SMS implementation (Twilio, AWS SNS, etc.)
        logger.info(f"SMS would be sent: {message[:50]}...")
        return False
    
    def send_alert_notification(self, alert_data: Dict[str, Any]) -> bool:
        """
        Send alert notification via email and/or SMS
        Args:
            alert_data: Alert data dictionary
        Returns:
            True if sent successfully, False otherwise
        """
        patient_id = alert_data.get('patient_id', 'unknown')
        anomaly_count = alert_data.get('anomaly_count', 0)
        
        subject = f"🚨 Health Alert: Patient {patient_id} - {anomaly_count} Anomaly(ies) Detected"
        
        # Build email body
        body_lines = [
            f"Health Alert Notification",
            f"=" * 50,
            f"Patient ID: {patient_id}",
            f"Timestamp: {alert_data.get('timestamp', 'unknown')}",
            f"Anomaly Count: {anomaly_count}",
            f"",
            f"Anomalies Detected:",
        ]
        
        for i, anomaly in enumerate(alert_data.get('anomalies', []), 1):
            pred = anomaly.get('prediction', {})
            body_lines.append(f"  {i}. Score: {pred.get('score', 0):.2f}, Risk: {pred.get('risk_level', 'unknown')}")
        
        body = "\n".join(body_lines)
        
        # Build HTML body
        html_body = f"""
        <html>
          <body>
            <h2>🚨 Health Alert Notification</h2>
            <p><strong>Patient ID:</strong> {patient_id}</p>
            <p><strong>Timestamp:</strong> {alert_data.get('timestamp', 'unknown')}</p>
            <p><strong>Anomaly Count:</strong> {anomaly_count}</p>
            <h3>Anomalies Detected:</h3>
            <ul>
        """
        for anomaly in alert_data.get('anomalies', []):
            pred = anomaly.get('prediction', {})
            html_body += f"<li>Score: {pred.get('score', 0):.2f}, Risk: {pred.get('risk_level', 'unknown')}</li>"
        html_body += """
            </ul>
          </body>
        </html>
        """
        
        # Send email
        email_sent = self.send_email(subject, body, html_body)
        
        # Send SMS if enabled
        sms_sent = self.send_sms(f"Alert: Patient {patient_id} has {anomaly_count} anomaly(ies)")
        
        return email_sent or sms_sent

