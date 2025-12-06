"""Alert Service that processes anomaly alerts"""
import sys
import os
import json
import logging
from typing import Dict, Any
from confluent_kafka import Consumer, Producer

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from redis_deduplicator import RedisDeduplicator
from notification_sender import NotificationSender
from alert_logger import AlertLogger

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AlertService:
    """Processes anomaly alerts with deduplication and notifications"""
    
    def __init__(self, bootstrap_servers: str, input_topic: str,
                 redis_host: str = 'redis', redis_port: int = 6379,
                 deduplication_ttl: int = 300):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        
        # Initialize components
        self.deduplicator = RedisDeduplicator(
            redis_host=redis_host,
            redis_port=redis_port,
            ttl_seconds=deduplication_ttl
        )
        self.notification_sender = NotificationSender()
        self.alert_logger = AlertLogger()
        
        # Configure consumer
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'alert-service',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([input_topic])
        
        logger.info(f"Initialized Alert Service: input={input_topic}")
    
    def _process_alert(self, alert_data: Dict[str, Any]) -> bool:
        """
        Process an alert: check deduplication, send notification, log
        Args:
            alert_data: Alert data dictionary
        Returns:
            True if alert was processed, False if duplicate
        """
        patient_id = alert_data.get('patient_id', 'unknown')
        anomaly_count = alert_data.get('anomaly_count', 0)
        
        # Generate alert key for deduplication
        alert_type = 'anomaly'  # Could be more specific based on alert data
        alert_key = self.deduplicator.generate_alert_key(patient_id, alert_type)
        
        # Check if duplicate
        if self.deduplicator.is_duplicate(alert_key):
            logger.debug(f"Duplicate alert detected and skipped: {alert_key}")
            return False
        
        # Mark as sent (before sending to avoid race conditions)
        self.deduplicator.mark_sent(alert_key)
        
        # Send notification
        notification_sent = self.notification_sender.send_alert_notification(alert_data)
        if notification_sent:
            logger.info(f"Notification sent for patient {patient_id}")
        else:
            logger.warning(f"Notification failed for patient {patient_id}")
        
        # Log alert for auditing
        self.alert_logger.log_alert(alert_data)
        
        return True
    
    def process_stream(self):
        """Main processing loop"""
        logger.info("Starting alert service stream processing...")
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                key = msg.key().decode('utf-8') if msg.key() else None
                value = msg.value().decode('utf-8')
                
                try:
                    alert_data = json.loads(value)
                    patient_id = alert_data.get('patient_id') or key or 'unknown'
                    
                    # Process alert
                    processed = self._process_alert(alert_data)
                    
                    if processed:
                        logger.info(f"Processed alert for patient {patient_id}")
                    else:
                        logger.debug(f"Skipped duplicate alert for patient {patient_id}")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse alert: {e}")
                except Exception as e:
                    logger.error(f"Error processing alert: {e}", exc_info=True)
                    
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.consumer.close()
            logger.info("Alert service stopped")


def main():
    """Main entry point"""
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'anomaly_alert')
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', '6379'))
    deduplication_ttl = int(os.getenv('DEDUPLICATION_TTL_SECONDS', '300'))
    
    service = AlertService(
        bootstrap_servers,
        input_topic,
        redis_host,
        redis_port,
        deduplication_ttl
    )
    service.process_stream()


if __name__ == '__main__':
    main()

