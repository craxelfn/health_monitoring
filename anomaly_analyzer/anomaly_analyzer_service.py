"""Anomaly Analyzer Service that checks predictions and publishes alerts"""
import sys
import os
import json
import logging
from datetime import datetime
from typing import Dict, Any
from confluent_kafka import Consumer, Producer

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from anomaly_threshold_checker import AnomalyThresholdChecker

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AnomalyAnalyzerService:
    """Analyzes ML predictions and publishes alerts and storage events"""
    
    def __init__(self, bootstrap_servers: str, input_topic: str, alert_topic: str, storage_topic: str,
                 critical_threshold: float = 0.7):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.alert_topic = alert_topic
        self.storage_topic = storage_topic
        self.threshold_checker = AnomalyThresholdChecker(critical_threshold)
        
        # Configure consumer
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'anomaly-analyzer-service',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([input_topic])
        
        # Configure producer
        producer_config = {
            'bootstrap.servers': bootstrap_servers
        }
        self.producer = Producer(producer_config)
        
        logger.info(f"Initialized Anomaly Analyzer: input={input_topic}, alert={alert_topic}, storage={storage_topic}")
    
    def _publish_alert(self, patient_id: str, result: Dict[str, Any], anomalies: list):
        """Publish anomaly alert to Kafka"""
        try:
            alert = {
                'patient_id': patient_id,
                'timestamp': datetime.now().isoformat(),
                'batch_size': result.get('batch_size', 0),
                'anomaly_count': len(anomalies),
                'anomalies': anomalies,
                'events': result.get('events', []),
                'predictions': result.get('predictions', [])
            }
            
            self.producer.produce(
                self.alert_topic,
                key=patient_id.encode('utf-8'),
                value=json.dumps(alert).encode('utf-8'),
                callback=self._delivery_callback
            )
            self.producer.poll(0)
            
            logger.warning(f"Published anomaly alert for patient {patient_id}: {len(anomalies)} anomalies")
            
        except Exception as e:
            logger.error(f"Error publishing anomaly alert: {e}", exc_info=True)
    
    def _publish_storage_event(self, patient_id: str, result: Dict[str, Any]):
        """Publish data to storage_event topic for archival"""
        try:
            storage_event = {
                'patient_id': patient_id,
                'timestamp': datetime.now().isoformat(),
                'batch_size': result.get('batch_size', 0),
                'events': result.get('events', []),
                'predictions': result.get('predictions', []),
                'has_anomaly': any(p.get('anomaly', False) for p in result.get('predictions', []))
            }
            
            self.producer.produce(
                self.storage_topic,
                key=patient_id.encode('utf-8'),
                value=json.dumps(storage_event).encode('utf-8'),
                callback=self._delivery_callback
            )
            self.producer.poll(0)
            
            logger.debug(f"Published storage event for patient {patient_id}")
            
        except Exception as e:
            logger.error(f"Error publishing storage event: {e}", exc_info=True)
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery"""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def process_stream(self):
        """Main processing loop"""
        logger.info("Starting anomaly analyzer stream processing...")
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
                    result = json.loads(value)
                    patient_id = result.get('patient_id') or key or 'unknown'
                    
                    predictions = result.get('predictions', [])
                    
                    # Check for anomalies
                    has_anomaly = self.threshold_checker.check_anomalies(predictions)
                    
                    if has_anomaly:
                        # Get anomaly details
                        anomalies = self.threshold_checker.get_anomalies(predictions)
                        # Publish alert
                        self._publish_alert(patient_id, result, anomalies)
                    
                    # Always publish to storage (for archival)
                    self._publish_storage_event(patient_id, result)
                    
                    logger.debug(f"Processed inference result for patient {patient_id}: {len(predictions)} predictions, {sum(1 for p in predictions if p.get('anomaly', False))} anomalies")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse inference result: {e}")
                except Exception as e:
                    logger.error(f"Error processing inference result: {e}", exc_info=True)
                    
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.consumer.close()
            self.producer.flush()
            logger.info("Anomaly analyzer service stopped")


def main():
    """Main entry point"""
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'inference_result')
    alert_topic = os.getenv('KAFKA_ALERT_TOPIC', 'anomaly_alert')
    storage_topic = os.getenv('KAFKA_STORAGE_TOPIC', 'storage_event')
    critical_threshold = float(os.getenv('CRITICAL_THRESHOLD', '0.7'))
    
    service = AnomalyAnalyzerService(
        bootstrap_servers,
        input_topic,
        alert_topic,
        storage_topic,
        critical_threshold
    )
    service.process_stream()


if __name__ == '__main__':
    main()

