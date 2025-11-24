"""Kafka Stream Processor for validation service"""
import sys
import os
import logging
from typing import Optional
from confluent_kafka import Consumer, Producer

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from event_processing_service import EventProcessingService

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ValidationService:
    """Kafka Stream Processor for sensor events validation"""
    
    def __init__(self, bootstrap_servers: str, input_topic: str, output_topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.processing_service = EventProcessingService()
        
        # Configure consumer
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'validation-service',
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
        
        logger.info(f"Initialized validation service: input={input_topic}, output={output_topic}")
    
    def _safely_process_event(self, value: str) -> Optional[str]:
        try:
            return self.processing_service.process(value)
        except Exception as e:
            logger.error(f"Unhandled error while processing event payload: {value}", exc_info=True)
            return None
    
    def _delivery_callback(self, err, msg):
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def process_stream(self):
        """Main processing loop"""
        logger.info("Starting validation stream processing...")
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
                
                logger.debug(f"Received event for patient {key} -> {value}")
                
                processed_value = self._safely_process_event(value)
                
                if processed_value:
                    logger.debug(f"Processed event for patient {key} -> {processed_value}")
                    self.producer.produce(
                        self.output_topic,
                        key=key,
                        value=processed_value.encode('utf-8'),
                        callback=self._delivery_callback
                    )
                    self.producer.poll(0)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.consumer.close()
            self.producer.flush()
            logger.info("Validation service stopped")


def main():
    """Main entry point"""
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'sensor_raw')
    output_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'validated_data')
    
    service = ValidationService(bootstrap_servers, input_topic, output_topic)
    service.process_stream()


if __name__ == '__main__':
    main()

