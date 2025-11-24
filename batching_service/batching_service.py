"""Batching service that groups events into 3-second time windows"""
import sys
import os
import json
import logging
import threading
import time
from typing import Dict, Any
from confluent_kafka import Consumer, Producer

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from time_window_batcher import TimeWindowBatcher

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchingService:
    """Batches validated events into 3-second time windows"""
    
    def __init__(self, bootstrap_servers: str, input_topic: str, output_topic: str, window_seconds: int = 3):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.batcher = TimeWindowBatcher(window_seconds=window_seconds)
        
        # Configure consumer
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'batching-service',
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
        
        # Start periodic flush thread
        self.running = True
        self.flush_thread = threading.Thread(target=self._periodic_flush, daemon=True)
        self.flush_thread.start()
        
        logger.info(f"Initialized batching service: input={input_topic}, output={output_topic}, window={window_seconds}s")
    
    def _periodic_flush(self):
        """Periodically flush batches that have timed out"""
        while self.running:
            try:
                time.sleep(0.5)  # Check every 500ms
                pending = self.batcher.get_pending_batches()
                
                for patient_id, events in pending.items():
                    if events:
                        self._publish_batch(patient_id, events)
                        
            except Exception as e:
                logger.error(f"Error in periodic flush: {e}", exc_info=True)
                time.sleep(1)
    
    def _publish_batch(self, patient_id: str, events: list):
        """Publish a batch to Kafka"""
        try:
            batch_message = {
                'patient_id': patient_id,
                'events': events,
                'batch_size': len(events),
                'timestamp': time.time()
            }
            
            self.producer.produce(
                self.output_topic,
                key=patient_id.encode('utf-8'),
                value=json.dumps(batch_message).encode('utf-8'),
                callback=self._delivery_callback
            )
            self.producer.poll(0)
            
            logger.info(f"Published batch for patient {patient_id}: {len(events)} events")
            
        except Exception as e:
            logger.error(f"Error publishing batch: {e}", exc_info=True)
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery"""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Batch delivered to {msg.topic()} [{msg.partition()}]")
    
    def process_stream(self):
        """Main processing loop"""
        logger.info("Starting batching stream processing...")
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                key = msg.key().decode('utf-8') if msg.key() else None
                value = msg.value().decode('utf-8')
                
                try:
                    event = json.loads(value)
                    patient_id = event.get('patient_id') or key or 'unknown'
                    
                    # Add event to batch
                    completed_batch = self.batcher.add_event(patient_id, event)
                    
                    # If window is complete, publish immediately
                    if completed_batch:
                        self._publish_batch(patient_id, completed_batch)
                    
                    logger.debug(f"Added event to batch for patient {patient_id}")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse event: {e}")
                except Exception as e:
                    logger.error(f"Error processing event: {e}", exc_info=True)
                    
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.running = False
            self.consumer.close()
            self.producer.flush()
            logger.info("Batching service stopped")


def main():
    """Main entry point"""
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'validated_data')
    output_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'inference_request')
    window_seconds = int(os.getenv('BATCH_WINDOW_SECONDS', '3'))
    
    service = BatchingService(bootstrap_servers, input_topic, output_topic, window_seconds)
    service.process_stream()


if __name__ == '__main__':
    main()

