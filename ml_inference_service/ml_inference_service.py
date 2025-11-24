"""ML Inference Service that runs PyTorch model on batches"""
import sys
import os
import json
import logging
from typing import Dict, Any
from confluent_kafka import Consumer, Producer

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from model_loader import ModelLoader
from feature_extractor import FeatureExtractor

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MLInferenceService:
    """Runs PyTorch ML model on batched events"""
    
    def __init__(self, bootstrap_servers: str, input_topic: str, output_topic: str, model_path: str = None):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        
        # Initialize model and feature extractor
        self.model_loader = ModelLoader(model_path)
        self.feature_extractor = FeatureExtractor()
        
        # Configure consumer
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'ml-inference-service',
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
        
        logger.info(f"Initialized ML Inference Service: input={input_topic}, output={output_topic}")
    
    def _process_batch(self, batch_message: Dict[str, Any]) -> Dict[str, Any]:
        """Process a batch through the ML model"""
        events = batch_message.get('events', [])
        patient_id = batch_message.get('patient_id', 'unknown')
        
        if not events:
            logger.warning(f"Empty batch for patient {patient_id}")
            return None
        
        # Extract features
        features = self.feature_extractor.extract_features(events)
        
        # Run inference
        predictions = self.model_loader.predict(features)
        
        # Create result message
        result = {
            'patient_id': patient_id,
            'batch_size': len(events),
            'events': events,
            'predictions': predictions,
            'timestamp': batch_message.get('timestamp')
        }
        
        logger.info(f"Processed batch for patient {patient_id}: {len(events)} events, {sum(1 for p in predictions if p.get('anomaly', False))} anomalies")
        
        return result
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery"""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Prediction delivered to {msg.topic()} [{msg.partition()}]")
    
    def process_stream(self):
        """Main processing loop"""
        logger.info("Starting ML inference stream processing...")
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
                    batch_message = json.loads(value)
                    patient_id = batch_message.get('patient_id') or key or 'unknown'
                    
                    # Process batch through ML model
                    result = self._process_batch(batch_message)
                    
                    if result:
                        # Publish predictions
                        self.producer.produce(
                            self.output_topic,
                            key=patient_id.encode('utf-8'),
                            value=json.dumps(result).encode('utf-8'),
                            callback=self._delivery_callback
                        )
                        self.producer.poll(0)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse batch message: {e}")
                except Exception as e:
                    logger.error(f"Error processing batch: {e}", exc_info=True)
                    
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.consumer.close()
            self.producer.flush()
            logger.info("ML inference service stopped")


def main():
    """Main entry point"""
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'inference_request')
    output_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'inference_result')
    model_path = os.getenv('ML_MODEL_PATH', None)
    
    service = MLInferenceService(bootstrap_servers, input_topic, output_topic, model_path)
    service.process_stream()


if __name__ == '__main__':
    main()

