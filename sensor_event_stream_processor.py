#!/usr/bin/env python3
"""
Sensor Event Stream Processor
Processes sensor events from Kafka, validates, normalizes, enriches, and publishes to output topic.
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional, Dict, Any, List
from confluent_kafka import Consumer, Producer

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ValidationResult:
    """Validation result with errors"""
    def __init__(self, valid: bool, errors: List[str] = None):
        self.valid = valid
        self.errors = errors or []
    
    @staticmethod
    def valid_result():
        return ValidationResult(True, [])
    
    @staticmethod
    def invalid_result(error: str):
        return ValidationResult(False, [error])
    
    def combine(self, other: 'ValidationResult') -> 'ValidationResult':
        if self.valid and other.valid:
            return ValidationResult.valid_result()
        all_errors = self.errors + other.errors
        return ValidationResult(False, all_errors)


class RequiredFieldsValidator:
    """Validates required fields are present"""
    
    def validate(self, event: Dict[str, Any]) -> ValidationResult:
        if not event.get('patient_id'):
            return ValidationResult.invalid_result("Patient ID is required")
        if not event.get('message_id'):
            return ValidationResult.invalid_result("Message ID is required")
        if not event.get('timestamp'):
            return ValidationResult.invalid_result("Timestamp is required")
        heart_rate = event.get('heart_rate')
        if heart_rate is None or heart_rate <= 0:
            return ValidationResult.invalid_result("Valid heart rate is required")
        return ValidationResult.valid_result()


class VitalSignsRangeValidator:
    """Validates vital signs are within acceptable ranges"""
    
    MIN_HEART_RATE = 30.0
    MAX_HEART_RATE = 250.0
    MIN_OXYGEN_SATURATION = 70.0
    MAX_OXYGEN_SATURATION = 100.0
    MIN_TEMPERATURE = 35.0
    MAX_TEMPERATURE = 42.0
    
    def validate(self, event: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult.valid_result()
        
        heart_rate = event.get('heart_rate')
        if heart_rate is not None:
            if heart_rate < self.MIN_HEART_RATE or heart_rate > self.MAX_HEART_RATE:
                result = result.combine(ValidationResult.invalid_result(
                    f"Heart rate out of range: {heart_rate:.1f}"
                ))
        
        oxygen_saturation = event.get('oxygen_saturation')
        if oxygen_saturation is not None:
            if oxygen_saturation < self.MIN_OXYGEN_SATURATION or oxygen_saturation > self.MAX_OXYGEN_SATURATION:
                result = result.combine(ValidationResult.invalid_result(
                    f"Oxygen saturation out of range: {oxygen_saturation:.1f}"
                ))
        
        temperature = event.get('temperature')
        if temperature is not None:
            if temperature < self.MIN_TEMPERATURE or temperature > self.MAX_TEMPERATURE:
                result = result.combine(ValidationResult.invalid_result(
                    f"Temperature out of range: {temperature:.1f}"
                ))
        
        return result


class CompositeValidator:
    """Combines multiple validators"""
    
    def __init__(self, validators: List):
        self.validators = validators
    
    def validate(self, event: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult.valid_result()
        for validator in self.validators:
            result = result.combine(validator.validate(event))
        return result


class TimestampNormalizationStrategy:
    """Normalizes timestamp format"""
    
    def normalize(self, event: Dict[str, Any]) -> Dict[str, Any]:
        timestamp = event.get('timestamp')
        
        if timestamp is None or 'T' in timestamp:
            return event
        
        try:
            epoch_ms = int(timestamp)
            # Convert epoch milliseconds to ISO format
            dt = datetime.fromtimestamp(epoch_ms / 1000.0)
            iso_timestamp = dt.isoformat()
            event = event.copy()
            event['timestamp'] = iso_timestamp
            return event
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse timestamp: {timestamp}")
            return event


class HrvEnrichmentStrategy:
    """Enriches event with HRV (Heart Rate Variability) calculation"""
    
    def enrich(self, event: Dict[str, Any]) -> Dict[str, Any]:
        if event.get('hrv') is not None or event.get('heart_rate') is None:
            return event
        
        heart_rate = event.get('heart_rate')
        hrv = self._calculate_hrv(heart_rate)
        event = event.copy()
        event['hrv'] = hrv
        return event
    
    def _calculate_hrv(self, heart_rate: float) -> float:
        rr_interval = 60000.0 / heart_rate
        hrv = rr_interval * 0.1
        return max(20.0, min(200.0, hrv))


class ActivityLevelEnrichmentStrategy:
    """Enriches event with default activity level if missing"""
    
    def enrich(self, event: Dict[str, Any]) -> Dict[str, Any]:
        if event.get('activity_level'):
            return event
        event = event.copy()
        event['activity_level'] = 'unknown'
        return event


class TimestampEnrichmentStrategy:
    """Enriches event with processed_at timestamp"""
    
    def enrich(self, event: Dict[str, Any]) -> Dict[str, Any]:
        event = event.copy()
        event['processed_at'] = int(datetime.now().timestamp() * 1000)
        return event


class CompositeEnrichmentStrategy:
    """Applies multiple enrichment strategies"""
    
    def __init__(self, strategies: List):
        self.strategies = strategies
    
    def enrich(self, event: Dict[str, Any]) -> Dict[str, Any]:
        enriched = event
        for strategy in self.strategies:
            enriched = strategy.enrich(enriched)
        return enriched


class EventProcessingService:
    """Main service for processing events"""
    
    def __init__(self):
        self.validator = CompositeValidator([
            RequiredFieldsValidator(),
            VitalSignsRangeValidator()
        ])
        self.normalizer = TimestampNormalizationStrategy()
        self.enricher = CompositeEnrichmentStrategy([
            HrvEnrichmentStrategy(),
            ActivityLevelEnrichmentStrategy(),
            TimestampEnrichmentStrategy()
        ])
    
    def process(self, json_event: str) -> Optional[str]:
        try:
            event = json.loads(json_event)
            
            validation_result = self.validator.validate(event)
            if not validation_result.valid:
                logger.warning(
                    f"Validation failed for event {event.get('message_id')}: "
                    f"{', '.join(validation_result.errors)}"
                )
                return None
            
            processed = self.enricher.enrich(self.normalizer.normalize(event))
            
            result = json.dumps(processed)
            logger.debug(f"Successfully processed event: {event.get('message_id')}")
            return result
        except json.JSONDecodeError as e:
            logger.error(f"Serialization error: {e}")
            return None
        except Exception as e:
            logger.error(f"Processing error for payload: {json_event}", exc_info=True)
            return None


class SensorEventStreamProcessor:
    """Kafka Stream Processor for sensor events"""
    
    def __init__(self, bootstrap_servers: str, input_topic: str, output_topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.processing_service = EventProcessingService()
        
        # Configure consumer
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'health-stream-processor',
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
        
        logger.info(f"Initialized stream processor: input={input_topic}, output={output_topic}")
    
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
        logger.info("Starting stream processing...")
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
            logger.info("Stream processor stopped")


def main():
    """Main entry point"""
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'sensor_raw')
    output_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'processed_data')
    
    processor = SensorEventStreamProcessor(bootstrap_servers, input_topic, output_topic)
    processor.process_stream()


if __name__ == '__main__':
    main()

