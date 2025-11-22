#!/usr/bin/env python3
"""
AI Anomaly Detection Service
Reads processed sensor events, batches them using SQLite, calls ML model endpoint,
and writes inference results to S3.
"""

import json
import logging
import os
import time
import threading
import sqlite3
from datetime import datetime
from typing import Dict, Any, List, Optional
from collections import defaultdict
from confluent_kafka import Consumer, Producer
import requests
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SQLiteBatcher:
    """SQLite-backed batching to prevent race conditions"""
    
    def __init__(self, db_path: str = '/tmp/anomaly_batch_db'):
        self.db_path = db_path
        os.makedirs(db_path, exist_ok=True)
        
        # SQLite connection with thread-safe mode
        db_file = os.path.join(db_path, 'batches.db')
        self.conn = sqlite3.connect(db_file, check_same_thread=False, timeout=30.0)
        self.conn.execute('PRAGMA journal_mode=WAL')  # Write-Ahead Logging for better concurrency
        self.lock = threading.Lock()
        
        # Create table if not exists
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS batches (
                patient_id TEXT PRIMARY KEY,
                events TEXT NOT NULL,
                created_at REAL NOT NULL,
                last_updated REAL NOT NULL
            )
        ''')
        self.conn.commit()
        logger.info(f"Initialized SQLite batcher at {db_file}")
    
    def add_event(self, patient_id: str, event: Dict[str, Any]) -> None:
        """Add event to batch for a patient"""
        with self.lock:
            # Get existing batch or create new one
            cursor = self.conn.execute(
                'SELECT events, created_at FROM batches WHERE patient_id = ?',
                (patient_id,)
            )
            row = cursor.fetchone()
            
            if row:
                batch = json.loads(row[0])
                created_at = row[1]
            else:
                batch = {
                    'patient_id': patient_id,
                    'events': [],
                    'created_at': time.time()
                }
                created_at = time.time()
            
            batch['events'].append(event)
            batch['last_updated'] = time.time()
            
            # Update or insert
            self.conn.execute(
                'INSERT OR REPLACE INTO batches (patient_id, events, created_at, last_updated) VALUES (?, ?, ?, ?)',
                (patient_id, json.dumps(batch), created_at, time.time())
            )
            self.conn.commit()
    
    def get_batch(self, patient_id: str) -> Optional[Dict[str, Any]]:
        """Get batch for a patient"""
        with self.lock:
            cursor = self.conn.execute(
                'SELECT events FROM batches WHERE patient_id = ?',
                (patient_id,)
            )
            row = cursor.fetchone()
            if row:
                batch = json.loads(row[0])
                return batch
            return None
    
    def remove_batch(self, patient_id: str) -> None:
        """Remove batch after processing"""
        with self.lock:
            self.conn.execute('DELETE FROM batches WHERE patient_id = ?', (patient_id,))
            self.conn.commit()
    
    def get_all_patients(self) -> List[str]:
        """Get all patient IDs with pending batches"""
        with self.lock:
            cursor = self.conn.execute('SELECT patient_id FROM batches')
            return [row[0] for row in cursor.fetchall()]
    
    def close(self):
        """Close SQLite connection"""
        self.conn.close()


class MLModelClient:
    """Client for calling ML model endpoint"""
    
    def __init__(self, endpoint_url: str, timeout: int = 30):
        self.endpoint_url = endpoint_url
        self.timeout = timeout
        logger.info(f"Initialized ML model client with endpoint: {endpoint_url}")
    
    def predict(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Call ML model endpoint with batch of events
        Returns predictions and anomaly scores
        """
        try:
            # Prepare features for model
            features = self._extract_features(events)
            
            # Call model endpoint
            response = requests.post(
                self.endpoint_url,
                json={'features': features, 'events': events},
                timeout=self.timeout,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            
            result = response.json()
            logger.debug(f"ML model prediction successful for {len(events)} events")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"ML model endpoint error: {e}")
            # Return default prediction (no anomaly) if model fails
            return {
                'predictions': [{'anomaly': False, 'score': 0.0} for _ in events],
                'model_error': str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error calling ML model: {e}", exc_info=True)
            return {
                'predictions': [{'anomaly': False, 'score': 0.0} for _ in events],
                'model_error': str(e)
            }
    
    def _extract_features(self, events: List[Dict[str, Any]]) -> List[List[float]]:
        """Extract feature vectors from events for ML model"""
        features = []
        for event in events:
            feature_vector = [
                event.get('heart_rate', 0.0),
                event.get('temperature', 0.0),
                event.get('oxygen_saturation', 0.0),
                event.get('blood_pressure_systolic', 0.0),
                event.get('blood_pressure_diastolic', 0.0),
                event.get('hrv', 0.0),
                event.get('age', 0.0) if 'age' in event else 0.0,
                event.get('weight_kg', 0.0) if 'weight_kg' in event else 0.0,
                event.get('height_m', 0.0) if 'height_m' in event else 0.0,
            ]
            features.append(feature_vector)
        return features


class S3Writer:
    """Writes inference results to S3"""
    
    def __init__(self, bucket_name: str, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None, region: str = 'us-east-1'):
        self.bucket_name = bucket_name
        self.region = region
        
        # Initialize S3 client
        if aws_access_key_id and aws_secret_access_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region
            )
        else:
            # Use default credentials (environment variables, IAM role, etc.)
            self.s3_client = boto3.client('s3', region_name=region)
        
        logger.info(f"Initialized S3 writer for bucket: {bucket_name}")
    
    def write_inference_result(self, patient_id: str, batch: Dict[str, Any],
                              predictions: Dict[str, Any]) -> bool:
        """Write inference results to S3"""
        try:
            # Prepare result document
            result = {
                'patient_id': patient_id,
                'timestamp': datetime.now().isoformat(),
                'batch_size': len(batch['events']),
                'events': batch['events'],
                'predictions': predictions.get('predictions', []),
                'model_metadata': {
                    'endpoint_called': True,
                    'model_error': predictions.get('model_error')
                }
            }
            
            # Generate S3 key (path)
            timestamp_str = datetime.now().strftime('%Y%m%d/%H%M%S')
            s3_key = f"anomaly-detection/{timestamp_str}/{patient_id}_{int(time.time())}.json"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(result, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"Successfully wrote inference results to S3: s3://{self.bucket_name}/{s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"S3 write error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error writing to S3: {e}", exc_info=True)
            return False


class AnomalyDetectionService:
    """Main anomaly detection service"""
    
    def __init__(self, bootstrap_servers: str, input_topic: str, output_topic: str,
                 ml_endpoint_url: str, s3_bucket: str, batch_size: int = 5,
                 batch_timeout_seconds: int = 3, s3_aws_key: Optional[str] = None,
                 s3_aws_secret: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds
        
        # Initialize components
        self.batcher = SQLiteBatcher()
        self.ml_client = MLModelClient(ml_endpoint_url)
        self.s3_writer = S3Writer(s3_bucket, s3_aws_key, s3_aws_secret)
        
        # Configure Kafka consumer
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'anomaly-detection-service',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([input_topic])
        
        # Configure Kafka producer (for alerts)
        producer_config = {
            'bootstrap.servers': bootstrap_servers
        }
        self.producer = Producer(producer_config)
        
        # Track batch timestamps
        self.batch_timestamps = defaultdict(float)
        
        # Start batch processing thread
        self.running = True
        self.batch_thread = threading.Thread(target=self._process_batches_periodically, daemon=True)
        self.batch_thread.start()
        
        logger.info(f"Initialized Anomaly Detection Service: input={input_topic}, output={output_topic}")
    
    def _process_batches_periodically(self):
        """Periodically check and process batches that meet criteria"""
        while self.running:
            try:
                current_time = time.time()
                patients = self.batcher.get_all_patients()
                
                for patient_id in patients:
                    batch = self.batcher.get_batch(patient_id)
                    if not batch:
                        continue
                    
                    events = batch.get('events', [])
                    created_at = batch.get('created_at', current_time)
                    last_updated = batch.get('last_updated', current_time)
                    
                    # Check if batch should be processed
                    should_process = False
                    
                    # Check batch size trigger
                    if len(events) >= self.batch_size:
                        should_process = True
                        logger.debug(f"Batch size trigger for patient {patient_id}: {len(events)} >= {self.batch_size}")
                    
                    # Check timeout trigger
                    elapsed = current_time - created_at
                    if elapsed >= self.batch_timeout_seconds:
                        should_process = True
                        logger.debug(f"Timeout trigger for patient {patient_id}: {elapsed:.2f}s >= {self.batch_timeout_seconds}s")
                    
                    if should_process:
                        self._process_batch(patient_id, batch)
                
                # Sleep before next check
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error in batch processing thread: {e}", exc_info=True)
                time.sleep(1)
    
    def _process_batch(self, patient_id: str, batch: Dict[str, Any]):
        """Process a batch: call ML model and write to S3"""
        try:
            events = batch.get('events', [])
            if not events:
                return
            
            logger.info(f"Processing batch for patient {patient_id}: {len(events)} events")
            
            # Call ML model
            predictions = self.ml_client.predict(events)
            
            # Write to S3
            self.s3_writer.write_inference_result(patient_id, batch, predictions)
            
            # Check for anomalies and publish alerts
            anomaly_detected = False
            for pred in predictions.get('predictions', []):
                if pred.get('anomaly', False):
                    anomaly_detected = True
                    break
            
            if anomaly_detected:
                self._publish_anomaly_alert(patient_id, batch, predictions)
            
            # Remove processed batch
            self.batcher.remove_batch(patient_id)
            
            logger.info(f"Successfully processed batch for patient {patient_id}")
            
        except Exception as e:
            logger.error(f"Error processing batch for patient {patient_id}: {e}", exc_info=True)
    
    def _publish_anomaly_alert(self, patient_id: str, batch: Dict[str, Any],
                               predictions: Dict[str, Any]):
        """Publish anomaly alert to Kafka"""
        try:
            alert = {
                'patient_id': patient_id,
                'timestamp': datetime.now().isoformat(),
                'batch_size': len(batch['events']),
                'anomalies': [
                    {
                        'event': event,
                        'prediction': pred
                    }
                    for event, pred in zip(batch['events'], predictions.get('predictions', []))
                    if pred.get('anomaly', False)
                ]
            }
            
            self.producer.produce(
                self.output_topic,
                key=patient_id.encode('utf-8'),
                value=json.dumps(alert).encode('utf-8'),
                callback=self._delivery_callback
            )
            self.producer.poll(0)
            
            logger.warning(f"Published anomaly alert for patient {patient_id}")
            
        except Exception as e:
            logger.error(f"Error publishing anomaly alert: {e}", exc_info=True)
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery"""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Alert delivered to {msg.topic()} [{msg.partition()}]")
    
    def process_stream(self):
        """Main processing loop"""
        logger.info("Starting anomaly detection stream processing...")
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
                    self.batcher.add_event(patient_id, event)
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
            self.batcher.close()
            logger.info("Anomaly detection service stopped")


def main():
    """Main entry point"""
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'processed_data')
    output_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'anomaly_alert')
    ml_endpoint_url = os.getenv('ML_MODEL_ENDPOINT', 'http://ml-model:8080/predict')
    s3_bucket = os.getenv('S3_BUCKET', 'health-monitoring-inferences')
    batch_size = int(os.getenv('BATCH_SIZE', '5'))
    batch_timeout = int(os.getenv('BATCH_TIMEOUT_SECONDS', '3'))
    s3_aws_key = os.getenv('AWS_ACCESS_KEY_ID')
    s3_aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    service = AnomalyDetectionService(
        bootstrap_servers=bootstrap_servers,
        input_topic=input_topic,
        output_topic=output_topic,
        ml_endpoint_url=ml_endpoint_url,
        s3_bucket=s3_bucket,
        batch_size=batch_size,
        batch_timeout_seconds=batch_timeout,
        s3_aws_key=s3_aws_key,
        s3_aws_secret=s3_aws_secret
    )
    
    service.process_stream()


if __name__ == '__main__':
    main()

