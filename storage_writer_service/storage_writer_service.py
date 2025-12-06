"""Storage Writer Service that buffers and uploads data to S3"""
import sys
import os
import json
import logging
from typing import Dict, Any
from confluent_kafka import Consumer

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from daily_buffer import DailyBuffer
from local_file_writer import LocalFileWriter
from s3_uploader import S3Uploader
from daily_upload_scheduler import DailyUploadScheduler

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StorageWriterService:
    """Buffers storage events and uploads to S3 daily"""
    
    def __init__(self, bootstrap_servers: str, input_topic: str,
                 s3_bucket: str, aws_access_key_id: str = None,
                 aws_secret_access_key: str = None, upload_time: str = "00:00",
                 local_files_dir: str = '/tmp/storage_files', delete_after_upload: bool = True,
                 file_generation_interval_minutes: int = 3):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        
        # Initialize components
        self.daily_buffer = DailyBuffer()
        self.local_file_writer = LocalFileWriter(base_dir=local_files_dir)
        self.s3_uploader = S3Uploader(
            bucket_name=s3_bucket,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            delete_after_upload=delete_after_upload
        )
        self.upload_scheduler = DailyUploadScheduler(
            daily_buffer=self.daily_buffer,
            local_file_writer=self.local_file_writer,
            s3_uploader=self.s3_uploader,
            upload_time=upload_time,
            file_generation_interval_minutes=file_generation_interval_minutes
        )
        
        # Start upload scheduler
        self.upload_scheduler.start()
        
        # Configure consumer
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'storage-writer-service',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([input_topic])
        
        logger.info(f"Initialized Storage Writer Service: input={input_topic}, bucket={s3_bucket}")
    
    def _process_storage_event(self, event_data: Dict[str, Any]):
        """
        Process storage event: add to daily buffer
        Args:
            event_data: Storage event data
        """
        patient_id = event_data.get('patient_id', 'unknown')
        
        # Add to daily buffer
        self.daily_buffer.add_event(patient_id, event_data)
        
        logger.debug(f"Buffered storage event for patient {patient_id}")
    
    def process_stream(self):
        """Main processing loop"""
        logger.info("Starting storage writer stream processing...")
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
                    event_data = json.loads(value)
                    patient_id = event_data.get('patient_id') or key or 'unknown'
                    
                    # Process storage event
                    self._process_storage_event(event_data)
                    
                    logger.debug(f"Processed storage event for patient {patient_id}")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse storage event: {e}")
                except Exception as e:
                    logger.error(f"Error processing storage event: {e}", exc_info=True)
                    
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.upload_scheduler.stop()
            self.consumer.close()
            logger.info("Storage writer service stopped")


def main():
    """Main entry point"""
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'storage_event')
    s3_bucket = os.getenv('S3_BUCKET', 'health-monitoring-storage')
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    upload_time = os.getenv('UPLOAD_TIME', '00:00')
    local_files_dir = os.getenv('LOCAL_FILES_DIR', '/tmp/storage_files')
    delete_after_upload = os.getenv('DELETE_AFTER_UPLOAD', 'true').lower() == 'true'
    file_generation_interval_minutes = int(os.getenv('FILE_GENERATION_INTERVAL_MINUTES', '3'))
    
    service = StorageWriterService(
        bootstrap_servers,
        input_topic,
        s3_bucket,
        aws_access_key_id,
        aws_secret_access_key,
        upload_time,
        local_files_dir,
        delete_after_upload,
        file_generation_interval_minutes
    )
    service.process_stream()


if __name__ == '__main__':
    main()

