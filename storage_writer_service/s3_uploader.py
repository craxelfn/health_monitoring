"""Uploads local JSON files to S3"""
import os
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Uploader:
    """Uploads local JSON files to S3"""
    
    def __init__(self, bucket_name: str, 
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 region: str = 'us-east-1',
                 delete_after_upload: bool = True):
        """
        Initialize S3 uploader
        Args:
            bucket_name: S3 bucket name
            aws_access_key_id: AWS access key (optional, can use env vars)
            aws_secret_access_key: AWS secret key (optional, can use env vars)
            region: AWS region
            delete_after_upload: Whether to delete local files after successful upload
        """
        self.bucket_name = bucket_name
        self.region = region
        self.delete_after_upload = delete_after_upload
        
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
        
        logger.info(f"Initialized S3 uploader for bucket: {bucket_name}")
    
    def _generate_s3_key(self, date_str: str, patient_id: str) -> str:
        """
        Generate S3 key following path structure: raw/date=YYYY-MM-DD/patient=ID/data.json.gz
        Args:
            date_str: Date in ISO format (YYYY-MM-DD)
            patient_id: Patient ID
        Returns:
            S3 key string
        """
        return f"raw/date={date_str}/patient={patient_id}/data.json.gz"
    
    def upload_local_file(self, local_file_path: str, date_str: str, patient_id: str) -> bool:
        """
        Upload a local file to S3
        Args:
            local_file_path: Path to local file to upload
            date_str: Date in ISO format (YYYY-MM-DD)
            patient_id: Patient ID
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(local_file_path):
            logger.error(f"Local file does not exist: {local_file_path}")
            return False
        
        try:
            # Generate S3 key
            s3_key = self._generate_s3_key(date_str, patient_id)
            
            # Get file size for logging
            file_size = os.path.getsize(local_file_path)
            
            # Read file content
            with open(local_file_path, 'rb') as f:
                file_content = f.read()
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=file_content,
                ContentType='application/gzip',
                ContentEncoding='gzip',
                Metadata={
                    'patient_id': patient_id,
                    'date': date_str,
                    'upload_timestamp': datetime.now().isoformat()
                }
            )
            
            logger.info(
                f"Successfully uploaded local file to S3: "
                f"s3://{self.bucket_name}/{s3_key} "
                f"({file_size / 1024:.2f} KB)"
            )
            
            # Delete local file if configured
            if self.delete_after_upload:
                try:
                    os.remove(local_file_path)
                    logger.debug(f"Deleted local file after successful upload: {local_file_path}")
                except Exception as e:
                    logger.warning(f"Could not delete local file {local_file_path}: {e}")
            
            return True
            
        except ClientError as e:
            logger.error(f"S3 upload error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading to S3: {e}", exc_info=True)
            return False
    
    def upload_daily_files(self, date_str: str, patient_file_paths: Dict[str, str]) -> int:
        """
        Upload multiple local files for all patients on a specific date
        Args:
            date_str: Date in ISO format (YYYY-MM-DD)
            patient_file_paths: Dictionary mapping patient_id to local file path
        Returns:
            Number of successful uploads
        """
        success_count = 0
        for patient_id, file_path in patient_file_paths.items():
            if self.upload_local_file(file_path, date_str, patient_id):
                success_count += 1
        return success_count

