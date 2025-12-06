"""Writes JSON files locally before uploading to S3"""
import os
import json
import gzip
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class LocalFileWriter:
    """Writes JSON files locally in organized directory structure"""
    
    def __init__(self, base_dir: str = '/tmp/storage_files'):
        """
        Initialize local file writer
        Args:
            base_dir: Base directory for storing local JSON files
        """
        self.base_dir = base_dir
        self._ensure_base_dir()
        logger.info(f"Initialized LocalFileWriter with base directory: {base_dir}")
    
    def _ensure_base_dir(self):
        """Ensure base directory exists"""
        try:
            os.makedirs(self.base_dir, exist_ok=True)
        except Exception as e:
            logger.warning(f"Could not create base directory {self.base_dir}: {e}")
            self.base_dir = None
    
    def _get_file_path(self, date_str: str, patient_id: str) -> str:
        """
        Generate local file path following structure: base_dir/raw/date=YYYY-MM-DD/patient=ID/data.json.gz
        Args:
            date_str: Date in ISO format (YYYY-MM-DD)
            patient_id: Patient ID
        Returns:
            Full file path
        """
        if not self.base_dir:
            raise ValueError("Base directory not available")
        
        # Create directory structure
        dir_path = os.path.join(
            self.base_dir,
            'raw',
            f'date={date_str}',
            f'patient={patient_id}'
        )
        os.makedirs(dir_path, exist_ok=True)
        
        # File path
        file_path = os.path.join(dir_path, 'data.json.gz')
        return file_path
    
    def write_patient_data(self, date_str: str, patient_id: str, events: List[Dict[str, Any]]) -> Optional[str]:
        """
        Write patient data to local JSON file (compressed)
        This will overwrite the file with all current events from the buffer.
        Args:
            date_str: Date in ISO format (YYYY-MM-DD)
            patient_id: Patient ID
            events: List of events to write (all events for this patient/date)
        Returns:
            File path if successful, None otherwise
        """
        if not events:
            logger.warning(f"No events to write for patient {patient_id} on {date_str}")
            return None
        
        if not self.base_dir:
            logger.error("Base directory not available")
            return None
        
        try:
            # Get file path
            file_path = self._get_file_path(date_str, patient_id)
            
            # Check if file exists (for logging)
            file_exists = os.path.exists(file_path)
            
            # Prepare data with all events (this represents the complete state for the day)
            data = {
                'patient_id': patient_id,
                'date': date_str,
                'last_updated_timestamp': datetime.now().isoformat(),
                'event_count': len(events),
                'events': events
            }
            
            # If file exists, add created timestamp from original file
            if file_exists:
                try:
                    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                        existing_data = json.loads(f.read())
                        if 'created_timestamp' in existing_data:
                            data['created_timestamp'] = existing_data['created_timestamp']
                except:
                    pass
            
            # If no created_timestamp, set it now
            if 'created_timestamp' not in data:
                data['created_timestamp'] = datetime.now().isoformat()
            
            # Convert to JSON
            json_data = json.dumps(data, indent=2)
            
            # Write compressed JSON file (overwrites existing file)
            with gzip.open(file_path, 'wt', encoding='utf-8') as f:
                f.write(json_data)
            
            file_size = os.path.getsize(file_path)
            action = "Updated" if file_exists else "Created"
            logger.info(
                f"{action} local file with {len(events)} events: {file_path} "
                f"({file_size / 1024:.2f} KB)"
            )
            
            return file_path
            
        except Exception as e:
            logger.error(f"Error writing local file: {e}", exc_info=True)
            return None
    
    def file_exists(self, date_str: str, patient_id: str) -> bool:
        """
        Check if file exists for patient and date
        Args:
            date_str: Date in ISO format (YYYY-MM-DD)
            patient_id: Patient ID
        Returns:
            True if file exists, False otherwise
        """
        try:
            file_path = self._get_file_path(date_str, patient_id)
            return os.path.exists(file_path)
        except:
            return False
    
    def get_file_path(self, date_str: str, patient_id: str) -> Optional[str]:
        """
        Get file path for patient and date
        Args:
            date_str: Date in ISO format (YYYY-MM-DD)
            patient_id: Patient ID
        Returns:
            File path if exists, None otherwise
        """
        try:
            file_path = self._get_file_path(date_str, patient_id)
            if os.path.exists(file_path):
                return file_path
            return None
        except:
            return None
    
    def delete_file(self, file_path: str) -> bool:
        """
        Delete a local file
        Args:
            file_path: Path to file to delete
        Returns:
            True if successful, False otherwise
        """
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.debug(f"Deleted local file: {file_path}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting file {file_path}: {e}")
            return False

