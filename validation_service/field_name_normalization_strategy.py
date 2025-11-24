"""Normalizes field names from CSV format to snake_case"""
import uuid
import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)


class FieldNameNormalizationStrategy:
    """Normalizes field names from CSV format to snake_case"""
    
    FIELD_MAPPING = {
        'Patient ID': 'patient_id',
        'patient_id': 'patient_id',  # Already correct
        'Age': 'age',
        'Gender_Male': 'gender_male',
        'Weight (kg)': 'weight_kg',
        'Height (m)': 'height_m',
        'Heart Rate': 'heart_rate',
        'Body Temperature': 'temperature',
        'Oxygen Saturation': 'oxygen_saturation',
        'Systolic Blood Pressure': 'blood_pressure_systolic',
        'Diastolic Blood Pressure': 'blood_pressure_diastolic',
        'Derived_HRV': 'hrv',
        'Derived_Pulse_Pressure': 'pulse_pressure',
        'Derived_BMI': 'bmi',
        'Derived_MAP': 'map',
        'Timestamp': 'timestamp',
        'timestamp': 'timestamp',  # Already correct
        'message_id': 'message_id',
        'Message ID': 'message_id',
    }
    
    def normalize(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Convert field names from CSV format to snake_case"""
        normalized = {}
        for key, value in event.items():
            # Map to snake_case if mapping exists, otherwise keep original
            new_key = self.FIELD_MAPPING.get(key, key.lower().replace(' ', '_').replace('(', '').replace(')', ''))
            normalized[new_key] = value
        
        # Ensure patient_id is properly extracted (handle both "Patient ID" and "patient_id")
        patient_id = normalized.get('patient_id')
        if patient_id is None or patient_id == '':
            # Try to get from original event's "Patient ID" field
            if 'Patient ID' in event:
                patient_id = event['Patient ID']
            # Also try lowercase version
            elif 'patient id' in event:
                patient_id = event['patient id']
        
        # Always ensure patient_id is set and is a string
        if patient_id is not None and patient_id != '':
            normalized['patient_id'] = str(patient_id)
        
        # Ensure message_id exists (generate if missing)
        if 'message_id' not in normalized or normalized['message_id'] is None:
            normalized['message_id'] = str(uuid.uuid4())
        
        # Normalize timestamp if needed
        timestamp = normalized.get('timestamp')
        if timestamp:
            normalized['timestamp'] = self._normalize_timestamp(timestamp)
        
        logger.debug(f"Normalized event: patient_id={normalized.get('patient_id')}, message_id={normalized.get('message_id')}")
        return normalized
    
    def _normalize_timestamp(self, timestamp: Any) -> str:
        """Normalize timestamp to ISO format"""
        if timestamp is None:
            return datetime.now().isoformat()
        
        if isinstance(timestamp, str):
            if 'T' in timestamp:
                return timestamp
            try:
                # Try parsing as datetime string
                dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
                return dt.isoformat()
            except (ValueError, TypeError):
                try:
                    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                    return dt.isoformat()
                except (ValueError, TypeError):
                    pass
        
        try:
            epoch_ms = int(timestamp)
            dt = datetime.fromtimestamp(epoch_ms / 1000.0)
            return dt.isoformat()
        except (ValueError, TypeError):
            logger.warning(f"Could not parse timestamp: {timestamp}")
            return datetime.now().isoformat()

