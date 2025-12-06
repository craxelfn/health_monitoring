"""Manages daily buffering of storage events"""
import os
import json
import logging
from datetime import datetime, date
from typing import Dict, Any, List
from collections import defaultdict

logger = logging.getLogger(__name__)


class DailyBuffer:
    """Buffers storage events by date and patient ID"""
    
    def __init__(self, buffer_dir: str = '/tmp/storage_buffer'):
        """
        Initialize daily buffer
        Args:
            buffer_dir: Directory to store buffered data
        """
        self.buffer_dir = buffer_dir
        self._ensure_buffer_dir()
        # patient_id -> date -> list of events
        self.buffer: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    
    def _ensure_buffer_dir(self):
        """Ensure buffer directory exists"""
        try:
            os.makedirs(self.buffer_dir, exist_ok=True)
        except Exception as e:
            logger.warning(f"Could not create buffer directory {self.buffer_dir}: {e}")
            self.buffer_dir = None
    
    def add_event(self, patient_id: str, event: Dict[str, Any]):
        """
        Add event to buffer
        Args:
            patient_id: Patient ID
            event: Storage event data
        """
        # Get current date
        current_date = date.today().isoformat()
        
        # Add to in-memory buffer
        self.buffer[patient_id][current_date].append(event)
        
        logger.debug(f"Added event to buffer: patient={patient_id}, date={current_date}")
    
    def get_patients_for_date(self, target_date: str) -> List[str]:
        """
        Get list of patients with data for a specific date
        Args:
            target_date: Date in ISO format (YYYY-MM-DD)
        Returns:
            List of patient IDs
        """
        patients = []
        for patient_id, dates in self.buffer.items():
            if target_date in dates:
                patients.append(patient_id)
        return patients
    
    def get_events_for_patient_date(self, patient_id: str, target_date: str) -> List[Dict[str, Any]]:
        """
        Get all events for a patient on a specific date
        Args:
            patient_id: Patient ID
            target_date: Date in ISO format (YYYY-MM-DD)
        Returns:
            List of events
        """
        return self.buffer.get(patient_id, {}).get(target_date, [])
    
    def clear_patient_date(self, patient_id: str, target_date: str):
        """
        Clear buffered data for a patient on a specific date
        Args:
            patient_id: Patient ID
            target_date: Date in ISO format (YYYY-MM-DD)
        """
        if patient_id in self.buffer and target_date in self.buffer[patient_id]:
            del self.buffer[patient_id][target_date]
            logger.debug(f"Cleared buffer for patient={patient_id}, date={target_date}")
    
    def get_all_dates(self) -> List[str]:
        """
        Get all dates that have buffered data
        Returns:
            List of dates in ISO format
        """
        dates = set()
        for patient_data in self.buffer.values():
            dates.update(patient_data.keys())
        return sorted(list(dates))

