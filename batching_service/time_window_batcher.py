"""Time window batcher for grouping events into 3-second windows"""
import time
import logging
from typing import Dict, Any, List, Optional
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)


class TimeWindowBatcher:
    """Batches events into 3-second time windows per patient"""
    
    def __init__(self, window_seconds: int = 3):
        self.window_seconds = window_seconds
        # patient_id -> {events: [], window_start: timestamp}
        self.batches: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            'events': [],
            'window_start': None
        })
        logger.info(f"Initialized TimeWindowBatcher with {window_seconds}s windows")
    
    def add_event(self, patient_id: str, event: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """
        Add event to batch. Returns batch if window is complete, None otherwise.
        """
        current_time = time.time()
        batch = self.batches[patient_id]
        
        # Initialize window start if this is the first event
        if batch['window_start'] is None:
            batch['window_start'] = current_time
        
        # Add event to batch
        batch['events'].append(event)
        
        # Check if window is complete (3 seconds elapsed)
        elapsed = current_time - batch['window_start']
        if elapsed >= self.window_seconds:
            # Window complete, return batch and reset
            completed_batch = batch['events'].copy()
            self.batches[patient_id] = {
                'events': [],
                'window_start': None
            }
            logger.debug(f"Window complete for patient {patient_id}: {len(completed_batch)} events")
            return completed_batch
        
        return None
    
    def get_pending_batches(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get all pending batches that should be flushed (timeout)"""
        current_time = time.time()
        pending = {}
        
        for patient_id, batch in list(self.batches.items()):
            if batch['window_start'] is not None:
                elapsed = current_time - batch['window_start']
                if elapsed >= self.window_seconds and len(batch['events']) > 0:
                    pending[patient_id] = batch['events'].copy()
                    # Reset batch
                    self.batches[patient_id] = {
                        'events': [],
                        'window_start': None
                    }
        
        return pending

