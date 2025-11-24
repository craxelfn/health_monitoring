"""Normalizes timestamp format"""
import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)


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

