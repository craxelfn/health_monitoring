"""Enriches event with processed_at timestamp"""
from datetime import datetime
from typing import Dict, Any


class TimestampEnrichmentStrategy:
    """Enriches event with processed_at timestamp"""
    
    def enrich(self, event: Dict[str, Any]) -> Dict[str, Any]:
        event = event.copy()
        event['processed_at'] = int(datetime.now().timestamp() * 1000)
        return event

