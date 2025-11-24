"""Enriches event with default activity level if missing"""
from typing import Dict, Any


class ActivityLevelEnrichmentStrategy:
    """Enriches event with default activity level if missing"""
    
    def enrich(self, event: Dict[str, Any]) -> Dict[str, Any]:
        if event.get('activity_level'):
            return event
        event = event.copy()
        event['activity_level'] = 'unknown'
        return event

