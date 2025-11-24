"""Enriches event with HRV (Heart Rate Variability) calculation"""
from typing import Dict, Any


class HrvEnrichmentStrategy:
    """Enriches event with HRV (Heart Rate Variability) calculation"""
    
    def enrich(self, event: Dict[str, Any]) -> Dict[str, Any]:
        if event.get('hrv') is not None or event.get('heart_rate') is None:
            return event
        
        heart_rate = event.get('heart_rate')
        hrv = self._calculate_hrv(heart_rate)
        event = event.copy()
        event['hrv'] = hrv
        return event
    
    def _calculate_hrv(self, heart_rate: float) -> float:
        rr_interval = 60000.0 / heart_rate
        hrv = rr_interval * 0.1
        return max(20.0, min(200.0, hrv))

