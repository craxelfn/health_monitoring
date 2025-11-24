"""Enriches event with BMI (Body Mass Index) calculation"""
from typing import Dict, Any


class BmiEnrichmentStrategy:
    """Enriches event with BMI (Body Mass Index) calculation"""
    
    def enrich(self, event: Dict[str, Any]) -> Dict[str, Any]:
        if event.get('bmi') is not None:
            return event
        
        weight_kg = event.get('weight_kg')
        height_m = event.get('height_m')
        
        if weight_kg is not None and height_m is not None and height_m > 0:
            bmi = weight_kg / (height_m ** 2)
            event = event.copy()
            event['bmi'] = round(bmi, 2)
        
        return event

