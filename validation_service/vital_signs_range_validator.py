"""Validates vital signs are within acceptable ranges"""
import sys
import os
from typing import Dict, Any

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from validation_result import ValidationResult


class VitalSignsRangeValidator:
    """Validates vital signs are within acceptable ranges"""
    
    MIN_HEART_RATE = 30.0
    MAX_HEART_RATE = 250.0
    MIN_OXYGEN_SATURATION = 70.0
    MAX_OXYGEN_SATURATION = 100.0
    MIN_TEMPERATURE = 35.0
    MAX_TEMPERATURE = 42.0
    
    def validate(self, event: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult.valid_result()
        
        heart_rate = event.get('heart_rate')
        if heart_rate is not None:
            if heart_rate < self.MIN_HEART_RATE or heart_rate > self.MAX_HEART_RATE:
                result = result.combine(ValidationResult.invalid_result(
                    f"Heart rate out of range: {heart_rate:.1f}"
                ))
        
        oxygen_saturation = event.get('oxygen_saturation')
        if oxygen_saturation is not None:
            if oxygen_saturation < self.MIN_OXYGEN_SATURATION or oxygen_saturation > self.MAX_OXYGEN_SATURATION:
                result = result.combine(ValidationResult.invalid_result(
                    f"Oxygen saturation out of range: {oxygen_saturation:.1f}"
                ))
        
        temperature = event.get('temperature')
        if temperature is not None:
            if temperature < self.MIN_TEMPERATURE or temperature > self.MAX_TEMPERATURE:
                result = result.combine(ValidationResult.invalid_result(
                    f"Temperature out of range: {temperature:.1f}"
                ))
        
        return result

