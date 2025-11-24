"""Validates required fields are present"""
import sys
import os
from typing import Dict, Any

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from validation_result import ValidationResult


class RequiredFieldsValidator:
    """Validates required fields are present"""
    
    def validate(self, event: Dict[str, Any]) -> ValidationResult:
        patient_id = event.get('patient_id')
        if patient_id is None or patient_id == '':
            return ValidationResult.invalid_result("Patient ID is required")
        if not event.get('message_id'):
            return ValidationResult.invalid_result("Message ID is required")
        if not event.get('timestamp'):
            return ValidationResult.invalid_result("Timestamp is required")
        heart_rate = event.get('heart_rate')
        if heart_rate is None or heart_rate <= 0:
            return ValidationResult.invalid_result("Valid heart rate is required")
        return ValidationResult.valid_result()

