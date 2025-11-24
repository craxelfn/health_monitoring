"""Combines multiple validators"""
import sys
import os
from typing import List, Dict, Any

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from validation_result import ValidationResult
from required_fields_validator import RequiredFieldsValidator
from vital_signs_range_validator import VitalSignsRangeValidator


class CompositeValidator:
    """Combines multiple validators"""
    
    def __init__(self, validators: List = None):
        if validators is None:
            self.validators = [
                RequiredFieldsValidator(),
                VitalSignsRangeValidator()
            ]
        else:
            self.validators = validators
    
    def validate(self, event: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult.valid_result()
        for validator in self.validators:
            result = result.combine(validator.validate(event))
        return result

