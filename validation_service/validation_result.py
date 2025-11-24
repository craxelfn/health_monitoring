"""Validation result with errors"""
from typing import List


class ValidationResult:
    """Validation result with errors"""
    def __init__(self, valid: bool, errors: List[str] = None):
        self.valid = valid
        self.errors = errors or []
    
    @staticmethod
    def valid_result():
        return ValidationResult(True, [])
    
    @staticmethod
    def invalid_result(error: str):
        return ValidationResult(False, [error])
    
    def combine(self, other: 'ValidationResult') -> 'ValidationResult':
        if self.valid and other.valid:
            return ValidationResult.valid_result()
        all_errors = self.errors + other.errors
        return ValidationResult(False, all_errors)

