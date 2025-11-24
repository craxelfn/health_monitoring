"""Main service for processing events"""
import sys
import os
import json
import logging
from typing import Optional

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from composite_validator import CompositeValidator
from field_name_normalization_strategy import FieldNameNormalizationStrategy
from timestamp_normalization_strategy import TimestampNormalizationStrategy
from composite_enrichment_strategy import CompositeEnrichmentStrategy

logger = logging.getLogger(__name__)


class EventProcessingService:
    """Main service for processing events"""
    
    def __init__(self):
        self.validator = CompositeValidator()
        self.field_normalizer = FieldNameNormalizationStrategy()
        self.timestamp_normalizer = TimestampNormalizationStrategy()
        self.enricher = CompositeEnrichmentStrategy()
    
    def process(self, json_event: str) -> Optional[str]:
        try:
            event = json.loads(json_event)
            
            # First normalize field names from CSV format to snake_case
            event = self.field_normalizer.normalize(event)
            
            # Then validate
            validation_result = self.validator.validate(event)
            if not validation_result.valid:
                logger.warning(
                    f"Validation failed for event {event.get('message_id')}: "
                    f"{', '.join(validation_result.errors)}"
                )
                return None
            
            # Normalize timestamp and enrich
            processed = self.enricher.enrich(self.timestamp_normalizer.normalize(event))
            
            result = json.dumps(processed)
            logger.debug(f"Successfully processed event: {processed.get('message_id')} for patient {processed.get('patient_id')}")
            return result
        except json.JSONDecodeError as e:
            logger.error(f"Serialization error: {e}")
            return None
        except Exception as e:
            logger.error(f"Processing error for payload: {json_event}", exc_info=True)
            return None

