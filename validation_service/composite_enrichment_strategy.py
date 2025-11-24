"""Applies multiple enrichment strategies"""
import sys
import os
from typing import List, Dict, Any

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from hrv_enrichment_strategy import HrvEnrichmentStrategy
from bmi_enrichment_strategy import BmiEnrichmentStrategy
from activity_level_enrichment_strategy import ActivityLevelEnrichmentStrategy
from timestamp_enrichment_strategy import TimestampEnrichmentStrategy


class CompositeEnrichmentStrategy:
    """Applies multiple enrichment strategies"""
    
    def __init__(self, strategies: List = None):
        if strategies is None:
            self.strategies = [
                HrvEnrichmentStrategy(),
                BmiEnrichmentStrategy(),
                ActivityLevelEnrichmentStrategy(),
                TimestampEnrichmentStrategy()
            ]
        else:
            self.strategies = strategies
    
    def enrich(self, event: Dict[str, Any]) -> Dict[str, Any]:
        enriched = event
        for strategy in self.strategies:
            enriched = strategy.enrich(enriched)
        return enriched

